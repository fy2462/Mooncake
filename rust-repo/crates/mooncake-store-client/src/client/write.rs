// ============================================================================
// Write operations: put, put_from, put_parts, batch_put
// 写操作：put、put_from、put_parts、batch_put
//
// C++ equivalent: real_client.cpp Put() / PutFrom() / PutParts() / BatchPut()
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicateConfig, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Put — three-phase write lifecycle (三阶段写生命周期):
    //   put_start  →  write_to_replica (per replica)  →  put_end
    //
    // If any replica write fails, PutRevoke is called to roll back the
    // allocation on the master. This matches the C++ behaviour.
    //
    // 如果任何副本写入失败，调用 PutRevoke 在 master 上回滚分配。
    // 这与 C++ 行为一致。
    // -----------------------------------------------------------------------

    /// Store a key-value pair into the Mooncake distributed store.
    ///
    /// # Three-phase lifecycle (三阶段生命周期)
    ///
    /// 1. **put_start** — ask the master to allocate replicas (Memory + NoF_SSD +
    ///    Disk, depending on `config`). The master returns a list of
    ///    `ReplicaDescriptor` with segment, offset, and base_addr.
    ///
    ///    请求 master 分配副本（Memory + NoF_SSD + Disk，取决于 config）。
    ///    master 返回包含 segment、offset 和 base_addr 的 ReplicaDescriptor 列表。
    ///    C++ 等价：`Client::PutStart()`。
    ///
    /// 2. **write_to_replica** — for each allocated replica, write the data via
    ///    local_memcpy (same-node fast path) or TransferEngine (RDMA/TCP).
    ///
    ///    对每个已分配的副本，通过 local_memcpy（同节点快速路径）或
    ///    TransferEngine（RDMA/TCP）写入数据。
    ///    C++ 等价：`Client::WriteToReplica()`。
    ///
    /// 3. **put_end** — notify the master that all replicas have been written,
    ///    committing the put. The replicas transition from Allocating to Written
    ///    status.
    ///
    ///    通知 master 所有副本已写入，提交本次 put。
    ///    副本状态从 Allocating 转换为 Written。
    ///    C++ 等价：`Client::PutEnd()`。
    ///
    /// # Error handling (错误处理)
    ///
    /// If any replica write fails, **PutRevoke** is sent to the master to release
    /// the allocated resources. Without this, the master would leak replica
    /// allocations.
    ///
    /// 如果任何副本写入失败，向 master 发送 PutRevoke 释放已分配的资源。
    /// 否则 master 会泄漏副本分配。C++ 等价：`Client::PutRevoke()`。
    ///
    /// # Arguments
    /// - `key` — unique key (must be non-empty). / 唯一键（必须非空）。
    /// - `value` — the payload bytes (must be non-empty). / 负载字节（必须非空）。
    /// - `config` — replication configuration (replica count, pinning, preferred
    ///   segments, etc.). Defaults are used if `None`.
    ///   复制配置（副本数、锁定、首选 segment 等）。如果为 None 则使用默认值。
    pub async fn put(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        tracing::info!(target: "te_debug", %key, value_len = value.len(), "put: ENTER");

        // Guard: empty key or value is invalid. / 守卫：空 key 或 value 无效。
        if key.is_empty() || value.is_empty() {
            tracing::error!(target: "te_debug", %key, "put: empty key or value");
            return Err(StoreError::InvalidParams(
                "key is empty or value has zero length".to_string(),
            ));
        }
        let cfg = config.unwrap_or_default();

        // Phase 1: put_start — allocate replicas on master.
        // 阶段 1：put_start —— 在 master 上分配副本。
        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: value.len() as u64,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        tracing::info!(target: "te_debug", %key, "put: calling put_start");
        let response = self
            .master
            .put_start(request)
            .await
            .map_err(|e| {
                tracing::error!(target: "te_debug", %key, error = %e, "put: put_start FAILED");
                StoreError::Internal(e.to_string())
            })?
            .into_inner();

        let replicas = self.replicas_from_proto(&response.replicas);
        tracing::info!(target: "te_debug", %key, replica_count = replicas.len(), "put: replicas allocated");
        if replicas.is_empty() {
            tracing::error!(target: "te_debug", %key, "put: no replicas allocated");
            return Err(StoreError::NoAvailableHandle);
        }

        // Phase 2: write_to_replica — write data to each allocated replica.
        // 阶段 2：write_to_replica —— 向每个已分配副本写入数据。
        for (i, replica) in replicas.iter().enumerate() {
            tracing::info!(
                target: "te_debug", %key, replica_idx = i, total = replicas.len(),
                seg_name = %replica.segment_name,
                "put: writing to replica"
            );
            if let Err(e) = self.write_to_replica(replica, value).await {
                tracing::error!(target: "te_debug", %key, replica_idx = i, error = %e, "put: write_to_replica FAILED, revoking");
                // On any replica write failure, revoke the entire allocation.
                // C++ 写失败时调用 PutRevoke 撤销已分配的资源。
                // 任何一个副本写入失败，撤销整个分配。
                let revoke_req = proto::PutRevokeRequest {
                    client_id: Some(self.client_id_proto()),
                    key: key.to_string(),
                    replica_type: 0,
                    tenant_id: String::new(),
                };
                let _ = self.master.put_revoke(revoke_req).await;
                return Err(e);
            }
        }

        // Phase 3: put_end — commit the put, transitioning replicas to Written.
        // 阶段 3：put_end —— 提交本次 put，将副本状态转换为 Written。
        tracing::info!(target: "te_debug", %key, "put: calling put_end");
        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
            tenant_id: String::new(),
        };
        self.master
            .put_end(end_request)
            .await
            .map_err(|e| {
                tracing::error!(target: "te_debug", %key, error = %e, "put: put_end FAILED");
                StoreError::Internal(e.to_string())
            })?;

        tracing::info!(target: "te_debug", %key, "put: EXIT (success)");
        Ok(())
    }

    /// Zero-copy put: write data directly from a caller-provided buffer via
    /// RDMA/TCP, bypassing the internal `local_buffer`.
    ///
    /// Uses the same three-phase lifecycle as [`put`](Self::put).
    ///
    /// 零拷贝写入：通过 RDMA/TCP 直接从调用者提供的缓冲区写入数据，
    /// 绕过内部 local_buffer。使用与 put() 相同的三阶段生命周期。
    ///
    /// # Safety
    /// `buffer` must be valid for reads of at least `size` bytes and must be
    /// pre-registered with the TE via [`register_buffer`](Self::register_buffer).
    ///
    /// buffer 必须可读取至少 size 字节，并且必须通过 register_buffer 预先向 TE 注册。
    /// C++ 等价：`Client::PutFrom()`。
    pub async unsafe fn put_from(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();

        // Phase 1: put_start / 阶段 1：put_start
        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: size as u64,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self
            .master
            .put_start(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        // Phase 2: zero_copy_write to each replica / 阶段 2：零拷贝写入每个副本
        for replica in &replicas {
            if let Err(e) = self.zero_copy_write(replica, buffer, size).await {
                // On failure, revoke the allocation. / 失败时撤销分配。
                // C++ 写失败时调用 PutRevoke 撤销已分配的资源
                let revoke_req = proto::PutRevokeRequest {
                    client_id: Some(self.client_id_proto()),
                    key: key.to_string(),
                    replica_type: 0,
                    tenant_id: String::new(),
                };
                let _ = self.master.put_revoke(revoke_req).await;
                return Err(e);
            }
        }

        // Phase 3: put_end / 阶段 3：put_end
        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
            tenant_id: String::new(),
        };
        self.master
            .put_end(end_request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Put parts — split a logical object into multiple slices, write them
    //            together in a single batch per replica.
    // 分段写入 —— 将一个逻辑对象拆分为多个切片，每个副本在单个批次中一起写入。
    //
    // The total length is derived from the sum of all slices and sent in
    // put_start. Each slice becomes a separate TransferRequest in one batch,
    // sharing a single segment_id and batch_id.
    //
    // 总长度从所有切片的总和计算并通过 put_start 发送。每个切片成为一个
    // 独立 TransferRequest，共享一个 segment_id 和一个 batch_id。
    // C++ equivalent: Client::PutParts()
    // -----------------------------------------------------------------------

    /// Write a key as multiple data slices. Useful when the data is not
    /// contiguous in memory (e.g. gathered from multiple buffers).
    ///
    /// 将 key 作为多个数据切片写入。当数据在内存中不连续时有用
    /// （例如从多个缓冲区收集而来）。
    ///
    /// All slices for a given replica are submitted as a single TransferEngine
    /// batch, which allows the TE to pipeline the transfers.
    ///
    /// 给定副本的所有切片以单个 TransferEngine 批次提交，允许 TE 流水线化传输。
    pub async fn put_parts(
        &mut self,
        key: &str,
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();
        let total_len: usize = values.iter().map(|v| v.len()).sum();

        // Phase 1: put_start with total_len / 阶段 1：put_start 带上 total_len
        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: total_len as u64,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self
            .master
            .put_start(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        if replicas.is_empty() {
            return Err(StoreError::NoAvailableHandle);
        }

        // Phase 2: for each replica, copy all slices into local_buffer
        // contiguously and submit as one batch.
        // 阶段 2：对每个副本，将所有切片连续拷贝到 local_buffer 并作为单个批次提交。
        for replica in &replicas {
            let segment_id = self.engine.open_segment(&replica.segment_name)?;
            let batch_id = self.engine.allocate_batch_id(values.len())?;

            // Build TransferRequests: each slice → one RDMA write at the correct
            // offset within the replica.
            // 构建传输请求：每个切片 → 在副本内正确偏移处的一次 RDMA 写。
            let requests: Vec<TransferRequest> = values
                .iter()
                .enumerate()
                .map(|(i, data)| {
                    let src_offset = values[..i].iter().map(|v| v.len()).sum::<usize>();
                    let tgt_offset = replica.offset + src_offset as u64;
                    // Copy slice into local_buffer at the correct offset.
                    // 将切片拷贝到 local_buffer 的正确偏移位置。
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            data.as_ptr(),
                            self.local_buffer[src_offset..].as_ptr() as *mut u8,
                            data.len(),
                        );
                    }
                    TransferRequest {
                        opcode: Opcode::Write,
                        source: unsafe {
                            self.local_buffer.as_ptr().add(src_offset) as *mut c_void
                        },
                        target_id: segment_id,
                        target_offset: tgt_offset,
                        length: data.len() as u64,
                    }
                })
                .collect();

            self.engine.submit_transfer(batch_id, &requests)?;

            // Poll each slice's transfer with 10s timeout.
            // 以 10s 超时轮询每个切片的传输状态。
            for i in 0..values.len() {
                let start = tokio::time::Instant::now();
                let timeout = tokio::time::Duration::from_secs(10);
                loop {
                    let status = self.engine.get_transfer_status(batch_id, i)?;
                    if status.status == TransferStatusEnum::Completed {
                        break;
                    }
                    if status.status == TransferStatusEnum::Failed {
                        // On any slice failure, revoke + cleanup.
                        // 任何切片失败，撤销 + 清理。
                        // C++ 写失败时调用 PutRevoke 撤销已分配的资源
                        let revoke_req = proto::PutRevokeRequest {
                            client_id: Some(self.client_id_proto()),
                            key: key.to_string(),
                            replica_type: 0,
                            tenant_id: String::new(),
                        };
                        let _ = self.master.put_revoke(revoke_req).await;
                        let _ = self.engine.free_batch_id(batch_id);
                        let _ = self.engine.close_segment(segment_id);
                        return Err(StoreError::OperationFailed(-1));
                    }
                    if start.elapsed() > timeout {
                        // Timeout: revoke + cleanup. / 超时：撤销 + 清理。
                        let revoke_req = proto::PutRevokeRequest {
                            client_id: Some(self.client_id_proto()),
                            key: key.to_string(),
                            replica_type: 0,
                            tenant_id: String::new(),
                        };
                        let _ = self.master.put_revoke(revoke_req).await;
                        let _ = self.engine.free_batch_id(batch_id);
                        let _ = self.engine.close_segment(segment_id);
                        return Err(StoreError::OperationFailed(-2));
                    }
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                }
            }

            // Cleanup per-replica resources. / 清理每个副本的资源。
            self.engine.free_batch_id(batch_id)?;
            self.engine.close_segment(segment_id)?;
        }

        // Phase 3: put_end / 阶段 3：put_end
        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
            tenant_id: String::new(),
        };
        self.master
            .put_end(end_request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Batch Put — write multiple keys with batched RPCs
    // 批量写入 —— 使用批量 RPC 写入多个 key
    //
    // C++ equivalent: Client::BatchPut():
    //   BatchPutStart → per-key TE writes → BatchPutEnd / BatchPutRevoke
    //
    // Uses a single BatchPutStart RPC to allocate replicas for all keys,
    // then writes each key's data via TE, then commits with a single
    // BatchPutEnd. Per-key error tolerance: failed keys record -1.
    //
    // 使用单次 BatchPutStart RPC 为所有 key 分配副本，
    // 然后通过 TE 写入每个 key 的数据，最后用单次 BatchPutEnd 提交。
    // 每个 key 独立容错：失败的 key 记录 -1。
    // -----------------------------------------------------------------------

    /// Batch-write multiple key-value pairs using batched RPCs.
    /// Allocates replicas for all keys in one BatchPutStart call, writes each
    /// key's data via TE, then commits with BatchPutEnd (or BatchPutRevoke on
    /// failure). Returns per-key status: `0` = success, `-1` = failure.
    ///
    /// 使用批量 RPC 写入多个键值对。
    /// 通过单次 BatchPutStart 为所有 key 分配副本，TE 写入数据，
    /// 然后通过 BatchPutEnd 提交（失败则 BatchPutRevoke）。
    /// 返回每个 key 的状态：0=成功, -1=失败。
    pub async fn batch_put(
        &mut self,
        keys: &[String],
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        if keys.len() != values.len() {
            return Err(StoreError::InvalidParams(
                "keys and values length mismatch".to_string(),
            ));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let cfg = config.unwrap_or_default();
        let replica_count = cfg.replica_num.max(1) as usize;
        let nof_count = cfg.nof_replica_num as usize;
        let per_key = replica_count + nof_count;

        // Phase 1: BatchPutStart — allocate replicas for all keys in one RPC.
        let slice_lengths: Vec<u64> = values.iter().map(|v| v.len() as u64).collect();
        let all_replicas = match self
            .batch_put_start(keys, &slice_lengths, &cfg, "")
            .await
        {
            Ok(r) => r,
            Err(_) => {
                // All allocation failed — all keys fail.
                return Ok(vec![-1; keys.len()]);
            }
        };

        // Phase 2: per-key TE writes. Replicas are returned in key order,
        // per_key replicas per key. Write each key's replicas.
        let mut statuses = vec![-1i32; keys.len()];
        let mut success_keys: Vec<String> = Vec::new();
        let mut failed_keys: Vec<String> = Vec::new();
        let mut ri = 0usize;

        for (ki, key) in keys.iter().enumerate() {
            if ri + per_key > all_replicas.len() {
                // No more replicas — this key (and subsequent) were skipped by master
                // because they already existed or allocation failed.
                break;
            }
            let replicas = &all_replicas[ri..ri + per_key];
            ri += per_key;

            let mut ok = true;
            for replica in replicas {
                if let Err(_) = self.write_to_replica(replica, values[ki]).await {
                    ok = false;
                    break;
                }
            }

            if ok {
                statuses[ki] = 0;
                success_keys.push(key.clone());
            } else {
                failed_keys.push(key.clone());
            }
        }

        // Phase 3: BatchPutEnd / BatchPutRevoke.
        if !success_keys.is_empty() {
            tracing::info!(
                "batch_put: calling batch_put_end for {} success keys: {:?}",
                success_keys.len(),
                &success_keys[..success_keys.len().min(3)]
            );
            match self
                .batch_put_end(&success_keys, 0 /* MEMORY */, "")
                .await
            {
                Ok(statuses) => {
                    tracing::info!(
                        "batch_put: batch_put_end returned statuses: {:?}",
                        statuses
                    );
                }
                Err(e) => {
                    tracing::error!("batch_put: batch_put_end FAILED: {:?}", e);
                }
            }
        }
        if !failed_keys.is_empty() {
            tracing::warn!("batch_put: {} keys failed writes, revoking", failed_keys.len());
            let _ = self.batch_put_revoke(&failed_keys, "", "").await;
        }

        Ok(statuses)
    }

    /// Zero-copy batch put from pre-registered buffers.
    ///
    /// 从预注册缓冲区进行零拷贝批量写入。
    ///
    /// # Safety
    /// All `buffers[i]` must be pre-registered with the TE and contain at least
    /// `sizes[i]` bytes of valid data.
    ///
    /// 所有 buffers[i] 必须预先向 TE 注册，且包含至少 sizes[i] 字节的有效数据。
    pub async unsafe fn batch_put_from(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        let mut statuses = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self
                .put_from(key, buffers[i], sizes[i], config.clone())
                .await
            {
                Ok(()) => statuses.push(0),
                Err(_) => statuses.push(-1), // per-key error tolerance / 按 key 容错
            }
        }
        Ok(statuses)
    }

    /// Zero-copy batch put from multiple buffers per key.
    ///
    /// Each key may be assembled from multiple non-contiguous memory regions.
    /// `all_buffers[i]` and `all_sizes[i]` correspond to the i-th key.
    /// All buffers must be pre-registered with the TransferEngine.
    ///
    /// C++ equivalent: `RealClient::batch_put_from_multi_buffers`
    ///
    /// 多缓冲区零拷贝批量写入。
    /// 每个 key 可由多个非连续内存区域拼接而成。
    /// all_buffers[i] 和 all_sizes[i] 对应第 i 个 key。
    /// 所有缓冲区必须预先向 TE 注册。
    ///
    /// # Safety
    /// All buffers in `all_buffers` must be pre-registered with the TE.
    pub async unsafe fn batch_put_from_multi_buffers(
        &mut self,
        keys: &[String],
        all_buffers: &[Vec<*mut c_void>],
        all_sizes: &[Vec<usize>],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        let mut statuses = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            let buffers = &all_buffers[i];
            let sizes = &all_sizes[i];
            let mut ok = true;
            for (j, (&buf, &sz)) in buffers.iter().zip(sizes.iter()).enumerate() {
                let part_key = if j == 0 {
                    key.clone()
                } else {
                    format!("{key}:part:{j}")
                };
                match self.put_from(&part_key, buf, sz, config.clone()).await {
                    Ok(()) => {}
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            statuses.push(if ok { 0 } else { -1 });
        }
        Ok(statuses)
    }
}

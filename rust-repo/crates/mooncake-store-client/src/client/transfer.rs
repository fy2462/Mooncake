// ============================================================================
// TransferEngine integration: replica selection, data transfer, buffer
// registration. This module implements the data-plane logic shared by
// read and write paths.
//
// TransferEngine 集成：副本选择、数据传输、缓冲区注册。
// 此模块实现了读写路径共享的数据面逻辑。
//
// Key patterns (关键模式):
//   - local_memcpy: fast path for same-node transfers (bypasses TE)
//   - write_to_replica / read_from_replica: TE transfer with per-replica
//     resource lifecycle (open_segment → allocate_batch_id → submit →
//     poll 10s → free_batch_id → close_segment)
//   - zero_copy_read / zero_copy_write: same TE pattern but with
//     caller-provided (pre-registered) buffers instead of local_buffer
//
// C++ equivalent: real_client.cpp (SelectBestReplica, WriteToReplica,
// ReadFromReplica, local memcpy paths)
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{BatchId, Opcode, TransferRequest, TransferStatusEnum};
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    pub(crate) async fn wait_for_transfer_batch(
        &self,
        batch_id: BatchId,
        task_count: usize,
        timeout: tokio::time::Duration,
    ) -> StoreResult<Vec<transfer_engine_ffi::TransferStatus>> {
        let statuses = self
            .wait_for_transfer_batch_terminal(batch_id, task_count, timeout)
            .await?;
        if statuses
            .iter()
            .all(|status| status.status == TransferStatusEnum::Completed)
        {
            return Ok(statuses);
        }
        Err(StoreError::OperationFailed(-1))
    }

    pub(crate) async fn wait_for_transfer_batch_terminal(
        &self,
        batch_id: BatchId,
        task_count: usize,
        timeout: tokio::time::Duration,
    ) -> StoreResult<Vec<transfer_engine_ffi::TransferStatus>> {
        let start = tokio::time::Instant::now();
        let mut statuses = vec![
            transfer_engine_ffi::TransferStatus {
                status: TransferStatusEnum::Waiting,
                transferred_bytes: 0,
            };
            task_count
        ];
        let mut terminal = vec![false; task_count];

        loop {
            let mut all_terminal = true;
            for task_id in 0..task_count {
                if terminal[task_id] {
                    continue;
                }
                let status = self.engine.get_transfer_status(batch_id, task_id)?;
                statuses[task_id] = status.clone();
                if status.status.is_terminal() {
                    terminal[task_id] = true;
                } else {
                    all_terminal = false;
                }
            }
            if all_terminal {
                return Ok(statuses);
            }
            if start.elapsed() > timeout {
                return Err(StoreError::OperationFailed(-2));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }
    }

    // -----------------------------------------------------------------------
    // LOCAL_MEMCPY — bypass TE for same-node transfers (matches C++ strategy)
    // 本地内存拷贝 —— 同节点传输绕过 TE（与 C++ 策略一致）
    //
    // When the target replica is hosted on a locally-mounted segment, we can
    // directly memcpy into/from the segment_buffer without involving the
    // TransferEngine at all. This avoids RDMA/TCP stack overhead entirely.
    //
    // 当目标副本位于本地挂载的 segment 上时，可以直接从 segment_buffer
    // 进行 memcpy，完全不需要 TransferEngine。这避免了 RDMA/TCP 栈的全部开销。
    // C++ equivalent: real_client.cpp local memcpy paths inside
    // ReadFromReplica / WriteToReplica.
    // -----------------------------------------------------------------------

    /// Check whether a replica's segment is locally mounted on this node.
    /// 检查副本的 segment 是否在本地节点上挂载。
    fn is_local_replica(&self, replica: &ReplicaDescriptor) -> bool {
        self.local_endpoints.read().contains(&replica.segment_name)
    }

    /// Direct memory copy into the local segment buffer (same-node write).
    /// Only call this when `is_local_replica(replica)` returns `true` and
    /// `segment_buffer` is `Some`.
    ///
    /// 直接内存拷贝到本地 segment 缓冲区（同节点写）。
    /// 仅在 is_local_replica(replica) 为 true 且 segment_buffer 为 Some 时调用。
    fn local_memcpy_write(&self, replica: &ReplicaDescriptor, data: &[u8]) -> StoreResult<()> {
        let seg = self
            .segment_buffer
            .as_ref()
            .ok_or_else(|| StoreError::Internal("no local segment buffer".into()))?;
        let offset = replica.offset as usize;
        let len = data.len();
        // Bounds check / 越界检查
        if offset + len > seg.len() {
            return Err(StoreError::InvalidParams(format!(
                "local write out of bounds: offset={} len={} segment_size={}",
                offset,
                len,
                seg.len()
            )));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), seg.as_ptr().add(offset) as *mut u8, len);
        }
        Ok(())
    }

    /// Direct memory copy from the local segment buffer (same-node read).
    /// Only call this when `is_local_replica(replica)` returns `true` and
    /// `segment_buffer` is `Some`.
    ///
    /// 直接从本地 segment 缓冲区内存拷贝（同节点读）。
    /// 仅在 is_local_replica(replica) 为 true 且 segment_buffer 为 Some 时调用。
    fn local_memcpy_read(&self, replica: &ReplicaDescriptor) -> StoreResult<Vec<u8>> {
        let seg = self
            .segment_buffer
            .as_ref()
            .ok_or_else(|| StoreError::Internal("no local segment buffer".into()))?;
        let offset = replica.offset as usize;
        let len = replica.size as usize;
        // Bounds check / 越界检查
        if offset + len > seg.len() {
            return Err(StoreError::InvalidParams(format!(
                "local read out of bounds: offset={} len={} segment_size={}",
                offset,
                len,
                seg.len()
            )));
        }
        let mut result = vec![0u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(seg.as_ptr().add(offset), result.as_mut_ptr(), len);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Buffer registration (zero-copy path)
    // 缓冲区注册（零拷贝路径）
    //
    // Externally-managed buffers must be registered with the TransferEngine
    // before they can be used as source/destination in zero-copy transfers.
    //
    // 外部管理的缓冲区必须先向 TransferEngine 注册，然后才能用作零拷贝传输的源/目标。
    // -----------------------------------------------------------------------

    /// Register an externally-managed buffer with the TransferEngine.
    ///
    /// After registration, the TE can DMA directly into/from this buffer,
    /// enabling true zero-copy I/O.
    ///
    /// 向 TransferEngine 注册外部管理的缓冲区。
    /// 注册后，TE 可以直接对此缓冲区进行 DMA 操作，实现真正的零拷贝 I/O。
    ///
    /// # Safety
    /// `buffer` must point to valid memory of at least `size` bytes and must
    /// remain alive until [`unregister_buffer`](Self::unregister_buffer) is called.
    ///
    /// buffer 必须指向至少 size 字节的有效内存，并且在调用 unregister_buffer
    /// 之前必须保持存活。
    pub unsafe fn register_buffer(
        &self,
        buffer: *mut c_void,
        size: usize,
        location: &str,
    ) -> StoreResult<()> {
        unsafe {
            self.engine
                .register_local_memory(buffer, size, location, true)?;
        }
        self.registered_buffers
            .write()
            .insert(buffer as usize, (size, location.to_string()));
        Ok(())
    }

    /// Unregister a previously-registered buffer from the TransferEngine.
    /// 从 TransferEngine 取消注册之前注册的缓冲区。
    ///
    /// # Safety
    /// `buffer` must have been previously registered via `register_buffer`.
    /// buffer 必须之前已通过 register_buffer 注册。
    pub unsafe fn unregister_buffer(&self, buffer: *mut c_void) -> StoreResult<()> {
        unsafe {
            self.engine.unregister_local_memory(buffer)?;
        }
        self.registered_buffers.write().remove(&(buffer as usize));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers / 内部辅助函数
    // -----------------------------------------------------------------------

    /// Convert Rust UUID to protobuf UUID (high/low u64 pair).
    /// 将 Rust UUID 转换为 protobuf UUID（high/low u64 对）。
    pub(crate) fn client_id_proto(&self) -> proto::Uuid {
        let (h, l) = self.client_id.as_u64_pair();
        proto::Uuid { high: h, low: l }
    }

    /// Query the master for the list of replicas hosting a given key.
    /// Returns an empty vector if the key is not found.
    ///
    /// 向 master 查询持有给定 key 的副本列表。
    /// 如果 key 未找到则返回空向量。
    /// C++ equivalent: Client::GetReplicaList()
    pub(crate) async fn fetch_replicas(
        &mut self,
        key: &str,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let request = proto::GetReplicaListRequest {
            key: key.to_string(),
            tenant_id: String::new(),
        };
        let response = self
            .master
            .get_replica_list(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);
        Ok(replicas)
    }

    /// Query the master for replica lists for multiple keys in one RPC.
    /// Results preserve input order and each key carries its own error.
    ///
    /// 批量向 master 查询多个 key 的副本列表。
    /// 返回顺序与输入一致，每个 key 独立携带错误。
    /// C++ equivalent: Client::BatchQuery() → BatchGetReplicaList()
    pub(crate) async fn fetch_batch_replicas(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<StoreResult<Vec<ReplicaDescriptor>>>> {
        let request = proto::BatchGetReplicaListRequest {
            keys: keys.to_vec(),
            tenant_id: String::new(),
        };
        let response = self
            .master
            .batch_get_replica_list(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        if response.results.len() != keys.len() {
            return Err(StoreError::Internal(format!(
                "BatchGetReplicaList response size mismatch: expected {}, got {}",
                keys.len(),
                response.results.len()
            )));
        }

        let results = response
            .results
            .into_iter()
            .zip(keys.iter())
            .map(|(result, key)| match result.status {
                0 => result
                    .response
                    .map(|response| self.replicas_from_proto(&response.replicas))
                    .ok_or_else(|| {
                        StoreError::Internal(format!("missing replica response for {key}"))
                    }),
                -1 => Err(StoreError::KeyNotFound(key.clone())),
                -5 => Err(StoreError::ReplicaNotReady),
                _ => Err(StoreError::Internal(result.error_message)),
            })
            .collect();
        Ok(results)
    }

    /// Convert protobuf replica descriptors into domain [`ReplicaDescriptor`]s.
    ///
    /// Maps proto enums:
    /// - `status`: 1=Allocating, 2=Written, 3=Complete, 4=Failed
    /// - `replica_type`: 1=Disk, 2=LocalDisk, 3=NoFSsd, default=Memory
    ///
    /// 将 protobuf 副本描述符转换为领域 ReplicaDescriptor。
    /// 映射 proto 枚举：status (1=Allocating, 2=Written, 3=Complete, 4=Failed)
    /// 和 replica_type (1=Disk, 2=LocalDisk, 3=NoFSsd, 默认=Memory)。
    pub(crate) fn replicas_from_proto(
        &self,
        replicas: &[proto::ReplicaDescriptor],
    ) -> Vec<ReplicaDescriptor> {
        replicas
            .iter()
            .filter_map(|r| {
                let sid = r.segment_id.as_ref()?;
                Some(ReplicaDescriptor {
                    refcnt: 0,
                    segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                    segment_name: r.segment_name.clone(),
                    offset: r.offset,
                    size: r.size,
                    base_addr: r.base_addr,
                    status: match r.status {
                        1 => mooncake_store_core::ReplicaStatus::Allocating,
                        2 => mooncake_store_core::ReplicaStatus::Written,
                        3 => mooncake_store_core::ReplicaStatus::Complete,
                        4 => mooncake_store_core::ReplicaStatus::Failed,
                        _ => mooncake_store_core::ReplicaStatus::Undefined,
                    },
                    replica_type: match r.replica_type {
                        1 => mooncake_store_core::ReplicaType::Disk,
                        2 => mooncake_store_core::ReplicaType::LocalDisk,
                        3 => mooncake_store_core::ReplicaType::NoFSsd,
                        _ => mooncake_store_core::ReplicaType::Memory,
                    },
                    holder_client_id: r
                        .holder_client_id
                        .as_ref()
                        .map(|id| Uuid::from_u64_pair(id.high, id.low)),
                    handle_valid: true,
                })
            })
            .collect()
    }

    /// Select the best replica from a list, matching C++ `SelectBestReplica`.
    ///
    /// # Priority order (优先级顺序 — C++ `real_client.cpp:286-325`)
    ///
    /// | Priority | Replica Type | Locality  | Behavior                        |
    /// |----------|-------------|-----------|----------------------------------|
    /// | 1        | MEMORY      | Local     | Return immediately (最优)       |
    /// | 2        | MEMORY      | Remote    | First seen (任意远程 MEMORY)     |
    /// | 3        | NOF_SSD     | Local     | Return immediately (次优)       |
    /// | 4        | NOF_SSD     | Remote    | First seen (任意远程 NOF_SSD)    |
    /// | 5        | LOCAL_DISK  | —         | Last one wins (覆盖 DISK)       |
    /// | 6        | DISK        | —         | Only if no LOCAL_DISK found      |
    ///
    /// # Algorithm (算法)
    ///
    /// **Pass 1** — scan for MEMORY and NOF_SSD:
    /// - If a local MEMORY replica is found, return it immediately (short-circuit).
    /// - If a local NOF_SSD replica is found, return it immediately.
    /// - Otherwise, remember the first remote MEMORY and first remote NOF_SSD.
    ///
    /// **第一遍** —— 扫描 MEMORY 和 NOF_SSD：
    /// - 找到本地 MEMORY 副本则立即返回（短路）。
    /// - 找到本地 NOF_SSD 副本则立即返回。
    /// - 否则记住第一个远程 MEMORY 和第一个远程 NOF_SSD。
    ///
    /// **Pass 2** — if no MEMORY/NOF_SSD found, scan for disk-based replicas:
    /// - LOCAL_DISK always overwrites any previous disk pick.
    /// - DISK is only chosen if no LOCAL_DISK was found.
    ///
    /// **第二遍** —— 如果没有找到 MEMORY/NOF_SSD，扫描基于磁盘的副本：
    /// - LOCAL_DISK 总是覆盖之前的磁盘选择。
    /// - DISK 仅在未找到任何 LOCAL_DISK 时被选择。
    ///
    /// Only replicas with `status == Complete` are considered.
    /// 仅考虑 status == Complete 的副本。
    ///
    /// 从副本列表中选择最优副本，完全匹配 C++ `SelectBestReplica` 逻辑。
    pub(crate) fn select_best_replica<'a>(
        &self,
        replicas: &'a [ReplicaDescriptor],
    ) -> Option<&'a ReplicaDescriptor> {
        let endpoints = self.local_endpoints.read();
        let mut first_memory: Option<&ReplicaDescriptor> = None;
        let mut first_nof: Option<&ReplicaDescriptor> = None;

        // Pass 1: prioritize local MEMORY/NOF_SSD, otherwise record first remote.
        // 第一遍：优先本地 MEMORY/NOF_SSD，否则记录第一个远程副本。
        for r in replicas {
            if r.status != mooncake_store_core::ReplicaStatus::Complete {
                continue; // skip non-ready replicas / 跳过未就绪的副本
            }
            match r.replica_type {
                mooncake_store_core::ReplicaType::Memory => {
                    if endpoints.contains(&r.segment_name) {
                        return Some(r); // 本地 MEMORY —— 最优 / local MEMORY — best
                    }
                    if first_memory.is_none() {
                        first_memory = Some(r); // 记录第一个远程 MEMORY / record first remote MEMORY
                    }
                }
                mooncake_store_core::ReplicaType::NoFSsd => {
                    if endpoints.contains(&r.segment_name) {
                        return Some(r); // 本地 NOF_SSD —— 次优 / local NOF_SSD — second best
                    }
                    if first_nof.is_none() {
                        first_nof = Some(r); // 记录第一个远程 NOF_SSD / record first remote NOF_SSD
                    }
                }
                _ => {} // disk types handled in pass 2 / 磁盘类型在第二遍处理
            }
        }

        // Return best memory/NOF found (local was already short-circuited above).
        // 返回找到的最佳 MEMORY/NOF（本地已在上面短路返回）。
        if let Some(r) = first_memory {
            return Some(r);
        }
        if let Some(r) = first_nof {
            return Some(r);
        }

        // Pass 2: LOCAL_DISK preferred over DISK.
        // 第二遍：LOCAL_DISK 优先于 DISK。
        let mut best: Option<&ReplicaDescriptor> = None;
        for r in replicas {
            if r.status != mooncake_store_core::ReplicaStatus::Complete {
                continue;
            }
            match r.replica_type {
                mooncake_store_core::ReplicaType::LocalDisk => {
                    best = Some(r); // LOCAL_DISK always overrides DISK / LOCAL_DISK 始终覆盖 DISK
                }
                mooncake_store_core::ReplicaType::Disk if best.is_none() => {
                    best = Some(r); // DISK only if no LOCAL_DISK / DISK 仅在没有任何 LOCAL_DISK 时
                }
                _ => {}
            }
        }
        best
    }

    // -----------------------------------------------------------------------
    // write_to_replica — generic write with local_memcpy fast path
    // 向副本写入 —— 带 local_memcpy 快速路径的通用写入
    //
    // Flow (流程):
    //   1. Check is_local_replica && segment_buffer → local_memcpy (fast path)
    //      → done, no TE resources needed.
    //      检查本地副本 + segment_buffer → local_memcpy（快速路径），无需 TE 资源。
    //
    //   2. (Remote path) Copy data into local_buffer → open_segment →
    //      allocate_batch_id → submit transfer → poll status (10s timeout) →
    //      free_batch_id → close_segment.
    //      (远程路径) 拷贝数据到 local_buffer → open_segment →
    //      allocate_batch_id → 提交传输 → 轮询状态 (10s 超时) →
    //      free_batch_id → close_segment。
    //
    // Resource cleanup: on failure or timeout, batch_id is freed and segment
    // is closed before returning the error. This prevents resource leaks in
    // the TransferEngine.
    //
    // 资源清理：在失败或超时时，先释放 batch_id 并关闭 segment 再返回错误。
    // 这防止了 TransferEngine 中的资源泄漏。
    //
    // C++ equivalent: Client::WriteToReplica() in real_client.cpp
    // -----------------------------------------------------------------------

    /// Write data to a specific replica. Automatically chooses local_memcpy
    /// fast path when the replica is local and segment_buffer is available.
    ///
    /// 向指定副本写入数据。当副本为本地且 segment_buffer 可用时自动选择
    /// local_memcpy 快速路径。
    pub(crate) async fn write_to_replica(
        &self,
        replica: &ReplicaDescriptor,
        data: &[u8],
    ) -> StoreResult<()> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            offset = replica.offset,
            base_addr = replica.base_addr,
            data_len = data.len(),
            is_local = self.is_local_replica(replica),
            has_seg_buf = self.segment_buffer.is_some(),
            "write_to_replica: ENTER"
        );

        // Fast path: local segment — direct memcpy, no TE overhead.
        // 快速路径：本地 segment —— 直接 memcpy，无 TE 开销。
        if self.is_local_replica(replica) && self.segment_buffer.is_some() {
            tracing::info!(target: "te_debug", "write_to_replica: taking LOCAL_MEMCPY fast path");
            let result = self.local_memcpy_write(replica, data);
            tracing::info!(target: "te_debug", ok = result.is_ok(), "write_to_replica: EXIT (local_memcpy)");
            return result;
        }

        // Remote path: validate data fits in local_buffer, then transfer via TE.
        // 远程路径：验证数据适合 local_buffer，然后通过 TE 传输。
        if data.len() > self.local_buffer.len() {
            tracing::error!(
                target: "te_debug",
                data_len = data.len(),
                buf_len = self.local_buffer.len(),
                "write_to_replica: data size exceeds local buffer"
            );
            return Err(StoreError::InvalidParams(format!(
                "data size {} exceeds local buffer size {}",
                data.len(),
                self.local_buffer.len()
            )));
        }

        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            local_buf_ptr = ?self.local_buffer.as_ptr(),
            local_buf_len = self.local_buffer.len(),
            "write_to_replica: opening segment"
        );
        // Step 1: open the segment on the TE. / 第 1 步：在 TE 上打开 segment。
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "write_to_replica: segment opened");

        // Step 2: allocate a batch_id for grouping transfer requests.
        // 第 2 步：分配 batch_id 用于分组传输请求。
        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "write_to_replica: batch_id allocated");

        // Step 3: copy data into the registered local_buffer (TE source).
        // 第 3 步：将数据拷贝到已注册的 local_buffer（TE 源）。
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.local_buffer.as_ptr() as *mut u8,
                data.len(),
            );
        }
        tracing::info!(target: "te_debug", data_len = data.len(), "write_to_replica: data copied to local_buffer");

        // Step 4: build and submit the transfer request.
        // 第 4 步：构建并提交传输请求。
        let target_offset = replica.base_addr + replica.offset;
        let request = TransferRequest {
            opcode: Opcode::Write,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset,
            length: data.len() as u64,
        };
        tracing::info!(
            target: "te_debug",
            src = ?request.source,
            tgt_id = request.target_id.0,
            tgt_off = request.target_offset,
            len = request.length,
            "write_to_replica: submitting transfer"
        );

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", "write_to_replica: transfer submitted, polling...");

        // Step 5: poll transfer status with 10s timeout.
        // 第 5 步：以 10s 超时轮询传输状态。
        // 10s is generous for RDMA (us-scale) but covers TCP retransmissions
        // and slow NVMe-oF targets. 10s 对 RDMA（微秒级）很充裕，但覆盖了 TCP
        // 重传和慢速 NVMe-oF 目标。
        let statuses = match self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            Ok(statuses) => statuses,
            Err(e) => {
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(e);
            }
        };
        tracing::info!(
            target: "te_debug",
            transferred = statuses[0].transferred_bytes,
            "write_to_replica: transfer COMPLETED"
        );

        // Step 6: cleanup resources. / 第 6 步：清理资源。
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "write_to_replica: freeing batch_id");
        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "write_to_replica: closing segment");
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", "write_to_replica: EXIT (success)");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // zero_copy_write — write directly from a caller-provided buffer
    // 零拷贝写入 —— 直接从调用者提供的缓冲区写入
    //
    // Unlike write_to_replica, this does NOT copy data into local_buffer first.
    // Instead, the caller's buffer (which must be pre-registered with the TE)
    // is used directly as the source of the RDMA transfer. This eliminates one
    // memcpy.
    //
    // 与 write_to_replica 不同，此方法不先将数据拷贝到 local_buffer。
    // 而是直接使用调用者的缓冲区（必须已向 TE 预先注册）作为 RDMA 传输的源。
    // 这消除了额外的 memcpy。
    //
    // Resource lifecycle (资源生命周期):
    //   open_segment → allocate_batch_id → submit → poll(10s) →
    //   free_batch_id → close_segment
    //
    // C++ equivalent: zero-copy path inside Client::WriteToReplica() when
    // the caller passes an externally-registered buffer.
    // -----------------------------------------------------------------------

    /// Zero-copy write to a replica using a caller-provided buffer.
    /// The buffer must be pre-registered with the TE via [`register_buffer`].
    ///
    /// 使用调用者提供的缓冲区进行零拷贝写入。
    /// 缓冲区必须通过 register_buffer 预先向 TE 注册。
    ///
    /// # Safety
    /// `buffer` must point to at least `size` bytes of valid memory that has
    /// been registered with the TE. / buffer 必须指向至少 size 字节的已向 TE
    /// 注册的有效内存。
    pub(crate) async unsafe fn zero_copy_write(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<()> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            buf = ?buffer,
            size,
            "zero_copy_write: ENTER"
        );

        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "zero_copy_write: segment opened");

        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "zero_copy_write: batch_id allocated");

        // Build transfer: source = caller's buffer directly (no intermediate copy).
        // 构建传输：源 = 直接使用调用者缓冲区（无中间拷贝）。
        let request = TransferRequest {
            opcode: Opcode::Write,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.base_addr + replica.offset,
            length: size as u64,
        };

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", "zero_copy_write: transfer submitted, polling...");

        if let Err(e) = self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e);
        }

        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", "zero_copy_write: EXIT (success)");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // read_from_replica — generic read with local_memcpy fast path
    // 从副本读取 —— 带 local_memcpy 快速路径的通用读取
    //
    // Mirror of write_to_replica (with Opcode::Read instead of Write).
    // write_to_replica 的镜像（Opcode::Read 代替 Write）。
    //
    // C++ equivalent: Client::ReadFromReplica() in real_client.cpp
    // -----------------------------------------------------------------------

    /// Read data from a specific replica. Automatically chooses local_memcpy
    /// fast path when the replica is local and segment_buffer is available.
    ///
    /// 从指定副本读取数据。当副本为本地且 segment_buffer 可用时自动选择
    /// local_memcpy 快速路径。
    pub(crate) async fn read_from_replica(
        &self,
        key: &str,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            offset = replica.offset,
            base_addr = replica.base_addr,
            replica_type = ?replica.replica_type,
            replica_size = replica.size,
            is_local = self.is_local_replica(replica),
            has_seg_buf = self.segment_buffer.is_some(),
            "read_from_replica: ENTER"
        );

        // LOCAL_DISK on remote node: use P2P offload RPC.
        // C++ equivalent: branch in real_client.cpp that calls
        // `batch_get_into_offload_object_internal`.
        if replica.replica_type == mooncake_store_core::ReplicaType::LocalDisk
            && !self.is_local_replica(replica)
        {
            return self.read_from_remote_local_disk(key, replica).await;
        }

        // Fast path: local segment — direct memcpy, no TE overhead.
        // 快速路径：本地 segment —— 直接 memcpy，无 TE 开销。
        if self.is_local_replica(replica) && self.segment_buffer.is_some() {
            tracing::info!(target: "te_debug", "read_from_replica: taking LOCAL_MEMCPY fast path");
            let result = self.local_memcpy_read(replica);
            tracing::info!(
                target: "te_debug",
                ok = result.is_ok(),
                data_len = result.as_ref().map(|v| v.len()).unwrap_or(0),
                "read_from_replica: EXIT (local_memcpy)"
            );
            return result;
        }

        // Remote path: validate object size fits in local_buffer.
        // 远程路径：验证对象大小适合 local_buffer。
        if replica.size > self.local_buffer.len() as u64 {
            tracing::error!(
                target: "te_debug",
                replica_size = replica.size,
                buf_len = self.local_buffer.len(),
                "read_from_replica: object size exceeds local buffer"
            );
            return Err(StoreError::InvalidParams(format!(
                "object size {} exceeds local buffer size {}",
                replica.size,
                self.local_buffer.len()
            )));
        }

        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            local_buf_ptr = ?self.local_buffer.as_ptr(),
            local_buf_len = self.local_buffer.len(),
            "read_from_replica: opening segment"
        );
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "read_from_replica: segment opened");

        let read_len = replica.size as usize;
        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, read_len, "read_from_replica: batch_id allocated");

        // Build transfer: Read from remote segment into local_buffer.
        // 构建传输：从远端 segment 读入 local_buffer。
        let target_offset = replica.base_addr + replica.offset;
        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void, // destination / 目标
            target_id: segment_id,
            target_offset, // source offset in remote segment / 远端 segment 中的源偏移
            length: read_len as u64,
        };
        tracing::info!(
            target: "te_debug",
            src = ?request.source,
            tgt_id = request.target_id.0,
            tgt_off = request.target_offset,
            len = request.length,
            "read_from_replica: submitting transfer"
        );

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", "read_from_replica: transfer submitted, polling...");

        let statuses = match self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            Ok(statuses) => statuses,
            Err(e) => {
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(e);
            }
        };
        let transferred = statuses[0].transferred_bytes;

        tracing::info!(target: "te_debug", batch_id = batch_id.0, "read_from_replica: freeing batch_id");
        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "read_from_replica: closing segment");
        self.engine.close_segment(segment_id)?;

        // Extract the read data from local_buffer. / 从 local_buffer 提取读取的数据。
        let result: Vec<u8> = self.local_buffer[..(transferred as usize)].to_vec();
        tracing::info!(
            target: "te_debug",
            result_len = result.len(),
            "read_from_replica: EXIT (success)"
        );
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // zero_copy_read — read directly into a caller-provided buffer
    // 零拷贝读取 —— 直接读入调用者提供的缓冲区
    //
    // Mirror of zero_copy_write (with Opcode::Read instead of Write).
    // zero_copy_write 的镜像（Opcode::Read 代替 Write）。
    //
    // The caller's buffer is the DESTINATION of the RDMA read, so no
    // intermediate copy from local_buffer is needed.
    //
    // 调用者的缓冲区是 RDMA 读的目标，因此不需要从 local_buffer 进行中间拷贝。
    // -----------------------------------------------------------------------

    /// Zero-copy read from a replica into a caller-provided buffer.
    /// The buffer must be pre-registered with the TE via [`register_buffer`].
    ///
    /// 从副本零拷贝读取到调用者提供的缓冲区。
    /// 缓冲区必须通过 register_buffer 预先向 TE 注册。
    ///
    /// # Returns
    /// The number of bytes actually transferred. / 实际传输的字节数。
    ///
    /// # Safety
    /// `buffer` must point to at least `size` bytes of valid memory that has
    /// been registered with the TE. / buffer 必须指向至少 size 字节的已向 TE
    /// 注册的有效内存。
    pub(crate) async unsafe fn zero_copy_read(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<usize> {
        tracing::info!(
            target: "te_debug",
            seg_name = %replica.segment_name,
            buf = ?buffer,
            size,
            "zero_copy_read: ENTER"
        );

        let segment_id: transfer_engine_ffi::SegmentId =
            self.engine.open_segment(&replica.segment_name)?;
        tracing::info!(target: "te_debug", seg_id = segment_id.0, "zero_copy_read: segment opened");

        let batch_id: transfer_engine_ffi::BatchId = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(segment_id);
                return Err(e.into());
            }
        };
        tracing::info!(target: "te_debug", batch_id = batch_id.0, "zero_copy_read: batch_id allocated");

        // Build transfer: source = caller's buffer (RDMA destination).
        // 构建传输：源 = 调用者缓冲区（RDMA 目标）。
        let request: TransferRequest = TransferRequest {
            opcode: Opcode::Read,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.base_addr + replica.offset,
            length: size as u64,
        };

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        tracing::info!(target: "te_debug", "zero_copy_read: transfer submitted, polling...");

        let statuses = match self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            Ok(statuses) => statuses,
            Err(e) => {
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                return Err(e);
            }
        };
        let transferred = statuses[0].transferred_bytes;

        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(segment_id);
            return Err(e.into());
        }
        self.engine.close_segment(segment_id)?;
        tracing::info!(target: "te_debug", transferred, "zero_copy_read: EXIT (success)");
        Ok(transferred as usize)
    }

    /// Read data from a remote LOCAL_DISK replica via P2P offload RPC.
    ///
    /// C++ equivalent: `RealClient::batch_get_into_offload_object_internal`
    ///
    /// 通过 P2P 卸载 RPC 从远端 LOCAL_DISK 副本读取数据。
    async fn read_from_remote_local_disk(
        &self,
        key: &str,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        let peer_addr = &replica.segment_name;
        let size = replica.size as i64;

        tracing::info!(
            target: "te_debug",
            peer_addr,
            replica_size = replica.size,
            "read_from_remote_local_disk: dispatching P2P offload read"
        );

        let result = crate::offload::client::batch_get_offload_objects(
            peer_addr,
            &[key.to_string()],
            &[size],
        )
        .await
        .map_err(|e| StoreError::Internal(format!("P2P offload read: {e}")))?;

        // Read data from peer's TE buffer into our local_buffer.
        let seg_id = self
            .engine
            .open_segment(&result.transfer_engine_addr)
            .map_err(|e| StoreError::Internal(format!("open peer segment: {e}")))?;
        let batch_id = match self.engine.allocate_batch_id(1) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                let _ = self.engine.close_segment(seg_id);
                return Err(StoreError::Internal(format!("allocate batch: {e}")));
            }
        };

        let read_len = replica.size as usize;
        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: seg_id,
            target_offset: result.pointers[0],
            length: read_len as u64,
        };

        if let Err(e) = self.engine.submit_transfer(batch_id, &[request]) {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(seg_id);
            return Err(StoreError::Internal(format!(
                "submit offload transfer: {e}"
            )));
        }

        if let Err(e) = self
            .wait_for_transfer_batch(batch_id, 1, tokio::time::Duration::from_secs(10))
            .await
        {
            let _ = self.engine.free_batch_id(batch_id);
            let _ = self.engine.close_segment(seg_id);
            return Err(StoreError::Internal(format!("poll offload transfer: {e}")));
        }

        if let Err(e) = self.engine.free_batch_id(batch_id) {
            let _ = self.engine.close_segment(seg_id);
            return Err(StoreError::Internal(format!("free offload batch: {e}")));
        }
        let _ = self.engine.close_segment(seg_id);

        // Fire-and-forget: release remote buffer.
        let release_addr = peer_addr.to_string();
        tokio::spawn(async move {
            crate::offload::client::release_offload_buffer(&release_addr, result.batch_id).await;
        });

        let data = self.local_buffer[..read_len].to_vec();
        Ok(data)
    }
}

use super::{
    finalize::{determine_finalize_decision, ReplicaTransferSummary},
    MooncakeClient,
};
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaType, ReplicateConfig, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest};

impl MooncakeClient {
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
                group_ids: cfg.group_ids.clone(),
            }),
        };

        let response = match self.master.put_start(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let err = Self::put_start_error_from_status(key, status);
                if matches!(err, StoreError::ObjectExists(_)) {
                    return Ok(());
                }
                return Err(err);
            }
        };
        let replicas = self.replicas_from_proto(&response.replicas);

        let mut transfer_summary = ReplicaTransferSummary::from_replicas(&replicas);
        let mut first_error = None;

        // Phase 2: zero_copy_write to each Memory/NoF replica / 阶段 2：零拷贝写入每个 Memory/NoF 副本
        for replica in &replicas {
            if !matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) {
                continue;
            }
            match self.zero_copy_write(replica, buffer, size).await {
                Ok(()) => transfer_summary.record_success(replica.replica_type),
                Err(e) => {
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // Phase 3: put_end / put_revoke according to C++ finalize decision.
        let decision = determine_finalize_decision(&cfg, &transfer_summary);
        self.finalize_put_for_key(key, decision, "").await?;
        if !decision.success {
            return Err(first_error.unwrap_or(StoreError::NoAvailableHandle));
        }

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
        if total_len > self.local_buffer.len() {
            return Err(StoreError::InvalidParams(format!(
                "object size {} exceeds local buffer size {}",
                total_len,
                self.local_buffer.len()
            )));
        }

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
                group_ids: cfg.group_ids.clone(),
            }),
        };

        let response = match self.master.put_start(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let err = Self::put_start_error_from_status(key, status);
                if matches!(err, StoreError::ObjectExists(_)) {
                    return Ok(());
                }
                return Err(err);
            }
        };
        let replicas = self.replicas_from_proto(&response.replicas);

        if replicas.is_empty() {
            return Err(StoreError::NoAvailableHandle);
        }

        let mut transfer_summary = ReplicaTransferSummary::from_replicas(&replicas);
        let mut first_error = None;

        // Phase 2: for each Memory/NoF replica, copy all slices into local_buffer
        // contiguously and submit as one batch.
        // 阶段 2：对每个副本，将所有切片连续拷贝到 local_buffer 并作为单个批次提交。
        for replica in &replicas {
            if !matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) {
                continue;
            }
            let segment_id = match self.engine.open_segment(&replica.segment_name) {
                Ok(segment_id) => segment_id,
                Err(e) => {
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(e.into());
                    }
                    continue;
                }
            };
            let batch_id = match self.engine.allocate_batch_id(values.len()) {
                Ok(batch_id) => batch_id,
                Err(e) => {
                    let _ = self.engine.close_segment(segment_id);
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(e.into());
                    }
                    continue;
                }
            };
            let mut replica_failed = false;

            // Build TransferRequests: each slice → one RDMA write at the correct
            // offset within the replica.
            // 构建传输请求：每个切片 → 在副本内正确偏移处的一次 RDMA 写。
            let requests: Vec<TransferRequest> = values
                .iter()
                .enumerate()
                .map(|(i, data)| {
                    let src_offset = values[..i].iter().map(|v| v.len()).sum::<usize>();
                    let tgt_offset = replica.base_addr + replica.offset + src_offset as u64;
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

            if let Err(e) = self.engine.submit_transfer(batch_id, &requests) {
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                transfer_summary.record_failure(replica.replica_type);
                if first_error.is_none() {
                    first_error = Some(e.into());
                }
                continue;
            }

            if let Err(e) = self
                .wait_for_transfer_batch(
                    batch_id,
                    values.len(),
                    tokio::time::Duration::from_secs(10),
                )
                .await
            {
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(segment_id);
                transfer_summary.record_failure(replica.replica_type);
                if first_error.is_none() {
                    first_error = Some(e);
                }
                replica_failed = true;
            }

            // Cleanup per-replica resources. / 清理每个副本的资源。
            if !replica_failed {
                self.engine.free_batch_id(batch_id)?;
                self.engine.close_segment(segment_id)?;
                transfer_summary.record_success(replica.replica_type);
            }
        }

        // Phase 3: put_end / put_revoke according to C++ finalize decision.
        let decision = determine_finalize_decision(&cfg, &transfer_summary);
        self.finalize_put_for_key(key, decision, "").await?;
        if !decision.success {
            return Err(first_error.unwrap_or(StoreError::NoAvailableHandle));
        }

        Ok(())
    }
}

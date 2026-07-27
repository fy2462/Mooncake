use super::{
    MooncakeClient,
    finalize::{ReplicaTransferSummary, determine_finalize_decision},
};
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaType, ReplicateConfig, StoreError};
use std::ffi::c_void;
use transfer_engine_ffi::{RegisteredSubmitOutcome, RegisteredTransferRequest};

impl MooncakeClient {
    /// `buffer` is never dereferenced directly. It must resolve to a live
    /// owner-bearing readable registration covering `size` bytes.
    pub async fn put_from(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let started_at = std::time::Instant::now();
        let cfg = config.unwrap_or_default();
        let tenant_id = self.tenant_id.clone();
        self.invalidate_hot_cache_key_for_tenant(key, &tenant_id);

        // Phase 1: put_start / 阶段 1：put_start
        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: size as u64,
            tenant_id: tenant_id.clone(),
            config: Some(self.replicate_config_to_proto(&cfg)),
        };

        let response = match self.master.put_start(self.rpc_request(request)).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let err = Self::put_start_error_from_status(key, status);
                if matches!(err, StoreError::ObjectExists(_)) {
                    if let Some(metrics) = &self.metrics {
                        metrics.observe_operation(
                            super::metrics::TransferOperationKind::Write,
                            "put_from",
                            size as u64,
                            started_at.elapsed(),
                        );
                    }
                    return Ok(());
                }
                return Err(err);
            }
        };
        let replicas = self.replicas_from_proto(&response.replicas);
        let buffer = self.resolve_readable_buffer_region(buffer, size)?;

        let mut transfer_summary = ReplicaTransferSummary::from_replicas(&replicas);
        let mut first_error = None;

        // Phase 2: zero_copy_write to each Memory/NoF replica / 阶段 2：零拷贝写入每个 Memory/NoF 副本
        for replica in &replicas {
            if replica.replica_type == ReplicaType::Disk {
                match self
                    .write_global_disk_parts(key, &tenant_id, replica, &[buffer.clone()], &[size])
                    .await
                {
                    Ok(()) => transfer_summary.record_success(ReplicaType::Disk),
                    Err(error) => {
                        transfer_summary.record_failure(ReplicaType::Disk);
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
                continue;
            }
            if !matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) {
                continue;
            }
            match self.zero_copy_write(replica, buffer.clone()).await {
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
        self.finalize_put_for_key(key, decision, &tenant_id).await?;
        self.finalize_global_disk_for_key(key, &transfer_summary, &tenant_id)
            .await?;
        if !decision.success {
            return Err(first_error.unwrap_or(StoreError::NoAvailableHandle));
        }

        if let Some(metrics) = &self.metrics {
            metrics.observe_operation(
                super::metrics::TransferOperationKind::Write,
                "put_from",
                size as u64,
                started_at.elapsed(),
            );
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Put parts — split a logical object into multiple slices, write them
    //            together in a single batch per replica.
    // 分段写入 —— 将一个逻辑对象拆分为多个切片，每个副本在单个批次中一起写入。
    //
    // The total length is derived from the sum of all slices and sent in
    // put_start. Each slice becomes a separate RegisteredTransferRequest in one batch,
    // sharing a single segment_id and batch_id.
    //
    // 总长度从所有切片的总和计算并通过 put_start 发送。每个切片成为一个
    // 独立 RegisteredTransferRequest，共享一个 segment_id 和一个 owned batch。
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
        let started_at = std::time::Instant::now();
        let cfg = config.unwrap_or_default();
        let tenant_id = self.tenant_id.clone();
        let total_len = values.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.len())
                .ok_or_else(|| StoreError::InvalidParams("object size overflow".to_string()))
        })?;
        if total_len > self.local_buffer.len() {
            return Err(StoreError::InvalidParams(format!(
                "object size {} exceeds local buffer size {}",
                total_len,
                self.local_buffer.len()
            )));
        }
        self.local_buffer.wait_until_available().await;
        let lease = self.local_buffer.lease()?;
        let mut offset = 0usize;
        for value in values {
            self.local_buffer.copy_from_slice(&lease, offset, value)?;
            offset += value.len();
        }
        let mut staging_lease = Some(lease);
        self.invalidate_hot_cache_key_for_tenant(key, &tenant_id);

        // Phase 1: put_start with total_len / 阶段 1：put_start 带上 total_len
        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: total_len as u64,
            tenant_id: tenant_id.clone(),
            config: Some(self.replicate_config_to_proto(&cfg)),
        };

        let response = match self.master.put_start(self.rpc_request(request)).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let err = Self::put_start_error_from_status(key, status);
                if matches!(err, StoreError::ObjectExists(_)) {
                    if let Some(metrics) = &self.metrics {
                        metrics.observe_operation(
                            super::metrics::TransferOperationKind::Write,
                            "put_parts",
                            total_len as u64,
                            started_at.elapsed(),
                        );
                    }
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

        // Phase 2: submit the already prepared, exclusively leased staging
        // payload to every Memory/NoF replica.
        for replica in &replicas {
            if replica.replica_type == ReplicaType::Disk {
                let payload = values
                    .iter()
                    .flat_map(|value| value.iter().copied())
                    .collect::<Vec<_>>();
                match self
                    .write_global_disk_replica(key, &tenant_id, replica, &payload)
                    .await
                {
                    Ok(()) => transfer_summary.record_success(ReplicaType::Disk),
                    Err(error) => {
                        transfer_summary.record_failure(ReplicaType::Disk);
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
                continue;
            }
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
            let mut replica_failed = false;

            // Build TransferRequests: each slice → one RDMA write at the correct
            // offset within the replica.
            // 构建传输请求：每个切片 → 在副本内正确偏移处的一次 RDMA 写。
            let active_lease = staging_lease
                .as_ref()
                .expect("staging lease is restored between replica transfers");
            let requests_result = (|| -> StoreResult<_> {
                let mut source_offset = 0usize;
                let mut requests = Vec::with_capacity(values.len());
                for data in values {
                    let target_offset =
                        Self::checked_replica_target_offset(replica, source_offset)?;
                    requests.push(RegisteredTransferRequest::write(
                        self.local_buffer.readable_region(
                            active_lease,
                            source_offset,
                            data.len(),
                        )?,
                        segment_id,
                        target_offset,
                        data.len(),
                    )?);
                    source_offset = source_offset.checked_add(data.len()).ok_or_else(|| {
                        StoreError::InvalidParams(
                            "put_parts source offset overflows usize".to_string(),
                        )
                    })?;
                }
                Ok(requests)
            })();
            let requests = match self.close_segment_on_prepare_error(segment_id, requests_result) {
                Ok(requests) => requests,
                Err(error) => {
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };

            let outcome = match self.engine.submit_transfer(requests) {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = self.engine.close_segment(segment_id);
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(error.into());
                    }
                    continue;
                }
            };
            let batch = match outcome {
                RegisteredSubmitOutcome::Submitted(batch) => batch,
                RegisteredSubmitOutcome::NativeRejected { error, batch } => {
                    let payload = staging_lease
                        .take()
                        .expect("staging lease is present before native handoff");
                    let release_error = match self
                        .release_failed_submission_owned(
                            batch,
                            segment_id,
                            std::time::Duration::from_secs(10),
                            payload,
                        )
                        .await
                    {
                        Ok(completion) => {
                            staging_lease = Some(completion.payload);
                            None
                        }
                        Err(release_error) => {
                            // A reaper error is reported only after the native
                            // batch is quiescent; reacquiring preserves the
                            // prepared staging bytes for later replicas.
                            staging_lease = self.local_buffer.lease().ok();
                            Some(release_error)
                        }
                    };
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(release_error.unwrap_or_else(|| error.into()));
                    }
                    if staging_lease.is_none() {
                        break;
                    }
                    continue;
                }
            };
            let payload = staging_lease
                .take()
                .expect("staging lease is present before native handoff");
            match self
                .wait_for_transfer_batch_terminal_owned(
                    batch,
                    segment_id,
                    std::time::Duration::from_secs(10),
                    payload,
                )
                .await
            {
                Ok(completion) => {
                    let completed = super::transfer::transfer_statuses_match_lengths(
                        &completion.statuses,
                        values.iter().map(|value| value.len() as u64),
                    );
                    staging_lease = Some(completion.payload);
                    if !completed {
                        transfer_summary.record_failure(replica.replica_type);
                        if first_error.is_none() {
                            first_error = Some(StoreError::OperationFailed(-1));
                        }
                        replica_failed = true;
                    } else if let Some(metrics) = &self.metrics {
                        metrics.observe_transfer_bytes(
                            super::metrics::TransferOperationKind::Write,
                            total_len as u64,
                        );
                    }
                }
                Err(e) => {
                    staging_lease = self.local_buffer.lease().ok();
                    transfer_summary.record_failure(replica.replica_type);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                    replica_failed = true;
                }
            }

            if !replica_failed {
                transfer_summary.record_success(replica.replica_type);
            }
            if staging_lease.is_none() {
                break;
            }
        }
        drop(staging_lease);

        // Phase 3: put_end / put_revoke according to C++ finalize decision.
        let decision = determine_finalize_decision(&cfg, &transfer_summary);
        self.finalize_put_for_key(key, decision, &tenant_id).await?;
        self.finalize_global_disk_for_key(key, &transfer_summary, &tenant_id)
            .await?;
        if !decision.success {
            return Err(first_error.unwrap_or(StoreError::NoAvailableHandle));
        }

        if let Some(metrics) = &self.metrics {
            metrics.observe_operation(
                super::metrics::TransferOperationKind::Write,
                "put_parts",
                total_len as u64,
                started_at.elapsed(),
            );
        }
        Ok(())
    }
}

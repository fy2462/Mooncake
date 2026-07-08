use super::{
    finalize::{determine_finalize_decision, ReplicaTransferSummary},
    write::BATCH_STATUS_OBJECT_ALREADY_EXISTS,
    MooncakeClient,
};
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaType, ReplicateConfig, StoreError};
use std::ffi::c_void;

pub(super) fn metadata_value_buffers(
    buffer: *mut c_void,
    metadata_buffer: *mut c_void,
    size: usize,
    metadata_size: usize,
) -> Option<(Vec<*mut c_void>, Vec<usize>)> {
    let mut buffers = Vec::with_capacity(2);
    let mut sizes = Vec::with_capacity(2);
    if metadata_size > 0 {
        buffers.push(metadata_buffer);
        sizes.push(metadata_size);
    }
    if size > 0 {
        buffers.push(buffer);
        sizes.push(size);
    }
    (!buffers.is_empty()).then_some((buffers, sizes))
}

impl MooncakeClient {
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
        let tenant_id = self.tenant_id.clone();
        // Phase 1: BatchPutStart — allocate replicas for all keys in one RPC.
        let slice_lengths: Vec<u64> = values.iter().map(|v| v.len() as u64).collect();
        let start_results = match self
            .batch_put_start_results(keys, &slice_lengths, &cfg, &tenant_id)
            .await
        {
            Ok(r) => r,
            Err(_) => {
                // All allocation failed — all keys fail.
                return Ok(vec![-1; keys.len()]);
            }
        };
        if start_results.len() != keys.len() {
            return Ok(vec![-1; keys.len()]);
        }

        // Phase 2: per-key TE writes. C++ BatchPutStart returns one expected
        // result per key; consume Rust's per-key results to avoid flattened
        // replica misalignment when one key fails allocation or already exists.
        let mut statuses = vec![-1i32; keys.len()];
        let mut decisions = Vec::new();

        for (ki, (_key, start_result)) in keys.iter().zip(start_results.iter()).enumerate() {
            if start_result.status == BATCH_STATUS_OBJECT_ALREADY_EXISTS {
                statuses[ki] = 0;
                continue;
            }
            if start_result.status != 0 || start_result.replicas.is_empty() {
                statuses[ki] = start_result.status;
                continue;
            }

            let mut transfer_summary =
                ReplicaTransferSummary::from_replicas(&start_result.replicas);
            for replica in &start_result.replicas {
                if !matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                ) {
                    continue;
                }
                match self.write_to_replica(replica, values[ki]).await {
                    Ok(()) => transfer_summary.record_success(replica.replica_type),
                    Err(_) => transfer_summary.record_failure(replica.replica_type),
                }
            }

            let decision = determine_finalize_decision(&cfg, &transfer_summary);
            if decision.success {
                statuses[ki] = 0;
            }
            decisions.push((ki, decision));
        }

        // Phase 3: BatchPutEnd / BatchPutRevoke.
        self.finalize_batch_put_groups(keys, &decisions, &mut statuses, &tenant_id)
            .await;

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
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            return Err(StoreError::InvalidParams(
                "keys, buffers, and sizes length mismatch".to_string(),
            ));
        }

        let all_buffers: Vec<Vec<*mut c_void>> =
            buffers.iter().map(|&buffer| vec![buffer]).collect();
        let all_sizes: Vec<Vec<usize>> = sizes.iter().map(|&size| vec![size]).collect();
        unsafe {
            self.batch_put_from_multi_buffers(keys, &all_buffers, &all_sizes, config)
                .await
        }
    }

    /// Zero-copy put from a data buffer plus a metadata buffer.
    ///
    /// The object layout matches C++ `RealClient::put_from_with_metadata`:
    /// metadata bytes are written first, followed by data bytes. Metadata-only
    /// zero-sized tensors are stored when `metadata_size > 0`.
    ///
    /// # Safety
    /// `buffer` and `metadata_buffer` must be valid and pre-registered with the
    /// TransferEngine for their respective sizes.
    pub async unsafe fn put_from_with_metadata(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        metadata_buffer: *mut c_void,
        size: usize,
        metadata_size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<i32> {
        let cfg = config.unwrap_or_default();
        if cfg.prefer_alloc_in_same_node {
            return Ok(-1);
        }
        let Some((buffers, sizes)) =
            metadata_value_buffers(buffer, metadata_buffer, size, metadata_size)
        else {
            return Ok(0);
        };

        let keys = vec![key.to_string()];
        let statuses = unsafe {
            self.batch_put_from_multi_buffers(&keys, &[buffers], &[sizes], Some(cfg))
                .await?
        };
        Ok(statuses.first().copied().unwrap_or(-1))
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
        if keys.len() != all_buffers.len() || keys.len() != all_sizes.len() {
            return Err(StoreError::InvalidParams(
                "keys, all_buffers, and all_sizes length mismatch".to_string(),
            ));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        for (idx, (buffers, sizes)) in all_buffers.iter().zip(all_sizes.iter()).enumerate() {
            if buffers.len() != sizes.len() {
                return Err(StoreError::InvalidParams(format!(
                    "buffers and sizes length mismatch for key index {idx}"
                )));
            }
            if buffers.is_empty() {
                return Err(StoreError::InvalidParams(format!(
                    "key index {idx} has no buffers"
                )));
            }
        }

        let cfg = config.unwrap_or_default();
        let tenant_id = self.tenant_id.clone();
        let slice_lengths: Vec<u64> = all_sizes
            .iter()
            .map(|sizes| sizes.iter().map(|&size| size as u64).sum())
            .collect();

        let start_results = match self
            .batch_put_start_results(keys, &slice_lengths, &cfg, &tenant_id)
            .await
        {
            Ok(results) => results,
            Err(_) => return Ok(vec![-1; keys.len()]),
        };
        if start_results.len() != keys.len() {
            return Ok(vec![-1; keys.len()]);
        }

        let mut statuses = vec![-1i32; keys.len()];
        let mut decisions = Vec::new();

        for (idx, (_key, start_result)) in keys.iter().zip(start_results.iter()).enumerate() {
            if start_result.status == BATCH_STATUS_OBJECT_ALREADY_EXISTS {
                statuses[idx] = 0;
                continue;
            }
            if start_result.status != 0 || start_result.replicas.is_empty() {
                statuses[idx] = start_result.status;
                continue;
            }

            let mut transfer_summary =
                ReplicaTransferSummary::from_replicas(&start_result.replicas);
            for replica in &start_result.replicas {
                if !matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                ) {
                    continue;
                }
                if unsafe {
                    self.write_parts_from_to_replica(replica, &all_buffers[idx], &all_sizes[idx])
                        .await
                }
                .is_err()
                {
                    transfer_summary.record_failure(replica.replica_type);
                } else {
                    transfer_summary.record_success(replica.replica_type);
                }
            }

            let decision = determine_finalize_decision(&cfg, &transfer_summary);
            if decision.success {
                statuses[idx] = 0;
            }
            decisions.push((idx, decision));
        }

        self.finalize_batch_put_groups(keys, &decisions, &mut statuses, &tenant_id)
            .await;

        Ok(statuses)
    }
}

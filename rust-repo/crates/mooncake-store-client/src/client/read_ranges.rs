use super::CachedQueryResultResponse;
use super::MooncakeClient;
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use std::collections::HashMap;
use std::ffi::c_void;
use transfer_engine_ffi::{RegisteredSubmitOutcome, RegisteredTransferRequest, TransferStatusEnum};

fn successful_range_bytes(results: &[Vec<Vec<i64>>]) -> u64 {
    results
        .iter()
        .flatten()
        .flatten()
        .filter_map(|result| u64::try_from(*result).ok())
        .fold(0_u64, u64::saturating_add)
}

impl MooncakeClient {
    /// Safe copy-based multi-range read.
    ///
    /// This variant accepts ordinary mutable slices and never registers or
    /// exposes caller pointers. It fetches each object through the safe Store
    /// path, then copies validated ranges into the destination buffers.
    pub async fn get_into_ranges_copy(
        &mut self,
        buffers: &mut [&mut [u8]],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        if buffers.len() != keys.len()
            || buffers.len() != dst_offsets.len()
            || buffers.len() != src_offsets.len()
            || buffers.len() != sizes.len()
        {
            return Err(StoreError::InvalidParams(
                "buffers, keys, dst_offsets, src_offsets, and sizes length mismatch".to_string(),
            ));
        }

        let mut results = Vec::with_capacity(buffers.len());
        for buf_idx in 0..buffers.len() {
            let key_count = keys[buf_idx].len();
            if dst_offsets[buf_idx].len() != key_count
                || src_offsets[buf_idx].len() != key_count
                || sizes[buf_idx].len() != key_count
            {
                return Err(StoreError::InvalidParams(format!(
                    "range matrix key dimension mismatch at buffer index {buf_idx}"
                )));
            }

            let mut buffer_results = Vec::with_capacity(key_count);
            for key_idx in 0..key_count {
                let range_count = sizes[buf_idx][key_idx].len();
                if dst_offsets[buf_idx][key_idx].len() != range_count
                    || src_offsets[buf_idx][key_idx].len() != range_count
                {
                    // C++/wheel contract: a mismatched range matrix reports
                    // negative sentinels for that key instead of aborting the
                    // whole call.
                    buffer_results.push(vec![-1; range_count]);
                    continue;
                }

                let value = match self.get(&keys[buf_idx][key_idx]).await {
                    Ok(value) => value,
                    Err(StoreError::KeyNotFound(_)) => {
                        buffer_results.push(vec![-1; range_count]);
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let mut range_results = vec![-1; range_count];
                for range_idx in 0..range_count {
                    let size = sizes[buf_idx][key_idx][range_idx];
                    let source_offset = src_offsets[buf_idx][key_idx][range_idx];
                    let destination_offset = dst_offsets[buf_idx][key_idx][range_idx];
                    let Some(source_end) = source_offset.checked_add(size) else {
                        continue;
                    };
                    let Some(destination_end) = destination_offset.checked_add(size) else {
                        continue;
                    };
                    let Some(source) = value.get(source_offset..source_end) else {
                        continue;
                    };
                    let Some(destination) =
                        buffers[buf_idx].get_mut(destination_offset..destination_end)
                    else {
                        continue;
                    };
                    destination.copy_from_slice(source);
                    range_results[range_idx] = i64::try_from(size).unwrap_or(-1);
                }
                buffer_results.push(range_results);
            }
            results.push(buffer_results);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Get into ranges (zero-copy multi-range read)
    // 多范围零拷贝读取：对多个 key 的多个偏移范围执行批量 RDMA 读
    //
    // C++ equivalent: Client::GetIntoRanges() — submits multiple range requests
    // in a single batch for efficiency.
    // -----------------------------------------------------------------------

    /// Zero-copy multi-range read: for each key, issue multiple RDMA reads at
    /// different offsets into a single destination buffer.
    ///
    /// # Arguments
    /// - `buffers[bi]` — destination buffer for batch `bi` (pre-registered).
    ///   批次 bi 的目标缓冲区（预先注册）。
    /// - `keys[bi]` — list of keys to read for batch `bi`.
    ///   批次 bi 要读取的 key 列表。
    /// - `dst_offsets[bi][ki]` — destination offsets within `buffers[bi]`.
    ///   buffers[bi] 内的目标偏移量列表。
    /// - `src_offsets[bi][ki]` — source offsets within each replica.
    ///   每个副本内的源偏移量列表。
    /// - `sizes[bi][ki]` — byte length for each range. / 每个范围的字节长度。
    ///
    /// # Returns (返回值)
    /// A 3D result matrix: `results[bi][ki][ri]` = bytes transferred, or -1 on
    /// error / timeout.
    /// 三维结果矩阵：results[bi][ki][ri] = 传输字节数，错误/超时时为 -1。
    ///
    /// Every destination range is resolved to a live owner-bearing writable
    /// registration before use.
    pub async fn get_into_ranges(
        &mut self,
        buffers: &[*mut c_void],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        let started_at = std::time::Instant::now();
        let result = self
            .get_into_ranges_internal(buffers, keys, dst_offsets, src_offsets, sizes, None)
            .await;
        if let Ok(results) = &result
            && let Some(metrics) = &self.metrics
        {
            metrics.observe_operation(
                super::metrics::TransferOperationKind::Read,
                "get_into_ranges",
                successful_range_bytes(results),
                started_at.elapsed(),
            );
        }
        result
    }

    /// Same as [`get_into_ranges`](Self::get_into_ranges), but reuses cached
    /// query results produced by [`batch_get_query_results`](Self::batch_get_query_results).
    ///
    /// C++ equivalent: `RealClient::get_into_ranges(..., QueryResultCache*)`.
    pub async fn get_into_ranges_with_query_cache(
        &mut self,
        buffers: &[*mut c_void],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
        query_result_cache: &HashMap<String, CachedQueryResultResponse>,
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        let started_at = std::time::Instant::now();
        let result = self
            .get_into_ranges_internal(
                buffers,
                keys,
                dst_offsets,
                src_offsets,
                sizes,
                Some(query_result_cache),
            )
            .await;
        if let Ok(results) = &result
            && let Some(metrics) = &self.metrics
        {
            metrics.observe_operation(
                super::metrics::TransferOperationKind::Read,
                "get_into_ranges",
                successful_range_bytes(results),
                started_at.elapsed(),
            );
        }
        result
    }

    async fn get_into_ranges_internal(
        &mut self,
        buffers: &[*mut c_void],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
        query_result_cache: Option<&HashMap<String, CachedQueryResultResponse>>,
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        if buffers.len() != keys.len()
            || buffers.len() != dst_offsets.len()
            || buffers.len() != src_offsets.len()
            || buffers.len() != sizes.len()
        {
            return Err(StoreError::InvalidParams(
                "buffers, keys, dst_offsets, src_offsets, and sizes length mismatch".to_string(),
            ));
        }

        for buf_idx in 0..keys.len() {
            let key_count = keys[buf_idx].len();
            if dst_offsets[buf_idx].len() != key_count
                || src_offsets[buf_idx].len() != key_count
                || sizes[buf_idx].len() != key_count
            {
                return Err(StoreError::InvalidParams(format!(
                    "range matrix key dimension mismatch at buffer index {buf_idx}"
                )));
            }
            // Range-dimension mismatches are reported per key as negative
            // sentinels by the read loop below (C++/wheel contract), so no
            // upfront validation error is raised here.
        }

        let count = buffers.len();
        let mut results = Vec::with_capacity(count);
        for buf_idx in 0..count {
            let mut buf_results = vec![];
            for (key_idx, key) in keys[buf_idx].iter().enumerate() {
                let range_count = sizes[buf_idx][key_idx].len();
                if dst_offsets[buf_idx][key_idx].len() != range_count
                    || src_offsets[buf_idx][key_idx].len() != range_count
                {
                    // C++/wheel contract: mismatched range matrices report
                    // negative sentinels for that key instead of aborting.
                    buf_results.push(vec![-1; range_count]);
                    continue;
                }
                let replicas = match query_result_cache.and_then(|cache| cache.get(key)) {
                    Some(cached) if cached.success && !cached.is_lease_expired() => {
                        cached.replicas.clone()
                    }
                    Some(cached) if !cached.success => {
                        tracing::warn!(
                            key = %key,
                            status = cached.error_status,
                            error = %cached.error_message,
                            "cached query result is an error"
                        );
                        buf_results.push(vec![-1; range_count]);
                        continue;
                    }
                    _ => match self.fetch_replicas(key).await {
                        Ok(replicas) => replicas,
                        // Missing keys report negative sentinels and let the
                        // remaining keys continue (C++/wheel contract).
                        Err(StoreError::KeyNotFound(_)) => {
                            buf_results.push(vec![-1; range_count]);
                            continue;
                        }
                        Err(error) => return Err(error),
                    },
                };
                let replica = match self.select_best_replica(&replicas) {
                    Some(r) => r,
                    None => {
                        // Key not found — mark all ranges as -1.
                        // 未找到 key —— 所有范围标记为 -1。
                        buf_results.push(vec![-1; range_count]);
                        continue;
                    }
                };

                let mut range_results: Vec<i64> = vec![-1; range_count];
                let mut valid_ranges = Vec::new();
                let mut staging_offset = 0_usize;
                for (ri, &sz) in sizes[buf_idx][key_idx].iter().enumerate() {
                    let Some(src_end) = src_offsets[buf_idx][key_idx][ri].checked_add(sz) else {
                        continue;
                    };
                    let dst = dst_offsets[buf_idx][key_idx][ri];
                    if dst.checked_add(sz).is_none() || src_end as u64 > replica.size {
                        continue;
                    }
                    let target = buffers[buf_idx].wrapping_byte_add(dst);
                    let Ok(region) = self.resolve_writable_buffer_region(target, sz) else {
                        continue;
                    };
                    let is_device = crate::data_plane_ffi::is_device_memory(
                        self.accelerator.as_ref(),
                        region.foreign_region(),
                    )?;
                    let range_staging_offset = if is_device {
                        let Some(end) = staging_offset.checked_add(sz) else {
                            continue;
                        };
                        if end > self.local_buffer.len() {
                            continue;
                        }
                        let offset = staging_offset;
                        staging_offset = end;
                        Some(offset)
                    } else {
                        None
                    };
                    valid_ranges.push((ri, region, range_staging_offset));
                }
                if valid_ranges.is_empty() {
                    buf_results.push(range_results);
                    continue;
                }

                if replica.replica_type == mooncake_store_core::ReplicaType::Disk {
                    let Some(storage) = self.global_disk.as_ref() else {
                        buf_results.push(range_results);
                        continue;
                    };
                    for (range_idx, region, _) in &valid_ranges {
                        let source_offset = src_offsets[buf_idx][key_idx][*range_idx] as u64;
                        let size = sizes[buf_idx][key_idx][*range_idx];
                        if let Ok(data) = storage
                            .read_range(
                                replica.segment_name.clone(),
                                replica.size,
                                source_offset,
                                size,
                            )
                            .await
                        {
                            if self
                                .accelerator
                                .copy_from_host(region.foreign_region(), &data)
                                .is_ok()
                            {
                                range_results[*range_idx] = size as i64;
                            }
                        }
                    }
                    buf_results.push(range_results);
                    continue;
                }

                let staging_lease = if staging_offset != 0 {
                    self.local_buffer.wait_until_available().await;
                    Some(self.local_buffer.lease()?)
                } else {
                    None
                };

                // Open the target segment on the TransferEngine.
                // 在 TransferEngine 上打开目标 segment。
                let seg = self.engine.open_segment(&replica.segment_name)?;

                // Build TransferRequests: one per range, all in the same batch.
                // 构建传输请求：每个范围一个，全部在同一批次中。
                let mut request_lengths = Vec::with_capacity(valid_ranges.len());
                let reqs = self.close_segment_on_prepare_error(
                    seg,
                    (|| -> StoreResult<_> {
                        let mut reqs = Vec::with_capacity(valid_ranges.len());
                        for (ri, region, range_staging_offset) in &valid_ranges {
                            let sz = sizes[buf_idx][key_idx][*ri];
                            let local = match *range_staging_offset {
                                Some(offset) => self.local_buffer.writable_region(
                                    staging_lease
                                        .as_ref()
                                        .expect("device range has a staging lease"),
                                    offset,
                                    sz,
                                )?,
                                None => region.registered_region(),
                            };
                            reqs.push(RegisteredTransferRequest::read(
                                local,
                                seg,
                                Self::checked_replica_target_offset(
                                    replica,
                                    src_offsets[buf_idx][key_idx][*ri],
                                )?,
                                sz,
                            )?);
                            request_lengths.push(sz as u64);
                        }
                        Ok(reqs)
                    })(),
                )?;

                let outcome = match self.engine.submit_transfer(reqs) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        let _ = self.engine.close_segment(seg);
                        return Err(error.into());
                    }
                };
                let batch = match outcome {
                    RegisteredSubmitOutcome::Submitted(batch) => batch,
                    RegisteredSubmitOutcome::NativeRejected { error, batch } => {
                        let _completion = self
                            .release_failed_submission_owned(
                                batch,
                                seg,
                                std::time::Duration::from_secs(10),
                                (valid_ranges, staging_lease, request_lengths),
                            )
                            .await?;
                        return Err(error.into());
                    }
                };

                let completion = self
                    .wait_for_transfer_batch_terminal_owned(
                        batch,
                        seg,
                        std::time::Duration::from_secs(10),
                        (valid_ranges, staging_lease, request_lengths),
                    )
                    .await?;
                let statuses = completion.statuses;
                let (valid_ranges, staging_lease, request_lengths) = completion.payload;
                let completed_bytes = statuses
                    .iter()
                    .zip(request_lengths.iter())
                    .filter(|(status, request_length)| {
                        status.status == TransferStatusEnum::Completed
                            && status.transferred_bytes == **request_length
                    })
                    .fold(0_u64, |total, (status, _)| {
                        total.saturating_add(status.transferred_bytes)
                    });
                for ((status, (range_idx, region, range_staging_offset)), request_length) in
                    statuses
                        .iter()
                        .zip(valid_ranges.iter())
                        .zip(request_lengths.iter())
                {
                    range_results[*range_idx] = if status.status == TransferStatusEnum::Completed
                        && status.transferred_bytes == *request_length
                    {
                        if let Some(offset) = *range_staging_offset {
                            let transferred =
                                usize::try_from(status.transferred_bytes).map_err(|_| {
                                    StoreError::Internal(
                                        "range transfer byte count cannot fit usize".to_string(),
                                    )
                                })?;
                            let end = offset.checked_add(transferred).ok_or_else(|| {
                                StoreError::Internal(
                                    "range staging result overflows usize".to_string(),
                                )
                            })?;
                            if end > self.local_buffer.len() {
                                return Err(StoreError::Internal(
                                    "range transfer exceeded staging buffer".to_string(),
                                ));
                            }
                            crate::data_plane_ffi::scatter_host_to_device(
                                self.accelerator.as_ref(),
                                region.foreign_region(),
                                &self.local_buffer.copy_to_vec(
                                    staging_lease.as_ref().ok_or_else(|| {
                                        StoreError::Internal(
                                            "device range lost its staging lease".to_string(),
                                        )
                                    })?,
                                    offset,
                                    transferred,
                                )?,
                            )?;
                        }
                        status.transferred_bytes as i64
                    } else {
                        -1
                    };
                }
                if let Some(metrics) = &self.metrics {
                    metrics.observe_transfer_bytes(
                        super::metrics::TransferOperationKind::Read,
                        completed_bytes,
                    );
                }
                drop(staging_lease);
                buf_results.push(range_results);
            }
            results.push(buf_results);
        }
        Ok(results)
    }
}

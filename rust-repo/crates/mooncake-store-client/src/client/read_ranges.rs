use super::CachedQueryResultResponse;
use super::MooncakeClient;
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use std::collections::HashMap;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};

impl MooncakeClient {
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
    /// # Safety
    /// All buffers must be registered and live for the duration.
    /// 所有缓冲区必须已注册并在传输期间保持存活。
    pub async unsafe fn get_into_ranges(
        &mut self,
        buffers: &[*mut c_void],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        self.get_into_ranges_internal(buffers, keys, dst_offsets, src_offsets, sizes, None)
            .await
    }

    /// Same as [`get_into_ranges`](Self::get_into_ranges), but reuses cached
    /// query results produced by [`batch_get_query_results`](Self::batch_get_query_results).
    ///
    /// C++ equivalent: `RealClient::get_into_ranges(..., QueryResultCache*)`.
    pub async unsafe fn get_into_ranges_with_query_cache(
        &mut self,
        buffers: &[*mut c_void],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
        query_result_cache: &HashMap<String, CachedQueryResultResponse>,
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        self.get_into_ranges_internal(
            buffers,
            keys,
            dst_offsets,
            src_offsets,
            sizes,
            Some(query_result_cache),
        )
        .await
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
            for key_idx in 0..key_count {
                let range_count = sizes[buf_idx][key_idx].len();
                if dst_offsets[buf_idx][key_idx].len() != range_count
                    || src_offsets[buf_idx][key_idx].len() != range_count
                {
                    return Err(StoreError::InvalidParams(format!(
                        "range dimension mismatch at buffer index {buf_idx}, key index {key_idx}"
                    )));
                }
            }
        }

        let count = buffers.len();
        let mut results = Vec::with_capacity(count);
        for buf_idx in 0..count {
            let mut buf_results = vec![];
            for (key_idx, key) in keys[buf_idx].iter().enumerate() {
                let range_count = sizes[buf_idx][key_idx].len();
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
                    _ => self.fetch_replicas(key).await?,
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

                // Open the target segment on the TransferEngine.
                // 在 TransferEngine 上打开目标 segment。
                let seg = self.engine.open_segment(&replica.segment_name)?;

                // Allocate a batch_id for this key's range group.
                // 为此 key 的范围组分配 batch_id。
                let batch_id = match self.engine.allocate_batch_id(valid_ranges.len()) {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = self.engine.close_segment(seg);
                        return Err(e.into());
                    }
                };

                // Build TransferRequests: one per range, all in the same batch.
                // 构建传输请求：每个范围一个，全部在同一批次中。
                let reqs: Vec<TransferRequest> = valid_ranges
                    .iter()
                    .map(|&(ri, region, range_staging_offset)| {
                        let sz = sizes[buf_idx][key_idx][ri];
                        TransferRequest {
                            opcode: Opcode::Read,
                            source: range_staging_offset.map_or_else(
                                || region.as_mut_ptr(),
                                |offset| self.local_buffer[offset..].as_mut_ptr().cast(),
                            ),
                            target_id: seg,
                            target_offset: replica.base_addr
                                + replica.offset
                                + src_offsets[buf_idx][key_idx][ri] as u64,
                            length: sz as u64,
                        }
                    })
                    .collect();

                if let Err(e) = self.engine.submit_transfer(batch_id, &reqs) {
                    let _ = self.engine.free_batch_id(batch_id);
                    let _ = self.engine.close_segment(seg);
                    return Err(e.into());
                }

                let statuses = match self
                    .wait_for_transfer_batch_terminal(
                        batch_id,
                        valid_ranges.len(),
                        tokio::time::Duration::from_secs(10),
                    )
                    .await
                {
                    Ok(statuses) => statuses,
                    Err(e) => {
                        let _ = self.engine.free_batch_id(batch_id);
                        let _ = self.engine.close_segment(seg);
                        return Err(e);
                    }
                };
                for (status, &(range_idx, region, range_staging_offset)) in
                    statuses.iter().zip(valid_ranges.iter())
                {
                    range_results[range_idx] = if status.status == TransferStatusEnum::Completed {
                        if let Some(offset) = range_staging_offset {
                            let transferred = status.transferred_bytes as usize;
                            crate::data_plane_ffi::scatter_host_to_device(
                                self.accelerator.as_ref(),
                                region.foreign_region(),
                                &self.local_buffer[offset..offset + transferred],
                            )?;
                        }
                        status.transferred_bytes as i64
                    } else {
                        -1
                    };
                }
                self.engine.free_batch_id(batch_id)?;
                self.engine.close_segment(seg)?;
                buf_results.push(range_results);
            }
            results.push(buf_results);
        }
        Ok(results)
    }
}

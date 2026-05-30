use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};

use super::{BufferHandle, MooncakeClient};

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Get
    // -----------------------------------------------------------------------

    pub async fn get(&mut self, key: &str) -> StoreResult<Vec<u8>> {
        tracing::info!(target: "te_debug", %key, "get: ENTER");

        // Level 0: check local hot cache (fastest — no network)
        if let Some(ref cache) = self.hot_cache {
            if let Some(data) = cache.get(key) {
                tracing::info!(target: "te_debug", %key, data_len = data.len(), "get: HIT hot cache");
                return Ok(data);
            }
        }

        // Level 1: fetch from memory store (gRPC → RDMA)
        tracing::info!(target: "te_debug", %key, "get: fetching replicas from master");
        let replicas = self.fetch_replicas(key).await?;
        tracing::info!(target: "te_debug", %key, replica_count = replicas.len(), "get: replicas received");

        let replica = self.select_best_replica(&replicas);
        match replica {
            Some(r) => {
                tracing::info!(
                    target: "te_debug", %key,
                    seg_name = %r.segment_name,
                    replica_type = ?r.replica_type,
                    "get: selected replica, calling read_from_replica"
                );
                let data = self.read_from_replica(r).await?;
                tracing::info!(target: "te_debug", %key, data_len = data.len(), "get: read_from_replica success");
                // Store in hot cache for future hits
                if let Some(ref cache) = self.hot_cache {
                    cache.put(key, &data);
                }
                Ok(data)
            }
            None => {
                tracing::info!(target: "te_debug", %key, "get: no replica found, trying remote source");
                // Level 2: remote source fallback (S3 / local FS)
                if let Some(ref handler) = self.miss_handler {
                    if handler.is_enabled() {
                        match handler.handle_miss(key).await {
                            Ok(data) => {
                                tracing::info!(target: "te_debug", %key, data_len = data.len(), "get: remote source success");
                                if let Some(ref cache) = self.hot_cache {
                                    cache.put(key, &data);
                                }
                                Ok(data)
                            }
                            Err(remote_err) => {
                                tracing::warn!(
                                    key = %key,
                                    error = %remote_err,
                                    "remote source miss handler failed"
                                );
                                Err(StoreError::KeyNotFound(key.to_string()))
                            }
                        }
                    } else {
                        tracing::info!(target: "te_debug", %key, "get: miss handler disabled, key not found");
                        Err(StoreError::KeyNotFound(key.to_string()))
                    }
                } else {
                    tracing::info!(target: "te_debug", %key, "get: no miss handler, key not found");
                    Err(StoreError::KeyNotFound(key.to_string()))
                }
            }
        }
    }

    pub async unsafe fn get_into(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<usize> {
        let replicas = self.fetch_replicas(key).await?;
        let replica = self
            .select_best_replica(&replicas)
            .ok_or(StoreError::KeyNotFound(key.to_string()))?;
        self.zero_copy_read(replica, buffer, size).await
    }

    // -----------------------------------------------------------------------
    // Get into ranges (zero-copy multi-range read)
    // -----------------------------------------------------------------------

    pub async unsafe fn get_into_ranges(
        &mut self,
        buffers: &[*mut c_void],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        let count = buffers
            .len()
            .min(keys.len())
            .min(dst_offsets.len())
            .min(src_offsets.len())
            .min(sizes.len());
        let mut results = Vec::with_capacity(count);
        for buf_idx in 0..count {
            let mut buf_results = vec![];
            for (key_idx, key) in keys[buf_idx].iter().enumerate() {
                let replicas = self.fetch_replicas(key).await?;
                let replica = match self.select_best_replica(&replicas) {
                    Some(r) => r,
                    None => {
                        buf_results.push(vec![-1]);
                        continue;
                    }
                };
                let seg = self.engine.open_segment(&replica.segment_name)?;
                let batch_id = match self
                    .engine
                    .allocate_batch_id(sizes[buf_idx][key_idx].len())
                {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = self.engine.close_segment(seg);
                        return Err(e.into());
                    }
                };

                let reqs: Vec<TransferRequest> = sizes[buf_idx][key_idx]
                    .iter()
                    .enumerate()
                    .map(|(ri, &sz)| TransferRequest {
                        opcode: Opcode::Read,
                        source: buffers[buf_idx].byte_add(dst_offsets[buf_idx][key_idx][ri]),
                        target_id: seg,
                        target_offset: replica.base_addr
                            + replica.offset
                            + src_offsets[buf_idx][key_idx][ri] as u64,
                        length: sz as u64,
                    })
                    .collect();

                self.engine.submit_transfer(batch_id, &reqs)?;

                let mut range_results: Vec<i64> = vec![0; sizes[buf_idx][key_idx].len()];
                let start = tokio::time::Instant::now();
                let timeout = tokio::time::Duration::from_secs(10);
                for ri in 0..sizes[buf_idx][key_idx].len() {
                    loop {
                        let status = self.engine.get_transfer_status(batch_id, ri)?;
                        if status.status == TransferStatusEnum::Completed {
                            range_results[ri] = status.transferred_bytes as i64;
                            break;
                        }
                        if status.status == TransferStatusEnum::Failed {
                            range_results[ri] = -1;
                            break;
                        }
                        if start.elapsed() > timeout {
                            range_results[ri] = -1;
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                    }
                }
                self.engine.free_batch_id(batch_id)?;
                self.engine.close_segment(seg)?;
                buf_results.push(range_results);
            }
            results.push(buf_results);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Batch Get
    // -----------------------------------------------------------------------

    pub async fn batch_get(&mut self, keys: &[String]) -> StoreResult<Vec<Option<Vec<u8>>>> {
        tracing::info!(target: "te_debug", key_count = keys.len(), "batch_get: ENTER");
        let mut results = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            tracing::info!(target: "te_debug", index = i, total = keys.len(), %key, "batch_get: processing key");
            match self.get(key).await {
                Ok(data) => {
                    tracing::info!(target: "te_debug", index = i, %key, data_len = data.len(), "batch_get: key OK");
                    results.push(Some(data));
                }
                Err(e) => {
                    tracing::warn!(target: "te_debug", index = i, %key, error = %e, "batch_get: key FAILED");
                    results.push(None);
                }
            }
        }
        let ok_count = results.iter().filter(|r| r.is_some()).count();
        tracing::info!(target: "te_debug", total = keys.len(), ok = ok_count, "batch_get: EXIT");
        Ok(results)
    }

    pub async unsafe fn batch_get_into(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
    ) -> StoreResult<Vec<i64>> {
        let mut results = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self.get_into(key, buffers[i], sizes[i]).await {
                Ok(n) => results.push(n as i64),
                Err(_) => results.push(-1),
            }
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Batch get into multi buffers
    // -----------------------------------------------------------------------

    pub async unsafe fn batch_get_into_multi_buffers(
        &mut self,
        keys: &[String],
        all_buffers: &[Vec<*mut c_void>],
        all_sizes: &[Vec<usize>],
        _prefer_same_node: bool,
    ) -> StoreResult<Vec<Vec<i64>>> {
        let mut results = vec![];
        for (key_idx, key) in keys.iter().enumerate() {
            let replicas = self.fetch_replicas(key).await?;
            let replica = match self.select_best_replica(&replicas) {
                Some(r) => r,
                None => {
                    results.push(vec![-1; all_buffers[key_idx].len()]);
                    continue;
                }
            };
            let seg = self.engine.open_segment(&replica.segment_name)?;
            let count = all_buffers[key_idx].len();
            let batch_id = match self.engine.allocate_batch_id(count) {
                Ok(id) => id,
                Err(e) => {
                    let _ = self.engine.close_segment(seg);
                    return Err(e.into());
                }
            };

            let reqs: Vec<TransferRequest> = (0..count)
                .map(|i| TransferRequest {
                    opcode: Opcode::Read,
                    source: all_buffers[key_idx][i],
                    target_id: seg,
                    target_offset: replica.base_addr + replica.offset,
                    length: all_sizes[key_idx][i] as u64,
                })
                .collect();

            self.engine.submit_transfer(batch_id, &reqs)?;

            let mut key_results: Vec<i64> = vec![0; count];
            let start = tokio::time::Instant::now();
            let timeout = tokio::time::Duration::from_secs(10);
            for i in 0..count {
                loop {
                    let status = self.engine.get_transfer_status(batch_id, i)?;
                    if status.status == TransferStatusEnum::Completed {
                        key_results[i] = status.transferred_bytes as i64;
                        break;
                    }
                    if status.status == TransferStatusEnum::Failed {
                        key_results[i] = -1;
                        break;
                    }
                    if start.elapsed() > timeout {
                        key_results[i] = -1;
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                }
            }
            self.engine.free_batch_id(batch_id)?;
            self.engine.close_segment(seg)?;
            results.push(key_results);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Buffer-based get (returns owned BufferHandle)
    // -----------------------------------------------------------------------

    pub async fn get_buffer(&mut self, key: &str) -> StoreResult<BufferHandle> {
        let data = self.get(key).await?;
        let size = data.len();
        Ok(BufferHandle {
            key: key.to_string(),
            size,
            data,
        })
    }

    pub async fn batch_get_buffer(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<Option<BufferHandle>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_buffer(key).await {
                Ok(bh) => results.push(Some(bh)),
                Err(_) => results.push(None),
            }
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // get_size
    // -----------------------------------------------------------------------

    /// Prefetch a list of keys from the remote source.
    ///
    /// Keys are fetched in parallel and stored in the hot cache (if attached).
    /// Use this to warm the cache before a training batch. Combine with
    /// [`put`](Self::put) to also store data in the Mooncake distributed store.
    pub async fn prefetch(&mut self, keys: &[String]) -> StoreResult<()> {
        let Some(ref handler) = self.miss_handler else {
            return Err(StoreError::Internal(
                "no remote source configured for prefetch".to_string(),
            ));
        };
        if !handler.is_enabled() {
            return Err(StoreError::Internal(
                "remote source is not enabled".to_string(),
            ));
        }
        tracing::info!(count = keys.len(), "starting prefetch");
        handler.batch_fetch(keys).await;
        Ok(())
    }

    pub async fn get_size(&mut self, key: &str) -> StoreResult<i64> {
        let replicas = self.fetch_replicas(key).await?;
        if replicas.is_empty() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }
        Ok(replicas[0].size as i64)
    }
}

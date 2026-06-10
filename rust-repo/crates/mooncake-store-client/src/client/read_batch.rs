use super::MooncakeClient;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Batch Get — batched metadata query with per-key error tolerance
    // 批量获取 —— 批量查询元数据，每个 key 独立容错
    //
    // Hot-cache hits are served locally. Remaining keys are queried through
    // BatchGetReplicaList, then read independently; failures are recorded as
    // `None` rather than aborting the entire batch.
    //
    // hot-cache 命中直接本地返回；剩余 key 通过 BatchGetReplicaList 批量查询，
    // 再逐 key 读取；失败记录为 None 而非中止整个批次。
    // C++ equivalent: Client::BatchGet()
    // -----------------------------------------------------------------------

    /// Batch fetch multiple keys. Each key is fetched independently; a failure
    /// for one key does **not** abort the batch — the corresponding slot is
    /// `None` while successful slots contain `Some(data)`.
    ///
    /// 批量获取多个 key。每个 key 独立获取；某个 key 的失败不会中止整个批次 ——
    /// 对应位置为 None，成功的位置包含 Some(data)。
    ///
    pub async fn batch_get(&mut self, keys: &[String]) -> StoreResult<Vec<Option<Vec<u8>>>> {
        tracing::info!(target: "te_debug", key_count = keys.len(), "batch_get: ENTER");
        let mut results = vec![None; keys.len()];
        let mut pending = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(ref cache) = self.hot_cache {
                if let Some(data) = cache.get(key) {
                    tracing::info!(target: "te_debug", index = i, %key, data_len = data.len(), "batch_get: HIT hot cache");
                    results[i] = Some(data);
                    continue;
                }
            }
            pending.push((i, key.clone()));
        }

        let pending_keys = pending
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let replica_results = if pending_keys.is_empty() {
            Vec::new()
        } else {
            match self.fetch_batch_replicas(&pending_keys).await {
                Ok(results) => results,
                Err(err) => pending_keys
                    .iter()
                    .map(|_| Err(StoreError::Internal(err.to_string())))
                    .collect(),
            }
        };

        for ((i, key), replica_result) in pending.into_iter().zip(replica_results.into_iter()) {
            tracing::info!(target: "te_debug", index = i, total = keys.len(), %key, "batch_get: processing key");
            let data_result = match replica_result {
                Ok(replicas) => match self.select_best_replica(&replicas) {
                    Some(replica) => self.read_from_replica(&key, replica).await,
                    None => Err(StoreError::KeyNotFound(key.clone())),
                },
                Err(err) => Err(err),
            };

            match data_result {
                Ok(data) => {
                    tracing::info!(target: "te_debug", index = i, %key, data_len = data.len(), "batch_get: key OK");
                    if let Some(ref cache) = self.hot_cache {
                        cache.put(&key, &data);
                    }
                    results[i] = Some(data);
                }
                Err(e) => {
                    tracing::warn!(target: "te_debug", index = i, %key, error = %e, "batch_get: key FAILED");
                    if let Some(ref handler) = self.miss_handler {
                        if handler.is_enabled() {
                            if let Ok(data) = handler.handle_miss(&key).await {
                                if let Some(ref cache) = self.hot_cache {
                                    cache.put(&key, &data);
                                }
                                results[i] = Some(data);
                            }
                        }
                    }
                }
            }
        }
        let ok_count = results.iter().filter(|r| r.is_some()).count();
        tracing::info!(target: "te_debug", total = keys.len(), ok = ok_count, "batch_get: EXIT");
        Ok(results)
    }

    /// Zero-copy batch get into pre-registered buffers.
    /// Each key is read directly into `buffers[i]` via RDMA, bypassing the
    /// internal local_buffer. Failures produce `-1` for that key.
    ///
    /// 零拷贝批量获取到预注册的缓冲区中。
    /// 每个 key 通过 RDMA 直接读入 buffers[i]，绕过内部 local_buffer。
    /// 失败时对应的 key 结果为 -1。
    ///
    /// # Safety
    /// All `buffers[i]` must be pre-registered with the TE and be at least
    /// `sizes[i]` bytes. / 所有 buffers[i] 必须预先向 TE 注册，且至少 sizes[i] 字节。
    pub async unsafe fn batch_get_into(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
    ) -> StoreResult<Vec<i64>> {
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            return Err(StoreError::InvalidParams(
                "keys, buffers, and sizes length mismatch".to_string(),
            ));
        }
        for (i, (&buffer, &size)) in buffers.iter().zip(sizes.iter()).enumerate() {
            self.resolve_writable_buffer_region(buffer, size)
                .map_err(|err| {
                    StoreError::InvalidParams(format!(
                        "invalid writable buffer for key index {i}: {err}"
                    ))
                })?;
        }
        let mut results = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self.get_into(key, buffers[i], sizes[i]).await {
                Ok(n) => results.push(n as i64),
                Err(_) => results.push(-1), // per-key error tolerance / 按 key 容错
            }
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Batch get into multi buffers — one key → multiple buffers/offsets
    // 批量获取到多缓冲区 —— 一个 key → 多个缓冲区/偏移量
    // -----------------------------------------------------------------------

    /// For each key, issue multiple RDMA reads into a set of pre-registered
    /// buffers. This is the most general form: one key can be scattered into
    /// N destination buffers at N different sizes.
    ///
    /// 对于每个 key，将多次 RDMA 读分发到一组预注册的缓冲区中。
    /// 这是最通用的形式：一个 key 可以分散到 N 个目标缓冲区，各有不同的大小。
    ///
    /// # Safety
    /// All buffers in `all_buffers` must be pre-registered with the TE.
    /// all_buffers 中的所有缓冲区必须预先向 TE 注册。
    pub async unsafe fn batch_get_into_multi_buffers(
        &mut self,
        keys: &[String],
        all_buffers: &[Vec<*mut c_void>],
        all_sizes: &[Vec<usize>],
        prefer_same_node: bool,
    ) -> StoreResult<Vec<i64>> {
        if keys.len() != all_buffers.len() || keys.len() != all_sizes.len() {
            return Err(StoreError::InvalidParams(
                "keys, all_buffers, and all_sizes length mismatch".to_string(),
            ));
        }
        for (idx, (buffers, sizes)) in all_buffers.iter().zip(all_sizes.iter()).enumerate() {
            if buffers.len() != sizes.len() {
                return Err(StoreError::InvalidParams(format!(
                    "buffers and sizes length mismatch for key index {idx}"
                )));
            }
            for (buffer_idx, (&buffer, &size)) in buffers.iter().zip(sizes.iter()).enumerate() {
                self.resolve_writable_buffer_region(buffer, size)
                    .map_err(|err| {
                        StoreError::InvalidParams(format!(
                            "invalid writable buffer for key index {idx}, buffer index {buffer_idx}: {err}"
                        ))
                    })?;
            }
        }

        let mut results = vec![];
        for (key_idx, key) in keys.iter().enumerate() {
            // Fetch and select replica. / 获取并选择副本。
            let replicas = match self.fetch_replicas(key).await {
                Ok(replicas) => replicas,
                Err(_) => {
                    results.push(-1);
                    continue;
                }
            };
            let local_replica = || {
                let endpoints = self.local_endpoints.read();
                replicas.iter().find(|r| {
                    r.status == mooncake_store_core::ReplicaStatus::Complete
                        && endpoints.contains(&r.segment_name)
                })
            };
            let replica = match prefer_same_node
                .then(local_replica)
                .flatten()
                .or_else(|| self.select_best_replica(&replicas))
            {
                Some(r) => r,
                None => {
                    results.push(-1);
                    continue;
                }
            };
            let total_capacity = all_sizes[key_idx].iter().sum::<usize>();
            if total_capacity < replica.size as usize {
                results.push(-1);
                continue;
            }

            // Open segment and allocate batch. / 打开 segment 并分配批次。
            let seg = self.engine.open_segment(&replica.segment_name)?;
            let count = all_buffers[key_idx].len();
            let request_count = all_sizes[key_idx]
                .iter()
                .scan(replica.size as usize, |remaining, &size| {
                    if *remaining == 0 {
                        Some(false)
                    } else {
                        *remaining = remaining.saturating_sub(size);
                        Some(true)
                    }
                })
                .filter(|included| *included)
                .count();
            if request_count == 0 {
                let _ = self.engine.close_segment(seg);
                results.push(0);
                continue;
            }
            let batch_id = match self.engine.allocate_batch_id(request_count) {
                Ok(id) => id,
                Err(e) => {
                    let _ = self.engine.close_segment(seg);
                    return Err(e.into());
                }
            };

            let mut remaining = replica.size as usize;
            let mut source_offset = 0usize;
            let mut request_to_buffer = Vec::with_capacity(request_count);
            let mut reqs = Vec::with_capacity(request_count);
            for i in 0..count {
                if remaining == 0 {
                    break;
                }
                let read_len = remaining.min(all_sizes[key_idx][i]);
                remaining -= read_len;
                reqs.push(TransferRequest {
                    opcode: Opcode::Read,
                    source: all_buffers[key_idx][i],
                    target_id: seg,
                    target_offset: replica.base_addr + replica.offset + source_offset as u64,
                    length: read_len as u64,
                });
                request_to_buffer.push(i);
                source_offset += read_len;
            }

            if let Err(e) = self.engine.submit_transfer(batch_id, &reqs) {
                let _ = self.engine.free_batch_id(batch_id);
                let _ = self.engine.close_segment(seg);
                return Err(e.into());
            }

            let statuses = match self
                .wait_for_transfer_batch_terminal(
                    batch_id,
                    request_to_buffer.len(),
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
            let failed = statuses
                .iter()
                .any(|status| status.status != TransferStatusEnum::Completed);
            let total_transferred = statuses
                .iter()
                .filter(|status| status.status == TransferStatusEnum::Completed)
                .map(|status| status.transferred_bytes as i64)
                .sum();
            self.engine.free_batch_id(batch_id)?;
            self.engine.close_segment(seg)?;
            results.push(if failed { -1 } else { total_transferred });
        }
        Ok(results)
    }
}

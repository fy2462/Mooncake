use super::{MooncakeClient, read::scoped_cache_key};
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use std::ffi::c_void;
use transfer_engine_ffi::{RegisteredSubmitOutcome, RegisteredTransferRequest, TransferStatusEnum};

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
        let started_at = std::time::Instant::now();
        tracing::info!(target: "te_debug", key_count = keys.len(), "batch_get: ENTER");
        let tenant_id = self.tenant_id.clone();
        let mut results = vec![None; keys.len()];
        let mut lease_deadlines = vec![None; keys.len()];
        let mut cache_replicas = vec![None; keys.len()];
        let mut pending = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            let cache_key = scoped_cache_key(&tenant_id, key);
            if let Some(ref cache) = self.hot_cache {
                if let Some(data) = cache.get(cache_key.as_ref()) {
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
        let query_results = if pending_keys.is_empty() {
            Vec::new()
        } else {
            match self.fetch_batch_query_responses(&pending_keys).await {
                Ok(results) => results,
                Err(err) => pending_keys
                    .iter()
                    .map(|_| super::CachedQueryResultResponse::failure(-2, err.to_string()))
                    .collect(),
            }
        };

        for ((i, key), query_result) in pending.into_iter().zip(query_results.into_iter()) {
            tracing::info!(target: "te_debug", index = i, total = keys.len(), %key, "batch_get: processing key");
            let data_result = if query_result.success {
                let lease_deadline = query_result.lease_valid_until;
                match self.select_best_replica(&query_result.replicas) {
                    Some(replica) => {
                        let replica = replica.clone();
                        let result = self.read_from_replica(&key, &replica).await;
                        if result.is_ok() {
                            lease_deadlines[i] = Some(lease_deadline);
                            cache_replicas[i] = Some(replica);
                        }
                        result
                    }
                    None => Err(StoreError::KeyNotFound(key.clone())),
                }
            } else {
                match query_result.error_status {
                    -1 => Err(StoreError::KeyNotFound(key.clone())),
                    -5 => Err(StoreError::ReplicaNotReady),
                    _ => Err(StoreError::Internal(query_result.error_message)),
                }
            };

            match data_result {
                Ok(data) => {
                    tracing::info!(target: "te_debug", index = i, %key, data_len = data.len(), "batch_get: key OK");
                    results[i] = Some(data);
                }
                Err(e) => {
                    tracing::warn!(target: "te_debug", index = i, %key, error = %e, "batch_get: key FAILED");
                    if let Some(ref handler) = self.miss_handler {
                        if handler.is_enabled() {
                            if let Ok(data) = handler.handle_miss(&key).await {
                                let cache_key = scoped_cache_key(&tenant_id, &key);
                                self.cache_value_if_admitted(cache_key.as_ref(), &data);
                                results[i] = Some(data);
                            }
                        }
                    }
                }
            }
        }
        // Match C++ BatchGet: all successful transfers are checked against
        // their read leases at one shared time after the batch has finished.
        let lease_check_time = std::time::Instant::now();
        for (result, deadline) in results.iter_mut().zip(&lease_deadlines) {
            if result.is_some() && deadline.is_some_and(|deadline| lease_check_time >= deadline) {
                *result = None;
            }
        }
        // Unlike C++, Rust's hot cache is checked before a new master query.
        // Admit only transfers that survived the lease check so an expired
        // transfer cannot become a later lease-bypassing cache hit.
        for (((key, result), replica), deadline) in keys
            .iter()
            .zip(&results)
            .zip(&cache_replicas)
            .zip(&lease_deadlines)
        {
            if let (Some(data), Some(replica), Some(deadline)) = (result, replica, deadline) {
                let cache_key = scoped_cache_key(&tenant_id, key);
                self.cache_replica_value_if_admitted(cache_key.as_ref(), data, replica, *deadline);
            }
        }
        let ok_count = results.iter().filter(|r| r.is_some()).count();
        tracing::info!(target: "te_debug", total = keys.len(), ok = ok_count, "batch_get: EXIT");
        if let Some(metrics) = &self.metrics {
            let bytes = results
                .iter()
                .filter_map(Option::as_ref)
                .try_fold(0_u64, |total, data| total.checked_add(data.len() as u64))
                .unwrap_or(u64::MAX);
            metrics.observe_batch_get(bytes, started_at.elapsed());
        }
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
    /// Every address/range is resolved to a live owner-bearing writable
    /// registration before it can be used.
    pub async fn batch_get_into(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
    ) -> StoreResult<Vec<i64>> {
        let started_at = std::time::Instant::now();
        let results = self
            .batch_get_into_results(keys, buffers, sizes)
            .await?
            .into_iter()
            .map(|result| result.map(|bytes| bytes as i64).unwrap_or(-1))
            .collect::<Vec<_>>();
        if let Some(metrics) = &self.metrics {
            let bytes = results
                .iter()
                .filter_map(|result| u64::try_from(*result).ok())
                .fold(0_u64, u64::saturating_add);
            metrics.observe_operation(
                super::metrics::TransferOperationKind::Read,
                "batch_get_into",
                bytes,
                started_at.elapsed(),
            );
        }
        Ok(results)
    }

    /// Batch zero-copy read preserving each key's structured error.
    ///
    /// Compatibility layers should use this method when they must translate
    /// per-key failures into a stable external error-code namespace. The
    /// legacy [`batch_get_into`](Self::batch_get_into) method intentionally
    /// retains its historical `-1` failure collapse.
    ///
    pub async fn batch_get_into_results(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
    ) -> StoreResult<Vec<StoreResult<usize>>> {
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
        let query_results = match self.fetch_batch_query_responses(keys).await {
            Ok(results) => results,
            Err(error) => {
                let message = error.to_string();
                return Ok(keys
                    .iter()
                    .map(|_| Err(StoreError::Internal(message.clone())))
                    .collect());
            }
        };
        let tenant_id = self.tenant_id.clone();
        let mut results = Vec::with_capacity(keys.len());
        let mut lease_deadlines = Vec::with_capacity(keys.len());
        for (i, (key, query_result)) in keys.iter().zip(query_results).enumerate() {
            if query_result.success {
                lease_deadlines.push(Some(query_result.lease_valid_until));
                results.push(
                    self.get_into_from_replicas(
                        key,
                        buffers[i],
                        sizes[i],
                        &query_result.replicas,
                        &tenant_id,
                    )
                    .await,
                );
            } else {
                lease_deadlines.push(None);
                results.push(Err(match query_result.error_status {
                    -1 => StoreError::KeyNotFound(key.clone()),
                    -5 => StoreError::ReplicaNotReady,
                    _ => StoreError::Internal(query_result.error_message),
                }));
            }
        }
        // Match C++ BatchGet: use one post-transfer timestamp for the entire
        // batch instead of refreshing or checking leases per key.
        let lease_check_time = std::time::Instant::now();
        for ((result, deadline), key) in results.iter_mut().zip(lease_deadlines).zip(keys) {
            if result.is_ok() && deadline.is_some_and(|deadline| lease_check_time >= deadline) {
                *result = Err(StoreError::LeaseExpired(key.clone()));
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
    /// All ranges are checked against live owner-bearing registrations before
    /// any transfer is submitted.
    pub async fn batch_get_into_multi_buffers(
        &mut self,
        keys: &[String],
        all_buffers: &[Vec<*mut c_void>],
        all_sizes: &[Vec<usize>],
        prefer_same_node: bool,
    ) -> StoreResult<Vec<i64>> {
        let started_at = std::time::Instant::now();
        if keys.len() != all_buffers.len() || keys.len() != all_sizes.len() {
            return Err(StoreError::InvalidParams(
                "keys, all_buffers, and all_sizes length mismatch".to_string(),
            ));
        }
        let all_regions = all_buffers
            .iter()
            .zip(all_sizes.iter())
            .enumerate()
            .map(|(idx, (buffers, sizes))| {
                if buffers.len() != sizes.len() {
                    return Err(StoreError::InvalidParams(format!(
                        "buffers and sizes length mismatch for key index {idx}"
                    )));
                }
                buffers
                    .iter()
                    .zip(sizes.iter())
                    .enumerate()
                    .map(|(buffer_idx, (&buffer, &size))| {
                        self.resolve_writable_buffer_region(buffer, size)
                            .map_err(|err| {
                                StoreError::InvalidParams(format!(
                                    "invalid writable buffer for key index {idx}, buffer index {buffer_idx}: {err}"
                                ))
                            })
                    })
                    .collect::<StoreResult<Vec<_>>>()
            })
            .collect::<StoreResult<Vec<_>>>()?;

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
                        && super::transfer_local::contains_same_process_endpoint(
                            &endpoints,
                            &r.segment_name,
                        )
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
            let object_size = match usize::try_from(replica.size) {
                Ok(size) => size,
                Err(_) => {
                    results.push(-1);
                    continue;
                }
            };
            let total_capacity = match all_sizes[key_idx]
                .iter()
                .try_fold(0usize, |total, size| total.checked_add(*size))
            {
                Some(capacity) => capacity,
                None => {
                    results.push(-1);
                    continue;
                }
            };
            if total_capacity < object_size {
                results.push(-1);
                continue;
            }

            if replica.replica_type == mooncake_store_core::ReplicaType::Disk {
                let data = match self.read_from_replica(key, replica).await {
                    Ok(data) => data,
                    Err(_) => {
                        results.push(-1);
                        continue;
                    }
                };
                let mut source_offset = 0usize;
                let mut failed = false;
                for (region, capacity) in all_regions[key_idx].iter().zip(all_sizes[key_idx].iter())
                {
                    if source_offset == data.len() {
                        break;
                    }
                    let end = source_offset.saturating_add(*capacity).min(data.len());
                    if self
                        .accelerator
                        .copy_from_host(region.foreign_region(), &data[source_offset..end])
                        .is_err()
                    {
                        failed = true;
                        break;
                    }
                    source_offset = end;
                }
                results.push(if failed {
                    -1
                } else {
                    i64::try_from(data.len()).unwrap_or(-1)
                });
                continue;
            }

            // Open segment and allocate batch. / 打开 segment 并分配批次。
            let seg = self.engine.open_segment(&replica.segment_name)?;
            let count = all_buffers[key_idx].len();
            let request_count = all_sizes[key_idx]
                .iter()
                .scan(object_size, |remaining, &size| {
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
            let mut remaining = object_size;
            let mut source_offset = 0usize;
            let mut request_lengths = Vec::with_capacity(request_count);
            let reqs = self.close_segment_on_prepare_error(
                seg,
                (|| -> StoreResult<_> {
                    let mut reqs = Vec::with_capacity(request_count);
                    for i in 0..count {
                        if remaining == 0 {
                            break;
                        }
                        let read_len = remaining.min(all_sizes[key_idx][i]);
                        remaining -= read_len;
                        reqs.push(RegisteredTransferRequest::read(
                            all_regions[key_idx][i].registered_region(),
                            seg,
                            Self::checked_replica_target_offset(replica, source_offset)?,
                            read_len,
                        )?);
                        request_lengths.push(read_len as u64);
                        source_offset += read_len;
                    }
                    Ok(reqs)
                })(),
            )?;

            let payload_regions = all_regions[key_idx].clone();
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
                            payload_regions,
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
                    payload_regions,
                )
                .await?;
            let failed = !super::transfer::transfer_statuses_match_lengths(
                &completion.statuses,
                request_lengths,
            );
            let total_transferred = completion
                .statuses
                .iter()
                .filter(|status| status.status == TransferStatusEnum::Completed)
                .map(|status| status.transferred_bytes as i64)
                .sum();
            drop(completion);
            if !failed && let Some(metrics) = &self.metrics {
                metrics.observe_transfer_bytes(
                    super::metrics::TransferOperationKind::Read,
                    u64::try_from(total_transferred).unwrap_or(u64::MAX),
                );
            }
            results.push(if failed { -1 } else { total_transferred });
        }
        if let Some(metrics) = &self.metrics {
            let bytes = results
                .iter()
                .filter_map(|result| u64::try_from(*result).ok())
                .fold(0_u64, u64::saturating_add);
            metrics.observe_operation(
                super::metrics::TransferOperationKind::Read,
                "batch_get_into_multi_buffers",
                bytes,
                started_at.elapsed(),
            );
        }
        Ok(results)
    }
}

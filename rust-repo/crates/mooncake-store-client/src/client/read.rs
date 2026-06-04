// ============================================================================
// Read operations: get, batch_get, zero-copy reads, prefetch, get_size
// 读操作：get、batch_get、零拷贝读、prefetch、get_size
//
// C++ equivalent: real_client.cpp Get() / BatchGet() / GetInto()
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};

use super::{BufferHandle, MooncakeClient};

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Get — three-level cache lookup
    // Get —— 三级缓存查找
    //
    // Level 0: hot_cache (local memory, no network)
    // Level 1: gRPC fetch replicas → select best → RDMA/TCP read
    // Level 2: remote source fallback (S3 / local FS), only if miss_handler enabled
    //
    // C++ equivalent: Client::Get() → GetReplicaList → SelectBestReplica → read
    // -----------------------------------------------------------------------

    /// Fetch the value for a given key from the distributed store.
    ///
    /// # Three-level cache hierarchy (三级缓存层次)
    ///
    /// | Level | Source        | Network | Latency     |
    /// |-------|---------------|---------|-------------|
    /// | 0     | `hot_cache`   | None    | ~ns         |
    /// | 1     | gRPC + TE     | Yes     | ~us (RDMA)  |
    /// | 2     | Remote source | Yes     | ~ms (S3/FS) |
    ///
    /// 1. **hot_cache (L0)** — check local in-memory cache first. Fastest path,
    ///    avoids all network I/O.
    ///    首先检查本地内存缓存。最快路径，避免所有网络 I/O。
    ///
    /// 2. **Distributed store (L1)** — call `fetch_replicas` (gRPC to master),
    ///    then `select_best_replica` for locality-aware selection, then
    ///    `read_from_replica` to RDMA/TCP the data into local memory.
    ///    On success, the result is stored in hot_cache for future hits.
    ///
    ///    调用 fetch_replicas（gRPC 到 master），然后 select_best_replica
    ///    进行本地性感知选择，再通过 read_from_replica 使用 RDMA/TCP 将数据
    ///    读入本地内存。成功后结果存入 hot_cache 以供将来命中。
    ///
    /// 3. **Remote source (L2)** — if no replica is found, and a `miss_handler`
    ///    is configured and enabled, fetch from the remote source (S3, local FS,
    ///    etc.). The result is also stored in hot_cache.
    ///
    ///    如果没有找到副本，且配置并启用了 miss_handler，则从远程数据源获取
    ///    （S3、本地文件系统等）。结果也会存入 hot_cache。
    ///
    /// # Returns (返回值)
    ///
    /// - `Ok(Vec<u8>)` — the value bytes. / 值字节。
    /// - `Err(KeyNotFound)` — key not in store and no remote source available.
    ///   key 不在存储中且没有可用的远程数据源。
    pub async fn get(&mut self, key: &str) -> StoreResult<Vec<u8>> {
        tracing::info!(target: "te_debug", %key, "get: ENTER");

        // Level 0: check local hot cache (fastest — no network)
        // 第 0 级：检查本地热缓存（最快 —— 零网络开销）
        if let Some(ref cache) = self.hot_cache {
            if let Some(data) = cache.get(key) {
                tracing::info!(target: "te_debug", %key, data_len = data.len(), "get: HIT hot cache");
                return Ok(data);
            }
        }

        // Level 1: fetch from memory store (gRPC → RDMA)
        // 第 1 级：从内存存储获取（gRPC → RDMA）
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
                let data = self.read_from_replica(key, r).await?;
                tracing::info!(target: "te_debug", %key, data_len = data.len(), "get: read_from_replica success");
                // Store in hot cache for future hits / 存入 hot_cache 以供将来命中
                if let Some(ref cache) = self.hot_cache {
                    cache.put(key, &data);
                }
                Ok(data)
            }
            None => {
                tracing::info!(target: "te_debug", %key, "get: no replica found, trying remote source");
                // Level 2: remote source fallback (S3 / local FS)
                // 第 2 级：远程数据源回退（S3 / 本地文件系统）
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

    /// Zero-copy read: transfer data for `key` directly into a caller-provided
    /// buffer via RDMA/TCP, bypassing the internal `local_buffer`.
    ///
    /// The buffer must be pre-registered with the TE via [`register_buffer`].
    ///
    /// 零拷贝读取：通过 RDMA/TCP 将 key 的数据直接传输到调用者提供的缓冲区，
    /// 绕过内部 local_buffer。缓冲区必须预先通过 register_buffer 向 TE 注册。
    ///
    /// # Safety (安全性)
    ///
    /// - `buffer` must be valid for writes of at least `size` bytes.
    ///   buffer 必须可写入至少 size 字节。
    /// - The buffer must remain alive until the transfer completes.
    ///   传输完成前缓冲区必须保持存活。
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
                // Fetch replicas and select the best one.
                // 获取副本列表并选择最优副本。
                let replicas = self.fetch_replicas(key).await?;
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
                let valid_ranges: Vec<usize> = sizes[buf_idx][key_idx]
                    .iter()
                    .enumerate()
                    .filter_map(|(ri, &sz)| {
                        let src_end = src_offsets[buf_idx][key_idx][ri].checked_add(sz)?;
                        let _dst_end = dst_offsets[buf_idx][key_idx][ri].checked_add(sz)?;
                        (src_end as u64 <= replica.size).then_some(ri)
                    })
                    .collect();
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
                    .map(|&ri| {
                        let sz = sizes[buf_idx][key_idx][ri];
                        TransferRequest {
                            opcode: Opcode::Read,
                            source: buffers[buf_idx].byte_add(dst_offsets[buf_idx][key_idx][ri]),
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

                // Poll each range's status with a 10s timeout.
                // 以 10s 超时轮询每个范围的状态。
                let start = tokio::time::Instant::now();
                let timeout = tokio::time::Duration::from_secs(10);
                for (request_idx, &range_idx) in valid_ranges.iter().enumerate() {
                    loop {
                        let status = match self.engine.get_transfer_status(batch_id, request_idx) {
                            Ok(status) => status,
                            Err(e) => {
                                let _ = self.engine.free_batch_id(batch_id);
                                let _ = self.engine.close_segment(seg);
                                return Err(e.into());
                            }
                        };
                        if status.status == TransferStatusEnum::Completed {
                            range_results[range_idx] = status.transferred_bytes as i64;
                            break;
                        }
                        if status.status == TransferStatusEnum::Failed {
                            range_results[range_idx] = -1;
                            break;
                        }
                        if start.elapsed() > timeout {
                            range_results[range_idx] = -1;
                            break;
                        }
                        // 50us poll interval — tight enough for RDMA latency.
                        // 50us 轮询间隔 —— 足够紧凑以匹配 RDMA 延迟。
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
    // Batch Get — sequential key iteration with per-key error tolerance
    // 批量获取 —— 顺序遍历 key，每个 key 独立容错
    //
    // Each key is fetched independently via get(); failures are recorded as
    // `None` in the result vector rather than aborting the entire batch.
    // This matches the C++ behavior: errors are absorbed per-key.
    //
    // 每个 key 通过 get() 独立获取；失败记录为 None 而非中止整个批次。
    // 这与 C++ 行为一致：错误按 key 吸收。
    // C++ equivalent: Client::BatchGet()
    // -----------------------------------------------------------------------

    /// Batch fetch multiple keys. Each key is fetched independently; a failure
    /// for one key does **not** abort the batch — the corresponding slot is
    /// `None` while successful slots contain `Some(data)`.
    ///
    /// 批量获取多个 key。每个 key 独立获取；某个 key 的失败不会中止整个批次 ——
    /// 对应位置为 None，成功的位置包含 Some(data)。
    ///
    /// This is a convenience wrapper that calls [`get`](Self::get) for each key.
    /// For better performance with many small keys, consider using
    /// [`batch_get_into`](Self::batch_get_into) with pre-registered buffers.
    ///
    /// 这是为每个 key 调用 get() 的便捷封装。对于许多小 key 的场景，
    /// 考虑使用 batch_get_into 配合预注册的缓冲区以获得更好的性能。
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
                    // Per-key error tolerance: log and continue.
                    // 按 key 容错：记录错误并继续。
                    tracing::warn!(target: "te_debug", index = i, %key, error = %e, "batch_get: key FAILED");
                    results.push(None);
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
        _prefer_same_node: bool,
    ) -> StoreResult<Vec<Vec<i64>>> {
        let mut results = vec![];
        for (key_idx, key) in keys.iter().enumerate() {
            // Fetch and select replica. / 获取并选择副本。
            let replicas = self.fetch_replicas(key).await?;
            let replica = match self.select_best_replica(&replicas) {
                Some(r) => r,
                None => {
                    // All buffers for this key get -1. / 此 key 的所有缓冲区得到 -1。
                    results.push(vec![-1; all_buffers[key_idx].len()]);
                    continue;
                }
            };

            // Open segment and allocate batch. / 打开 segment 并分配批次。
            let seg = self.engine.open_segment(&replica.segment_name)?;
            let count = all_buffers[key_idx].len();
            let batch_id = match self.engine.allocate_batch_id(count) {
                Ok(id) => id,
                Err(e) => {
                    let _ = self.engine.close_segment(seg);
                    return Err(e.into());
                }
            };

            // All buffers for this key share the same source offset
            // (replica.base_addr + replica.offset) but read different sizes.
            // 此 key 的所有缓冲区共享相同的源偏移，但读取不同的大小。
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

            // Poll with 10s timeout. / 以 10s 超时轮询。
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
    // 基于 BufferHandle 的获取（返回拥有所有权的 BufferHandle）
    // -----------------------------------------------------------------------

    /// Fetch a key and return an owned [`BufferHandle`] containing the key name,
    /// size, and data. Convenience wrapper around [`get`](Self::get).
    ///
    /// 获取 key 并返回拥有所有权的 BufferHandle，包含 key 名称、大小和数据。
    /// 这是 get() 的便捷封装。
    pub async fn get_buffer(&mut self, key: &str) -> StoreResult<BufferHandle> {
        let data = self.get(key).await?;
        let size = data.len();
        Ok(BufferHandle {
            key: key.to_string(),
            size,
            data,
        })
    }

    /// Batch version of [`get_buffer`](Self::get_buffer). Per-key error tolerance:
    /// failed keys produce `None`.
    ///
    /// get_buffer 的批量版本。按 key 容错：失败的 key 产生 None。
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
    // Prefetch — warm the hot cache from remote source
    // 预取 —— 从远程数据源预热热缓存
    // -----------------------------------------------------------------------

    /// Prefetch a list of keys from the remote source.
    ///
    /// Keys are fetched in parallel and stored in the hot cache (if attached).
    /// Use this to warm the cache before a training batch. Combine with
    /// [`put`](Self::put) to also store data in the Mooncake distributed store.
    ///
    /// 从远程数据源预取多个 key。
    /// Key 被并行获取并存储到 hot cache 中（如果已挂载）。
    /// 用于在训练批次之前预热缓存。结合 put() 可以同时将数据存入 Mooncake 分布式存储。
    ///
    /// # Errors
    /// Returns `Internal` error if no remote source is configured or it is disabled.
    /// 如果没有配置远程数据源或已被禁用，返回 Internal 错误。
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

    /// Return the size (in bytes) of the first replica for `key`.
    /// Useful for checking object size before calling [`get_into`](Self::get_into)
    /// to allocate a properly-sized buffer.
    ///
    /// 返回 key 的第一个副本的大小（字节）。
    /// 在调用 get_into 之前用于检查对象大小以分配合适大小的缓冲区。
    ///
    /// C++ equivalent: Client::GetSize()
    pub async fn get_size(&mut self, key: &str) -> StoreResult<i64> {
        let replicas = self.fetch_replicas(key).await?;
        if replicas.is_empty() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }
        Ok(replicas[0].size as i64)
    }
}

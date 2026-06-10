// ============================================================================
// Read operations: get, batch_get, zero-copy reads, prefetch, get_size
// 读操作：get、batch_get、零拷贝读、prefetch、get_size
//
// C++ equivalent: real_client.cpp Get() / BatchGet() / GetInto()
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use std::borrow::Cow;
use std::ffi::c_void;

use super::MooncakeClient;

fn scoped_cache_key<'a>(tenant_id: &str, key: &'a str) -> Cow<'a, str> {
    if tenant_id.is_empty() {
        Cow::Borrowed(key)
    } else {
        Cow::Owned(format!("{tenant_id}\0{key}"))
    }
}

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
        self.get_for_tenant(key, "").await
    }

    pub(crate) async fn get_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
    ) -> StoreResult<Vec<u8>> {
        tracing::info!(target: "te_debug", %key, "get: ENTER");
        let cache_key = scoped_cache_key(tenant_id, key);

        // Level 0: check local hot cache (fastest — no network)
        // 第 0 级：检查本地热缓存（最快 —— 零网络开销）
        if let Some(ref cache) = self.hot_cache {
            if let Some(data) = cache.get(cache_key.as_ref()) {
                tracing::info!(target: "te_debug", %key, data_len = data.len(), "get: HIT hot cache");
                return Ok(data);
            }
        }

        // Level 1: fetch from memory store (gRPC → RDMA)
        // 第 1 级：从内存存储获取（gRPC → RDMA）
        tracing::info!(target: "te_debug", %key, "get: fetching replicas from master");
        let replicas = self.fetch_replicas_for_tenant(key, tenant_id).await?;
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
                let data = self.read_from_replica_for_tenant(key, tenant_id, r).await?;
                tracing::info!(target: "te_debug", %key, data_len = data.len(), "get: read_from_replica success");
                // Store in hot cache for future hits / 存入 hot_cache 以供将来命中
                if let Some(ref cache) = self.hot_cache {
                    cache.put(cache_key.as_ref(), &data);
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
                                    cache.put(cache_key.as_ref(), &data);
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
        let object_size = replica.size as usize;
        if size < object_size {
            return Err(StoreError::InvalidParams(format!(
                "buffer too small for key {key}: required={object_size}, available={size}"
            )));
        }
        self.resolve_writable_buffer_region(buffer, object_size)?;
        if replica.replica_type == mooncake_store_core::ReplicaType::LocalDisk
            && !self.local_endpoints.read().contains(&replica.segment_name)
        {
            let data = self.read_from_replica(key, replica).await?;
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), buffer as *mut u8, data.len());
            }
            return Ok(data.len());
        }
        self.zero_copy_read(replica, buffer, object_size).await
    }
}

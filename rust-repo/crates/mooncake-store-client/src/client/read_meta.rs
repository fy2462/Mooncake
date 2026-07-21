use super::{BufferHandle, MooncakeClient};
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;

impl MooncakeClient {
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

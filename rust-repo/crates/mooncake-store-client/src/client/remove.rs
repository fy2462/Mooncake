// ============================================================================
// Remove & Exist operations — key deletion and presence check
// 删除和存在性操作 —— key 删除和存在性检查
//
// All remove/exists operations are forwarded to the master via gRPC.
// The master coordinates the actual deletion across replicas.
//
// 所有 remove/exists 操作通过 gRPC 转发给 master。
// master 协调跨副本的实际删除操作。
//
// C++ equivalent: real_client.cpp Remove() / Exists() / BatchRemove() /
// BatchIsExist() / RemoveByRegex() / RemoveAll()
// ============================================================================

use mooncake_store_core::error::StoreResult;

use super::MooncakeClient;
use crate::local_storage_backend::AttachedLocalStorage;
use crate::proto;

fn cleanup_local_storage_after_remove_all(local_storage: Option<&AttachedLocalStorage>) {
    if let Some(storage) = local_storage {
        if let Err(error) = storage.remove_all() {
            tracing::warn!(
                %error,
                "remove_all succeeded on master but local offload storage cleanup failed"
            );
        }
    }
}

impl MooncakeClient {
    fn invalidate_hot_cache_regex_for_tenant(&self, pattern: &str, tenant_id: &str) {
        if let Some(ref cache) = self.hot_cache {
            if cache
                .remove_by_regex_for_tenant(tenant_id, pattern)
                .is_err()
            {
                tracing::warn!(
                    pattern = %pattern,
                    "remove_by_regex succeeded on master but local hot-cache regex parsing failed; clearing cache"
                );
                cache.clear();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Remove / Exist — single-key operations
    // 删除 / 存在性检查 —— 单 key 操作
    // -----------------------------------------------------------------------

    /// Remove a single key from the distributed store.
    ///
    /// 从分布式存储中删除单个 key。
    ///
    /// # Arguments
    /// - `key` — the key to remove. / 要删除的 key。
    /// - `force` — if `true`, force removal even if the key is pinned or locked.
    ///   如果为 true，即使 key 被固定或锁定时也强制删除。
    ///
    /// C++ equivalent: `Client::Remove(key, force)`
    pub async fn remove(&mut self, key: &str, force: bool) -> StoreResult<()> {
        let tenant_id = self.tenant_id.clone();
        let request = proto::RemoveRequest {
            key: key.to_string(),
            force,
            tenant_id: tenant_id.clone(),
        };
        self.master
            .remove(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?;
        self.invalidate_hot_cache_key_for_tenant(key, &tenant_id);
        Ok(())
    }

    /// Check whether a key exists in the distributed store.
    ///
    /// 检查 key 是否存在于分布式存储中。
    ///
    /// C++ equivalent: `Client::Exists(key)`
    pub async fn exists(&mut self, key: &str) -> StoreResult<bool> {
        let request = proto::ExistKeyRequest {
            key: key.to_string(),
            tenant_id: self.tenant_id.clone(),
        };
        let response = self
            .master
            .exist_key(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.exists)
    }

    // -----------------------------------------------------------------------
    // Batch Remove / Exist — multi-key operations
    // 批量删除 / 存在性检查 —— 多 key 操作
    // -----------------------------------------------------------------------

    /// Remove multiple keys in a single batch request.
    ///
    /// 在单次批量请求中删除多个 key。
    ///
    /// # Returns
    /// `Vec<i32>` with `0` = removed, non-zero = failure, aligned with `keys`.
    /// Vec<i32>，0 = 已删除，非零 = 失败，与 keys 对齐。
    ///
    /// C++ equivalent: `Client::BatchRemove(keys, force)`
    pub async fn batch_remove(&mut self, keys: &[String], force: bool) -> StoreResult<Vec<i32>> {
        let tenant_id = self.tenant_id.clone();
        let request = proto::BatchRemoveRequest {
            keys: keys.to_vec(),
            force,
            tenant_id: tenant_id.clone(),
        };
        let response = self
            .master
            .batch_remove(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if response.statuses.len() != keys.len() {
            return Err(StoreError::Internal(format!(
                "BatchRemove response size mismatch: expected {}, got {}",
                keys.len(),
                response.statuses.len()
            )));
        }
        for (key, status) in keys.iter().zip(response.statuses.iter()) {
            if *status == 0 {
                self.invalidate_hot_cache_key_for_tenant(key, &tenant_id);
            }
        }
        Ok(response.statuses)
    }

    /// Check existence for multiple keys in a single batch request.
    ///
    /// 在单次批量请求中检查多个 key 的存在性。
    ///
    /// # Returns
    /// `Vec<bool>` aligned with `keys`. / Vec<bool>，与 keys 对齐。
    ///
    /// C++ equivalent: `Client::BatchIsExist(keys)`
    pub async fn batch_is_exist(&mut self, keys: &[String]) -> StoreResult<Vec<bool>> {
        let request = proto::BatchExistKeyRequest {
            keys: keys.to_vec(),
            tenant_id: self.tenant_id.clone(),
        };
        let response = self
            .master
            .batch_exist_key(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if response.results.len() != keys.len() {
            return Err(StoreError::Internal(format!(
                "BatchExistKey response size mismatch: expected {}, got {}",
                keys.len(),
                response.results.len()
            )));
        }
        Ok(response.results)
    }

    // -----------------------------------------------------------------------
    // Remove by regex / Remove all — pattern-based bulk deletion
    // 正则删除 / 删除全部 —— 基于模式的批量删除
    // -----------------------------------------------------------------------

    /// Remove all keys matching a regex pattern.
    ///
    /// 删除所有匹配正则表达式的 key。
    ///
    /// # Arguments
    /// - `pattern` — regex pattern (e.g. `"prefix_.*"`). / 正则表达式模式。
    /// - `force` — force removal even for pinned/locked keys. / 强制删除固定/锁定的 key。
    ///
    /// # Returns
    /// The number of keys actually removed. / 实际删除的 key 数量。
    ///
    /// C++ equivalent: `Client::RemoveByRegex(pattern, force)`
    pub async fn remove_by_regex(&mut self, pattern: &str, force: bool) -> StoreResult<i64> {
        let tenant_id = self.tenant_id.clone();
        let request = proto::RemoveByRegexRequest {
            pattern: pattern.to_string(),
            force,
            tenant_id: tenant_id.clone(),
        };
        let response = self
            .master
            .remove_by_regex(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if response.removed_count > 0 {
            self.invalidate_hot_cache_regex_for_tenant(pattern, &tenant_id);
        }
        Ok(response.removed_count)
    }

    /// Remove all keys from the store using the master's RemoveAll RPC.
    ///
    /// 从存储中删除所有 key，直接调用 master 的 RemoveAll RPC。
    ///
    /// # Warning (警告)
    /// This is a destructive operation — use with caution.
    /// 这是一个破坏性操作 —— 请谨慎使用。
    ///
    /// C++ equivalent: `Client::RemoveAll(force)`
    pub async fn remove_all(&mut self, force: bool) -> StoreResult<i64> {
        let tenant_id = self.tenant_id.clone();
        let request = proto::RemoveAllRequest {
            force,
            tenant_id: tenant_id.clone(),
        };
        let response = self
            .master
            .remove_all(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if let Some(ref cache) = self.hot_cache {
            cache.clear();
        }
        cleanup_local_storage_after_remove_all(self.local_storage.as_ref());
        if let Some(storage) = self.global_disk.as_ref().cloned() {
            if let Err(error) = storage.remove_all_for_tenant(tenant_id).await {
                tracing::warn!(
                    %error,
                    "remove_all succeeded on master but global DISK tenant cleanup failed"
                );
            }
        }
        Ok(response.removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::cleanup_local_storage_after_remove_all;
    use crate::local_storage_backend::{
        AttachedLocalStorage, LocalStorageBackend, LocalStorageConfig,
    };
    use std::sync::Arc;

    #[test]
    fn remove_all_cleanup_clears_attached_local_storage() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = Arc::new(LocalStorageBackend::new(LocalStorageConfig {
            root_dir: temp_dir.path().to_path_buf(),
            fsdir: "offload".to_string(),
            enable_eviction: true,
            quota_bytes: 1024,
        }));
        storage.init().unwrap();
        storage.write_object("tenant:key", b"value").unwrap();
        assert!(storage.exists("tenant:key"));

        cleanup_local_storage_after_remove_all(Some(&AttachedLocalStorage::FilePerKey(
            Arc::clone(&storage),
        )));

        assert!(!storage.exists("tenant:key"));
        assert_eq!(storage.scan_meta().unwrap(), Vec::<(String, u64)>::new());
    }
}

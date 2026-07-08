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
use mooncake_store_core::StoreError;

use super::read::scoped_cache_key;
use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    fn invalidate_hot_cache_key_for_tenant(&self, key: &str, tenant_id: &str) {
        if let Some(ref cache) = self.hot_cache {
            cache.remove(scoped_cache_key(tenant_id, key).as_ref());
        }
    }

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
            .remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
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
            .exist_key(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
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
            .batch_remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
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
            .batch_exist_key(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
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
            .remove_by_regex(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
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
        let request = proto::RemoveAllRequest { force, tenant_id };
        let response = self
            .master
            .remove_all(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        if let Some(ref cache) = self.hot_cache {
            cache.clear();
        }
        Ok(response.removed_count)
    }
}

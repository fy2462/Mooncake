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

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
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
        let request = proto::RemoveRequest {
            key: key.to_string(),
            force,
        };
        self.master
            .remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
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
        let request = proto::BatchRemoveRequest {
            keys: keys.to_vec(),
            force,
        };
        let response = self
            .master
            .batch_remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
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
        let request = proto::RemoveByRegexRequest {
            pattern: pattern.to_string(),
            force,
        };
        let response = self
            .master
            .remove_by_regex(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.removed_count)
    }

    /// Remove all keys from the store. Convenience wrapper that calls
    /// [`remove_by_regex`](Self::remove_by_regex) with pattern `".*"` and
    /// `force = true`.
    ///
    /// 从存储中删除所有 key。便捷封装，以 ".*" 和 force=true
    /// 调用 remove_by_regex。
    ///
    /// # Warning (警告)
    /// This is a destructive operation — use with caution.
    /// 这是一个破坏性操作 —— 请谨慎使用。
    ///
    /// C++ equivalent: `Client::RemoveAll()`
    pub async fn remove_all(&mut self) -> StoreResult<i64> {
        self.remove_by_regex(".*", true).await
    }
}

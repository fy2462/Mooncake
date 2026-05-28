use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Remove / Exist
    // -----------------------------------------------------------------------

    pub async fn remove(&mut self, key: &str, force: bool) -> StoreResult<()> {
        let request = proto::RemoveRequest { key: key.to_string(), force };
        self.master
            .remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn exists(&mut self, key: &str) -> StoreResult<bool> {
        let request = proto::ExistKeyRequest { key: key.to_string() };
        let response = self
            .master
            .exist_key(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.exists)
    }

    // -----------------------------------------------------------------------
    // Batch Remove / Exist
    // -----------------------------------------------------------------------

    pub async fn batch_remove(
        &mut self,
        keys: &[String],
        force: bool,
    ) -> StoreResult<Vec<i32>> {
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

    pub async fn batch_is_exist(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<bool>> {
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
    // Remove by regex / Remove all
    // -----------------------------------------------------------------------

    pub async fn remove_by_regex(
        &mut self,
        pattern: &str,
        force: bool,
    ) -> StoreResult<i64> {
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

    pub async fn remove_all(&mut self) -> StoreResult<i64> {
        self.remove_by_regex(".*", true).await
    }
}

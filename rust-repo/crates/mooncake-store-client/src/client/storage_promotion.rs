use super::storage::PromotionTaskItem;
use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::collections::HashMap;

impl MooncakeClient {
    /// Heartbeat to poll for promotion tasks from the master.
    ///
    /// Returns a map of `key → size` for objects that should be promoted
    /// from disk back to memory.
    ///
    /// 向 master 发送心跳以轮询 promotion 任务。
    /// 返回 key → size 的映射，表示应从磁盘晋升回内存的对象。
    ///
    /// C++ equivalent: `Client::PromotionObjectHeartbeat()`
    pub async fn promotion_object_heartbeat(&mut self) -> StoreResult<HashMap<String, i64>> {
        let tasks = self.promotion_object_heartbeat_tasks().await?;
        Ok(tasks
            .into_iter()
            .map(|task| (task.key, task.size))
            .collect())
    }

    /// Heartbeat to poll tenant-scoped promotion tasks from the master.
    ///
    /// C++ equivalent: `Client::PromotionObjectHeartbeat()` returning
    /// `std::vector<PromotionTaskItem>`.
    pub async fn promotion_object_heartbeat_tasks(
        &mut self,
    ) -> StoreResult<Vec<PromotionTaskItem>> {
        let response = self
            .master
            .promotion_object_heartbeat(proto::PromotionObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        if !response.tasks.is_empty() {
            return Ok(response.tasks.into_iter().map(Into::into).collect());
        }
        Ok(response
            .objects
            .into_iter()
            .map(|(key, size)| PromotionTaskItem {
                tenant_id: String::new(),
                key,
                size,
            })
            .collect())
    }

    /// Allocate a memory replica for a promoted object.
    ///
    /// Called before writing the promoted data. The returned
    /// [`ReplicaDescriptor`] can be passed to
    /// [`write_to_replica`](Self::write_to_replica) to store the data.
    ///
    /// 为晋升对象分配内存副本。
    /// 在写入晋升数据之前调用。返回的 ReplicaDescriptor 可以传递给
    /// write_to_replica 来存储数据。
    ///
    /// C++ equivalent: `Client::PromotionAllocStart(key, size, preferred_segments)`
    pub async fn promotion_alloc_start(
        &mut self,
        key: &str,
        size: u64,
        preferred_segments: Vec<String>,
    ) -> StoreResult<ReplicaDescriptor> {
        let tenant_id = self.tenant_id.clone();
        self.promotion_alloc_start_for_tenant(key, &tenant_id, size, preferred_segments)
            .await
    }

    pub async fn promotion_alloc_start_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
        size: u64,
        preferred_segments: Vec<String>,
    ) -> StoreResult<ReplicaDescriptor> {
        let response = self
            .master
            .promotion_alloc_start(proto::PromotionAllocStartRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                size,
                preferred_segments,
                tenant_id: tenant_id.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let descriptor = response
            .memory_descriptor
            .as_ref()
            .ok_or(StoreError::OperationFailed(-1))?;
        Ok(self
            .replicas_from_proto(std::slice::from_ref(descriptor))
            .remove(0))
    }

    /// Notify the master that a promotion has completed successfully.
    /// The promoted data is now available in memory.
    ///
    /// 通知 master promotion 已成功完成。晋升数据现在可在内存中使用。
    ///
    /// C++ equivalent: `Client::NotifyPromotionSuccess(key)`
    pub async fn notify_promotion_success(&mut self, key: &str) -> StoreResult<()> {
        let tenant_id = self.tenant_id.clone();
        self.notify_promotion_success_for_tenant(key, &tenant_id)
            .await
    }

    pub async fn notify_promotion_success_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
    ) -> StoreResult<()> {
        self.master
            .notify_promotion_success(proto::NotifyPromotionSuccessRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                tenant_id: tenant_id.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Notify the master that a promotion has failed.
    /// The master can retry or assign the task to a different node.
    ///
    /// 通知 master promotion 失败。Master 可以重试或将任务分配给其他节点。
    ///
    /// C++ equivalent: `Client::NotifyPromotionFailure(key)`
    pub async fn notify_promotion_failure(&mut self, key: &str) -> StoreResult<()> {
        let tenant_id = self.tenant_id.clone();
        self.notify_promotion_failure_for_tenant(key, &tenant_id)
            .await
    }

    pub async fn notify_promotion_failure_for_tenant(
        &mut self,
        key: &str,
        tenant_id: &str,
    ) -> StoreResult<()> {
        self.master
            .notify_promotion_failure(proto::NotifyPromotionFailureRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                tenant_id: tenant_id.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // High-level offload / promotion — full cycle with local disk I/O
    // 高级 offload / promotion —— 含本地磁盘 I/O 的完整循环
    // -----------------------------------------------------------------------
}

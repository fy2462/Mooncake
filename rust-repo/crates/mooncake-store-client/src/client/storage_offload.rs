use super::MooncakeClient;
use super::storage::OffloadTaskItem;
use crate::proto;
use mooncake_store_core::error::{StoreError, StoreResult};
use std::collections::HashMap;

impl MooncakeClient {
    /// Heartbeat to poll for offloading tasks from the master.
    ///
    /// Returns a map of `key → size` for objects that should be offloaded
    /// from memory to local disk.
    ///
    /// 向 master 发送心跳以轮询 offloading 任务。
    /// 返回 key → size 的映射，表示应从内存卸载到本地磁盘的对象。
    ///
    /// C++ equivalent: `Client::OffloadObjectHeartbeat(enable_offloading)`
    pub async fn offload_object_heartbeat(
        &mut self,
        enable_offloading: bool,
    ) -> StoreResult<HashMap<String, i64>> {
        let tasks = self
            .offload_object_heartbeat_tasks(enable_offloading)
            .await?;
        let mut objects = HashMap::with_capacity(tasks.len());
        for task in tasks {
            if objects.insert(task.key.clone(), task.size).is_some() {
                return Err(StoreError::InvalidParams(format!(
                    "key-only offload API cannot represent duplicate tenant-scoped key {:?}; \
                     use offload_object_heartbeat_tasks",
                    task.key
                )));
            }
            self.pending_legacy_offload_tasks
                .insert(task.key.clone(), task);
        }
        Ok(objects)
    }

    /// Heartbeat to poll tenant-scoped offloading tasks from the master.
    ///
    /// C++ equivalent: `Client::OffloadObjectHeartbeat(enable_offloading)`
    /// returning `std::vector<OffloadTaskItem>`.
    pub async fn offload_object_heartbeat_tasks(
        &mut self,
        enable_offloading: bool,
    ) -> StoreResult<Vec<OffloadTaskItem>> {
        let response = self
            .master
            .offload_object_heartbeat(self.rpc_request(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if !response.tasks.is_empty() {
            let tasks = response
                .tasks
                .into_iter()
                .map(OffloadTaskItem::from)
                .collect::<Vec<_>>();
            if let Some(task) = tasks
                .iter()
                .find(|task| task.generation_id.is_nil() || task.size < 0)
            {
                return Err(StoreError::Internal(format!(
                    "master returned invalid offload task for key {:?}",
                    task.key
                )));
            }
            return Ok(tasks);
        }
        if !response.objects.is_empty() {
            return Err(StoreError::Internal(
                "master returned legacy offload objects without generation-bearing tasks"
                    .to_string(),
            ));
        }
        Ok(Vec::new())
    }

    /// Report the local SSD capacity to the master.
    ///
    /// The master uses this information for storage-aware replica placement
    /// decisions.
    ///
    /// 向 master 报告本地 SSD 容量。
    /// Master 使用此信息进行存储感知的副本放置决策。
    ///
    /// C++ equivalent: `Client::ReportSsdCapacity(bytes)`
    pub async fn report_ssd_capacity(&mut self, bytes: i64) -> StoreResult<()> {
        self.master
            .report_ssd_capacity(self.rpc_request(proto::ReportSsdCapacityRequest {
                client_id: Some(self.client_id_proto()),
                ssd_total_capacity_bytes: bytes,
            }))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    /// Notify the master that a set of objects has been successfully offloaded
    /// to local disk, along with their storage metadata.
    ///
    /// 通知 master 一组对象已成功卸载到本地磁盘，随附其存储元数据。
    ///
    /// C++ equivalent: `Client::NotifyOffloadSuccess(keys, metadatas)`
    pub async fn notify_offload_success(
        &mut self,
        keys: Vec<String>,
        metadatas: Vec<proto::StorageObjectMetadata>,
    ) -> StoreResult<()> {
        if keys.len() != metadatas.len() {
            return Err(StoreError::InvalidParams(
                "keys and metadatas must have the same length".to_string(),
            ));
        }
        let tasks = keys
            .iter()
            .map(|key| {
                self.pending_legacy_offload_tasks
                    .get(key)
                    .cloned()
                    .ok_or_else(|| {
                        StoreError::InvalidParams(format!(
                            "no generation-bearing offload task retained for key {key:?}; \
                             use offload_object_heartbeat before the key-only notify API, or use \
                             notify_offload_success_tasks"
                        ))
                    })
            })
            .collect::<StoreResult<Vec<_>>>()?;
        self.notify_offload_success_tasks(tasks.clone(), metadatas)
            .await?;
        for task in tasks {
            if self
                .pending_legacy_offload_tasks
                .get(&task.key)
                .is_some_and(|pending| pending.generation_id == task.generation_id)
            {
                self.pending_legacy_offload_tasks.remove(&task.key);
            }
        }
        Ok(())
    }

    /// Notify the master of tenant-scoped offload completion.
    ///
    /// C++ equivalent: `Client::NotifyOffloadSuccess(tasks, metadatas)`.
    pub async fn notify_offload_success_tasks(
        &mut self,
        tasks: Vec<OffloadTaskItem>,
        metadatas: Vec<proto::StorageObjectMetadata>,
    ) -> StoreResult<()> {
        let stale = self
            .notify_offload_success_tasks_inner(tasks, metadatas, None)
            .await?;
        if !stale.is_empty() {
            return Err(StoreError::Internal(
                "master returned recovery-only stale results for a normal offload".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) async fn notify_offload_success_tasks_for_recovery(
        &mut self,
        tasks: Vec<OffloadTaskItem>,
        metadatas: Vec<proto::StorageObjectMetadata>,
        recovery_session_id: uuid::Uuid,
    ) -> StoreResult<Vec<OffloadTaskItem>> {
        self.notify_offload_success_tasks_inner(tasks, metadatas, Some(recovery_session_id))
            .await
    }

    async fn notify_offload_success_tasks_inner(
        &mut self,
        tasks: Vec<OffloadTaskItem>,
        metadatas: Vec<proto::StorageObjectMetadata>,
        recovery_session_id: Option<uuid::Uuid>,
    ) -> StoreResult<Vec<OffloadTaskItem>> {
        let response = self
            .master
            .notify_offload_success(
                self.rpc_request(proto::NotifyOffloadSuccessRequest {
                    client_id: Some(self.client_id_proto()),
                    keys: tasks.iter().map(|task| task.key.clone()).collect(),
                    metadatas,
                    tasks: tasks
                        .into_iter()
                        .map(|task| proto::OffloadTaskItem {
                            tenant_id: task.tenant_id,
                            key: task.key,
                            size: task.size,
                            generation_id: (!task.generation_id.is_nil())
                                .then(|| Self::uuid_to_proto_uuid(task.generation_id)),
                        })
                        .collect(),
                    recovery_session_id: recovery_session_id.map(Self::uuid_to_proto_uuid),
                }),
            )
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response
            .stale_recovery_tasks
            .into_iter()
            .map(OffloadTaskItem::from)
            .collect())
    }

    // -----------------------------------------------------------------------
    // Promotion — move hot data from disk back to memory
    // Promotion —— 将热数据从磁盘移回内存
    //
    // Promotion is also heartbeat-driven:
    //   1. Client calls promotion_object_heartbeat.
    //   2. Master returns objects to promote (key → size).
    //   3. Client calls promotion_alloc_start to allocate a memory replica.
    //   4. Client reads data from disk, writes it to the allocated memory
    //      replica via write_to_replica.
    //   5. Client calls notify_promotion_success (or notify_promotion_failure).
    //
    // Promotion 也是心跳驱动的：
    //   1. 客户端调用 promotion_object_heartbeat。
    //   2. Master 返回需要晋升的对象列表（key → size）。
    //   3. 客户端调用 promotion_alloc_start 分配内存副本。
    //   4. 客户端从磁盘读取数据，通过 write_to_replica 写入分配的内存副本。
    //   5. 客户端调用 notify_promotion_success（或 notify_promotion_failure）。
    // -----------------------------------------------------------------------
}

use super::MooncakeClient;
use super::storage::OffloadTaskItem;
use crate::proto;
use mooncake_store_core::error::StoreResult;
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
        Ok(tasks
            .into_iter()
            .map(|task| (task.key, task.size))
            .collect())
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
            return Ok(response.tasks.into_iter().map(Into::into).collect());
        }
        Ok(response
            .objects
            .into_iter()
            .map(|(key, size)| OffloadTaskItem {
                tenant_id: String::new(),
                key,
                size,
            })
            .collect())
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
        let tasks = keys
            .into_iter()
            .map(|key| OffloadTaskItem {
                tenant_id: self.tenant_id.clone(),
                key,
                size: 0,
            })
            .collect();
        self.notify_offload_success_tasks(tasks, metadatas).await
    }

    /// Notify the master of tenant-scoped offload completion.
    ///
    /// C++ equivalent: `Client::NotifyOffloadSuccess(tasks, metadatas)`.
    pub async fn notify_offload_success_tasks(
        &mut self,
        tasks: Vec<OffloadTaskItem>,
        metadatas: Vec<proto::StorageObjectMetadata>,
    ) -> StoreResult<()> {
        self.master
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
                        })
                        .collect(),
                }),
            )
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
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

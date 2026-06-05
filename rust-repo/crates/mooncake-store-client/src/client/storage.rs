// ============================================================================
// Storage management operations: segment mount, offloading, promotion.
// 存储管理操作：segment 挂载、卸载（offload）、晋升（promotion）。
//
// These operations manage the lifecycle of data on storage tiers:
//   - Memory (fastest, DRAM)
//   - NoF_SSD (NVMe-oF remote SSD)
//   - LocalDisk (local SSD/HDD)
//   - Disk (remote disk)
//
// Offloading moves cold data from memory to disk; promotion moves hot data
// from disk back to memory. The master coordinates these tier transitions.
//
// 这些操作管理存储层级上的数据生命周期：Memory（最快，DRAM）、NoF_SSD（NVMe-oF
// 远端 SSD）、LocalDisk（本地 SSD/HDD）、Disk（远端磁盘）。
// Offload 将冷数据从内存移到磁盘；Promotion 将热数据从磁盘移回内存。
// Master 协调这些层级转换。
//
// C++ equivalent: real_client.cpp MountLocalDiskSegment() /
// OffloadObjectHeartbeat() / PromotionObjectHeartbeat() / etc.
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{NoFSegment, NoFSegmentOwnerInfo, ReplicaDescriptor, StoreError};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentUsage {
    pub total_size: u64,
    pub used_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub fs_dir: String,
    pub enable_disk_eviction: bool,
    pub quota_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffloadTaskItem {
    pub tenant_id: String,
    pub key: String,
    pub size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionTaskItem {
    pub tenant_id: String,
    pub key: String,
    pub size: i64,
}

impl From<proto::OffloadTaskItem> for OffloadTaskItem {
    fn from(task: proto::OffloadTaskItem) -> Self {
        Self {
            tenant_id: task.tenant_id,
            key: task.key,
            size: task.size,
        }
    }
}

impl From<proto::PromotionTaskItem> for PromotionTaskItem {
    fn from(task: proto::PromotionTaskItem) -> Self {
        Self {
            tenant_id: task.tenant_id,
            key: task.key,
            size: task.size,
        }
    }
}

impl MooncakeClient {
    fn uuid_to_proto_uuid(id: Uuid) -> proto::Uuid {
        let (high, low) = id.as_u64_pair();
        proto::Uuid { high, low }
    }

    fn uuid_from_proto_uuid(id: &proto::Uuid) -> Uuid {
        Uuid::from_u64_pair(id.high, id.low)
    }

    fn optional_uuid_to_proto_uuid(id: Uuid) -> Option<proto::Uuid> {
        (!id.is_nil()).then(|| Self::uuid_to_proto_uuid(id))
    }

    fn nof_segment_to_proto(segment: &NoFSegment) -> proto::NoFSegment {
        proto::NoFSegment {
            id: Self::optional_uuid_to_proto_uuid(segment.id),
            name: segment.name.clone(),
            base: segment.base,
            size: segment.size,
            te_endpoint: segment.te_endpoint.clone(),
            client_id: Self::optional_uuid_to_proto_uuid(segment.client_id),
        }
    }

    fn nof_segment_from_proto(segment: &proto::NoFSegment) -> NoFSegment {
        NoFSegment {
            id: segment
                .id
                .as_ref()
                .map(Self::uuid_from_proto_uuid)
                .unwrap_or_else(Uuid::nil),
            name: segment.name.clone(),
            base: segment.base,
            size: segment.size,
            te_endpoint: segment.te_endpoint.clone(),
            client_id: segment
                .client_id
                .as_ref()
                .map(Self::uuid_from_proto_uuid)
                .unwrap_or_else(Uuid::nil),
        }
    }

    fn nof_owner_info_from_proto(owner: &proto::NoFSegmentOwnerInfo) -> NoFSegmentOwnerInfo {
        NoFSegmentOwnerInfo {
            segment_id: owner
                .segment_id
                .as_ref()
                .map(Self::uuid_from_proto_uuid)
                .unwrap_or_else(Uuid::nil),
            client_id: owner
                .client_id
                .as_ref()
                .map(Self::uuid_from_proto_uuid)
                .unwrap_or_else(Uuid::nil),
        }
    }

    // -----------------------------------------------------------------------
    // Segment mount — register a local disk segment with the master
    // Segment 挂载 —— 向 master 注册本地磁盘 segment
    // -----------------------------------------------------------------------

    /// Mount a local disk segment on this node.
    ///
    /// This tells the master that this node is willing to accept local-disk
    /// replicas. When `enable_offloading` is `true`, the node also participates
    /// in the offloading workflow (moving cold data from memory to local disk).
    ///
    /// 在本地节点上挂载一个本地磁盘 segment。
    /// 这告诉 master 本节点愿意接受本地磁盘副本。
    /// 当 enable_offloading 为 true 时，本节点也参与 offloading 工作流
    /// （将冷数据从内存移到本地磁盘）。
    ///
    /// C++ equivalent: `Client::MountLocalDiskSegment(enable_offloading)`
    pub async fn mount_local_disk_segment(&mut self, enable_offloading: bool) -> StoreResult<()> {
        self.master
            .mount_local_disk_segment(proto::MountLocalDiskSegmentRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Mount a NoF (NVMe-over-Fabric) segment with the master.
    ///
    /// C++ equivalent: `MasterClient::MountNoFSegment(segment, client_id)`.
    pub async fn mount_nof_segment(&mut self, segment: &NoFSegment) -> StoreResult<()> {
        self.master
            .mount_no_f_segment(proto::MountNoFSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segment: Some(Self::nof_segment_to_proto(segment)),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Re-register NoF segments after a client restart.
    ///
    /// C++ equivalent: `MasterClient::ReMountNoFSegment(segments, client_id)`.
    pub async fn remount_nof_segments(&mut self, segments: &[NoFSegment]) -> StoreResult<()> {
        self.master
            .re_mount_no_f_segment(proto::ReMountNoFSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segments: segments.iter().map(Self::nof_segment_to_proto).collect(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Unmount a NoF segment by id.
    ///
    /// C++ equivalent: `MasterClient::UnmountNoFSegment(segment_id, client_id)`.
    pub async fn unmount_nof_segment(&mut self, segment_id: Uuid) -> StoreResult<()> {
        self.master
            .unmount_no_f_segment(proto::UnmountNoFSegmentRequest {
                segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
                client_id: Some(self.client_id_proto()),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Query all mounted NoF segments.
    ///
    /// C++ equivalent: `MasterClient::GetAllNoFSegments()`.
    pub async fn get_all_nof_segments(&mut self) -> StoreResult<Vec<NoFSegment>> {
        let response = self
            .master
            .get_all_no_f_segments(proto::GetAllNoFSegmentsRequest {})
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response
            .segments
            .iter()
            .map(Self::nof_segment_from_proto)
            .collect())
    }

    /// Query owners for NoF segments with a given name.
    ///
    /// C++ equivalent: `MasterClient::GetNoFSegmentsByName(segment_name)`.
    pub async fn get_nof_segments_by_name(
        &mut self,
        segment_name: &str,
    ) -> StoreResult<Vec<NoFSegmentOwnerInfo>> {
        let response = self
            .master
            .get_no_f_segments_by_name(proto::GetNoFSegmentsByNameRequest {
                segment_name: segment_name.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response
            .owners
            .iter()
            .map(Self::nof_owner_info_from_proto)
            .collect())
    }

    /// Query aggregate capacity usage for a mounted segment name.
    ///
    /// C++ equivalent: `MasterClient::QuerySegments(segment_name)`.
    pub async fn query_segments(&mut self, segment_name: &str) -> StoreResult<SegmentUsage> {
        let response = self
            .master
            .query_segments(proto::QuerySegmentsRequest {
                segment_name: segment_name.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(SegmentUsage {
            total_size: response.total_size,
            used_size: response.used_size,
        })
    }

    /// Query the storage configuration advertised by the master.
    ///
    /// C++ equivalent: `MasterClient::GetStorageConfig()`.
    pub async fn get_storage_config(&mut self) -> StoreResult<StorageConfig> {
        let response = self
            .master
            .get_storage_config(proto::GetStorageConfigRequest {})
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(StorageConfig {
            fs_dir: response.fs_dir,
            enable_disk_eviction: response.enable_disk_eviction,
            quota_bytes: response.quota_bytes,
        })
    }

    /// Query segment status by name.
    ///
    /// Returns the proto enum value as `i32` to avoid duplicating enum mapping.
    pub async fn query_segment_status(&mut self, segment_name: &str) -> StoreResult<i32> {
        let response = self
            .master
            .query_segment_status(proto::QuerySegmentStatusRequest {
                segment_name: segment_name.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.status)
    }

    /// Query segment status by id.
    ///
    /// Returns the proto enum value as `i32` to avoid duplicating enum mapping.
    pub async fn query_segment_status_by_id(&mut self, segment_id: Uuid) -> StoreResult<i32> {
        let response = self
            .master
            .query_segment_status_by_id(proto::QuerySegmentStatusByIdRequest {
                segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.status)
    }

    /// Return the HA filesystem directory configured on the master.
    ///
    /// C++ equivalent: `MasterClient::GetFsdir()`.
    pub async fn get_fsdir(&mut self) -> StoreResult<String> {
        let response = self
            .master
            .get_fsdir(proto::GetFsdirRequest {})
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.fs_dir)
    }

    /// Check master service readiness and return its version string.
    ///
    /// C++ equivalent: `MasterClient::ServiceReady()`.
    pub async fn service_ready(&mut self) -> StoreResult<String> {
        let response = self
            .master
            .service_ready(proto::ServiceReadyRequest {})
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.version)
    }

    /// Return all keys across tenants for admin/debug use.
    ///
    /// C++ equivalent: `MasterClient::GetAllKeysForAdmin()`.
    pub async fn get_all_keys_for_admin(&mut self) -> StoreResult<Vec<String>> {
        let response = self
            .master
            .get_all_keys_for_admin(proto::GetAllKeysForAdminRequest {})
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.keys)
    }

    /// Return all memory segment names for admin/debug use.
    ///
    /// C++ equivalent: `MasterClient::GetAllSegmentsForAdmin()`.
    pub async fn get_all_segments_for_admin(&mut self) -> StoreResult<Vec<String>> {
        let response = self
            .master
            .get_all_segments_for_admin(proto::GetAllSegmentsForAdminRequest {})
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.segments)
    }

    /// Query segment usage for admin/debug use.
    ///
    /// C++ equivalent: `MasterClient::QuerySegmentForAdmin(segment)`.
    pub async fn query_segment_for_admin(
        &mut self,
        segment_name: &str,
    ) -> StoreResult<SegmentUsage> {
        let response = self
            .master
            .query_segment_for_admin(proto::QuerySegmentsRequest {
                segment_name: segment_name.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(SegmentUsage {
            total_size: response.total_size,
            used_size: response.used_size,
        })
    }

    /// Calculate store-side cache reuse metrics.
    ///
    /// C++ equivalent: `MasterClient::CalcCacheStats()`.
    pub async fn calc_cache_stats(&mut self) -> StoreResult<HashMap<String, f64>> {
        let response = self
            .master
            .calc_cache_stats(proto::CalcCacheStatsRequest {})
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.stats)
    }

    // -----------------------------------------------------------------------
    // Offloading — move cold data from memory to disk
    // Offloading —— 将冷数据从内存移到磁盘
    //
    // Offloading is a heartbeat-driven protocol:
    //   1. Client calls offload_object_heartbeat periodically.
    //   2. Master responds with a list of objects to offload (key → size).
    //   3. Client reads the objects from memory, writes them to local disk,
    //      and calls notify_offload_success.
    //
    // Offloading 是心跳驱动的协议：
    //   1. 客户端定期调用 offload_object_heartbeat。
    //   2. Master 返回需要 offload 的对象列表（key → size）。
    //   3. 客户端从内存读取对象，写入本地磁盘，然后调用 notify_offload_success。
    // -----------------------------------------------------------------------

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
            .offload_object_heartbeat(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
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
            .report_ssd_capacity(proto::ReportSsdCapacityRequest {
                client_id: Some(self.client_id_proto()),
                ssd_total_capacity_bytes: bytes,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
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
                tenant_id: String::new(),
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
            .notify_offload_success(proto::NotifyOffloadSuccessRequest {
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
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
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
        self.promotion_alloc_start_for_tenant(key, "", size, preferred_segments)
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
        self.notify_promotion_success_for_tenant(key, "").await
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
        self.notify_promotion_failure_for_tenant(key, "").await
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

    /// Execute a complete offload cycle:
    /// 1. Heartbeat to get objects-to-offload from master.
    /// 2. Read object data from memory.
    /// 3. Write data to local disk.
    /// 4. Notify master of success.
    ///
    /// Requires a [`LocalStorageBackend`] to be attached via
    /// [`with_local_storage_backend`](Self::with_local_storage_backend).
    ///
    /// Returns the number of objects successfully offloaded.
    ///
    /// 执行完整的 offload 循环：
    /// 1. 心跳获取待 offload 对象。
    /// 2. 从内存读取对象数据。
    /// 3. 将数据写入本地磁盘。
    /// 4. 通知 master 成功。
    ///
    /// 需要先通过 with_local_storage_backend 挂载本地存储后端。
    ///
    /// 返回成功 offload 的对象数量。
    pub async fn offload_objects(&mut self, enable_offloading: bool) -> StoreResult<usize> {
        let objects = self.offload_object_heartbeat(enable_offloading).await?;
        if objects.is_empty() {
            return Ok(0);
        }

        if self.offload_rpc_address().is_empty() {
            let _ = self.start_offload_server().await?;
        }
        let transport_endpoint = self.offload_rpc_address();

        let storage = self.local_storage.as_ref().ok_or_else(|| {
            StoreError::Internal("no local storage backend configured".to_string())
        })?;
        let storage = Arc::clone(storage);

        let mut offloaded = 0usize;
        let mut success_keys = Vec::with_capacity(objects.len());
        let mut metadatas = Vec::with_capacity(objects.len());

        for (key, size) in &objects {
            // Read object data from memory.
            let data = match self.get(key).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %key, %e, "offload: failed to get object from memory, skipping");
                    continue;
                }
            };

            // Write to local disk (blocking I/O).
            let key_owned = key.clone();
            let s = Arc::clone(&storage);
            let write_result =
                tokio::task::spawn_blocking(move || s.write_object(&key_owned, &data))
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))??;

            // Log any evicted keys.
            for evicted_key in &write_result {
                tracing::info!(target: "storage_debug", %evicted_key, "offload: evicted old file");
            }

            offloaded += 1;
            success_keys.push(key.clone());
            metadatas.push(proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: *size,
                transport_endpoint: transport_endpoint.clone(),
            });
        }

        if !success_keys.is_empty() {
            self.notify_offload_success(success_keys, metadatas).await?;
        }

        Ok(offloaded)
    }

    /// Execute a complete promotion cycle:
    /// 1. Heartbeat to get objects-to-promote from master.
    /// 2. Read data from local disk.
    /// 3. Allocate a memory replica via `promotion_alloc_start`.
    /// 4. Write data to the allocated replica via `write_to_replica`.
    /// 5. Notify master of success or failure.
    ///
    /// Requires a [`LocalStorageBackend`] to be attached via
    /// [`with_local_storage_backend`](Self::with_local_storage_backend).
    ///
    /// Returns the number of objects successfully promoted.
    ///
    /// 执行完整的 promotion 循环：
    /// 1. 心跳获取待 promotion 对象。
    /// 2. 从本地磁盘读取数据。
    /// 3. 通过 promotion_alloc_start 分配内存副本。
    /// 4. 通过 write_to_replica 将数据写入分配的副本。
    /// 5. 通知 master 成功或失败。
    ///
    /// 需要先通过 with_local_storage_backend 挂载本地存储后端。
    ///
    /// 返回成功 promotion 的对象数量。
    pub async fn promote_objects(&mut self) -> StoreResult<usize> {
        let objects = self.promotion_object_heartbeat().await?;
        if objects.is_empty() {
            return Ok(0);
        }

        let storage = self.local_storage.as_ref().ok_or_else(|| {
            StoreError::Internal("no local storage backend configured".to_string())
        })?;
        let storage = Arc::clone(storage);

        let mut promoted = 0usize;

        for (key, size) in &objects {
            // Read from local disk (blocking I/O).
            let key_owned = key.clone();
            let data = {
                let s = Arc::clone(&storage);
                tokio::task::spawn_blocking(move || s.read_object(&key_owned))
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))??
            };

            // Allocate a memory replica.
            let replica = match self.promotion_alloc_start(key, *size as u64, vec![]).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %key, %e, "promotion: alloc failed");
                    let _ = self.notify_promotion_failure(key).await;
                    continue;
                }
            };

            // Write data to the allocated memory replica.
            match self.write_to_replica(&replica, &data).await {
                Ok(()) => {
                    self.notify_promotion_success(key).await?;
                    promoted += 1;
                }
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %key, %e, "promotion: write_to_replica failed");
                    let _ = self.notify_promotion_failure(key).await;
                }
            }
        }

        Ok(promoted)
    }

    // -----------------------------------------------------------------------
    // Dynamic segment mount/unmount
    // 动态 segment 挂载/卸载
    //
    // C++ equivalent: RealClient::mountSegment / unmountSegment /
    // allocateAndMountSegment / unmountAndFreeSegment
    // -----------------------------------------------------------------------

    /// Mount a memory segment with the given name, size, and base address.
    /// The memory must already be allocated and registered with the
    /// TransferEngine before calling this (for externally-mapped segments).
    /// After mounting, the segment is registered as a local endpoint for
    /// locality-aware replica selection.
    ///
    /// 挂载指定名称、大小和基地址的内存 segment。
    /// 调用前内存必须已分配并已向 TransferEngine 注册（用于外部映射的 segment）。
    /// 挂载后，该 segment 被注册为本地端点，用于本地性感知副本选择。
    ///
    /// For internally-allocated segments (where this node allocates memory and
    /// opens the segment on the TE), this is handled automatically in
    /// [`create`](Self::create) when `global_segment_size > 0`.
    ///
    /// 对于内部分配的 segment（本节点分配内存并在 TE 上打开 segment），
    /// 在 create() 中 global_segment_size > 0 时自动处理。
    ///
    /// C++ equivalent: `Client::MountSegment()`
    pub async fn mount_segment(
        &mut self,
        segment_name: &str,
        size: u64,
        base_addr: u64,
    ) -> StoreResult<()> {
        let response = self
            .master
            .mount_segment(proto::MountSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segment_name: segment_name.to_string(),
                size,
                base_addr,
                te_endpoint: self.local_hostname.clone(),
                protocol: self.protocol.clone(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let segment_id = response.segment_id.as_ref().ok_or_else(|| {
            StoreError::Internal("MountSegment response missing segment_id".to_string())
        })?;
        self.mounted_segment_ids.write().insert(
            segment_name.to_string(),
            Uuid::from_u64_pair(segment_id.high, segment_id.low),
        );
        // Register as a local endpoint for subsequent locality checks
        self.register_local_endpoint(segment_name);
        Ok(())
    }

    /// Unmount a previously mounted segment from the master.
    ///
    /// 从 master 卸载之前挂载的 segment。
    ///
    /// # Arguments
    /// - `segment_name` — the name of the segment to unmount.
    ///   要卸载的 segment 名称。
    /// - `grace_period_ms` — if > 0, schedules a graceful unmount where the
    ///   master waits for the grace period before actually removing the
    ///   segment. If 0, unmounts immediately.
    ///   如果 > 0，安排优雅卸载——master 在优雅期等待后再实际删除 segment。
    ///   如果为 0，立即卸载。
    ///
    /// C++ equivalent: `Client::UnmountSegment()`
    pub async fn unmount_segment(
        &mut self,
        segment_name: &str,
        grace_period_ms: u64,
    ) -> StoreResult<()> {
        let segment_id = self
            .mounted_segment_ids
            .read()
            .get(segment_name)
            .copied()
            .ok_or_else(|| StoreError::SegmentNotFound(segment_name.to_string()))?;
        let segment_id_proto = Self::uuid_to_proto_uuid(segment_id);

        if grace_period_ms > 0 {
            self.master
                .graceful_unmount_segment(proto::GracefulUnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                    grace_period_ms,
                })
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        } else {
            self.master
                .unmount_segment(proto::UnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                })
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        };
        self.mounted_segment_ids.write().remove(segment_name);
        self.unregister_local_endpoint(segment_name);
        Ok(())
    }
}

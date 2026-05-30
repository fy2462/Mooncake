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
use mooncake_store_core::{ReplicaDescriptor, StoreError};
use std::collections::HashMap;

use super::MooncakeClient;
use crate::proto;

impl MooncakeClient {
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
        let response = self
            .master
            .offload_object_heartbeat(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.objects)
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
        self.master
            .notify_offload_success(proto::NotifyOffloadSuccessRequest {
                client_id: Some(self.client_id_proto()),
                keys,
                metadatas,
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
        let response = self
            .master
            .promotion_object_heartbeat(proto::PromotionObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.objects)
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
        let response = self
            .master
            .promotion_alloc_start(proto::PromotionAllocStartRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                size,
                preferred_segments,
                tenant_id: String::new(),
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
        self.master
            .notify_promotion_success(proto::NotifyPromotionSuccessRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                tenant_id: String::new(),
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
        self.master
            .notify_promotion_failure(proto::NotifyPromotionFailureRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                tenant_id: String::new(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }
}

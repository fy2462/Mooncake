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
use mooncake_store_core::{NoFSegment, NoFSegmentOwnerInfo};
use std::collections::HashMap;
use uuid::Uuid;

use super::MooncakeClient;
use crate::proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentUsage {
    pub total_size: u64,
    pub used_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDetail {
    pub segment_name: String,
    pub segment_id: Uuid,
    pub client_id: Uuid,
    pub base_address: u64,
    pub size_bytes: u64,
    pub te_endpoint: String,
    pub protocol: String,
    pub status: i32,
    pub allocator_used_bytes: u64,
    pub allocator_capacity_bytes: u64,
    pub nof: bool,
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

pub(super) fn local_storage_key(tenant_id: &str, key: &str) -> String {
    if tenant_id.is_empty() {
        key.to_string()
    } else {
        format!("{tenant_id}\0{key}")
    }
}

impl MooncakeClient {
    pub(super) fn uuid_to_proto_uuid(id: Uuid) -> proto::Uuid {
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
            .mount_local_disk_segment(self.rpc_request(proto::MountLocalDiskSegmentRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            }))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    /// Mount a NoF (NVMe-over-Fabric) segment with the master.
    ///
    /// C++ equivalent: `MasterClient::MountNoFSegment(segment, client_id)`.
    pub async fn mount_nof_segment(&mut self, segment: &NoFSegment) -> StoreResult<()> {
        self.master
            .mount_no_f_segment(self.rpc_request(proto::MountNoFSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segment: Some(Self::nof_segment_to_proto(segment)),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    /// Re-register NoF segments after a client restart.
    ///
    /// C++ equivalent: `MasterClient::ReMountNoFSegment(segments, client_id)`.
    pub async fn remount_nof_segments(&mut self, segments: &[NoFSegment]) -> StoreResult<()> {
        self.master
            .re_mount_no_f_segment(self.rpc_request(proto::ReMountNoFSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segments: segments.iter().map(Self::nof_segment_to_proto).collect(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    /// Unmount a NoF segment by id.
    ///
    /// C++ equivalent: `MasterClient::UnmountNoFSegment(segment_id, client_id)`.
    pub async fn unmount_nof_segment(&mut self, segment_id: Uuid) -> StoreResult<()> {
        self.master
            .unmount_no_f_segment(self.rpc_request(proto::UnmountNoFSegmentRequest {
                segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
                client_id: Some(self.client_id_proto()),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }

    /// Query all mounted NoF segments.
    ///
    /// C++ equivalent: `MasterClient::GetAllNoFSegments()`.
    pub async fn get_all_nof_segments(&mut self) -> StoreResult<Vec<NoFSegment>> {
        let response = self
            .master
            .get_all_no_f_segments(self.rpc_request(proto::GetAllNoFSegmentsRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
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
            .get_no_f_segments_by_name(self.rpc_request(proto::GetNoFSegmentsByNameRequest {
                segment_name: segment_name.to_string(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
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
            .query_segments(self.rpc_request(proto::QuerySegmentsRequest {
                segment_name: segment_name.to_string(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(SegmentUsage {
            total_size: response.total_size,
            used_size: response.used_size,
        })
    }

    /// Query detailed metadata and allocator usage for all memory and NoF segments.
    ///
    /// C++ equivalent: `MasterClient::GetSegmentsDetail()`.
    pub async fn get_segments_detail(&mut self) -> StoreResult<Vec<SegmentDetail>> {
        let response = self
            .master
            .get_segments_detail(self.rpc_request(proto::GetSegmentsDetailRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response
            .segments
            .into_iter()
            .map(|segment| SegmentDetail {
                segment_name: segment.segment_name,
                segment_id: segment
                    .segment_id
                    .as_ref()
                    .map(Self::uuid_from_proto_uuid)
                    .unwrap_or_else(Uuid::nil),
                client_id: segment
                    .client_id
                    .as_ref()
                    .map(Self::uuid_from_proto_uuid)
                    .unwrap_or_else(Uuid::nil),
                base_address: segment.base_address,
                size_bytes: segment.size_bytes,
                te_endpoint: segment.te_endpoint,
                protocol: segment.protocol,
                status: segment.status,
                allocator_used_bytes: segment.allocator_used_bytes,
                allocator_capacity_bytes: segment.allocator_capacity_bytes,
                nof: segment.nof,
            })
            .collect())
    }

    /// Query the storage configuration advertised by the master.
    ///
    /// C++ equivalent: `MasterClient::GetStorageConfig()`.
    pub async fn get_storage_config(&mut self) -> StoreResult<StorageConfig> {
        let response = self
            .master
            .get_storage_config(self.rpc_request(proto::GetStorageConfigRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
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
            .query_segment_status(self.rpc_request(proto::QuerySegmentStatusRequest {
                segment_name: segment_name.to_string(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.status)
    }

    /// Query segment status by id.
    ///
    /// Returns the proto enum value as `i32` to avoid duplicating enum mapping.
    pub async fn query_segment_status_by_id(&mut self, segment_id: Uuid) -> StoreResult<i32> {
        let response = self
            .master
            .query_segment_status_by_id(self.rpc_request(proto::QuerySegmentStatusByIdRequest {
                segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.status)
    }

    /// Return the HA filesystem directory configured on the master.
    ///
    /// C++ equivalent: `MasterClient::GetFsdir()`.
    pub async fn get_fsdir(&mut self) -> StoreResult<String> {
        let response = self
            .master
            .get_fsdir(self.rpc_request(proto::GetFsdirRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.fs_dir)
    }

    /// Check master service readiness and return its version string.
    ///
    /// C++ equivalent: `MasterClient::ServiceReady()`.
    pub async fn service_ready(&mut self) -> StoreResult<String> {
        let response = self
            .master
            .service_ready(self.rpc_request(proto::ServiceReadyRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.version)
    }

    /// Return all keys across tenants for admin/debug use.
    ///
    /// C++ equivalent: `MasterClient::GetAllKeysForAdmin()`.
    pub async fn get_all_keys_for_admin(&mut self) -> StoreResult<Vec<String>> {
        let response = self
            .master
            .get_all_keys_for_admin(self.rpc_request(proto::GetAllKeysForAdminRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.keys)
    }

    /// Return all memory segment names for admin/debug use.
    ///
    /// C++ equivalent: `MasterClient::GetAllSegmentsForAdmin()`.
    pub async fn get_all_segments_for_admin(&mut self) -> StoreResult<Vec<String>> {
        let response = self
            .master
            .get_all_segments_for_admin(self.rpc_request(proto::GetAllSegmentsForAdminRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
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
            .query_segment_for_admin(self.rpc_request(proto::QuerySegmentsRequest {
                segment_name: segment_name.to_string(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
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
            .calc_cache_stats(self.rpc_request(proto::CalcCacheStatsRequest {}))
            .await
            .map_err(Self::rpc_status_to_error)?
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
}

//! # Master Service — gRPC 服务入口 / Service Entry Point
//!
//! 本模块是 Master 服务的核心层，包含：
//! - `MasterServiceImpl`: Master gRPC 服务的核心实现结构体，持有所有共享状态和后台 worker 句柄。
//! - 子模块分解各功能域：objects (对象 CRUD)、cluster (集群管理)、replication (副本复制)、
//!   tasks (任务队列)、batches (批量操作)、workers (后台线程)、background_ops (周期性操作) 等。
//!
//! This module is the core service layer of Master, containing:
//! - `MasterServiceImpl`: core implementation struct holding all shared state and background worker handles.
//! - Sub-modules for: objects CRUD, cluster management, replica replication,
//!   task queue, batch operations, background workers, periodic ops, etc.
//!
//! ```text
//! MasterServiceImpl
//!   ├── state: Arc<MasterState>      — 共享并发安全的全局状态
//!   ├── metadata_state                — HTTP metadata 注册状态
//!   ├── graceful_unmount_scheduler    — 优雅卸载调度器（延迟释放 segment）
//!   ├── processing_reaper             — 后台任务回收器（清理超时 offload/promotion）
//!   ├── eviction_worker               — 后台驱逐 worker（自动水位触发驱逐）
//!   ├── client_monitor_worker         — 客户端存活监控 worker
//!   └── oplog_manager                 — 操作日志管理器（热备同步用）
//! ```

mod background_ops;
pub mod cluster;
mod grpc_batches;
mod grpc_objects;
mod grpc_objects_put;
mod grpc_objects_query;
mod grpc_objects_upsert;
mod grpc_replication;
mod grpc_tasks;
mod grpc_trait;
pub(crate) mod helpers;
pub(crate) mod nof_probe;
mod proto_conv;
#[cfg(feature = "spdk-nof-probe")]
mod spdk_rs_probe;
pub(crate) mod state;
mod workers;

use crate::allocator::{
    CACHELIB_SLAB_SIZE, MemoryAllocatorKind, SegmentAllocationError, SegmentAllocator,
};
use crate::count_min_sketch::CountMinSketch;
use crate::ha::LoadedSnapshot;
use crate::http_metadata::MetadataState;
use crate::kv_event::{KvEventPublisher, KvEventStatus};
use crate::metrics;
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::storage_backend::LocalDiskSnapshotEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use crate::tenant_quota::{TenantQuotaError, TenantQuotaSnapshot, TenantQuotaTable};
use crate::tenant_quota_policy_store::{
    TenantQuotaPolicySnapshot, load_tenant_quota_policy, save_tenant_quota_policy,
};
use chrono::Utc;
use dashmap::DashMap;
use mooncake_store_core::{
    NoFSegmentOwnerInfo, ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType,
    ReplicateConfig, TaskInfo, TaskStatus, TaskType,
};
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use self::background_ops::{
    clear_offloading_task, clear_promotion_task, push_offloading_queue,
    release_staged_promotion_replica, run_automatic_eviction_once, run_eviction_cycle,
    try_push_promotion_queue,
};
pub(crate) use self::helpers::sync_cache_total_accounting;
use self::helpers::{
    account_removed_object_quota, addresses_for_client, allocate_memory_replicas,
    allocate_nof_replicas, bump_view_version, choose_drain_target_segment, cleanup_stale_handles,
    client_id_by_nof_segment_name, client_id_by_replica_segment_name, client_id_by_segment_name,
    default_drain_target_segments, get_alive_clients_snapshot, has_pending_task_capacity,
    host_from_segment_name, is_lease_expired, make_tenant_scoped_key, normalize_tenant_id,
    object_owner_client_id, processing_task_capacity, register_metadata_segments,
    release_object_replicas, release_replicas, release_replicas_scheduled, split_scoped_key,
    storage_fs_dir_for_client, sync_client_segments, sync_nof_segment_usage, sync_segment_usage,
    unmount_nof_segment_owned, unmount_segment_owned, upsert_client_addresses, validate_user_key,
};
use self::nof_probe::probe_nof_endpoint;
use self::proto_conv::{
    config_from_proto, nof_segment_from_proto, nof_segment_owner_to_proto, nof_segment_to_proto,
    replica_from_proto, replica_to_proto, replica_type_from_i32, task_status_from_proto,
    task_status_to_proto, task_type_to_proto, uuid_from_proto, uuid_to_proto,
};
use self::state::{
    ActiveDrainTask, DrainJobEntry, LocalDiskSegmentEntry, MasterState, ReplicationTaskEntry,
    ReplicationTaskKind,
};
pub use self::state::{MasterRuntimeConfig, NoFSegmentEntry, ObjectEntry, SegmentEntry, TaskEntry};
use self::workers::{
    ClientMonitorWorker, DrainWorker, EvictionWorker, GracefulUnmountScheduler, NofHeartbeatWorker,
    ProcessingReaper,
};

/// Master 服务的核心实现，持有所有共享状态和后台 worker。
/// state 通过 Arc 在线程间共享，后台 worker 各自持有 Arc 克隆以访问状态。
/// 析构时（Drop）自动停止所有后台线程。
///
/// Core implementation of the Master service. Holds all shared state and
/// background worker handles. State is shared via Arc across threads;
/// each worker clones its own Arc to access state.
/// On Drop, all background threads are stopped automatically.
pub struct MasterServiceImpl {
    state: Arc<MasterState>,
    metadata_state: MetadataState,
    graceful_unmount_scheduler: GracefulUnmountScheduler,
    processing_reaper: ProcessingReaper,
    eviction_worker: EvictionWorker,
    client_monitor_worker: ClientMonitorWorker,
    drain_worker: DrainWorker,
    nof_heartbeat_worker: NofHeartbeatWorker,
    oplog_manager: parking_lot::Mutex<crate::oplog::OpLogManager>,
    kv_event_publisher: Arc<KvEventPublisher>,
}

/// 空间不足时返回给客户端的提示信息 / Hint returned to client on insufficient space.
const PUT_NO_SPACE_HELPER_STR: &str = " due to insufficient space. Consider lowering eviction_high_watermark_ratio or mounting more segments.";

/// ReplicaCopy 任务的 payload 结构。
/// Payload for ReplicaCopy tasks — describes a single key copy from source to targets.
#[derive(Serialize)]
struct ReplicaCopyPayload<'a> {
    key: &'a str,
    source: &'a str,
    targets: &'a [String],
}

/// ReplicaMove 任务的 payload 结构。
/// Payload for ReplicaMove tasks — describes a single key move from source to target.
#[derive(Serialize)]
struct ReplicaMovePayload<'a> {
    key: &'a str,
    source: &'a str,
    target: &'a str,
}

impl MasterServiceImpl {
    fn tenant_quota_capacity_bytes(&self) -> u64 {
        let configured = self.state.runtime_config.tenant_quota_pool_capacity_bytes;
        if configured > 0 {
            configured
        } else {
            self.state.allocator.read().usage_totals().0
        }
    }

    fn tenant_quota_status(error: TenantQuotaError) -> Status {
        match error {
            TenantQuotaError::QuotaExceeded => Status::resource_exhausted("tenant quota exceeded"),
            TenantQuotaError::TenantNotRegistered => {
                Status::resource_exhausted("tenant not registered")
            }
            TenantQuotaError::InvalidArgument => Status::invalid_argument("invalid tenant quota"),
            TenantQuotaError::AccountingMismatch => {
                Status::failed_precondition("tenant quota accounting mismatch")
            }
            TenantQuotaError::TenantNotEmpty => Status::failed_precondition("tenant not empty"),
        }
    }

    pub(crate) fn reserve_tenant_quota(&self, tenant_id: &str, bytes: u64) -> Result<(), Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Ok(());
        }
        let capacity = self.tenant_quota_capacity_bytes();
        let mut quotas = self.state.tenant_quotas.write();
        quotas.recompute_effective_quotas(capacity);
        quotas
            .reserve(tenant_id, bytes)
            .map_err(Self::tenant_quota_status)
    }

    pub(crate) fn commit_tenant_quota(&self, tenant_id: &str, bytes: u64) -> Result<(), Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Ok(());
        }
        self.state
            .tenant_quotas
            .write()
            .commit(tenant_id, bytes)
            .map_err(Self::tenant_quota_status)
    }

    pub(crate) fn abort_tenant_quota(&self, tenant_id: &str, bytes: u64) {
        if self.state.runtime_config.enable_tenant_quota {
            let _ = self.state.tenant_quotas.write().abort(tenant_id, bytes);
        }
    }

    pub(crate) fn account_removed_object_quota(&self, object: &ObjectEntry) {
        account_removed_object_quota(&self.state, object);
    }

    pub fn set_service_available(&self, available: bool) {
        self.state
            .service_available
            .store(available, Ordering::Release);
    }

    pub fn is_service_available(&self) -> bool {
        self.state.service_available.load(Ordering::Acquire)
    }

    pub fn list_tenant_quota_snapshots(&self) -> Result<Vec<TenantQuotaSnapshot>, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Err(Status::failed_precondition("tenant quota is disabled"));
        }
        Ok(self.state.tenant_quotas.read().list_snapshots())
    }

    pub fn get_tenant_quota_snapshot(
        &self,
        tenant_id: &str,
    ) -> Result<Option<TenantQuotaSnapshot>, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Err(Status::failed_precondition("tenant quota is disabled"));
        }
        Ok(self.state.tenant_quotas.read().get_snapshot(tenant_id))
    }

    pub fn upsert_tenant_quota_policy(
        &self,
        tenant_id: &str,
        requested_quota_bytes: u64,
    ) -> Result<TenantQuotaSnapshot, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Err(Status::failed_precondition("tenant quota is disabled"));
        }
        let capacity = self.tenant_quota_capacity_bytes();
        let mut quotas = self.state.tenant_quotas.write();
        let mut next = quotas.clone();
        next.upsert_policy(tenant_id, requested_quota_bytes, capacity)
            .map_err(Self::tenant_quota_status)?;
        self.save_tenant_quota_policy_snapshot(&Self::tenant_quota_policy_snapshot_from_table(
            &next,
        ))?;
        *quotas = next;
        Ok(quotas
            .get_snapshot(tenant_id)
            .expect("tenant policy exists after upsert"))
    }

    pub fn delete_tenant_quota_policy(
        &self,
        tenant_id: &str,
    ) -> Result<Option<TenantQuotaSnapshot>, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Err(Status::failed_precondition("tenant quota is disabled"));
        }
        let capacity = self.tenant_quota_capacity_bytes();
        let mut quotas = self.state.tenant_quotas.write();
        let mut next = quotas.clone();
        let deleted = next
            .erase_policy(tenant_id, capacity)
            .map_err(Self::tenant_quota_status)?;
        self.save_tenant_quota_policy_snapshot(&Self::tenant_quota_policy_snapshot_from_table(
            &next,
        ))?;
        *quotas = next;
        Ok(deleted)
    }

    fn tenant_quota_policy_snapshot_from_table(
        quotas: &TenantQuotaTable,
    ) -> TenantQuotaPolicySnapshot {
        TenantQuotaPolicySnapshot {
            tenant_quotas: quotas
                .list_snapshots()
                .into_iter()
                .filter(|s| s.has_explicit_policy)
                .map(|s| (s.tenant_id, s.requested_quota_bytes))
                .collect(),
        }
    }

    fn save_tenant_quota_policy_snapshot(
        &self,
        snapshot: &TenantQuotaPolicySnapshot,
    ) -> Result<(), Status> {
        save_tenant_quota_policy(
            &self.state.runtime_config.tenant_quota_connector_type,
            &self.state.runtime_config.tenant_quota_connector_uri,
            &self.state.runtime_config.cluster_id,
            snapshot,
        )
        .map_err(Status::internal)
    }

    /// 简化构造函数，使用默认运行时配置 / Simple constructor with default runtime config.
    pub fn new(backend_type: Option<StorageBackendType>, backup_dir: Option<PathBuf>) -> Self {
        Self::new_with_runtime_config(backend_type, backup_dir, MasterRuntimeConfig::default())
    }

    /// 使用自定义运行时配置（无快照后端）/ With custom runtime config, no snapshot backend.
    pub fn with_runtime_config(runtime_config: MasterRuntimeConfig) -> Self {
        Self::new_with_runtime_config(None, None, runtime_config)
    }

    /// 带快照后端的构造函数（无 oplog）/ With snapshot backend, no oplog.
    pub fn new_with_runtime_config(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
    ) -> Self {
        Self::new_with_runtime_config_and_oplog(backend_type, backup_dir, runtime_config, None)
    }

    /// 完整构造函数：初始化所有 DashMap 存储、分配器、快照恢复、后台 worker 和 oplog。
    /// 如果提供了 StorageBackend 且快照文件存在，则从快照恢复 segment / 对象 / 任务状态。
    ///
    /// Full constructor: initializes all DashMap storage, allocators, snapshot restore,
    /// background workers, and oplog. If a StorageBackend is provided and a snapshot
    /// file exists, restores segment/object/task state from the snapshot.
    pub fn new_with_runtime_config_and_oplog(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
        mut oplog_manager: Option<crate::oplog::OpLogManager>,
    ) -> Self {
        let snapshot_backup_dir = backup_dir.clone();
        let storage_backend = match (backend_type, backup_dir) {
            (Some(btype), Some(dir)) => RwLock::new(Some(StorageBackend::new(btype, &dir))),
            _ => RwLock::new(None),
        };
        let mut tenant_quotas = TenantQuotaTable::new(runtime_config.default_tenant_quota_bytes);
        if runtime_config.enable_tenant_quota {
            let policy_snapshot = load_tenant_quota_policy(
                &runtime_config.tenant_quota_connector_type,
                &runtime_config.tenant_quota_connector_uri,
                &runtime_config.cluster_id,
            )
            .unwrap_or_else(|e| panic!("failed to load tenant quota policy: {e}"));
            for (tenant_id, quota) in policy_snapshot.tenant_quotas {
                tenant_quotas
                    .upsert_policy(
                        &tenant_id,
                        quota,
                        runtime_config.tenant_quota_pool_capacity_bytes,
                    )
                    .unwrap_or_else(|e| {
                        panic!("invalid tenant quota policy for {tenant_id}: {e:?}")
                    });
            }
        }

        // 初始化所有 DashMap 存储 — 每个表负责一类数据的并发读写
        // Initialize all DashMap stores — each table handles one category of concurrent read/write
        let kv_event_publisher = Arc::new(KvEventPublisher::new(
            runtime_config.kv_event_config.clone(),
        ));
        let state = Arc::new(MasterState {
            // ── 客户端注册 / client registry ──
            // client_id → ClientEntry：客户端信息、地址、心跳时间
            clients: DashMap::new(),
            // ok_clients 集合：心跳正常的客户端子集，仅本表中有记录的客户端被视为 alive
            ok_clients: DashMap::new(),

            // ── 对象存储 / object store ──
            // key → ObjectEntry：对象的全部元数据（副本列表、大小、last_access、pin 状态等）
            objects: DashMap::new(),
            // 正在 PutStart 中的 key 集合：防止同一 key 的并发 PutStart 冲突
            processing_keys: DashMap::new(),
            // client_id → Set<key>：每个客户端拥有的对象索引，加速客户端下线时批量清理
            client_objects: DashMap::new(),

            // ── Segment 管理 / segment management ──
            // segment_id → SegmentEntry：已挂载的 Memory segment（物理内存段）
            segments: DashMap::new(),
            // segment_id → NoFSegmentEntry：已挂载的 NoF (NVMe-oF) segment
            nof_segments: DashMap::new(),
            // client_id → LocalDiskSegmentEntry：每个客户端的本地磁盘 segment（offload/promotion 队列）
            local_disk_segments: DashMap::new(),

            // ── 任务队列 / task queues ──
            // task_id → TaskEntry：Copy/Move 异步任务（创建 → 分配 worker → 完成）
            tasks: DashMap::new(),
            // key → ReplicationTaskEntry：进行中的副本复制/迁移任务
            replication_tasks: DashMap::new(),

            // ── 后台操作 / background operations ──
            // key → OffloadingTaskEntry：进行中的 offload 任务（内存 → 本地磁盘）
            offloading_tasks: DashMap::new(),
            // key → PromotionTaskEntry：进行中的 promotion 任务（本地磁盘 → 内存）
            promotion_tasks: DashMap::new(),
            // CountMinSketch 频率统计：approximate 访问次数，用于 promotion 准入控制
            promotion_sketch: RwLock::new(CountMinSketch::new()),
            // Transient promotion candidates rejected by watermark/capacity gates.
            promotion_candidates: DashMap::new(),
            // Drain job 表：job_id → DrainJobEntry
            drain_jobs: DashMap::new(),

            // ── 分配器 / allocators ──
            // Memory segment 的段内空间分配器（offset 连续分配 / cachelib slab 分配）
            allocator: RwLock::new(
                SegmentAllocator::new()
                    .with_strategy(runtime_config.allocation_strategy)
                    .with_memory_allocator(runtime_config.memory_allocator_kind),
            ),
            // NoF segment 的段内空间分配器
            nof_allocator: RwLock::new(
                SegmentAllocator::new()
                    .with_strategy(runtime_config.allocation_strategy)
                    .with_memory_allocator(runtime_config.memory_allocator_kind),
            ),

            // ── 持久化 / persistence ──
            // 快照后端的抽象接口（local-disk / hf3fs），用于 HA 状态备份与恢复
            storage_backend,

            // ── 全局计数器 / global counters ──
            // 全局在途 promotion 计数：CAS 式限流，防止并发 promotion 打爆内存
            promotion_in_flight: AtomicUsize::new(0),
            promotion_candidate_count: AtomicUsize::new(0),
            promotion_retry_cursor: AtomicUsize::new(0),
            // 全局视图版本号：段拓扑变更时 +1，Client Ping 时返回，Client 感知版本变化后重新 discover
            view_version: AtomicI64::new(0),

            // ── 运行时 / runtime ──
            // 运行时配置：lease TTL、淘汰水位线、promotion 参数等（只读，无需锁）
            runtime_config: runtime_config.clone(),
            // 服务可用性门控：HA 模式 standby 时为 false，晋升 leader 后置 true
            service_available: AtomicBool::new(true),
            // 租户配额表：per-tenant 存储配额分配与追踪
            tenant_quotas: RwLock::new(tenant_quotas),

            // ── 远端回源 / remote pull ──
            // key → PendingRemotePullEntry：缓存未命中时从 S3 等远端回拉数据的追踪状态
            pending_remote_pulls: DashMap::new(),
            // segment_id → NoFHeartbeatState：NoF segment 的心跳探测状态（下次探测时间、连续失败次数）
            nof_heartbeat_states: DashMap::new(),
            // KV event publisher shared with eviction/offload background paths.
            kv_event_publisher: Arc::clone(&kv_event_publisher),
        });
        let metadata_state = MetadataState::new("");
        // 创建各后台 worker，各自持有 state 的 Arc 克隆
        let graceful_unmount_scheduler = GracefulUnmountScheduler::new(state.clone());
        let processing_reaper = ProcessingReaper::new(state.clone());
        let eviction_worker = EvictionWorker::new(state.clone());
        let client_monitor_worker = ClientMonitorWorker::new(state.clone(), metadata_state.clone());
        let drain_worker = DrainWorker::new(state.clone());
        let nof_heartbeat_worker =
            NofHeartbeatWorker::new(state.clone(), Box::new(probe_nof_endpoint));

        // 如果提供了快照后端，尝试从快照恢复状态
        // If a snapshot backend is provided, try to restore from snapshot
        if let Some(ref backend) = *state.storage_backend.read() {
            if let Some(ref backup_dir) = snapshot_backup_dir {
                let snapshot_path = backup_dir.join("master_snapshot.msgpack");
                let legacy_path = backup_dir.join("master_snapshot.json");
                let existing = if snapshot_path.exists() {
                    snapshot_path
                } else if legacy_path.exists() {
                    legacy_path
                } else {
                    std::path::PathBuf::new()
                };
                // 加载快照前先备份原文件，防止恢复过程中数据损坏
                // Backup original snapshot before loading to prevent corruption during restore
                if !existing.as_os_str().is_empty() && existing.exists() {
                    let backup_path = backup_dir.join("mooncake_snapshot_restore_backup");
                    if let Err(e) = std::fs::create_dir_all(&backup_path) {
                        tracing::warn!("Failed to create snapshot backup dir: {}", e);
                    } else if let Err(e) = std::fs::copy(
                        &existing,
                        backup_path.join(existing.file_name().unwrap_or_default()),
                    ) {
                        tracing::warn!("Failed to backup snapshot: {}", e);
                    }
                }
            }
            // 从快照恢复状态：先恢复 segment（含分配器），再恢复对象和任务
            // Restore from snapshot: segments first (incl. allocators), then objects and tasks
            if let Ok(Some((segments, nof_segments, objects, tasks, local_disk_segments))) =
                backend.load_with_local_disk()
            {
                // 恢复 Memory segments / Restore Memory segments
                for seg in segments {
                    state.segments.insert(
                        seg.segment.id,
                        SegmentEntry {
                            segment: seg.segment.clone(),
                            used: seg.used,
                            client_id: seg.client_id,
                            status: seg.status,
                        },
                    );
                    state
                        .allocator
                        .write()
                        .add_segment(seg.segment, seg.used, seg.client_id);
                }
                // 恢复 NoF segments / Restore NoF segments
                for seg in nof_segments {
                    let status = seg.status;
                    state.nof_segments.insert(
                        seg.segment.id,
                        NoFSegmentEntry {
                            segment: seg.segment.clone(),
                            used: seg.used,
                            status,
                        },
                    );
                    state.nof_allocator.write().add_segment(
                        mooncake_store_core::Segment {
                            id: seg.segment.id,
                            name: seg.segment.name.clone(),
                            base: seg.segment.base,
                            size: seg.segment.size,
                            te_endpoint: seg.segment.te_endpoint.clone(),
                            protocol: String::new(),
                        },
                        seg.used,
                        seg.segment.client_id,
                    );
                }
                // 恢复对象 / Restore objects
                for (key, mut object) in objects {
                    sync_cache_total_accounting(&mut object);
                    state.objects.insert(key, object);
                }
                // 恢复任务 / Restore tasks
                for task in tasks {
                    state.tasks.insert(task.info.id, task);
                }
                // 恢复持久化的本地磁盘状态；promotion 队列仅属于运行时状态。
                // Restore persisted local-disk state; promotion queues are runtime-only.
                for local_disk in local_disk_segments {
                    state.local_disk_segments.insert(
                        local_disk.client_id,
                        state::LocalDiskSegmentEntry {
                            enable_offloading: local_disk.enable_offloading,
                            offloading_objects: local_disk.offloading_objects,
                            promotion_objects: HashMap::new(),
                            ssd_total_capacity_bytes: local_disk.ssd_total_capacity_bytes,
                        },
                    );
                }
                tracing::info!("Restored state from snapshot");
                // 从快照恢复了完整状态
            }
        }

        let oplog_manager = oplog_manager
            .take()
            .unwrap_or_else(|| crate::oplog::OpLogManager::new(None, 0));

        Self {
            state,
            metadata_state,
            graceful_unmount_scheduler,
            processing_reaper,
            eviction_worker,
            client_monitor_worker,
            drain_worker,
            nof_heartbeat_worker,
            oplog_manager: parking_lot::Mutex::new(oplog_manager),
            kv_event_publisher,
        }
    }

    pub fn kv_event_status(&self) -> KvEventStatus {
        self.kv_event_publisher.status()
    }

    fn medium_for_replica_type(replica_type: ReplicaType) -> &'static str {
        match replica_type {
            ReplicaType::Memory | ReplicaType::All => "cpu",
            ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::NoFSsd => "disk",
        }
    }

    fn medium_for_object(object: &ObjectEntry) -> &'static str {
        if object
            .replicas
            .iter()
            .any(|replica| replica.replica_type == ReplicaType::Memory)
        {
            "cpu"
        } else if object.replicas.iter().any(|replica| {
            matches!(
                replica.replica_type,
                ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::NoFSsd
            )
        }) {
            "disk"
        } else {
            "cpu"
        }
    }

    fn publish_kv_stored(&self, scoped_key: &str, replica_type: ReplicaType, object: &ObjectEntry) {
        let medium = if replica_type == ReplicaType::All {
            Self::medium_for_object(object)
        } else {
            Self::medium_for_replica_type(replica_type)
        };
        self.kv_event_publisher.publish_stored(
            object.user_key_for_event(scoped_key),
            medium,
            &object.tenant_id,
            &object.group_id,
        );
    }

    fn publish_kv_removed(&self, scoped_key: &str, object: &ObjectEntry) {
        self.kv_event_publisher.publish_removed(
            object.user_key_for_event(scoped_key),
            Self::medium_for_object(object),
            &object.tenant_id,
            &object.group_id,
        );
    }

    fn publish_kv_removed_with_medium(&self, scoped_key: &str, object: &ObjectEntry, medium: &str) {
        self.kv_event_publisher.publish_removed(
            object.user_key_for_event(scoped_key),
            medium,
            &object.tenant_id,
            &object.group_id,
        );
    }

    /// 保存当前 Master 状态快照到 storage_backend。
    /// 通过 spawn_blocking 在 blocking 线程池中执行，避免阻塞 async runtime。
    /// 同时记录成功/失败计数和耗时指标，供监控系统采集。
    ///
    /// Saves current Master state snapshot to storage_backend.
    /// Runs on blocking thread pool via spawn_blocking to avoid blocking async runtime.
    /// Records success/failure counts and duration metrics for monitoring.
    pub fn save_snapshot(&self) {
        let state = self.state.clone();
        let timeout = state.runtime_config.snapshot_child_timeout;
        let retention_count = state.runtime_config.snapshot_retention_count;
        tokio::spawn(async move {
            let save_task = tokio::task::spawn_blocking(move || {
                let start = std::time::Instant::now();
                let guard = state.storage_backend.read();
                if let Some(ref backend) = *guard {
                    let local_disk_segments = state
                        .local_disk_segments
                        .iter()
                        .map(|entry| {
                            let client_id = *entry.key();
                            (
                                client_id,
                                LocalDiskSnapshotEntry {
                                    client_id,
                                    enable_offloading: entry.enable_offloading,
                                    offloading_objects: entry.offloading_objects.clone(),
                                    ssd_total_capacity_bytes: entry.ssd_total_capacity_bytes,
                                },
                            )
                        })
                        .collect::<DashMap<_, _>>();
                    match backend.save_with_local_disk(
                        &state.segments,
                        &state.nof_segments,
                        &state.objects,
                        &state.tasks,
                        &local_disk_segments,
                    ) {
                        Err(e) => {
                            metrics::SNAPSHOT_FAIL_COUNT.inc();
                            tracing::error!("Failed to save snapshot: {}", e);
                        }
                        _ => match backend.retain_latest_snapshot(retention_count) {
                            Err(e) => {
                                metrics::SNAPSHOT_FAIL_COUNT.inc();
                                tracing::error!("Failed to retain snapshot history: {}", e);
                            }
                            _ => {
                                metrics::SNAPSHOT_DURATION_MS
                                    .set(start.elapsed().as_millis() as i64);
                                metrics::SNAPSHOT_SUCCESS_COUNT.inc();
                            }
                        },
                    }
                }
            });

            match tokio::time::timeout(timeout, save_task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    metrics::SNAPSHOT_FAIL_COUNT.inc();
                    tracing::error!("Snapshot save task failed to join: {}", e);
                }
                Err(_) => {
                    metrics::SNAPSHOT_FAIL_COUNT.inc();
                    tracing::error!("Snapshot save timed out after {:?}", timeout);
                }
            }
        });
    }

    pub fn capture_loaded_snapshot(&self, snapshot_id: impl Into<String>) -> LoadedSnapshot {
        let snapshot_sequence_id = self.oplog_manager.lock().latest_sequence();
        LoadedSnapshot {
            snapshot_id: snapshot_id.into(),
            snapshot_sequence_id,
            segments: self
                .state
                .segments
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
            nof_segments: self
                .state
                .nof_segments
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
            objects: self
                .state
                .objects
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
            tasks: self
                .state
                .tasks
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
            local_disk_segments: self
                .state
                .local_disk_segments
                .iter()
                .map(|entry| LocalDiskSnapshotEntry {
                    client_id: *entry.key(),
                    enable_offloading: entry.enable_offloading,
                    offloading_objects: entry.offloading_objects.clone(),
                    ssd_total_capacity_bytes: entry.ssd_total_capacity_bytes,
                })
                .collect(),
        }
    }

    /// 获取 MetadataState 的克隆（HTTP metadata 服务使用）。
    /// Returns a clone of MetadataState (used by HTTP metadata server).
    pub fn metadata_state(&self) -> MetadataState {
        self.metadata_state.clone()
    }

    /// 按 segment 名称查找 segment UUID / Look up segment UUID by name.
    pub fn segment_id_by_name(&self, segment_name: &str) -> Option<Uuid> {
        self.state
            .segments
            .iter()
            .find(|entry| entry.segment.name == segment_name)
            .map(|entry| entry.segment.id)
    }

    /// Build a consistent detail snapshot for all Memory and NoF segments.
    /// Used by both gRPC `GetSegmentsDetail` and the admin HTTP endpoint.
    pub fn segments_detail_snapshot(&self) -> Vec<proto::SegmentDetailInfo> {
        let memory_used = self.state.allocator.read();
        let nof_used = self.state.nof_allocator.read();
        let mut segments =
            Vec::with_capacity(self.state.segments.len() + self.state.nof_segments.len());

        for entry in self.state.segments.iter() {
            let segment = &entry.segment;
            segments.push(proto::SegmentDetailInfo {
                segment_name: segment.name.clone(),
                segment_id: Some(uuid_to_proto(segment.id)),
                client_id: Some(uuid_to_proto(entry.client_id)),
                base_address: segment.base,
                size_bytes: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: segment.protocol.clone(),
                status: entry.status.into(),
                allocator_used_bytes: memory_used.used_bytes(&segment.id).unwrap_or(entry.used),
                allocator_capacity_bytes: segment.size,
                nof: false,
            });
        }

        for entry in self.state.nof_segments.iter() {
            let segment = &entry.segment;
            segments.push(proto::SegmentDetailInfo {
                segment_name: segment.name.clone(),
                segment_id: Some(uuid_to_proto(segment.id)),
                client_id: Some(uuid_to_proto(segment.client_id)),
                base_address: segment.base,
                size_bytes: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: "nof".to_string(),
                status: entry.status.into(),
                allocator_used_bytes: nof_used.used_bytes(&segment.id).unwrap_or(entry.used),
                allocator_capacity_bytes: segment.size,
                nof: true,
            });
        }

        segments
    }

    #[doc(hidden)]
    pub fn replica_refcnts_for_test(
        &self,
        key: &str,
        replica_type: ReplicaType,
        tenant_id: &str,
    ) -> Vec<u32> {
        let scoped_key = make_tenant_scoped_key(tenant_id, key);
        self.state
            .objects
            .get(&scoped_key)
            .map(|object| {
                object
                    .replicas
                    .iter()
                    .filter(|replica| replica.replica_type == replica_type)
                    .map(|replica| replica.refcnt)
                    .collect()
            })
            .unwrap_or_default()
    }

    #[doc(hidden)]
    pub fn set_replica_handle_valid_for_test(
        &self,
        key: &str,
        segment_name: &str,
        tenant_id: &str,
        handle_valid: bool,
    ) -> bool {
        let scoped_key = make_tenant_scoped_key(tenant_id, key);
        let Some(mut object) = self.state.objects.get_mut(&scoped_key) else {
            return false;
        };
        let Some(replica) = object
            .replicas
            .iter_mut()
            .find(|replica| replica.segment_name == segment_name)
        else {
            return false;
        };
        replica.handle_valid = handle_valid;
        true
    }

    #[doc(hidden)]
    pub fn drain_task_for_test(&self, job_id: Uuid) -> Option<TaskEntry> {
        let job = self.state.drain_jobs.get(&job_id)?;
        let task_id = *job.active_tasks.keys().next()?;
        self.state.tasks.get(&task_id).map(|task| task.clone())
    }

    /// 测试用：执行一轮指定目标数量的驱逐循环。
    /// Testing helper: run one eviction cycle with a target count.
    pub fn run_eviction_cycle_for_test(&self, target_count: usize) -> Vec<String> {
        run_eviction_cycle(&self.state, target_count)
    }

    /// 测试用：执行一轮自动驱逐。
    /// Testing helper: run one automatic eviction cycle.
    pub fn run_automatic_eviction_once_for_test(&self) -> Vec<String> {
        run_automatic_eviction_once(&self.state)
    }

    #[doc(hidden)]
    pub fn promotion_candidate_count_for_test(&self) -> usize {
        self.state
            .promotion_candidate_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 获取 oplog 管理器的可变引用 / Returns mutable reference to oplog manager.
    pub fn oplog_manager(&self) -> &parking_lot::Mutex<crate::oplog::OpLogManager> {
        &self.oplog_manager
    }

    /// Initialize the service view version for a leadership term.
    pub fn set_view_version(&self, version: i64) {
        self.state.view_version.store(version, Ordering::Relaxed);
    }

    /// Build a standby controller that syncs into this service's state.
    /// 构建一个同步到当前 service 状态的 standby controller。
    pub fn create_ha_standby_controller(
        &self,
        spec: crate::ha::HABackendSpec,
        config: crate::ha::MasterServiceSupervisorConfig,
    ) -> crate::ha::CapabilityDrivenStandbyController {
        crate::ha::CapabilityDrivenStandbyController::new_with_state(
            spec,
            config,
            self.state.clone(),
        )
    }
}

/// Drop 实现：依次停止所有后台 worker 线程，确保干净退出。
/// Drop impl: stops all background workers in order to ensure clean shutdown.
impl Drop for MasterServiceImpl {
    fn drop(&mut self) {
        self.graceful_unmount_scheduler.stop();
        self.processing_reaper.stop();
        self.eviction_worker.stop();
        self.client_monitor_worker.stop();
        self.nof_heartbeat_worker.stop();
        self.drain_worker.stop();
    }
}

impl Default for MasterServiceImpl {
    fn default() -> Self {
        Self::new(None, None)
    }
}

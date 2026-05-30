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
//! ## 架构 / Architecture
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
mod grpc_batches;
mod grpc_cluster;
mod grpc_objects;
mod grpc_replication;
mod grpc_tasks;
mod grpc_trait;
pub(crate) mod helpers;
mod proto_conv;
pub(crate) mod state;
mod workers;

use crate::allocator::SegmentAllocator;
use crate::count_min_sketch::CountMinSketch;
use crate::http_metadata::MetadataState;
use crate::metrics;
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::storage_backend::{StorageBackend, StorageBackendType};
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
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::Arc;
use std::time::SystemTime;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use self::background_ops::{
    clear_offloading_task, clear_promotion_task, push_offloading_queue,
    release_staged_promotion_replica, run_automatic_eviction_once, run_eviction_cycle,
    try_push_promotion_queue,
};
use self::helpers::{
    addresses_for_client, allocate_nof_replicas, bump_view_version, cleanup_stale_handles,
    client_id_by_nof_segment_name, client_id_by_replica_segment_name, client_id_by_segment_name,
    get_alive_clients_snapshot, host_from_segment_name, is_lease_expired,
    make_tenant_scoped_key, normalize_tenant_id, object_owner_client_id,
    preferred_nof_segment_names, register_metadata_segments, release_object_replicas,
    release_replicas, release_replicas_scheduled, validate_user_key,
    split_scoped_key, sync_client_segments, sync_nof_segment_usage,
    sync_segment_usage, unmount_nof_segment_owned, unmount_segment_owned, upsert_client_addresses,
    DEFAULT_TENANT,
};
use self::proto_conv::{
    config_from_proto, nof_segment_from_proto, nof_segment_owner_to_proto, nof_segment_to_proto,
    replica_from_proto, replica_to_proto, task_status_from_proto, task_status_to_proto,
    task_type_to_proto, uuid_from_proto, uuid_to_proto,
};
use self::state::{
    ActiveDrainTask, DrainJobEntry, LocalDiskSegmentEntry, MasterState, ReplicationTaskEntry,
    ReplicationTaskKind,
};
pub use self::state::{MasterRuntimeConfig, NoFSegmentEntry, ObjectEntry, SegmentEntry, TaskEntry};
use self::workers::{
    ClientMonitorWorker, EvictionWorker, GracefulUnmountScheduler, ProcessingReaper,
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
    oplog_manager: parking_lot::Mutex<crate::oplog::OpLogManager>,
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

        // 初始化所有 DashMap 存储 — 每个表负责一类数据的并发读写
        // Initialize all DashMap stores — each table handles one category of concurrent read/write
        let state = Arc::new(MasterState {
            clients: DashMap::new(),
            objects: DashMap::new(),
            processing_keys: DashMap::new(),
            client_objects: DashMap::new(),
            segments: DashMap::new(),
            nof_segments: DashMap::new(),
            local_disk_segments: DashMap::new(),
            tasks: DashMap::new(),
            replication_tasks: DashMap::new(),
            offloading_tasks: DashMap::new(),
            promotion_tasks: DashMap::new(),
            promotion_sketch: RwLock::new(CountMinSketch::new()),
            drain_jobs: DashMap::new(),
            allocator: RwLock::new(
                SegmentAllocator::new()
                    .with_strategy(runtime_config.allocation_strategy)
                    .with_memory_allocator(runtime_config.memory_allocator_kind),
            ),
            nof_allocator: RwLock::new(
                SegmentAllocator::new()
                    .with_strategy(runtime_config.allocation_strategy)
                    .with_memory_allocator(runtime_config.memory_allocator_kind),
            ),
            storage_backend,
            promotion_in_flight: AtomicUsize::new(0),
            view_version: AtomicI64::new(0),
            runtime_config: runtime_config.clone(),
            pending_remote_pulls: DashMap::new(),
        });
        let metadata_state = MetadataState::new("");
        // 创建各后台 worker，各自持有 state 的 Arc 克隆
        let graceful_unmount_scheduler = GracefulUnmountScheduler::new(state.clone());
        let processing_reaper = ProcessingReaper::new(state.clone());
        let eviction_worker = EvictionWorker::new(state.clone());
        let client_monitor_worker = ClientMonitorWorker::new(state.clone());

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
            if let Ok(Some((segments, nof_segments, objects, tasks))) = backend.load() {
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
                for (key, object) in objects {
                    state.objects.insert(key, object);
                }
                // 恢复任务 / Restore tasks
                for task in tasks {
                    state.tasks.insert(task.info.id, task);
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
            oplog_manager: parking_lot::Mutex::new(oplog_manager),
        }
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
        tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();
            let guard = state.storage_backend.read();
            if let Some(ref backend) = *guard {
                if let Err(e) = backend.save(
                    &state.segments,
                    &state.nof_segments,
                    &state.objects,
                    &state.tasks,
                ) {
                    metrics::SNAPSHOT_FAIL_COUNT.inc();
                    tracing::error!("Failed to save snapshot: {}", e);
                } else {
                    metrics::SNAPSHOT_DURATION_MS.set(start.elapsed().as_millis() as i64);
                    metrics::SNAPSHOT_SUCCESS_COUNT.inc();
                }
            }
        });
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

    /// 获取 oplog 管理器的可变引用 / Returns mutable reference to oplog manager.
    pub fn oplog_manager(&self) -> &parking_lot::Mutex<crate::oplog::OpLogManager> {
        &self.oplog_manager
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
    }
}

impl Default for MasterServiceImpl {
    fn default() -> Self {
        Self::new(None, None)
    }
}

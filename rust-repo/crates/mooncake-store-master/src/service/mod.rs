mod background_ops;
mod grpc_batches;
mod grpc_cluster;
mod grpc_objects;
mod grpc_replication;
mod grpc_tasks;
mod grpc_trait;
mod helpers;
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
    NoFSegmentOwnerInfo, ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig,
    TaskInfo, TaskStatus, TaskType,
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
    addresses_for_client, allocate_nof_replicas, bump_view_version,
    cleanup_stale_handles, client_id_by_nof_segment_name,
    get_alive_clients_snapshot, is_lease_expired,
    release_object_replicas, client_id_by_replica_segment_name,
    client_id_by_segment_name, host_from_segment_name, object_owner_client_id,
    preferred_nof_segment_names, register_metadata_segments, release_replicas,
    release_replicas_scheduled, sync_client_segments, sync_nof_segment_usage,
    sync_segment_usage, unmount_nof_segment_owned, unmount_segment_owned,
    upsert_client_addresses,
};
use self::proto_conv::{
    config_from_proto, nof_segment_from_proto, nof_segment_owner_to_proto, nof_segment_to_proto,
    replica_from_proto, replica_to_proto, task_status_from_proto, task_status_to_proto,
    task_type_to_proto, uuid_from_proto, uuid_to_proto,
};
use self::state::{
    ActiveDrainTask, DrainJobEntry, LocalDiskSegmentEntry, MasterState,
    ReplicationTaskEntry, ReplicationTaskKind,
};
pub use self::state::{MasterRuntimeConfig, NoFSegmentEntry, ObjectEntry, SegmentEntry, TaskEntry};
use self::workers::{
    ClientMonitorWorker, EvictionWorker, GracefulUnmountScheduler, ProcessingReaper,
};

/// Master 服务的核心实现，持有所有共享状态和后台 worker。
/// state 通过 Arc 在线程间共享，后台 worker 各自持有 Arc 克隆以访问状态。
/// 析构时（Drop）自动停止所有后台线程。
pub struct MasterServiceImpl {
    state: Arc<MasterState>,
    metadata_state: MetadataState,
    graceful_unmount_scheduler: GracefulUnmountScheduler,
    processing_reaper: ProcessingReaper,
    eviction_worker: EvictionWorker,
    client_monitor_worker: ClientMonitorWorker,
    oplog_manager: parking_lot::Mutex<crate::oplog::OpLogManager>,
}

const PUT_NO_SPACE_HELPER_STR: &str = " due to insufficient space. Consider lowering eviction_high_watermark_ratio or mounting more segments.";

#[derive(Serialize)]
struct ReplicaCopyPayload<'a> {
    key: &'a str,
    source: &'a str,
    targets: &'a [String],
}

#[derive(Serialize)]
struct ReplicaMovePayload<'a> {
    key: &'a str,
    source: &'a str,
    target: &'a str,
}

impl MasterServiceImpl {
    pub fn new(backend_type: Option<StorageBackendType>, backup_dir: Option<PathBuf>) -> Self {
        Self::new_with_runtime_config(backend_type, backup_dir, MasterRuntimeConfig::default())
    }

    pub fn with_runtime_config(runtime_config: MasterRuntimeConfig) -> Self {
        Self::new_with_runtime_config(None, None, runtime_config)
    }

    pub fn new_with_runtime_config(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
    ) -> Self {
        Self::new_with_runtime_config_and_oplog(backend_type, backup_dir, runtime_config, None)
    }

    /// 完整构造函数：初始化所有 DashMap 存储、分配器、快照恢复、后台 worker 和 oplog。
    /// 如果提供了 StorageBackend 且快照文件存在，则从快照恢复 segment / 对象 / 任务状态。
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
        let graceful_unmount_scheduler = GracefulUnmountScheduler::new(state.clone());
        let processing_reaper = ProcessingReaper::new(state.clone());
        let eviction_worker = EvictionWorker::new(state.clone());
        let client_monitor_worker = ClientMonitorWorker::new(state.clone());

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
                if !existing.as_os_str().is_empty() && existing.exists() {
                    let backup_path = backup_dir.join("mooncake_snapshot_restore_backup");
                    if let Err(e) = std::fs::create_dir_all(&backup_path) {
                        tracing::warn!("Failed to create snapshot backup dir: {}", e);
                    } else if let Err(e) = std::fs::copy(&existing, backup_path.join(existing.file_name().unwrap_or_default())) {
                        tracing::warn!("Failed to backup snapshot: {}", e);
                    }
                }
            }
            // 从快照恢复状态：先恢复 segment（含分配器），再恢复对象和任务
            if let Ok(Some((segments, nof_segments, objects, tasks))) = backend.load() {
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
                    state.allocator.write().add_segment(seg.segment, seg.used, seg.client_id);
                }
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
                    state.nof_allocator.write().add_segment(mooncake_store_core::Segment {
                        id: seg.segment.id,
                        name: seg.segment.name.clone(),
                        base: seg.segment.base,
                        size: seg.segment.size,
                        te_endpoint: seg.segment.te_endpoint.clone(),
                        protocol: String::new(),
                    }, seg.used, seg.segment.client_id);
                }
                for (key, object) in objects {
                    state.objects.insert(key, object);
                }
                for task in tasks {
                    state.tasks.insert(task.info.id, task);
                }
                tracing::info!("Restored state from snapshot");
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

    pub fn metadata_state(&self) -> MetadataState {
        self.metadata_state.clone()
    }

    pub fn segment_id_by_name(&self, segment_name: &str) -> Option<Uuid> {
        self.state
            .segments
            .iter()
            .find(|entry| entry.segment.name == segment_name)
            .map(|entry| entry.segment.id)
    }

    pub fn run_eviction_cycle_for_test(&self, target_count: usize) -> Vec<String> {
        run_eviction_cycle(&self.state, target_count)
    }

    pub fn run_automatic_eviction_once_for_test(&self) -> Vec<String> {
        run_automatic_eviction_once(&self.state)
    }

    pub fn oplog_manager(&self) -> &parking_lot::Mutex<crate::oplog::OpLogManager> {
        &self.oplog_manager
    }
}

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

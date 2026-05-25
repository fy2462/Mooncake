mod background_ops;
mod grpc_batches;
mod grpc_cluster;
mod grpc_objects;
mod grpc_replication;
mod grpc_tasks;
mod grpc_trait;
mod helpers;
mod proto_conv;
mod state;
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
    client_id_by_nof_segment_name, release_object_replicas,
    client_id_by_replica_segment_name, client_id_by_segment_name, host_from_segment_name,
    object_owner_client_id, preferred_nof_segment_names, register_metadata_segments,
    release_replicas, release_replicas_scheduled, sync_client_segments,
    sync_nof_segment_usage, sync_segment_usage,
    unmount_nof_segment_owned, unmount_segment_owned, upsert_client_addresses,
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

pub struct MasterServiceImpl {
    state: Arc<MasterState>,
    metadata_state: MetadataState,
    graceful_unmount_scheduler: GracefulUnmountScheduler,
    processing_reaper: ProcessingReaper,
    eviction_worker: EvictionWorker,
    client_monitor_worker: ClientMonitorWorker,
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
        let snapshot_backup_dir = backup_dir.clone();
        let storage_backend = match (backend_type, backup_dir) {
            (Some(btype), Some(dir)) => RwLock::new(Some(StorageBackend::new(btype, &dir))),
            _ => RwLock::new(None),
        };

        let state = Arc::new(MasterState {
            clients: DashMap::new(),
            objects: DashMap::new(),
            processing_keys: DashMap::new(),
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
        });
        let metadata_state = MetadataState::new("");
        let graceful_unmount_scheduler = GracefulUnmountScheduler::new(state.clone());
        let processing_reaper = ProcessingReaper::new(state.clone());
        let eviction_worker = EvictionWorker::new(state.clone());
        let client_monitor_worker = ClientMonitorWorker::new(state.clone());

        if let Some(ref backend) = *state.storage_backend.read() {
            if let Some(ref backup_dir) = snapshot_backup_dir {
                let snapshot_path = backup_dir.join("master_snapshot.json");
                if snapshot_path.exists() {
                    let backup_path = backup_dir.join("mooncake_snapshot_restore_backup");
                    if let Err(e) = std::fs::create_dir_all(&backup_path) {
                        tracing::warn!("Failed to create snapshot backup dir: {}", e);
                    } else if let Err(e) = std::fs::copy(&snapshot_path, backup_path.join("master_snapshot.json")) {
                        tracing::warn!("Failed to backup snapshot: {}", e);
                    }
                }
            }
            if let Ok(Some((segments, nof_segments, objects, tasks))) = backend.load() {
                for seg in segments {
                    state.segments.insert(
                        seg.id,
                        SegmentEntry {
                            segment: seg.clone(),
                            status: proto::SegmentStatus::Active,
                        },
                    );
                    state.allocator.write().add_segment(seg);
                }
                for seg in nof_segments {
                    state.nof_segments.insert(
                        seg.segment.id,
                        NoFSegmentEntry {
                            segment: seg.segment.clone(),
                            used: seg.used,
                            status: proto::SegmentStatus::Active,
                        },
                    );
                    state.nof_allocator.write().add_segment(mooncake_store_core::Segment {
                        id: seg.segment.id,
                        name: seg.segment.name.clone(),
                        size: seg.segment.size,
                        used: seg.used,
                        client_id: seg.segment.client_id,
                    });
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

        Self {
            state,
            metadata_state,
            graceful_unmount_scheduler,
            processing_reaper,
            eviction_worker,
            client_monitor_worker,
        }
    }

    pub fn save_snapshot(&self) {
        let start = std::time::Instant::now();
        if let Some(ref backend) = *self.state.storage_backend.read() {
            if let Err(e) = backend.save(
                &self.state.segments,
                &self.state.nof_segments,
                &self.state.objects,
                &self.state.tasks,
            ) {
                metrics::SNAPSHOT_FAIL_COUNT.inc();
                tracing::error!("Failed to save snapshot: {}", e);
            } else {
                metrics::SNAPSHOT_DURATION_MS.set(start.elapsed().as_millis() as i64);
                metrics::SNAPSHOT_SUCCESS_COUNT.inc();
            }
        }
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

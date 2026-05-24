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
use crate::http_metadata::MetadataState;
use crate::metrics;
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use chrono::Utc;
use dashmap::DashMap;
use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, TaskInfo, TaskStatus, TaskType,
};
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
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
    addresses_for_client, client_id_by_segment_name, host_from_segment_name,
    object_owner_client_id, register_metadata_segments, sync_client_segments, sync_segment_usage,
    unmount_segment_owned, upsert_client_addresses,
};
use self::proto_conv::{
    config_from_proto, replica_from_proto, replica_to_proto, task_status_from_proto,
    task_status_to_proto, task_type_to_proto, uuid_from_proto, uuid_to_proto,
};
use self::state::{
    LocalDiskSegmentEntry, MasterState, ReplicationTaskEntry, ReplicationTaskKind, TaskEntry,
};
pub use self::state::{MasterRuntimeConfig, ObjectEntry, SegmentEntry};
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
        let storage_backend = match (backend_type, backup_dir) {
            (Some(btype), Some(dir)) => RwLock::new(Some(StorageBackend::new(btype, &dir))),
            _ => RwLock::new(None),
        };

        let state = Arc::new(MasterState {
            clients: DashMap::new(),
            objects: DashMap::new(),
            segments: DashMap::new(),
            local_disk_segments: DashMap::new(),
            tasks: DashMap::new(),
            replication_tasks: DashMap::new(),
            offloading_tasks: DashMap::new(),
            promotion_tasks: DashMap::new(),
            promotion_access_counts: DashMap::new(),
            allocator: RwLock::new(
                SegmentAllocator::new()
                    .with_strategy(runtime_config.allocation_strategy)
                    .with_memory_allocator(runtime_config.memory_allocator_kind),
            ),
            storage_backend,
            promotion_in_flight: AtomicUsize::new(0),
            runtime_config: runtime_config.clone(),
        });
        let metadata_state = MetadataState::new("");
        let graceful_unmount_scheduler = GracefulUnmountScheduler::new(state.clone());
        let processing_reaper = ProcessingReaper::new(state.clone());
        let eviction_worker = EvictionWorker::new(state.clone());
        let client_monitor_worker = ClientMonitorWorker::new(state.clone());

        if let Some(ref backend) = *state.storage_backend.read() {
            if let Ok(Some((segments, objects))) = backend.load() {
                for seg in segments {
                    state.segments.insert(
                        seg.id,
                        SegmentEntry {
                            segment: seg.clone(),
                        },
                    );
                    state.allocator.write().add_segment(seg);
                }
                for (key, object) in objects {
                    state.objects.insert(key, object);
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
            if let Err(e) = backend.save(&self.state.segments, &self.state.objects) {
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

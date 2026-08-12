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

use crate::TenantId;
use crate::allocator::{
    AllocationStrategy, AllocatorSnapshotConfig, CACHELIB_MAX_SEGMENT_SIZE, CACHELIB_SLAB_SIZE,
    MemoryAllocatorKind, SegmentAllocationError, SegmentAllocator,
};
use crate::count_min_sketch::CountMinSketch;
use crate::ha::{HaError, LoadedSnapshot};
use crate::http_metadata::MetadataState;
use crate::kv_event::{KvEventPublisher, KvEventStatus};
use crate::metrics;
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::storage_backend::LocalDiskSnapshotEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use crate::tenant_quota::{TenantQuotaError, TenantQuotaSnapshot, TenantQuotaTable};
use crate::tenant_quota_policy_store::{
    TenantQuotaPolicySnapshot, advance_tenant_quota_policy_term, load_tenant_quota_policy,
    save_tenant_quota_policy,
};
use chrono::Utc;
use dashmap::DashMap;
use mooncake_store_core::{
    NoFSegmentOwnerInfo, ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType,
    ReplicateConfig, TaskInfo, TaskStatus, TaskType, stable_memory_segment_id,
};
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub(crate) use self::background_ops::{
    cancel_promotion_task, clear_offloading_task, clear_promotion_task,
};
use self::background_ops::{
    detach_staged_promotion_replica, push_offloading_queue, run_automatic_eviction_once,
    run_automatic_nof_eviction_once, run_eviction_cycle, run_nof_eviction_cycle,
    run_promotion_candidate_retry, run_tenant_quota_eviction, try_push_promotion_queue,
};
pub(crate) use self::helpers::sync_cache_total_accounting;
use self::helpers::{
    account_cache_total_removal, account_removed_object_quota, allocate_memory_replicas,
    allocate_nof_replicas, allocating_memory_quota_charge, bump_view_version,
    checked_allocating_memory_quota_charge, checked_completed_memory_quota_charge,
    checked_durable_committed_memory_quota_charge, checked_requested_memory_quota_charge,
    choose_drain_target_segment, clear_invalid_handles_for_key_locked,
    client_id_by_exact_replica_segment, client_id_by_replica_segment_id, clone_object_for_mutation,
    completed_memory_quota_charge, default_drain_target_segments, get_alive_clients_snapshot,
    global_disk_replica, has_pending_task_capacity, host_from_segment_name, is_lease_expired,
    object_has_inflight_write, object_owner_client_id, processing_task_capacity,
    query_ip_addresses_for_client, register_metadata_segments,
    release_committed_memory_quota_charge, release_object_replicas, release_replicas,
    requested_memory_quota_charge, resolve_request_tenant, resolve_write_tenant,
    settle_additional_memory_quota_charge, settle_and_release_memory_quota_charge,
    storage_fs_dir_for_client, sync_client_segments, sync_nof_segment_usage, sync_segment_usage,
    unique_active_memory_segment_id, unique_active_replica_segment_identity, unique_task_id,
    unmount_nof_segment_owned_durable, unmount_nof_segment_owned_durable_locked,
    unmount_segment_owned_durable_locked, upsert_client_addresses, validate_user_key,
};
use self::nof_probe::probe_nof_endpoint;
use self::proto_conv::{
    config_from_proto, nof_segment_from_proto, nof_segment_owner_to_proto, nof_segment_to_proto,
    replica_from_proto, replica_to_proto_for_state, request_replica_type_from_i32,
    request_task_status_from_i32, task_status_to_proto, task_type_to_proto, uuid_from_proto,
    uuid_to_proto,
};
pub(crate) use self::state::ReplicationTaskEntry;
use self::state::{ActiveDrainTask, DrainJobEntry, LocalDiskSegmentEntry, MasterState};
pub use self::state::{
    DelayedReplicaReleaseEntry, GracefulUnmountSnapshotEntry, MasterRuntimeConfig, NoFSegmentEntry,
    ObjectEntry, ReplicationTaskKind, ReplicationTaskSnapshotEntry, SegmentEntry, TaskEntry,
};
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
    /// Serializes detached `spawn_blocking` snapshot writers. A Tokio timeout
    /// cannot stop a blocking task, so the flag is cleared by that task's RAII
    /// guard only when it has actually exited.
    snapshot_save_in_flight: Arc<AtomicBool>,
    #[cfg(test)]
    policy_save_test_barrier: parking_lot::Mutex<Option<std::sync::Arc<PolicySaveTestBarrier>>>,
    metadata_state: MetadataState,
    graceful_unmount_scheduler: GracefulUnmountScheduler,
    processing_reaper: ProcessingReaper,
    eviction_worker: EvictionWorker,
    client_monitor_worker: ClientMonitorWorker,
    drain_worker: DrainWorker,
    nof_heartbeat_worker: NofHeartbeatWorker,
    oplog_manager: Arc<crate::oplog::OpLogManager>,
    kv_event_publisher: Arc<KvEventPublisher>,
}

struct SnapshotSaveInFlightGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for SnapshotSaveInFlightGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

enum SnapshotSaveOutcome {
    BackendDisabled,
    Cancelled,
    Saved(Duration),
}

pub(crate) fn canonicalize_snapshot_task_payload(
    task: &TaskEntry,
    canonical_task_key: &str,
) -> Result<String, String> {
    let mut payload =
        serde_json::from_str::<serde_json::Value>(&task.payload).map_err(|error| {
            format!(
                "snapshot task {} payload is invalid JSON: {error}",
                task.info.id
            )
        })?;
    let object = payload.as_object_mut().ok_or_else(|| {
        format!(
            "snapshot task {} payload is not a JSON object",
            task.info.id
        )
    })?;
    let raw_key = object
        .get("key")
        .and_then(serde_json::Value::as_str)
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("snapshot task {} payload has no key", task.info.id))?;

    let (tenant_id, user_key) = match object.get("tenant_id") {
        Some(value) => {
            let raw_tenant = value.as_str().ok_or_else(|| {
                format!(
                    "snapshot task {} payload tenant id is not a string",
                    task.info.id
                )
            })?;
            let tenant_id = TenantId::new(raw_tenant.to_owned()).map_err(|error| {
                format!(
                    "snapshot task {} payload has invalid tenant identity: {error}",
                    task.info.id
                )
            })?;
            if raw_key.contains('\0') {
                let (key_tenant, user_key) =
                    TenantId::parse_scoped_key(&raw_key).map_err(|error| {
                        format!(
                            "snapshot task {} payload has invalid scoped key: {error}",
                            task.info.id
                        )
                    })?;
                if user_key.contains('\0') {
                    return Err(format!(
                        "snapshot task {} payload key contains multiple tenant delimiters",
                        task.info.id
                    ));
                }
                if key_tenant != tenant_id {
                    return Err(format!(
                        "snapshot task {} payload tenant conflicts with its scoped key",
                        task.info.id
                    ));
                }
                (tenant_id, user_key)
            } else {
                (tenant_id, raw_key)
            }
        }
        None => TenantId::parse_scoped_key(&raw_key).map_err(|error| {
            format!(
                "snapshot task {} payload has invalid legacy key: {error}",
                task.info.id
            )
        })?,
    };
    if user_key.is_empty() || user_key.contains('\0') {
        return Err(format!(
            "snapshot task {} payload has an invalid user key",
            task.info.id
        ));
    }
    let payload_scoped_key = tenant_id.make_scoped_key(&user_key);
    if payload_scoped_key != canonical_task_key {
        return Err(format!(
            "snapshot task {} payload identity {:?} conflicts with task key {:?}",
            task.info.id, payload_scoped_key, canonical_task_key
        ));
    }

    let source = object
        .get("source")
        .and_then(serde_json::Value::as_str)
        .filter(|source| !source.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("snapshot task {} payload has no source", task.info.id))?;
    match task.info.task_type {
        TaskType::ReplicaCopy => {
            let targets = object
                .get("targets")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    format!("snapshot copy task {} payload has no targets", task.info.id)
                })?;
            if targets.is_empty()
                || targets
                    .iter()
                    .any(|target| target.as_str().map(str::is_empty) != Some(false))
            {
                return Err(format!(
                    "snapshot copy task {} payload has invalid targets",
                    task.info.id
                ));
            }
        }
        TaskType::ReplicaMove => {
            let target = object
                .get("target")
                .and_then(serde_json::Value::as_str)
                .filter(|target| !target.is_empty())
                .ok_or_else(|| {
                    format!("snapshot move task {} payload has no target", task.info.id)
                })?;
            if target == source.as_str() {
                return Err(format!(
                    "snapshot move task {} payload uses the source as its target",
                    task.info.id
                ));
            }
        }
    }

    object.insert(
        "tenant_id".to_owned(),
        serde_json::Value::String(tenant_id.into_string()),
    );
    object.insert("key".to_owned(), serde_json::Value::String(user_key));
    serde_json::to_string(&payload).map_err(|error| {
        format!(
            "snapshot task {} canonical payload serialization failed: {error}",
            task.info.id
        )
    })
}

fn is_active_drain_task(task: &TaskEntry) -> bool {
    matches!(
        task.info.status,
        TaskStatus::Pending | TaskStatus::Processing
    ) && task.info.message.starts_with("drain ")
}

pub(crate) fn restore_loaded_snapshot_state(
    state: &MasterState,
    mut segments: Vec<SegmentEntry>,
    mut nof_segments: Vec<NoFSegmentEntry>,
    mut objects: Vec<(String, ObjectEntry)>,
    mut tasks: Vec<TaskEntry>,
    replication_tasks: Vec<ReplicationTaskSnapshotEntry>,
    local_disk_segments: Vec<LocalDiskSnapshotEntry>,
    graceful_unmounts: Vec<GracefulUnmountSnapshotEntry>,
    mut delayed_replica_releases: Vec<state::DelayedReplicaReleaseEntry>,
    allocator_config: Option<AllocatorSnapshotConfig>,
) -> Result<(), String> {
    if let Some(snapshot_config) = allocator_config {
        let runtime_config = AllocatorSnapshotConfig {
            allocation_strategy: state.runtime_config.allocation_strategy,
            memory_allocator_kind: state.runtime_config.memory_allocator_kind,
            offset_max_allocation_nodes: state.runtime_config.offset_max_allocation_nodes,
        };
        if snapshot_config != runtime_config {
            return Err(format!(
                "snapshot allocator configuration {:?} does not match runtime {:?}",
                snapshot_config, runtime_config
            ));
        }
    }
    let mut memory_segment_ids = HashSet::with_capacity(segments.len());
    for entry in &segments {
        if entry.segment.id.is_nil()
            || entry.segment.name.is_empty()
            || entry.segment.size == 0
            || entry.client_id.is_nil()
        {
            return Err(
                "snapshot contains a Memory segment with incomplete durable identity".to_string(),
            );
        }
        if !memory_segment_ids.insert(entry.segment.id) {
            return Err(format!(
                "snapshot contains duplicate Memory segment UUID {}",
                entry.segment.id
            ));
        }
        if entry.segment.protocol == "cxl" {
            if entry.segment.size != state.runtime_config.cxl_size {
                return Err(format!(
                    "snapshot CXL segment {} size {} does not match runtime CXL size {}",
                    entry.segment.id, entry.segment.size, state.runtime_config.cxl_size
                ));
            }
        } else if state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
            && entry.segment.size % CACHELIB_SLAB_SIZE != 0
        {
            return Err(format!(
                "snapshot Memory segment {} size is not Cachelib slab aligned",
                entry.segment.id
            ));
        }
        if state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
            && entry.segment.size > CACHELIB_MAX_SEGMENT_SIZE
        {
            return Err(format!(
                "snapshot Memory segment {} exceeds Cachelib slab index capacity",
                entry.segment.id
            ));
        }
    }
    if !nof_segments.is_empty() && !state.runtime_config.enable_nof {
        return Err("snapshot contains NoF segments but NoF is not enabled".to_string());
    }
    let mut nof_segment_ids = HashSet::with_capacity(nof_segments.len());
    for entry in &nof_segments {
        if entry.segment.id.is_nil()
            || entry.segment.name.is_empty()
            || entry.segment.size == 0
            || entry.segment.client_id.is_nil()
        {
            return Err(
                "snapshot contains a NoF segment with incomplete durable identity".to_string(),
            );
        }
        if entry.status == proto::SegmentStatus::GracefullyUnmounting {
            return Err(format!(
                "snapshot NoF segment {} has unsupported graceful-unmount status",
                entry.segment.id
            ));
        }
        if !nof_segment_ids.insert(entry.segment.id) {
            return Err(format!(
                "snapshot contains duplicate NoF segment UUID {}",
                entry.segment.id
            ));
        }
        if memory_segment_ids.contains(&entry.segment.id) {
            return Err(format!(
                "snapshot Memory and NoF topology share segment UUID {}",
                entry.segment.id
            ));
        }
        if state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike {
            if entry.segment.size % CACHELIB_SLAB_SIZE != 0 {
                return Err(format!(
                    "snapshot NoF segment {} size is not Cachelib slab aligned",
                    entry.segment.id
                ));
            }
            if entry.segment.size > CACHELIB_MAX_SEGMENT_SIZE {
                return Err(format!(
                    "snapshot NoF segment {} exceeds Cachelib slab index capacity",
                    entry.segment.id
                ));
            }
        }
    }
    let restore_instant = std::time::Instant::now();
    let cxl_segment_ids = segments
        .iter()
        .filter(|entry| entry.segment.protocol == "cxl")
        .map(|entry| entry.segment.id)
        .collect::<HashSet<_>>();
    if !cxl_segment_ids.is_empty() && !state.runtime_config.enable_cxl {
        return Err("snapshot contains CXL segments but CXL is not enabled".to_string());
    }
    if !cxl_segment_ids.is_empty()
        && state.runtime_config.allocation_strategy != AllocationStrategy::Cxl
    {
        return Err(
            "snapshot contains CXL aliases but runtime allocation_strategy is not cxl".to_string(),
        );
    }
    if state.runtime_config.allocation_strategy == AllocationStrategy::Cxl
        && segments.iter().any(|entry| entry.segment.protocol != "cxl")
    {
        return Err(
            "runtime allocation_strategy=cxl cannot restore non-CXL Memory segments".to_string(),
        );
    }
    // Drain jobs are runtime scheduling state and are deliberately not part of
    // the Store snapshot contract. A restored DRAINING status would otherwise
    // strand the segment forever with no job able to make progress. Recovery
    // therefore aborts unfinished drains back to ACTIVE; terminal
    // UNAVAILABLE and graceful-unmount states remain authoritative.
    for segment in &mut segments {
        if segment.status == proto::SegmentStatus::Draining {
            segment.status = proto::SegmentStatus::Active;
        }
    }
    for segment in &mut nof_segments {
        if segment.status == proto::SegmentStatus::Draining {
            segment.status = proto::SegmentStatus::Active;
        }
    }
    let mut local_disk_storage_by_client = HashMap::new();
    let mut local_disk_storage_ids = HashSet::new();
    for entry in &local_disk_segments {
        let storage_id = if entry.storage_id.is_nil() {
            entry.client_id
        } else {
            entry.storage_id
        };
        if storage_id.is_nil() {
            return Err("snapshot LocalDisk entry has no durable storage identity".to_string());
        }
        if !local_disk_storage_ids.insert(storage_id) {
            return Err(format!(
                "snapshot contains duplicate LocalDisk storage identity {storage_id}"
            ));
        }
        if entry.client_id.is_nil() {
            continue;
        }
        if let Some(previous) =
            local_disk_storage_by_client.insert(entry.client_id, entry.storage_id)
            && previous != entry.storage_id
        {
            return Err(format!(
                "snapshot maps LocalDisk client {} to multiple storage identities",
                entry.client_id
            ));
        }
    }

    let mut canonical_object_keys = HashSet::new();
    let mut canonical_objects = Vec::with_capacity(objects.len());
    for (durable_key, mut object) in objects {
        let (tenant_id, user_key) = TenantId::parse_scoped_key(&durable_key)
            .map_err(|error| format!("snapshot object has invalid tenant identity: {error}"))?;
        if user_key.contains('\0') {
            return Err(format!(
                "snapshot object key contains multiple tenant delimiters: {durable_key:?}"
            ));
        }
        if object.tenant_id != tenant_id {
            return Err(format!(
                "snapshot object tenant mismatch for key {durable_key:?}: metadata={}, key={tenant_id}",
                object.tenant_id
            ));
        }
        if object.user_key.is_empty() {
            object.user_key = user_key.clone();
        } else if object.user_key != user_key {
            return Err(format!(
                "snapshot object user-key mismatch for key {durable_key:?}: metadata={:?}",
                object.user_key
            ));
        }
        if object.quota_committed && object.pending_replaced_quota_charge_bytes != 0 {
            return Err(format!(
                "snapshot committed object {durable_key:?} retains a pending replaced quota charge"
            ));
        }
        if object.size == 0 {
            return Err(format!(
                "snapshot object {durable_key:?} has zero logical size"
            ));
        }
        if object.replicas.is_empty() {
            return Err(format!(
                "snapshot object {durable_key:?} has no replica descriptors"
            ));
        }
        if object.quota_committed && object.reserved_quota_charge_bytes != 0 {
            return Err(format!(
                "snapshot committed object {durable_key:?} retains reserved quota bytes"
            ));
        }
        if !object.quota_committed && object.committed_quota_charge_bytes != 0 {
            return Err(format!(
                "snapshot uncommitted object {durable_key:?} retains committed quota bytes"
            ));
        }
        let mut replica_locations = HashSet::new();
        for replica in &object.replicas {
            if replica.replica_type == ReplicaType::All {
                return Err(format!(
                    "snapshot object {durable_key:?} contains an ALL replica descriptor"
                ));
            }
            if replica.size != object.size {
                return Err(format!(
                    "snapshot object {durable_key:?} replica size {} does not match object size {}",
                    replica.size, object.size
                ));
            }
            replica.offset.checked_add(replica.size).ok_or_else(|| {
                format!("snapshot object {durable_key:?} contains an overflowing replica range")
            })?;
            if matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) && replica.segment_id.is_nil()
            {
                return Err(format!(
                    "snapshot object {durable_key:?} contains a nil-segment {:?} replica",
                    replica.replica_type
                ));
            }
            let location = (
                replica.segment_id,
                replica.offset,
                replica.size,
                replica.replica_type,
                replica.local_disk_storage_id,
                replica.local_disk_generation_id,
            );
            if !replica_locations.insert(location) {
                return Err(format!(
                    "snapshot object {durable_key:?} contains a duplicate replica location"
                ));
            }
        }
        let canonical_key = tenant_id.make_scoped_key(&user_key);
        if !canonical_object_keys.insert(canonical_key.clone()) {
            return Err(format!(
                "snapshot contains duplicate canonical object key {canonical_key:?}"
            ));
        }
        canonical_objects.push((canonical_key, object));
    }
    objects = canonical_objects;

    for (_, object) in &mut objects {
        for replica in &mut object.replicas {
            match replica.replica_type {
                ReplicaType::Memory | ReplicaType::NoFSsd => {
                    // A process address or transport endpoint from a snapshot
                    // can never be routed in the new Master term. Preserve
                    // only durable segment identity and byte range until the
                    // owning client supplies current runtime coordinates.
                    replica.handle_valid = false;
                    replica.base_addr = 0;
                    replica.protocol.clear();
                }
                ReplicaType::LocalDisk => {
                    if replica.local_disk_storage_id.is_none() {
                        replica.local_disk_storage_id =
                            replica.holder_client_id.and_then(|client_id| {
                                local_disk_storage_by_client
                                    .get(&client_id)
                                    .copied()
                                    // Legacy catalog snapshots keyed LocalDisk state by
                                    // client UUID, so the holder itself is the final
                                    // fallback only when no durable mapping exists.
                                    .or(Some(client_id))
                            });
                    }
                    // Never infer a byte generation from key/size/storage alone.
                    // Legacy snapshots without an exact generation remain
                    // fail-closed and are removed if the next inventory cannot
                    // present a Master-issued identity.
                    // Preserve process metadata for snapshot continuity, but
                    // never restore it as a live route before inventory recovery.
                    replica.handle_valid = false;
                }
                ReplicaType::Disk | ReplicaType::All => {}
            }
        }
    }
    let mut restored_replication_tasks = Vec::with_capacity(replication_tasks.len());
    let mut replication_keys = HashSet::new();
    for snapshot_task in replication_tasks {
        let (raw_key, mut task) = snapshot_task.into_runtime(restore_instant);
        let (tenant_id, user_key) = TenantId::parse_scoped_key(&raw_key).map_err(|error| {
            format!("snapshot replication task has invalid tenant identity: {error}")
        })?;
        if user_key.contains('\0') {
            return Err(format!(
                "snapshot replication task key contains multiple tenant delimiters: {raw_key:?}"
            ));
        }
        let key = tenant_id.make_scoped_key(&user_key);
        for replica in std::iter::once(&mut task.source)
            .chain(task.targets.iter_mut())
            .chain(task.existing_move_target.iter_mut())
        {
            if matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) {
                replica.handle_valid = false;
                replica.base_addr = 0;
                replica.protocol.clear();
            }
        }
        if !replication_keys.insert(key.clone()) {
            return Err(format!(
                "snapshot contains duplicate native replication task for {key:?}"
            ));
        }
        restored_replication_tasks.push((key, task));
    }

    let covered_targets = restored_replication_tasks
        .iter()
        .flat_map(|(key, task)| {
            task.targets.iter().map(move |target| {
                (
                    key.clone(),
                    target.segment_id,
                    target.offset,
                    target.size,
                    target.replica_type,
                )
            })
        })
        .collect::<HashSet<_>>();
    let mut orphaned_staged_releases = Vec::new();
    for (key, object) in &mut objects {
        let preserves_committed_generation =
            object.put_start_time.is_some() && object.quota_committed;
        let mut retained = Vec::with_capacity(object.replicas.len());
        let mut orphaned = Vec::new();
        for replica in std::mem::take(&mut object.replicas) {
            let keep = replica.status == ReplicaStatus::Complete
                || preserves_committed_generation
                || covered_targets.contains(&(
                    key.clone(),
                    replica.segment_id,
                    replica.offset,
                    replica.size,
                    replica.replica_type,
                ));
            if keep {
                retained.push(replica);
            } else if matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) {
                orphaned.push(replica);
            }
        }
        object.replicas = retained;
        if !orphaned.is_empty() {
            tracing::warn!(
                key,
                removed = orphaned.len(),
                "quarantined orphan non-complete replicas from snapshot"
            );
            orphaned_staged_releases.push((key.clone(), orphaned));
            object.reserved_quota_charge_bytes = 0;
            // Uncommitted Put/Upsert allocations and malformed legacy targets
            // reach here. Durable replication-task targets were retained
            // above, while delayed old buffers live in their separate
            // reservation table and remain represented in allocator recovery.
            object.pending_replaced_quota_charge_bytes = 0;
            if object
                .replicas
                .iter()
                .any(|replica| replica.status == ReplicaStatus::Complete)
            {
                object.quota_committed = true;
            }
        }
        if !object_has_inflight_write(object) {
            object.put_start_time = None;
        }
    }
    objects.retain(|(_, object)| !object.replicas.is_empty());
    let object_indices = objects
        .iter()
        .enumerate()
        .map(|(index, (key, _))| (key.clone(), index))
        .collect::<HashMap<_, _>>();

    let mut restored_task_ids = HashSet::new();
    for task in &mut tasks {
        if task.info.id.is_nil() {
            return Err("snapshot contains a nil task UUID".to_string());
        }
        if task
            .info
            .assigned_client
            .is_some_and(|client_id| client_id.is_nil())
        {
            return Err(format!(
                "snapshot task {} has a nil assigned client UUID",
                task.info.id
            ));
        }
        if task.info.last_updated_at < task.info.created_at {
            return Err(format!(
                "snapshot task {} update timestamp precedes creation timestamp",
                task.info.id
            ));
        }
        let active = matches!(
            task.info.status,
            TaskStatus::Pending | TaskStatus::Processing
        );
        if active && task.info.assigned_client.is_none() {
            return Err(format!(
                "snapshot active task {} has no assigned client",
                task.info.id
            ));
        }
        let (tenant_id, user_key) = TenantId::parse_scoped_key(&task.key).map_err(|error| {
            format!(
                "snapshot task {} has invalid tenant identity: {error}",
                task.info.id
            )
        })?;
        if user_key.contains('\0') {
            return Err(format!(
                "snapshot task {} key contains multiple tenant delimiters",
                task.info.id
            ));
        }
        task.key = tenant_id.make_scoped_key(&user_key);
        let canonical_payload = canonicalize_snapshot_task_payload(task, &task.key)?;
        task.payload = canonical_payload;
        if !restored_task_ids.insert(task.info.id) {
            return Err(format!(
                "snapshot contains duplicate task UUID {}",
                task.info.id
            ));
        }
        if active && !object_indices.contains_key(&task.key) {
            return Err(format!(
                "snapshot active task {} references missing object {:?}",
                task.info.id, task.key
            ));
        }
        if task.max_retry_attempts == 0 {
            task.max_retry_attempts = state.runtime_config.max_task_retry_attempts;
        }
    }
    let task_count_before_drain_abort = tasks.len();
    tasks.retain(|task| !is_active_drain_task(task));
    if tasks.len() != task_count_before_drain_abort {
        tracing::warn!(
            removed = task_count_before_drain_abort - tasks.len(),
            "discarded active Drain tasks whose runtime-only jobs cannot be restored"
        );
    }

    for (key, task) in &restored_replication_tasks {
        let object_index = object_indices.get(key).copied().ok_or_else(|| {
            format!("snapshot replication task references missing object {key:?}")
        })?;
        let object = &objects[object_index].1;
        if task.client_id.is_nil() {
            return Err(format!(
                "snapshot replication task for {key:?} requires a client"
            ));
        }
        match task.kind {
            ReplicationTaskKind::Copy if task.existing_move_target.is_some() => {
                return Err(format!(
                    "snapshot Copy task for {key:?} cannot carry an existing Move target"
                ));
            }
            ReplicationTaskKind::Move
                if !matches!(
                    (task.targets.as_slice(), task.existing_move_target.as_ref()),
                    ([_], None) | ([], Some(_))
                ) =>
            {
                return Err(format!(
                    "snapshot Move task for {key:?} requires one allocated or one existing target"
                ));
            }
            _ => {}
        }
        let same_location = |left: &ReplicaDescriptor, right: &ReplicaDescriptor| {
            left.segment_id == right.segment_id
                && left.offset == right.offset
                && left.size == right.size
                && left.replica_type == right.replica_type
        };
        let source = object
            .replicas
            .iter()
            .find(|replica| same_location(replica, &task.source))
            .ok_or_else(|| {
                format!("snapshot replication task for {key:?} has a missing source replica")
            })?;
        if source.status != ReplicaStatus::Complete
            || !matches!(
                source.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            )
        {
            return Err(format!(
                "snapshot replication task for {key:?} has a non-complete source replica"
            ));
        }
        let mut target_locations = HashSet::with_capacity(task.targets.len());
        for target in &task.targets {
            let target_location = (
                target.segment_id,
                target.offset,
                target.size,
                target.replica_type,
                target.local_disk_storage_id,
                target.local_disk_generation_id,
            );
            if !target_locations.insert(target_location) {
                return Err(format!(
                    "snapshot replication task for {key:?} has a duplicate target replica"
                ));
            }
            let restored_target = object
                .replicas
                .iter()
                .find(|replica| same_location(replica, target))
                .ok_or_else(|| {
                    format!("snapshot replication task for {key:?} has a missing target replica")
                })?;
            if restored_target.status != ReplicaStatus::Allocating {
                return Err(format!(
                    "snapshot replication task for {key:?} has a non-allocating target replica"
                ));
            }
        }
        if let Some(existing_target) = &task.existing_move_target {
            let restored_target = object
                .replicas
                .iter()
                .find(|replica| same_location(replica, existing_target))
                .ok_or_else(|| {
                    format!("snapshot Move task for {key:?} has a missing existing target replica")
                })?;
            if same_location(restored_target, source)
                || restored_target.status != ReplicaStatus::Complete
                || !matches!(
                    restored_target.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                )
            {
                return Err(format!(
                    "snapshot Move task for {key:?} has an invalid existing target replica"
                ));
            }
        }
        if object.replicas.iter().any(|replica| {
            replica.status != ReplicaStatus::Complete
                && !target_locations.contains(&(
                    replica.segment_id,
                    replica.offset,
                    replica.size,
                    replica.replica_type,
                    replica.local_disk_storage_id,
                    replica.local_disk_generation_id,
                ))
        }) {
            return Err(format!(
                "snapshot replication task for {key:?} does not own every non-complete replica"
            ));
        }
        let expected_reservation = task
            .targets
            .iter()
            .filter(|target| target.replica_type == ReplicaType::Memory)
            .try_fold(0u64, |total, target| total.checked_add(target.size))
            .ok_or_else(|| {
                format!("snapshot replication task reservation overflows for {key:?}")
            })?;
        if task.reserved_quota_charge_bytes != expected_reservation {
            return Err(format!(
                "snapshot replication task reservation mismatch for {key:?}: durable={}, expected={expected_reservation}",
                task.reserved_quota_charge_bytes
            ));
        }
    }
    for (key, task) in &restored_replication_tasks {
        let object_index = object_indices[key];
        let object = &mut objects[object_index].1;
        let source = object
            .replicas
            .iter_mut()
            .find(|replica| {
                replica.segment_id == task.source.segment_id
                    && replica.offset == task.source.offset
                    && replica.replica_type == task.source.replica_type
            })
            .expect("replication source was validated immediately above");
        source.inc_refcnt();
    }
    let restored_replication_tasks = restored_replication_tasks
        .into_iter()
        .map(|(key, task)| {
            let object_index = object_indices.get(&key).copied().ok_or_else(|| {
                format!("snapshot replication task lost validated object {key:?}")
            })?;
            let tenant_id = objects[object_index].1.tenant_id.clone();
            Ok((key, task, tenant_id))
        })
        .collect::<Result<Vec<_>, String>>()?;

    // Build the replacement quota ledger before touching live state. Snapshot
    // charges are attacker-controlled durable input; saturating an aggregate
    // overflow would silently under-specify the restored ledger and would
    // also make older-candidate fallback impossible after replacement began.
    let mut restored_tenant_quotas = state.tenant_quotas.read().clone();
    restored_tenant_quotas.reset_usage();
    for (_, object) in &mut objects {
        if object.quota_committed {
            let charge = checked_durable_committed_memory_quota_charge(object).map_err(|_| {
                format!(
                    "snapshot committed quota charge overflows for tenant {} object {:?}",
                    object.tenant_id, object.user_key
                )
            })?;
            object.reserved_quota_charge_bytes = 0;
            object.committed_quota_charge_bytes = charge;
            object.pending_replaced_quota_charge_bytes = 0;
            if state.runtime_config.enable_tenant_quota {
                restored_tenant_quotas
                    .restore_object_checked(&object.tenant_id, charge)
                    .map_err(|_| {
                        format!(
                            "snapshot committed quota ledger overflows for tenant {}",
                            object.tenant_id
                        )
                    })?;
            }
        } else {
            let charge = checked_allocating_memory_quota_charge(object).map_err(|_| {
                format!(
                    "snapshot allocating quota charge overflows for tenant {} object {:?}",
                    object.tenant_id, object.user_key
                )
            })?;
            object.reserved_quota_charge_bytes = charge;
            object.committed_quota_charge_bytes = 0;
            if state.runtime_config.enable_tenant_quota {
                restored_tenant_quotas
                    .restore_replacement_checked(
                        &object.tenant_id,
                        charge,
                        object.pending_replaced_quota_charge_bytes,
                    )
                    .map_err(|_| {
                        format!(
                            "snapshot replacement quota ledger overflows for tenant {}",
                            object.tenant_id
                        )
                    })?;
            }
        }
    }
    if state.runtime_config.enable_tenant_quota {
        for (_, task, tenant_id) in &restored_replication_tasks {
            restored_tenant_quotas
                .restore_reservation_checked(tenant_id, task.reserved_quota_charge_bytes)
                .map_err(|_| {
                    format!("snapshot task quota ledger overflows for tenant {tenant_id}")
                })?;
        }
    }

    let restore_epoch_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes Unix epoch: {error}"))
        .and_then(|duration| {
            u64::try_from(duration.as_millis())
                .map_err(|_| "current epoch milliseconds exceed u64".to_string())
        })?;
    if restore_epoch_ms == 0 {
        return Err("current epoch milliseconds must be non-zero".into());
    }
    let release_delay_ms =
        u64::try_from(state.runtime_config.put_start_release_timeout.as_millis())
            .map_err(|_| "delayed release timeout exceeds u64 milliseconds".to_string())?;
    let orphan_release_deadline = restore_epoch_ms
        .checked_add(release_delay_ms)
        .ok_or_else(|| "orphan delayed release deadline overflows u64".to_string())?;
    for (scoped_key, replicas) in orphaned_staged_releases {
        delayed_replica_releases.push(state::DelayedReplicaReleaseEntry {
            id: Uuid::new_v4(),
            scoped_key,
            deadline_epoch_ms: orphan_release_deadline,
            replicas,
        });
    }
    let mut restored_graceful_unmounts = HashMap::new();
    for pending in graceful_unmounts {
        if pending.segment_id.is_nil()
            || pending.client_id.is_nil()
            || pending.deadline_epoch_ms == 0
        {
            return Err("snapshot graceful unmount identity and deadline must be non-zero".into());
        }
        let segment = segments
            .iter()
            .find(|segment| segment.segment.id == pending.segment_id)
            .ok_or_else(|| {
                format!(
                    "snapshot graceful unmount references missing segment {}",
                    pending.segment_id
                )
            })?;
        if segment.client_id != pending.client_id {
            return Err(format!(
                "snapshot graceful unmount owner mismatch for segment {}",
                pending.segment_id
            ));
        }
        if segment.status != proto::SegmentStatus::GracefullyUnmounting {
            return Err(format!(
                "snapshot graceful unmount references segment {} with status {:?}",
                pending.segment_id, segment.status
            ));
        }
        if restored_graceful_unmounts
            .insert(pending.segment_id, pending.clone())
            .is_some()
        {
            return Err(format!(
                "snapshot contains duplicate graceful unmount for segment {}",
                pending.segment_id
            ));
        }
    }
    // v1-v3 native snapshots and older catalog snapshots persisted status=4
    // but had no deadline sidecar. Preserve the intent and execute it as
    // already expired instead of leaving an unusable segment stuck forever.
    for segment in &segments {
        if segment.status == proto::SegmentStatus::GracefullyUnmounting {
            restored_graceful_unmounts
                .entry(segment.segment.id)
                .or_insert_with(|| GracefulUnmountSnapshotEntry {
                    segment_id: segment.segment.id,
                    client_id: segment.client_id,
                    deadline_epoch_ms: restore_epoch_ms,
                });
        }
    }

    let mut memory_replicas: HashMap<Uuid, Vec<ReplicaDescriptor>> = HashMap::new();
    let mut nof_replicas: HashMap<Uuid, Vec<ReplicaDescriptor>> = HashMap::new();
    for (_, object) in &objects {
        for replica in &object.replicas {
            match replica.replica_type {
                ReplicaType::Memory => memory_replicas
                    .entry(replica.segment_id)
                    .or_default()
                    .push(replica.clone()),
                ReplicaType::NoFSsd => nof_replicas
                    .entry(replica.segment_id)
                    .or_default()
                    .push(replica.clone()),
                ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => {}
            }
        }
    }
    let mut restored_delayed_replica_releases = HashMap::new();
    for mut entry in delayed_replica_releases {
        let (tenant_id, user_key) = TenantId::parse_scoped_key(&entry.scoped_key)
            .map_err(|error| format!("invalid delayed release scoped key: {error}"))?;
        if tenant_id.make_scoped_key(&user_key) != entry.scoped_key {
            return Err("delayed release contains a non-canonical scoped key".into());
        }
        if entry.id.is_nil()
            || entry.deadline_epoch_ms == 0
            || entry.replicas.is_empty()
            || restored_delayed_replica_releases.contains_key(&entry.id)
        {
            return Err("snapshot contains an invalid or duplicate delayed replica release".into());
        }
        for replica in &mut entry.replicas {
            match replica.replica_type {
                ReplicaType::Memory => {
                    replica.handle_valid = false;
                    replica.base_addr = 0;
                    replica.protocol.clear();
                    memory_replicas
                        .entry(replica.segment_id)
                        .or_default()
                        .push(replica.clone());
                }
                ReplicaType::NoFSsd => {
                    replica.handle_valid = false;
                    replica.base_addr = 0;
                    replica.protocol.clear();
                    nof_replicas
                        .entry(replica.segment_id)
                        .or_default()
                        .push(replica.clone());
                }
                ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => {
                    return Err(format!(
                        "delayed release {} contains non-allocator replica type {:?}",
                        entry.id, replica.replica_type
                    ));
                }
            }
        }
        restored_delayed_replica_releases.insert(entry.id, entry);
    }

    if let Some(segment_id) = memory_replicas
        .keys()
        .find(|segment_id| !memory_segment_ids.contains(segment_id))
    {
        return Err(format!(
            "snapshot Memory replica references missing segment {segment_id}"
        ));
    }
    if let Some(segment_id) = nof_replicas
        .keys()
        .find(|segment_id| !nof_segment_ids.contains(segment_id))
    {
        return Err(format!(
            "snapshot NoF replica references missing segment {segment_id}"
        ));
    }

    let mut restored_memory_allocator = SegmentAllocator::new()
        .with_strategy(state.runtime_config.allocation_strategy)
        .with_memory_allocator(state.runtime_config.memory_allocator_kind)
        .try_with_offset_max_allocation_nodes(state.runtime_config.offset_max_allocation_nodes)?
        .with_cxl_capacity(if state.runtime_config.enable_cxl {
            state.runtime_config.cxl_size
        } else {
            0
        });
    let mut restored_nof_allocator = SegmentAllocator::new()
        .with_strategy(
            if state.runtime_config.allocation_strategy == AllocationStrategy::Cxl {
                AllocationStrategy::Random
            } else {
                state.runtime_config.allocation_strategy
            },
        )
        .with_memory_allocator(state.runtime_config.memory_allocator_kind)
        .try_with_offset_max_allocation_nodes(state.runtime_config.offset_max_allocation_nodes)?;
    let mut restored_segments = Vec::with_capacity(segments.len());
    for mut entry in segments {
        let is_cxl = cxl_segment_ids.contains(&entry.segment.id);
        entry.segment.base = 0;
        entry.segment.te_endpoint.clear();
        if !is_cxl {
            entry.segment.protocol.clear();
        }
        if is_cxl {
            restored_memory_allocator.restore_cxl_alias(entry.segment.clone(), entry.client_id)?;
            restored_segments.push(entry);
            continue;
        }
        let rebuilt_used = restored_memory_allocator.restore_segment(
            entry.segment.clone(),
            entry.client_id,
            memory_replicas
                .get(&entry.segment.id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        )?;
        restored_memory_allocator.invalidate_segment_runtime(&entry.segment.id)?;
        if entry.used != rebuilt_used {
            tracing::warn!(
                segment_id = %entry.segment.id,
                snapshot_used = entry.used,
                rebuilt_used,
                "recomputed Memory allocator usage from live replica ranges"
            );
        }
        entry.used = rebuilt_used;
        restored_segments.push(entry);
    }
    let cxl_replicas = cxl_segment_ids
        .iter()
        .flat_map(|segment_id| {
            memory_replicas
                .get(segment_id)
                .into_iter()
                .flat_map(|replicas| replicas.iter().cloned())
        })
        .collect::<Vec<_>>();
    if !cxl_segment_ids.is_empty() {
        restored_memory_allocator.restore_cxl_allocations(&cxl_replicas)?;
    }
    let mut restored_nof_segments = Vec::with_capacity(nof_segments.len());
    for mut entry in nof_segments {
        entry.segment.base = 0;
        entry.segment.te_endpoint.clear();
        let allocator_segment = mooncake_store_core::Segment {
            id: entry.segment.id,
            name: entry.segment.name.clone(),
            base: entry.segment.base,
            size: entry.segment.size,
            te_endpoint: entry.segment.te_endpoint.clone(),
            protocol: String::new(),
            host_id: String::new(),
        };
        let rebuilt_used = restored_nof_allocator.restore_segment(
            allocator_segment,
            entry.segment.client_id,
            nof_replicas
                .get(&entry.segment.id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        )?;
        restored_nof_allocator.invalidate_segment_runtime(&entry.segment.id)?;
        if entry.used != rebuilt_used {
            tracing::warn!(
                segment_id = %entry.segment.id,
                snapshot_used = entry.used,
                rebuilt_used,
                "recomputed NoF allocator usage from live replica ranges"
            );
        }
        entry.used = rebuilt_used;
        restored_nof_segments.push(entry);
    }

    let _snapshot_guard = state.key_mutations.lock_snapshot();
    for object in state.objects.iter() {
        let mut object = object.value().clone();
        account_cache_total_removal(&mut object);
    }
    state.clients.clear();
    state.ok_clients.clear();
    state.objects.clear();
    state.processing_keys.clear();
    state.client_objects.clear();
    state.segments.clear();
    state.delayed_replica_releases.clear();
    state.graceful_unmounts.clear();
    state.nof_segments.clear();
    state.local_disk_segments.clear();
    state.local_disk_client_sessions.clear();
    state.tasks.clear();
    state.replication_tasks.clear();
    state.offloading_tasks.clear();
    state.promotion_tasks.clear();
    state.drain_jobs.clear();
    state.pending_remote_pulls.clear();
    state.nof_heartbeat_states.clear();
    state.clear_transient_promotion_candidates();
    state
        .promotion_in_flight
        .store(0, std::sync::atomic::Ordering::Release);
    *state.tenant_quotas.write() = restored_tenant_quotas;

    *state.allocator.write() = restored_memory_allocator;
    *state.nof_allocator.write() = restored_nof_allocator;
    for entry in restored_segments {
        state.segments.insert(entry.segment.id, entry);
    }
    for (release_id, entry) in restored_delayed_replica_releases {
        state.delayed_replica_releases.insert(release_id, entry);
    }
    for (segment_id, pending) in restored_graceful_unmounts {
        state.graceful_unmounts.insert(segment_id, pending);
    }
    for entry in restored_nof_segments {
        state.nof_segments.insert(entry.segment.id, entry);
    }
    for (key, mut object) in objects {
        // A committed object remains readable while an additional Copy/Move
        // or promotion replica is Allocating. Only an uncommitted Put/Upsert
        // owns the object-level processing gate; durable replication tasks and
        // orphan staged replicas are handled by their own recovery paths.
        let processing = object_has_inflight_write(&object) && !replication_keys.contains(&key);
        sync_cache_total_accounting(&mut object);
        if processing {
            state.processing_keys.insert(key.clone(), ());
        } else if !object.client_id.is_nil() {
            state
                .client_objects
                .entry(object.client_id)
                .or_default()
                .insert(key.clone());
        }
        state.objects.insert(key, object);
    }
    for task in tasks {
        state.tasks.insert(task.info.id, task);
    }
    for (key, task, _) in restored_replication_tasks {
        state.replication_tasks.insert(key, task);
    }
    for local_disk in local_disk_segments {
        let storage_id = if local_disk.storage_id.is_nil() {
            local_disk.client_id
        } else {
            local_disk.storage_id
        };
        state.local_disk_segments.insert(
            storage_id,
            state::LocalDiskSegmentEntry {
                active_client_id: None,
                persisted_client_id: (!local_disk.client_id.is_nil())
                    .then_some(local_disk.client_id),
                recovery_complete: false,
                recovery_session_id: None,
                recovered_objects: HashSet::new(),
                enable_offloading: local_disk.enable_offloading,
                offloading_objects: local_disk.offloading_objects,
                promotion_objects: HashMap::new(),
                // Capacity is a live process-session hint, not durable
                // scheduling authority.
                ssd_total_capacity_bytes: 0,
                ssd_capacity_metric_accounted: false,
            },
        );
    }
    metrics::sync_memory_metrics(state);
    Ok(())
}

/// Abort runtime-only Drain scheduling state when a caught-up standby becomes
/// leader. Durable terminal segment statuses have already arrived through the
/// oplog; only orphaned DRAINING states are normalized.
pub(crate) fn abort_orphaned_drain_segments_after_recovery(state: &MasterState) {
    let _snapshot_guard = state.key_mutations.lock_snapshot();
    let orphaned_task_ids = state
        .tasks
        .iter()
        .filter(|task| is_active_drain_task(task.value()))
        .map(|task| *task.key())
        .collect::<Vec<_>>();
    if !orphaned_task_ids.is_empty()
        && state
            .persist_task_state_batch_or_fence(
                &[],
                &orphaned_task_ids,
                "abort_orphaned_drain_tasks",
            )
            .is_err()
    {
        return;
    }
    let durable_statuses = state
        .segments
        .iter()
        .filter(|segment| segment.status == proto::SegmentStatus::Draining)
        .map(|segment| {
            (
                segment.segment.id,
                false,
                proto::SegmentStatus::Active as i32,
            )
        })
        .chain(
            state
                .nof_segments
                .iter()
                .filter(|segment| segment.status == proto::SegmentStatus::Draining)
                .map(|segment| {
                    (
                        segment.segment.id,
                        true,
                        proto::SegmentStatus::Active as i32,
                    )
                }),
        )
        .collect::<Vec<_>>();
    if !durable_statuses.is_empty()
        && let Err(error) = state
            .oplog_manager
            .record_segment_status_batch_durable(&durable_statuses)
    {
        state.fence_after_durability_failure("abort_orphaned_drain_segments", &error);
        return;
    }
    for task_id in orphaned_task_ids {
        state.tasks.remove(&task_id);
    }
    state.drain_jobs.clear();
    for mut segment in state.segments.iter_mut() {
        if segment.status == proto::SegmentStatus::Draining {
            segment.status = proto::SegmentStatus::Active;
        }
    }
    for mut segment in state.nof_segments.iter_mut() {
        if segment.status == proto::SegmentStatus::Draining {
            segment.status = proto::SegmentStatus::Active;
        }
    }
}

/// 空间不足时返回给客户端的提示信息 / Hint returned to client on insufficient space.
const PUT_NO_SPACE_HELPER_STR: &str = " due to insufficient space. Consider lowering eviction_high_watermark_ratio or mounting more segments.";

/// ReplicaCopy 任务的 payload 结构。
/// Payload for ReplicaCopy tasks — describes a single key copy from source to targets.
#[derive(Serialize)]
struct ReplicaCopyPayload<'a> {
    tenant_id: &'a TenantId,
    key: &'a str,
    source: &'a str,
    targets: &'a [String],
}

/// ReplicaMove 任务的 payload 结构。
/// Payload for ReplicaMove tasks — describes a single key move from source to target.
#[derive(Serialize)]
struct ReplicaMovePayload<'a> {
    tenant_id: &'a TenantId,
    key: &'a str,
    source: &'a str,
    target: &'a str,
}

/// Test-only barrier that makes the tenant-quota policy delete's
/// admission-disabled window observable between policy erasure and the
/// connector save. Production builds compile the helper calls to no-ops.
#[cfg(test)]
pub(crate) struct PolicySaveTestBarrier {
    started: std::sync::Mutex<bool>,
    started_cv: std::sync::Condvar,
    released: std::sync::Mutex<bool>,
    released_cv: std::sync::Condvar,
}

#[cfg(test)]
impl PolicySaveTestBarrier {
    pub(crate) fn new() -> Self {
        Self {
            started: std::sync::Mutex::new(false),
            started_cv: std::sync::Condvar::new(),
            released: std::sync::Mutex::new(false),
            released_cv: std::sync::Condvar::new(),
        }
    }

    pub(crate) fn wait_started(&self) {
        let mut started = self.started.lock().unwrap();
        while !*started {
            started = self.started_cv.wait(started).unwrap();
        }
    }

    pub(crate) fn release_save(&self) {
        let mut released = self.released.lock().unwrap();
        *released = true;
        self.released_cv.notify_all();
    }

    fn signal_started(&self) {
        let mut started = self.started.lock().unwrap();
        *started = true;
        self.started_cv.notify_all();
    }

    fn wait_until_released(&self) {
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.released_cv.wait(released).unwrap();
        }
    }
}

impl MasterServiceImpl {
    pub(crate) fn resolve_write_tenant(&self, raw: &str) -> Result<TenantId, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Ok(TenantId::default());
        }
        let tenant_id = resolve_write_tenant(raw, true)?;
        if !self.state.tenant_quotas.read().is_registered(&tenant_id) {
            return Err(Status::resource_exhausted("tenant not registered"));
        }
        Ok(tenant_id)
    }

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
            TenantQuotaError::TenantNotFound => Status::not_found("tenant not found"),
            TenantQuotaError::InvalidArgument => Status::invalid_argument("invalid tenant quota"),
            TenantQuotaError::AccountingMismatch => {
                Status::failed_precondition("tenant quota accounting mismatch")
            }
            TenantQuotaError::TenantNotEmpty => Status::failed_precondition("tenant not empty"),
        }
    }

    fn tenant_quota_mutation_status(&self, operation: &str, error: TenantQuotaError) -> Status {
        if error == TenantQuotaError::AccountingMismatch {
            self.state.fence_after_invariant_failure(
                operation,
                "tenant quota ledger does not match authoritative object state",
            );
            Status::unavailable("tenant quota accounting invariant failed during object mutation")
        } else {
            Self::tenant_quota_status(error)
        }
    }

    fn is_tenant_quota_exceeded_status(status: &Status) -> bool {
        status.code() == tonic::Code::ResourceExhausted
            && status.message() == "tenant quota exceeded"
    }

    pub(crate) fn reserve_tenant_quota(
        &self,
        tenant_id: &TenantId,
        bytes: u64,
    ) -> Result<(), Status> {
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

    fn evict_tenant_quota_deficit(
        &self,
        tenant_id: &TenantId,
        incoming_bytes: u64,
        protected_key: Option<&str>,
    ) -> Result<(), Status> {
        let capacity = self.tenant_quota_capacity_bytes();
        let deficit = {
            let mut quotas = self.state.tenant_quotas.write();
            quotas.recompute_effective_quotas(capacity);
            quotas.compute_deficit(tenant_id, incoming_bytes)
        };
        run_tenant_quota_eviction(&self.state, tenant_id, protected_key, deficit).map_err(
            |error| Status::unavailable(format!("tenant quota eviction failed: {error}")),
        )?;
        if self.state.service_fenced.load(Ordering::Acquire) {
            return Err(Status::unavailable(
                "master fenced during tenant quota eviction",
            ));
        }
        Ok(())
    }

    /// Reserve quota with the C++ PutStart/UpsertStart admission policy.
    ///
    /// The caller must not hold a key-mutation guard: eviction acquires the
    /// selected tenant keys through the same striped coordinator.
    pub(crate) fn reserve_tenant_quota_with_eviction(
        &self,
        tenant_id: &TenantId,
        bytes: u64,
        protected_key: Option<&str>,
    ) -> Result<(), Status> {
        const MAX_TENANT_QUOTA_EVICTION_RETRIES: usize = 2;
        for attempt in 0..=MAX_TENANT_QUOTA_EVICTION_RETRIES {
            match self.reserve_tenant_quota(tenant_id, bytes) {
                Ok(()) => return Ok(()),
                Err(status)
                    if Self::is_tenant_quota_exceeded_status(&status)
                        && attempt < MAX_TENANT_QUOTA_EVICTION_RETRIES =>
                {
                    self.evict_tenant_quota_deficit(tenant_id, bytes, protected_key)?;
                }
                Err(status) => return Err(status),
            }
        }
        unreachable!("bounded tenant quota admission loop always returns")
    }

    pub(crate) fn settle_tenant_quota(
        &self,
        tenant_id: &TenantId,
        reserved_bytes: u64,
        committed_bytes: u64,
        register_committed_charge: bool,
        pending_replaced_bytes: u64,
    ) -> Result<(), Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Ok(());
        }
        let mut quotas = self.state.tenant_quotas.write();
        let mut projected = quotas.clone();
        projected
            .settle(
                tenant_id,
                reserved_bytes,
                committed_bytes,
                register_committed_charge,
            )
            .map_err(|error| self.tenant_quota_mutation_status("settle_tenant_quota", error))?;
        if pending_replaced_bytes != 0 {
            projected
                .release(tenant_id, pending_replaced_bytes)
                .map_err(|error| {
                    self.tenant_quota_mutation_status("settle_tenant_quota_replaced_release", error)
                })?;
        }
        *quotas = projected;
        Ok(())
    }

    pub(crate) fn register_tenant_metadata_object(&self, tenant_id: &TenantId) {
        if self.state.runtime_config.enable_tenant_quota {
            self.state.tenant_quotas.write().register_object(tenant_id);
        }
    }

    pub(crate) fn abort_tenant_quota(
        &self,
        tenant_id: &TenantId,
        bytes: u64,
    ) -> Result<(), Status> {
        if self.state.runtime_config.enable_tenant_quota
            && let Err(error) = self.state.tenant_quotas.write().abort(tenant_id, bytes)
        {
            self.state.fence_after_invariant_failure(
                "abort_tenant_quota",
                &format!("tenant={tenant_id} bytes={bytes} error={error:?}"),
            );
            return Err(Status::unavailable(
                "tenant quota accounting invariant failed while aborting reservation",
            ));
        }
        Ok(())
    }

    pub(crate) fn account_removed_object_quota(&self, object: &ObjectEntry) -> Result<(), Status> {
        account_removed_object_quota(&self.state, object).map_err(|_| {
            Status::unavailable("tenant quota accounting invariant failed while removing object")
        })
    }

    pub fn set_service_available(&self, available: bool) {
        if available && self.state.service_fenced.load(Ordering::Acquire) {
            tracing::error!(
                "refusing to reopen master service after an ambiguous durability failure"
            );
        }
        let available = available && !self.state.service_fenced.load(Ordering::Acquire);
        {
            // Close the atomic gate before waiting for in-flight workers. A
            // worker that raced with this store rechecks availability after
            // acquiring its read guard and therefore cannot mutate standby
            // state after this exclusive drain completes.
            if !available {
                self.state.service_available.store(false, Ordering::Release);
                self.state.drain_foreground_requests();
            }
            let _background_mutation_guard = self.state.background_mutation_gate.write();
            // Serialize the serving gate transition with due graceful
            // scheduler and foreground mutation epochs as well.
            let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
            self.state
                .service_available
                .store(available, Ordering::Release);
        }
        self.graceful_unmount_scheduler
            .sync_from_state(&self.state, available);
    }

    pub fn is_service_available(&self) -> bool {
        self.state.service_available.load(Ordering::Acquire)
    }

    pub(crate) fn begin_external_mutation(&self) -> Option<parking_lot::RwLockReadGuard<'_, ()>> {
        self.state.begin_background_mutation()
    }

    pub(crate) fn begin_foreground_request(&self) -> Result<state::ForegroundRequestGuard, Status> {
        self.state
            .begin_foreground_request()
            .ok_or_else(|| Status::unavailable("master service is not serving this request"))
    }

    pub fn is_service_fenced(&self) -> bool {
        self.state.service_fenced.load(Ordering::Acquire)
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
        // C++ GetTenantQuotaSnapshot always succeeds and returns the table's
        // optional row; with multi-tenancy disabled the table is empty, so a
        // snapshot query reports "no snapshot" rather than an error.
        if !self.state.runtime_config.enable_tenant_quota {
            return Ok(None);
        }
        let tenant_id = resolve_request_tenant(tenant_id, true)?;
        self.get_tenant_quota_snapshot_for_tenant(&tenant_id)
    }

    pub(crate) fn get_tenant_quota_snapshot_for_tenant(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantQuotaSnapshot>, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Ok(None);
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
        if tenant_id.is_empty() {
            return Err(Status::invalid_argument("invalid tenant id"));
        }
        let tenant_id = resolve_request_tenant(tenant_id, true)?;
        self.upsert_tenant_quota_policy_for_tenant(&tenant_id, requested_quota_bytes)
    }

    /// Test-only accessor delegating to the production LocalDisk usage
    /// derivation used by SSD metrics.
    #[doc(hidden)]
    pub fn local_ssd_usage_metrics_for_test(&self) -> std::collections::HashMap<Uuid, u64> {
        helpers::local_ssd_usage_metrics(&self.state)
            .into_iter()
            .map(|(client_id, metrics)| (client_id, metrics.used_bytes))
            .collect()
    }

    pub(crate) fn upsert_tenant_quota_policy_for_tenant(
        &self,
        tenant_id: &TenantId,
        requested_quota_bytes: u64,
    ) -> Result<TenantQuotaSnapshot, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Err(Status::failed_precondition("tenant quota is disabled"));
        }
        let Some(_background_mutation_guard) = self.state.begin_background_mutation() else {
            return Err(Status::unavailable(
                "master service is not serving tenant quota mutations",
            ));
        };
        let _policy_mutation_guard = self.state.tenant_quota_policy_mutations.lock();
        let capacity = self.tenant_quota_capacity_bytes();
        let mut next = self.state.tenant_quotas.read().clone();
        next.upsert_policy(tenant_id, requested_quota_bytes, capacity)
            .map_err(Self::tenant_quota_status)?;
        next.recompute_effective_quotas(capacity);
        let policy_snapshot = self.tenant_quota_policy_snapshot_from_table(&next)?;
        self.save_tenant_quota_policy_snapshot(&policy_snapshot)?;
        let mut quotas = self.state.tenant_quotas.write();
        quotas
            .upsert_policy(tenant_id, requested_quota_bytes, capacity)
            .map_err(Self::tenant_quota_status)?;
        quotas.recompute_effective_quotas(capacity);
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
        if tenant_id.is_empty() {
            return Err(Status::invalid_argument("invalid tenant id"));
        }
        let tenant_id = resolve_request_tenant(tenant_id, true)?;
        self.delete_tenant_quota_policy_for_tenant(&tenant_id)
    }

    pub(crate) fn delete_tenant_quota_policy_for_tenant(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantQuotaSnapshot>, Status> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Err(Status::failed_precondition("tenant quota is disabled"));
        }
        let Some(_background_mutation_guard) = self.state.begin_background_mutation() else {
            return Err(Status::unavailable(
                "master service is not serving tenant quota mutations",
            ));
        };
        let _policy_mutation_guard = self.state.tenant_quota_policy_mutations.lock();
        let capacity = self.tenant_quota_capacity_bytes();
        // C++ deletion semantics: the tenant's explicit policy is removed from
        // the live admission table BEFORE the connector save completes, so
        // concurrent reservations observe TENANT_NOT_REGISTERED while
        // persistence is still in flight. A failed save rolls the policy back.
        let (deleted, previous_requested) = {
            let mut quotas = self.state.tenant_quotas.write();
            let previous_requested = quotas
                .list_snapshots()
                .into_iter()
                .find(|snapshot| snapshot.tenant_id == *tenant_id)
                .map(|snapshot| snapshot.requested_quota_bytes);
            let deleted = quotas
                .erase_policy(tenant_id, capacity)
                .map_err(Self::tenant_quota_status)?;
            (deleted, previous_requested)
        };
        let policy_snapshot = {
            let quotas = self.state.tenant_quotas.read();
            self.tenant_quota_policy_snapshot_from_table(&quotas)?
        };
        #[cfg(test)]
        if let Some(barrier) = self.policy_save_test_barrier.lock().take() {
            barrier.signal_started();
            barrier.wait_until_released();
        }
        let save_result = self.save_tenant_quota_policy_snapshot(&policy_snapshot);
        if let Err(status) = save_result {
            if let Some(requested) = previous_requested {
                let mut quotas = self.state.tenant_quotas.write();
                let _ = quotas.upsert_policy(tenant_id, requested, capacity);
                quotas.recompute_effective_quotas(capacity);
            }
            return Err(status);
        }
        Ok(deleted)
    }

    #[cfg(test)]
    pub(crate) fn install_policy_save_barrier(&self) -> std::sync::Arc<PolicySaveTestBarrier> {
        let barrier = std::sync::Arc::new(PolicySaveTestBarrier::new());
        *self.policy_save_test_barrier.lock() = Some(barrier.clone());
        barrier
    }

    fn tenant_quota_policy_snapshot_from_table(
        &self,
        quotas: &TenantQuotaTable,
    ) -> Result<TenantQuotaPolicySnapshot, Status> {
        let producer_view_version = self.state.leadership_view_version.load(Ordering::Acquire);
        Ok(TenantQuotaPolicySnapshot {
            producer_view_version,
            tenant_quotas: quotas
                .list_snapshots()
                .into_iter()
                .filter(|s| s.has_explicit_policy)
                .map(|s| (s.tenant_id.as_str().to_owned(), s.requested_quota_bytes))
                .collect(),
        })
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
        .map_err(|error| {
            let durability_error = crate::ha::HaError::Snapshot(format!(
                "tenant quota policy persistence failed: {error}"
            ));
            self.state
                .fence_after_durability_failure("save_tenant_quota_policy", &durability_error);
            Status::unavailable(durability_error.to_string())
        })
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
        Self::try_new_with_runtime_config(backend_type, backup_dir, runtime_config)
            .unwrap_or_else(|error| panic!("failed to initialize master service: {error}"))
    }

    /// Production constructor for a standalone service. Unlike the historical
    /// convenience constructor, a configured native snapshot backend is
    /// fail-closed: an unreadable, unsupported, or logically invalid snapshot
    /// prevents a service instance from being returned.
    pub fn try_new_with_runtime_config(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
    ) -> Result<Self, HaError> {
        Self::try_new_with_runtime_config_and_oplog(backend_type, backup_dir, runtime_config, None)
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
        oplog_manager: Option<crate::oplog::OpLogManager>,
    ) -> Self {
        Self::try_new_with_runtime_config_and_oplog(
            backend_type,
            backup_dir,
            runtime_config,
            oplog_manager,
        )
        .unwrap_or_else(|error| panic!("failed to initialize master service: {error}"))
    }

    /// Fallible full constructor used by production startup paths that must
    /// never turn snapshot corruption into an empty, writable Master.
    pub fn try_new_with_runtime_config_and_oplog(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
        oplog_manager: Option<crate::oplog::OpLogManager>,
    ) -> Result<Self, HaError> {
        Self::try_new_with_runtime_config_and_oplog_with_availability(
            backend_type,
            backup_dir,
            runtime_config,
            oplog_manager,
            true,
        )
    }

    /// Construct an HA standby with the serving gate closed before any restored
    /// deadline can be scheduled by a background worker.
    pub(crate) fn new_standby_with_runtime_config(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
    ) -> Self {
        Self::try_new_standby_with_runtime_config(backend_type, backup_dir, runtime_config)
            .unwrap_or_else(|error| panic!("failed to initialize standby master service: {error}"))
    }

    /// Fallible HA standby constructor. The serving gate starts closed and a
    /// native snapshot error aborts this candidacy before a supervisor or gRPC
    /// server can be created.
    pub(crate) fn try_new_standby_with_runtime_config(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
    ) -> Result<Self, HaError> {
        Self::try_new_with_runtime_config_and_oplog_with_availability(
            backend_type,
            backup_dir,
            runtime_config,
            None,
            false,
        )
    }

    fn try_new_with_runtime_config_and_oplog_with_availability(
        backend_type: Option<StorageBackendType>,
        backup_dir: Option<PathBuf>,
        runtime_config: MasterRuntimeConfig,
        oplog_manager: Option<crate::oplog::OpLogManager>,
        initial_service_available: bool,
    ) -> Result<Self, HaError> {
        let oplog_manager =
            Arc::new(oplog_manager.unwrap_or_else(|| crate::oplog::OpLogManager::new(None, 0)));
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
                let typed_tenant_id = TenantId::new(tenant_id.clone())
                    .unwrap_or_else(|e| panic!("invalid tenant quota policy for {tenant_id}: {e}"));
                tenant_quotas
                    .upsert_policy(
                        &typed_tenant_id,
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
        let memory_allocator = SegmentAllocator::new()
            .with_strategy(runtime_config.allocation_strategy)
            .with_memory_allocator(runtime_config.memory_allocator_kind)
            .try_with_offset_max_allocation_nodes(runtime_config.offset_max_allocation_nodes)
            .map_err(HaError::InvalidParams)?
            .with_cxl_capacity(if runtime_config.enable_cxl {
                runtime_config.cxl_size
            } else {
                0
            });
        let nof_allocator = SegmentAllocator::new()
            .with_strategy(
                if runtime_config.allocation_strategy == AllocationStrategy::Cxl {
                    AllocationStrategy::Random
                } else {
                    runtime_config.allocation_strategy
                },
            )
            .with_memory_allocator(runtime_config.memory_allocator_kind)
            .try_with_offset_max_allocation_nodes(runtime_config.offset_max_allocation_nodes)
            .map_err(HaError::InvalidParams)?;
        let state = Arc::new(MasterState {
            // ── 客户端注册 / client registry ──
            // client_id → ClientEntry：客户端信息、地址、心跳时间
            clients: DashMap::new(),
            // ok_clients 集合：心跳正常的客户端子集，仅本表中有记录的客户端被视为 alive
            ok_clients: DashMap::new(),

            // ── 对象存储 / object store ──
            // key → ObjectEntry：对象的全部元数据（副本列表、大小、last_access、pin 状态等）
            objects: DashMap::new(),
            key_mutations: state::KeyMutationCoordinator::default(),
            // 正在 PutStart 中的 key 集合：防止同一 key 的并发 PutStart 冲突
            processing_keys: DashMap::new(),
            // client_id → Set<key>：每个客户端拥有的对象索引，加速客户端下线时批量清理
            client_objects: DashMap::new(),

            // ── Segment 管理 / segment management ──
            // segment_id → SegmentEntry：已挂载的 Memory segment（物理内存段）
            segments: DashMap::new(),
            delayed_replica_releases: DashMap::new(),
            // segment_id → persisted epoch deadline/owner for delayed unmount.
            graceful_unmounts: DashMap::new(),
            // segment_id → NoFSegmentEntry：已挂载的 NoF (NVMe-oF) segment
            nof_segments: DashMap::new(),
            // storage_id → LocalDiskSegmentEntry：持久磁盘命名空间及当前进程会话
            local_disk_segments: DashMap::new(),
            // ephemeral client_id → durable storage_id reverse session index
            local_disk_client_sessions: DashMap::new(),

            // ── 任务队列 / task queues ──
            // task_id → TaskEntry：Copy/Move 异步任务（创建 → 分配 worker → 完成）
            tasks: DashMap::new(),
            task_clock: state::TaskLifecycleClock::default(),
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
            allocator: RwLock::new(memory_allocator),
            // NoF segment 的段内空间分配器
            nof_allocator: RwLock::new(nof_allocator),
            nof_eviction_requested: AtomicBool::new(false),

            // ── 持久化 / persistence ──
            // 快照后端的抽象接口（local-disk / hf3fs），用于 HA 状态备份与恢复
            storage_backend,
            // RPC handlers and background workers append to the same ordered log.
            oplog_manager: Arc::clone(&oplog_manager),

            // ── 全局计数器 / global counters ──
            // 全局在途 promotion 计数：CAS 式限流，防止并发 promotion 打爆内存
            promotion_in_flight: AtomicUsize::new(0),
            promotion_candidate_count: AtomicUsize::new(0),
            promotion_retry_cursor: AtomicUsize::new(0),
            // 全局视图版本号：段拓扑变更时 +1，Client Ping 时返回，Client 感知版本变化后重新 discover
            view_version: AtomicI64::new(0),
            // Election-issued term used for cross-process connector fencing.
            leadership_view_version: std::sync::atomic::AtomicU64::new(0),

            // ── 运行时 / runtime ──
            // 运行时配置：lease TTL、淘汰水位线、promotion 参数等（只读，无需锁）
            runtime_config: runtime_config.clone(),
            // Keep the serving/scheduler gate closed until native snapshot
            // loading and logical restoration have both succeeded.
            service_available: AtomicBool::new(false),
            foreground_request_gate: Arc::new(state::ForegroundRequestGate::new()),
            background_mutation_gate: RwLock::new(()),
            service_fenced: AtomicBool::new(false),
            // 租户配额表：per-tenant 存储配额分配与追踪
            tenant_quotas: RwLock::new(tenant_quotas),
            tenant_quota_policy_mutations: parking_lot::Mutex::new(()),

            // ── 远端回源 / remote pull ──
            // key → PendingRemotePullEntry：缓存未命中时从 S3 等远端回拉数据的追踪状态
            pending_remote_pulls: DashMap::new(),
            // segment_id → NoFHeartbeatState：NoF segment 的心跳探测状态（下次探测时间、连续失败次数）
            nof_heartbeat_states: DashMap::new(),
            // KV event publisher shared with eviction/offload background paths.
            kv_event_publisher: Arc::clone(&kv_event_publisher),
        });
        let metadata_state = MetadataState::from_environment("");
        // 创建各后台 worker，各自持有 state 的 Arc 克隆
        let graceful_unmount_scheduler = GracefulUnmountScheduler::new(state.clone());
        let processing_reaper = ProcessingReaper::new(state.clone());
        let eviction_worker = EvictionWorker::new(state.clone());
        let client_monitor_worker = ClientMonitorWorker::new(state.clone(), metadata_state.clone());
        let drain_worker = DrainWorker::new(state.clone());
        let nof_heartbeat_worker =
            NofHeartbeatWorker::new(state.clone(), Box::new(probe_nof_endpoint));

        let mut snapshot_restore_error = None;

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
            match backend.load_with_runtime_state() {
                Ok(Some((
                    segments,
                    nof_segments,
                    objects,
                    tasks,
                    replication_tasks,
                    graceful_unmounts,
                    delayed_replica_releases,
                    local_disk_segments,
                    allocator_config,
                ))) => {
                    match restore_loaded_snapshot_state(
                        &state,
                        segments,
                        nof_segments,
                        objects,
                        tasks,
                        replication_tasks,
                        local_disk_segments,
                        graceful_unmounts,
                        delayed_replica_releases,
                        allocator_config,
                    ) {
                        Ok(()) => tracing::info!("Restored state from snapshot"),
                        Err(error) => {
                            tracing::error!(
                                %error,
                                "Rejected invalid snapshot while rebuilding allocator state"
                            );
                            snapshot_restore_error = Some(HaError::Snapshot(format!(
                                "native snapshot restore rejected: {error}"
                            )));
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(%error, "Failed to load master snapshot");
                    snapshot_restore_error = Some(HaError::Snapshot(format!(
                        "failed to load native master snapshot: {error}"
                    )));
                }
            }
        }

        if snapshot_restore_error.is_none() {
            state
                .service_available
                .store(initial_service_available, Ordering::Release);
        }
        graceful_unmount_scheduler
            .sync_from_state(&state, state.service_available.load(Ordering::Acquire));

        let service = Self {
            state,
            snapshot_save_in_flight: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            policy_save_test_barrier: parking_lot::Mutex::new(None),
            metadata_state,
            graceful_unmount_scheduler,
            processing_reaper,
            eviction_worker,
            client_monitor_worker,
            drain_worker,
            nof_heartbeat_worker,
            oplog_manager,
            kv_event_publisher,
        };
        if let Some(error) = snapshot_restore_error {
            // Dropping the fully assembled service stops every worker. No
            // caller can accidentally publish the empty/partially restored
            // state behind a gRPC server.
            return Err(error);
        }
        Ok(service)
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
    pub fn preflight_snapshot_writer(&self) -> Result<bool, HaError> {
        let guard = self.state.storage_backend.read();
        let Some(backend) = guard.as_ref() else {
            return Ok(false);
        };
        backend
            .preflight_snapshot_writer()
            .map_err(|error| HaError::Snapshot(error.to_string()))?;
        Ok(true)
    }

    pub fn save_snapshot(&self) {
        if !self.is_service_available() {
            tracing::debug!("Skipping snapshot save because the master is not serving");
            return;
        }
        if self.is_service_fenced() {
            tracing::error!("Skipping snapshot save because the master is durability-fenced");
            return;
        }
        if self
            .snapshot_save_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tracing::debug!("Skipping snapshot save because a previous writer is still running");
            return;
        }

        let state = self.state.clone();
        let in_flight = self.snapshot_save_in_flight.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = cancelled.clone();
        let timeout = state.runtime_config.snapshot_child_timeout;
        let retention_count = state.runtime_config.snapshot_retention_count;
        tokio::spawn(async move {
            let worker_in_flight = in_flight.clone();
            let save_task = tokio::task::spawn_blocking(
                move || -> Result<SnapshotSaveOutcome, String> {
                    let _in_flight_guard = SnapshotSaveInFlightGuard {
                        flag: worker_in_flight,
                    };
                    if worker_cancelled.load(Ordering::Acquire) {
                        return Ok(SnapshotSaveOutcome::Cancelled);
                    }
                    let start = std::time::Instant::now();

                    let guard = state.storage_backend.read();
                    let Some(backend) = guard.as_ref() else {
                        return Ok(SnapshotSaveOutcome::BackendDisabled);
                    };

                    // Hold the global mutation barrier only while cloning live
                    // metadata. Encoding, fsync, retention copies and pruning
                    // operate on the owned DTO after this lexical scope exits.
                    let capture_attempt = {
                        let _snapshot_guard = state.key_mutations.lock_snapshot();
                        if !state.service_available.load(Ordering::Acquire) {
                            worker_cancelled.store(true, Ordering::Release);
                        }
                        if state.service_fenced.load(Ordering::Acquire) {
                            worker_cancelled.store(true, Ordering::Release);
                        }
                        let last_included_seq = state.oplog_manager.latest_sequence();
                        let mut local_disk_segments =
                            Vec::with_capacity(state.local_disk_segments.len());
                        let mut local_capture_cancelled = worker_cancelled.load(Ordering::Acquire);
                        if !local_capture_cancelled {
                            for entry in state.local_disk_segments.iter() {
                                if worker_cancelled.load(Ordering::Acquire) {
                                    local_capture_cancelled = true;
                                    break;
                                }
                                let storage_id = *entry.key();
                                let mut offloading_objects =
                                    HashMap::with_capacity(entry.offloading_objects.len());
                                for (key, size) in &entry.offloading_objects {
                                    if worker_cancelled.load(Ordering::Acquire) {
                                        local_capture_cancelled = true;
                                        break;
                                    }
                                    offloading_objects.insert(key.clone(), *size);
                                }
                                local_disk_segments.push(LocalDiskSnapshotEntry {
                                    storage_id,
                                    client_id: entry
                                        .active_client_id
                                        .or(entry.persisted_client_id)
                                        .unwrap_or_else(Uuid::nil),
                                    enable_offloading: entry.enable_offloading,
                                    offloading_objects,
                                    ssd_total_capacity_bytes: 0,
                                });
                                if local_capture_cancelled {
                                    break;
                                }
                            }
                        }
                        if local_capture_cancelled {
                            worker_cancelled.store(true, Ordering::Release);
                        }

                        let capture_attempt = StorageBackend::capture_runtime_state_cancellable(
                            &state.segments,
                            &state.nof_segments,
                            &state.objects,
                            &state.tasks,
                            &state.replication_tasks,
                            &state.graceful_unmounts,
                            &state.delayed_replica_releases,
                            local_disk_segments,
                            Some(AllocatorSnapshotConfig {
                                allocation_strategy: state.runtime_config.allocation_strategy,
                                memory_allocator_kind: state.runtime_config.memory_allocator_kind,
                                offset_max_allocation_nodes: state
                                    .runtime_config
                                    .offset_max_allocation_nodes,
                            }),
                            last_included_seq,
                            &worker_cancelled,
                        );
                        let sequence_after_capture = state.oplog_manager.latest_sequence();
                        if sequence_after_capture != last_included_seq {
                            tracing::error!(
                                last_included_seq,
                                sequence_after_capture,
                                "cancelled native snapshot because oplog advanced inside the mutation barrier"
                            );
                            worker_cancelled.store(true, Ordering::Release);
                        }
                        capture_attempt
                    };
                    // Conversion (and, on cancellation, destruction) of the
                    // owned DTO occurs after `_snapshot_guard` has dropped.
                    let Some(captured) = capture_attempt.into_snapshot() else {
                        return Ok(SnapshotSaveOutcome::Cancelled);
                    };

                    if worker_cancelled.load(Ordering::Acquire) {
                        return Ok(SnapshotSaveOutcome::Cancelled);
                    }
                    if !state.service_available.load(Ordering::Acquire) {
                        return Ok(SnapshotSaveOutcome::Cancelled);
                    }
                    if state.service_fenced.load(Ordering::Acquire) {
                        return Ok(SnapshotSaveOutcome::Cancelled);
                    }

                    backend
                        .save_captured_snapshot(captured)
                        .map_err(|error| error.to_string())?;
                    backend
                        .retain_latest_snapshot(retention_count)
                        .map_err(|error| error.to_string())?;
                    Ok(SnapshotSaveOutcome::Saved(start.elapsed()))
                },
            );

            match tokio::time::timeout(timeout, save_task).await {
                Ok(Ok(Ok(SnapshotSaveOutcome::Saved(elapsed)))) => {
                    metrics::SNAPSHOT_DURATION_MS.set(elapsed.as_millis() as i64);
                    metrics::SNAPSHOT_SUCCESS_COUNT.inc();
                }
                Ok(Ok(Ok(SnapshotSaveOutcome::BackendDisabled))) => {}
                Ok(Ok(Ok(SnapshotSaveOutcome::Cancelled))) => {
                    metrics::SNAPSHOT_FAIL_COUNT.inc();
                    tracing::error!("Snapshot save was cancelled");
                }
                Ok(Ok(Err(error))) => {
                    metrics::SNAPSHOT_FAIL_COUNT.inc();
                    tracing::error!("Failed to save snapshot: {}", error);
                }
                Ok(Err(error)) => {
                    // A panic unwinds through SnapshotSaveInFlightGuard. Store
                    // defensively as well in case Tokio rejected the task before
                    // the closure began.
                    in_flight.store(false, Ordering::Release);
                    metrics::SNAPSHOT_FAIL_COUNT.inc();
                    tracing::error!("Snapshot save task failed to join: {}", error);
                }
                Err(_) => {
                    // spawn_blocking cannot be force-aborted. Signal cooperative
                    // cancellation so a capture still holding the global barrier
                    // exits between records. The in-flight flag intentionally
                    // remains set until the detached writer itself finishes.
                    cancelled.store(true, Ordering::Release);
                    metrics::SNAPSHOT_FAIL_COUNT.inc();
                    tracing::error!("Snapshot save timed out after {:?}", timeout);
                }
            }
        });
    }

    pub fn capture_loaded_snapshot(&self, snapshot_id: impl Into<String>) -> LoadedSnapshot {
        let _snapshot_guard = self.state.key_mutations.lock_snapshot();
        let snapshot_sequence_id = self.oplog_manager.latest_sequence();
        LoadedSnapshot {
            snapshot_id: snapshot_id.into(),
            snapshot_sequence_id,
            allocator_config: Some(AllocatorSnapshotConfig {
                allocation_strategy: self.state.runtime_config.allocation_strategy,
                memory_allocator_kind: self.state.runtime_config.memory_allocator_kind,
                offset_max_allocation_nodes: self.state.runtime_config.offset_max_allocation_nodes,
            }),
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
            replication_tasks: {
                let snapshot_instant = std::time::Instant::now();
                self.state
                    .replication_tasks
                    .iter()
                    .map(|entry| {
                        ReplicationTaskSnapshotEntry::capture(
                            entry.key(),
                            entry.value(),
                            snapshot_instant,
                        )
                    })
                    .collect()
            },
            graceful_unmounts: self
                .state
                .graceful_unmounts
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
            delayed_replica_releases: self
                .state
                .delayed_replica_releases
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
            local_disk_segments: self
                .state
                .local_disk_segments
                .iter()
                .map(|entry| LocalDiskSnapshotEntry {
                    storage_id: *entry.key(),
                    client_id: entry
                        .active_client_id
                        .or(entry.persisted_client_id)
                        .unwrap_or_else(Uuid::nil),
                    enable_offloading: entry.enable_offloading,
                    offloading_objects: entry.offloading_objects.clone(),
                    ssd_total_capacity_bytes: 0,
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
                host_id: segment.host_id.clone(),
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
                host_id: String::new(),
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
        let scoped_key = TenantId::new(tenant_id.to_owned())
            .expect("test tenant id must be valid")
            .make_scoped_key(key);
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
    pub fn remove_tenant_registration_for_test(&self, tenant_id: &str) {
        let tenant_id = TenantId::new(tenant_id.to_owned()).expect("test tenant id must be valid");
        let capacity = self.tenant_quota_capacity_bytes();
        self.state
            .tenant_quotas
            .write()
            .remove_registration_for_test(&tenant_id, capacity);
    }

    #[doc(hidden)]
    pub fn set_replica_handle_valid_for_test(
        &self,
        key: &str,
        segment_name: &str,
        tenant_id: &str,
        handle_valid: bool,
    ) -> bool {
        let scoped_key = TenantId::new(tenant_id.to_owned())
            .expect("test tenant id must be valid")
            .make_scoped_key(key);
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
    pub fn has_replica_for_test(&self, key: &str, segment_name: &str, tenant_id: &str) -> bool {
        let scoped_key = TenantId::new(tenant_id.to_owned())
            .expect("test tenant id must be valid")
            .make_scoped_key(key);
        self.state.objects.get(&scoped_key).is_some_and(|object| {
            object
                .replicas
                .iter()
                .any(|replica| replica.segment_name == segment_name)
        })
    }

    #[doc(hidden)]
    pub fn drain_task_for_test(&self, job_id: Uuid) -> Option<TaskEntry> {
        let job = self.state.drain_jobs.get(&job_id)?;
        let task_id = *job.active_tasks.keys().next()?;
        self.state.tasks.get(&task_id).map(|task| task.clone())
    }

    #[doc(hidden)]
    pub fn process_drain_jobs_once_for_test(&self) {
        background_ops::process_drain_jobs(&self.state);
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

    /// Testing helper: run one NoF-specific eviction cycle.
    pub fn run_nof_eviction_cycle_for_test(&self, target_count: usize) -> Vec<String> {
        run_nof_eviction_cycle(&self.state, target_count)
    }

    /// Testing helper: evaluate NoF pressure/watermark and run one cycle.
    pub fn run_automatic_nof_eviction_once_for_test(&self) -> Vec<String> {
        run_automatic_nof_eviction_once(&self.state)
    }

    #[doc(hidden)]
    pub fn reap_expired_background_tasks_for_test(&self) {
        background_ops::reap_expired_background_tasks(&self.state, std::time::Instant::now());
    }

    #[doc(hidden)]
    pub fn clear_invalid_handles_for_test(&self) {
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let alive_clients = get_alive_clients_snapshot(&self.state);
        helpers::clear_invalid_handles_locked(&self.state, &alive_clients);
    }

    #[doc(hidden)]
    pub fn expire_client_for_test(&self, client_id: Uuid) {
        if let Some(mut entry) = self.state.clients.get_mut(&client_id) {
            entry.last_ping = SystemTime::now()
                .checked_sub(Duration::from_secs(3600))
                .unwrap_or(entry.last_ping);
        }
    }

    #[doc(hidden)]
    pub fn promotion_candidate_count_for_test(&self) -> usize {
        self.state
            .promotion_candidate_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[doc(hidden)]
    pub fn has_promotion_candidate_for_test(&self, key: &str, tenant_id: &str) -> bool {
        self.state.promotion_candidates.contains_key(
            &TenantId::new(tenant_id.to_owned())
                .expect("test tenant id must be valid")
                .make_scoped_key(key),
        )
    }

    #[doc(hidden)]
    pub fn make_promotion_candidates_due_for_test(&self) {
        let now = std::time::Instant::now();
        for mut candidate in self.state.promotion_candidates.iter_mut() {
            candidate.retry_after = now;
        }
    }

    #[doc(hidden)]
    pub fn age_promotion_candidates_for_test(&self, age: Duration) {
        for mut candidate in self.state.promotion_candidates.iter_mut() {
            candidate.first_seen = candidate
                .first_seen
                .checked_sub(age)
                .unwrap_or(candidate.first_seen);
            candidate.last_seen = candidate
                .last_seen
                .checked_sub(age)
                .unwrap_or(candidate.last_seen);
        }
    }

    #[doc(hidden)]
    pub fn set_promotion_candidate_retry_count_for_test(
        &self,
        key: &str,
        tenant_id: &str,
        retry_count: u32,
    ) {
        if let Some(mut candidate) = self.state.promotion_candidates.get_mut(
            &TenantId::new(tenant_id.to_owned())
                .expect("test tenant id must be valid")
                .make_scoped_key(key),
        ) {
            candidate.retry_count = retry_count;
        }
    }

    #[doc(hidden)]
    pub fn run_promotion_candidate_retry_for_test(&self) {
        run_promotion_candidate_retry(&self.state, 256);
    }

    #[doc(hidden)]
    pub fn run_promotion_candidate_retry_with_budget_for_test(&self, partitions: usize) {
        run_promotion_candidate_retry(&self.state, partitions);
    }

    #[doc(hidden)]
    pub fn clear_promotion_candidates_for_reload_for_test(&self) {
        self.state.clear_transient_promotion_candidates();
    }

    #[cfg(test)]
    fn queued_oplog_command_count_for_test(&self) -> usize {
        self.oplog_manager.queued_command_count_for_test()
    }

    /// 获取 oplog 管理器引用 / Returns a reference to the oplog manager.
    pub fn oplog_manager(&self) -> &crate::oplog::OpLogManager {
        self.oplog_manager.as_ref()
    }

    /// Replace the active oplog backend without exposing a service-wide lock.
    pub fn replace_oplog_manager(
        &self,
        replacement: crate::oplog::OpLogManager,
    ) -> Result<(), HaError> {
        self.oplog_manager.replace_with(replacement)
    }

    /// Initialize the service view version for a leadership term.
    pub fn set_view_version(&self, version: i64) {
        self.state.view_version.store(version, Ordering::Relaxed);
    }

    /// Set the election-issued term used for cross-process connector fencing.
    pub fn set_leadership_view_version(&self, version: u64) {
        self.state
            .leadership_view_version
            .store(version, Ordering::Release);
    }

    /// Establish a connector-side term barrier before an HA leader serves.
    ///
    /// The returned connector snapshot includes every old-term mutation that
    /// serialized before the barrier. Replacing only the policy layer keeps
    /// the usage/reservation ledger reconstructed by standby replay intact.
    pub fn prepare_tenant_quota_leadership_term(
        &self,
        producer_view_version: u64,
    ) -> Result<(), HaError> {
        if !self.state.runtime_config.enable_tenant_quota {
            return Ok(());
        }
        if self.state.leadership_view_version.load(Ordering::Acquire) != producer_view_version {
            return Err(HaError::InvalidBackend(
                "tenant quota leadership term does not match acquired view".into(),
            ));
        }
        let snapshot = advance_tenant_quota_policy_term(
            &self.state.runtime_config.tenant_quota_connector_type,
            &self.state.runtime_config.tenant_quota_connector_uri,
            &self.state.runtime_config.cluster_id,
            producer_view_version,
        )
        .map_err(|error| {
            HaError::InvalidBackend(format!(
                "failed to establish tenant quota policy term barrier: {error}"
            ))
        })?;
        if snapshot.producer_view_version != producer_view_version {
            return Err(HaError::InvalidBackend(format!(
                "tenant quota connector returned term {}, expected {producer_view_version}",
                snapshot.producer_view_version
            )));
        }
        let policies = snapshot
            .tenant_quotas
            .into_iter()
            .map(|(tenant_id, quota)| {
                TenantId::new(tenant_id.clone())
                    .map(|tenant_id| (tenant_id, quota))
                    .map_err(|error| {
                        HaError::InvalidBackend(format!(
                            "invalid tenant quota policy tenant {tenant_id:?}: {error}"
                        ))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        self.state
            .tenant_quotas
            .write()
            .replace_policies(&policies, self.tenant_quota_capacity_bytes())
            .map_err(|error| {
                HaError::InvalidBackend(format!(
                    "failed to apply tenant quota policy term barrier: {error:?}"
                ))
            })
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

#[cfg(test)]
mod snapshot_restore_tests {
    use super::*;
    use crate::ha::{
        CatalogBackedSnapshotProvider, EmbeddedSnapshotCatalogStore, LocalFileSnapshotObjectStore,
        SnapshotDescriptor, SnapshotObjectStore, SnapshotProvider,
    };
    use crate::service::background_ops::reap_expired_background_tasks;
    use rmpv::Value;
    use std::io::Cursor;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::time::Instant;
    use tempfile::TempDir;

    const SNAPSHOT_CODEC_CLUSTER: &str = "snapshot-codec-cluster";

    fn snapshot_codec_provider(
        root: &TempDir,
    ) -> (
        CatalogBackedSnapshotProvider,
        Arc<LocalFileSnapshotObjectStore>,
    ) {
        let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
        let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
        (
            CatalogBackedSnapshotProvider::new(
                SNAPSHOT_CODEC_CLUSTER,
                Box::new(catalog),
                object_store.clone(),
            ),
            object_store,
        )
    }

    fn publish_service_snapshot(
        provider: &CatalogBackedSnapshotProvider,
        service: &MasterServiceImpl,
        snapshot_id: &str,
        producer_view_version: u64,
    ) -> SnapshotDescriptor {
        let snapshot = service.capture_loaded_snapshot(snapshot_id);
        provider
            .publish_loaded_snapshot(&snapshot, producer_view_version)
            .unwrap()
    }

    fn restore_loaded_snapshot(
        service: &MasterServiceImpl,
        snapshot: LoadedSnapshot,
    ) -> Result<(), String> {
        restore_loaded_snapshot_state(
            &service.state,
            snapshot.segments,
            snapshot.nof_segments,
            snapshot.objects,
            snapshot.tasks,
            snapshot.replication_tasks,
            snapshot.local_disk_segments,
            snapshot.graceful_unmounts,
            snapshot.delayed_replica_releases,
            snapshot.allocator_config,
        )
    }

    #[tokio::test]
    async fn cpp_parity_master_service_promotion_snapshot_local_disk_replica_round_trip() {
        verify_local_disk_replica_snapshot_round_trip().await;
    }

    #[tokio::test]
    async fn cpp_parity_master_service_promotion_snapshot_mixed_memory_and_local_disk_round_trip() {
        verify_mixed_memory_and_local_disk_snapshot_round_trip().await;
    }

    #[tokio::test]
    async fn cpp_parity_master_service_promotion_snapshot_enable_offloading_preserved() {
        verify_local_disk_enable_offloading_snapshot_round_trip().await;
    }

    #[tokio::test]
    async fn cpp_parity_master_service_promotion_snapshot_multiple_local_disk_holders_round_trip() {
        verify_multiple_local_disk_holders_snapshot_round_trip().await;
    }

    #[tokio::test]
    async fn cpp_parity_master_service_promotion_snapshot_in_flight_task_safe() {
        verify_in_flight_promotion_snapshot_safe().await;
    }

    #[derive(Clone)]
    struct LocalDiskSnapshotFixture {
        client_id: Uuid,
        storage_id: Uuid,
        segment_id: Uuid,
        segment_name: String,
        key: String,
        size: u64,
        endpoint: String,
        generation_id: Uuid,
    }

    fn local_disk_snapshot_service() -> MasterServiceImpl {
        MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            offload_on_evict: true,
            ..Default::default()
        })
    }

    async fn create_snapshot_local_disk_fixture(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        size: u64,
        keep_memory: bool,
    ) -> LocalDiskSnapshotFixture {
        let segment_name = format!("snapshot-memory-{key}");
        let endpoint = format!("snapshot-local-disk-{key}:17813");
        let mounted = MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: segment_name.clone(),
                size: SNAPSHOT_CHILD_SEGMENT_SIZE,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE + (u64::from(key.as_bytes()[0]) << 24),
                te_endpoint: format!("memory-{key}:17813"),
                protocol: "tcp".into(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();
        let segment_id = uuid_from_proto(&mounted);
        let storage_id = Uuid::new_v4();
        let recovery_session_id = Uuid::new_v4();
        for recovery_complete in [false, true] {
            MasterService::mount_local_disk_segment(
                service,
                Request::new(proto::MountLocalDiskSegmentRequest {
                    client_id: Some(uuid_to_proto(client_id)),
                    enable_offloading: true,
                    storage_id: Some(uuid_to_proto(storage_id)),
                    recovery_complete,
                    recovery_session_id: Some(uuid_to_proto(recovery_session_id)),
                }),
            )
            .await
            .unwrap();
        }
        MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                slice_length: size,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: segment_name.clone(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();

        service
            .state
            .objects
            .get_mut(&TenantId::default().make_scoped_key(key))
            .unwrap()
            .lease_timeout = Some(SystemTime::UNIX_EPOCH);
        assert!(service.run_eviction_cycle_for_test(1).is_empty());
        let tasks = MasterService::offload_object_heartbeat(
            service,
            Request::new(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(uuid_to_proto(client_id)),
                enable_offloading: true,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].key, key);
        let generation_id = tasks[0]
            .generation_id
            .as_ref()
            .map(uuid_from_proto)
            .expect("Master-issued offload task has a generation");

        MasterService::notify_offload_success(
            service,
            Request::new(proto::NotifyOffloadSuccessRequest {
                client_id: Some(uuid_to_proto(client_id)),
                keys: vec![key.into()],
                metadatas: vec![proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: key.len() as i64,
                    data_size: size as i64,
                    transport_endpoint: endpoint.clone(),
                }],
                tasks,
                recovery_session_id: None,
            }),
        )
        .await
        .unwrap();

        if !keep_memory {
            service
                .state
                .objects
                .get_mut(&TenantId::default().make_scoped_key(key))
                .unwrap()
                .lease_timeout = Some(SystemTime::UNIX_EPOCH);
            assert_eq!(
                service.run_eviction_cycle_for_test(1),
                vec![key.to_string()]
            );
            let replicas = snapshot_child_replicas(service, key).await;
            assert_eq!(replicas.len(), 1);
            assert_eq!(
                replicas[0].replica_type,
                proto::replica_descriptor::ReplicaType::LocalDisk as i32
            );
        } else {
            assert_eq!(snapshot_child_replicas(service, key).await.len(), 2);
        }

        LocalDiskSnapshotFixture {
            client_id,
            storage_id,
            segment_id,
            segment_name,
            key: key.into(),
            size,
            endpoint,
            generation_id,
        }
    }

    async fn restore_local_disk_snapshot(
        provider: &CatalogBackedSnapshotProvider,
        source: &MasterServiceImpl,
        snapshot_id: &str,
    ) -> (LoadedSnapshot, MasterServiceImpl) {
        publish_service_snapshot(provider, source, snapshot_id, 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let first = loaded.clone();
        assert!(first.objects.iter().any(|(_, object)| {
            object
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::LocalDisk)
        }));
        let restored = local_disk_snapshot_service();
        restore_loaded_snapshot(&restored, loaded).unwrap();

        for (key, _) in &first.objects {
            let user_key = TenantId::parse_scoped_key(key)
                .map(|(_, user_key)| user_key)
                .unwrap_or_else(|_| key.clone());
            let error = MasterService::get_replica_list(
                &restored,
                Request::new(proto::GetReplicaListRequest {
                    key: user_key,
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        }

        let second = restored.capture_loaded_snapshot(format!("{snapshot_id}-second"));
        let mut first_local_disks = first.local_disk_segments.clone();
        first_local_disks.sort_by_key(|entry| entry.storage_id);
        let mut second_local_disks = second.local_disk_segments.clone();
        second_local_disks.sort_by_key(|entry| entry.storage_id);
        assert_eq!(second_local_disks, first_local_disks);
        assert_eq!(second.objects.len(), first.objects.len());
        for (key, first_object) in &first.objects {
            let second_object = second
                .objects
                .iter()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, object)| object)
                .expect("object survives second snapshot");
            let first_disk = first_object
                .replicas
                .iter()
                .filter(|replica| replica.replica_type == ReplicaType::LocalDisk)
                .map(|replica| {
                    (
                        replica.segment_name.clone(),
                        replica.size,
                        replica.holder_client_id,
                        replica.local_disk_storage_id,
                        replica.local_disk_generation_id,
                    )
                })
                .collect::<Vec<_>>();
            let second_disk = second_object
                .replicas
                .iter()
                .filter(|replica| replica.replica_type == ReplicaType::LocalDisk)
                .map(|replica| {
                    (
                        replica.segment_name.clone(),
                        replica.size,
                        replica.holder_client_id,
                        replica.local_disk_storage_id,
                        replica.local_disk_generation_id,
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(second_disk, first_disk, "durable LocalDisk descriptor");
        }
        (first, restored)
    }

    async fn recover_snapshot_local_disk(
        service: &MasterServiceImpl,
        fixture: &LocalDiskSnapshotFixture,
        enable_offloading: bool,
    ) {
        let restored_replica = service.state.objects.iter().find_map(|object| {
            object
                .replicas
                .iter()
                .find(|replica| {
                    replica.replica_type == ReplicaType::LocalDisk
                        && replica.local_disk_storage_id == Some(fixture.storage_id)
                })
                .cloned()
        });
        let restored_replica = restored_replica.unwrap_or_else(|| {
            let inventory = service
                .state
                .objects
                .iter()
                .map(|object| {
                    (
                        object.key().clone(),
                        object
                            .replicas
                            .iter()
                            .map(|replica| (replica.replica_type, replica.local_disk_storage_id))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();
            panic!("restored object retains dormant LocalDisk descriptor: {inventory:?}")
        });
        assert_eq!(
            restored_replica.local_disk_storage_id,
            Some(fixture.storage_id)
        );
        assert_eq!(
            restored_replica.local_disk_generation_id,
            Some(fixture.generation_id)
        );
        assert_eq!(restored_replica.size, fixture.size);
        assert_eq!(restored_replica.status, ReplicaStatus::Complete);
        let recovery_session_id = Uuid::new_v4();
        MasterService::mount_local_disk_segment(
            service,
            Request::new(proto::MountLocalDiskSegmentRequest {
                client_id: Some(uuid_to_proto(fixture.client_id)),
                enable_offloading: false,
                storage_id: Some(uuid_to_proto(fixture.storage_id)),
                recovery_complete: false,
                recovery_session_id: Some(uuid_to_proto(recovery_session_id)),
            }),
        )
        .await
        .unwrap();
        let response = MasterService::notify_offload_success(
            service,
            Request::new(proto::NotifyOffloadSuccessRequest {
                client_id: Some(uuid_to_proto(fixture.client_id)),
                keys: vec![fixture.key.clone()],
                metadatas: vec![proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: fixture.key.len() as i64,
                    data_size: fixture.size as i64,
                    transport_endpoint: fixture.endpoint.clone(),
                }],
                tasks: vec![proto::OffloadTaskItem {
                    tenant_id: String::new(),
                    key: fixture.key.clone(),
                    size: fixture.size as i64,
                    generation_id: Some(uuid_to_proto(fixture.generation_id)),
                }],
                recovery_session_id: Some(uuid_to_proto(recovery_session_id)),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.stale_recovery_tasks.is_empty());
        MasterService::mount_local_disk_segment(
            service,
            Request::new(proto::MountLocalDiskSegmentRequest {
                client_id: Some(uuid_to_proto(fixture.client_id)),
                enable_offloading,
                storage_id: Some(uuid_to_proto(fixture.storage_id)),
                recovery_complete: true,
                recovery_session_id: Some(uuid_to_proto(recovery_session_id)),
            }),
        )
        .await
        .unwrap();
    }

    async fn remount_snapshot_memory(
        service: &MasterServiceImpl,
        fixture: &LocalDiskSnapshotFixture,
    ) {
        MasterService::re_mount_segment(
            service,
            Request::new(proto::ReMountSegmentRequest {
                client_id: Some(uuid_to_proto(fixture.client_id)),
                segment_names: vec![fixture.segment_name.clone()],
                segment_sizes: vec![SNAPSHOT_CHILD_SEGMENT_SIZE],
                base_addrs: vec![
                    SNAPSHOT_CHILD_SEGMENT_BASE + (u64::from(fixture.key.as_bytes()[0]) << 24),
                ],
                te_endpoints: vec![format!("memory-{}:17813", fixture.key)],
                protocols: vec!["tcp".into()],
                segment_ids: vec![uuid_to_proto(fixture.segment_id)],
                host_ids: vec![String::new()],
            }),
        )
        .await
        .unwrap();
    }

    async fn assert_snapshot_local_disk_replica(
        service: &MasterServiceImpl,
        fixture: &LocalDiskSnapshotFixture,
        expected_replica_types: &[i32],
    ) {
        let replicas = snapshot_child_replicas(service, &fixture.key).await;
        assert_eq!(
            replicas
                .iter()
                .map(|replica| replica.replica_type)
                .collect::<Vec<_>>(),
            expected_replica_types
        );
        let disk = replicas
            .iter()
            .find(|replica| {
                replica.replica_type == proto::replica_descriptor::ReplicaType::LocalDisk as i32
            })
            .expect("public query returns LocalDisk replica");
        assert_eq!(
            disk.local_disk_client_id.as_ref().map(uuid_from_proto),
            Some(fixture.client_id)
        );
        assert_eq!(disk.object_size, fixture.size);
        assert_eq!(disk.size, fixture.size);
        assert_eq!(disk.transport_endpoint, fixture.endpoint);
        assert_eq!(
            disk.local_disk_storage_id.as_ref().map(uuid_from_proto),
            Some(fixture.storage_id)
        );
        assert_eq!(
            disk.local_disk_generation_id.as_ref().map(uuid_from_proto),
            Some(fixture.generation_id)
        );
    }

    async fn verify_local_disk_replica_snapshot_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = local_disk_snapshot_service();
        let fixture = create_snapshot_local_disk_fixture(
            &source,
            Uuid::new_v4(),
            "snapshot-local-disk-only",
            4096,
            false,
        )
        .await;
        let (_first, restored) =
            restore_local_disk_snapshot(&provider, &source, "20260811_010000_001").await;
        recover_snapshot_local_disk(&restored, &fixture, false).await;
        assert_snapshot_local_disk_replica(
            &restored,
            &fixture,
            &[proto::replica_descriptor::ReplicaType::LocalDisk as i32],
        )
        .await;
    }

    async fn verify_mixed_memory_and_local_disk_snapshot_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = local_disk_snapshot_service();
        let fixture = create_snapshot_local_disk_fixture(
            &source,
            Uuid::new_v4(),
            "snapshot-mixed-replicas",
            8192,
            true,
        )
        .await;
        let (_first, restored) =
            restore_local_disk_snapshot(&provider, &source, "20260811_010000_002").await;
        remount_snapshot_memory(&restored, &fixture).await;
        recover_snapshot_local_disk(&restored, &fixture, false).await;
        assert_snapshot_local_disk_replica(
            &restored,
            &fixture,
            &[
                proto::replica_descriptor::ReplicaType::Memory as i32,
                proto::replica_descriptor::ReplicaType::LocalDisk as i32,
            ],
        )
        .await;
    }

    async fn verify_local_disk_enable_offloading_snapshot_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = local_disk_snapshot_service();
        let fixture = create_snapshot_local_disk_fixture(
            &source,
            Uuid::new_v4(),
            "snapshot-offloading-policy",
            12288,
            false,
        )
        .await;
        MasterService::offload_object_heartbeat(
            &source,
            Request::new(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(uuid_to_proto(fixture.client_id)),
                enable_offloading: true,
            }),
        )
        .await
        .unwrap();
        let (first, restored) =
            restore_local_disk_snapshot(&provider, &source, "20260811_010000_003").await;
        let persisted = first
            .local_disk_segments
            .iter()
            .find(|entry| entry.storage_id == fixture.storage_id)
            .unwrap();
        assert!(persisted.enable_offloading);
        assert_eq!(persisted.client_id, fixture.client_id);
        recover_snapshot_local_disk(&restored, &fixture, true).await;
        assert!(
            restored
                .state
                .local_disk_segments
                .get(&fixture.storage_id)
                .unwrap()
                .enable_offloading
        );
        assert_snapshot_local_disk_replica(
            &restored,
            &fixture,
            &[proto::replica_descriptor::ReplicaType::LocalDisk as i32],
        )
        .await;
    }

    async fn verify_multiple_local_disk_holders_snapshot_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = local_disk_snapshot_service();
        let first_fixture = create_snapshot_local_disk_fixture(
            &source,
            Uuid::new_v4(),
            "snapshot-holder-a",
            16384,
            false,
        )
        .await;
        let second_fixture = create_snapshot_local_disk_fixture(
            &source,
            Uuid::new_v4(),
            "snapshot-holder-b",
            32768,
            false,
        )
        .await;
        let (first, restored) =
            restore_local_disk_snapshot(&provider, &source, "20260811_010000_004").await;
        assert_eq!(first.local_disk_segments.len(), 2);
        recover_snapshot_local_disk(&restored, &first_fixture, false).await;
        recover_snapshot_local_disk(&restored, &second_fixture, false).await;
        assert_snapshot_local_disk_replica(
            &restored,
            &first_fixture,
            &[proto::replica_descriptor::ReplicaType::LocalDisk as i32],
        )
        .await;
        assert_snapshot_local_disk_replica(
            &restored,
            &second_fixture,
            &[proto::replica_descriptor::ReplicaType::LocalDisk as i32],
        )
        .await;
        assert_ne!(first_fixture.client_id, second_fixture.client_id);
        assert_ne!(first_fixture.storage_id, second_fixture.storage_id);
    }

    async fn verify_in_flight_promotion_snapshot_safe() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            offload_on_evict: true,
            promotion_on_hit: true,
            promotion_admission_threshold: 1,
            ..Default::default()
        });
        let fixture = create_snapshot_local_disk_fixture(
            &source,
            Uuid::new_v4(),
            "snapshot-in-flight-promotion",
            1024,
            false,
        )
        .await;
        let scoped_key = TenantId::default().make_scoped_key(&fixture.key);
        assert!(source.state.promotion_tasks.contains_key(&scoped_key));
        assert!(
            source
                .state
                .local_disk_segments
                .get(&fixture.storage_id)
                .unwrap()
                .promotion_objects
                .contains_key(&scoped_key)
        );
        let source_disk_refcnt = source
            .state
            .objects
            .get(&scoped_key)
            .unwrap()
            .replicas
            .iter()
            .find(|replica| replica.replica_type == ReplicaType::LocalDisk)
            .unwrap()
            .refcnt;
        assert_eq!(source_disk_refcnt, 1, "promotion pins its LocalDisk source");

        let (_first, restored) =
            restore_local_disk_snapshot(&provider, &source, "20260811_010000_005").await;
        assert!(restored.state.promotion_tasks.is_empty());
        let restored_local_disk = restored
            .state
            .local_disk_segments
            .get(&fixture.storage_id)
            .unwrap();
        assert!(restored_local_disk.promotion_objects.is_empty());
        drop(restored_local_disk);
        let restored_disk_refcnt = restored
            .state
            .objects
            .get(&scoped_key)
            .unwrap()
            .replicas
            .iter()
            .find(|replica| replica.replica_type == ReplicaType::LocalDisk)
            .unwrap()
            .refcnt;
        assert_eq!(restored_disk_refcnt, 0, "transient source pin is discarded");

        recover_snapshot_local_disk(&restored, &fixture, false).await;
        assert_snapshot_local_disk_replica(
            &restored,
            &fixture,
            &[proto::replica_descriptor::ReplicaType::LocalDisk as i32],
        )
        .await;
    }

    const SNAPSHOT_CHILD_SEGMENT_BASE: u64 = 0x310000000;
    const SNAPSHOT_CHILD_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

    async fn mount_snapshot_child_segment(
        service: &MasterServiceImpl,
        client_id: Uuid,
        segment_name: &str,
    ) {
        MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: segment_name.into(),
                size: SNAPSHOT_CHILD_SEGMENT_SIZE,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE,
                te_endpoint: segment_name.into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn put_snapshot_child_object(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        group_id: Option<&str>,
        complete: bool,
    ) {
        MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    group_ids: group_id.into_iter().map(str::to_owned).collect(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        if complete {
            MasterService::put_end(
                service,
                Request::new(proto::PutEndRequest {
                    client_id: Some(uuid_to_proto(client_id)),
                    key: key.into(),
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap();
        }
    }

    async fn snapshot_child_replicas(
        service: &MasterServiceImpl,
        key: &str,
    ) -> Vec<proto::ReplicaDescriptor> {
        MasterService::get_replica_list(
            service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas
    }

    async fn snapshot_child_exists(service: &MasterServiceImpl, key: &str) -> bool {
        MasterService::exist_key(
            service,
            Request::new(proto::ExistKeyRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .exists
    }

    async fn snapshot_child_keys(service: &MasterServiceImpl) -> Vec<String> {
        let mut keys = MasterService::get_all_keys(
            service,
            Request::new(proto::GetAllKeysRequest {
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .keys;
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_child_restore_rebuilds_grouped_object_routing() {
        const KEY: &str = "snapshot_grouped_route_key";
        const GROUP_ID: &str = "snapshot-group-on-distinct-route";
        const SEGMENT_NAME: &str = "grouped_snapshot_segment";

        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, SEGMENT_NAME).await;
        put_snapshot_child_object(&source, client_id, KEY, Some(GROUP_ID), true).await;
        assert_eq!(snapshot_child_replicas(&source, KEY).await.len(), 1);

        publish_service_snapshot(&provider, &source, "20240701_130000_000", 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let grouped = loaded
            .objects
            .iter()
            .find(|(_, object)| object.user_key == KEY)
            .expect("grouped object must survive production catalog decoding");
        assert_eq!(grouped.1.group_id, GROUP_ID);

        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        mount_snapshot_child_segment(&restored, client_id, SEGMENT_NAME).await;
        assert_eq!(snapshot_child_replicas(&restored, KEY).await.len(), 1);

        MasterService::remove(
            &restored,
            Request::new(proto::RemoveRequest {
                key: KEY.into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert!(!snapshot_child_exists(&restored, KEY).await);
    }

    #[test]
    fn cpp_parity_snapshot_child_persist_state_uses_etcd_oplog_boundary_in_snapshot_descriptor() {
        const SNAPSHOT_ID: &str = "20240601_120000_456";
        const EXPECTED_SEQUENCE: u64 = 123;
        const PRODUCER_VIEW_VERSION: u64 = 19;

        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let service = MasterServiceImpl::default();
        let replacement = {
            let manager = crate::oplog::OpLogManager::new(
                Some(Box::new(crate::oplog::InMemoryOpLog::new(16))),
                0,
            );
            manager.set_initial_sequence_id(EXPECTED_SEQUENCE).unwrap();
            manager
        };
        service.replace_oplog_manager(replacement).unwrap();

        let descriptor =
            publish_service_snapshot(&provider, &service, SNAPSHOT_ID, PRODUCER_VIEW_VERSION);

        assert_eq!(descriptor.snapshot_id, SNAPSHOT_ID);
        assert_eq!(descriptor.last_included_seq, EXPECTED_SEQUENCE);
        assert_eq!(descriptor.producer_view_version, PRODUCER_VIEW_VERSION);
        assert!(descriptor.created_at_ms > 0);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_child_restore_falls_back_when_latest_metadata_is_corrupt() {
        const OLD_KEY: &str = "restore_fallback_key_1";
        const NEW_KEY: &str = "restore_fallback_key_2";
        const SEGMENT_NAME: &str = "restore_fallback_segment";

        let root = tempfile::tempdir().unwrap();
        let (provider, object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, SEGMENT_NAME).await;

        put_snapshot_child_object(&source, client_id, OLD_KEY, None, true).await;
        assert_eq!(snapshot_child_replicas(&source, OLD_KEY).await.len(), 1);
        let older = publish_service_snapshot(&provider, &source, "20240702_120000_000", 1);

        put_snapshot_child_object(&source, client_id, NEW_KEY, None, true).await;
        assert_eq!(snapshot_child_replicas(&source, NEW_KEY).await.len(), 1);
        let newest = publish_service_snapshot(&provider, &source, "20240702_120500_000", 2);
        object_store
            .upload_buffer(
                &format!("{}metadata", newest.object_prefix),
                b"corrupted-snapshot-payload",
            )
            .unwrap();

        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.snapshot_id, older.snapshot_id);
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        mount_snapshot_child_segment(&restored, client_id, SEGMENT_NAME).await;

        assert_eq!(snapshot_child_replicas(&restored, OLD_KEY).await.len(), 1);
        assert_eq!(snapshot_child_keys(&restored).await, vec![OLD_KEY]);
        assert!(!snapshot_child_exists(&restored, NEW_KEY).await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_child_restore_cleans_non_complete_replica() {
        const CLEAN_KEY: &str = "clean_object";
        const DIRTY_KEY: &str = "dirty_incomplete";
        const SEGMENT_NAME: &str = "noncomplete_restore_segment";

        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, SEGMENT_NAME).await;
        put_snapshot_child_object(&source, client_id, CLEAN_KEY, None, true).await;
        assert_eq!(snapshot_child_replicas(&source, CLEAN_KEY).await.len(), 1);
        put_snapshot_child_object(&source, client_id, DIRTY_KEY, None, false).await;
        assert_eq!(
            snapshot_child_keys(&source).await,
            vec![CLEAN_KEY, DIRTY_KEY]
        );

        publish_service_snapshot(&provider, &source, "20240701_120000_000", 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        mount_snapshot_child_segment(&restored, client_id, SEGMENT_NAME).await;

        assert_eq!(snapshot_child_replicas(&restored, CLEAN_KEY).await.len(), 1);
        assert_eq!(snapshot_child_keys(&restored).await, vec![CLEAN_KEY]);
        assert!(!snapshot_child_exists(&restored, DIRTY_KEY).await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_child_restore_cleans_expired_lease() {
        const EXPIRED_KEY: &str = "expired_lease_object";
        const VALID_KEY: &str = "normal_lease_object";
        const SEGMENT_NAME: &str = "expired_lease_restore_segment";

        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, SEGMENT_NAME).await;
        put_snapshot_child_object(&source, client_id, EXPIRED_KEY, None, true).await;
        put_snapshot_child_object(&source, client_id, VALID_KEY, None, true).await;
        assert_eq!(snapshot_child_replicas(&source, VALID_KEY).await.len(), 1);

        publish_service_snapshot(&provider, &source, "20240701_120000_001", 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        mount_snapshot_child_segment(&restored, client_id, SEGMENT_NAME).await;

        assert_eq!(snapshot_child_replicas(&restored, VALID_KEY).await.len(), 1);
        assert_eq!(snapshot_child_keys(&restored).await, vec![VALID_KEY]);
        assert!(!snapshot_child_exists(&restored, EXPIRED_KEY).await);
    }

    #[derive(PartialEq, Debug)]
    struct SnapshotObjectProbe {
        key: String,
        replicas: Vec<(String, i32, i32, u64, u32)>,
    }

    #[derive(PartialEq, Debug)]
    struct SnapshotSegmentProbe {
        id: String,
        name: String,
        size: u64,
        client: String,
        status: i32,
    }

    fn snapshot_state_probe(
        snapshot: &LoadedSnapshot,
    ) -> (Vec<SnapshotObjectProbe>, Vec<SnapshotSegmentProbe>, usize) {
        let mut objects = snapshot
            .objects
            .iter()
            .map(|(key, object)| SnapshotObjectProbe {
                key: key.clone(),
                replicas: object
                    .replicas
                    .iter()
                    .map(|replica| {
                        (
                            replica.segment_name.clone(),
                            replica.replica_type as i32,
                            replica.status as i32,
                            replica.size,
                            replica.refcnt,
                        )
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        let mut segments = snapshot
            .segments
            .iter()
            .map(|entry| SnapshotSegmentProbe {
                id: entry.segment.id.to_string(),
                name: entry.segment.name.clone(),
                size: entry.segment.size,
                client: entry.client_id.to_string(),
                status: entry.status as i32,
            })
            .collect::<Vec<_>>();
        segments.sort_by(|left, right| left.id.cmp(&right.id));
        (objects, segments, snapshot.tasks.len())
    }

    async fn snapshot_roundtrip_restored_service(
        provider: &CatalogBackedSnapshotProvider,
        source: &MasterServiceImpl,
        client_id: Uuid,
        snapshot_id: &str,
        segment_name: &str,
        expected_object_count: usize,
        expected_segment_count: usize,
    ) -> MasterServiceImpl {
        let first = source.capture_loaded_snapshot(snapshot_id);
        assert_eq!(first.objects.len(), expected_object_count);
        assert_eq!(first.segments.len(), expected_segment_count);
        publish_service_snapshot(provider, source, snapshot_id, 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        let second = restored.capture_loaded_snapshot(format!("{snapshot_id}-second"));
        assert_eq!(
            snapshot_state_probe(&second),
            snapshot_state_probe(&first),
            "second save must match first"
        );
        mount_snapshot_child_segment(&restored, client_id, segment_name).await;
        restored
    }

    async fn snapshot_child_put_complete(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        segment_name: &str,
    ) {
        put_snapshot_child_object(service, client_id, key, None, true).await;
    }

    async fn snapshot_put_start_end_flow_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-put-end:1").await;
        put_snapshot_child_object(&source, client_id, "test_key", None, true).await;
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 1);

        let restored = snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_001",
            "snapshot-put-end:1",
            1,
            1,
        )
        .await;
        assert_eq!(
            snapshot_child_replicas(&restored, "test_key").await.len(),
            1
        );
        assert!(snapshot_child_exists(&restored, "test_key").await);
    }

    async fn snapshot_random_put_start_end_flow_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-random-put:1").await;
        for index in 0..10 {
            put_snapshot_child_object(&source, client_id, &format!("test_key{index}"), None, true)
                .await;
        }
        // The C++ RandomPutStartEndFlow body queries every completed object,
        // which grants a fresh lease that keeps the object restorable.
        for index in 0..10 {
            assert_eq!(
                snapshot_child_replicas(&source, &format!("test_key{index}"))
                    .await
                    .len(),
                1
            );
        }

        let restored = snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_002",
            "snapshot-random-put:1",
            10,
            1,
        )
        .await;
        assert_eq!(snapshot_child_keys(&restored).await.len(), 10);
    }

    async fn snapshot_regex_lookup_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-regex:1").await;
        for index in 0..10 {
            put_snapshot_child_object(&source, client_id, &format!("test_key{index}"), None, true)
                .await;
        }
        // The C++ GetReplicaListByRegex body performs the regex lookup on the
        // live service, granting leases that keep objects restorable.
        assert_eq!(
            MasterService::get_replica_list_by_regex(
                &source,
                Request::new(proto::GetReplicaListByRegexRequest {
                    key_regex: "^test_key".into(),
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap()
            .into_inner()
            .entries
            .len(),
            10
        );

        let restored = snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_003",
            "snapshot-regex:1",
            10,
            1,
        )
        .await;
        let keys = MasterService::get_replica_list_by_regex(
            &restored,
            Request::new(proto::GetReplicaListByRegexRequest {
                key_regex: "^test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .entries;
        assert_eq!(keys.len(), 10);
    }

    async fn snapshot_get_replica_list_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-get:1").await;
        put_snapshot_child_object(&source, client_id, "test_key", None, true).await;
        // The C++ GetReplicaList body queries the key before the snapshot.
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 1);

        let restored = snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_004",
            "snapshot-get:1",
            1,
            1,
        )
        .await;
        assert_eq!(
            snapshot_child_replicas(&restored, "test_key").await.len(),
            1
        );
    }

    async fn snapshot_remove_object_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-remove:1").await;
        put_snapshot_child_object(&source, client_id, "test_key", None, true).await;
        MasterService::remove(
            &source,
            Request::new(proto::RemoveRequest {
                key: "test_key".into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_keys(&source).await.len(), 0);

        snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_005",
            "snapshot-remove:1",
            0,
            1,
        )
        .await;
    }

    async fn snapshot_random_remove_object_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-random-remove:1").await;
        let mut state = Uuid::new_v4().as_u128();
        for _ in 0..10 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let key = format!("test_key{}", state % 1000);
            put_snapshot_child_object(&source, client_id, &key, None, true).await;
            MasterService::remove(
                &source,
                Request::new(proto::RemoveRequest {
                    key: key.clone(),
                    force: true,
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap();
        }
        assert_eq!(snapshot_child_keys(&source).await.len(), 0);

        snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_006",
            "snapshot-random-remove:1",
            0,
            1,
        )
        .await;
    }

    async fn snapshot_remove_by_regex_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-remove-regex:1").await;
        for index in 0..10 {
            put_snapshot_child_object(&source, client_id, &format!("test_key{index}"), None, true)
                .await;
        }
        let removed = MasterService::remove_by_regex(
            &source,
            Request::new(proto::RemoveByRegexRequest {
                pattern: "^test_key".into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .removed_count;
        assert_eq!(removed, 10);

        snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_007",
            "snapshot-remove-regex:1",
            0,
            1,
        )
        .await;
    }

    async fn snapshot_remove_all_preserves_keys_test(provider: &CatalogBackedSnapshotProvider) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-remove-all:1").await;
        for index in 0..10 {
            put_snapshot_child_object(&source, client_id, &format!("test_key{index}"), None, true)
                .await;
            assert!(snapshot_child_exists(&source, &format!("test_key{index}")).await);
        }
        assert_eq!(snapshot_child_keys(&source).await.len(), 10);

        let restored = snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            "20260806_000000_008",
            "snapshot-remove-all:1",
            10,
            1,
        )
        .await;
        for index in 0..10 {
            assert!(snapshot_child_exists(&restored, &format!("test_key{index}")).await);
        }
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_put_start_end_flow() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_put_start_end_flow_test(&provider).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_random_put_start_end_flow() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_random_put_start_end_flow_test(&provider).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_get_replica_list_by_regex() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_regex_lookup_test(&provider).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_get_replica_list() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_get_replica_list_test(&provider).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_remove_object() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_remove_object_test(&provider).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_random_remove_object() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_random_remove_object_test(&provider).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_remove_by_regex() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_remove_by_regex_test(&provider).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_remove_all_preserves_keys() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        snapshot_remove_all_preserves_keys_test(&provider).await;
    }

    async fn snapshot_roundtrip_empty(provider: &CatalogBackedSnapshotProvider, snapshot_id: &str) {
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-empty:1").await;
        snapshot_roundtrip_restored_service(
            provider,
            &source,
            client_id,
            snapshot_id,
            "snapshot-empty:1",
            0,
            1,
        )
        .await;
    }

    async fn snapshot_put_keys(
        service: &MasterServiceImpl,
        client_id: Uuid,
        keys: &[&str],
        grant_lease: bool,
    ) {
        for key in keys {
            put_snapshot_child_object(service, client_id, key, None, true).await;
            if grant_lease {
                assert!(snapshot_child_exists(service, key).await);
            }
        }
    }

    async fn mount_snapshot_segment_named(
        service: &MasterServiceImpl,
        client_id: Uuid,
        segment_name: &str,
        index: u64,
    ) -> proto::Uuid {
        MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: segment_name.into(),
                size: SNAPSHOT_CHILD_SEGMENT_SIZE,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE + index * 0x1000000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap()
    }

    async fn put_snapshot_object_custom(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        size: u64,
        replica_num: u32,
    ) {
        MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                slice_length: size,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn put_snapshot_object_preferred(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        preferred_segment: &str,
    ) {
        MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: preferred_segment.into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_get_replica_list_by_regex_complex() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-regex-complex:1").await;
        let keys = [
            "test_key_01",
            "test_key_02",
            "test_key_10",
            "prod_key_alpha",
            "prod_key_beta",
            "data_part_1_chunk_a",
            "data_part_2_chunk_b",
            "config/user/settings.json",
            "logs/app-2025-08-13.log",
            "short",
            "a_very_very_very_long_key_that_tests_length_limits",
            "test-key-extra",
            "another_key",
        ];
        snapshot_put_keys(&source, client_id, &keys, true).await;
        assert_eq!(
            MasterService::get_replica_list_by_regex(
                &source,
                Request::new(proto::GetReplicaListByRegexRequest {
                    key_regex: "^test_key_".into(),
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap()
            .into_inner()
            .entries
            .len(),
            3
        );

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_010000_001",
            "snapshot-regex-complex:1",
            13,
            1,
        )
        .await;
        assert_eq!(
            MasterService::get_replica_list_by_regex(
                &restored,
                Request::new(proto::GetReplicaListByRegexRequest {
                    key_regex: "^test_key_".into(),
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap()
            .into_inner()
            .entries
            .len(),
            3
        );
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_remove_by_regex_complex() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-remove-regex-complex:1").await;
        let keys = [
            "test_key_01",
            "test_key_02",
            "test_key_10",
            "prod_key_alpha",
            "prod_key_beta",
            "data_part_1_chunk_a",
            "data_part_2_chunk_b",
            "config/user/settings.json",
            "logs/app-2025-08-13.log",
            "short",
            "a_very_very_very_long_key_that_tests_length_limits",
            "test-key-extra",
            "another_key",
        ];
        snapshot_put_keys(&source, client_id, &keys, true).await;
        let removed = MasterService::remove_by_regex(
            &source,
            Request::new(proto::RemoveByRegexRequest {
                pattern: "^test_key_".into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .removed_count;
        assert_eq!(removed, 3);

        snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_010000_002",
            "snapshot-remove-regex-complex:1",
            10,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_mount_unmount_offset_allocator() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_id = MasterService::mount_segment(
            &source,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: "snapshot-mount:1".into(),
                size: SNAPSHOT_CHILD_SEGMENT_SIZE,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE,
                te_endpoint: "snapshot-mount:1".into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_id),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        let remounted = MasterService::mount_segment(
            &source,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: "snapshot-mount:1".into(),
                size: SNAPSHOT_CHILD_SEGMENT_SIZE,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE,
                te_endpoint: "snapshot-mount:1".into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();
        assert_eq!(remounted, segment_id);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_010000_003",
            "snapshot-mount:1",
            0,
            1,
        )
        .await;
        assert!(snapshot_child_keys(&restored).await.is_empty());
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_random_mount_unmount() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let mut state = Uuid::new_v4().as_u128();
        for _ in 0..10 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let size = 16 * 1024 * 1024 * (1 + (state % 10) as u64);
            let mounted = MasterService::mount_segment(
                &source,
                Request::new(proto::MountSegmentRequest {
                    client_id: Some(uuid_to_proto(client_id)),
                    segment_name: "random-mount:1".into(),
                    size,
                    base_addr: 0x310000000,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                    host_id: String::new(),
                }),
            )
            .await
            .unwrap()
            .into_inner()
            .segment_id
            .unwrap();
            MasterService::unmount_segment(
                &source,
                Request::new(proto::UnmountSegmentRequest {
                    segment_id: Some(mounted),
                    client_id: Some(uuid_to_proto(client_id)),
                }),
            )
            .await
            .unwrap();
        }
        snapshot_roundtrip_empty(&provider, "20260806_010000_004").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_put_start_invalid_params() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-invalid:1").await;
        for (index, (replica_num, slice_length)) in
            [(0u32, 1024u64), (1, 0)].into_iter().enumerate()
        {
            let result = MasterService::put_start(
                &source,
                Request::new(proto::PutStartRequest {
                    client_id: Some(uuid_to_proto(client_id)),
                    key: format!("invalid-{index}"),
                    slice_length,
                    tenant_id: String::new(),
                    config: Some(proto::ReplicateConfig {
                        replica_num,
                        ..Default::default()
                    }),
                }),
            )
            .await;
            assert!(result.is_err());
        }
        snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_010000_005",
            "snapshot-invalid:1",
            0,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_exist_key() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-batch-exist:1").await;
        let keys = [
            "test_key0",
            "test_key1",
            "test_key2",
            "test_key3",
            "test_key4",
            "test_key5",
            "test_key6",
            "test_key7",
            "test_key8",
            "test_key9",
        ];
        snapshot_put_keys(&source, client_id, &keys, true).await;

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_010000_006",
            "snapshot-batch-exist:1",
            10,
            1,
        )
        .await;
        let mut batch_keys = keys.iter().map(|key| key.to_string()).collect::<Vec<_>>();
        batch_keys.push("missing".into());
        let results = MasterService::batch_exist_key(
            &restored,
            Request::new(proto::BatchExistKeyRequest {
                keys: batch_keys,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .results;
        assert_eq!(&results[..10], &[true; 10]);
        assert!(!results[10]);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_replica_segments_are_unique() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        for index in 0..20 {
            mount_snapshot_segment_named(&source, client_id, &format!("segment_{index}"), index)
                .await;
        }
        let started = MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "replica_uniqueness_test_key".into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 10,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 10);
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "replica_uniqueness_test_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let replicas = snapshot_child_replicas(&source, "replica_uniqueness_test_key").await;
        assert_eq!(replicas.len(), 10);
        let unique = replicas
            .iter()
            .map(|replica| replica.segment_name.clone())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), 10);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_020000_001",
            "segment_0",
            1,
            20,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "replica_uniqueness_test_key").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_replication_factor_two_single_segment() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "single_segment").await;
        put_snapshot_object_custom(
            &source,
            client_id,
            "replication_factor_two_single_segment",
            1024,
            2,
        )
        .await;
        let replicas =
            snapshot_child_replicas(&source, "replication_factor_two_single_segment").await;
        assert_eq!(replicas.len(), 1);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_020000_002",
            "single_segment",
            1,
            1,
        )
        .await;
        assert_eq!(
            snapshot_child_replicas(&restored, "replication_factor_two_single_segment")
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_cleanup_stale_handles() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_id =
            mount_snapshot_segment_named(&source, client_id, "stale-handles:1", 0).await;
        put_snapshot_child_object(&source, client_id, "segment_object", None, true).await;
        assert_eq!(
            snapshot_child_replicas(&source, "segment_object")
                .await
                .len(),
            1
        );
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_id),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        assert!(!snapshot_child_exists(&source, "segment_object").await);
        snapshot_roundtrip_empty(&provider, "20260806_020000_003").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_unmount_immediate_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment1 = mount_snapshot_segment_named(&source, client_id, "segment1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment2", 1).await;
        put_snapshot_object_preferred(&source, client_id, "key1", "segment1").await;
        put_snapshot_object_preferred(&source, client_id, "key2", "segment2").await;
        assert!(snapshot_child_exists(&source, "key1").await);
        assert!(snapshot_child_exists(&source, "key2").await);
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment1),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        let mut absent = false;
        for _ in 0..50 {
            if !snapshot_child_exists(&source, "key1").await {
                absent = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(absent, "key1 should be removed after unmount");
        assert!(snapshot_child_exists(&source, "key2").await);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_020000_004",
            "segment2",
            1,
            1,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "key2").await);
        assert!(!snapshot_child_exists(&restored, "key1").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_readable_after_partial_unmount() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment1 = mount_snapshot_segment_named(&source, client_id, "segment1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment2", 1).await;
        put_snapshot_object_custom(&source, client_id, "replicated_key", 1024 * 1024, 2).await;
        let replicas = snapshot_child_replicas(&source, "replicated_key").await;
        assert_eq!(replicas.len(), 2);
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment1),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        assert!(snapshot_child_exists(&source, "replicated_key").await);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_020000_005",
            "segment2",
            1,
            1,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "replicated_key").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_remove_leased_object() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-remove-lease:1").await;
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        assert!(snapshot_child_exists(&source, "test_key").await);
        let blocked = MasterService::remove(
            &source,
            Request::new(proto::RemoveRequest {
                key: "test_key".into(),
                force: false,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);
        MasterService::remove(
            &source,
            Request::new(proto::RemoveRequest {
                key: "test_key".into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        snapshot_roundtrip_empty(&provider, "20260806_020000_006").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_remove_all_leased_object() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-remove-all-lease:1").await;
        for index in 0..10 {
            let key = format!("test_key{index}");
            put_snapshot_child_object(&source, client_id, &key, None, true).await;
            if index >= 5 {
                assert!(snapshot_child_exists(&source, &key).await);
            }
        }
        let removed = MasterService::remove_all(
            &source,
            Request::new(proto::RemoveAllRequest {
                force: false,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .removed_count;
        assert_eq!(removed, 5);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_020000_007",
            "snapshot-remove-all-lease:1",
            5,
            1,
        )
        .await;
        for index in 5..10 {
            assert!(snapshot_child_exists(&restored, &format!("test_key{index}")).await);
        }
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_remove_soft_pin_object() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-soft-pin:1").await;
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        assert!(snapshot_child_exists(&source, "test_key").await);
        MasterService::remove(
            &source,
            Request::new(proto::RemoveRequest {
                key: "test_key".into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        snapshot_roundtrip_empty(&provider, "20260806_020000_008").await;
    }

    async fn snapshot_batch_replica_clear(
        service: &MasterServiceImpl,
        client_id: Uuid,
        keys: &[&str],
        segment_name: &str,
    ) -> Vec<String> {
        MasterService::batch_replica_clear(
            service,
            Request::new(proto::BatchReplicaClearRequest {
                object_keys: keys.iter().map(|key| key.to_string()).collect(),
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: segment_name.to_string(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .cleared_keys
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_all_segments() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-clear-all:1").await;
        let keys = [
            "batch_clear_key_0",
            "batch_clear_key_1",
            "batch_clear_key_2",
            "batch_clear_key_3",
            "batch_clear_key_4",
        ];
        snapshot_put_keys(&source, client_id, &keys, false).await;
        let cleared = snapshot_batch_replica_clear(&source, client_id, &keys, "").await;
        assert_eq!(cleared.len(), 5);
        snapshot_roundtrip_empty(&provider, "20260806_030000_001").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_specific_segment() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment2", 1).await;
        MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "segment_specific_key".into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "segment1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "segment_specific_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let cleared =
            snapshot_batch_replica_clear(&source, client_id, &["segment_specific_key"], "segment1")
                .await;
        assert_eq!(cleared, vec!["segment_specific_key".to_string()]);
        snapshot_roundtrip_empty(&provider, "20260806_030000_002").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_lease_active() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-clear-lease:1").await;
        put_snapshot_child_object(&source, client_id, "lease_active_key", None, true).await;
        assert_eq!(
            snapshot_child_replicas(&source, "lease_active_key")
                .await
                .len(),
            1
        );
        let cleared =
            snapshot_batch_replica_clear(&source, client_id, &["lease_active_key"], "").await;
        assert!(cleared.is_empty());
        assert!(snapshot_child_exists(&source, "lease_active_key").await);

        snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_030000_003",
            "snapshot-clear-lease:1",
            1,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_different_client() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id1 = Uuid::new_v4();
        let client_id2 = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id1, "snapshot-clear-client:1").await;
        put_snapshot_child_object(&source, client_id1, "client_specific_key", None, true).await;
        let cleared =
            snapshot_batch_replica_clear(&source, client_id2, &["client_specific_key"], "").await;
        assert!(cleared.is_empty());
        assert!(snapshot_child_exists(&source, "client_specific_key").await);

        snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id1,
            "20260806_030000_004",
            "snapshot-clear-client:1",
            1,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_nonexistent_keys() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-clear-missing:1").await;
        let cleared = snapshot_batch_replica_clear(
            &source,
            client_id,
            &["non_existent_key1", "non_existent_key2"],
            "",
        )
        .await;
        assert!(cleared.is_empty());
        snapshot_roundtrip_empty(&provider, "20260806_030000_005").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_empty_keys() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-clear-empty:1").await;
        let cleared = snapshot_batch_replica_clear(&source, client_id, &[], "").await;
        assert!(cleared.is_empty());
        snapshot_roundtrip_empty(&provider, "20260806_030000_006").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_empty_string_keys() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id, "snapshot-clear-empty-string:1").await;
        put_snapshot_child_object(&source, client_id, "valid_key", None, true).await;
        let cleared = snapshot_batch_replica_clear(
            &source,
            client_id,
            &["", "valid_key", "", "another_empty"],
            "",
        )
        .await;
        assert_eq!(cleared, vec!["valid_key".to_string()]);
        snapshot_roundtrip_empty(&provider, "20260806_030000_007").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_replica_clear_mixed_scenario() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id1 = Uuid::new_v4();
        let client_id2 = Uuid::new_v4();
        mount_snapshot_child_segment(&source, client_id1, "snapshot-clear-mixed:1").await;
        put_snapshot_child_object(&source, client_id1, "mixed_key1", None, true).await;
        put_snapshot_child_object(&source, client_id1, "mixed_key2", None, true).await;
        put_snapshot_child_object(&source, client_id2, "mixed_key3", None, true).await;
        let cleared = snapshot_batch_replica_clear(
            &source,
            client_id1,
            &["mixed_key1", "mixed_key2", "mixed_key3", "non_existent", ""],
            "",
        )
        .await;
        assert_eq!(cleared.len(), 2);
        assert!(!snapshot_child_exists(&source, "mixed_key1").await);
        assert!(!snapshot_child_exists(&source, "mixed_key2").await);
        assert!(snapshot_child_exists(&source, "mixed_key3").await);

        snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id2,
            "20260806_030000_008",
            "snapshot-clear-mixed:1",
            1,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_copy_start() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        for index in 0..4 {
            mount_snapshot_segment_named(
                &source,
                client_id,
                &format!("segment_{}", index + 1),
                index,
            )
            .await;
        }
        let missing = MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "non_existent_key".into(),
                source: "segment_1".into(),
                targets: vec!["segment_2".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        let started = MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                targets: vec!["segment_2".into(), "segment_3".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.targets.len(), 2);
        MasterService::copy_end(
            &source,
            Request::new(proto::CopyEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let skip_started = MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                targets: vec!["segment_3".into(), "segment_4".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(skip_started.targets.len(), 1);
        MasterService::copy_end(
            &source,
            Request::new(proto::CopyEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 4);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_040000_001",
            "segment_1",
            1,
            4,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "test_key").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_move_start() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        for index in 0..3 {
            mount_snapshot_segment_named(
                &source,
                client_id,
                &format!("segment_{}", index + 1),
                index,
            )
            .await;
        }
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        let same = MasterService::move_start(
            &source,
            Request::new(proto::MoveStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                target: "segment_1".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(same.code(), tonic::Code::InvalidArgument);
        let started = MasterService::move_start(
            &source,
            Request::new(proto::MoveStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                target: "segment_2".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.target.unwrap().segment_name, "segment_2");
        MasterService::move_end(
            &source,
            Request::new(proto::MoveEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 1);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_040000_002",
            "segment_2",
            1,
            3,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "test_key").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_copy_revoke() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_1 = mount_snapshot_segment_named(&source, client_id, "segment_1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_2", 1).await;
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                targets: vec!["segment_2".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_1),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        let revoked = MasterService::copy_revoke(
            &source,
            Request::new(proto::CopyRevokeRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await;
        assert!(revoked.is_ok() || revoked.unwrap_err().code() == tonic::Code::NotFound);
        assert!(!snapshot_child_exists(&source, "test_key").await);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_040000_003",
            "segment_2",
            0,
            1,
        )
        .await;
        assert!(snapshot_child_keys(&restored).await.is_empty());
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_move_revoke() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_1 = mount_snapshot_segment_named(&source, client_id, "segment_1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_2", 1).await;
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        MasterService::move_start(
            &source,
            Request::new(proto::MoveStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                target: "segment_2".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_1),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        let revoked = MasterService::move_revoke(
            &source,
            Request::new(proto::MoveRevokeRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await;
        assert!(revoked.is_ok() || revoked.unwrap_err().code() == tonic::Code::NotFound);
        assert!(!snapshot_child_exists(&source, "test_key").await);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_040000_004",
            "segment_2",
            0,
            1,
        )
        .await;
        assert!(snapshot_child_keys(&restored).await.is_empty());
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_copy_end() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_1 = mount_snapshot_segment_named(&source, client_id, "segment_1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_2", 1).await;
        let segment_3 = mount_snapshot_segment_named(&source, client_id, "segment_3", 2).await;
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                targets: vec!["segment_2".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::copy_end(
            &source,
            Request::new(proto::CopyEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 2);

        MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                targets: vec!["segment_3".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_1),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        let gone = MasterService::copy_end(
            &source,
            Request::new(proto::CopyEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(gone.code(), tonic::Code::FailedPrecondition);
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 1);

        MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_2".into(),
                targets: vec!["segment_3".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_3),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        let target_gone = MasterService::copy_end(
            &source,
            Request::new(proto::CopyEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(target_gone.code(), tonic::Code::FailedPrecondition);
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 1);

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_040000_005",
            "segment_2",
            1,
            1,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "test_key").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_move_end() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_1", 0).await;
        let segment_2 = mount_snapshot_segment_named(&source, client_id, "segment_2", 1).await;
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_1").await;
        MasterService::move_start(
            &source,
            Request::new(proto::MoveStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_1".into(),
                target: "segment_2".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::move_end(
            &source,
            Request::new(proto::MoveEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 1);

        MasterService::move_start(
            &source,
            Request::new(proto::MoveStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                source: "segment_2".into(),
                target: "segment_1".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_2),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        let ended = MasterService::move_end(
            &source,
            Request::new(proto::MoveEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await;
        let ended_code = ended.as_ref().err().map(|status| status.code());
        assert!(
            ended.is_ok()
                || matches!(
                    ended_code,
                    Some(tonic::Code::FailedPrecondition) | Some(tonic::Code::NotFound)
                )
        );

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_040000_006",
            "segment_1",
            0,
            1,
        )
        .await;
        assert!(snapshot_child_keys(&restored).await.is_empty());
    }

    async fn snapshot_task_roundtrip(
        provider: &CatalogBackedSnapshotProvider,
        source: &MasterServiceImpl,
        client_id: Uuid,
        snapshot_id: &str,
        segment_name: &str,
        expected_objects: usize,
        expected_segments: usize,
        expected_tasks: usize,
    ) -> MasterServiceImpl {
        let first = source.capture_loaded_snapshot(snapshot_id);
        assert_eq!(first.objects.len(), expected_objects);
        assert_eq!(first.segments.len(), expected_segments);
        assert_eq!(first.tasks.len(), expected_tasks);
        publish_service_snapshot(provider, source, snapshot_id, 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        let second = restored.capture_loaded_snapshot(format!("{snapshot_id}-second"));
        assert_eq!(
            snapshot_state_probe(&second),
            snapshot_state_probe(&first),
            "second save must match first"
        );
        mount_snapshot_child_segment(&restored, client_id, segment_name).await;
        restored
    }

    async fn snapshot_roundtrip_no_remount(
        provider: &CatalogBackedSnapshotProvider,
        source: &MasterServiceImpl,
        snapshot_id: &str,
        expected_objects: usize,
        expected_segments: usize,
    ) {
        let first = source.capture_loaded_snapshot(snapshot_id);
        assert_eq!(first.objects.len(), expected_objects);
        assert_eq!(first.segments.len(), expected_segments);
        publish_service_snapshot(provider, source, snapshot_id, 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        let second = restored.capture_loaded_snapshot(format!("{snapshot_id}-second"));
        assert_eq!(
            snapshot_state_probe(&second),
            snapshot_state_probe(&first),
            "second save must match first"
        );
    }

    async fn snapshot_roundtrip_any_state(
        provider: &CatalogBackedSnapshotProvider,
        source: &MasterServiceImpl,
        snapshot_id: &str,
    ) {
        let all_keys = MasterService::get_all_keys(
            source,
            Request::new(proto::GetAllKeysRequest {
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .keys;
        snapshot_grant_leases(source, &all_keys).await;
        let first = source.capture_loaded_snapshot(snapshot_id);
        publish_service_snapshot(provider, source, snapshot_id, 1);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();
        let second = restored.capture_loaded_snapshot(format!("{snapshot_id}-second"));
        assert_eq!(
            snapshot_state_probe(&second),
            snapshot_state_probe(&first),
            "second save must match first"
        );
    }

    async fn snapshot_soft_pin_service(allow_evict_soft_pinned: bool) -> MasterServiceImpl {
        MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(200),
            soft_pin_ttl: std::time::Duration::from_secs(10),
            allow_evict_soft_pinned_objects: allow_evict_soft_pinned,
            eviction_interval: std::time::Duration::from_millis(5),
            ..Default::default()
        })
    }

    async fn snapshot_put_soft_pinned(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        size: u64,
    ) -> bool {
        let Ok(response) = MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                slice_length: size,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    with_soft_pin: true,
                    ..Default::default()
                }),
            }),
        )
        .await
        else {
            return false;
        };
        if response.into_inner().replicas.is_empty() {
            return false;
        }
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        true
    }

    async fn snapshot_put_unpinned(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        size: u64,
    ) -> bool {
        let Ok(response) = MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                slice_length: size,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    ..Default::default()
                }),
            }),
        )
        .await
        else {
            return false;
        };
        if response.into_inner().replicas.is_empty() {
            return false;
        }
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        true
    }

    async fn mount_snapshot_segment_with_endpoint(
        service: &MasterServiceImpl,
        client_id: Uuid,
        segment_name: &str,
        index: u64,
        te_endpoint: &str,
    ) {
        MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: segment_name.into(),
                size: SNAPSHOT_CHILD_SEGMENT_SIZE,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE + index * 0x1000000,
                te_endpoint: te_endpoint.into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn snapshot_batch_query_ip(
        service: &MasterServiceImpl,
        client_ids: &[Uuid],
    ) -> std::collections::HashMap<String, Vec<String>> {
        MasterService::batch_query_ip(
            service,
            Request::new(proto::BatchQueryIpRequest {
                client_ids: client_ids.iter().map(|id| uuid_to_proto(*id)).collect(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .ips
        .into_iter()
        .map(|(client, ip_list)| (client, ip_list.addresses))
        .collect()
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_with_endpoint(
            &source,
            client_id,
            "test_segment",
            0,
            "127.0.0.1:12345",
        )
        .await;
        let ips = snapshot_batch_query_ip(&source, &[client_id]).await;
        assert_eq!(
            ips.get(&client_id.to_string()),
            Some(&vec!["127.0.0.1".to_string()])
        );
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_060000_001", 0, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip_multiple_segments() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_with_endpoint(&source, client_id, "segment1", 0, "127.0.0.1:12345")
            .await;
        mount_snapshot_segment_with_endpoint(&source, client_id, "segment2", 1, "127.0.0.2:23456")
            .await;
        let ips = snapshot_batch_query_ip(&source, &[client_id]).await;
        assert_eq!(
            ips.get(&client_id.to_string()).map(|list| list.len()),
            Some(2)
        );
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_060000_002", 0, 2).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip_empty_client_id() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_with_endpoint(
            &source,
            client_id,
            "test_segment",
            0,
            "127.0.0.1:12345",
        )
        .await;
        let ips = snapshot_batch_query_ip(&source, &[]).await;
        assert!(ips.is_empty());
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_060000_003", 0, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip_bracketed_ipv6() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_with_endpoint(&source, client_id, "test_segment", 0, "[::1]:17813")
            .await;
        let ips = snapshot_batch_query_ip(&source, &[client_id]).await;
        assert_eq!(
            ips.get(&client_id.to_string()),
            Some(&vec!["::1".to_string()])
        );
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_060000_004", 0, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip_link_local_with_scope() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_with_endpoint(
            &source,
            client_id,
            "test_segment",
            0,
            "fe80::a236:bcff:fecb:a1be%eno2:15773",
        )
        .await;
        let ips = snapshot_batch_query_ip(&source, &[client_id]).await;
        assert_eq!(
            ips.get(&client_id.to_string()),
            Some(&vec!["fe80::a236:bcff:fecb:a1be%eno2".to_string()])
        );
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_060000_005", 0, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip_ipv6_no_port() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_with_endpoint(
            &source,
            client_id,
            "test_segment",
            0,
            "[2001:db8::1]",
        )
        .await;
        let ips = snapshot_batch_query_ip(&source, &[client_id]).await;
        assert_eq!(
            ips.get(&client_id.to_string()),
            Some(&vec!["2001:db8::1".to_string()])
        );
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_060000_006", 0, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip_mixed_ipv4_ipv6() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_with_endpoint(
            &source,
            client_id,
            "segment1",
            0,
            "192.168.1.1:12345",
        )
        .await;
        mount_snapshot_segment_with_endpoint(&source, client_id, "segment2", 1, "[::1]:17813")
            .await;
        let ips = snapshot_batch_query_ip(&source, &[client_id]).await;
        let addresses = ips.get(&client_id.to_string()).expect("client ips");
        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"192.168.1.1".to_string()));
        assert!(addresses.contains(&"::1".to_string()));
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_060000_007", 0, 2).await;
    }

    async fn snapshot_grant_leases(service: &MasterServiceImpl, keys: &[String]) {
        for key in keys {
            let _ = snapshot_child_exists(service, key).await;
        }
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_concurrent_mount_unmount() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = Arc::new(MasterServiceImpl::default());
        let mut tasks = Vec::new();
        for task_index in 0..4 {
            let source = Arc::clone(&source);
            tasks.push(tokio::spawn(async move {
                let client_id = Uuid::new_v4();
                for _ in 0..100 {
                    let mounted = MasterService::mount_segment(
                        &*source,
                        Request::new(proto::MountSegmentRequest {
                            client_id: Some(uuid_to_proto(client_id)),
                            segment_name: format!("segment_{task_index}"),
                            size: SNAPSHOT_CHILD_SEGMENT_SIZE,
                            base_addr: SNAPSHOT_CHILD_SEGMENT_BASE + task_index as u64 * 0x1000000,
                            te_endpoint: String::new(),
                            protocol: String::new(),
                            host_id: String::new(),
                        }),
                    )
                    .await
                    .unwrap()
                    .into_inner()
                    .segment_id
                    .unwrap();
                    MasterService::unmount_segment(
                        &*source,
                        Request::new(proto::UnmountSegmentRequest {
                            segment_id: Some(mounted),
                            client_id: Some(uuid_to_proto(client_id)),
                        }),
                    )
                    .await
                    .unwrap();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_070000_001", 0, 0).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_concurrent_write_and_remove_all() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = Arc::new(MasterServiceImpl::default());
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "concurrent-write:1", 0).await;
        let mut writers = Vec::new();
        for thread_index in 0..4 {
            let source = Arc::clone(&source);
            writers.push(tokio::spawn(async move {
                for object_index in 0..100 {
                    put_snapshot_object_custom(
                        &source,
                        client_id,
                        &format!("key_{thread_index}_{object_index}"),
                        1024,
                        1,
                    )
                    .await;
                }
            }));
        }
        for writer in writers {
            writer.await.unwrap();
        }
        let mut keys = Vec::new();
        for thread_index in 0..4 {
            for object_index in 0..100 {
                keys.push(format!("key_{thread_index}_{object_index}"));
            }
        }
        snapshot_grant_leases(&source, &keys).await;
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_070000_002", 400, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_concurrent_read_and_remove_all() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = Arc::new(MasterServiceImpl::default());
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "concurrent-read:1", 0).await;
        let keys = (0..1000)
            .map(|index| format!("pre_key_{index}"))
            .collect::<Vec<_>>();
        for key in &keys {
            put_snapshot_object_custom(&source, client_id, key, 1024, 1).await;
        }
        let mut readers = Vec::new();
        for _ in 0..4 {
            let source = Arc::clone(&source);
            let keys = keys.clone();
            readers.push(tokio::spawn(async move {
                for key in &keys {
                    let _ = MasterService::get_replica_list(
                        &*source,
                        Request::new(proto::GetReplicaListRequest {
                            key: key.clone(),
                            tenant_id: String::new(),
                        }),
                    )
                    .await;
                }
            }));
        }
        for reader in readers {
            reader.await.unwrap();
        }
        snapshot_grant_leases(&source, &keys).await;
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_070000_003", 1000, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_concurrent_remove_all_operations() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "concurrent-remove-all:1", 0).await;
        let keys = (0..1000)
            .map(|index| format!("pre_key_{index}"))
            .collect::<Vec<_>>();
        for key in &keys {
            put_snapshot_object_custom(&source, client_id, key, 1024, 1).await;
        }
        snapshot_grant_leases(&source, &keys).await;
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_070000_004", 1000, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_concurrent_mount_local_disk_segment() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = Arc::new(MasterServiceImpl::with_runtime_config(
            MasterRuntimeConfig {
                enable_offload: true,
                ..Default::default()
            },
        ));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let source = Arc::clone(&source);
            tasks.push(tokio::spawn(async move {
                let client_id = Uuid::new_v4();
                let storage_id = Uuid::new_v4();
                let session_id = Uuid::new_v4();
                MasterService::mount_local_disk_segment(
                    &*source,
                    Request::new(proto::MountLocalDiskSegmentRequest {
                        client_id: Some(uuid_to_proto(client_id)),
                        enable_offloading: false,
                        storage_id: Some(uuid_to_proto(storage_id)),
                        recovery_complete: false,
                        recovery_session_id: Some(uuid_to_proto(session_id)),
                    }),
                )
                .await
                .unwrap();
                MasterService::mount_local_disk_segment(
                    &*source,
                    Request::new(proto::MountLocalDiskSegmentRequest {
                        client_id: Some(uuid_to_proto(client_id)),
                        enable_offloading: true,
                        storage_id: Some(uuid_to_proto(storage_id)),
                        recovery_complete: true,
                        recovery_session_id: Some(uuid_to_proto(session_id)),
                    }),
                )
                .await
                .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_070000_005", 0, 0).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_single_slice_multi_replica_flow() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        for index in 0..3 {
            mount_snapshot_segment_named(&source, client_id, &format!("segment_{index}"), index)
                .await;
        }
        let started = MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "multi_slice_object".into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 3,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 3);
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "multi_slice_object".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            snapshot_child_replicas(&source, "multi_slice_object")
                .await
                .len(),
            3
        );

        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_080000_001",
            "segment_0",
            1,
            3,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "multi_slice_object").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_unmount_segment_performance() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_id = MasterService::mount_segment(
            &source,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: "perf_test_segment".into(),
                size: 256 * 1024 * 1024,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();
        for index in 0..1000 {
            put_snapshot_object_custom(&source, client_id, &format!("key_{index}"), 1024, 1).await;
        }
        let started = std::time::Instant::now();
        MasterService::unmount_segment(
            &source,
            Request::new(proto::UnmountSegmentRequest {
                segment_id: Some(segment_id),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        )
        .await
        .unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_080000_002", 0, 0).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_batch_query_ip_multiple_segments_empty_te_endpoint() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment2", 1).await;
        let ips = snapshot_batch_query_ip(&source, &[client_id]).await;
        assert!(ips.contains_key(&client_id.to_string()));
        assert!(ips.get(&client_id.to_string()).unwrap().is_empty());
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_080000_003", 0, 2).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_fetch_tasks_returns_assigned_only_and_drains_queue() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let other_client = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_0", 0).await;
        mount_snapshot_segment_named(&source, other_client, "segment_1", 1).await;
        put_snapshot_object_preferred(&source, client_id, "fetch_tasks_key_0", "segment_0").await;
        assert_eq!(
            snapshot_child_replicas(&source, "fetch_tasks_key_0")
                .await
                .len(),
            1
        );
        MasterService::create_copy_task(
            &source,
            Request::new(proto::CreateCopyTaskRequest {
                key: "fetch_tasks_key_0".into(),
                tenant_id: String::new(),
                targets: vec!["segment_1".into()],
            }),
        )
        .await
        .unwrap();
        MasterService::create_move_task(
            &source,
            Request::new(proto::CreateMoveTaskRequest {
                key: "fetch_tasks_key_0".into(),
                source: "segment_0".into(),
                target: "segment_1".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let fetched0 = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(client_id)),
                batch_size: 16,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(fetched0.len(), 2);
        let fetched1 = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(other_client)),
                batch_size: 16,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert!(fetched1.is_empty());
        let fetched0_again = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(client_id)),
                batch_size: 16,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert!(fetched0_again.is_empty());
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_080000_004",
            "segment_0",
            1,
            2,
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_evict_object() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(2000),
            eviction_interval: std::time::Duration::from_millis(5),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        MasterService::mount_segment(
            &source,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: "test_segment".into(),
                size: 1024 * 1024 * 16 * 15,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let mut success_puts = 0usize;
        for index in 0..16_434 {
            let key = format!("test_key{index}");
            if snapshot_put_soft_pinned(&source, client_id, &key, 1024 * 15).await {
                success_puts += 1;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
        assert!(success_puts > 16_384);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_090000_001").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_try_evict_leased_object() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(500),
            eviction_interval: std::time::Duration::from_millis(5),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        MasterService::mount_segment(
            &source,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: "test_segment".into(),
                size: 1024 * 1024 * 16,
                base_addr: SNAPSHOT_CHILD_SEGMENT_BASE,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let mut success_puts = 0usize;
        let mut failed_puts = 0usize;
        for index in 0..26 {
            let key = format!("test_key{index}");
            if snapshot_put_soft_pinned(&source, client_id, &key, 1024 * 1024).await {
                assert!(snapshot_child_exists(&source, &key).await);
                success_puts += 1;
            } else {
                failed_puts += 1;
            }
        }
        assert!(success_puts > 0);
        assert!(failed_puts > 0);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_090000_002").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_soft_pin_objects_can_be_evicted() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = snapshot_soft_pin_service(true).await;
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        let mut success_puts = 0usize;
        for index in 0..66 {
            let key = format!("test_key{index}");
            if snapshot_put_soft_pinned(&source, client_id, &key, 1024 * 1024).await {
                success_puts += 1;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        assert!(success_puts > 16);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_090000_003").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_soft_pin_objects_not_allow_evict() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = snapshot_soft_pin_service(false).await;
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        let mut success_keys = Vec::new();
        for index in 0..66 {
            let key = format!("test_key{index}");
            if snapshot_put_soft_pinned(&source, client_id, &key, 1024 * 1024).await {
                success_keys.push(key);
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        assert!(success_keys.len() <= 17);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_090000_004").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_soft_pin_objects_not_evicted_before_other_objects() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(200),
            soft_pin_ttl: std::time::Duration::from_secs(10),
            allow_evict_soft_pinned_objects: true,
            eviction_ratio: 0.5,
            eviction_interval: std::time::Duration::from_millis(5),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        for _ in 0..5 {
            for index in 0..2 {
                assert!(
                    snapshot_put_soft_pinned(
                        &source,
                        client_id,
                        &format!("pin_key{index}"),
                        1024 * 1024,
                    )
                    .await
                );
            }
            let mut failed_puts = 0usize;
            for index in 0..20 {
                if !snapshot_put_unpinned(&source, client_id, &format!("key{index}"), 1024 * 1024)
                    .await
                {
                    failed_puts += 1;
                }
            }
            assert!(failed_puts > 0);
            tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
            for index in 0..2 {
                assert!(snapshot_child_exists(&source, &format!("pin_key{index}")).await);
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let _ = MasterService::remove_all(
                &source,
                Request::new(proto::RemoveAllRequest {
                    force: true,
                    tenant_id: String::new(),
                }),
            )
            .await;
        }
        snapshot_roundtrip_any_state(&provider, &source, "20260806_100000_001").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_soft_pin_extended_on_get() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(200),
            soft_pin_ttl: std::time::Duration::from_millis(1000),
            allow_evict_soft_pinned_objects: true,
            eviction_ratio: 0.5,
            eviction_interval: std::time::Duration::from_millis(5),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        for _ in 0..3 {
            for index in 0..2 {
                assert!(
                    snapshot_put_soft_pinned(
                        &source,
                        client_id,
                        &format!("pin_key{index}"),
                        1024 * 1024,
                    )
                    .await
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            for index in 0..2 {
                assert!(snapshot_child_exists(&source, &format!("pin_key{index}")).await);
            }
            let mut failed_puts = 0usize;
            for index in 0..16 {
                if !snapshot_put_unpinned(&source, client_id, &format!("key{index}"), 1024 * 1024)
                    .await
                {
                    failed_puts += 1;
                }
            }
            assert!(failed_puts > 0);
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            for index in 0..2 {
                assert!(snapshot_child_exists(&source, &format!("pin_key{index}")).await);
            }
            let _ = MasterService::remove_all(
                &source,
                Request::new(proto::RemoveAllRequest {
                    force: true,
                    tenant_id: String::new(),
                }),
            )
            .await;
        }
        snapshot_roundtrip_any_state(&provider, &source, "20260806_100000_002").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_protect_copy_move_source_from_eviction() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(100),
            client_live_ttl: std::time::Duration::from_secs(600),
            eviction_interval: std::time::Duration::from_millis(5),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_1", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_2", 1).await;
        put_snapshot_object_preferred(&source, client_id, "copy_key", "segment_1").await;
        put_snapshot_object_preferred(&source, client_id, "move_key", "segment_1").await;
        MasterService::copy_start(
            &source,
            Request::new(proto::CopyStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "copy_key".into(),
                source: "segment_1".into(),
                targets: vec!["segment_2".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::move_start(
            &source,
            Request::new(proto::MoveStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "move_key".into(),
                source: "segment_1".into(),
                target: "segment_2".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        for index in 0..4096 {
            let key = format!("test_key_{index}");
            if snapshot_put_unpinned(&source, client_id, &key, 1024 * 1024).await {
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
        let _ = MasterService::remove_all(
            &source,
            Request::new(proto::RemoveAllRequest {
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await;
        assert!(
            MasterService::copy_end(
                &source,
                Request::new(proto::CopyEndRequest {
                    client_id: Some(uuid_to_proto(client_id)),
                    key: "copy_key".into(),
                    tenant_id: String::new(),
                }),
            )
            .await
            .is_ok()
        );
        assert!(
            MasterService::move_end(
                &source,
                Request::new(proto::MoveEndRequest {
                    client_id: Some(uuid_to_proto(client_id)),
                    key: "move_key".into(),
                    tenant_id: String::new(),
                }),
            )
            .await
            .is_ok()
        );
        snapshot_roundtrip_any_state(&provider, &source, "20260806_100000_003").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_offload_object_heartbeat() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "hb-mem", 0).await;
        let storage_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        MasterService::mount_local_disk_segment(
            &source,
            Request::new(proto::MountLocalDiskSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                enable_offloading: false,
                storage_id: Some(uuid_to_proto(storage_id)),
                recovery_complete: false,
                recovery_session_id: Some(uuid_to_proto(session_id)),
            }),
        )
        .await
        .unwrap();
        MasterService::mount_local_disk_segment(
            &source,
            Request::new(proto::MountLocalDiskSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                enable_offloading: false,
                storage_id: Some(uuid_to_proto(storage_id)),
                recovery_complete: true,
                recovery_session_id: Some(uuid_to_proto(session_id)),
            }),
        )
        .await
        .unwrap();
        for index in 0..3000 {
            put_snapshot_object_preferred(&source, client_id, &format!("hb-p1-{index}"), "hb-mem")
                .await;
        }
        let first = MasterService::offload_object_heartbeat(
            &source,
            Request::new(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(uuid_to_proto(client_id)),
                enable_offloading: true,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(first.len(), 0);
        for index in 0..3000 {
            put_snapshot_object_preferred(&source, client_id, &format!("hb-p2-{index}"), "hb-mem")
                .await;
        }
        let second = MasterService::offload_object_heartbeat(
            &source,
            Request::new(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(uuid_to_proto(client_id)),
                enable_offloading: true,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(second.len(), 3000);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_110000_001").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_put_start_expiring() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            put_start_discard_timeout: std::time::Duration::from_millis(100),
            put_start_release_timeout: std::time::Duration::from_millis(200),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        for index in 0..3 {
            mount_snapshot_segment_named(&source, client_id, &format!("segment_{index}"), index)
                .await;
        }
        let started = MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key_1".into(),
                slice_length: 6 * 1024 * 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 3,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 3);
        let duplicate = MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key_1".into(),
                slice_length: 6 * 1024 * 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 3,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(duplicate.code(), tonic::Code::AlreadyExists);
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key_1".into(),
                slice_length: 6 * 1024 * 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 3,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key_1".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert!(snapshot_child_exists(&source, "test_key_1").await);
        let blocked = MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "test_key_2".into(),
                slice_length: 6 * 1024 * 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 3,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(blocked.code(), tonic::Code::ResourceExhausted);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_110000_002").await;
    }

    fn snapshot_ssd_service(root: &tempfile::TempDir) -> MasterServiceImpl {
        MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            storage_fs_dir: root.path().to_string_lossy().into_owned(),
            cluster_id: "ssd-snapshot-cluster".into(),
            enable_disk_eviction: true,
            quota_bytes: 1024 * 1024 * 1024,
            ..Default::default()
        })
    }

    async fn snapshot_ssd_put_start(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
    ) -> Vec<proto::ReplicaDescriptor> {
        let started = MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: key.into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        started.replicas
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_ssd_put_end_both_replica() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = snapshot_ssd_service(&root);
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        let replicas = snapshot_ssd_put_start(&source, client_id, "disk_key").await;
        assert_eq!(replicas.len(), 2);
        assert!(
            replicas
                .iter()
                .any(|r| r.replica_type == proto::replica_descriptor::ReplicaType::Memory as i32)
        );
        assert!(
            replicas
                .iter()
                .any(|r| r.replica_type == proto::replica_descriptor::ReplicaType::Disk as i32)
        );
        let blocked = MasterService::get_replica_list(
            &source,
            Request::new(proto::GetReplicaListRequest {
                key: "disk_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "disk_key").await.len(), 2);
        let restored = snapshot_roundtrip_restored_service(
            &provider,
            &source,
            client_id,
            "20260806_120000_001",
            "test_segment",
            1,
            1,
        )
        .await;
        assert!(snapshot_child_exists(&restored, "disk_key").await);
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_ssd_put_revoke_disk_replica() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = snapshot_ssd_service(&root);
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        snapshot_ssd_put_start(&source, client_id, "disk_key").await;
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "disk_key").await.len(), 1);
        MasterService::put_revoke(
            &source,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "disk_key").await.len(), 1);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_120000_002").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_ssd_put_revoke_memory_replica() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = snapshot_ssd_service(&root);
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        snapshot_ssd_put_start(&source, client_id, "disk_key").await;
        MasterService::put_revoke(
            &source,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let not_ready = MasterService::get_replica_list(
            &source,
            Request::new(proto::GetReplicaListRequest {
                key: "disk_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(not_ready.code(), tonic::Code::FailedPrecondition);
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(snapshot_child_replicas(&source, "disk_key").await.len(), 1);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_120000_003").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_ssd_put_revoke_both_replica() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = snapshot_ssd_service(&root);
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        snapshot_ssd_put_start(&source, client_id, "disk_key").await;
        MasterService::put_revoke(
            &source,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let not_ready = MasterService::get_replica_list(
            &source,
            Request::new(proto::GetReplicaListRequest {
                key: "disk_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(not_ready.code(), tonic::Code::FailedPrecondition);
        MasterService::put_revoke(
            &source,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let not_found = MasterService::get_replica_list(
            &source,
            Request::new(proto::GetReplicaListRequest {
                key: "disk_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(not_found.code(), tonic::Code::NotFound);
        snapshot_roundtrip_any_state(&provider, &source, "20260806_120000_004").await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_ssd_remove_key() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = snapshot_ssd_service(&root);
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "test_segment", 0).await;
        snapshot_ssd_put_start(&source, client_id, "disk_key").await;
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: "disk_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::remove(
            &source,
            Request::new(proto::RemoveRequest {
                key: "disk_key".into(),
                force: false,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let not_found = MasterService::get_replica_list(
            &source,
            Request::new(proto::GetReplicaListRequest {
                key: "disk_key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(not_found.code(), tonic::Code::NotFound);
        snapshot_roundtrip_no_remount(&provider, &source, "20260806_120000_005", 0, 1).await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_create_copy_task() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        for index in 0..3 {
            mount_snapshot_segment_named(&source, client_id, &format!("segment_{index}"), index)
                .await;
        }
        put_snapshot_object_preferred(&source, client_id, "test_key_1", "segment_0").await;
        assert_eq!(
            snapshot_child_replicas(&source, "test_key_1").await.len(),
            1
        );
        let task = MasterService::create_copy_task(
            &source,
            Request::new(proto::CreateCopyTaskRequest {
                key: "test_key_1".into(),
                tenant_id: String::new(),
                targets: vec!["segment_1".into(), "segment_2".into()],
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .task_id
        .unwrap();
        assert!(
            task.high != 0 || task.low != 0,
            "copy task id must be non-zero"
        );
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_001",
            "segment_0",
            1,
            3,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_create_move_task() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        for index in 0..3 {
            mount_snapshot_segment_named(&source, client_id, &format!("segment_{index}"), index)
                .await;
        }
        put_snapshot_object_preferred(&source, client_id, "test_key_1", "segment_0").await;
        assert_eq!(
            snapshot_child_replicas(&source, "test_key_1").await.len(),
            1
        );
        let task = MasterService::create_move_task(
            &source,
            Request::new(proto::CreateMoveTaskRequest {
                key: "test_key_1".into(),
                source: "segment_0".into(),
                target: "segment_1".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .task_id
        .unwrap();
        assert!(task.high != 0 || task.low != 0);
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_002",
            "segment_0",
            1,
            3,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_query_task() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_0", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_1", 1).await;
        put_snapshot_object_preferred(&source, client_id, "test_key", "segment_0").await;
        assert_eq!(snapshot_child_replicas(&source, "test_key").await.len(), 1);
        let task = MasterService::create_copy_task(
            &source,
            Request::new(proto::CreateCopyTaskRequest {
                key: "test_key".into(),
                tenant_id: String::new(),
                targets: vec!["segment_1".into()],
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .task_id
        .unwrap();
        let queried = MasterService::query_task(
            &source,
            Request::new(proto::QueryTaskRequest {
                task_id: Some(task),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(queried.id, Some(task));
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_003",
            "segment_0",
            1,
            2,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_fetch_tasks_empty() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_0", 0).await;
        let fetched = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(client_id)),
                batch_size: 16,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert!(fetched.is_empty());
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_004",
            "segment_0",
            0,
            1,
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_fetch_tasks_respects_batch_size() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_0", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_1", 1).await;
        put_snapshot_object_preferred(&source, client_id, "fetch_tasks_key_1", "segment_0").await;
        assert_eq!(
            snapshot_child_replicas(&source, "fetch_tasks_key_1")
                .await
                .len(),
            1
        );
        MasterService::create_copy_task(
            &source,
            Request::new(proto::CreateCopyTaskRequest {
                key: "fetch_tasks_key_1".into(),
                tenant_id: String::new(),
                targets: vec!["segment_1".into()],
            }),
        )
        .await
        .unwrap();
        MasterService::create_move_task(
            &source,
            Request::new(proto::CreateMoveTaskRequest {
                key: "fetch_tasks_key_1".into(),
                source: "segment_0".into(),
                target: "segment_1".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let first = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(client_id)),
                batch_size: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(first.len(), 1);
        let second = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(client_id)),
                batch_size: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(second.len(), 1);
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_005",
            "segment_0",
            1,
            2,
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_update_task_success() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_0", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_1", 1).await;
        put_snapshot_object_preferred(&source, client_id, "update_task_key_success", "segment_0")
            .await;
        assert_eq!(
            snapshot_child_replicas(&source, "update_task_key_success")
                .await
                .len(),
            1
        );
        let task = MasterService::create_copy_task(
            &source,
            Request::new(proto::CreateCopyTaskRequest {
                key: "update_task_key_success".into(),
                tenant_id: String::new(),
                targets: vec!["segment_1".into()],
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .task_id
        .unwrap();
        let fetched = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(client_id)),
                batch_size: 16,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(fetched.len(), 1);
        MasterService::mark_task_to_complete(
            &source,
            Request::new(proto::MarkTaskToCompleteRequest {
                client_id: Some(uuid_to_proto(client_id)),
                request: Some(proto::TaskCompleteRequest {
                    id: Some(task),
                    status: proto::TaskStatus::TaskSuccess as i32,
                    message: "done".into(),
                }),
            }),
        )
        .await
        .unwrap();
        let queried = MasterService::query_task(
            &source,
            Request::new(proto::QueryTaskRequest {
                task_id: Some(task),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(queried.status, proto::TaskStatus::TaskSuccess as i32);
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_006",
            "segment_0",
            1,
            2,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_update_task_wrong_client() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let other_client = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_0", 0).await;
        mount_snapshot_segment_named(&source, client_id, "segment_1", 1).await;
        put_snapshot_object_preferred(&source, client_id, "update_task_wrong_client", "segment_0")
            .await;
        assert_eq!(
            snapshot_child_replicas(&source, "update_task_wrong_client")
                .await
                .len(),
            1
        );
        let task = MasterService::create_move_task(
            &source,
            Request::new(proto::CreateMoveTaskRequest {
                key: "update_task_wrong_client".into(),
                source: "segment_0".into(),
                target: "segment_1".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .task_id
        .unwrap();
        let fetched = MasterService::fetch_tasks(
            &source,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(client_id)),
                batch_size: 16,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks;
        assert_eq!(fetched.len(), 1);
        let rejected = MasterService::mark_task_to_complete(
            &source,
            Request::new(proto::MarkTaskToCompleteRequest {
                client_id: Some(uuid_to_proto(other_client)),
                request: Some(proto::TaskCompleteRequest {
                    id: Some(task),
                    status: proto::TaskStatus::TaskSuccess as i32,
                    message: "should_not_work".into(),
                }),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_007",
            "segment_0",
            1,
            2,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_update_task_not_found() {
        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        mount_snapshot_segment_named(&source, client_id, "segment_0", 0).await;
        let rejected = MasterService::mark_task_to_complete(
            &source,
            Request::new(proto::MarkTaskToCompleteRequest {
                client_id: Some(uuid_to_proto(client_id)),
                request: Some(proto::TaskCompleteRequest {
                    id: Some(proto::Uuid {
                        high: 0xdead,
                        low: 0xbeef,
                    }),
                    status: proto::TaskStatus::TaskFailed as i32,
                    message: "not_found".into(),
                }),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::NotFound);
        snapshot_task_roundtrip(
            &provider,
            &source,
            client_id,
            "20260806_050000_008",
            "segment_0",
            0,
            1,
            0,
        )
        .await;
    }

    fn invalid_integer_task_id_payload() -> Vec<u8> {
        let task = Value::Array(vec![
            Value::from(12345_i32),
            Value::from(0_i32),
            Value::from(0_i32),
            Value::from("payload"),
            Value::from(0_i64),
            Value::from(0_i64),
            Value::from("message"),
            Value::from("assigned"),
        ]);
        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &Value::Array(vec![task])).unwrap();
        zstd::stream::encode_all(Cursor::new(encoded), 3).unwrap()
    }

    #[tokio::test]
    async fn cpp_parity_master_snapshot_codec_empty_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let (provider, object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let descriptor = publish_service_snapshot(&provider, &source, "20260803_030001_001", 1);

        for object_name in ["metadata", "segments", "task_manager"] {
            let payload = object_store
                .download_buffer(&format!("{}{object_name}", descriptor.object_prefix))
                .unwrap();
            assert!(
                !payload.is_empty(),
                "{object_name} payload must be nonempty"
            );
        }

        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();

        let restored_view = restored.capture_loaded_snapshot("restored-empty");
        assert!(restored_view.objects.is_empty());
        assert!(restored_view.segments.is_empty());
        assert!(restored_view.tasks.is_empty());
        let fetched = MasterService::fetch_tasks(
            &restored,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(Uuid::new_v4())),
                batch_size: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(fetched.tasks.is_empty());
    }

    #[tokio::test]
    async fn cpp_parity_master_snapshot_codec_memory_replica_round_trip() {
        const SEGMENT_BASE: u64 = 0x300000000;
        const SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
        const KEY: &str = "memory_replica_key";
        const SEGMENT_NAME: &str = "codec_test_segment";

        let root = tempfile::tempdir().unwrap();
        let (provider, _object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        MasterService::mount_segment(
            &source,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: SEGMENT_NAME.into(),
                size: SEGMENT_SIZE,
                base_addr: SEGMENT_BASE,
                te_endpoint: SEGMENT_NAME.into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::put_start(
            &source,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: KEY.into(),
                slice_length: 1024,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &source,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(client_id)),
                key: KEY.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();

        // Rust's production catalog loader intentionally drops completed
        // objects whose read lease and soft pin have both expired. PutEnd,
        // like C++, grants a zero-duration lease. Establish the ordinary
        // public read lease before capture so this exact replica is a valid
        // recovery candidate rather than bypassing expiry through private
        // state or a test-only codec path.
        let source_replicas = MasterService::get_replica_list(
            &source,
            Request::new(proto::GetReplicaListRequest {
                key: KEY.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas;
        assert_eq!(source_replicas.len(), 1);

        publish_service_snapshot(&provider, &source, "20260803_030002_002", 2);
        let loaded = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap()
            .unwrap();
        let restored = MasterServiceImpl::default();
        restore_loaded_snapshot(&restored, loaded).unwrap();

        // Snapshot addresses and endpoints belong to the old process term and
        // are deliberately restored fail-closed. Rebind the durable segment
        // identity through the public remount path before serving the replica.
        MasterService::mount_segment(
            &restored,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_to_proto(client_id)),
                segment_name: SEGMENT_NAME.into(),
                size: SEGMENT_SIZE,
                base_addr: SEGMENT_BASE,
                te_endpoint: SEGMENT_NAME.into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();

        let replicas = MasterService::get_replica_list(
            &restored,
            Request::new(proto::GetReplicaListRequest {
                key: KEY.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas;
        assert_eq!(replicas.len(), 1);
        assert_eq!(
            replicas[0].replica_type,
            proto::replica_descriptor::ReplicaType::Memory as i32
        );
        assert_eq!(replicas[0].segment_name, SEGMENT_NAME);
        assert_eq!(replicas[0].transport_endpoint, SEGMENT_NAME);
        assert_eq!(replicas[0].size, 1024);
    }

    #[test]
    fn cpp_parity_master_snapshot_codec_corrupt_payloads_fail() {
        let root = tempfile::tempdir().unwrap();
        let (provider, object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let descriptor = publish_service_snapshot(&provider, &source, "20260803_030003_003", 3);
        for (object_name, payload) in [
            ("metadata", [1_u8, 2, 3]),
            ("segments", [4_u8, 5, 6]),
            ("task_manager", [7_u8, 8, 9]),
        ] {
            object_store
                .upload_buffer(
                    &format!("{}{object_name}", descriptor.object_prefix),
                    &payload,
                )
                .unwrap();
        }

        let error = provider
            .load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
            .unwrap_err();
        assert!(
            matches!(error, HaError::Snapshot(_)),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn cpp_parity_master_snapshot_codec_invalid_task_field_type_returns_error_without_unwind() {
        let root = tempfile::tempdir().unwrap();
        let (provider, object_store) = snapshot_codec_provider(&root);
        let source = MasterServiceImpl::default();
        let older_id = "20260803_030004_004";
        publish_service_snapshot(&provider, &source, older_id, 4);
        let malformed = publish_service_snapshot(&provider, &source, "20260803_030005_005", 5);
        object_store
            .upload_buffer(
                &format!("{}task_manager", malformed.object_prefix),
                &invalid_integer_task_id_payload(),
            )
            .unwrap();

        let fallback = catch_unwind(AssertUnwindSafe(|| {
            provider.load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
        }));
        let fallback = fallback
            .expect("integer task id must not unwind")
            .unwrap()
            .unwrap();
        assert_eq!(fallback.snapshot_id, older_id);

        let isolated_root = tempfile::tempdir().unwrap();
        let (isolated_provider, isolated_store) = snapshot_codec_provider(&isolated_root);
        let isolated =
            publish_service_snapshot(&isolated_provider, &source, "20260803_030006_006", 6);
        isolated_store
            .upload_buffer(
                &format!("{}task_manager", isolated.object_prefix),
                &invalid_integer_task_id_payload(),
            )
            .unwrap();
        let isolated_result = catch_unwind(AssertUnwindSafe(|| {
            isolated_provider.load_latest_snapshot(SNAPSHOT_CODEC_CLUSTER)
        }));
        let error = isolated_result
            .expect("integer task id must not unwind")
            .unwrap_err();
        assert!(
            matches!(error, HaError::Snapshot(_)),
            "unexpected error: {error}"
        );
    }

    fn pending_move_task(key: &str, payload: &str) -> TaskEntry {
        let now = chrono::Utc::now();
        let id = Uuid::new_v4();
        TaskEntry {
            info: mooncake_store_core::TaskInfo {
                id,
                task_type: mooncake_store_core::TaskType::ReplicaMove,
                status: TaskStatus::Pending,
                created_at: now,
                last_updated_at: now,
                assigned_client: Some(Uuid::new_v4()),
                message: String::new(),
            },
            key: key.into(),
            payload: payload.into(),
            max_retry_attempts: 1,
        }
    }

    fn disk_object(tenant_id: TenantId, user_key: &str) -> ObjectEntry {
        ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: Uuid::nil(),
                segment_name: "global-disk".into(),
                offset: 0,
                size: 16,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Disk,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 0,
                protocol: String::new(),
            }],
            size: 16,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::nil(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id,
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            disk_allocated_bytes_accounted: 0,
            user_key: user_key.into(),
        }
    }

    #[tokio::test]
    async fn cpp_parity_empty_snapshot_replaces_live_task_state() {
        let service = MasterServiceImpl::default();
        let source_client = Uuid::new_v4();
        let target_client = Uuid::new_v4();
        for (client_id, segment_name) in [(source_client, "seg1"), (target_client, "seg2")] {
            MasterService::mount_segment(
                &service,
                Request::new(proto::MountSegmentRequest {
                    client_id: Some(uuid_to_proto(client_id)),
                    segment_name: segment_name.into(),
                    size: 4096,
                    base_addr: 0x100000000,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                    host_id: String::new(),
                }),
            )
            .await
            .unwrap();
        }
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(uuid_to_proto(source_client)),
                key: "reset-key".into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    nof_replica_num: 0,
                    with_soft_pin: false,
                    with_hard_pin: false,
                    preferred_segment: "seg1".into(),
                    prefer_alloc_in_same_node: false,
                    preferred_segments: Vec::new(),
                    preferred_nof_segments: Vec::new(),
                    data_type: proto::ObjectDataType::Unknown as i32,
                    group_ids: Vec::new(),
                    host_id: String::new(),
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(uuid_to_proto(source_client)),
                key: "reset-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let task_id = MasterService::create_copy_task(
            &service,
            Request::new(proto::CreateCopyTaskRequest {
                key: "reset-key".into(),
                targets: vec!["seg2".into()],
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .task_id
        .unwrap();
        let pending = MasterService::query_task(
            &service,
            Request::new(proto::QueryTaskRequest {
                task_id: Some(task_id.clone()),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(pending.status, proto::TaskStatus::TaskPending as i32);

        restore_loaded_snapshot_state(
            &service.state,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        let query_error = MasterService::query_task(
            &service,
            Request::new(proto::QueryTaskRequest {
                task_id: Some(task_id.clone()),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(query_error.code(), tonic::Code::NotFound);

        let fetched = MasterService::fetch_tasks(
            &service,
            Request::new(proto::FetchTasksRequest {
                client_id: Some(uuid_to_proto(source_client)),
                batch_size: 10,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(fetched.tasks.is_empty());
    }

    #[test]
    fn invalid_snapshot_task_does_not_replace_live_state() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let now = chrono::Utc::now();
        let invalid_task = TaskEntry {
            info: mooncake_store_core::TaskInfo {
                id: Uuid::new_v4(),
                task_type: mooncake_store_core::TaskType::ReplicaCopy,
                status: TaskStatus::Pending,
                created_at: now,
                last_updated_at: now,
                assigned_client: None,
                message: String::new(),
            },
            key: "missing".into(),
            payload: String::new(),
            max_retry_attempts: 1,
        };

        let result = restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![invalid_task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert!(state.tasks.is_empty());
    }

    #[test]
    fn aggregate_overflowing_snapshot_quota_candidate_does_not_replace_live_state() {
        let mut state = MasterState::empty();
        state.runtime_config.enable_tenant_quota = true;
        let tenant = TenantId::default();
        state
            .tenant_quotas
            .write()
            .restore_object_checked(&tenant, 123)
            .unwrap();
        let quota_before = state.tenant_quotas.read().list_snapshots();
        let sentinel_key = tenant.make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(tenant.clone(), "sentinel"),
        );

        let mut segments = Vec::new();
        let mut objects = Vec::new();
        for index in 0..2 {
            let segment_id = Uuid::new_v4();
            let segment_name = format!("overflow-segment-{index}");
            segments.push(SegmentEntry {
                segment: mooncake_store_core::Segment {
                    id: segment_id,
                    name: segment_name.clone(),
                    base: 0,
                    size: u64::MAX,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                    host_id: String::new(),
                },
                used: u64::MAX,
                client_id: Uuid::new_v4(),
                status: crate::proto::SegmentStatus::Active,
            });
            let user_key = format!("overflow-object-{index}");
            objects.push((
                tenant.make_scoped_key(&user_key),
                ObjectEntry {
                    replicas: vec![ReplicaDescriptor {
                        segment_id,
                        segment_name,
                        offset: 0,
                        size: u64::MAX,
                        status: ReplicaStatus::Complete,
                        replica_type: ReplicaType::Memory,
                        holder_client_id: None,
                        local_disk_storage_id: None,
                        local_disk_generation_id: None,
                        refcnt: 0,
                        handle_valid: true,
                        base_addr: 0,
                        protocol: String::new(),
                    }],
                    size: u64::MAX,
                    last_access: SystemTime::now(),
                    hard_pinned: false,
                    data_type: Default::default(),
                    client_id: Uuid::nil(),
                    put_start_time: None,
                    lease_timeout: None,
                    soft_pin_timeout: None,
                    tenant_id: tenant.clone(),
                    group_id: String::new(),
                    quota_committed: true,
                    reserved_quota_charge_bytes: 0,
                    committed_quota_charge_bytes: 0,
                    pending_replaced_quota_charge_bytes: 0,
                    memory_cache_total_accounted: false,
                    disk_cache_total_accounted: false,
                    disk_allocated_bytes_accounted: 0,
                    user_key,
                },
            ));
        }

        let result = restore_loaded_snapshot_state(
            &state,
            segments,
            Vec::new(),
            objects,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert_eq!(state.tenant_quotas.read().list_snapshots(), quota_before);
        assert!(state.segments.is_empty());
    }

    #[test]
    fn single_object_quota_multiplication_overflow_does_not_replace_live_state() {
        let mut state = MasterState::empty();
        state.runtime_config.enable_tenant_quota = true;
        let tenant = TenantId::default();
        state
            .tenant_quotas
            .write()
            .restore_object_checked(&tenant, 123)
            .unwrap();
        let quota_before = state.tenant_quotas.read().list_snapshots();
        let sentinel_key = tenant.make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(tenant.clone(), "sentinel"),
        );

        let object_size = u64::MAX / 2 + 1;
        let mut segments = Vec::new();
        let mut replicas = Vec::new();
        for index in 0..2 {
            let segment_id = Uuid::new_v4();
            let segment_name = format!("multiplication-overflow-segment-{index}");
            segments.push(SegmentEntry {
                segment: mooncake_store_core::Segment {
                    id: segment_id,
                    name: segment_name.clone(),
                    base: 0,
                    size: object_size,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                    host_id: String::new(),
                },
                used: object_size,
                client_id: Uuid::new_v4(),
                status: crate::proto::SegmentStatus::Active,
            });
            replicas.push(ReplicaDescriptor {
                segment_id,
                segment_name,
                offset: 0,
                size: object_size,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 0,
                protocol: String::new(),
            });
        }
        let user_key = "multiplication-overflow-object";
        let objects = vec![(
            tenant.make_scoped_key(user_key),
            ObjectEntry {
                replicas,
                size: object_size,
                last_access: SystemTime::now(),
                hard_pinned: false,
                data_type: Default::default(),
                client_id: Uuid::nil(),
                put_start_time: None,
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: tenant,
                group_id: String::new(),
                quota_committed: true,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                disk_allocated_bytes_accounted: 0,
                user_key: user_key.into(),
            },
        )];

        let result = restore_loaded_snapshot_state(
            &state,
            segments,
            Vec::new(),
            objects,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert_eq!(state.tenant_quotas.read().list_snapshots(), quota_before);
        assert!(state.segments.is_empty());
    }

    #[test]
    fn overflowing_replication_reservation_snapshot_does_not_replace_live_state() {
        let state = MasterState::empty();
        let tenant = TenantId::default();
        let sentinel_key = tenant.make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(tenant.clone(), "sentinel"),
        );

        let object_size = u64::MAX / 2 + 1;
        let owner_id = Uuid::new_v4();
        let source_segment_id = Uuid::new_v4();
        let first_target_segment_id = Uuid::new_v4();
        let second_target_segment_id = Uuid::new_v4();
        let segment_entry = |segment_id| SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: segment_id.to_string(),
                base: 0,
                size: object_size,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            used: object_size,
            client_id: owner_id,
            status: crate::proto::SegmentStatus::Active,
        };
        let replica = |segment_id, status| ReplicaDescriptor {
            segment_id,
            segment_name: segment_id.to_string(),
            offset: 0,
            size: object_size,
            status,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(owner_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };
        let source = replica(source_segment_id, ReplicaStatus::Complete);
        let first_target = replica(first_target_segment_id, ReplicaStatus::Allocating);
        let second_target = replica(second_target_segment_id, ReplicaStatus::Allocating);
        let user_key = "overflowing-replication";
        let scoped_key = tenant.make_scoped_key(user_key);
        let object = ObjectEntry {
            replicas: vec![source.clone(), first_target.clone(), second_target.clone()],
            size: object_size,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: owner_id,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant,
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: object_size,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            disk_allocated_bytes_accounted: 0,
            user_key: user_key.into(),
        };
        let task = ReplicationTaskSnapshotEntry {
            key: scoped_key.clone(),
            client_id: owner_id,
            start_age_millis: 0,
            kind: ReplicationTaskKind::Copy,
            source,
            targets: vec![first_target, second_target],
            existing_move_target: None,
            reserved_quota_charge_bytes: u64::MAX,
        };

        let result = restore_loaded_snapshot_state(
            &state,
            vec![
                segment_entry(source_segment_id),
                segment_entry(first_target_segment_id),
                segment_entry(second_target_segment_id),
            ],
            Vec::new(),
            vec![(scoped_key, object)],
            Vec::new(),
            vec![task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert!(state.replication_tasks.is_empty());
        assert!(state.segments.is_empty());
    }

    #[test]
    fn snapshot_restore_preserves_exact_existing_move_target() {
        let state = MasterState::empty();
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("existing-move-target");
        let owner_id = Uuid::new_v4();
        let source_segment_id = Uuid::new_v4();
        let target_segment_id = Uuid::new_v4();
        let segment = |id, name: &str| SegmentEntry {
            segment: mooncake_store_core::Segment {
                id,
                name: name.into(),
                base: 0,
                size: 4096,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            used: 16,
            client_id: owner_id,
            status: crate::proto::SegmentStatus::Active,
        };
        let replica = |id, name: &str| ReplicaDescriptor {
            segment_id: id,
            segment_name: name.into(),
            offset: 0,
            size: 16,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(owner_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };
        let source = replica(source_segment_id, "source");
        let existing_target = replica(target_segment_id, "target");
        let object = ObjectEntry {
            replicas: vec![source.clone(), existing_target.clone()],
            size: 16,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: owner_id,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id,
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 32,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            disk_allocated_bytes_accounted: 0,
            user_key: "existing-move-target".into(),
        };
        let task = ReplicationTaskSnapshotEntry {
            key: key.clone(),
            client_id: owner_id,
            start_age_millis: 25,
            kind: ReplicationTaskKind::Move,
            source,
            targets: Vec::new(),
            existing_move_target: Some(existing_target),
            reserved_quota_charge_bytes: 0,
        };

        restore_loaded_snapshot_state(
            &state,
            vec![
                segment(source_segment_id, "source"),
                segment(target_segment_id, "target"),
            ],
            Vec::new(),
            vec![(key.clone(), object)],
            Vec::new(),
            vec![task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        let restored_task = state.replication_tasks.get(&key).unwrap();
        assert!(restored_task.targets.is_empty());
        assert_eq!(
            restored_task
                .existing_move_target
                .as_ref()
                .map(|target| target.segment_id),
            Some(target_segment_id)
        );
        let restored_object = state.objects.get(&key).unwrap();
        assert_eq!(
            restored_object
                .replicas
                .iter()
                .find(|replica| replica.segment_id == source_segment_id)
                .map(|replica| replica.refcnt),
            Some(1)
        );
    }

    #[test]
    fn legacy_unscoped_snapshot_object_key_is_canonicalized() {
        let state = MasterState::empty();

        restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            vec![(
                "legacy-key".into(),
                disk_object(TenantId::default(), "legacy-key"),
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        assert!(
            state
                .objects
                .contains_key(&TenantId::default().make_scoped_key("legacy-key"))
        );
        assert!(!state.objects.contains_key("legacy-key"));
    }

    #[test]
    fn mismatched_snapshot_task_payload_does_not_replace_live_state() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let tenant = TenantId::new("tenant-a".into()).unwrap();
        let task = pending_move_task(
            &tenant.make_scoped_key("object-a"),
            r#"{"tenant_id":"tenant-b","key":"object-b","source":"a","target":"b"}"#,
        );

        let result = restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            vec![(
                tenant.make_scoped_key("object-a"),
                disk_object(tenant, "object-a"),
            )],
            vec![task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert!(!state.objects.contains_key("tenant-a\0object-a"));
        assert!(state.tasks.is_empty());
    }

    #[test]
    fn legacy_snapshot_task_payload_is_canonicalized() {
        let state = MasterState::empty();
        let task = pending_move_task(
            "legacy-key",
            r#"{"key":"legacy-key","source":"a","target":"b"}"#,
        );
        let task_id = task.info.id;

        restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            vec![(
                "legacy-key".into(),
                disk_object(TenantId::default(), "legacy-key"),
            )],
            vec![task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        let restored = state.tasks.get(&task_id).unwrap();
        assert_eq!(
            restored.key,
            TenantId::default().make_scoped_key("legacy-key")
        );
        let payload: serde_json::Value = serde_json::from_str(&restored.payload).unwrap();
        assert_eq!(payload["tenant_id"], "default");
        assert_eq!(payload["key"], "legacy-key");
    }

    #[test]
    fn snapshot_task_payload_shape_must_match_task_type() {
        let state = MasterState::empty();
        let task = pending_move_task(
            "object-a",
            r#"{"key":"object-a","source":"a","targets":["b"]}"#,
        );

        let result = restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            vec![(
                "object-a".into(),
                disk_object(TenantId::default(), "object-a"),
            )],
            vec![task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.is_empty());
        assert!(state.tasks.is_empty());
    }

    #[test]
    fn active_snapshot_task_requires_an_assigned_client() {
        let state = MasterState::empty();
        let mut task = pending_move_task(
            "object-a",
            r#"{"key":"object-a","source":"a","target":"b"}"#,
        );
        task.info.assigned_client = None;

        let result = restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            vec![(
                "object-a".into(),
                disk_object(TenantId::default(), "object-a"),
            )],
            vec![task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.is_empty());
        assert!(state.tasks.is_empty());
    }

    #[test]
    fn snapshot_replica_size_must_match_its_object() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let mut invalid = disk_object(TenantId::default(), "invalid-size");
        invalid.replicas[0].size = invalid.size / 2;

        let result = restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            vec![(TenantId::default().make_scoped_key("invalid-size"), invalid)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
    }

    #[test]
    fn snapshot_replication_task_cannot_claim_a_different_sized_target_range() {
        let state = MasterState::empty();
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("task-size");
        let source_segment = Uuid::new_v4();
        let target_segment = Uuid::new_v4();
        let memory_replica =
            |segment_id, offset, size, status| mooncake_store_core::ReplicaDescriptor {
                segment_id,
                segment_name: String::new(),
                offset,
                size,
                status,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: false,
                base_addr: 0,
                protocol: String::new(),
            };
        let source = memory_replica(source_segment, 0, 16, ReplicaStatus::Complete);
        let target = memory_replica(target_segment, 32, 16, ReplicaStatus::Allocating);
        let mut object = disk_object(tenant_id, "task-size");
        object.replicas = vec![source.clone(), target.clone()];
        object.size = 16;
        object.committed_quota_charge_bytes = 16;

        let mut mismatched_target = target;
        mismatched_target.size = 8;
        let task = ReplicationTaskSnapshotEntry {
            key: key.clone(),
            client_id: Uuid::new_v4(),
            start_age_millis: 0,
            kind: ReplicationTaskKind::Copy,
            source,
            targets: vec![mismatched_target],
            existing_move_target: None,
            reserved_quota_charge_bytes: 16,
        };

        let result = restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            vec![(key, object)],
            Vec::new(),
            vec![task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.is_empty());
        assert!(state.replication_tasks.is_empty());
    }

    #[test]
    fn snapshot_restore_rejects_allocator_kind_drift_before_state_replacement() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );

        let result = restore_loaded_snapshot_state(
            &state,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some(AllocatorSnapshotConfig {
                allocation_strategy: state.runtime_config.allocation_strategy,
                memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
                offset_max_allocation_nodes: None,
            }),
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
    }

    #[test]
    fn snapshot_restore_reapplies_offset_node_budget_to_rebuilt_segments() {
        let maximum = 4;
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            offset_max_allocation_nodes: Some(maximum),
            ..MasterRuntimeConfig::default()
        });
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "restored-offset-limit".to_string(),
                base: 16 * 1024,
                size: 1_000,
                te_endpoint: "127.0.0.1:12345".to_string(),
                protocol: "tcp".to_string(),
                host_id: "restored-offset-host".to_string(),
            },
            used: 0,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let runtime_segment = segment.segment.clone();
        let snapshot_config = AllocatorSnapshotConfig {
            allocation_strategy: service.state.runtime_config.allocation_strategy,
            memory_allocator_kind: service.state.runtime_config.memory_allocator_kind,
            offset_max_allocation_nodes: Some(maximum),
        };

        restore_loaded_snapshot_state(
            &service.state,
            vec![segment],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some(snapshot_config),
        )
        .expect("restore matching allocator configuration");
        assert_eq!(
            service
                .capture_loaded_snapshot("offset-node-budget")
                .allocator_config,
            Some(snapshot_config)
        );

        let mut allocator = service.state.allocator.write();
        allocator
            .rebind_segment(runtime_segment, client_id)
            .expect("restore requires a runtime remount before allocation");
        for _ in 0..3 {
            allocator
                .allocate_from_segment_id(segment_id, 100)
                .expect("three sequential allocations fit four active nodes");
        }
        assert!(matches!(
            allocator.allocate_from_segment_id(segment_id, 100),
            Err(SegmentAllocationError::NoAvailableHandle)
        ));
    }

    #[test]
    fn snapshot_restore_aborts_orphaned_runtime_drain() {
        let state = MasterState::empty();
        let segment_id = Uuid::new_v4();
        let scoped_key = TenantId::default().make_scoped_key("drain-object");
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "orphaned-drain".into(),
                base: 0,
                size: 4096,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            used: 0,
            client_id: Uuid::new_v4(),
            status: proto::SegmentStatus::Draining,
        };
        let mut drain_task = pending_move_task(
            &scoped_key,
            r#"{"tenant_id":"default","key":"drain-object","source":"orphaned-drain","target":"target"}"#,
        );
        drain_task.info.message = format!("drain {scoped_key} from orphaned-drain to target");

        restore_loaded_snapshot_state(
            &state,
            vec![segment],
            Vec::new(),
            vec![(
                scoped_key.clone(),
                disk_object(TenantId::default(), "drain-object"),
            )],
            vec![drain_task],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            proto::SegmentStatus::Active
        );
        assert!(state.tasks.is_empty());
        assert!(state.objects.contains_key(&scoped_key));
    }

    #[test]
    fn snapshot_restore_rejects_cxl_strategy_drift_before_state_replacement() {
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_cxl: true,
            cxl_size: crate::allocator::CACHELIB_SLAB_SIZE,
            allocation_strategy: AllocationStrategy::Random,
            ..Default::default()
        });
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        service.state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let segment_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "cxl-alias".into(),
                base: 0,
                size: crate::allocator::CACHELIB_SLAB_SIZE,
                te_endpoint: String::new(),
                protocol: "cxl".into(),
                host_id: String::new(),
            },
            used: 0,
            client_id: Uuid::new_v4(),
            status: proto::SegmentStatus::Active,
        };

        let result = restore_loaded_snapshot_state(
            &service.state,
            vec![segment],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(service.state.objects.contains_key(&sentinel_key));
        assert!(service.state.segments.get(&segment_id).is_none());
    }

    #[test]
    fn snapshot_restore_rejects_cross_type_segment_uuid_before_state_replacement() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let memory = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "memory".into(),
                base: 0,
                size: 4096,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            used: 0,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let nof = NoFSegmentEntry {
            segment: mooncake_store_core::NoFSegment {
                id: segment_id,
                name: "nof".into(),
                base: 0,
                size: 4096,
                te_endpoint: String::new(),
                client_id,
            },
            used: 0,
            status: proto::SegmentStatus::Active,
        };

        let result = restore_loaded_snapshot_state(
            &state,
            vec![memory],
            vec![nof],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert!(state.segments.is_empty());
        assert!(state.nof_segments.is_empty());
    }

    #[test]
    fn snapshot_restore_rejects_disabled_nof_and_unsupported_status() {
        for gracefully_unmounting in [false, true] {
            let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
                enable_nof: gracefully_unmounting,
                ..Default::default()
            });
            let sentinel_key = TenantId::default().make_scoped_key("sentinel");
            service.state.objects.insert(
                sentinel_key.clone(),
                disk_object(TenantId::default(), "sentinel"),
            );
            let segment_id = Uuid::new_v4();
            let nof = NoFSegmentEntry {
                segment: mooncake_store_core::NoFSegment {
                    id: segment_id,
                    name: "nof".into(),
                    base: 0,
                    size: 4096,
                    te_endpoint: String::new(),
                    client_id: Uuid::new_v4(),
                },
                used: 0,
                status: if gracefully_unmounting {
                    proto::SegmentStatus::GracefullyUnmounting
                } else {
                    proto::SegmentStatus::Active
                },
            };

            let result = restore_loaded_snapshot_state(
                &service.state,
                Vec::new(),
                vec![nof],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
            );

            assert!(result.is_err());
            assert!(service.state.objects.contains_key(&sentinel_key));
            assert!(!service.state.nof_segments.contains_key(&segment_id));
        }
    }

    #[test]
    fn snapshot_restore_rejects_invalid_graceful_intent_before_state_replacement() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "graceful-snapshot".into(),
                base: 0x1000,
                size: 4096,
                te_endpoint: "rdma://stale".into(),
                protocol: "rdma".into(),
                host_id: "host-a".into(),
            },
            used: 0,
            client_id,
            status: proto::SegmentStatus::GracefullyUnmounting,
        };
        let pending = GracefulUnmountSnapshotEntry {
            segment_id,
            client_id,
            deadline_epoch_ms: 0,
        };

        let result = restore_loaded_snapshot_state(
            &state,
            vec![segment],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![pending],
            Vec::new(),
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert!(!state.segments.contains_key(&segment_id));
        assert!(state.graceful_unmounts.is_empty());
    }

    #[test]
    fn snapshot_restore_retains_delayed_replica_allocator_range() {
        let state = MasterState::empty();
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "delayed-range-segment".into(),
                base: 0x1000,
                size: 1024,
                te_endpoint: "rdma://stale".into(),
                protocol: "rdma".into(),
                host_id: "host-a".into(),
            },
            used: 100,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let release_id = Uuid::new_v4();
        let scoped_key = TenantId::default().make_scoped_key("retired");
        let delayed = state::DelayedReplicaReleaseEntry {
            id: release_id,
            scoped_key,
            deadline_epoch_ms: u64::MAX - 1,
            replicas: vec![ReplicaDescriptor {
                segment_id,
                segment_name: segment.segment.name.clone(),
                offset: 0,
                size: 100,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
                holder_client_id: Some(client_id),
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: segment.segment.base,
                protocol: "rdma".into(),
            }],
        };

        restore_loaded_snapshot_state(
            &state,
            vec![segment],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![delayed],
            None,
        )
        .unwrap();

        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(100));
        let restored = state
            .delayed_replica_releases
            .get(&release_id)
            .expect("delayed release restored");
        assert!(!restored.replicas[0].handle_valid);
        assert_eq!(restored.replicas[0].base_addr, 0);
    }

    #[test]
    fn snapshot_restore_rejects_allocator_replica_segment_name_mismatch() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "authoritative-segment".into(),
                base: 0x1000,
                size: 1024,
                te_endpoint: "rdma://stale".into(),
                protocol: "rdma".into(),
                host_id: "host-a".into(),
            },
            used: 100,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let delayed = state::DelayedReplicaReleaseEntry {
            id: Uuid::new_v4(),
            scoped_key: TenantId::default().make_scoped_key("retired"),
            deadline_epoch_ms: u64::MAX - 1,
            replicas: vec![ReplicaDescriptor {
                segment_id,
                segment_name: "wrong-segment".into(),
                offset: 0,
                size: 100,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
                holder_client_id: Some(client_id),
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: segment.segment.base,
                protocol: "rdma".into(),
            }],
        };

        let error = restore_loaded_snapshot_state(
            &state,
            vec![segment],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![delayed],
            None,
        )
        .unwrap_err();

        assert!(error.contains("segment name"));
        assert!(state.objects.contains_key(&sentinel_key));
        assert!(state.segments.get(&segment_id).is_none());
        assert!(state.delayed_replica_releases.is_empty());
    }

    #[test]
    fn snapshot_restore_keeps_committed_object_readable_and_quarantines_orphan_promotion_range() {
        let state = MasterState::empty();
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "orphan-promotion-segment".into(),
                base: 0x2000,
                size: 1024,
                te_endpoint: "rdma://stale".into(),
                protocol: "rdma".into(),
                host_id: "host-a".into(),
            },
            used: 16,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let scoped_key = TenantId::default().make_scoped_key("promoted");
        let mut object = disk_object(TenantId::default(), "promoted");
        object.replicas.push(ReplicaDescriptor {
            segment_id,
            segment_name: segment.segment.name.clone(),
            offset: 0,
            size: object.size,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: segment.segment.base,
            protocol: "rdma".into(),
        });

        restore_loaded_snapshot_state(
            &state,
            vec![segment],
            Vec::new(),
            vec![(scoped_key.clone(), object)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        assert!(!state.processing_keys.contains_key(&scoped_key));
        reap_expired_background_tasks(&state, Instant::now());

        let restored = state.objects.get(&scoped_key).unwrap();
        assert_eq!(restored.replicas.len(), 1);
        assert_eq!(restored.replicas[0].replica_type, ReplicaType::Disk);
        drop(restored);
        let delayed = state
            .delayed_replica_releases
            .iter()
            .next()
            .expect("orphan staged range is quarantined");
        assert_eq!(delayed.scoped_key, scoped_key);
        assert_eq!(delayed.replicas.len(), 1);
        assert_eq!(delayed.replicas[0].segment_id, segment_id);
    }

    #[test]
    fn snapshot_restore_preserves_in_place_upsert_charge_and_processing_generation() {
        let state = MasterState::empty();
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "in-place-upsert-segment".into(),
                base: 0x3000,
                size: 1024,
                te_endpoint: "rdma://stale".into(),
                protocol: "rdma".into(),
                host_id: "host-a".into(),
            },
            used: 16,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let scoped_key = TenantId::default().make_scoped_key("in-place");
        let mut object = disk_object(TenantId::default(), "in-place");
        object.client_id = client_id;
        object.put_start_time = Some(SystemTime::now());
        object.replicas = vec![ReplicaDescriptor {
            segment_id,
            segment_name: segment.segment.name.clone(),
            offset: 0,
            size: object.size,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: segment.segment.base,
            protocol: "rdma".into(),
        }];
        object.committed_quota_charge_bytes = object.size;

        restore_loaded_snapshot_state(
            &state,
            vec![segment],
            Vec::new(),
            vec![(scoped_key.clone(), object)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        let restored = state.objects.get(&scoped_key).unwrap();
        assert!(restored.quota_committed);
        assert_eq!(restored.committed_quota_charge_bytes, restored.size);
        assert!(restored.put_start_time.is_some());
        assert_eq!(restored.replicas[0].status, ReplicaStatus::Allocating);
        assert!(state.processing_keys.contains_key(&scoped_key));
        assert!(
            state
                .client_objects
                .get(&client_id)
                .is_none_or(|keys| !keys.contains(&scoped_key))
        );
        assert_eq!(
            state.allocator.read().used_bytes(&segment_id),
            Some(restored.size)
        );

        let manager = crate::oplog::OpLogManager::new(None, 0);
        manager
            .record_object_image_durable(&scoped_key, &restored)
            .expect("in-place Upsert image must satisfy durable quota validation");
    }

    #[test]
    fn snapshot_restore_rejects_live_and_delayed_range_overlap() {
        let state = MasterState::empty();
        let sentinel_key = TenantId::default().make_scoped_key("sentinel");
        state.objects.insert(
            sentinel_key.clone(),
            disk_object(TenantId::default(), "sentinel"),
        );
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: "overlap-segment".into(),
                base: 0x2000,
                size: 1024,
                te_endpoint: "rdma://stale".into(),
                protocol: "rdma".into(),
                host_id: "host-a".into(),
            },
            used: 100,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let replica = ReplicaDescriptor {
            segment_id,
            segment_name: segment.segment.name.clone(),
            offset: 0,
            size: 100,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: segment.segment.base,
            protocol: "rdma".into(),
        };
        let key = TenantId::default().make_scoped_key("live");
        let mut object = disk_object(TenantId::default(), "live");
        object.size = 100;
        object.replicas = vec![replica.clone()];
        object.committed_quota_charge_bytes = 100;
        let delayed = state::DelayedReplicaReleaseEntry {
            id: Uuid::new_v4(),
            scoped_key: TenantId::default().make_scoped_key("retired"),
            deadline_epoch_ms: u64::MAX - 1,
            replicas: vec![replica],
        };

        let result = restore_loaded_snapshot_state(
            &state,
            vec![segment],
            Vec::new(),
            vec![(key, object)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![delayed],
            None,
        );

        assert!(result.is_err());
        assert!(state.objects.contains_key(&sentinel_key));
        assert!(state.delayed_replica_releases.is_empty());
    }

    #[test]
    fn tenant_quota_abort_mismatch_fences_master() {
        let temp = tempfile::tempdir().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_tenant_quota: true,
            tenant_quota_connector_type: "file".into(),
            tenant_quota_connector_uri: temp.path().join("quota.yaml").display().to_string(),
            ..Default::default()
        });

        let error = service
            .abort_tenant_quota(&TenantId::default(), 1)
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.is_service_fenced());
        assert!(!service.is_service_available());
    }
}

#[cfg(test)]
mod tenant_quota_parity_tests {
    use super::*;

    fn quota_service() -> MasterServiceImpl {
        MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_tenant_quota: true,
            tenant_quota_pool_capacity_bytes: 2_000,
            tenant_quota_connector_uri: "/tmp/mooncake-tq-parity-policy.yaml".to_string(),
            ..Default::default()
        })
    }

    fn committed_object(
        tenant_id: &TenantId,
        user_key: &str,
        size: u64,
    ) -> (SegmentEntry, String, ObjectEntry) {
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = SegmentEntry {
            segment: mooncake_store_core::Segment {
                id: segment_id,
                name: format!("tq-parity-segment-{user_key}"),
                base: 0x3000,
                size: size.max(1024),
                te_endpoint: String::new(),
                protocol: "tcp".to_string(),
                host_id: String::new(),
            },
            used: size,
            client_id,
            status: proto::SegmentStatus::Active,
        };
        let object = ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id,
                segment_name: segment.segment.name.clone(),
                offset: 0,
                size,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: Some(client_id),
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 0x3000,
                protocol: "tcp".to_string(),
            }],
            size,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: size,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            disk_allocated_bytes_accounted: 0,
            user_key: user_key.to_string(),
        };
        (segment, tenant_id.make_scoped_key(user_key), object)
    }

    // RebuildUsageCreatesAndRemovesOrphans: the production snapshot restore
    // path rebuilds committed quota charges, keeps explicit policies, and a
    // later empty restore removes only the accounting-only orphan.
    #[test]
    fn cpp_parity_restore_rebuilds_then_removes_quota_orphan() {
        let service = quota_service();
        service.upsert_tenant_quota_policy("tenant-a", 100).unwrap();
        let tenant_a = TenantId::new("tenant-a".to_string()).unwrap();
        let orphan = TenantId::new("orphan".to_string()).unwrap();

        let (segment_a, key_a, object_a) = committed_object(&tenant_a, "obj-a", 40);
        let (segment_o, key_o, object_o) = committed_object(&orphan, "obj-o", 20);
        restore_loaded_snapshot_state(
            &service.state,
            vec![segment_a, segment_o],
            Vec::new(),
            vec![(key_a, object_a), (key_o, object_o)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        let a_snapshot = service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap();
        let o_snapshot = service
            .get_tenant_quota_snapshot("orphan")
            .unwrap()
            .unwrap();
        assert_eq!(a_snapshot.committed_count, 1);
        assert_eq!(a_snapshot.metadata_object_count, 1);
        assert!(a_snapshot.has_explicit_policy);
        assert_eq!(o_snapshot.committed_count, 1);
        assert_eq!(o_snapshot.metadata_object_count, 1);
        assert!(!o_snapshot.has_explicit_policy);
        assert!(o_snapshot.over_quota);

        restore_loaded_snapshot_state(
            &service.state,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();

        assert!(
            service
                .get_tenant_quota_snapshot("orphan")
                .unwrap()
                .is_none()
        );
        assert!(
            service
                .get_tenant_quota_snapshot("tenant-a")
                .unwrap()
                .is_some()
        );
    }

    // ShardedTenantQuotaTableTest.ConcurrentReserveNeverExceedsQuota: twenty
    // synchronized reservations of 100 against a 1,000 quota stop at exactly
    // ten successes with final reserved bytes exactly 1,000.
    #[tokio::test]
    async fn cpp_parity_twenty_concurrent_reservations_stop_exactly_at_quota() {
        let service = Arc::new(quota_service());
        service
            .upsert_tenant_quota_policy("tenant-a", 1_000)
            .unwrap();
        let tenant = TenantId::new("tenant-a".to_string()).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(20));
        let mut workers = Vec::new();
        for _ in 0..20 {
            let service = Arc::clone(&service);
            let tenant = tenant.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                service.reserve_tenant_quota(&tenant, 100).is_ok()
            }));
        }
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|ok| *ok)
            .count();
        assert_eq!(successes, 10);
        let snapshot = service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.reserved_bytes, 1_000);
    }

    // ShardedTenantQuotaTableTest.DifferentShardsUpdateIndependently: two
    // tenants complete 1,000 reserve(1)/abort(1) cycles concurrently with zero
    // failures and both end with reserved_bytes 0.
    #[tokio::test]
    async fn cpp_parity_two_tenants_complete_concurrent_reserve_abort_cycles() {
        let service = Arc::new(quota_service());
        service
            .upsert_tenant_quota_policy("tenant-a", 1_000)
            .unwrap();
        service
            .upsert_tenant_quota_policy("tenant-b", 1_000)
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut workers = Vec::new();
        for tenant_name in ["tenant-a", "tenant-b"] {
            let service = Arc::clone(&service);
            let barrier = Arc::clone(&barrier);
            let tenant = TenantId::new(tenant_name.to_string()).unwrap();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..1_000 {
                    service.reserve_tenant_quota(&tenant, 1).unwrap();
                    service.abort_tenant_quota(&tenant, 1).unwrap();
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        for tenant_name in ["tenant-a", "tenant-b"] {
            let snapshot = service
                .get_tenant_quota_snapshot(tenant_name)
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.reserved_bytes, 0);
        }
    }

    // ShardedTenantQuotaTableTest.RecomputeCanRunWithAccounting: recomputation
    // racing 1,000 reserve/abort cycles stays consistent with zero failures
    // and the final ledger is clean.
    #[tokio::test]
    async fn cpp_parity_recompute_and_accounting_are_concurrently_consistent() {
        let service = Arc::new(quota_service());
        service
            .upsert_tenant_quota_policy("tenant-a", 1_000)
            .unwrap();
        let tenant = TenantId::new("tenant-a".to_string()).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let accounting = {
            let service = Arc::clone(&service);
            let tenant = tenant.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..1_000 {
                    service.reserve_tenant_quota(&tenant, 1).unwrap();
                    service.abort_tenant_quota(&tenant, 1).unwrap();
                }
            })
        };
        let recompute = {
            let service = Arc::clone(&service);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..1_000 {
                    let capacity = service.tenant_quota_capacity_bytes();
                    service
                        .state
                        .tenant_quotas
                        .write()
                        .recompute_effective_quotas(capacity);
                }
            })
        };
        accounting.join().unwrap();
        recompute.join().unwrap();

        let snapshot = service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.effective_quota_bytes, 1_000);
    }

    #[tokio::test]
    async fn cpp_parity_delete_disables_positive_and_zero_reservations_before_connector_save_finishes()
     {
        // C++ MasterServiceTenantQuotaTest.DeletePolicyBlocksValidatedReservationsBeforeConnectorSave:
        // once deletion has disabled tenant-a admission but while the connector
        // save is deliberately paused, both 1-byte and zero-byte reservations
        // fail TENANT_NOT_REGISTERED; after save resumes, deletion succeeds
        // with no snapshot.
        let service = std::sync::Arc::new(quota_service());
        let barrier = service.install_policy_save_barrier();
        let tenant = TenantId::new("tenant-a".to_owned()).expect("valid tenant");
        service
            .upsert_tenant_quota_policy("tenant-a", 1000)
            .unwrap();

        let delete_tenant = tenant.clone();
        let delete_service = std::sync::Arc::clone(&service);
        let delete_handle = std::thread::spawn(move || {
            delete_service.delete_tenant_quota_policy_for_tenant(&delete_tenant)
        });

        barrier.wait_started();

        let one_byte = service.reserve_tenant_quota(&tenant, 1).unwrap_err();
        assert_eq!(one_byte.code(), tonic::Code::ResourceExhausted);
        assert!(one_byte.message().contains("tenant not registered"));
        let zero_byte = service.reserve_tenant_quota(&tenant, 0).unwrap_err();
        assert_eq!(zero_byte.code(), tonic::Code::ResourceExhausted);
        assert!(zero_byte.message().contains("tenant not registered"));

        barrier.release_save();
        let delete_result = delete_handle.join().expect("delete thread joined");
        assert!(delete_result.is_ok(), "{delete_result:?}");
        assert!(
            service
                .get_tenant_quota_snapshot("tenant-a")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn tenant_quota_delete_and_upsert_persist_in_mutation_order() {
        let temp = tempfile::tempdir().unwrap();
        let policy_path = temp.path().join("quota.yaml");
        let service = Arc::new(MasterServiceImpl::with_runtime_config(
            MasterRuntimeConfig {
                enable_tenant_quota: true,
                tenant_quota_pool_capacity_bytes: 2_000,
                tenant_quota_connector_type: "file".to_string(),
                tenant_quota_connector_uri: policy_path.display().to_string(),
                ..Default::default()
            },
        ));
        service
            .upsert_tenant_quota_policy("tenant-a", 1_000)
            .unwrap();
        let barrier = service.install_policy_save_barrier();
        let tenant = TenantId::new("tenant-a".to_string()).unwrap();

        let delete_service = Arc::clone(&service);
        let delete_tenant = tenant.clone();
        let delete = std::thread::spawn(move || {
            delete_service.delete_tenant_quota_policy_for_tenant(&delete_tenant)
        });
        barrier.wait_started();

        let (tx, rx) = std::sync::mpsc::channel();
        let upsert_service = Arc::clone(&service);
        let upsert = std::thread::spawn(move || {
            let result = upsert_service.upsert_tenant_quota_policy("tenant-a", 777);
            tx.send(result).unwrap();
        });
        let premature = rx.recv_timeout(Duration::from_millis(100)).ok();
        let upsert_overtook_delete = premature.is_some();
        barrier.release_save();
        delete.join().unwrap().unwrap();
        let upsert_result = match premature {
            Some(result) => result,
            None => rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        };
        upsert.join().unwrap();
        upsert_result.unwrap();

        assert!(
            !upsert_overtook_delete,
            "upsert overtook an in-flight delete save"
        );
        let persisted = crate::tenant_quota_policy_store::load_tenant_quota_policy(
            "file",
            policy_path.to_str().unwrap(),
            "mooncake_cluster",
        )
        .unwrap();
        assert_eq!(persisted.tenant_quotas.get("tenant-a"), Some(&777));
    }

    #[test]
    fn failed_tenant_quota_delete_restores_effective_quotas() {
        let temp = tempfile::tempdir().unwrap();
        let policy_path = temp.path().join("quota.yaml");
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_tenant_quota: true,
            tenant_quota_pool_capacity_bytes: 2_000,
            tenant_quota_connector_type: "file".to_string(),
            tenant_quota_connector_uri: policy_path.display().to_string(),
            ..Default::default()
        });
        service
            .upsert_tenant_quota_policy("tenant-a", 1_000)
            .unwrap();
        service
            .upsert_tenant_quota_policy("tenant-b", 1_000)
            .unwrap();

        std::fs::remove_file(&policy_path).unwrap();
        std::fs::create_dir(&policy_path).unwrap();
        let error = service.delete_tenant_quota_policy("tenant-a").unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);

        let tenant_a = service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap();
        let tenant_b = service
            .get_tenant_quota_snapshot("tenant-b")
            .unwrap()
            .unwrap();
        assert_eq!(tenant_a.effective_quota_bytes, 1_000);
        assert_eq!(tenant_b.effective_quota_bytes, 1_000);
    }
}

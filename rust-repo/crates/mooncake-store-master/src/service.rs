use crate::allocator::SegmentAllocator;
use crate::eviction::EvictionManager;
use crate::http_metadata::MetadataState;
use crate::metrics;
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use chrono::Utc;
use dashmap::DashMap;
use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, TaskInfo, TaskStatus,
    TaskType,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};
use tonic::{Request, Response, Status};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Master state
// ---------------------------------------------------------------------------

pub(crate) struct MasterState {
    pub(crate) clients: DashMap<Uuid, ClientEntry>,
    pub(crate) objects: DashMap<String, ObjectEntry>,
    pub(crate) segments: DashMap<Uuid, SegmentEntry>,
    pub(crate) local_disk_segments: DashMap<Uuid, LocalDiskSegmentEntry>,
    pub(crate) tasks: DashMap<Uuid, TaskEntry>,
    pub(crate) offloading_tasks: DashMap<String, OffloadingTaskEntry>,
    pub(crate) promotion_tasks: DashMap<String, PromotionTaskEntry>,
    pub(crate) promotion_access_counts: DashMap<String, u8>,
    pub(crate) allocator: RwLock<SegmentAllocator>,
    storage_backend: RwLock<Option<StorageBackend>>,
    promotion_in_flight: AtomicUsize,
    runtime_config: MasterRuntimeConfig,
}

pub(crate) struct ClientEntry {
    pub(crate) info: mooncake_store_core::ClientInfo,
    pub(crate) last_ping: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub replicas: Vec<ReplicaDescriptor>,
    pub size: u64,
    pub last_access: SystemTime,
    pub soft_pinned: bool,
}

pub struct SegmentEntry {
    pub segment: mooncake_store_core::Segment,
}

pub(crate) struct LocalDiskSegmentEntry {
    pub(crate) enable_offloading: bool,
    pub(crate) offloading_objects: HashMap<String, i64>,
    pub(crate) promotion_objects: HashMap<String, i64>,
    pub(crate) ssd_total_capacity_bytes: i64,
}

#[derive(Debug, Clone)]
pub struct TaskEntry {
    pub info: TaskInfo,
    pub key: String,
    pub payload: String,
    pub max_retry_attempts: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct OffloadingTaskEntry {
    pub(crate) client_id: Uuid,
    pub(crate) start_time: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct PromotionTaskEntry {
    pub(crate) holder_id: Uuid,
    pub(crate) object_size: u64,
    pub(crate) staged_segment_id: Option<Uuid>,
    pub(crate) staged_offset: Option<u64>,
    pub(crate) start_time: Instant,
}

// ---------------------------------------------------------------------------
// MasterServiceImpl
// ---------------------------------------------------------------------------

pub struct MasterServiceImpl {
    state: Arc<MasterState>,
    metadata_state: MetadataState,
    graceful_unmount_scheduler: GracefulUnmountScheduler,
    processing_reaper: ProcessingReaper,
    eviction_worker: EvictionWorker,
}

#[derive(Debug, Clone)]
pub struct MasterRuntimeConfig {
    pub put_start_release_timeout: Duration,
    pub promotion_on_hit: bool,
    pub promotion_admission_threshold: u8,
    pub promotion_queue_limit: usize,
    pub reaper_interval: Duration,
    pub eviction_interval: Duration,
    pub eviction_high_watermark_ratio: f64,
    pub eviction_ratio: f64,
    pub soft_pin_ttl: Duration,
    pub lease_ttl: Duration,
    pub offload_on_evict: bool,
    pub offload_force_evict: bool,
}

impl Default for MasterRuntimeConfig {
    fn default() -> Self {
        Self {
            put_start_release_timeout: Duration::from_secs(30),
            promotion_on_hit: true,
            promotion_admission_threshold: 1,
            promotion_queue_limit: 1024,
            reaper_interval: Duration::from_millis(100),
            eviction_interval: Duration::from_millis(100),
            eviction_high_watermark_ratio: 0.95,
            eviction_ratio: 0.05,
            soft_pin_ttl: Duration::from_secs(1800),
            lease_ttl: Duration::from_secs(3600),
            offload_on_evict: false,
            offload_force_evict: false,
        }
    }
}

#[derive(Debug, Clone)]
struct GracefulUnmountRecord {
    segment_id: Uuid,
    client_id: Uuid,
    expire_at: Instant,
}

impl PartialEq for GracefulUnmountRecord {
    fn eq(&self, other: &Self) -> bool {
        self.segment_id == other.segment_id
            && self.client_id == other.client_id
            && self.expire_at == other.expire_at
    }
}

impl Eq for GracefulUnmountRecord {}

impl PartialOrd for GracefulUnmountRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GracefulUnmountRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        other.expire_at.cmp(&self.expire_at)
    }
}

struct GracefulUnmountSchedulerState {
    queue: BinaryHeap<GracefulUnmountRecord>,
    stopping: bool,
}

struct GracefulUnmountSchedulerInner {
    state: Mutex<GracefulUnmountSchedulerState>,
    condvar: Condvar,
}

struct GracefulUnmountScheduler {
    inner: Arc<GracefulUnmountSchedulerInner>,
    worker: Option<JoinHandle<()>>,
}

struct ProcessingReaperInner {
    state: Mutex<bool>,
    condvar: Condvar,
}

struct ProcessingReaper {
    inner: Arc<ProcessingReaperInner>,
    worker: Option<JoinHandle<()>>,
}

struct EvictionWorkerInner {
    state: Mutex<bool>,
    condvar: Condvar,
}

struct EvictionWorker {
    inner: Arc<EvictionWorkerInner>,
    worker: Option<JoinHandle<()>>,
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
            offloading_tasks: DashMap::new(),
            promotion_tasks: DashMap::new(),
            promotion_access_counts: DashMap::new(),
            allocator: RwLock::new(SegmentAllocator::new()),
            storage_backend,
            promotion_in_flight: AtomicUsize::new(0),
            runtime_config: runtime_config.clone(),
        });
        let metadata_state = MetadataState::new("");
        let graceful_unmount_scheduler = GracefulUnmountScheduler::new(state.clone());
        let processing_reaper = ProcessingReaper::new(state.clone());
        let eviction_worker = EvictionWorker::new(state.clone());

        // Load existing state from snapshot
        if let Some(ref backend) = *state.storage_backend.read() {
            if let Ok(Some((segments, objects))) = backend.load() {
                for seg in segments {
                    state.segments.insert(seg.id, SegmentEntry { segment: seg.clone() });
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
    }
}

impl Default for MasterServiceImpl {
    fn default() -> Self {
        Self::new(None, None)
    }
}

// ---------------------------------------------------------------------------
// Helpers: UUID conversions
// ---------------------------------------------------------------------------

fn uuid_to_proto(id: Uuid) -> proto::Uuid {
    let (high, low) = id.as_u64_pair();
    proto::Uuid { high, low }
}

fn uuid_from_proto(p: &proto::Uuid) -> Uuid {
    Uuid::from_u64_pair(p.high, p.low)
}

fn replica_to_proto(r: &ReplicaDescriptor) -> proto::ReplicaDescriptor {
    proto::ReplicaDescriptor {
        segment_id: Some(uuid_to_proto(r.segment_id)),
        segment_name: r.segment_name.clone(),
        offset: r.offset,
        status: r.status as i32,
        replica_type: r.replica_type as i32,
        slice_key_hash: vec![],
        size: r.size,
        holder_client_id: r.holder_client_id.map(uuid_to_proto),
    }
}

fn replica_from_proto(p: &proto::ReplicaDescriptor) -> ReplicaDescriptor {
    ReplicaDescriptor {
        segment_id: p.segment_id.as_ref().map_or(Uuid::nil(), uuid_from_proto),
        segment_name: p.segment_name.clone(),
        offset: p.offset,
        size: p.size,
        status: match p.status {
            1 => ReplicaStatus::Allocating,
            2 => ReplicaStatus::Written,
            3 => ReplicaStatus::Complete,
            4 => ReplicaStatus::Failed,
            _ => ReplicaStatus::Undefined,
        },
        replica_type: match p.replica_type {
            1 => ReplicaType::Disk,
            2 => ReplicaType::LocalDisk,
            _ => ReplicaType::Memory,
        },
        holder_client_id: p.holder_client_id.as_ref().map(uuid_from_proto),
    }
}

fn config_from_proto(c: &proto::ReplicateConfig) -> ReplicateConfig {
    ReplicateConfig {
        replica_num: c.replica_num,
        with_soft_pin: c.with_soft_pin,
        with_hard_pin: c.with_hard_pin,
        preferred_segment: c.preferred_segment.clone(),
        prefer_alloc_in_same_node: c.prefer_alloc_in_same_node,
    }
}

fn task_type_to_proto(task_type: TaskType) -> i32 {
    match task_type {
        TaskType::ReplicaCopy => proto::TaskType::ReplicaCopy as i32,
        TaskType::ReplicaMove => proto::TaskType::ReplicaMove as i32,
    }
}

fn task_status_to_proto(status: TaskStatus) -> i32 {
    match status {
        TaskStatus::Pending => proto::TaskStatus::TaskPending as i32,
        TaskStatus::Processing => proto::TaskStatus::TaskProcessing as i32,
        TaskStatus::Success => proto::TaskStatus::TaskSuccess as i32,
        TaskStatus::Failed => proto::TaskStatus::TaskFailed as i32,
    }
}

fn task_status_from_proto(status: i32) -> TaskStatus {
    match status {
        1 => TaskStatus::Processing,
        2 => TaskStatus::Success,
        3 => TaskStatus::Failed,
        _ => TaskStatus::Pending,
    }
}

fn host_from_segment_name(name: &str) -> String {
    name.split(':').next().unwrap_or(name).to_string()
}

fn port_from_segment_name(name: &str) -> u16 {
    name.split(':')
        .nth(1)
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0)
}

fn merge_addresses(existing: &[String], new_addresses: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut merged = existing.to_vec();
    for address in new_addresses {
        if !address.is_empty() && !merged.iter().any(|v| v == &address) {
            merged.push(address);
        }
    }
    merged
}

fn upsert_client_addresses(state: &MasterState, client_id: Uuid, addresses: Vec<String>) {
    let now = Utc::now();
    if let Some(mut entry) = state.clients.get_mut(&client_id) {
        entry.info.addresses = merge_addresses(&entry.info.addresses, addresses);
        entry.info.last_seen = now;
        entry.last_ping = SystemTime::now();
        return;
    }

    state.clients.insert(client_id, ClientEntry {
        info: mooncake_store_core::ClientInfo {
            id: client_id,
            addresses,
            segments: vec![],
            last_seen: now,
        },
        last_ping: SystemTime::now(),
    });
}

fn sync_client_segments(state: &MasterState, client_id: Uuid) {
    let segments = state
        .segments
        .iter()
        .filter(|entry| entry.segment.client_id == client_id)
        .map(|entry| entry.segment.clone())
        .collect::<Vec<_>>();
    if let Some(mut client) = state.clients.get_mut(&client_id) {
        client.info.segments = segments;
        client.info.last_seen = Utc::now();
    }
}

async fn register_metadata_segments(metadata_state: &MetadataState, segment_names: &[String]) {
    for segment_name in segment_names {
        let host = host_from_segment_name(segment_name);
        if host.is_empty() {
            continue;
        }
        metadata_state
            .register_node(host, port_from_segment_name(segment_name), vec![])
            .await;
    }
}

fn client_id_by_segment_name(state: &MasterState, segment_name: &str) -> Option<Uuid> {
    state
        .segments
        .iter()
        .find(|entry| entry.segment.name == segment_name)
        .map(|entry| entry.segment.client_id)
}

fn unmount_segment_owned(state: &MasterState, segment_id: Uuid, client_id: Uuid) -> bool {
    let owned = state
        .segments
        .get(&segment_id)
        .map(|entry| entry.segment.client_id == client_id)
        .unwrap_or(false);
    if !owned {
        return false;
    }

    state.segments.remove(&segment_id);
    state.allocator.write().remove_segment(&segment_id);
    sync_client_segments(state, client_id);
    metrics::SEGMENT_COUNT.set(state.segments.len() as i64);
    true
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

impl GracefulUnmountScheduler {
    fn new(state: Arc<MasterState>) -> Self {
        let inner = Arc::new(GracefulUnmountSchedulerInner {
            state: Mutex::new(GracefulUnmountSchedulerState {
                queue: BinaryHeap::new(),
                stopping: false,
            }),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let worker = thread::spawn(move || {
            loop {
                let mut guard = worker_inner.state.lock().expect("scheduler mutex poisoned");
                while !guard.stopping && guard.queue.is_empty() {
                    guard = worker_inner
                        .condvar
                        .wait(guard)
                        .expect("scheduler condvar wait failed");
                }
                if guard.stopping {
                    break;
                }

                let Some(next) = guard.queue.peek().cloned() else {
                    continue;
                };
                let now = Instant::now();
                if next.expire_at > now {
                    let timeout = next.expire_at.saturating_duration_since(now);
                    let (g, timeout_res) = worker_inner
                        .condvar
                        .wait_timeout(guard, timeout)
                        .expect("scheduler condvar timeout failed");
                    guard = g;
                    if guard.stopping {
                        break;
                    }
                    if !timeout_res.timed_out() {
                        continue;
                    }
                }

                let mut expired = Vec::new();
                let now = Instant::now();
                while let Some(record) = guard.queue.peek().cloned() {
                    if record.expire_at > now {
                        break;
                    }
                    expired.push(record);
                    guard.queue.pop();
                }
                drop(guard);

                for record in expired {
                    unmount_segment_owned(&state, record.segment_id, record.client_id);
                }
            }
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    fn schedule(&self, segment_id: Uuid, client_id: Uuid, grace_period_ms: u64) {
        let mut guard = self.inner.state.lock().expect("scheduler mutex poisoned");
        if guard.stopping {
            return;
        }
        guard.queue.push(GracefulUnmountRecord {
            segment_id,
            client_id,
            expire_at: Instant::now() + std::time::Duration::from_millis(grace_period_ms),
        });
        drop(guard);
        self.inner.condvar.notify_all();
    }

    #[allow(dead_code)]
    fn remove_client_records(&self, client_id: Uuid) {
        let mut guard = self.inner.state.lock().expect("scheduler mutex poisoned");
        let mut retained = BinaryHeap::new();
        while let Some(record) = guard.queue.pop() {
            if record.client_id != client_id {
                retained.push(record);
            }
        }
        guard.queue = retained;
        drop(guard);
        self.inner.condvar.notify_all();
    }

    fn stop(&mut self) {
        {
            let mut guard = self.inner.state.lock().expect("scheduler mutex poisoned");
            if guard.stopping {
                return;
            }
            guard.stopping = true;
        }
        self.inner.condvar.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn release_staged_promotion_replica(
    state: &MasterState,
    key: &str,
    segment_id: Uuid,
    offset: u64,
) {
    if let Some(mut object) = state.objects.get_mut(key) {
        let mut removed = Vec::new();
        object.replicas.retain(|replica| {
            let matched = replica.replica_type == ReplicaType::Memory
                && replica.segment_id == segment_id
                && replica.offset == offset
                && replica.status == ReplicaStatus::Allocating;
            if matched {
                removed.push(replica.clone());
            }
            !matched
        });
        drop(object);
        if !removed.is_empty() {
            state.allocator.write().release(&removed);
            sync_segment_usage(state, [segment_id]);
        }
    }
}

fn reap_expired_background_tasks(state: &MasterState, now: Instant) {
    let ttl = state.runtime_config.put_start_release_timeout;

    let expired_offloads = state
        .offloading_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_offloads {
        clear_offloading_task(state, &key);
    }

    let expired_promotions = state
        .promotion_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_promotions {
        if let Some(task) = clear_promotion_task(state, &key) {
            if let (Some(segment_id), Some(offset)) = (task.staged_segment_id, task.staged_offset) {
                release_staged_promotion_replica(state, &key, segment_id, offset);
            }
            if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.holder_id) {
                local_disk.promotion_objects.remove(&key);
            }
        }
    }
}

impl ProcessingReaper {
    fn new(state: Arc<MasterState>) -> Self {
        let inner = Arc::new(ProcessingReaperInner {
            state: Mutex::new(false),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let interval = state.runtime_config.reaper_interval;
        let worker = thread::spawn(move || loop {
            let guard = worker_inner.state.lock().expect("reaper mutex poisoned");
            let (guard, _) = worker_inner
                .condvar
                .wait_timeout(guard, interval)
                .expect("reaper condvar timeout failed");
            if *guard {
                break;
            }
            drop(guard);
            reap_expired_background_tasks(&state, Instant::now());
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    fn stop(&mut self) {
        {
            let mut stopping = self.inner.state.lock().expect("reaper mutex poisoned");
            if *stopping {
                return;
            }
            *stopping = true;
        }
        self.inner.condvar.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl EvictionWorker {
    fn new(state: Arc<MasterState>) -> Self {
        let inner = Arc::new(EvictionWorkerInner {
            state: Mutex::new(false),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let interval = state.runtime_config.eviction_interval;
        let worker = thread::spawn(move || loop {
            let guard = worker_inner.state.lock().expect("eviction mutex poisoned");
            let (guard, _) = worker_inner
                .condvar
                .wait_timeout(guard, interval)
                .expect("eviction condvar timeout failed");
            if *guard {
                break;
            }
            drop(guard);
            let _ = run_automatic_eviction_once(&state);
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    fn stop(&mut self) {
        {
            let mut stopping = self.inner.state.lock().expect("eviction mutex poisoned");
            if *stopping {
                return;
            }
            *stopping = true;
        }
        self.inner.condvar.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn addresses_for_client(state: &MasterState, client_id: Uuid) -> Vec<String> {
    if let Some(entry) = state.clients.get(&client_id) {
        if !entry.info.addresses.is_empty() {
            return entry.info.addresses.clone();
        }
    }

    let mut addresses = Vec::new();
    for segment in state.segments.iter() {
        if segment.segment.client_id == client_id {
            let host = host_from_segment_name(&segment.segment.name);
            if !host.is_empty() && !addresses.iter().any(|v| v == &host) {
                addresses.push(host);
            }
        }
    }
    addresses
}

fn sync_segment_usage(
    state: &MasterState,
    segment_ids: impl IntoIterator<Item = Uuid>,
) {
    let allocator = state.allocator.read();
    for segment_id in segment_ids {
        let Some(used) = allocator.used_bytes(&segment_id) else {
            continue;
        };
        if let Some(mut entry) = state.segments.get_mut(&segment_id) {
            entry.segment.used = used;
        }
    }
}

fn memory_usage_ratio(state: &MasterState) -> f64 {
    let (total_bytes, used_bytes) = state.allocator.read().usage_totals();
    if total_bytes == 0 {
        return 0.0;
    }
    used_bytes as f64 / total_bytes as f64
}

fn push_offloading_queue(state: &MasterState, client_id: Uuid, key: &str, size: u64) {
    let mut local_disk = match state.local_disk_segments.get_mut(&client_id) {
        Some(entry) => entry,
        None => return,
    };
    if !local_disk.enable_offloading {
        return;
    }
    local_disk.offloading_objects.insert(key.to_string(), size as i64);
    state.offloading_tasks.insert(
        key.to_string(),
        OffloadingTaskEntry {
            client_id,
            start_time: Instant::now(),
        },
    );
}

fn clear_offloading_task(state: &MasterState, key: &str) {
    if let Some((_, task)) = state.offloading_tasks.remove(key) {
        if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.client_id) {
            local_disk.offloading_objects.remove(key);
        }
    }
}

fn clear_promotion_task(state: &MasterState, key: &str) -> Option<PromotionTaskEntry> {
    let removed = state.promotion_tasks.remove(key).map(|(_, task)| task);
    if removed.is_some() {
        state.promotion_in_flight.fetch_sub(1, AtomicOrdering::Relaxed);
    }
    removed
}

fn evict_memory_replicas(object: &mut ObjectEntry) -> Vec<ReplicaDescriptor> {
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let should_remove =
            replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete;
        if should_remove {
            removed.push(replica.clone());
        }
        !should_remove
    });
    removed
}

fn evict_redundant_memory_replicas(object: &mut ObjectEntry) -> Vec<ReplicaDescriptor> {
    let total_memory = object
        .replicas
        .iter()
        .filter(|replica| replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete)
        .count();
    if total_memory <= 1 {
        return Vec::new();
    }

    let mut kept_one = false;
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let is_memory_complete =
            replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete;
        if !is_memory_complete {
            return true;
        }
        if !kept_one {
            kept_one = true;
            return true;
        }
        removed.push(replica.clone());
        false
    });
    removed
}

fn run_eviction_cycle(state: &MasterState, target_count: usize) -> Vec<String> {
    let manager = EvictionManager::new(
        state.runtime_config.soft_pin_ttl,
        state.runtime_config.lease_ttl,
    );
    let candidate_data = state
        .objects
        .iter()
        .map(|entry| {
            (
                entry.key().clone(),
                entry.replicas.clone(),
                entry.soft_pinned,
                entry.last_access,
            )
        })
        .collect::<Vec<_>>();
    let candidate_refs = candidate_data
        .iter()
        .map(|(key, replicas, soft_pinned, last_access)| {
            (key.as_str(), replicas.as_slice(), *soft_pinned, *last_access)
        })
        .collect::<Vec<_>>();
    let selected = manager.select_for_eviction(&candidate_refs, target_count);

    let mut evicted = Vec::new();
    for key in selected {
        if let Some(mut object) = state.objects.get_mut(&key) {
            let has_local_disk = object
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::LocalDisk);
            let owner_client = object
                .replicas
                .iter()
                .find(|replica| replica.replica_type == ReplicaType::Memory)
                .and_then(|replica| client_id_by_segment_name(state, &replica.segment_name));

            let should_offload =
                state.runtime_config.offload_on_evict && !has_local_disk && owner_client.is_some();
            if should_offload {
                push_offloading_queue(state, owner_client.unwrap(), &key, object.size);
                if !state.offloading_tasks.contains_key(&key) && !state.runtime_config.offload_force_evict {
                    continue;
                }
            }

            let removed = if should_offload && !has_local_disk && state.offloading_tasks.contains_key(&key) {
                evict_redundant_memory_replicas(&mut object)
            } else {
                evict_memory_replicas(&mut object)
            };
            let became_empty = object.replicas.is_empty();
            drop(object);
            if !removed.is_empty() {
                let segment_ids = removed.iter().map(|replica| replica.segment_id).collect::<Vec<_>>();
                state.allocator.write().release(&removed);
                sync_segment_usage(state, segment_ids);
                evicted.push(key.clone());
            }
            if became_empty {
                state.objects.remove(&key);
                clear_offloading_task(state, &key);
                clear_promotion_task(state, &key);
            }
        }
    }

    evicted
}

fn automatic_eviction_target_count(state: &MasterState) -> usize {
    let used_ratio = memory_usage_ratio(state);
    if used_ratio <= state.runtime_config.eviction_high_watermark_ratio {
        return 0;
    }

    let object_count = state.objects.len();
    if object_count == 0 {
        return 0;
    }

    let evict_ratio_target = state
        .runtime_config
        .eviction_ratio
        .max(used_ratio - state.runtime_config.eviction_high_watermark_ratio
            + state.runtime_config.eviction_ratio);
    let target = (object_count as f64 * evict_ratio_target).ceil() as usize;
    target.max(1)
}

fn run_automatic_eviction_once(state: &MasterState) -> Vec<String> {
    let target_count = automatic_eviction_target_count(state);
    if target_count == 0 {
        return Vec::new();
    }
    run_eviction_cycle(state, target_count)
}

fn try_push_promotion_queue(state: &MasterState, key: &str) {
    if !state.runtime_config.promotion_on_hit {
        return;
    }

    let current_freq = {
        let mut entry = state
            .promotion_access_counts
            .entry(key.to_string())
            .or_insert(0);
        if *entry < u8::MAX {
            *entry += 1;
        }
        *entry
    };
    let threshold = state.runtime_config.promotion_admission_threshold.max(1);
    if current_freq < threshold {
        return;
    }

    let (holder_id, object_size) = match state.objects.get(key) {
        Some(object) => {
            let any_memory = object
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete);
            if any_memory {
                return;
            }
            let Some(local_disk) = object.replicas.iter().find(|replica| {
                replica.replica_type == ReplicaType::LocalDisk
                    && replica.status == ReplicaStatus::Complete
                    && replica.holder_client_id.is_some()
            }) else {
                return;
            };
            (local_disk.holder_client_id.unwrap(), local_disk.size)
        }
        None => return,
    };

    if state.promotion_tasks.contains_key(key) {
        return;
    }
    if !state.local_disk_segments.contains_key(&holder_id) {
        return;
    }
    if state
        .promotion_in_flight
        .fetch_add(1, AtomicOrdering::Relaxed)
        >= state.runtime_config.promotion_queue_limit
    {
        state.promotion_in_flight.fetch_sub(1, AtomicOrdering::Relaxed);
        return;
    }
    if let Some(mut local_disk) = state.local_disk_segments.get_mut(&holder_id) {
        local_disk
            .promotion_objects
            .entry(key.to_string())
            .or_insert(object_size as i64);
    }
    state.promotion_tasks.insert(
        key.to_string(),
        PromotionTaskEntry {
            holder_id,
            object_size,
            staged_segment_id: None,
            staged_offset: None,
            start_time: Instant::now(),
        },
    );
}

// ---------------------------------------------------------------------------
// gRPC service impl
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl MasterService for MasterServiceImpl {
    // ---- Ping ----
    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let derived_addresses = req
            .mounted_segments
            .iter()
            .map(|segment| host_from_segment_name(segment))
            .collect::<Vec<_>>();
        if !derived_addresses.is_empty() {
            upsert_client_addresses(&self.state, client_id, derived_addresses);
        }

        if let Some(mut entry) = self.state.clients.get_mut(&client_id) {
            entry.last_ping = SystemTime::now();
            entry.info.last_seen = Utc::now();
        }
        register_metadata_segments(&self.metadata_state, &req.mounted_segments).await;
        metrics::PING_REQUESTS.inc();
        Ok(Response::new(proto::PingResponse {}))
    }

    // ---- MountSegment ----
    async fn mount_segment(
        &self,
        request: Request<proto::MountSegmentRequest>,
    ) -> Result<Response<proto::MountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let segment_id = Uuid::new_v4();
        let host = host_from_segment_name(&req.segment_name);

        let segment = mooncake_store_core::Segment {
            id: segment_id,
            name: req.segment_name.clone(),
            size: req.size,
            used: 0,
            client_id,
        };

        self.state.segments.insert(segment_id, SegmentEntry { segment: segment.clone() });
        upsert_client_addresses(&self.state, client_id, vec![host]);
        sync_client_segments(&self.state, client_id);
        register_metadata_segments(&self.metadata_state, &[req.segment_name.clone()]).await;

        let mut allocator = self.state.allocator.write();
        allocator.add_segment(segment);

        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::MountSegmentResponse {}))
    }

    // ---- UnmountSegment ----
    async fn unmount_segment(
        &self,
        request: Request<proto::UnmountSegmentRequest>,
    ) -> Result<Response<proto::UnmountSegmentResponse>, Status> {
        let req = request.into_inner();
        let segment_id = uuid_from_proto(req.segment_id.as_ref().ok_or(Status::invalid_argument("missing segment_id"))?);
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);

        if !unmount_segment_owned(&self.state, segment_id, client_id) {
            return Err(Status::not_found("segment not found for client"));
        }
        Ok(Response::new(proto::UnmountSegmentResponse {}))
    }

    // ---- GracefulUnmountSegment ----
    async fn graceful_unmount_segment(
        &self,
        request: Request<proto::GracefulUnmountSegmentRequest>,
    ) -> Result<Response<proto::GracefulUnmountSegmentResponse>, Status> {
        let req = request.into_inner();
        let segment_id = uuid_from_proto(req.segment_id.as_ref().ok_or(Status::invalid_argument("missing segment_id"))?);
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let owned = self
            .state
            .segments
            .get(&segment_id)
            .map(|entry| entry.segment.client_id == client_id)
            .unwrap_or(false);
        if !owned {
            return Err(Status::not_found("segment not found for client"));
        }
        self.graceful_unmount_scheduler
            .schedule(segment_id, client_id, req.grace_period_ms);
        Ok(Response::new(proto::GracefulUnmountSegmentResponse {}))
    }

    // ---- ReMountSegment ----
    async fn re_mount_segment(
        &self,
        request: Request<proto::ReMountSegmentRequest>,
    ) -> Result<Response<proto::ReMountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        if req.segment_names.len() != req.segment_sizes.len() {
            return Err(Status::invalid_argument("segment_names and segment_sizes must have same length"));
        }

        let addresses = req
            .segment_names
            .iter()
            .map(|name| host_from_segment_name(name))
            .collect::<Vec<_>>();
        upsert_client_addresses(&self.state, client_id, addresses);
        register_metadata_segments(&self.metadata_state, &req.segment_names).await;

        for (segment_name, size) in req.segment_names.iter().zip(req.segment_sizes.iter()) {
            let exists = self
                .state
                .segments
                .iter()
                .any(|entry| entry.segment.client_id == client_id && entry.segment.name == *segment_name);
            if exists {
                continue;
            }

            let segment = mooncake_store_core::Segment {
                id: Uuid::new_v4(),
                name: segment_name.clone(),
                size: *size,
                used: 0,
                client_id,
            };
            self.state.segments.insert(segment.id, SegmentEntry { segment: segment.clone() });
            self.state.allocator.write().add_segment(segment);
        }
        sync_client_segments(&self.state, client_id);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::ReMountSegmentResponse {}))
    }

    // ---- MountLocalDiskSegment ----
    async fn mount_local_disk_segment(
        &self,
        request: Request<proto::MountLocalDiskSegmentRequest>,
    ) -> Result<Response<proto::MountLocalDiskSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        if let Some(mut entry) = self.state.local_disk_segments.get_mut(&client_id) {
            entry.enable_offloading = req.enable_offloading;
        } else {
            self.state.local_disk_segments.insert(
                client_id,
                LocalDiskSegmentEntry {
                    enable_offloading: req.enable_offloading,
                    offloading_objects: HashMap::new(),
                    promotion_objects: HashMap::new(),
                    ssd_total_capacity_bytes: 0,
                },
            );
        }
        Ok(Response::new(proto::MountLocalDiskSegmentResponse {}))
    }

    // ---- OffloadObjectHeartbeat ----
    async fn offload_object_heartbeat(
        &self,
        request: Request<proto::OffloadObjectHeartbeatRequest>,
    ) -> Result<Response<proto::OffloadObjectHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&client_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        entry.enable_offloading = req.enable_offloading;
        if !req.enable_offloading {
            let keys = entry.offloading_objects.keys().cloned().collect::<Vec<_>>();
            entry.offloading_objects.clear();
            drop(entry);
            for key in keys {
                clear_offloading_task(&self.state, &key);
            }
            return Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
                objects: HashMap::new(),
            }));
        }
        let objects = std::mem::take(&mut entry.offloading_objects);
        Ok(Response::new(proto::OffloadObjectHeartbeatResponse { objects }))
    }

    // ---- ReportSsdCapacity ----
    async fn report_ssd_capacity(
        &self,
        request: Request<proto::ReportSsdCapacityRequest>,
    ) -> Result<Response<proto::ReportSsdCapacityResponse>, Status> {
        let req = request.into_inner();
        if req.ssd_total_capacity_bytes < 0 {
            return Err(Status::invalid_argument("ssd_total_capacity_bytes must be non-negative"));
        }
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&client_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        entry.ssd_total_capacity_bytes = req.ssd_total_capacity_bytes;
        Ok(Response::new(proto::ReportSsdCapacityResponse {}))
    }

    // ---- NotifyOffloadSuccess ----
    async fn notify_offload_success(
        &self,
        request: Request<proto::NotifyOffloadSuccessRequest>,
    ) -> Result<Response<proto::NotifyOffloadSuccessResponse>, Status> {
        let req = request.into_inner();
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        if req.keys.len() != req.metadatas.len() {
            return Err(Status::invalid_argument("keys and metadatas must have same length"));
        }
        for (key, metadata) in req.keys.iter().zip(req.metadatas.iter()) {
            clear_offloading_task(&self.state, key);
            let replica = ReplicaDescriptor {
                segment_id: Uuid::nil(),
                segment_name: metadata.transport_endpoint.clone(),
                offset: 0,
                size: metadata.data_size.max(0) as u64,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::LocalDisk,
                holder_client_id: Some(client_id),
            };
            if let Some(mut object) = self.state.objects.get_mut(key) {
                if let Some(existing) = object.replicas.iter_mut().find(|existing| {
                    existing.replica_type == ReplicaType::LocalDisk
                        && existing.holder_client_id == Some(client_id)
                }) {
                    *existing = replica.clone();
                } else {
                    object.replicas.push(replica);
                }
                object.size = metadata.data_size.max(0) as u64;
            } else {
                self.state.objects.insert(
                    key.clone(),
                    ObjectEntry {
                        replicas: vec![replica],
                        size: metadata.data_size.max(0) as u64,
                        last_access: SystemTime::now(),
                        soft_pinned: false,
                    },
                );
            }
        }
        Ok(Response::new(proto::NotifyOffloadSuccessResponse {}))
    }

    // ---- PromotionObjectHeartbeat ----
    async fn promotion_object_heartbeat(
        &self,
        request: Request<proto::PromotionObjectHeartbeatRequest>,
    ) -> Result<Response<proto::PromotionObjectHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&client_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        let mut objects = HashMap::new();
        if let Some((key, size)) = entry
            .promotion_objects
            .iter()
            .next()
            .map(|(key, size)| (key.clone(), *size))
        {
            entry.promotion_objects.remove(&key);
            objects.insert(key, size);
        }
        Ok(Response::new(proto::PromotionObjectHeartbeatResponse { objects }))
    }

    // ---- PromotionAllocStart ----
    async fn promotion_alloc_start(
        &self,
        request: Request<proto::PromotionAllocStartRequest>,
    ) -> Result<Response<proto::PromotionAllocStartResponse>, Status> {
        let req = request.into_inner();
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let mut task = self
            .state
            .promotion_tasks
            .get_mut(&req.key)
            .ok_or(Status::failed_precondition("promotion task not found"))?;
        if task.holder_id != client_id {
            return Err(Status::permission_denied("promotion task assigned to different client"));
        }
        if task.object_size != req.size {
            return Err(Status::invalid_argument("size mismatch"));
        }
        let object_exists = self.state.objects.contains_key(&req.key);
        if !object_exists {
            return Err(Status::not_found("key not found"));
        }

        let mut config = ReplicateConfig::default();
        if let Some(preferred) = req.preferred_segments.first() {
            config.preferred_segment = preferred.clone();
        }
        let replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate(&req.key, req.size, 1, &config)
        };
        let Some(mut staged) = replicas.into_iter().next() else {
            return Err(Status::resource_exhausted("no available memory segment"));
        };
        sync_segment_usage(&self.state, [staged.segment_id]);
        let staged_segment_id = staged.segment_id;
        let staged_offset = staged.offset;
        staged.status = ReplicaStatus::Allocating;
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            object.replicas.push(staged.clone());
        }
        task.staged_segment_id = Some(staged_segment_id);
        task.staged_offset = Some(staged_offset);
        task.start_time = Instant::now();
        Ok(Response::new(proto::PromotionAllocStartResponse {
            memory_descriptor: Some(replica_to_proto(&staged)),
        }))
    }

    // ---- NotifyPromotionSuccess ----
    async fn notify_promotion_success(
        &self,
        request: Request<proto::NotifyPromotionSuccessRequest>,
    ) -> Result<Response<proto::NotifyPromotionSuccessResponse>, Status> {
        let req = request.into_inner();
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let task = self
            .state
            .promotion_tasks
            .get(&req.key)
            .ok_or(Status::failed_precondition("promotion task not found"))?
            .clone();
        if task.holder_id != client_id {
            return Err(Status::permission_denied("promotion task assigned to different client"));
        }
        let Some(segment_id) = task.staged_segment_id else {
            return Err(Status::failed_precondition("promotion buffer not allocated"));
        };
        let Some(offset) = task.staged_offset else {
            return Err(Status::failed_precondition("promotion buffer not allocated"));
        };
        let mut committed = false;
        let mut object = self
            .state
            .objects
            .get_mut(&req.key)
            .ok_or(Status::not_found("key not found"))?;
        if let Some(replica) = object.replicas.iter_mut().find(|replica| {
            replica.replica_type == ReplicaType::Memory
                && replica.segment_id == segment_id
                && replica.offset == offset
                && replica.status == ReplicaStatus::Allocating
        }) {
            replica.status = ReplicaStatus::Complete;
            committed = true;
        }
        drop(object);
        clear_promotion_task(&self.state, &req.key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&req.key);
        }
        if !committed {
            return Err(Status::failed_precondition("promotion replica not ready"));
        }
        Ok(Response::new(proto::NotifyPromotionSuccessResponse {}))
    }

    // ---- NotifyPromotionFailure ----
    async fn notify_promotion_failure(
        &self,
        request: Request<proto::NotifyPromotionFailureRequest>,
    ) -> Result<Response<proto::NotifyPromotionFailureResponse>, Status> {
        let req = request.into_inner();
        let client_id =
            uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let Some(task) = self.state.promotion_tasks.get(&req.key).map(|task| task.clone()) else {
            return Ok(Response::new(proto::NotifyPromotionFailureResponse {}));
        };
        if task.holder_id != client_id {
            return Err(Status::permission_denied("promotion task assigned to different client"));
        }
        if let (Some(segment_id), Some(offset)) = (task.staged_segment_id, task.staged_offset) {
            release_staged_promotion_replica(&self.state, &req.key, segment_id, offset);
        }
        clear_promotion_task(&self.state, &req.key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&req.key);
        }
        Ok(Response::new(proto::NotifyPromotionFailureResponse {}))
    }

    // ---- ExistKey ----
    async fn exist_key(
        &self,
        request: Request<proto::ExistKeyRequest>,
    ) -> Result<Response<proto::ExistKeyResponse>, Status> {
        let req = request.into_inner();
        let exists = self.state.objects.contains_key(&req.key);
        metrics::GET_REQUESTS.inc();
        Ok(Response::new(proto::ExistKeyResponse { exists }))
    }

    // ---- GetAllKeys ----
    async fn get_all_keys(
        &self,
        _request: Request<proto::GetAllKeysRequest>,
    ) -> Result<Response<proto::GetAllKeysResponse>, Status> {
        let keys: Vec<String> = self.state.objects.iter().map(|entry| entry.key().clone()).collect();
        Ok(Response::new(proto::GetAllKeysResponse { keys }))
    }

    // ---- GetAllSegments ----
    async fn get_all_segments(
        &self,
        _request: Request<proto::GetAllSegmentsRequest>,
    ) -> Result<Response<proto::GetAllSegmentsResponse>, Status> {
        let segments: Vec<String> = self.state.segments.iter().map(|entry| entry.segment.name.clone()).collect();
        Ok(Response::new(proto::GetAllSegmentsResponse { segments }))
    }

    // ---- PutStart ----
    async fn put_start(
        &self,
        request: Request<proto::PutStartRequest>,
    ) -> Result<Response<proto::PutStartResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();

        if self.state.objects.contains_key(&key) {
            return Err(Status::already_exists(format!("object already exists: {}", key)));
        }

        let config = req.config.as_ref().map(config_from_proto).unwrap_or_default();
        let replica_count = if config.replica_num == 0 { 1 } else { config.replica_num as usize };

        let replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate(&key, req.slice_length, replica_count, &config)
        };
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(key, ObjectEntry {
            replicas,
            size: req.slice_length,
            last_access: SystemTime::now(),
            soft_pinned: config.with_soft_pin,
        });

        metrics::PUT_START_REQUESTS.inc();
        Ok(Response::new(proto::PutStartResponse { replicas: proto_replicas }))
    }

    // ---- PutEnd ----
    async fn put_end(
        &self,
        request: Request<proto::PutEndRequest>,
    ) -> Result<Response<proto::PutEndResponse>, Status> {
        let req = request.into_inner();
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            for r in &mut entry.replicas {
                if r.status == ReplicaStatus::Allocating {
                    r.status = ReplicaStatus::Complete;
                }
            }
            let size = entry.size;
            drop(entry);
            let client_id =
                uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
            push_offloading_queue(&self.state, client_id, &req.key, size);
        }
        Ok(Response::new(proto::PutEndResponse {}))
    }

    // ---- AddReplica ----
    async fn add_replica(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
        let req = request.into_inner();
        let replica = req.replica.as_ref().map(replica_from_proto).ok_or(Status::invalid_argument("missing replica"))?;
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            if replica.replica_type == ReplicaType::LocalDisk {
                if let Some(existing) = entry.replicas.iter_mut().find(|existing| {
                    existing.replica_type == ReplicaType::LocalDisk
                        && existing.holder_client_id == replica.holder_client_id
                }) {
                    *existing = replica;
                } else {
                    entry.replicas.push(replica);
                }
            } else {
                entry.replicas.push(replica);
            }
        } else if replica.replica_type == ReplicaType::LocalDisk {
            self.state.objects.insert(
                req.key.clone(),
                ObjectEntry {
                    size: replica.size,
                    replicas: vec![replica],
                    last_access: SystemTime::now(),
                    soft_pinned: false,
                },
            );
        }
        Ok(Response::new(proto::AddReplicaResponse {}))
    }

    // ---- GetReplicaList ----
    async fn get_replica_list(
        &self,
        request: Request<proto::GetReplicaListRequest>,
    ) -> Result<Response<proto::GetReplicaListResponse>, Status> {
        let req = request.into_inner();
        match self.state.objects.get_mut(&req.key) {
            Some(mut entry) => {
                entry.last_access = SystemTime::now();
                let replicas = entry.replicas.iter().map(replica_to_proto).collect();
                let promotion_eligible = !entry
                    .replicas
                    .iter()
                    .any(|replica| replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete)
                    && entry
                        .replicas
                        .iter()
                        .any(|replica| replica.replica_type == ReplicaType::LocalDisk && replica.status == ReplicaStatus::Complete);
                drop(entry);
                if promotion_eligible {
                    try_push_promotion_queue(&self.state, &req.key);
                }
                metrics::GET_REQUESTS.inc();
                Ok(Response::new(proto::GetReplicaListResponse { replicas }))
            }
            None => Err(Status::not_found(format!("key not found: {}", req.key))),
        }
    }

    // ---- Remove ----
    async fn remove(
        &self,
        request: Request<proto::RemoveRequest>,
    ) -> Result<Response<proto::RemoveResponse>, Status> {
        let req = request.into_inner();
        if let Some((_, object)) = self.state.objects.remove(&req.key) {
            clear_offloading_task(&self.state, &req.key);
            clear_promotion_task(&self.state, &req.key);
            let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
            self.state.allocator.write().release(&object.replicas);
            sync_segment_usage(&self.state, segment_ids);
        }
        metrics::REMOVE_REQUESTS.inc();
        Ok(Response::new(proto::RemoveResponse {}))
    }

    // ---- RemoveByRegex ----
    async fn remove_by_regex(
        &self,
        request: Request<proto::RemoveByRegexRequest>,
    ) -> Result<Response<proto::RemoveByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut removed = 0i64;
        let keys_to_remove: Vec<String> = self
            .state
            .objects
            .iter()
            .filter(|entry| pattern.is_match(entry.key()))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            if let Some((_, object)) = self.state.objects.remove(&key) {
                clear_offloading_task(&self.state, &key);
                clear_promotion_task(&self.state, &key);
                let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&object.replicas);
                sync_segment_usage(&self.state, segment_ids);
                removed += 1;
            }
        }

        metrics::REMOVE_BY_REGEX_REQUESTS.inc();
        metrics::REMOVE_REQUESTS.inc_by(removed as u64);
        Ok(Response::new(proto::RemoveByRegexResponse { removed_count: removed }))
    }

    // ---- BatchExistKey ----
    async fn batch_exist_key(
        &self,
        request: Request<proto::BatchExistKeyRequest>,
    ) -> Result<Response<proto::BatchExistKeyResponse>, Status> {
        let req = request.into_inner();
        let results: Vec<bool> = req.keys.iter().map(|k| self.state.objects.contains_key(k)).collect();
        Ok(Response::new(proto::BatchExistKeyResponse { results }))
    }

    // ---- BatchQueryIp ----
    async fn batch_query_ip(
        &self,
        request: Request<proto::BatchQueryIpRequest>,
    ) -> Result<Response<proto::BatchQueryIpResponse>, Status> {
        let req = request.into_inner();
        let mut ips = std::collections::HashMap::new();
        for cid in &req.client_ids {
            let id = uuid_from_proto(cid);
            let addresses = addresses_for_client(&self.state, id);
            if !addresses.is_empty() {
                ips.insert(
                    id.to_string(),
                    proto::IpList { addresses },
                );
            }
        }
        Ok(Response::new(proto::BatchQueryIpResponse { ips }))
    }

    // ---- BatchReplicaClear ----
    async fn batch_replica_clear(
        &self,
        request: Request<proto::BatchReplicaClearRequest>,
    ) -> Result<Response<proto::BatchReplicaClearResponse>, Status> {
        let req = request.into_inner();
        let mut cleared = vec![];
        for key in &req.object_keys {
            if let Some((_, object)) = self.state.objects.remove(key) {
                clear_offloading_task(&self.state, key);
                clear_promotion_task(&self.state, key);
                let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&object.replicas);
                sync_segment_usage(&self.state, segment_ids);
                cleared.push(key.clone());
            }
        }
        Ok(Response::new(proto::BatchReplicaClearResponse { cleared_keys: cleared }))
    }

    // ---- QueryByRegex ----
    async fn query_by_regex(
        &self,
        request: Request<proto::QueryByRegexRequest>,
    ) -> Result<Response<proto::QueryByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        for entry in self.state.objects.iter() {
            if pattern.is_match(entry.key()) {
                let r = entry.replicas.iter().map(replica_to_proto).collect();
                entries.push(proto::query_by_regex_response::Entry {
                    key: entry.key().clone(),
                    replicas: r,
                });
            }
        }
        Ok(Response::new(proto::QueryByRegexResponse { entries }))
    }

    // ---- QuerySegments ----
    async fn query_segments(
        &self,
        request: Request<proto::QuerySegmentsRequest>,
    ) -> Result<Response<proto::QuerySegmentsResponse>, Status> {
        let req = request.into_inner();
        for entry in self.state.segments.iter() {
            if entry.segment.name == req.segment_name {
                return Ok(Response::new(proto::QuerySegmentsResponse {
                    total_size: entry.segment.size,
                    used_size: entry.segment.used,
                }));
            }
        }
        Err(Status::not_found("segment not found"))
    }

    // ---- QueryIp ----
    async fn query_ip(
        &self,
        request: Request<proto::QueryIpRequest>,
    ) -> Result<Response<proto::QueryIpResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let addresses = addresses_for_client(&self.state, client_id);
        if addresses.is_empty() {
            Err(Status::not_found("client not found"))
        } else {
            Ok(Response::new(proto::QueryIpResponse { addresses }))
        }
    }

    // ---- Upsert ----
    async fn upsert(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        let req = request.into_inner();
        let config = req.config.as_ref().map(config_from_proto).unwrap_or_default();
        let replica_count = if config.replica_num == 0 { 1 } else { config.replica_num as usize };

        // Match the C++ store behavior: only reuse placement when the object
        // size stays the same, otherwise release old space and allocate again.
        let replicas = if let Some(existing) = self.state.objects.get(&req.key) {
            if existing.size == req.slice_length {
                existing.replicas.clone()
            } else {
                let old_replicas = existing.replicas.clone();
                drop(existing);
                let segment_ids: Vec<Uuid> = old_replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&old_replicas);
                sync_segment_usage(&self.state, segment_ids);
                let mut allocator = self.state.allocator.write();
                allocator.allocate(&req.key, req.slice_length, replica_count, &config)
            }
        } else {
            let mut allocator = self.state.allocator.write();
            allocator.allocate(&req.key, req.slice_length, replica_count, &config)
        };
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(req.key.clone(), ObjectEntry {
            replicas,
            size: req.slice_length,
            last_access: SystemTime::now(),
            soft_pinned: config.with_soft_pin,
        });

        Ok(Response::new(proto::UpsertResponse { replicas: proto_replicas }))
    }

    // ---- CreateCopyTask ----
    async fn create_copy_task(
        &self,
        request: Request<proto::CreateCopyTaskRequest>,
    ) -> Result<Response<proto::CreateCopyTaskResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("missing key"));
        }
        if req.targets.is_empty() {
            return Err(Status::invalid_argument("missing targets"));
        }
        let object = self.state.objects.get(&req.key).ok_or(Status::not_found("key not found"))?;
        if object.replicas.is_empty() {
            return Err(Status::failed_precondition("object has no source replicas"));
        }
        for target in &req.targets {
            if client_id_by_segment_name(&self.state, target).is_none() {
                return Err(Status::invalid_argument(format!("target segment not mounted: {target}")));
            }
        }
        let source_segment = object.replicas[0].segment_name.clone();
        let assigned_client = client_id_by_segment_name(&self.state, &source_segment)
            .ok_or(Status::failed_precondition("source segment missing"))?;
        let task_key = req.key.clone();
        let task_payload = serde_json::to_string(&ReplicaCopyPayload {
            key: &req.key,
            source: &source_segment,
            targets: &req.targets,
        })
        .map_err(|e| Status::internal(format!("serialize task payload: {e}")))?;
        drop(object);
        if !self.state.objects.contains_key(&req.key) {
            return Err(Status::not_found("key not found"));
        }
        let task_id = Uuid::new_v4();
        let now = Utc::now();
        self.state.tasks.insert(task_id, TaskEntry {
            info: TaskInfo {
                id: task_id,
                task_type: TaskType::ReplicaCopy,
                status: TaskStatus::Pending,
                created_at: now,
                last_updated_at: now,
                assigned_client: Some(assigned_client),
                message: format!("copy {} to {} target(s)", req.key, req.targets.len()),
            },
            key: task_key,
            payload: task_payload,
            max_retry_attempts: 0,
        });
        Ok(Response::new(proto::CreateCopyTaskResponse { task_id: Some(uuid_to_proto(task_id)) }))
    }

    // ---- CreateMoveTask ----
    async fn create_move_task(
        &self,
        request: Request<proto::CreateMoveTaskRequest>,
    ) -> Result<Response<proto::CreateMoveTaskResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() || req.source.is_empty() || req.target.is_empty() {
            return Err(Status::invalid_argument("missing key/source/target"));
        }
        if req.source == req.target {
            return Err(Status::invalid_argument("source and target must differ"));
        }
        let object = self.state.objects.get(&req.key).ok_or(Status::not_found("key not found"))?;
        if !object.replicas.iter().any(|replica| replica.segment_name == req.source) {
            return Err(Status::invalid_argument("source segment not found"));
        }
        let assigned_client = client_id_by_segment_name(&self.state, &req.source)
            .ok_or(Status::failed_precondition("source segment missing"))?;
        if client_id_by_segment_name(&self.state, &req.target).is_none() {
            return Err(Status::invalid_argument("target segment not mounted"));
        }
        let task_key = req.key.clone();
        let task_payload = serde_json::to_string(&ReplicaMovePayload {
            key: &req.key,
            source: &req.source,
            target: &req.target,
        })
        .map_err(|e| Status::internal(format!("serialize task payload: {e}")))?;
        drop(object);
        let task_id = Uuid::new_v4();
        let now = Utc::now();
        self.state.tasks.insert(task_id, TaskEntry {
            info: TaskInfo {
                id: task_id,
                task_type: TaskType::ReplicaMove,
                status: TaskStatus::Pending,
                created_at: now,
                last_updated_at: now,
                assigned_client: Some(assigned_client),
                message: format!("move {} from {} to {}", req.key, req.source, req.target),
            },
            key: task_key,
            payload: task_payload,
            max_retry_attempts: 0,
        });
        Ok(Response::new(proto::CreateMoveTaskResponse { task_id: Some(uuid_to_proto(task_id)) }))
    }

    // ---- QueryTask ----
    async fn query_task(
        &self,
        request: Request<proto::QueryTaskRequest>,
    ) -> Result<Response<proto::QueryTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = uuid_from_proto(req.task_id.as_ref().ok_or(Status::invalid_argument("missing task_id"))?);
        let task = self.state.tasks.get(&task_id).ok_or(Status::not_found("task not found"))?;
        Ok(Response::new(proto::QueryTaskResponse {
            id: Some(uuid_to_proto(task.info.id)),
            task_type: task_type_to_proto(task.info.task_type),
            status: task_status_to_proto(task.info.status),
            created_at_ms_epoch: task.info.created_at.timestamp_millis(),
            last_updated_at_ms_epoch: task.info.last_updated_at.timestamp_millis(),
            assigned_client: task.info.assigned_client.map(uuid_to_proto),
            message: task.info.message.clone(),
        }))
    }

    // ---- FetchTasks ----
    async fn fetch_tasks(
        &self,
        request: Request<proto::FetchTasksRequest>,
    ) -> Result<Response<proto::FetchTasksResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let batch_size = if req.batch_size == 0 { usize::MAX } else { req.batch_size as usize };

        let mut pending = self
            .state
            .tasks
            .iter()
            .filter(|entry| {
                entry.info.assigned_client == Some(client_id)
                    && entry.info.status == TaskStatus::Pending
            })
            .map(|entry| (entry.key().to_owned(), entry.info.created_at))
            .collect::<Vec<_>>();
        pending.sort_by_key(|(_, created_at)| *created_at);

        let mut tasks = Vec::new();
        for (task_id, _) in pending.into_iter().take(batch_size) {
            if let Some(mut task) = self.state.tasks.get_mut(&task_id) {
                task.info.status = TaskStatus::Processing;
                task.info.last_updated_at = Utc::now();
                tasks.push(proto::TaskAssignment {
                    id: Some(uuid_to_proto(task.info.id)),
                    r#type: task_type_to_proto(task.info.task_type),
                    payload: task.payload.clone(),
                    created_at_ms_epoch: task.info.created_at.timestamp_millis(),
                    max_retry_attempts: task.max_retry_attempts,
                });
            }
        }

        Ok(Response::new(proto::FetchTasksResponse { tasks }))
    }

    // ---- MarkTaskToComplete ----
    async fn mark_task_to_complete(
        &self,
        request: Request<proto::MarkTaskToCompleteRequest>,
    ) -> Result<Response<proto::MarkTaskToCompleteResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let task_req = req.request.as_ref().ok_or(Status::invalid_argument("missing request"))?;
        let task_id = uuid_from_proto(task_req.id.as_ref().ok_or(Status::invalid_argument("missing task id"))?);
        let mut task = self.state.tasks.get_mut(&task_id).ok_or(Status::not_found("task not found"))?;
        if task.info.assigned_client != Some(client_id) {
            return Err(Status::permission_denied("task assigned to different client"));
        }
        task.info.status = task_status_from_proto(task_req.status);
        task.info.message = task_req.message.clone();
        task.info.last_updated_at = Utc::now();
        Ok(Response::new(proto::MarkTaskToCompleteResponse {}))
    }

    // ---- BatchPutEnd ----
    async fn batch_put_end(
        &self,
        request: Request<proto::BatchPutEndRequest>,
    ) -> Result<Response<proto::BatchPutEndResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .entries
            .iter()
            .map(|entry| {
                if let Some(mut obj) = self.state.objects.get_mut(&entry.key) {
                    let size = obj.size;
                    for r in &mut obj.replicas {
                        if r.status == ReplicaStatus::Allocating {
                            r.status = ReplicaStatus::Complete;
                        }
                    }
                    drop(obj);
                    if let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) {
                        push_offloading_queue(&self.state, client_id, &entry.key, size);
                    }
                    0
                } else {
                    -1
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutEndResponse { statuses }))
    }

    // ---- BatchPutRevoke ----
    async fn batch_put_revoke(
        &self,
        request: Request<proto::BatchPutRevokeRequest>,
    ) -> Result<Response<proto::BatchPutRevokeResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|key| {
                if let Some((_, object)) = self.state.objects.remove(key) {
                    clear_offloading_task(&self.state, key);
                    clear_promotion_task(&self.state, key);
                    let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&object.replicas);
                    sync_segment_usage(&self.state, segment_ids);
                    0
                } else {
                    -1
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutRevokeResponse { statuses }))
    }

    // ---- BatchRemove ----
    async fn batch_remove(
        &self,
        request: Request<proto::BatchRemoveRequest>,
    ) -> Result<Response<proto::BatchRemoveResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|key| {
                if let Some((_, object)) = self.state.objects.remove(key) {
                    clear_offloading_task(&self.state, key);
                    clear_promotion_task(&self.state, key);
                    let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&object.replicas);
                    sync_segment_usage(&self.state, segment_ids);
                }
                0
            })
            .collect();
        metrics::BATCH_REMOVE_REQUESTS.inc_by(req.keys.len() as u64);
        Ok(Response::new(proto::BatchRemoveResponse { statuses }))
    }

    // ---- BatchUpsertEnd ----
    async fn batch_upsert_end(
        &self,
        request: Request<proto::BatchUpsertEndRequest>,
    ) -> Result<Response<proto::BatchUpsertEndResponse>, Status> {
        let req = request.into_inner();
        let mut all_replicas = Vec::new();

        for entry in &req.entries {
            let config = entry.config.as_ref().map(config_from_proto).unwrap_or_default();
            let replica_count = if config.replica_num == 0 { 1 } else { config.replica_num as usize };

            let replicas = if let Some(existing) = self.state.objects.get(&entry.key) {
                if existing.size == entry.slice_length {
                    existing.replicas.clone()
                } else {
                    let old_replicas = existing.replicas.clone();
                    drop(existing);
                    let segment_ids: Vec<Uuid> = old_replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&old_replicas);
                    sync_segment_usage(&self.state, segment_ids);
                    let mut allocator = self.state.allocator.write();
                    allocator.allocate(&entry.key, entry.slice_length, replica_count, &config)
                }
            } else {
                let mut allocator = self.state.allocator.write();
                allocator.allocate(&entry.key, entry.slice_length, replica_count, &config)
            };

            let proto_r: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();
            sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));
            self.state.objects.insert(entry.key.clone(), ObjectEntry {
                replicas,
                size: entry.slice_length,
                last_access: SystemTime::now(),
                soft_pinned: config.with_soft_pin,
            });
            all_replicas.extend(proto_r);
        }

        Ok(Response::new(proto::BatchUpsertEndResponse { replicas: all_replicas }))
    }
}

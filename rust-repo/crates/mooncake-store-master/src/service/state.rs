use crate::allocator::{AllocationStrategy, MemoryAllocatorKind, SegmentAllocator};
use crate::count_min_sketch::CountMinSketch;
use crate::storage_backend::StorageBackend;
use dashmap::DashMap;
use mooncake_store_core::{NoFSegment, ObjectDataType, ReplicaDescriptor, TaskInfo};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

pub(crate) struct MasterState {
    pub(crate) clients: DashMap<Uuid, ClientEntry>,
    pub(crate) objects: DashMap<String, ObjectEntry>,
    pub(crate) processing_keys: DashMap<String, ()>,
    pub(crate) segments: DashMap<Uuid, SegmentEntry>,
    pub(crate) nof_segments: DashMap<Uuid, NoFSegmentEntry>,
    pub(crate) local_disk_segments: DashMap<Uuid, LocalDiskSegmentEntry>,
    pub(crate) tasks: DashMap<Uuid, TaskEntry>,
    pub(crate) replication_tasks: DashMap<String, ReplicationTaskEntry>,
    pub(crate) offloading_tasks: DashMap<String, OffloadingTaskEntry>,
    pub(crate) promotion_tasks: DashMap<String, PromotionTaskEntry>,
    pub(crate) promotion_sketch: RwLock<CountMinSketch>,
    pub(crate) drain_jobs: DashMap<Uuid, DrainJobEntry>,
    pub(crate) allocator: RwLock<SegmentAllocator>,
    pub(crate) nof_allocator: RwLock<SegmentAllocator>,
    pub(crate) storage_backend: RwLock<Option<StorageBackend>>,
    pub(crate) promotion_in_flight: AtomicUsize,
    pub(crate) view_version: AtomicI64,
    pub(crate) runtime_config: MasterRuntimeConfig,
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
    #[serde(default)]
    pub hard_pinned: bool,
    #[serde(default)]
    pub data_type: ObjectDataType,
    #[serde(skip)]
    pub put_start_time: Option<SystemTime>,
    #[serde(skip)]
    pub lease_timeout: Option<SystemTime>,
    #[serde(skip)]
    pub soft_pin_timeout: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct SegmentEntry {
    pub segment: mooncake_store_core::Segment,
    pub used: u64,
    pub client_id: Uuid,
    pub status: crate::proto::SegmentStatus,
}

#[derive(Debug, Clone)]
pub struct NoFSegmentEntry {
    pub segment: NoFSegment,
    pub used: u64,
    pub status: crate::proto::SegmentStatus,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicationTaskKind {
    Copy,
    Move,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicationTaskEntry {
    pub(crate) client_id: Uuid,
    pub(crate) kind: ReplicationTaskKind,
    pub(crate) source: ReplicaDescriptor,
    pub(crate) targets: Vec<ReplicaDescriptor>,
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

#[derive(Debug, Clone)]
pub struct MasterRuntimeConfig {
    pub put_start_discard_timeout: Duration,
    pub put_start_release_timeout: Duration,
    pub allocation_strategy: AllocationStrategy,
    pub memory_allocator_kind: MemoryAllocatorKind,
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
    pub client_live_ttl: Duration,
    pub client_monitor_interval: Duration,
    pub storage_fs_dir: String,
    pub enable_disk_eviction: bool,
    pub quota_bytes: u64,
    /// CXL memory path (e.g., "/dev/dax0.0"). Empty means CXL is disabled.
    pub cxl_path: String,
    pub cxl_size: u64,
    pub enable_cxl: bool,
}

impl Default for MasterRuntimeConfig {
    fn default() -> Self {
        Self {
            put_start_discard_timeout: Duration::from_secs(30),
            put_start_release_timeout: Duration::from_secs(600),
            allocation_strategy: AllocationStrategy::Random,
            memory_allocator_kind: MemoryAllocatorKind::Offset,
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
            client_live_ttl: Duration::from_secs(30),
            client_monitor_interval: Duration::from_secs(1),
            storage_fs_dir: String::new(),
            enable_disk_eviction: false,
            quota_bytes: 0,
            cxl_path: String::new(),
            cxl_size: 8 * 1024 * 1024 * 1024,
            enable_cxl: false,
        }
    }
}

/// Tracks an in-flight drain unit task for a single key during segment draining.
#[derive(Debug, Clone)]
#[allow(dead_code, reason = "partially implemented drain feature")]
pub(crate) struct ActiveDrainTask {
    pub(crate) task_id: Uuid,
    pub(crate) key: String,
    pub(crate) source_segment: String,
    pub(crate) target_segment: String,
    pub(crate) bytes: u64,
    pub(crate) unit_key: String,
}

/// A drain job that moves objects from draining segments to target segments.
#[derive(Debug, Clone)]
#[allow(dead_code, reason = "partially implemented drain feature")]
pub(crate) struct DrainJobEntry {
    pub(crate) id: Uuid,
    pub(crate) status: crate::proto::JobStatus,
    pub(crate) segments: Vec<String>,
    pub(crate) target_segments: Vec<String>,
    pub(crate) max_concurrency: u32,
    pub(crate) created_at: SystemTime,
    pub(crate) last_updated_at: SystemTime,
    pub(crate) message: String,
    pub(crate) succeeded_units: u64,
    pub(crate) failed_units: u64,
    pub(crate) blocked_units: u64,
    pub(crate) migrated_bytes: u64,
    pub(crate) active_tasks: HashMap<Uuid, ActiveDrainTask>,
    pub(crate) completed_unit_keys: HashSet<String>,
    pub(crate) retry_counts: HashMap<String, u32>,
    pub(crate) terminal_failed_unit_keys: HashSet<String>,
}

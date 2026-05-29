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

// MasterState: master 服务的全局状态容器，所有数据结构均为并发安全类型。
// DashMap 用于高并发读写（分段锁），RwLock 用于需要事务性一致性的操作。
// processing_keys 跟踪正在 PutStart 但未 Complete 的 key，防止并发冲突。
// client_objects 维护每个客户端的对象索引，加速客户端下线时批量清理。
pub(crate) struct MasterState {
    pub(crate) clients: DashMap<Uuid, ClientEntry>,
    pub(crate) objects: DashMap<String, ObjectEntry>,
    pub(crate) processing_keys: DashMap<String, ()>,
    pub(crate) client_objects: DashMap<Uuid, HashSet<String>>,
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
    /// Tracks in-flight remote source pulls so only one node fetches a given key.
    pub(crate) pending_remote_pulls: DashMap<String, RemotePullEntry>,
}

/// Tracks which node is currently fetching a key from the remote source.
#[derive(Debug, Clone)]
pub(crate) struct RemotePullEntry {
    #[allow(dead_code)]
    pub(crate) puller_client_id: Uuid,
    pub(crate) started_at: Instant,
}

pub(crate) struct ClientEntry {
    pub(crate) info: mooncake_store_core::ClientInfo,
    pub(crate) last_ping: SystemTime,
}

// ObjectEntry: 对象的完整元数据。put_start_time/lease_timeout/soft_pin_timeout
// 标记为 serde(skip) 因为这些是运行时状态，不应持久化到快照中。
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
    #[serde(default)]
    pub client_id: Uuid,
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

// ReplicationTaskKind: 区分 Copy（保留源副本）和 Move（完成后删除源副本）两种复制语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicationTaskKind {
    Copy,
    Move,
}

// ReplicationTaskEntry: 记录进行中的复制任务，包含源和目标副本描述符，用于 CopyEnd/MoveEnd 验证。
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

// MasterRuntimeConfig: 运行时配置参数，控制 lease TTL、eviction 水位线、promotion 策略等。
// 所有 Duration 字段使用 std::time::Duration 表示。
#[derive(Debug, Clone)]
pub struct MasterRuntimeConfig {
    /// 未完成的 PutStart 超过此时间后被新的同 key PutStart 丢弃。
    pub put_start_discard_timeout: Duration,
    /// 丢弃或 release 的副本延迟释放时间，防止仍在传输中的 RDMA 访问已回收内存。
    pub put_start_release_timeout: Duration,
    /// 段选择策略：Random（随机）或 FreeRatioFirst（空闲率优先）。
    pub allocation_strategy: AllocationStrategy,
    /// 段内内存分配器：Offset（简单连续分配）或 CachelibLike（slab + class 分配）。
    pub memory_allocator_kind: MemoryAllocatorKind,
    /// 是否开启 promotion-on-hit：读磁盘副本时自动将热点对象提升到内存。
    pub promotion_on_hit: bool,
    /// 提升准入阈值：对象被访问达到此次数后才放入提升队列。
    pub promotion_admission_threshold: u8,
    /// 提升队列最大长度，超过后新的提升请求被丢弃。
    pub promotion_queue_limit: usize,
    /// 后台 reaper 轮询间隔，用于清理过期的 offload / promotion / PutStart 任务。
    pub reaper_interval: Duration,
    /// 自动淘汰检查的轮询间隔。
    pub eviction_interval: Duration,
    /// 触发淘汰的内存使用率水位（0.0 ~ 1.0），超过后启动淘汰。
    pub eviction_high_watermark_ratio: f64,
    /// 每次淘汰尝试释放的内存比例（0.0 ~ 1.0）。
    pub eviction_ratio: f64,
    /// 软锁定（soft pin）对象的租约时长，过期后软锁定失效但仍优先保留。
    pub soft_pin_ttl: Duration,
    /// KV 对象默认租约时长：PutEnd / GetReplicaList 授时，淘汰时超过此 TTL 的对象允许驱逐。
    pub lease_ttl: Duration,
    /// 淘汰时是否触发 offload（将内存副本写入本地磁盘）。
    pub offload_on_evict: bool,
    /// 淘汰时是否强制驱逐（即使有 soft_pin 也驱逐）。
    pub offload_force_evict: bool,
    /// 客户端心跳 TTL，超过此时间未 ping 的客户端视为下线。
    pub client_live_ttl: Duration,
    /// 客户端监控（client monitor）轮询间隔，检查下线客户端并释放其资源。
    pub client_monitor_interval: Duration,
    /// HA 快照存储目录路径。
    pub storage_fs_dir: String,
    /// 透传给客户端的磁盘淘汰开关，master 端淘汰逻辑暂未消费此字段。
    pub enable_disk_eviction: bool,
    /// 透传给客户端的存储配额（字节），master 端暂未实现配额限流。
    pub quota_bytes: u64,
    /// Enable remote source (S3) fallback for cache misses.
    pub remote_source_enabled: bool,
    /// TTL for a pending remote pull entry before it is considered stale.
    pub remote_pull_ttl: Duration,
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
            remote_source_enabled: false,
            remote_pull_ttl: Duration::from_secs(60),
        }
    }
}

/// Tracks an in-flight drain unit task for a single key during segment draining.
#[derive(Debug, Clone)]
pub(crate) struct ActiveDrainTask {
    pub(crate) source_segment: String,
    pub(crate) target_segment: String,
}

/// A drain job that moves objects from draining segments to target segments.
#[derive(Debug, Clone)]
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
    pub(crate) terminal_failed_unit_keys: HashSet<String>,
}

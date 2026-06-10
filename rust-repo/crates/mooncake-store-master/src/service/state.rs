//! # MasterState — Master 核心状态机 / Core State Machine
//!
//! MasterState 是 master 服务的全局状态容器，所有数据结构均为并发安全类型。
//! DashMap 用于高并发读写（分段锁），RwLock 用于需要事务性一致性的操作。
//!
//! MasterState is the global state container of the Master service.
//! All data structures are concurrency-safe types.
//! DashMap is used for high-concurrency reads/writes (sharded locks),
//! while RwLock is used for operations requiring transactional consistency.
//!
//! ## 核心数据结构 / Core Data Structures
//!
//! | 字段 / Field | 类型 / Type | 用途 / Purpose |
//! |--------------|-------------|----------------|
//! | `clients` | `DashMap<Uuid, ClientEntry>` | 客户端注册表：记录客户端信息、地址、心跳时间 |
//! | `objects` | `DashMap<String, ObjectEntry>` | 对象元数据表：key → 副本列表、大小、租约、pin 状态 |
//! | `processing_keys` | `DashMap<String, ()>` | 进行中的 PutStart key 集合：防止并发写冲突 |
//! | `client_objects` | `DashMap<Uuid, HashSet<String>>` | 每个客户端拥有的对象索引：加速客户端下线时批量清理 |
//! | `segments` | `DashMap<Uuid, SegmentEntry>` | Memory segment 注册表：容量、使用量、所属客户端 |
//! | `nof_segments` | `DashMap<Uuid, NoFSegmentEntry>` | NoF (NVMe-oF) segment 注册表：远程 SSD 存储分配 |
//! | `local_disk_segments` | `DashMap<Uuid, LocalDiskSegmentEntry>` | 本地磁盘 segment：offload/promotion 队列、SSD 容量 |
//! | `tasks` | `DashMap<Uuid, TaskEntry>` | 任务队列：Copy/Move 任务的创建、分配、执行 |
//! | `replication_tasks` | `DashMap<String, ReplicationTaskEntry>` | 进行中的复制任务：按 key 索引 |
//! | `offloading_tasks` | `DashMap<String, OffloadingTaskEntry>` | 进行中的下沉任务：内存 → 本地磁盘 |
//! | `promotion_tasks` | `DashMap<String, PromotionTaskEntry>` | 进行中的提升任务：本地磁盘 → 内存 |
//! | `promotion_sketch` | `RwLock<CountMinSketch>` | Count-Min Sketch 频率统计：promotion 准入控制 |
//! | `drain_jobs` | `DashMap<Uuid, DrainJobEntry>` | Drain 任务：segment 下线前的数据迁移 |
//! | `allocator` | `RwLock<SegmentAllocator>` | Memory segment 分配器：管理内存段的空间分配 |
//! | `nof_allocator` | `RwLock<SegmentAllocator>` | NoF segment 分配器：管理远程 SSD 段的空间分配 |
//! | `storage_backend` | `RwLock<Option<StorageBackend>>` | 快照持久化后端：状态的定期备份与恢复 |
//! | `promotion_in_flight` | `AtomicUsize` | 全局在途晋升计数：防止并发晋升超过队列限制 |
//! | `view_version` | `AtomicI64` | 全局视图版本号：通知客户端拓扑变更 |
//! | `runtime_config` | `MasterRuntimeConfig` | 运行时配置：所有可调参数的汇总 |
//! | `pending_remote_pulls` | `DashMap<String, RemotePullEntry>` | 进行中的远端拉取：S3 等远端源的回源协调 |

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

/// Master 服务的全局状态容器。
/// processing_keys 跟踪正在 PutStart 但未 Complete 的 key，防止并发冲突。
/// client_objects 维护每个客户端的对象索引，加速客户端下线时批量清理。
///
/// Global state container for the Master service.
/// `processing_keys` tracks keys that are in PutStart but not yet Complete, preventing concurrent conflicts.
/// `client_objects` maintains per-client object indices for fast batch cleanup on client disconnection.
pub(crate) struct MasterState {
    /// 客户端注册表 / Client registry: client_id → address, last_ping, etc.
    pub(crate) clients: DashMap<Uuid, ClientEntry>,
    /// 已完成 remount 的客户端 / Clients that completed remount and may receive OK on Ping.
    pub(crate) ok_clients: DashMap<Uuid, ()>,
    /// 对象元数据表 / Object metadata store: key → replicas, size, lease, pin status.
    pub(crate) objects: DashMap<String, ObjectEntry>,
    /// 进行中的 key 集合 / In-flight key set: keys currently in PutStart (not yet PutEnd).
    pub(crate) processing_keys: DashMap<String, ()>,
    /// 每个客户端的对象索引 / Per-client object index: client_id → set of owned keys.
    /// 加速客户端下线时的 O(N_keys) 查找 / Enables O(N_keys) lookup on client disconnection.
    pub(crate) client_objects: DashMap<Uuid, HashSet<String>>,
    /// Memory segment 注册表 / Memory segment registry: segment_id → capacity, usage, owner.
    pub(crate) segments: DashMap<Uuid, SegmentEntry>,
    /// NoF (NVMe-oF) segment 注册表 / NoF segment registry: segment_id → remote SSD storage.
    pub(crate) nof_segments: DashMap<Uuid, NoFSegmentEntry>,
    /// 本地磁盘 segment 注册表 / Local disk segment registry: client_id → offload/promotion queues.
    pub(crate) local_disk_segments: DashMap<Uuid, LocalDiskSegmentEntry>,
    /// 任务队列 / Task queue: task_id → TaskEntry (copy/move tasks).
    pub(crate) tasks: DashMap<Uuid, TaskEntry>,
    /// 进行中的复制任务 / In-flight replication tasks: key → ReplicationTaskEntry.
    pub(crate) replication_tasks: DashMap<String, ReplicationTaskEntry>,
    /// 进行中的下沉任务 / In-flight offloading tasks: key → OffloadingTaskEntry.
    pub(crate) offloading_tasks: DashMap<String, OffloadingTaskEntry>,
    /// 进行中的提升任务 / In-flight promotion tasks: key → PromotionTaskEntry.
    pub(crate) promotion_tasks: DashMap<String, PromotionTaskEntry>,
    /// Count-Min Sketch 频率统计 / Frequency sketch: approximate access counts for promotion gating.
    pub(crate) promotion_sketch: RwLock<CountMinSketch>,
    /// Drain 任务注册表 / Drain job registry: job_id → DrainJobEntry.
    pub(crate) drain_jobs: DashMap<Uuid, DrainJobEntry>,
    /// Memory 分配器 / Memory segment allocator: manages space allocation within Memory segments.
    pub(crate) allocator: RwLock<SegmentAllocator>,
    /// NoF 分配器 / NoF segment allocator: manages space allocation within NoF segments.
    pub(crate) nof_allocator: RwLock<SegmentAllocator>,
    /// 快照存储后端 / Snapshot storage backend: periodic backup and restore.
    pub(crate) storage_backend: RwLock<Option<StorageBackend>>,
    /// 全局在途晋升计数 / Global in-flight promotion counter: prevents exceeding queue limit.
    pub(crate) promotion_in_flight: AtomicUsize,
    /// 全局视图版本号 / Global view version: notifies clients of topology changes.
    pub(crate) view_version: AtomicI64,
    /// 运行时配置 / Runtime configuration: all tunable parameters.
    pub(crate) runtime_config: MasterRuntimeConfig,
    /// Tracks in-flight remote source pulls so only one node fetches a given key.
    /// 远端回源协调表：确保同一 key 只有一个节点从远端（如 S3）拉取数据。
    pub(crate) pending_remote_pulls: DashMap<String, RemotePullEntry>,
    /// NoF 心跳状态表 / NoF heartbeat state table: segment_id → heartbeat tracking state.
    pub(crate) nof_heartbeat_states: DashMap<Uuid, NoFHeartbeatState>,
}

impl MasterState {
    /// Create a completely empty MasterState (for standby bootstrap).
    pub(crate) fn empty() -> Self {
        use crate::allocator::SegmentAllocator;
        use crate::count_min_sketch::CountMinSketch;
        use parking_lot::RwLock;
        use std::sync::atomic::{AtomicI64, AtomicUsize};
        Self {
            clients: DashMap::new(),
            ok_clients: DashMap::new(),
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
            allocator: RwLock::new(SegmentAllocator::new()),
            nof_allocator: RwLock::new(SegmentAllocator::new()),
            storage_backend: RwLock::new(None),
            promotion_in_flight: AtomicUsize::new(0),
            view_version: AtomicI64::new(0),
            runtime_config: MasterRuntimeConfig::default(),
            pending_remote_pulls: DashMap::new(),
            nof_heartbeat_states: DashMap::new(),
        }
    }
}

/// Tracks the start time of a remote fetch for a key.
/// 记录某个 key 的回源拉取开始时间。
#[derive(Debug, Clone)]
pub(crate) struct RemotePullEntry {
    /// 拉取开始时间，用于 TTL 过期判断 / Pull start time for TTL expiry.
    pub(crate) started_at: Instant,
}

/// 客户端条目：包含客户端信息和最后心跳时间戳。
/// Client entry: contains client info and last ping timestamp.
pub(crate) struct ClientEntry {
    pub(crate) info: mooncake_store_core::ClientInfo,
    /// 最后心跳时间 / Last ping time, used for liveness detection.
    pub(crate) last_ping: SystemTime,
}

/// ObjectEntry: 对象的完整元数据。
/// put_start_time/lease_timeout/soft_pin_timeout 标记为 serde(skip)
/// 因为这些是运行时状态，不应持久化到快照中。
///
/// ObjectEntry: complete metadata for an object.
/// `put_start_time`/`lease_timeout`/`soft_pin_timeout` are marked `serde(skip)`
/// because they are runtime-only state and should not be persisted in snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    /// 副本列表 / Replica list: describes each copy's location, status, type.
    pub replicas: Vec<ReplicaDescriptor>,
    /// 对象大小（字节）/ Object size in bytes.
    pub size: u64,
    /// 最后访问时间 / Last access time (for LRU eviction ranking).
    pub last_access: SystemTime,
    /// 硬固定状态：禁止驱逐 / Hard-pinned: never evicted.
    #[serde(default)]
    pub hard_pinned: bool,
    /// 数据类型：KV cache / Tensor / Weight 等 / Data type categorization.
    #[serde(default)]
    pub data_type: ObjectDataType,
    /// 对象所有者客户端 ID / Owning client ID.
    #[serde(default)]
    pub client_id: Uuid,
    /// PutStart 时间戳（运行时，不序列化）/ PutStart timestamp (runtime, not serialized).
    #[serde(skip)]
    pub put_start_time: Option<SystemTime>,
    /// 租约超时时间（运行时）/ Lease timeout (runtime).
    #[serde(skip)]
    pub lease_timeout: Option<SystemTime>,
    /// 软锁定超时时间（运行时）/ Soft-pin timeout (runtime).
    #[serde(skip)]
    pub soft_pin_timeout: Option<SystemTime>,
    /// 租户标识符（规范化后，未设置时默认为 "default"）。
    /// Tenant identifier (normalized; defaults to "default" when not set).
    /// C++ equivalent: ObjectMetadata::tenant_id
    #[serde(default = "default_tenant_id")]
    pub tenant_id: String,
    /// Optional group id for grouped lease/routing semantics.
    /// 分组租约/路由语义使用的可选 group id。
    #[serde(default)]
    pub group_id: String,
    /// 用户提供的原始 key（不包含租户作用域前缀）。
    /// Original user-provided key (without tenant scope prefix).
    /// C++ equivalent: ObjectMetadata::user_key
    #[serde(default)]
    pub user_key: String,
}

impl ObjectEntry {
    /// 授予对象租约，刷新 lease 和 soft-pin 超时。
    /// C++ equivalent: ObjectMetadata::GrantLease(key_ttl, soft_ttl)
    ///
    /// Grant a lease on this object. Extends `lease_timeout` to
    /// `max(lease_timeout, now + key_ttl)`. If `soft_pin_timeout` is set,
    /// also extends it to `max(soft_pin_timeout, now + soft_ttl)`.
    pub fn grant_lease(&mut self, key_ttl: Duration, soft_ttl: Duration) {
        let now = SystemTime::now();
        let next_lease = now + key_ttl;
        self.lease_timeout = Some(match self.lease_timeout {
            Some(t) if t > next_lease => t,
            _ => next_lease,
        });
        if let Some(ref mut soft) = self.soft_pin_timeout {
            let next_soft = now + soft_ttl;
            if next_soft > *soft {
                *soft = next_soft;
            }
        }
    }

    /// 检查软锁定是否有效（超时未过期）。
    /// C++ equivalent: ObjectMetadata::IsSoftPinned()
    ///
    /// Returns true if `soft_pin_timeout` is set and the current time
    /// has not yet exceeded it.
    pub fn is_soft_pinned(&self) -> bool {
        self.soft_pin_timeout
            .map_or(false, |t| SystemTime::now() < t)
    }

    /// 检查软锁定是否有效（相对于给定时间点）。
    /// C++ equivalent: ObjectMetadata::IsSoftPinned(now)
    pub fn is_soft_pinned_at(&self, now: SystemTime) -> bool {
        self.soft_pin_timeout.map_or(false, |t| now < t)
    }
}

/// Default tenant identifier — matches C++ NormalizeTenantId in types.h.
/// 默认租户标识符 —— 对应 C++ types.h 中的 NormalizeTenantId。
fn default_tenant_id() -> String {
    "default".to_string()
}

/// Segment 条目：Memory segment 的注册信息。
/// Segment entry: registration info for a Memory segment.
#[derive(Debug, Clone)]
pub struct SegmentEntry {
    /// Segment 的静态属性（ID、名称、基地址、大小、端点）
    pub segment: mooncake_store_core::Segment,
    /// 已使用的字节数 / Bytes currently used.
    pub used: u64,
    /// 所属客户端 ID / Owning client ID.
    pub client_id: Uuid,
    /// Segment 状态 / Segment status: Active, Draining, etc.
    pub status: crate::proto::SegmentStatus,
}

/// NoF (NVMe-oF) Segment 条目：远程 SSD segment 的注册信息。
/// NoF segment entry: registration info for a remote NVMe-oF SSD segment.
#[derive(Debug, Clone)]
pub struct NoFSegmentEntry {
    /// NoF segment 属性（含传输端点）/ NoF segment properties (includes transport endpoint).
    pub segment: NoFSegment,
    /// 已使用字节数 / Bytes used.
    pub used: u64,
    /// Segment 状态 / Segment status.
    pub status: crate::proto::SegmentStatus,
}

/// NoF segment 心跳追踪状态 / NoF segment heartbeat tracking state.
/// C++ equivalent: `NoFHeartbeatState` in master_service.h.
#[derive(Debug, Clone)]
pub(crate) struct NoFHeartbeatState {
    pub(crate) segment_id: Uuid,
    pub(crate) segment_name: String,
    pub(crate) te_endpoint: String,
    /// 下次探测时间 / Next probe time.
    pub(crate) next_probe_at: Instant,
    /// 最后一次成功探测的时间 / Last successful probe time.
    pub(crate) last_success_at: Instant,
    /// 连续失败次数 / Consecutive probe failures.
    pub(crate) consecutive_failures: u32,
}

/// 本地磁盘 segment 条目：管理每个客户端的 SSD 存储和 offload/promotion 队列。
/// Local disk segment entry: manages per-client SSD storage and offload/promotion queues.
pub(crate) struct LocalDiskSegmentEntry {
    /// 是否启用 offload（下沉）/ Whether offloading is enabled.
    pub(crate) enable_offloading: bool,
    /// 待 offload 的对象队列 / Queue of objects pending offload: key → size.
    pub(crate) offloading_objects: HashMap<String, i64>,
    /// 待 promotion 的对象队列 / Queue of objects pending promotion: key → size.
    pub(crate) promotion_objects: HashMap<String, i64>,
    /// 本地 SSD 总容量（字节）/ Total local SSD capacity in bytes.
    pub(crate) ssd_total_capacity_bytes: i64,
}

/// 任务条目：Copy/Move 任务的完整信息。
/// Task entry: complete info for a Copy/Move task.
#[derive(Debug, Clone)]
pub struct TaskEntry {
    /// 任务基本信息 / Task metadata: ID, type, status, assigned_client, timestamps.
    pub info: TaskInfo,
    /// 关联的 key / Associated object key.
    pub key: String,
    /// 序列化后的任务 payload（JSON）/ Serialized task payload (JSON).
    pub payload: String,
    /// 最大重试次数 / Maximum retry attempts.
    pub max_retry_attempts: u32,
}

/// ReplicationTaskKind: 区分 Copy（保留源副本）和 Move（完成后删除源副本）两种复制语义。
/// Distinguishes between Copy (keep source replica) and Move (delete source replica after completion).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicationTaskKind {
    Copy,
    Move,
}

/// ReplicationTaskEntry: 记录进行中的复制任务，包含源和目标副本描述符，用于 CopyEnd/MoveEnd 验证。
/// Records an in-flight replication task with source and target descriptors, used for CopyEnd/MoveEnd validation.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationTaskEntry {
    /// 发起任务的客户端 / Client that initiated the replication.
    pub(crate) client_id: Uuid,
    /// 任务开始时间 / Task start time (for TTL expiry).
    pub(crate) start_time: Instant,
    /// 复制类型 / Replication kind: Copy or Move.
    pub(crate) kind: ReplicationTaskKind,
    /// 源副本描述符 / Source replica descriptor.
    pub(crate) source: ReplicaDescriptor,
    /// 目标副本描述符列表 / Target replica descriptor list.
    pub(crate) targets: Vec<ReplicaDescriptor>,
}

/// Offloading 任务条目：追踪一次内存→本地磁盘下沉任务。
/// Offloading task entry: tracks a single memory→local-disk offload operation.
#[derive(Debug, Clone)]
pub(crate) struct OffloadingTaskEntry {
    /// 下沉目标客户端 / Client where the offload is happening.
    pub(crate) client_id: Uuid,
    /// 被 offload 固定的源 Memory 副本 / Source memory replica pinned during offload.
    pub(crate) source: ReplicaDescriptor,
    /// 任务开始时间 / Task start time (for TTL expiry).
    pub(crate) start_time: Instant,
}

/// Promotion 任务条目：追踪一次本地磁盘→内存提升任务。
/// Promotion task entry: tracks a single local-disk→memory promotion operation.
#[derive(Debug, Clone)]
pub(crate) struct PromotionTaskEntry {
    /// 持有磁盘副本的客户端 / Client holding the disk replica.
    pub(crate) holder_id: Uuid,
    /// 对象大小 / Object size in bytes.
    pub(crate) object_size: u64,
    /// 被 promotion 固定的源 LocalDisk 副本 / Source LocalDisk replica pinned during promotion.
    pub(crate) source: ReplicaDescriptor,
    /// 暂存的 segment ID（分配后）/ Staged segment ID (after allocation).
    pub(crate) staged_segment_id: Option<Uuid>,
    /// 暂存的偏移量（分配后）/ Staged offset (after allocation).
    pub(crate) staged_offset: Option<u64>,
    /// 任务开始时间 / Task start time.
    pub(crate) start_time: Instant,
}

/// MasterRuntimeConfig: 运行时配置参数，控制 lease TTL、eviction 水位线、promotion 策略等。
/// 所有 Duration 字段使用 std::time::Duration 表示。
///
/// Runtime configuration: controls lease TTL, eviction watermarks, promotion strategy, etc.
/// All Duration fields are represented as std::time::Duration.
#[derive(Debug, Clone)]
pub struct MasterRuntimeConfig {
    /// 未完成的 PutStart 超过此时间后被新的同 key PutStart 丢弃。
    /// Unfinished PutStart is discarded if it exceeds this timeout when a new PutStart for the same key arrives.
    pub put_start_discard_timeout: Duration,
    /// 丢弃或 release 的副本延迟释放时间，防止仍在传输中的 RDMA 访问已回收内存。
    /// Delayed release time for discarded/released replicas — prevents RDMA in-flight from accessing reclaimed memory.
    pub put_start_release_timeout: Duration,
    /// 段选择策略：Random（随机）或 FreeRatioFirst（空闲率优先）。
    /// Segment selection strategy: Random or FreeRatioFirst.
    pub allocation_strategy: AllocationStrategy,
    /// 段内内存分配器：Offset（简单连续分配）或 CachelibLike（slab + class 分配）。
    /// Memory allocator within segment: Offset (simple sequential) or CachelibLike (slab + class).
    pub memory_allocator_kind: MemoryAllocatorKind,
    /// 是否开启 promotion-on-hit：读磁盘副本时自动将热点对象提升到内存。
    /// Whether promotion-on-hit is enabled: auto-promote hot objects from disk to memory on read.
    pub promotion_on_hit: bool,
    /// 提升准入阈值：对象被访问达到此次数后才放入提升队列。
    /// Promotion admission threshold: object must be accessed this many times before entering the promotion queue.
    pub promotion_admission_threshold: u8,
    /// 提升队列最大长度，超过后新的提升请求被丢弃。
    /// Maximum promotion queue length; new promotion requests are dropped when exceeded.
    pub promotion_queue_limit: usize,
    /// 单次 PromotionObjectHeartbeat 最多返回给一个客户端的任务数。
    /// Maximum promotion tasks returned to one client per heartbeat.
    pub promotion_max_per_heartbeat: usize,
    /// 后台 reaper 轮询间隔，用于清理过期的 offload / promotion / PutStart 任务。
    /// Background reaper poll interval for cleaning up expired offload/promotion/PutStart tasks.
    pub reaper_interval: Duration,
    /// 自动淘汰检查的轮询间隔。
    /// Automatic eviction check poll interval.
    pub eviction_interval: Duration,
    /// 触发淘汰的内存使用率水位（0.0 ~ 1.0），超过后启动淘汰。
    /// Memory usage ratio watermark (0.0 ~ 1.0); eviction triggers when exceeded.
    pub eviction_high_watermark_ratio: f64,
    /// 每次淘汰尝试释放的内存比例（0.0 ~ 1.0）。
    /// Fraction of memory to free per eviction cycle (0.0 ~ 1.0).
    pub eviction_ratio: f64,
    /// 软锁定（soft pin）对象的租约时长，过期后软锁定失效但仍优先保留。
    /// Soft-pin TTL: after expiry the soft pin is released but the object is still preferred.
    pub soft_pin_ttl: Duration,
    /// KV 对象默认租约时长：PutEnd / GetReplicaList 授时，淘汰时超过此 TTL 的对象允许驱逐。
    /// Default KV lease TTL: granted at PutEnd/GetReplicaList; objects exceeding this are evictable.
    pub lease_ttl: Duration,
    /// 全局 offload 开关；关闭时拒绝注册本地磁盘 offload segment。
    /// Global offload gate; local disk offload segments cannot register when disabled.
    pub enable_offload: bool,
    /// 全局 NoF 开关；关闭时拒绝 NoF segment 和 NoF replica 操作。
    /// Global NoF gate; NoF segment and NoF replica operations are unavailable when disabled.
    pub enable_nof: bool,
    /// 淘汰时是否触发 offload（将内存副本写入本地磁盘）。
    /// Whether to trigger offload (write memory replicas to local disk) on eviction.
    pub offload_on_evict: bool,
    /// 淘汰时是否强制驱逐（即使有 soft_pin 也驱逐）。
    /// Whether to force eviction even for soft-pinned objects.
    pub offload_force_evict: bool,
    /// 客户端心跳 TTL，超过此时间未 ping 的客户端视为下线。
    /// Client heartbeat TTL: clients not pinging within this period are considered offline.
    pub client_live_ttl: Duration,
    /// 客户端监控（client monitor）轮询间隔，检查下线客户端并释放其资源。
    /// Client monitor poll interval: checks for offline clients and releases their resources.
    pub client_monitor_interval: Duration,
    /// HA 快照存储目录路径。
    /// HA snapshot storage directory path.
    pub storage_fs_dir: String,
    /// Cluster ID appended to storage_fs_dir for client-visible fsdir.
    /// 客户端可见 fsdir 使用的 cluster ID。
    pub cluster_id: String,
    /// 透传给客户端的磁盘淘汰开关，master 端淘汰逻辑暂未消费此字段。
    /// Disk eviction flag forwarded to clients; Master eviction logic does not currently consume this.
    pub enable_disk_eviction: bool,
    /// 透传给客户端的存储配额（字节），master 端暂未实现配额限流。
    /// Storage quota in bytes forwarded to clients; Master does not currently enforce quota.
    pub quota_bytes: u64,
    /// Enable remote source (S3) fallback for cache misses.
    /// 启用远端源（S3）回源：缓存未命中时从远端拉取数据。
    pub remote_source_enabled: bool,
    /// TTL for a pending remote pull entry before it is considered stale.
    /// 远端拉取条目的 TTL：超过后视为过期，允许其他节点重新拉取。
    pub remote_pull_ttl: Duration,
    /// NoF 心跳探测间隔 / NoF heartbeat probe interval.
    pub nof_heartbeat_interval: Duration,
    /// NoF 心跳探测超时 / NoF heartbeat probe timeout.
    pub nof_heartbeat_probe_timeout: Duration,
    /// NoF 心跳连续失败阈值，超过后卸载 segment / NoF heartbeat consecutive failure threshold; unmounts segment when exceeded.
    pub nof_heartbeat_failures_threshold: u32,
    /// Timeout for a single asynchronous snapshot save task.
    pub snapshot_child_timeout: Duration,
    /// Number of historical snapshots retained after successful saves.
    pub snapshot_retention_count: usize,
    /// Maximum retained finished client tasks.
    pub max_total_finished_tasks: usize,
    /// Maximum pending client tasks.
    pub max_total_pending_tasks: usize,
    /// Maximum concurrently processing client tasks.
    pub max_total_processing_tasks: usize,
    /// Pending task timeout; zero disables expiration.
    pub pending_task_timeout: Duration,
    /// Processing task timeout; zero disables expiration.
    pub processing_task_timeout: Duration,
    /// Retry limit copied into newly submitted tasks.
    pub max_task_retry_attempts: u32,
}

/// 默认运行时配置：生产环境建议通过 CLI 参数覆盖这些值。
/// Default runtime config; override via CLI args for production.
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
            promotion_max_per_heartbeat: 1,
            reaper_interval: Duration::from_millis(100),
            eviction_interval: Duration::from_millis(100),
            eviction_high_watermark_ratio: 0.95,
            eviction_ratio: 0.05,
            soft_pin_ttl: Duration::from_secs(1800),
            lease_ttl: Duration::from_secs(3600),
            enable_offload: false,
            enable_nof: true,
            offload_on_evict: false,
            offload_force_evict: false,
            client_live_ttl: Duration::from_secs(10),
            client_monitor_interval: Duration::from_secs(1),
            storage_fs_dir: String::new(),
            cluster_id: "mooncake".to_string(),
            enable_disk_eviction: true,
            quota_bytes: 0,
            remote_source_enabled: false,
            remote_pull_ttl: Duration::from_secs(60),
            nof_heartbeat_interval: Duration::from_secs(10),
            nof_heartbeat_probe_timeout: Duration::from_secs(1),
            nof_heartbeat_failures_threshold: 3,
            snapshot_child_timeout: Duration::from_secs(300),
            snapshot_retention_count: 2,
            max_total_finished_tasks: 10_000,
            max_total_pending_tasks: 10_000,
            max_total_processing_tasks: 10_000,
            pending_task_timeout: Duration::from_secs(300),
            processing_task_timeout: Duration::from_secs(300),
            max_task_retry_attempts: 10,
        }
    }
}

/// Tracks an in-flight drain unit task for a single key during segment draining.
/// 记录 segment drain 过程中单个 key 的迁移单元任务。
#[derive(Debug, Clone)]
pub(crate) struct ActiveDrainTask {
    /// 源 segment 名称 / Source segment name.
    pub(crate) source_segment: String,
    /// 目标 segment 名称 / Target segment name.
    pub(crate) target_segment: String,
    /// 迁移的字节数 / Bytes to migrate.
    pub(crate) bytes: u64,
    /// unit_key = "{key}@{source_segment}", used for dedup and retry tracking.
    /// C++ equivalent: ActiveDrainTask::unit_key
    pub(crate) unit_key: String,
}

impl ActiveDrainTask {
    /// Build the unit_key used for deduplication and retry tracking.
    /// 构建用于去重和重试跟踪的 unit_key。
    /// C++ equivalent: ActiveDrainTask::unit_key = "{key}@{source_segment}"
    pub(crate) fn unit_key_for(key: &str, source_segment: &str) -> String {
        format!("{key}@{source_segment}")
    }
}

/// A drain job that moves objects from draining segments to target segments.
/// Drain 任务：将对象从 draining segment 迁移到 target segment。
#[derive(Debug, Clone)]
pub(crate) struct DrainJobEntry {
    /// 任务 ID / Job ID.
    pub(crate) id: Uuid,
    /// 任务状态 / Job status: Created, Planning, Running, Succeeded, Failed, Canceled.
    pub(crate) status: crate::proto::JobStatus,
    /// 待 drain 的源 segment 名称列表 / Source segment names to drain.
    pub(crate) segments: Vec<String>,
    /// 目标 segment 名称列表 / Target segment names.
    pub(crate) target_segments: Vec<String>,
    /// 最大并发 drain 单元数 / Max concurrent drain units.
    pub(crate) max_concurrency: u32,
    /// 任务创建时间 / Job creation time.
    pub(crate) created_at: SystemTime,
    /// 最后更新时间 / Last update time.
    pub(crate) last_updated_at: SystemTime,
    /// 状态消息 / Status message.
    pub(crate) message: String,
    /// 已成功迁移的单元数 / Succeeded unit count.
    pub(crate) succeeded_units: u64,
    /// 已失败的单元数 / Failed unit count.
    pub(crate) failed_units: u64,
    /// 被阻塞的单元数 / Blocked unit count.
    pub(crate) blocked_units: u64,
    /// 已迁移的总字节数 / Total migrated bytes.
    pub(crate) migrated_bytes: u64,
    /// 活跃的任务映射 / Active task map: unit_id → ActiveDrainTask.
    pub(crate) active_tasks: HashMap<Uuid, ActiveDrainTask>,
    /// 已完成的单元 key 集合 / Completed unit key set.
    pub(crate) completed_unit_keys: HashSet<String>,
    /// 终极失败的单元 key 集合（不可重试）/ Terminal failed unit key set (non-retryable).
    pub(crate) terminal_failed_unit_keys: HashSet<String>,
    /// 每个 unit_key 的重试计数，超过 kMaxDrainUnitRetries(3) 后标记为 terminal_failed。
    /// Retry count per unit_key; after exceeding kMaxDrainUnitRetries(3), marked terminal_failed.
    /// C++ equivalent: DrainJob::retry_counts
    pub(crate) retry_counts: HashMap<String, u32>,
}

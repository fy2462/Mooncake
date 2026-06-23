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
use crate::tenant_quota::TenantQuotaTable;
use dashmap::DashMap;
use mooncake_store_core::{NoFSegment, ObjectDataType, ReplicaDescriptor, TaskInfo};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

mod state_config;
mod state_drain;
pub use state_config::MasterRuntimeConfig;
pub(crate) use state_drain::{ActiveDrainTask, DrainJobEntry};

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
    /// Per-tenant quota admission/accounting table.
    pub(crate) tenant_quotas: RwLock<TenantQuotaTable>,
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
            tenant_quotas: RwLock::new(TenantQuotaTable::new(0)),
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
    /// Whether this object has already moved from reserved quota to used quota.
    #[serde(default)]
    pub quota_committed: bool,
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

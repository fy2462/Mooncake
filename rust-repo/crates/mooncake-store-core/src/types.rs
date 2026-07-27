//! # Domain types for the Mooncake Store.
//!
//! This module defines the core data structures that represent the storage
//! topology (segments, replicas), configuration (replication policy), background
//! tasks (copy/move), and client metadata. All types are serializable via serde
//! so they can be persisted to etcd/Redis and exchanged over the wire.
//!
//! # Mooncake Store 领域类型
//!
//! 本模块定义了表示存储拓扑（segment、replica）、配置（复制策略）、
//! 后台任务（copy/move）和客户端元数据的核心数据结构。
//! 所有类型都通过 serde 可序列化，以便持久化到 etcd/Redis 并通过网络传输。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Segment — storage topology
// Segment — 存储拓扑
// ---------------------------------------------------------------------------

/// A logical partition of storage capacity on a single node.
///
/// Each segment represents a contiguous region of memory or disk that can
/// host multiple replicas. Segments are registered by storage clients when
/// they join the cluster and are tracked by the master/allocator.
///
/// 单个节点上的一块逻辑存储分区。
///
/// 每个 segment 表示一段连续的内存或磁盘区域，可以托管多个 replica。
/// Segment 由存储客户端在加入集群时注册，并由 master/allocator 跟踪管理。
/// 对应 C++ Client::GetLocalEndpoints() 返回的 segment 元数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    /// Unique identifier for this segment.
    /// 此 segment 的唯一标识符。
    pub id: Uuid,
    /// Human-readable name (typically the node hostname + index).
    /// 可读名称（通常是节点主机名 + 索引）。
    pub name: String,
    /// Physical base address of the segment buffer on the storage node.
    /// 存储节点上 segment 缓冲区的物理基地址。
    pub base: u64,
    /// Total size of this segment in bytes.
    /// segment 的总大小（字节）。
    pub size: u64,
    /// Transfer-engine endpoint string (e.g. RDMA LID + QP info).
    /// 传输引擎端点字符串（例如 RDMA LID + QP 信息）。
    pub te_endpoint: String,
    /// Transport protocol identifier (e.g. "rdma", "tcp").
    /// 传输协议标识符（例如 "rdma"、"tcp"）。
    pub protocol: String,
    /// Stable physical-host identity used for same-node placement.
    ///
    /// This is intentionally separate from `name` and `te_endpoint`: both may
    /// contain ports, aliases, or transport-only addresses. Older Rust
    /// snapshots omit the field and restore it as empty.
    #[serde(default)]
    pub host_id: String,
}

/// Stable identity for one client-owned Memory segment mount request.
///
/// The identity is shared by Client cleanup, Master publication, and HA replay
/// so an ambiguous RPC response cannot create an unaddressable duplicate.
pub fn stable_memory_segment_id(
    client_id: Uuid,
    segment_name: &str,
    base: u64,
    size: u64,
    te_endpoint: &str,
    protocol: &str,
    host_id: &str,
) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"mooncake-rust-memory-segment-v1");
    digest.update(client_id.as_bytes());
    digest.update(base.to_le_bytes());
    digest.update(size.to_le_bytes());
    for value in [segment_name, te_endpoint, protocol, host_id] {
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value.as_bytes());
    }
    let hash = digest.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    // Mark the opaque deterministic namespace key as a name-based UUID.
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Resolve the stable host identity used by the C++ Store.
///
/// Ordinary `host:port` endpoints lose their port, raw IPv6 literals remain
/// intact, and loopback/wildcard endpoints return an empty identity because
/// they cannot identify a physical node across the cluster.
pub fn resolve_host_id(local_hostname: &str) -> String {
    let trimmed = local_hostname.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let host = if let Some(rest) = trimmed.strip_prefix('[') {
        rest.find(']').map(|end| &rest[..end]).unwrap_or(trimmed)
    } else if trimmed == "::1" || trimmed == "::" || trimmed.matches(':').count() > 1 {
        trimmed
    } else {
        trimmed.split(':').next().unwrap_or(trimmed).trim()
    };

    match host.to_ascii_lowercase().as_str() {
        "localhost" | "127.0.0.1" | "0.0.0.0" | "::1" | "[::1]" | "::" | "[::]" => String::new(),
        _ => host.to_string(),
    }
}

/// A segment backed by NoF (NVMe-over-Fabric) instead of regular memory/disk.
///
/// Unlike [`Segment`], this variant is tied to a specific client that owns the
/// NVMe device. NoF segments enable direct SSD-to-SSD data movement without
/// host-memory bounce buffers.
///
/// 基于 NoF（NVMe-over-Fabric）而非普通内存/磁盘的 segment。
///
/// 与 [`Segment`] 不同，此变体绑定到拥有 NVMe 设备的特定客户端。
/// NoF segment 支持直接的 SSD-to-SSD 数据传输，无需主机内存中转缓冲区。
/// 对应 C++ 中的 NoFSegment 概念。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoFSegment {
    /// Unique segment identifier.
    /// 唯一 segment 标识符。
    pub id: Uuid,
    /// Human-readable name.
    /// 可读名称。
    pub name: String,
    /// Base address on the NVMe device.
    /// NVMe 设备上的基地址。
    pub base: u64,
    /// Segment size in bytes.
    /// segment 大小（字节）。
    pub size: u64,
    /// Transfer-engine endpoint string.
    /// 传输引擎端点字符串。
    pub te_endpoint: String,
    /// The client that owns this NVMe device.
    /// 拥有此 NVMe 设备的客户端 ID。
    pub client_id: Uuid,
}

/// Associates a NoF segment with its owning client.
///
/// Used during allocation to determine which client should serve a NoF replica.
///
/// 将 NoF segment 与其所属客户端关联。
///
/// 在分配过程中用于确定哪个客户端应该为一个 NoF replica 提供服务。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoFSegmentOwnerInfo {
    /// The NoF segment ID.
    /// NoF segment 的 ID。
    pub segment_id: Uuid,
    /// The owning client ID.
    /// 所属客户端 ID。
    pub client_id: Uuid,
}

// ---------------------------------------------------------------------------
// Replica — data placement unit
// Replica — 数据放置单元
// ---------------------------------------------------------------------------

/// Lifecycle status of a replica.
///
/// Replicas progress from `Undefined` → `Allocating` → `Written` → `Complete`
/// during normal put operations. `Failed` indicates an unrecoverable error.
///
/// Replica 的生命周期状态。
///
/// 在正常的 put 操作中，replica 按 `Undefined` → `Allocating` → `Written` →
/// `Complete` 顺序推进。`Failed` 表示不可恢复的错误。
/// 对应 C++ ReplicaStatus 枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaStatus {
    /// Initial state — replica slot exists but is not yet claimed.
    /// 初始状态——replica 槽位存在但尚未被占用。
    Undefined = 0,
    /// Space has been reserved but data has not been written yet.
    /// 空间已预留但数据尚未写入。
    Allocating = 1,
    /// Data transfer is in progress or partially complete.
    /// 数据传输进行中或部分完成。
    Written = 2,
    /// All data has been written and the replica is ready for reads.
    /// 所有数据已写入，replica 可供读取。
    Complete = 3,
    /// An unrecoverable error occurred; the replica must not be used.
    /// 发生不可恢复的错误；replica 不可使用。
    Failed = 4,
}

impl ReplicaStatus {
    /// Decode a replica-descriptor wire value using the historical fallback.
    pub fn from_replica_wire(value: i32) -> Self {
        Self::try_from(value).unwrap_or(Self::Undefined)
    }
}

impl TryFrom<i32> for ReplicaStatus {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Undefined),
            1 => Ok(Self::Allocating),
            2 => Ok(Self::Written),
            3 => Ok(Self::Complete),
            4 => Ok(Self::Failed),
            _ => Err(format!("unknown ReplicaStatus: {value}")),
        }
    }
}

impl From<ReplicaStatus> for i32 {
    fn from(value: ReplicaStatus) -> Self {
        value as Self
    }
}

/// Storage medium type for a replica.
///
/// Determines where data physically resides and which transfer path to use.
///
/// Replica 的存储介质类型。
///
/// 决定数据物理存放位置以及使用哪种传输路径。
/// 对应 C++ ReplicaType 枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplicaType {
    /// Regular DRAM-backed storage.
    /// 常规 DRAM 内存存储。
    Memory = 0,
    /// Block-device / file-backed disk storage.
    /// 块设备 / 文件支持的磁盘存储。
    Disk = 1,
    /// Local NVMe/SATA SSD storage on the host.
    /// 主机上的本地 NVMe/SATA SSD 存储。
    LocalDisk = 2,
    /// NVMe-over-Fabric remote SSD.
    /// NVMe-over-Fabric 远程 SSD。
    NoFSsd = 3,
    /// Wildcard — matches any replica type (used in queries).
    /// 通配符——匹配所有 replica 类型（用于查询）。
    All = 4,
}

impl ReplicaType {
    /// Decode a concrete replica descriptor using the historical fallback.
    /// `All` is a query selector, not a concrete storage location, so wire
    /// value 4 and unknown values both retain the existing Memory fallback.
    pub fn from_replica_wire(value: i32) -> Self {
        match Self::try_from(value) {
            Ok(Self::Disk) => Self::Disk,
            Ok(Self::LocalDisk) => Self::LocalDisk,
            Ok(Self::NoFSsd) => Self::NoFSsd,
            Ok(Self::Memory | Self::All) | Err(_) => Self::Memory,
        }
    }
}

/// Convert from the proto wire format (i32) to the internal enum.
/// Unknown values fall back to Memory.
/// 从 proto 线格式 (i32) 转换为内部枚举。未知值回退为 Memory。
impl TryFrom<i32> for ReplicaType {
    type Error = String;
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Memory),
            1 => Ok(Self::Disk),
            2 => Ok(Self::LocalDisk),
            3 => Ok(Self::NoFSsd),
            4 => Ok(Self::All),
            _ => Err(format!("unknown ReplicaType: {}", v)),
        }
    }
}

impl From<ReplicaType> for i32 {
    fn from(value: ReplicaType) -> Self {
        value as Self
    }
}

/// Semantic category of stored data.
///
/// Used by the allocator to make placement decisions (e.g. co-locate kvcache
/// with compute, separate weights onto read-optimized segments).
///
/// 存储数据的语义类别。
///
/// 分配器使用此信息做放置决策（例如将 kvcache 与计算放在一起，
/// 将 weights 放到读取优化的 segment 上）。
/// 对应 C++ ObjectDataType 枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ObjectDataType {
    /// Unknown or unspecified data type.
    /// 未知或未指定的数据类型。
    #[default]
    Unknown = 0,
    /// KV-cache tensors for LLM inference.
    /// 用于 LLM 推理的 KV-cache 张量。
    Kvcache = 1,
    /// Generic tensor data.
    /// 通用张量数据。
    Tensor = 2,
    /// Model weights / parameters.
    /// 模型权重/参数。
    Weight = 3,
    /// Training / evaluation samples.
    /// 训练/评估样本。
    Sample = 4,
    /// Intermediate activations.
    /// 中间激活值。
    Activation = 5,
    /// Gradients during backpropagation.
    /// 反向传播梯度。
    Gradient = 6,
    /// Optimizer state (e.g. Adam momentum/variance).
    /// 优化器状态（例如 Adam 动量/方差）。
    OptimizerState = 7,
    /// Arbitrary metadata (manifests, indices, etc.).
    /// 任意元数据（清单、索引等）。
    Metadata = 8,
    /// Catch-all for unclassified data.
    /// 未分类数据的兜底类型。
    General = 9,
}

impl TryFrom<i32> for ObjectDataType {
    type Error = String;
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Kvcache),
            2 => Ok(Self::Tensor),
            3 => Ok(Self::Weight),
            4 => Ok(Self::Sample),
            5 => Ok(Self::Activation),
            6 => Ok(Self::Gradient),
            7 => Ok(Self::OptimizerState),
            8 => Ok(Self::Metadata),
            9 => Ok(Self::General),
            _ => Err(format!("unknown ObjectDataType: {}", v)),
        }
    }
}

/// Describes a single replica — the unit of data placement.
///
/// Each replica represents a copy (or fragment) of a stored object placed on
/// a specific segment. The master/allocator tracks replicas to decide where
/// to route reads and writes.
///
/// 描述单个 replica——数据放置的基本单位。
///
/// 每个 replica 代表存储对象在特定 segment 上的一个副本（或片段）。
/// master/allocator 跟踪 replica 以决定读写的路由目标。
/// 对应 C++ Replica 类（replica.h）。
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicaDescriptor {
    /// The segment this replica lives on.
    /// 此 replica 所在的 segment ID。
    pub segment_id: Uuid,
    /// Cached segment name for faster lookups (denormalized).
    /// 缓存的 segment 名称，用于加速查找（反范式设计）。
    pub segment_name: String,
    /// Byte offset within the segment where this replica's data begins.
    /// segment 内此 replica 数据起始位置的字节偏移量。
    pub offset: u64,
    /// Size of this replica in bytes.
    /// 此 replica 的大小（字节）。
    pub size: u64,
    /// Current lifecycle status (Undefined → Allocating → Written → Complete).
    /// 当前生命周期状态（Undefined → Allocating → Written → Complete）。
    pub status: ReplicaStatus,
    /// Storage medium type.
    /// 存储介质类型。
    pub replica_type: ReplicaType,
    /// The client that currently holds (owns) this replica, if any.
    /// 当前持有（拥有）此 replica 的客户端（如果有）。
    pub holder_client_id: Option<Uuid>,
    /// Durable identity of the LocalDisk storage namespace.
    ///
    /// This is independent from `holder_client_id`, which identifies the
    /// currently active process. It is `None` for non-LocalDisk replicas and
    /// legacy snapshots that predate restart-safe LocalDisk ownership.
    #[serde(default)]
    pub local_disk_storage_id: Option<Uuid>,
    /// Identity of the exact durable LocalDisk record generation.
    ///
    /// Unlike the namespace storage ID, this changes for every admitted
    /// overwrite and prevents an older same-size record from reattaching.
    #[serde(default)]
    pub local_disk_generation_id: Option<Uuid>,
    /// Reference count — tracks in-flight operations (copy/move/promotion).
    /// Eviction and release MUST check `is_busy()` before freeing.
    /// 引用计数——跟踪正在进行的操作（copy/move/promotion）。
    /// 驱逐和释放操作必须在释放前检查 `is_busy()`。
    #[serde(skip, default = "default_refcnt")]
    pub refcnt: u32,
    /// Tracks whether the RDMA memory / NoF handle is still valid.
    /// C++ equivalent: Replica::has_invalid_mem_handle() / has_invalid_nof_handle().
    /// master_service.cpp:1368-1372 — PutEnd skips replicas whose handle became invalid.
    /// master_service.cpp:1985-1994 — CopyEnd/MoveEnd abort if source handle invalidated.
    /// 跟踪 RDMA 内存 / NoF 句柄是否仍然有效。
    /// 对应 C++ Replica::has_invalid_mem_handle() / has_invalid_nof_handle()。
    /// master_service.cpp:1368-1372——PutEnd 跳过句柄失效的 replica。
    /// master_service.cpp:1985-1994——如果源句柄失效，CopyEnd/MoveEnd 中止。
    #[serde(default = "default_handle_valid")]
    pub handle_valid: bool,
    /// Physical base address of the segment buffer on the storage node.
    /// Used to compute target_offset = base_addr + offset for TE transfers.
    /// C++ equivalent: AllocatedBuffer::Descriptor::buffer_address_
    /// 存储节点上 segment 缓冲区的物理基地址。
    /// 用于计算 TE 传输的 target_offset = base_addr + offset。
    /// 对应 C++ AllocatedBuffer::Descriptor::buffer_address_。
    #[serde(default)]
    pub base_addr: u64,
    /// Transport protocol used by a memory replica (for example `rdma` or `tcp`).
    /// Empty for legacy snapshots and replica types without a memory transport.
    #[serde(default)]
    pub protocol: String,
}

/// Default reference count for a freshly-created replica descriptor.
/// 新创建的 replica descriptor 的默认引用计数。
fn default_refcnt() -> u32 {
    0
}

/// Default handle-valid flag — handles start as valid.
/// 默认句柄有效标志——句柄初始为有效。
fn default_handle_valid() -> bool {
    true
}

impl Clone for ReplicaDescriptor {
    /// Custom clone: always resets `refcnt` to 0 on the clone, because the
    /// clone is a new logical reference and should not inherit in-flight counts.
    /// 自定义克隆：克隆时始终将 `refcnt` 重置为 0，因为克隆体是新的逻辑引用，
    /// 不应继承进行中的计数。
    fn clone(&self) -> Self {
        Self {
            segment_id: self.segment_id,
            segment_name: self.segment_name.clone(),
            offset: self.offset,
            size: self.size,
            status: self.status,
            replica_type: self.replica_type,
            holder_client_id: self.holder_client_id,
            local_disk_storage_id: self.local_disk_storage_id,
            local_disk_generation_id: self.local_disk_generation_id,
            refcnt: 0,
            handle_valid: self.handle_valid,
            base_addr: self.base_addr,
            protocol: self.protocol.clone(),
        }
    }
}

impl ReplicaDescriptor {
    /// Returns true if any in-flight operation holds a reference to this replica.
    /// Returns `true` if this replica has in-flight operations — callers MUST
    /// NOT evict or free it while busy.
    /// 返回 `true` 表示此 replica 有进行中的操作——调用方不得在 busy 状态下驱逐或释放它。
    pub fn is_busy(&self) -> bool {
        self.refcnt > 0
    }

    /// Increment the reference count (saturating at `u32::MAX`).
    /// Call before starting a copy/move that targets or sources this replica.
    /// 递增引用计数（在 `u32::MAX` 处饱和）。
    /// 在启动以此 replica 为源或目标的 copy/move 之前调用。
    pub fn inc_refcnt(&mut self) {
        self.refcnt = self.refcnt.saturating_add(1);
    }

    /// Decrement the reference count (saturating at 0).
    /// Call when a copy/move completes or fails.
    /// 递减引用计数（在 0 处饱和）。
    /// 在 copy/move 完成或失败时调用。
    pub fn dec_refcnt(&mut self) {
        self.refcnt = self.refcnt.saturating_sub(1);
    }
}

/// Deterministic generation used only while importing a pre-generation
/// LocalDisk record/snapshot. Every newly admitted offload uses a random
/// Master-issued generation instead.
pub fn legacy_local_disk_generation_id(storage_id: Uuid, scoped_key: &str) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"mooncake-localdisk-legacy-generation-v1\0");
    hasher.update(storage_id.as_bytes());
    hasher.update(scoped_key.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // Mark the deterministic value as an RFC 4122 variant, version 8 UUID.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

// ---------------------------------------------------------------------------
// ReplicateConfig — replication policy
// ReplicateConfig — 复制策略
// ---------------------------------------------------------------------------

/// Configuration that controls how many replicas to create and where to place
/// them on a per-object (PutStart) basis.
///
/// 控制每个对象（PutStart）创建多少副本以及放置位置的配置。
///
/// 对应 C++ ReplicateConfig（allocator.h）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateConfig {
    /// Number of DRAM (Memory) replicas.
    /// DRAM（内存）副本数量。
    pub replica_num: u32,
    /// Number of NoF (NVMe-over-Fabric) replicas.
    /// NoF（NVMe-over-Fabric）副本数量。
    pub nof_replica_num: u32,
    /// If true, request soft pinning (best-effort NUMA locality).
    /// 如果为 true，请求软固定（尽力而为的 NUMA 本地性）。
    pub with_soft_pin: bool,
    /// If true, request hard pinning (strict NUMA locality, fail if unavailable).
    /// 如果为 true，请求硬固定（严格 NUMA 本地性，不可用时失败）。
    pub with_hard_pin: bool,
    /// Preferred segment name for placement (single-value hint, legacy).
    /// 优先放置的 segment 名称（单值提示，遗留字段）。
    pub preferred_segment: String,
    /// Preferred segment names for placement (multi-value).
    /// 优先放置的 segment 名称列表（多值）。
    pub preferred_segments: Vec<String>,
    /// Preferred NoF segment names for placement.
    /// 优先放置的 NoF segment 名称列表。
    pub preferred_nof_segments: Vec<String>,
    /// If true, prefer to allocate all replicas on the same node as the primary.
    /// 如果为 true，优先将所有副本分配在与主副本相同的节点上。
    pub prefer_alloc_in_same_node: bool,
    /// Semantic data type — affects placement heuristics.
    /// 语义数据类型——影响放置启发式算法。
    pub data_type: ObjectDataType,
    /// Stable writer host identity for LocalFirst placement.
    ///
    /// A normal Store client overwrites this with its own resolved host id,
    /// matching C++ `Client::AttachHostId`. RPC-only callers may set it
    /// explicitly.
    #[serde(default)]
    pub host_id: String,
    /// Optional group id per key. Grouped objects share lease refresh semantics.
    /// 每个 key 可选的 group id。分组对象共享租约刷新语义。
    pub group_ids: Vec<String>,
}

/// Metadata persisted alongside each stored object in the backend (etcd/Redis/S3).
///
/// 与每个存储对象一起持久化在后端（etcd/Redis/S3）中的元数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageObjectMetadata {
    /// Storage bucket identifier.
    /// 存储桶标识符。
    pub bucket_id: i64,
    /// Byte offset within the bucket.
    /// 桶内的字节偏移量。
    pub offset: i64,
    /// Serialized key length.
    /// 序列化后的 key 长度。
    pub key_size: i64,
    /// Serialized data length.
    /// 序列化后的数据长度。
    pub data_size: i64,
    /// Transport endpoint for direct data access.
    /// 用于直接数据访问的传输端点。
    pub transport_endpoint: String,
}

impl Default for ReplicateConfig {
    /// Sensible defaults: 1 DRAM replica, 0 NoF replicas, no pinning,
    /// no placement preferences.
    /// 合理的默认值：1 个 DRAM 副本，0 个 NoF 副本，无固定，无放置偏好。
    fn default() -> Self {
        Self {
            replica_num: 1,
            nof_replica_num: 0,
            with_soft_pin: false,
            with_hard_pin: false,
            preferred_segment: String::new(),
            preferred_segments: vec![],
            preferred_nof_segments: vec![],
            prefer_alloc_in_same_node: false,
            data_type: ObjectDataType::Unknown,
            host_id: String::new(),
            group_ids: vec![],
        }
    }
}

// ---------------------------------------------------------------------------
// Task — background work (copy/move)
// Task — 后台工作（copy/move）
// ---------------------------------------------------------------------------

/// Type of background task the master orchestrates.
/// master 编排的后台任务类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskType {
    /// Copy a replica from one segment to another (e.g. for redundancy).
    /// 将 replica 从一个 segment 复制到另一个（例如用于冗余）。
    ReplicaCopy = 0,
    /// Move a replica from one segment to another (e.g. for load balancing).
    /// 将 replica 从一个 segment 移动到另一个（例如用于负载均衡）。
    ReplicaMove = 1,
}

impl From<TaskType> for i32 {
    fn from(t: TaskType) -> i32 {
        t as i32
    }
}

/// Execution state of a background task.
/// 后台任务的执行状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Waiting to be picked up by a worker client.
    /// 等待 worker 客户端获取。
    Pending = 0,
    /// Currently being executed.
    /// 正在执行中。
    Processing = 1,
    /// Completed successfully.
    /// 已成功完成。
    Success = 2,
    /// Completed with an error.
    /// 以错误结束。
    Failed = 3,
}

impl From<TaskStatus> for i32 {
    fn from(s: TaskStatus) -> i32 {
        s as i32
    }
}

impl TryFrom<i32> for TaskStatus {
    type Error = String;
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Processing),
            2 => Ok(Self::Success),
            3 => Ok(Self::Failed),
            _ => Err(format!("unknown TaskStatus: {}", v)),
        }
    }
}

/// Full record of a background task tracked by the master.
/// master 跟踪的后台任务完整记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    /// Unique task identifier.
    /// 唯一任务标识符。
    pub id: Uuid,
    /// Copy or Move.
    /// 复制或移动。
    pub task_type: TaskType,
    /// Current execution status.
    /// 当前执行状态。
    pub status: TaskStatus,
    /// When the task was created.
    /// 任务创建时间。
    pub created_at: DateTime<Utc>,
    /// When the task was last updated (status change, assignment, etc.).
    /// 任务最近更新时间（状态变更、分配等）。
    pub last_updated_at: DateTime<Utc>,
    /// The client that is currently working on this task, if any.
    /// 当前正在执行此任务的客户端（如果有）。
    pub assigned_client: Option<Uuid>,
    /// Human-readable diagnostic / error message.
    /// 人类可读的诊断/错误消息。
    pub message: String,
}

/// A task that has been dispatched to a specific worker client.
///
/// Includes the serialized payload and retry policy.
///
/// 已派发给特定 worker 客户端的任务。
///
/// 包含序列化的负载和重试策略。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignment {
    /// Unique task identifier.
    /// 唯一任务标识符。
    pub id: Uuid,
    /// Copy or Move.
    /// 复制或移动。
    pub task_type: TaskType,
    /// Serialized task payload (format depends on task_type).
    /// 序列化的任务负载（格式取决于 task_type）。
    pub payload: String,
    /// Epoch timestamp in milliseconds when the task was assigned.
    /// 任务分配时的 epoch 时间戳（毫秒）。
    pub created_at_ms_epoch: i64,
    /// Maximum number of retry attempts before the task is marked Failed.
    /// 任务被标记为 Failed 之前的最大重试次数。
    pub max_retry_attempts: u32,
}

/// Request sent by a worker client to mark a task as complete (success or failure).
/// worker 客户端发送的将任务标记为完成（成功或失败）的请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteRequest {
    /// The task being completed.
    /// 正在完成的任务 ID。
    pub id: Uuid,
    /// Final status: Success or Failed.
    /// 最终状态：Success 或 Failed。
    pub status: TaskStatus,
    /// Diagnostic message (error details on failure, optional note on success).
    /// 诊断消息（失败时为错误详情，成功时可选）。
    pub message: String,
}

// ---------------------------------------------------------------------------
// Client info — node registration
// Client info — 节点注册
// ---------------------------------------------------------------------------

/// Metadata the master keeps about each registered storage client.
///
/// Clients register when they join the cluster and periodically heartbeat.
///
/// master 维护的每个已注册存储客户端的元数据。
///
/// 客户端在加入集群时注册，并定期发送心跳。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    /// Unique client identifier (assigned by master on registration).
    /// 唯一客户端标识符（注册时由 master 分配）。
    pub id: Uuid,
    /// Network addresses this client can be reached at (host:port list).
    /// 可访问此客户端的网络地址列表（host:port 列表）。
    pub addresses: Vec<String>,
    /// Segments this client contributes to the storage pool.
    /// 此客户端贡献给存储池的 segment 列表。
    pub segments: Vec<Segment>,
    /// Timestamp of the last heartbeat — used for dead-client detection.
    /// 最后心跳时间戳——用于检测失联客户端。
    pub last_seen: DateTime<Utc>,
}

pub(crate) mod accessors;
pub(crate) mod background;
pub(crate) mod batch_eviction;
pub(crate) mod batch_types;
pub(crate) mod batches;
pub(crate) mod buffer;
mod config;
mod endpoint;
mod engine;
pub(crate) mod finalize;
pub(crate) mod global_disk;
pub(crate) mod ha;
mod http;
pub(crate) mod lifecycle;
mod lifecycle_state;
mod metrics;
pub(crate) mod nof_register;
pub(crate) mod offload_read;
pub(crate) mod read;
pub(crate) mod read_batch;
pub(crate) mod read_meta;
pub(crate) mod read_ranges;
pub(crate) mod reaper;
pub(crate) mod remove;
mod replica_selection;
pub(crate) mod replication;
pub(crate) mod staging;
pub(crate) mod storage;
pub(crate) mod storage_local;
pub(crate) mod storage_offload;
pub(crate) mod storage_promotion;
pub(crate) mod tasks;
#[cfg(test)]
mod tests;
pub(crate) mod transfer;
pub(crate) mod transfer_local;
pub(crate) mod transfer_meta;
pub(crate) mod transfer_read;
pub(crate) mod transfer_write;
pub(crate) mod types;
pub(crate) mod upsert;
pub(crate) mod write;
pub(crate) mod write_batch;
pub(crate) mod write_parts;

pub use background::{ClientBackgroundConfig, ClientBackgroundHandle};
pub use batch_types::{BatchPutStartResult, BatchUpsertEntry};
pub use engine::InitializedTransferEngine;
pub use http::ClientHttpConfig;
pub use replica_selection::{ReplicaScorer, ReplicaSelectionPolicy, builtin_remote_replica_score};
pub use storage::{OffloadTaskItem, PromotionTaskItem, SegmentDetail};
pub use transfer_local::BufferRegistrationId;
pub use types::{BufferHandle, CachedQueryResultResponse, ClientHealthStatus};

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::data_plane_ffi::AcceleratorBackend;
use crate::hot_cache::HotCacheAdmission;
use crate::proto;
use crate::{LocalHotCache, MissHandler, RemoteSource};

use self::http::ClientHttpServerState;
use self::lifecycle_state::{
    HealthState, LocalDiskMountState, OffloadServerState, RemountState, ShutdownState,
};

// ---------------------------------------------------------------------------
// MooncakeClient — primary client for the Mooncake distributed store
// MooncakeClient —— Mooncake 分布式存储的主客户端
//
// C++ equivalent: `class Client` in real_client.h / real_client.cpp
// C++ 等价类：`real_client.h / real_client.cpp` 中的 `class Client`
// ---------------------------------------------------------------------------

pub(crate) struct OwnedStoreSegment {
    pub(crate) segment_id: Uuid,
    pub(crate) segment_name: String,
    pub(crate) size: u64,
    pub(crate) buffer: crate::memory_ffi::OwnedSegmentBuffer,
}

impl OwnedStoreSegment {
    pub(crate) fn base_addr(&self) -> u64 {
        self.buffer.as_ptr() as u64
    }
}

#[derive(Clone)]
pub(crate) struct MountedExternalSegment {
    pub(crate) segment_id: Uuid,
    pub(crate) segment_name: String,
    pub(crate) size: u64,
    pub(crate) base_addr: u64,
    pub(crate) te_endpoint: String,
    pub(crate) protocol: String,
    pub(crate) host_id: String,
}

/// The main client for interacting with the Mooncake distributed K/V store.
///
/// Each `MooncakeClient` holds:
/// - A gRPC connection to the **master service** (metadata / allocation).
/// - A [`TransferEngine`] instance for RDMA or TCP data-plane transfers.
/// - Optional **hot cache** for local fast-path lookups.
/// - Optional **remote source** miss handler for cache-miss fallback (e.g. S3).
///
/// # Lifecycle
///
/// 1. [`create`](Self::create) — bootstraps the engine, registers memory, mounts segments.
/// 2. Data operations (`get`, `put`, `upsert`, `remove`, ...).
/// 3. [`tear_down_all`](Self::tear_down_all) — unregisters buffers and shuts down.
///
/// Mooncake 分布式 K/V 存储的主客户端。
/// 每个 MooncakeClient 持有：到 master 服务（元数据/分配）的 gRPC 连接、
/// TransferEngine（RDMA 或 TCP 数据面传输）、可选的 hot cache（本地快速路径）、
/// 可选的 remote source（缓存未命中回退，如 S3）。
/// 生命周期：create() 引导 → 数据操作 → tear_down_all() 清理。
pub struct MooncakeClient {
    /// gRPC client stub connected to the Mooncake master service.
    /// Used for all metadata operations: replica lookup, put_start/put_end,
    /// segment mount, task management, etc.
    ///
    /// 连接到 Mooncake master 服务的 gRPC 客户端存根。
    /// 用于所有元数据操作：副本查找、put_start/put_end、segment 挂载、任务管理等。
    pub(crate) master: proto::master_service_client::MasterServiceClient<metrics::MetricsChannel>,

    /// The TransferEngine instance that performs RDMA / TCP data-plane transfers.
    /// Wrapped in `Arc` so it can be shared across async tasks.
    ///
    /// 执行 RDMA / TCP 数据面传输的 TransferEngine 实例。
    /// 使用 Arc 包装，可在多个异步任务间共享。C++ 等价：`std::shared_ptr<TransferEngine>`。
    pub(crate) engine: engine::ClientTransferEngine,
    /// Keeps an automatically selected client port reserved for this
    /// process, matching C++ RealClient's AutoPortBinder lifetime.
    pub(crate) _auto_port_reservation: Option<endpoint::PortReservation>,

    /// Safe strategy interface; its native unsafe implementation lives only
    /// in `data_plane_ffi`, outside the client business layer.
    pub(crate) accelerator: Arc<dyn AcceleratorBackend>,

    /// Unique identifier for this client instance, assigned at creation time.
    /// Sent to the master in every request so the master can track client state.
    ///
    /// 此客户端实例的唯一标识符，在创建时分配。
    /// 每次请求都会发送给 master，以便 master 追踪客户端状态。
    pub(crate) client_id: Uuid,

    /// The hostname (and optionally port) of this node, e.g. "node01:12345".
    /// Used as the **transport endpoint** name for segment resolution.
    ///
    /// 本节点的主机名（可含端口），如 "node01:12345"。
    /// 用作 segment 解析的传输端点名称。
    pub(crate) local_hostname: String,
    /// Stable endpoint identity published for client-owned memory segments.
    /// P2P uses the native ip:port; metadata-backed discovery uses the
    /// logical client endpoint under which Transfer Engine metadata is stored.
    pub(crate) local_transport_endpoint: String,
    /// Stable physical-host identity attached to placement requests.
    pub(crate) host_id: String,

    /// Transport protocol used for data-plane transfers ("tcp", "rdma", etc.).
    /// Stored so MountSegment / ReMountSegment can include it in gRPC requests.
    /// 数据面传输使用的协议（"tcp"、"rdma" 等）。存储以供 MountSegment/ReMountSegment 在 gRPC 请求中包含。
    pub(crate) protocol: String,

    /// Master-advertised capacity alignment for owned Memory segments.
    pub(crate) memory_segment_alignment: usize,

    /// Whether tenant IDs participate in object identity on the connected
    /// Master. When disabled, every request is normalized to `default`.
    pub(crate) enable_tenant_scope: bool,

    /// Scratch buffer for staging data before/after TransferEngine operations.
    /// In `write_to_replica`: data is first memcpy'd here, then TE transfers it.
    /// In `read_from_replica`: TE reads into this buffer, then we copy out.
    /// Size is controlled by `local_buffer_size` in [`create`](Self::create).
    ///
    /// 用于在 TransferEngine 操作前后暂存数据的临时缓冲区。
    /// write_to_replica: 先 memcpy 数据至此，再通过 TE 传输。
    /// read_from_replica: TE 读取到此缓冲区，再拷贝出去。
    /// 大小由 create() 中的 local_buffer_size 控制。
    pub(crate) local_buffer: staging::StagingBuffer,

    /// Client-owned Store segments. A total capacity larger than
    /// `MC_MAX_MR_SIZE` is represented by multiple entries with the same
    /// transport endpoint/name but distinct UUIDs and base addresses.
    ///
    /// Every entry owns its allocation until the exact Master segment has
    /// been unmounted and Transfer Engine registration has been removed.
    pub(crate) owned_store_segments: Vec<OwnedStoreSegment>,

    /// Native CXL mapping registration. Unlike `segment_buffer`, the backing
    /// mapping is owned by the installed Transfer Engine transport.
    pub(crate) cxl_segment_registration: Option<crate::memory_ffi::CxlSegmentRegistration>,
    pub(crate) cxl_segment_id: Option<Uuid>,

    /// Exact generations of externally registered user buffers.
    /// Populated via
    /// [`register_owned_buffer`](MooncakeClient::register_owned_buffer) for
    /// zero-copy read/write paths. The TE must be told about these buffers so
    /// it can DMA directly into/from them.
    ///
    /// 每个 in-flight region 持有 generation lease；注销会拒绝 busy generation。
    /// 通过 register_owned_buffer() 填充，用于零拷贝读写路径。
    /// TE 必须被告知这些缓冲区，以便直接进行 DMA 操作。
    /// C++ 等价：`registered_buffers_` map。
    pub(crate) registered_buffers: RwLock<HashMap<usize, transfer_engine_ffi::RegisteredMemory>>,

    /// Shutdown flag. When set to `true`, operations should stop and the client
    /// is considered closed. Checked via [`is_closed`](Self::is_closed).
    ///
    /// 关闭标志。设置为 true 后，操作应停止，客户端被视为已关闭。
    /// 通过 is_closed() 检查。
    shutdown_state: ShutdownState,

    /// Set of locally-mounted segment transport endpoints, used for
    /// [`select_best_replica`](Self::select_best_replica) locality checks.
    /// When a segment endpoint is in this set, its replicas are treated as
    /// "local" and can use the fast `local_memcpy` path.
    ///
    /// C++ equivalent: `Client::GetLocalEndpoints()` → `segment.te_endpoint`.
    ///
    /// 本地已挂载 segment 的传输端点集合，用于 select_best_replica 本地性检查。
    /// 当 segment 端点在此集合中时，其副本被视为"本地"，可使用 local_memcpy 快速路径。
    /// C++ 等价：Client::GetLocalEndpoints() → segment.te_endpoint。
    pub(crate) local_endpoints: RwLock<HashSet<String>>,

    /// Master-assigned segment IDs and their logical names. Names are not
    /// unique: C++ mounts every `max_mr_size` chunk under the same local
    /// hostname, so UUID is the authoritative identity.
    pub(crate) mounted_segment_ids: RwLock<HashMap<Uuid, String>>,
    pub(crate) mounted_external_segments: RwLock<HashMap<Uuid, MountedExternalSegment>>,
    pub(crate) mounted_owned_external_registrations:
        RwLock<HashMap<Uuid, transfer_engine_ffi::RegisteredMemory>>,

    /// Exact NoF descriptors owned by this client process. A Master failover
    /// invalidates snapshot-era device addresses, so heartbeat remount must
    /// resubmit the current base and transfer endpoint.
    pub(crate) mounted_nof_segments: RwLock<HashMap<Uuid, mooncake_store_core::NoFSegment>>,

    /// Optional remote source miss handler for cache-miss fallback.
    /// When a key is not found in the distributed store, and this handler is
    /// enabled, the client will attempt to fetch the data from this source
    /// (e.g. S3, local filesystem).
    ///
    /// 可选的远程数据源未命中处理器（缓存未命中回退）。
    /// 当 key 在分布式存储中未找到且此处理器启用时，客户端将尝试从此数据源获取数据
    /// （如 S3、本地文件系统）。
    pub(crate) miss_handler: Option<MissHandler<Arc<dyn RemoteSource>>>,

    /// Local hot cache for fast-path lookups (avoids gRPC + RDMA round-trip).
    /// This is an in-memory L1 cache that sits before the distributed store.
    /// When attached, `get()` checks this cache first before making any network
    /// calls. Successful fetches (from replicas or remote source) populate it.
    ///
    /// 本地热缓存，用于快速路径查找（避免 gRPC + RDMA 往返）。
    /// 这是位于分布式存储之前的 L1 内存缓存。
    /// 挂载后，get() 在任何网络调用之前首先检查此缓存。
    /// 成功的获取（来自副本或远程数据源）会填充此缓存。
    pub(crate) hot_cache: Option<Arc<LocalHotCache>>,
    pub(crate) hot_cache_admission: Option<HotCacheAdmission>,

    /// Local storage backend for persisting offloaded data to local disk.
    /// When set, the full offload cycle (heartbeat → read memory → write disk
    /// → notify master) and promotion cycle (heartbeat → read disk → alloc
    /// memory replica → write replica → notify master) is enabled.
    ///
    /// 本地存储后端，用于将 offload 数据持久化到本地磁盘。
    /// 设置后，完整的 offload 循环和 promotion 循环将启用。
    pub(crate) local_storage: Option<crate::local_storage_backend::AttachedLocalStorage>,

    /// Shared-filesystem data plane for Master-issued global `Disk` replicas.
    pub(crate) global_disk: Option<Arc<global_disk::GlobalDiskStorage>>,

    /// Guard to ensure at most one remount is in progress at any time.
    /// 确保同一时间最多只有一个 remount 在进行中。C++ equivalent: remount_segment_future.valid()
    remount_state: RemountState,
    /// Desired LocalDisk mount mode for HA remount after leader failover.
    local_disk_mount_state: LocalDiskMountState,

    /// Whether the last ping to the master succeeded.
    /// 最后一次 ping master 是否成功。C++ equivalent: Client::is_ping_healthy()
    health_state: HealthState,

    /// Offload RPC server task and its published endpoint.
    offload_server_state: OffloadServerState,

    /// Optional health and Prometheus HTTP endpoint task owned by this client.
    client_http_server_state: ClientHttpServerState,

    /// Persistent per-client metrics registry. Disabled only when
    /// `MC_STORE_CLIENT_METRIC` explicitly selects a false value.
    pub(crate) metrics: Option<Arc<metrics::ClientMetrics>>,
    metrics_reporter_state: metrics::MetricsReporterState,

    /// Currently connected master address (`host:port`).
    /// C++ equivalent: `Client::current_master_view_.leader_address`.
    pub(crate) master_addr: RwLock<String>,

    /// Candidate master addresses used for client-side failover.
    /// This intentionally stores plain addresses rather than depending on the
    /// master crate's HA coordinator types, keeping the client crate standalone.
    pub(crate) master_candidates: RwLock<Vec<String>>,

    /// Per-request timeout applied to master RPCs. `None` disables client-side
    /// deadlines and matches C++ when MC_RPC_TIMEOUT_MS is negative.
    pub(crate) rpc_request_timeout: Option<Duration>,

    /// Default tenant used by convenience APIs that do not take an explicit
    /// tenant parameter. Empty string preserves the legacy/default namespace.
    pub(crate) tenant_id: String,

    /// Generation-bearing tasks retained only for the legacy key-only offload
    /// API. The task-native API and built-in offload loop do not use this map.
    pub(crate) pending_legacy_offload_tasks: HashMap<String, storage::OffloadTaskItem>,

    replica_selection_policy: ReplicaSelectionPolicy,
}

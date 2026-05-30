pub(crate) mod read;
pub(crate) mod remove;
pub(crate) mod storage;
pub(crate) mod tasks;
pub(crate) mod transfer;
pub(crate) mod upsert;
pub(crate) mod write;

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::sync::Arc;
use tonic::transport::Channel;
use transfer_engine_ffi::TransferEngine;
use uuid::Uuid;

use crate::proto;
use crate::{LocalHotCache, MissHandler, RemoteSource, RemoteSourceConfig};

// ---------------------------------------------------------------------------
// BufferHandle — owned get result (key + data + size triple)
// 拥有所有权的读取结果句柄，包含 key、data 和 size 三元组
// ---------------------------------------------------------------------------

/// Owned buffer returned by [`get_buffer`](MooncakeClient::get_buffer).
///
/// Unlike the low-level `get()` which returns a raw `Vec<u8>`, this struct
/// bundles the key and byte-length together so the caller does not have to
/// track them separately.
///
/// 与返回裸 Vec<u8> 的低级 get() 不同，BufferHandle 将 key 和字节长度绑定在一起，
/// 调用者无需单独追踪这些信息。对应 C++ 的 `BufferHandle` 结构体。
pub struct BufferHandle {
    /// The raw payload bytes. / 原始负载字节。
    pub data: Vec<u8>,
    /// The key this data was fetched for. / 此数据对应的 key。
    pub key: String,
    /// Byte length of the payload (== data.len()). / 负载的字节长度。
    pub size: usize,
}

// ---------------------------------------------------------------------------
// MooncakeClient — primary client for the Mooncake distributed store
// MooncakeClient —— Mooncake 分布式存储的主客户端
//
// C++ equivalent: `class Client` in real_client.h / real_client.cpp
// C++ 等价类：`real_client.h / real_client.cpp` 中的 `class Client`
// ---------------------------------------------------------------------------

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
    pub(crate) master: proto::master_service_client::MasterServiceClient<Channel>,

    /// The TransferEngine instance that performs RDMA / TCP data-plane transfers.
    /// Wrapped in `Arc` so it can be shared across async tasks.
    ///
    /// 执行 RDMA / TCP 数据面传输的 TransferEngine 实例。
    /// 使用 Arc 包装，可在多个异步任务间共享。C++ 等价：`std::shared_ptr<TransferEngine>`。
    pub(crate) engine: Arc<TransferEngine>,

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

    /// Scratch buffer for staging data before/after TransferEngine operations.
    /// In `write_to_replica`: data is first memcpy'd here, then TE transfers it.
    /// In `read_from_replica`: TE reads into this buffer, then we copy out.
    /// Size is controlled by `local_buffer_size` in [`create`](Self::create).
    ///
    /// 用于在 TransferEngine 操作前后暂存数据的临时缓冲区。
    /// write_to_replica: 先 memcpy 数据至此，再通过 TE 传输。
    /// read_from_replica: TE 读取到此缓冲区，再拷贝出去。
    /// 大小由 create() 中的 local_buffer_size 控制。
    pub(crate) local_buffer: Vec<u8>,

    /// Segment memory buffer (only for storage nodes with global_segment_size > 0).
    /// Must be kept alive for the lifetime of the client so the TE can access it.
    ///
    /// Segment 内存缓冲区（仅当 global_segment_size > 0 时分配，用于存储节点）。
    /// 必须在客户端整个生命周期内保持存活，以便 TE 能够访问。
    pub(crate) segment_buffer: Option<Vec<u8>>,

    /// Map of externally-registered user buffers: `ptr_addr → (size, location)`.
    /// Populated via [`register_buffer`](MooncakeClient::register_buffer) for
    /// zero-copy read/write paths. The TE must be told about these buffers so it
    /// can DMA directly into/from them.
    ///
    /// 外部注册的用户缓冲区映射表：指针地址 → (大小, 位置)。
    /// 通过 register_buffer() 填充，用于零拷贝读写路径。
    /// TE 必须被告知这些缓冲区，以便直接进行 DMA 操作。
    /// C++ 等价：`registered_buffers_` map。
    pub(crate) registered_buffers: RwLock<HashMap<usize, (usize, String)>>,

    /// Shutdown flag. When set to `true`, operations should stop and the client
    /// is considered closed. Checked via [`is_closed`](Self::is_closed).
    ///
    /// 关闭标志。设置为 true 后，操作应停止，客户端被视为已关闭。
    /// 通过 is_closed() 检查。
    pub(crate) tear_down: Arc<RwLock<bool>>,

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
}

impl MooncakeClient {
    /// Create a new Mooncake client, bootstrapping the TransferEngine,
    /// registering memory, and mounting a segment if needed.
    ///
    /// # Initialization flow (初始化流程)
    ///
    /// 1. **Connect to master** — establish a gRPC channel to the master service.
    ///    连接到 master —— 建立到 master 服务的 gRPC 通道。
    ///
    /// 2. **Parse local host** — extract IP and port from `local_host` string.
    ///    解析本地主机 —— 从 local_host 字符串中提取 IP 和端口。
    ///
    /// 3. **Create TransferEngine** — initialize the data-plane engine with
    ///    `metadata_conn_string` (etcd/redis) and the local host info.
    ///    创建 TransferEngine —— 使用 metadata_conn_string（etcd/redis）和本地主机信息
    ///    初始化数据面引擎。C++ 等价：`TransferEngine::Create(...)`。
    ///
    /// 4. **Install transport** — if protocol is not "tcp", install an RDMA-capable
    ///    transport (e.g. "rdma", "nvmeof") with the given device; otherwise
    ///    install plain TCP.
    ///    安装传输层 —— 如果协议不是 "tcp"，安装支持 RDMA 的传输层（如 "rdma"、"nvmeof"）
    ///    并指定设备；否则安装纯 TCP。
    ///
    /// 5. **Discover topology** — let the TE discover the cluster topology
    ///    (available devices, NICs, peer nodes).
    ///    发现拓扑 —— 让 TE 发现集群拓扑（可用设备、网卡、对等节点）。
    ///
    /// 6. **Register local buffer** — register the scratch `local_buffer` with
    ///    the TE so it can use it as source/destination for data transfers.
    ///    注册本地缓冲区 —— 向 TE 注册 local_buffer 暂存区，
    ///    使其可用作数据传输的源/目标。
    ///
    /// 7. **Allocate segment buffer** (if `global_segment_size > 0`) — allocate a
    ///    large memory region, register it with the TE, call `open_segment` so the
    ///    TE knows which registered memory backs this segment, and send a
    ///    `MountSegmentRequest` to the master to announce the segment.
    ///
    ///    分配 segment 缓冲区（如果 global_segment_size > 0）—— 分配大块内存，
    ///    向 TE 注册，调用 open_segment 让 TE 知道哪块注册内存支撑此 segment，
    ///    并向 master 发送 MountSegmentRequest 宣告此 segment。
    ///    C++ 等价：`Client::MountSegment(...)`。
    ///
    /// 8. **Register local endpoint** — insert `local_host` into the local
    ///    endpoints set for `select_best_replica` locality checks.
    ///    注册本地端点 —— 将 local_host 插入本地端点集合，用于 select_best_replica
    ///    的本地性判断。C++ 等价：`Client::GetLocalEndpoints()`。
    pub async fn create(
        master_addr: &str,
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
    ) -> StoreResult<Self> {
        // Step 1: Connect to master via gRPC. / 通过 gRPC 连接 master。
        let master_url = format!("http://{master_addr}");
        let channel = Channel::from_shared(master_url)
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let mut master = proto::master_service_client::MasterServiceClient::new(channel);

        // Step 2: Parse IP and port from local_host. / 从 local_host 解析 IP 和端口。
        let parts: Vec<&str> = local_host.split(':').collect();
        let ip = parts.first().copied().unwrap_or(local_host);
        let port: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);

        // Step 3: Create TransferEngine. / 创建 TransferEngine。
        let engine = TransferEngine::create(metadata_conn_string, local_host, ip, port, true)?;

        // Step 4: Install the appropriate transport. / 安装合适的传输层。
        if protocol != "tcp" {
            engine.install_transport(protocol, Some(device))?;
        } else {
            engine.install_transport("tcp", None)?;
        }

        // Step 5: Discover cluster topology. / 发现集群拓扑。
        engine.discover_topology()?;

        let engine = Arc::new(engine);

        // Step 6: Allocate and register the scratch local_buffer. / 分配并注册 local_buffer。
        let local_buffer = vec![0u8; local_buffer_size as usize];
        unsafe {
            engine.register_local_memory(
                local_buffer.as_ptr() as *mut c_void,
                local_buffer_size as usize,
                "cpu:0",
                true,
            )?;
        }

        let client_id = Uuid::new_v4();

        // Step 7: If this node is a storage node (global_segment_size > 0),
        // allocate, register, open, and mount a segment.
        // 如果本节点是存储节点（global_segment_size > 0），分配、注册、打开并挂载 segment。
        let mut segment_buffer: Option<Vec<u8>> = None;
        if global_segment_size > 0 {
            // Allocate and register segment memory with the TE so that
            // remote nodes can read from / write to this segment via RDMA/TCP.
            //
            // 分配 segment 内存并向 TE 注册，使远端节点可以通过 RDMA/TCP 读写此 segment。
            let seg_buf = vec![0u8; global_segment_size as usize];
            let base_addr = seg_buf.as_ptr() as u64;
            unsafe {
                engine.register_local_memory(
                    seg_buf.as_ptr() as *mut c_void,
                    global_segment_size as usize,
                    "cpu:0",
                    true,
                )?;
            }

            // Create a local TE segment so the transfer engine can discover
            // and resolve this node's segment memory for remote transfers.
            // Without this openSegment, the TE on this node does not know
            // which registered memory backs the segment.
            //
            // 创建本地 TE segment，使传输引擎能够发现并解析此节点的 segment 内存
            // 以进行远程传输。没有此 open_segment，本节点上的 TE 不知道
            // 哪块注册内存支撑此 segment。C++ 等价：`TransferEngine::openSegment(...)`。
            engine.open_segment(local_host)?;

            segment_buffer = Some(seg_buf);

            // Notify master about this segment so peers can discover it.
            // 通知 master 此 segment，使对等节点可以发现它。
            let request = proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: local_host.to_string(),
                size: global_segment_size,
                base_addr,
            };
            master
                .mount_segment(request)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        }

        // Step 8: Register the current node's hostname as a local endpoint.
        // Used by select_best_replica for locality-aware replica selection.
        //
        // 将当前节点的 hostname 注册为本地端点（传输地址），
        // 用于 SelectBestReplica 的本地性优先判断。
        // C++ 等价：Client::GetLocalEndpoints() 返回所有已挂载 segment 的 te_endpoint。
        let mut endpoints = HashSet::new();
        endpoints.insert(local_host.to_string());

        Ok(Self {
            master,
            engine,
            client_id,
            local_hostname: local_host.to_string(),
            local_buffer,
            segment_buffer,
            registered_buffers: RwLock::new(HashMap::new()),
            tear_down: Arc::new(RwLock::new(false)),
            local_endpoints: RwLock::new(endpoints),
            miss_handler: None,
            hot_cache: None,
        })
    }

    // -----------------------------------------------------------------------
    // Simple accessors / 简单访问器
    // -----------------------------------------------------------------------

    /// Return this node's hostname. / 返回本节点的主机名。
    pub fn get_hostname(&self) -> String {
        self.local_hostname.clone()
    }

    /// Send a ping to the master to verify the connection is alive.
    /// 向 master 发送 ping 以验证连接是否存活。
    pub async fn health_check(&mut self) -> StoreResult<()> {
        let request = proto::PingRequest {
            client_id: Some(self.client_id_proto()),
            mounted_segments: vec![],
            tenant_id: String::new(),
        };
        self.master
            .ping(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Register a local transport endpoint (e.g. the `te_endpoint` of a newly
    /// mounted segment). After registration, replicas hosted on this endpoint
    /// are considered "local" by [`select_best_replica`](Self::select_best_replica)
    /// and can use the fast `local_memcpy` path.
    ///
    /// 注册一个本地传输端点（例如新挂载 segment 的 te_endpoint）。
    /// 注册后，位于此端点上的副本会被 select_best_replica 视为"本地"，
    /// 可以使用 local_memcpy 快速路径。
    /// C++ 等价：mounted_segments_ 中 segment.te_endpoint 被加入 GetLocalEndpoints()。
    pub fn register_local_endpoint(&self, endpoint: &str) {
        self.local_endpoints.write().insert(endpoint.to_string());
    }

    /// Unregister a local transport endpoint (e.g. when a segment is unmounted).
    /// 取消注册一个本地传输端点（例如 segment 卸载时）。
    pub fn unregister_local_endpoint(&self, endpoint: &str) {
        self.local_endpoints.write().remove(endpoint);
    }

    /// Attach a remote source for cache-miss fallback.
    ///
    /// The remote source is only consulted when `config.enabled` is `true`.
    /// When disabled, [`get`](Self::get) returns `KeyNotFound` as usual.
    ///
    /// Builder-pattern method: call it on the client after `create()`.
    ///
    /// 附加一个远程数据源用于缓存未命中回退。
    /// 仅当 config.enabled 为 true 时才查询远程数据源。
    /// 禁用时，get() 照常返回 KeyNotFound。
    /// 构建器模式方法：在 create() 之后调用。
    pub fn with_remote_source(
        mut self,
        source: impl RemoteSource + 'static,
        config: RemoteSourceConfig,
    ) -> Self {
        self.miss_handler = Some(MissHandler::new(
            Arc::new(source) as Arc<dyn RemoteSource>,
            config,
        ));
        self
    }

    /// Attach a local hot cache for two-level caching:
    /// 1. `get()` checks this before gRPC fetch (fastest path — zero network).
    /// 2. Remote-source fetches and prefetches are stored here for future hits.
    ///
    /// Builder-pattern method: call it on the client after `create()`.
    ///
    /// 附加一个本地热缓存用于二级缓存：
    /// 1. get() 在 gRPC 获取之前检查此缓存（最快路径 —— 零网络开销）。
    /// 2. 远程数据源获取和预取的结果存储在这里以供将来命中。
    /// 构建器模式方法：在 create() 之后调用。
    pub fn with_hot_cache(mut self, cache: Arc<LocalHotCache>) -> Self {
        self.hot_cache = Some(cache);
        self
    }

    /// Returns `true` if the client has been torn down. / 如果客户端已关闭则返回 true。
    pub fn is_closed(&self) -> bool {
        *self.tear_down.read()
    }

    /// Tear down the client: set the shutdown flag, unregister the local buffer
    /// and all user-registered buffers from the TransferEngine.
    ///
    /// After this call the client should not be used for further operations.
    ///
    /// 关闭客户端：设置关闭标志，从 TransferEngine 中取消注册本地缓冲区和
    /// 所有用户注册的缓冲区。此调用后客户端不应再用于任何操作。
    /// C++ 等价：`Client::TearDownAll()`。
    pub async fn tear_down_all(&mut self) -> StoreResult<()> {
        *self.tear_down.write() = true;

        // unregister local buffer / 取消注册本地缓冲区
        unsafe {
            let _ = self
                .engine
                .unregister_local_memory(self.local_buffer.as_ptr() as *mut c_void);
        }

        // unregister all user-registered buffers / 取消注册所有用户注册的缓冲区
        let ptrs: Vec<usize> = self.registered_buffers.read().keys().copied().collect();
        for ptr in &ptrs {
            unsafe {
                let _ = self.engine.unregister_local_memory(*ptr as *mut c_void);
            }
        }
        self.registered_buffers.write().clear();
        Ok(())
    }
}

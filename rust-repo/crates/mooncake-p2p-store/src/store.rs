//! P2P Store — distributed checkpoint storage backed by the Transfer Engine.
//! P2P 存储 — 基于 Transfer Engine 的分布式检查点存储。
//!
//! # Design / 设计
//!
//! `P2pStore` combines three subsystems:
//! `P2pStore` 组合了三个子系统：
//!
//! 1. **Transfer Engine** (`Arc<TransferEngine>`): Handles RDMA/TCP data
//!    movement. Shared via Arc because the engine is a heavyweight resource.
//!    处理 RDMA/TCP 数据传输。通过 Arc 共享，因为引擎是重量级资源。
//!
//! 2. **Metadata Store** (`Mutex<MetadataStore>`): Manages payload metadata
//!    in etcd. Protected by a Mutex because etcd operations are async but
//!    the client is not Sync.
//!    管理 etcd 中的负载元数据。由 Mutex 保护，因为 etcd 操作是异步的，
//!    但客户端不是 Sync 的。
//!
//! 3. **Catalog** (`Mutex<HashMap<String, CatalogEntry>>`): Tracks which
//!    payloads are locally registered and their memory addresses.
//!    Used for unregistering (cleanup).
//!    跟踪哪些负载已本地注册及其内存地址。用于取消注册（清理）。
//!
//! # Store Operations / 存储操作
//!
//! ## Register (Write Path / 写入路径)
//!
//! `register(name, addr_list, size_list, max_shard_size, location, force_create)`
//!
//! 1. Validate inputs (non-empty, matching lengths)
//! 2. Check catalog for duplicate registration → `PayloadOpened`
//! 3. Register each memory buffer with the Transfer Engine
//! 4. Split each buffer into shards of `max_shard_size` bytes
//! 5. Create `Location` entries pointing to this node's segment
//! 6. Write `Payload` metadata to etcd (create or put)
//! 7. Insert into local catalog
//!
//! ## Get Replica (Read Path / 读取路径)
//!
//! `get_replica(name, addr_list, size_list)`
//!
//! 1. Look up Payload metadata from etcd
//! 2. Register local receive buffers with Transfer Engine
//! 3. For each shard: pick a location (with retry), open segment,
//!    submit RDMA Read, poll until terminal, close segment
//! 4. Shards are written sequentially into the local buffers
//!
//! ## Unregister (Cleanup / 清理)
//!
//! `unregister(name)`
//!
//! 1. Check catalog for existence → `PayloadNotOpened`
//! 2. Atomically clear the gold locations in etcd (CAS loop)
//! 3. On success: remove from catalog, unregister memory
//!
//! # Concurrency / 并发
//!
//! - The catalog lock is held across register/get_replica/unregister to
//!   prevent double-registration and use-after-free.
//! - The metadata lock is held per-etcd-operation, not across the entire
//!   method, to avoid holding it during slow RDMA operations.
//! - etcd transactions provide atomicity for metadata updates across nodes.
//!
//! - catalog 锁在 register/get_replica/unregister 期间持有，防止重复注册和释放后使用。
//! - metadata 锁在每次 etcd 操作期间持有，而非整个方法期间，避免在慢速 RDMA 操作期间持锁。
//! - etcd 事务为跨节点元数据更新提供原子性。

use crate::error::P2pStoreError;
use crate::metadata::{Location, MetadataStore, Payload, PayloadInfo, Shard, METADATA_KEY_PREFIX};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;
use transfer_engine_ffi::{Opcode, TransferEngine, TransferRequest};

/// Maximum size of a single transfer chunk: 4 GiB.
/// 单次传输块的最大大小：4 GiB。
///
/// Transfers larger than this are split across multiple shards.
/// 超过此大小的传输会分割为多个分片。
pub const MAX_CHUNK_SIZE: u64 = 4096 * 1024 * 1024;

/// A registered memory buffer with its address and size.
/// 已注册的内存缓冲区，包含地址和大小。
#[derive(Debug, Clone)]
pub struct Buffer {
    pub addr: usize,
    pub size: u64,
}

/// Internal catalog entry tracking the memory addresses of a registered payload.
/// 内部目录条目，跟踪已注册负载的内存地址。
#[derive(Debug, Clone)]
struct CatalogEntry {
    addr_list: Vec<usize>,
}

/// The P2P Store instance.
/// P2P Store 实例。
///
/// Manages distributed checkpoint data across a cluster. Each node creates
/// one P2pStore, registers its local tensors/checkpoints, and can read
/// replicas from other nodes.
/// 管理集群中的分布式检查点数据。每个节点创建一个 P2pStore，
/// 注册其本地张量/检查点，并可从其他节点读取副本。
pub struct P2pStore {
    /// This node's segment name (IP:port), used as the segment identifier
    /// in Location entries.
    /// 本节点的段名称（IP:port），用作 Location 条目中的段标识符。
    local_server_name: String,
    /// Maps payload names to their registered memory addresses.
    /// 将负载名称映射到其注册的内存地址。
    catalog: Mutex<HashMap<String, CatalogEntry>>,
    /// etcd-backed metadata store for sharing payload info across nodes.
    /// 基于 etcd 的元数据存储，用于跨节点共享负载信息。
    metadata: Mutex<MetadataStore>,
    /// The underlying Transfer Engine for RDMA/TCP data movement.
    /// 底层 Transfer Engine，用于 RDMA/TCP 数据移动。
    engine: Arc<TransferEngine>,
}

impl P2pStore {
    /// Create a new P2pStore.
    /// 创建新的 P2pStore。
    ///
    /// This initializes all three subsystems:
    /// 这会初始化所有三个子系统：
    ///
    /// 1. Creates a `MetadataStore` connected to the etcd cluster.
    ///    创建连接到 etcd 集群的 `MetadataStore`。
    /// 2. Creates a `TransferEngine` and installs the transport (TCP or RDMA).
    ///    创建 `TransferEngine` 并安装传输协议（TCP 或 RDMA）。
    /// 3. Initializes an empty local catalog.
    ///    初始化空的本地目录。
    ///
    /// # Parameters / 参数
    ///
    /// - `metadata_conn_string`: etcd connection string for peer discovery.
    ///   etcd 连接字符串，用于节点发现。
    /// - `local_server_name`: This node's `IP:port` identifier.
    ///   本节点的 `IP:port` 标识符。
    /// - `nic_priority_matrix`: RDMA NIC priority (semicolon-separated).
    ///   If empty, falls back to TCP transport.
    ///   RDMA 网卡优先级（分号分隔）。如果为空，回退到 TCP 传输。
    pub async fn new(
        metadata_conn_string: &str,
        local_server_name: &str,
        nic_priority_matrix: &str,
    ) -> Result<Self, P2pStoreError> {
        let metadata = MetadataStore::new(metadata_conn_string, METADATA_KEY_PREFIX).await?;

        let parts: Vec<&str> = local_server_name.split(':').collect();
        let ip = parts.first().copied().unwrap_or(local_server_name);
        let port: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);

        let engine =
            TransferEngine::create(metadata_conn_string, local_server_name, ip, port, true)
                .map_err(|_| P2pStoreError::TransferEngine)?;

        // Install transport: RDMA if a NIC priority matrix is provided, otherwise TCP.
        // 安装传输协议：如果提供了网卡优先级矩阵则使用 RDMA，否则使用 TCP。
        if nic_priority_matrix.is_empty() {
            engine
                .install_transport("tcp", None)
                .map_err(|_| P2pStoreError::TransferEngine)?;
        } else {
            engine
                .install_transport("rdma", Some(nic_priority_matrix))
                .map_err(|_| P2pStoreError::TransferEngine)?;
        }

        let engine = Arc::new(engine);

        Ok(Self {
            local_server_name: local_server_name.to_string(),
            catalog: Mutex::new(HashMap::new()),
            metadata: Mutex::new(metadata),
            engine,
        })
    }

    /// Get the actual IP:port this node is listening on.
    /// 获取本节点实际监听的 IP:port。
    ///
    /// Useful after construction since the port may have been auto-assigned.
    /// 在构造后很有用，因为端口可能是自动分配的。
    pub fn get_local_server_name(&self) -> Result<String, P2pStoreError> {
        self.engine
            .get_local_ip_and_port()
            .map_err(|_| P2pStoreError::TransferEngine)
    }

    /// Register a payload (local memory buffers) with the P2P store.
    /// 向 P2P 存储注册一个负载（本地内存缓冲区）。
    ///
    /// This is the write path: it makes local data available for other nodes
    /// to read via RDMA/TCP.
    /// 这是写入路径：使本地数据可通过 RDMA/TCP 供其他节点读取。
    ///
    /// # Parameters / 参数
    ///
    /// - `name`: Unique payload name (used as etcd key).
    ///   唯一负载名称（用作 etcd 键）。
    /// - `addr_list`: List of memory buffer addresses to register.
    ///   要注册的内存缓冲区地址列表。
    /// - `size_list`: Corresponding sizes for each buffer.
    ///   每个缓冲区的对应大小。
    /// - `max_shard_size`: Maximum bytes per shard when splitting.
    ///   分割时每个分片的最大字节数。
    /// - `location`: Device string (e.g., "cpu:0") for memory registration.
    ///   内存注册的设备字符串（如 "cpu:0"）。
    /// - `force_create`: If true, overwrite existing metadata. If false,
    ///   fail if the name already exists in etcd.
    ///   如果为 true，覆盖已有元数据。如果为 false，etcd 中名称已存在时失败。
    pub async fn register(
        &self,
        name: &str,
        addr_list: &[usize],
        size_list: &[u64],
        max_shard_size: u64,
        location: &str,
        force_create: bool,
    ) -> Result<(), P2pStoreError> {
        // Validate inputs: must be non-empty and have matching lengths.
        // 验证输入：必须非空且长度匹配。
        if addr_list.is_empty() || addr_list.len() != size_list.len() {
            return Err(P2pStoreError::InvalidArgument);
        }

        // Check for duplicate registration.
        // 检查重复注册。
        {
            let catalog = self.catalog.lock();
            if catalog.contains_key(name) {
                return Err(P2pStoreError::PayloadOpened);
            }
        }

        // Build the Payload metadata structure.
        // 构建 Payload 元数据结构。
        let mut payload = Payload {
            name: name.to_string(),
            size: 0,
            size_list: size_list.to_vec(),
            max_shard_size,
            shards: Vec::new(),
        };

        // Register each memory buffer with the Transfer Engine and
        // split it into shards. Each shard gets a Location pointing
        // to this node's segment.
        // 向 Transfer Engine 注册每个内存缓冲区，并将其分割为分片。
        // 每个分片获得一个指向本节点段的 Location。
        for i in 0..addr_list.len() {
            let addr = addr_list[i] as *mut c_void;
            let size = size_list[i];

            // Register memory so remote peers can access it.
            // 注册内存以便远程节点可以访问。
            unsafe {
                self.engine
                    .register_local_memory(addr, size as usize, location, true)
                    .map_err(|_| P2pStoreError::TransferEngine)?;
            }

            payload.size += size;
            let mut offset: u64 = 0;
            while offset < size {
                let shard_len = max_shard_size.min(size - offset);
                payload.shards.push(Shard {
                    length: shard_len,
                    gold: vec![Location {
                        segment_name: self.local_server_name.clone(),
                        offset: addr as u64 + offset,
                    }],
                    replica_list: Vec::new(),
                });
                offset += max_shard_size;
            }
        }

        // Write metadata to etcd.
        // 将元数据写入 etcd。
        {
            let mut meta = self.metadata.lock();
            if force_create {
                meta.put(name, &payload).await?;
            } else {
                meta.create(name, &payload).await?;
            }
        }

        // Track in local catalog for later unregister.
        // 记录到本地目录以供后续取消注册。
        self.catalog.lock().insert(
            name.to_string(),
            CatalogEntry {
                addr_list: addr_list.to_vec(),
            },
        );

        Ok(())
    }

    /// Unregister a previously registered payload.
    /// 取消注册之前注册的负载。
    ///
    /// This is the cleanup path: it atomically clears the gold locations
    /// in etcd (using a CAS loop to handle concurrent modifications),
    /// then removes the local catalog entry and unregisters memory.
    /// 这是清理路径：原子性地清除 etcd 中的 gold 位置
    /// （使用 CAS 循环处理并发修改），
    /// 然后移除本地目录条目并注销内存。
    ///
    /// The CAS loop is needed because multiple nodes may try to unregister
    /// the same payload concurrently (e.g., during distributed cleanup).
    /// CAS 循环是必要的，因为多个节点可能同时尝试取消注册同一负载
    /// （如在分布式清理期间）。
    pub async fn unregister(&self, name: &str) -> Result<(), P2pStoreError> {
        let catalog_entry = {
            let catalog = self.catalog.lock();
            catalog
                .get(name)
                .cloned()
                .ok_or(P2pStoreError::PayloadNotOpened)?
        };

        // CAS loop: read, modify (clear gold), write back atomically.
        // CAS 循环：读取、修改（清除 gold）、原子写回。
        loop {
            let (payload, revision) = {
                let mut meta = self.metadata.lock();
                meta.get(name).await?
            };
            let payload = payload.ok_or(P2pStoreError::PayloadNotFound)?;

            let mut cleared = payload.clone();
            for shard in &mut cleared.shards {
                shard.gold.clear();
            }

            let success = {
                let mut meta = self.metadata.lock();
                meta.update(name, &cleared, revision).await?
            };
            if success {
                self.catalog.lock().remove(name);
                // Unregister each memory buffer from the Transfer Engine.
                // 从 Transfer Engine 注销每个内存缓冲区。
                for i in 0..catalog_entry.addr_list.len() {
                    unsafe {
                        self.engine
                            .unregister_local_memory(catalog_entry.addr_list[i] as *mut c_void)
                            .map_err(|_| P2pStoreError::TransferEngine)?;
                    }
                }
                return Ok(());
            }
            // CAS failed — another node modified the metadata. Retry.
            // CAS 失败 — 另一个节点修改了元数据。重试。
        }
    }

    /// List available payloads with the given name prefix.
    /// 列出具有给定名称前缀的可用负载。
    ///
    /// Returns lightweight `PayloadInfo` summaries (no shard/location details).
    /// 返回轻量级 `PayloadInfo` 摘要（无分片/位置详情）。
    pub async fn list(&self, prefix: &str) -> Result<Vec<PayloadInfo>, P2pStoreError> {
        let mut meta = self.metadata.lock();
        let payloads: Vec<Payload> = meta.list(prefix).await?;
        Ok(payloads
            .into_iter()
            .map(|p| PayloadInfo {
                name: p.name,
                max_shard_size: p.max_shard_size,
                total_size: p.size,
                size_list: p.size_list,
            })
            .collect())
    }

    /// Read a payload replica into local buffers.
    /// 将负载副本读入本地缓冲区。
    ///
    /// This is the read path: it fetches all shards of a payload from remote
    /// nodes and writes them into the provided local buffers.
    /// 这是读取路径：从远程节点获取负载的所有分片，
    /// 并写入提供的本地缓冲区中。
    ///
    /// # How it works / 工作原理
    ///
    /// 1. Look up the Payload metadata from etcd.
    ///    从 etcd 查找 Payload 元数据。
    /// 2. Register the local receive buffers with the Transfer Engine.
    ///    向 Transfer Engine 注册本地接收缓冲区。
    /// 3. For each shard:
    ///    a. Pick a location using `get_location(retry=3)` for fallback.
    ///       使用 `get_location(retry=3)` 选择位置以支持回退。
    ///    b. Open the remote segment.
    ///       打开远程段。
    ///    c. Submit an RDMA Read transfer request.
    ///       提交 RDMA 读取传输请求。
    ///    d. Poll until the transfer reaches a terminal state.
    ///       轮询直到传输达到终态。
    ///    e. Free the batch and close the segment.
    ///       释放批次并关闭段。
    /// 4. Shards are written sequentially: task 0 → addr[0][0..max_shard],
    ///    task 1 → addr[0][max_shard..2*max_shard], etc.
    ///    分片按顺序写入：任务 0 → addr[0][0..max_shard]，
    ///    任务 1 → addr[0][max_shard..2*max_shard]，依此类推。
    ///
    /// # Parameters / 参数
    ///
    /// - `name`: Name of the payload to read.
    ///   要读取的负载名称。
    /// - `addr_list`: Local memory buffers to receive the data.
    ///   接收数据的本地内存缓冲区。
    /// - `size_list`: Sizes of the local buffers (must match payload metadata).
    ///   本地缓冲区的大小（必须与负载元数据匹配）。
    pub async fn get_replica(
        &self,
        name: &str,
        addr_list: &[usize],
        size_list: &[u64],
    ) -> Result<(), P2pStoreError> {
        let (payload_opt, _) = {
            let mut meta = self.metadata.lock();
            meta.get(name).await?
        };
        let payload = payload_opt.ok_or(P2pStoreError::PayloadNotFound)?;

        let max_shard_size = payload.max_shard_size;

        // Register all local receive buffers.
        // 注册所有本地接收缓冲区。
        for i in 0..addr_list.len() {
            let addr = addr_list[i] as *mut c_void;
            let size = size_list[i] as usize;
            unsafe {
                self.engine
                    .register_local_memory(addr, size, "cpu:0", true)
                    .map_err(|_| P2pStoreError::TransferEngine)?;
            }
        }

        // Iterate over all shards and fetch each one via RDMA Read.
        // 遍历所有分片，通过 RDMA Read 获取每个分片。
        let mut task_id = 0usize;
        for i in 0..addr_list.len() {
            let size = size_list[i];
            let mut local_off: u64 = 0;
            while local_off < size && task_id < payload.shards.len() {
                let source = addr_list[i] as *mut c_void;
                let shard = &payload.shards[task_id];

                // Try up to 3 retries for location selection.
                // 最多尝试 3 次位置选择重试。
                if let Some(loc) = shard.get_location(3) {
                    let segment_id = self
                        .engine
                        .open_segment(&loc.segment_name)
                        .map_err(|_| P2pStoreError::TransferEngine)?;

                    let batch_id = self
                        .engine
                        .allocate_batch_id(1)
                        .map_err(|_| P2pStoreError::TransferEngine)?;

                    // Build a Read request: pull data from remote segment into local buffer.
                    // 构建读取请求：从远程段拉取数据到本地缓冲区。
                    let request = TransferRequest {
                        opcode: Opcode::Read,
                        source: unsafe { source.offset(local_off as isize) },
                        target_id: segment_id,
                        target_offset: loc.offset,
                        length: shard.length,
                    };

                    self.engine
                        .submit_transfer(batch_id, &[request])
                        .map_err(|_| P2pStoreError::TransferEngine)?;

                    // Poll until the transfer completes (or fails).
                    // 轮询直到传输完成（或失败）。
                    loop {
                        let status = self
                            .engine
                            .get_transfer_status(batch_id, 0)
                            .map_err(|_| P2pStoreError::TransferEngine)?;
                        if status.status.is_terminal() {
                            break;
                        }
                    }

                    self.engine
                        .free_batch_id(batch_id)
                        .map_err(|_| P2pStoreError::TransferEngine)?;
                    self.engine
                        .close_segment(segment_id)
                        .map_err(|_| P2pStoreError::TransferEngine)?;
                }

                task_id += 1;
                local_off += max_shard_size;
            }
        }

        Ok(())
    }
}

use super::MooncakeClient;
use crate::local_storage_backend::{
    AttachedLocalStorage, LocalStorageBackend, OffsetAllocatorStorageBackend,
};
use crate::proto;
use crate::{
    LocalHotCache, MissHandler, MissHandlerSnapshot, MissHandlerStats, RemoteSource,
    RemoteSourceConfig,
};
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // Simple accessors / 简单访问器
    // -----------------------------------------------------------------------

    /// Return this node's hostname. / 返回本节点的主机名。
    pub fn get_hostname(&self) -> String {
        self.local_hostname.clone()
    }

    /// Return the currently connected master address.
    pub fn current_master_addr(&self) -> String {
        self.master_addr.read().clone()
    }

    /// Return the current client-side HA candidate list.
    pub fn master_candidates(&self) -> Vec<String> {
        self.master_candidates.read().clone()
    }

    /// Replace the client-side HA candidate list.
    pub fn set_master_candidates(&self, candidates: Vec<String>) -> StoreResult<()> {
        if candidates.is_empty() {
            return Err(StoreError::InvalidParams(
                "at least one master candidate is required".to_string(),
            ));
        }
        if candidates.iter().any(|addr| addr.trim().is_empty()) {
            return Err(StoreError::InvalidParams(
                "master candidate address must not be empty".to_string(),
            ));
        }
        *self.master_candidates.write() = candidates;
        Ok(())
    }

    /// Switch this client to a new master address.
    ///
    /// This is the Rust equivalent of C++ `Client::SwitchLeader`: stale or
    /// duplicate views are filtered by the caller, while this method performs
    /// the actual channel swap atomically from the client's perspective.
    pub async fn switch_master(&mut self, master_addr: &str) -> StoreResult<()> {
        let next_master = Self::connect_master_addr(master_addr).await?;
        self.master = next_master;
        *self.master_addr.write() = master_addr.trim().to_string();
        self.last_ping_success.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Try every configured master candidate except the current one until one
    /// connects. Returns the connected address.
    pub async fn failover_master(&mut self) -> StoreResult<String> {
        let current = self.current_master_addr();
        let mut candidates = self.master_candidates();
        candidates.sort();
        candidates.dedup();

        let mut last_error = None;
        for addr in candidates {
            if addr == current {
                continue;
            }
            match self.switch_master(&addr).await {
                Ok(()) => return Ok(addr),
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            StoreError::Internal("no alternate master candidate connected".to_string())
        }))
    }

    /// Send a ping to the master. If the master returns NeedRemount, trigger an
    /// asynchronous ReMountSegment call (at most one in-flight at any time).
    ///
    /// 向 master 发送 ping。如果 master 返回 NeedRemount，触发异步 ReMountSegment 调用
    /// （同一时间最多只有一个在途）。C++ equivalent: Client::Ping in client_service.cpp:3530
    pub async fn health_check(&mut self) -> StoreResult<()> {
        let request = proto::PingRequest {
            client_id: Some(self.client_id_proto()),
            mounted_segments: vec![],
            tenant_id: self.tenant_id.clone(),
        };
        let response = match self.master.ping(self.rpc_request(request)).await {
            Ok(response) => response,
            Err(first_error) => {
                self.last_ping_success.store(false, Ordering::SeqCst);
                self.failover_master().await?;
                self.master
                    .ping(self.rpc_request(proto::PingRequest {
                        client_id: Some(self.client_id_proto()),
                        mounted_segments: vec![],
                        tenant_id: self.tenant_id.clone(),
                    }))
                    .await
                    .map_err(|second_error| {
                        self.last_ping_success.store(false, Ordering::SeqCst);
                        let mapped = Self::rpc_status_to_error(second_error);
                        StoreError::Internal(format!(
                            "ping failed before failover ({first_error}); after failover: {mapped}"
                        ))
                    })?
            }
        }
        .into_inner();

        self.last_ping_success.store(true, Ordering::SeqCst);

        // C++ client_service.cpp:3547 — check client_status for NeedRemount
        // C++ 中检查 client_status 是否为 NeedRemount
        if response.client_status == proto::ClientStatus::NeedRemount as i32 {
            self.try_trigger_remount();
        }

        Ok(())
    }

    /// Trigger an asynchronous ReMountSegment if one is not already in progress.
    /// 如果没有正在进行的 remount，触发异步 ReMountSegment。
    ///
    /// C++ equivalent: the std::async + remount_segment_future guard in client_service.cpp:L3546-L3551
    fn try_trigger_remount(&self) {
        // Ensure at most one remount segment task is running.
        // 确保同一时间最多只有一个 remount 任务在运行。
        if self.remount_in_progress.swap(true, Ordering::SeqCst) {
            return; // already in progress / 已有在途
        }

        if self.segment_name.is_empty() || self.segment_size == 0 {
            self.remount_in_progress.store(false, Ordering::SeqCst);
            return; // not a storage node / 非存储节点
        }

        let mut master = self.master.clone();
        let client_id = self.client_id_proto();
        let segment_name = self.segment_name.clone();
        let segment_size = self.segment_size;
        let te_endpoint = self.local_hostname.clone();
        let protocol = self.protocol.clone();
        let rpc_request_timeout = self.rpc_request_timeout;
        // SAFETY: segment_buffer is allocated in create() and never moved/reallocated
        // during the client's lifetime, so its pointer remains valid.
        let base_addr = self
            .segment_buffer
            .as_ref()
            .map(|buf| buf.as_ptr() as u64)
            .unwrap_or(0);
        let remount_flag = self.remount_in_progress.clone();

        // Spawn a background task so we don't block the caller.
        // 启动后台任务，不阻塞调用方。
        tokio::spawn(async move {
            let request = proto::ReMountSegmentRequest {
                client_id: Some(client_id),
                segment_names: vec![segment_name],
                segment_sizes: vec![segment_size],
                base_addrs: vec![base_addr],
                te_endpoints: vec![te_endpoint],
                protocols: vec![protocol],
            };
            match master
                .re_mount_segment(Self::rpc_request_with_timeout(request, rpc_request_timeout))
                .await
            {
                Ok(_) => {
                    tracing::info!("ReMountSegment succeeded");
                }
                Err(e) => {
                    tracing::error!("ReMountSegment failed: {}", e);
                }
            }
            remount_flag.store(false, Ordering::SeqCst);
        });
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

    /// Return a snapshot of remote miss / hot-cache fallback statistics.
    ///
    /// This exposes the Rust client's local miss-handler counters. It is not a
    /// replacement for the C++ master's `CalcCacheStats`, which requires a
    /// master-side RPC that is not present in the Rust proto surface.
    pub fn miss_handler_snapshot(&self) -> Option<MissHandlerSnapshot> {
        self.miss_handler.as_ref().map(MissHandler::snapshot)
    }

    /// Return raw remote miss / hot-cache fallback counters.
    pub fn miss_handler_stats(&self) -> Option<MissHandlerStats> {
        self.miss_handler.as_ref().map(MissHandler::stats)
    }

    /// Attach a local storage backend for offload/promotion to local disk.
    ///
    /// When set, [`offload_objects`](Self::offload_objects) and
    /// [`promote_objects`](Self::promote_objects) will perform actual disk I/O
    /// (write, read, delete) as part of the offload/promotion cycle.
    ///
    /// Builder-pattern method: call it on the client after `create()`.
    ///
    /// 附加一个本地存储后端用于 offload/promotion 到本地磁盘。
    /// 设置后，offload_objects() 和 promote_objects() 将在 offload/promotion
    /// 循环中执行实际的磁盘 I/O（写、读、删）。
    ///
    /// 构建器模式方法：在 create() 之后调用。
    pub fn with_local_storage_backend(mut self, backend: Arc<LocalStorageBackend>) -> Self {
        self.local_storage = Some(AttachedLocalStorage::FilePerKey(backend));
        self
    }

    /// Attach an offset-allocator SSD backend for offload and promotion.
    pub fn with_offset_allocator_storage_backend(
        mut self,
        backend: Arc<OffsetAllocatorStorageBackend>,
    ) -> Self {
        self.local_storage = Some(AttachedLocalStorage::OffsetAllocator(backend));
        self
    }

    /// Start the P2P offload RPC server on an auto-allocated port.
    /// This enables peers to read offloaded data from this node's local SSD.
    /// Must be called after `with_local_storage_backend` and from within a tokio runtime.
    ///
    /// C++ equivalent: offload_rpc_server_ startup in `RealClient::setup_internal`.
    ///
    /// 在自动分配的端口上启动 P2P 卸载 RPC 服务器。
    /// 这使得对等节点可以从本节点的本地 SSD 读取卸载的数据。
    /// 必须在 with_local_storage_backend 之后、tokio 运行时内调用。
    pub async fn start_offload_server(&self) -> StoreResult<u16> {
        let storage = self.local_storage.as_ref().ok_or_else(|| {
            StoreError::Internal(
                "no local storage backend — call with_local_storage_backend first".to_string(),
            )
        })?;

        let handler = crate::offload::server::OffloadReadHandler {
            storage: storage.clone(),
            engine: Arc::clone(&self.engine),
            pool: Arc::new(crate::offload::buffer::OffloadBufferPool::new()),
            te_endpoint: self.local_hostname.clone(),
        };

        let (port, handle) = crate::offload::server::start_offload_server(handler).await;
        *self.offload_server_handle.write() = Some(handle);
        self.offload_server_port
            .store(port, std::sync::atomic::Ordering::SeqCst);

        // Build the RPC address: hostname (without port) + offload port.
        let addr = if let Some(pos) = self.local_hostname.rfind(':') {
            format!("{}:{port}", &self.local_hostname[..pos])
        } else {
            format!("{}:{port}", self.local_hostname)
        };
        *self.offload_rpc_addr.write() = addr;

        Ok(port)
    }

    /// Returns the P2P offload RPC address (`hostname:port`) if the server is running.
    /// C++ equivalent: `RealClient::local_rpc_addr`
    pub fn offload_rpc_address(&self) -> String {
        self.offload_rpc_addr.read().clone()
    }

    /// Returns `true` if the last ping to the master was successful.
    /// This is a cheap, non-blocking call suitable for polling loops.
    ///
    /// C++ equivalent: `Client::is_ping_healthy()`
    ///
    /// 如果最后一次向 master 的 ping 成功则返回 true。
    /// 这是一个轻量的、非阻塞的调用，适合轮询循环。
    pub fn is_ping_healthy(&self) -> bool {
        self.last_ping_success.load(Ordering::SeqCst)
    }

    /// Manually trigger a ReMountSegment request. Only one remount may be
    /// in-flight at a time; subsequent calls while one is pending are no-ops.
    /// This is also called automatically from [`health_check`](Self::health_check)
    /// when the master returns `NeedRemount`.
    ///
    /// C++ equivalent: `Client::ReMountSegment`
    ///
    /// 手动触发 ReMountSegment 请求。同一时间最多只有一个 remount 在途；
    /// 在已有的 remount 完成前，后续调用为 no-op。
    /// 当 master 返回 NeedRemount 时，health_check 也会自动调用此方法。
    pub fn remount_segment(&self) {
        self.try_trigger_remount();
    }
}

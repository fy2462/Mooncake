use super::MooncakeClient;
use crate::hot_cache::HotCacheAdmission;
use crate::local_storage_backend::{
    AttachedLocalStorage, BucketStorageBackend, DistributedStorageBackend, LocalStorageBackend,
    OffsetAllocatorStorageBackend,
};
use crate::proto;
use crate::{
    LocalHotCache, MissHandler, MissHandlerSnapshot, MissHandlerStats, RemoteSource,
    RemoteSourceConfig,
};
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
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
        let mut next_master = Self::connect_master_addr(master_addr, self.metrics.clone()).await?;
        let storage_config = next_master
            .get_storage_config(Self::rpc_request_with_timeout(
                proto::GetStorageConfigRequest {},
                self.rpc_request_timeout,
            ))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        let failover_alignment = Self::validate_memory_segment_alignment(
            &storage_config.memory_allocator,
            storage_config.memory_segment_alignment,
        )?;
        if failover_alignment != self.memory_segment_alignment {
            return Err(StoreError::InvalidParams(format!(
                "failover Master Memory segment alignment changed from {} to {}",
                self.memory_segment_alignment, failover_alignment
            )));
        }
        match (&self.global_disk, storage_config.fs_dir.is_empty()) {
            (None, true) => {}
            (Some(storage), false) => storage.validate_advertised_config(
                &storage_config.fs_dir,
                storage_config.enable_disk_eviction,
                storage_config.quota_bytes,
                storage_config.enable_tenant_scope,
            )?,
            (None, false) => {
                return Err(StoreError::InvalidParams(
                    "failover Master enables global DISK but this client was initialized without it"
                        .to_string(),
                ));
            }
            (Some(_), true) => {
                return Err(StoreError::InvalidParams(
                    "failover Master disables global DISK for a client initialized with it"
                        .to_string(),
                ));
            }
        }
        self.master = next_master;
        *self.master_addr.write() = master_addr.trim().to_string();
        self.health_state.record_failure();
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
                Ok(()) => match self.ping_current_master_and_remount().await {
                    Ok(()) => return Ok(addr),
                    Err(err) => last_error = Some(err),
                },
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            StoreError::Internal("no alternate master candidate connected".to_string())
        }))
    }

    async fn ping_current_master_and_remount(&mut self) -> StoreResult<()> {
        let response = self
            .master
            .ping(self.rpc_request(proto::PingRequest {
                client_id: Some(self.client_id_proto()),
                mounted_segments: vec![],
                tenant_id: self.tenant_id.clone(),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if response.client_status == proto::ClientStatus::NeedRemount as i32 {
            self.remount_all().await?;
        }
        self.health_state.record_success();
        Ok(())
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
                self.health_state.record_failure();
                self.failover_master().await?;
                self.master
                    .ping(self.rpc_request(proto::PingRequest {
                        client_id: Some(self.client_id_proto()),
                        mounted_segments: vec![],
                        tenant_id: self.tenant_id.clone(),
                    }))
                    .await
                    .map_err(|second_error| {
                        self.health_state.record_failure();
                        let mapped = Self::rpc_status_to_error(second_error);
                        StoreError::Internal(format!(
                            "ping failed before failover ({first_error}); after failover: {mapped}"
                        ))
                    })?
            }
        }
        .into_inner();

        self.health_state.record_success();

        // C++ client_service.cpp:3547 — check client_status for NeedRemount
        // C++ 中检查 client_status 是否为 NeedRemount
        if response.client_status == proto::ClientStatus::NeedRemount as i32 {
            if let Err(error) = self.remount_all().await {
                self.health_state.record_failure();
                return Err(error);
            }
        }

        Ok(())
    }

    /// Restore every mounted storage role after a Master failover.
    ///
    /// Memory is remounted first. A LocalDisk namespace then performs the same
    /// Begin → inventory report → Commit transaction as process restart; a
    /// local-disk-only client therefore no longer loops forever in NeedRemount.
    async fn remount_all(&mut self) -> StoreResult<()> {
        if !self.remount_state.try_start() {
            return Ok(());
        }
        let result = async {
            let mut segment_names = self
                .owned_store_segments
                .iter()
                .map(|segment| segment.segment_name.clone())
                .collect::<Vec<_>>();
            let mut segment_sizes = self
                .owned_store_segments
                .iter()
                .map(|segment| segment.size)
                .collect::<Vec<_>>();
            let mut base_addrs = self
                .owned_store_segments
                .iter()
                .map(super::OwnedStoreSegment::base_addr)
                .collect::<Vec<_>>();
            let mut te_endpoints =
                vec![self.local_transport_endpoint.clone(); self.owned_store_segments.len()];
            let mut protocols = vec![self.protocol.clone(); self.owned_store_segments.len()];
            let mut host_ids = vec![self.host_id.clone(); self.owned_store_segments.len()];
            let mut segment_ids = self
                .owned_store_segments
                .iter()
                .map(|segment| Self::uuid_to_proto_uuid(segment.segment_id))
                .collect::<Vec<_>>();
            let mut external_segments = self
                .mounted_external_segments
                .read()
                .values()
                .cloned()
                .collect::<Vec<_>>();
            external_segments.sort_by_key(|segment| segment.segment_id);
            for segment in external_segments {
                segment_names.push(segment.segment_name);
                segment_sizes.push(segment.size);
                base_addrs.push(segment.base_addr);
                te_endpoints.push(segment.te_endpoint);
                protocols.push(segment.protocol);
                host_ids.push(segment.host_id);
                segment_ids.push(Self::uuid_to_proto_uuid(segment.segment_id));
            }
            if let Some(registration) = &self.cxl_segment_registration {
                let segment_id = self.cxl_segment_id.ok_or_else(|| {
                    StoreError::Internal("CXL registration is missing segment UUID".to_string())
                })?;
                segment_names.push(self.local_hostname.clone());
                segment_sizes.push(registration.len() as u64);
                base_addrs.push(registration.base_addr());
                te_endpoints.push(self.local_hostname.clone());
                protocols.push("cxl".to_string());
                host_ids.push(self.host_id.clone());
                segment_ids.push(Self::uuid_to_proto_uuid(segment_id));
            }
            // A compute-only or NoF-only client sends empty vectors but still
            // completes the Master session handshake.
            let request = proto::ReMountSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segment_names,
                segment_sizes,
                base_addrs,
                te_endpoints,
                protocols,
                segment_ids,
                host_ids,
            };
            self.master
                .re_mount_segment(self.rpc_request(request))
                .await
                .map_err(Self::rpc_status_to_error)?;

            let nof_segments = self
                .mounted_nof_segments
                .read()
                .values()
                .cloned()
                .collect::<Vec<_>>();
            if !nof_segments.is_empty() {
                self.remount_nof_segments(&nof_segments).await?;
            }

            if let Some(enable_offloading) = self.local_disk_mount_state.desired_enable_offloading()
            {
                self.mount_local_disk_segment(enable_offloading).await?;
            }
            Ok(())
        }
        .await;
        self.remount_state.finish();
        result
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
        // Preserve the historical builder behavior: an explicitly attached
        // cache admits on the first successful miss.
        self.hot_cache_admission = Some(HotCacheAdmission::new(1));
        self
    }

    pub fn is_hot_cache_enabled(&self) -> bool {
        self.hot_cache.is_some()
    }

    pub fn local_hot_cache_block_count(&self) -> usize {
        self.hot_cache
            .as_ref()
            .map_or(0, |cache| cache.block_count())
    }

    pub fn hot_cache_admission_count(&self, key: &str) -> u8 {
        let cache_key = super::read::scoped_cache_key(&self.tenant_id, key);
        self.hot_cache_admission
            .as_ref()
            .map_or(0, |admission| admission.count(cache_key.as_ref()))
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

    /// Attach the C++-default bucket SSD backend for offload and promotion.
    pub fn with_bucket_storage_backend(mut self, backend: Arc<BucketStorageBackend>) -> Self {
        self.local_storage = Some(AttachedLocalStorage::Bucket(backend));
        self
    }

    /// Attach the Distributed/HF3FS backend to the normal offload, promotion
    /// and persistent LocalDisk recovery path.
    pub fn with_distributed_storage_backend(
        mut self,
        backend: Arc<DistributedStorageBackend>,
    ) -> Self {
        self.local_storage = Some(AttachedLocalStorage::Distributed(backend));
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

        let pool =
            crate::offload::buffer::OffloadBufferPool::from_environment(self.local_buffer.len())
                .map_err(StoreError::InvalidParams)?;
        let handler = crate::offload::server::OffloadReadHandler {
            storage: storage.clone(),
            engine: self.engine.required_arc()?,
            pool,
            te_endpoint: self.local_hostname.clone(),
        };

        let (port, handle) = crate::offload::server::start_offload_server(handler)
            .await
            .map_err(StoreError::Internal)?;
        // Build the RPC address: hostname (without port) + offload port.
        let addr = if let Some(pos) = self.local_hostname.rfind(':') {
            format!("{}:{port}", &self.local_hostname[..pos])
        } else {
            format!("{}:{port}", self.local_hostname)
        };
        self.offload_server_state.record_started(handle, port, addr);

        Ok(port)
    }

    /// Returns the P2P offload RPC address (`hostname:port`) if the server is running.
    /// C++ equivalent: `RealClient::local_rpc_addr`
    pub fn offload_rpc_address(&self) -> String {
        self.offload_server_state.address()
    }

    /// Returns `true` if the last ping to the master was successful.
    /// This is a cheap, non-blocking call suitable for polling loops.
    ///
    /// C++ equivalent: `Client::is_ping_healthy()`
    ///
    /// 如果最后一次向 master 的 ping 成功则返回 true。
    /// 这是一个轻量的、非阻塞的调用，适合轮询循环。
    pub fn is_ping_healthy(&self) -> bool {
        self.health_state.is_healthy()
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
    pub async fn remount_segment(&mut self) -> StoreResult<()> {
        self.remount_all().await
    }
}

use super::MooncakeClient;
use super::buffer::OwnedBuffer;
use super::config::ClientConfig;
use super::http::{ClientHttpConfig, ClientHttpServerState, ClientHttpSnapshot};
use crate::proto;
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use transfer_engine_ffi::TransferEngine;
use uuid::Uuid;

const MIN_SEGMENT_SIZE: u64 = 1024;
const MAX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

fn release_failed_store_segment(
    engine: &TransferEngine,
    segment_name: &str,
    mut buffer: crate::memory_ffi::OwnedSegmentBuffer,
) {
    let _ = engine.remove_local_segment(segment_name);
    let unregistered =
        unsafe { engine.unregister_local_memory(buffer.as_ptr() as *mut std::ffi::c_void) };
    if let Err(error) = unregistered {
        tracing::error!(
            %error,
            "leaking partially created Store segment because Transfer Engine unregister failed"
        );
        std::mem::forget(buffer);
    } else if !buffer.release() {
        tracing::error!(
            "leaking partially created Store segment because CUDA host unregister failed"
        );
    }
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
    /// 4. **Install transport** — install the requested protocol and pass a
    ///    device/topology matrix only for protocols that use one.
    ///    安装传输层 —— 安装请求的协议，仅对需要设备/拓扑矩阵的协议传入该参数。
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
        Self::create_with_http_config(
            master_addr,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            ClientHttpConfig::default(),
        )
        .await
    }

    pub async fn create_with_http_config(
        master_addr: &str,
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
        http: ClientHttpConfig,
    ) -> StoreResult<Self> {
        Self::create_with_config(ClientConfig {
            master_addrs: &[master_addr.to_string()],
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            tenant_id: "",
            http,
        })
        .await
    }

    pub async fn create_for_tenant(
        master_addr: &str,
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
        tenant_id: &str,
    ) -> StoreResult<Self> {
        Self::create_with_master_candidates(
            &[master_addr.to_string()],
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
        )
        .await
        .map(|mut client| {
            client.tenant_id = tenant_id.to_string();
            client
        })
    }

    pub async fn create_with_master_candidates_for_tenant(
        master_addrs: &[String],
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
        tenant_id: &str,
    ) -> StoreResult<Self> {
        Self::create_with_config(ClientConfig {
            master_addrs,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            tenant_id,
            http: ClientHttpConfig::default(),
        })
        .await
    }

    /// Create a client by trying multiple master addresses in order.
    ///
    /// This covers the C++ client's HA bootstrap behavior at the store-client
    /// level without coupling this crate to a specific HA backend. Callers that
    /// watch etcd/Redis/K8s leader views can update the candidate set and call
    /// [`switch_master`](Self::switch_master) or [`failover_master`](Self::failover_master).
    pub async fn create_with_master_candidates(
        master_addrs: &[String],
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
    ) -> StoreResult<Self> {
        Self::create_with_config(ClientConfig {
            master_addrs,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            tenant_id: "",
            http: ClientHttpConfig::default(),
        })
        .await
    }

    async fn create_with_config(config: ClientConfig<'_>) -> StoreResult<Self> {
        if config.master_addrs.is_empty() {
            return Err(StoreError::InvalidParams(
                "at least one master address is required".to_string(),
            ));
        }
        Self::validate_global_segment_size(config.global_segment_size)?;
        Self::validate_local_buffer_size(config.local_buffer_size)?;
        Self::validate_client_http_config(config.http)?;

        let mut last_error = None;
        for addr in config.master_addrs {
            match Self::connect_master_addr(addr).await {
                Ok(master) => {
                    return Self::create_with_connected_master(addr, master, &config).await;
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StoreError::Internal("failed to connect to any master candidate".to_string())
        }))
    }

    async fn create_with_connected_master(
        selected_master_addr: &str,
        mut master: proto::master_service_client::MasterServiceClient<Channel>,
        config: &ClientConfig<'_>,
    ) -> StoreResult<Self> {
        let ClientConfig {
            master_addrs,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            tenant_id,
            http,
        } = *config;
        // Step 2: Parse IP and port from local_host. / 从 local_host 解析 IP 和端口。
        let parts: Vec<&str> = local_host.split(':').collect();
        let ip = parts.first().copied().unwrap_or(local_host);
        let port: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
        let effective_protocol =
            Self::effective_transport_protocol(protocol, std::env::var("MC_FORCE_TCP").ok());
        let rpc_request_timeout = Self::rpc_timeout_from_env("MC_RPC_TIMEOUT_MS", 30_000);

        // Step 3: Create TransferEngine. / 创建 TransferEngine。
        let auto_discover = Self::resolve_auto_discover(
            effective_protocol,
            device,
            std::env::var("MC_MS_AUTO_DISC").ok().as_deref(),
        );
        let engine =
            TransferEngine::create(metadata_conn_string, local_host, ip, port, auto_discover)?;

        // Step 4: Install the appropriate transport. / 安装合适的传输层。
        if effective_protocol != "tcp" {
            let topology_matrix = Self::transport_topology_matrix_from_env(
                effective_protocol,
                device,
                std::env::var("MC_MS_FILTERS").ok().as_deref(),
            );
            engine.install_transport(effective_protocol, topology_matrix.as_deref())?;
        } else {
            engine.install_transport("tcp", None)?;
        }

        // Step 5: Discover cluster topology. / 发现集群拓扑。
        engine.discover_topology()?;

        let engine = Arc::new(engine);

        // Step 6: Allocate and register the scratch local_buffer. / 分配并注册 local_buffer。
        let local_buffer_size_usize = usize::try_from(local_buffer_size).map_err(|_| {
            StoreError::InvalidParams("local_buffer_size exceeds addressable memory".to_string())
        })?;
        let mut local_buffer = OwnedBuffer::allocate(local_buffer_size_usize);
        local_buffer
            .populate_before_registration(effective_protocol)
            .map_err(|error| {
                StoreError::Internal(format!(
                    "failed to populate local HugeTLB buffer before registration: {error}"
                ))
            })?;
        crate::memory_ffi::register_local_memory(&engine, &local_buffer, "cpu:0", true)?;

        let client_id = Uuid::new_v4();
        let mut segment_name = String::new();
        let mut segment_size = 0u64;
        let mut mounted_segment_ids = HashMap::new();

        // Step 7: If this node is a storage node (global_segment_size > 0),
        // allocate, register, open, and mount a segment.
        // 如果本节点是存储节点（global_segment_size > 0），分配、注册、打开并挂载 segment。
        let mut segment_buffer: Option<crate::memory_ffi::OwnedSegmentBuffer> = None;
        if global_segment_size > 0 {
            let global_segment_size_usize = usize::try_from(global_segment_size).map_err(|_| {
                StoreError::InvalidParams(
                    "global_segment_size exceeds addressable memory".to_string(),
                )
            })?;
            // Allocate and register segment memory with the TE so that
            // remote nodes can read from / write to this segment via RDMA/TCP.
            //
            // 分配 segment 内存并向 TE 注册，使远端节点可以通过 RDMA/TCP 读写此 segment。
            let seg_buf = crate::memory_ffi::allocate_store_segment(
                global_segment_size_usize,
                effective_protocol,
            )
            .map_err(|error| {
                StoreError::Internal(format!(
                    "failed to populate segment HugeTLB buffer before registration: {error}"
                ))
            })?;
            let base_addr = seg_buf.as_ptr() as u64;
            crate::memory_ffi::register_local_memory(&engine, &seg_buf, "cpu:0", true)?;

            // Create a local TE segment so the transfer engine can discover
            // and resolve this node's segment memory for remote transfers.
            // Without this openSegment, the TE on this node does not know
            // which registered memory backs the segment.
            //
            // 创建本地 TE segment，使传输引擎能够发现并解析此节点的 segment 内存
            // 以进行远程传输。没有此 open_segment，本节点上的 TE 不知道
            // 哪块注册内存支撑此 segment。C++ 等价：`TransferEngine::openSegment(...)`。
            if let Err(error) = engine.open_segment(local_host) {
                release_failed_store_segment(&engine, local_host, seg_buf);
                return Err(error.into());
            }

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
                te_endpoint: local_host.to_string(),
                protocol: effective_protocol.to_string(),
            };
            let mount_response = match master
                .mount_segment(Self::rpc_request_with_timeout(request, rpc_request_timeout))
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    release_failed_store_segment(&engine, local_host, seg_buf);
                    return Err(Self::rpc_status_to_error(status));
                }
            };
            if let Some(id) = mount_response.segment_id {
                mounted_segment_ids
                    .insert(local_host.to_string(), Uuid::from_u64_pair(id.high, id.low));
            }
            segment_buffer = Some(seg_buf);
            segment_name = local_host.to_string();
            segment_size = global_segment_size;
        }

        // Step 8: Register the current node's hostname as a local endpoint.
        // Used by select_best_replica for locality-aware replica selection.
        //
        // 将当前节点的 hostname 注册为本地端点（传输地址），
        // 用于 SelectBestReplica 的本地性优先判断。
        // C++ 等价：Client::GetLocalEndpoints() 返回所有已挂载 segment 的 te_endpoint。
        let mut endpoints = HashSet::new();
        endpoints.insert(local_host.to_string());

        let client = Self {
            master,
            engine,
            accelerator: Arc::new(crate::data_plane_ffi::NativeAcceleratorBackend),
            client_id,
            local_hostname: local_host.to_string(),
            protocol: effective_protocol.to_string(),
            local_buffer,
            segment_buffer,
            registered_buffers: RwLock::new(HashMap::new()),
            shutdown_state: Default::default(),
            local_endpoints: RwLock::new(endpoints),
            mounted_segment_ids: RwLock::new(mounted_segment_ids),
            miss_handler: None,
            hot_cache: None,
            local_storage: None,
            segment_name,
            segment_size,
            remount_state: Default::default(),
            health_state: Default::default(),
            offload_server_state: Default::default(),
            client_http_server_state: ClientHttpServerState::default(),
            master_addr: RwLock::new(selected_master_addr.to_string()),
            master_candidates: RwLock::new(master_addrs.to_vec()),
            rpc_request_timeout,
            tenant_id: tenant_id.to_string(),
            replica_selection_policy: super::ReplicaSelectionPolicy::from_env(),
        };
        client
            .client_http_server_state
            .start(
                http,
                ClientHttpSnapshot::new(
                    client.health_state.shared_flag(),
                    client.shutdown_state.shared_flag(),
                ),
            )
            .await;
        Ok(client)
    }

    pub(super) async fn connect_master_addr(
        master_addr: &str,
    ) -> StoreResult<proto::master_service_client::MasterServiceClient<Channel>> {
        let master_url = Self::normalize_master_url(master_addr)?;
        let mut endpoint =
            Channel::from_shared(master_url).map_err(|e| StoreError::Internal(e.to_string()))?;
        if let Some(timeout) = Self::rpc_timeout_from_env("MC_RPC_CONNECT_TIMEOUT_MS", 30_000) {
            endpoint = endpoint.connect_timeout(timeout);
        }
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(proto::master_service_client::MasterServiceClient::new(
            channel,
        ))
    }

    pub(super) fn normalize_master_url(master_addr: &str) -> StoreResult<String> {
        let trimmed = master_addr.trim();
        if trimmed.is_empty() {
            return Err(StoreError::InvalidParams(
                "master address must not be empty".to_string(),
            ));
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            Ok(trimmed.to_string())
        } else {
            Ok(format!("http://{trimmed}"))
        }
    }

    pub(super) fn rpc_timeout_from_env(name: &str, default_ms: u64) -> Option<Duration> {
        Self::rpc_timeout_from_value(std::env::var(name).ok().as_deref(), default_ms)
    }

    pub(super) fn rpc_timeout_from_value(value: Option<&str>, default_ms: u64) -> Option<Duration> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None => Some(Duration::from_millis(default_ms)),
            Some(value) => match value.parse::<i64>() {
                Ok(ms) if ms < 0 => None,
                Ok(ms) => Some(Duration::from_millis(ms as u64)),
                Err(_) => Some(Duration::from_millis(default_ms)),
            },
        }
    }

    pub(super) fn rpc_request<T>(&self, message: T) -> tonic::Request<T> {
        Self::rpc_request_with_timeout(message, self.rpc_request_timeout)
    }

    pub(super) fn rpc_request_with_timeout<T>(
        message: T,
        timeout: Option<Duration>,
    ) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        if let Some(timeout) = timeout {
            request.set_timeout(timeout);
        }
        request
    }

    pub(super) fn rpc_status_to_error(status: tonic::Status) -> StoreError {
        if status.code() == tonic::Code::DeadlineExceeded {
            StoreError::RpcTimeout(status.to_string())
        } else {
            StoreError::Internal(status.to_string())
        }
    }

    pub(super) fn validate_global_segment_size(global_segment_size: u64) -> StoreResult<()> {
        if global_segment_size == 0 || global_segment_size >= MIN_SEGMENT_SIZE {
            return Ok(());
        }
        Err(StoreError::InvalidParams(format!(
            "global_segment_size must be 0 or at least {MIN_SEGMENT_SIZE}"
        )))
    }

    pub(super) fn validate_local_buffer_size(local_buffer_size: u64) -> StoreResult<()> {
        if local_buffer_size == 0 {
            return Ok(());
        }
        if !(MIN_SEGMENT_SIZE..=MAX_SEGMENT_SIZE).contains(&local_buffer_size) {
            return Err(StoreError::InvalidParams(format!(
                "local_buffer_size must be 0 or between {MIN_SEGMENT_SIZE} and {MAX_SEGMENT_SIZE}"
            )));
        }
        Ok(())
    }

    pub(super) fn validate_client_http_config(config: ClientHttpConfig) -> StoreResult<()> {
        if config.enabled && config.port == 0 {
            return Err(StoreError::InvalidParams(
                "client_http_port must be between 1 and 65535".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn effective_transport_protocol<'a>(
        requested_protocol: &'a str,
        force_tcp_env: Option<String>,
    ) -> &'a str {
        if force_tcp_env.is_some() {
            "tcp"
        } else {
            requested_protocol
        }
    }

    pub(super) fn resolve_auto_discover(
        protocol: &str,
        device: &str,
        env_value: Option<&str>,
    ) -> bool {
        if let Some(value) = env_value {
            match Self::parse_stoi_prefix(value) {
                Ok(1) => return true,
                Ok(0) => return false,
                _ => {}
            }
        }

        matches!(protocol, "rdma" | "efa") && device.trim().is_empty()
    }

    pub(super) fn parse_stoi_prefix(value: &str) -> Result<i32, std::num::ParseIntError> {
        let trimmed = value.trim_start();
        let mut end = 0usize;
        for (idx, ch) in trimmed.char_indices() {
            if idx == 0 && (ch == '+' || ch == '-') {
                end = ch.len_utf8();
                continue;
            }
            if ch.is_ascii_digit() {
                end = idx + ch.len_utf8();
                continue;
            }
            break;
        }
        trimmed[..end].parse::<i32>()
    }

    pub(super) fn transport_topology_matrix_from_env(
        protocol: &str,
        device: &str,
        ms_filters_env: Option<&str>,
    ) -> Option<String> {
        match protocol {
            "rdma" | "efa" => {
                let explicit = device.trim();
                let filters = if !explicit.is_empty() {
                    Some(explicit.to_string())
                } else {
                    Self::trim_filter_list(ms_filters_env)
                }?;
                let devices: Vec<&str> = filters
                    .split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .collect();
                Some(serde_json::json!({"cpu:0": [devices, []]}).to_string())
            }
            "ub" => {
                let explicit = device.trim();
                if explicit.is_empty() {
                    Some("bonding_dev_0".to_string())
                } else {
                    Some(explicit.to_string())
                }
            }
            _ => None,
        }
    }

    fn trim_filter_list(value: Option<&str>) -> Option<String> {
        let filters: Vec<&str> = value?
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .collect();
        if filters.is_empty() {
            None
        } else {
            Some(filters.join(","))
        }
    }
}

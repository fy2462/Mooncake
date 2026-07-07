use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tonic::transport::Channel;
use transfer_engine_ffi::TransferEngine;
use uuid::Uuid;

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
        Self::create_for_tenant(
            master_addr,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            "",
        )
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
        Self::create_with_master_candidates_inner(
            master_addrs,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            tenant_id,
        )
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
        Self::create_with_master_candidates_inner(
            master_addrs,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            "",
        )
        .await
    }

    async fn create_with_master_candidates_inner(
        master_addrs: &[String],
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
        tenant_id: &str,
    ) -> StoreResult<Self> {
        if master_addrs.is_empty() {
            return Err(StoreError::InvalidParams(
                "at least one master address is required".to_string(),
            ));
        }

        let mut last_error = None;
        for addr in master_addrs {
            match Self::connect_master_addr(addr).await {
                Ok(master) => {
                    return Self::create_with_connected_master(
                        addr,
                        master_addrs,
                        master,
                        metadata_conn_string,
                        local_host,
                        protocol,
                        device,
                        global_segment_size,
                        local_buffer_size,
                        tenant_id,
                    )
                    .await;
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
        master_candidates: &[String],
        mut master: proto::master_service_client::MasterServiceClient<Channel>,
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
        tenant_id: &str,
    ) -> StoreResult<Self> {
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
        let mut segment_name = String::new();
        let mut segment_size = 0u64;
        let mut mounted_segment_ids = HashMap::new();

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
            segment_name = local_host.to_string();
            segment_size = global_segment_size;

            // Notify master about this segment so peers can discover it.
            // 通知 master 此 segment，使对等节点可以发现它。
            let request = proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: segment_name.clone(),
                size: global_segment_size,
                base_addr,
                te_endpoint: local_host.to_string(),
                protocol: protocol.to_string(),
            };
            let mount_response = master
                .mount_segment(request)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?
                .into_inner();
            if let Some(id) = mount_response.segment_id {
                mounted_segment_ids
                    .insert(segment_name.clone(), Uuid::from_u64_pair(id.high, id.low));
            }
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
            protocol: protocol.to_string(),
            local_buffer,
            segment_buffer,
            registered_buffers: RwLock::new(HashMap::new()),
            tear_down: Arc::new(RwLock::new(false)),
            local_endpoints: RwLock::new(endpoints),
            mounted_segment_ids: RwLock::new(mounted_segment_ids),
            miss_handler: None,
            hot_cache: None,
            local_storage: None,
            segment_name,
            segment_size,
            remount_in_progress: Arc::new(AtomicBool::new(false)),
            last_ping_success: Arc::new(AtomicBool::new(false)),
            offload_server_handle: RwLock::new(None),
            offload_server_port: Arc::new(std::sync::atomic::AtomicU16::new(0)),
            offload_rpc_addr: RwLock::new(String::new()),
            master_addr: RwLock::new(selected_master_addr.to_string()),
            master_candidates: RwLock::new(master_candidates.to_vec()),
            tenant_id: tenant_id.to_string(),
        })
    }

    pub(super) async fn connect_master_addr(
        master_addr: &str,
    ) -> StoreResult<proto::master_service_client::MasterServiceClient<Channel>> {
        let master_url = Self::normalize_master_url(master_addr)?;
        let channel = Channel::from_shared(master_url)
            .map_err(|e| StoreError::Internal(e.to_string()))?
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
}

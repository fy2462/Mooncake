use super::buffer::OwnedBuffer;
use super::config::ClientConfig;
use super::endpoint::ResolvedClientEndpoint;
use super::http::{ClientHttpConfig, ClientHttpServerState, ClientHttpSnapshot};
use super::metrics::{ClientMetrics, MetricsChannel};
use super::{MooncakeClient, OwnedStoreSegment};
use crate::hot_cache::{HotCacheAdmission, LocalHotCacheSettings};
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
const DEFAULT_MAX_MR_SIZE: u64 = 0x10000000000;

pub(super) fn release_failed_store_segment(
    engine: &TransferEngine,
    mut buffer: crate::memory_ffi::OwnedSegmentBuffer,
) {
    let unregistered = crate::memory_ffi::unregister_local_memory(engine, &buffer);
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

pub(super) fn release_failed_store_segments(
    engine: &TransferEngine,
    segment_name: &str,
    buffers: Vec<crate::memory_ffi::OwnedSegmentBuffer>,
) {
    let _ = engine.remove_local_segment(segment_name);
    for buffer in buffers {
        release_failed_store_segment(engine, buffer);
    }
}

fn release_failed_cxl_segment(
    engine: &TransferEngine,
    registration: crate::memory_ffi::CxlSegmentRegistration,
) {
    if let Err(error) = crate::memory_ffi::unregister_cxl_segment(engine, &registration) {
        tracing::error!(
            %error,
            "leaking partially mounted CXL segment because native unregister failed"
        );
        std::mem::forget(registration);
    }
}

pub(super) fn unmount_confirms_segment_absent<T>(result: Result<T, tonic::Status>) -> bool {
    match result {
        Ok(_) => true,
        Err(status) => status.code() == tonic::Code::NotFound,
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
        Self::create_with_http_config_for_tenant(
            master_addr,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            "",
            http,
        )
        .await
    }

    /// Create a client with one authoritative default tenant and optional
    /// client-owned HTTP health/metrics server.
    ///
    /// Keeping both values in the initial [`ClientConfig`] avoids constructing
    /// a default-tenant client and mutating its identity after lifecycle
    /// initialization.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_with_http_config_for_tenant(
        master_addr: &str,
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
        tenant_id: &str,
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
            tenant_id,
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
        Self::create_with_http_config_for_tenant(
            master_addr,
            metadata_conn_string,
            local_host,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            tenant_id,
            ClientHttpConfig::default(),
        )
        .await
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
        let endpoint = ResolvedClientEndpoint::from_environment(config.local_host)?;

        let metrics = ClientMetrics::from_env()?;
        let mut last_error = None;
        for addr in config.master_addrs {
            match Self::connect_master_addr(addr, metrics.clone()).await {
                Ok(master) => {
                    return Self::create_with_connected_master(
                        addr, master, &config, metrics, endpoint,
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
        mut master: proto::master_service_client::MasterServiceClient<MetricsChannel>,
        config: &ClientConfig<'_>,
        metrics: Option<Arc<ClientMetrics>>,
        endpoint: ResolvedClientEndpoint,
    ) -> StoreResult<Self> {
        let ClientConfig {
            master_addrs,
            metadata_conn_string,
            local_host: _,
            protocol,
            device,
            global_segment_size,
            local_buffer_size,
            tenant_id,
            http,
        } = *config;
        let ResolvedClientEndpoint {
            server_name,
            host,
            port,
            reservation,
        } = endpoint;
        let local_host = server_name.as_str();
        let ip = host.as_str();
        let effective_protocol =
            Self::effective_transport_protocol(protocol, std::env::var("MC_FORCE_TCP").ok());
        let host_id = mooncake_store_core::resolve_host_id(local_host);
        let rpc_request_timeout = Self::rpc_timeout_from_env("MC_RPC_TIMEOUT_MS", 30_000);
        let storage_config = master
            .get_storage_config(Self::rpc_request_with_timeout(
                proto::GetStorageConfigRequest {},
                rpc_request_timeout,
            ))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        let memory_segment_alignment = Self::validate_memory_segment_alignment(
            &storage_config.memory_allocator,
            storage_config.memory_segment_alignment,
        )?;
        let global_disk = if storage_config.fs_dir.is_empty() {
            None
        } else {
            Some(Arc::new(super::global_disk::GlobalDiskStorage::new(
                &storage_config.fs_dir,
                storage_config.enable_disk_eviction,
                storage_config.quota_bytes,
                storage_config.enable_tenant_scope,
            )?))
        };
        let uses_transfer_engine = Self::uses_transfer_engine(effective_protocol);
        if !uses_transfer_engine && global_segment_size > 0 {
            return Err(StoreError::InvalidParams(
                "rpc_only clients cannot mount a data-plane segment".to_string(),
            ));
        }
        if uses_transfer_engine
            && Self::tent_mode_requested(
                std::env::var_os("MC_USE_TENT").is_some(),
                std::env::var_os("MC_USE_TEV1").is_some(),
            )
        {
            return Err(StoreError::InvalidParams(
                "TENT-backed Store clients are outside classic Store parity and require a separately versioned Transfer Engine capability ABI"
                    .to_string(),
            ));
        }

        // Steps 3-5: rpc_only is a metadata/control-plane client and must not
        // create, configure, or discover a native Transfer Engine.
        let engine = if uses_transfer_engine {
            let auto_discover = Self::resolve_auto_discover(
                effective_protocol,
                device,
                std::env::var("MC_MS_AUTO_DISC").ok().as_deref(),
            );
            let engine = TransferEngine::create(
                metadata_conn_string,
                local_host,
                ip,
                u64::from(port),
                auto_discover,
            )?;

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
            engine.discover_topology()?;
            Some(Arc::new(engine))
        } else {
            None
        };

        // Step 6: Allocate and register the scratch local_buffer. / 分配并注册 local_buffer。
        let local_buffer_size_usize = usize::try_from(local_buffer_size).map_err(|_| {
            StoreError::InvalidParams("local_buffer_size exceeds addressable memory".to_string())
        })?;
        let mut local_buffer = OwnedBuffer::allocate_for_registration(local_buffer_size_usize, 1)
            .map_err(|error| {
            StoreError::Internal(format!(
                "failed to allocate registered local buffer: {error}"
            ))
        })?;
        local_buffer
            .populate_before_registration(effective_protocol)
            .map_err(|error| {
                StoreError::Internal(format!(
                    "failed to populate local HugeTLB buffer before registration: {error}"
                ))
            })?;
        let local_buffer = if let Some(engine) = engine.as_ref() {
            let registration = crate::memory_ffi::register_owned_local_memory(
                engine,
                local_buffer,
                "cpu:0",
                true,
            )?;
            super::staging::StagingBuffer::registered(registration)
        } else {
            super::staging::StagingBuffer::rpc_only(local_buffer)
        };

        let client_id = Uuid::new_v4();
        let mut mounted_segment_ids = HashMap::new();

        // Step 7: Split total Store capacity into independently registered
        // MRs. Chunks intentionally share the local hostname, matching C++;
        // UUID and base address are their authoritative identities.
        let mut owned_store_segments: Vec<OwnedStoreSegment> = Vec::new();
        let mut cxl_segment_registration = None;
        let mut cxl_segment_id = None;
        if effective_protocol == "cxl" {
            let engine = engine
                .as_deref()
                .expect("CXL protocol requires Transfer Engine");
            let cxl_size =
                Self::cxl_device_size_from_value(std::env::var("MC_CXL_DEV_SIZE").ok().as_deref())?;
            let cxl_size_usize = usize::try_from(cxl_size).map_err(|_| {
                StoreError::InvalidParams("MC_CXL_DEV_SIZE exceeds addressable memory".to_string())
            })?;
            let registration = crate::memory_ffi::register_cxl_segment(engine, cxl_size_usize)?;
            let expected_segment_id = mooncake_store_core::stable_memory_segment_id(
                client_id,
                local_host,
                registration.base_addr(),
                cxl_size,
                local_host,
                "cxl",
                &host_id,
            );
            let request = proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: local_host.to_string(),
                size: cxl_size,
                base_addr: registration.base_addr(),
                te_endpoint: local_host.to_string(),
                protocol: "cxl".to_string(),
                host_id: host_id.clone(),
            };
            let mount_response = match master
                .mount_segment(Self::rpc_request_with_timeout(request, rpc_request_timeout))
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let unmounted = unmount_confirms_segment_absent(
                        master
                            .unmount_segment(Self::rpc_request_with_timeout(
                                proto::UnmountSegmentRequest {
                                    segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                                    client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                                },
                                rpc_request_timeout,
                            ))
                            .await,
                    );
                    if unmounted {
                        release_failed_cxl_segment(engine, registration);
                    } else {
                        tracing::error!(
                            segment_id = %expected_segment_id,
                            "leaking CXL registration because ambiguous mount could not be unmounted"
                        );
                        std::mem::forget(registration);
                    }
                    return Err(Self::rpc_status_to_error(status));
                }
            };
            let Some(id) = mount_response.segment_id else {
                let unmounted = unmount_confirms_segment_absent(
                    master
                        .unmount_segment(Self::rpc_request_with_timeout(
                            proto::UnmountSegmentRequest {
                                segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                                client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                            },
                            rpc_request_timeout,
                        ))
                        .await,
                );
                if unmounted {
                    release_failed_cxl_segment(engine, registration);
                } else {
                    tracing::error!(
                        segment_id = %expected_segment_id,
                        "leaking CXL registration because UUID-less mount could not be unmounted"
                    );
                    std::mem::forget(registration);
                }
                return Err(StoreError::Internal(
                    "CXL MountSegment response missing segment_id".to_string(),
                ));
            };
            let id = Uuid::from_u64_pair(id.high, id.low);
            if id != expected_segment_id {
                let returned_unmounted = unmount_confirms_segment_absent(
                    master
                        .unmount_segment(Self::rpc_request_with_timeout(
                            proto::UnmountSegmentRequest {
                                segment_id: Some(Self::uuid_to_proto_uuid(id)),
                                client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                            },
                            rpc_request_timeout,
                        ))
                        .await,
                );
                let expected_unmounted = unmount_confirms_segment_absent(
                    master
                        .unmount_segment(Self::rpc_request_with_timeout(
                            proto::UnmountSegmentRequest {
                                segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                                client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                            },
                            rpc_request_timeout,
                        ))
                        .await,
                );
                if returned_unmounted && expected_unmounted {
                    release_failed_cxl_segment(engine, registration);
                } else {
                    tracing::error!(
                        returned_segment_id = %id,
                        expected_segment_id = %expected_segment_id,
                        "leaking CXL registration after Master returned a non-canonical UUID"
                    );
                    std::mem::forget(registration);
                }
                return Err(StoreError::Internal(
                    "CXL MountSegment returned a non-canonical segment UUID".to_string(),
                ));
            }
            mounted_segment_ids.insert(id, local_host.to_string());
            cxl_segment_id = Some(id);
            cxl_segment_registration = Some(registration);
        } else if global_segment_size > 0 {
            let engine = engine
                .as_deref()
                .expect("data-plane segment validation requires Transfer Engine");
            let max_mr_size = Self::resolve_max_mr_size(
                effective_protocol,
                global_segment_size,
                std::env::var("MC_MAX_MR_SIZE").ok().as_deref(),
            )?;
            let mut prepared = Vec::new();
            for chunk_size in Self::split_segment_capacity_aligned(
                global_segment_size,
                max_mr_size,
                memory_segment_alignment as u64,
            )? {
                let chunk_size = usize::try_from(chunk_size).map_err(|_| {
                    StoreError::InvalidParams(
                        "Store segment chunk exceeds addressable memory".to_string(),
                    )
                })?;
                let seg_buf = crate::memory_ffi::allocate_store_segment(
                    chunk_size,
                    effective_protocol,
                    memory_segment_alignment,
                )
                .map_err(|error| {
                    StoreError::Internal(format!(
                        "failed to populate segment buffer before registration: {error}"
                    ))
                })?;
                if let Err(error) =
                    crate::memory_ffi::register_local_memory(engine, &seg_buf, "cpu:0", true)
                {
                    release_failed_store_segments(engine, local_host, prepared);
                    return Err(error);
                }
                prepared.push(seg_buf);
            }
            if let Err(error) = engine.open_segment(local_host) {
                release_failed_store_segments(engine, local_host, prepared);
                return Err(error.into());
            }

            let mut prepared = prepared.into_iter();
            while let Some(seg_buf) = prepared.next() {
                let size = seg_buf.len() as u64;
                let expected_segment_id = mooncake_store_core::stable_memory_segment_id(
                    client_id,
                    local_host,
                    seg_buf.as_ptr() as u64,
                    size,
                    local_host,
                    effective_protocol,
                    &host_id,
                );
                let request = proto::MountSegmentRequest {
                    client_id: Some(proto::Uuid {
                        high: client_id.as_u64_pair().0,
                        low: client_id.as_u64_pair().1,
                    }),
                    segment_name: local_host.to_string(),
                    size,
                    base_addr: seg_buf.as_ptr() as u64,
                    te_endpoint: local_host.to_string(),
                    protocol: effective_protocol.to_string(),
                    host_id: host_id.clone(),
                };
                let mount_response = match master
                    .mount_segment(Self::rpc_request_with_timeout(request, rpc_request_timeout))
                    .await
                {
                    Ok(response) => response.into_inner(),
                    Err(status) => {
                        let current_unmounted = unmount_confirms_segment_absent(
                            master
                                .unmount_segment(Self::rpc_request_with_timeout(
                                    proto::UnmountSegmentRequest {
                                        segment_id: Some(Self::uuid_to_proto_uuid(
                                            expected_segment_id,
                                        )),
                                        client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                                    },
                                    rpc_request_timeout,
                                ))
                                .await,
                        );
                        let mut endpoint_safe_to_remove = current_unmounted;
                        for mounted in owned_store_segments.drain(..) {
                            let unmounted = unmount_confirms_segment_absent(
                                master
                                    .unmount_segment(Self::rpc_request_with_timeout(
                                        proto::UnmountSegmentRequest {
                                            segment_id: Some(Self::uuid_to_proto_uuid(
                                                mounted.segment_id,
                                            )),
                                            client_id: Some(proto::Uuid {
                                                high: client_id.as_u64_pair().0,
                                                low: client_id.as_u64_pair().1,
                                            }),
                                        },
                                        rpc_request_timeout,
                                    ))
                                    .await,
                            );
                            if unmounted {
                                release_failed_store_segment(engine, mounted.buffer);
                            } else {
                                endpoint_safe_to_remove = false;
                                tracing::error!(
                                    segment_id = %mounted.segment_id,
                                    "leaking Store segment because startup rollback unmount failed"
                                );
                                std::mem::forget(mounted);
                            }
                        }
                        let mut buffers = Vec::new();
                        if current_unmounted {
                            buffers.push(seg_buf);
                        } else {
                            tracing::error!(
                                segment_id = %expected_segment_id,
                                "leaking Store segment because ambiguous startup mount could not be unmounted"
                            );
                            std::mem::forget(seg_buf);
                        }
                        buffers.extend(prepared);
                        if endpoint_safe_to_remove {
                            release_failed_store_segments(engine, local_host, buffers);
                        } else {
                            for buffer in buffers {
                                release_failed_store_segment(engine, buffer);
                            }
                        }
                        return Err(Self::rpc_status_to_error(status));
                    }
                };
                let Some(id) = mount_response.segment_id else {
                    let current_unmounted = unmount_confirms_segment_absent(
                        master
                            .unmount_segment(Self::rpc_request_with_timeout(
                                proto::UnmountSegmentRequest {
                                    segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                                    client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                                },
                                rpc_request_timeout,
                            ))
                            .await,
                    );
                    if current_unmounted {
                        release_failed_store_segment(engine, seg_buf);
                    } else {
                        tracing::error!(
                            segment_id = %expected_segment_id,
                            "leaking Store segment because UUID-less mount could not be unmounted"
                        );
                        std::mem::forget(seg_buf);
                    }
                    for buffer in prepared {
                        release_failed_store_segment(engine, buffer);
                    }
                    for mounted in owned_store_segments {
                        std::mem::forget(mounted);
                    }
                    return Err(StoreError::Internal(
                        "MountSegment response missing segment_id".to_string(),
                    ));
                };
                let segment_id = Uuid::from_u64_pair(id.high, id.low);
                if segment_id != expected_segment_id {
                    let returned_unmounted = unmount_confirms_segment_absent(
                        master
                            .unmount_segment(Self::rpc_request_with_timeout(
                                proto::UnmountSegmentRequest {
                                    segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
                                    client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                                },
                                rpc_request_timeout,
                            ))
                            .await,
                    );
                    let expected_unmounted = unmount_confirms_segment_absent(
                        master
                            .unmount_segment(Self::rpc_request_with_timeout(
                                proto::UnmountSegmentRequest {
                                    segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                                    client_id: Some(Self::uuid_to_proto_uuid(client_id)),
                                },
                                rpc_request_timeout,
                            ))
                            .await,
                    );
                    if returned_unmounted && expected_unmounted {
                        release_failed_store_segment(engine, seg_buf);
                    } else {
                        tracing::error!(
                            returned_segment_id = %segment_id,
                            expected_segment_id = %expected_segment_id,
                            "leaking Store segment after Master returned a non-canonical UUID"
                        );
                        std::mem::forget(seg_buf);
                    }
                    for buffer in prepared {
                        release_failed_store_segment(engine, buffer);
                    }
                    for mounted in owned_store_segments {
                        std::mem::forget(mounted);
                    }
                    return Err(StoreError::Internal(
                        "MountSegment returned a non-canonical segment UUID".to_string(),
                    ));
                }
                mounted_segment_ids.insert(segment_id, local_host.to_string());
                owned_store_segments.push(OwnedStoreSegment {
                    segment_id,
                    segment_name: local_host.to_string(),
                    size,
                    buffer: seg_buf,
                });
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

        let hot_cache_settings =
            LocalHotCacheSettings::from_environment().map_err(StoreError::InvalidParams)?;
        let (hot_cache, hot_cache_admission) = match hot_cache_settings {
            Some(settings) => {
                tracing::info!(
                    total_size = settings.total_size,
                    block_size = settings.block_size,
                    max_entries = settings.max_entries,
                    admission_threshold = settings.admission_threshold,
                    "local hot cache enabled"
                );
                (
                    Some(Arc::new(crate::LocalHotCache::new_with_block_size(
                        settings.total_size,
                        settings.block_size,
                        settings.max_entries,
                    ))),
                    Some(HotCacheAdmission::new(settings.admission_threshold)),
                )
            }
            None => (None, None),
        };

        let client = Self {
            master,
            engine: engine.map_or_else(
                super::engine::ClientTransferEngine::disabled,
                super::engine::ClientTransferEngine::enabled,
            ),
            _auto_port_reservation: reservation,
            accelerator: Arc::new(crate::data_plane_ffi::NativeAcceleratorBackend),
            client_id,
            local_hostname: local_host.to_string(),
            host_id,
            protocol: effective_protocol.to_string(),
            memory_segment_alignment,
            enable_tenant_scope: storage_config.enable_tenant_scope,
            local_buffer,
            owned_store_segments,
            cxl_segment_registration,
            cxl_segment_id,
            registered_buffers: RwLock::new(HashMap::new()),
            shutdown_state: Default::default(),
            local_endpoints: RwLock::new(endpoints),
            mounted_segment_ids: RwLock::new(mounted_segment_ids),
            mounted_external_segments: RwLock::new(HashMap::new()),
            mounted_nof_segments: RwLock::new(HashMap::new()),
            miss_handler: None,
            hot_cache,
            hot_cache_admission,
            local_storage: None,
            global_disk,
            remount_state: Default::default(),
            local_disk_mount_state: Default::default(),
            health_state: Default::default(),
            offload_server_state: Default::default(),
            client_http_server_state: ClientHttpServerState::default(),
            metrics,
            metrics_reporter_state: Default::default(),
            master_addr: RwLock::new(selected_master_addr.to_string()),
            master_candidates: RwLock::new(master_addrs.to_vec()),
            rpc_request_timeout,
            tenant_id: tenant_id.to_string(),
            pending_legacy_offload_tasks: HashMap::new(),
            replica_selection_policy: super::ReplicaSelectionPolicy::from_env(),
        };
        client.metrics_reporter_state.start(client.metrics.clone());
        client
            .client_http_server_state
            .start(
                http,
                ClientHttpSnapshot::new(
                    client.health_state.shared_flag(),
                    client.shutdown_state.shared_flag(),
                    client.metrics.clone(),
                ),
            )
            .await;
        Ok(client)
    }

    pub(super) async fn connect_master_addr(
        master_addr: &str,
        metrics: Option<Arc<ClientMetrics>>,
    ) -> StoreResult<proto::master_service_client::MasterServiceClient<MetricsChannel>> {
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
            MetricsChannel::new(channel, metrics),
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

    pub(super) fn cxl_device_size_from_value(value: Option<&str>) -> StoreResult<u64> {
        let value = value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                StoreError::InvalidParams("MC_CXL_DEV_SIZE must be set for CXL".to_string())
            })?;
        match value.parse::<u64>() {
            Ok(size) if size > 0 => Ok(size),
            _ => Err(StoreError::InvalidParams(
                "MC_CXL_DEV_SIZE must be a positive integer".to_string(),
            )),
        }
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
        match status.code() {
            tonic::Code::DeadlineExceeded => StoreError::RpcTimeout(status.to_string()),
            tonic::Code::Cancelled if status.message() == "Timeout expired" => {
                StoreError::RpcTimeout(status.to_string())
            }
            tonic::Code::NotFound => StoreError::KeyNotFound(status.message().to_string()),
            tonic::Code::AlreadyExists => StoreError::ObjectExists(status.message().to_string()),
            tonic::Code::Unavailable => StoreError::ServiceUnavailable,
            _ => StoreError::Internal(status.to_string()),
        }
    }

    pub(super) fn resolve_max_mr_size(
        protocol: &str,
        total_size: u64,
        configured: Option<&str>,
    ) -> StoreResult<u64> {
        if total_size == 0 || protocol == "cxl" {
            return Ok(total_size.max(1));
        }
        if let Some(value) = configured {
            let max_mr_size = value.trim().parse::<u64>().map_err(|_| {
                StoreError::InvalidParams("MC_MAX_MR_SIZE must be a positive integer".to_string())
            })?;
            if max_mr_size == 0 {
                return Err(StoreError::InvalidParams(
                    "MC_MAX_MR_SIZE must be greater than zero".to_string(),
                ));
            }
            if usize::try_from(max_mr_size).is_err() {
                return Err(StoreError::InvalidParams(
                    "MC_MAX_MR_SIZE exceeds addressable memory".to_string(),
                ));
            }
            return Ok(max_mr_size);
        }

        // Native C++ reads the post-install device clamp from globalConfig().
        // The existing safe FFI intentionally exposes neither that mutable
        // global nor NIC topology. Requiring an explicit cap for transports
        // with device MR limits prevents Rust from registering a silently
        // truncated range and advertising inaccessible memory to Master.
        if matches!(protocol, "rdma" | "efa" | "cxi") {
            return Err(StoreError::InvalidParams(format!(
                "MC_MAX_MR_SIZE is required for {protocol} Store segments because the existing \
                 Transfer Engine FFI does not expose the device-clamped MR limit"
            )));
        }
        Ok(DEFAULT_MAX_MR_SIZE)
    }

    pub(super) fn split_segment_capacity(
        total_size: u64,
        max_mr_size: u64,
    ) -> StoreResult<Vec<u64>> {
        if total_size == 0 {
            return Ok(Vec::new());
        }
        if max_mr_size == 0 {
            return Err(StoreError::InvalidParams(
                "max MR size must be greater than zero".to_string(),
            ));
        }
        let mut remaining = total_size;
        let mut chunks = Vec::new();
        while remaining > 0 {
            let chunk = remaining.min(max_mr_size);
            chunks.push(chunk);
            remaining -= chunk;
        }
        Ok(chunks)
    }

    pub(super) fn split_segment_capacity_aligned(
        total_size: u64,
        max_mr_size: u64,
        alignment: u64,
    ) -> StoreResult<Vec<u64>> {
        if alignment == 0 || !alignment.is_power_of_two() {
            return Err(StoreError::InvalidParams(
                "Memory segment alignment must be a non-zero power of two".to_string(),
            ));
        }
        if total_size % alignment != 0 {
            return Err(StoreError::InvalidParams(format!(
                "global_segment_size {total_size} must be aligned to Master requirement \
                 {alignment}"
            )));
        }
        let aligned_max = max_mr_size / alignment * alignment;
        if aligned_max == 0 {
            return Err(StoreError::InvalidParams(format!(
                "MC_MAX_MR_SIZE {max_mr_size} is smaller than Memory segment alignment \
                 {alignment}"
            )));
        }
        Self::split_segment_capacity(total_size, aligned_max)
    }

    pub(super) fn validate_memory_segment_alignment(
        allocator: &str,
        alignment: u64,
    ) -> StoreResult<usize> {
        match allocator {
            "offset" if alignment == 1 => Ok(1),
            "cachelib" if alignment > 1 && alignment.is_power_of_two() => {
                usize::try_from(alignment).map_err(|_| {
                    StoreError::InvalidParams(
                        "Master Memory segment alignment exceeds addressable memory".to_string(),
                    )
                })
            }
            _ => Err(StoreError::InvalidParams(format!(
                "invalid Master Memory allocator alignment: allocator={allocator:?}, alignment={alignment}"
            ))),
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

    pub(super) fn uses_transfer_engine(protocol: &str) -> bool {
        protocol != "rpc_only"
    }

    pub(super) const fn tent_mode_requested(
        use_tent_present: bool,
        use_tev1_present: bool,
    ) -> bool {
        use_tent_present || use_tev1_present
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

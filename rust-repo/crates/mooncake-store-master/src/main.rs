//! # Mooncake Master — 入口点 / Entry Point
//!
//! 主函数负责解析 CLI 参数、初始化 tracing、并根据是否启用 HA 模式
//! 选择不同的启动路径：`run_standalone`（单节点）或 `run_ha_loop`（高可用主备循环）。
//!
//! The main function parses CLI arguments, initializes tracing, and chooses
//! between `run_standalone` (single-node) or `run_ha_loop` (HA leader/standby loop).

use clap::Parser;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::admin_http::AdminRuntimeState;
use mooncake_store_master::ha::{
    CatalogBackedSnapshotProvider, HABackendSpec, HABackendType, HaError, LeaderCoordinator,
    LeadershipMonitorHandle, LeadershipSession, MasterServiceSupervisor,
    MasterServiceSupervisorConfig, MasterView, parse_snapshot_object_store_type,
};
use mooncake_store_master::http_metadata::{
    bind_metadata_listener, serve_metadata_listener_with_service_gate,
};
use mooncake_store_master::metrics;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

use mooncake_store_master::main_args::Args;
use mooncake_store_master::main_config::{
    build_ha_spec, build_master_service, build_runtime_config, create_coordinator,
    ensure_supported_rpc_protocol, new_supervisor, parse_snapshot_config,
    preflight_snapshot_pipeline, publish_catalog_snapshot, resolve_cluster_id,
    snapshot_dir_for_cluster, validate_ha_backend_for_serving,
};

mod main_ha;
mod main_server;

#[cfg(not(test))]
const LEADER_OPLOG_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const LEADER_OPLOG_REQUEST_TIMEOUT: Duration = Duration::from_millis(50);
const LEADER_OPLOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// 主入口：初始化日志系统，解析参数，按模式分派。
/// Main entry point: init logging, parse args, dispatch by mode.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    if args.enable_ha {
        main_ha::run_ha_loop(args, shutdown_signal()).await
    } else {
        main_server::run_standalone(args, shutdown_signal()).await
    }
}

fn shutdown_signal() -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_process_shutdown().await;
        info!("Process shutdown signal received");
        let _ = shutdown_tx.send(true);
    });
    shutdown_rx
}

// One writer broadcasts process shutdown to many async wait points. Each
// consumer clones the receiver and waits inside its own tokio::select!.
#[cfg(unix)]
async fn wait_for_process_shutdown() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_process_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn wait_and_continue(
    coordinator: &LeaderCoordinator,
    current_view: &Option<MasterView>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> bool {
    let version = current_view.as_ref().map(|v| v.view_version).unwrap_or(0);
    tokio::select! {
        result = coordinator.wait_for_view_change(version, Duration::from_secs(1)) => {
            match result {
                Ok(Some(v)) => info!(
                    "View changed: leader={}, version={}",
                    v.leader_address, v.view_version
                ),
                Ok(None) => {}
                Err(e) => warn!("View wait error: {}", e),
            }
            false
        }
        changed = shutdown_rx.changed() => {
            changed.is_err() || *shutdown_rx.borrow()
        }
    }
}

async fn warmup_with_renewal(
    coordinator: &LeaderCoordinator,
    session: &LeadershipSession,
    ttl_secs: i64,
    mut shutdown_rx: watch::Receiver<bool>,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(ttl_secs as u64);
    info!("Warmup {}s, renewing each second", ttl_secs);
    while tokio::time::Instant::now() < deadline {
        if coordinator.try_renew_leadership(session).await.is_err() {
            warn!("Warmup renewal failed");
            return false;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    info!("Warmup interrupted by shutdown");
                    return false;
                }
            }
        }
    }
    info!("Warmup complete");
    true
}

async fn start_leadership_monitor(
    coordinator: &LeaderCoordinator,
    session: &LeadershipSession,
    tx: tokio::sync::watch::Sender<bool>,
    service: Arc<MasterServiceImpl>,
) -> Result<LeadershipMonitorHandle, HaError> {
    let mut role_rx = coordinator.subscribe_role_for_session(session)?;
    let h = tokio::spawn(async move {
        loop {
            if role_rx.changed().await.is_err() {
                warn!("LeadershipMonitor: role channel closed, shutting down");
                service.set_service_available(false);
                let _ = tx.send(true);
                break;
            }
            if *role_rx.borrow() == mooncake_store_master::ha::LeaderRole::Standby {
                warn!("LeadershipMonitor: leadership lost, shutting down");
                service.set_service_available(false);
                let _ = tx.send(true);
                break;
            }
        }
    });
    Ok(LeadershipMonitorHandle::new(h))
}

// ---------------------------------------------------------------------------
// Helpers — 工具函数
// ---------------------------------------------------------------------------

async fn build_leader_oplog_manager(
    spec: &HABackendSpec,
    view_version: u64,
) -> Option<mooncake_store_master::oplog::OpLogManager> {
    if spec.backend_type != HABackendType::Etcd || spec.connstring.trim().is_empty() {
        return None;
    }
    let endpoints: Vec<String> = spec
        .connstring
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
        .collect();

    let connect_options = etcd_client::ConnectOptions::new()
        .with_timeout(LEADER_OPLOG_REQUEST_TIMEOUT)
        .with_connect_timeout(LEADER_OPLOG_CONNECT_TIMEOUT);
    let client = match etcd_client::Client::connect(endpoints, Some(connect_options)).await {
        Ok(client) => client,
        Err(e) => {
            warn!("Failed to connect etcd for leader oplog: {}", e);
            return None;
        }
    };
    let oplog_prefix = format!("/oplog/{}", spec.cluster_namespace);
    let election_key = format!(
        "mooncake-store/{}/master_view",
        spec.cluster_namespace.trim_end_matches('/')
    );
    let store = match mooncake_store_master::oplog::EtcdOpLogStore::new_leader(
        client,
        &oplog_prefix,
        election_key,
        view_version,
    )
    .await
    {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to initialize leader oplog store: {}", e);
            return None;
        }
    };
    Some(mooncake_store_master::oplog::OpLogManager::new(
        Some(Box::new(store)),
        view_version,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_master::MasterRuntimeConfig;
    use mooncake_store_master::proto;
    use mooncake_store_master::proto::master_service_server::MasterService;
    use prost::Message;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::codegen::{Body, BoxFuture, Service, StdError, http};
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};
    use uuid::Uuid;

    const STALLED_TRANSACTION_DURATION: Duration = Duration::from_millis(300);
    const MAX_BOUNDED_TRANSACTION_DURATION: Duration = Duration::from_millis(200);
    const MAX_FAIL_FAST_DURATION: Duration = Duration::from_millis(50);

    #[derive(Clone, PartialEq, Message)]
    struct EmptyEtcdMessage {}

    struct ImmediateEmptyResponse;

    impl tonic::server::UnaryService<EmptyEtcdMessage> for ImmediateEmptyResponse {
        type Response = EmptyEtcdMessage;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, _request: Request<EmptyEtcdMessage>) -> Self::Future {
            Box::pin(async { Ok(Response::new(EmptyEtcdMessage {})) })
        }
    }

    struct StalledTransaction {
        calls: Arc<AtomicUsize>,
    }

    impl tonic::server::UnaryService<EmptyEtcdMessage> for StalledTransaction {
        type Response = EmptyEtcdMessage;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, _request: Request<EmptyEtcdMessage>) -> Self::Future {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::AcqRel);
                tokio::time::sleep(STALLED_TRANSACTION_DURATION).await;
                Ok(Response::new(EmptyEtcdMessage {}))
            })
        }
    }

    #[derive(Clone)]
    struct StalledEtcdKv {
        transaction_calls: Arc<AtomicUsize>,
    }

    impl<B> Service<http::Request<B>> for StalledEtcdKv
    where
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            match request.uri().path() {
                "/etcdserverpb.KV/Range" => Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Ok(grpc.unary(ImmediateEmptyResponse, request).await)
                }),
                "/etcdserverpb.KV/Txn" => {
                    let calls = Arc::clone(&self.transaction_calls);
                    Box::pin(async move {
                        let mut grpc =
                            tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                        Ok(grpc.unary(StalledTransaction { calls }, request).await)
                    })
                }
                _ => Box::pin(async move {
                    let mut response = http::Response::new(tonic::body::empty_body());
                    response.headers_mut().insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    Ok(response)
                }),
            }
        }
    }

    impl tonic::server::NamedService for StalledEtcdKv {
        const NAME: &'static str = "etcdserverpb.KV";
    }

    fn proto_uuid(id: Uuid) -> proto::Uuid {
        proto::Uuid {
            high: id.as_u64_pair().0,
            low: id.as_u64_pair().1,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_leader_oplog_transaction_is_bounded_poisoned_and_fences_service() {
        let transaction_calls = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(
            Server::builder()
                .add_service(StalledEtcdKv {
                    transaction_calls: Arc::clone(&transaction_calls),
                })
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let spec = HABackendSpec {
            backend_type: HABackendType::Etcd,
            connstring: format!("http://{address}"),
            cluster_namespace: "stalled-oplog".into(),
            pod_identity: None,
        };
        let manager = build_leader_oplog_manager(&spec, 1)
            .await
            .expect("initialize leader oplog against responsive range endpoint");
        let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
            None,
            None,
            MasterRuntimeConfig::default(),
            Some(manager),
        );
        let client_id = Uuid::new_v4();

        let started = std::time::Instant::now();
        let error = MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: "stalled-oplog:1".into(),
                size: 1024,
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .expect_err("a stalled durable mount must fail closed");
        let first_duration = started.elapsed();

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(
            first_duration < MAX_BOUNDED_TRANSACTION_DURATION,
            "stalled etcd transaction remained in flight for {first_duration:?}"
        );
        assert!(service.is_service_fenced());
        assert!(!service.is_service_available());
        assert_eq!(transaction_calls.load(Ordering::Acquire), 1);

        let fail_fast_started = std::time::Instant::now();
        service
            .oplog_manager()
            .record_remove_durable("default\0must-not-retry")
            .expect_err("a timed-out etcd oplog writer must remain poisoned");
        let fail_fast_duration = fail_fast_started.elapsed();

        assert!(
            fail_fast_duration < MAX_FAIL_FAST_DURATION,
            "poisoned writer did not fail fast: {fail_fast_duration:?}"
        );
        assert_eq!(
            transaction_calls.load(Ordering::Acquire),
            1,
            "poisoned writer issued another etcd transaction"
        );

        server.abort();
    }
}

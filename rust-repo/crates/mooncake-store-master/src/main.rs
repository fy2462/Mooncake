//! # Mooncake Master — 入口点 / Entry Point
//!
//! 主函数负责解析 CLI 参数、初始化 tracing、并根据是否启用 HA 模式
//! 选择不同的启动路径：`run_standalone`（单节点）或 `run_ha_loop`（高可用主备循环）。
//!
//! The main function parses CLI arguments, initializes tracing, and chooses
//! between `run_standalone` (single-node) or `run_ha_loop` (HA leader/standby loop).

use clap::Parser;
use mooncake_store_master::allocator::{AllocationStrategy, MemoryAllocatorKind};
use mooncake_store_master::ha::{
    parse_ha_backend_type, HABackendSpec, HABackendType, HaError, LeaderCoordinator,
    LeadershipMonitorHandle, LeadershipSession, MasterServiceSupervisor,
    MasterServiceSupervisorConfig, MasterView,
};
use mooncake_store_master::http_metadata::serve_metadata_http;
use mooncake_store_master::metrics;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

// CLI 参数定义，使用 clap derive 宏。
// CLI argument definitions via clap derive macro.
#[derive(Parser, Debug)]
#[command(
    name = "mooncake-master",
    version,
    about = "Mooncake distributed KV cache — Master Service"
)]
struct Args {
    /// gRPC 服务监听地址 / gRPC server bind address
    #[arg(long, default_value = "0.0.0.0")]
    rpc_address: String,

    /// gRPC 服务监听端口 / gRPC server port
    #[arg(long, default_value_t = 50051)]
    rpc_port: u16,

    /// HTTP metadata 服务监听地址 / HTTP metadata server bind address
    #[arg(long, default_value = "0.0.0.0")]
    http_metadata_server_host: String,

    /// HTTP metadata 服务监听端口 / HTTP metadata server port
    #[arg(long, default_value_t = 8080)]
    http_metadata_server_port: u16,

    /// Prometheus metrics 暴露端口 / Metrics HTTP server port
    #[arg(long, default_value_t = 9003)]
    metrics_port: u16,

    /// gRPC 服务线程数 / Number of gRPC server threads
    #[arg(long, default_value_t = 4)]
    rpc_thread_num: usize,

    /// Segment 分配策略: "random" 或 "free_ratio_first"
    /// Segment allocation strategy: "random" or "free_ratio_first"
    #[arg(long, default_value = "random")]
    allocation_strategy: String,

    /// Segment 内内存分配器: "offset" 或 "cachelib"
    /// Memory allocator within segment: "offset" or "cachelib"
    #[arg(long, default_value = "offset")]
    memory_allocator: String,

    /// KV 对象默认租约 TTL（毫秒）/ Default KV lease TTL in milliseconds
    #[arg(long, default_value_t = 5000)]
    default_kv_lease_ttl_ms: u64,

    /// 驱逐高水位比例 (0.0~1.0)，超过后触发自动驱逐
    /// Eviction high watermark ratio: auto-eviction triggers above this
    #[arg(long, default_value_t = 0.95)]
    eviction_high_watermark_ratio: f64,

    /// 每次驱逐释放的内存比例 (0.0~1.0)
    /// Fraction of memory to free per eviction cycle
    #[arg(long, default_value_t = 0.05)]
    eviction_ratio: f64,

    /// 驱逐时是否触发 offload（下沉到本地磁盘）
    /// Whether to offload to local disk on eviction
    #[arg(long)]
    offload_on_evict: bool,

    /// 是否强制驱逐（即使对象被 soft_pin 也驱逐）
    /// Force eviction even for soft-pinned objects
    #[arg(long)]
    offload_force_evict: bool,

    /// 是否启用高可用 (HA) 模式 / Enable High Availability (HA) mode
    #[arg(long)]
    enable_ha: bool,

    /// etcd 端点列表，分号分隔（HA 模式） / etcd endpoints, semicolon-separated (HA mode)
    #[arg(long)]
    etcd_endpoints: Option<String>,

    /// HA backend type: "etcd", "redis", or "k8s".
    /// HA 后端类型："etcd"、"redis" 或 "k8s"。
    #[arg(long, default_value = "etcd")]
    ha_backend_type: String,

    /// HA backend connection string. Etcd may fall back to --etcd-endpoints.
    /// For k8s, use "namespace/lease" (or "lease" for the default namespace).
    /// HA 后端连接串。etcd 可回退到 --etcd-endpoints。
    /// k8s 使用 "namespace/lease"（或 "lease" 表示默认 namespace）。
    #[arg(long)]
    ha_backend_connstring: Option<String>,

    /// Cluster id / namespace for HA keys and oplog paths.
    /// HA key 和 oplog 路径使用的 cluster id / namespace。
    #[arg(long)]
    cluster_id: Option<String>,

    /// 快照后端类型: "local-disk" 或 "hf3fs"
    /// Snapshot backend type: "local-disk" or "hf3fs"
    #[arg(long)]
    snapshot_backend_type: Option<String>,

    /// 快照备份目录路径 / Snapshot backup directory path
    #[arg(long)]
    snapshot_backup_dir: Option<String>,

    /// HA lease TTL in seconds
    /// HA 租约 TTL，单位秒
    #[arg(long, default_value_t = 30)]
    ha_lease_ttl_secs: i64,
}

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
        run_ha_loop(args).await
    } else {
        run_standalone(args).await
    }
}

// ---------------------------------------------------------------------------
// HA perpetual retry loop
// HA 永久重试循环
//
// 流程 / Flow:
//   1. 读取当前 view → 进入 standby 模式
//   2. 若不存在 leader → 尝试获取 leadership → warmup → 启动 gRPC 服务
//   3. 若已存在 leader → 等待 view 变更后重新检查
//
//   1. Read current view → enter standby mode
//   2. If no leader → try acquire leadership → warmup → start gRPC server
//   3. If leader exists → wait for view change and re-check
// ---------------------------------------------------------------------------

// Helper: enter standby and sleep before continuing the outer loop.
async fn back_to_standby(supervisor: &mut MasterServiceSupervisor, sleep_secs: u64) {
    supervisor.enable_standby_updates();
    let _ = supervisor.enter_standby_mode(None);
    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
}

// Helper: release leadership, enter standby, and sleep.
async fn release_and_retry(
    coordinator: &LeaderCoordinator,
    supervisor: &mut MasterServiceSupervisor,
    session: &LeadershipSession,
) {
    let _ = coordinator.release_leadership(session).await;
    back_to_standby(supervisor, 1).await;
}

async fn run_ha_loop(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let (snapshot_backend_type, snapshot_dir) = parse_snapshot_config(&args);
    let ha_spec = build_ha_spec(&args)?;
    let cluster_id = ha_spec.cluster_namespace.clone();
    let runtime_config = build_runtime_config(&args)?;
    let supervisor_config = MasterServiceSupervisorConfig {
        local_hostname: format!("{}:{}", args.rpc_address, args.rpc_port),
        cluster_id,
        enable_snapshot_restore: snapshot_dir.is_some(),
        snapshot_backup_dir: snapshot_dir.clone(),
        snapshot_backend_type,
    };
    let leader_addr = format!("{}:{}", args.rpc_address, args.rpc_port);
    info!("HA loop started");

    // --- Outer loop: re-create coordinator each iteration (matches C++) ---
    loop {
        let coordinator = match create_coordinator(&ha_spec).await {
            Ok(c) => c,
            Err(e) => {
                if e.downcast_ref::<HaError>().map_or(false, |h| h.is_fatal()) {
                    return Err(e);
                }
                warn!("Coordinator creation failed: {}, retrying in 1s", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let service_arc = build_master_service(
            snapshot_backend_type,
            snapshot_dir_for_cluster(snapshot_dir.clone(), &ha_spec.cluster_namespace),
            runtime_config.clone(),
        );
        let mut supervisor = new_supervisor(&ha_spec, &supervisor_config, service_arc.clone());
        if let Err(e) = supervisor.enter_standby_mode(None) {
            warn!("enter_standby_mode failed: {}, retrying in 1s", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        // --- Inner loop: read view, try to acquire leadership ---
        loop {
            supervisor.begin_candidacy();

            let current_view = match coordinator.read_current_view().await {
                Ok(v) => v,
                Err(e) => {
                    warn!("read_current_view failed: {}", e);
                    if e.is_fatal() {
                        return Err(Box::new(e));
                    }
                    back_to_standby(&mut supervisor, 1).await;
                    break; // re-create coordinator
                }
            };

            info!(
                "Current view: {}",
                current_view
                    .as_ref()
                    .map(|v| v.leader_address.as_str())
                    .unwrap_or("none")
            );
            supervisor.update_observed_leader(current_view.clone());

            // --- Path A: no leader → bid for it ---
            if current_view.is_some() {
                wait_and_continue(&coordinator, &current_view).await;
                continue; // back to top of inner loop
            }

            info!("No leader — acquiring leadership");
            let acquire = match coordinator
                .try_acquire_leadership(&leader_addr, args.ha_lease_ttl_secs)
                .await
            {
                Ok(r) if r.acquired => r,
                Ok(_) => {
                    info!("Not acquired, waiting");
                    continue;
                }
                Err(e) => {
                    warn!("Acquire error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break; // re-create coordinator
                }
            };

            let session = match acquire.session {
                Some(session) => session,
                None => {
                    warn!("Leadership acquired without a session, retrying");
                    back_to_standby(&mut supervisor, 1).await;
                    break;
                }
            };
            if service_arc.oplog_manager().lock().store().is_none() {
                match build_leader_oplog_manager(&ha_spec, 0).await {
                    Some(manager) => {
                        *service_arc.oplog_manager().lock() = manager;
                    }
                    None => {
                        warn!("Leader oplog store is unavailable, releasing leadership");
                        release_and_retry(&coordinator, &mut supervisor, &session).await;
                        break;
                    }
                }
            }
            if let Some(view) = &acquire.view {
                service_arc.set_view_version(view.view_version as i64);
                service_arc
                    .oplog_manager()
                    .lock()
                    .set_view_version(view.view_version);
            }
            info!("Leadership acquired, lease TTL={}s", args.ha_lease_ttl_secs);

            // Promote standby. Match C++: stop accepting standby runtime callbacks
            // before promotion, so LeaderWarmup cannot be overwritten by standby.
            supervisor.disable_standby_updates();
            if let Err(e) = supervisor.promote_to_leader_warmup() {
                error!("Promotion failed: {}", e);
                supervisor.enable_standby_updates();
                release_and_retry(&coordinator, &mut supervisor, &session).await;
                break;
            }

            // Start keepalive (background tokio task renews lease every 3s).
            let keepalive_handle = match coordinator.start_leadership_keepalive(&session).await {
                Ok(handle) => handle,
                Err(e) => {
                    error!("Keepalive start failed: {}", e);
                    supervisor.enable_standby_updates();
                    release_and_retry(&coordinator, &mut supervisor, &session).await;
                    break;
                }
            };

            // Active warmup: renew lease every second.
            if !warmup_with_renewal(&coordinator, &session, args.ha_lease_ttl_secs).await {
                supervisor.enable_standby_updates();
                release_and_retry(&coordinator, &mut supervisor, &session).await;
                break;
            }

            // Preflight: final renewal before serving.
            if let Err(e) = coordinator.try_renew_leadership(&session).await {
                warn!("Preflight renewal failed: {}", e);
                supervisor.enable_standby_updates();
                release_and_retry(&coordinator, &mut supervisor, &session).await;
                break;
            }

            supervisor.activate_serving_state();

            // LeadershipMonitor + server. Monitor MUST exist (fallback: dummy tx).
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let monitor = match start_leadership_monitor(&coordinator, &session, shutdown_tx).await
            {
                Ok(handle) => handle,
                Err(e) => {
                    warn!("Leadership monitor start failed: {}", e);
                    supervisor.enable_standby_updates();
                    release_and_retry(&coordinator, &mut supervisor, &session).await;
                    break;
                }
            };

            let server_result = run_leader_server(service_arc.clone(), &args, shutdown_rx).await;

            match &server_result {
                Ok(()) => info!("gRPC server exited cleanly"),
                Err(e) => error!("gRPC server error: {}", e),
            }

            // Cleanup.
            drop(monitor);
            drop(keepalive_handle);
            supervisor.deactivate_serving_state();
            supervisor.enable_standby_updates();
            let _ = coordinator.release_leadership(&session).await;

            // Re-read view before re-entering standby.
            if let Ok(post_view) = coordinator.read_current_view().await {
                supervisor.update_observed_leader(post_view.clone());
                let _ = supervisor.enter_standby_mode(post_view);
            } else {
                let _ = supervisor.enter_standby_mode(None);
            }

            info!("Returning to standby loop");
            break; // re-create coordinator
        }
    }
}

// --- Helpers ---

fn parse_snapshot_config(
    args: &Args,
) -> (
    Option<mooncake_store_master::storage_backend::StorageBackendType>,
    Option<std::path::PathBuf>,
) {
    let backend = args.snapshot_backend_type.as_deref().and_then(|s| {
        if s == "local-disk" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::LocalDisk)
        } else if s == "hf3fs" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::Hf3fs)
        } else {
            None
        }
    });
    let dir = args
        .snapshot_backup_dir
        .clone()
        .map(std::path::PathBuf::from);
    (backend, dir)
}

fn new_supervisor(
    ha_spec: &HABackendSpec,
    config: &MasterServiceSupervisorConfig,
    service: Arc<MasterServiceImpl>,
) -> MasterServiceSupervisor {
    let controller = service.create_ha_standby_controller(ha_spec.clone(), config.clone());
    MasterServiceSupervisor::new(Box::new(controller))
}

fn build_master_service(
    snapshot_backend_type: Option<mooncake_store_master::storage_backend::StorageBackendType>,
    snapshot_dir: Option<std::path::PathBuf>,
    runtime_config: MasterRuntimeConfig,
) -> Arc<MasterServiceImpl> {
    Arc::new(MasterServiceImpl::new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir,
        runtime_config,
    ))
}

fn snapshot_dir_for_cluster(
    snapshot_dir: Option<std::path::PathBuf>,
    cluster_id: &str,
) -> Option<std::path::PathBuf> {
    snapshot_dir.map(|dir| {
        let cluster_id = cluster_id.trim();
        if cluster_id.is_empty() {
            dir
        } else {
            dir.join(cluster_id)
        }
    })
}

async fn wait_and_continue(coordinator: &LeaderCoordinator, current_view: &Option<MasterView>) {
    let version = current_view.as_ref().map(|v| v.view_version).unwrap_or(0);
    match coordinator
        .wait_for_view_change(version, Duration::from_secs(1))
        .await
    {
        Ok(Some(v)) => info!(
            "View changed: leader={}, version={}",
            v.leader_address, v.view_version
        ),
        Ok(None) => {}
        Err(e) => warn!("View wait error: {}", e),
    }
}

async fn warmup_with_renewal(
    coordinator: &LeaderCoordinator,
    session: &LeadershipSession,
    ttl_secs: i64,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(ttl_secs as u64);
    info!("Warmup {}s, renewing each second", ttl_secs);
    while tokio::time::Instant::now() < deadline {
        if coordinator.try_renew_leadership(session).await.is_err() {
            warn!("Warmup renewal failed");
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    info!("Warmup complete");
    true
}

async fn start_leadership_monitor(
    coordinator: &LeaderCoordinator,
    session: &LeadershipSession,
    tx: tokio::sync::watch::Sender<bool>,
) -> Result<LeadershipMonitorHandle, HaError> {
    let mut role_rx = coordinator.subscribe_role_for_session(session)?;
    let h = tokio::spawn(async move {
        loop {
            if role_rx.changed().await.is_err() {
                warn!("LeadershipMonitor: role channel closed, shutting down");
                let _ = tx.send(true);
                break;
            }
            if *role_rx.borrow() == mooncake_store_master::ha::LeaderRole::Standby {
                warn!("LeadershipMonitor: leadership lost, shutting down");
                let _ = tx.send(true);
                break;
            }
        }
    });
    Ok(LeadershipMonitorHandle::new(h))
}

// ---------------------------------------------------------------------------
// Standalone (non-HA) path — existing behaviour preserved
// 单机模式（非 HA）路径 — 保留原有行为
// ---------------------------------------------------------------------------
async fn run_standalone(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // --- Metrics HTTP server ---
    // Prometheus metrics HTTP 服务
    let metrics_addr = SocketAddr::new(args.rpc_address.parse()?, args.metrics_port);
    tokio::spawn(metrics::serve_metrics_http(metrics_addr));

    // --- Master gRPC service ---
    // 构建 Master gRPC 服务
    let snapshot_backend_type = args.snapshot_backend_type.as_deref().and_then(|s| {
        if s == "local-disk" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::LocalDisk)
        } else if s == "hf3fs" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::Hf3fs)
        } else {
            None
        }
    });
    let snapshot_dir = args
        .snapshot_backup_dir
        .clone()
        .map(std::path::PathBuf::from);

    let rpc_addr = SocketAddr::new(args.rpc_address.parse()?, args.rpc_port);
    let metadata_addr = SocketAddr::new(
        args.http_metadata_server_host.parse()?,
        args.http_metadata_server_port,
    );

    let runtime_config = build_runtime_config(&args)?;
    let service = MasterServiceImpl::new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir,
        runtime_config,
    );
    let service_arc = std::sync::Arc::new(service);
    service_arc
        .metadata_state()
        .set_master_addr(format!("http://{}", rpc_addr))
        .await;

    // 启动 HTTP metadata 服务
    tokio::spawn(serve_metadata_http(
        metadata_addr,
        service_arc.metadata_state(),
    ));

    // Periodic snapshot — 定时快照（每 30 秒）
    {
        let svc = service_arc.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                svc.save_snapshot();
            }
        });
    }

    info!("Mooncake Master starting on {}", rpc_addr);
    ensure_supported_rpc_protocol()?;
    // 启动 gRPC server，阻塞直到服务停止
    tonic::transport::Server::builder()
        .add_service(
            mooncake_store_master::proto::master_service_server::MasterServiceServer::from_arc(
                service_arc,
            ),
        )
        .serve(rpc_addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Args {
        Args {
            rpc_address: "127.0.0.1".to_string(),
            rpc_port: 50051,
            http_metadata_server_host: "127.0.0.1".to_string(),
            http_metadata_server_port: 8080,
            metrics_port: 9003,
            rpc_thread_num: 4,
            allocation_strategy: "random".to_string(),
            memory_allocator: "offset".to_string(),
            default_kv_lease_ttl_ms: 5000,
            eviction_high_watermark_ratio: 0.95,
            eviction_ratio: 0.05,
            offload_on_evict: false,
            offload_force_evict: false,
            enable_ha: true,
            etcd_endpoints: None,
            ha_backend_type: "etcd".to_string(),
            ha_backend_connstring: None,
            cluster_id: Some("cluster-a".to_string()),
            snapshot_backend_type: None,
            snapshot_backup_dir: None,
            ha_lease_ttl_secs: 30,
        }
    }

    #[test]
    fn test_build_ha_spec_etcd_falls_back_to_etcd_endpoints() {
        let mut args = base_args();
        args.etcd_endpoints = Some("http://127.0.0.1:2379".to_string());

        let spec = build_ha_spec(&args).unwrap();

        assert_eq!(spec.backend_type, HABackendType::Etcd);
        assert_eq!(spec.connstring, "http://127.0.0.1:2379");
        assert_eq!(spec.cluster_namespace, "cluster-a");
    }

    #[test]
    fn test_build_ha_spec_redis_uses_explicit_connstring() {
        let mut args = base_args();
        args.ha_backend_type = "redis".to_string();
        args.ha_backend_connstring = Some("redis://127.0.0.1:6379".to_string());

        let spec = build_ha_spec(&args).unwrap();

        assert_eq!(spec.backend_type, HABackendType::Redis);
        assert_eq!(spec.connstring, "redis://127.0.0.1:6379");
    }

    #[test]
    fn test_build_ha_spec_k8s_uses_explicit_connstring() {
        let mut args = base_args();
        args.ha_backend_type = "k8s".to_string();
        args.ha_backend_connstring = Some("ns-a/lease-a".to_string());

        let spec = build_ha_spec(&args).unwrap();

        assert_eq!(spec.backend_type, HABackendType::K8s);
        assert_eq!(spec.connstring, "ns-a/lease-a");
    }

    #[test]
    fn test_build_ha_spec_k8s_requires_connstring() {
        let mut args = base_args();
        args.ha_backend_type = "k8s".to_string();

        let err = build_ha_spec(&args).unwrap_err();

        assert!(matches!(err, HaError::InvalidParams(_)));
    }

    #[tokio::test]
    async fn test_create_coordinator_k8s_reports_unavailable() {
        let spec = HABackendSpec {
            backend_type: HABackendType::K8s,
            connstring: "ns-a/lease-a".to_string(),
            cluster_namespace: "cluster-a".to_string(),
        };

        let err = match create_coordinator(&spec).await {
            Ok(_) => panic!("k8s coordinator should be unavailable in the current Rust build"),
            Err(err) => err,
        };
        let ha_error = err.downcast_ref::<HaError>().unwrap();

        assert_eq!(
            ha_error,
            &HaError::UnavailableInCurrentMode(
                "K8s HA backend is not implemented in Rust coordinator".into()
            )
        );
    }

    #[test]
    fn test_build_master_service_returns_fresh_instance_per_call() {
        let runtime_config = build_runtime_config(&base_args()).unwrap();

        let first = build_master_service(None, None, runtime_config.clone());
        let second = build_master_service(None, None, runtime_config);

        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn test_snapshot_dir_for_cluster_scopes_ha_snapshots() {
        let root = std::path::PathBuf::from("/tmp/mooncake-snapshots");

        assert_eq!(
            snapshot_dir_for_cluster(Some(root.clone()), "cluster-a").unwrap(),
            root.join("cluster-a")
        );
        assert_eq!(
            snapshot_dir_for_cluster(Some(root.clone()), "").unwrap(),
            root
        );
        assert!(snapshot_dir_for_cluster(None, "cluster-a").is_none());
    }

    #[test]
    fn test_validate_rpc_protocol_rejects_unsupported_rdma() {
        assert!(validate_rpc_protocol(None).is_ok());
        assert!(validate_rpc_protocol(Some("tcp")).is_ok());

        let err = validate_rpc_protocol(Some("rdma")).unwrap_err();
        let ha_error = err.downcast_ref::<HaError>().unwrap();
        assert!(matches!(ha_error, HaError::UnavailableInCurrentMode(_)));
    }
}

// ---------------------------------------------------------------------------
// Helpers — 工具函数
// ---------------------------------------------------------------------------

/// 从 CLI 参数构建 MasterRuntimeConfig。
/// Build MasterRuntimeConfig from CLI args.
fn build_runtime_config(args: &Args) -> Result<MasterRuntimeConfig, Box<dyn std::error::Error>> {
    Ok(MasterRuntimeConfig {
        allocation_strategy: AllocationStrategy::parse(&args.allocation_strategy)
            .ok_or("allocation_strategy must be 'random' or 'free_ratio_first'")?,
        memory_allocator_kind: MemoryAllocatorKind::parse(&args.memory_allocator)
            .ok_or("memory_allocator must be 'offset' or 'cachelib'")?,
        lease_ttl: Duration::from_millis(args.default_kv_lease_ttl_ms),
        eviction_high_watermark_ratio: args.eviction_high_watermark_ratio,
        eviction_ratio: args.eviction_ratio,
        offload_on_evict: args.offload_on_evict,
        offload_force_evict: args.offload_force_evict,
        ..Default::default()
    })
}

/// 创建 LeaderCoordinator：根据 HA backend spec 选择后端。
/// Create LeaderCoordinator from the resolved HA backend spec.
async fn create_coordinator(
    spec: &HABackendSpec,
) -> Result<LeaderCoordinator, Box<dyn std::error::Error>> {
    match spec.backend_type {
        HABackendType::Etcd => {
            let endpoints: Vec<String> = spec
                .connstring
                .split(';')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.to_string())
                .collect();
            if endpoints.is_empty() {
                return Err(Box::new(HaError::InvalidParams(
                    "etcd HA backend requires a non-empty connection string".into(),
                )));
            }
            Ok(LeaderCoordinator::new_etcd(endpoints, &spec.cluster_namespace).await?)
        }
        HABackendType::Redis => {
            Ok(LeaderCoordinator::new_redis(&spec.connstring, &spec.cluster_namespace).await?)
        }
        HABackendType::K8s => Err(Box::new(HaError::UnavailableInCurrentMode(
            "K8s HA backend is not implemented in Rust coordinator".into(),
        ))),
        HABackendType::Unknown => Err(Box::new(HaError::InvalidParams(
            "unknown HA backend type".into(),
        ))),
    }
}

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

    let client = match etcd_client::Client::connect(endpoints, None).await {
        Ok(client) => client,
        Err(e) => {
            warn!("Failed to connect etcd for leader oplog: {}", e);
            return None;
        }
    };
    let oplog_prefix = format!("/oplog/{}", spec.cluster_namespace);
    let store = match mooncake_store_master::oplog::EtcdOpLogStore::new(client, &oplog_prefix).await
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

/// 构建 HA 后端规格，标识使用的协调后端类型。
/// Build HA backend spec describing which coordination backend is used.
fn build_ha_spec(args: &Args) -> Result<HABackendSpec, HaError> {
    let backend_type = parse_ha_backend_type(&args.ha_backend_type).ok_or_else(|| {
        HaError::InvalidParams(format!("unknown HA backend type: {}", args.ha_backend_type))
    })?;
    let cluster_namespace = resolve_cluster_id(args);
    let connstring = match backend_type {
        HABackendType::Etcd => args
            .ha_backend_connstring
            .clone()
            .or_else(|| args.etcd_endpoints.clone())
            .unwrap_or_default(),
        HABackendType::Redis => args.ha_backend_connstring.clone().unwrap_or_default(),
        HABackendType::K8s => args.ha_backend_connstring.clone().unwrap_or_default(),
        HABackendType::Unknown => String::new(),
    };

    if connstring.trim().is_empty() {
        return Err(HaError::InvalidParams(format!(
            "HA backend connection string must be set for backend_type={}",
            backend_type.as_str()
        )));
    }

    Ok(HABackendSpec {
        backend_type,
        connstring,
        cluster_namespace,
    })
}

fn resolve_cluster_id(args: &Args) -> String {
    args.cluster_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("MC_STORE_CLUSTER_ID")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| "mooncake".to_string())
}

/// Start the full gRPC server along with metrics, HTTP metadata, and snapshots.
/// Returns when the gRPC server stops (leadership lost / shutdown).
///
/// 启动完整的 gRPC 服务以及 metrics、HTTP metadata 和定时快照。
/// 当 gRPC server 停止时返回（leadership 丢失或主动关闭）。
async fn run_leader_server(
    service_arc: Arc<MasterServiceImpl>,
    args: &Args,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut background_tasks = Vec::new();

    // Metrics HTTP server.
    let metrics_addr = SocketAddr::new(args.rpc_address.parse()?, args.metrics_port);
    background_tasks.push(tokio::spawn(metrics::serve_metrics_http(metrics_addr)));

    let rpc_addr = SocketAddr::new(args.rpc_address.parse()?, args.rpc_port);
    let metadata_addr = SocketAddr::new(
        args.http_metadata_server_host.parse()?,
        args.http_metadata_server_port,
    );

    service_arc
        .metadata_state()
        .set_master_addr(format!("http://{}", rpc_addr))
        .await;

    background_tasks.push(tokio::spawn(serve_metadata_http(
        metadata_addr,
        service_arc.metadata_state(),
    )));

    // Periodic snapshot every 30s.
    {
        let svc = service_arc.clone();
        let mut snapshot_shutdown_rx = shutdown_rx.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                        svc.save_snapshot();
                    }
                    changed = snapshot_shutdown_rx.changed() => {
                        if changed.is_err() || *snapshot_shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }));
    }

    info!("Mooncake Master (HA leader) starting on {}", rpc_addr);
    ensure_supported_rpc_protocol()?;

    // Serve with shutdown signal — LeadershipMonitor triggers graceful stop on lease loss.
    // C++ equivalent: LeadershipMonitor callback calls server.stop().
    let serve_future = tonic::transport::Server::builder()
        .add_service(
            mooncake_store_master::proto::master_service_server::MasterServiceServer::from_arc(
                service_arc,
            ),
        )
        .serve_with_shutdown(rpc_addr, async move {
            loop {
                if shutdown_rx.changed().await.is_err() || *shutdown_rx.borrow() {
                    info!("Shutdown signal received, stopping gRPC server");
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

    let result = serve_future.await;
    for task in background_tasks {
        task.abort();
    }
    result?;
    Ok(())
}

fn ensure_supported_rpc_protocol() -> Result<(), Box<dyn std::error::Error>> {
    validate_rpc_protocol(std::env::var("MC_RPC_PROTOCOL").ok().as_deref())
}

fn validate_rpc_protocol(protocol: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    match protocol {
        Some("rdma") => Err(Box::new(HaError::UnavailableInCurrentMode(
            "Rust tonic master server does not support coro_rpc RDMA init_ibv".into(),
        ))),
        _ => Ok(()),
    }
}

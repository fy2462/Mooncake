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
    HABackendSpec, HABackendType, HaError, LeaderCoordinator, LeadershipMonitorHandle,
    MasterServiceSupervisor, MasterServiceSupervisorConfig, MasterView,
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

    /// Kubernetes 命名空间（HA 模式 via K8s lease）
    /// Kubernetes namespace (HA mode via K8s lease)
    #[arg(long)]
    k8s_namespace: Option<String>,

    /// Kubernetes lease 名称 / Kubernetes lease name
    #[arg(long)]
    k8s_lease_name: Option<String>,

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
    let _ = supervisor.enter_standby_mode(None);
    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
}

// Helper: release leadership, enter standby, and sleep.
async fn release_and_retry(
    coordinator: &LeaderCoordinator,
    supervisor: &mut MasterServiceSupervisor,
    lease_id: i64,
) {
    let _ = coordinator.release_leadership(lease_id).await;
    back_to_standby(supervisor, 1).await;
}

async fn run_ha_loop(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let (snapshot_backend_type, snapshot_dir) = parse_snapshot_config(&args);
    let runtime_config = build_runtime_config(&args)?;
    let leader_oplog_manager = build_leader_oplog_manager(&args, 0).await;
    let service_arc = std::sync::Arc::new(MasterServiceImpl::new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir.clone(),
        runtime_config,
    ));
    if let Some(manager) = leader_oplog_manager {
        *service_arc.oplog_manager().lock() = manager;
    }
    let ha_spec = build_ha_spec(&args);
    let supervisor_config = MasterServiceSupervisorConfig {
        local_hostname: format!("{}:{}", args.rpc_address, args.rpc_port),
        cluster_id: "default".to_string(),
        enable_snapshot_restore: snapshot_dir.is_some(),
        snapshot_backup_dir: snapshot_dir,
        snapshot_backend_type,
    };
    let leader_addr = format!("{}:{}", args.rpc_address, args.rpc_port);
    info!("HA loop started");

    // --- Outer loop: re-create coordinator each iteration (matches C++) ---
    loop {
        let coordinator = match create_coordinator(&args).await {
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

            let lease_id = acquire.lease_id.unwrap_or(0);
            if service_arc.oplog_manager().lock().store().is_none() {
                match build_leader_oplog_manager(&args, 0).await {
                    Some(manager) => {
                        *service_arc.oplog_manager().lock() = manager;
                    }
                    None => {
                        warn!("Leader oplog store is unavailable, releasing leadership");
                        release_and_retry(&coordinator, &mut supervisor, lease_id).await;
                        break;
                    }
                }
            }
            if let Some(view) = &acquire.view {
                service_arc
                    .oplog_manager()
                    .lock()
                    .set_view_version(view.view_version);
            }
            info!("Leadership acquired, lease TTL={}s", args.ha_lease_ttl_secs);

            // Promote standby.
            if let Err(e) = supervisor.promote_to_leader_warmup() {
                error!("Promotion failed: {}", e);
                release_and_retry(&coordinator, &mut supervisor, lease_id).await;
                break;
            }

            // Start keepalive (background tokio task renews lease every 3s).
            let keepalive_handle = match coordinator.start_leadership_keepalive(lease_id).await {
                Ok(handle) => handle,
                Err(e) => {
                    error!("Keepalive start failed: {}", e);
                    release_and_retry(&coordinator, &mut supervisor, lease_id).await;
                    break;
                }
            };

            // Active warmup: renew lease every second.
            if !warmup_with_renewal(&coordinator, lease_id, args.ha_lease_ttl_secs).await {
                release_and_retry(&coordinator, &mut supervisor, lease_id).await;
                break;
            }

            // Preflight: final renewal before serving.
            if let Err(e) = coordinator.try_renew_leadership(lease_id).await {
                warn!("Preflight renewal failed: {}", e);
                release_and_retry(&coordinator, &mut supervisor, lease_id).await;
                break;
            }

            supervisor.disable_standby_updates();
            supervisor.activate_serving_state();

            // LeadershipMonitor + server. Monitor MUST exist (fallback: dummy tx).
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let monitor = start_leadership_monitor(&args, lease_id, shutdown_tx).await;

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
            let _ = coordinator.release_leadership(lease_id).await;

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
    lease_id: i64,
    ttl_secs: i64,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(ttl_secs as u64);
    info!("Warmup {}s, renewing each second", ttl_secs);
    while tokio::time::Instant::now() < deadline {
        if coordinator.try_renew_leadership(lease_id).await.is_err() {
            warn!("Warmup renewal failed");
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    info!("Warmup complete");
    true
}

async fn start_leadership_monitor(
    args: &Args,
    lease_id: i64,
    tx: tokio::sync::watch::Sender<bool>,
) -> Option<LeadershipMonitorHandle> {
    // Fallback: if we can't create a monitor coordinator, send shutdown immediately
    // so the server starts and immediately stops — don't serve without a monitor.
    let c = match create_coordinator(args).await {
        Ok(c) => c,
        Err(e) => {
            error!(
                "Monitor coordinator creation failed: {}, sending immediate shutdown",
                e
            );
            let _ = tx.send(true);
            return None;
        }
    };
    let h = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            if c.try_renew_leadership(lease_id).await.is_err() {
                warn!("LeadershipMonitor: lease lost, shutting down");
                let _ = tx.send(true);
                break;
            }
        }
    });
    Some(LeadershipMonitorHandle::new(h))
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

/// 创建 LeaderCoordinator：根据配置选择 etcd 或 k8s 后端。
/// Create LeaderCoordinator: pick etcd or k8s backend based on config.
async fn create_coordinator(args: &Args) -> Result<LeaderCoordinator, Box<dyn std::error::Error>> {
    let endpoints: Vec<String> = args
        .etcd_endpoints
        .as_deref()
        .unwrap_or("")
        .split(';')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    if endpoints.is_empty() && args.k8s_namespace.is_some() {
        Err(Box::new(HaError::InvalidBackend(
            "K8s HA backend is not implemented in Rust coordinator".into(),
        )))
    } else if !endpoints.is_empty() {
        // 使用 etcd 进行 Leader 选举
        Ok(LeaderCoordinator::new_etcd(endpoints, "default").await?)
    } else {
        error!("HA mode enabled but neither etcd_endpoints nor k8s_namespace provided");
        Err("HA requires etcd or K8s configuration".into())
    }
}

async fn build_leader_oplog_manager(
    args: &Args,
    view_version: u64,
) -> Option<mooncake_store_master::oplog::OpLogManager> {
    let endpoints: Vec<String> = args
        .etcd_endpoints
        .as_deref()
        .unwrap_or("")
        .split(';')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if endpoints.is_empty() {
        return None;
    }

    let client = match etcd_client::Client::connect(endpoints, None).await {
        Ok(client) => client,
        Err(e) => {
            warn!("Failed to connect etcd for leader oplog: {}", e);
            return None;
        }
    };
    let store =
        match mooncake_store_master::oplog::EtcdOpLogStore::new(client, "/oplog/default").await {
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
fn build_ha_spec(args: &Args) -> HABackendSpec {
    let has_etcd = args
        .etcd_endpoints
        .as_deref()
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let has_k8s = args.k8s_namespace.is_some();

    HABackendSpec {
        backend_type: if has_etcd {
            HABackendType::Etcd
        } else if has_k8s {
            HABackendType::K8s
        } else {
            HABackendType::Unknown
        },
        connstring: args.etcd_endpoints.clone().unwrap_or_default(),
        cluster_namespace: "default".to_string(),
    }
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

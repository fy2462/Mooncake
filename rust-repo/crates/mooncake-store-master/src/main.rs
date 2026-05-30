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
    CapabilityDrivenStandbyController, HABackendSpec, HABackendType, LeaderCoordinator,
    MasterServiceSupervisor, MasterServiceSupervisorConfig,
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
async fn run_ha_loop(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // --- Create LeaderCoordinator ---
    // 创建 Leader 协调器（etcd 或 k8s 实现）
    let coordinator = create_coordinator(&args).await?;

    // --- Parse snapshot / storage config ---
    // 解析快照和存储配置
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

    // --- Build the gRPC service once (reusable via Arc clones) ---
    // 一次性构建 gRPC 服务（通过 Arc 克隆复用）
    let runtime_config = build_runtime_config(&args)?;
    let service = MasterServiceImpl::new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir.clone(),
        runtime_config,
    );
    let service_arc = std::sync::Arc::new(service);

    // --- Identify HA backend spec ---
    // 确定 HA 后端规格
    let ha_spec = build_ha_spec(&args);

    // --- Create standby controller + supervisor ---
    // 创建 standby 控制器和 supervisor（热备状态管理）
    let supervisor_config = MasterServiceSupervisorConfig {
        local_hostname: format!("{}:{}", args.rpc_address, args.rpc_port),
        cluster_id: "default".to_string(),
        enable_snapshot_restore: snapshot_dir.is_some(),
        snapshot_backup_dir: snapshot_dir.clone(),
        snapshot_backend_type,
    };
    let standby_controller = CapabilityDrivenStandbyController::new(ha_spec, supervisor_config);
    let mut supervisor = MasterServiceSupervisor::new(Box::new(standby_controller));

    info!("HA loop started — entering standby mode");
    // HA 循环已启动 — 进入 standby 模式

    let leader_addr = format!("{}:{}", args.rpc_address, args.rpc_port);

    // --- Perpetual retry loop ---
    // 永久重试循环：持续监控 view 变更并在适当时机竞争 leader
    loop {
        // 1. Read current view and enter standby
        // 读取当前 view 并进入 standby
        let current_view = match coordinator.read_current_view().await {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to read current view: {}, retrying in 5s", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        info!(
            "Current view: {:?}",
            current_view
                .as_ref()
                .map(|v| v.leader_address.as_str())
                .unwrap_or("none")
        );

        if let Err(e) = supervisor.enter_standby_mode(current_view.clone()) {
            warn!("Failed to enter standby mode: {}, retrying in 5s", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        // 2. If no leader — try to acquire
        // 若当前无 leader — 尝试获取 leadership
        if current_view.is_none() {
            info!("No leader detected, attempting to acquire leadership");
            // 未检测到 leader，尝试获取 leadership
            supervisor.begin_candidacy();

            match coordinator
                .try_acquire_leadership(&leader_addr, args.ha_lease_ttl_secs)
                .await
            {
                Ok(result) if result.acquired => {
                    info!("Leadership acquired. Lease TTL={}s", args.ha_lease_ttl_secs);
                    // 成功获取 leadership
                    let lease_id = result.lease_id.unwrap_or(0);

                    // Promote standby → warmup
                    // 从 standby 提升到 warmup 状态
                    if let Err(e) = supervisor.promote_to_leader_warmup() {
                        error!("Promotion failed: {}, releasing and retrying", e);
                        let _ = coordinator.release_leadership(lease_id).await;
                        continue;
                    }

                    // Start keepalive — 启动 lease 续约
                    let keepalive_handle =
                        match coordinator.start_leadership_keepalive(lease_id).await {
                            Ok(h) => h,
                            Err(e) => {
                                error!("Failed to start keepalive: {}, releasing and retrying", e);
                                let _ = coordinator.release_leadership(lease_id).await;
                                continue;
                            }
                        };

                    // Warmup: wait for lease_ttl to pass so state is stable
                    // 预热阶段：等待 lease_ttl 时间让状态稳定
                    let warmup_duration = Duration::from_secs(args.ha_lease_ttl_secs as u64);
                    let warmup_deadline = tokio::time::Instant::now() + warmup_duration;
                    info!(
                        "Warmup phase started ({}s), renewing lease each second",
                        args.ha_lease_ttl_secs
                    );
                    while tokio::time::Instant::now() < warmup_deadline {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    info!("Warmup complete");
                    // 预热完成

                    supervisor.activate_serving_state();

                    // Start gRPC + metadata + metrics + snapshots
                    // 启动 gRPC 服务、metadata HTTP、metrics 和定时快照
                    let server_result =
                        run_leader_server(service_arc.clone(), &args, keepalive_handle).await;

                    match &server_result {
                        Ok(()) => info!("gRPC server exited cleanly"),
                        // gRPC 服务正常退出
                        Err(e) => warn!("gRPC server exited with error: {}", e),
                    }

                    // Release leadership on exit
                    // 退出时释放 leadership
                    info!("Releasing leadership");
                    if let Err(e) = coordinator.release_leadership(lease_id).await {
                        error!("Failed to release leadership: {}", e);
                    }

                    info!("Returning to standby");
                    // 回到 standby 状态
                    continue;
                }
                Ok(_not_acquired) => {
                    info!("Leadership not acquired — view will change, retrying");
                    // 未获取到 leadership — view 会变更，重试
                }
                Err(e) => {
                    warn!("Leadership acquisition error: {}, retrying", e);
                }
            }
        }

        // 3. Wait for view change before retrying
        // 等待 view 变更后重试
        let known_version = current_view.as_ref().map(|v| v.view_version).unwrap_or(0);
        info!(
            "Waiting for view change (version={}) for up to 30s",
            known_version
        );
        match coordinator
            .wait_for_view_change(known_version, Duration::from_secs(30))
            .await
        {
            Ok(Some(new_view)) => {
                info!(
                    "View changed: new leader={}, version={}",
                    new_view.leader_address, new_view.view_version
                );
            }
            Ok(None) => {
                info!("View change timed out, re-reading current view");
                // View 变更等待超时，重新读取当前 view
            }
            Err(e) => {
                warn!("Error waiting for view change: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
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
        // 使用 Kubernetes lease 进行 Leader 选举
        Ok(LeaderCoordinator::new_k8s(
            args.k8s_namespace.as_deref().unwrap(),
            args.k8s_lease_name.as_deref().unwrap_or("mooncake-master"),
        )
        .await?)
    } else if !endpoints.is_empty() {
        // 使用 etcd 进行 Leader 选举
        Ok(LeaderCoordinator::new_etcd(endpoints).await?)
    } else {
        error!("HA mode enabled but neither etcd_endpoints nor k8s_namespace provided");
        Err("HA requires etcd or K8s configuration".into())
    }
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
        cluster_namespace: args.k8s_namespace.clone().unwrap_or_default(),
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
    _keepalive_handle: mooncake_store_master::ha::LeadershipHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    // Metrics HTTP server
    let metrics_addr = SocketAddr::new(args.rpc_address.parse()?, args.metrics_port);
    tokio::spawn(metrics::serve_metrics_http(metrics_addr));

    let rpc_addr = SocketAddr::new(args.rpc_address.parse()?, args.rpc_port);
    let metadata_addr = SocketAddr::new(
        args.http_metadata_server_host.parse()?,
        args.http_metadata_server_port,
    );

    service_arc
        .metadata_state()
        .set_master_addr(format!("http://{}", rpc_addr))
        .await;

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

    info!("Mooncake Master (HA leader) starting on {}", rpc_addr);
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

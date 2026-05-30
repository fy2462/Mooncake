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

#[derive(Parser, Debug)]
#[command(
    name = "mooncake-master",
    version,
    about = "Mooncake distributed KV cache — Master Service"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    rpc_address: String,

    #[arg(long, default_value_t = 50051)]
    rpc_port: u16,

    #[arg(long, default_value = "0.0.0.0")]
    http_metadata_server_host: String,

    #[arg(long, default_value_t = 8080)]
    http_metadata_server_port: u16,

    #[arg(long, default_value_t = 9003)]
    metrics_port: u16,

    #[arg(long, default_value_t = 4)]
    rpc_thread_num: usize,

    #[arg(long, default_value = "random")]
    allocation_strategy: String,

    #[arg(long, default_value = "offset")]
    memory_allocator: String,

    #[arg(long, default_value_t = 5000)]
    default_kv_lease_ttl_ms: u64,

    #[arg(long, default_value_t = 0.95)]
    eviction_high_watermark_ratio: f64,

    #[arg(long, default_value_t = 0.05)]
    eviction_ratio: f64,

    #[arg(long)]
    offload_on_evict: bool,

    #[arg(long)]
    offload_force_evict: bool,

    #[arg(long)]
    enable_ha: bool,

    #[arg(long)]
    etcd_endpoints: Option<String>,

    #[arg(long)]
    k8s_namespace: Option<String>,

    #[arg(long)]
    k8s_lease_name: Option<String>,

    #[arg(long)]
    snapshot_backend_type: Option<String>,

    #[arg(long)]
    snapshot_backup_dir: Option<String>,

    /// HA lease TTL in seconds
    #[arg(long, default_value_t = 30)]
    ha_lease_ttl_secs: i64,
}

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
// ---------------------------------------------------------------------------
async fn run_ha_loop(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // --- Create LeaderCoordinator ---
    let coordinator = create_coordinator(&args).await?;

    // --- Parse snapshot / storage config ---
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
    let runtime_config = build_runtime_config(&args)?;
    let service = MasterServiceImpl::new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir.clone(),
        runtime_config,
    );
    let service_arc = std::sync::Arc::new(service);

    // --- Identify HA backend spec ---
    let ha_spec = build_ha_spec(&args);

    // --- Create standby controller + supervisor ---
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

    let leader_addr = format!("{}:{}", args.rpc_address, args.rpc_port);

    // --- Perpetual retry loop ---
    loop {
        // 1. Read current view and enter standby
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
        if current_view.is_none() {
            info!("No leader detected, attempting to acquire leadership");
            supervisor.begin_candidacy();

            match coordinator
                .try_acquire_leadership(&leader_addr, args.ha_lease_ttl_secs)
                .await
            {
                Ok(result) if result.acquired => {
                    info!("Leadership acquired. Lease TTL={}s", args.ha_lease_ttl_secs);
                    let lease_id = result.lease_id.unwrap_or(0);

                    // Promote standby → warmup
                    if let Err(e) = supervisor.promote_to_leader_warmup() {
                        error!("Promotion failed: {}, releasing and retrying", e);
                        let _ = coordinator.release_leadership(lease_id).await;
                        continue;
                    }

                    // Start keepalive
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

                    supervisor.activate_serving_state();

                    // Start gRPC + metadata + metrics + snapshots
                    let server_result =
                        run_leader_server(service_arc.clone(), &args, keepalive_handle).await;

                    match &server_result {
                        Ok(()) => info!("gRPC server exited cleanly"),
                        Err(e) => warn!("gRPC server exited with error: {}", e),
                    }

                    // Release leadership on exit
                    info!("Releasing leadership");
                    if let Err(e) = coordinator.release_leadership(lease_id).await {
                        error!("Failed to release leadership: {}", e);
                    }

                    info!("Returning to standby");
                    continue;
                }
                Ok(_not_acquired) => {
                    info!("Leadership not acquired — view will change, retrying");
                }
                Err(e) => {
                    warn!("Leadership acquisition error: {}, retrying", e);
                }
            }
        }

        // 3. Wait for view change before retrying
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
// ---------------------------------------------------------------------------
async fn run_standalone(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // --- Metrics HTTP server ---
    let metrics_addr = SocketAddr::new(args.rpc_address.parse()?, args.metrics_port);
    tokio::spawn(metrics::serve_metrics_http(metrics_addr));

    // --- Master gRPC service ---
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

    tokio::spawn(serve_metadata_http(
        metadata_addr,
        service_arc.metadata_state(),
    ));

    // Periodic snapshot
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
// Helpers
// ---------------------------------------------------------------------------

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
        Ok(LeaderCoordinator::new_k8s(
            args.k8s_namespace.as_deref().unwrap(),
            args.k8s_lease_name.as_deref().unwrap_or("mooncake-master"),
        )
        .await?)
    } else if !endpoints.is_empty() {
        Ok(LeaderCoordinator::new_etcd(endpoints).await?)
    } else {
        error!("HA mode enabled but neither etcd_endpoints nor k8s_namespace provided");
        Err("HA requires etcd or K8s configuration".into())
    }
}

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

    // Periodic snapshot
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

use clap::Parser;
use mooncake_store_master::ha::{LeaderCoordinator, LeaderRole};
use mooncake_store_master::http_metadata::serve_metadata_http;
use mooncake_store_master::metrics;
use mooncake_store_master::MasterServiceImpl;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::fmt;

#[derive(Parser, Debug)]
#[command(name = "mooncake-master", version, about = "Mooncake distributed KV cache — Master Service")]
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

    #[arg(long, default_value_t = 5000)]
    default_kv_lease_ttl_ms: u64,

    #[arg(long, default_value_t = 0.95)]
    eviction_high_watermark_ratio: f64,

    #[arg(long, default_value_t = 0.05)]
    eviction_ratio: f64,

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))).init();

    let args = Args::parse();

    // --- Leader election (HA mode) ---
    let mut coordinator: Option<LeaderCoordinator> = None;
    if args.enable_ha {
        let endpoints: Vec<String> = args
            .etcd_endpoints
            .as_deref()
            .unwrap_or("")
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        if endpoints.is_empty() && args.k8s_namespace.is_some() {
            coordinator = Some(
                LeaderCoordinator::new_k8s(
                    args.k8s_namespace.as_deref().unwrap(),
                    args.k8s_lease_name.as_deref().unwrap_or("mooncake-master"),
                )
                .await?,
            );
        } else if !endpoints.is_empty() {
            coordinator = Some(LeaderCoordinator::new_etcd(endpoints).await?);
        } else {
            tracing::error!("HA mode enabled but neither etcd_endpoints nor k8s_namespace provided");
            return Err("HA requires etcd or K8s configuration".into());
        }
    }

    if let Some(ref coordinator) = coordinator {
        info!("Waiting for leader election...");
        let role = coordinator.wait_for_role().await?;
        match role {
            LeaderRole::Leader => info!("Elected as LEADER, starting service"),
            LeaderRole::Standby => {
                info!("Running as STANDBY, waiting for leadership change...");
                coordinator.watch_leadership_change().await;
                info!("Became LEADER, starting service");
            }
        }
    }

    // --- Metrics HTTP server ---
    let metrics_addr = SocketAddr::new(
        args.rpc_address.parse()?,
        args.metrics_port,
    );
    tokio::spawn(metrics::serve_metrics_http(metrics_addr));

    // --- Master gRPC service ---
    let snapshot_backend_type = args.snapshot_backend_type.as_deref().and_then(|s| {
        if s == "local-disk" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::LocalDisk)
        } else {
            None
        }
    });
    let snapshot_dir = args.snapshot_backup_dir.map(std::path::PathBuf::from);

    let rpc_addr = SocketAddr::new(args.rpc_address.parse()?, args.rpc_port);
    let metadata_addr = SocketAddr::new(
        args.http_metadata_server_host.parse()?,
        args.http_metadata_server_port,
    );

    let service = MasterServiceImpl::new(snapshot_backend_type, snapshot_dir);
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
            mooncake_store_master::proto::master_service_server::MasterServiceServer::from_arc(service_arc),
        )
        .serve(rpc_addr)
        .await?;

    Ok(())
}

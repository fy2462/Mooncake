//! # Mooncake Master — 入口点 / Entry Point
//!
//! 主函数负责解析 CLI 参数、初始化 tracing、并根据是否启用 HA 模式
//! 选择不同的启动路径：`run_standalone`（单节点）或 `run_ha_loop`（高可用主备循环）。
//!
//! The main function parses CLI arguments, initializes tracing, and chooses
//! between `run_standalone` (single-node) or `run_ha_loop` (HA leader/standby loop).

use clap::Parser;
use mooncake_store_master::admin_http::AdminRuntimeState;
use mooncake_store_master::allocator::{AllocationStrategy, MemoryAllocatorKind};
use mooncake_store_master::ha::{
    create_catalog_backed_snapshot_provider, parse_ha_backend_type,
    parse_snapshot_catalog_store_type, parse_snapshot_object_store_type,
    CatalogBackedSnapshotProvider, HABackendSpec, HABackendType, HaError, LeaderCoordinator,
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

use main_args::Args;

mod main_args;
mod main_ha;
mod main_server;
#[cfg(test)]
mod main_tests;

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
        main_ha::run_ha_loop(args).await
    } else {
        main_server::run_standalone(args).await
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
        } else if s == "file-per-key" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::FilePerKey)
        } else if s == "bucket" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::Bucket)
        } else if s == "offset-allocator" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::OffsetAllocator)
        } else if s == "distributed" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::Distributed)
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

fn build_catalog_snapshot_publisher(
    args: &Args,
    cluster_id: &str,
) -> Result<Option<CatalogBackedSnapshotProvider>, Box<dyn std::error::Error>> {
    let Some(object_store_type) = args
        .snapshot_object_store_type
        .as_deref()
        .map(parse_snapshot_object_store_type)
        .transpose()?
    else {
        return Ok(None);
    };
    let catalog_store_type = parse_snapshot_catalog_store_type(&args.snapshot_catalog_store_type)?;
    Ok(Some(create_catalog_backed_snapshot_provider(
        cluster_id.to_string(),
        object_store_type,
        catalog_store_type,
        args.snapshot_backup_dir.clone().map(Into::into),
        args.snapshot_catalog_store_connstring.as_deref(),
    )?))
}

fn publish_catalog_snapshot(
    service: &MasterServiceImpl,
    publisher: &CatalogBackedSnapshotProvider,
    producer_view_version: u64,
    retention_count: usize,
) {
    let snapshot = service.capture_loaded_snapshot(String::new());
    match publisher.publish_loaded_snapshot(&snapshot, producer_view_version) {
        Ok(descriptor) => {
            if let Err(error) = publisher.prune_snapshots(retention_count) {
                warn!("Catalog snapshot retention prune failed: {}", error);
            }
            info!(
                "Catalog snapshot published: id={}, seq={}, view={}",
                descriptor.snapshot_id,
                descriptor.last_included_seq,
                descriptor.producer_view_version
            );
        }
        Err(error) => warn!("Catalog snapshot publish failed: {}", error),
    }
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
// Helpers — 工具函数
// ---------------------------------------------------------------------------

/// 从 CLI 参数构建 MasterRuntimeConfig。
/// Build MasterRuntimeConfig from CLI args.
fn build_runtime_config(args: &Args) -> Result<MasterRuntimeConfig, Box<dyn std::error::Error>> {
    if args.allocation_strategy == "cxl" {
        return Err("allocation_strategy 'cxl' is not supported by the Rust master yet".into());
    }
    Ok(MasterRuntimeConfig {
        allocation_strategy: AllocationStrategy::parse(&args.allocation_strategy)
            .ok_or("allocation_strategy must be 'random' or 'free_ratio_first'")?,
        memory_allocator_kind: MemoryAllocatorKind::parse(&args.memory_allocator)
            .ok_or("memory_allocator must be 'offset' or 'cachelib'")?,
        lease_ttl: Duration::from_millis(args.default_kv_lease_ttl_ms),
        client_live_ttl: Duration::from_secs(args.client_ttl_secs),
        storage_fs_dir: args.root_fs_dir.clone(),
        eviction_high_watermark_ratio: args.eviction_high_watermark_ratio,
        eviction_ratio: args.eviction_ratio,
        enable_offload: args.enable_offload,
        enable_nof: !args.disable_nof,
        offload_on_evict: args.offload_on_evict,
        offload_force_evict: args.offload_force_evict,
        enable_disk_eviction: args.enable_disk_eviction,
        quota_bytes: args.quota_bytes,
        nof_heartbeat_interval: Duration::from_secs(args.nof_heartbeat_interval_sec),
        nof_heartbeat_probe_timeout: Duration::from_millis(args.nof_heartbeat_probe_timeout_ms),
        nof_heartbeat_failures_threshold: args.nof_heartbeat_failures_threshold,
        snapshot_child_timeout: Duration::from_secs(args.snapshot_child_timeout_seconds),
        snapshot_retention_count: args.snapshot_retention_count as usize,
        put_start_discard_timeout: Duration::from_secs(args.put_start_discard_timeout_sec),
        put_start_release_timeout: Duration::from_secs(args.put_start_release_timeout_sec),
        promotion_max_per_heartbeat: args.promotion_max_per_heartbeat,
        max_total_finished_tasks: args.max_total_finished_tasks,
        max_total_pending_tasks: args.max_total_pending_tasks,
        max_total_processing_tasks: args.max_total_processing_tasks,
        pending_task_timeout: Duration::from_secs(args.pending_task_timeout_secs),
        processing_task_timeout: Duration::from_secs(args.processing_task_timeout_secs),
        max_task_retry_attempts: args.max_task_retry_attempts,
        cluster_id: resolve_cluster_id(args),
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
        HABackendType::K8s => Ok(LeaderCoordinator::new_k8s(&spec.connstring)?),
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

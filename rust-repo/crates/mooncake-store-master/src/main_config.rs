use crate::allocator::{AllocationStrategy, MemoryAllocatorKind};
use crate::ha::{
    create_catalog_backed_snapshot_provider, parse_ha_backend_type,
    parse_snapshot_catalog_store_type, parse_snapshot_object_store_type,
    CatalogBackedSnapshotProvider, HABackendSpec, HABackendType, HaError, LeaderCoordinator,
    MasterServiceSupervisor, MasterServiceSupervisorConfig,
};
use crate::main_args::Args;
use crate::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub fn parse_snapshot_config(
    args: &Args,
) -> (
    Option<crate::storage_backend::StorageBackendType>,
    Option<std::path::PathBuf>,
) {
    let backend = args.snapshot_backend_type.as_deref().and_then(|s| {
        if s == "local-disk" {
            Some(crate::storage_backend::StorageBackendType::LocalDisk)
        } else if s == "hf3fs" {
            Some(crate::storage_backend::StorageBackendType::Hf3fs)
        } else if s == "file-per-key" {
            Some(crate::storage_backend::StorageBackendType::FilePerKey)
        } else if s == "bucket" {
            Some(crate::storage_backend::StorageBackendType::Bucket)
        } else if s == "offset-allocator" {
            Some(crate::storage_backend::StorageBackendType::OffsetAllocator)
        } else if s == "distributed" {
            Some(crate::storage_backend::StorageBackendType::Distributed)
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

pub fn build_catalog_snapshot_publisher(
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

pub fn publish_catalog_snapshot(
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

pub fn new_supervisor(
    ha_spec: &HABackendSpec,
    config: &MasterServiceSupervisorConfig,
    service: Arc<MasterServiceImpl>,
) -> MasterServiceSupervisor {
    let controller = service.create_ha_standby_controller(ha_spec.clone(), config.clone());
    MasterServiceSupervisor::new(Box::new(controller))
}

pub fn build_master_service(
    snapshot_backend_type: Option<crate::storage_backend::StorageBackendType>,
    snapshot_dir: Option<std::path::PathBuf>,
    runtime_config: MasterRuntimeConfig,
) -> Arc<MasterServiceImpl> {
    Arc::new(MasterServiceImpl::new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir,
        runtime_config,
    ))
}

pub fn snapshot_dir_for_cluster(
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

pub fn build_runtime_config(
    args: &Args,
) -> Result<MasterRuntimeConfig, Box<dyn std::error::Error>> {
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
        enable_tenant_quota: args.enable_tenant_quota,
        default_tenant_quota_bytes: args.default_tenant_quota_bytes,
        tenant_quota_pool_capacity_bytes: args.tenant_quota_pool_capacity_bytes,
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

pub async fn create_coordinator(
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

pub fn build_ha_spec(args: &Args) -> Result<HABackendSpec, HaError> {
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

pub fn resolve_cluster_id(args: &Args) -> String {
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

pub fn ensure_supported_rpc_protocol() -> Result<(), Box<dyn std::error::Error>> {
    validate_rpc_protocol(std::env::var("MC_RPC_PROTOCOL").ok().as_deref())
}

pub fn validate_rpc_protocol(protocol: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    match protocol {
        Some("rdma") => Err(Box::new(HaError::UnavailableInCurrentMode(
            "Rust tonic master server does not support coro_rpc RDMA init_ibv".into(),
        ))),
        _ => Ok(()),
    }
}

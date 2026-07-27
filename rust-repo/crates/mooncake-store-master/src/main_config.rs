use crate::allocator::{AllocationStrategy, MemoryAllocatorKind};
use crate::ha::{
    CatalogBackedSnapshotProvider, HABackendSpec, HABackendType, HaError, K8sPodIdentity,
    LeaderCoordinator, MasterServiceSupervisor, MasterServiceSupervisorConfig,
    create_catalog_backed_snapshot_provider, parse_ha_backend_type,
    parse_snapshot_catalog_store_type, parse_snapshot_object_store_type,
};
use crate::main_args::Args;
use crate::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub fn parse_snapshot_config(
    args: &Args,
) -> Result<
    (
        Option<crate::storage_backend::StorageBackendType>,
        Option<std::path::PathBuf>,
    ),
    HaError,
> {
    let backend = args
        .snapshot_backend_type
        .as_deref()
        .map(|value| match value {
            "local-disk" => Ok(crate::storage_backend::StorageBackendType::LocalDisk),
            "hf3fs" => Ok(crate::storage_backend::StorageBackendType::Hf3fs),
            "file-per-key" => Ok(crate::storage_backend::StorageBackendType::FilePerKey),
            "bucket" => Ok(crate::storage_backend::StorageBackendType::Bucket),
            "offset-allocator" => Ok(crate::storage_backend::StorageBackendType::OffsetAllocator),
            "distributed" => Ok(crate::storage_backend::StorageBackendType::Distributed),
            other => Err(HaError::InvalidParams(format!(
                "unknown snapshot backend type: {other}"
            ))),
        })
        .transpose()?;
    let dir = args
        .snapshot_backup_dir
        .clone()
        .map(std::path::PathBuf::from);
    if backend.is_some() != dir.is_some() {
        return Err(HaError::InvalidParams(
            "--snapshot-backend-type and --snapshot-backup-dir must be configured together".into(),
        ));
    }
    Ok((backend, dir))
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

pub fn preflight_snapshot_pipeline(
    args: &Args,
    cluster_id: &str,
    service: &MasterServiceImpl,
) -> Result<Option<CatalogBackedSnapshotProvider>, Box<dyn std::error::Error>> {
    let native_writer_available = service.preflight_snapshot_writer()?;
    let catalog_publisher = build_catalog_snapshot_publisher(args, cluster_id)?;
    if let Some(publisher) = &catalog_publisher {
        publisher.preflight()?;
    }
    if args.enable_snapshot && !native_writer_available && catalog_publisher.is_none() {
        return Err(HaError::InvalidParams(
            "--enable-snapshot requires a native snapshot backend or snapshot object store".into(),
        )
        .into());
    }
    Ok(catalog_publisher)
}

pub fn publish_catalog_snapshot(
    service: &MasterServiceImpl,
    publisher: &CatalogBackedSnapshotProvider,
    producer_view_version: u64,
    retention_count: usize,
) {
    if !service.is_service_available() {
        warn!("Catalog snapshot publish skipped because the master is not serving");
        return;
    }
    if service.is_service_fenced() {
        warn!("Catalog snapshot publish skipped because the master is durability-fenced");
        return;
    }
    let snapshot = service.capture_loaded_snapshot(String::new());
    if !service.is_service_available() {
        warn!("Catalog snapshot publish skipped because the master stopped serving during capture");
        return;
    }
    if service.is_service_fenced() {
        warn!(
            "Catalog snapshot publish skipped because the master became durability-fenced during capture"
        );
        return;
    }
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
) -> Result<Arc<MasterServiceImpl>, HaError> {
    Ok(Arc::new(
        MasterServiceImpl::try_new_standby_with_runtime_config(
            snapshot_backend_type,
            snapshot_dir,
            runtime_config,
        )?,
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
    if !args.enable_cxl && args.allocation_strategy == "cxl" {
        return Err("allocation_strategy 'cxl' requires --enable-cxl".into());
    }
    if args.enable_cxl {
        if args.cxl_path.trim().is_empty() {
            return Err("cxl_path must not be empty when CXL is enabled".into());
        }
        if args.cxl_size == 0 || args.cxl_size % crate::allocator::CACHELIB_SLAB_SIZE != 0 {
            return Err(format!(
                "cxl_size must be positive and aligned to {}",
                crate::allocator::CACHELIB_SLAB_SIZE
            )
            .into());
        }
        if args.cxl_size > crate::allocator::CACHELIB_MAX_SEGMENT_SIZE {
            return Err(format!(
                "cxl_size exceeds the Cachelib u32 slab index capacity {}",
                crate::allocator::CACHELIB_MAX_SEGMENT_SIZE
            )
            .into());
        }
    }
    if args.offloading_queue_limit == 0 || args.offloading_queue_limit > 100_000_000 {
        return Err("offloading_queue_limit must be between 1 and 100000000".into());
    }
    if args.offload_cap_ratio < 0.0 || args.offload_cap_ratio > 1.0 {
        return Err("offload_cap_ratio must be between 0.0 and 1.0".into());
    }
    if !(0.0..=1.0).contains(&args.nof_eviction_high_watermark_ratio) {
        return Err("nof_eviction_high_watermark_ratio must be between 0.0 and 1.0".into());
    }
    if !(0.0..=1.0).contains(&args.nof_eviction_ratio) {
        return Err("nof_eviction_ratio must be between 0.0 and 1.0".into());
    }
    if args.promotion_queue_limit == 0 {
        return Err("promotion_queue_limit must be positive".into());
    }
    Ok(MasterRuntimeConfig {
        // ── 分配策略 / allocation strategy ──
        // 段选择：random、free_ratio_first、ssd_free_ratio_first 或 local_first
        allocation_strategy: if args.enable_cxl {
            AllocationStrategy::Cxl
        } else {
            AllocationStrategy::parse(&args.allocation_strategy)
                .ok_or("allocation_strategy must be 'random', 'free_ratio_first', 'ssd_free_ratio_first', or 'local_first'")?
        },
        // 段内分配器：offset（连续分配）或 cachelib（slab + class）
        memory_allocator_kind: if args.enable_cxl {
            MemoryAllocatorKind::CachelibLike
        } else {
            MemoryAllocatorKind::parse(&args.memory_allocator)
                .ok_or("memory_allocator must be 'offset' or 'cachelib'")?
        },
        enable_cxl: args.enable_cxl,
        cxl_path: args.cxl_path.clone(),
        cxl_size: args.cxl_size,

        // ── 租约 & 超时 / lease & timeout ──
        // KV 对象默认 lease TTL：PutEnd/GetReplicaList 加时，超时后允许驱逐
        lease_ttl: Duration::from_millis(args.default_kv_lease_ttl_ms),
        // 客户端心跳 TTL：超时未 ping 视为下线，后台清理其 segment 和副本
        client_live_ttl: Duration::from_secs(args.client_ttl_secs),
        // 未完成 PutStart 超时，新同 key PutStart 可抢占
        put_start_discard_timeout: Duration::from_secs(args.put_start_discard_timeout_sec),
        // 副本延迟释放时间（600s），防止 RDMA 还在传输时回收内存
        put_start_release_timeout: Duration::from_secs(args.put_start_release_timeout_sec),

        // ── 淘汰 / eviction ──
        // 内存使用率超过此水位（0.95）触发驱逐
        eviction_high_watermark_ratio: args.eviction_high_watermark_ratio,
        // 每次驱逐释放的内存比例（0.05 = 5%）
        eviction_ratio: args.eviction_ratio,
        // NoF 使用率和驱逐比例独立于 Memory。
        nof_eviction_high_watermark_ratio: args.nof_eviction_high_watermark_ratio,
        nof_eviction_ratio: args.nof_eviction_ratio,
        // 淘汰时先把数据下沉到本地磁盘再驱逐内存
        offload_on_evict: args.offload_on_evict,
        // 与 C++ 一致：第一轮保护 soft pin，可配置第二轮允许驱逐。
        allow_evict_soft_pinned_objects: args.allow_evict_soft_pinned_objects,
        // offload 无法入队或达到强制阈值时直接驱逐。
        offload_force_evict: args.offload_force_evict,
        // 每个本地磁盘 segment 的 offload 队列上限
        offloading_queue_limit: args.offloading_queue_limit,
        // 单轮 eviction 最多入队多少比例的 offload 任务
        offload_cap_ratio: args.offload_cap_ratio,
        // 透传给客户端：是否启用磁盘层淘汰
        enable_disk_eviction: args.enable_disk_eviction,
        // 透传给客户端：存储配额上限（字节）
        quota_bytes: args.quota_bytes,

        // ── Offload / Promotion 开关 ──
        // 全局 offload 开关：关闭后不可注册本地磁盘 offload segment
        enable_offload: args.enable_offload,
        // 全局 NoF 开关：关闭后拒绝 NoF segment 和 NoF replica
        enable_nof: args.enable_nof,

        // ── 租户配额 / tenant quota ──
        enable_tenant_quota: args.enable_multi_tenants || args.enable_tenant_quota,
        default_tenant_quota_bytes: args.default_tenant_quota_bytes,
        tenant_quota_connector_type: args.tenant_quota_connector_type.clone(),
        tenant_quota_connector_uri: args.tenant_quota_connector_uri.clone(),
        // 计算有效配额的容量池（0 = 用内存总容量）
        tenant_quota_pool_capacity_bytes: args.tenant_quota_pool_capacity_bytes,

        // ── HA 快照 / snapshot ──
        // 快照数据目录
        storage_fs_dir: args.root_fs_dir.clone(),
        // 单次异步快照保存超时
        snapshot_child_timeout: Duration::from_secs(args.snapshot_child_timeout_seconds),
        // 保留的历史快照数量
        snapshot_retention_count: args.snapshot_retention_count as usize,
        // 集群标识符，追加到 storage_fs_dir 末尾作为子目录
        cluster_id: resolve_cluster_id(args),

        // ── Promotion / 热数据升温 ──
        // 与 C++ 默认一致：关闭；启用后命中次数达到阈值才进入有界队列
        promotion_on_hit: args.promotion_on_hit,
        promotion_admission_threshold: args.promotion_admission_threshold,
        promotion_queue_limit: args.promotion_queue_limit,
        // 单次心跳最多返回给一个客户端的 promotion 任务数
        promotion_max_per_heartbeat: args.promotion_max_per_heartbeat,

        // ── 任务队列容量 / task queue capacity ──
        // 已完成的 client task 保留上限（超过后淘汰最旧的）
        max_total_finished_tasks: args.max_total_finished_tasks,
        // 待处理任务上限
        max_total_pending_tasks: args.max_total_pending_tasks,
        // 并发处理中任务上限
        max_total_processing_tasks: args.max_total_processing_tasks,
        // Pending 任务超时（0 = 禁用过期）
        pending_task_timeout: Duration::from_secs(args.pending_task_timeout_secs),
        // Processing 任务超时（0 = 禁用过期）
        processing_task_timeout: Duration::from_secs(args.processing_task_timeout_secs),
        // 新创建任务的默认重试次数
        max_task_retry_attempts: args.max_task_retry_attempts,

        // ── KV Events / indexer publisher ──
        // Optional RFC #1527 event stream consumed by global KV indexers.
        kv_event_config: crate::kv_event::KvEventConfig {
            enabled: args.enable_kv_events,
            bind_endpoint: args.kv_events_bind_endpoint.clone(),
            model_name: args.kv_events_model_name.clone(),
            backend_id: args.kv_events_backend_id.clone(),
            tenant_id: args.kv_events_tenant_id.clone(),
            additional_salt: args.kv_events_additional_salt.clone(),
            lora_name: args.kv_events_lora_name.clone(),
            block_size: args.kv_events_block_size,
            dp_rank: args.kv_events_dp_rank,
            emit_legacy_compat: args.kv_events_emit_legacy_compat,
            emit_object_key: args.kv_events_emit_object_key,
            queue_capacity: args.kv_events_queue_capacity,
        },

        // ── NoF 心跳探测 / NoF heartbeat probe ──
        // 探测间隔
        nof_heartbeat_interval: Duration::from_secs(args.nof_heartbeat_interval_sec),
        // 单次探测超时
        nof_heartbeat_probe_timeout: Duration::from_millis(args.nof_heartbeat_probe_timeout_ms),
        // 连续失败超过此值则卸载 NoF segment
        nof_heartbeat_failures_threshold: args.nof_heartbeat_failures_threshold,

        // 其余字段使用默认值（soft_pin_ttl、eviction_interval、reaper_interval 等）
        // remaining fields use defaults (soft_pin_ttl, eviction_interval, reaper_interval, etc.)
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
        HABackendType::K8s => Ok(LeaderCoordinator::new_k8s(
            &spec.connstring,
            spec.pod_identity.clone(),
        )?),
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
        pod_identity: build_k8s_pod_identity(args, backend_type),
    })
}

/// Validate the complete production HA capability set before connecting to a
/// coordinator. Accepting election alone would let Redis/Kubernetes repeatedly
/// acquire and release leadership because no shared oplog can be installed.
pub fn validate_ha_backend_for_serving(spec: &HABackendSpec) -> Result<(), HaError> {
    let capabilities = spec.backend_type.capabilities();
    if !capabilities.discovery || !capabilities.leader_election {
        return Err(HaError::InvalidParams(format!(
            "HA backend {} does not implement discovery and leader election",
            spec.backend_type.as_str()
        )));
    }
    if !capabilities.shared_oplog {
        return Err(HaError::UnavailableInCurrentMode(format!(
            "HA backend {} supports discovery/election but has no shared ordered oplog; \
             production HA currently requires etcd",
            spec.backend_type.as_str()
        )));
    }
    Ok(())
}

fn build_k8s_pod_identity(args: &Args, backend_type: HABackendType) -> Option<K8sPodIdentity> {
    if backend_type != HABackendType::K8s {
        return None;
    }
    let pod_name = resolve_optional_arg_or_env(&args.pod_name, "POD_NAME")?;
    let namespace = resolve_optional_arg_or_env(&args.pod_namespace, "POD_NAMESPACE")?;
    Some(K8sPodIdentity {
        namespace,
        pod_name,
    })
}

fn resolve_optional_arg_or_env(value: &Option<String>, env_key: &str) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var(env_key)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
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
            "Rust master control-plane RPC does not support RDMA transport".into(),
        ))),
        _ => Ok(()),
    }
}

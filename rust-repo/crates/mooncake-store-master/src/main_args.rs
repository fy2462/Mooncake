use clap::Parser;

// CLI 参数定义，使用 clap derive 宏。
// CLI argument definitions via clap derive macro.
#[derive(Parser, Debug)]
#[command(
    name = "mooncake-master",
    version,
    about = "Mooncake distributed KV cache — Master Service"
)]
pub struct Args {
    /// gRPC 服务监听地址 / gRPC server bind address
    #[arg(long, default_value = "0.0.0.0")]
    pub rpc_address: String,

    /// gRPC 服务监听端口 / gRPC server port
    #[arg(long, default_value_t = 50051)]
    pub rpc_port: u16,

    /// HTTP metadata 服务监听地址 / HTTP metadata server bind address
    #[arg(long, default_value = "0.0.0.0")]
    pub http_metadata_server_host: String,

    /// HTTP metadata 服务监听端口 / HTTP metadata server port
    #[arg(long, default_value_t = 8080)]
    pub http_metadata_server_port: u16,

    /// Prometheus metrics 暴露端口 / Metrics HTTP server port
    #[arg(long, default_value_t = 9003)]
    pub metrics_port: u16,

    /// gRPC 服务线程数 / Number of gRPC server threads
    #[arg(long, default_value_t = 16)]
    pub rpc_thread_num: usize,

    /// Segment 分配策略: "random", "free_ratio_first", "ssd_free_ratio_first" 或 "local_first"；"cxl" 当前会明确拒绝
    /// Segment allocation strategy: "random", "free_ratio_first", "ssd_free_ratio_first", or "local_first"; "cxl" is explicitly unsupported
    #[arg(long, default_value = "random")]
    pub allocation_strategy: String,

    /// Segment 内内存分配器: "offset" 或 "cachelib"
    /// Memory allocator within segment: "offset" or "cachelib"
    #[arg(long, default_value = "offset")]
    pub memory_allocator: String,

    /// KV 对象默认租约 TTL（毫秒）/ Default KV lease TTL in milliseconds
    #[arg(long, default_value_t = 10_000)]
    pub default_kv_lease_ttl_ms: u64,

    /// Client heartbeat TTL in seconds.
    #[arg(
        long,
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub client_ttl_secs: u64,

    /// Root directory for storage backend paths returned to clients.
    #[arg(long, default_value = "")]
    pub root_fs_dir: String,

    /// 驱逐高水位比例 (0.0~1.0)，超过后触发自动驱逐
    /// Eviction high watermark ratio: auto-eviction triggers above this
    #[arg(long, default_value_t = 0.90)]
    pub eviction_high_watermark_ratio: f64,

    /// 每次驱逐释放的内存比例 (0.0~1.0)
    /// Fraction of memory to free per eviction cycle
    #[arg(long, default_value_t = 0.05)]
    pub eviction_ratio: f64,

    /// 是否启用全局 offload/promotion 功能
    /// Whether to enable global offload/promotion workflows
    #[arg(long)]
    pub enable_offload: bool,

    /// 是否启用 NoF (NVMe-oF) 功能
    /// Enable NoF (NVMe-oF) workflows
    #[arg(long, default_value_t = true)]
    pub enable_nof: bool,

    /// 驱逐时是否触发 offload（下沉到本地磁盘）
    /// Whether to offload to local disk on eviction
    #[arg(long)]
    pub offload_on_evict: bool,

    /// 是否强制驱逐（即使对象被 soft_pin 也驱逐）
    /// Force eviction even for soft-pinned objects
    #[arg(long)]
    pub offload_force_evict: bool,

    /// Maximum pending offload objects per local disk segment.
    #[arg(long, default_value_t = 50_000)]
    pub offloading_queue_limit: usize,

    /// Per-cycle offload cap as a fraction of offloading_queue_limit.
    #[arg(long, default_value_t = 0.5)]
    pub offload_cap_ratio: f64,

    /// Enable disk eviction feature for storage backend.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub enable_disk_eviction: bool,

    /// Quota for storage backend in bytes; zero means client default.
    #[arg(long, default_value_t = 0)]
    pub quota_bytes: u64,

    /// Enable strict multi-tenant namespace and quota admission.
    #[arg(long)]
    pub enable_multi_tenants: bool,

    /// Deprecated alias for --enable-multi-tenants.
    #[arg(long)]
    pub enable_tenant_quota: bool,

    /// Deprecated compatibility knob; strict mode ignores default tenant quotas.
    #[arg(long, default_value_t = 0)]
    pub default_tenant_quota_bytes: u64,

    /// Tenant quota policy connector type: "file" is supported by Rust master.
    #[arg(long, default_value = "file")]
    pub tenant_quota_connector_type: String,

    /// Tenant quota policy connector URI, usually a YAML file path.
    #[arg(long, default_value = "")]
    pub tenant_quota_connector_uri: String,

    /// Capacity used for effective tenant quota allocation. Zero means memory capacity.
    #[arg(long, default_value_t = 0)]
    pub tenant_quota_pool_capacity_bytes: u64,

    /// NoF heartbeat probe interval in seconds.
    #[arg(
        long,
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub nof_heartbeat_interval_sec: u64,

    /// NoF heartbeat probe timeout in milliseconds.
    #[arg(
        long,
        default_value_t = 1000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub nof_heartbeat_probe_timeout_ms: u64,

    /// Consecutive NoF heartbeat failures before unmounting a NoF segment.
    #[arg(
        long,
        default_value_t = 3,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub nof_heartbeat_failures_threshold: u32,

    /// Timeout for discarding uncompleted PutStart operations in seconds.
    #[arg(long, default_value_t = 30)]
    pub put_start_discard_timeout_sec: u64,

    /// Timeout for releasing uncompleted PutStart allocations in seconds.
    #[arg(long, default_value_t = 600)]
    pub put_start_release_timeout_sec: u64,

    /// 每次 promotion heartbeat 返回给单个客户端的最大任务数
    /// Max promotion tasks returned to one client per heartbeat
    #[arg(long, default_value_t = 1)]
    pub promotion_max_per_heartbeat: usize,

    /// Maximum number of completed client tasks retained by the master.
    #[arg(long, default_value_t = 10_000)]
    pub max_total_finished_tasks: usize,

    /// Maximum number of pending client tasks.
    #[arg(long, default_value_t = 10_000)]
    pub max_total_pending_tasks: usize,

    /// Maximum number of concurrently processing client tasks.
    #[arg(long, default_value_t = 10_000)]
    pub max_total_processing_tasks: usize,

    /// Pending client task timeout in seconds; zero disables expiration.
    #[arg(long, default_value_t = 300)]
    pub pending_task_timeout_secs: u64,

    /// Processing client task timeout in seconds; zero disables expiration.
    #[arg(long, default_value_t = 300)]
    pub processing_task_timeout_secs: u64,

    /// Retry limit attached to newly submitted client tasks.
    #[arg(long, default_value_t = 10)]
    pub max_task_retry_attempts: u32,

    /// Enable RFC #1527 KV cache event publisher over ZMQ.
    #[arg(long)]
    pub enable_kv_events: bool,

    /// ZMQ PUB bind endpoint for KV events, e.g. tcp://0.0.0.0:5557.
    #[arg(long, default_value = "tcp://0.0.0.0:5557")]
    pub kv_events_bind_endpoint: String,

    /// Deprecated: model identity is supplied through indexer registration, not master events.
    #[arg(long, default_value = "")]
    pub kv_events_model_name: String,

    /// backend_id for published KV events (cache owner identity).
    #[arg(long, default_value = "")]
    pub kv_events_backend_id: String,

    /// Deprecated: tenant_id is taken from each object on events.
    #[arg(long, default_value = "default")]
    pub kv_events_tenant_id: String,

    /// Deprecated: additional_salt is supplied through indexer registration, not master events.
    #[arg(long, default_value = "")]
    pub kv_events_additional_salt: String,

    /// Deprecated: LoRA context is not stamped by the master publisher.
    #[arg(long, default_value = "")]
    pub kv_events_lora_name: String,

    /// Deprecated: block_size is supplied through indexer registration, not master events.
    #[arg(long, default_value_t = 0)]
    pub kv_events_block_size: u32,

    /// Deprecated: dp_rank is supplied through indexer registration, not master events.
    #[arg(long, default_value_t = 0)]
    pub kv_events_dp_rank: u32,

    /// Include vLLM/SGLang-compatible type/block_hashes fields.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub kv_events_emit_legacy_compat: bool,

    /// Include Mooncake object_key in published KV events.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub kv_events_emit_object_key: bool,

    /// Max pending events in the async publisher queue; oldest is dropped when full.
    #[arg(long, default_value_t = 65_536)]
    pub kv_events_queue_capacity: usize,

    /// 是否启用高可用 (HA) 模式 / Enable High Availability (HA) mode
    #[arg(long)]
    pub enable_ha: bool,

    /// etcd 端点列表，分号分隔（HA 模式） / etcd endpoints, semicolon-separated (HA mode)
    #[arg(long)]
    pub etcd_endpoints: Option<String>,

    /// HA backend type: "etcd", "redis", or "k8s".
    /// HA 后端类型："etcd"、"redis" 或 "k8s"。
    #[arg(long, default_value = "etcd")]
    pub ha_backend_type: String,

    /// HA backend connection string. Etcd may fall back to --etcd-endpoints.
    /// For k8s, use "namespace/lease" (or "lease" for the default namespace).
    /// HA 后端连接串。etcd 可回退到 --etcd-endpoints。
    /// k8s 使用 "namespace/lease"（或 "lease" 表示默认 namespace）。
    #[arg(long)]
    pub ha_backend_connstring: Option<String>,

    /// Cluster id / namespace for HA keys and oplog paths.
    /// HA key 和 oplog 路径使用的 cluster id / namespace。
    #[arg(long)]
    pub cluster_id: Option<String>,

    /// 快照后端类型: "local-disk"、"hf3fs"、"file-per-key"、"bucket"、"offset-allocator" 或 "distributed"
    /// Snapshot backend type: "local-disk", "hf3fs", "file-per-key", "bucket", "offset-allocator", or "distributed"
    #[arg(long)]
    pub snapshot_backend_type: Option<String>,

    /// 快照备份目录路径 / Snapshot backup directory path
    #[arg(long)]
    pub snapshot_backup_dir: Option<String>,

    /// Snapshot payload store used by the C++-compatible catalog provider: local or s3.
    #[arg(long)]
    pub snapshot_object_store_type: Option<String>,

    /// Snapshot catalog used to resolve the latest C++-compatible snapshot.
    #[arg(long, default_value = "embedded")]
    pub snapshot_catalog_store_type: String,

    /// Snapshot catalog connection string. Redis falls back to the HA connection string.
    #[arg(long)]
    pub snapshot_catalog_store_connstring: Option<String>,

    /// Enable periodic snapshots.
    #[arg(long)]
    pub enable_snapshot: bool,

    /// Periodic snapshot interval in seconds.
    #[arg(
        long,
        default_value_t = 600,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub snapshot_interval_seconds: u64,

    /// Timeout for one snapshot save task in seconds.
    #[arg(
        long,
        default_value_t = 300,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub snapshot_child_timeout_seconds: u64,

    /// Number of historical snapshot files to retain.
    #[arg(
        long,
        default_value_t = 2,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub snapshot_retention_count: u64,

    /// HA lease TTL in seconds
    /// HA 租约 TTL，单位秒
    #[arg(long, default_value_t = 30)]
    pub ha_lease_ttl_secs: i64,

    /// Pod name for K8s label-based leader routing. Defaults to POD_NAME.
    #[arg(long)]
    pub pod_name: Option<String>,

    /// Pod namespace for K8s label-based leader routing. Defaults to POD_NAMESPACE.
    #[arg(long)]
    pub pod_namespace: Option<String>,
}

use super::*;
use crate::kv_event::KvEventConfig;

/// MasterRuntimeConfig: 运行时配置参数，控制 lease TTL、eviction 水位线、promotion 策略等。
/// 所有 Duration 字段使用 std::time::Duration 表示。
///
/// Runtime configuration: controls lease TTL, eviction watermarks, promotion strategy, etc.
/// All Duration fields are represented as std::time::Duration.
#[derive(Debug, Clone)]
pub struct MasterRuntimeConfig {
    /// 未完成的 PutStart 超过此时间后被新的同 key PutStart 丢弃。
    /// Unfinished PutStart is discarded if it exceeds this timeout when a new PutStart for the same key arrives.
    pub put_start_discard_timeout: Duration,
    /// 丢弃或 release 的副本延迟释放时间，防止仍在传输中的 RDMA 访问已回收内存。
    /// Delayed release time for discarded/released replicas — prevents RDMA in-flight from accessing reclaimed memory.
    pub put_start_release_timeout: Duration,
    /// 段选择策略：Random（随机）或 FreeRatioFirst（空闲率优先）。
    /// Segment selection strategy: Random or FreeRatioFirst.
    pub allocation_strategy: AllocationStrategy,
    /// 段内内存分配器：Offset（简单连续分配）或 CachelibLike（slab + class 分配）。
    /// Memory allocator within segment: Offset (simple sequential) or CachelibLike (slab + class).
    pub memory_allocator_kind: MemoryAllocatorKind,
    /// Optional C++-compatible active partition-node budget for each Offset segment.
    /// `None` preserves Rust's historical unlimited behavior.
    pub offset_max_allocation_nodes: Option<u64>,
    /// Enable the single shared CXL allocator and CXL-only segment aliases.
    pub enable_cxl: bool,
    /// DAX device identity used by CXL clients and deployment validation.
    pub cxl_path: String,
    /// Capacity of the single shared CXL allocator.
    pub cxl_size: u64,
    /// 是否开启 promotion-on-hit：读磁盘副本时自动将热点对象提升到内存。
    /// Whether promotion-on-hit is enabled: auto-promote hot objects from disk to memory on read.
    pub promotion_on_hit: bool,
    /// 提升准入阈值：对象被访问达到此次数后才放入提升队列。
    /// Promotion admission threshold: object must be accessed this many times before entering the promotion queue.
    pub promotion_admission_threshold: u8,
    /// 提升队列最大长度，超过后新的提升请求被丢弃。
    /// Maximum promotion queue length; new promotion requests are dropped when exceeded.
    pub promotion_queue_limit: usize,
    /// 单次 PromotionObjectHeartbeat 最多返回给一个客户端的任务数。
    /// Maximum promotion tasks returned to one client per heartbeat.
    pub promotion_max_per_heartbeat: usize,
    /// 后台 reaper 轮询间隔，用于清理过期的 offload / promotion / PutStart 任务。
    /// Background reaper poll interval for cleaning up expired offload/promotion/PutStart tasks.
    pub reaper_interval: Duration,
    /// 自动淘汰检查的轮询间隔。
    /// Automatic eviction check poll interval.
    pub eviction_interval: Duration,
    /// 触发淘汰的内存使用率水位（0.0 ~ 1.0），超过后启动淘汰。
    /// Memory usage ratio watermark (0.0 ~ 1.0); eviction triggers when exceeded.
    pub eviction_high_watermark_ratio: f64,
    /// 每次淘汰尝试释放的内存比例（0.0 ~ 1.0）。
    /// Fraction of memory to free per eviction cycle (0.0 ~ 1.0).
    pub eviction_ratio: f64,
    /// NoF usage high watermark, independent from Memory pressure.
    pub nof_eviction_high_watermark_ratio: f64,
    /// Target object fraction for each NoF eviction cycle.
    pub nof_eviction_ratio: f64,
    /// 软锁定（soft pin）对象的租约时长，过期后软锁定失效但仍优先保留。
    /// Soft-pin TTL: after expiry the soft pin is released but the object is still preferred.
    pub soft_pin_ttl: Duration,
    /// KV 对象默认租约时长：PutEnd / GetReplicaList 授时，淘汰时超过此 TTL 的对象允许驱逐。
    /// Default KV lease TTL: granted at PutEnd/GetReplicaList; objects exceeding this are evictable.
    pub lease_ttl: Duration,
    /// 全局 offload 开关；关闭时拒绝注册本地磁盘 offload segment。
    /// Global offload gate; local disk offload segments cannot register when disabled.
    pub enable_offload: bool,
    /// 全局 NoF 开关；关闭时拒绝 NoF segment 和 NoF replica 操作。
    /// Global NoF gate; NoF segment and NoF replica operations are unavailable when disabled.
    pub enable_nof: bool,
    /// 淘汰时是否触发 offload（将内存副本写入本地磁盘）。
    /// Whether to trigger offload (write memory replicas to local disk) on eviction.
    pub offload_on_evict: bool,
    /// Whether a second eviction pass may select objects whose soft pin is
    /// still active. This is independent from forcing eviction when offload
    /// admission fails or reaches its cap.
    pub allow_evict_soft_pinned_objects: bool,
    /// offload 无法入队时是否强制驱逐 Memory 副本。
    /// Whether to force Memory eviction when offload cannot be queued.
    pub offload_force_evict: bool,
    /// Maximum pending offload objects per local disk segment.
    pub offloading_queue_limit: usize,
    /// Per-eviction-cycle offload cap as a fraction of offloading_queue_limit.
    pub offload_cap_ratio: f64,
    /// 客户端心跳 TTL，超过此时间未 ping 的客户端视为下线。
    /// Client heartbeat TTL: clients not pinging within this period are considered offline.
    pub client_live_ttl: Duration,
    /// 客户端监控（client monitor）轮询间隔，检查下线客户端并释放其资源。
    /// Client monitor poll interval: checks for offline clients and releases their resources.
    pub client_monitor_interval: Duration,
    /// HA 快照存储目录路径。
    /// HA snapshot storage directory path.
    pub storage_fs_dir: String,
    /// Cluster ID appended to storage_fs_dir for client-visible fsdir.
    /// 客户端可见 fsdir 使用的 cluster ID。
    pub cluster_id: String,
    /// 透传给客户端的磁盘淘汰开关，master 端淘汰逻辑暂未消费此字段。
    /// Disk eviction flag forwarded to clients; Master eviction logic does not currently consume this.
    pub enable_disk_eviction: bool,
    /// 透传给客户端的存储配额（字节），master 端暂未实现配额限流。
    /// Storage quota in bytes forwarded to clients; Master does not currently enforce quota.
    pub quota_bytes: u64,
    /// Enables strict multi-tenant quota admission and accounting.
    pub enable_tenant_quota: bool,
    /// Deprecated compatibility field; strict mode ignores default tenant quotas.
    pub default_tenant_quota_bytes: u64,
    /// Tenant quota policy connector type.
    pub tenant_quota_connector_type: String,
    /// Tenant quota policy connector URI.
    pub tenant_quota_connector_uri: String,
    /// Capacity used to compute effective tenant quotas. Zero means memory capacity.
    pub tenant_quota_pool_capacity_bytes: u64,
    /// Enable remote source (S3) fallback for cache misses.
    /// 启用远端源（S3）回源：缓存未命中时从远端拉取数据。
    pub remote_source_enabled: bool,
    /// TTL for a pending remote pull entry before it is considered stale.
    /// 远端拉取条目的 TTL：超过后视为过期，允许其他节点重新拉取。
    pub remote_pull_ttl: Duration,
    /// NoF 心跳探测间隔 / NoF heartbeat probe interval.
    pub nof_heartbeat_interval: Duration,
    /// NoF 心跳探测超时 / NoF heartbeat probe timeout.
    pub nof_heartbeat_probe_timeout: Duration,
    /// NoF 心跳连续失败阈值，超过后卸载 segment / NoF heartbeat consecutive failure threshold; unmounts segment when exceeded.
    pub nof_heartbeat_failures_threshold: u32,
    /// Timeout for a single asynchronous snapshot save task.
    pub snapshot_child_timeout: Duration,
    /// Number of historical snapshots retained after successful saves.
    pub snapshot_retention_count: usize,
    /// Maximum retained finished client tasks.
    pub max_total_finished_tasks: usize,
    /// Maximum pending client tasks.
    pub max_total_pending_tasks: usize,
    /// Maximum concurrently processing client tasks.
    pub max_total_processing_tasks: usize,
    /// Pending task timeout; zero disables expiration.
    pub pending_task_timeout: Duration,
    /// Processing task timeout; zero disables expiration.
    pub processing_task_timeout: Duration,
    /// Retry limit copied into newly submitted tasks.
    pub max_task_retry_attempts: u32,
    /// Optional RFC #1527 KV events publisher config.
    pub kv_event_config: KvEventConfig,
}

/// 默认运行时配置：生产环境建议通过 CLI 参数覆盖这些值。
/// Default runtime config; override via CLI args for production.
impl Default for MasterRuntimeConfig {
    fn default() -> Self {
        Self {
            put_start_discard_timeout: Duration::from_secs(30),
            put_start_release_timeout: Duration::from_secs(600),
            allocation_strategy: AllocationStrategy::Random,
            memory_allocator_kind: MemoryAllocatorKind::Offset,
            offset_max_allocation_nodes: None,
            enable_cxl: false,
            cxl_path: "/dev/dax0.0".to_string(),
            cxl_size: 8 * 1024 * 1024 * 1024,
            promotion_on_hit: false,
            promotion_admission_threshold: 2,
            promotion_queue_limit: 50_000,
            promotion_max_per_heartbeat: 1,
            reaper_interval: Duration::from_millis(100),
            eviction_interval: Duration::from_millis(100),
            eviction_high_watermark_ratio: 0.95,
            eviction_ratio: 0.05,
            nof_eviction_high_watermark_ratio: 0.90,
            nof_eviction_ratio: 0.05,
            soft_pin_ttl: Duration::from_secs(1800),
            lease_ttl: Duration::from_secs(3600),
            enable_offload: false,
            enable_nof: true,
            offload_on_evict: false,
            allow_evict_soft_pinned_objects: true,
            offload_force_evict: false,
            offloading_queue_limit: 50_000,
            offload_cap_ratio: 0.5,
            client_live_ttl: Duration::from_secs(10),
            client_monitor_interval: Duration::from_secs(1),
            storage_fs_dir: String::new(),
            cluster_id: "mooncake".to_string(),
            enable_disk_eviction: true,
            quota_bytes: 0,
            enable_tenant_quota: false,
            default_tenant_quota_bytes: 0,
            tenant_quota_connector_type: "file".to_string(),
            tenant_quota_connector_uri: String::new(),
            tenant_quota_pool_capacity_bytes: 0,
            remote_source_enabled: false,
            remote_pull_ttl: Duration::from_secs(60),
            nof_heartbeat_interval: Duration::from_secs(10),
            nof_heartbeat_probe_timeout: Duration::from_secs(1),
            nof_heartbeat_failures_threshold: 3,
            snapshot_child_timeout: Duration::from_secs(300),
            snapshot_retention_count: 2,
            max_total_finished_tasks: 10_000,
            max_total_pending_tasks: 10_000,
            max_total_processing_tasks: 10_000,
            pending_task_timeout: Duration::from_secs(300),
            processing_task_timeout: Duration::from_secs(300),
            max_task_retry_attempts: 10,
            kv_event_config: KvEventConfig::default(),
        }
    }
}

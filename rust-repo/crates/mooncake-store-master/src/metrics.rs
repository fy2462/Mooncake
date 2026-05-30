// =============================================================================
// Prometheus Metrics — Prometheus 监控指标
// =============================================================================
// Defines all Prometheus metrics for the mooncake-store-master service and
// provides an HTTP endpoint for scraping.
// 定义 mooncake-store-master 服务的所有 Prometheus 指标，
// 并提供 HTTP 端点供抓取。
//
// Metrics organized by category / 按类别组织:
// 1. Base operation counters (put/get/remove/mount/etc.)
//    基础操作计数器（put/get/remove/mount 等）
// 2. Batch operation counters
//    批量操作计数器
// 3. Gauges (global state like segment count, memory usage)
//    仪表值（全局状态：segment 数量、内存使用等）
// 4. Cache hit counters
//    缓存命中计数器
// 5. Transfer latency histograms
//    传输延迟直方图
// 6. Snapshot metrics
//    快照指标
// 7. HA/OpLog replication metrics
//    HA/OpLog 复制指标
//
// All metrics are registered lazily via lazy_static and re-registered
// explicitly in register_metrics() to handle Prometheus' duplicate
// registration policy.
// 所有指标通过 lazy_static 延迟初始化，并在 register_metrics() 中
// 显式重新注册，以处理 Prometheus 的重复注册策略。

use axum::{routing::get, Router};
use lazy_static::lazy_static;
use prometheus::{register_histogram, Encoder, Histogram, IntCounter, IntGauge, TextEncoder};
use std::net::SocketAddr;

// =============================================================================
// Base operation counters — 基础操作计数器
// =============================================================================
// Count total requests and failures for each RPC method.
// 统计每个 RPC 方法的总请求数和失败数。

lazy_static! {
    pub static ref PUT_START_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_put_start_total", "total put_start requests").unwrap();
    pub static ref PUT_START_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_put_start_failures_total",
        "total failed put_start requests"
    )
    .unwrap();
    pub static ref PUT_END_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_put_end_total", "total put_end requests").unwrap();
    pub static ref PUT_END_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_put_end_failures_total",
        "total failed put_end requests"
    )
    .unwrap();
    pub static ref PUT_REVOKE_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_put_revoke_total",
        "total put_revoke requests"
    )
    .unwrap();
    pub static ref PUT_REVOKE_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_put_revoke_failures_total",
        "total failed put_revoke requests"
    )
    .unwrap();
    pub static ref GET_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_get_total",
        "total get_replica_list requests"
    )
    .unwrap();
    pub static ref GET_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_get_failures_total",
        "total failed get_replica_list requests"
    )
    .unwrap();
    pub static ref GET_BY_REGEX_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_get_by_regex_total",
        "total query_by_regex requests"
    )
    .unwrap();
    pub static ref GET_BY_REGEX_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_get_by_regex_failures_total",
        "total failed query_by_regex requests"
    )
    .unwrap();
    pub static ref EXIST_KEY_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_exist_key_total", "total exist_key requests").unwrap();
    pub static ref EXIST_KEY_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_exist_key_failures_total",
        "total failed exist_key requests"
    )
    .unwrap();
    pub static ref REMOVE_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_remove_total", "total remove requests").unwrap();
    pub static ref REMOVE_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_remove_failures_total",
        "total failed remove requests"
    )
    .unwrap();
    pub static ref REMOVE_BY_REGEX_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_remove_by_regex_total",
        "total remove_by_regex requests"
    )
    .unwrap();
    pub static ref REMOVE_BY_REGEX_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_remove_by_regex_failures_total",
        "total failed remove_by_regex requests"
    )
    .unwrap();
    pub static ref REMOVE_ALL_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_remove_all_total",
        "total remove_all requests"
    )
    .unwrap();
    pub static ref REMOVE_ALL_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_remove_all_failures_total",
        "total failed remove_all requests"
    )
    .unwrap();
    pub static ref PING_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_ping_total", "total client pings").unwrap();
    pub static ref PING_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_ping_failures_total", "total failed pings").unwrap();
    pub static ref MOUNT_SEGMENT_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_mount_segment_total",
        "total mount_segment requests"
    )
    .unwrap();
    pub static ref MOUNT_SEGMENT_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_mount_segment_failures_total",
        "total failed mount_segment requests"
    )
    .unwrap();
    pub static ref UNMOUNT_SEGMENT_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_unmount_segment_total",
        "total unmount_segment requests"
    )
    .unwrap();
    pub static ref UNMOUNT_SEGMENT_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_unmount_segment_failures_total",
        "total failed unmount_segment requests"
    )
    .unwrap();
    pub static ref UPSERT_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_upsert_total", "total upsert requests").unwrap();
    pub static ref UPSERT_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_upsert_failures_total",
        "total failed upsert requests"
    )
    .unwrap();
    /// Generic error counter for all internal errors.
    /// 通用错误计数器，统计所有内部错误。
    pub static ref ERROR_COUNTER: IntCounter =
        IntCounter::new("mooncake_store_errors_total", "total error count").unwrap();
}

// =============================================================================
// Batch operation counters — 批量操作计数器
// =============================================================================
// Count batch requests and failures (counted per item).
// 统计批量请求和失败数（按条目计数）。

lazy_static! {
    pub static ref BATCH_EXIST_KEY_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_exist_key_total",
        "total batch exist_key requests (items)"
    )
    .unwrap();
    pub static ref BATCH_EXIST_KEY_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_exist_key_failures_total",
        "total failed items in batch exist_key requests"
    )
    .unwrap();
    pub static ref BATCH_QUERY_IP_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_query_ip_total",
        "total batch query_ip requests (items)"
    )
    .unwrap();
    pub static ref BATCH_QUERY_IP_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_query_ip_failures_total",
        "total failed items in batch query_ip requests"
    )
    .unwrap();
    pub static ref BATCH_REPLICA_CLEAR_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_replica_clear_total",
        "total batch replica_clear requests (items)"
    )
    .unwrap();
    pub static ref BATCH_REPLICA_CLEAR_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_replica_clear_failures_total",
        "total failed items in batch replica_clear requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_END_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_end_total",
        "total batch put_end requests (items)"
    )
    .unwrap();
    pub static ref BATCH_PUT_END_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_end_failures_total",
        "total failed items in batch put_end requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_REVOKE_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_revoke_total",
        "total batch put_revoke requests (items)"
    )
    .unwrap();
    pub static ref BATCH_PUT_REVOKE_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_revoke_failures_total",
        "total failed items in batch put_revoke requests"
    )
    .unwrap();
    pub static ref BATCH_REMOVE_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_remove_total",
        "total batch remove requests (items)"
    )
    .unwrap();
    pub static ref BATCH_REMOVE_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_remove_failures_total",
        "total failed items in batch remove requests"
    )
    .unwrap();
    pub static ref BATCH_UPSERT_END_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_upsert_end_total",
        "total batch upsert_end requests (items)"
    )
    .unwrap();
    pub static ref BATCH_UPSERT_END_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_upsert_end_failures_total",
        "total failed items in batch upsert_end requests"
    )
    .unwrap();
}

// =============================================================================
// Gauges (global state) — 仪表值（全局状态）
// =============================================================================
// Snapshot of current system state — updated periodically by the master.
// 当前系统状态快照 —— 由 master 定期更新。

lazy_static! {
    pub static ref SEGMENT_COUNT: IntGauge =
        IntGauge::new("mooncake_store_segments", "number of mounted segments").unwrap();
    pub static ref OBJECT_COUNT: IntGauge =
        IntGauge::new("mooncake_store_objects", "number of stored objects").unwrap();
    pub static ref KEY_COUNT: IntGauge =
        IntGauge::new("mooncake_store_keys", "number of stored keys").unwrap();
    pub static ref SOFT_PIN_KEY_COUNT: IntGauge =
        IntGauge::new("mooncake_store_soft_pin_keys", "number of soft-pinned keys").unwrap();
    pub static ref ACTIVE_CLIENTS: IntGauge = IntGauge::new(
        "mooncake_store_active_clients",
        "number of active connected clients"
    )
    .unwrap();
    pub static ref ALLOCATED_MEM_SIZE: IntGauge = IntGauge::new(
        "mooncake_store_allocated_mem_bytes",
        "total allocated memory bytes"
    )
    .unwrap();
    pub static ref TOTAL_MEM_CAPACITY: IntGauge = IntGauge::new(
        "mooncake_store_total_mem_capacity_bytes",
        "total memory capacity bytes"
    )
    .unwrap();
    pub static ref ALLOCATED_FILE_SIZE: IntGauge = IntGauge::new(
        "mooncake_store_allocated_file_bytes",
        "total allocated file bytes"
    )
    .unwrap();
    pub static ref TOTAL_FILE_CAPACITY: IntGauge = IntGauge::new(
        "mooncake_store_total_file_capacity_bytes",
        "total file capacity bytes"
    )
    .unwrap();
}

// =============================================================================
// Cache hit counters — 缓存命中计数器
// =============================================================================

lazy_static! {
    pub static ref MEM_CACHE_HITS: IntCounter = IntCounter::new(
        "mooncake_store_mem_cache_hits_total",
        "total memory cache hits"
    )
    .unwrap();
    pub static ref FILE_CACHE_HITS: IntCounter = IntCounter::new(
        "mooncake_store_file_cache_hits_total",
        "total file cache hits"
    )
    .unwrap();
    pub static ref MEM_CACHE_TOTAL: IntCounter = IntCounter::new(
        "mooncake_store_mem_cache_requests_total",
        "total memory cache requests"
    )
    .unwrap();
    pub static ref FILE_CACHE_TOTAL: IntCounter = IntCounter::new(
        "mooncake_store_file_cache_requests_total",
        "total file cache requests"
    )
    .unwrap();
    pub static ref VALID_GETS: IntCounter = IntCounter::new(
        "mooncake_store_valid_gets_total",
        "total valid get requests"
    )
    .unwrap();
}

// =============================================================================
// Transfer latency histograms (microseconds) — 传输延迟直方图（微秒）
// =============================================================================
// Histogram buckets chosen to cover typical transfer latencies from microseconds
// to seconds (125 us to 1 second).
// 直方图桶设计覆盖从微秒到秒的典型传输延迟（125 us 到 1 second）。

const LATENCY_BUCKETS: &[f64] = &[
    125.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0, 750.0, 1000.0, 1500.0, 2000.0, 3000.0, 5000.0,
    7000.0, 15000.0, 20000.0, 50000.0, 100000.0, 200000.0, 500000.0, 1000000.0,
];

lazy_static! {
    pub static ref PUT_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_put_latency_us",
        "Put transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    )
    .unwrap();
    pub static ref GET_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_get_latency_us",
        "Get transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    )
    .unwrap();
    pub static ref BATCH_PUT_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_batch_put_latency_us",
        "Batch put transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    )
    .unwrap();
    pub static ref BATCH_GET_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_batch_get_latency_us",
        "Batch get transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    )
    .unwrap();
    /// Master RPC latency (all RPC methods combined).
    /// Master RPC 延迟（所有 RPC 方法综合）。
    pub static ref MASTER_RPC_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_master_rpc_latency_us",
        "Master RPC latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    )
    .unwrap();
    /// Total bytes read via transfer engine (cumulative counter).
    /// 通过 transfer engine 读取的总字节数（累积计数器）。
    pub static ref TRANSFER_READ_BYTES: IntCounter = IntCounter::new(
        "mooncake_store_transfer_read_bytes_total",
        "total bytes read via transfer engine"
    )
    .unwrap();
    /// Total bytes written via transfer engine (cumulative counter).
    /// 通过 transfer engine 写入的总字节数（累积计数器）。
    pub static ref TRANSFER_WRITE_BYTES: IntCounter = IntCounter::new(
        "mooncake_store_transfer_write_bytes_total",
        "total bytes written via transfer engine"
    )
    .unwrap();
    /// Distribution of stored value sizes.
    /// 存储值大小的分布直方图。
    pub static ref VALUE_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "mooncake_store_value_size_bytes",
        "Distribution of stored value sizes",
        vec![
            64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0,
            16777216.0
        ]
    )
    .unwrap();
}

// =============================================================================
// Snapshot metrics — 快照指标
// =============================================================================

lazy_static! {
    pub static ref SNAPSHOT_DURATION_MS: IntGauge = IntGauge::new(
        "mooncake_store_snapshot_duration_ms",
        "last snapshot duration in milliseconds"
    )
    .unwrap();
    pub static ref SNAPSHOT_SUCCESS_COUNT: IntCounter = IntCounter::new(
        "mooncake_store_snapshot_success_total",
        "total successful snapshots"
    )
    .unwrap();
    pub static ref SNAPSHOT_FAIL_COUNT: IntCounter = IntCounter::new(
        "mooncake_store_snapshot_fail_total",
        "total failed snapshots"
    )
    .unwrap();
}

// =============================================================================
// HA / OpLog replication metrics — HA / OpLog 复制指标
// =============================================================================

lazy_static! {
    /// Latest OpLog sequence ID on the primary (leader).
    /// 主节点（leader）上的最新 OpLog 序列号。
    pub static ref OPLOG_LAST_SEQ_ID: IntGauge = IntGauge::new(
        "mooncake_store_oplog_last_seq_id",
        "latest OpLog sequence ID on primary"
    )
    .unwrap();
    /// Latest OpLog sequence ID applied on the standby.
    /// 备节点上的最新已应用 OpLog 序列号。
    pub static ref OPLOG_APPLIED_SEQ_ID: IntGauge = IntGauge::new(
        "mooncake_store_oplog_applied_seq_id",
        "standby applied OpLog sequence ID"
    )
    .unwrap();
    /// Replication lag: primary_seq - standby_applied_seq.
    /// 复制延迟：主序列号 - 备已应用序列号。
    pub static ref OPLOG_STANDBY_LAG: IntGauge = IntGauge::new(
        "mooncake_store_oplog_standby_lag",
        "standby replication lag in entries"
    )
    .unwrap();
    /// Pending out-of-order OpLog entries awaiting sequencing.
    /// 等待排序的乱序 OpLog 条目数。
    pub static ref OPLOG_PENDING_ENTRIES: IntGauge = IntGauge::new(
        "mooncake_store_oplog_pending_entries",
        "pending out-of-order OpLog entries"
    )
    .unwrap();
    /// Size of the pending mutation retry queue.
    /// 待处理变更重试队列的大小。
    pub static ref PENDING_MUTATION_QUEUE_SIZE: IntGauge = IntGauge::new(
        "mooncake_store_pending_mutation_queue_size",
        "pending mutation retry queue size"
    )
    .unwrap();
    /// Total OpLog entries skipped (e.g., duplicates or no-ops).
    /// 已跳过的 OpLog 条目总数（如重复或无操作）。
    pub static ref OPLOG_SKIPPED_ENTRIES: IntCounter = IntCounter::new(
        "mooncake_store_oplog_skipped_entries_total",
        "total skipped OpLog entries"
    )
    .unwrap();
    /// Total OpLog checksum validation failures.
    /// OpLog 校验和验证失败总数。
    pub static ref OPLOG_CHECKSUM_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_oplog_checksum_failures_total",
        "total OpLog checksum failures"
    )
    .unwrap();
}

// =============================================================================
// Registration & HTTP server — 注册与 HTTP 服务器
// =============================================================================

/// Re-register a counter with Prometheus (lazy_static auto-registers on first use,
/// this is for explicit re-registration to handle duplicates).
/// 重新向 Prometheus 注册计数器（lazy_static 在首次使用时自动注册，
/// 此为显式重新注册以处理重复）。
fn register_counter(c: &IntCounter) {
    let _ = prometheus::register(Box::new(c.clone()));
}

/// Re-register a gauge with Prometheus.
/// 重新向 Prometheus 注册仪表值。
fn register_gauge(g: &IntGauge) {
    let _ = prometheus::register(Box::new(g.clone()));
}

/// Register all metrics with Prometheus.
/// 向 Prometheus 注册所有指标。
///
/// Called on service startup to ensure all metrics are available before
/// any requests are served.
/// 在服务启动时调用，确保所有指标在处理任何请求前可用。
pub fn register_metrics() {
    // Base counters
    register_counter(&PUT_START_REQUESTS);
    register_counter(&PUT_START_FAILURES);
    register_counter(&PUT_END_REQUESTS);
    register_counter(&PUT_END_FAILURES);
    register_counter(&PUT_REVOKE_REQUESTS);
    register_counter(&PUT_REVOKE_FAILURES);
    register_counter(&GET_REQUESTS);
    register_counter(&GET_FAILURES);
    register_counter(&GET_BY_REGEX_REQUESTS);
    register_counter(&GET_BY_REGEX_FAILURES);
    register_counter(&EXIST_KEY_REQUESTS);
    register_counter(&EXIST_KEY_FAILURES);
    register_counter(&REMOVE_REQUESTS);
    register_counter(&REMOVE_FAILURES);
    register_counter(&REMOVE_BY_REGEX_REQUESTS);
    register_counter(&REMOVE_BY_REGEX_FAILURES);
    register_counter(&REMOVE_ALL_REQUESTS);
    register_counter(&REMOVE_ALL_FAILURES);
    register_counter(&PING_REQUESTS);
    register_counter(&PING_FAILURES);
    register_counter(&MOUNT_SEGMENT_REQUESTS);
    register_counter(&MOUNT_SEGMENT_FAILURES);
    register_counter(&UNMOUNT_SEGMENT_REQUESTS);
    register_counter(&UNMOUNT_SEGMENT_FAILURES);
    register_counter(&UPSERT_REQUESTS);
    register_counter(&UPSERT_FAILURES);
    register_counter(&ERROR_COUNTER);

    // Batch counters
    register_counter(&BATCH_EXIST_KEY_REQUESTS);
    register_counter(&BATCH_EXIST_KEY_FAILURES);
    register_counter(&BATCH_QUERY_IP_REQUESTS);
    register_counter(&BATCH_QUERY_IP_FAILURES);
    register_counter(&BATCH_REPLICA_CLEAR_REQUESTS);
    register_counter(&BATCH_REPLICA_CLEAR_FAILURES);
    register_counter(&BATCH_PUT_END_REQUESTS);
    register_counter(&BATCH_PUT_END_FAILURES);
    register_counter(&BATCH_PUT_REVOKE_REQUESTS);
    register_counter(&BATCH_PUT_REVOKE_FAILURES);
    register_counter(&BATCH_REMOVE_REQUESTS);
    register_counter(&BATCH_REMOVE_FAILURES);
    register_counter(&BATCH_UPSERT_END_REQUESTS);
    register_counter(&BATCH_UPSERT_END_FAILURES);

    // Gauges
    register_gauge(&SEGMENT_COUNT);
    register_gauge(&OBJECT_COUNT);
    register_gauge(&KEY_COUNT);
    register_gauge(&SOFT_PIN_KEY_COUNT);
    register_gauge(&ACTIVE_CLIENTS);
    register_gauge(&ALLOCATED_MEM_SIZE);
    register_gauge(&TOTAL_MEM_CAPACITY);
    register_gauge(&ALLOCATED_FILE_SIZE);
    register_gauge(&TOTAL_FILE_CAPACITY);

    // Cache counters
    register_counter(&MEM_CACHE_HITS);
    register_counter(&FILE_CACHE_HITS);
    register_counter(&MEM_CACHE_TOTAL);
    register_counter(&FILE_CACHE_TOTAL);
    register_counter(&VALID_GETS);

    // Transfer bytes
    register_counter(&TRANSFER_READ_BYTES);
    register_counter(&TRANSFER_WRITE_BYTES);

    // Snapshot metrics
    register_gauge(&SNAPSHOT_DURATION_MS);
    register_counter(&SNAPSHOT_SUCCESS_COUNT);
    register_counter(&SNAPSHOT_FAIL_COUNT);

    // HA / OpLog metrics
    register_gauge(&OPLOG_LAST_SEQ_ID);
    register_gauge(&OPLOG_APPLIED_SEQ_ID);
    register_gauge(&OPLOG_STANDBY_LAG);
    register_gauge(&OPLOG_PENDING_ENTRIES);
    register_gauge(&PENDING_MUTATION_QUEUE_SIZE);
    register_counter(&OPLOG_SKIPPED_ENTRIES);
    register_counter(&OPLOG_CHECKSUM_FAILURES);
}

/// Start the Prometheus metrics HTTP server.
/// 启动 Prometheus 指标 HTTP 服务器。
///
/// Registers all metrics and serves them at GET /metrics in Prometheus text format.
/// This function blocks indefinitely — call in a spawned task.
/// 注册所有指标并在 GET /metrics 提供 Prometheus 文本格式。
/// 此函数无限期阻塞 —— 在 spawned task 中调用。
pub async fn serve_metrics_http(addr: SocketAddr) {
    register_metrics();

    let app = Router::new().route("/metrics", get(metrics_handler));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// HTTP handler for GET /metrics.
/// GET /metrics 的 HTTP 处理器。
///
/// Gathers all registered Prometheus metrics and encodes them in text format.
/// 收集所有已注册的 Prometheus 指标并编码为文本格式。
async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

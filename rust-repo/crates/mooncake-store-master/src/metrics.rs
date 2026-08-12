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

use crate::admin_http::{AdminRuntimeState, admin_router};
use axum::{Router, routing::get};
use lazy_static::lazy_static;
use prometheus::{
    Encoder, Histogram, IntCounter, IntGauge, TextEncoder, core::Collector, register_histogram,
};
use std::net::SocketAddr;

mod batch;
mod cache;
mod operations;

pub use batch::{
    BATCH_EXIST_KEY_FAILED_ITEMS, BATCH_EXIST_KEY_FAILURES, BATCH_EXIST_KEY_ITEMS,
    BATCH_EXIST_KEY_PARTIAL_SUCCESSES, BATCH_EXIST_KEY_REQUESTS, BATCH_PUT_END_FAILURES,
    BATCH_PUT_END_REQUESTS, BATCH_PUT_REVOKE_FAILURES, BATCH_PUT_REVOKE_REQUESTS,
    BATCH_QUERY_IP_FAILURES, BATCH_QUERY_IP_REQUESTS, BATCH_REMOVE_FAILURES, BATCH_REMOVE_REQUESTS,
    BATCH_REPLICA_CLEAR_FAILURES, BATCH_REPLICA_CLEAR_REQUESTS, BATCH_UPSERT_END_FAILURES,
    BATCH_UPSERT_END_REQUESTS,
};
pub use cache::{
    FILE_CACHE_HIT_BYTES, FILE_CACHE_HITS, FILE_CACHE_TOTAL, MEM_CACHE_HIT_BYTES, MEM_CACHE_HITS,
    MEM_CACHE_TOTAL, TOTAL_GETS, VALID_GETS,
};
pub use operations::{
    ERROR_COUNTER, EXIST_KEY_FAILURES, EXIST_KEY_REQUESTS, GET_BY_REGEX_FAILURES,
    GET_BY_REGEX_REQUESTS, GET_FAILURES, GET_REQUESTS, MOUNT_SEGMENT_FAILURES,
    MOUNT_SEGMENT_REQUESTS, PING_FAILURES, PING_REQUESTS, PUT_END_FAILURES, PUT_END_REQUESTS,
    PUT_REVOKE_FAILURES, PUT_REVOKE_REQUESTS, PUT_START_ALLOCATION_FAILURES, PUT_START_FAILURES,
    PUT_START_REQUESTS, REMOVE_ALL_FAILURES, REMOVE_ALL_REQUESTS, REMOVE_BY_REGEX_FAILURES,
    REMOVE_BY_REGEX_REQUESTS, REMOVE_FAILURES, REMOVE_REQUESTS, UNMOUNT_SEGMENT_FAILURES,
    UNMOUNT_SEGMENT_REQUESTS, UPSERT_FAILURES, UPSERT_REQUESTS,
};

/// Stable C++-compatible cache-stat indices. New values must be appended.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheHitStat {
    MemoryHits = 0,
    SsdHits = 1,
    MemoryTotal = 2,
    SsdTotal = 3,
    MemoryHitRate = 4,
    SsdHitRate = 5,
    OverallHitRate = 6,
    ValidGetRate = 7,
}

impl CacheHitStat {
    pub const MEMORY_CURRENT_CACHED_OBJECTS: Self = Self::MemoryTotal;
    pub const SSD_CURRENT_CACHED_OBJECTS: Self = Self::SsdTotal;
    pub const MEMORY_HITS_PER_CURRENT_CACHED_OBJECT: Self = Self::MemoryHitRate;
    pub const SSD_HITS_PER_CURRENT_CACHED_OBJECT: Self = Self::SsdHitRate;
    pub const OVERALL_HITS_PER_CURRENT_CACHED_OBJECT: Self = Self::OverallHitRate;
}

#[derive(Debug, Clone, PartialEq)]
pub struct CacheStats([f64; 8]);

impl std::ops::Index<CacheHitStat> for CacheStats {
    type Output = f64;

    fn index(&self, index: CacheHitStat) -> &Self::Output {
        &self.0[index as usize]
    }
}

fn rounded_ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        (numerator / denominator * 100.0).round() / 100.0
    } else {
        0.0
    }
}

/// Calculate the Store-observed cache statistics exposed by the C++ API.
/// Hit counters are cumulative while cache totals are current-object gauges,
/// so the three reuse values are intentionally not bounded by one.
pub fn calculate_cache_stats() -> CacheStats {
    let memory_hits = MEM_CACHE_HITS.get() as f64;
    let ssd_hits = FILE_CACHE_HITS.get() as f64;
    let memory_total = MEM_CACHE_TOTAL.get() as f64;
    let ssd_total = FILE_CACHE_TOTAL.get() as f64;
    CacheStats([
        memory_hits,
        ssd_hits,
        memory_total,
        ssd_total,
        rounded_ratio(memory_hits, memory_total),
        rounded_ratio(ssd_hits, ssd_total),
        rounded_ratio(memory_hits + ssd_hits, memory_total + ssd_total),
        rounded_ratio(VALID_GETS.get() as f64, TOTAL_GETS.get() as f64),
    ])
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

// Promotion retry lifecycle counters. Names intentionally match the C++ master.
lazy_static! {
    pub static ref PROMOTION_IN_FLIGHT: IntGauge = IntGauge::new(
        "master_promotion_in_flight",
        "Current number of in-flight L2->L1 promotion tasks"
    )
    .unwrap();
    pub static ref PROMOTION_ADMITTED: IntCounter = IntCounter::new(
        "master_promotion_admitted_total",
        "Total promotion tasks admitted past all gates and enqueued"
    )
    .unwrap();
    pub static ref PROMOTION_COMPLETED: IntCounter = IntCounter::new(
        "master_promotion_completed_total",
        "Total promotion tasks committed via NotifyPromotionSuccess"
    )
    .unwrap();
    pub static ref PROMOTION_COMPLETED_BYTES: IntCounter = IntCounter::new(
        "master_promotion_completed_bytes_total",
        "Total bytes promoted from LOCAL_DISK to MEMORY"
    )
    .unwrap();
    pub static ref PROMOTION_EXPIRED: IntCounter = IntCounter::new(
        "master_promotion_expired_total",
        "Total promotion tasks expired via the reaper (put_start_release_timeout_sec)"
    )
    .unwrap();
    pub static ref PROMOTION_FAILED: IntCounter = IntCounter::new(
        "master_promotion_failed_total",
        "Total promotion tasks aborted by holder via NotifyPromotionFailure"
    )
    .unwrap();
    pub static ref PROMOTION_CANCELLED: IntCounter = IntCounter::new(
        "master_promotion_cancelled_total",
        "Total promotion tasks removed because the prerequisite went away"
    )
    .unwrap();
    pub static ref PROMOTION_REJECTED_FREQUENCY: IntCounter = IntCounter::new(
        "master_promotion_rejected_frequency_total",
        "Promotion attempts rejected because CountMinSketch frequency was below promotion_admission_threshold"
    )
    .unwrap();
    pub static ref PROMOTION_REJECTED_WATERMARK: IntCounter = IntCounter::new(
        "master_promotion_rejected_watermark_total",
        "Promotion attempts rejected because DRAM was at or above the eviction high watermark"
    )
    .unwrap();
    pub static ref PROMOTION_REJECTED_CAP: IntCounter = IntCounter::new(
        "master_promotion_rejected_cap_total",
        "Promotion attempts rejected because promotion_in_flight was at promotion_queue_limit"
    )
    .unwrap();
    pub static ref PROMOTION_CANDIDATE_RECORDED: IntCounter = IntCounter::new(
        "master_promotion_candidate_recorded_total",
        "promotion candidates recorded for background retry"
    )
    .unwrap();
    pub static ref PROMOTION_CANDIDATE_ADMITTED: IntCounter = IntCounter::new(
        "master_promotion_candidate_admitted_total",
        "promotion candidates admitted by background retry"
    )
    .unwrap();
    pub static ref PROMOTION_CANDIDATE_ADMISSION_REJECTED: IntCounter = IntCounter::new(
        "master_promotion_candidate_admission_rejected_total",
        "promotion candidates transiently rejected during background retry"
    )
    .unwrap();
    pub static ref PROMOTION_CANDIDATE_EXPIRED_EVALUATED: IntCounter = IntCounter::new(
        "master_promotion_candidate_expired_evaluated_total",
        "evaluated promotion candidates removed by age or retry budget"
    )
    .unwrap();
    pub static ref PROMOTION_CANDIDATE_EXPIRED_UNEVALUATED: IntCounter = IntCounter::new(
        "master_promotion_candidate_expired_unevaluated_total",
        "unevaluated promotion candidates removed by age"
    )
    .unwrap();
    pub static ref PROMOTION_CANDIDATE_DROPPED_LIMIT: IntCounter = IntCounter::new(
        "master_promotion_candidate_dropped_limit_total",
        "promotion candidates refused because the candidate table is full"
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
    pub static ref OPLOG_GAP_RESOLVE_ATTEMPTS: IntCounter = IntCounter::new(
        "mooncake_store_oplog_gap_resolve_attempts_total",
        "total OpLog gap resolution attempts"
    )
    .unwrap();
    pub static ref OPLOG_GAP_RESOLVE_SUCCESS: IntCounter = IntCounter::new(
        "mooncake_store_oplog_gap_resolve_success_total",
        "total successful OpLog gap resolutions"
    )
    .unwrap();
    pub static ref OPLOG_ETCD_WRITE_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_oplog_etcd_write_failures_total",
        "total failed etcd OpLog write operations"
    )
    .unwrap();
    pub static ref OPLOG_ETCD_WRITE_RETRIES: IntCounter = IntCounter::new(
        "mooncake_store_oplog_etcd_write_retries_total",
        "total etcd OpLog write retry attempts"
    )
    .unwrap();
    pub static ref OPLOG_WATCH_DISCONNECTIONS: IntCounter = IntCounter::new(
        "mooncake_store_oplog_watch_disconnections_total",
        "total OpLog watch disconnections"
    )
    .unwrap();
    pub static ref OPLOG_APPLIED_ENTRIES: IntCounter = IntCounter::new(
        "mooncake_store_oplog_applied_entries_total",
        "total OpLog entries successfully applied on standby"
    )
    .unwrap();
    pub static ref OPLOG_BATCH_COMMITS: IntCounter = IntCounter::new(
        "mooncake_store_oplog_batch_commits_total",
        "total OpLog batches committed to etcd"
    )
    .unwrap();
    pub static ref OPLOG_SYNC_BATCH_COMMITS: IntCounter = IntCounter::new(
        "mooncake_store_oplog_sync_batch_commits_total",
        "total sync OpLog batches committed to etcd"
    )
    .unwrap();
    pub static ref OPLOG_ETCD_WRITE_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_oplog_etcd_write_latency_us",
        "latency of etcd OpLog write operations in microseconds",
        vec![
            100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0, 100_000.0, 500_000.0,
            1_000_000.0, 5_000_000.0
        ]
    )
    .unwrap();
    pub static ref OPLOG_APPLY_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_oplog_apply_latency_us",
        "latency of applying OpLog entries in microseconds",
        vec![
            50.0, 100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0, 100_000.0,
            500_000.0, 1_000_000.0
        ]
    )
    .unwrap();
    pub static ref OPLOG_WRITER_SUBMITTED: IntCounter = IntCounter::new(
        "mooncake_store_oplog_writer_submitted_total",
        "total durable OpLog records submitted to the writer"
    )
    .unwrap();
    pub static ref OPLOG_WRITER_QUEUE_REJECTIONS: IntCounter = IntCounter::new(
        "mooncake_store_oplog_writer_queue_rejections_total",
        "total OpLog writer commands rejected because the queue was full"
    )
    .unwrap();
    pub static ref OPLOG_WRITER_QUEUE_DEPTH: IntGauge = IntGauge::new(
        "mooncake_store_oplog_writer_queue_depth",
        "current number of commands queued for the OpLog writer"
    )
    .unwrap();
    pub static ref OPLOG_WRITER_BATCH_RECORDS: Histogram = register_histogram!(
        "mooncake_store_oplog_writer_batch_records",
        "number of durable OpLog records in each writer batch",
        vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 100.0]
    )
    .unwrap();
    pub static ref OPLOG_WRITER_QUEUE_WAIT_US: Histogram = register_histogram!(
        "mooncake_store_oplog_writer_queue_wait_us",
        "time OpLog records wait before writer batching in microseconds",
        vec![
            100.0,
            500.0,
            1_000.0,
            5_000.0,
            10_000.0,
            50_000.0,
            100_000.0,
            500_000.0,
            1_000_000.0,
            5_000_000.0,
            20_000_000.0
        ]
    )
    .unwrap();
    pub static ref OPLOG_WRITER_DURABLE_WAIT_US: Histogram = register_histogram!(
        "mooncake_store_oplog_writer_durable_wait_us",
        "time durable OpLog submissions wait for completion in microseconds",
        vec![
            100.0,
            500.0,
            1_000.0,
            5_000.0,
            10_000.0,
            50_000.0,
            100_000.0,
            500_000.0,
            1_000_000.0,
            5_000_000.0,
            20_000_000.0
        ]
    )
    .unwrap();
    pub static ref OPLOG_WRITER_POISON_EVENTS: IntCounter = IntCounter::new(
        "mooncake_store_oplog_writer_poison_events_total",
        "total terminal OpLog writer poison events"
    )
    .unwrap();
    pub static ref OPLOG_WRITER_POST_POISON_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_oplog_writer_post_poison_failures_total",
        "total OpLog writer commands rejected after terminal poison"
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

/// Re-register a histogram with Prometheus.
fn register_histogram_metric(histogram: &Histogram) {
    let _ = prometheus::register(Box::new(histogram.clone()));
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
    register_counter(&PUT_START_ALLOCATION_FAILURES);
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
    register_counter(&BATCH_EXIST_KEY_ITEMS);
    register_counter(&BATCH_EXIST_KEY_PARTIAL_SUCCESSES);
    register_counter(&BATCH_EXIST_KEY_FAILED_ITEMS);
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

    // Cache metrics
    register_counter(&MEM_CACHE_HITS);
    register_counter(&FILE_CACHE_HITS);
    register_counter(&MEM_CACHE_HIT_BYTES);
    register_counter(&FILE_CACHE_HIT_BYTES);
    register_gauge(&MEM_CACHE_TOTAL);
    register_gauge(&FILE_CACHE_TOTAL);
    register_counter(&VALID_GETS);
    register_counter(&TOTAL_GETS);

    register_counter(&PROMOTION_CANDIDATE_RECORDED);
    register_counter(&PROMOTION_CANDIDATE_ADMITTED);
    register_counter(&PROMOTION_CANDIDATE_ADMISSION_REJECTED);
    register_counter(&PROMOTION_CANDIDATE_EXPIRED_EVALUATED);
    register_counter(&PROMOTION_CANDIDATE_EXPIRED_UNEVALUATED);
    register_counter(&PROMOTION_CANDIDATE_DROPPED_LIMIT);

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
    register_counter(&OPLOG_GAP_RESOLVE_ATTEMPTS);
    register_counter(&OPLOG_GAP_RESOLVE_SUCCESS);
    register_counter(&OPLOG_ETCD_WRITE_FAILURES);
    register_counter(&OPLOG_ETCD_WRITE_RETRIES);
    register_counter(&OPLOG_WATCH_DISCONNECTIONS);
    register_counter(&OPLOG_APPLIED_ENTRIES);
    register_counter(&OPLOG_BATCH_COMMITS);
    register_counter(&OPLOG_SYNC_BATCH_COMMITS);
    register_counter(&OPLOG_WRITER_SUBMITTED);
    register_counter(&OPLOG_WRITER_QUEUE_REJECTIONS);
    register_gauge(&OPLOG_WRITER_QUEUE_DEPTH);
    register_histogram_metric(&OPLOG_WRITER_BATCH_RECORDS);
    register_histogram_metric(&OPLOG_WRITER_QUEUE_WAIT_US);
    register_histogram_metric(&OPLOG_WRITER_DURABLE_WAIT_US);
    register_counter(&OPLOG_WRITER_POISON_EVENTS);
    register_counter(&OPLOG_WRITER_POST_POISON_FAILURES);
    register_histogram_metric(&OPLOG_APPLY_LATENCY_US);
}

/// Encode C++-compatible aliases from the live Rust HA gauge state.
///
/// These aliases are derived on demand and do not maintain a second metric
/// state or rename the ordinary Rust Prometheus families.
pub fn encode_ha_compat_metrics() -> Result<String, String> {
    let aliases = [
        (
            "ha_oplog_last_sequence_id",
            "latest OpLog sequence ID on primary",
            OPLOG_LAST_SEQ_ID.get(),
        ),
        (
            "ha_oplog_applied_sequence_id",
            "standby applied OpLog sequence ID",
            OPLOG_APPLIED_SEQ_ID.get(),
        ),
        (
            "ha_oplog_standby_lag",
            "standby replication lag in entries",
            OPLOG_STANDBY_LAG.get(),
        ),
    ];
    let mut metric_families = Vec::with_capacity(aliases.len());
    for (name, help, value) in aliases {
        let gauge = IntGauge::new(name, help).map_err(|error| error.to_string())?;
        gauge.set(value);
        metric_families.extend(gauge.collect());
    }

    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .map_err(|error| error.to_string())?;
    String::from_utf8(buffer).map_err(|error| error.to_string())
}

/// Return a compact snapshot of the current HA sequence gauges.
pub fn ha_summary() -> String {
    format!(
        "last_seq={} applied_seq={} standby_lag={}",
        OPLOG_LAST_SEQ_ID.get(),
        OPLOG_APPLIED_SEQ_ID.get(),
        OPLOG_STANDBY_LAG.get()
    )
}

/// Start the Prometheus metrics HTTP server.
/// 启动 Prometheus 指标 HTTP 服务器。
///
/// Registers all metrics and serves them at GET /metrics in Prometheus text format.
/// This function blocks indefinitely — call in a spawned task.
/// 注册所有指标并在 GET /metrics 提供 Prometheus 文本格式。
/// 此函数无限期阻塞 —— 在 spawned task 中调用。
pub async fn serve_metrics_http(addr: SocketAddr) {
    serve_metrics_http_with_admin(addr, AdminRuntimeState::serving(None)).await;
}

pub async fn serve_metrics_http_with_admin(addr: SocketAddr, admin_state: AdminRuntimeState) {
    let app = metrics_admin_router(admin_state);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// Build the production metrics and administrative HTTP surface.
pub fn metrics_admin_router(admin_state: AdminRuntimeState) -> Router {
    register_metrics();
    Router::new()
        .route("/metrics", get(metrics_handler))
        .merge(admin_router(admin_state))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::MasterRuntimeState;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    async fn get(router: &Router, path: &str) -> (StatusCode, String) {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn test_admin_http_metrics_returns_master_text() {
        let response = metrics_admin_router(AdminRuntimeState::serving(None))
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("master_"));
    }

    #[tokio::test]
    async fn test_admin_http_starting_always_available_matrix() {
        let state = AdminRuntimeState::new(MasterRuntimeState::Starting, None, false);
        let router = metrics_admin_router(state);
        let mut bodies = std::collections::HashMap::new();

        for path in [
            "/metrics",
            "/metrics/summary",
            "/health",
            "/role",
            "/ha_status",
            "/leader",
        ] {
            let (status, body) = get(&router, path).await;
            assert_eq!(status, StatusCode::OK, "path={path}");
            bodies.insert(path, body);
        }

        let health: Value = serde_json::from_str(bodies["/health"].as_str()).unwrap();
        let leader: Value = serde_json::from_str(bodies["/leader"].as_str()).unwrap();
        assert_eq!(health["ha_state"], "starting");
        assert_eq!(bodies["/role"], "standby");
        assert_eq!(bodies["/ha_status"], "starting");
        assert_eq!(leader["present"], false);
    }
}

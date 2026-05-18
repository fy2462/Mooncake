use axum::{routing::get, Router};
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, Encoder, Histogram, IntCounter,
    IntGauge, TextEncoder,
};
use std::net::SocketAddr;

// =========================================================================
// Base operation counters
// =========================================================================

lazy_static! {
    pub static ref PUT_START_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_put_start_total", "total put_start requests").unwrap();
    pub static ref PUT_START_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_put_start_failures_total", "total failed put_start requests").unwrap();
    pub static ref PUT_END_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_put_end_total", "total put_end requests").unwrap();
    pub static ref PUT_END_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_put_end_failures_total", "total failed put_end requests").unwrap();
    pub static ref PUT_REVOKE_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_put_revoke_total", "total put_revoke requests").unwrap();
    pub static ref PUT_REVOKE_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_put_revoke_failures_total", "total failed put_revoke requests").unwrap();
    pub static ref GET_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_get_total", "total get_replica_list requests").unwrap();
    pub static ref GET_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_get_failures_total", "total failed get_replica_list requests").unwrap();
    pub static ref GET_BY_REGEX_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_get_by_regex_total", "total query_by_regex requests").unwrap();
    pub static ref GET_BY_REGEX_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_get_by_regex_failures_total", "total failed query_by_regex requests").unwrap();
    pub static ref EXIST_KEY_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_exist_key_total", "total exist_key requests").unwrap();
    pub static ref EXIST_KEY_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_exist_key_failures_total", "total failed exist_key requests").unwrap();
    pub static ref REMOVE_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_remove_total", "total remove requests").unwrap();
    pub static ref REMOVE_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_remove_failures_total", "total failed remove requests").unwrap();
    pub static ref REMOVE_BY_REGEX_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_remove_by_regex_total", "total remove_by_regex requests").unwrap();
    pub static ref REMOVE_BY_REGEX_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_remove_by_regex_failures_total", "total failed remove_by_regex requests").unwrap();
    pub static ref REMOVE_ALL_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_remove_all_total", "total remove_all requests").unwrap();
    pub static ref REMOVE_ALL_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_remove_all_failures_total", "total failed remove_all requests").unwrap();
    pub static ref PING_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_ping_total", "total client pings").unwrap();
    pub static ref PING_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_ping_failures_total", "total failed pings").unwrap();
    pub static ref MOUNT_SEGMENT_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_mount_segment_total", "total mount_segment requests").unwrap();
    pub static ref MOUNT_SEGMENT_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_mount_segment_failures_total", "total failed mount_segment requests").unwrap();
    pub static ref UNMOUNT_SEGMENT_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_unmount_segment_total", "total unmount_segment requests").unwrap();
    pub static ref UNMOUNT_SEGMENT_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_unmount_segment_failures_total", "total failed unmount_segment requests").unwrap();
    pub static ref UPSERT_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_upsert_total", "total upsert requests").unwrap();
    pub static ref UPSERT_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_upsert_failures_total", "total failed upsert requests").unwrap();
    pub static ref ERROR_COUNTER: IntCounter =
        IntCounter::new("mooncake_store_errors_total", "total error count").unwrap();
}

// =========================================================================
// Batch operation counters
// =========================================================================

lazy_static! {
    pub static ref BATCH_EXIST_KEY_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_batch_exist_key_total", "total batch exist_key requests (items)").unwrap();
    pub static ref BATCH_EXIST_KEY_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_batch_exist_key_failures_total", "total failed items in batch exist_key requests").unwrap();
    pub static ref BATCH_QUERY_IP_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_batch_query_ip_total", "total batch query_ip requests (items)").unwrap();
    pub static ref BATCH_QUERY_IP_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_batch_query_ip_failures_total", "total failed items in batch query_ip requests").unwrap();
    pub static ref BATCH_REPLICA_CLEAR_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_batch_replica_clear_total", "total batch replica_clear requests (items)").unwrap();
    pub static ref BATCH_REPLICA_CLEAR_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_batch_replica_clear_failures_total", "total failed items in batch replica_clear requests").unwrap();
    pub static ref BATCH_PUT_END_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_batch_put_end_total", "total batch put_end requests (items)").unwrap();
    pub static ref BATCH_PUT_END_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_batch_put_end_failures_total", "total failed items in batch put_end requests").unwrap();
    pub static ref BATCH_PUT_REVOKE_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_batch_put_revoke_total", "total batch put_revoke requests (items)").unwrap();
    pub static ref BATCH_PUT_REVOKE_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_batch_put_revoke_failures_total", "total failed items in batch put_revoke requests").unwrap();
    pub static ref BATCH_REMOVE_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_batch_remove_total", "total batch remove requests (items)").unwrap();
    pub static ref BATCH_REMOVE_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_batch_remove_failures_total", "total failed items in batch remove requests").unwrap();
    pub static ref BATCH_UPSERT_END_REQUESTS: IntCounter =
        IntCounter::new("mooncake_store_batch_upsert_end_total", "total batch upsert_end requests (items)").unwrap();
    pub static ref BATCH_UPSERT_END_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_batch_upsert_end_failures_total", "total failed items in batch upsert_end requests").unwrap();
}

// =========================================================================
// Gauges (global state)
// =========================================================================

lazy_static! {
    pub static ref SEGMENT_COUNT: IntGauge =
        IntGauge::new("mooncake_store_segments", "number of mounted segments").unwrap();
    pub static ref OBJECT_COUNT: IntGauge =
        IntGauge::new("mooncake_store_objects", "number of stored objects").unwrap();
    pub static ref KEY_COUNT: IntGauge =
        IntGauge::new("mooncake_store_keys", "number of stored keys").unwrap();
    pub static ref SOFT_PIN_KEY_COUNT: IntGauge =
        IntGauge::new("mooncake_store_soft_pin_keys", "number of soft-pinned keys").unwrap();
    pub static ref ACTIVE_CLIENTS: IntGauge =
        IntGauge::new("mooncake_store_active_clients", "number of active connected clients").unwrap();
    pub static ref ALLOCATED_MEM_SIZE: IntGauge =
        IntGauge::new("mooncake_store_allocated_mem_bytes", "total allocated memory bytes").unwrap();
    pub static ref TOTAL_MEM_CAPACITY: IntGauge =
        IntGauge::new("mooncake_store_total_mem_capacity_bytes", "total memory capacity bytes").unwrap();
    pub static ref ALLOCATED_FILE_SIZE: IntGauge =
        IntGauge::new("mooncake_store_allocated_file_bytes", "total allocated file bytes").unwrap();
    pub static ref TOTAL_FILE_CAPACITY: IntGauge =
        IntGauge::new("mooncake_store_total_file_capacity_bytes", "total file capacity bytes").unwrap();
}

// =========================================================================
// Cache hit counters
// =========================================================================

lazy_static! {
    pub static ref MEM_CACHE_HITS: IntCounter =
        IntCounter::new("mooncake_store_mem_cache_hits_total", "total memory cache hits").unwrap();
    pub static ref FILE_CACHE_HITS: IntCounter =
        IntCounter::new("mooncake_store_file_cache_hits_total", "total file cache hits").unwrap();
    pub static ref MEM_CACHE_TOTAL: IntCounter =
        IntCounter::new("mooncake_store_mem_cache_requests_total", "total memory cache requests").unwrap();
    pub static ref FILE_CACHE_TOTAL: IntCounter =
        IntCounter::new("mooncake_store_file_cache_requests_total", "total file cache requests").unwrap();
    pub static ref VALID_GETS: IntCounter =
        IntCounter::new("mooncake_store_valid_gets_total", "total valid get requests").unwrap();
}

// =========================================================================
// Transfer latency histograms (microseconds)
// =========================================================================

const LATENCY_BUCKETS: &[f64] = &[
    125.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0, 750.0, 1000.0,
    1500.0, 2000.0, 3000.0, 5000.0, 7000.0, 15000.0, 20000.0,
    50000.0, 100000.0, 200000.0, 500000.0, 1000000.0,
];

lazy_static! {
    pub static ref PUT_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_put_latency_us",
        "Put transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    ).unwrap();
    pub static ref GET_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_get_latency_us",
        "Get transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    ).unwrap();
    pub static ref BATCH_PUT_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_batch_put_latency_us",
        "Batch put transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    ).unwrap();
    pub static ref BATCH_GET_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_batch_get_latency_us",
        "Batch get transfer latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    ).unwrap();
    pub static ref MASTER_RPC_LATENCY_US: Histogram = register_histogram!(
        "mooncake_store_master_rpc_latency_us",
        "Master RPC latency in microseconds",
        LATENCY_BUCKETS.to_vec()
    ).unwrap();
    pub static ref TRANSFER_READ_BYTES: IntCounter =
        IntCounter::new("mooncake_store_transfer_read_bytes_total", "total bytes read via transfer engine").unwrap();
    pub static ref TRANSFER_WRITE_BYTES: IntCounter =
        IntCounter::new("mooncake_store_transfer_write_bytes_total", "total bytes written via transfer engine").unwrap();
    pub static ref VALUE_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "mooncake_store_value_size_bytes",
        "Distribution of stored value sizes",
        vec![64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0]
    ).unwrap();
}

// =========================================================================
// Snapshot metrics
// =========================================================================

lazy_static! {
    pub static ref SNAPSHOT_DURATION_MS: IntGauge =
        IntGauge::new("mooncake_store_snapshot_duration_ms", "last snapshot duration in milliseconds").unwrap();
    pub static ref SNAPSHOT_SUCCESS_COUNT: IntCounter =
        IntCounter::new("mooncake_store_snapshot_success_total", "total successful snapshots").unwrap();
    pub static ref SNAPSHOT_FAIL_COUNT: IntCounter =
        IntCounter::new("mooncake_store_snapshot_fail_total", "total failed snapshots").unwrap();
}

// =========================================================================
// HA metrics (OpLog replication)
// =========================================================================

lazy_static! {
    pub static ref OPLOG_LAST_SEQ_ID: IntGauge =
        IntGauge::new("mooncake_store_oplog_last_seq_id", "latest OpLog sequence ID on primary").unwrap();
    pub static ref OPLOG_APPLIED_SEQ_ID: IntGauge =
        IntGauge::new("mooncake_store_oplog_applied_seq_id", "standby applied OpLog sequence ID").unwrap();
    pub static ref OPLOG_STANDBY_LAG: IntGauge =
        IntGauge::new("mooncake_store_oplog_standby_lag", "standby replication lag in entries").unwrap();
    pub static ref OPLOG_PENDING_ENTRIES: IntGauge =
        IntGauge::new("mooncake_store_oplog_pending_entries", "pending out-of-order OpLog entries").unwrap();
    pub static ref PENDING_MUTATION_QUEUE_SIZE: IntGauge =
        IntGauge::new("mooncake_store_pending_mutation_queue_size", "pending mutation retry queue size").unwrap();
    pub static ref OPLOG_SKIPPED_ENTRIES: IntCounter =
        IntCounter::new("mooncake_store_oplog_skipped_entries_total", "total skipped OpLog entries").unwrap();
    pub static ref OPLOG_CHECKSUM_FAILURES: IntCounter =
        IntCounter::new("mooncake_store_oplog_checksum_failures_total", "total OpLog checksum failures").unwrap();
}

// =========================================================================
// Registration & HTTP server
// =========================================================================

fn register_counter(c: &IntCounter) {
    let _ = prometheus::register(Box::new(c.clone()));
}

fn register_gauge(g: &IntGauge) {
    let _ = prometheus::register(Box::new(g.clone()));
}

pub fn register_metrics() {
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

    register_gauge(&SEGMENT_COUNT);
    register_gauge(&OBJECT_COUNT);
    register_gauge(&KEY_COUNT);
    register_gauge(&SOFT_PIN_KEY_COUNT);
    register_gauge(&ACTIVE_CLIENTS);
    register_gauge(&ALLOCATED_MEM_SIZE);
    register_gauge(&TOTAL_MEM_CAPACITY);
    register_gauge(&ALLOCATED_FILE_SIZE);
    register_gauge(&TOTAL_FILE_CAPACITY);

    register_counter(&MEM_CACHE_HITS);
    register_counter(&FILE_CACHE_HITS);
    register_counter(&MEM_CACHE_TOTAL);
    register_counter(&FILE_CACHE_TOTAL);
    register_counter(&VALID_GETS);

    register_counter(&TRANSFER_READ_BYTES);
    register_counter(&TRANSFER_WRITE_BYTES);

    register_gauge(&SNAPSHOT_DURATION_MS);
    register_counter(&SNAPSHOT_SUCCESS_COUNT);
    register_counter(&SNAPSHOT_FAIL_COUNT);

    register_gauge(&OPLOG_LAST_SEQ_ID);
    register_gauge(&OPLOG_APPLIED_SEQ_ID);
    register_gauge(&OPLOG_STANDBY_LAG);
    register_gauge(&OPLOG_PENDING_ENTRIES);
    register_gauge(&PENDING_MUTATION_QUEUE_SIZE);
    register_counter(&OPLOG_SKIPPED_ENTRIES);
    register_counter(&OPLOG_CHECKSUM_FAILURES);
}

pub async fn serve_metrics_http(addr: SocketAddr) {
    register_metrics();

    let app = Router::new().route("/metrics", get(metrics_handler));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

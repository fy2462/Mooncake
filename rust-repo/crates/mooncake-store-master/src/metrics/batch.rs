use lazy_static::lazy_static;
use prometheus::IntCounter;

pub(crate) fn record_batch_outcome(
    total: usize,
    failed: usize,
    requests: &IntCounter,
    failures: &IntCounter,
    partial_successes: &IntCounter,
    items: &IntCounter,
    failed_items: &IntCounter,
) {
    requests.inc();
    items.inc_by(total as u64);
    failed_items.inc_by(failed as u64);
    if total > 0 && failed == total {
        failures.inc();
    } else if failed > 0 {
        partial_successes.inc();
    }
}

lazy_static! {
    pub static ref BATCH_GET_REPLICA_LIST_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_get_replica_list_total",
        "total batch get_replica_list requests"
    )
    .unwrap();
    pub static ref BATCH_GET_REPLICA_LIST_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_get_replica_list_failures_total",
        "total failed batch get_replica_list requests"
    )
    .unwrap();
    pub static ref BATCH_GET_REPLICA_LIST_PARTIAL_SUCCESSES: IntCounter = IntCounter::new(
        "mooncake_store_batch_get_replica_list_partial_successes_total",
        "total partially successful batch get_replica_list requests"
    )
    .unwrap();
    pub static ref BATCH_GET_REPLICA_LIST_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_get_replica_list_items_total",
        "total items processed by batch get_replica_list requests"
    )
    .unwrap();
    pub static ref BATCH_GET_REPLICA_LIST_FAILED_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_get_replica_list_failed_items_total",
        "total failed items in batch get_replica_list requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_START_REQUESTS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_start_total",
        "total batch put_start requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_START_FAILURES: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_start_failures_total",
        "total failed batch put_start requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_START_PARTIAL_SUCCESSES: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_start_partial_successes_total",
        "total partially successful batch put_start requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_START_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_start_items_total",
        "total items processed by batch put_start requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_START_FAILED_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_start_failed_items_total",
        "total failed items in batch put_start requests"
    )
    .unwrap();
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
    pub static ref BATCH_EXIST_KEY_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_exist_key_items_total",
        "total items processed by batch exist_key requests"
    )
    .unwrap();
    pub static ref BATCH_EXIST_KEY_PARTIAL_SUCCESSES: IntCounter = IntCounter::new(
        "mooncake_store_batch_exist_key_partial_successes_total",
        "total batch exist_key requests with partial per-item failures"
    )
    .unwrap();
    pub static ref BATCH_EXIST_KEY_FAILED_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_exist_key_failed_items_total",
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
    pub static ref BATCH_PUT_END_PARTIAL_SUCCESSES: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_end_partial_successes_total",
        "total partially successful batch put_end requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_END_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_end_items_total",
        "total items processed by batch put_end requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_END_FAILED_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_end_failed_items_total",
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
    pub static ref BATCH_PUT_REVOKE_PARTIAL_SUCCESSES: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_revoke_partial_successes_total",
        "total partially successful batch put_revoke requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_REVOKE_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_revoke_items_total",
        "total items processed by batch put_revoke requests"
    )
    .unwrap();
    pub static ref BATCH_PUT_REVOKE_FAILED_ITEMS: IntCounter = IntCounter::new(
        "mooncake_store_batch_put_revoke_failed_items_total",
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

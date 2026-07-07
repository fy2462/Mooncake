use lazy_static::lazy_static;
use prometheus::IntCounter;

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

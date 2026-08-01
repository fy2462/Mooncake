use mooncake_store_master::metrics;
use prometheus::IntGauge;
use std::sync::{Mutex, MutexGuard};

static METRICS_TEST_LOCK: Mutex<()> = Mutex::new(());

fn metrics_test_lock() -> MutexGuard<'static, ()> {
    METRICS_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct GaugeRestore(Vec<(IntGauge, i64)>);

impl GaugeRestore {
    fn new(gauges: &[&IntGauge]) -> Self {
        Self(
            gauges
                .iter()
                .map(|gauge| ((*gauge).clone(), gauge.get()))
                .collect(),
        )
    }
}

impl Drop for GaugeRestore {
    fn drop(&mut self) {
        for (gauge, value) in &self.0 {
            gauge.set(*value);
        }
    }
}

#[test]
fn cpp_parity_ha_metric_sets_last_sequence_id() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[&metrics::OPLOG_LAST_SEQ_ID]);
    metrics::OPLOG_LAST_SEQ_ID.set(123);
    assert_eq!(metrics::OPLOG_LAST_SEQ_ID.get(), 123);
}

#[test]
fn cpp_parity_ha_metric_sets_applied_sequence_id() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[&metrics::OPLOG_APPLIED_SEQ_ID]);
    metrics::OPLOG_APPLIED_SEQ_ID.set(456);
    assert_eq!(metrics::OPLOG_APPLIED_SEQ_ID.get(), 456);
}

#[test]
fn cpp_parity_ha_metric_sets_standby_lag() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[&metrics::OPLOG_STANDBY_LAG]);
    metrics::OPLOG_STANDBY_LAG.set(10);
    assert_eq!(metrics::OPLOG_STANDBY_LAG.get(), 10);
}

#[test]
fn cpp_parity_ha_metric_sets_pending_oplog_entries() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[&metrics::OPLOG_PENDING_ENTRIES]);
    metrics::OPLOG_PENDING_ENTRIES.set(7);
    assert_eq!(metrics::OPLOG_PENDING_ENTRIES.get(), 7);
}

#[test]
fn cpp_parity_ha_metric_sets_pending_mutation_queue_size() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[&metrics::PENDING_MUTATION_QUEUE_SIZE]);
    metrics::PENDING_MUTATION_QUEUE_SIZE.set(5);
    assert_eq!(metrics::PENDING_MUTATION_QUEUE_SIZE.get(), 5);
}

#[test]
fn cpp_parity_ha_metric_increments_skipped_entries_once() {
    let _lock = metrics_test_lock();
    let baseline = metrics::OPLOG_SKIPPED_ENTRIES.get();
    metrics::OPLOG_SKIPPED_ENTRIES.inc();
    assert_eq!(metrics::OPLOG_SKIPPED_ENTRIES.get(), baseline + 1);
}

#[test]
fn cpp_parity_ha_metric_increments_checksum_failures_by_two() {
    let _lock = metrics_test_lock();
    let baseline = metrics::OPLOG_CHECKSUM_FAILURES.get();
    metrics::OPLOG_CHECKSUM_FAILURES.inc_by(2);
    assert_eq!(metrics::OPLOG_CHECKSUM_FAILURES.get(), baseline + 2);
}

#[test]
fn cpp_parity_ha_metric_increments_gap_resolve_counters() {
    let _lock = metrics_test_lock();
    let attempts = metrics::OPLOG_GAP_RESOLVE_ATTEMPTS.get();
    let successes = metrics::OPLOG_GAP_RESOLVE_SUCCESS.get();
    metrics::OPLOG_GAP_RESOLVE_ATTEMPTS.inc_by(3);
    metrics::OPLOG_GAP_RESOLVE_SUCCESS.inc();
    assert_eq!(metrics::OPLOG_GAP_RESOLVE_ATTEMPTS.get(), attempts + 3);
    assert_eq!(metrics::OPLOG_GAP_RESOLVE_SUCCESS.get(), successes + 1);
}

#[test]
fn cpp_parity_ha_metric_increments_etcd_failures_and_retries() {
    let _lock = metrics_test_lock();
    let failures = metrics::OPLOG_ETCD_WRITE_FAILURES.get();
    let retries = metrics::OPLOG_ETCD_WRITE_RETRIES.get();
    metrics::OPLOG_ETCD_WRITE_FAILURES.inc_by(4);
    metrics::OPLOG_ETCD_WRITE_RETRIES.inc_by(5);
    assert_eq!(metrics::OPLOG_ETCD_WRITE_FAILURES.get(), failures + 4);
    assert_eq!(metrics::OPLOG_ETCD_WRITE_RETRIES.get(), retries + 5);
}

#[test]
fn cpp_parity_ha_metric_increments_watch_disconnects_and_applied_entries() {
    let _lock = metrics_test_lock();
    let disconnects = metrics::OPLOG_WATCH_DISCONNECTIONS.get();
    let applied = metrics::OPLOG_APPLIED_ENTRIES.get();
    metrics::OPLOG_WATCH_DISCONNECTIONS.inc_by(2);
    metrics::OPLOG_APPLIED_ENTRIES.inc_by(10);
    assert_eq!(metrics::OPLOG_WATCH_DISCONNECTIONS.get(), disconnects + 2);
    assert_eq!(metrics::OPLOG_APPLIED_ENTRIES.get(), applied + 10);
}

#[test]
fn cpp_parity_ha_metric_observes_etcd_write_latencies_without_panic() {
    let _lock = metrics_test_lock();
    let baseline = metrics::OPLOG_ETCD_WRITE_LATENCY_US.get_sample_count();
    metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(100.0);
    metrics::OPLOG_ETCD_WRITE_LATENCY_US.observe(5_000.0);
    assert_eq!(
        metrics::OPLOG_ETCD_WRITE_LATENCY_US.get_sample_count(),
        baseline + 2
    );
}

#[test]
fn cpp_parity_ha_metric_observes_apply_latencies_without_panic() {
    let _lock = metrics_test_lock();
    let baseline = metrics::OPLOG_APPLY_LATENCY_US.get_sample_count();
    metrics::OPLOG_APPLY_LATENCY_US.observe(50.0);
    metrics::OPLOG_APPLY_LATENCY_US.observe(1_000.0);
    assert_eq!(
        metrics::OPLOG_APPLY_LATENCY_US.get_sample_count(),
        baseline + 2
    );
}

#[test]
fn cpp_parity_ha_metric_serialization_exposes_cpp_names() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[
        &metrics::OPLOG_LAST_SEQ_ID,
        &metrics::OPLOG_APPLIED_SEQ_ID,
        &metrics::OPLOG_STANDBY_LAG,
    ]);
    metrics::OPLOG_LAST_SEQ_ID.set(1);
    metrics::OPLOG_APPLIED_SEQ_ID.set(1);
    metrics::OPLOG_STANDBY_LAG.set(0);

    let encoded = metrics::encode_ha_compat_metrics().unwrap();
    assert!(!encoded.is_empty());
    assert!(encoded.contains("ha_oplog_last_sequence_id"));
    assert!(encoded.contains("ha_oplog_applied_sequence_id"));
    assert!(encoded.contains("ha_oplog_standby_lag"));
}

#[test]
fn cpp_parity_ha_metric_summary_contains_sequence_labels() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[
        &metrics::OPLOG_LAST_SEQ_ID,
        &metrics::OPLOG_APPLIED_SEQ_ID,
        &metrics::OPLOG_STANDBY_LAG,
    ]);
    metrics::OPLOG_LAST_SEQ_ID.set(100);
    metrics::OPLOG_APPLIED_SEQ_ID.set(95);
    metrics::OPLOG_STANDBY_LAG.set(5);

    let summary = metrics::ha_summary();
    assert!(!summary.is_empty());
    assert!(summary.contains("last_seq"));
    assert!(summary.contains("applied_seq"));
}

#[test]
fn cpp_parity_ha_metric_global_accesses_share_state() {
    let _lock = metrics_test_lock();
    let _restore = GaugeRestore::new(&[&metrics::OPLOG_LAST_SEQ_ID]);
    let first = &*metrics::OPLOG_LAST_SEQ_ID;
    let second = &*metrics::OPLOG_LAST_SEQ_ID;
    first.set(1_234);
    assert_eq!(second.get(), 1_234);
}

#[test]
fn cpp_parity_ha_metric_concurrent_applied_entry_increments_are_exact() {
    let _lock = metrics_test_lock();
    let baseline = metrics::OPLOG_APPLIED_ENTRIES.get();
    let threads = (0..8)
        .map(|_| {
            let counter = metrics::OPLOG_APPLIED_ENTRIES.clone();
            std::thread::spawn(move || {
                for _ in 0..1_000 {
                    counter.inc();
                }
            })
        })
        .collect::<Vec<_>>();

    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(metrics::OPLOG_APPLIED_ENTRIES.get(), baseline + 8_000);
}

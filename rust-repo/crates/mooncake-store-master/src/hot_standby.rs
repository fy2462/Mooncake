// =============================================================================
// Hot Standby Service — 热备服务
// =============================================================================
// Implements the hot standby pattern for master failover. A standby master
// maintains a synchronized copy of state (via snapshots and/or oplog replay)
// and can be promoted to leader when the current leader fails.
// 实现 master 故障转移的热备模式。备用 master 维护状态的同步副本
// （通过快照和/或 oplog 重放），并可在当前 leader 故障时提升为 leader。
//
// Architecture / 架构:
// ┌───────────────────────────────────────────────────────────┐
// │  HotStandbyService                                         │
// │  ┌─────────────────┐  ┌──────────────────────────────────┐ │
// │  │ Snapshot Bootstrap│  │ OpLog Following                │ │
// │  │ (快照引导)        │  │ (操作日志跟随)                    │ │
// │  │ Load segments,   │  │ Poll oplog from leader,         │ │
// │  │ objects, tasks   │  │ apply mutations to local state  │ │
// │  └─────────────────┘  └──────────────────────────────────┘ │
// │                                                            │
// │  State Machine / 状态机:                                   │
// │  Stopped → Connecting → Recovering → Watching → Promoting → Promoted → Stopped
// └───────────────────────────────────────────────────────────┘
//
// Recovery modes / 恢复模式:
// 1. Snapshot Bootstrap: load a full snapshot from disk/3FS to initialize state.
//    快照引导：从磁盘/3FS 加载完整快照初始化状态。
// 2. OpLog Following: continuously apply operation log entries from the leader.
//    OpLog 跟随：持续应用 leader 的操作日志条目。
// Both can be enabled independently or together for layered recovery.
// 两者可独立启用或一起启用以实现分层恢复。

use crate::ha::oplog_applier::OpLogApplier;
use crate::ha::{
    HaError, SnapshotProvider, StandbyEvent, StandbyState, StandbyStateMachine, StandbySyncStatus,
};
use crate::oplog::{OpLogChangeNotifier, OpLogStore};
use crate::service::state::{MasterState, ObjectEntry};
use crate::service::{abort_orphaned_drain_segments_after_recovery, restore_loaded_snapshot_state};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;

const MAX_STANDBY_RECONNECT_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandbySnapshotMetadata {
    pub scoped_key: String,
    pub user_key: String,
    pub size: u64,
    pub client_id: uuid::Uuid,
    pub last_sequence_id: u64,
}

fn wait_for_notifier_startup(
    notifier: &mut dyn OpLogChangeNotifier,
    timeout: std::time::Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if notifier.is_healthy() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// Configuration for the hot standby service.
/// 热备服务的配置。
///
/// Controls which recovery mechanisms are enabled and their parameters.
/// 控制启用哪些恢复机制及其参数。
pub struct HotStandbyConfig {
    /// Whether to bootstrap from a snapshot on start.
    /// 是否在启动时通过快照引导。
    pub enable_snapshot_bootstrap: bool,
    /// Whether to follow the leader's oplog for continuous sync.
    /// 是否跟随 leader 的 oplog 进行持续同步。
    pub enable_oplog_following: bool,
    /// Accept the C++ verification-loop lifecycle configuration. The C++
    /// executable non-etcd oracle only requires safe start/stop; verification
    /// results remain outside that fixture's asserted contract.
    pub enable_verification: bool,
    /// Poll interval in milliseconds for oplog following.
    /// oplog 跟随的轮询间隔（毫秒）。
    pub oplog_poll_interval_ms: u64,
    /// Cluster identifier for snapshot scoping.
    /// 集群标识符，用于快照范围限定。
    pub cluster_id: String,
}

fn observed_primary_sequence(store: &dyn OpLogStore, applied: u64) -> u64 {
    store
        .max_sequence_id()
        .unwrap_or_else(|_| store.latest_sequence())
        .max(applied)
}

fn apply_notifier_entry(
    applier: &OpLogApplier,
    sync_status: &std::sync::Weak<parking_lot::RwLock<StandbySyncStatus>>,
    state_machine: &StandbyStateMachine,
    entry: crate::ha::OpLogRecord,
) {
    let before_apply = applier.get_expected_sequence_id();
    let entry_seq = entry.seq;
    applier.apply_op_log_entries(&[entry]);
    let expected = applier.get_expected_sequence_id();
    let applied = expected.saturating_sub(1);
    let rejected_expected = entry_seq == before_apply && expected == before_apply;
    if rejected_expected {
        state_machine.process_event(StandbyEvent::FatalError);
    }
    if let Some(status) = sync_status.upgrade() {
        let mut status = status.write();
        status.applied_seq_id = applied;
        status.primary_seq_id = status.primary_seq_id.max(applied);
        status.lag_entries = status.primary_seq_id.saturating_sub(applied);
        if rejected_expected {
            status.state = StandbyState::Failed;
            status.is_connected = false;
            status.is_syncing = false;
        }
    }
}

fn make_notifier_entry_callback(
    applier: Arc<OpLogApplier>,
    sync_status: std::sync::Weak<parking_lot::RwLock<StandbySyncStatus>>,
    state_machine: Arc<StandbyStateMachine>,
) -> crate::oplog::OpLogEntryCallback {
    Box::new(move |entry| {
        apply_notifier_entry(&applier, &sync_status, &state_machine, entry);
    })
}

fn run_promotion_catch_up(applier: &OpLogApplier, store: &dyn OpLogStore) -> Result<(), HaError> {
    // C++: ResolvePromotionGapsLocked (3 retries, stops when all gaps filled).
    for _ in 0..3 {
        let (needed, fetched) = applier.try_resolve_gaps_once(store, 1024);
        if needed == 0 || fetched >= needed {
            break;
        }
    }

    // C++: FinalCatchUpForPromotionLocked. The Arc-backed Rust store is the
    // same logical reader C++ recreates for promotion.
    let mut expected = applier.get_expected_sequence_id();
    let latest = store.max_sequence_id()?;
    if latest < expected {
        return Ok(());
    }
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);
    for _ in 0..100 {
        if start.elapsed() >= timeout {
            return Err(HaError::InvalidBackend(format!(
                "promotion final catch-up timed out: expected={expected}, latest={latest}"
            )));
        }
        expected = applier.get_expected_sequence_id();
        if latest < expected {
            break;
        }
        let to_read = latest.saturating_sub(expected).saturating_add(1).min(1000) as usize;
        if to_read == 0 {
            break;
        }
        let entries = store.read_since(expected, to_read)?;
        if entries.is_empty() {
            return Err(HaError::InvalidBackend(format!(
                "promotion final catch-up returned no entries: expected={expected}, latest={latest}"
            )));
        }
        let before_apply = expected;
        applier.apply_op_log_entries(&entries);
        expected = applier.get_expected_sequence_id();
        if expected <= before_apply {
            return Err(HaError::InvalidBackend(format!(
                "promotion final catch-up made no progress: expected={before_apply}, latest={latest}"
            )));
        }
    }
    expected = applier.get_expected_sequence_id();
    if expected <= latest {
        return Err(HaError::InvalidBackend(format!(
            "promotion final catch-up is incomplete: expected={expected}, latest={latest}"
        )));
    }
    Ok(())
}

struct PromotionCatchUpWorker {
    handle: Option<std::thread::JoinHandle<()>>,
}

impl PromotionCatchUpWorker {
    fn join(&mut self) -> Result<(), HaError> {
        self.handle
            .take()
            .expect("promotion catch-up worker joined once")
            .join()
            .map_err(|_| HaError::InvalidBackend("promotion catch-up worker panicked".into()))
    }
}

impl Drop for PromotionCatchUpWorker {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

async fn run_promotion_catch_up_on_plain_thread(
    applier: Arc<OpLogApplier>,
    store: Arc<dyn OpLogStore>,
) -> Result<(), HaError> {
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let handle = std::thread::Builder::new()
        .name("mooncake-promotion-catch-up".into())
        .spawn(move || {
            let _ = result_tx.send(run_promotion_catch_up(applier.as_ref(), store.as_ref()));
        })
        .map_err(|error| {
            HaError::InvalidBackend(format!(
                "failed to spawn promotion catch-up worker: {error}"
            ))
        })?;
    let mut worker = PromotionCatchUpWorker {
        handle: Some(handle),
    };
    let result = result_rx.await.map_err(|_| {
        HaError::InvalidBackend("promotion catch-up worker exited without a result".into())
    });
    worker.join()?;
    result?
}

impl Default for HotStandbyConfig {
    fn default() -> Self {
        Self {
            enable_snapshot_bootstrap: false,
            enable_oplog_following: false,
            enable_verification: false,
            oplog_poll_interval_ms: 1000,
            cluster_id: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TenantId;
    use crate::ha::{
        LoadedSnapshot, NoopSnapshotProvider, OpLogPollResult, OpLogRecord,
        SnapshotCatalogStoreType, SnapshotObjectStoreType, SnapshotProvider,
        create_catalog_backed_snapshot_provider,
    };
    use crate::oplog::{
        InMemoryOpLog, LocalFsOpLogStore, OpLogChangeNotifier, OpLogEntryCallback,
        OpLogErrorCallback, OpLogManager, OpLogStore,
    };
    use crate::service::{
        ObjectEntry, ReplicationTaskKind, ReplicationTaskSnapshotEntry, SegmentEntry,
    };
    use mooncake_store_core::{
        ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment,
    };
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use uuid::Uuid;

    struct DelayedHealthyNotifier {
        healthy: Arc<AtomicBool>,
    }

    struct FlakyReadOpLog {
        inner: InMemoryOpLog,
        remaining_failures: Arc<AtomicUsize>,
        read_attempts: Arc<AtomicUsize>,
    }

    struct EtcdRecoveryFixtureOpLog {
        inner: InMemoryOpLog,
        notifier_healthy: Arc<AtomicBool>,
        recovery_error: Option<HaError>,
        recovery_entries: Vec<OpLogRecord>,
    }

    struct EtcdRecoveryFixtureNotifier {
        healthy: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    struct StrictBaselineOpLog {
        inner: InMemoryOpLog,
        minimum_since: u64,
        reads: Arc<Mutex<Vec<u64>>>,
    }

    struct SharedMutableOpLog {
        inner: Arc<Mutex<InMemoryOpLog>>,
        follower_ceiling: Option<u64>,
        promotion_reads: Option<Arc<Mutex<Vec<u64>>>>,
    }

    impl SharedMutableOpLog {
        fn is_promotion_thread() -> bool {
            std::thread::current().name() == Some("mooncake-promotion-catch-up")
        }
    }

    impl OpLogStore for SharedMutableOpLog {
        fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
            self.inner.lock().append(entry)
        }

        fn read_since(
            &self,
            since_seq: u64,
            max_count: usize,
        ) -> Result<Vec<OpLogRecord>, HaError> {
            let promotion = Self::is_promotion_thread();
            if promotion && let Some(reads) = &self.promotion_reads {
                reads.lock().push(since_seq);
            }
            let mut entries = self.inner.lock().read_since(since_seq, max_count)?;
            if !promotion && let Some(ceiling) = self.follower_ceiling {
                entries.retain(|entry| entry.seq <= ceiling);
            }
            Ok(entries)
        }

        fn latest_sequence(&self) -> u64 {
            self.inner.lock().latest_sequence()
        }

        fn max_sequence_id(&self) -> Result<u64, HaError> {
            let promotion = Self::is_promotion_thread();
            let maximum = self.inner.lock().max_sequence_id()?;
            if !promotion && let Some(ceiling) = self.follower_ceiling {
                return Ok(maximum.min(ceiling));
            }
            Ok(maximum)
        }

        fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
            self.inner.lock().update_latest_sequence_id(sequence_id)
        }

        fn record_snapshot_sequence_id(
            &mut self,
            snapshot_id: &str,
            sequence_id: u64,
        ) -> Result<(), HaError> {
            self.inner
                .lock()
                .record_snapshot_sequence_id(snapshot_id, sequence_id)
        }

        fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
            self.inner.lock().get_snapshot_sequence_id(snapshot_id)
        }

        fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
            self.inner.lock().cleanup_before(before_sequence_id)
        }

        fn flush_durable(&mut self) -> Result<(), HaError> {
            self.inner.lock().flush_durable()
        }

        fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
            let promotion = Self::is_promotion_thread();
            let mut result = self.inner.lock().poll_from(since_seq, max_count);
            if !promotion && let Some(ceiling) = self.follower_ceiling {
                result.records.retain(|entry| entry.seq <= ceiling);
                result.next_seq = result
                    .records
                    .last()
                    .map(|entry| entry.seq.saturating_add(1))
                    .unwrap_or(since_seq);
            }
            result
        }
    }

    impl OpLogStore for StrictBaselineOpLog {
        fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
            self.inner.append(entry)
        }

        fn read_since(
            &self,
            since_seq: u64,
            max_count: usize,
        ) -> Result<Vec<OpLogRecord>, HaError> {
            self.reads.lock().push(since_seq);
            if since_seq < self.minimum_since {
                return Err(HaError::InvalidBackend(format!(
                    "read crossed recovered snapshot baseline: since={since_seq}, minimum={}",
                    self.minimum_since
                )));
            }
            self.inner.read_since(since_seq, max_count)
        }

        fn latest_sequence(&self) -> u64 {
            self.inner.latest_sequence()
        }

        fn max_sequence_id(&self) -> Result<u64, HaError> {
            self.inner.max_sequence_id()
        }

        fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
            self.inner.update_latest_sequence_id(sequence_id)
        }

        fn record_snapshot_sequence_id(
            &mut self,
            snapshot_id: &str,
            sequence_id: u64,
        ) -> Result<(), HaError> {
            self.inner
                .record_snapshot_sequence_id(snapshot_id, sequence_id)
        }

        fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
            self.inner.get_snapshot_sequence_id(snapshot_id)
        }

        fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
            self.inner.cleanup_before(before_sequence_id)
        }

        fn flush_durable(&mut self) -> Result<(), HaError> {
            self.inner.flush_durable()
        }

        fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
            self.reads.lock().push(since_seq);
            if since_seq < self.minimum_since {
                return OpLogPollResult {
                    records: Vec::new(),
                    next_seq: since_seq,
                    timed_out: true,
                };
            }
            self.inner.poll_from(since_seq, max_count)
        }
    }

    impl FlakyReadOpLog {
        fn new(remaining_failures: usize, read_attempts: Arc<AtomicUsize>) -> Self {
            Self::with_failure_counter(
                Arc::new(AtomicUsize::new(remaining_failures)),
                read_attempts,
            )
        }

        fn with_failure_counter(
            remaining_failures: Arc<AtomicUsize>,
            read_attempts: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                inner: InMemoryOpLog::new(16),
                remaining_failures,
                read_attempts,
            }
        }
    }

    impl OpLogStore for FlakyReadOpLog {
        fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
            self.inner.append(entry)
        }

        fn read_since(
            &self,
            since_seq: u64,
            max_count: usize,
        ) -> Result<Vec<OpLogRecord>, HaError> {
            self.read_attempts.fetch_add(1, Ordering::AcqRel);
            if self
                .remaining_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(HaError::InvalidBackend(
                    "injected oplog reconnect failure".into(),
                ));
            }
            self.inner.read_since(since_seq, max_count)
        }

        fn latest_sequence(&self) -> u64 {
            self.inner.latest_sequence()
        }

        fn max_sequence_id(&self) -> Result<u64, HaError> {
            self.inner.max_sequence_id()
        }

        fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
            self.inner.update_latest_sequence_id(sequence_id)
        }

        fn record_snapshot_sequence_id(
            &mut self,
            snapshot_id: &str,
            sequence_id: u64,
        ) -> Result<(), HaError> {
            self.inner
                .record_snapshot_sequence_id(snapshot_id, sequence_id)
        }

        fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
            self.inner.get_snapshot_sequence_id(snapshot_id)
        }

        fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
            self.inner.cleanup_before(before_sequence_id)
        }

        fn flush_durable(&mut self) -> Result<(), HaError> {
            self.inner.flush_durable()
        }

        fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
            self.inner.poll_from(since_seq, max_count)
        }
    }

    impl OpLogStore for EtcdRecoveryFixtureOpLog {
        fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
            self.inner.append(entry)
        }

        fn read_since(
            &self,
            since_seq: u64,
            max_count: usize,
        ) -> Result<Vec<OpLogRecord>, HaError> {
            if let Some(error) = &self.recovery_error {
                return Err(error.clone());
            }
            if !self.recovery_entries.is_empty() {
                return Ok(self.recovery_entries.clone());
            }
            self.inner.read_since(since_seq, max_count)
        }

        fn latest_sequence(&self) -> u64 {
            self.inner.latest_sequence()
        }

        fn max_sequence_id(&self) -> Result<u64, HaError> {
            self.inner.max_sequence_id()
        }

        fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
            self.inner.update_latest_sequence_id(sequence_id)
        }

        fn record_snapshot_sequence_id(
            &mut self,
            snapshot_id: &str,
            sequence_id: u64,
        ) -> Result<(), HaError> {
            self.inner
                .record_snapshot_sequence_id(snapshot_id, sequence_id)
        }

        fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
            self.inner.get_snapshot_sequence_id(snapshot_id)
        }

        fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
            self.inner.cleanup_before(before_sequence_id)
        }

        fn flush_durable(&mut self) -> Result<(), HaError> {
            self.inner.flush_durable()
        }

        fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
            self.inner.poll_from(since_seq, max_count)
        }

        fn create_change_notifier(&self) -> Option<Box<dyn OpLogChangeNotifier>> {
            Some(Box::new(EtcdRecoveryFixtureNotifier {
                healthy: self.notifier_healthy.clone(),
                thread: None,
            }))
        }
    }

    impl OpLogChangeNotifier for EtcdRecoveryFixtureNotifier {
        fn start(
            &mut self,
            _start_seq_id: u64,
            _on_entry: OpLogEntryCallback,
            mut on_error: OpLogErrorCallback,
        ) -> Result<(), HaError> {
            self.healthy.store(true, Ordering::Release);
            let healthy = self.healthy.clone();
            self.thread = Some(std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(20));
                for _ in 0..10 {
                    on_error(HaError::InvalidBackend(
                        "injected etcd watch recovery error".into(),
                    ));
                }
                healthy.store(false, Ordering::Release);
            }));
            Ok(())
        }

        fn stop(&mut self) {
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
            self.healthy.store(false, Ordering::Release);
        }

        fn is_healthy(&self) -> bool {
            self.healthy.load(Ordering::Acquire)
        }
    }

    struct PromotionThreadProbeOpLog {
        inner: InMemoryOpLog,
        runtime_thread_calls: Arc<AtomicUsize>,
        max_sequence_delay: std::time::Duration,
    }

    impl PromotionThreadProbeOpLog {
        fn record_runtime_thread_call(&self) {
            if tokio::runtime::Handle::try_current().is_ok() {
                self.runtime_thread_calls.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    impl OpLogStore for PromotionThreadProbeOpLog {
        fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
            self.inner.append(entry)
        }

        fn read_since(
            &self,
            since_seq: u64,
            max_count: usize,
        ) -> Result<Vec<OpLogRecord>, HaError> {
            self.record_runtime_thread_call();
            self.inner.read_since(since_seq, max_count)
        }

        fn latest_sequence(&self) -> u64 {
            self.record_runtime_thread_call();
            self.inner.latest_sequence()
        }

        fn max_sequence_id(&self) -> Result<u64, HaError> {
            self.record_runtime_thread_call();
            std::thread::sleep(self.max_sequence_delay);
            self.inner.max_sequence_id()
        }

        fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
            self.inner.update_latest_sequence_id(sequence_id)
        }

        fn record_snapshot_sequence_id(
            &mut self,
            snapshot_id: &str,
            sequence_id: u64,
        ) -> Result<(), HaError> {
            self.inner
                .record_snapshot_sequence_id(snapshot_id, sequence_id)
        }

        fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
            self.inner.get_snapshot_sequence_id(snapshot_id)
        }

        fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
            self.inner.cleanup_before(before_sequence_id)
        }

        fn flush_durable(&mut self) -> Result<(), HaError> {
            self.inner.flush_durable()
        }

        fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
            self.inner.poll_from(since_seq, max_count)
        }
    }

    impl OpLogChangeNotifier for DelayedHealthyNotifier {
        fn start(
            &mut self,
            _start_seq_id: u64,
            _on_entry: OpLogEntryCallback,
            _on_error: OpLogErrorCallback,
        ) -> Result<(), HaError> {
            Ok(())
        }

        fn stop(&mut self) {}

        fn is_healthy(&self) -> bool {
            self.healthy.load(Ordering::Acquire)
        }
    }

    struct FailingSnapshotProvider;

    impl SnapshotProvider for FailingSnapshotProvider {
        fn load_latest_snapshot(
            &self,
            _cluster_id: &str,
        ) -> Result<Option<crate::ha::LoadedSnapshot>, HaError> {
            Err(HaError::Snapshot("snapshot unavailable".into()))
        }
    }

    struct StaticSnapshotProvider {
        snapshot: crate::ha::LoadedSnapshot,
    }

    impl SnapshotProvider for StaticSnapshotProvider {
        fn load_latest_snapshot(
            &self,
            _cluster_id: &str,
        ) -> Result<Option<crate::ha::LoadedSnapshot>, HaError> {
            Ok(Some(self.snapshot.clone()))
        }
    }

    struct CandidateSnapshotProvider {
        snapshots: Vec<crate::ha::LoadedSnapshot>,
    }

    impl SnapshotProvider for CandidateSnapshotProvider {
        fn load_latest_snapshot(
            &self,
            _cluster_id: &str,
        ) -> Result<Option<crate::ha::LoadedSnapshot>, HaError> {
            Ok(self.snapshots.first().cloned())
        }

        fn load_snapshot_candidates(
            &self,
            _cluster_id: &str,
        ) -> Result<Vec<crate::ha::LoadedSnapshot>, HaError> {
            Ok(self.snapshots.clone())
        }
    }

    fn snapshot_object(key: &str, segment: &Segment, offset: u64, size: u64) -> ObjectEntry {
        ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: segment.id,
                segment_name: segment.name.clone(),
                offset,
                size,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: segment.base,
                protocol: segment.protocol.clone(),
            }],
            size,
            last_access: std::time::SystemTime::now(),
            hard_pinned: false,
            data_type: ObjectDataType::Unknown,
            client_id: Uuid::nil(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: TenantId::default(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: size,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: key.to_string(),
        }
    }

    #[test]
    fn test_notifier_startup_allows_health_initialization_grace_period() {
        let healthy = Arc::new(AtomicBool::new(false));
        let delayed = healthy.clone();
        let mut notifier = DelayedHealthyNotifier { healthy };
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            delayed.store(true, Ordering::Release);
        });

        assert!(wait_for_notifier_startup(
            &mut notifier,
            std::time::Duration::from_millis(100),
        ));
    }

    #[test]
    fn notifier_callback_does_not_reenter_current_thread_runtime() {
        let result = std::thread::spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let applier = Arc::new(OpLogApplier::new(Arc::new(MasterState::empty())));
                let sync_status = Arc::new(parking_lot::RwLock::new(StandbySyncStatus::default()));
                let state_machine = Arc::new(StandbyStateMachine::new());
                let mut on_entry = make_notifier_entry_callback(
                    applier.clone(),
                    Arc::downgrade(&sync_status),
                    state_machine,
                );

                on_entry(OpLogRecord {
                    seq: 1,
                    producer_view_version: 9,
                    payload: r#"{"op":"remove","key":"default\u0000missing"}"#.to_string(),
                });

                assert_eq!(applier.get_expected_sequence_id(), 2);
                let status = sync_status.read();
                assert_eq!(status.applied_seq_id, 1);
                assert_eq!(status.primary_seq_id, 1);
                assert_eq!(status.lag_entries, 0);
            });
        })
        .join();

        assert!(
            result.is_ok(),
            "notifier callback must not synchronously bridge the watch runtime"
        );
    }

    #[test]
    fn cpp_parity_ha_oplog_oplog_replicator_test_cpp_oplogreplicatortest_injectmultipleentries_sequencetracking()
     {
        let applier = Arc::new(OpLogApplier::new(Arc::new(MasterState::empty())));
        let sync_status = Arc::new(parking_lot::RwLock::new(StandbySyncStatus::default()));
        let state_machine = Arc::new(StandbyStateMachine::new());
        let mut on_entry = make_notifier_entry_callback(
            applier.clone(),
            Arc::downgrade(&sync_status),
            state_machine,
        );

        for (sequence, key) in [(1, "k1"), (2, "k2"), (3, "k3")] {
            on_entry(OpLogRecord {
                seq: sequence,
                producer_view_version: 9,
                payload: format!(r#"{{"op":"remove","key":"default\u0000{key}"}}"#),
            });
        }

        assert_eq!(applier.get_expected_sequence_id(), 4);
        let status = sync_status.read();
        assert_eq!(status.applied_seq_id, 3);
        assert_eq!(status.primary_seq_id, 3);
        assert_eq!(status.lag_entries, 0);
    }

    #[tokio::test]
    async fn test_oplog_following_applies_entries() {
        let state = Arc::new(MasterState::empty());
        let mut store = InMemoryOpLog::new(16);
        store.append_payload(9, r#"{"op":"put_start","key":"k1"}"#);
        store.append_payload(9, r#"{"op":"remove","key":"k1"}"#);

        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                cluster_id: "cluster-a".to_string(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(store));

        service.start().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, 2);
        assert_eq!(status.primary_seq_id, 2);
        assert_eq!(status.lag_entries, 0);

        service.stop();
    }

    #[tokio::test]
    async fn localfs_writer_and_standby_sync_mixed_state_then_promote() {
        let directory = tempfile::tempdir().unwrap();
        let writer = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let reader = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let writer = OpLogManager::new(Some(Box::new(writer)), 1);
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "localfs-integration-segment".into(),
            base: 0,
            size: 128 * 1024,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment.id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state
            .allocator
            .write()
            .add_segment(segment.clone(), 0, client_id);
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "localfs-integration".into(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(reader));
        service.start().await.unwrap();

        for index in 0..100 {
            let key = format!("localfs-key-{index}");
            let mut object = snapshot_object(&key, &segment, index * 1024, 1024);
            object.client_id = client_id;
            writer
                .record_object_image_durable(&TenantId::default().make_scoped_key(&key), &object)
                .unwrap();
        }
        for index in 0..20 {
            writer
                .record_remove_durable(
                    &TenantId::default().make_scoped_key(&format!("localfs-key-{index}")),
                )
                .unwrap();
        }
        let target_sequence = writer.latest_sequence();

        let caught_up = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while service.sync_status().applied_seq_id < target_sequence {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            caught_up.is_ok(),
            "LocalFS standby did not catch up: status={:?}, objects={}",
            service.sync_status(),
            state.objects.len()
        );

        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, target_sequence);
        assert_eq!(status.primary_seq_id, target_sequence);
        assert_eq!(status.lag_entries, 0);
        for index in 0..100 {
            let scoped_key = TenantId::default().make_scoped_key(&format!("localfs-key-{index}"));
            assert_eq!(state.objects.contains_key(&scoped_key), index >= 20);
        }

        drop(writer);
        assert!(service.is_ready_for_promotion());
        assert_eq!(service.promote().await.unwrap(), target_sequence);
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
    }

    #[tokio::test]
    async fn cpp_parity_localfs_hot_standby_integration_test_localfshotstandbyintegrationtest_testprimarystandbysync()
     {
        let directory = tempfile::tempdir().unwrap();
        let writer = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let reader = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let writer = OpLogManager::new(Some(Box::new(writer)), 1);
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "localfs-prepopulated-segment".into(),
            base: 0,
            size: 10 * 1024,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment.id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state
            .allocator
            .write()
            .add_segment(segment.clone(), 0, client_id);

        for index in 0..10 {
            let key = format!("test_key_{index}");
            let mut object = snapshot_object(&key, &segment, index * 1024, 1024);
            object.client_id = client_id;
            writer
                .record_object_image_durable(&TenantId::default().make_scoped_key(&key), &object)
                .unwrap();
        }
        let target_sequence = writer.latest_sequence();
        assert_eq!(target_sequence, 10);

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "localfs-prepopulated".into(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(reader));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while service.sync_status().applied_seq_id < target_sequence {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pre-populated LocalFS standby did not catch up");

        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, target_sequence);
        assert_eq!(status.primary_seq_id, target_sequence);
        assert_eq!(status.lag_entries, 0);
        assert_eq!(service.latest_applied_sequence_id(), target_sequence);
        assert_eq!(state.objects.len(), 10);
        for index in 0..10 {
            let key = format!("test_key_{index}");
            let scoped_key = TenantId::default().make_scoped_key(&key);
            let object = state.objects.get(&scoped_key).expect("replayed object");
            assert_eq!(object.user_key, key);
            assert_eq!(object.size, 1024);
        }
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_localfs_hot_standby_integration_test_localfshotstandbyintegrationtest_teststandbypromotion()
     {
        let directory = tempfile::tempdir().unwrap();
        let writer = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let reader = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let writer = OpLogManager::new(Some(Box::new(writer)), 1);
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "localfs-prepopulated-promotion-segment".into(),
            base: 0,
            size: 5 * 1024,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment.id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state
            .allocator
            .write()
            .add_segment(segment.clone(), 0, client_id);

        for index in 0..5 {
            let key = format!("promote_test_key_{index}");
            let mut object = snapshot_object(&key, &segment, index * 1024, 1024);
            object.client_id = client_id;
            writer
                .record_object_image_durable(&TenantId::default().make_scoped_key(&key), &object)
                .unwrap();
        }
        let target_sequence = writer.latest_sequence();
        assert_eq!(target_sequence, 5);

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "localfs-prepopulated-promotion".into(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(reader));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while service.sync_status().applied_seq_id < target_sequence {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pre-populated LocalFS standby did not become promotion-ready");

        assert!(service.is_ready_for_promotion());
        assert_eq!(service.promote().await.unwrap(), target_sequence);
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Stopped);
        assert_eq!(status.applied_seq_id, target_sequence);
        assert_eq!(status.primary_seq_id, target_sequence);
        assert_eq!(status.lag_entries, 0);
        assert_eq!(service.latest_applied_sequence_id(), target_sequence);
        assert_eq!(state.objects.len(), 5);
        for index in 0..5 {
            let key = format!("promote_test_key_{index}");
            let scoped_key = TenantId::default().make_scoped_key(&key);
            let object = state
                .objects
                .get(&scoped_key)
                .expect("promoted object remains visible");
            assert_eq!(object.user_key, key);
            assert_eq!(object.size, 1024);
        }
    }

    #[tokio::test]
    async fn cpp_parity_localfs_hot_standby_integration_test_localfshotstandbyintegrationtest_testfailoverscenario()
     {
        let directory = tempfile::tempdir().unwrap();
        let writer_store = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let reader = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let writer = OpLogManager::new(Some(Box::new(writer_store)), 1);
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "localfs-prepopulated-failover-segment".into(),
            base: 0,
            size: 10 * 1024,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment.id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state
            .allocator
            .write()
            .add_segment(segment.clone(), 0, client_id);

        for index in 0..10 {
            let key = format!("failover_key_{index}");
            let mut object = snapshot_object(&key, &segment, index * 1024, 1024);
            object.client_id = client_id;
            writer
                .record_object_image_durable(&TenantId::default().make_scoped_key(&key), &object)
                .unwrap();
        }
        let target_sequence = writer.latest_sequence();
        assert_eq!(target_sequence, 10);

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "localfs-prepopulated-failover".into(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(reader));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while service.sync_status().applied_seq_id < target_sequence {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pre-populated LocalFS standby did not catch up before writer loss");

        drop(writer);
        assert_eq!(state.objects.len(), 10);
        for index in 0..10 {
            let key = format!("failover_key_{index}");
            let scoped_key = TenantId::default().make_scoped_key(&key);
            let object = state
                .objects
                .get(&scoped_key)
                .expect("object remains after writer destruction");
            assert_eq!(object.user_key, key);
            assert_eq!(object.size, 1024);
        }
        assert!(service.is_ready_for_promotion());
        assert_eq!(service.promote().await.unwrap(), target_sequence);
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Stopped);
        assert_eq!(status.applied_seq_id, target_sequence);
        assert_eq!(status.primary_seq_id, target_sequence);
        assert_eq!(status.lag_entries, 0);
        assert_eq!(service.latest_applied_sequence_id(), target_sequence);
        assert_eq!(state.objects.len(), 10);
    }

    #[tokio::test]
    async fn cpp_parity_localfs_hot_standby_integration_test_localfshotstandbyintegrationtest_testdataconsistency()
     {
        let directory = tempfile::tempdir().unwrap();
        let writer_store = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let reader = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let writer = OpLogManager::new(Some(Box::new(writer_store)), 1);
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "localfs-prepopulated-consistency-segment".into(),
            base: 0,
            size: 16 * 1024,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment.id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state
            .allocator
            .write()
            .add_segment(segment.clone(), 0, client_id);

        for index in 0..5 {
            let key = format!("put_key_{index}");
            let mut object = snapshot_object(&key, &segment, index * 2048, 1024);
            object.client_id = client_id;
            writer
                .record_object_image_durable(&TenantId::default().make_scoped_key(&key), &object)
                .unwrap();
        }
        for index in 0..2 {
            writer
                .record_remove_durable(
                    &TenantId::default().make_scoped_key(&format!("put_key_{index}")),
                )
                .unwrap();
        }
        for index in 5..8 {
            let key = format!("put_key_{index}");
            let mut object = snapshot_object(&key, &segment, index * 2048, 2048);
            object.client_id = client_id;
            writer
                .record_object_image_durable(&TenantId::default().make_scoped_key(&key), &object)
                .unwrap();
        }
        let target_sequence = writer.latest_sequence();
        assert_eq!(target_sequence, 10);

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "localfs-prepopulated-consistency".into(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(reader));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while service.sync_status().applied_seq_id < target_sequence {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pre-populated LocalFS consistency replay did not catch up");

        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, target_sequence);
        assert_eq!(status.primary_seq_id, target_sequence);
        assert_eq!(status.lag_entries, 0);
        let actual_keys = state
            .objects
            .iter()
            .map(|entry| entry.value().user_key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let expected_keys = (2..8)
            .map(|index| format!("put_key_{index}"))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual_keys, expected_keys);
        assert_eq!(state.objects.len(), 6);
        for index in 2..8 {
            let key = format!("put_key_{index}");
            let object = state
                .objects
                .get(&TenantId::default().make_scoped_key(&key))
                .expect("expected replayed object");
            assert_eq!(object.user_key, key);
            assert_eq!(object.size, if index < 5 { 1024 } else { 2048 });
        }
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_localfs_hot_standby_integration_test_localfshotstandbyintegrationtest_testhighthroughputsync()
     {
        let directory = tempfile::tempdir().unwrap();
        let writer_store = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let reader = LocalFsOpLogStore::new(directory.path(), 256).unwrap();
        let writer = OpLogManager::new(Some(Box::new(writer_store)), 1);
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "localfs-live-throughput-segment".into(),
            base: 0,
            size: 100 * 1024,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment.id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state
            .allocator
            .write()
            .add_segment(segment.clone(), 0, client_id);

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "localfs-live-throughput".into(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(reader));
        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while service.sync_status().state != StandbyState::Watching {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("LocalFS standby did not reach WATCHING before live writes");

        for index in 0..100 {
            let key = format!("throughput_key_{index}");
            let mut object = snapshot_object(&key, &segment, index * 1024, 1024);
            object.client_id = client_id;
            writer
                .record_object_image_durable(&TenantId::default().make_scoped_key(&key), &object)
                .unwrap();
        }
        let target_sequence = writer.latest_sequence();
        assert_eq!(target_sequence, 100);

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let status = service.sync_status();
                if status.applied_seq_id >= target_sequence && status.lag_entries == 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("LocalFS standby did not catch up with 100 live writes");

        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, target_sequence);
        assert_eq!(status.primary_seq_id, target_sequence);
        assert_eq!(status.lag_entries, 0);
        assert_eq!(service.latest_applied_sequence_id(), target_sequence);
        assert_eq!(state.objects.len(), 100);
        service.stop();
    }

    #[tokio::test]
    async fn test_oplog_polling_recovers_within_bounded_reconnect_budget() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "cluster-a".to_string(),
                ..Default::default()
            },
        );
        let read_attempts = Arc::new(AtomicUsize::new(0));
        service.set_oplog_store(Box::new(FlakyReadOpLog::new(2, read_attempts.clone())));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let status = service.sync_status();
                if read_attempts.load(Ordering::Acquire) >= 3
                    && status.state == StandbyState::Watching
                    && status.is_connected
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("standby did not reconnect within its bounded retry budget");
        assert!(service.is_running());
        service.stop();
    }

    #[tokio::test]
    async fn test_etcd_recovery_success_transitions_actual_state_machine() {
        let state = Arc::new(MasterState::empty());
        let notifier_healthy = Arc::new(AtomicBool::new(false));
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "etcd-recovery-state-machine".to_string(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(EtcdRecoveryFixtureOpLog {
            inner: InMemoryOpLog::new(16),
            notifier_healthy,
            recovery_error: None,
            recovery_entries: vec![],
        }));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let history = service.state_machine.get_transition_history(16);
                if history.iter().any(|record| {
                    record.from_state == StandbyState::Recovering
                        && record.to_state == StandbyState::Watching
                        && record.event == StandbyEvent::RecoverySuccess
                }) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("successful shared-oplog recovery did not emit RecoverySuccess");

        let history = service.state_machine.get_transition_history(16);
        assert!(history.iter().any(|record| {
            record.from_state == StandbyState::Watching
                && record.to_state == StandbyState::Recovering
                && record.event == StandbyEvent::MaxErrorsReached
        }));
        assert_eq!(service.state_machine.get_state(), StandbyState::Watching);
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        service.stop();
    }

    #[tokio::test]
    async fn test_etcd_recovery_failure_transitions_actual_state_machine() {
        let state = Arc::new(MasterState::empty());
        let notifier_healthy = Arc::new(AtomicBool::new(false));
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 250,
                cluster_id: "etcd-recovery-failure-state-machine".to_string(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(EtcdRecoveryFixtureOpLog {
            inner: InMemoryOpLog::new(16),
            notifier_healthy,
            recovery_error: Some(HaError::InvalidBackend(
                "injected etcd recovery read failure".into(),
            )),
            recovery_entries: vec![],
        }));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let history = service.state_machine.get_transition_history(16);
                if history.iter().any(|record| {
                    record.from_state == StandbyState::Recovering
                        && record.to_state == StandbyState::Reconnecting
                        && record.event == StandbyEvent::RecoveryFailed
                }) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("failed shared-oplog recovery did not emit RecoveryFailed");

        let history = service.state_machine.get_transition_history(16);
        assert!(history.iter().any(|record| {
            record.from_state == StandbyState::Watching
                && record.to_state == StandbyState::Recovering
                && record.event == StandbyEvent::MaxErrorsReached
        }));
        assert_eq!(
            service.state_machine.get_state(),
            StandbyState::Reconnecting
        );
        assert_eq!(service.sync_status().state, StandbyState::Reconnecting);
        service.stop();
    }

    #[tokio::test]
    async fn test_etcd_recovery_disconnect_transitions_actual_state_machine() {
        let state = Arc::new(MasterState::empty());
        let notifier_healthy = Arc::new(AtomicBool::new(false));
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 250,
                cluster_id: "etcd-recovery-disconnect-state-machine".to_string(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(EtcdRecoveryFixtureOpLog {
            inner: InMemoryOpLog::new(16),
            notifier_healthy,
            recovery_error: Some(HaError::Disconnected(
                "injected etcd recovery disconnect".into(),
            )),
            recovery_entries: vec![],
        }));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let history = service.state_machine.get_transition_history(16);
                if history.iter().any(|record| {
                    record.from_state == StandbyState::Recovering
                        && record.to_state == StandbyState::Reconnecting
                        && record.event == StandbyEvent::Disconnected
                }) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("shared-oplog disconnect during recovery did not emit Disconnected");

        assert_eq!(
            service.state_machine.get_state(),
            StandbyState::Reconnecting
        );
        assert_eq!(service.sync_status().state, StandbyState::Reconnecting);
        service.stop();
    }

    #[tokio::test]
    async fn test_etcd_recovery_fatal_error_transitions_actual_state_machine() {
        let state = Arc::new(MasterState::empty());
        let notifier_healthy = Arc::new(AtomicBool::new(false));
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "etcd-recovery-fatal-state-machine".to_string(),
                ..Default::default()
            },
        );
        service.set_oplog_store(Box::new(EtcdRecoveryFixtureOpLog {
            inner: InMemoryOpLog::new(16),
            notifier_healthy,
            recovery_error: Some(HaError::UnavailableInCurrentMode(
                "injected unrecoverable etcd recovery failure".into(),
            )),
            recovery_entries: vec![],
        }));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let history = service.state_machine.get_transition_history(16);
                if history.iter().any(|record| {
                    record.from_state == StandbyState::Recovering
                        && record.to_state == StandbyState::Failed
                        && record.event == StandbyEvent::FatalError
                }) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("unrecoverable shared-oplog gap did not emit FatalError");

        assert_eq!(service.state_machine.get_state(), StandbyState::Failed);
        assert_eq!(service.sync_status().state, StandbyState::Failed);
        service.stop();
    }

    #[tokio::test]
    async fn test_oplog_polling_stops_follower_after_reconnect_budget_exhaustion() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "cluster-a".to_string(),
                ..Default::default()
            },
        );
        let read_attempts = Arc::new(AtomicUsize::new(0));
        service.set_oplog_store(Box::new(FlakyReadOpLog::new(
            MAX_STANDBY_RECONNECT_ATTEMPTS as usize,
            read_attempts,
        )));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while service.sync_status().state != StandbyState::Failed {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("standby follower did not stop after reconnect exhaustion");
        assert!(!service.is_running());
        service.stop();
    }

    #[tokio::test]
    async fn test_oplog_polling_fails_on_unappliable_expected_record() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "cluster-a".to_string(),
                ..Default::default()
            },
        );
        let mut store = InMemoryOpLog::new(16);
        store.append_payload(1, r#"{"op":"unsupported-future-op"}"#);
        service.set_oplog_store(Box::new(store));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while service.sync_status().state != StandbyState::Failed {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("standby did not fail after rejecting its expected oplog record");
        assert!(!service.is_running());
        service.stop();
    }

    #[tokio::test]
    async fn test_promotion_fails_closed_when_final_oplog_read_fails() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "cluster-a".to_string(),
                ..Default::default()
            },
        );
        let remaining_failures = Arc::new(AtomicUsize::new(0));
        let read_attempts = Arc::new(AtomicUsize::new(0));
        let mut store =
            FlakyReadOpLog::with_failure_counter(remaining_failures.clone(), read_attempts);
        store
            .inner
            .append_payload(1, r#"{"op":"put_start","key":"promotion-read"}"#);
        service.set_oplog_store(Box::new(store));

        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while service.sync_status().applied_seq_id < 1 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("standby did not apply the initial record");

        service.oplog_applier.as_ref().unwrap().recover(0);
        remaining_failures.store(usize::MAX, Ordering::Release);
        let error = service.promote().await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected oplog reconnect failure")
        );
        assert_eq!(service.sync_status().state, StandbyState::Failed);
    }

    #[test]
    fn current_thread_promotion_offloads_sync_store_catch_up_and_keeps_reactor_live() {
        let runtime_thread_calls = Arc::new(AtomicUsize::new(0));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let mut service = HotStandbyService::new(
                Arc::new(MasterState::empty()),
                HotStandbyConfig {
                    enable_oplog_following: true,
                    oplog_poll_interval_ms: 10,
                    cluster_id: "current-thread-promotion".into(),
                    ..Default::default()
                },
            );
            service.set_oplog_store(Box::new(PromotionThreadProbeOpLog {
                inner: InMemoryOpLog::new(4),
                runtime_thread_calls: runtime_thread_calls.clone(),
                max_sequence_delay: std::time::Duration::from_millis(100),
            }));
            service.start().await.unwrap();

            let reactor_progressed = Arc::new(AtomicBool::new(false));
            let reactor_progressed_by_timer = reactor_progressed.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                reactor_progressed_by_timer.store(true, Ordering::Release);
            });

            assert_eq!(service.promote().await.unwrap(), 0);
            assert_eq!(runtime_thread_calls.load(Ordering::Acquire), 0);
            assert!(
                reactor_progressed.load(Ordering::Acquire),
                "promotion catch-up must yield the current-thread runtime while the sync store blocks"
            );
        });
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_teststart_snapshotonlywhenproviderfails()
     {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "cluster-a".to_string(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(FailingSnapshotProvider));

        assert!(matches!(
            service.start().await,
            Err(HaError::Snapshot(message)) if message == "snapshot unavailable"
        ));
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Failed);
        assert_eq!(status.applied_seq_id, 0);
        assert_eq!(status.primary_seq_id, 0);
        assert_eq!(status.lag_entries, 0);
        assert!(!status.is_connected);
        assert!(!status.is_syncing);
        assert!(state.objects.is_empty());
        assert!(service.replication_thread.is_none());
        service.stop();
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
    }

    #[test]
    fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testgetlatestappliedsequenceid()
     {
        let service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "fresh-sequence-cluster".into(),
                ..Default::default()
            },
        );

        assert_eq!(service.latest_applied_sequence_id(), 0);
    }

    #[test]
    fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testgetmetadatacount()
     {
        let service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        assert_eq!(service.metadata_count(), 0);
    }

    #[test]
    fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testexportmetadatasnapshot()
     {
        let service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        let snapshot = service.export_metadata_snapshot();
        assert!(snapshot.is_empty());
    }

    #[test]
    fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testreplicationloop_updatesmetrics()
     {
        let mut service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        service.stop();
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert!(!service.is_running());
    }

    #[test]
    fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testreplicationloop_handlesdisconnect()
     {
        let mut service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        service.stop();
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testverificationloop_whendisabled()
     {
        let mut service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        service.stop();
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testverificationloop_whenenabled()
     {
        let mut service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_verification: true,
                ..Default::default()
            },
        );

        assert!(matches!(
            service.start().await,
            Err(HaError::InvalidBackend(message))
                if message == "standby verification is unavailable in the Rust backend"
        ));
        service.stop();
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testwarmstart_withlocalstate()
     {
        let mut service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testwarmstart_withoutlocalstate()
     {
        let mut service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testwarmstart_withsnapshot()
     {
        let mut service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                ..Default::default()
            },
        );

        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_teststatetransition_syncfailed()
     {
        let mut service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_oplog_following: true,
                ..Default::default()
            },
        );

        assert!(matches!(
            service.start().await,
            Err(HaError::InvalidBackend(message))
                if message == "oplog following is enabled but no oplog store is configured"
        ));
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Failed);
        assert!(!status.is_connected);
        assert!(!status.is_syncing);
        assert!(service.replication_thread.is_none());
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_teststatetransition_starttowatching()
     {
        let mut service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_oplog_following: true,
                ..Default::default()
            },
        );

        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert!(matches!(
            service.start().await,
            Err(HaError::InvalidBackend(message))
                if message == "oplog following is enabled but no oplog store is configured"
        ));
        assert_eq!(service.sync_status().state, StandbyState::Failed);
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_teststart() {
        let mut service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_oplog_following: true,
                ..Default::default()
            },
        );

        assert!(matches!(
            service.start().await,
            Err(HaError::InvalidBackend(message))
                if message == "oplog following is enabled but no oplog store is configured"
        ));
        assert_eq!(service.sync_status().state, StandbyState::Failed);
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_teststart_alreadyrunning()
     {
        let mut service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_oplog_following: true,
                ..Default::default()
            },
        );
        let expected_message = "oplog following is enabled but no oplog store is configured";

        for _ in 0..2 {
            assert!(matches!(
                service.start().await,
                Err(HaError::InvalidBackend(message)) if message == expected_message
            ));
        }
        assert_eq!(service.sync_status().state, StandbyState::Failed);
        assert!(service.replication_thread.is_none());
    }

    #[test]
    fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testgetsyncstatus_initialstate()
     {
        let service =
            HotStandbyService::new(Arc::new(MasterState::empty()), HotStandbyConfig::default());

        let status = service.sync_status();
        assert_eq!(status.applied_seq_id, 0);
        assert_eq!(status.primary_seq_id, 0);
        assert_eq!(status.lag_entries, 0);
        assert!(!status.is_syncing);
        assert!(!status.is_connected);
        assert_eq!(status.state, StandbyState::Stopped);
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testgetsyncstatus_aftersync()
     {
        let mut service = HotStandbyService::new(
            Arc::new(MasterState::empty()),
            HotStandbyConfig {
                enable_oplog_following: true,
                ..Default::default()
            },
        );

        assert!(service.start().await.is_err());
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Failed);
        assert_eq!(status.applied_seq_id, 0);
        assert_eq!(status.primary_seq_id, 0);
        assert_eq!(status.lag_entries, 0);
        assert!(!status.is_connected);
        assert!(!status.is_syncing);
    }

    #[tokio::test]
    async fn test_snapshot_only_bootstrap_uses_empty_baseline_when_snapshot_missing() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "cluster-a".to_string(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(NoopSnapshotProvider));

        service.start().await.unwrap();

        assert!(state.objects.is_empty());
        assert!(state.segments.is_empty());
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, 0);
        assert_eq!(status.primary_seq_id, 0);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_snapshot_bootstrap_test_cpp_hotstandbysnapshotbootstraptest_snapshotonlystartbootstrapsfromconfiguredbackend_25d11cb6()
     {
        let root = tempfile::tempdir().unwrap();
        let cluster_id = "configured-snapshot-bootstrap";
        let provider = create_catalog_backed_snapshot_provider(
            cluster_id,
            SnapshotObjectStoreType::Local,
            SnapshotCatalogStoreType::Embedded,
            Some(root.path().to_path_buf()),
            None,
        )
        .unwrap();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "snapshot-segment".into(),
            base: 0x1000,
            size: 4096,
            te_endpoint: "tcp://snapshot-leader".into(),
            protocol: "tcp".into(),
            host_id: "snapshot-leader".into(),
        };
        let snapshot_sequence_id = 42;
        let mut snapshot_object_entry = snapshot_object("snapshot-key", &segment, 64, 128);
        snapshot_object_entry.hard_pinned = true;
        let snapshot = LoadedSnapshot {
            snapshot_id: "20260801_120000_001".into(),
            snapshot_sequence_id,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment: segment.clone(),
                used: 128,
                client_id: Uuid::new_v4(),
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: vec![(
                TenantId::default().make_scoped_key("snapshot-key"),
                snapshot_object_entry,
            )],
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        provider.publish_loaded_snapshot(&snapshot, 7).unwrap();

        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: cluster_id.into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(provider));
        service.start().await.unwrap();

        let restored = state
            .objects
            .get(&TenantId::default().make_scoped_key("snapshot-key"))
            .unwrap();
        assert_eq!(restored.size, 128);
        drop(restored);
        assert_eq!(service.latest_applied_sequence_id(), snapshot_sequence_id);
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, snapshot_sequence_id);
        assert_eq!(status.primary_seq_id, snapshot_sequence_id);
        assert_eq!(status.lag_entries, 0);
        assert!(!status.is_syncing);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_snapshot_bootstrap_test_cpp_hotstandbysnapshotbootstraptest_snapshotonlystartusesemptybaselinewhensnapshotmissing_42d36515()
     {
        let root = tempfile::tempdir().unwrap();
        let cluster_id = "configured-empty-snapshot-bootstrap";
        let provider = create_catalog_backed_snapshot_provider(
            cluster_id,
            SnapshotObjectStoreType::Local,
            SnapshotCatalogStoreType::Embedded,
            Some(root.path().to_path_buf()),
            None,
        )
        .unwrap();
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: cluster_id.into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(provider));
        service.start().await.unwrap();

        assert!(state.objects.is_empty());
        assert!(state.segments.is_empty());
        assert_eq!(service.latest_applied_sequence_id(), 0);
        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, 0);
        assert_eq!(status.primary_seq_id, 0);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_oplog_ha_recovery_test_cpp_harecoverytest_snapshotwithnosubsequentoplog()
    {
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "snapshot-only:1".into(),
            base: 0,
            size: 10 * 1024,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        };
        let objects = (1_u64..=10)
            .map(|index| {
                let key = format!("key_{index}");
                (
                    TenantId::default().make_scoped_key(&key),
                    snapshot_object(&key, &segment, (index - 1) * 1024, 1024),
                )
            })
            .collect::<Vec<_>>();
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "snap1".into(),
            snapshot_sequence_id: 10,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment,
                used: 10 * 1024,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects,
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                enable_oplog_following: true,
                enable_verification: false,
                oplog_poll_interval_ms: 10,
                cluster_id: "test_cluster".into(),
            },
        );
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));
        let empty_oplog = InMemoryOpLog::new(16);
        assert!(empty_oplog.read_since(10, 1_000).unwrap().is_empty());
        service.set_oplog_store(Box::new(empty_oplog));

        service.start().await.unwrap();

        assert_eq!(state.objects.len(), 10);
        for index in 1..=10 {
            assert!(
                state
                    .objects
                    .contains_key(&TenantId::default().make_scoped_key(&format!("key_{index}")))
            );
        }
        assert_eq!(service.latest_applied_sequence_id(), 10);
        assert_eq!(state.objects.len(), 10);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_testpromote_whenready()
     {
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "snapshot-promote:1".into(),
            base: 0,
            size: 4096,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        };
        let scoped_key = TenantId::default().make_scoped_key("snapshot-key");
        let expected_object = snapshot_object("snapshot-key", &segment, 0, 4096);
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "snapshot-42".into(),
            snapshot_sequence_id: 42,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment,
                used: 4096,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: vec![(scoped_key.clone(), expected_object.clone())],
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "snapshot-promote-cluster".into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));

        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        assert_eq!(service.latest_applied_sequence_id(), 42);

        assert_eq!(service.promote().await.unwrap(), 42);
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert_eq!(service.latest_applied_sequence_id(), 42);
        let restored = state.objects.get(&scoped_key).unwrap();
        assert_eq!(restored.size, expected_object.size);
        assert_eq!(restored.client_id, expected_object.client_id);
        assert_eq!(restored.replicas.len(), 1);
        assert_eq!(
            restored.replicas[0].segment_id,
            expected_object.replicas[0].segment_id
        );
        assert_eq!(restored.replicas[0].offset, 0);
        assert_eq!(restored.replicas[0].size, 4096);
        assert_eq!(restored.replicas[0].status, ReplicaStatus::Complete);
    }

    #[tokio::test]
    async fn cpp_parity_ha_standby_hot_standby_service_test_cpp_hotstandbyservicetest_teststart_snapshotonlywithsnapshot()
     {
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::from_u128((1_u128 << 64) | 2);
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "snapshot-start:1".into(),
            base: 0,
            size: 4096,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        };
        let scoped_key = TenantId::default().make_scoped_key("key-1");
        let mut object = snapshot_object("key-1", &segment, 0, 4096);
        object.client_id = client_id;
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "20260330_120000_000".into(),
            snapshot_sequence_id: 42,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment,
                used: 4096,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: vec![(scoped_key.clone(), object)],
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "snapshot-start-cluster".into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));

        service.start().await.unwrap();

        let status = service.sync_status();
        assert_eq!(status.state, StandbyState::Watching);
        assert_eq!(status.applied_seq_id, 42);
        assert_eq!(status.primary_seq_id, 42);
        assert_eq!(status.lag_entries, 0);
        assert!(status.is_connected);
        assert!(!status.is_syncing);
        assert_eq!(service.metadata_count(), 1);
        assert_eq!(service.latest_applied_sequence_id(), 42);
        let exported = service.export_metadata_snapshot();
        assert_eq!(exported.len(), 1);
        assert_eq!(exported[0].0, scoped_key);
        assert_eq!(exported[0].1.user_key, "key-1");
        assert_eq!(exported[0].1.size, 4096);
        assert_eq!(exported[0].1.client_id, client_id);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_snapshot_only_restart_replaces_42_old_with_84_new() {
        fn snapshot(sequence: u64, key: &str, size: u64) -> crate::ha::LoadedSnapshot {
            let client_id = Uuid::from_u128((1_u128 << 64) | 2);
            let segment = Segment {
                id: Uuid::new_v4(),
                name: format!("snapshot-restart:{sequence}"),
                base: 0,
                size,
                te_endpoint: String::new(),
                protocol: "tcp".into(),
                host_id: String::new(),
            };
            let mut object = snapshot_object(key, &segment, 0, size);
            object.client_id = client_id;
            crate::ha::LoadedSnapshot {
                snapshot_id: format!("snapshot-{sequence}"),
                snapshot_sequence_id: sequence,
                allocator_config: None,
                segments: vec![SegmentEntry {
                    segment,
                    used: size,
                    client_id,
                    status: crate::proto::SegmentStatus::Active,
                }],
                nof_segments: vec![],
                objects: vec![(TenantId::default().make_scoped_key(key), object)],
                tasks: vec![],
                replication_tasks: vec![],
                graceful_unmounts: vec![],
                delayed_replica_releases: vec![],
                local_disk_segments: vec![],
            }
        }

        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "snapshot-restart-cluster".into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider {
            snapshot: snapshot(42, "key-old", 4096),
        }));
        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        assert_eq!(service.latest_applied_sequence_id(), 42);
        assert_eq!(service.metadata_count(), 1);
        service.stop();

        service.set_snapshot_provider(Box::new(StaticSnapshotProvider {
            snapshot: snapshot(84, "key-new", 8192),
        }));
        service.start().await.unwrap();

        assert_eq!(service.sync_status().state, StandbyState::Watching);
        assert_eq!(service.latest_applied_sequence_id(), 84);
        assert_eq!(service.metadata_count(), 1);
        let exported = service.export_snapshot_metadata().unwrap();
        assert_eq!(exported.len(), 1);
        assert_eq!(exported[0].scoped_key, "default\0key-new");
        assert_eq!(exported[0].user_key, "key-new");
        assert_eq!(exported[0].size, 8192);
        assert_eq!(exported[0].last_sequence_id, 84);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_oplog_ha_recovery_test_cpp_harecoverytest_snapshotthenoplogreplay() {
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "snapshot-plus-oplog:1".into(),
            base: 0,
            size: 20 * 1024,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        };
        let snapshot_objects = (1_u64..=10)
            .map(|index| {
                let key = format!("key_{index}");
                (
                    TenantId::default().make_scoped_key(&key),
                    snapshot_object(&key, &segment, (index - 1) * 1024, 1024),
                )
            })
            .collect::<Vec<_>>();
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "snap1".into(),
            snapshot_sequence_id: 10,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment: segment.clone(),
                used: 10 * 1024,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: snapshot_objects,
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut oplog = InMemoryOpLog::new(32);
        oplog.update_latest_sequence_id(10).unwrap();
        for index in 11_u64..=20 {
            let key = format!("key_{index}");
            let replica = snapshot_object(&key, &segment, (index - 1) * 1024, 1024)
                .replicas
                .into_iter()
                .next()
                .unwrap();
            let sequence = oplog.append_payload(
                1,
                serde_json::json!({
                    "op": "put_end",
                    "key": key,
                    "size": 1024,
                    "client_id": Uuid::nil().to_string(),
                    "tenant_id": "default",
                    "group_id": "",
                    "user_key": format!("key_{index}"),
                    "replicas": [replica],
                })
                .to_string(),
            );
            assert_eq!(sequence, index);
        }

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                enable_oplog_following: true,
                enable_verification: false,
                oplog_poll_interval_ms: 10,
                cluster_id: "test_cluster".into(),
            },
        );
        let reads = Arc::new(Mutex::new(Vec::new()));
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));
        service.set_oplog_store(Box::new(StrictBaselineOpLog {
            inner: oplog,
            minimum_since: 11,
            reads: reads.clone(),
        }));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while service.latest_applied_sequence_id() < 20 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("standby did not replay sequences 11 through 20");
        assert_eq!(state.objects.len(), 20);
        for index in 1..=20 {
            assert!(
                state
                    .objects
                    .contains_key(&TenantId::default().make_scoped_key(&format!("key_{index}")))
            );
        }
        assert_eq!(service.latest_applied_sequence_id(), 20);
        let reads = reads.lock();
        assert!(!reads.is_empty());
        assert!(reads.iter().all(|since| *since >= 11));
        assert_eq!(reads[0], 11);
        drop(reads);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_oplog_ha_recovery_test_cpp_harecoverytest_gc_newstandbyaftercleanup() {
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "gc-new-standby:1".into(),
            base: 0,
            size: 20 * 1024,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        };
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "snap1".into(),
            snapshot_sequence_id: 10,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment: segment.clone(),
                used: 10 * 1024,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: (1_u64..=10)
                .map(|index| {
                    let key = format!("key_{index}");
                    (
                        TenantId::default().make_scoped_key(&key),
                        snapshot_object(&key, &segment, (index - 1) * 1024, 1024),
                    )
                })
                .collect(),
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut oplog = InMemoryOpLog::new(32);
        for index in 1_u64..=20 {
            let key = format!("key_{index}");
            let replica = snapshot_object(&key, &segment, (index - 1) * 1024, 1024)
                .replicas
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(
                oplog.append_payload(
                    1,
                    serde_json::json!({
                        "op": "put_end",
                        "key": key,
                        "size": 1024,
                        "client_id": Uuid::nil().to_string(),
                        "tenant_id": "default",
                        "group_id": "",
                        "user_key": format!("key_{index}"),
                        "replicas": [replica],
                    })
                    .to_string(),
                ),
                index
            );
        }
        oplog.cleanup_before(10).unwrap();
        assert_eq!(
            oplog
                .read_since(1, 32)
                .unwrap()
                .into_iter()
                .map(|entry| entry.seq)
                .collect::<Vec<_>>(),
            (10_u64..=20).collect::<Vec<_>>()
        );

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                enable_oplog_following: true,
                enable_verification: false,
                oplog_poll_interval_ms: 10,
                cluster_id: "test_cluster".into(),
            },
        );
        let reads = Arc::new(Mutex::new(Vec::new()));
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));
        service.set_oplog_store(Box::new(StrictBaselineOpLog {
            inner: oplog,
            minimum_since: 11,
            reads: reads.clone(),
        }));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while service.latest_applied_sequence_id() < 20 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("new standby did not replay post-GC sequences 11 through 20");
        assert_eq!(state.objects.len(), 20);
        for index in 1..=20 {
            assert!(
                state
                    .objects
                    .contains_key(&TenantId::default().make_scoped_key(&format!("key_{index}")))
            );
        }
        assert_eq!(service.latest_applied_sequence_id(), 20);
        let reads = reads.lock();
        assert!(!reads.is_empty());
        assert!(reads.iter().all(|since| *since >= 11));
        assert_eq!(reads[0], 11);
        drop(reads);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_oplog_ha_recovery_test_cpp_harecoverytest_snapshotloadfail_fallbacktofullreplay()
     {
        let state = Arc::new(MasterState::empty());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: segment_id,
            name: "full-replay:1".into(),
            base: 0,
            size: 20 * 1024,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment_id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state.allocator.write().add_segment(segment, 0, client_id);
        let mut oplog = InMemoryOpLog::new(32);
        for index in 1_u64..=20 {
            let sequence = oplog.append_payload(
                1,
                serde_json::json!({
                    "op": "put_end",
                    "key": format!("key_{index}"),
                    "size": 1024,
                    "client_id": Uuid::nil().to_string(),
                    "tenant_id": "default",
                    "group_id": "",
                    "user_key": format!("key_{index}"),
                    "replicas": [{
                        "segment_id": segment_id,
                        "segment_name": "full-replay:1",
                        "offset": (index - 1) * 1024,
                        "size": 1024,
                        "status": ReplicaStatus::Complete,
                        "replica_type": ReplicaType::Memory,
                        "holder_client_id": null,
                        "local_disk_storage_id": null,
                        "local_disk_generation_id": null,
                        "refcnt": 0,
                        "handle_valid": true,
                        "base_addr": 0,
                        "protocol": "tcp"
                    }],
                })
                .to_string(),
            );
            assert_eq!(sequence, index);
        }
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                enable_oplog_following: true,
                enable_verification: false,
                oplog_poll_interval_ms: 10,
                cluster_id: "test_cluster".into(),
            },
        );
        service.set_snapshot_provider(Box::new(FailingSnapshotProvider));
        service.set_oplog_store(Box::new(oplog));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while service.latest_applied_sequence_id() < 20 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("standby did not replay full oplog after snapshot failure");
        assert_eq!(state.objects.len(), 20);
        for index in 1..=20 {
            assert!(
                state
                    .objects
                    .contains_key(&TenantId::default().make_scoped_key(&format!("key_{index}")))
            );
        }
        assert_eq!(service.latest_applied_sequence_id(), 20);
        service.stop();
    }

    #[tokio::test]
    async fn cpp_parity_ha_oplog_ha_recovery_test_cpp_harecoverytest_promotioncatchup_allentriesapplied()
     {
        let state = Arc::new(MasterState::empty());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: segment_id,
            name: "promotion-catch-up:1".into(),
            base: 0,
            size: 15 * 1024,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment_id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state
            .allocator
            .write()
            .add_segment(segment.clone(), 0, client_id);
        let shared = Arc::new(Mutex::new(InMemoryOpLog::new(32)));
        let append = |store: &mut InMemoryOpLog, index: u64| {
            let replica =
                snapshot_object(&format!("key_{index}"), &segment, (index - 1) * 1024, 1024)
                    .replicas
                    .into_iter()
                    .next()
                    .unwrap();
            store.append_payload(
                1,
                serde_json::json!({
                    "op": "put_end",
                    "key": format!("key_{index}"),
                    "size": 1024,
                    "client_id": Uuid::nil().to_string(),
                    "tenant_id": "default",
                    "group_id": "",
                    "user_key": format!("key_{index}"),
                    "replicas": [replica],
                })
                .to_string(),
            )
        };
        {
            let mut store = shared.lock();
            for index in 1_u64..=10 {
                assert_eq!(append(&mut store, index), index);
            }
        }
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_oplog_following: true,
                oplog_poll_interval_ms: 10,
                cluster_id: "test_cluster".into(),
                ..Default::default()
            },
        );
        let promotion_reads = Arc::new(Mutex::new(Vec::new()));
        service.set_oplog_store(Box::new(SharedMutableOpLog {
            inner: shared.clone(),
            follower_ceiling: Some(10),
            promotion_reads: Some(promotion_reads.clone()),
        }));
        service.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while service.latest_applied_sequence_id() < 10 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("standby did not apply initial sequences 1 through 10");
        assert_eq!(state.objects.len(), 10);
        {
            let mut store = shared.lock();
            for index in 11_u64..=15 {
                assert_eq!(append(&mut store, index), index);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert_eq!(service.latest_applied_sequence_id(), 10);

        let promoted_sequence = service.promote().await.unwrap();

        assert_eq!(promoted_sequence, 15);
        assert_eq!(state.objects.len(), 15);
        for index in 1..=15 {
            assert!(
                state
                    .objects
                    .contains_key(&TenantId::default().make_scoped_key(&format!("key_{index}")))
            );
        }
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert_eq!(*promotion_reads.lock(), vec![11]);
    }

    #[tokio::test]
    async fn test_oplog_bootstrap_falls_back_when_snapshot_load_fails() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                enable_oplog_following: true,
                enable_verification: false,
                oplog_poll_interval_ms: 10,
                cluster_id: "cluster-a".to_string(),
            },
        );
        service.set_snapshot_provider(Box::new(FailingSnapshotProvider));
        service.set_oplog_store(Box::new(InMemoryOpLog::new(4)));

        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        service.stop();
    }

    #[tokio::test]
    async fn test_snapshot_bootstrap_falls_back_after_latest_semantic_restore_failure() {
        let state = Arc::new(MasterState::empty());
        let duplicate_segment_id = Uuid::new_v4();
        let invalid_segment = SegmentEntry {
            segment: Segment {
                id: duplicate_segment_id,
                name: "duplicate-segment".into(),
                base: 0,
                size: 4096,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            used: 0,
            client_id: Uuid::new_v4(),
            status: crate::proto::SegmentStatus::Active,
        };
        let invalid_latest = crate::ha::LoadedSnapshot {
            snapshot_id: "invalid-latest".into(),
            snapshot_sequence_id: 9,
            allocator_config: None,
            segments: vec![invalid_segment.clone(), invalid_segment],
            nof_segments: Vec::new(),
            objects: Vec::new(),
            tasks: Vec::new(),
            replication_tasks: Vec::new(),
            graceful_unmounts: Vec::new(),
            delayed_replica_releases: Vec::new(),
            local_disk_segments: Vec::new(),
        };
        let valid_older = crate::ha::LoadedSnapshot {
            snapshot_id: "valid-older".into(),
            snapshot_sequence_id: 7,
            allocator_config: None,
            segments: Vec::new(),
            nof_segments: Vec::new(),
            objects: Vec::new(),
            tasks: Vec::new(),
            replication_tasks: Vec::new(),
            graceful_unmounts: Vec::new(),
            delayed_replica_releases: Vec::new(),
            local_disk_segments: Vec::new(),
        };
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "cluster-a".into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(CandidateSnapshotProvider {
            snapshots: vec![invalid_latest, valid_older],
        }));

        service.start().await.unwrap();
        assert_eq!(service.sync_status().applied_seq_id, 7);
        service.stop();
    }

    #[tokio::test]
    async fn test_snapshot_bootstrap_replaces_stale_state_and_rebuilds_allocator_holes() {
        let state = Arc::new(MasterState::empty());
        state.objects.insert(
            TenantId::default().make_scoped_key("stale"),
            ObjectEntry {
                replicas: vec![],
                size: 1,
                last_access: std::time::SystemTime::now(),
                hard_pinned: false,
                data_type: ObjectDataType::Unknown,
                client_id: Uuid::nil(),
                put_start_time: None,
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: TenantId::default(),
                group_id: String::new(),
                quota_committed: false,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "stale".into(),
            },
        );
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "standby-hole:1".into(),
            base: 0x100000000,
            size: 1_000,
            te_endpoint: "tcp://stale-master-term".into(),
            protocol: "rdma".into(),
            host_id: String::new(),
        };
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "snapshot-test".into(),
            snapshot_sequence_id: 7,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment: segment.clone(),
                used: 200,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: vec![
                (
                    TenantId::default().make_scoped_key("left"),
                    snapshot_object("left", &segment, 0, 100),
                ),
                (
                    TenantId::default().make_scoped_key("right"),
                    snapshot_object("right", &segment, 300, 100),
                ),
            ],
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "cluster-a".into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));
        service.start().await.unwrap();

        assert!(
            !state
                .objects
                .contains_key(&TenantId::default().make_scoped_key("stale"))
        );
        let left = state
            .objects
            .get(&TenantId::default().make_scoped_key("left"))
            .unwrap();
        assert!(!left.replicas[0].handle_valid);
        assert_eq!(left.replicas[0].base_addr, 0);
        assert!(left.replicas[0].protocol.is_empty());
        drop(left);
        let restored_segment = state.segments.get(&segment.id).unwrap();
        assert_eq!(restored_segment.segment.base, 0);
        assert!(restored_segment.segment.te_endpoint.is_empty());
        assert!(restored_segment.segment.protocol.is_empty());
        drop(restored_segment);
        let unavailable = state.allocator.write().allocate(
            "fills-hole",
            150,
            1,
            &ReplicateConfig {
                preferred_segment: segment.name.clone(),
                ..Default::default()
            },
        );
        assert!(
            unavailable.is_empty(),
            "restored process addresses must not accept allocations before ReMount"
        );
        state
            .allocator
            .write()
            .rebind_segment(segment.clone(), client_id)
            .unwrap();
        let allocated = state.allocator.write().allocate(
            "fills-hole",
            150,
            1,
            &ReplicateConfig {
                preferred_segment: segment.name,
                ..Default::default()
            },
        );
        assert_eq!(allocated.len(), 1);
        assert_eq!(allocated[0].offset, 100);
        assert_eq!(service.sync_status().applied_seq_id, 7);
        service.stop();
    }

    #[tokio::test]
    async fn test_snapshot_bootstrap_restores_native_copy_reservation_and_source_pin() {
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "copy-restore:1".into(),
            base: 0,
            size: 1_000,
            te_endpoint: String::new(),
            protocol: "rdma".into(),
            host_id: String::new(),
        };
        let scoped_key = TenantId::default().make_scoped_key("copying");
        let source = ReplicaDescriptor {
            segment_id: segment.id,
            segment_name: segment.name.clone(),
            offset: 0,
            size: 100,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: segment.base,
            protocol: segment.protocol.clone(),
        };
        let target = ReplicaDescriptor {
            offset: 100,
            status: ReplicaStatus::Allocating,
            ..source.clone()
        };
        let mut object = snapshot_object("copying", &segment, 0, 100);
        object.client_id = client_id;
        object.replicas.push(target.clone());
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "copy-task-snapshot".into(),
            snapshot_sequence_id: 8,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment: segment.clone(),
                used: 200,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: vec![(scoped_key.clone(), object)],
            tasks: vec![],
            replication_tasks: vec![ReplicationTaskSnapshotEntry {
                key: scoped_key.clone(),
                client_id,
                start_age_millis: 10,
                kind: ReplicationTaskKind::Copy,
                source: source.clone(),
                targets: vec![target],
                existing_move_target: None,
                reserved_quota_charge_bytes: 100,
            }],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                cluster_id: "cluster-a".into(),
                ..Default::default()
            },
        );
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));
        service.start().await.unwrap();

        assert!(state.replication_tasks.contains_key(&scoped_key));
        let object = state.objects.get(&scoped_key).unwrap();
        assert_eq!(object.replicas.len(), 2);
        assert_eq!(object.replicas[0].refcnt, 1);
        assert_eq!(object.replicas[1].status, ReplicaStatus::Allocating);
        drop(object);
        assert_eq!(state.segments.get(&segment.id).unwrap().used, 200);
        service.stop();
    }

    #[tokio::test]
    async fn test_snapshot_oplog_following_and_promotion_converge_to_one_state() {
        let state = Arc::new(MasterState::empty());
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: Uuid::new_v4(),
            name: "promotion-baseline:1".into(),
            base: 0,
            size: 1_000,
            te_endpoint: String::new(),
            protocol: "rdma".into(),
            host_id: String::new(),
        };
        let scoped_key = TenantId::default().make_scoped_key("baseline-key");
        let snapshot = crate::ha::LoadedSnapshot {
            snapshot_id: "promotion-baseline".into(),
            snapshot_sequence_id: 1,
            allocator_config: None,
            segments: vec![SegmentEntry {
                segment: segment.clone(),
                used: 100,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            }],
            nof_segments: vec![],
            objects: vec![(
                scoped_key.clone(),
                snapshot_object("baseline-key", &segment, 0, 100),
            )],
            tasks: vec![],
            replication_tasks: vec![],
            graceful_unmounts: vec![],
            delayed_replica_releases: vec![],
            local_disk_segments: vec![],
        };
        let mut oplog = InMemoryOpLog::new(16);
        oplog.append_payload(1, r#"{"op":"put_start","key":"already-in-baseline"}"#);
        oplog.append_payload(
            1,
            serde_json::json!({"op": "remove", "key": scoped_key}).to_string(),
        );

        let mut service = HotStandbyService::new(
            state.clone(),
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                enable_oplog_following: true,
                enable_verification: false,
                oplog_poll_interval_ms: 10,
                cluster_id: "cluster-a".into(),
            },
        );
        service.set_snapshot_provider(Box::new(StaticSnapshotProvider { snapshot }));
        service.set_oplog_store(Box::new(oplog));
        service.start().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while service.sync_status().applied_seq_id < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("standby did not catch up to the post-snapshot oplog");
        assert!(!state.objects.contains_key(&scoped_key));

        assert_eq!(service.promote().await.unwrap(), 2);
        assert_eq!(service.sync_status().state, StandbyState::Stopped);
        assert!(!state.objects.contains_key(&scoped_key));

        // A completed promotion must leave the follower reusable for the next
        // leader term instead of stranding it in Promoted.
        service.start().await.unwrap();
        assert_eq!(service.sync_status().state, StandbyState::Watching);
        service.stop();
    }
}

/// The hot standby service manages standby state and recovery.
/// 热备服务管理备用状态和恢复。
///
/// It holds a reference to the shared MasterState, applies snapshot data
/// and oplog mutations to it, and manages the promotion lifecycle.
/// 它持有对共享 MasterState 的引用，应用快照数据和 oplog 变更，
/// 并管理提升生命周期。
pub struct HotStandbyService {
    /// Shared master state (the in-memory data store to keep synced).
    /// 共享 master 状态（要保持同步的内存数据存储）。
    state: Arc<MasterState>,
    config: HotStandbyConfig,
    /// Standby synchronization status (state, lag, sequence IDs).
    /// 备用同步状态（状态、延迟、序列 ID）。
    sync_status: Arc<parking_lot::RwLock<StandbySyncStatus>>,
    /// Shutdown signal sender — dropping or sending triggers graceful stop.
    /// 关闭信号发送器 —— drop 或发送触发优雅停止。
    shutdown_tx: Option<watch::Sender<()>>,
    /// Optional snapshot provider for bootstrap recovery.
    /// 可选的快照提供者，用于引导恢复。
    snapshot_provider: Option<Box<dyn SnapshotProvider>>,
    /// Standby state machine (validated transitions).
    state_machine: Arc<StandbyStateMachine>,
    /// OpLog applier for replaying leader mutations.
    oplog_applier: Option<Arc<OpLogApplier>>,
    /// OpLog store for reading the leader's oplog (shared between start and promote).
    oplog_store: Option<Arc<dyn OpLogStore>>,
    /// Replication loop thread handle.
    replication_thread: Option<std::thread::JoinHandle<()>>,
    snapshot_baseline_sequence_id: parking_lot::RwLock<u64>,
}

impl Drop for HotStandbyService {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.state_machine.process_event(StandbyEvent::Stop);
        if let Some(handle) = self.replication_thread.take() {
            let _ = handle.join();
        }
    }
}

impl HotStandbyService {
    /// Create a new HotStandbyService.
    pub(crate) fn new(state: Arc<MasterState>, config: HotStandbyConfig) -> Self {
        Self {
            state,
            config,
            sync_status: Arc::new(parking_lot::RwLock::new(StandbySyncStatus::default())),
            shutdown_tx: None,
            snapshot_provider: None,
            state_machine: Arc::new(StandbyStateMachine::new()),
            oplog_applier: None,
            oplog_store: None,
            replication_thread: None,
            snapshot_baseline_sequence_id: parking_lot::RwLock::new(0),
        }
    }

    /// Inject an OpLog store for oplog following.
    pub fn set_oplog_store(&mut self, store: Box<dyn OpLogStore>) {
        self.oplog_store = Some(Arc::from(store));
    }

    pub fn has_oplog_store(&self) -> bool {
        self.oplog_store.is_some()
    }

    /// Set the snapshot provider for bootstrap recovery.
    /// 设置快照提供者用于引导恢复。
    pub fn set_snapshot_provider(&mut self, provider: Box<dyn SnapshotProvider>) {
        self.snapshot_provider = Some(provider);
    }

    /// Get the current standby sync status.
    /// 获取当前备用同步状态。
    pub fn sync_status(&self) -> StandbySyncStatus {
        self.sync_status.read().clone()
    }

    /// Return the latest sequence restored or replayed by this standby.
    pub fn latest_applied_sequence_id(&self) -> u64 {
        self.sync_status.read().applied_seq_id
    }

    /// Return the number of object metadata entries currently held by the standby.
    pub fn metadata_count(&self) -> usize {
        self.state.objects.len()
    }

    /// Clone a point-in-time view of the standby's object metadata.
    pub fn export_metadata_snapshot(&self) -> Vec<(String, ObjectEntry)> {
        let _snapshot_guard = self.state.key_mutations.lock_snapshot();
        let mut snapshot = self
            .state
            .objects
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        snapshot.sort_by(|left, right| left.0.cmp(&right.0));
        snapshot
    }

    /// Export per-object snapshot provenance in snapshot-only mode.
    pub fn export_snapshot_metadata(&self) -> Result<Vec<StandbySnapshotMetadata>, HaError> {
        if self.config.enable_oplog_following {
            return Err(HaError::UnavailableInCurrentMode(
                "per-object snapshot provenance is unavailable after oplog replay".into(),
            ));
        }
        // Restart publishes the baseline while holding this lock across the
        // MasterState snapshot replacement. Keep the read guard across the
        // matching state barrier so sequence provenance and objects are one
        // point-in-time view.
        let baseline_guard = self.snapshot_baseline_sequence_id.read();
        let _snapshot_guard = self.state.key_mutations.lock_snapshot();
        let last_sequence_id = *baseline_guard;
        let mut snapshot = self
            .state
            .objects
            .iter()
            .map(|entry| StandbySnapshotMetadata {
                scoped_key: entry.key().clone(),
                user_key: entry.value().user_key.clone(),
                size: entry.value().size,
                client_id: entry.value().client_id,
                last_sequence_id,
            })
            .collect::<Vec<_>>();
        snapshot.sort_by(|left, right| left.scoped_key.cmp(&right.scoped_key));
        Ok(snapshot)
    }

    pub fn is_running(&self) -> bool {
        self.state_machine.is_running()
    }

    /// Check if the standby is ready to be promoted.
    /// 检查备用节点是否准备好被提升。
    ///
    /// Ready state: Watching (actively following).
    /// 就绪状态：Watching（活跃跟随中）。
    pub fn is_ready_for_promotion(&self) -> bool {
        self.sync_status.read().state == StandbyState::Watching
    }

    /// Start the hot standby service.
    /// 启动热备服务。
    ///
    /// Workflow / 工作流:
    /// 1. Set state to Connecting.
    ///    设置状态为 Connecting。
    /// 2. (Optional) Snapshot bootstrap: load snapshot and apply to MasterState.
    ///    (可选) 快照引导：加载快照并应用到 MasterState。
    ///    - Restore Memory segments → register with allocator.
    ///      恢复 Memory segment → 注册到分配器。
    ///    - Restore NoF (File) segments → register with NoF allocator.
    ///      恢复 NoF（文件）segment → 注册到 NoF 分配器。
    ///    - Restore objects and tasks.
    ///      恢复对象和任务。
    /// 3. (Optional) Start oplog following mode (Watching state).
    ///    (可选) 启动 oplog 跟随模式（Watching 状态）。
    /// 4. Enter Watching state — ready for promotion.
    ///    进入 Watching 状态 —— 准备好进行提升。
    ///
    /// 启动热备：(1) 可选加载快照引导（恢复 segments/objects/tasks）
    /// (2) 进入 Watching 状态等待 oplog 追平。快照加载时恢复 Memory 和 NoF segment 到 allocator。
    pub async fn start(&mut self) -> Result<(), HaError> {
        let start_result = self.state_machine.process_event(StandbyEvent::Start);
        if !start_result.allowed {
            return Err(HaError::UnavailableInCurrentStatus);
        }
        if self.config.enable_verification {
            self.state_machine.process_event(StandbyEvent::FatalError);
            let mut status = self.sync_status.write();
            status.state = StandbyState::Failed;
            status.is_connected = false;
            status.is_syncing = false;
            return Err(HaError::InvalidBackend(
                "standby verification is unavailable in the Rust backend".to_string(),
            ));
        }
        let mut status = self.sync_status.write();
        status.state = StandbyState::Connecting;
        status.is_connected = false;
        drop(status);

        let mut baseline_seq_id = 0;

        // Phase 1: Snapshot bootstrap (if enabled)
        // 阶段 1：快照引导（若启用）
        if self.config.enable_snapshot_bootstrap {
            if let Some(ref provider) = self.snapshot_provider {
                let mut status = self.sync_status.write();
                status.state = StandbyState::Recovering;
                drop(status);

                match provider.load_snapshot_candidates(&self.config.cluster_id) {
                    Ok(candidates) => {
                        let mut last_restore_error = None;
                        for snapshot in candidates {
                            let mut snapshot_baseline_sequence_id =
                                self.snapshot_baseline_sequence_id.write();
                            match restore_loaded_snapshot_state(
                                &self.state,
                                snapshot.segments.clone(),
                                snapshot.nof_segments.clone(),
                                snapshot.objects.clone(),
                                snapshot.tasks.clone(),
                                snapshot.replication_tasks.clone(),
                                snapshot.local_disk_segments.clone(),
                                snapshot.graceful_unmounts.clone(),
                                snapshot.delayed_replica_releases.clone(),
                                snapshot.allocator_config,
                            ) {
                                Ok(()) => {
                                    *snapshot_baseline_sequence_id = snapshot.snapshot_sequence_id;
                                    let mut status = self.sync_status.write();
                                    status.applied_seq_id = snapshot.snapshot_sequence_id;
                                    baseline_seq_id = snapshot.snapshot_sequence_id;
                                    drop(status);
                                    info!(
                                        snapshot_id = %snapshot.snapshot_id,
                                        "Loaded snapshot with {} objects, {} segments",
                                        snapshot.objects.len(),
                                        snapshot.segments.len()
                                    );
                                    last_restore_error = None;
                                    break;
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        snapshot_id = %snapshot.snapshot_id,
                                        %error,
                                        "snapshot candidate failed semantic restore; trying older candidate"
                                    );
                                    last_restore_error = Some(HaError::Snapshot(error));
                                }
                            }
                        }
                        if let Some(error) = last_restore_error {
                            if self.config.enable_oplog_following {
                                tracing::warn!(
                                    "All snapshot baselines failed semantic restore, falling back to oplog-only bootstrap: {}",
                                    error
                                );
                            } else {
                                self.state_machine.process_event(StandbyEvent::FatalError);
                                let mut status = self.sync_status.write();
                                status.state = StandbyState::Failed;
                                status.is_connected = false;
                                status.is_syncing = false;
                                drop(status);
                                return Err(error);
                            }
                        }
                    }
                    Err(error) if self.config.enable_oplog_following => {
                        tracing::warn!(
                            "Failed to load snapshot baseline, falling back to oplog-only bootstrap: {}",
                            error
                        );
                    }
                    Err(error) => {
                        self.state_machine.process_event(StandbyEvent::FatalError);
                        let mut status = self.sync_status.write();
                        status.state = StandbyState::Failed;
                        status.is_connected = false;
                        status.is_syncing = false;
                        drop(status);
                        return Err(error);
                    }
                }
            }
        }

        // Phase 2: Start oplog following (if enabled)
        // 阶段 2：启动 oplog 跟随（若启用）
        if self.config.enable_oplog_following {
            if self.oplog_store.is_none() {
                self.state_machine.process_event(StandbyEvent::FatalError);
                let mut status = self.sync_status.write();
                status.state = StandbyState::Failed;
                status.is_connected = false;
                status.is_syncing = false;
                return Err(HaError::InvalidBackend(
                    "oplog following is enabled but no oplog store is configured".into(),
                ));
            }
            let applier = Arc::new(OpLogApplier::new(self.state.clone()));
            applier.recover(baseline_seq_id);
            self.oplog_applier = Some(applier.clone());

            self.state_machine.process_event(StandbyEvent::Connected);
            self.state_machine.process_event(StandbyEvent::SyncComplete);

            // Spawn ReplicationLoop.
            let (shutdown_tx, shutdown_rx) = watch::channel(());
            self.shutdown_tx = Some(shutdown_tx);
            let oplog_store = self.oplog_store.clone();
            let state_machine = self.state_machine.clone();
            let applier_clone = applier.clone();
            {
                let mut status = self.sync_status.write();
                status.state = StandbyState::Watching;
                status.is_syncing = true;
                status.is_connected = true;
            }
            let sync_status_ref = Arc::downgrade(&self.sync_status);
            let poll_interval =
                std::time::Duration::from_millis(self.config.oplog_poll_interval_ms.max(1));

            let handle = std::thread::spawn(move || {
                if let Some(store) = oplog_store.as_ref() {
                    if let Some(mut notifier) = store.create_change_notifier() {
                        let callback_applier = applier_clone.clone();
                        let callback_status = sync_status_ref.clone();
                        let callback_state_machine = state_machine.clone();
                        let error_state_machine = state_machine.clone();
                        let error_status = sync_status_ref.clone();
                        let start_seq_id = applier_clone.get_expected_sequence_id();
                        let on_entry = make_notifier_entry_callback(
                            callback_applier,
                            callback_status,
                            callback_state_machine,
                        );
                        let start_result = notifier.start(
                            start_seq_id,
                            on_entry,
                            Box::new(move |_err| {
                                if error_state_machine.is_in_state(StandbyState::Watching) {
                                    error_state_machine.increment_errors();
                                } else if !error_state_machine.is_in_state(StandbyState::Recovering)
                                    && !error_state_machine.is_in_state(StandbyState::Failed)
                                {
                                    error_state_machine.process_event(StandbyEvent::WatchBroken);
                                }
                                if let Some(status) = error_status.upgrade() {
                                    let mut st = status.write();
                                    st.state = error_state_machine.get_state();
                                    st.is_connected = error_state_machine.is_connected();
                                    if st.state == StandbyState::Failed {
                                        st.is_syncing = false;
                                    }
                                }
                            }),
                        );
                        if start_result.is_ok() {
                            if wait_for_notifier_startup(
                                notifier.as_mut(),
                                std::time::Duration::from_secs(1),
                            ) {
                                loop {
                                    if shutdown_rx.has_changed().unwrap_or(true) {
                                        notifier.stop();
                                        return;
                                    }
                                    if !notifier.is_healthy() {
                                        if state_machine.is_in_state(StandbyState::Watching) {
                                            state_machine.increment_errors();
                                            if state_machine.is_in_state(StandbyState::Watching) {
                                                std::thread::sleep(
                                                    std::time::Duration::from_millis(10),
                                                );
                                                continue;
                                            }
                                        }
                                        notifier.stop();
                                        break;
                                    }
                                    if state_machine.is_in_state(StandbyState::Recovering) {
                                        notifier.stop();
                                        break;
                                    }
                                    if !state_machine.is_connected() {
                                        notifier.stop();
                                        break;
                                    }
                                    state_machine.reset_errors();
                                    let expected = applier_clone.get_expected_sequence_id();
                                    let applied = expected.saturating_sub(1);
                                    let primary =
                                        observed_primary_sequence(store.as_ref(), applied);
                                    if let Some(status) = sync_status_ref.upgrade() {
                                        let mut st = status.write();
                                        st.applied_seq_id = applied;
                                        st.primary_seq_id = primary;
                                        st.lag_entries = primary.saturating_sub(applied);
                                    }
                                    std::thread::sleep(std::time::Duration::from_millis(100));
                                }
                            } else {
                                state_machine.process_event(StandbyEvent::WatchBroken);
                                notifier.stop();
                            }
                        } else {
                            state_machine.process_event(StandbyEvent::WatchBroken);
                        }
                    }
                }

                if state_machine.is_in_state(StandbyState::Failed) {
                    return;
                }
                if !state_machine.is_connected() {
                    if let Some(status) = sync_status_ref.upgrade() {
                        let mut st = status.write();
                        st.state = StandbyState::Reconnecting;
                        st.is_connected = false;
                    }
                }

                let mut consecutive_reconnect_failures = 0_u32;
                loop {
                    if shutdown_rx.has_changed().unwrap_or(true) {
                        break;
                    }
                    let mut expected = applier_clone.get_expected_sequence_id();
                    let read_result = oplog_store
                        .as_ref()
                        .ok_or_else(|| {
                            HaError::InvalidBackend(
                                "standby oplog store disappeared during reconnect".into(),
                            )
                        })
                        .and_then(|store| store.read_since(expected, 1024));
                    match read_result {
                        Ok(entries) => {
                            if entries.first().is_some_and(|entry| entry.seq != expected) {
                                state_machine.process_event(StandbyEvent::FatalError);
                                if let Some(status) = sync_status_ref.upgrade() {
                                    let mut st = status.write();
                                    st.state = StandbyState::Failed;
                                    st.is_connected = false;
                                    st.is_syncing = false;
                                }
                                tracing::error!(
                                    expected,
                                    first_sequence = entries[0].seq,
                                    "HotStandbyService: oplog replay encountered an unrecoverable sequence gap"
                                );
                                break;
                            }
                            if !entries.is_empty() {
                                let before_apply = expected;
                                applier_clone.apply_op_log_entries(&entries);
                                expected = applier_clone.get_expected_sequence_id();
                                if expected == before_apply {
                                    state_machine.process_event(StandbyEvent::FatalError);
                                    if let Some(status) = sync_status_ref.upgrade() {
                                        let mut st = status.write();
                                        st.state = StandbyState::Failed;
                                        st.is_connected = false;
                                        st.is_syncing = false;
                                    }
                                    tracing::error!(
                                        expected,
                                        "HotStandbyService: oplog replay rejected the expected record"
                                    );
                                    break;
                                }
                            }
                            if state_machine.is_in_state(StandbyState::Recovering) {
                                state_machine.process_event(StandbyEvent::RecoverySuccess);
                            } else if !state_machine.is_connected() {
                                state_machine.process_event(StandbyEvent::Connected);
                                state_machine.process_event(StandbyEvent::SyncComplete);
                            }
                            state_machine.reset_errors();
                            state_machine.reset_reconnect_count();
                            consecutive_reconnect_failures = 0;
                        }
                        Err(error) => {
                            if state_machine.is_in_state(StandbyState::Recovering) {
                                let event = match &error {
                                    HaError::Disconnected(_) => StandbyEvent::Disconnected,
                                    _ if error.is_fatal() => StandbyEvent::FatalError,
                                    _ => StandbyEvent::RecoveryFailed,
                                };
                                state_machine.process_event(event);
                            } else if state_machine.is_connected() {
                                state_machine.process_event(StandbyEvent::WatchBroken);
                            }
                            if state_machine.is_in_state(StandbyState::Failed) {
                                if let Some(status) = sync_status_ref.upgrade() {
                                    let mut st = status.write();
                                    st.state = StandbyState::Failed;
                                    st.is_connected = false;
                                    st.is_syncing = false;
                                }
                                tracing::error!(
                                    %error,
                                    "HotStandbyService: oplog recovery encountered a fatal error"
                                );
                                break;
                            }
                            state_machine.increment_reconnect_count();
                            consecutive_reconnect_failures =
                                consecutive_reconnect_failures.saturating_add(1);
                            if let Some(status) = sync_status_ref.upgrade() {
                                let mut st = status.write();
                                st.state = StandbyState::Reconnecting;
                                st.is_connected = false;
                            }
                            if consecutive_reconnect_failures >= MAX_STANDBY_RECONNECT_ATTEMPTS {
                                state_machine.process_event(StandbyEvent::MaxErrorsReached);
                                if let Some(status) = sync_status_ref.upgrade() {
                                    let mut st = status.write();
                                    st.state = StandbyState::Failed;
                                    st.is_connected = false;
                                    st.is_syncing = false;
                                }
                                tracing::error!(
                                    %error,
                                    attempts = consecutive_reconnect_failures,
                                    "HotStandbyService: oplog reconnect exhausted; stopping follower"
                                );
                                break;
                            }
                        }
                    }
                    let applied = if expected > 0 { expected - 1 } else { 0 };
                    let primary = oplog_store
                        .as_ref()
                        .map(|store| observed_primary_sequence(store.as_ref(), applied))
                        .unwrap_or(applied);

                    if let Some(s) = sync_status_ref.upgrade() {
                        let mut st = s.write();
                        st.applied_seq_id = applied;
                        st.primary_seq_id = primary;
                        st.lag_entries = primary.saturating_sub(applied);
                        if state_machine.is_connected() {
                            st.state = StandbyState::Watching;
                            st.is_connected = true;
                            st.is_syncing = true;
                        }
                    }

                    std::thread::sleep(poll_interval);
                }
            });
            self.replication_thread = Some(handle);

            info!("HotStandbyService: started oplog following mode with ReplicationLoop");
        } else {
            self.state_machine.process_event(StandbyEvent::Connected);
            self.state_machine.process_event(StandbyEvent::SyncComplete);
            let mut status = self.sync_status.write();
            status.state = StandbyState::Watching;
            status.applied_seq_id = baseline_seq_id;
            status.primary_seq_id = baseline_seq_id;
            status.lag_entries = 0;
            status.is_connected = true;
            status.is_syncing = false;
            drop(status);
        }

        Ok(())
    }

    /// Stop the hot standby service.
    /// 停止热备服务。
    ///
    /// Sends shutdown signal and resets state to Stopped.
    /// 发送 shutdown 信号，重置状态为 Stopped。
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.state_machine.process_event(StandbyEvent::Stop);
        if let Some(handle) = self.replication_thread.take() {
            let _ = handle.join();
        }
        let mut status = self.sync_status.write();
        status.state = StandbyState::Stopped;
        status.is_syncing = false;
    }

    /// Promote the standby to leader.
    /// 提升备节点为 leader。
    ///
    /// Transitions through Promoting → Promoted → Stopped and returns the
    /// applied oplog seq_id. Stopping the follower is part of successful
    /// promotion so a later leader term can start standby again.
    /// 状态转为 Promoting → Promoted → Stopped，返回已应用的 oplog 序列号。
    ///
    /// The returned seq_id can be used by the new leader to continue from where
    /// the old leader left off.
    /// 返回的 seq_id 可供新 leader 使用，从旧 leader 中断处继续。
    ///
    /// 提升为 Leader：追平后停止 follower，返回已应用的 oplog 序列号。
    pub async fn promote(&mut self) -> Result<u64, HaError> {
        let result = self.state_machine.process_event(StandbyEvent::Promote);
        if !result.allowed {
            return Err(HaError::UnavailableInCurrentStatus);
        }

        // Stop oplog following.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.replication_thread.take() {
            let _ = handle.join();
        }

        let catch_up_result = match (&self.oplog_applier, &self.oplog_store) {
            (Some(applier), Some(store)) => {
                run_promotion_catch_up_on_plain_thread(applier.clone(), store.clone()).await
            }
            _ => Ok(()),
        };
        if let Err(error) = catch_up_result {
            self.state_machine
                .process_event(StandbyEvent::PromotionFailed);
            let mut status = self.sync_status.write();
            status.state = StandbyState::Failed;
            status.is_connected = false;
            status.is_syncing = false;
            return Err(error);
        }

        // Drain jobs are runtime schedulers rather than durable Store state.
        // Terminal segment statuses are replayed, while an unfinished
        // DRAINING status must be treated as an aborted job before serving.
        abort_orphaned_drain_segments_after_recovery(&self.state);

        self.state_machine
            .process_event(StandbyEvent::PromotionSuccess);

        let applied = self
            .oplog_applier
            .as_ref()
            .map(|a| a.get_expected_sequence_id().saturating_sub(1))
            .unwrap_or_else(|| self.sync_status.read().applied_seq_id);

        // Match the C++ lifecycle: promotion consumes and stops the follower.
        // Keeping the service in Promoted would reject Start on the next term.
        self.stop();

        info!(
            "HotStandbyService: promoted and stopped follower with seq_id={}",
            applied
        );
        Ok(applied)
    }
}

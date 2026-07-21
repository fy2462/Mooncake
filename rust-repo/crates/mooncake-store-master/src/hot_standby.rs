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
// │  Stopped → Connecting → Recovering → Watching → Promoting → Promoted
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
use crate::oplog::OpLogStore;
use crate::service::state::MasterState;
use crate::service::sync_cache_total_accounting;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;

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
    /// Poll interval in milliseconds for oplog following.
    /// oplog 跟随的轮询间隔（毫秒）。
    pub oplog_poll_interval_ms: u64,
    /// Cluster identifier for snapshot scoping.
    /// 集群标识符，用于快照范围限定。
    pub cluster_id: String,
}

impl Default for HotStandbyConfig {
    fn default() -> Self {
        Self {
            enable_snapshot_bootstrap: false,
            enable_oplog_following: false,
            oplog_poll_interval_ms: 1000,
            cluster_id: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::SnapshotProvider;
    use crate::oplog::InMemoryOpLog;

    struct FailingSnapshotProvider;

    impl SnapshotProvider for FailingSnapshotProvider {
        fn load_latest_snapshot(
            &self,
            _cluster_id: &str,
        ) -> Result<Option<crate::ha::LoadedSnapshot>, HaError> {
            Err(HaError::Snapshot("snapshot unavailable".into()))
        }
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
    async fn test_snapshot_only_bootstrap_propagates_snapshot_error() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state,
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
    }

    #[tokio::test]
    async fn test_oplog_bootstrap_falls_back_when_snapshot_load_fails() {
        let state = Arc::new(MasterState::empty());
        let mut service = HotStandbyService::new(
            state,
            HotStandbyConfig {
                enable_snapshot_bootstrap: true,
                enable_oplog_following: true,
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

    /// Check if the standby is ready to be promoted.
    /// 检查备用节点是否准备好被提升。
    ///
    /// Ready states: Watching (actively following) or Promoted (already promoted).
    /// 就绪状态：Watching（活跃跟随中）或 Promoted（已提升）。
    pub fn is_ready_for_promotion(&self) -> bool {
        matches!(
            self.sync_status.read().state,
            StandbyState::Watching | StandbyState::Promoted
        )
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

                match provider.load_latest_snapshot(&self.config.cluster_id) {
                    Ok(Some(snapshot)) => {
                        // Restore memory segments into allocator
                        // 恢复内存 segment 到分配器
                        for seg in &snapshot.segments {
                            let sid = seg.segment.id;
                            if !self.state.segments.contains_key(&sid) {
                                self.state.segments.insert(sid, seg.clone());
                                // Register with the memory allocator so future allocations work
                                // 注册到内存分配器，以支持后续分配
                                self.state.allocator.write().add_segment(
                                    seg.segment.clone(),
                                    seg.used,
                                    seg.client_id,
                                );
                            }
                        }

                        // Restore NoF (file-based) segments into NoF allocator
                        // 恢复 NoF（基于文件的）segment 到 NoF 分配器
                        for nof in &snapshot.nof_segments {
                            let sid = nof.segment.id;
                            if !self.state.nof_segments.contains_key(&sid) {
                                self.state.nof_segments.insert(sid, nof.clone());
                                self.state.nof_allocator.write().add_segment(
                                    mooncake_store_core::Segment {
                                        id: nof.segment.id,
                                        name: nof.segment.name.clone(),
                                        base: nof.segment.base,
                                        size: nof.segment.size,
                                        te_endpoint: nof.segment.te_endpoint.clone(),
                                        protocol: String::new(),
                                    },
                                    nof.used,
                                    nof.segment.client_id,
                                );
                            }
                        }

                        // Restore objects and tasks
                        // 恢复对象和任务
                        for entry in &snapshot.objects {
                            let mut object = entry.1.clone();
                            sync_cache_total_accounting(&mut object);
                            self.state.objects.insert(entry.0.clone(), object);
                        }
                        for task in &snapshot.tasks {
                            self.state.tasks.insert(task.info.id, task.clone());
                        }
                        for local_disk in &snapshot.local_disk_segments {
                            self.state.local_disk_segments.insert(
                                local_disk.client_id,
                                crate::service::state::LocalDiskSegmentEntry {
                                    enable_offloading: local_disk.enable_offloading,
                                    offloading_objects: local_disk.offloading_objects.clone(),
                                    promotion_objects: Default::default(),
                                    ssd_total_capacity_bytes: local_disk.ssd_total_capacity_bytes,
                                },
                            );
                        }

                        let mut status = self.sync_status.write();
                        status.applied_seq_id = snapshot.snapshot_sequence_id;
                        baseline_seq_id = snapshot.snapshot_sequence_id;
                        drop(status);
                        info!(
                            "Loaded snapshot with {} objects, {} segments",
                            snapshot.objects.len(),
                            snapshot.segments.len()
                        );
                    }
                    Ok(None) => {}
                    Err(error) if self.config.enable_oplog_following => {
                        tracing::warn!(
                            "Failed to load snapshot baseline, falling back to oplog-only bootstrap: {}",
                            error
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        // Phase 2: Start oplog following (if enabled)
        // 阶段 2：启动 oplog 跟随（若启用）
        if self.config.enable_oplog_following {
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
                        let callback_store = store.clone();
                        let error_state_machine = state_machine.clone();
                        let start_seq_id = applier_clone.get_expected_sequence_id();
                        let start_result = notifier.start(
                            start_seq_id,
                            Box::new(move |entry| {
                                callback_applier.apply_op_log_entries(&[entry]);
                                let expected = callback_applier.get_expected_sequence_id();
                                let applied = expected.saturating_sub(1);
                                let primary = callback_store.latest_sequence();
                                if let Some(status) = callback_status.upgrade() {
                                    let mut st = status.write();
                                    st.applied_seq_id = applied;
                                    st.primary_seq_id = primary;
                                    st.lag_entries = primary.saturating_sub(applied);
                                }
                            }),
                            Box::new(move |_err| {
                                error_state_machine.process_event(StandbyEvent::WatchBroken);
                            }),
                        );
                        if start_result.is_ok() {
                            loop {
                                if shutdown_rx.has_changed().unwrap_or(true) {
                                    notifier.stop();
                                    return;
                                }
                                if !notifier.is_healthy() {
                                    state_machine.process_event(StandbyEvent::WatchBroken);
                                    notifier.stop();
                                    break;
                                }
                                let expected = applier_clone.get_expected_sequence_id();
                                let applied = expected.saturating_sub(1);
                                let primary = store.latest_sequence();
                                if let Some(status) = sync_status_ref.upgrade() {
                                    let mut st = status.write();
                                    st.applied_seq_id = applied;
                                    st.primary_seq_id = primary;
                                    st.lag_entries = primary.saturating_sub(applied);
                                }
                                std::thread::sleep(std::time::Duration::from_millis(100));
                            }
                        }
                    }
                }

                loop {
                    if shutdown_rx.has_changed().unwrap_or(true) {
                        break;
                    }
                    if !state_machine.is_connected() {
                        std::thread::sleep(poll_interval);
                        continue;
                    }
                    let mut expected = applier_clone.get_expected_sequence_id();
                    if let Some(store) = oplog_store.as_ref() {
                        match store.read_since(expected, 1024) {
                            Ok(entries) if !entries.is_empty() => {
                                applier_clone.apply_op_log_entries(&entries);
                                expected = applier_clone.get_expected_sequence_id();
                            }
                            Ok(_) => {}
                            Err(_) => {
                                state_machine.process_event(StandbyEvent::WatchBroken);
                            }
                        }
                    }
                    let applied = if expected > 0 { expected - 1 } else { 0 };
                    let primary = oplog_store
                        .as_ref()
                        .map(|s| s.latest_sequence())
                        .unwrap_or(applied);

                    if let Some(s) = sync_status_ref.upgrade() {
                        let mut st = s.write();
                        st.applied_seq_id = applied;
                        st.primary_seq_id = primary;
                        st.lag_entries = primary.saturating_sub(applied);
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
    /// Transitions through Promoting → Promoted, returns the applied oplog seq_id.
    /// 状态转为 Promoting → Promoted，返回已应用的 oplog 序列号。
    ///
    /// The returned seq_id can be used by the new leader to continue from where
    /// the old leader left off.
    /// 返回的 seq_id 可供新 leader 使用，从旧 leader 中断处继续。
    ///
    /// 提升为 Leader：状态转为 Promoting → Promoted，返回已应用的 oplog 序列号。
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

        // Gap resolution + final catch-up.
        // C++: ResolvePromotionGapsLocked (3 retries, stops when all gaps filled).
        if let Some(ref applier) = self.oplog_applier {
            if let Some(ref store) = self.oplog_store {
                for _ in 0..3 {
                    let (needed, fetched) = applier.try_resolve_gaps_once(store.as_ref(), 1024);
                    // Match C++: stop when all needed entries were fetched.
                    if needed == 0 || fetched >= needed {
                        break;
                    }
                }
                // Final catch-up (C++: FinalCatchUpForPromotionLocked).
                // Uses same store (shared Arc) — C++ creates a NEW store, but in
                // Rust the Arc clone preserves the store reference.
                let mut expected = applier.get_expected_sequence_id();
                let latest = store.latest_sequence();
                if latest >= expected {
                    let start = std::time::Instant::now();
                    let timeout = std::time::Duration::from_secs(30);
                    for _ in 0..100 {
                        if start.elapsed() >= timeout {
                            break;
                        }
                        expected = applier.get_expected_sequence_id();
                        if latest < expected {
                            break;
                        }
                        let to_read = ((latest - expected + 1) as usize).min(1000);
                        if to_read == 0 {
                            break;
                        }
                        if let Ok(entries) = store.read_since(expected, to_read) {
                            if entries.is_empty() {
                                break;
                            }
                            applier.apply_op_log_entries(&entries);
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        self.state_machine
            .process_event(StandbyEvent::PromotionSuccess);

        let applied = self
            .oplog_applier
            .as_ref()
            .map(|a| a.get_expected_sequence_id().saturating_sub(1))
            .unwrap_or(0);

        let mut status = self.sync_status.write();
        status.state = StandbyState::Promoted;
        drop(status);

        info!("HotStandbyService: promoted with seq_id={}", applied);
        Ok(applied)
    }
}

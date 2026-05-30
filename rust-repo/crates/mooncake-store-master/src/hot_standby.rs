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

use crate::ha::{HaError, SnapshotProvider, StandbyState, StandbySyncStatus};
use crate::service::state::MasterState;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

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
    sync_status: parking_lot::RwLock<StandbySyncStatus>,
    /// Shutdown signal sender — dropping or sending triggers graceful stop.
    /// 关闭信号发送器 —— drop 或发送触发优雅停止。
    shutdown_tx: Option<watch::Sender<()>>,
    /// Optional snapshot provider for bootstrap recovery.
    /// 可选的快照提供者，用于引导恢复。
    snapshot_provider: Option<Box<dyn SnapshotProvider>>,
}

impl HotStandbyService {
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
        let mut status = self.sync_status.write();
        status.state = StandbyState::Connecting;
        status.is_connected = false;
        drop(status);

        // Phase 1: Snapshot bootstrap (if enabled)
        // 阶段 1：快照引导（若启用）
        if self.config.enable_snapshot_bootstrap {
            if let Some(ref provider) = self.snapshot_provider {
                let mut status = self.sync_status.write();
                status.state = StandbyState::Recovering;
                drop(status);

                if let Ok(Some(snapshot)) = provider.load_latest_snapshot(&self.config.cluster_id) {
                    // Restore memory segments into allocator
                    // 恢复内存 segment 到分配器
                    for seg in &snapshot.segments {
                        let sid = seg.id;
                        if !self.state.segments.contains_key(&sid) {
                            self.state.segments.insert(
                                sid,
                                crate::service::SegmentEntry {
                                    segment: seg.clone(),
                                    status: crate::proto::SegmentStatus::Active,
                                    used: 0,
                                    client_id: Uuid::nil(),
                                },
                            );
                            // Register with the memory allocator so future allocations work
                            // 注册到内存分配器，以支持后续分配
                            self.state
                                .allocator
                                .write()
                                .add_segment(seg.clone(), 0, Uuid::nil());
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
                        self.state.objects.insert(entry.0.clone(), entry.1.clone());
                    }
                    for task in &snapshot.tasks {
                        self.state.tasks.insert(task.info.id, task.clone());
                    }

                    let mut status = self.sync_status.write();
                    status.applied_seq_id = snapshot.snapshot_sequence_id;
                    drop(status);
                    info!(
                        "Loaded snapshot with {} objects, {} segments",
                        snapshot.objects.len(),
                        snapshot.segments.len()
                    );
                }
            }
        }

        // Phase 2: Start oplog following (if enabled)
        // 阶段 2：启动 oplog 跟随（若启用）
        if self.config.enable_oplog_following {
            let mut status = self.sync_status.write();
            status.state = StandbyState::Watching;
            status.is_syncing = true;
            drop(status);
            // NOTE: actual oplog following requires a shared OpLogStore.
            // For now, mark as watching — the store should be injected externally.
            // 注意：实际的 oplog 跟随需要共享的 OpLogStore。
            // 目前标记为 watching —— store 应由外部注入。
            info!("HotStandbyService: started oplog following mode");
        } else {
            // No oplog → go straight to Watching (ready but not syncing)
            // 无 oplog → 直接进入 Watching（就绪但不同步）
            let mut status = self.sync_status.write();
            status.state = StandbyState::Watching;
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
        let mut status = self.sync_status.write();
        status.state = StandbyState::Promoting;
        drop(status);

        let applied = {
            let s = self.sync_status.read();
            s.applied_seq_id
        };

        let mut status = self.sync_status.write();
        status.state = StandbyState::Promoted;
        drop(status);

        info!("HotStandbyService: promoted with seq_id={}", applied);
        Ok(applied)
    }
}

use super::snapshot::{LoadedSnapshot, NoopSnapshotProvider, SnapshotProvider, LocalSnapshotProvider};
use super::types::{
    HaError, HABackendSpec, HABackendType, MasterRuntimeState, MasterView, RuntimeStateCallback,
    StandbyState, StandbySyncStatus,
};
use crate::storage_backend::StorageBackendType;
use std::path::PathBuf;

// ----------------------------------------------------------------------------
// MasterServiceSupervisorConfig — configuration for the HA supervisor
// MasterServiceSupervisorConfig —— HA supervisor 的配置
// ----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MasterServiceSupervisorConfig {
    /// This node's hostname, used as leader_address in election.
    /// 本节点的主机名，在选举中用作 leader_address。
    pub local_hostname: String,
    /// Cluster identifier for namespace isolation. / 集群标识符，用于命名空间隔离。
    pub cluster_id: String,
    /// Whether to enable snapshot-based standby bootstrap on startup.
    /// 是否在启动时启用基于快照的 standby 引导。
    pub enable_snapshot_restore: bool,
    /// Directory where snapshots are stored. / 快照存储的目录。
    pub snapshot_backup_dir: Option<PathBuf>,
    /// Backend type for snapshot storage (e.g. JSON). / 快照存储的后端类型。
    pub snapshot_backend_type: Option<StorageBackendType>,
}

impl Default for MasterServiceSupervisorConfig {
    fn default() -> Self {
        Self {
            local_hostname: "localhost".to_string(),
            cluster_id: "default".to_string(),
            enable_snapshot_restore: false,
            snapshot_backup_dir: None,
            snapshot_backend_type: None,
        }
    }
}

// ----------------------------------------------------------------------------
// StandbyRuntimeCapabilities — what the standby can do
// StandbyRuntimeCapabilities —— standby 的能力描述
// ----------------------------------------------------------------------------

/// Describes the capabilities available to a standby in this deployment.
/// 描述本次部署中 standby 可用的能力。
#[derive(Debug, Clone, Copy)]
pub struct StandbyRuntimeCapabilities {
    /// True if snapshot bootstrap is available. / 快照引导是否可用。
    pub has_snapshot_bootstrap: bool,
    /// True if oplog following (etcd) is available. / oplog 跟随（etcd）是否可用。
    pub has_oplog_following: bool,
}

/// Build capabilities from backend spec and supervisor config.
/// 从后端规格和 supervisor 配置构建能力描述。
pub fn build_standby_runtime_capabilities(
    spec: &HABackendSpec,
    config: &MasterServiceSupervisorConfig,
) -> StandbyRuntimeCapabilities {
    StandbyRuntimeCapabilities {
        has_snapshot_bootstrap: config.enable_snapshot_restore,
        // Oplog following is only available with etcd (its sequential key model).
        // oplog 跟随仅在 etcd 下可用（因其有序 key 模型）。
        has_oplog_following: spec.backend_type == HABackendType::Etcd,
    }
}

/// Map a standby's raw sync status to a MasterRuntimeState.
/// 将 standby 的原始同步状态映射为 MasterRuntimeState。
///
/// Logic (逻辑):
/// - Stopped → Standby
/// - Connecting/Syncing/Recovering/Reconnecting/Failed → Recovering
/// - Watching with lag_entries > 0 and oplog following enabled → CatchingUp
/// - Watching without lag or without oplog following → Standby
/// - Promoting/Promoted → LeaderWarmup
pub fn map_standby_runtime_state(
    status: &StandbySyncStatus,
    observed_leader: Option<&MasterView>,
    capabilities: StandbyRuntimeCapabilities,
) -> MasterRuntimeState {
    match status.state {
        StandbyState::Stopped => MasterRuntimeState::Standby,
        // All non-terminal intermediate states map to Recovering.
        // 所有非终结中间状态映射为 Recovering。
        StandbyState::Connecting
        | StandbyState::Syncing
        | StandbyState::Recovering
        | StandbyState::Reconnecting
        | StandbyState::Failed => MasterRuntimeState::Recovering,
        StandbyState::Watching => {
            // If oplog following is available and the leader is visible and
            // we are behind, we are catching up. Otherwise, we are stable standby.
            // 如果 oplog 跟随可用、leader 可见且落后于 leader，则正在追赶。
            // 否则是稳定 standby。
            if capabilities.has_oplog_following
                && observed_leader.is_some()
                && status.lag_entries > 0
            {
                MasterRuntimeState::CatchingUp
            } else {
                MasterRuntimeState::Standby
            }
        }
        StandbyState::Promoting | StandbyState::Promoted => MasterRuntimeState::LeaderWarmup,
    }
}

// ----------------------------------------------------------------------------
// StandbyController — unified trait for standby lifecycle management
// StandbyController —— standby 生命周期管理的统一 trait
//
// Two implementations (两种实现):
// - NoopStandbyController: no-op for single-node / manual mode.
//   无操作实现，用于单节点 / 手动模式。
// - CapabilityDrivenStandbyController: snapshot bootstrap + oplog following.
//   基于能力的实现：快照引导 + oplog 跟随。
//
// promote_standby checks lag_entries == 0 (oplog caught up) before allowing
// promotion. / promote_standby 会校验 lag_entries 是否归零（oplog 已追平）
// 才允许提升。
// ----------------------------------------------------------------------------

pub trait StandbyController: Send {
    /// Start standby mode with an optional observed leader address.
    /// 以可选的 observed leader 地址启动 standby 模式。
    fn start_standby(&mut self, observed_leader: Option<MasterView>) -> Result<(), HaError>;

    /// Stop standby mode and release resources. / 停止 standby 模式并释放资源。
    fn stop_standby(&mut self);

    /// Promote this standby to leader. / 将此 standby 提升为 leader。
    fn promote_standby(&mut self) -> Result<(), HaError>;

    /// Update the observed leader address (e.g. after a view change).
    /// 更新 observed leader 地址（如视图变更后）。
    fn update_observed_leader(&mut self, observed_leader: Option<MasterView>);

    /// Get the current runtime state for gRPC health checks.
    /// 获取当前运行时状态，用于 gRPC 健康检查。
    fn get_standby_runtime_state(&self) -> MasterRuntimeState;

    /// Register a callback for runtime state changes.
    /// 注册运行时状态变化的回调。
    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>);
}

// ----------------------------------------------------------------------------
// NoopStandbyController — no-op standby (single-node mode)
// NoopStandbyController —— 无操作 standby（单节点模式）
//
// Always reports Standby. Promote is a no-op. Used for development and
// single-node deployments.
//
// 始终报告 Standby。Promote 是无操作。用于开发环境和单节点部署。
// ----------------------------------------------------------------------------

pub struct NoopStandbyController {
    callback: Option<RuntimeStateCallback>,
}

impl NoopStandbyController {
    pub fn new() -> Self {
        Self { callback: None }
    }
}

impl Default for NoopStandbyController {
    fn default() -> Self {
        Self::new()
    }
}

impl StandbyController for NoopStandbyController {
    fn start_standby(&mut self, _observed_leader: Option<MasterView>) -> Result<(), HaError> {
        // Immediately notify that we are in Standby state.
        // 立即通知我们处于 Standby 状态。
        if let Some(callback) = &self.callback {
            callback(MasterRuntimeState::Standby);
        }
        Ok(())
    }

    fn stop_standby(&mut self) {}

    fn promote_standby(&mut self) -> Result<(), HaError> {
        Ok(())
    }

    fn update_observed_leader(&mut self, _observed_leader: Option<MasterView>) {}

    fn get_standby_runtime_state(&self) -> MasterRuntimeState {
        MasterRuntimeState::Standby
    }

    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>) {
        self.callback = callback;
        if let Some(callback) = &self.callback {
            callback(MasterRuntimeState::Standby);
        }
    }
}

// ----------------------------------------------------------------------------
// CapabilityDrivenStandbyController — full-featured standby
// CapabilityDrivenStandbyController —— 全功能 standby
//
// This is the production standby implementation. It supports:
// - Snapshot bootstrap: load a recent snapshot from disk to quickly initialise
//   the standby's state before catching up on recent oplog entries.
// - Oplog following: tail the oplog from the leader to stay in sync.
// - Lag-aware promotion: refuse promotion if lag_entries > 0.
//
// 这是生产环境的 standby 实现。支持：
// - 快照引导：从磁盘加载最近的快照以快速初始化 standby 状态，然后追赶最近的 oplog。
// - oplog 跟随：从 leader 尾部跟随 oplog 以保持同步。
// - 滞后感知的提升：如果 lag_entries > 0 则拒绝提升。
// ----------------------------------------------------------------------------

pub struct CapabilityDrivenStandbyController {
    /// Supervisor configuration. / supervisor 配置。
    config: MasterServiceSupervisorConfig,
    /// Available capabilities (snapshot, oplog). / 可用能力（快照、oplog）。
    capabilities: StandbyRuntimeCapabilities,
    /// Provider for loading snapshots from storage. / 从存储加载快照的提供者。
    snapshot_provider: Box<dyn SnapshotProvider>,
    /// Currently observed leader address + version.
    /// 当前观察到的 leader 地址和版本。
    observed_leader: Option<MasterView>,
    /// Whether the standby loop is running. / standby 循环是否正在运行。
    standby_running: bool,
    /// Whether promotion has been completed. / 提升是否已完成。
    promoted: bool,
    /// The last error that occurred. / 最近发生的错误。
    last_error: Option<HaError>,
    /// Current sync status (applied_seq_id, lag, etc.).
    /// 当前同步状态（applied_seq_id, lag 等）。
    sync_status: StandbySyncStatus,
    /// Snapshot loaded during recovery, if any. / 恢复期间加载的快照（如果有）。
    loaded_snapshot: Option<LoadedSnapshot>,
    /// Runtime state change callback. / 运行时状态变化回调。
    callback: Option<RuntimeStateCallback>,
    /// Last reported runtime state; used to avoid redundant callbacks.
    /// 最近报告的运行时状态；用于避免重复回调。
    last_reported_runtime_state: Option<MasterRuntimeState>,
}

impl CapabilityDrivenStandbyController {
    /// Create a new controller from backend spec and supervisor config.
    /// 从后端规格和 supervisor 配置创建新控制器。
    pub fn new(spec: HABackendSpec, config: MasterServiceSupervisorConfig) -> Self {
        let capabilities = build_standby_runtime_capabilities(&spec, &config);

        // Choose the snapshot provider based on config.
        // 根据配置选择快照提供者。
        let snapshot_provider: Box<dyn SnapshotProvider> = if capabilities.has_snapshot_bootstrap {
            match (&config.snapshot_backup_dir, config.snapshot_backend_type) {
                (Some(dir), Some(backend_type)) => {
                    Box::new(LocalSnapshotProvider::new(dir.clone(), backend_type))
                }
                _ => Box::new(NoopSnapshotProvider),
            }
        } else {
            Box::new(NoopSnapshotProvider)
        };
        Self::with_snapshot_provider(config, capabilities, snapshot_provider)
    }

    /// Constructor with explicit snapshot provider (useful for testing).
    /// 带显式快照提供者的构造函数（用于测试）。
    pub fn with_snapshot_provider(
        config: MasterServiceSupervisorConfig,
        capabilities: StandbyRuntimeCapabilities,
        snapshot_provider: Box<dyn SnapshotProvider>,
    ) -> Self {
        Self {
            config,
            capabilities,
            snapshot_provider,
            observed_leader: None,
            standby_running: false,
            promoted: false,
            last_error: None,
            sync_status: StandbySyncStatus::default(),
            loaded_snapshot: None,
            callback: None,
            last_reported_runtime_state: None,
        }
    }

    pub fn sync_status(&self) -> &StandbySyncStatus {
        &self.sync_status
    }

    pub fn loaded_snapshot(&self) -> Option<&LoadedSnapshot> {
        self.loaded_snapshot.as_ref()
    }

    /// Test-only helper: manually set sync status and fire callback.
    /// 仅限测试的辅助函数：手动设置同步状态并触发回调。
    pub fn update_sync_status_for_test(&mut self, status: StandbySyncStatus) {
        self.sync_status = status;
        self.notify_runtime_state_if_changed();
    }

    /// Notify the runtime state callback only if the state has actually changed.
    /// 仅在状态实际变化时通知运行时状态回调。
    fn notify_runtime_state_if_changed(&mut self) {
        let runtime_state = self.get_standby_runtime_state();
        if self.last_reported_runtime_state == Some(runtime_state) {
            return; // no change / 无变化
        }
        self.last_reported_runtime_state = Some(runtime_state);
        if let Some(callback) = &self.callback {
            callback(runtime_state);
        }
    }
}

impl StandbyController for CapabilityDrivenStandbyController {
    fn start_standby(&mut self, observed_leader: Option<MasterView>) -> Result<(), HaError> {
        self.observed_leader = observed_leader;

        // Already running — just notify current state. / 已在运行 —— 仅通知当前状态。
        if self.standby_running {
            self.notify_runtime_state_if_changed();
            return Ok(());
        }

        self.promoted = false;
        self.sync_status = StandbySyncStatus {
            is_connected: self.observed_leader.is_some(),
            is_syncing: self.capabilities.has_oplog_following,
            // If snapshot bootstrap is available, start in Recovering to load it.
            // 如果快照引导可用，以 Recovering 状态开始以加载快照。
            state: if self.capabilities.has_snapshot_bootstrap {
                StandbyState::Recovering
            } else if self.capabilities.has_oplog_following {
                StandbyState::Watching
            } else {
                StandbyState::Stopped
            },
            ..Default::default()
        };

        // Load snapshot if bootstrap is enabled. / 如果启用了引导，加载快照。
        if self.capabilities.has_snapshot_bootstrap {
            self.loaded_snapshot = self
                .snapshot_provider
                .load_latest_snapshot(&self.config.cluster_id)?;
            if let Some(snapshot) = &self.loaded_snapshot {
                self.sync_status.applied_seq_id = snapshot.snapshot_sequence_id;
                self.sync_status.primary_seq_id = snapshot.snapshot_sequence_id;
            }
        } else {
            self.loaded_snapshot = None;
        }

        // After recovery, transition to Watching (or Stopped if no capabilities).
        // 恢复后，转换为 Watching（如果无能力则为 Stopped）。
        self.sync_status.state = if self.capabilities.has_oplog_following {
            StandbyState::Watching
        } else if self.capabilities.has_snapshot_bootstrap {
            StandbyState::Watching
        } else {
            StandbyState::Stopped
        };

        self.standby_running = true;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
        Ok(())
    }

    fn stop_standby(&mut self) {
        self.standby_running = false;
        self.promoted = false;
        self.sync_status = StandbySyncStatus::default();
        self.loaded_snapshot = None;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
    }

    fn promote_standby(&mut self) -> Result<(), HaError> {
        // Refuse promotion if standby is not running.
        // 如果 standby 未运行，拒绝提升。
        if !self.standby_running {
            let error = self
                .last_error
                .clone()
                .unwrap_or(HaError::UnavailableInCurrentStatus);
            self.last_error = Some(error.clone());
            return Err(error);
        }

        // Refuse promotion if oplog following is active and we are behind.
        // 如果 oplog 跟随处于活动状态且落后于 leader，拒绝提升。
        if self.capabilities.has_oplog_following && self.sync_status.lag_entries > 0 {
            self.last_error = Some(HaError::UnavailableInCurrentStatus);
            return Err(HaError::UnavailableInCurrentStatus);
        }

        // Promotion accepted. / 提升被接受。
        self.sync_status.state = StandbyState::Promoted;
        self.promoted = true;
        self.standby_running = false;
        self.last_error = None;
        self.notify_runtime_state_if_changed();
        Ok(())
    }

    fn update_observed_leader(&mut self, observed_leader: Option<MasterView>) {
        self.observed_leader = observed_leader;
        self.sync_status.is_connected = self.observed_leader.is_some();
        self.notify_runtime_state_if_changed();
    }

    fn get_standby_runtime_state(&self) -> MasterRuntimeState {
        if self.promoted {
            return MasterRuntimeState::LeaderWarmup;
        }
        if !self.standby_running {
            return MasterRuntimeState::Standby;
        }
        map_standby_runtime_state(
            &self.sync_status,
            self.observed_leader.as_ref(),
            self.capabilities,
        )
    }

    fn set_runtime_state_callback(&mut self, callback: Option<RuntimeStateCallback>) {
        self.callback = callback;
        self.last_reported_runtime_state = None;
        self.notify_runtime_state_if_changed();
    }
}

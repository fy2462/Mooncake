// ============================================================================
// High Availability (HA) — leader election, hot standby, snapshot restore.
// 高可用 (HA) —— Leader 选举、热备、快照恢复。
//
// This module implements the master service's HA subsystem, which includes:
//
//   1. Leader election — via Etcd campaign, Redis SET NX, or K8s Lease.
//      Leader 选举 —— 通过 etcd campaign、Redis SET NX 或 K8s Lease。
//
//   2. Runtime state machine — master transitions through lifecycle states:
//      Starting → Standby → Candidate → Recovering → CatchingUp →
//      LeaderWarmup → Serving. Only Serving/LeaderWarmup states admit
//      client requests. The `MasterServiceSupervisor` manages these
//      transitions and exposes the current state to gRPC handlers.
//
//      运行时状态机 —— master 通过生命周期状态转换：
//      Starting → Standby → Candidate → Recovering → CatchingUp →
//      LeaderWarmup → Serving。只有 Serving/LeaderWarmup 状态接受客户端请求。
//      MasterServiceSupervisor 管理这些转换并向 gRPC 处理器暴露当前状态。
//
//   3. Hot standby — `StandbyController` implementations track leader status
//      and can promote a standby to leader. Two implementations:
//      - `NoopStandbyController`: no HA (single-node or manual mode).
//      - `CapabilityDrivenStandbyController`: uses snapshot bootstrap and/or
//        oplog following to prepare the standby before promotion.
//
//      热备 —— StandbyController 实现追踪 leader 状态并可将 standby 提升为 leader。
//      两种实现：NoopStandbyController（无 HA，单节点或手动模式）和
//      CapabilityDrivenStandbyController（使用快照引导和/或 oplog 跟随准备 standby）。
//
//   4. Leader keepalive — background tokio task renews the lease periodically;
//      if renewal fails, the leader is demoted to Standby via the watch channel.
//
//      Leader 续约 —— 后台 tokio 任务定期续约租约；续约失败时通过 watch channel
//      降级为 Standby。
//
// C++ equivalent: ha_service.* / master_ha.cpp
// ============================================================================

use crate::service::NoFSegmentEntry;
use crate::service::ObjectEntry;
use crate::service::TaskEntry;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use mooncake_store_core::Segment;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{error, info, warn};

// ----------------------------------------------------------------------------
// RuntimeStateCallback (运行时状态回调)
// ----------------------------------------------------------------------------

/// Type alias for a callback invoked whenever the master's runtime state changes.
/// Used by `StandbyController` and `MasterServiceSupervisor` to propagate state
/// transitions to the gRPC service layer.
///
/// 运行时状态变化时的回调函数类型别名。
/// 由 StandbyController 和 MasterServiceSupervisor 用于将状态转换传播到 gRPC 服务层。
pub type RuntimeStateCallback = Arc<dyn Fn(MasterRuntimeState) + Send + Sync>;

// ----------------------------------------------------------------------------
// HaError — HA subsystem error types / HA 子系统错误类型
// ----------------------------------------------------------------------------

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HaError {
    /// HA backend configuration is invalid (e.g. unknown backend type).
    /// HA 后端配置无效（如未知的后端类型）。
    #[error("invalid ha backend: {0}")]
    InvalidBackend(String),

    /// Snapshot load/restore failed. / 快照加载/恢复失败。
    #[error("snapshot error: {0}")]
    Snapshot(String),

    /// Operation not allowed in the current state (e.g. promote while not standby).
    /// 当前状态不允许该操作（如非 standby 状态尝试提升）。
    #[error("unavailable in current status")]
    UnavailableInCurrentStatus,
}

// ----------------------------------------------------------------------------
// LeaderRole — two-state leadership model
// LeaderRole —— Leader 选举的两态模型
//
// Leader: actively serving client requests / 负责服务客户端请求
// Standby: waiting to take over / 等待接管
// ----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderRole {
    Leader,
    Standby,
}

// ----------------------------------------------------------------------------
// HABackendType — supported HA backends / 支持的 HA 后端
//
// Etcd: distributed lease + campaign / 分布式租约 + campaign
// Redis: SET NX PX for leader key / SET NX PX 抢 leader key
// K8s: Kubernetes Lease resource / Kubernetes Lease 资源
// ----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HABackendType {
    Unknown,
    Etcd,
    Redis,
    K8s,
}

impl HABackendType {
    /// Human-readable backend name. / 人类可读的后端名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Etcd => "etcd",
            Self::Redis => "redis",
            Self::K8s => "k8s",
        }
    }
}

/// Parse a backend type string from configuration. / 从配置字符串解析后端类型。
pub fn parse_ha_backend_type(value: &str) -> Option<HABackendType> {
    match value {
        "etcd" => Some(HABackendType::Etcd),
        "redis" => Some(HABackendType::Redis),
        "k8s" => Some(HABackendType::K8s),
        _ => None,
    }
}

/// Specification for the HA backend connection.
/// HA 后端连接的规格说明。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HABackendSpec {
    /// The backend type (Etcd, Redis, K8s). / 后端类型。
    pub backend_type: HABackendType,
    /// Connection string (e.g. "http://etcd:2379" or "redis://localhost:6379").
    /// 连接字符串。
    pub connstring: String,
    /// Cluster namespace for isolation. / 集群命名空间，用于隔离。
    pub cluster_namespace: String,
}

/// The master's view of the cluster leader (who is currently the leader).
/// Master 对集群 leader 的视图（当前谁是 leader）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterView {
    /// Address of the leader node. / leader 节点的地址。
    pub leader_address: String,
    /// Monotonic version counter incremented on each leadership change.
    /// 每次 leader 变更时递增的单调版本计数器。
    pub view_version: u64,
}

// ----------------------------------------------------------------------------
// MasterRuntimeState — lifecycle state machine for the master service
// MasterRuntimeState —— master 服务的生命周期状态机
//
// State transitions (状态转换):
//
//   Starting ──→ Standby ──→ Candidate ──→ Recovering ──→ CatchingUp
//                                                     ↓
//                                               LeaderWarmup ──→ Serving
//
// - Starting: initial state, HA subsystem not yet ready.
//   初始状态，HA 子系统尚未就绪。
// - Standby: passive standby, watching leader but not serving.
//   被动 standby，观察 leader 但不处理请求。
// - Candidate: competing in the election (e.g. etcd campaign in progress).
//   参与选举竞争（如正在执行 etcd campaign）。
// - Recovering: loading snapshot and/or catching up oplog after election win.
//   赢得选举后加载快照和/或追赶 oplog。
// - CatchingUp: oplog following in progress, trailing behind the leader.
//   oplog 跟随进行中，落后于 leader。
// - LeaderWarmup: elected leader, applying final state but not yet accepting client traffic.
//   已当选 leader，正在应用最终状态但尚未接受客户端流量。
// - Serving: fully operational leader accepting client requests.
//   完全运作的 leader，接受客户端请求。
//
// C++ equivalent: MasterRuntimeState in ha_service.h
// ----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterRuntimeState {
    Starting,
    Standby,
    Candidate,
    Recovering,
    CatchingUp,
    LeaderWarmup,
    Serving,
}

impl MasterRuntimeState {
    /// Human-readable state name. / 人类可读的状态名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Standby => "standby",
            Self::Candidate => "candidate",
            Self::Recovering => "recovering",
            Self::CatchingUp => "catching_up",
            Self::LeaderWarmup => "leader_warmup",
            Self::Serving => "serving",
        }
    }

    /// Map state to role: LeaderWarmup and Serving are leader states;
    /// all others are standby. / 将状态映射为角色：LeaderWarmup 和 Serving
    /// 是 leader 状态；其余均为 standby。
    pub fn role(self) -> &'static str {
        match self {
            Self::LeaderWarmup | Self::Serving => "leader",
            Self::Starting
            | Self::Standby
            | Self::Candidate
            | Self::Recovering
            | Self::CatchingUp => "standby",
        }
    }
}

// ----------------------------------------------------------------------------
// StandbyState — hot standby sync state machine
// StandbyState —— 热备节点的同步状态机
//
// Stopped → Connecting → Recovering → Watching → Promoted
//                                          ↓
//                                       Promoting
//
// C++ equivalent: StandbyState in ha_service.h
// ----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandbyState {
    /// Standby is not running. / Standby 未运行。
    Stopped,
    /// Connecting to the leader for initial sync. / 正在连接 leader 进行初始同步。
    Connecting,
    /// Synchronising oplog from leader. / 正在从 leader 同步 oplog。
    Syncing,
    /// Recovering from a snapshot. / 正在从快照恢复。
    Recovering,
    /// Reconnecting after a connection loss. / 连接断开后重新连接。
    Reconnecting,
    /// Sync failed. / 同步失败。
    Failed,
    /// Watching the leader for oplog entries. / 观察 leader 的 oplog 条目。
    Watching,
    /// Being promoted to leader. / 正在被提升为 leader。
    Promoting,
    /// Promotion completed. / 提升完成。
    Promoted,
}

/// Synchronisation status for a hot standby node.
/// 热备节点的同步状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandbySyncStatus {
    /// Last sequence ID applied on this standby. / 此 standby 最后应用的序列 ID。
    pub applied_seq_id: u64,
    /// Current sequence ID on the primary (leader). / 主节点（leader）当前的序列 ID。
    pub primary_seq_id: u64,
    /// Number of oplog entries this standby is behind. / 此 standby 落后的 oplog 条目数。
    pub lag_entries: u64,
    /// Whether the standby is actively syncing. / standby 是否正在主动同步。
    pub is_syncing: bool,
    /// Whether the standby is connected to the leader. / standby 是否连接到 leader。
    pub is_connected: bool,
    /// Current standby state. / 当前 standby 状态。
    pub state: StandbyState,
}

impl Default for StandbySyncStatus {
    fn default() -> Self {
        Self {
            applied_seq_id: 0,
            primary_seq_id: 0,
            lag_entries: 0,
            is_syncing: false,
            is_connected: false,
            state: StandbyState::Stopped,
        }
    }
}

// ----------------------------------------------------------------------------
// LoadedSnapshot — snapshot data loaded during standby recovery
// LoadedSnapshot —— standby 恢复期间加载的快照数据
// ----------------------------------------------------------------------------

/// A fully-loaded snapshot containing all state needed to bootstrap a standby.
/// 已完全加载的快照，包含引导 standby 所需的所有状态。
#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    /// Human-readable snapshot identifier (e.g. "snapshot-1712345678000").
    /// 人类可读的快照标识符。
    pub snapshot_id: String,
    /// Sequence ID at the time the snapshot was taken. / 快照拍摄时的序列 ID。
    pub snapshot_sequence_id: u64,
    /// Memory segments at snapshot time. / 快照时的内存 segment。
    pub segments: Vec<Segment>,
    /// NVMe-oF segments at snapshot time. / 快照时的 NVMe-oF segment。
    pub nof_segments: Vec<NoFSegmentEntry>,
    /// Objects and their replicas at snapshot time. / 快照时的对象及其副本。
    pub objects: Vec<(String, ObjectEntry)>,
    /// Pending tasks at snapshot time. / 快照时的待处理任务。
    pub tasks: Vec<TaskEntry>,
}

// ----------------------------------------------------------------------------
// SnapshotProvider — trait for loading snapshots
// SnapshotProvider —— 加载快照的 trait
//
// Different backends implement snapshot storage differently:
// - NoopSnapshotProvider: always returns None (no snapshots).
// - LocalSnapshotProvider: reads from local disk via StorageBackend.
//
// 不同的后端以不同方式实现快照存储：
// - NoopSnapshotProvider 始终返回 None（无快照）。
// - LocalSnapshotProvider 通过 StorageBackend 从本地磁盘读取。
// ----------------------------------------------------------------------------

/// Trait for loading snapshots during standby bootstrap.
/// standby 引导期间加载快照的 trait。
pub trait SnapshotProvider: Send + Sync {
    /// Load the latest snapshot for the given cluster.
    /// 加载给定集群的最新快照。
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError>;
}

/// No-op snapshot provider: always returns None.
/// 空操作快照提供者：始终返回 None。
pub struct NoopSnapshotProvider;

impl SnapshotProvider for NoopSnapshotProvider {
    fn load_latest_snapshot(&self, _cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        Ok(None)
    }
}

/// Local disk snapshot provider: reads master state from a directory.
/// 本地磁盘快照提供者：从目录读取 master 状态。
pub struct LocalSnapshotProvider {
    /// Root directory for snapshot files. / 快照文件的根目录。
    root_dir: PathBuf,
    /// Storage format backend type (e.g. JSON, binary). / 存储格式后端类型。
    backend_type: StorageBackendType,
}

impl LocalSnapshotProvider {
    pub fn new(root_dir: PathBuf, backend_type: StorageBackendType) -> Self {
        Self {
            root_dir,
            backend_type,
        }
    }
}

impl SnapshotProvider for LocalSnapshotProvider {
    fn load_latest_snapshot(&self, cluster_id: &str) -> Result<Option<LoadedSnapshot>, HaError> {
        // If cluster_id is specified, use a cluster-specific subdirectory.
        // 如果指定了 cluster_id，使用集群特定的子目录。
        let dir = if cluster_id.is_empty() {
            self.root_dir.clone()
        } else {
            self.root_dir.join(cluster_id)
        };

        // Load segments, NOF segments, objects, and tasks from the backend.
        // 从后端加载 segments、NOF segments、objects 和 tasks。
        let backend = StorageBackend::new(self.backend_type, &dir);
        let Some((segments, nof_segments, objects, tasks)) = backend
            .load()
            .map_err(|error| HaError::Snapshot(error.to_string()))?
        else {
            return Ok(None);
        };

        // Derive snapshot_id from the file modification time if available.
        // 如果可用，从文件修改时间推导 snapshot_id。
        let snapshot_path = dir.join("master_snapshot.json");
        let snapshot_id = std::fs::metadata(&snapshot_path)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
            .map(|ts| format!("snapshot-{}", ts.as_millis()))
            .unwrap_or_else(|| "snapshot-latest".to_string());

        Ok(Some(LoadedSnapshot {
            snapshot_id,
            snapshot_sequence_id: 0,
            // Extract the Segment domain object from each SegmentEntry wrapper.
            // 从每个 SegmentEntry 封装中提取 Segment 领域对象。
            segments: segments
                .into_iter()
                .map(|s: crate::service::SegmentEntry| s.segment)
                .collect(),
            nof_segments,
            objects,
            tasks,
        }))
    }
}

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

// ----------------------------------------------------------------------------
// MasterServiceSupervisor — top-level HA coordinator
// MasterServiceSupervisor —— 顶层 HA 协调器
//
// Coordinates leader election and hot standby switching. Manages the
// `runtime_state` field used by gRPC handlers to decide whether to accept
// client requests (only in Serving/LeaderWarmup states).
//
// 协调 Leader 选举和热备切换。管理 runtime_state 字段，gRPC 处理器
// 使用该字段决定是否接受客户端请求（仅在 Serving/LeaderWarmup 状态接受）。
//
// C++ equivalent: MasterServiceSupervisor in ha_service.h
// ----------------------------------------------------------------------------

pub struct MasterServiceSupervisor {
    /// Current runtime state, shared with gRPC handlers via Arc.
    /// 当前运行时状态，通过 Arc 与 gRPC 处理器共享。
    runtime_state: Arc<Mutex<MasterRuntimeState>>,
    /// Currently observed leader, if any. / 当前观察到的 leader（如果有）。
    observed_leader: Arc<Mutex<Option<MasterView>>>,
    /// The underlying standby controller (Noop or CapabilityDriven).
    /// 底层 standby 控制器（Noop 或 CapabilityDriven）。
    standby_controller: Box<dyn StandbyController>,
}

impl MasterServiceSupervisor {
    /// Create a new supervisor with a given standby controller.
    /// The supervisor registers a runtime state callback that updates
    /// `runtime_state` atomically whenever the standby controller changes state.
    ///
    /// 用给定的 standby 控制器创建新的 supervisor。
    /// supervisor 注册一个运行时状态回调，当 standby 控制器状态变化时
    /// 原子性地更新 runtime_state。
    pub fn new(mut standby_controller: Box<dyn StandbyController>) -> Self {
        let runtime_state = Arc::new(Mutex::new(MasterRuntimeState::Starting));
        let runtime_state_for_callback = runtime_state.clone();
        standby_controller.set_runtime_state_callback(Some(Arc::new(move |state| {
            *runtime_state_for_callback
                .lock()
                .expect("supervisor runtime state mutex poisoned") = state;
        })));

        Self {
            runtime_state,
            observed_leader: Arc::new(Mutex::new(None)),
            standby_controller,
        }
    }

    /// Enter standby mode, optionally observing a known leader.
    /// 进入 standby 模式，可选择观察已知的 leader。
    pub fn enter_standby_mode(
        &mut self,
        observed_leader: Option<MasterView>,
    ) -> Result<(), HaError> {
        *self
            .observed_leader
            .lock()
            .expect("supervisor observed leader mutex poisoned") = observed_leader.clone();
        self.standby_controller.start_standby(observed_leader)
    }

    /// Transition to Candidate state (competing in election).
    /// 转换为 Candidate 状态（参与选举竞争）。
    pub fn begin_candidacy(&self) {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::Candidate;
    }

    /// Promote to LeaderWarmup: the standby has been selected as leader but
    /// is still finalising state before serving.
    ///
    /// 提升为 LeaderWarmup：standby 已被选为 leader 但仍在为服务准备最终状态。
    pub fn promote_to_leader_warmup(&mut self) -> Result<(), HaError> {
        self.standby_controller.promote_standby()?;
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::LeaderWarmup;
        Ok(())
    }

    /// Activate the Serving state: the master is now handling client traffic.
    /// 激活 Serving 状态：master 现在处理客户端流量。
    pub fn activate_serving_state(&self) {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::Serving;
    }

    /// Read the current runtime state. / 读取当前运行时状态。
    pub fn runtime_state(&self) -> MasterRuntimeState {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned")
    }

    /// Read the currently observed leader. / 读取当前观察到的 leader。
    pub fn observed_leader(&self) -> Option<MasterView> {
        self.observed_leader
            .lock()
            .expect("supervisor observed leader mutex poisoned")
            .clone()
    }
}

// ----------------------------------------------------------------------------
// OpLog — operation log for replay / 操作日志 —— 用于重放
//
// Each leader produces OpLog records that standbys consume to stay in sync.
// The oplog can be backed by etcd's sequential key model, Redis, or a local
// file. Standbys replay oplog entries after loading a recent snapshot.
//
// 每个 leader 产生 OpLog 记录，standby 消费这些记录以保持同步。
// Oplog 可由 etcd 的有序 key 模型、Redis 或本地文件支持。
// Standby 在加载最近的快照后重放 oplog 条目。
// ----------------------------------------------------------------------------

/// A single oplog entry. / 单个 oplog 条目。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpLogRecord {
    /// Monotonic sequence number. / 单调递增的序列号。
    pub seq: u64,
    /// View version of the leader that produced this entry.
    /// 产生此条目的 leader 的视图版本。
    pub producer_view_version: u64,
    /// Serialised operation payload (JSON). / 序列化的操作负载（JSON）。
    pub payload: String,
}

/// Result of polling the oplog for new entries.
/// 轮询 oplog 获取新条目的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpLogPollResult {
    pub records: Vec<OpLogRecord>,
    /// Next sequence number to poll from. / 下次轮询的起始序列号。
    pub next_seq: u64,
    /// Whether the poll timed out without finding any new records.
    /// 轮询是否超时且未找到任何新记录。
    pub timed_out: bool,
}

// ----------------------------------------------------------------------------
// AcquireLeadershipResult — outcome of a leadership acquisition attempt
// AcquireLeadershipResult —— 尝试获取 Leader 权的结果
// ----------------------------------------------------------------------------

/// Result of an attempt to acquire leadership.
/// 尝试获取 Leader 权的结果。
#[derive(Debug, Clone)]
pub struct AcquireLeadershipResult {
    /// Whether leadership was successfully acquired. / 是否成功获取 leadership。
    pub acquired: bool,
    /// The current master view after the attempt. / 尝试后的当前 master 视图。
    pub view: Option<MasterView>,
    /// The lease ID if acquired (etcd: lease ID, Redis: TTL in seconds).
    /// 如果获取成功，租约 ID（etcd: lease ID, Redis: TTL 秒数）。
    pub lease_id: Option<i64>,
}

// ----------------------------------------------------------------------------
// LeadershipHandle — RAII handle for leader keepalive
// LeadershipHandle —— Leader 续约的 RAII 句柄
//
/// Leader keepalive handle: on drop, automatically cancels the background
/// renewal task, preventing resource leaks.
///
/// 领导者 keepalive 句柄：Drop 时自动取消后台续约任务，防止资源泄漏。
pub struct LeadershipHandle {
    /// Oneshot sender to cancel the keepalive task. None after cancellation.
    /// 用于取消续约任务的 oneshot sender。取消后为 None。
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for LeadershipHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ----------------------------------------------------------------------------
// LeaderCoordinator — leader election and keepalive using various backends
// LeaderCoordinator —— 使用各种后端的 Leader 选举和续约
//
// This is the core election infrastructure. It abstracts over Etcd, Redis,
// K8s, and Manual backends, providing a uniform interface for:
//   - Reading the current leader view
//   - Acquiring leadership via the backend's election protocol
//   - Maintaining leadership with periodic keepalive
//   - Releasing leadership cleanly
//   - Watching for leadership changes via watch channel
//
// 这是核心选举基础设施。它抽象了 Etcd、Redis、K8s 和 Manual 后端，
// 提供统一接口用于：读取当前 leader 视图、通过后端选举协议获取 leadership、
// 定期续约维持 leadership、干净地释放 leadership、通过 watch channel 监视角色变更。
//
// C++ equivalent: LeaderCoordinator in ha_service.h
// ----------------------------------------------------------------------------

pub struct LeaderCoordinator {
    /// The backend performing the actual election. / 执行实际选举的后端。
    backend: CoordinatorBackend,
    /// Sender side of the role change watch channel. / 角色变更 watch channel 的发送端。
    role_tx: watch::Sender<LeaderRole>,
    /// Receiver side of the role change watch channel. / 角色变更 watch channel 的接收端。
    role_rx: watch::Receiver<LeaderRole>,
}

/// Supported coordinator backends. / 支持的协调器后端。
enum CoordinatorBackend {
    Etcd {
        client: etcd_client::Client,
        /// Key path used for the etcd election campaign.
        /// 用于 etcd election campaign 的 key 路径。
        election_key: String,
    },
    Redis {
        client: redis::Client,
        /// Key used for SET NX PX. / 用于 SET NX PX 的 key。
        election_key: String,
    },
    K8s {
        /// Kubernetes namespace. / Kubernetes 命名空间。
        namespace: String,
        /// Kubernetes Lease resource name. / Kubernetes Lease 资源名称。
        lease_name: String,
    },
    /// Manual mode: leadership is externally controlled via the watch channel.
    /// 手动模式：leadership 通过 watch channel 外部控制。
    Manual,
}

impl LeaderCoordinator {
    /// Shared constructor for all backends. / 所有后端的共享构造函数。
    fn with_backend(backend: CoordinatorBackend, initial_role: LeaderRole) -> Self {
        let (role_tx, role_rx) = watch::channel(initial_role);
        Self {
            backend,
            role_tx,
            role_rx,
        }
    }

    /// Create an Etcd-backed coordinator. / 创建 Etcd 支持的协调器。
    pub async fn new_etcd(endpoints: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let client = etcd_client::Client::connect(endpoints, None).await?;
        Ok(Self::with_backend(
            CoordinatorBackend::Etcd {
                client,
                election_key: "/mooncake/master/leader".to_string(),
            },
            LeaderRole::Standby,
        ))
    }

    /// Create a Redis-backed coordinator. / 创建 Redis 支持的协调器。
    pub async fn new_redis(connstring: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = redis::Client::open(connstring)?;
        Ok(Self::with_backend(
            CoordinatorBackend::Redis {
                client,
                election_key: "mooncake:master:leader".to_string(),
            },
            LeaderRole::Standby,
        ))
    }

    /// Create a K8s Lease-backed coordinator. / 创建 K8s Lease 支持的协调器。
    pub async fn new_k8s(
        namespace: &str,
        lease_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::with_backend(
            CoordinatorBackend::K8s {
                namespace: namespace.to_string(),
                lease_name: lease_name.to_string(),
            },
            LeaderRole::Standby,
        ))
    }

    /// Create a Manual-mode coordinator with external role control.
    /// Returns both the coordinator and a Sender for external role manipulation.
    ///
    /// 创建手动模式的协调器，支持外部角色控制。
    /// 返回协调器和用于外部角色操作的 Sender。
    pub fn new_manual(initial_role: LeaderRole) -> (Self, watch::Sender<LeaderRole>) {
        let (role_tx, role_rx) = watch::channel(initial_role);
        (
            Self {
                backend: CoordinatorBackend::Manual,
                role_tx: role_tx.clone(),
                role_rx,
            },
            role_tx,
        )
    }

    /// Read the current leader view from the backend.
    ///
    /// 从后端读取当前 Leader 视图。
    /// - Etcd: uses the election leader API / 使用 election leader API
    /// - Redis: GET the election key / GET 选举 key
    /// - K8s/Manual: always returns None / 始终返回 None
    pub async fn read_current_view(&self) -> Result<Option<MasterView>, HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd {
                client,
                election_key,
            } => {
                let mut client = client.clone();
                match client.leader(election_key.clone()).await {
                    Ok(resp) => match resp.kv() {
                        Some(kv) => {
                            let addr = kv.value_str().unwrap_or("").to_string();
                            Ok(Some(MasterView {
                                leader_address: addr,
                                view_version: kv.version() as u64,
                            }))
                        }
                        None => Ok(None),
                    },
                    Err(_) => Ok(None),
                }
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let result: Option<String> = redis::cmd("GET")
                    .arg(election_key)
                    .query_async(&mut conn)
                    .await
                    .ok();
                if let Some(ref v) = result {
                    // Value format: "address|version" / 值格式："address|version"
                    let parts: Vec<&str> = v.splitn(2, '|').collect();
                    return Ok(Some(MasterView {
                        leader_address: parts.first().copied().unwrap_or("").to_string(),
                        view_version: parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Try to acquire leadership via the backend.
    ///
    /// 尝试通过后端获取 Leader 权。
    /// - Etcd: grant lease + campaign. / 授予租约 + campaign。
    /// - Redis: SET NX PX (atomic compare-and-set with TTL).
    ///   SET NX PX（带 TTL 的原子比较并设置）。
    /// - Manual: always succeeds. / 始终成功。
    /// - K8s: not yet implemented. / 尚未实现。
    pub async fn try_acquire_leadership(
        &self,
        leader_address: &str,
        lease_ttl_secs: i64,
    ) -> Result<AcquireLeadershipResult, HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd {
                client,
                election_key,
            } => {
                let mut client = client.clone();
                let current = self.read_current_view().await?;

                // Step 1: Grant a TTL lease. / 第 1 步：授予 TTL 租约。
                let lease_resp = client
                    .lease_grant(lease_ttl_secs, None)
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("etcd lease grant error: {e}")))?;
                let lease_id = lease_resp.id();

                // Step 2: Campaign for leadership. / 第 2 步：竞选 leadership。
                let name = election_key.clone();
                let value = leader_address.to_string();
                match client.campaign(name, value, lease_id).await {
                    Ok(resp) => {
                        let acquired = resp
                            .leader()
                            .and_then(|l| l.name_str().ok())
                            .map(|n| n == leader_address)
                            .unwrap_or(false);
                        if acquired {
                            let _ = self.role_tx.send(LeaderRole::Leader);
                            info!(
                                "Leadership acquired: address={}, lease_id={}",
                                leader_address, lease_id
                            );
                            Ok(AcquireLeadershipResult {
                                acquired: true,
                                view: Some(MasterView {
                                    leader_address: leader_address.to_string(),
                                    view_version: 1,
                                }),
                                lease_id: Some(lease_id),
                            })
                        } else {
                            Ok(AcquireLeadershipResult {
                                acquired: false,
                                view: current,
                                lease_id: None,
                            })
                        }
                    }
                    Err(e) => {
                        warn!("etcd campaign failed: {}", e);
                        Ok(AcquireLeadershipResult {
                            acquired: false,
                            view: current,
                            lease_id: None,
                        })
                    }
                }
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let ttl_ms = (lease_ttl_secs * 1000) as usize;
                // Redis value format: "leader_address|view_version"
                // Redis 值格式："leader_address|view_version"
                let value = format!("{}|{}", leader_address, "1");
                let result: Option<String> = redis::cmd("SET")
                    .arg(election_key)
                    .arg(&value)
                    .arg("NX") // only if not exists / 仅当不存在时
                    .arg("PX") // TTL in milliseconds / TTL 以毫秒计
                    .arg(ttl_ms)
                    .query_async(&mut conn)
                    .await
                    .ok();
                if result.as_deref() == Some("OK") {
                    let _ = self.role_tx.send(LeaderRole::Leader);
                    Ok(AcquireLeadershipResult {
                        acquired: true,
                        view: Some(MasterView {
                            leader_address: leader_address.to_string(),
                            view_version: 1,
                        }),
                        lease_id: Some(lease_ttl_secs), // TTL seconds stored as lease_id
                    })
                } else {
                    Ok(AcquireLeadershipResult {
                        acquired: false,
                        view: self.read_current_view().await?,
                        lease_id: None,
                    })
                }
            }
            CoordinatorBackend::Manual => {
                // Manual mode: instant leadership. / 手动模式：立即成为 leader。
                let _ = self.role_tx.send(LeaderRole::Leader);
                Ok(AcquireLeadershipResult {
                    acquired: true,
                    view: Some(MasterView {
                        leader_address: leader_address.to_string(),
                        view_version: 1,
                    }),
                    lease_id: None,
                })
            }
            _ => Err(HaError::InvalidBackend(
                "backend does not support leadership acquisition".into(),
            )),
        }
    }

    /// Start a background keepalive task. On failure, the leader is demoted
    /// to Standby via the watch channel.
    ///
    /// 启动 Leader 续约后台任务。
    /// - Etcd: lease_keep_alive stream with 3s interval.
    ///   lease_keep_alive 流，3s 间隔。
    /// - Redis: PEXPIRE with 3s interval. / PEXPIRE，3s 间隔。
    /// - 续约失败时自动降级为 Standby，通过 role_tx channel 通知。
    pub async fn start_leadership_keepalive(
        &self,
        lease_id: i64,
    ) -> Result<LeadershipHandle, HaError> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let mut client = client.clone();
                let role_tx = self.role_tx.clone();
                tokio::spawn(async move {
                    // Create the keepalive stream from the lease.
                    // 从租约创建 keepalive 流。
                    let (mut keeper, _stream) = match client.lease_keep_alive(lease_id).await {
                        Ok(res) => res,
                        Err(e) => {
                            error!("Failed to create lease keepalive: {}", e);
                            let _ = role_tx.send(LeaderRole::Standby);
                            return;
                        }
                    };
                    loop {
                        tokio::select! {
                            // Renew every 3 seconds. / 每 3 秒续约一次。
                            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                                if let Err(e) = keeper.keep_alive().await {
                                    error!("Lease keepalive error: {}, leadership lost", e);
                                    let _ = role_tx.send(LeaderRole::Standby);
                                    return;
                                }
                            }
                            _ = &mut cancel_rx => {
                                info!("Leadership keepalive cancelled");
                                return;
                            }
                        }
                    }
                });
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let _conn = match client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Redis keepalive connection failed: {}", e);
                        let _ = self.role_tx.send(LeaderRole::Standby);
                        return Ok(LeadershipHandle {
                            cancel_tx: Some(cancel_tx),
                        });
                    }
                };
                let lease_ms = lease_id * 1000;
                let role_tx = self.role_tx.clone();
                let ek = election_key.clone();
                let client2 = client.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            // Renew every 3 seconds via PEXPIRE. / 每 3 秒通过 PEXPIRE 续约。
                            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                                if let Ok(mut c) = client2.get_multiplexed_async_connection().await {
                                    let result: Result<(), _> = redis::cmd("PEXPIRE")
                                        .arg(&ek)
                                        .arg(lease_ms)
                                        .query_async(&mut c)
                                        .await;
                                    if result.is_err() {
                                        let _ = role_tx.send(LeaderRole::Standby);
                                        return;
                                    }
                                }
                            }
                            _ = &mut cancel_rx => {
                                info!("Redis leadership keepalive cancelled");
                                return;
                            }
                        }
                    }
                });
            }
            // No keepalive needed for K8s (Lease handles it) or Manual.
            // K8s（Lease 自行处理）或 Manual 无需续约。
            _ => {}
        }
        Ok(LeadershipHandle {
            cancel_tx: Some(cancel_tx),
        })
    }

    /// Release leadership gracefully. / 优雅释放 Leader 权。
    /// - Etcd: calls resign. / 调用 resign。
    /// - Redis: DEL the election key. / DEL 选举 key。
    /// - Manual: sends Standby via watch channel. / 通过 watch channel 发送 Standby。
    pub async fn release_leadership(&self, _lease_id: i64) -> Result<(), HaError> {
        match &self.backend {
            CoordinatorBackend::Etcd { client, .. } => {
                let mut client = client.clone();
                client
                    .resign(None)
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("etcd resign error: {e}")))?;
                let _ = self.role_tx.send(LeaderRole::Standby);
                info!("Leadership released via resign");
                Ok(())
            }
            CoordinatorBackend::Redis {
                client,
                election_key,
            } => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| HaError::InvalidBackend(format!("redis connect: {e}")))?;
                let _: Result<(), _> = redis::cmd("DEL")
                    .arg(election_key)
                    .query_async(&mut conn)
                    .await;
                let _ = self.role_tx.send(LeaderRole::Standby);
                info!("Redis leadership released");
                Ok(())
            }
            CoordinatorBackend::Manual => {
                let _ = self.role_tx.send(LeaderRole::Standby);
                Ok(())
            }
            _ => Err(HaError::InvalidBackend(
                "backend does not support leadership release".into(),
            )),
        }
    }

    /// Wait for a view change: poll every 200ms, return when `view_version`
    /// differs from `known_version`, or when the timeout expires.
    ///
    /// 等待 Leader 视图变更（直到超时），每 200ms 轮询一次，
    /// 检测到 view_version 变化即返回，超时返回 None。
    pub async fn wait_for_view_change(
        &self,
        known_version: u64,
        timeout: Duration,
    ) -> Result<Option<MasterView>, HaError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            if let Some(view) = self.read_current_view().await? {
                if view.view_version != known_version {
                    return Ok(Some(view));
                }
            }
            // 200ms poll interval / 200ms 轮询间隔
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait for a role assignment from the backend. Returns the current role.
    /// 等待后端分配角色。返回当前角色。
    pub async fn wait_for_role(&self) -> Result<LeaderRole, Box<dyn std::error::Error>> {
        match &self.backend {
            CoordinatorBackend::Etcd { .. } => {
                info!("Etcd leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::K8s {
                namespace,
                lease_name,
            } => {
                info!(
                    "K8s Lease election initialized: namespace={}, lease={}",
                    namespace, lease_name
                );
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::Redis { .. } => {
                info!("Redis leader election initialized");
                Ok(*self.role_rx.borrow())
            }
            CoordinatorBackend::Manual => Ok(*self.role_rx.borrow()),
        }
    }

    /// Block until leadership changes, returning only when this node becomes
    /// the Leader. Uses the watch channel rather than polling.
    ///
    /// 阻塞等待 Leader 角色变更（通过 watch channel），仅在成为 Leader 时返回。
    /// 使用 watch channel 而非轮询。
    pub async fn watch_leadership_change(&self) {
        // If already leader, return immediately. / 如果已是 leader，立即返回。
        if *self.role_rx.borrow() == LeaderRole::Leader {
            return;
        }
        let mut role_rx = self.role_rx.clone();
        loop {
            if role_rx.changed().await.is_err() {
                return;
            }
            if *role_rx.borrow() == LeaderRole::Leader {
                info!("Leadership changed: this instance became leader");
                return;
            }
        }
    }

    /// Test-only: inject a role change via the watch channel.
    /// 仅限测试：通过 watch channel 注入角色变更。
    pub fn set_role_for_test(&self, role: LeaderRole) {
        let _ = self.role_tx.send(role);
    }
}

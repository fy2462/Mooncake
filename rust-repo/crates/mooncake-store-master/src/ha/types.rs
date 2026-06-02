use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

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

impl HaError {
    /// Returns true for errors that should cause the HA loop to exit immediately.
    /// C++ equivalent: `IsFatalHABackendError` in master_service_supervisor.cpp.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            HaError::InvalidBackend(_) | HaError::UnavailableInCurrentStatus
        )
    }
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

/// Events that drive standby state transitions.
/// C++ equivalent: `StandbyEvent` in standby_state_machine.h.
///
/// 驱动 standby 状态转换的事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandbyEvent {
    Start,
    Stop,
    Promote,
    Connected,
    ConnectionFailed,
    Disconnected,
    SyncComplete,
    SyncFailed,
    WatchHealthy,
    WatchBroken,
    RecoverySuccess,
    RecoveryFailed,
    PromotionSuccess,
    PromotionFailed,
    MaxErrorsReached,
    FatalError,
}

/// Result of processing a state transition event.
/// C++ equivalent: `StateTransitionResult` in standby_state_machine.h.
///
/// 处理状态转换事件的结果。
#[derive(Debug, Clone)]
pub struct StateTransitionResult {
    pub allowed: bool,
    pub old_state: StandbyState,
    pub new_state: StandbyState,
    pub reason: String,
}

/// Callback invoked on every state change.
/// C++ equivalent: `StateChangeCallback` in standby_state_machine.h.
pub type StateChangeCallback = Arc<dyn Fn(StandbyState, StandbyState, StandbyEvent) + Send + Sync>;

/// Record of a state transition for debug history.
/// C++ equivalent: `TransitionRecord` in standby_state_machine.h.
#[derive(Debug, Clone)]
pub struct TransitionRecord {
    pub timestamp: std::time::Instant,
    pub from_state: StandbyState,
    pub to_state: StandbyState,
    pub event: StandbyEvent,
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
// OpLog — types / 操作日志类型
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

impl LeadershipHandle {
    /// Create a new LeadershipHandle wrapping the given cancel sender.
    /// 创建一个新的 LeadershipHandle，封装给定的取消 sender。
    pub(crate) fn new(cancel_tx: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            cancel_tx: Some(cancel_tx),
        }
    }
}

impl Drop for LeadershipHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
    }
}

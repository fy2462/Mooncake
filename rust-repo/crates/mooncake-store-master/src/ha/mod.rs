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

pub mod coordinator;
pub mod oplog_applier;
pub mod snapshot;
pub mod standby;
pub mod state_machine;
pub mod supervisor;
pub mod types;

// Re-export all public items to preserve the original `pub mod ha` API surface.
// 重新导出所有 public 项，以保持原始 `pub mod ha` 的 API 表面。
pub use coordinator::LeaderCoordinator;
pub use snapshot::{LoadedSnapshot, LocalSnapshotProvider, NoopSnapshotProvider, SnapshotProvider};
pub use standby::{
    build_standby_runtime_capabilities, map_standby_runtime_state,
    CapabilityDrivenStandbyController, MasterServiceSupervisorConfig, NoopStandbyController,
    StandbyController, StandbyRuntimeCapabilities,
};
pub use state_machine::StandbyStateMachine;
pub use supervisor::{LeadershipMonitorHandle, MasterServiceSupervisor};
pub use types::{
    parse_ha_backend_type, AcquireLeadershipResult, HABackendSpec, HABackendType, HaError,
    LeaderRole, LeadershipHandle, MasterRuntimeState, MasterView, OpLogPollResult, OpLogRecord,
    RuntimeStateCallback, StandbyEvent, StandbyState, StandbySyncStatus, StateTransitionResult,
};

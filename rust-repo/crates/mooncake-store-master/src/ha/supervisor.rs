use super::standby::StandbyController;
use super::types::{HaError, MasterRuntimeState, MasterView};
use std::sync::{Arc, Mutex};

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

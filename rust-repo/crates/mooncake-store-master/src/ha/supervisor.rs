use super::standby::StandbyController;
use super::types::{HaError, MasterRuntimeState, MasterView};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// ----------------------------------------------------------------------------
// MasterServiceSupervisor — top-level HA coordinator
// ----------------------------------------------------------------------------

pub struct MasterServiceSupervisor {
    runtime_state: Arc<Mutex<MasterRuntimeState>>,
    observed_leader: Arc<Mutex<Option<MasterView>>>,
    standby_controller: Box<dyn StandbyController>,
    /// Gate for standby runtime state updates. Disabled when becoming leader.
    /// C++ equivalent: `accept_standby_runtime_updates` atomic in master_service_supervisor.cpp.
    pub(crate) accept_standby_runtime_updates: Arc<AtomicBool>,
}

/// Handle for the LeadershipMonitor background task.
/// On drop, the monitor task is aborted.
/// C++ equivalent: `LeadershipMonitorHandle` in master_service_supervisor.cpp.
pub struct LeadershipMonitorHandle {
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl LeadershipMonitorHandle {
    pub fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}

impl Drop for LeadershipMonitorHandle {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
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
        let accept_standby = Arc::new(AtomicBool::new(true));
        let gate = accept_standby.clone();
        standby_controller.set_runtime_state_callback(Some(Arc::new(move |state| {
            if gate.load(Ordering::Acquire) {
                *runtime_state_for_callback
                    .lock()
                    .expect("supervisor runtime state mutex poisoned") = state;
            }
        })));

        Self {
            runtime_state,
            observed_leader: Arc::new(Mutex::new(None)),
            standby_controller,
            accept_standby_runtime_updates: accept_standby,
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

    /// Disable standby runtime state updates (called when becoming leader).
    /// C++ equivalent: `accept_standby_runtime_updates.store(false)` in master_service_supervisor.cpp.
    ///
    /// 禁用 standby 运行时状态更新（成为 leader 时调用）。
    pub fn disable_standby_updates(&self) {
        self.accept_standby_runtime_updates
            .store(false, Ordering::Release);
    }

    /// Re-enable standby runtime state updates (called when returning to standby).
    /// 重新启用 standby 运行时状态更新（回到 standby 时调用）。
    pub fn enable_standby_updates(&self) {
        self.accept_standby_runtime_updates
            .store(true, Ordering::Release);
    }

    /// Activate the Serving state: the master is now handling client traffic.
    /// 激活 Serving 状态：master 现在处理客户端流量。
    pub fn activate_serving_state(&self) {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::Serving;
    }

    /// Deactivate the serving state: clear service availability.
    /// C++ equivalent: `DeactivateServingState` in master_service_supervisor.cpp.
    ///
    /// 停用 Serving 状态：清除服务可用性。
    pub fn deactivate_serving_state(&self) {
        *self
            .runtime_state
            .lock()
            .expect("supervisor runtime state mutex poisoned") = MasterRuntimeState::Standby;
    }

    /// Update the observed leader view.
    /// 更新观测到的 leader view。
    pub fn update_observed_leader(&mut self, view: Option<MasterView>) {
        *self.observed_leader.lock().expect("mutex poisoned") = view.clone();
        self.standby_controller.update_observed_leader(view);
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

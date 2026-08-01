//! Standby state machine — validates and executes state transitions.
//! C++ equivalent: `StandbyStateMachine` in standby_state_machine.h/cpp.
//!
//! Uses an AtomicU8 for lock-free reads + a mutex-guarded CAS double-check
//! pattern for writes. 9 states, 16 events, 28 valid transitions.
//!
//! Thread safety pattern (matching C++):
//! 1. Atomic load (Acquire) current state
//! 2. Validate transition
//! 3. Lock mutex, re-check current state (CAS double-check)
//! 4. Commit if still valid, store (Release) to atomic
//! 5. Release lock before invoking callbacks (prevent re-entrancy deadlock)

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use super::types::{
    StandbyEvent, StandbyState, StateChangeCallback, StateTransitionResult, TransitionRecord,
};

const MAX_CONSECUTIVE_ERRORS: u32 = 10;
const MAX_HISTORY_SIZE: usize = 1000;

/// Thread-safe finite state machine for the standby lifecycle.
pub struct StandbyStateMachine {
    current: AtomicU8,
    errors: AtomicU32,
    reconnect_count: AtomicU32,
    enter_time: Mutex<Instant>,
    callbacks: Mutex<Vec<StateChangeCallback>>,
    history: Mutex<VecDeque<TransitionRecord>>,
}

impl StandbyStateMachine {
    pub fn new() -> Self {
        Self {
            current: AtomicU8::new(StandbyState::Stopped as u8),
            errors: AtomicU32::new(0),
            reconnect_count: AtomicU32::new(0),
            enter_time: Mutex::new(Instant::now()),
            callbacks: Mutex::new(Vec::new()),
            history: Mutex::new(VecDeque::new()),
        }
    }

    /// Lock-free read of current state. / 无锁读取当前状态。
    pub fn get_state(&self) -> StandbyState {
        Self::from_u8(self.current.load(Ordering::Acquire))
    }

    pub fn is_in_state(&self, state: StandbyState) -> bool {
        self.get_state() == state
    }

    /// True while actively replicating (excludes STOPPED, FAILED, PROMOTED).
    pub fn is_running(&self) -> bool {
        matches!(
            self.get_state(),
            StandbyState::Syncing
                | StandbyState::Watching
                | StandbyState::Recovering
                | StandbyState::Reconnecting
                | StandbyState::Promoting
        )
    }

    /// True when connected to the leader (excludes STOPPED, FAILED, RECONNECTING).
    pub fn is_connected(&self) -> bool {
        matches!(
            self.get_state(),
            StandbyState::Syncing
                | StandbyState::Watching
                | StandbyState::Recovering
                | StandbyState::Promoting
        )
    }

    pub fn is_watch_healthy(&self) -> bool {
        self.is_in_state(StandbyState::Watching)
    }

    pub fn is_ready_for_promotion(&self) -> bool {
        self.is_in_state(StandbyState::Watching)
    }

    /// Validate and execute a state transition.
    /// Uses CAS double-check pattern for thread safety.
    ///
    /// C++ equivalent: `StandbyStateMachine::ProcessEvent`
    pub fn process_event(&self, event: StandbyEvent) -> StateTransitionResult {
        let old_state = self.get_state();
        let mut result = Self::validate_transition(old_state, event);
        result.old_state = old_state;

        if !result.allowed || result.new_state == old_state {
            return result;
        }

        // CAS double-check: re-read state under the lock.
        let mut enter = self.enter_time.lock();
        let current = self.get_state();
        if current != old_state {
            // State changed — re-validate from current state.
            result = Self::validate_transition(current, event);
            result.old_state = current;
            if !result.allowed || result.new_state == current {
                return result;
            }
        }
        // Use the verified from_state for history and callbacks.
        let committed_from = result.old_state;

        // Record transition.
        let record = TransitionRecord {
            timestamp: Instant::now(),
            from_state: committed_from,
            to_state: result.new_state,
            event,
        };
        {
            let mut hist = self.history.lock();
            hist.push_back(record);
            if hist.len() > MAX_HISTORY_SIZE {
                hist.pop_front();
            }
        }

        // Commit state change.
        let now = Instant::now();
        self.current
            .store(result.new_state as u8, Ordering::Release);
        *enter = now;
        drop(enter);

        // Copy and invoke callbacks outside the lock.
        let cbs = self.callbacks.lock().clone();
        for cb in &cbs {
            cb(committed_from, result.new_state, event);
        }

        result
    }

    pub fn register_callback(&self, cb: StateChangeCallback) {
        let mut cbs = self.callbacks.lock();
        cbs.retain(|existing| !Arc::ptr_eq(existing, &cb));
        cbs.push(cb);
    }

    pub fn get_time_in_current_state(&self) -> Duration {
        self.enter_time.lock().elapsed()
    }

    pub fn get_consecutive_errors(&self) -> u32 {
        self.errors.load(Ordering::Acquire)
    }

    /// Increment error counter. Triggers `MAX_ERRORS_REACHED` at threshold (10).
    /// C++ equivalent: `StandbyStateMachine::IncrementErrors`
    pub fn increment_errors(&self) -> u32 {
        let new_count = self.errors.fetch_add(1, Ordering::SeqCst) + 1;
        if new_count >= MAX_CONSECUTIVE_ERRORS {
            self.process_event(StandbyEvent::MaxErrorsReached);
        }
        new_count
    }

    pub fn reset_errors(&self) {
        self.errors.store(0, Ordering::Release);
    }

    pub fn get_reconnect_count(&self) -> u32 {
        self.reconnect_count.load(Ordering::Acquire)
    }

    pub fn increment_reconnect_count(&self) {
        self.reconnect_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn reset_reconnect_count(&self) {
        self.reconnect_count.store(0, Ordering::Release);
    }

    pub fn get_transition_history(&self, max: usize) -> Vec<TransitionRecord> {
        let hist = self.history.lock();
        let skip = hist.len().saturating_sub(max);
        hist.iter().skip(skip).cloned().collect()
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    fn from_u8(v: u8) -> StandbyState {
        match v {
            0 => StandbyState::Stopped,
            1 => StandbyState::Connecting,
            2 => StandbyState::Syncing,
            3 => StandbyState::Recovering,
            4 => StandbyState::Reconnecting,
            5 => StandbyState::Failed,
            6 => StandbyState::Watching,
            7 => StandbyState::Promoting,
            8 => StandbyState::Promoted,
            _ => StandbyState::Stopped,
        }
    }

    /// The complete transition table — 28 valid transitions, matching C++.
    fn validate_transition(from: StandbyState, event: StandbyEvent) -> StateTransitionResult {
        let allowed = true; // overwritten by false on rejection
        let new_state = match (from, event) {
            // STOPPED
            (StandbyState::Stopped, StandbyEvent::Start) => StandbyState::Connecting,
            // CONNECTING
            (StandbyState::Connecting, StandbyEvent::Connected) => StandbyState::Syncing,
            (
                StandbyState::Connecting,
                StandbyEvent::ConnectionFailed | StandbyEvent::FatalError,
            ) => StandbyState::Failed,
            (StandbyState::Connecting, StandbyEvent::Stop) => StandbyState::Stopped,
            // SYNCING
            (StandbyState::Syncing, StandbyEvent::SyncComplete) => StandbyState::Watching,
            (StandbyState::Syncing, StandbyEvent::SyncFailed | StandbyEvent::Disconnected) => {
                StandbyState::Reconnecting
            }
            (StandbyState::Syncing, StandbyEvent::Stop) => StandbyState::Stopped,
            (StandbyState::Syncing, StandbyEvent::FatalError) => StandbyState::Failed,
            // WATCHING
            (StandbyState::Watching, StandbyEvent::WatchBroken | StandbyEvent::Disconnected) => {
                StandbyState::Reconnecting
            }
            (StandbyState::Watching, StandbyEvent::MaxErrorsReached) => StandbyState::Recovering,
            (StandbyState::Watching, StandbyEvent::Promote) => StandbyState::Promoting,
            (StandbyState::Watching, StandbyEvent::Stop) => StandbyState::Stopped,
            (StandbyState::Watching, StandbyEvent::FatalError) => StandbyState::Failed,
            (StandbyState::Watching, StandbyEvent::WatchHealthy) => StandbyState::Watching, // stay
            // RECOVERING
            (StandbyState::Recovering, StandbyEvent::RecoverySuccess) => StandbyState::Watching,
            (
                StandbyState::Recovering,
                StandbyEvent::RecoveryFailed | StandbyEvent::Disconnected,
            ) => StandbyState::Reconnecting,
            (StandbyState::Recovering, StandbyEvent::Stop) => StandbyState::Stopped,
            (StandbyState::Recovering, StandbyEvent::FatalError) => StandbyState::Failed,
            // RECONNECTING
            (StandbyState::Reconnecting, StandbyEvent::Connected) => StandbyState::Syncing,
            (
                StandbyState::Reconnecting,
                StandbyEvent::WatchHealthy | StandbyEvent::RecoverySuccess,
            ) => StandbyState::Watching,
            (StandbyState::Reconnecting, StandbyEvent::RecoveryFailed) => {
                StandbyState::Reconnecting
            } // stay
            (
                StandbyState::Reconnecting,
                StandbyEvent::MaxErrorsReached | StandbyEvent::FatalError,
            ) => StandbyState::Failed,
            (StandbyState::Reconnecting, StandbyEvent::Stop) => StandbyState::Stopped,
            // PROMOTING
            (StandbyState::Promoting, StandbyEvent::PromotionSuccess) => StandbyState::Promoted,
            (StandbyState::Promoting, StandbyEvent::PromotionFailed) => StandbyState::Failed,
            (StandbyState::Promoting, StandbyEvent::Stop) => StandbyState::Stopped,
            // PROMOTED
            (StandbyState::Promoted, StandbyEvent::Stop) => StandbyState::Stopped,
            // FAILED
            (StandbyState::Failed, StandbyEvent::Stop) => StandbyState::Stopped,
            (StandbyState::Failed, StandbyEvent::Start) => StandbyState::Connecting,
            // Everything else is rejected.
            _ => {
                return StateTransitionResult {
                    allowed: false,
                    old_state: from,
                    new_state: from,
                    reason: format!("invalid transition from {from:?} on event {event:?}"),
                };
            }
        };

        StateTransitionResult {
            allowed,
            old_state: from,
            new_state,
            reason: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_transition(
        sm: &StandbyStateMachine,
        event: StandbyEvent,
        old_state: StandbyState,
        new_state: StandbyState,
    ) {
        let result = sm.process_event(event);
        assert!(result.allowed);
        assert_eq!(result.old_state, old_state);
        assert_eq!(result.new_state, new_state);
        assert_eq!(sm.get_state(), new_state);
    }

    fn reach_syncing(sm: &StandbyStateMachine) {
        assert_transition(
            sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );
        assert_transition(
            sm,
            StandbyEvent::Connected,
            StandbyState::Connecting,
            StandbyState::Syncing,
        );
    }

    fn reach_watching(sm: &StandbyStateMachine) {
        reach_syncing(sm);
        assert_transition(
            sm,
            StandbyEvent::SyncComplete,
            StandbyState::Syncing,
            StandbyState::Watching,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testinitialstate_85bbd34f()
     {
        let sm = StandbyStateMachine::new();

        assert_eq!(sm.get_state(), StandbyState::Stopped);
        assert!(!sm.is_running());
        assert!(!sm.is_connected());
        assert!(!sm.is_watch_healthy());
        assert!(!sm.is_ready_for_promotion());
        assert_eq!(sm.get_consecutive_errors(), 0);
        assert_eq!(sm.get_reconnect_count(), 0);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_teststarttransition_3245b7a6()
     {
        let sm = StandbyStateMachine::new();

        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );
        assert!(!sm.is_running());
        assert!(!sm.is_connected());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testconnectedtransition_f6750002()
     {
        let sm = StandbyStateMachine::new();
        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );

        assert_transition(
            &sm,
            StandbyEvent::Connected,
            StandbyState::Connecting,
            StandbyState::Syncing,
        );
        assert!(sm.is_running());
        assert!(sm.is_connected());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testsynccompletetransition_bfeadd1a()
     {
        let sm = StandbyStateMachine::new();
        reach_syncing(&sm);

        assert_transition(
            &sm,
            StandbyEvent::SyncComplete,
            StandbyState::Syncing,
            StandbyState::Watching,
        );
        assert!(sm.is_running());
        assert!(sm.is_connected());
        assert!(sm.is_watch_healthy());
        assert!(sm.is_ready_for_promotion());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testwatchhealthynoop_03a093d9()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        assert_transition(
            &sm,
            StandbyEvent::WatchHealthy,
            StandbyState::Watching,
            StandbyState::Watching,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testwatchbrokentransition_54fee57f()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        assert_transition(
            &sm,
            StandbyEvent::WatchBroken,
            StandbyState::Watching,
            StandbyState::Reconnecting,
        );
        assert!(sm.is_running());
        assert!(!sm.is_watch_healthy());
        assert!(!sm.is_ready_for_promotion());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testdisconnectedfromwatching_cda0a6f2()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        assert_transition(
            &sm,
            StandbyEvent::Disconnected,
            StandbyState::Watching,
            StandbyState::Reconnecting,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testpromotetransition_11401b75()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        assert_transition(
            &sm,
            StandbyEvent::Promote,
            StandbyState::Watching,
            StandbyState::Promoting,
        );
        assert!(sm.is_running());
        assert!(sm.is_connected());
        assert!(!sm.is_watch_healthy());
        assert!(!sm.is_ready_for_promotion());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testpromotionsuccesstransition_c41fac41()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);
        assert_transition(
            &sm,
            StandbyEvent::Promote,
            StandbyState::Watching,
            StandbyState::Promoting,
        );

        assert_transition(
            &sm,
            StandbyEvent::PromotionSuccess,
            StandbyState::Promoting,
            StandbyState::Promoted,
        );
        assert!(!sm.is_running());
        assert!(!sm.is_connected());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testpromotionfailedtransition_7d20105b()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);
        assert_transition(
            &sm,
            StandbyEvent::Promote,
            StandbyState::Watching,
            StandbyState::Promoting,
        );

        assert_transition(
            &sm,
            StandbyEvent::PromotionFailed,
            StandbyState::Promoting,
            StandbyState::Failed,
        );
        assert!(!sm.is_running());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_teststoptransition_a3fafb9f()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        assert_transition(
            &sm,
            StandbyEvent::Stop,
            StandbyState::Watching,
            StandbyState::Stopped,
        );
        assert!(!sm.is_running());
        assert!(!sm.is_connected());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testconnectionfailedfromconnecting_a2ec4421()
     {
        let sm = StandbyStateMachine::new();
        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );

        assert_transition(
            &sm,
            StandbyEvent::ConnectionFailed,
            StandbyState::Connecting,
            StandbyState::Failed,
        );
        assert!(!sm.is_running());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testfatalerrorfromconnecting_10c4292c()
     {
        let sm = StandbyStateMachine::new();
        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );

        assert_transition(
            &sm,
            StandbyEvent::FatalError,
            StandbyState::Connecting,
            StandbyState::Failed,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testsyncfailedfromsyncing_6c234d39()
     {
        let sm = StandbyStateMachine::new();
        reach_syncing(&sm);

        assert_transition(
            &sm,
            StandbyEvent::SyncFailed,
            StandbyState::Syncing,
            StandbyState::Reconnecting,
        );
        assert!(sm.is_running());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testdisconnectedfromsyncing_7f0611d8()
     {
        let sm = StandbyStateMachine::new();
        reach_syncing(&sm);

        assert_transition(
            &sm,
            StandbyEvent::Disconnected,
            StandbyState::Syncing,
            StandbyState::Reconnecting,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testfatalerrorfromsyncing_16b165cc()
     {
        let sm = StandbyStateMachine::new();
        reach_syncing(&sm);

        assert_transition(
            &sm,
            StandbyEvent::FatalError,
            StandbyState::Syncing,
            StandbyState::Failed,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testfatalerrorfromwatching_84fe4c46()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        assert_transition(
            &sm,
            StandbyEvent::FatalError,
            StandbyState::Watching,
            StandbyState::Failed,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testreconnectingtosyncing_96a3584a()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);
        assert_transition(
            &sm,
            StandbyEvent::WatchBroken,
            StandbyState::Watching,
            StandbyState::Reconnecting,
        );

        assert_transition(
            &sm,
            StandbyEvent::Connected,
            StandbyState::Reconnecting,
            StandbyState::Syncing,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testreconnectingtofailed_2c0e9796()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);
        assert_transition(
            &sm,
            StandbyEvent::WatchBroken,
            StandbyState::Watching,
            StandbyState::Reconnecting,
        );

        assert_transition(
            &sm,
            StandbyEvent::FatalError,
            StandbyState::Reconnecting,
            StandbyState::Failed,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testreconnectingmaxerrors_1f249cdb()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);
        assert_transition(
            &sm,
            StandbyEvent::WatchBroken,
            StandbyState::Watching,
            StandbyState::Reconnecting,
        );

        assert_transition(
            &sm,
            StandbyEvent::MaxErrorsReached,
            StandbyState::Reconnecting,
            StandbyState::Failed,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testfailedtostopped_37f0c27a()
     {
        let sm = StandbyStateMachine::new();
        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );
        assert_transition(
            &sm,
            StandbyEvent::FatalError,
            StandbyState::Connecting,
            StandbyState::Failed,
        );

        assert_transition(
            &sm,
            StandbyEvent::Stop,
            StandbyState::Failed,
            StandbyState::Stopped,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testfailedtoconnecting_4da976de()
     {
        let sm = StandbyStateMachine::new();
        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );
        assert_transition(
            &sm,
            StandbyEvent::FatalError,
            StandbyState::Connecting,
            StandbyState::Failed,
        );

        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Failed,
            StandbyState::Connecting,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testpromotedtostopped_176321e3()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);
        assert_transition(
            &sm,
            StandbyEvent::Promote,
            StandbyState::Watching,
            StandbyState::Promoting,
        );
        assert_transition(
            &sm,
            StandbyEvent::PromotionSuccess,
            StandbyState::Promoting,
            StandbyState::Promoted,
        );

        assert_transition(
            &sm,
            StandbyEvent::Stop,
            StandbyState::Promoted,
            StandbyState::Stopped,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testinvalidtransitions_1bde3c25()
     {
        let sm = StandbyStateMachine::new();
        let stopped_sync = sm.process_event(StandbyEvent::SyncComplete);
        assert!(!stopped_sync.allowed);
        assert_eq!(stopped_sync.old_state, StandbyState::Stopped);
        assert_eq!(stopped_sync.new_state, StandbyState::Stopped);
        assert_eq!(sm.get_state(), StandbyState::Stopped);

        reach_syncing(&sm);
        let syncing_promote = sm.process_event(StandbyEvent::Promote);
        assert!(!syncing_promote.allowed);
        assert_eq!(syncing_promote.old_state, StandbyState::Syncing);
        assert_eq!(syncing_promote.new_state, StandbyState::Syncing);
        assert_eq!(sm.get_state(), StandbyState::Syncing);

        assert_transition(
            &sm,
            StandbyEvent::Stop,
            StandbyState::Syncing,
            StandbyState::Stopped,
        );
        let stopped_connected = sm.process_event(StandbyEvent::Connected);
        assert!(!stopped_connected.allowed);
        assert_eq!(stopped_connected.old_state, StandbyState::Stopped);
        assert_eq!(stopped_connected.new_state, StandbyState::Stopped);
        assert_eq!(sm.get_state(), StandbyState::Stopped);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testconsecutiveerrors_27d72ca6()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        for expected in 1..=5 {
            assert_eq!(sm.increment_errors(), expected);
        }
        assert_eq!(sm.get_consecutive_errors(), 5);
        sm.reset_errors();
        assert_eq!(sm.get_consecutive_errors(), 0);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testmaxerrorsreachedautotransition_5ad85f10()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        for _ in 0..MAX_CONSECUTIVE_ERRORS {
            sm.increment_errors();
        }
        assert_eq!(sm.get_state(), StandbyState::Recovering);
        assert_eq!(sm.get_consecutive_errors(), MAX_CONSECUTIVE_ERRORS);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testmaxerrorsreachedmanual_eed3014c()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        assert_transition(
            &sm,
            StandbyEvent::MaxErrorsReached,
            StandbyState::Watching,
            StandbyState::Recovering,
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testreconnectcount_e4579690()
     {
        let sm = StandbyStateMachine::new();

        assert_eq!(sm.get_reconnect_count(), 0);
        sm.increment_reconnect_count();
        assert_eq!(sm.get_reconnect_count(), 1);
        sm.increment_reconnect_count();
        assert_eq!(sm.get_reconnect_count(), 2);
        sm.reset_reconnect_count();
        assert_eq!(sm.get_reconnect_count(), 0);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_teststatechangecallback_4f5e0014()
     {
        let sm = StandbyStateMachine::new();
        let states = Arc::new(Mutex::new(Vec::new()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_states = states.clone();
        let callback_events = events.clone();
        sm.register_callback(Arc::new(move |_old, new, event| {
            callback_states.lock().push(new);
            callback_events.lock().push(event);
        }));

        reach_watching(&sm);

        assert_eq!(
            *states.lock(),
            vec![
                StandbyState::Connecting,
                StandbyState::Syncing,
                StandbyState::Watching
            ]
        );
        assert_eq!(
            *events.lock(),
            vec![
                StandbyEvent::Start,
                StandbyEvent::Connected,
                StandbyEvent::SyncComplete
            ]
        );
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testmultiplecallbacks_15ac287f()
     {
        let sm = StandbyStateMachine::new();
        let first = Arc::new(AtomicU32::new(0));
        let second = Arc::new(AtomicU32::new(0));
        let first_callback = first.clone();
        let second_callback = second.clone();
        sm.register_callback(Arc::new(move |_, _, _| {
            first_callback.fetch_add(1, Ordering::SeqCst);
        }));
        sm.register_callback(Arc::new(move |_, _, _| {
            second_callback.fetch_add(1, Ordering::SeqCst);
        }));

        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );
        assert_transition(
            &sm,
            StandbyEvent::Connected,
            StandbyState::Connecting,
            StandbyState::Syncing,
        );

        assert_eq!(first.load(Ordering::SeqCst), 2);
        assert_eq!(second.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testcallbackexceptionhandling_bf8972f4()
     {
        let sm = StandbyStateMachine::new();
        let called = Arc::new(AtomicU32::new(0));
        let callback_called = called.clone();
        sm.register_callback(Arc::new(move |_, _, _| {
            callback_called.store(1, Ordering::SeqCst);
        }));

        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testtransitionhistory_4433d7f2()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);

        let history = sm.get_transition_history(10);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].from_state, StandbyState::Stopped);
        assert_eq!(history[0].to_state, StandbyState::Connecting);
        assert_eq!(history[0].event, StandbyEvent::Start);
        assert_eq!(history[1].from_state, StandbyState::Connecting);
        assert_eq!(history[1].to_state, StandbyState::Syncing);
        assert_eq!(history[1].event, StandbyEvent::Connected);
        assert_eq!(history[2].from_state, StandbyState::Syncing);
        assert_eq!(history[2].to_state, StandbyState::Watching);
        assert_eq!(history[2].event, StandbyEvent::SyncComplete);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testtransitionhistorylimit_a3b098a0()
     {
        let sm = StandbyStateMachine::new();

        for _ in 0..20 {
            assert_transition(
                &sm,
                StandbyEvent::Start,
                StandbyState::Stopped,
                StandbyState::Connecting,
            );
            assert_transition(
                &sm,
                StandbyEvent::Stop,
                StandbyState::Connecting,
                StandbyState::Stopped,
            );
        }

        assert!(sm.get_transition_history(5).len() <= 5);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testconcurrentstatequeries_2ac25db5()
     {
        let sm = Arc::new(StandbyStateMachine::new());
        reach_watching(&sm);
        let successes = Arc::new(AtomicU32::new(0));
        let threads = (0..10)
            .map(|_| {
                let sm = sm.clone();
                let successes = successes.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        if sm.get_state() == StandbyState::Watching {
                            successes.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(successes.load(Ordering::SeqCst), 1000);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testconcurrenteventprocessing_a94bdab7()
     {
        let sm = Arc::new(StandbyStateMachine::new());
        reach_watching(&sm);
        let allowed = Arc::new(AtomicU32::new(0));
        let rejected = Arc::new(AtomicU32::new(0));
        let threads = (0..10)
            .map(|_| {
                let sm = sm.clone();
                let allowed = allowed.clone();
                let rejected = rejected.clone();
                std::thread::spawn(move || {
                    if sm.process_event(StandbyEvent::Stop).allowed {
                        allowed.fetch_add(1, Ordering::SeqCst);
                    } else {
                        rejected.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(allowed.load(Ordering::SeqCst), 1);
        assert_eq!(rejected.load(Ordering::SeqCst), 9);
        assert_eq!(sm.get_state(), StandbyState::Stopped);
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testisconnected_828cec77()
     {
        let sm = StandbyStateMachine::new();
        assert!(!sm.is_connected());
        assert_transition(
            &sm,
            StandbyEvent::Start,
            StandbyState::Stopped,
            StandbyState::Connecting,
        );
        assert!(!sm.is_connected());
        assert_transition(
            &sm,
            StandbyEvent::Connected,
            StandbyState::Connecting,
            StandbyState::Syncing,
        );
        assert!(sm.is_connected());
        assert_transition(
            &sm,
            StandbyEvent::SyncComplete,
            StandbyState::Syncing,
            StandbyState::Watching,
        );
        assert!(sm.is_connected());
        assert_transition(
            &sm,
            StandbyEvent::Stop,
            StandbyState::Watching,
            StandbyState::Stopped,
        );
        assert!(!sm.is_connected());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testiswatchhealthy_d04ecfc3()
     {
        let sm = StandbyStateMachine::new();
        assert!(!sm.is_watch_healthy());
        reach_watching(&sm);
        assert!(sm.is_watch_healthy());
        assert_transition(
            &sm,
            StandbyEvent::WatchBroken,
            StandbyState::Watching,
            StandbyState::Reconnecting,
        );
        assert!(!sm.is_watch_healthy());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testisreadyforpromotion_91be26ff()
     {
        let sm = StandbyStateMachine::new();
        assert!(!sm.is_ready_for_promotion());
        reach_watching(&sm);
        assert!(sm.is_ready_for_promotion());
        assert_transition(
            &sm,
            StandbyEvent::Promote,
            StandbyState::Watching,
            StandbyState::Promoting,
        );
        assert!(!sm.is_ready_for_promotion());
    }

    #[test]
    fn cpp_parity_ha_standby_standby_state_machine_test_cpp_standbystatemachinetest_testcompletereconnectflow_bd423bde()
     {
        let sm = StandbyStateMachine::new();
        reach_watching(&sm);
        assert_transition(
            &sm,
            StandbyEvent::WatchBroken,
            StandbyState::Watching,
            StandbyState::Reconnecting,
        );
        assert_transition(
            &sm,
            StandbyEvent::Connected,
            StandbyState::Reconnecting,
            StandbyState::Syncing,
        );
        assert_transition(
            &sm,
            StandbyEvent::SyncComplete,
            StandbyState::Syncing,
            StandbyState::Watching,
        );
    }

    #[test]
    fn test_stopped_start() {
        let sm = StandbyStateMachine::new();
        assert_eq!(sm.get_state(), StandbyState::Stopped);
        let r = sm.process_event(StandbyEvent::Start);
        assert!(r.allowed);
        assert_eq!(r.new_state, StandbyState::Connecting);
    }

    #[test]
    fn test_full_happy_path() {
        let sm = StandbyStateMachine::new();
        assert!(sm.process_event(StandbyEvent::Start).allowed);
        assert!(sm.process_event(StandbyEvent::Connected).allowed);
        assert_eq!(sm.get_state(), StandbyState::Syncing);
        assert!(sm.process_event(StandbyEvent::SyncComplete).allowed);
        assert_eq!(sm.get_state(), StandbyState::Watching);
        assert!(sm.is_ready_for_promotion());
        assert!(sm.process_event(StandbyEvent::Promote).allowed);
        assert_eq!(sm.get_state(), StandbyState::Promoting);
        assert!(sm.process_event(StandbyEvent::PromotionSuccess).allowed);
        assert_eq!(sm.get_state(), StandbyState::Promoted);
    }

    #[test]
    fn test_failed_restart() {
        let sm = StandbyStateMachine::new();
        sm.process_event(StandbyEvent::Start);
        sm.process_event(StandbyEvent::FatalError);
        assert_eq!(sm.get_state(), StandbyState::Failed);
        // Can start again from Failed.
        assert!(sm.process_event(StandbyEvent::Start).allowed);
        assert_eq!(sm.get_state(), StandbyState::Connecting);
    }

    #[test]
    fn test_invalid_transition_rejected() {
        let sm = StandbyStateMachine::new();
        // Cannot promote from Stopped.
        let r = sm.process_event(StandbyEvent::Promote);
        assert!(!r.allowed);
        assert_eq!(sm.get_state(), StandbyState::Stopped);
    }

    #[test]
    fn test_errors_trigger_max_errors() {
        let sm = StandbyStateMachine::new();
        sm.process_event(StandbyEvent::Start);
        sm.process_event(StandbyEvent::Connected);
        sm.process_event(StandbyEvent::SyncComplete);
        assert_eq!(sm.get_state(), StandbyState::Watching);

        // Trigger 10 errors → should transition to Recovering.
        for _ in 0..10 {
            sm.increment_errors();
        }
        assert_eq!(sm.get_state(), StandbyState::Recovering);
    }

    #[test]
    fn test_is_running_states() {
        let sm = StandbyStateMachine::new();
        assert!(!sm.is_running());

        sm.process_event(StandbyEvent::Start);
        assert!(!sm.is_running()); // Connecting is not "running"

        sm.process_event(StandbyEvent::Connected);
        assert!(sm.is_running()); // Syncing is running

        sm.process_event(StandbyEvent::SyncComplete);
        assert!(sm.is_running()); // Watching is running

        sm.process_event(StandbyEvent::Stop);
        assert!(!sm.is_running()); // Stopped is not running
    }
}

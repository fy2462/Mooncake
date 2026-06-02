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
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;
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
                }
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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};

#[derive(Clone, Default)]
pub(super) struct ShutdownState {
    closed: Arc<AtomicBool>,
}

impl ShutdownState {
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(super) fn shared_flag(&self) -> Arc<AtomicBool> {
        self.closed.clone()
    }
}

#[derive(Clone, Default)]
pub(super) struct HealthState {
    last_ping_succeeded: Arc<AtomicBool>,
}

impl HealthState {
    pub(super) fn is_healthy(&self) -> bool {
        self.last_ping_succeeded.load(Ordering::SeqCst)
    }

    pub(super) fn record_success(&self) {
        self.last_ping_succeeded.store(true, Ordering::SeqCst);
    }

    pub(super) fn record_failure(&self) {
        self.last_ping_succeeded.store(false, Ordering::SeqCst);
    }

    pub(super) fn shared_flag(&self) -> Arc<AtomicBool> {
        self.last_ping_succeeded.clone()
    }
}

#[derive(Clone, Default)]
pub(super) struct RemountState {
    in_progress: Arc<AtomicBool>,
}

#[derive(Default)]
pub(super) struct LocalDiskMountState {
    mounted: AtomicBool,
    enable_offloading: AtomicBool,
}

impl LocalDiskMountState {
    pub(super) fn record_mounted(&self, enable_offloading: bool) {
        self.enable_offloading
            .store(enable_offloading, Ordering::SeqCst);
        self.mounted.store(true, Ordering::SeqCst);
    }

    pub(super) fn desired_enable_offloading(&self) -> Option<bool> {
        self.mounted
            .load(Ordering::SeqCst)
            .then(|| self.enable_offloading.load(Ordering::SeqCst))
    }
}

impl RemountState {
    pub(super) fn try_start(&self) -> bool {
        !self.in_progress.swap(true, Ordering::SeqCst)
    }

    pub(super) fn finish(&self) {
        self.in_progress.store(false, Ordering::SeqCst);
    }
}

#[derive(Default)]
pub(super) struct OffloadServerState {
    handle: parking_lot::RwLock<Option<tokio::task::JoinHandle<()>>>,
    port: AtomicU16,
    address: parking_lot::RwLock<String>,
}

impl OffloadServerState {
    pub(super) fn record_started(
        &self,
        handle: tokio::task::JoinHandle<()>,
        port: u16,
        address: String,
    ) {
        *self.handle.write() = Some(handle);
        self.port.store(port, Ordering::SeqCst);
        *self.address.write() = address;
    }

    pub(super) fn stop(&self) {
        if let Some(handle) = self.handle.write().take() {
            handle.abort();
        }
    }

    pub(super) fn address(&self) -> String {
        self.address.read().clone()
    }

    pub(super) fn is_running(&self) -> bool {
        self.handle
            .read()
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    #[cfg(test)]
    fn port(&self) -> u16 {
        self.port.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn has_handle(&self) -> bool {
        self.handle.read().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HealthState, LocalDiskMountState, OffloadServerState, RemountState, ShutdownState,
    };

    #[test]
    fn shutdown_state_is_open_until_closed() {
        let state = ShutdownState::default();

        assert!(!state.is_closed());
        state.close();
        assert!(state.is_closed());
    }

    #[test]
    fn health_state_tracks_the_last_ping_result() {
        let state = HealthState::default();

        assert!(!state.is_healthy());
        state.record_success();
        assert!(state.is_healthy());
        state.record_failure();
        assert!(!state.is_healthy());
    }

    #[test]
    fn remount_state_allows_only_one_in_flight_operation() {
        let state = RemountState::default();

        assert!(state.try_start());
        assert!(!state.try_start());
        state.finish();
        assert!(state.try_start());
    }

    #[test]
    fn local_disk_mount_state_remembers_the_requested_mode() {
        let state = LocalDiskMountState::default();
        assert_eq!(state.desired_enable_offloading(), None);
        state.record_mounted(false);
        assert_eq!(state.desired_enable_offloading(), Some(false));
        state.record_mounted(true);
        assert_eq!(state.desired_enable_offloading(), Some(true));
    }

    #[tokio::test]
    async fn offload_server_state_records_and_stops_a_server() {
        let state = OffloadServerState::default();
        let handle = tokio::spawn(std::future::pending());

        state.record_started(handle, 12_345, "127.0.0.1:12345".to_string());

        assert_eq!(state.port(), 12_345);
        assert_eq!(state.address(), "127.0.0.1:12345");
        assert!(state.is_running());
        state.stop();
        assert!(!state.has_handle());
        assert!(!state.is_running());
    }

    #[tokio::test]
    async fn offload_server_state_does_not_advertise_a_finished_server_as_running() {
        let state = OffloadServerState::default();
        let handle = tokio::spawn(async {});
        state.record_started(handle, 12_345, "127.0.0.1:12345".to_string());

        for _ in 0..10 {
            if !state.is_running() {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(!state.is_running());
    }
}

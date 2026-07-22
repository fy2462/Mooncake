use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnregisterResult {
    Success,
    RuntimeUnloading,
    Error,
}

pub(crate) trait PinOps: Send + Sync {
    fn register_region(&self, address: usize, size: usize) -> Result<(), String>;
    fn unregister_region(&self, address: usize) -> (UnregisterResult, Option<String>);
}

struct ActiveRegion {
    token: u64,
    address: usize,
    size: usize,
    active: bool,
}

#[derive(Default)]
struct ManagerState {
    pinned_bytes: usize,
    regions: Vec<ActiveRegion>,
}

struct ManagerInner {
    enabled: bool,
    limit_bytes: usize,
    ops: Arc<dyn PinOps>,
    next_token: AtomicU64,
    state: Mutex<ManagerState>,
}

pub(crate) struct PinnedMemoryManager {
    inner: Arc<ManagerInner>,
}

pub(crate) struct PinnedRegion {
    manager: Arc<ManagerInner>,
    token: u64,
    address: usize,
    size: usize,
    released: bool,
    release_succeeded: bool,
}

impl PinnedMemoryManager {
    pub(crate) fn new(enabled: bool, limit_bytes: usize, ops: Arc<dyn PinOps>) -> Self {
        Self {
            inner: Arc::new(ManagerInner {
                enabled,
                limit_bytes,
                ops,
                next_token: AtomicU64::new(1),
                state: Mutex::new(ManagerState::default()),
            }),
        }
    }

    pub(crate) fn try_pin(&self, address: usize, size: usize, owner: &str) -> Option<PinnedRegion> {
        if !self.inner.enabled || address == 0 || size == 0 {
            return None;
        }
        let end = address.checked_add(size)?;
        let token = self.inner.next_token.fetch_add(1, Ordering::Relaxed);
        {
            let mut state = self.inner.state.lock().expect("pin manager poisoned");
            let overlaps = state.regions.iter().any(|region| {
                let region_end = region.address.saturating_add(region.size);
                address < region_end && end > region.address
            });
            if overlaps {
                tracing::warn!(
                    owner,
                    size,
                    "skip CUDA host pin: range overlaps active region"
                );
                return None;
            }
            if size > self.inner.limit_bytes || state.pinned_bytes > self.inner.limit_bytes - size {
                tracing::warn!(
                    owner,
                    size,
                    pinned = state.pinned_bytes,
                    limit = self.inner.limit_bytes,
                    "skip CUDA host pin: quota exceeded"
                );
                return None;
            }
            state.regions.push(ActiveRegion {
                token,
                address,
                size,
                active: false,
            });
            state.pinned_bytes += size;
        }

        if let Err(error) = self.inner.ops.register_region(address, size) {
            self.inner.remove_inactive(token);
            tracing::warn!(owner, size, %error, "CUDA host registration failed; using pageable memory");
            return None;
        }
        if let Some(region) = self
            .inner
            .state
            .lock()
            .expect("pin manager poisoned")
            .regions
            .iter_mut()
            .find(|region| region.token == token)
        {
            region.active = true;
        }
        Some(PinnedRegion {
            manager: self.inner.clone(),
            token,
            address,
            size,
            released: false,
            release_succeeded: true,
        })
    }

    #[cfg(test)]
    fn pinned_bytes_for_test(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("pin manager poisoned")
            .pinned_bytes
    }
}

impl ManagerInner {
    fn remove_inactive(&self, token: u64) {
        let mut state = self.state.lock().expect("pin manager poisoned");
        let Some(index) = state
            .regions
            .iter()
            .position(|region| region.token == token && !region.active)
        else {
            return;
        };
        let region = state.regions.swap_remove(index);
        state.pinned_bytes = state.pinned_bytes.saturating_sub(region.size);
    }

    fn release(&self, token: u64, address: usize, size: usize) -> bool {
        {
            let mut state = self.state.lock().expect("pin manager poisoned");
            let Some(region) = state
                .regions
                .iter_mut()
                .find(|region| region.token == token && region.active)
            else {
                return true;
            };
            region.active = false;
        }
        let (result, error) = self.ops.unregister_region(address);
        match result {
            UnregisterResult::Success => {}
            UnregisterResult::RuntimeUnloading => {
                tracing::warn!(size, "skip CUDA host unregister: runtime is unloading");
            }
            UnregisterResult::Error => {
                tracing::error!(
                    size,
                    error = error.as_deref().unwrap_or("unknown CUDA error"),
                    "CUDA host unregister failed; backing memory must be retained"
                );
                return false;
            }
        }
        self.remove_inactive(token);
        true
    }
}

impl PinnedRegion {
    pub(crate) fn release(&mut self) -> bool {
        if self.released {
            return self.release_succeeded;
        }
        self.released = true;
        self.release_succeeded = self.manager.release(self.token, self.address, self.size);
        self.release_succeeded
    }
}

impl Drop for PinnedRegion {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct FakePinOps {
        register_succeeds: AtomicBool,
        unregister_result: Mutex<UnregisterResult>,
        register_calls: AtomicUsize,
        unregister_calls: AtomicUsize,
    }

    impl FakePinOps {
        fn new() -> Self {
            Self {
                register_succeeds: AtomicBool::new(true),
                unregister_result: Mutex::new(UnregisterResult::Success),
                register_calls: AtomicUsize::new(0),
                unregister_calls: AtomicUsize::new(0),
            }
        }
    }

    impl PinOps for FakePinOps {
        fn register_region(&self, _address: usize, _size: usize) -> Result<(), String> {
            self.register_calls.fetch_add(1, Ordering::Relaxed);
            self.register_succeeds
                .load(Ordering::Relaxed)
                .then_some(())
                .ok_or_else(|| "fake register failure".into())
        }

        fn unregister_region(&self, _address: usize) -> (UnregisterResult, Option<String>) {
            self.unregister_calls.fetch_add(1, Ordering::Relaxed);
            let result = *self.unregister_result.lock().unwrap();
            let error =
                (result == UnregisterResult::Error).then(|| "fake unregister failure".into());
            (result, error)
        }
    }

    fn manager(limit: usize) -> (PinnedMemoryManager, Arc<FakePinOps>) {
        let ops = Arc::new(FakePinOps::new());
        (PinnedMemoryManager::new(true, limit, ops.clone()), ops)
    }

    #[test]
    fn quota_rejects_and_release_refunds() {
        let (manager, ops) = manager(64);
        let mut first = manager.try_pin(0x1000, 64, "first").unwrap();
        assert!(manager.try_pin(0x2000, 1, "over quota").is_none());
        assert_eq!(ops.register_calls.load(Ordering::Relaxed), 1);
        assert!(first.release());
        assert_eq!(manager.pinned_bytes_for_test(), 0);
        assert!(manager.try_pin(0x2000, 64, "second").is_some());
    }

    #[test]
    fn overlap_is_rejected_but_adjacent_ranges_are_allowed() {
        let (manager, ops) = manager(128);
        let _first = manager.try_pin(0x1010, 32, "first").unwrap();
        assert!(manager.try_pin(0x1010, 32, "duplicate").is_none());
        assert!(manager.try_pin(0x1020, 16, "overlap").is_none());
        assert!(manager.try_pin(0x1030, 16, "adjacent").is_some());
        assert_eq!(ops.register_calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn registration_failure_refunds_reservation() {
        let (manager, ops) = manager(32);
        ops.register_succeeds.store(false, Ordering::Relaxed);
        assert!(manager.try_pin(0x1000, 32, "failure").is_none());
        assert_eq!(manager.pinned_bytes_for_test(), 0);
        ops.register_succeeds.store(true, Ordering::Relaxed);
        assert!(manager.try_pin(0x1000, 32, "retry").is_some());
    }

    #[test]
    fn runtime_unloading_releases_but_unregister_error_retains_reservation() {
        let (manager, ops) = manager(32);
        let mut unloading = manager.try_pin(0x1000, 32, "unloading").unwrap();
        *ops.unregister_result.lock().unwrap() = UnregisterResult::RuntimeUnloading;
        assert!(unloading.release());
        assert_eq!(manager.pinned_bytes_for_test(), 0);

        *ops.unregister_result.lock().unwrap() = UnregisterResult::Success;
        let mut failed = manager.try_pin(0x2000, 32, "failed").unwrap();
        *ops.unregister_result.lock().unwrap() = UnregisterResult::Error;
        assert!(!failed.release());
        assert_eq!(manager.pinned_bytes_for_test(), 32);
        assert!(manager.try_pin(0x2000, 32, "same range").is_none());
        assert!(manager.try_pin(0x3000, 32, "quota retained").is_none());
    }

    #[test]
    fn invalid_ranges_and_disabled_manager_do_not_call_runtime() {
        let (manager, ops) = manager(usize::MAX);
        assert!(manager.try_pin(0, 4, "null").is_none());
        assert!(manager.try_pin(usize::MAX - 1, 4, "overflow").is_none());
        assert!(manager.try_pin(0x1000, 0, "empty").is_none());
        let disabled = PinnedMemoryManager::new(false, usize::MAX, ops.clone());
        assert!(disabled.try_pin(0x1000, 4, "disabled").is_none());
        assert_eq!(ops.register_calls.load(Ordering::Relaxed), 0);
    }
}

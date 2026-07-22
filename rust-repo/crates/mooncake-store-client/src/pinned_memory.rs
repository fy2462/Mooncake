use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[cfg_attr(not(feature = "cuda-host-pin"), allow(dead_code))]
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

struct UnavailablePinOps;

impl PinOps for UnavailablePinOps {
    fn register_region(&self, _address: usize, _size: usize) -> Result<(), String> {
        Err("CUDA host pin runtime is unavailable".into())
    }

    fn unregister_region(&self, _address: usize) -> (UnregisterResult, Option<String>) {
        (
            UnregisterResult::Error,
            Some("CUDA host pin runtime is unavailable".into()),
        )
    }
}

fn parse_pinned_memory_limit(value: Option<&str>) -> Option<usize> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    let limit = value.parse::<u64>().ok()?;
    if limit == 0 {
        return None;
    }
    usize::try_from(limit).ok()
}

pub(crate) fn global_pinned_memory_manager() -> &'static PinnedMemoryManager {
    static MANAGER: OnceLock<PinnedMemoryManager> = OnceLock::new();
    MANAGER.get_or_init(|| {
        let raw = std::env::var("MC_STORE_PIN_MEMORY_MAX_BYTES").ok();
        let limit = parse_pinned_memory_limit(raw.as_deref());
        let ops = default_pin_ops();
        let enabled = limit.is_some() && ops.is_some();
        if raw
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty() && value.trim() != "0" && limit.is_none())
        {
            tracing::warn!(
                value = raw.as_deref().unwrap_or_default(),
                "invalid MC_STORE_PIN_MEMORY_MAX_BYTES; Store segment pinning disabled"
            );
        }
        if limit.is_some() && ops.is_none() {
            tracing::info!("Store segment pinning requested but CUDA runtime is unavailable");
        }
        PinnedMemoryManager::new(
            enabled,
            limit.unwrap_or(0),
            ops.unwrap_or_else(|| Arc::new(UnavailablePinOps)),
        )
    })
}

#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
fn default_pin_ops() -> Option<Arc<dyn PinOps>> {
    CudaPinOps::load().map(|ops| Arc::new(ops) as Arc<dyn PinOps>)
}

#[cfg(not(all(feature = "cuda-host-pin", target_os = "linux")))]
fn default_pin_ops() -> Option<Arc<dyn PinOps>> {
    None
}

#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
struct CudaPinOps {
    handle: *mut std::ffi::c_void,
    host_register: unsafe extern "C" fn(*mut std::ffi::c_void, usize, u32) -> i32,
    host_unregister: unsafe extern "C" fn(*mut std::ffi::c_void) -> i32,
    get_error_string: unsafe extern "C" fn(i32) -> *const std::ffi::c_char,
    get_last_error: unsafe extern "C" fn() -> i32,
}

#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
unsafe impl Send for CudaPinOps {}
#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
unsafe impl Sync for CudaPinOps {}

#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
impl CudaPinOps {
    fn load() -> Option<Self> {
        use std::ffi::CString;

        let mut handle = std::ptr::null_mut();
        for soname in [
            "libcudart.so",
            "libcudart.so.13",
            "libcudart.so.12",
            "libcudart.so.11.0",
        ] {
            let name = CString::new(soname).expect("CUDA soname contains no NUL");
            handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
            if !handle.is_null() {
                break;
            }
        }
        if handle.is_null() {
            return None;
        }

        let host_register = unsafe { load_symbol(handle, b"cudaHostRegister\0") };
        let host_unregister = unsafe { load_symbol(handle, b"cudaHostUnregister\0") };
        let get_error_string = unsafe { load_symbol(handle, b"cudaGetErrorString\0") };
        let get_last_error = unsafe { load_symbol(handle, b"cudaGetLastError\0") };
        match (
            host_register,
            host_unregister,
            get_error_string,
            get_last_error,
        ) {
            (
                Some(host_register),
                Some(host_unregister),
                Some(get_error_string),
                Some(get_last_error),
            ) => Some(Self {
                handle,
                host_register,
                host_unregister,
                get_error_string,
                get_last_error,
            }),
            _ => {
                unsafe { libc::dlclose(handle) };
                None
            }
        }
    }

    fn error_string(&self, code: i32) -> String {
        let ptr = unsafe { (self.get_error_string)(code) };
        if ptr.is_null() {
            return format!("CUDA error {code}");
        }
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
unsafe fn load_symbol<T: Copy>(handle: *mut std::ffi::c_void, name: &[u8]) -> Option<T> {
    let symbol = unsafe { libc::dlsym(handle, name.as_ptr().cast()) };
    if symbol.is_null() {
        None
    } else {
        Some(unsafe { std::mem::transmute_copy(&symbol) })
    }
}

#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
impl PinOps for CudaPinOps {
    fn register_region(&self, address: usize, size: usize) -> Result<(), String> {
        const CUDA_HOST_REGISTER_PORTABLE: u32 = 1;
        let result = unsafe {
            (self.host_register)(
                address as *mut std::ffi::c_void,
                size,
                CUDA_HOST_REGISTER_PORTABLE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            let error = self.error_string(result);
            unsafe { (self.get_last_error)() };
            Err(error)
        }
    }

    fn unregister_region(&self, address: usize) -> (UnregisterResult, Option<String>) {
        const CUDA_ERROR_CUDART_UNLOADING: i32 = 29;
        let result = unsafe { (self.host_unregister)(address as *mut std::ffi::c_void) };
        match result {
            0 => (UnregisterResult::Success, None),
            CUDA_ERROR_CUDART_UNLOADING => (
                UnregisterResult::RuntimeUnloading,
                Some(self.error_string(result)),
            ),
            _ => (UnregisterResult::Error, Some(self.error_string(result))),
        }
    }
}

#[cfg(all(feature = "cuda-host-pin", target_os = "linux"))]
impl Drop for CudaPinOps {
    fn drop(&mut self) {
        unsafe { libc::dlclose(self.handle) };
    }
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

/// Owns an allocation whose backing memory may be registered with CUDA.
/// A failed unregister intentionally leaks `allocation` to prevent a dangling
/// CUDA mapping.
pub(crate) struct PinnedAllocation<T> {
    allocation: std::mem::ManuallyDrop<T>,
    pin: Option<PinnedRegion>,
    released: bool,
    release_succeeded: bool,
}

impl<T> PinnedAllocation<T> {
    pub(crate) fn new(allocation: T, pin: Option<PinnedRegion>) -> Self {
        Self {
            allocation: std::mem::ManuallyDrop::new(allocation),
            pin,
            released: false,
            release_succeeded: true,
        }
    }

    pub(crate) fn release(&mut self) -> bool {
        if self.released {
            return self.release_succeeded;
        }
        if let Some(mut pin) = self.pin.take()
            && !pin.release()
        {
            self.released = true;
            self.release_succeeded = false;
            return false;
        }
        unsafe { std::mem::ManuallyDrop::drop(&mut self.allocation) };
        self.released = true;
        true
    }
}

impl<T> std::ops::Deref for PinnedAllocation<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.allocation
    }
}

impl<T> std::ops::DerefMut for PinnedAllocation<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.allocation
    }
}

impl<T> Drop for PinnedAllocation<T> {
    fn drop(&mut self) {
        let _ = self.release();
    }
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

    #[test]
    fn pinned_memory_limit_parsing_matches_cpp_configuration() {
        assert_eq!(parse_pinned_memory_limit(None), None);
        assert_eq!(parse_pinned_memory_limit(Some("")), None);
        assert_eq!(parse_pinned_memory_limit(Some("  \t\n")), None);
        assert_eq!(parse_pinned_memory_limit(Some("0")), None);
        assert_eq!(parse_pinned_memory_limit(Some(" 4096 ")), Some(4096));
        assert_eq!(parse_pinned_memory_limit(Some("-1")), None);
        assert_eq!(parse_pinned_memory_limit(Some("4KiB")), None);
        assert_eq!(
            parse_pinned_memory_limit(Some("18446744073709551616")),
            None
        );
    }

    #[test]
    fn pinned_allocation_frees_only_after_safe_unregister() {
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (manager, ops) = manager(16);
        let pin = manager.try_pin(0x1000, 16, "allocation");
        drop(PinnedAllocation::new(DropProbe(drops.clone()), pin));
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        let pin = manager.try_pin(0x2000, 16, "leaked allocation");
        *ops.unregister_result.lock().unwrap() = UnregisterResult::Error;
        drop(PinnedAllocation::new(DropProbe(drops.clone()), pin));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(manager.pinned_bytes_for_test(), 16);
    }
}

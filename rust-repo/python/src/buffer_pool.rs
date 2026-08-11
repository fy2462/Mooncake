use crate::client::{PythonMooncakeClient, SharedClient};
use mooncake_store_client::{BufferRegistrationId, MooncakeClient, RegisteredBufferAllocation};
use parking_lot::{Condvar, Mutex};
use pyo3::exceptions::{PyBufferError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyMemoryView;
use std::collections::HashMap;
use std::ffi::{c_int, c_void};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn pool_err(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[derive(Debug)]
struct ActiveRegion {
    allocation_size: usize,
}

#[derive(Debug, Default)]
struct PoolState {
    active: HashMap<u64, ActiveRegion>,
    total_bytes: usize,
    reserved_bytes: usize,
    reserved_regions: usize,
    next_lease_id: u64,
    closed: bool,
    closing: bool,
}

struct PoolCore {
    client: SharedClient,
    max_bytes: usize,
    min_size_class: usize,
    max_size_class: usize,
    alignment: usize,
    block_on_exhaustion: bool,
    default_timeout: Option<Duration>,
    max_regions: Option<usize>,
    state: Mutex<PoolState>,
    available: Condvar,
}

impl PoolCore {
    fn with_client<T>(
        &self,
        operation: impl FnOnce(&MooncakeClient) -> PyResult<T>,
    ) -> PyResult<T> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let slot = self.client.lock().await;
            let client = slot
                .as_ref()
                .ok_or_else(|| pool_err("MooncakeClient already closed or consumed"))?;
            operation(client)
        })
    }

    fn allocation_size(&self, requested_size: usize) -> PyResult<usize> {
        isize::try_from(requested_size)
            .map_err(|_| pool_err("requested buffer size exceeds Python buffer capacity"))?;
        let size = requested_size.max(1);
        if size > self.max_bytes {
            return Err(pool_err("requested buffer size exceeds pool capacity"));
        }
        Ok(size)
    }

    fn has_capacity(&self, state: &PoolState, size: usize) -> bool {
        let bytes_available = state
            .total_bytes
            .checked_add(state.reserved_bytes)
            .and_then(|used| self.max_bytes.checked_sub(used))
            .is_some_and(|remaining| size <= remaining);
        let region_available = self.max_regions.is_none_or(|limit| {
            state
                .active
                .len()
                .checked_add(state.reserved_regions)
                .is_some_and(|count| count < limit)
        });
        bytes_available && region_available
    }

    fn reserve(&self, size: usize, block: bool, timeout: Option<Duration>) -> PyResult<()> {
        let deadline = timeout
            .map(|duration| {
                Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| pool_err("timeout is too large"))
            })
            .transpose()?;
        let mut state = self.state.lock();
        loop {
            if state.closed {
                return Err(pool_err("buffer pool is closed"));
            }
            if state.closing {
                return Err(pool_err("buffer pool is closing"));
            }
            if self.has_capacity(&state, size) {
                state.reserved_bytes = state
                    .reserved_bytes
                    .checked_add(size)
                    .ok_or_else(|| pool_err("buffer pool reservation overflow"))?;
                state.reserved_regions = state
                    .reserved_regions
                    .checked_add(1)
                    .ok_or_else(|| pool_err("buffer pool region count overflow"))?;
                return Ok(());
            }
            if !block {
                return Err(pool_err("buffer pool is exhausted"));
            }
            match deadline {
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(pool_err("timed out waiting for buffer"));
                    };
                    if self.available.wait_for(&mut state, remaining).timed_out()
                        && !self.has_capacity(&state, size)
                    {
                        return Err(pool_err("timed out waiting for buffer"));
                    }
                }
                None => self.available.wait(&mut state),
            }
        }
    }

    fn cancel_reservation(&self, size: usize) -> PyResult<()> {
        let mut state = self.state.lock();
        state.reserved_bytes = state
            .reserved_bytes
            .checked_sub(size)
            .ok_or_else(|| pool_err("buffer pool reservation byte underflow"))?;
        state.reserved_regions = state
            .reserved_regions
            .checked_sub(1)
            .ok_or_else(|| pool_err("buffer pool reservation count underflow"))?;
        self.available.notify_all();
        Ok(())
    }

    fn commit_reservation(&self, size: usize) -> PyResult<u64> {
        let mut state = self.state.lock();
        let Some(reserved_bytes) = state.reserved_bytes.checked_sub(size) else {
            self.available.notify_all();
            return Err(pool_err("buffer pool reservation byte underflow"));
        };
        let Some(reserved_regions) = state.reserved_regions.checked_sub(1) else {
            self.available.notify_all();
            return Err(pool_err("buffer pool reservation count underflow"));
        };
        state.reserved_bytes = reserved_bytes;
        state.reserved_regions = reserved_regions;
        if state.closed || state.closing {
            self.available.notify_all();
            return Err(pool_err("buffer pool is closing"));
        }
        let Some(lease_id) = state.next_lease_id.checked_add(1) else {
            self.available.notify_all();
            return Err(pool_err("buffer pool lease identity overflow"));
        };
        let Some(total_bytes) = state.total_bytes.checked_add(size) else {
            self.available.notify_all();
            return Err(pool_err("buffer pool byte accounting overflow"));
        };
        state.next_lease_id = lease_id;
        state.total_bytes = total_bytes;
        state.active.insert(
            lease_id,
            ActiveRegion {
                allocation_size: size,
            },
        );
        Ok(lease_id)
    }

    fn unregister(&self, registration_id: BufferRegistrationId) -> PyResult<()> {
        self.with_client(|client| {
            client
                .unregister_buffer_handle(registration_id)
                .map_err(pool_err)
        })
    }

    fn release_region(&self, lease_id: u64, registration_id: BufferRegistrationId) -> PyResult<()> {
        {
            let state = self.state.lock();
            if !state.active.contains_key(&lease_id) {
                return Err(pool_err("buffer lease is not active"));
            }
        }
        self.unregister(registration_id)?;
        let mut state = self.state.lock();
        let region = state
            .active
            .remove(&lease_id)
            .ok_or_else(|| pool_err("buffer lease is not active"))?;
        state.total_bytes = state
            .total_bytes
            .checked_sub(region.allocation_size)
            .ok_or_else(|| pool_err("buffer pool byte accounting underflow"))?;
        self.available.notify_all();
        Ok(())
    }

    fn close(&self) -> PyResult<()> {
        let mut state = self.state.lock();
        if state.closed {
            return Ok(());
        }
        state.closing = true;
        while state.reserved_regions != 0 {
            self.available.wait(&mut state);
        }
        if !state.active.is_empty() {
            state.closing = false;
            self.available.notify_all();
            return Err(pool_err("cannot close buffer pool with active leases"));
        }
        state.closed = true;
        state.closing = false;
        self.available.notify_all();
        Ok(())
    }
}

struct LeaseRegion {
    lease_id: u64,
    registration_id: BufferRegistrationId,
    allocation: RegisteredBufferAllocation,
}

#[pyclass(name = "BufferLease", skip_from_py_object)]
pub(crate) struct BufferLeasePy {
    pool: Arc<PoolCore>,
    requested_size: usize,
    region: Mutex<Option<LeaseRegion>>,
    exports: AtomicUsize,
}

impl BufferLeasePy {
    fn release_internal(&self, check_exports: bool) -> PyResult<()> {
        if check_exports && self.exports.load(Ordering::Acquire) != 0 {
            return Err(pool_err("cannot release buffer while exported views exist"));
        }
        let Some(region) = self.region.lock().take() else {
            return Ok(());
        };
        if let Err(error) = self
            .pool
            .release_region(region.lease_id, region.registration_id)
        {
            *self.region.lock() = Some(region);
            return Err(error);
        }
        Ok(())
    }

    fn allocation(&self) -> PyResult<RegisteredBufferAllocation> {
        self.region
            .lock()
            .as_ref()
            .map(|region| region.allocation.clone())
            .ok_or_else(|| pool_err("buffer lease is closed"))
    }
}

impl Drop for BufferLeasePy {
    fn drop(&mut self) {
        let _ = self.release_internal(false);
    }
}

#[pymethods]
impl BufferLeasePy {
    #[getter]
    fn ptr(&self) -> PyResult<usize> {
        Ok(self.allocation()?.as_ptr() as usize)
    }

    #[getter]
    fn size(&self) -> usize {
        self.requested_size
    }

    #[getter]
    fn buffer<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    fn release(&self) -> PyResult<()> {
        self.release_internal(true)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_value: Option<&Bound<'_, PyAny>>,
        traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (exc_type, exc_value, traceback);
        self.release()
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("buffer view pointer is null"));
        }
        let lease = slf.borrow();
        let allocation = lease.allocation()?;
        let requested_size = lease.requested_size;
        lease.exports.fetch_add(1, Ordering::AcqRel);
        drop(lease);

        unsafe {
            (*view).obj = slf.into_any().into_ptr();
            (*view).buf = allocation.as_ptr();
            (*view).len = requested_size as isize;
            (*view).readonly = 0;
            (*view).itemsize = 1;
            (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
                b"B\0".as_ptr() as *mut _
            } else {
                ptr::null_mut()
            };
            (*view).ndim = 1;
            (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
                &mut (*view).len
            } else {
                ptr::null_mut()
            };
            (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
                &mut (*view).itemsize
            } else {
                ptr::null_mut()
            };
            (*view).suboffsets = ptr::null_mut();
            (*view).internal = ptr::null_mut();
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        let previous = self.exports.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "buffer export count underflow");
    }
}

#[pyclass(name = "BufferPool", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct BufferPoolPy {
    core: Arc<PoolCore>,
}

fn shared_client_from_store(store: &Bound<'_, PyAny>, depth: usize) -> PyResult<SharedClient> {
    if let Ok(client) = store.extract::<PyRef<'_, PythonMooncakeClient>>() {
        return Ok(client.inner.clone());
    }
    if depth < 4
        && let Ok(inner) = store.getattr("_store")
    {
        return shared_client_from_store(&inner, depth + 1);
    }
    Err(PyRuntimeError::new_err(
        "BufferPool store parameter must be a Rust MooncakeClient or wrapper exposing _store",
    ))
}

fn parse_timeout(timeout: Option<f64>, field: &str) -> PyResult<Option<Duration>> {
    match timeout {
        Some(value) if !value.is_finite() || value < 0.0 => Err(pool_err(format!(
            "{field} must be a finite non-negative number"
        ))),
        Some(value) => Duration::try_from_secs_f64(value)
            .map(Some)
            .map_err(|_| pool_err(format!("{field} is too large"))),
        None => Ok(None),
    }
}

#[pymethods]
impl BufferPoolPy {
    #[new]
    #[pyo3(signature = (
        store,
        max_bytes = 0,
        min_size_class = 64 * 1024,
        max_size_class = None,
        alignment = 8 * 1024 * 1024,
        block_on_exhaustion = true,
        default_timeout = None,
        max_regions = None,
        prewarm_size = None,
        prewarm_count = 0
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        store: &Bound<'_, PyAny>,
        max_bytes: usize,
        min_size_class: usize,
        max_size_class: Option<usize>,
        alignment: usize,
        block_on_exhaustion: bool,
        default_timeout: Option<f64>,
        max_regions: Option<usize>,
        prewarm_size: Option<usize>,
        prewarm_count: usize,
    ) -> PyResult<Self> {
        if min_size_class == 0 {
            return Err(pool_err("min_size_class must be positive"));
        }
        if alignment < std::mem::size_of::<*const c_void>() || !alignment.is_power_of_two() {
            return Err(pool_err(
                "alignment must be a power of two and at least sizeof(void*)",
            ));
        }
        if max_regions == Some(0) {
            return Err(pool_err("max_regions must be greater than zero"));
        }
        let client = shared_client_from_store(store, 0)?;
        let capacity_client = Arc::clone(&client);
        let local_capacity = py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let slot = capacity_client.lock().await;
                slot.as_ref()
                    .map(MooncakeClient::local_buffer_capacity)
                    .ok_or_else(|| pool_err("MooncakeClient already closed or consumed"))
            })
        })?;
        if local_capacity == 0 {
            return Err(pool_err(
                "BufferPool requires a client configured with a local buffer",
            ));
        }
        let max_bytes = if max_bytes == 0 {
            local_capacity.checked_mul(2).unwrap_or(local_capacity)
        } else {
            max_bytes.max(local_capacity)
        };
        let max_size_class = max_size_class.unwrap_or(max_bytes);
        let core = Arc::new(PoolCore {
            client,
            max_bytes,
            min_size_class,
            max_size_class,
            alignment,
            block_on_exhaustion,
            default_timeout: parse_timeout(default_timeout, "default_timeout")?,
            max_regions,
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        });
        let pool = Self { core };
        if let Some(size) = prewarm_size
            && prewarm_count > 0
        {
            pool.prewarm(size, prewarm_count)?;
        }
        Ok(pool)
    }

    #[pyo3(signature = (size, block = None, timeout = None))]
    fn acquire(
        &self,
        py: Python<'_>,
        size: usize,
        block: Option<bool>,
        timeout: Option<f64>,
    ) -> PyResult<BufferLeasePy> {
        let allocation_size = self.core.allocation_size(size)?;
        let should_block = block.unwrap_or(self.core.block_on_exhaustion);
        let timeout = match timeout {
            Some(value) => parse_timeout(Some(value), "timeout")?,
            None => self.core.default_timeout,
        };
        let core = Arc::clone(&self.core);
        py.detach(move || {
            core.reserve(allocation_size, should_block, timeout)?;
            let allocation =
                match RegisteredBufferAllocation::allocate(allocation_size, core.alignment) {
                    Ok(allocation) => allocation,
                    Err(error) => {
                        core.cancel_reservation(allocation_size)?;
                        return Err(pool_err(error));
                    }
                };
            let registration_id = match core.with_client(|client| {
                client
                    .register_owned_buffer(allocation.clone(), "cpu:0")
                    .map_err(pool_err)
            }) {
                Ok(registration_id) => registration_id,
                Err(error) => {
                    core.cancel_reservation(allocation_size)?;
                    return Err(error);
                }
            };
            let lease_id = match core.commit_reservation(allocation_size) {
                Ok(lease_id) => lease_id,
                Err(error) => {
                    let _ = core.unregister(registration_id);
                    return Err(error);
                }
            };
            Ok(BufferLeasePy {
                pool: core,
                requested_size: size,
                region: Mutex::new(Some(LeaseRegion {
                    lease_id,
                    registration_id,
                    allocation,
                })),
                exports: AtomicUsize::new(0),
            })
        })
    }

    #[pyo3(signature = (size, block = None, timeout = None))]
    fn buffer(
        &self,
        py: Python<'_>,
        size: usize,
        block: Option<bool>,
        timeout: Option<f64>,
    ) -> PyResult<BufferLeasePy> {
        self.acquire(py, size, block, timeout)
    }

    #[pyo3(signature = (size, count))]
    fn prewarm(&self, size: usize, count: usize) -> PyResult<()> {
        let _ = count;
        self.core.allocation_size(size).map(|_| ())
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        py.detach(move || core.close())
    }

    #[getter]
    fn borrowed_bytes(&self) -> usize {
        self.core.state.lock().total_bytes
    }

    #[getter]
    fn capacity_bytes(&self) -> usize {
        self.core.max_bytes
    }

    #[getter]
    fn min_size_class(&self) -> usize {
        self.core.min_size_class
    }

    #[getter]
    fn max_size_class(&self) -> usize {
        self.core.max_size_class
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_value: Option<&Bound<'_, PyAny>>,
        traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (exc_type, exc_value, traceback);
        self.close(py)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core(max_bytes: usize, max_regions: Option<usize>) -> PoolCore {
        PoolCore {
            client: Arc::new(tokio::sync::Mutex::new(None)),
            max_bytes,
            min_size_class: 4096,
            max_size_class: max_bytes,
            alignment: 4096,
            block_on_exhaustion: true,
            default_timeout: None,
            max_regions,
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        }
    }

    #[test]
    fn reservation_accounts_bytes_and_regions_before_allocation() {
        let core = core(8192, Some(1));
        core.reserve(4096, false, None).unwrap();
        {
            let state = core.state.lock();
            assert_eq!(state.reserved_bytes, 4096);
            assert_eq!(state.reserved_regions, 1);
            assert!(!core.has_capacity(&state, 1));
        }
        core.cancel_reservation(4096).unwrap();
        let state = core.state.lock();
        assert_eq!(state.reserved_bytes, 0);
        assert_eq!(state.reserved_regions, 0);
        assert!(core.has_capacity(&state, 8192));
    }

    #[test]
    fn nonblocking_acquire_fails_when_capacity_is_reserved() {
        let core = core(4096, None);
        core.reserve(4096, false, None).unwrap();
        assert!(core.reserve(1, false, None).is_err());
        core.cancel_reservation(4096).unwrap();
    }

    #[test]
    fn commit_overflow_releases_reservation() {
        let core = core(4096, None);
        core.reserve(1, false, None).unwrap();
        core.state.lock().next_lease_id = u64::MAX;
        assert!(core.commit_reservation(1).is_err());
        let state = core.state.lock();
        assert_eq!(state.reserved_bytes, 0);
        assert_eq!(state.reserved_regions, 0);
        assert!(state.active.is_empty());
    }

    #[test]
    fn close_rejects_active_lease_without_corrupting_open_state() {
        let core = core(4096, None);
        {
            let mut state = core.state.lock();
            state.total_bytes = 4096;
            state.active.insert(
                1,
                ActiveRegion {
                    allocation_size: 4096,
                },
            );
        }
        assert!(core.close().is_err());
        let state = core.state.lock();
        assert!(!state.closed);
        assert!(!state.closing);
    }

    #[test]
    fn timeout_validation_rejects_negative_nan_and_infinity() {
        assert!(parse_timeout(Some(-1.0), "timeout").is_err());
        assert!(parse_timeout(Some(f64::NAN), "timeout").is_err());
        assert!(parse_timeout(Some(f64::INFINITY), "timeout").is_err());
        assert!(parse_timeout(Some(f64::MAX), "timeout").is_err());
        assert_eq!(
            parse_timeout(Some(0.25), "timeout").unwrap(),
            Some(Duration::from_millis(250))
        );
    }
}

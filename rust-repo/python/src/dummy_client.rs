use crate::client::{PythonMooncakeClient, SharedClient, take_client, try_client_slot};
use crate::replicate_config::ReplicateConfigPy;
use crate::to_py_err;
use mooncake_store_client::{
    BufferRegistrationId, DummyIpcChannel, DummyMemoryPool, INVALID_PHYSICAL_DEVICE_ID,
    ShmRegisterRequest,
};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::ffi::c_void;
use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;
use transfer_engine_ffi::StableMemoryOwner;

struct DummyPoolOwner {
    pool: Arc<DummyMemoryPool>,
}

impl fmt::Debug for DummyPoolOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DummyPoolOwner")
            .field("base", &format_args!("{:#x}", self.pool.base_addr()))
            .field("len", &self.pool.len())
            .finish()
    }
}

unsafe impl StableMemoryOwner for DummyPoolOwner {
    // The Arc owns the non-resizing DummyMemoryPool allocation for the entire
    // registration lifetime.
    fn base_address(&self) -> NonNull<c_void> {
        NonNull::new(self.pool.base_ptr()).expect("DummyMemoryPool has a non-null base")
    }

    fn length(&self) -> usize {
        self.pool.len()
    }
}

#[pyclass(name = "MooncakeDummyClient")]
pub(crate) struct PythonMooncakeDummyClient {
    inner: SharedClient,
    mem_pool: Arc<DummyMemoryPool>,
    local_buffer_pool: Option<DummyMemoryPool>,
    registered_pool: Mutex<Option<BufferRegistrationId>>,
}

impl PythonMooncakeDummyClient {
    fn checked_ptr(&self, addr: u64, size: usize) -> PyResult<*mut c_void> {
        self.mem_pool.checked_ptr(addr, size).map_err(to_py_err)
    }

    fn pool_ptr(&self) -> *mut c_void {
        self.mem_pool.base_ptr()
    }

    fn checked_ptrs(&self, addrs: &[u64], sizes: &[usize]) -> PyResult<Vec<*mut c_void>> {
        self.mem_pool.checked_ptrs(addrs, sizes).map_err(to_py_err)
    }

    fn register_pool_via_ipc(
        socket_path: &str,
        pool: &DummyMemoryPool,
        client_id_first: u64,
        client_id_second: u64,
        is_local_buffer: bool,
    ) -> PyResult<()> {
        let fd = pool
            .fd()
            .ok_or_else(|| to_py_err("dummy memory pool is not fd-backed"))?;
        let request = ShmRegisterRequest {
            client_id_first,
            client_id_second,
            dummy_base_addr: pool.base_addr() as u64,
            shm_size: pool.len() as u64,
            device_id: INVALID_PHYSICAL_DEVICE_ID,
            is_local_buffer,
        };
        DummyIpcChannel::register_shm_fd(socket_path, fd, request)
            .map(|_| ())
            .map_err(to_py_err)
    }
}

#[pymethods]
impl PythonMooncakeDummyClient {
    /// Create an in-process dummy client sharing an existing MooncakeClient.
    ///
    /// server_address and ipc_socket_path are accepted for C++ API shape, but
    /// this implementation does not perform cross-process IPC.
    #[staticmethod]
    #[pyo3(signature = (real_client, mem_pool_size, local_buffer_size = 0, server_address = String::new(), ipc_socket_path = String::new()))]
    fn setup_dummy(
        real_client: &Bound<'_, PythonMooncakeClient>,
        mem_pool_size: usize,
        local_buffer_size: usize,
        server_address: String,
        ipc_socket_path: String,
    ) -> PyResult<Self> {
        let _ = server_address;
        let inner = real_client.borrow().inner.clone();
        let use_ipc = !ipc_socket_path.is_empty();
        let mem_pool = Arc::new(if use_ipc {
            DummyMemoryPool::new_shared(mem_pool_size).map_err(to_py_err)?
        } else {
            DummyMemoryPool::new(mem_pool_size).map_err(to_py_err)?
        });
        let local_buffer_pool = if use_ipc && local_buffer_size > 0 {
            Some(DummyMemoryPool::new_shared(local_buffer_size).map_err(to_py_err)?)
        } else {
            None
        };
        let dummy = Self {
            inner,
            mem_pool,
            local_buffer_pool,
            registered_pool: Mutex::new(None),
        };

        {
            let guard = try_client_slot(&dummy.inner)?;
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            if use_ipc {
                let (client_id_first, client_id_second) = client.client_id().as_u64_pair();
                Self::register_pool_via_ipc(
                    &ipc_socket_path,
                    dummy.mem_pool.as_ref(),
                    client_id_first,
                    client_id_second,
                    false,
                )?;
                if let Some(local_buffer_pool) = dummy.local_buffer_pool.as_ref() {
                    Self::register_pool_via_ipc(
                        &ipc_socket_path,
                        local_buffer_pool,
                        client_id_first,
                        client_id_second,
                        true,
                    )?;
                }
            } else {
                let registration_id = client
                    .register_owned_buffer(
                        DummyPoolOwner {
                            pool: Arc::clone(&dummy.mem_pool),
                        },
                        "cpu:0",
                    )
                    .map_err(to_py_err)?;
                *dummy.registered_pool.lock() = Some(registration_id);
            }
        }
        Ok(dummy)
    }

    /// Allocate from the dummy memory pool and return a process-local address.
    fn alloc_from_mem_pool(&self, size: usize) -> PyResult<u64> {
        self.mem_pool.alloc(size).map_err(to_py_err)
    }

    /// Copy bytes into a dummy allocation.
    fn write_mem(&self, addr: u64, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        self.mem_pool
            .write(addr, data.as_bytes())
            .map_err(to_py_err)
    }

    /// Read bytes from a dummy allocation.
    fn read_mem<'py>(
        &self,
        py: Python<'py>,
        addr: u64,
        size: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let data = self.mem_pool.read(addr, size).map_err(to_py_err)?;
        Ok(PyBytes::new(py, &data))
    }

    #[pyo3(signature = (key, value, config = None))]
    fn put<'py>(
        &self,
        py: Python<'py>,
        key: String,
        value: Bound<'py, PyBytes>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data = value.as_bytes().to_vec();
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.put(&key, &data, cfg).await;
            result.map_err(to_py_err)
        })
    }

    fn get<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.get(&key).await;
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (keys, values, config = None))]
    fn put_batch<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.batch_put(&keys, &slices, cfg).await;
            let statuses = result.map_err(to_py_err)?;
            Ok(statuses.into_iter().find(|status| *status < 0).unwrap_or(0))
        })
    }

    fn batch_get<'py>(&self, py: Python<'py>, keys: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get(&keys).await;
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (key, force = false))]
    fn remove<'py>(
        &self,
        py: Python<'py>,
        key: String,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.remove(&key, force).await;
            result.map_err(to_py_err)
        })
    }

    fn exists<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.exists(&key).await;
            result.map_err(to_py_err)
        })
    }

    fn batch_is_exist<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_is_exist(&keys).await;
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (keys, force = false))]
    fn batch_remove<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<String>,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_remove(&keys, force).await;
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (key, addr, size, config = None))]
    fn put_from(
        &self,
        key: String,
        addr: u64,
        size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<()> {
        let ptr = self.checked_ptr(addr, size)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.put_from(&key, ptr, size, cfg).await;
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (keys, addrs, sizes, config = None))]
    fn batch_put_from(
        &self,
        keys: Vec<String>,
        addrs: Vec<u64>,
        sizes: Vec<usize>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Vec<i32>> {
        let ptrs = self.checked_ptrs(&addrs, &sizes)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_put_from(&keys, &ptrs, &sizes, cfg).await;
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (key, addr, metadata_addr, size, metadata_size, config = None))]
    fn put_from_with_metadata(
        &self,
        key: String,
        addr: u64,
        metadata_addr: u64,
        size: usize,
        metadata_size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<i32> {
        let ptr = self.checked_ptr(addr, size)?;
        let metadata_ptr = self.checked_ptr(metadata_addr, metadata_size)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client
                .put_from_with_metadata(&key, ptr, metadata_ptr, size, metadata_size, cfg)
                .await;
            result.map_err(to_py_err)
        })
    }

    fn get_into(&self, key: String, addr: u64, size: usize) -> PyResult<usize> {
        let ptr = self.checked_ptr(addr, size)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.get_into(&key, ptr, size).await;
            result.map_err(to_py_err)
        })
    }

    fn batch_get_into(
        &self,
        keys: Vec<String>,
        addrs: Vec<u64>,
        sizes: Vec<usize>,
    ) -> PyResult<Vec<i64>> {
        let ptrs = self.checked_ptrs(&addrs, &sizes)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get_into(&keys, &ptrs, &sizes).await;
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (keys, all_addrs, all_sizes, prefer_same_node = false))]
    fn batch_get_into_multi_buffers(
        &self,
        keys: Vec<String>,
        all_addrs: Vec<Vec<u64>>,
        all_sizes: Vec<Vec<usize>>,
        prefer_same_node: bool,
    ) -> PyResult<Vec<i64>> {
        if all_addrs.len() != all_sizes.len() {
            return Err(to_py_err("all_addrs and all_sizes must have same length"));
        }
        let ptrs = all_addrs
            .iter()
            .zip(all_sizes.iter())
            .map(|(addrs, sizes)| self.checked_ptrs(addrs, sizes))
            .collect::<PyResult<Vec<_>>>()?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client
                .batch_get_into_multi_buffers(&keys, &ptrs, &all_sizes, prefer_same_node)
                .await;
            result.map_err(to_py_err)
        })
    }

    fn health_check<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.health_check().await;
            result.map_err(to_py_err)
        })
    }

    fn tear_down_all(&self) -> PyResult<()> {
        let mut registered_pool = self.registered_pool.lock();
        if let Some(registration_id) = *registered_pool {
            let guard = try_client_slot(&self.inner)?;
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client is closed or currently in use"))?;
            client
                .unregister_buffer_handle(registration_id)
                .map_err(to_py_err)?;
            *registered_pool = None;
        }
        drop(registered_pool);
        self.mem_pool.reset();
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "MooncakeDummyClient(pool_size={}, allocations={}, ipc={})",
            self.mem_pool.len(),
            self.mem_pool.allocations_len(),
            self.mem_pool.fd().is_some()
        )
    }
}

impl Drop for PythonMooncakeDummyClient {
    fn drop(&mut self) {
        let _ = self.tear_down_all();
    }
}

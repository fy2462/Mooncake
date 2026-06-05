use crate::client::{take_client, PythonMooncakeClient};
use crate::replicate_config::ReplicateConfigPy;
use crate::to_py_err;
use mooncake_store_client::{DummyMemoryPool, MooncakeClient};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::ffi::c_void;
use std::sync::Arc;

#[pyclass(name = "MooncakeDummyClient")]
pub(crate) struct PythonMooncakeDummyClient {
    inner: Arc<Mutex<Option<MooncakeClient>>>,
    mem_pool: DummyMemoryPool,
    registered_pool: Mutex<bool>,
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
        let _ = (local_buffer_size, server_address, ipc_socket_path);
        let inner = real_client.borrow().inner.clone();
        let dummy = Self {
            inner,
            mem_pool: DummyMemoryPool::new(mem_pool_size).map_err(to_py_err)?,
            registered_pool: Mutex::new(false),
        };

        {
            let guard = dummy.inner.lock();
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            unsafe {
                client
                    .register_buffer(dummy.pool_ptr(), mem_pool_size, "cpu:0")
                    .map_err(to_py_err)?;
            }
            *dummy.registered_pool.lock() = true;
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
            let mut client = take_client(&inner)?;
            let result = client.put(&key, &data, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn get<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.get(&key).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.batch_put(&keys, &slices, cfg).await;
            *inner.lock() = Some(client);
            let statuses = result.map_err(to_py_err)?;
            Ok(statuses.into_iter().find(|status| *status < 0).unwrap_or(0))
        })
    }

    fn batch_get<'py>(&self, py: Python<'py>, keys: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.batch_get(&keys).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.remove(&key, force).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn exists<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.exists(&key).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.batch_is_exist(&keys).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.batch_remove(&keys, force).await;
            *inner.lock() = Some(client);
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
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.put_from(&key, ptr, size, cfg) }.await;
            *inner.lock() = Some(client);
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
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_put_from(&keys, &ptrs, &sizes, cfg) }.await;
            *inner.lock() = Some(client);
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
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe {
                client
                    .put_from_with_metadata(&key, ptr, metadata_ptr, size, metadata_size, cfg)
                    .await
            };
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn get_into(&self, key: String, addr: u64, size: usize) -> PyResult<usize> {
        let ptr = self.checked_ptr(addr, size)?;
        let inner = self.inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.get_into(&key, ptr, size) }.await;
            *inner.lock() = Some(client);
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
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_get_into(&keys, &ptrs, &sizes) }.await;
            *inner.lock() = Some(client);
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
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe {
                client
                    .batch_get_into_multi_buffers(&keys, &ptrs, &all_sizes, prefer_same_node)
                    .await
            };
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn health_check<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.health_check().await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn tear_down_all(&self) -> PyResult<()> {
        if *self.registered_pool.lock() {
            let guard = self.inner.lock();
            if let Some(client) = guard.as_ref() {
                unsafe {
                    let _ = client.unregister_buffer(self.pool_ptr());
                }
            }
            *self.registered_pool.lock() = false;
        }
        self.mem_pool.reset();
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "MooncakeDummyClient(in_process, pool_size={}, allocations={})",
            self.mem_pool.len(),
            self.mem_pool.allocations_len()
        )
    }
}

impl Drop for PythonMooncakeDummyClient {
    fn drop(&mut self) {
        let _ = self.tear_down_all();
    }
}

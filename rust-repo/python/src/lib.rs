use mooncake_store_client::MooncakeClient;
use mooncake_store_core::ReplicateConfig;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;

pyo3::create_exception!(
    mooncake_store,
    StoreErrorPy,
    pyo3::exceptions::PyException,
    "Mooncake Store error"
);

// ---------------------------------------------------------------------------
// ReplicateConfig
// ---------------------------------------------------------------------------

#[pyclass(name = "ReplicateConfig", from_py_object)]
#[derive(Clone)]
struct ReplicateConfigPy {
    #[pyo3(get, set)]
    replica_num: u32,
    #[pyo3(get, set)]
    with_soft_pin: bool,
    #[pyo3(get, set)]
    with_hard_pin: bool,
    #[pyo3(get, set)]
    preferred_segment: String,
    #[pyo3(get, set)]
    prefer_alloc_in_same_node: bool,
}

#[pymethods]
impl ReplicateConfigPy {
    #[new]
    #[pyo3(signature = (
        replica_num = 1,
        with_soft_pin = false,
        with_hard_pin = false,
        preferred_segment = String::new(),
        prefer_alloc_in_same_node = false,
    ))]
    fn new(
        replica_num: u32,
        with_soft_pin: bool,
        with_hard_pin: bool,
        preferred_segment: String,
        prefer_alloc_in_same_node: bool,
    ) -> Self {
        Self {
            replica_num,
            with_soft_pin,
            with_hard_pin,
            preferred_segment,
            prefer_alloc_in_same_node,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ReplicateConfig(replica_num={}, with_soft_pin={}, with_hard_pin={}, preferred_segment='{}')",
            self.replica_num, self.with_soft_pin, self.with_hard_pin, self.preferred_segment
        )
    }
}

impl ReplicateConfigPy {
    fn to_core(&self) -> ReplicateConfig {
        ReplicateConfig {
            replica_num: self.replica_num,
            with_soft_pin: self.with_soft_pin,
            with_hard_pin: self.with_hard_pin,
            preferred_segment: self.preferred_segment.clone(),
            prefer_alloc_in_same_node: self.prefer_alloc_in_same_node,
        }
    }
}

// ---------------------------------------------------------------------------
// PythonMooncakeClient
// ---------------------------------------------------------------------------

#[pyclass(name = "MooncakeClient")]
struct PythonMooncakeClient {
    inner: Arc<Mutex<Option<MooncakeClient>>>,
}

#[pymethods]
impl PythonMooncakeClient {
    #[staticmethod]
    fn create<'py>(
        py: Python<'py>,
        local_hostname: String,
        metadata_server: String,
        master_server_addr: String,
        protocol: String,
        device: String,
        global_segment_size: i64,
        local_buffer_size: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let local_hostname = if local_hostname.is_empty() {
            "localhost".to_string()
        } else {
            local_hostname
        };
        let protocol = if protocol.is_empty() {
            "tcp".to_string()
        } else {
            protocol
        };
        let gss = if global_segment_size < 0 {
            0u64
        } else {
            global_segment_size as u64
        };
        let lbs = if local_buffer_size < 0 {
            268435456u64
        } else {
            local_buffer_size as u64
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = MooncakeClient::create(
                &master_server_addr,
                &metadata_server,
                &local_hostname,
                &protocol,
                &device,
                gss,
                lbs,
            )
            .await
            .map_err(|e| StoreErrorPy::new_err(e.to_string()))?;

            Ok(PythonMooncakeClient {
                inner: Arc::new(Mutex::new(Some(client))),
            })
        })
    }

    #[pyo3(signature = (key, value, config = None))]
    fn put<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        value: Bound<'py, PyBytes>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data = value.as_bytes().to_vec();
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard
                    .take()
                    .ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.put(&key, &data, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(|e| StoreErrorPy::new_err(e.to_string()))
        })
    }

    fn get<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard
                    .take()
                    .ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.get(&key).await;
            *inner.lock() = Some(client);
            let data = result.map_err(|e| StoreErrorPy::new_err(e.to_string()))?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                PyBytes::new(py, &data).unbind()
            })
        })
    }

    fn remove<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard
                    .take()
                    .ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.remove(&key).await;
            *inner.lock() = Some(client);
            result.map_err(|e| StoreErrorPy::new_err(e.to_string()))
        })
    }

    fn exists<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard
                    .take()
                    .ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.exists(&key).await;
            *inner.lock() = Some(client);
            result.map_err(|e| StoreErrorPy::new_err(e.to_string()))
        })
    }

    fn close(&self) {
        let mut guard = self.inner.lock();
        *guard = None;
    }

    fn __repr__(&self) -> String {
        "MooncakeClient(...)".to_string()
    }
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn _mooncake_store(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PythonMooncakeClient>()?;
    m.add_class::<ReplicateConfigPy>()?;
    m.add("StoreError", m.py().get_type::<StoreErrorPy>())?;
    Ok(())
}

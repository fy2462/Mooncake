use mooncake_store_client::MooncakeClient;
use mooncake_store_core::ReplicateConfig;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::sync::Arc;
use uuid::Uuid;

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
    #[pyo3(get, set)]
    nof_replica_num: u32,
}

#[pymethods]
impl ReplicateConfigPy {
    #[new]
    #[pyo3(signature = (
        replica_num = 1,
        nof_replica_num = 0,
        with_soft_pin = false,
        with_hard_pin = false,
        preferred_segment = String::new(),
        prefer_alloc_in_same_node = false,
    ))]
    fn new(
        replica_num: u32,
        nof_replica_num: u32,
        with_soft_pin: bool,
        with_hard_pin: bool,
        preferred_segment: String,
        prefer_alloc_in_same_node: bool,
    ) -> Self {
        Self {
            replica_num,
            nof_replica_num,
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
            nof_replica_num: self.nof_replica_num,
            with_soft_pin: self.with_soft_pin,
            with_hard_pin: self.with_hard_pin,
            preferred_segment: self.preferred_segment.clone(),
            prefer_alloc_in_same_node: self.prefer_alloc_in_same_node,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bytes_to_py(py: Python<'_>, data: &[u8]) -> Py<PyBytes> {
    PyBytes::new(py, data).unbind()
}

fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    StoreErrorPy::new_err(e.to_string())
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
            .map_err(to_py_err)?;

            Ok(PythonMooncakeClient {
                inner: Arc::new(Mutex::new(Some(client))),
            })
        })
    }

    // -------------------------------------------------------------------
    // put / get / remove / exists
    // -------------------------------------------------------------------

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
            result.map_err(to_py_err)
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
            let data = result.map_err(to_py_err)?;
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
            result.map_err(to_py_err)
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
            result.map_err(to_py_err)
        })
    }

    // -------------------------------------------------------------------
    // batch_put
    // -------------------------------------------------------------------

    #[pyo3(signature = (keys, values, config = None))]
    fn batch_put<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config.map(|c| c.borrow().to_core());
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.batch_put(&keys, &slices, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // -------------------------------------------------------------------
    // batch_get
    // -------------------------------------------------------------------

    fn batch_get<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.batch_get(&keys).await;
            *inner.lock() = Some(client);
            let results = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                let out: Vec<Option<Py<PyBytes>>> = results
                    .into_iter()
                    .map(|opt| opt.map(|d| bytes_to_py(py, &d)))
                    .collect();
                out.into_pyobject(py).unwrap().unbind()
            })
        })
    }

    // -------------------------------------------------------------------
    // batch_remove / batch_is_exist
    // -------------------------------------------------------------------

    fn batch_remove<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.batch_remove(&keys).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn batch_is_exist<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.batch_is_exist(&keys).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // -------------------------------------------------------------------
    // remove_by_regex / remove_all
    // -------------------------------------------------------------------

    fn remove_by_regex<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        pattern: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let removed = client.remove_by_regex(&pattern).await;
            *inner.lock() = Some(client);
            removed.map_err(to_py_err)
        })
    }

    fn remove_all<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let removed = client.remove_all().await;
            *inner.lock() = Some(client);
            removed.map_err(to_py_err)
        })
    }

    // -------------------------------------------------------------------
    // get_size / get_hostname / health_check
    // -------------------------------------------------------------------

    fn get_size<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let size = client.get_size(&key).await;
            *inner.lock() = Some(client);
            size.map_err(to_py_err)
        })
    }

    fn get_hostname(&self) -> PyResult<String> {
        match self.inner.lock().as_ref() {
            Some(client) => Ok(client.get_hostname()),
            None => Err(StoreErrorPy::new_err("client already closed")),
        }
    }

    fn health_check<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.health_check().await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // -------------------------------------------------------------------
    // tear_down_all / is_closed
    // -------------------------------------------------------------------

    fn tear_down_all<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.tear_down_all().await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn is_closed(&self) -> bool {
        match self.inner.lock().as_ref() {
            Some(client) => client.is_closed(),
            None => true,
        }
    }

    // -------------------------------------------------------------------
    // upsert / upsert_parts
    // -------------------------------------------------------------------

    #[pyo3(signature = (key, value, config = None))]
    fn upsert<'py>(
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
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.upsert(&key, &data, cfg).await;
            *inner.lock() = Some(client);
            let replicas = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                let out: Vec<Py<PyAny>> = replicas.iter().map(|r| {
                    let d = PyDict::new(py);
                    d.set_item("segment_name", &r.segment_name).ok();
                    d.set_item("offset", r.offset).ok();
                    d.set_item("segment_id", r.segment_id.to_string()).ok();
                    d.into()
                }).collect();
                out.into_pyobject(py).unwrap().unbind()
            })
        })
    }

    #[pyo3(signature = (key, values, config = None))]
    fn upsert_parts<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config.map(|c| c.borrow().to_core());
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.upsert_parts(&key, &slices, cfg).await;
            *inner.lock() = Some(client);
            let replicas = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                let out: Vec<Py<PyAny>> = replicas.iter().map(|r| {
                    let d = PyDict::new(py);
                    d.set_item("segment_name", &r.segment_name).ok();
                    d.set_item("offset", r.offset).ok();
                    d.set_item("segment_id", r.segment_id.to_string()).ok();
                    d.into()
                }).collect();
                out.into_pyobject(py).unwrap().unbind()
            })
        })
    }

    // -------------------------------------------------------------------
    // Tasks
    // -------------------------------------------------------------------

    fn create_copy_task<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        targets: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.create_copy_task(&key, &targets).await;
            *inner.lock() = Some(client);
            let task_id = result.map_err(to_py_err)?;
            Ok(task_id.to_string())
        })
    }

    fn create_move_task<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        source: String,
        target: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.create_move_task(&key, &source, &target).await;
            *inner.lock() = Some(client);
            let task_id = result.map_err(to_py_err)?;
            Ok(task_id.to_string())
        })
    }

    fn query_task<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        task_id_str: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let task_id = Uuid::parse_str(&task_id_str)
            .map_err(|e| StoreErrorPy::new_err(format!("invalid UUID: {e}")))?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| StoreErrorPy::new_err("client already closed"))?
            };
            let result = client.query_task(task_id).await;
            *inner.lock() = Some(client);
            let resp = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                let d = PyDict::new(py);
                if let Some(id) = &resp.id {
                    d.set_item("task_id", Uuid::from_u64_pair(id.high, id.low).to_string()).ok();
                }
                d.set_item("status", resp.status).ok();
                d.set_item("message", &resp.message).ok();
                d.unbind()
            })
        })
    }

    fn close(&self) {
        let mut guard = self.inner.lock();
        *guard = None;
    }

    fn __repr__(&self) -> String {
        if self.inner.lock().is_some() {
            "MooncakeClient(connected)".to_string()
        } else {
            "MooncakeClient(closed)".to_string()
        }
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

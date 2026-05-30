use crate::remote_config::PyRemoteSourceConfig;
use crate::replicate_config::ReplicateConfigPy;
use mooncake_store_client::proto::StorageObjectMetadata;
use mooncake_store_client::MooncakeClient;
use parking_lot::Mutex;
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::ffi::c_void;
use std::sync::Arc;
use uuid::Uuid;

use super::to_py_err;

#[pyclass(name = "MooncakeClient")]
pub(crate) struct PythonMooncakeClient {
    pub(crate) inner: Arc<Mutex<Option<MooncakeClient>>>,
    pub(crate) registered_py_buffers: Arc<Mutex<Vec<(usize, Py<PyAny>)>>>,
}

// -- helpers --

fn bytes_to_py(py: Python<'_>, data: &[u8]) -> Py<PyBytes> {
    PyBytes::new(py, data).unbind()
}

fn get_buffer_ptr(obj: &Bound<'_, PyAny>) -> PyResult<(*mut c_void, usize)> {
    let buf = PyBuffer::<u8>::get(obj)?;
    Ok((buf.buf_ptr() as *mut c_void, buf.item_count()))
}

pub(crate) fn take_client(inner: &Arc<Mutex<Option<MooncakeClient>>>) -> PyResult<MooncakeClient> {
    inner
        .lock()
        .take()
        .ok_or_else(|| to_py_err("client already closed"))
}

pub(crate) fn replicas_to_py(replicas: Vec<mooncake_store_core::ReplicaDescriptor>) -> Py<PyAny> {
    let py = unsafe { Python::assume_attached() };
    let out: Vec<Py<PyAny>> = replicas
        .iter()
        .map(|r| {
            let d = PyDict::new(py);
            d.set_item("segment_name", &r.segment_name).ok();
            d.set_item("offset", r.offset).ok();
            d.set_item("segment_id", r.segment_id.to_string()).ok();
            d.into()
        })
        .collect();
    out.into_pyobject(py).expect("replicas_to_py: into_pyobject failed").unbind()
}

// -- Python-exported methods --

#[pymethods]
impl PythonMooncakeClient {
    // ===================================================================
    // create
    // ===================================================================

    #[staticmethod]
    #[pyo3(signature = (
        local_hostname,
        metadata_server,
        master_server_addr,
        protocol = String::new(),
        device = String::new(),
        global_segment_size = -1,
        local_buffer_size = -1,
        remote_config = None::<PyRemoteSourceConfig>,
    ))]
    fn create<'py>(
        py: Python<'py>,
        local_hostname: String,
        metadata_server: String,
        master_server_addr: String,
        protocol: String,
        device: String,
        global_segment_size: i64,
        local_buffer_size: i64,
        remote_config: Option<PyRemoteSourceConfig>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use mooncake_store_client::LocalFsSource;

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
            let mut client = MooncakeClient::create(
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

            // Wire up remote source if configured
            if let Some(ref py_cfg) = remote_config {
                let config = py_cfg.to_core();

                if let Some(ref s3_py) = py_cfg.s3_config {
                    #[cfg(feature = "s3")]
                    {
                        use mooncake_store_client::S3RemoteSource;
                        let source = S3RemoteSource::new(&s3_py.to_core())
                            .await
                            .map_err(|e| to_py_err(format!("S3 init failed: {e}")))?;
                        client = client.with_remote_source(source, config);
                    }
                    #[cfg(not(feature = "s3"))]
                    {
                        let _ = s3_py; // used only with s3 feature
                        return Err(to_py_err(
                            "S3 remote source configured but 's3' feature is not enabled. \
                             Rebuild with --features s3",
                        ));
                    }
                } else if let Some(ref root) = py_cfg.local_fs_root {
                    let source = LocalFsSource::new(root.clone());
                    client = client.with_remote_source(source, config);
                } else if config.enabled {
                    return Err(to_py_err(
                        "RemoteSourceConfig.enabled=true requires s3_config or local_fs_root",
                    ));
                }
            }

            Ok(PythonMooncakeClient {
                inner: Arc::new(Mutex::new(Some(client))),
                registered_py_buffers: Arc::new(Mutex::new(Vec::new())),
            })
        })
    }

    // ===================================================================
    // put / put_parts / get / remove / exists
    // ===================================================================

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
            let mut client = take_client(&inner)?;
            let result = client.put(&key, &data, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (key, values, config = None))]
    fn put_parts<'py>(
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
            let mut client = take_client(&inner)?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.put_parts(&key, &slices, cfg).await;
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
            let mut client = take_client(&inner)?;
            let result = client.get(&key).await;
            *inner.lock() = Some(client);
            let data = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                PyBytes::new(py, &data).unbind()
            })
        })
    }

    #[pyo3(signature = (key, force = false))]
    fn remove<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.remove(&key, force).await;
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
            let mut client = take_client(&inner)?;
            let result = client.exists(&key).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // batch operations
    // ===================================================================

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
            let mut client = take_client(&inner)?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.batch_put(&keys, &slices, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn batch_get<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.batch_get(&keys).await;
            *inner.lock() = Some(client);
            let results = result.map_err(to_py_err)?;
            let py = unsafe { Python::assume_attached() };
            let list = PyList::new(py, results.iter().map(|opt| {
                opt.as_ref().map_or(py.None(), |d| PyBytes::new(py, d).into())
            }));
            Ok(list.unbind())
        })
    }

    #[pyo3(signature = (keys, force = false))]
    fn batch_remove<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.batch_remove(&keys, force).await;
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
            let mut client = take_client(&inner)?;
            let result = client.batch_is_exist(&keys).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // prefetch
    // ===================================================================

    /// Prefetch a list of keys from the configured remote source (S3 / local FS).
    /// Fetched data is stored in the local hot cache.
    ///
    /// Use this BEFORE a training batch to warm the cache with keys you know
    /// will be accessed soon.
    fn prefetch<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.prefetch(&keys).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // remove_by_regex / remove_all / get_size
    // ===================================================================

    #[pyo3(signature = (pattern, force = false))]
    fn remove_by_regex<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        pattern: String,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let removed = client.remove_by_regex(&pattern, force).await;
            *inner.lock() = Some(client);
            removed.map_err(to_py_err)
        })
    }

    fn remove_all<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let removed = client.remove_all().await;
            *inner.lock() = Some(client);
            removed.map_err(to_py_err)
        })
    }

    fn get_size<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let size = client.get_size(&key).await;
            *inner.lock() = Some(client);
            size.map_err(to_py_err)
        })
    }

    // ===================================================================
    // health / tear down / close
    // ===================================================================

    fn get_hostname(&self) -> PyResult<String> {
        match self.inner.lock().as_ref() {
            Some(client) => Ok(client.get_hostname()),
            None => Err(to_py_err("client already closed")),
        }
    }

    fn health_check<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.health_check().await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn tear_down_all<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
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

    fn close(&self) {
        *self.inner.lock() = None;
        self.registered_py_buffers.lock().clear();
    }

    fn __repr__(&self) -> String {
        if self.inner.lock().is_some() {
            "MooncakeClient(connected)".to_string()
        } else {
            "MooncakeClient(closed)".to_string()
        }
    }

    // ===================================================================
    // upsert / upsert_parts
    // ===================================================================

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
            let mut client = take_client(&inner)?;
            let result = client.upsert(&key, &data, cfg).await;
            *inner.lock() = Some(client);
            let replicas = result.map_err(to_py_err)?;
            Ok(replicas_to_py(replicas))
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
            let mut client = take_client(&inner)?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.upsert_parts(&key, &slices, cfg).await;
            *inner.lock() = Some(client);
            let replicas = result.map_err(to_py_err)?;
            Ok(replicas_to_py(replicas))
        })
    }

    // ===================================================================
    // Task management
    // ===================================================================

    fn create_copy_task<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        targets: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
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
            let mut client = take_client(&inner)?;
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
        let task_id =
            Uuid::parse_str(&task_id_str).map_err(|e| to_py_err(format!("invalid UUID: {e}")))?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.query_task(task_id).await;
            *inner.lock() = Some(client);
            let resp = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                let d = PyDict::new(py);
                if let Some(id) = &resp.id {
                    d.set_item("task_id", Uuid::from_u64_pair(id.high, id.low).to_string())
                        .ok();
                }
                d.set_item("status", resp.status).ok();
                d.set_item("message", &resp.message).ok();
                d.unbind()
            })
        })
    }

    fn fetch_tasks<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        batch_size: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let tasks = client.fetch_tasks(batch_size).await.map_err(to_py_err)?;
            *inner.lock() = Some(client);
            let py = unsafe { Python::assume_attached() };
            let out: Vec<Py<PyAny>> = tasks
                .iter()
                    .map(|t| {
                        let d = PyDict::new(py);
                        if let Some(id) = &t.id {
                            d.set_item("id", Uuid::from_u64_pair(id.high, id.low).to_string())
                                .ok();
                        }
                        d.set_item("task_type", t.r#type).ok();
                        d.set_item("payload", &t.payload).ok();
                        d.set_item("created_at_ms_epoch", t.created_at_ms_epoch)
                            .ok();
                        d.set_item("max_retry_attempts", t.max_retry_attempts).ok();
                        d.into()
                    })
                    .collect();
            Ok(out.into_pyobject(py)?.unbind())
        })
    }

    #[pyo3(signature = (task_id_str, status, message = String::new()))]
    fn mark_task_to_complete<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        task_id_str: String,
        status: i32,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let task_id =
            Uuid::parse_str(&task_id_str).map_err(|e| to_py_err(format!("invalid UUID: {e}")))?;
        let inner = slf.borrow().inner.clone();

        let proto_status = mooncake_store_client::proto::TaskStatus::try_from(status)
            .map_err(|_| to_py_err(format!("invalid task status: {status}")))?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client
                .mark_task_to_complete(task_id, proto_status, &message)
                .await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Endpoint management
    // ===================================================================

    fn register_local_endpoint(&self, endpoint: String) -> PyResult<()> {
        match self.inner.lock().as_ref() {
            Some(client) => {
                client.register_local_endpoint(&endpoint);
                Ok(())
            }
            None => Err(to_py_err("client already closed")),
        }
    }

    fn unregister_local_endpoint(&self, endpoint: String) -> PyResult<()> {
        match self.inner.lock().as_ref() {
            Some(client) => {
                client.unregister_local_endpoint(&endpoint);
                Ok(())
            }
            None => Err(to_py_err("client already closed")),
        }
    }

    // ===================================================================
    // Zero-copy read (sync — uses block_on to work around non-Send futures)
    // ===================================================================

    fn get_into(slf: &Bound<'_, Self>, key: String, buffer: Bound<'_, PyAny>) -> PyResult<usize> {
        let (ptr, size) = get_buffer_ptr(&buffer)?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.get_into(&key, ptr, size) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn batch_get_into(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
    ) -> PyResult<Vec<i64>> {
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            return Err(to_py_err("keys, buffers, sizes must have same length"));
        }
        let ptrs: Vec<*mut c_void> = buffers
            .iter()
            .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
            .collect::<PyResult<_>>()?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_get_into(&keys, &ptrs, &sizes) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (keys, all_buffers, all_sizes, prefer_same_node = false))]
    fn batch_get_into_multi_buffers(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        all_buffers: Vec<Vec<Bound<'_, PyAny>>>,
        all_sizes: Vec<Vec<usize>>,
        prefer_same_node: bool,
    ) -> PyResult<Vec<Vec<i64>>> {
        let ptrs: Vec<Vec<*mut c_void>> = all_buffers
            .iter()
            .map(|bufs| {
                bufs.iter()
                    .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<_>>()?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe {
                client.batch_get_into_multi_buffers(&keys, &ptrs, &all_sizes, prefer_same_node)
            }
            .await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Zero-copy write (sync — uses block_on to work around non-Send futures)
    // ===================================================================

    #[pyo3(signature = (key, buffer, size, config = None))]
    fn put_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<()> {
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.put_from(&key, ptr, size, cfg) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    #[pyo3(signature = (keys, buffers, sizes, config = None))]
    fn batch_put_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Vec<i32>> {
        let ptrs: Vec<*mut c_void> = buffers
            .iter()
            .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
            .collect::<PyResult<_>>()?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_put_from(&keys, &ptrs, &sizes, cfg) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Buffer-based get
    // ===================================================================

    fn get_buffer<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.get_buffer(&key).await;
            *inner.lock() = Some(client);
            let bh = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                PyBytes::new(py, &bh.data).unbind()
            })
        })
    }

    fn batch_get_buffer<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.batch_get_buffer(&keys).await;
            *inner.lock() = Some(client);
            let results = result.map_err(to_py_err)?;
            let py = unsafe { Python::assume_attached() };
            let out: Vec<Option<Py<PyBytes>>> = results
                .into_iter()
                .map(|opt| opt.map(|bh| PyBytes::new(py, &bh.data).unbind()))
                .collect();
            Ok(out.into_pyobject(py).expect("batch_get_buffer into_pyobject").unbind())
        })
    }

    // ===================================================================
    // Buffer registration
    // ===================================================================

    fn register_buffer(
        slf: &Bound<'_, Self>,
        buffer: Bound<'_, PyAny>,
        size: usize,
        location: String,
    ) -> PyResult<()> {
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        {
            let slf_ref = slf.borrow();
            let guard = slf_ref.inner.lock();
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            unsafe { client.register_buffer(ptr, size, &location) }.map_err(to_py_err)?;
        }
        slf.borrow()
            .registered_py_buffers
            .lock()
            .push((ptr as usize, buffer.into_any().unbind()));
        Ok(())
    }

    fn unregister_buffer(slf: &Bound<'_, Self>, buffer: Bound<'_, PyAny>) -> PyResult<()> {
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        {
            let slf_ref = slf.borrow();
            let guard = slf_ref.inner.lock();
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            unsafe { client.unregister_buffer(ptr) }.map_err(to_py_err)?;
        }
        slf.borrow()
            .registered_py_buffers
            .lock()
            .retain(|(addr, _)| *addr != ptr as usize);
        Ok(())
    }

    // ===================================================================
    // Zero-copy upsert (sync — uses block_on)
    // ===================================================================

    #[pyo3(signature = (key, buffer, size, config = None))]
    fn upsert_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Py<PyAny>> {
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        let replicas = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.upsert_from(&key, ptr, size, cfg) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })?;
        Ok(replicas_to_py(replicas))
    }

    #[pyo3(signature = (keys, buffers, sizes, config = None))]
    fn batch_upsert_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Py<PyAny>> {
        let ptrs: Vec<*mut c_void> = buffers
            .iter()
            .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
            .collect::<PyResult<_>>()?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        let results = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_upsert_from(&keys, &ptrs, &sizes, cfg) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })?;
        let py = unsafe { Python::assume_attached() };
        let out: Vec<Py<PyAny>> = results
            .iter()
            .map(|replicas| replicas_to_py(replicas.clone()))
            .collect();
        Ok(out.into_pyobject(py)?.unbind())
    }

    // ===================================================================
    // Storage / offload
    // ===================================================================

    fn mount_local_disk_segment<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        enable_offloading: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.mount_local_disk_segment(enable_offloading).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn offload_object_heartbeat<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        enable_offloading: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.offload_object_heartbeat(enable_offloading).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn report_ssd_capacity<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        bytes: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.report_ssd_capacity(bytes).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn notify_offload_success<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        metadatas: Vec<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();
        let proto_metas: Vec<StorageObjectMetadata> = metadatas
            .iter()
            .map(|d| StorageObjectMetadata {
                bucket_id: d
                    .get_item("bucket_id")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                offset: d
                    .get_item("offset")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                key_size: d
                    .get_item("key_size")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                data_size: d
                    .get_item("data_size")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                transport_endpoint: d
                    .get_item("transport_endpoint")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<String>().ok())
                    .unwrap_or_default(),
            })
            .collect();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.notify_offload_success(keys, proto_metas).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Promotion
    // ===================================================================

    fn promotion_object_heartbeat<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.promotion_object_heartbeat().await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn promotion_alloc_start<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        size: i64,
        preferred_segments: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client
                .promotion_alloc_start(&key, size as u64, preferred_segments)
                .await;
            *inner.lock() = Some(client);
            let replica = result.map_err(to_py_err)?;
            Ok({
                let py = unsafe { Python::assume_attached() };
                let d = PyDict::new(py);
                d.set_item("segment_name", &replica.segment_name).ok();
                d.set_item("offset", replica.offset).ok();
                d.set_item("size", replica.size).ok();
                d.set_item("segment_id", replica.segment_id.to_string())
                    .ok();
                d.unbind()
            })
        })
    }

    fn notify_promotion_success<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.notify_promotion_success(&key).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    fn notify_promotion_failure<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.notify_promotion_failure(&key).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }
}

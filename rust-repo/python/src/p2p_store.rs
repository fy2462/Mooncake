use mooncake_p2p_store::{P2pStore, P2pStoreError, PayloadInfo};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use super::to_py_err;

#[pyclass(name = "P2pStore")]
pub(crate) struct P2pStorePy {
    inner: Arc<Mutex<Option<P2pStore>>>,
}

fn map_p2p_err(e: P2pStoreError) -> PyErr {
    to_py_err(format!("P2P Store error: {e:?}"))
}

#[pymethods]
impl P2pStorePy {
    #[staticmethod]
    fn create(
        metadata_conn_string: String,
        local_server_name: String,
        nic_priority_matrix: String,
    ) -> PyResult<Self> {
        let store = tokio::runtime::Handle::current()
            .block_on(async {
                P2pStore::new(
                    &metadata_conn_string,
                    &local_server_name,
                    &nic_priority_matrix,
                )
                .await
            })
            .map_err(map_p2p_err)?;

        Ok(P2pStorePy {
            inner: Arc::new(Mutex::new(Some(store))),
        })
    }

    fn get_local_server_name(&self) -> PyResult<String> {
        let guard = self.inner.lock();
        let store = guard
            .as_ref()
            .ok_or_else(|| to_py_err("P2pStore already closed"))?;
        store.get_local_server_name().map_err(map_p2p_err)
    }

    #[pyo3(signature = (name, addr_list, size_list, max_shard_size, location, force_create = false))]
    fn register(
        slf: &Bound<'_, Self>,
        name: String,
        addr_list: Vec<usize>,
        size_list: Vec<u64>,
        max_shard_size: u64,
        location: String,
        force_create: bool,
    ) -> PyResult<()> {
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store
                .register(
                    &name,
                    &addr_list,
                    &size_list,
                    max_shard_size,
                    &location,
                    force_create,
                )
                .await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })
    }

    fn unregister(slf: &Bound<'_, Self>, name: String) -> PyResult<()> {
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store.unregister(&name).await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })
    }

    fn list(slf: &Bound<'_, Self>, prefix: String) -> PyResult<Py<PyAny>> {
        let inner = slf.borrow().inner.clone();
        let payloads = tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store.list(&prefix).await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })?;
        Ok({
            let py = unsafe { Python::assume_attached() };
            let out: Vec<Py<PyAny>> = payloads.iter().map(|p| payload_info_to_py(py, p)).collect();
            out.into_pyobject(py).unwrap().unbind()
        })
    }

    fn get_replica(
        slf: &Bound<'_, Self>,
        name: String,
        addr_list: Vec<usize>,
        size_list: Vec<u64>,
    ) -> PyResult<()> {
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store.get_replica(&name, &addr_list, &size_list).await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })
    }

    fn close(&self) {
        *self.inner.lock() = None;
    }

    fn __repr__(&self) -> String {
        if self.inner.lock().is_some() {
            "P2pStore(connected)".to_string()
        } else {
            "P2pStore(closed)".to_string()
        }
    }
}

fn payload_info_to_py(py: Python<'_>, p: &PayloadInfo) -> Py<PyAny> {
    let d = PyDict::new(py);
    d.set_item("name", &p.name).ok();
    d.set_item("max_shard_size", p.max_shard_size).ok();
    d.set_item("total_size", p.total_size).ok();
    d.set_item("size_list", &p.size_list).ok();
    d.into()
}

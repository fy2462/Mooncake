use crate::to_py_err;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Mutex, MutexGuard};
use transfer_engine_ffi::{
    Opcode, OwnedBatchId as BatchId, TentEngine, TentIntent, TentPriority, TentRequestOptions,
    TentTransferRequest, TentTransport,
};

#[pyclass(name = "TransferIntent", eq, eq_int, from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyTransferIntent {
    Unspecified = 0,
    ForegroundGet = 1,
    BackgroundPrefetch = 2,
    Migration = 3,
    Checkpoint = 4,
    WeightLoading = 5,
    StagingInternal = 6,
}

impl From<PyTransferIntent> for TentIntent {
    fn from(value: PyTransferIntent) -> Self {
        TentIntent::try_from(value as i32).expect("Python intent values are validated by the enum")
    }
}

#[pyclass(name = "TransferPriority", eq, eq_int, from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyTransferPriority {
    High = 0,
    Medium = 1,
    Low = 2,
}

impl From<PyTransferPriority> for TentPriority {
    fn from(value: PyTransferPriority) -> Self {
        TentPriority::try_from(value as i32)
            .expect("Python priority values are validated by the enum")
    }
}

#[pyclass(name = "TransferRequest", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTransferRequest {
    request: TentTransferRequest,
}

#[pymethods]
impl PyTransferRequest {
    #[new]
    #[pyo3(signature = (
        opcode,
        source,
        target_id,
        target_offset,
        length,
        *,
        priority = PyTransferPriority::High,
        transport_hint = 0,
        policy_name = None,
        deadline_ns = 0,
        intent_type = PyTransferIntent::Unspecified
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        opcode: i32,
        source: usize,
        target_id: u64,
        target_offset: u64,
        length: u64,
        priority: PyTransferPriority,
        transport_hint: i32,
        policy_name: Option<String>,
        deadline_ns: u64,
        intent_type: PyTransferIntent,
    ) -> PyResult<Self> {
        let opcode = Opcode::from_i32(opcode)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("opcode must be 0 or 1"))?;
        let transport = TentTransport::try_from(transport_hint).map_err(|value| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "unsupported TENT transport hint: {value}"
            ))
        })?;
        if policy_name
            .as_deref()
            .is_some_and(|name| name.as_bytes().contains(&0))
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "policy_name cannot contain NUL bytes",
            ));
        }
        Ok(Self {
            // SAFETY: construction only records the foreign address. The
            // expert Python caller must keep it registered and alive through
            // the terminal transfer state before submitting this request.
            request: unsafe {
                TentTransferRequest::from_raw_parts(
                    opcode,
                    source as *mut c_void,
                    target_id,
                    target_offset,
                    length,
                    TentRequestOptions {
                        priority: priority.into(),
                        transport,
                        policy_name,
                        deadline_ns,
                        intent: intent_type.into(),
                    },
                )
            },
        })
    }

    #[getter]
    fn deadline_ns(&self) -> u64 {
        self.request.options.deadline_ns
    }

    #[getter]
    fn policy_name(&self) -> Option<&str> {
        self.request.options.policy_name.as_deref()
    }
}

impl PyTransferRequest {
    fn to_native(&self) -> TentTransferRequest {
        self.request.clone()
    }
}

#[pyclass(name = "TentMetricsStatus", frozen)]
pub struct PyTentMetricsStatus {
    #[pyo3(get)]
    pub tent_available: bool,
    #[pyo3(get)]
    pub metrics_enabled: bool,
    #[pyo3(get)]
    pub metrics_initialized: bool,
    #[pyo3(get)]
    pub http_port: Option<u16>,
    #[pyo3(get)]
    pub log_only: bool,
}

#[pyclass(name = "TransferStatus", frozen)]
pub struct PyTransferStatus {
    #[pyo3(get)]
    pub status: i32,
    #[pyo3(get)]
    pub transferred_bytes: u64,
}

#[pyclass(name = "TransferEngine")]
pub struct PyTransferEngine {
    inner: TentEngine,
    batches: Mutex<HashMap<u64, BatchId>>,
}

impl PyTransferEngine {
    fn lock_batches(&self) -> PyResult<MutexGuard<'_, HashMap<u64, BatchId>>> {
        self.batches
            .lock()
            .map_err(|_| PyRuntimeError::new_err("transfer batch registry is poisoned"))
    }

    fn unknown_batch(batch_id: u64) -> PyErr {
        PyValueError::new_err(format!("unknown or released transfer batch {batch_id}"))
    }
}

#[pymethods]
impl PyTransferEngine {
    #[new]
    #[pyo3(signature = (config_path = None, config_overrides = None))]
    fn new(
        config_path: Option<&str>,
        config_overrides: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let config_overrides = config_overrides.unwrap_or_default();
        let overrides = config_overrides
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        Ok(Self {
            inner: TentEngine::create(config_path, &overrides).map_err(to_py_err)?,
            batches: Mutex::new(HashMap::new()),
        })
    }

    fn available(&self) -> bool {
        self.inner.available()
    }

    fn open_segment(&self, segment_name: &str) -> PyResult<u64> {
        self.inner.open_segment(segment_name).map_err(to_py_err)
    }

    fn close_segment(&self, segment_id: u64) -> PyResult<()> {
        self.inner.close_segment(segment_id).map_err(to_py_err)
    }

    fn register_memory(&self, address: usize, size: usize) -> PyResult<()> {
        // SAFETY: this expert Python API accepts a foreign allocation address;
        // the Python caller owns its lifetime through explicit unregister.
        unsafe { self.inner.register_foreign_memory(address, size) }.map_err(to_py_err)
    }

    fn unregister_memory(&self, address: usize, size: usize) -> PyResult<()> {
        // SAFETY: the Python caller must pass the same live, idle registration
        // previously supplied to `register_memory`.
        unsafe { self.inner.unregister_foreign_memory(address, size) }.map_err(to_py_err)
    }

    fn allocate_batch_id(&self, batch_size: usize) -> PyResult<u64> {
        let mut batch = self.inner.allocate_batch(batch_size).map_err(to_py_err)?;
        let batch_id = batch.as_raw();
        let mut batches = self.lock_batches()?;
        if batches.contains_key(&batch_id) {
            drop(batches);
            let _ = self.inner.free_batch(&mut batch);
            return Err(PyRuntimeError::new_err(format!(
                "native transfer engine reused active batch id {batch_id}"
            )));
        }
        batches.insert(batch_id, batch);
        Ok(batch_id)
    }

    fn submit_transfer(
        &self,
        py: Python<'_>,
        batch_id: u64,
        requests: Vec<Py<PyTransferRequest>>,
    ) -> PyResult<()> {
        let requests = requests
            .iter()
            .map(|request| request.borrow(py).to_native())
            .collect::<Vec<_>>();
        py.detach(|| {
            let batches = self.lock_batches()?;
            let batch = batches
                .get(&batch_id)
                .ok_or_else(|| Self::unknown_batch(batch_id))?;
            // SAFETY: this expert Python API deals in foreign addresses. The
            // caller must keep every registered owner alive until terminal.
            unsafe { self.inner.submit_transfer(batch, &requests) }.map_err(to_py_err)
        })
    }

    fn cancel_transfer(&self, py: Python<'_>, batch_id: u64, task_id: usize) -> PyResult<()> {
        py.detach(|| {
            let batches = self.lock_batches()?;
            let batch = batches
                .get(&batch_id)
                .ok_or_else(|| Self::unknown_batch(batch_id))?;
            self.inner
                .cancel_transfer(batch, task_id)
                .map_err(to_py_err)
        })
    }

    fn get_transfer_status(&self, batch_id: u64, task_id: usize) -> PyResult<PyTransferStatus> {
        let batches = self.lock_batches()?;
        let batch = batches
            .get(&batch_id)
            .ok_or_else(|| Self::unknown_batch(batch_id))?;
        let status = self
            .inner
            .transfer_status(batch, task_id)
            .map_err(to_py_err)?;
        Ok(PyTransferStatus {
            status: status.status as i32,
            transferred_bytes: status.transferred_bytes as u64,
        })
    }

    fn tent_metrics_status(&self) -> PyResult<PyTentMetricsStatus> {
        let status = self.inner.metrics_status().map_err(to_py_err)?;
        Ok(PyTentMetricsStatus {
            tent_available: status.tent_available,
            metrics_enabled: status.metrics_enabled,
            metrics_initialized: status.metrics_initialized,
            http_port: status.http_port,
            log_only: status.is_log_only(),
        })
    }

    fn free_batch_id(&self, batch_id: u64) -> PyResult<()> {
        let mut batches = self.lock_batches()?;
        let batch = batches
            .get_mut(&batch_id)
            .ok_or_else(|| Self::unknown_batch(batch_id))?;
        self.inner.free_batch(batch).map_err(to_py_err)?;
        batches.remove(&batch_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_request_defaults_match_legacy_submission() {
        let request = PyTransferRequest::new(
            0,
            0x1000,
            7,
            64,
            4096,
            PyTransferPriority::High,
            0,
            None,
            0,
            PyTransferIntent::Unspecified,
        )
        .unwrap();
        assert!(request.request.options.is_legacy_compatible());
    }

    #[test]
    fn python_request_rejects_invalid_transport_and_policy() {
        assert!(
            PyTransferRequest::new(
                0,
                0x1000,
                7,
                0,
                1,
                PyTransferPriority::High,
                99,
                None,
                0,
                PyTransferIntent::Unspecified,
            )
            .is_err()
        );
        assert!(
            PyTransferRequest::new(
                0,
                0x1000,
                7,
                0,
                1,
                PyTransferPriority::High,
                0,
                Some("bad\0policy".to_string()),
                0,
                PyTransferIntent::Unspecified,
            )
            .is_err()
        );
    }
}

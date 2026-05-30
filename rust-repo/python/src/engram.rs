use super::to_py_err;
use crate::client::PythonMooncakeClient;
use mooncake_store_client::{EngramStore, EngramStoreConfig, MooncakeClient};
use parking_lot::Mutex;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(name = "EngramStoreConfig", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct EngramStoreConfigPy {
    #[pyo3(get, set)]
    pub table_vocab_sizes: Vec<i64>,
    #[pyo3(get, set)]
    pub embedding_dim: usize,
    #[pyo3(get, set)]
    pub buffer_location: String,
}

#[pymethods]
impl EngramStoreConfigPy {
    #[new]
    #[pyo3(signature = (
        table_vocab_sizes = vec![1024],
        embedding_dim = 64,
        buffer_location = "cpu:0".to_string(),
    ))]
    fn new(table_vocab_sizes: Vec<i64>, embedding_dim: usize, buffer_location: String) -> Self {
        Self {
            table_vocab_sizes,
            embedding_dim,
            buffer_location,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "EngramStoreConfig(tables={}, dim={}, location='{}')",
            self.table_vocab_sizes.len(),
            self.embedding_dim,
            self.buffer_location
        )
    }
}

impl EngramStoreConfigPy {
    fn to_core(&self) -> EngramStoreConfig {
        EngramStoreConfig {
            table_vocab_sizes: self.table_vocab_sizes.clone(),
            embedding_dim: self.embedding_dim,
            buffer_location: self.buffer_location.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// EngramStorePy
//
// Note: async lookup/populate methods are not yet exposed because the
// EngramClient trait returns non-Send futures (raw pointers captured across
// await points). These will be added when the trait is refactored.
// ---------------------------------------------------------------------------

#[pyclass(name = "EngramStore")]
pub(crate) struct EngramStorePy {
    inner: Arc<Mutex<Option<EngramStore<MooncakeClient>>>>,
    num_heads: usize,
    embedding_dim: usize,
    embed_keys: Vec<String>,
}

#[pymethods]
impl EngramStorePy {
    #[staticmethod]
    #[pyo3(signature = (layer_id, config, client))]
    fn new(
        layer_id: i32,
        config: Bound<'_, EngramStoreConfigPy>,
        client: Bound<'_, PythonMooncakeClient>,
    ) -> PyResult<Self> {
        let cfg = config.borrow().to_core();
        let inner_client = {
            let client_ref = client.borrow();
            let mut guard = client_ref.inner.lock();
            guard
                .take()
                .ok_or_else(|| to_py_err("MooncakeClient already closed or consumed"))?
        };
        let num_heads = cfg.table_vocab_sizes.len();
        let embedding_dim = cfg.embedding_dim;

        let store = EngramStore::new(layer_id, cfg, inner_client).map_err(to_py_err)?;
        let embed_keys = store.get_store_keys().to_vec();

        // Mark the source client as consumed
        client.borrow().registered_py_buffers.lock().clear();

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(store))),
            num_heads,
            embedding_dim,
            embed_keys,
        })
    }

    /// Extract the MooncakeClient back out. EngramStore is consumed.
    fn into_inner(&self) -> PyResult<PythonMooncakeClient> {
        let store = self
            .inner
            .lock()
            .take()
            .ok_or_else(|| to_py_err("EngramStore already closed"))?;
        let client = store.into_inner();
        Ok(PythonMooncakeClient {
            inner: Arc::new(Mutex::new(Some(client))),
            registered_py_buffers: Arc::new(Mutex::new(Vec::new())),
        })
    }

    // -- accessors --

    #[getter]
    fn num_heads(&self) -> usize {
        self.num_heads
    }

    #[getter]
    fn embedding_dim(&self) -> usize {
        self.embedding_dim
    }

    #[getter]
    fn store_keys(&self) -> Vec<String> {
        self.embed_keys.clone()
    }

    fn __repr__(&self) -> String {
        if self.inner.lock().is_some() {
            format!(
                "EngramStore(heads={}, dim={})",
                self.num_heads, self.embedding_dim
            )
        } else {
            "EngramStore(closed)".to_string()
        }
    }
}

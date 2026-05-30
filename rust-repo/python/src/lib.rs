mod client;
mod engram;
mod p2p_store;
pub mod remote_config;
mod replicate_config;

use pyo3::prelude::*;
use std::sync::OnceLock;

pyo3::create_exception!(
    mooncake_store,
    StoreErrorPy,
    pyo3::exceptions::PyException,
    "Mooncake Store error"
);

fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    StoreErrorPy::new_err(e.to_string())
}

// ---------------------------------------------------------------------------
// Tracing initialization (callable from Python for debugging)
// ---------------------------------------------------------------------------

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Enable TE debug tracing. Call once before any MooncakeClient operations.
/// Logs are written to stderr with the `te_debug` target.
///
/// From Python:
///     import _mooncake_store
///     _mooncake_store.enable_te_debug_tracing()
#[pyfunction]
fn enable_te_debug_tracing() {
    TRACING_INIT.get_or_init(|| {
        use tracing_subscriber::fmt::format::FmtSpan;
        use tracing_subscriber::EnvFilter;
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("te_debug=info")),
            )
            .init();
    });
}

// ---------------------------------------------------------------------------
// Module init
// ---------------------------------------------------------------------------

#[pymodule]
fn _mooncake_store(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<client::PythonMooncakeClient>()?;
    m.add_class::<replicate_config::ReplicateConfigPy>()?;
    m.add_class::<remote_config::PyS3Config>()?;
    m.add_class::<remote_config::PyRemoteSourceConfig>()?;
    m.add_class::<engram::EngramStoreConfigPy>()?;
    m.add_class::<engram::EngramStorePy>()?;
    m.add_class::<p2p_store::P2pStorePy>()?;
    m.add("StoreError", m.py().get_type::<StoreErrorPy>())?;
    m.add_function(wrap_pyfunction!(enable_te_debug_tracing, m)?)?;
    Ok(())
}

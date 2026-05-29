mod client;
mod engram;
mod p2p_store;
mod remote_config;
mod replicate_config;

use pyo3::prelude::*;

pyo3::create_exception!(
    mooncake_store,
    StoreErrorPy,
    pyo3::exceptions::PyException,
    "Mooncake Store error"
);

fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    StoreErrorPy::new_err(e.to_string())
}

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
    Ok(())
}

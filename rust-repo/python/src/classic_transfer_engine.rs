use crate::to_py_err;
use mooncake_store_client::InitializedTransferEngine;
use pyo3::prelude::*;
use std::sync::Arc;

/// Caller-owned classic Transfer Engine handle for Store client injection.
///
/// This is intentionally distinct from the optional TENT `TransferEngine`
/// Python class: Store parity requires the classic transfer-engine-ffi ABI.
#[pyclass(name = "ClassicTransferEngine", frozen)]
pub struct PyClassicTransferEngine {
    pub(crate) inner: Arc<InitializedTransferEngine>,
}

#[pymethods]
impl PyClassicTransferEngine {
    #[new]
    #[pyo3(signature = (metadata_server, local_hostname, protocol = String::from("tcp"), device = String::new()))]
    fn new(
        metadata_server: String,
        local_hostname: String,
        protocol: String,
        device: String,
    ) -> PyResult<Self> {
        parse_explicit_endpoint(&local_hostname)?;
        let topology = (protocol != "tcp" && !device.is_empty()).then_some(device.as_str());
        let engine = InitializedTransferEngine::create(
            &metadata_server,
            &local_hostname,
            &protocol,
            topology,
        )
        .map_err(to_py_err)?;
        Ok(Self {
            inner: Arc::new(engine),
        })
    }
}

fn parse_explicit_endpoint(endpoint: &str) -> PyResult<(String, u16)> {
    let endpoint = endpoint.trim();
    let (host, port) = if let Some(bracketed) = endpoint.strip_prefix('[') {
        let (host, suffix) = bracketed
            .split_once(']')
            .ok_or_else(|| to_py_err("invalid bracketed local_hostname"))?;
        let port = suffix
            .strip_prefix(':')
            .ok_or_else(|| to_py_err("local_hostname must include an explicit port"))?;
        (host, port)
    } else {
        endpoint
            .rsplit_once(':')
            .ok_or_else(|| to_py_err("local_hostname must include an explicit port"))?
    };
    if host.is_empty() {
        return Err(to_py_err("local_hostname must include a non-empty host"));
    }
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| to_py_err("local_hostname contains an invalid explicit port"))?;
    Ok((host.to_string(), port))
}

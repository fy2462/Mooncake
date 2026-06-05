use crate::to_py_err;
use mooncake_store_client::DummyIpcChannel;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass(name = "MooncakeDummyIpcChannel")]
pub(crate) struct PythonMooncakeDummyIpcChannel {
    inner: DummyIpcChannel,
}

#[pymethods]
impl PythonMooncakeDummyIpcChannel {
    #[staticmethod]
    fn connect(socket_path: String) -> PyResult<Self> {
        let inner = DummyIpcChannel::connect(socket_path).map_err(to_py_err)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    fn connect_abstract(socket_name: String) -> PyResult<Self> {
        let inner = DummyIpcChannel::connect_abstract(&socket_name).map_err(to_py_err)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    #[pyo3(signature = (socket_path, unlink_existing = true))]
    fn listen_once(socket_path: String, unlink_existing: bool) -> PyResult<Self> {
        let inner =
            DummyIpcChannel::listen_once(socket_path, unlink_existing).map_err(to_py_err)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    fn listen_abstract_once(socket_name: String) -> PyResult<Self> {
        let inner = DummyIpcChannel::listen_abstract_once(&socket_name).map_err(to_py_err)?;
        Ok(Self { inner })
    }

    fn send_fd(&self, fd: i32) -> PyResult<()> {
        self.inner.send_fd(fd).map_err(to_py_err)
    }

    fn recv_fd(&self) -> PyResult<i32> {
        self.inner.recv_fd().map_err(to_py_err)
    }

    fn send_bytes(&mut self, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        self.inner.send_bytes(data.as_bytes()).map_err(to_py_err)
    }

    fn recv_bytes<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let data = self.inner.recv_bytes().map_err(to_py_err)?;
        Ok(PyBytes::new(py, &data))
    }
}

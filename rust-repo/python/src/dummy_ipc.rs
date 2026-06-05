use crate::to_py_err;
use mooncake_store_client::{
    DummyIpcChannel, ShmRegisterRequest, INVALID_PHYSICAL_DEVICE_ID, IPC_SHM_FD_REQUEST,
    IPC_SHM_REGISTER, SHM_SEG_HOT_CACHE,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::mem;

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

    fn send_raw(&mut self, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        self.inner.send_raw(data.as_bytes()).map_err(to_py_err)
    }

    fn recv_exact<'py>(&mut self, py: Python<'py>, len: usize) -> PyResult<Bound<'py, PyBytes>> {
        let data = self.inner.recv_exact(len).map_err(to_py_err)?;
        Ok(PyBytes::new(py, &data))
    }

    fn send_fd_with_bytes(&self, fd: i32, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        self.inner
            .send_fd_with_bytes(fd, data.as_bytes())
            .map_err(to_py_err)
    }

    fn recv_fd_with_bytes<'py>(
        &self,
        py: Python<'py>,
        len: usize,
    ) -> PyResult<(i32, Bound<'py, PyBytes>)> {
        let (fd, data) = self.inner.recv_fd_with_bytes(len).map_err(to_py_err)?;
        Ok((fd, PyBytes::new(py, &data)))
    }

    fn send_request_type(&mut self, request_type: u32) -> PyResult<()> {
        self.inner
            .send_request_type(request_type)
            .map_err(to_py_err)
    }

    fn send_bytes(&mut self, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        self.inner.send_bytes(data.as_bytes()).map_err(to_py_err)
    }

    fn recv_bytes<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let data = self.inner.recv_bytes().map_err(to_py_err)?;
        Ok(PyBytes::new(py, &data))
    }

    #[staticmethod]
    #[pyo3(signature = (socket_name, fd, client_id_first, client_id_second, dummy_base_addr, shm_size, is_local_buffer, device_id = INVALID_PHYSICAL_DEVICE_ID))]
    fn register_shm_fd(
        socket_name: String,
        fd: i32,
        client_id_first: u64,
        client_id_second: u64,
        dummy_base_addr: u64,
        shm_size: u64,
        is_local_buffer: bool,
        device_id: i32,
    ) -> PyResult<i32> {
        let request = ShmRegisterRequest {
            client_id_first,
            client_id_second,
            dummy_base_addr,
            shm_size,
            device_id,
            is_local_buffer,
        };
        DummyIpcChannel::register_shm_fd(&socket_name, fd, request).map_err(to_py_err)
    }

    #[staticmethod]
    fn request_hot_cache_fd(
        socket_name: String,
        client_id_first: u64,
        client_id_second: u64,
    ) -> PyResult<(i32, u64)> {
        let (fd, response) =
            DummyIpcChannel::request_hot_cache_fd(&socket_name, client_id_first, client_id_second)
                .map_err(to_py_err)?;
        Ok((fd, response.shm_size))
    }

    #[staticmethod]
    fn ipc_shm_register() -> u32 {
        IPC_SHM_REGISTER
    }

    #[staticmethod]
    fn ipc_shm_fd_request() -> u32 {
        IPC_SHM_FD_REQUEST
    }

    #[staticmethod]
    fn shm_seg_hot_cache() -> u32 {
        SHM_SEG_HOT_CACHE
    }

    #[staticmethod]
    fn shm_register_request_size() -> usize {
        mem::size_of::<ShmRegisterRequest>()
    }
}

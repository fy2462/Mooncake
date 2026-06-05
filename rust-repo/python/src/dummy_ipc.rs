use crate::to_py_err;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::fs;
use std::io::{Read, Write};
use std::mem;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;

#[pyclass(name = "MooncakeDummyIpcChannel")]
pub(crate) struct PythonMooncakeDummyIpcChannel {
    stream: UnixStream,
}

fn send_fd(stream: &UnixStream, fd: RawFd) -> PyResult<()> {
    let byte = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: byte.as_ptr() as *mut libc::c_void,
        iov_len: byte.len(),
    };
    let mut control = vec![0u8; unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as _) as usize }];
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = control.len() as _;

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(to_py_err("failed to build fd control message"));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as _) as _;
        let data = libc::CMSG_DATA(cmsg) as *mut RawFd;
        *data = fd;
        msg.msg_controllen = (*cmsg).cmsg_len as _;

        if libc::sendmsg(stream.as_raw_fd(), &msg, 0) < 0 {
            return Err(to_py_err(std::io::Error::last_os_error()));
        }
    }
    Ok(())
}

fn recv_fd(stream: &UnixStream) -> PyResult<RawFd> {
    let mut byte = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr() as *mut libc::c_void,
        iov_len: byte.len(),
    };
    let mut control = vec![0u8; unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as _) as usize }];
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = control.len() as _;

    unsafe {
        if libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) < 0 {
            return Err(to_py_err(std::io::Error::last_os_error()));
        }
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null()
            || (*cmsg).cmsg_level != libc::SOL_SOCKET
            || (*cmsg).cmsg_type != libc::SCM_RIGHTS
        {
            return Err(to_py_err("message did not contain a file descriptor"));
        }
        Ok(*(libc::CMSG_DATA(cmsg) as *const RawFd))
    }
}

#[pymethods]
impl PythonMooncakeDummyIpcChannel {
    #[staticmethod]
    fn connect(socket_path: String) -> PyResult<Self> {
        let stream = UnixStream::connect(socket_path).map_err(to_py_err)?;
        Ok(Self { stream })
    }

    #[staticmethod]
    #[pyo3(signature = (socket_path, unlink_existing = true))]
    fn listen_once(socket_path: String, unlink_existing: bool) -> PyResult<Self> {
        if unlink_existing && Path::new(&socket_path).exists() {
            fs::remove_file(&socket_path).map_err(to_py_err)?;
        }
        let listener = UnixListener::bind(socket_path).map_err(to_py_err)?;
        let (stream, _) = listener.accept().map_err(to_py_err)?;
        Ok(Self { stream })
    }

    fn send_fd(&self, fd: i32) -> PyResult<()> {
        send_fd(&self.stream, fd)
    }

    fn recv_fd(&self) -> PyResult<i32> {
        recv_fd(&self.stream)
    }

    fn send_bytes(&mut self, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        let bytes = data.as_bytes();
        let len = (bytes.len() as u64).to_be_bytes();
        self.stream.write_all(&len).map_err(to_py_err)?;
        self.stream.write_all(bytes).map_err(to_py_err)
    }

    fn recv_bytes<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let mut len_bytes = [0u8; 8];
        self.stream.read_exact(&mut len_bytes).map_err(to_py_err)?;
        let len = u64::from_be_bytes(len_bytes) as usize;
        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data).map_err(to_py_err)?;
        Ok(PyBytes::new(py, &data))
    }
}

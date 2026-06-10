use super::{DummyClientError, DummyClientResult};
#[cfg(target_os = "linux")]
use std::mem;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream;

#[cfg(target_os = "linux")]
fn abstract_socket_addr(name: &str) -> DummyClientResult<(libc::sockaddr_un, libc::socklen_t)> {
    let bytes = name.as_bytes();
    let path_len = unsafe { mem::zeroed::<libc::sockaddr_un>() }.sun_path.len();
    if bytes.is_empty() {
        return Err(DummyClientError::InvalidInput(
            "abstract socket name must not be empty".to_string(),
        ));
    }
    if bytes.len() + 1 > path_len {
        return Err(DummyClientError::InvalidInput(format!(
            "abstract socket name too long: {} bytes",
            bytes.len()
        )));
    }

    let mut addr: libc::sockaddr_un = unsafe { mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    addr.sun_path[0] = 0;
    for (idx, byte) in bytes.iter().enumerate() {
        addr.sun_path[idx + 1] = *byte as libc::c_char;
    }
    let len = (mem::size_of::<libc::sa_family_t>() + 1 + bytes.len()) as libc::socklen_t;
    Ok((addr, len))
}

#[cfg(target_os = "linux")]
pub(super) fn connect_abstract_socket(name: &str) -> DummyClientResult<UnixStream> {
    let (addr, len) = abstract_socket_addr(name)?;
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let rc = unsafe { libc::connect(fd, &addr as *const _ as *const libc::sockaddr, len) };
    if rc < 0 {
        let err = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(err.into());
    }
    Ok(unsafe { UnixStream::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
pub(super) fn listen_abstract_socket_once(name: &str) -> DummyClientResult<UnixStream> {
    let (addr, len) = abstract_socket_addr(name)?;
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let bind_rc = unsafe { libc::bind(fd, &addr as *const _ as *const libc::sockaddr, len) };
    if bind_rc < 0 {
        let err = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(err.into());
    }
    if unsafe { libc::listen(fd, 1) } < 0 {
        let err = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(err.into());
    }
    let client_fd = unsafe { libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut()) };
    let accept_err = std::io::Error::last_os_error();
    unsafe {
        libc::close(fd);
    }
    if client_fd < 0 {
        return Err(accept_err.into());
    }
    Ok(unsafe { UnixStream::from_raw_fd(client_fd) })
}

#[cfg(not(target_os = "linux"))]
pub(super) fn connect_abstract_socket(_name: &str) -> DummyClientResult<UnixStream> {
    Err(DummyClientError::InvalidInput(
        "Linux abstract Unix sockets are not supported on this platform".to_string(),
    ))
}

#[cfg(not(target_os = "linux"))]
pub(super) fn listen_abstract_socket_once(_name: &str) -> DummyClientResult<UnixStream> {
    Err(DummyClientError::InvalidInput(
        "Linux abstract Unix sockets are not supported on this platform".to_string(),
    ))
}

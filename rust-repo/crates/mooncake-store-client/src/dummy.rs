use parking_lot::Mutex;
use std::collections::HashMap;
use std::ffi::c_void;
use std::fs;
use std::io::{Read, Write};
use std::mem;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DummyClientError {
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type DummyClientResult<T> = Result<T, DummyClientError>;

pub struct DummyMemoryPool {
    mem_pool: Vec<u8>,
    next_offset: Mutex<usize>,
    allocations: Mutex<HashMap<u64, usize>>,
}

impl DummyMemoryPool {
    pub fn new(mem_pool_size: usize) -> DummyClientResult<Self> {
        if mem_pool_size == 0 {
            return Err(DummyClientError::InvalidInput(
                "mem_pool_size must be greater than 0".to_string(),
            ));
        }
        Ok(Self {
            mem_pool: vec![0; mem_pool_size],
            next_offset: Mutex::new(0),
            allocations: Mutex::new(HashMap::new()),
        })
    }

    pub fn base_ptr(&self) -> *mut c_void {
        self.mem_pool.as_ptr() as *mut c_void
    }

    pub fn len(&self) -> usize {
        self.mem_pool.len()
    }

    pub fn allocations_len(&self) -> usize {
        self.allocations.lock().len()
    }

    pub fn reset(&self) {
        self.allocations.lock().clear();
        *self.next_offset.lock() = 0;
    }

    pub fn alloc(&self, size: usize) -> DummyClientResult<u64> {
        if size == 0 {
            return Err(DummyClientError::InvalidInput(
                "allocation size must be greater than 0".to_string(),
            ));
        }
        let align = 64usize;
        let mut next = self.next_offset.lock();
        let aligned = (*next + align - 1) & !(align - 1);
        let end = aligned
            .checked_add(size)
            .ok_or_else(|| DummyClientError::InvalidInput("allocation size overflow".into()))?;
        if end > self.mem_pool.len() {
            return Err(DummyClientError::InvalidInput(
                "dummy memory pool exhausted".to_string(),
            ));
        }
        *next = end;
        let addr = unsafe { self.mem_pool.as_ptr().add(aligned) } as u64;
        self.allocations.lock().insert(addr, size);
        Ok(addr)
    }

    pub fn checked_ptr(&self, addr: u64, size: usize) -> DummyClientResult<*mut c_void> {
        let alloc_size = self.allocations.lock().get(&addr).copied().ok_or_else(|| {
            DummyClientError::InvalidInput(format!("unknown dummy address: {addr:#x}"))
        })?;
        if size > alloc_size {
            return Err(DummyClientError::InvalidInput(format!(
                "dummy buffer too small: requested={size}, allocated={alloc_size}"
            )));
        }
        Ok(addr as usize as *mut c_void)
    }

    pub fn checked_ptrs(
        &self,
        addrs: &[u64],
        sizes: &[usize],
    ) -> DummyClientResult<Vec<*mut c_void>> {
        if addrs.len() != sizes.len() {
            return Err(DummyClientError::InvalidInput(
                "addresses and sizes must have same length".to_string(),
            ));
        }
        addrs
            .iter()
            .zip(sizes.iter())
            .map(|(&addr, &size)| self.checked_ptr(addr, size))
            .collect()
    }

    pub fn write(&self, addr: u64, data: &[u8]) -> DummyClientResult<()> {
        let ptr = self.checked_ptr(addr, data.len())? as *mut u8;
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
        }
        Ok(())
    }

    pub fn read(&self, addr: u64, size: usize) -> DummyClientResult<Vec<u8>> {
        let ptr = self.checked_ptr(addr, size)? as *const u8;
        let data = unsafe { std::slice::from_raw_parts(ptr, size) };
        Ok(data.to_vec())
    }
}

pub struct DummyIpcChannel {
    stream: UnixStream,
}

impl DummyIpcChannel {
    pub fn connect(socket_path: impl AsRef<Path>) -> DummyClientResult<Self> {
        let stream = UnixStream::connect(socket_path)?;
        Ok(Self { stream })
    }

    pub fn listen_once(
        socket_path: impl AsRef<Path>,
        unlink_existing: bool,
    ) -> DummyClientResult<Self> {
        let path = socket_path.as_ref();
        if unlink_existing && path.exists() {
            fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        let (stream, _) = listener.accept()?;
        Ok(Self { stream })
    }

    pub fn connect_abstract(socket_name: &str) -> DummyClientResult<Self> {
        let stream = connect_abstract_socket(socket_name)?;
        Ok(Self { stream })
    }

    pub fn listen_abstract_once(socket_name: &str) -> DummyClientResult<Self> {
        let stream = listen_abstract_socket_once(socket_name)?;
        Ok(Self { stream })
    }

    pub fn send_fd(&self, fd: RawFd) -> DummyClientResult<()> {
        send_fd(&self.stream, fd)
    }

    pub fn recv_fd(&self) -> DummyClientResult<RawFd> {
        recv_fd(&self.stream)
    }

    pub fn send_bytes(&mut self, data: &[u8]) -> DummyClientResult<()> {
        let len = (data.len() as u64).to_be_bytes();
        self.stream.write_all(&len)?;
        self.stream.write_all(data)?;
        Ok(())
    }

    pub fn recv_bytes(&mut self) -> DummyClientResult<Vec<u8>> {
        let mut len_bytes = [0u8; 8];
        self.stream.read_exact(&mut len_bytes)?;
        let len = u64::from_be_bytes(len_bytes) as usize;
        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data)?;
        Ok(data)
    }
}

fn send_fd(stream: &UnixStream, fd: RawFd) -> DummyClientResult<()> {
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
            return Err(DummyClientError::InvalidInput(
                "failed to build fd control message".to_string(),
            ));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as _) as _;
        let data = libc::CMSG_DATA(cmsg) as *mut RawFd;
        *data = fd;

        if libc::sendmsg(stream.as_raw_fd(), &msg, 0) < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(())
}

fn recv_fd(stream: &UnixStream) -> DummyClientResult<RawFd> {
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
            return Err(std::io::Error::last_os_error().into());
        }
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null()
            || (*cmsg).cmsg_level != libc::SOL_SOCKET
            || (*cmsg).cmsg_type != libc::SCM_RIGHTS
        {
            return Err(DummyClientError::InvalidInput(
                "message did not contain a file descriptor".to_string(),
            ));
        }
        Ok(*(libc::CMSG_DATA(cmsg) as *const RawFd))
    }
}

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
fn connect_abstract_socket(name: &str) -> DummyClientResult<UnixStream> {
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
fn listen_abstract_socket_once(name: &str) -> DummyClientResult<UnixStream> {
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
fn connect_abstract_socket(_name: &str) -> DummyClientResult<UnixStream> {
    Err(DummyClientError::InvalidInput(
        "Linux abstract Unix sockets are not supported on this platform".to_string(),
    ))
}

#[cfg(not(target_os = "linux"))]
fn listen_abstract_socket_once(_name: &str) -> DummyClientResult<UnixStream> {
    Err(DummyClientError::InvalidInput(
        "Linux abstract Unix sockets are not supported on this platform".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::FromRawFd;

    #[test]
    fn memory_pool_allocates_and_checks_bounds() {
        let pool = DummyMemoryPool::new(128).unwrap();
        let addr = pool.alloc(16).unwrap();
        pool.write(addr, b"hello").unwrap();
        assert_eq!(pool.read(addr, 5).unwrap(), b"hello");
        assert!(pool.checked_ptr(addr, 17).is_err());
        assert!(pool.alloc(10_000).is_err());
        pool.reset();
        assert_eq!(pool.allocations_len(), 0);
    }

    #[test]
    fn ipc_channel_sends_and_receives_fd() {
        let (left, right) = UnixStream::pair().unwrap();
        let left = DummyIpcChannel { stream: left };
        let right = DummyIpcChannel { stream: right };
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

        left.send_fd(fds[0]).unwrap();
        let received = right.recv_fd().unwrap();

        let mut writer = unsafe { fs::File::from_raw_fd(fds[1]) };
        writer.write_all(b"ok").unwrap();
        drop(writer);

        let mut reader = unsafe { fs::File::from_raw_fd(received) };
        let mut data = String::new();
        reader.read_to_string(&mut data).unwrap();
        assert_eq!(data, "ok");

        unsafe {
            libc::close(fds[0]);
        }
    }
}

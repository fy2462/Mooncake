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

pub const IPC_SHM_REGISTER: u32 = 0;
pub const IPC_SHM_FD_REQUEST: u32 = 1;
pub const SHM_SEG_HOT_CACHE: u32 = 0;
pub const INVALID_PHYSICAL_DEVICE_ID: i32 = -1;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmRegisterRequest {
    pub client_id_first: u64,
    pub client_id_second: u64,
    pub dummy_base_addr: u64,
    pub shm_size: u64,
    pub device_id: i32,
    pub is_local_buffer: bool,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmFdRequest {
    pub client_id_first: u64,
    pub client_id_second: u64,
    pub segment_type: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmFdResponse {
    pub status: i32,
    pub shm_size: u64,
}

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
    #[cfg(test)]
    pub(crate) fn from_stream(stream: UnixStream) -> Self {
        Self { stream }
    }
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
    pub fn send_raw(&mut self, data: &[u8]) -> DummyClientResult<()> {
        self.stream.write_all(data)?;
        Ok(())
    }
    pub fn recv_exact(&mut self, len: usize) -> DummyClientResult<Vec<u8>> {
        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data)?;
        Ok(data)
    }

    pub fn send_fd_with_bytes(&self, fd: RawFd, data: &[u8]) -> DummyClientResult<()> {
        send_fd_with_bytes(&self.stream, fd, data)
    }

    pub fn recv_fd_with_bytes(&self, len: usize) -> DummyClientResult<(RawFd, Vec<u8>)> {
        recv_fd_with_bytes(&self.stream, len)
    }

    pub fn send_request_type(&mut self, request_type: u32) -> DummyClientResult<()> {
        self.stream.write_all(&request_type.to_ne_bytes())?;
        Ok(())
    }

    pub fn register_shm_fd(
        socket_name: &str,
        fd: RawFd,
        request: ShmRegisterRequest,
    ) -> DummyClientResult<i32> {
        let mut channel = Self::connect_abstract(socket_name)?;
        channel.send_request_type(IPC_SHM_REGISTER)?;
        channel.send_fd_with_bytes(fd, as_bytes(&request))?;
        let status = channel.recv_i32()?;
        if status != 0 {
            return Err(DummyClientError::InvalidInput(format!(
                "real client failed to map shared memory, status={status}"
            )));
        }
        Ok(status)
    }

    pub fn request_hot_cache_fd(
        socket_name: &str,
        client_id_first: u64,
        client_id_second: u64,
    ) -> DummyClientResult<(RawFd, ShmFdResponse)> {
        let mut channel = Self::connect_abstract(socket_name)?;
        channel.send_request_type(IPC_SHM_FD_REQUEST)?;
        let request = ShmFdRequest {
            client_id_first,
            client_id_second,
            segment_type: SHM_SEG_HOT_CACHE,
        };
        channel.send_raw(as_bytes(&request))?;
        let (fd, data) = channel.recv_fd_with_bytes(mem::size_of::<ShmFdResponse>())?;
        let response = read_pod::<ShmFdResponse>(&data)?;
        if response.status != 0 {
            unsafe {
                libc::close(fd);
            }
            return Err(DummyClientError::InvalidInput(format!(
                "real client failed to return hot cache fd, status={}",
                response.status
            )));
        }
        Ok((fd, response))
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

    fn recv_i32(&mut self) -> DummyClientResult<i32> {
        let data = self.recv_exact(mem::size_of::<i32>())?;
        Ok(i32::from_ne_bytes(data.try_into().map_err(|_| {
            DummyClientError::InvalidInput("invalid i32 response size".to_string())
        })?))
    }
}

fn send_fd(stream: &UnixStream, fd: RawFd) -> DummyClientResult<()> {
    let byte = [0u8; 1];
    send_fd_with_bytes(stream, fd, &byte)
}

fn recv_fd(stream: &UnixStream) -> DummyClientResult<RawFd> {
    let (fd, _) = recv_fd_with_bytes(stream, 1)?;
    Ok(fd)
}

fn send_fd_with_bytes(stream: &UnixStream, fd: RawFd, data: &[u8]) -> DummyClientResult<()> {
    if data.is_empty() {
        return Err(DummyClientError::InvalidInput(
            "fd payload must not be empty".to_string(),
        ));
    }
    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
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

        let sent = libc::sendmsg(stream.as_raw_fd(), &msg, 0);
        if sent < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if sent as usize != iov.iov_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "short sendmsg while sending fd",
            )
            .into());
        }
    }
    Ok(())
}

fn recv_fd_with_bytes(stream: &UnixStream, len: usize) -> DummyClientResult<(RawFd, Vec<u8>)> {
    if len == 0 {
        return Err(DummyClientError::InvalidInput(
            "fd payload length must be greater than 0".to_string(),
        ));
    }
    let mut data = vec![0u8; len];
    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };
    let mut control = vec![0u8; unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as _) as usize }];
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = control.len() as _;

    unsafe {
        let received = libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_WAITALL);
        if received < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if received as usize != len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short recvmsg while receiving fd",
            )
            .into());
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
        Ok((*(libc::CMSG_DATA(cmsg) as *const RawFd), data))
    }
}

fn as_bytes<T>(value: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(value as *const T as *const u8, mem::size_of::<T>()) }
}

fn read_pod<T: Copy>(data: &[u8]) -> DummyClientResult<T> {
    if data.len() != mem::size_of::<T>() {
        return Err(DummyClientError::InvalidInput(format!(
            "invalid payload size: got {}, expected {}",
            data.len(),
            mem::size_of::<T>()
        )));
    }
    let mut value = mem::MaybeUninit::<T>::uninit();
    unsafe {
        std::ptr::copy_nonoverlapping(
            data.as_ptr(),
            value.as_mut_ptr() as *mut u8,
            mem::size_of::<T>(),
        );
        Ok(value.assume_init())
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

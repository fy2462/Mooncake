use parking_lot::Mutex;
use std::collections::HashMap;
use std::ffi::c_void;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::mem;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use thiserror::Error;

mod dummy_fd;
mod dummy_socket;

use dummy_fd::{as_bytes, read_pod, recv_fd, recv_fd_with_bytes, send_fd, send_fd_with_bytes};
use dummy_socket::{connect_abstract_socket, listen_abstract_socket_once};

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

enum DummyMemoryBacking {
    Heap(Vec<u8>),
    Shared {
        file: File,
        base_addr: usize,
        len: usize,
    },
}

pub struct DummyMemoryPool {
    backing: DummyMemoryBacking,
    next_offset: Mutex<usize>,
    allocations: Mutex<HashMap<u64, usize>>,
}

unsafe impl Send for DummyMemoryPool {}
unsafe impl Sync for DummyMemoryPool {}

impl DummyMemoryPool {
    pub fn new(mem_pool_size: usize) -> DummyClientResult<Self> {
        if mem_pool_size == 0 {
            return Err(DummyClientError::InvalidInput(
                "mem_pool_size must be greater than 0".to_string(),
            ));
        }
        Ok(Self {
            backing: DummyMemoryBacking::Heap(vec![0; mem_pool_size]),
            next_offset: Mutex::new(0),
            allocations: Mutex::new(HashMap::new()),
        })
    }

    pub fn new_shared(mem_pool_size: usize) -> DummyClientResult<Self> {
        if mem_pool_size == 0 {
            return Err(DummyClientError::InvalidInput(
                "mem_pool_size must be greater than 0".to_string(),
            ));
        }
        let (file, base_addr) = create_shared_mapping(mem_pool_size)?;
        Ok(Self {
            backing: DummyMemoryBacking::Shared {
                file,
                base_addr,
                len: mem_pool_size,
            },
            next_offset: Mutex::new(0),
            allocations: Mutex::new(HashMap::new()),
        })
    }

    pub fn base_ptr(&self) -> *mut c_void {
        self.base_addr() as *mut c_void
    }

    pub fn base_addr(&self) -> usize {
        match &self.backing {
            DummyMemoryBacking::Heap(mem_pool) => mem_pool.as_ptr() as usize,
            DummyMemoryBacking::Shared { base_addr, .. } => *base_addr,
        }
    }

    pub fn len(&self) -> usize {
        match &self.backing {
            DummyMemoryBacking::Heap(mem_pool) => mem_pool.len(),
            DummyMemoryBacking::Shared { len, .. } => *len,
        }
    }

    pub fn fd(&self) -> Option<RawFd> {
        match &self.backing {
            DummyMemoryBacking::Heap(_) => None,
            DummyMemoryBacking::Shared { file, .. } => Some(file.as_raw_fd()),
        }
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
        if end > self.len() {
            return Err(DummyClientError::InvalidInput(
                "dummy memory pool exhausted".to_string(),
            ));
        }
        *next = end;
        let addr = (self.base_addr() + aligned) as u64;
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

impl Drop for DummyMemoryPool {
    fn drop(&mut self) {
        if let DummyMemoryBacking::Shared { base_addr, len, .. } = &self.backing {
            unsafe {
                libc::munmap(*base_addr as *mut libc::c_void, *len);
            }
        }
    }
}

fn create_shared_mapping(len: usize) -> DummyClientResult<(File, usize)> {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "mooncake_dummy_shm_{}_{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    fs::remove_file(&path)?;
    file.set_len(len as u64)?;
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((file, ptr as usize))
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

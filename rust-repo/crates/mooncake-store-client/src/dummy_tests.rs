use crate::dummy::{
    DummyIpcChannel, DummyMemoryPool, ShmFdRequest, ShmFdResponse, ShmRegisterRequest,
    INVALID_PHYSICAL_DEVICE_ID,
};
use std::fs;
use std::io::{Read, Write};
use std::mem;
use std::os::fd::FromRawFd;

fn as_bytes<T>(value: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(value as *const T as *const u8, mem::size_of::<T>()) }
}

fn read_pod<T: Copy>(data: &[u8]) -> T {
    let mut value = mem::MaybeUninit::<T>::uninit();
    unsafe {
        std::ptr::copy_nonoverlapping(
            data.as_ptr(),
            value.as_mut_ptr() as *mut u8,
            mem::size_of::<T>(),
        );
        value.assume_init()
    }
}

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
    let (left, right) = std::os::unix::net::UnixStream::pair().unwrap();
    let client = DummyIpcChannel::from_stream(left);
    let server = DummyIpcChannel::from_stream(right);
    let mut fds = [0; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    client.send_fd(fds[0]).unwrap();
    let received = server.recv_fd().unwrap();

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

#[test]
fn ipc_channel_sends_fd_with_cxx_payload() {
    assert_eq!(mem::size_of::<ShmRegisterRequest>(), 40);
    assert_eq!(mem::size_of::<ShmFdRequest>(), 24);
    assert_eq!(mem::size_of::<ShmFdResponse>(), 16);

    let request = ShmRegisterRequest {
        client_id_first: 1,
        client_id_second: 2,
        dummy_base_addr: 0x1000,
        shm_size: 4096,
        device_id: INVALID_PHYSICAL_DEVICE_ID,
        is_local_buffer: true,
    };
    let (left, right) = std::os::unix::net::UnixStream::pair().unwrap();
    let client = DummyIpcChannel::from_stream(left);
    let server = DummyIpcChannel::from_stream(right);
    let mut fds = [0; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    client
        .send_fd_with_bytes(fds[0], as_bytes(&request))
        .unwrap();
    let (received, payload) = server
        .recv_fd_with_bytes(mem::size_of::<ShmRegisterRequest>())
        .unwrap();
    assert_eq!(read_pod::<ShmRegisterRequest>(&payload), request);

    let mut writer = unsafe { fs::File::from_raw_fd(fds[1]) };
    writer.write_all(b"ipc").unwrap();
    drop(writer);
    let mut reader = unsafe { fs::File::from_raw_fd(received) };
    let mut data = String::new();
    reader.read_to_string(&mut data).unwrap();
    assert_eq!(data, "ipc");

    unsafe {
        libc::close(fds[0]);
    }
}

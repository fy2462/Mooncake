use super::{DummyClientError, DummyClientResult};
use std::mem;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;

pub(super) fn send_fd(stream: &UnixStream, fd: RawFd) -> DummyClientResult<()> {
    let byte = [0u8; 1];
    send_fd_with_bytes(stream, fd, &byte)
}

pub(super) fn recv_fd(stream: &UnixStream) -> DummyClientResult<RawFd> {
    let (fd, _) = recv_fd_with_bytes(stream, 1)?;
    Ok(fd)
}

pub(super) fn send_fd_with_bytes(
    stream: &UnixStream,
    fd: RawFd,
    data: &[u8],
) -> DummyClientResult<()> {
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

pub(super) fn recv_fd_with_bytes(
    stream: &UnixStream,
    len: usize,
) -> DummyClientResult<(RawFd, Vec<u8>)> {
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

pub(super) fn as_bytes<T>(value: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(value as *const T as *const u8, mem::size_of::<T>()) }
}

pub(super) fn read_pod<T: Copy>(data: &[u8]) -> DummyClientResult<T> {
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

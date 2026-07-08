use std::ops::{Deref, DerefMut};

/// Client-owned memory used for TE-registered local and segment buffers.
pub(crate) enum OwnedBuffer {
    Vec(Vec<u8>),
    #[cfg(target_os = "linux")]
    Mmap {
        ptr: std::ptr::NonNull<u8>,
        len: usize,
    },
}

impl OwnedBuffer {
    pub(crate) fn allocate(size: usize) -> Self {
        if size == 0 {
            return Self::Vec(Vec::new());
        }
        #[cfg(target_os = "linux")]
        {
            if hugepage_requested() {
                if let Some(buffer) = unsafe { allocate_hugepage_mmap(size, hugepage_size()) } {
                    return buffer;
                }
                tracing::warn!(
                    size,
                    "hugepage mmap failed, falling back to regular Vec allocation"
                );
            }
        }
        Self::Vec(vec![0u8; size])
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Vec(buffer) => buffer.as_ptr(),
            #[cfg(target_os = "linux")]
            Self::Mmap { ptr, .. } => ptr.as_ptr(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Vec(buffer) => buffer.len(),
            #[cfg(target_os = "linux")]
            Self::Mmap { len, .. } => *len,
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Vec(buffer) => buffer.as_slice(),
            #[cfg(target_os = "linux")]
            Self::Mmap { ptr, len } => unsafe { std::slice::from_raw_parts(ptr.as_ptr(), *len) },
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Vec(buffer) => buffer.as_mut_slice(),
            #[cfg(target_os = "linux")]
            Self::Mmap { ptr, len } => unsafe {
                std::slice::from_raw_parts_mut(ptr.as_ptr(), *len)
            },
        }
    }
}

impl Deref for OwnedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for OwnedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

#[cfg(target_os = "linux")]
impl Drop for OwnedBuffer {
    fn drop(&mut self) {
        if let Self::Mmap { ptr, len } = self {
            unsafe {
                libc::munmap(ptr.as_ptr() as *mut libc::c_void, *len);
            }
        }
    }
}

#[cfg(target_os = "linux")]
unsafe impl Send for OwnedBuffer {}
#[cfg(target_os = "linux")]
unsafe impl Sync for OwnedBuffer {}

#[cfg(target_os = "linux")]
fn hugepage_requested() -> bool {
    matches!(
        std::env::var("MOONCAKE_USE_HUGEPAGE")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1" | "true" | "TRUE" | "on" | "ON" | "yes" | "YES")
    )
}

#[cfg(target_os = "linux")]
fn hugepage_size() -> Option<usize> {
    match std::env::var("MOONCAKE_HUGEPAGE_SIZE").ok() {
        Some(value) if value.trim().eq_ignore_ascii_case("1gb") => Some(1024 * 1024 * 1024),
        Some(value) if value.trim().eq_ignore_ascii_case("2mb") => Some(2 * 1024 * 1024),
        Some(value) => value.trim().parse::<usize>().ok(),
        None => Some(2 * 1024 * 1024),
    }
}

#[cfg(target_os = "linux")]
unsafe fn allocate_hugepage_mmap(size: usize, page_size: Option<usize>) -> Option<OwnedBuffer> {
    let page_size = page_size.unwrap_or(2 * 1024 * 1024);
    let len = align_up(size, page_size)?;
    let mut flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB;
    if page_size == 2 * 1024 * 1024 {
        flags |= 21 << libc::MAP_HUGE_SHIFT;
    } else if page_size == 1024 * 1024 * 1024 {
        flags |= 30 << libc::MAP_HUGE_SHIFT;
    }
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            -1,
            0,
        )
    };
    let ptr = std::ptr::NonNull::new(ptr as *mut u8)?;
    if ptr.as_ptr() as *mut libc::c_void == libc::MAP_FAILED {
        return None;
    }
    Some(OwnedBuffer::Mmap { ptr, len })
}

#[cfg(target_os = "linux")]
fn align_up(size: usize, alignment: usize) -> Option<usize> {
    if alignment == 0 {
        return Some(size);
    }
    let rem = size % alignment;
    if rem == 0 {
        Some(size)
    } else {
        size.checked_add(alignment - rem)
    }
}

#[cfg(test)]
mod tests {
    use super::OwnedBuffer;

    #[test]
    fn vec_fallback_is_mutable_and_stable() {
        let mut buffer = OwnedBuffer::allocate(16);
        assert_eq!(buffer.len(), 16);
        let ptr = buffer.as_ptr();
        buffer[0] = 7;
        assert_eq!(buffer[0], 7);
        assert_eq!(buffer.as_ptr(), ptr);
    }
}

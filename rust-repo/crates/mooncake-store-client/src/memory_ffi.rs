//! Centralized unsafe boundary for client-owned native memory.
//!
//! Code under `client/` only receives safe owned-buffer and registration
//! operations from this module. Raw mmap pointers and Transfer Engine memory
//! registration never escape this adapter.

use mooncake_store_core::error::StoreResult;
use std::ffi::c_void;
use std::io;
use std::ops::{Deref, DerefMut, Range};
use transfer_engine_ffi::TransferEngine;

/// Client-owned memory used for TE-registered local and segment buffers.
pub(crate) enum OwnedBuffer {
    Vec(Vec<u8>),
    #[cfg(target_os = "linux")]
    Mmap {
        ptr: std::ptr::NonNull<u8>,
        len: usize,
        page_size: usize,
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
                return Self::allocate_with_hugepage_allocator(size, |page_size| unsafe {
                    allocate_hugepage_mmap(size, page_size)
                });
            }
        }
        Self::Vec(vec![0u8; size])
    }

    #[cfg(target_os = "linux")]
    fn allocate_with_hugepage_allocator(
        size: usize,
        allocate_hugepage: impl FnOnce(usize) -> Option<Self>,
    ) -> Self {
        let page_size = hugepage_size().unwrap_or(2 * 1024 * 1024);
        if let Some(buffer) = allocate_hugepage(page_size) {
            return buffer;
        }
        tracing::warn!(
            size,
            "hugepage mmap failed, falling back to regular Vec allocation"
        );
        Self::Vec(vec![0u8; size])
    }

    pub(crate) fn populate_before_registration(&mut self, protocol: &str) -> io::Result<()> {
        if protocol != "rdma" {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        if let Self::Mmap {
            ptr,
            len,
            page_size,
        } = self
        {
            let page_count = len.div_ceil(*page_size);
            let workers = std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(16)
                .min(page_count);
            unsafe {
                populate_hugetlb_pages(ptr.as_ptr(), *len, *page_size, workers)?;
            }
        }
        Ok(())
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
            Self::Mmap { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), *len)
            },
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Vec(buffer) => buffer.as_mut_slice(),
            #[cfg(target_os = "linux")]
            Self::Mmap { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts_mut(ptr.as_ptr(), *len)
            },
        }
    }
}

pub(crate) fn register_local_memory(
    engine: &TransferEngine,
    buffer: &OwnedBuffer,
    location: &str,
    remote_accessible: bool,
) -> StoreResult<()> {
    // SAFETY: `OwnedBuffer` owns a stable allocation which remains alive in
    // `MooncakeClient` until after the Transfer Engine is shut down.
    unsafe {
        engine.register_local_memory(
            buffer.as_ptr() as *mut c_void,
            buffer.len(),
            location,
            remote_accessible,
        )?;
    }
    Ok(())
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
        if let Self::Mmap { ptr, len, .. } = self {
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
unsafe fn allocate_hugepage_mmap(size: usize, page_size: usize) -> Option<OwnedBuffer> {
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
    Some(OwnedBuffer::Mmap {
        ptr,
        len,
        page_size,
    })
}

fn page_ranges(page_count: usize, workers: usize) -> io::Result<Vec<Range<usize>>> {
    if page_count == 0 || workers == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "page count and worker count must be non-zero",
        ));
    }
    let workers = workers.min(page_count);
    let pages_per_worker = page_count.div_ceil(workers);
    Ok((0..page_count)
        .step_by(pages_per_worker)
        .map(|start| start..(start + pages_per_worker).min(page_count))
        .collect())
}

/// Populate a HugeTLB mapping by touching the first byte of every huge page.
///
/// # Safety
///
/// `ptr..ptr.add(len)` must be a valid, uniquely writable mapping for the
/// duration of this call. `page_size` must describe that mapping's page size.
unsafe fn populate_hugetlb_pages(
    ptr: *mut u8,
    len: usize,
    page_size: usize,
    workers: usize,
) -> io::Result<()> {
    if ptr.is_null() || len == 0 || page_size == 0 || workers == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mapping, length, page size, and worker count must be valid",
        ));
    }
    let ranges = page_ranges(len.div_ceil(page_size), workers)?;
    let base_addr = ptr as usize;
    std::thread::scope(|scope| -> io::Result<()> {
        let mut handles = Vec::with_capacity(ranges.len());
        for range in ranges {
            handles.push(scope.spawn(move || {
                for page_index in range {
                    let address = base_addr + page_index * page_size;
                    unsafe {
                        std::ptr::write_volatile(address as *mut u8, 0);
                    }
                }
            }));
        }
        for handle in handles {
            handle
                .join()
                .map_err(|_| io::Error::other("HugeTLB population worker panicked"))?;
        }
        Ok(())
    })
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
    use super::{OwnedBuffer, page_ranges, populate_hugetlb_pages};

    #[test]
    fn vec_fallback_is_mutable_and_stable() {
        let mut buffer = OwnedBuffer::allocate(16);
        assert_eq!(buffer.len(), 16);
        let ptr = buffer.as_ptr();
        buffer[0] = 7;
        assert_eq!(buffer[0], 7);
        assert_eq!(buffer.as_ptr(), ptr);
    }

    #[test]
    fn page_ranges_cover_every_page_once_without_overlap() {
        let ranges = page_ranges(5, 3).expect("valid page ranges");

        assert_eq!(ranges, vec![0..2, 2..4, 4..5]);
        assert_eq!(
            ranges.into_iter().flatten().collect::<Vec<_>>(),
            (0..5).collect::<Vec<_>>()
        );
    }

    #[test]
    fn page_ranges_reject_zero_sized_inputs() {
        assert!(page_ranges(0, 1).is_err());
        assert!(page_ranges(1, 0).is_err());
    }

    #[test]
    fn population_touches_only_the_first_byte_of_each_page() {
        let mut mapping = vec![0xff; 13];

        unsafe {
            populate_hugetlb_pages(mapping.as_mut_ptr(), mapping.len(), 4, 3)
                .expect("population succeeds");
        }

        assert_eq!(
            mapping,
            vec![
                0, 0xff, 0xff, 0xff, 0, 0xff, 0xff, 0xff, 0, 0xff, 0xff, 0xff, 0
            ]
        );
    }

    #[test]
    fn population_rejects_zero_sized_inputs() {
        let mut byte = 0u8;
        assert!(unsafe { populate_hugetlb_pages(&mut byte, 0, 1, 1) }.is_err());
        assert!(unsafe { populate_hugetlb_pages(&mut byte, 1, 0, 1) }.is_err());
        assert!(unsafe { populate_hugetlb_pages(&mut byte, 1, 1, 0) }.is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_hugepage_allocation_falls_back_to_vec() {
        let buffer = OwnedBuffer::allocate_with_hugepage_allocator(16, |_| None);

        assert!(matches!(buffer, OwnedBuffer::Vec(_)));
        assert_eq!(buffer.len(), 16);
    }
}

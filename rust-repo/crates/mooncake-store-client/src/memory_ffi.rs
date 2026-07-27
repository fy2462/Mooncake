//! Centralized unsafe boundary for client-owned native memory.
//!
//! Code under `client/` only receives safe owned-buffer and registration
//! operations from this module. Raw mmap pointers and Transfer Engine memory
//! registration never escape this adapter.

use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use std::ffi::c_void;
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut, Range};
use std::ptr::NonNull;
use std::sync::Arc;
use transfer_engine_ffi::{
    RegisteredMemory, RegisteredMemoryAccess, StableMemoryOwner, TransferEngine,
};

use crate::pinned_memory::{PinnedAllocation, global_pinned_memory_manager};

pub(crate) type OwnedSegmentBuffer = PinnedAllocation<OwnedBuffer>;

/// Owner-backed registration used by the peer offload read server.
///
/// The registration owns the exact bytes returned by the local-storage
/// backend. Dropping it unregisters first; if native unregistration cannot be
/// proven, `RegisteredMemory` deliberately leaks the owner instead of freeing
/// memory that a remote DMA may still reference.
#[derive(Debug)]
pub(crate) struct RemoteReadableRegistration {
    registration: RegisteredMemory,
    pointer: u64,
    len: usize,
}

impl RemoteReadableRegistration {
    pub(crate) fn register(engine: &Arc<TransferEngine>, bytes: Vec<u8>) -> StoreResult<Self> {
        if bytes.is_empty() {
            return Err(StoreError::InvalidParams(
                "remote offload buffer must not be empty".to_string(),
            ));
        }
        let len = bytes.len();
        let registration = engine.register_owned_memory(
            RemoteReadBuffer(bytes),
            "cpu:0",
            true,
            RegisteredMemoryAccess::ReadOnly,
        )?;
        let region = registration.readable_region(0, len)?;
        let pointer = u64::try_from(region.as_ptr() as usize).map_err(|_| {
            StoreError::InvalidParams(
                "remote offload buffer address exceeds the protocol range".to_string(),
            )
        })?;
        drop(region);
        Ok(Self {
            registration,
            pointer,
            len,
        })
    }

    pub(crate) fn pointer(&self) -> u64 {
        self.pointer
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[derive(Debug)]
struct RemoteReadBuffer(Vec<u8>);

unsafe impl StableMemoryOwner for RemoteReadBuffer {
    fn base_address(&self) -> NonNull<c_void> {
        NonNull::new(self.0.as_ptr().cast_mut().cast()).expect("remote offload buffer is non-empty")
    }

    fn length(&self) -> usize {
        self.0.len()
    }
}

/// Cloneable owner capability for a Python-visible registered buffer.
///
/// The FFI registration and the Python lease each retain one clone, so the
/// aligned allocation cannot be freed while either native DMA or an exported
/// Python view may still reference it.
#[derive(Clone)]
pub struct RegisteredBufferAllocation {
    buffer: Arc<OwnedBuffer>,
}

impl RegisteredBufferAllocation {
    pub fn allocate(size: usize, alignment: usize) -> StoreResult<Self> {
        if size == 0 {
            return Err(StoreError::InvalidParams(
                "registered allocation size must be greater than zero".to_string(),
            ));
        }
        let buffer = OwnedBuffer::allocate_aligned(size, alignment).map_err(|error| {
            StoreError::Internal(format!(
                "failed to allocate {size}-byte registered buffer aligned to {alignment}: {error}"
            ))
        })?;
        Ok(Self {
            buffer: Arc::new(buffer),
        })
    }

    pub fn as_ptr(&self) -> *mut c_void {
        self.buffer.as_ptr() as *mut c_void
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for RegisteredBufferAllocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredBufferAllocation")
            .field("base", &(self.buffer.as_ptr() as usize))
            .field("len", &self.buffer.len())
            .finish()
    }
}

unsafe impl StableMemoryOwner for RegisteredBufferAllocation {
    fn base_address(&self) -> NonNull<c_void> {
        NonNull::new(self.as_ptr()).expect("registered allocation is non-empty")
    }

    fn length(&self) -> usize {
        self.len()
    }
}

/// Registration lease for the CXL mapping owned by the native Transfer
/// Engine. Rust never dereferences or frees this address; it only unregisters
/// the exact range before dropping the engine.
pub(crate) struct CxlSegmentRegistration {
    base_addr: usize,
    len: usize,
}

impl CxlSegmentRegistration {
    pub(crate) fn base_addr(&self) -> u64 {
        self.base_addr as u64
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

/// Client-owned memory used for TE-registered local and segment buffers.
pub(crate) enum OwnedBuffer {
    Vec(Vec<u8>),
    Aligned {
        ptr: std::ptr::NonNull<u8>,
        len: usize,
        layout: std::alloc::Layout,
    },
    #[cfg(target_os = "linux")]
    Mmap {
        ptr: std::ptr::NonNull<u8>,
        len: usize,
        page_size: usize,
    },
}

struct RegisteredOwnedBuffer {
    buffer: OwnedBuffer,
}

impl fmt::Debug for RegisteredOwnedBuffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredOwnedBuffer")
            .field("base", &(self.buffer.as_ptr() as usize))
            .field("len", &self.buffer.len())
            .finish()
    }
}

// SAFETY: the wrapper is never exposed for direct byte access. Shared
// references are used only by the registration state to query the immutable
// base address and length; all actual reads/writes are coordinated by typed
// region leases.
unsafe impl Sync for RegisteredOwnedBuffer {}

// SAFETY: this adapter exclusively owns the stable allocation. Store code can
// access its bytes only through bounded RegisteredMemory operations or
// RegisteredTransferRequest capabilities, which reject overlapping local and
// native access.
unsafe impl StableMemoryOwner for RegisteredOwnedBuffer {
    fn base_address(&self) -> NonNull<c_void> {
        NonNull::new(self.buffer.as_ptr().cast_mut().cast())
            .expect("registered Store buffer is non-empty")
    }

    fn length(&self) -> usize {
        self.buffer.len()
    }
}

impl OwnedBuffer {
    pub(crate) fn allocate(size: usize) -> Self {
        Self::Vec(vec![0u8; size])
    }

    pub(crate) fn allocate_aligned(size: usize, alignment: usize) -> io::Result<Self> {
        if alignment <= 1 {
            return Ok(Self::allocate(size));
        }
        let layout = std::alloc::Layout::from_size_align(size, alignment).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid Store segment alignment: {error}"),
            )
        })?;
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(ptr).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to allocate {size} bytes aligned to {alignment}"),
            )
        })?;
        Ok(Self::Aligned {
            ptr,
            len: size,
            layout,
        })
    }

    pub(crate) fn allocate_for_registration(size: usize, alignment: usize) -> io::Result<Self> {
        if size == 0 {
            return Ok(Self::Vec(Vec::new()));
        }
        let policy = HugepagePolicy::from_environment();
        if !policy.requested {
            return Self::allocate_aligned(size, alignment);
        }

        #[cfg(target_os = "linux")]
        {
            return Self::allocate_with_hugepage_allocator(
                size,
                alignment,
                policy,
                |page_size| unsafe { allocate_hugepage_mmap(size, page_size) },
            );
        }
        #[cfg(not(target_os = "linux"))]
        {
            if policy.strict {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "MC_STORE_USE_HUGEPAGE requires Linux HugeTLB support",
                ));
            }
            Self::allocate_aligned(size, alignment)
        }
    }

    #[cfg(target_os = "linux")]
    fn allocate_with_hugepage_allocator(
        size: usize,
        alignment: usize,
        policy: HugepagePolicy,
        allocate_hugepage: impl FnOnce(usize) -> Option<Self>,
    ) -> io::Result<Self> {
        if let Some(buffer) = allocate_hugepage(policy.page_size) {
            if buffer.as_ptr() as usize % alignment.max(1) == 0 {
                return Ok(buffer);
            }
            if policy.strict {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "HugeTLB page size {} does not satisfy Store alignment {alignment}",
                        policy.page_size
                    ),
                ));
            }
        }
        if policy.strict {
            return Err(io::Error::other(format!(
                "MC_STORE_USE_HUGEPAGE requested {size} bytes with page size {}, but HugeTLB mmap failed",
                policy.page_size
            )));
        }

        tracing::warn!(
            size,
            page_size = policy.page_size,
            "legacy hugepage request failed; falling back to regular aligned allocation"
        );
        Self::allocate_aligned(size, alignment)
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
            Self::Aligned { ptr, .. } => ptr.as_ptr(),
            #[cfg(target_os = "linux")]
            Self::Mmap { ptr, .. } => ptr.as_ptr(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Vec(buffer) => buffer.len(),
            Self::Aligned { len, .. } => *len,
            #[cfg(target_os = "linux")]
            Self::Mmap { len, .. } => *len,
        }
    }

    /// Copy bytes into this stable allocation after validating the complete
    /// destination range. Raw pointer mutation stays inside this memory
    /// adapter; Store client code only sees a safe, bounds-checked operation.
    pub(crate) fn copy_from_slice(&self, offset: usize, source: &[u8]) -> StoreResult<()> {
        let end = offset.checked_add(source.len()).ok_or_else(|| {
            StoreError::InvalidParams(format!(
                "memory write range overflow: offset={offset} len={}",
                source.len()
            ))
        })?;
        if end > self.len() {
            return Err(StoreError::InvalidParams(format!(
                "memory write out of bounds: offset={offset} len={} capacity={}",
                source.len(),
                self.len()
            )));
        }
        // SAFETY: this allocation remains stable for its entire owner
        // lifetime, the checked range is contained in it, and Store mutation
        // coordination prevents overlapping local writes to the same replica.
        unsafe {
            std::ptr::copy_nonoverlapping(
                source.as_ptr(),
                self.as_ptr().add(offset) as *mut u8,
                source.len(),
            );
        }
        Ok(())
    }

    /// Copy a validated range out of this stable allocation.
    pub(crate) fn copy_to_vec(&self, offset: usize, len: usize) -> StoreResult<Vec<u8>> {
        let end = offset.checked_add(len).ok_or_else(|| {
            StoreError::InvalidParams(format!(
                "memory read range overflow: offset={offset} len={len}"
            ))
        })?;
        if end > self.len() {
            return Err(StoreError::InvalidParams(format!(
                "memory read out of bounds: offset={offset} len={len} capacity={}",
                self.len()
            )));
        }
        let mut result = vec![0u8; len];
        // SAFETY: the checked source range is contained in the stable
        // allocation and `result` owns a writable range of exactly `len`.
        unsafe {
            std::ptr::copy_nonoverlapping(self.as_ptr().add(offset), result.as_mut_ptr(), len);
        }
        Ok(result)
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Vec(buffer) => buffer.as_slice(),
            Self::Aligned { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), *len)
            },
            #[cfg(target_os = "linux")]
            Self::Mmap { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), *len)
            },
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Vec(buffer) => buffer.as_mut_slice(),
            Self::Aligned { ptr, len, .. } => unsafe {
                std::slice::from_raw_parts_mut(ptr.as_ptr(), *len)
            },
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

/// Consume a Store-owned allocation into the owner-bearing safe registration
/// boundary used by client staging transfers.
pub(crate) fn register_owned_local_memory(
    engine: &Arc<TransferEngine>,
    buffer: OwnedBuffer,
    location: &str,
    remote_accessible: bool,
) -> StoreResult<RegisteredMemory> {
    if buffer.len() == 0 {
        return Err(StoreError::InvalidParams(
            "registered local buffer must not be empty".to_string(),
        ));
    }
    Ok(engine.register_owned_memory(
        RegisteredOwnedBuffer { buffer },
        location,
        remote_accessible,
        RegisteredMemoryAccess::ReadWrite,
    )?)
}

pub(crate) fn unregister_local_memory(
    engine: &TransferEngine,
    buffer: &OwnedBuffer,
) -> StoreResult<()> {
    // SAFETY: the address comes from the same still-live OwnedBuffer used for
    // registration. Callers release the allocation only after this succeeds.
    unsafe {
        engine.unregister_local_memory(buffer.as_ptr() as *mut c_void)?;
    }
    Ok(())
}

pub(crate) fn register_cxl_segment(
    engine: &TransferEngine,
    len: usize,
) -> StoreResult<CxlSegmentRegistration> {
    if len == 0 {
        return Err(mooncake_store_core::StoreError::InvalidParams(
            "MC_CXL_DEV_SIZE must be positive".to_string(),
        ));
    }
    // SAFETY: the installed native CXL transport owns this mapping for the
    // lifetime of `engine`. We validate the returned pointer and retain this
    // registration lease until it is explicitly unregistered.
    let base = unsafe { engine.get_base_addr() };
    if base.is_null() {
        return Err(mooncake_store_core::StoreError::Internal(
            "CXL transport did not expose a mapped base address".to_string(),
        ));
    }
    // SAFETY: the native CXL transport created this mapping from the same
    // MC_CXL_DEV_SIZE value before exposing it through get_base_addr().
    unsafe {
        engine.register_local_memory(base, len, "cpu:0", true)?;
    }
    Ok(CxlSegmentRegistration {
        base_addr: base as usize,
        len,
    })
}

pub(crate) fn unregister_cxl_segment(
    engine: &TransferEngine,
    registration: &CxlSegmentRegistration,
) -> StoreResult<()> {
    // SAFETY: this is the exact still-live native mapping registered by
    // `register_cxl_segment`; the engine owns it until engine teardown.
    unsafe {
        engine.unregister_local_memory(registration.base_addr as *mut c_void)?;
    }
    Ok(())
}

pub(crate) fn allocate_store_segment(
    size: usize,
    protocol: &str,
    alignment: usize,
) -> io::Result<OwnedSegmentBuffer> {
    let mut buffer = OwnedBuffer::allocate_for_registration(size, alignment)?;
    buffer.populate_before_registration(protocol)?;
    let pin = is_host_store_segment_protocol(protocol)
        .then(|| {
            global_pinned_memory_manager().try_pin(
                buffer.as_ptr() as usize,
                buffer.len(),
                "Rust Store setup segment",
            )
        })
        .flatten();
    Ok(PinnedAllocation::new(buffer, pin))
}

fn is_host_store_segment_protocol(protocol: &str) -> bool {
    matches!(protocol, "" | "tcp" | "rdma" | "efa" | "cxi" | "rpc_only")
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

impl Drop for OwnedBuffer {
    fn drop(&mut self) {
        match self {
            Self::Aligned { ptr, layout, .. } => unsafe {
                std::alloc::dealloc(ptr.as_ptr(), *layout);
            },
            #[cfg(target_os = "linux")]
            Self::Mmap { ptr, len, .. } => unsafe {
                libc::munmap(ptr.as_ptr() as *mut libc::c_void, *len);
            },
            Self::Vec(_) => {}
        }
    }
}

unsafe impl Send for OwnedBuffer {}
unsafe impl Sync for OwnedBuffer {}

const HUGEPAGE_2_MIB: usize = 2 * 1024 * 1024;
const HUGEPAGE_1_GIB: usize = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HugepagePolicy {
    requested: bool,
    strict: bool,
    page_size: usize,
}

impl HugepagePolicy {
    fn from_environment() -> Self {
        Self::from_values(
            std::env::var_os("MC_STORE_USE_HUGEPAGE").is_some(),
            std::env::var("MC_STORE_HUGEPAGE_SIZE").ok().as_deref(),
            std::env::var("MOONCAKE_USE_HUGEPAGE").ok().as_deref(),
            std::env::var("MOONCAKE_HUGEPAGE_SIZE").ok().as_deref(),
        )
    }

    fn from_values(
        cpp_requested: bool,
        cpp_page_size: Option<&str>,
        legacy_requested: Option<&str>,
        legacy_page_size: Option<&str>,
    ) -> Self {
        if cpp_requested {
            return Self {
                requested: true,
                strict: true,
                page_size: parse_hugepage_size(cpp_page_size),
            };
        }
        let legacy_requested = legacy_requested.is_some_and(parse_true);
        Self {
            requested: legacy_requested,
            strict: false,
            page_size: parse_hugepage_size(legacy_page_size),
        }
    }
}

fn parse_true(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes"
    )
}

fn parse_hugepage_size(value: Option<&str>) -> usize {
    let Some(value) = value.map(str::trim) else {
        return HUGEPAGE_2_MIB;
    };
    if value.eq_ignore_ascii_case("2mb") || value.parse::<usize>() == Ok(HUGEPAGE_2_MIB) {
        HUGEPAGE_2_MIB
    } else if value.eq_ignore_ascii_case("1gb") || value.parse::<usize>() == Ok(HUGEPAGE_1_GIB) {
        HUGEPAGE_1_GIB
    } else {
        tracing::warn!(
            value,
            "invalid MC_STORE_HUGEPAGE_SIZE; using the C++ default 2 MiB"
        );
        HUGEPAGE_2_MIB
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
    use super::{
        HUGEPAGE_1_GIB, HUGEPAGE_2_MIB, HugepagePolicy, OwnedBuffer, page_ranges,
        populate_hugetlb_pages,
    };

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
    fn aligned_store_allocation_preserves_base_alignment() {
        let mut buffer = OwnedBuffer::allocate_aligned(4096, 4096).unwrap();
        assert_eq!(buffer.as_ptr() as usize % 4096, 0);
        assert_eq!(buffer.len(), 4096);
        buffer[4095] = 9;
        assert_eq!(buffer[4095], 9);
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
    fn legacy_hugepage_failure_falls_back_but_cpp_request_is_strict() {
        let legacy = HugepagePolicy {
            requested: true,
            strict: false,
            page_size: HUGEPAGE_2_MIB,
        };
        let buffer =
            OwnedBuffer::allocate_with_hugepage_allocator(16, 1, legacy, |_| None).unwrap();

        assert!(matches!(buffer, OwnedBuffer::Vec(_)));
        assert_eq!(buffer.len(), 16);

        let strict = HugepagePolicy {
            strict: true,
            ..legacy
        };
        assert!(OwnedBuffer::allocate_with_hugepage_allocator(16, 1, strict, |_| None).is_err());
    }

    #[test]
    fn cpp_hugepage_environment_has_strict_precedence() {
        let cpp = HugepagePolicy::from_values(true, Some("1GB"), Some("false"), Some("2MB"));
        assert!(cpp.requested);
        assert!(cpp.strict);
        assert_eq!(cpp.page_size, HUGEPAGE_1_GIB);

        let invalid_cpp =
            HugepagePolicy::from_values(true, Some("invalid"), Some("true"), Some("1GB"));
        assert!(invalid_cpp.strict);
        assert_eq!(invalid_cpp.page_size, HUGEPAGE_2_MIB);

        let legacy = HugepagePolicy::from_values(false, None, Some("yes"), Some("2097152"));
        assert!(legacy.requested);
        assert!(!legacy.strict);
        assert_eq!(legacy.page_size, HUGEPAGE_2_MIB);
    }
}

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
use std::sync::atomic::{AtomicUsize, Ordering};
use transfer_engine_ffi::{
    RegisteredMemory, RegisteredMemoryAccess, StableMemoryOwner, TransferEngine,
};

use crate::pinned_memory::{PinnedAllocation, global_pinned_memory_manager};

pub(crate) type OwnedSegmentBuffer = PinnedAllocation<OwnedBuffer>;

const STORE_ARENA_MIN_ALIGNMENT: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreArenaStats {
    pub capacity: usize,
    pub default_alignment: usize,
    pub reserved: usize,
    pub peak: usize,
    pub successful: usize,
    pub failed: usize,
}

struct StoreArenaBacking {
    buffer: OwnedBuffer,
}

/// Monotonic Store-segment allocator used in place of the C++ raw-pointer
/// singleton. Each successful reservation returns a capability for one unique
/// range while the shared owner keeps the complete backing alive.
pub struct StoreSegmentArena {
    backing: Arc<StoreArenaBacking>,
    capacity: usize,
    default_alignment: usize,
    reserved: AtomicUsize,
    peak: AtomicUsize,
    successful: AtomicUsize,
    failed: AtomicUsize,
}

impl StoreSegmentArena {
    pub fn new(requested_capacity: usize, default_alignment: usize) -> io::Result<Self> {
        if requested_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Store arena capacity must be greater than zero",
            ));
        }
        if default_alignment != 0 && !default_alignment.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Store arena alignment must be a power of two",
            ));
        }
        let default_alignment = default_alignment.max(STORE_ARENA_MIN_ALIGNMENT);
        let capacity = align_up(requested_capacity, HUGEPAGE_2_MIB).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Store arena capacity alignment overflow",
            )
        })?;
        let mut buffer = OwnedBuffer::allocate_aligned(capacity, HUGEPAGE_2_MIB)?;
        prefault_system_pages(&mut buffer);
        Ok(Self {
            backing: Arc::new(StoreArenaBacking { buffer }),
            capacity,
            default_alignment,
            reserved: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            successful: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
        })
    }

    pub fn allocate(
        &self,
        size: usize,
        requested_alignment: usize,
    ) -> io::Result<StoreArenaAllocation> {
        if size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Store arena allocation size must be greater than zero",
            ));
        }
        if requested_alignment != 0 && !requested_alignment.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Store arena allocation alignment must be a power of two",
            ));
        }
        let alignment = self.default_alignment.max(requested_alignment);
        if alignment > HUGEPAGE_2_MIB {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Store arena allocation alignment exceeds backing alignment",
            ));
        }
        let aligned_size = match align_up(size, alignment) {
            Some(value) => value,
            None => return self.failed_allocation("Store arena allocation size overflow"),
        };

        let mut raw = self.reserved.load(Ordering::Relaxed);
        let (offset, next) = loop {
            let Some(offset) = align_up(raw, alignment) else {
                return self.failed_allocation("Store arena allocation offset overflow");
            };
            let Some(next) = offset.checked_add(aligned_size) else {
                return self.failed_allocation("Store arena allocation end overflow");
            };
            if next > self.capacity {
                return self.failed_allocation("Store arena capacity exhausted");
            }
            match self.reserved.compare_exchange_weak(
                raw,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break (offset, next),
                Err(observed) => raw = observed,
            }
        };

        self.successful.fetch_add(1, Ordering::Relaxed);
        self.peak.fetch_max(next, Ordering::Relaxed);
        Ok(StoreArenaAllocation {
            backing: Arc::clone(&self.backing),
            offset,
            len: size,
        })
    }

    fn failed_allocation<T>(&self, message: &'static str) -> io::Result<T> {
        self.failed.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::new(io::ErrorKind::OutOfMemory, message))
    }

    pub fn stats(&self) -> StoreArenaStats {
        StoreArenaStats {
            capacity: self.capacity,
            default_alignment: self.default_alignment,
            reserved: self.reserved.load(Ordering::Relaxed),
            peak: self.peak.load(Ordering::Relaxed),
            successful: self.successful.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

pub struct StoreArenaAllocation {
    backing: Arc<StoreArenaBacking>,
    offset: usize,
    len: usize,
}

impl StoreArenaAllocation {
    pub fn as_ptr(&self) -> *mut u8 {
        // SAFETY: construction validates offset + len within the stable
        // backing allocation, which remains alive through the retained Arc.
        unsafe { self.backing.buffer.as_ptr().add(self.offset).cast_mut() }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl fmt::Debug for StoreArenaAllocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreArenaAllocation")
            .field("base", &(self.as_ptr() as usize))
            .field("len", &self.len)
            .finish()
    }
}

impl Deref for StoreArenaAllocation {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: every successful CAS grants a unique in-bounds range. The
        // private backing has no whole-buffer mutable accessor after arena
        // publication and the Arc outlives this slice capability.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }
}

impl DerefMut for StoreArenaAllocation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: StoreArenaAllocation is not Clone and distinct allocations
        // own disjoint CAS-reserved ranges, so this mutable slice is unique.
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr(), self.len) }
    }
}

fn prefault_system_pages(buffer: &mut OwnedBuffer) {
    const FALLBACK_PAGE_SIZE: usize = 4096;
    for offset in (0..buffer.len()).step_by(FALLBACK_PAGE_SIZE) {
        // A volatile write forces a real writable page to be established
        // before the arena is shared with allocating threads.
        unsafe { std::ptr::write_volatile(buffer.as_ptr().add(offset).cast_mut(), 0) };
    }
}

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
        if alignment == 0 || !alignment.is_power_of_two() {
            return Err(StoreError::InvalidParams(format!(
                "registered allocation alignment must be a positive power of two, got {alignment}"
            )));
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
/// Engine. Rust performs only bounds-checked copies through this live mapping
/// and never frees it; it unregisters the exact range before dropping the
/// engine.
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

    pub(crate) fn copy_from_host(&self, offset: u64, source: &[u8]) -> StoreResult<()> {
        let offset = usize::try_from(offset).map_err(|_| {
            StoreError::InvalidParams("CXL replica offset exceeds addressable memory".to_string())
        })?;
        let end = offset.checked_add(source.len()).ok_or_else(|| {
            StoreError::InvalidParams("CXL write range overflows address space".to_string())
        })?;
        if end > self.len {
            return Err(StoreError::InvalidParams(format!(
                "CXL write range {offset}..{end} exceeds mapping length {}",
                self.len
            )));
        }
        // SAFETY: register_cxl_segment established a live writable mapping of
        // self.len bytes, and the checked range lies wholly inside it.
        unsafe {
            std::ptr::copy_nonoverlapping(
                source.as_ptr(),
                (self.base_addr as *mut u8).add(offset),
                source.len(),
            );
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub(crate) fn copy_to_host(&self, offset: u64, len: usize) -> StoreResult<Vec<u8>> {
        let offset = usize::try_from(offset).map_err(|_| {
            StoreError::InvalidParams("CXL replica offset exceeds addressable memory".to_string())
        })?;
        let end = offset.checked_add(len).ok_or_else(|| {
            StoreError::InvalidParams("CXL read range overflows address space".to_string())
        })?;
        if end > self.len {
            return Err(StoreError::InvalidParams(format!(
                "CXL read range {offset}..{end} exceeds mapping length {}",
                self.len
            )));
        }
        let mut destination = vec![0_u8; len];
        // SAFETY: register_cxl_segment established a live readable mapping of
        // self.len bytes, and the checked range lies wholly inside it.
        unsafe {
            std::ptr::copy_nonoverlapping(
                (self.base_addr as *const u8).add(offset),
                destination.as_mut_ptr(),
                len,
            );
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        Ok(destination)
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
    const REGULAR_REGISTRATION_ALIGNMENT: usize = 4096;

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

        #[cfg(target_os = "linux")]
        {
            return Self::allocate_for_registration_with_allocators(
                size,
                alignment,
                |page_size| unsafe { allocate_hugepage_mmap(size, page_size) },
                Self::allocate_regular_registration,
            );
        }

        #[cfg(not(target_os = "linux"))]
        {
            let policy = HugepagePolicy::from_environment();
            if policy.requested && policy.strict {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "MC_STORE_USE_HUGEPAGE requires Linux HugeTLB support",
                ));
            }
            Self::allocate_regular_registration(size, alignment)
        }
    }

    #[cfg(target_os = "linux")]
    fn allocate_for_registration_with_allocators(
        size: usize,
        alignment: usize,
        allocate_hugepage: impl FnOnce(usize) -> Option<Self>,
        allocate_regular: impl FnOnce(usize, usize) -> io::Result<Self>,
    ) -> io::Result<Self> {
        if size == 0 {
            return Ok(Self::Vec(Vec::new()));
        }
        let policy = HugepagePolicy::from_environment();
        if !policy.requested {
            return allocate_regular(size, alignment);
        }
        Self::allocate_with_hugepage_allocator(
            size,
            alignment,
            policy,
            allocate_hugepage,
            allocate_regular,
        )
    }

    fn allocate_regular_registration(size: usize, alignment: usize) -> io::Result<Self> {
        if alignment > 1 && !alignment.is_power_of_two() {
            return Self::allocate_aligned(size, alignment);
        }
        Self::allocate_aligned(size, alignment.max(Self::REGULAR_REGISTRATION_ALIGNMENT))
    }

    #[cfg(target_os = "linux")]
    fn allocate_with_hugepage_allocator(
        size: usize,
        alignment: usize,
        policy: HugepagePolicy,
        allocate_hugepage: impl FnOnce(usize) -> Option<Self>,
        allocate_regular: impl FnOnce(usize, usize) -> io::Result<Self>,
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
        allocate_regular(size, alignment)
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
            unsafe {
                populate_hugetlb_mapping(ptr.as_ptr(), *len, *page_size)?;
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
    crate::utils::string_to_bool(value).unwrap_or(false)
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
unsafe fn populate_hugetlb_mapping(ptr: *mut u8, len: usize, page_size: usize) -> io::Result<()> {
    if ptr.is_null() || len == 0 || page_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mapping, length, and page size must be valid",
        ));
    }
    let page_count = len.div_ceil(page_size);
    let workers = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(16)
        .min(page_count);
    unsafe { populate_hugetlb_pages(ptr, len, page_size, workers) }
}

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
        HUGEPAGE_1_GIB, HUGEPAGE_2_MIB, HugepagePolicy, OwnedBuffer, RegisteredBufferAllocation,
        StoreSegmentArena, page_ranges, populate_hugetlb_mapping, populate_hugetlb_pages,
    };
    use mooncake_store_core::StoreError;

    #[test]
    fn cpp_parity_one_mib_segment_budget_starts_as_two_mib_with_zero_counters() {
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        let stats = arena.stats();
        assert_eq!(stats.capacity, HUGEPAGE_2_MIB);
        assert_eq!(stats.default_alignment, 64);
        assert_eq!(stats.reserved, 0);
        assert_eq!(stats.peak, 0);
        assert_eq!(stats.successful, 0);
        assert_eq!(stats.failed, 0);
    }

    #[test]
    fn cpp_parity_one_kib_allocation_updates_count_and_reservation() {
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        let allocation = arena.allocate(1024, 64).expect("one KiB fits");
        assert!(!allocation.as_ptr().is_null());
        assert_eq!(allocation.len(), 1024);
        let stats = arena.stats();
        assert_eq!(stats.successful, 1);
        assert!((1024..=1088).contains(&stats.reserved));
    }

    #[test]
    fn cpp_parity_registered_zero_size_is_rejected_without_success_count() {
        let registered_error = RegisteredBufferAllocation::allocate(0, 64)
            .expect_err("registered zero-size allocation must fail");
        assert!(matches!(registered_error, StoreError::InvalidParams(_)));
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        assert!(arena.allocate(0, 64).is_err());
        let stats = arena.stats();
        assert_eq!(stats.successful, 0);
        assert_eq!(stats.reserved, 0);
    }

    #[test]
    fn cpp_parity_usize_max_rejects_once_without_reservation() {
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        assert!(arena.allocate(usize::MAX, 64).is_err());
        let stats = arena.stats();
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.successful, 0);
        assert_eq!(stats.reserved, 0);
    }

    #[test]
    fn cpp_parity_usize_max_minus_ten_alignment_overflow_is_nonmutating() {
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        assert!(arena.allocate(usize::MAX - 10, 64).is_err());
        let stats = arena.stats();
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.successful, 0);
        assert_eq!(stats.reserved, 0);
    }

    #[test]
    fn cpp_parity_half_usize_max_exceeds_budget_and_counts_once() {
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        assert!(arena.allocate(usize::MAX / 2, 64).is_err());
        let stats = arena.stats();
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.successful, 0);
        assert_eq!(stats.reserved, 0);
    }

    #[test]
    fn cpp_parity_exact_six_size_matrix_is_64_aligned_and_fully_writable() {
        let arena = StoreSegmentArena::new(HUGEPAGE_2_MIB, 64).expect("valid arena");
        let mut allocations = Vec::new();
        for size in [1, 63, 64, 65, 100, 1000] {
            let mut allocation = arena.allocate(size, 64).expect("matrix allocation fits");
            assert_eq!(allocation.as_ptr() as usize % 64, 0);
            allocation.fill(0xaa);
            assert!(allocation.iter().all(|byte| *byte == 0xaa));
            allocations.push(allocation);
        }
        assert_eq!(allocations.len(), 6);
    }

    #[test]
    fn cpp_parity_repeated_oom_never_advances_reserved_past_capacity() {
        let arena = StoreSegmentArena::new(1024, 64).expect("valid arena");
        let mut allocations = Vec::new();
        while let Ok(allocation) = arena.allocate(64, 0) {
            allocations.push(allocation);
        }
        let full = arena.stats();
        assert!(full.reserved <= full.capacity);
        assert!(full.failed > 0);
        let reserved = full.reserved;
        assert!(arena.allocate(1, 0).is_err());
        assert_eq!(arena.stats().reserved, reserved);
    }

    #[test]
    fn cpp_parity_sixteen_thread_oom_accounts_every_attempt() {
        let arena =
            std::sync::Arc::new(StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena"));
        let attempts_per_thread = arena.stats().capacity / (64 * 16) + 100;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let mut threads = Vec::new();
        for _ in 0..16 {
            let arena = std::sync::Arc::clone(&arena);
            let barrier = std::sync::Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                let mut successful = 0;
                let mut failed = 0;
                for _ in 0..attempts_per_thread {
                    match arena.allocate(64, 0) {
                        Ok(mut allocation) => {
                            allocation.fill(0xbb);
                            successful += 1;
                        }
                        Err(_) => failed += 1,
                    }
                }
                (successful, failed)
            }));
        }
        let (successful, failed) = threads
            .into_iter()
            .map(|thread| thread.join().expect("allocator thread"))
            .fold((0, 0), |totals, values| {
                (totals.0 + values.0, totals.1 + values.1)
            });
        assert!(successful > 0);
        assert!(failed > 0);
        assert_eq!(successful + failed, 16 * attempts_per_thread);
        let stats = arena.stats();
        assert_eq!(stats.successful, successful);
        assert_eq!(stats.failed, failed);
        assert!(stats.reserved <= stats.capacity);
    }

    #[test]
    fn cpp_parity_ten_concurrent_store_allocations_return_unique_addresses() {
        let arena =
            std::sync::Arc::new(StoreSegmentArena::new(HUGEPAGE_2_MIB, 64).expect("valid arena"));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(10));
        let threads = (0..10)
            .map(|_| {
                let arena = std::sync::Arc::clone(&arena);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    arena.allocate(1024, 64)
                })
            })
            .collect::<Vec<_>>();
        let allocations = threads
            .into_iter()
            .filter_map(|thread| thread.join().expect("allocator thread").ok())
            .collect::<Vec<_>>();
        assert!(!allocations.is_empty());
        let addresses = allocations
            .iter()
            .map(|allocation| allocation.as_ptr() as usize)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(addresses.len(), allocations.len());
    }

    #[test]
    fn cpp_parity_eight_threads_attempt_8000_validate_every_success() {
        let arena =
            std::sync::Arc::new(StoreSegmentArena::new(64 * 1024 * 1024, 64).expect("valid arena"));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let threads = (0..8_u8)
            .map(|thread_index| {
                let arena = std::sync::Arc::clone(&arena);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let mut allocations = Vec::new();
                    for _ in 0..1000 {
                        if let Ok(mut allocation) = arena.allocate(1024, 64) {
                            allocation.fill(0xcc + thread_index);
                            allocations.push(allocation);
                        }
                    }
                    allocations
                })
            })
            .collect::<Vec<_>>();
        let allocations = threads
            .into_iter()
            .flat_map(|thread| thread.join().expect("allocator thread"))
            .collect::<Vec<_>>();
        let addresses = allocations
            .iter()
            .map(|allocation| allocation.as_ptr() as usize)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(addresses.len(), allocations.len());
    }

    #[test]
    fn cpp_parity_stats_samples_remain_bounded_during_eight_thread_load() {
        let arena =
            std::sync::Arc::new(StoreSegmentArena::new(64 * 1024 * 1024, 64).expect("valid arena"));
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(9));
        let workers = (0..8)
            .map(|_| {
                let arena = std::sync::Arc::clone(&arena);
                let stop = std::sync::Arc::clone(&stop);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let _ = arena.allocate(128, 0);
                    }
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for _ in 0..100 {
            let stats = arena.stats();
            assert!(stats.reserved <= stats.capacity);
            std::thread::yield_now();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for worker in workers {
            worker.join().expect("allocator thread");
        }
        let stats = arena.stats();
        assert!(stats.reserved <= stats.capacity);
        assert!(stats.peak >= stats.reserved);
    }

    #[test]
    fn cpp_parity_sixty_four_byte_fill_stops_with_bounded_reservation() {
        let arena = StoreSegmentArena::new(4096, 64).expect("valid arena");
        let mut allocations = Vec::new();
        while let Ok(allocation) = arena.allocate(64, 0) {
            allocations.push(allocation);
        }
        assert!(!allocations.is_empty());
        let stats = arena.stats();
        assert!(stats.reserved <= stats.capacity);
        assert!(stats.failed > 0);
    }

    #[test]
    fn cpp_parity_eighty_mixed_store_allocation_attempts_validate_every_success() {
        let arena = StoreSegmentArena::new(64 * 1024 * 1024, 64).expect("valid arena");
        let mut allocations = Vec::new();
        for size in [1, 16, 64, 256, 1024, 4096, 16_384, 65_536] {
            for _ in 0..10 {
                if let Ok(mut allocation) = arena.allocate(size, 0) {
                    let address = allocation.as_ptr();
                    allocation.fill(0xdd);
                    assert_eq!(allocation.as_ptr(), address);
                    assert_eq!(allocation.len(), size);
                    assert_eq!(address as usize % 64, 0);
                    assert!(allocation.iter().all(|byte| *byte == 0xdd));
                    allocations.push(allocation);
                }
            }
        }
        assert!(!allocations.is_empty());
    }

    #[test]
    fn cpp_parity_peak_tracks_512_then_1024_reservations() {
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        let first = arena.allocate(512, 0).expect("first allocation");
        let first_stats = arena.stats();
        assert!(first_stats.peak >= 512);
        let second = arena.allocate(1024, 0).expect("second allocation");
        let second_stats = arena.stats();
        assert!(second_stats.peak >= first_stats.peak);
        assert!(second_stats.peak >= 1536);
        assert_eq!(first.len() + second.len(), 1536);
    }

    #[test]
    fn cpp_parity_exact_six_request_reservation_is_monotonic() {
        let arena = StoreSegmentArena::new(1024 * 1024, 64).expect("valid arena");
        let mut allocations = Vec::new();
        let mut prior = 0;
        for size in [1, 63, 64, 65, 4096, 1024] {
            allocations.push(arena.allocate(size, 0).expect("allocation fits"));
            let stats = arena.stats();
            assert!(stats.reserved >= prior);
            assert!(stats.peak >= stats.reserved);
            prior = stats.reserved;
        }
    }

    #[test]
    fn cpp_parity_one_byte_then_two_mib_aligned_reservation_orders_addresses() {
        let arena = StoreSegmentArena::new(8 * 1024 * 1024, 64).expect("valid arena");
        let first = arena.allocate(1, 64).expect("first allocation");
        let second = arena
            .allocate(4 * 1024 * 1024, HUGEPAGE_2_MIB)
            .expect("aligned allocation");
        let first_base = first.as_ptr() as usize;
        let second_base = second.as_ptr() as usize;
        let first_end = first_base.checked_add(first.len()).expect("first end");
        let second_end = second_base.checked_add(second.len()).expect("second end");
        assert_eq!(second_base % HUGEPAGE_2_MIB, 0);
        assert!(second_base > first_base);
        assert!(first_end <= second_base);
        assert!(second_end <= arena.backing.buffer.as_ptr() as usize + arena.stats().capacity);
    }

    #[test]
    fn cpp_parity_registered_allocation_rejects_alignment_100() {
        let error = RegisteredBufferAllocation::allocate(1024, 100)
            .expect_err("alignment 100 must be rejected");
        assert!(matches!(error, StoreError::InvalidParams(_)));
    }

    #[test]
    fn cpp_parity_exact_power_of_two_alignment_matrix_is_accepted() {
        let mut allocations = Vec::new();
        for alignment in [64, 128, 256, 512, 4096] {
            let allocation = RegisteredBufferAllocation::allocate(1024, alignment)
                .expect("power-of-two alignment");
            assert!(!allocation.as_ptr().is_null());
            assert_eq!(allocation.as_ptr() as usize % alignment, 0);
            // SAFETY: the owner retains a live allocation of exactly 1,024
            // writable bytes for the duration of these bounded accesses.
            unsafe {
                std::ptr::write_bytes(allocation.as_ptr().cast::<u8>(), 0xa5, 1024);
                assert_eq!(allocation.as_ptr().cast::<u8>().read(), 0xa5);
                assert_eq!(allocation.as_ptr().cast::<u8>().add(1023).read(), 0xa5);
            }
            allocations.push(allocation);
        }
        assert_eq!(allocations.len(), 5);
    }

    #[test]
    fn cpp_parity_exact_sixteen_mib_fit_then_one_kib_oom() {
        let arena = StoreSegmentArena::new(16 * 1024 * 1024, 64).expect("valid arena");
        let allocation = arena
            .allocate(16 * 1024 * 1024, 64)
            .expect("exact capacity fits");
        assert_eq!(allocation.len(), 16 * 1024 * 1024);
        assert!(arena.allocate(1024, 64).is_err());
        assert!(arena.stats().failed >= 1);
    }

    #[test]
    fn cpp_parity_concurrent_policy_readers_preserve_four_mib_and_128_alignment() {
        let arena =
            std::sync::Arc::new(StoreSegmentArena::new(4 * 1024 * 1024, 128).expect("valid arena"));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let readers = (0..16)
            .map(|_| {
                let arena = std::sync::Arc::clone(&arena);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let stats = arena.stats();
                    (stats.capacity, stats.default_alignment)
                })
            })
            .collect::<Vec<_>>();
        for reader in readers {
            assert_eq!(
                reader.join().expect("policy reader"),
                (4 * 1024 * 1024, 128)
            );
        }
        let allocation = arena.allocate(256, 0).expect("allocation fits");
        assert_eq!(allocation.as_ptr() as usize % 128, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cpp_parity_linux_four_mib_segment_is_resident_or_page_readable() {
        let arena = StoreSegmentArena::new(4 * 1024 * 1024, 64).expect("valid arena");
        let base = arena.backing.buffer.as_ptr();
        let capacity = arena.stats().capacity;
        assert!(!base.is_null());
        assert!(capacity > 0);
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        assert!(page_size > 0);
        let page_count = capacity.div_ceil(page_size);
        let mut residency = vec![0_u8; page_count];
        let result = unsafe {
            libc::mincore(
                base.cast_mut().cast::<libc::c_void>(),
                capacity,
                residency.as_mut_ptr(),
            )
        };
        if result == 0 {
            let resident = residency.iter().filter(|entry| **entry & 1 == 1).count();
            assert!(resident * 100 > page_count * 95);
        } else {
            let mut checksum = 0_u8;
            for offset in (0..capacity).step_by(page_size) {
                checksum ^= unsafe { base.add(offset).read_volatile() };
            }
            std::hint::black_box(checksum);
        }
    }

    #[test]
    fn cpp_parity_first_four_mib_segment_and_optional_second_are_immediately_writable() {
        let arena = StoreSegmentArena::new(8 * 1024 * 1024, 64).expect("valid arena");
        let mut first = arena.allocate(4 * 1024 * 1024, 0).expect("first four MiB");
        first.fill(0xab);
        assert!(
            (0..first.len())
                .step_by(4096)
                .all(|offset| first[offset] == 0xab)
        );
        if let Ok(mut second) = arena.allocate(4 * 1024 * 1024, 0) {
            second.fill(0xcd);
            assert!(
                (0..second.len())
                    .step_by(4096)
                    .all(|offset| second[offset] == 0xcd)
            );
            assert_eq!(first[0], 0xab);
            let first_end = (first.as_ptr() as usize)
                .checked_add(first.len())
                .expect("first end");
            assert!(first_end <= second.as_ptr() as usize);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cpp_parity_injected_legacy_fallback_is_fully_writable_at_three_points() {
        let policy = HugepagePolicy {
            requested: true,
            strict: false,
            page_size: HUGEPAGE_2_MIB,
        };
        let mut buffer = OwnedBuffer::allocate_with_hugepage_allocator(
            1024 * 1024,
            64,
            policy,
            |_| None,
            OwnedBuffer::allocate_regular_registration,
        )
        .expect("legacy regular-page fallback");
        assert!(!buffer.as_ptr().is_null());
        assert_eq!(buffer.len(), 1024 * 1024);
        buffer.fill(0xef);
        assert_eq!(buffer[0], 0xef);
        assert_eq!(buffer[512 * 1024], 0xef);
        assert_eq!(buffer[1024 * 1024 - 1], 0xef);
    }
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
    fn regular_registration_fallback_is_page_aligned_for_odd_size() {
        let mut buffer = OwnedBuffer::allocate_regular_registration(65_000, 64).unwrap();
        assert_eq!(buffer.as_ptr() as usize % 4096, 0);
        buffer[0] = 0xcd;
        buffer[64_999] = 0xab;
        assert_eq!(buffer[0], 0xcd);
        assert_eq!(buffer[64_999], 0xab);
    }

    #[test]
    fn regular_registration_repeated_allocate_free_preserves_alignment() {
        for value in 0..8_u8 {
            let mut buffer = OwnedBuffer::allocate_regular_registration(128 * 1024, 64).unwrap();
            assert_eq!(buffer.as_ptr() as usize % 4096, 0);
            buffer.fill(value);
            assert_eq!(buffer[0], value);
            assert_eq!(buffer[buffer.len() - 1], value);
        }
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
    fn store_arena_prefault_touches_each_four_kib_page_boundary() {
        let mut buffer = OwnedBuffer::allocate_aligned(8193, 4096).expect("aligned buffer");
        buffer.fill(0xff);

        super::prefault_system_pages(&mut buffer);

        assert_eq!(buffer[0], 0);
        assert_eq!(buffer[4096], 0);
        assert_eq!(buffer[8192], 0);
        assert_eq!(buffer[1], 0xff);
        assert_eq!(buffer[4095], 0xff);
        assert_eq!(buffer[4097], 0xff);
    }

    #[test]
    fn store_arena_policy_rejects_invalid_inputs_before_backing_allocation() {
        assert!(StoreSegmentArena::new(0, 64).is_err());
        assert!(StoreSegmentArena::new(1024, 100).is_err());
        assert!(StoreSegmentArena::new(usize::MAX, 64).is_err());
    }

    #[test]
    fn store_arena_range_owner_keeps_backing_alive_after_arena_drop() {
        let mut allocation = {
            let arena = StoreSegmentArena::new(1024, 64).expect("valid arena");
            arena.allocate(1024, 64).expect("allocation fits")
        };

        allocation.fill(0x5a);
        assert!(allocation.iter().all(|byte| *byte == 0x5a));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cpp_parity_mmap_arena_fallback_test_cpp_mmaparenafallbacktest_populatehugetlbmappingusesconfiguredpagestride_906d02d3()
     {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "memory_ffi::tests::configured_hugetlb_stride_subprocess_helper",
                "--nocapture",
            ])
            .env("MC_STORE_HUGEPAGE_STRIDE_SUBPROCESS", "1")
            .env("MC_STORE_USE_HUGEPAGE", "1")
            .env("MC_STORE_HUGEPAGE_SIZE", "2MB")
            .output()
            .expect("spawn isolated HugeTLB stride verifier");
        assert!(
            output.status.success(),
            "HugeTLB stride subprocess failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn configured_hugetlb_stride_subprocess_helper() {
        if std::env::var_os("MC_STORE_HUGEPAGE_STRIDE_SUBPROCESS").is_none() {
            return;
        }

        const PAGE_COUNT: usize = 3;
        const MAP_SIZE: usize = PAGE_COUNT * HUGEPAGE_2_MIB;
        let mapping = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                MAP_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert_ne!(mapping, libc::MAP_FAILED);
        let bytes = mapping.cast::<u8>();
        for page in 0..PAGE_COUNT {
            unsafe { bytes.add(page * HUGEPAGE_2_MIB).write(0xab) };
        }

        let policy = HugepagePolicy::from_environment();
        assert!(policy.requested);
        assert!(policy.strict);
        assert_eq!(policy.page_size, HUGEPAGE_2_MIB);
        unsafe { populate_hugetlb_mapping(bytes, MAP_SIZE, policy.page_size) }
            .expect("configured population succeeds");
        for page in 0..PAGE_COUNT {
            assert_eq!(unsafe { bytes.add(page * HUGEPAGE_2_MIB).read() }, 0);
        }
        assert_eq!(unsafe { libc::munmap(mapping, MAP_SIZE) }, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cpp_parity_mmap_arena_fallback_test_cpp_mmaparenafallbacktest_explicithugepagerequestdoesnotsilentlyfallbacktoregularpages_16ac58db()
     {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "memory_ffi::tests::strict_hugetlb_failure_subprocess_helper",
                "--nocapture",
            ])
            .env("MC_STORE_STRICT_HUGEPAGE_SUBPROCESS", "1")
            .env("MC_STORE_USE_HUGEPAGE", "1")
            .env("MC_STORE_HUGEPAGE_SIZE", "2MB")
            .output()
            .expect("spawn isolated strict HugeTLB verifier");
        assert!(
            output.status.success(),
            "strict HugeTLB subprocess failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn strict_hugetlb_failure_subprocess_helper() {
        if std::env::var_os("MC_STORE_STRICT_HUGEPAGE_SUBPROCESS").is_none() {
            return;
        }
        let hugepage_attempts = std::cell::Cell::new(0);
        let regular_attempts = std::cell::Cell::new(0);
        let result = OwnedBuffer::allocate_for_registration_with_allocators(
            64 * 1024,
            64,
            |_| {
                hugepage_attempts.set(hugepage_attempts.get() + 1);
                None
            },
            |size, alignment| {
                regular_attempts.set(regular_attempts.get() + 1);
                OwnedBuffer::allocate_regular_registration(size, alignment)
            },
        );
        let error = match result {
            Ok(_) => panic!("explicit HugeTLB failure must fail closed"),
            Err(error) => error,
        };
        assert_eq!(hugepage_attempts.get(), 1);
        assert_eq!(regular_attempts.get(), 0);
        assert!(error.to_string().contains("HugeTLB mmap failed"));
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
        let buffer = OwnedBuffer::allocate_with_hugepage_allocator(
            16,
            1,
            legacy,
            |_| None,
            OwnedBuffer::allocate_regular_registration,
        )
        .unwrap();

        assert_eq!(buffer.len(), 16);
        assert_eq!(buffer.as_ptr() as usize % 4096, 0);

        let strict = HugepagePolicy {
            strict: true,
            ..legacy
        };
        assert!(
            OwnedBuffer::allocate_with_hugepage_allocator(
                16,
                1,
                strict,
                |_| None,
                OwnedBuffer::allocate_regular_registration,
            )
            .is_err()
        );
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

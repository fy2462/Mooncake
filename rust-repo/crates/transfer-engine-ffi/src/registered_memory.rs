//! Owner-bearing capabilities for Transfer Engine memory registrations.
//!
//! The raw registration ABI remains available for low-level adapters, but
//! Store/client code should use this module so allocation ownership, address
//! range, access permission, engine identity, generation, and in-flight region
//! leases travel together.

use crate::{
    Opcode, OwnedBatchId as BatchId, SegmentId, TransferEngine, TransferEngineError,
    TransferEngineResult, TransferRequest, TransferStatus,
};
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

/// An allocation owner whose address remains stable for its entire lifetime.
///
/// # Safety
///
/// Implementors must guarantee that:
///
/// - `base_address()` points to `length()` contiguous bytes;
/// - the allocation remains valid and at the same address until `self` drops;
/// - reads and writes permitted by the registration are valid for that range;
/// - dropping `self` releases the allocation owner only after native access has
///   quiesced.
pub unsafe trait StableMemoryOwner: fmt::Debug + Send + Sync + 'static {
    fn base_address(&self) -> NonNull<c_void>;
    fn length(&self) -> usize;
}

/// Local access permitted through a typed registration capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisteredMemoryAccess {
    ReadOnly,
    ReadWrite,
}

impl RegisteredMemoryAccess {
    fn writable(self) -> bool {
        matches!(self, Self::ReadWrite)
    }
}

/// Opaque identity for one exact engine-bound registration generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RegisteredMemoryId {
    engine_instance_id: u64,
    base: usize,
    generation: u64,
}

impl RegisteredMemoryId {
    pub fn base_address(self) -> usize {
        self.base
    }

    pub fn generation(self) -> u64 {
        self.generation
    }
}

pub(crate) struct RegistrationRegistry {
    registrations: BTreeMap<usize, Weak<RegisteredMemoryState>>,
    blocked_after_failed_unregister: BTreeMap<usize, (usize, u64)>,
}

impl RegistrationRegistry {
    pub(crate) fn new() -> Self {
        Self {
            registrations: BTreeMap::new(),
            blocked_after_failed_unregister: BTreeMap::new(),
        }
    }

    fn retain_live(&mut self) {
        self.registrations
            .retain(|_, registration| registration.strong_count() != 0);
    }

    fn remove_generation(&mut self, base: usize, generation: u64) {
        let matches = self
            .registrations
            .get(&base)
            .is_some_and(|registration| match registration.upgrade() {
                Some(state) => state.generation == generation,
                None => true,
            });
        if matches {
            self.registrations.remove(&base);
        }
    }
}

impl fmt::Debug for RegistrationRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistrationRegistry")
            .field("registrations", &self.registrations.len())
            .field(
                "blocked_after_failed_unregister",
                &self.blocked_after_failed_unregister.len(),
            )
            .finish()
    }
}

struct RegisteredMemoryState {
    engine: Arc<TransferEngine>,
    base: usize,
    len: usize,
    location: String,
    generation: u64,
    access: RegisteredMemoryAccess,
    active: AtomicBool,
    safe_in_flight: AtomicBool,
    owner: ManuallyDrop<Box<dyn StableMemoryOwner>>,
}

impl fmt::Debug for RegisteredMemoryState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredMemoryState")
            .field("engine_instance_id", &self.engine.instance_id)
            .field("base", &format_args!("{:#x}", self.base))
            .field("len", &self.len)
            .field("location", &self.location)
            .field("generation", &self.generation)
            .field("access", &self.access)
            .field("active", &self.active.load(Ordering::Acquire))
            .field(
                "safe_in_flight",
                &self.safe_in_flight.load(Ordering::Acquire),
            )
            .finish()
    }
}

impl Drop for RegisteredMemoryState {
    fn drop(&mut self) {
        let mut release_owner = true;
        if self.active.swap(false, Ordering::AcqRel) {
            let mut registry = self
                .engine
                .memory_registrations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let unregistered = unsafe {
                self.engine
                    .unregister_local_memory(self.base as *mut c_void)
            };
            if let Err(error) = unregistered {
                release_owner = false;
                registry
                    .blocked_after_failed_unregister
                    .insert(self.base, (self.len, self.generation));
                tracing::error!(
                    %error,
                    generation = self.generation,
                    base = self.base,
                    "leaking registered memory owner because native unregistration failed"
                );
            }
            registry.remove_generation(self.base, self.generation);
        }
        if release_owner {
            unsafe {
                ManuallyDrop::drop(&mut self.owner);
            }
        }
    }
}

/// Owner-bearing handle for one exact native memory registration.
///
/// This handle is intentionally non-`Clone`. Region capabilities clone only
/// the internal lease, and explicit unregistration is rejected while any such
/// lease exists.
#[derive(Debug)]
pub struct RegisteredMemory {
    state: Option<Arc<RegisteredMemoryState>>,
}

impl RegisteredMemory {
    fn state(&self) -> TransferEngineResult<&Arc<RegisteredMemoryState>> {
        self.state
            .as_ref()
            .ok_or(TransferEngineError::MemoryRegistrationAlreadyReleased)
    }

    pub fn id(&self) -> TransferEngineResult<RegisteredMemoryId> {
        let state = self.state()?;
        Ok(RegisteredMemoryId {
            engine_instance_id: state.engine.instance_id,
            base: state.base,
            generation: state.generation,
        })
    }

    pub fn len(&self) -> TransferEngineResult<usize> {
        Ok(self.state()?.len)
    }

    pub fn location(&self) -> TransferEngineResult<&str> {
        Ok(&self.state()?.location)
    }

    pub fn readable_region(
        &self,
        offset: usize,
        len: usize,
    ) -> TransferEngineResult<ReadableRegisteredMemoryRegion> {
        let state = Arc::clone(self.state()?);
        validate_subregion(&state, offset, len)?;
        Ok(ReadableRegisteredMemoryRegion { state, offset, len })
    }

    pub fn writable_region(
        &self,
        offset: usize,
        len: usize,
    ) -> TransferEngineResult<WritableRegisteredMemoryRegion> {
        let state = Arc::clone(self.state()?);
        if !state.access.writable() {
            return Err(TransferEngineError::InvalidMemoryRegistration(
                "registration is read-only".to_string(),
            ));
        }
        validate_subregion(&state, offset, len)?;
        Ok(WritableRegisteredMemoryRegion { state, offset, len })
    }

    /// Copy bytes into this registration while no region capability or native
    /// transfer can access it.
    ///
    /// Requiring `&mut self` and rejecting outstanding `Arc` leases makes this
    /// a safe staging-buffer operation: callers cannot mutate bytes while a
    /// `RegisteredTransferRequest` still owns the same registration.
    pub fn copy_from_slice(&mut self, offset: usize, source: &[u8]) -> TransferEngineResult<()> {
        let state = self.state()?;
        ensure_exclusive_local_access(state)?;
        if !state.access.writable() {
            return Err(TransferEngineError::InvalidMemoryRegistration(
                "registration is read-only".to_string(),
            ));
        }
        validate_subregion(state, offset, source.len())?;
        unsafe {
            std::ptr::copy_nonoverlapping(
                source.as_ptr(),
                (state.base + offset) as *mut u8,
                source.len(),
            );
        }
        Ok(())
    }

    /// Copy one exact range out of this registration while no region
    /// capability or native transfer can access it.
    pub fn copy_to_vec(&mut self, offset: usize, len: usize) -> TransferEngineResult<Vec<u8>> {
        let state = self.state()?;
        ensure_exclusive_local_access(state)?;
        validate_subregion(state, offset, len)?;
        let mut result = vec![0_u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(
                (state.base + offset) as *const u8,
                result.as_mut_ptr(),
                len,
            );
        }
        Ok(result)
    }
}

fn ensure_exclusive_local_access(state: &Arc<RegisteredMemoryState>) -> TransferEngineResult<()> {
    if !state.active.load(Ordering::Acquire) {
        return Err(TransferEngineError::MemoryRegistrationAlreadyReleased);
    }
    let active_leases = Arc::strong_count(state).saturating_sub(1);
    if active_leases != 0 || state.safe_in_flight.load(Ordering::Acquire) {
        return Err(TransferEngineError::MemoryRegistrationBusy {
            generation: state.generation,
            active_leases: active_leases.max(1),
        });
    }
    Ok(())
}

fn validate_subregion(
    state: &RegisteredMemoryState,
    offset: usize,
    len: usize,
) -> TransferEngineResult<()> {
    let end = offset.checked_add(len).ok_or_else(|| {
        TransferEngineError::InvalidMemoryRegistration(
            "registered-memory subregion overflows usize".to_string(),
        )
    })?;
    if offset >= state.len || end > state.len {
        return Err(TransferEngineError::InvalidMemoryRegistration(format!(
            "subregion {offset}..{end} exceeds registration length {}",
            state.len
        )));
    }
    Ok(())
}

/// Read capability for one bounded registered-memory subregion.
#[derive(Clone, Debug)]
pub struct ReadableRegisteredMemoryRegion {
    state: Arc<RegisteredMemoryState>,
    offset: usize,
    len: usize,
}

impl ReadableRegisteredMemoryRegion {
    pub fn as_ptr(&self) -> *const c_void {
        (self.state.base + self.offset) as *const c_void
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

/// Write capability for one bounded registered-memory subregion.
#[derive(Clone, Debug)]
pub struct WritableRegisteredMemoryRegion {
    state: Arc<RegisteredMemoryState>,
    offset: usize,
    len: usize,
}

impl WritableRegisteredMemoryRegion {
    pub fn as_mut_ptr(&self) -> *mut c_void {
        (self.state.base + self.offset) as *mut c_void
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

/// A transfer request whose local payload is proven to be a live,
/// engine-bound registered-memory region.
///
/// The request owns a region lease. A read requires writable local memory
/// because the native engine fills it; a write requires readable local
/// memory because the native engine consumes it.
#[derive(Clone, Debug)]
pub enum RegisteredTransferRequest {
    Read {
        local: WritableRegisteredMemoryRegion,
        target_id: SegmentId,
        target_offset: u64,
        length: usize,
    },
    Write {
        local: ReadableRegisteredMemoryRegion,
        target_id: SegmentId,
        target_offset: u64,
        length: usize,
    },
}

impl RegisteredTransferRequest {
    pub fn read(
        local: WritableRegisteredMemoryRegion,
        target_id: SegmentId,
        target_offset: u64,
        length: usize,
    ) -> TransferEngineResult<Self> {
        validate_transfer_region(local.len(), target_offset, length)?;
        Ok(Self::Read {
            local,
            target_id,
            target_offset,
            length,
        })
    }

    pub fn write(
        local: ReadableRegisteredMemoryRegion,
        target_id: SegmentId,
        target_offset: u64,
        length: usize,
    ) -> TransferEngineResult<Self> {
        validate_transfer_region(local.len(), target_offset, length)?;
        Ok(Self::Write {
            local,
            target_id,
            target_offset,
            length,
        })
    }

    fn state(&self) -> &Arc<RegisteredMemoryState> {
        match self {
            Self::Read { local, .. } => &local.state,
            Self::Write { local, .. } => &local.state,
        }
    }

    fn raw_request(&self) -> TransferEngineResult<TransferRequest> {
        let (opcode, source, target_id, target_offset, length) = match self {
            Self::Read {
                local,
                target_id,
                target_offset,
                length,
            } => (
                Opcode::Read,
                local.as_mut_ptr(),
                *target_id,
                *target_offset,
                *length,
            ),
            Self::Write {
                local,
                target_id,
                target_offset,
                length,
            } => (
                Opcode::Write,
                local.as_ptr().cast_mut(),
                *target_id,
                *target_offset,
                *length,
            ),
        };
        Ok(TransferRequest {
            opcode,
            source,
            target_id,
            target_offset,
            length: u64::try_from(length).map_err(|_| {
                TransferEngineError::InvalidMemoryRegistration(
                    "registered transfer length exceeds u64".to_string(),
                )
            })?,
        })
    }
}

fn validate_transfer_region(
    region_len: usize,
    target_offset: u64,
    length: usize,
) -> TransferEngineResult<()> {
    if length == 0 || length > region_len {
        return Err(TransferEngineError::InvalidMemoryRegistration(format!(
            "registered transfer length {length} is outside region length {region_len}"
        )));
    }
    let length_u64 = u64::try_from(length).map_err(|_| {
        TransferEngineError::InvalidMemoryRegistration(
            "registered transfer length exceeds u64".to_string(),
        )
    })?;
    target_offset.checked_add(length_u64).ok_or_else(|| {
        TransferEngineError::InvalidMemoryRegistration(
            "registered transfer target range overflows u64".to_string(),
        )
    })?;
    Ok(())
}

/// The result of a safe registered-memory submission.
///
/// Native rejection does not prove that no slice was admitted, so the rejected
/// variant also owns a batch guard. The caller must poll/try-release it exactly
/// like a successful submission.
#[derive(Debug)]
pub enum RegisteredSubmitOutcome {
    Submitted(SubmittedRegisteredBatch),
    NativeRejected {
        error: TransferEngineError,
        batch: SubmittedRegisteredBatch,
    },
}

impl RegisteredSubmitOutcome {
    pub fn batch(&self) -> &SubmittedRegisteredBatch {
        match self {
            Self::Submitted(batch) | Self::NativeRejected { batch, .. } => batch,
        }
    }

    pub fn batch_mut(&mut self) -> &mut SubmittedRegisteredBatch {
        match self {
            Self::Submitted(batch) | Self::NativeRejected { batch, .. } => batch,
        }
    }
}

/// Owns an allocated native batch and every registered-memory lease referenced
/// by its requests until `try_release` proves native quiescence.
#[derive(Debug)]
pub struct SubmittedRegisteredBatch {
    engine: Arc<TransferEngine>,
    batch_id: Option<BatchId>,
    requests: Option<Vec<RegisteredTransferRequest>>,
    claimed_registrations: Option<Vec<Arc<RegisteredMemoryState>>>,
}

impl SubmittedRegisteredBatch {
    pub fn batch_id(&self) -> TransferEngineResult<&BatchId> {
        self.batch_id
            .as_ref()
            .ok_or(TransferEngineError::BatchAlreadyReleased)
    }

    pub fn request_count(&self) -> usize {
        self.requests.as_ref().map_or(0, Vec::len)
    }

    pub fn transfer_status(&self, task_id: usize) -> TransferEngineResult<TransferStatus> {
        let batch_id = self.batch_id()?;
        if task_id >= self.request_count() {
            return Err(TransferEngineError::InvalidMemoryRegistration(format!(
                "task id {task_id} exceeds registered batch size {}",
                self.request_count()
            )));
        }
        self.engine.get_owned_transfer_status(batch_id, task_id)
    }

    /// Try to release the native batch. `BatchBusy` retains every lease and
    /// may be retried later. Any other error also retains the guard because
    /// quiescence was not proven.
    pub fn try_release(&mut self) -> TransferEngineResult<()> {
        let batch_id = self
            .batch_id
            .as_mut()
            .ok_or(TransferEngineError::BatchAlreadyReleased)?;
        self.engine.free_owned_batch_id(batch_id)?;
        self.batch_id.take();
        self.requests.take();
        if let Some(claims) = self.claimed_registrations.take() {
            for state in claims {
                state.safe_in_flight.store(false, Ordering::Release);
            }
        }
        Ok(())
    }

    pub fn is_released(&self) -> bool {
        self.batch_id.is_none()
    }
}

impl Drop for SubmittedRegisteredBatch {
    fn drop(&mut self) {
        let Some(batch_id) = self.batch_id.take() else {
            return;
        };
        let requests = self.requests.take().unwrap_or_default();
        let claims = self.claimed_registrations.take().unwrap_or_default();
        tracing::error!(
            batch_id = batch_id.as_raw(),
            request_count = requests.len(),
            "leaking abandoned registered transfer batch because native quiescence was not proven"
        );
        // Leaking the leases is the only memory-safe fallback: native code may
        // still access them, and Drop cannot block indefinitely.
        std::mem::forget(batch_id);
        std::mem::forget(requests);
        std::mem::forget(claims);
    }
}

impl TransferEngine {
    /// Register stable owner-backed memory and return a typed capability.
    pub fn register_owned_memory<O>(
        self: &Arc<Self>,
        owner: O,
        location: &str,
        remote_accessible: bool,
        access: RegisteredMemoryAccess,
    ) -> TransferEngineResult<RegisteredMemory>
    where
        O: StableMemoryOwner,
    {
        let base = owner.base_address().as_ptr() as usize;
        let len = owner.length();
        if base == 0 || len == 0 {
            return Err(TransferEngineError::InvalidMemoryRegistration(
                "base address and length must be non-zero".to_string(),
            ));
        }
        if location.trim().is_empty() {
            return Err(TransferEngineError::InvalidMemoryRegistration(
                "location must not be empty".to_string(),
            ));
        }
        let end = base.checked_add(len).ok_or_else(|| {
            TransferEngineError::InvalidMemoryRegistration(
                "registered-memory range overflows usize".to_string(),
            )
        })?;

        let generation = self
            .next_memory_registration_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                TransferEngineError::InvalidMemoryRegistration(
                    "registration generation exhausted".to_string(),
                )
            })?;
        let state = Arc::new(RegisteredMemoryState {
            engine: Arc::clone(self),
            base,
            len,
            location: location.to_string(),
            generation,
            access,
            active: AtomicBool::new(false),
            safe_in_flight: AtomicBool::new(false),
            owner: ManuallyDrop::new(Box::new(owner)),
        });

        let mut registry = self
            .memory_registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.retain_live();
        for existing in registry.registrations.values().filter_map(Weak::upgrade) {
            let existing_end = existing.base.checked_add(existing.len).ok_or_else(|| {
                TransferEngineError::InvalidMemoryRegistration(
                    "stored registration range overflows usize".to_string(),
                )
            })?;
            if base < existing_end && existing.base < end {
                return Err(TransferEngineError::InvalidMemoryRegistration(format!(
                    "range {base:#x}..{end:#x} overlaps generation {} at {:#x}..{existing_end:#x}",
                    existing.generation, existing.base
                )));
            }
        }
        for (&blocked_base, &(blocked_len, blocked_generation)) in
            &registry.blocked_after_failed_unregister
        {
            let blocked_end = blocked_base.checked_add(blocked_len).ok_or_else(|| {
                TransferEngineError::InvalidMemoryRegistration(
                    "blocked registration range overflows usize".to_string(),
                )
            })?;
            if base < blocked_end && blocked_base < end {
                return Err(TransferEngineError::InvalidMemoryRegistration(format!(
                    "range {base:#x}..{end:#x} overlaps leaked generation {blocked_generation} at {blocked_base:#x}..{blocked_end:#x}"
                )));
            }
        }

        unsafe {
            self.register_local_memory(base as *mut c_void, len, location, remote_accessible)?;
        }
        state.active.store(true, Ordering::Release);
        registry.registrations.insert(base, Arc::downgrade(&state));
        Ok(RegisteredMemory { state: Some(state) })
    }

    /// Unregister a typed registration after all region leases quiesce.
    pub fn unregister_owned_memory(
        &self,
        registration: &mut RegisteredMemory,
    ) -> TransferEngineResult<()> {
        let state = registration.state()?;
        if state.engine.instance_id != self.instance_id {
            return Err(TransferEngineError::MemoryRegistrationOwnershipMismatch);
        }
        let active_leases = Arc::strong_count(state).saturating_sub(1);
        if active_leases != 0 {
            return Err(TransferEngineError::MemoryRegistrationBusy {
                generation: state.generation,
                active_leases,
            });
        }

        {
            let mut registry = self
                .memory_registrations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            unsafe {
                self.unregister_local_memory(state.base as *mut c_void)?;
            }
            state.active.store(false, Ordering::Release);
            registry.remove_generation(state.base, state.generation);
        }
        registration.state.take();
        Ok(())
    }

    /// Submit owner-bearing registered-memory requests without exposing raw
    /// pointers or `unsafe` to the caller.
    pub fn submit_registered_transfer(
        self: &Arc<Self>,
        requests: Vec<RegisteredTransferRequest>,
    ) -> TransferEngineResult<RegisteredSubmitOutcome> {
        if requests.is_empty() {
            return Err(TransferEngineError::InvalidMemoryRegistration(
                "registered transfer batch must not be empty".to_string(),
            ));
        }
        for request in &requests {
            let state = request.state();
            if state.engine.instance_id != self.instance_id {
                return Err(TransferEngineError::MemoryRegistrationOwnershipMismatch);
            }
            if !state.active.load(Ordering::Acquire) {
                return Err(TransferEngineError::MemoryRegistrationAlreadyReleased);
            }
        }

        let mut claimed_registrations = Vec::<Arc<RegisteredMemoryState>>::new();
        for request in &requests {
            let state = request.state();
            if claimed_registrations
                .iter()
                .any(|claimed| Arc::ptr_eq(claimed, state))
            {
                continue;
            }
            if state
                .safe_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                for claimed in claimed_registrations {
                    claimed.safe_in_flight.store(false, Ordering::Release);
                }
                return Err(TransferEngineError::MemoryRegistrationInFlight {
                    generation: state.generation,
                });
            }
            claimed_registrations.push(Arc::clone(state));
        }

        let raw_requests = requests
            .iter()
            .map(RegisteredTransferRequest::raw_request)
            .collect::<TransferEngineResult<Vec<_>>>()
            .inspect_err(|_| {
                for claimed in &claimed_registrations {
                    claimed.safe_in_flight.store(false, Ordering::Release);
                }
            })?;
        let batch_id = self
            .allocate_owned_batch_id(requests.len())
            .inspect_err(|_| {
                for claimed in &claimed_registrations {
                    claimed.safe_in_flight.store(false, Ordering::Release);
                }
            })?;
        let submit_result = unsafe { self.submit_owned_transfer(&batch_id, &raw_requests) };
        let batch = SubmittedRegisteredBatch {
            engine: Arc::clone(self),
            batch_id: Some(batch_id),
            requests: Some(requests),
            claimed_registrations: Some(claimed_registrations),
        };
        Ok(match submit_result {
            Ok(()) => RegisteredSubmitOutcome::Submitted(batch),
            Err(error) => RegisteredSubmitOutcome::NativeRejected { error, batch },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::validate_transfer_region;

    #[test]
    fn registered_transfer_bounds_are_checked_before_native_submission() {
        assert!(validate_transfer_region(16, 8, 16).is_ok());
        assert!(validate_transfer_region(16, 0, 0).is_err());
        assert!(validate_transfer_region(16, 0, 17).is_err());
        assert!(validate_transfer_region(16, u64::MAX, 1).is_err());
    }
}

use super::buffer::OwnedBuffer;
use mooncake_store_core::{StoreError, error::StoreResult};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use transfer_engine_ffi::{
    ReadableRegisteredMemoryRegion, RegisteredMemory, WritableRegisteredMemoryRegion,
};

/// One registered scratch allocation with an explicitly transferable
/// exclusive payload lease.
///
/// `MooncakeClient` keeps the slot while a submitted transfer (or a future
/// cancellation reaper) owns `StagingBufferLease`. Rust-side mutation and
/// reuse are rejected until the lease drops.
pub(crate) struct StagingBuffer {
    registration: Mutex<Option<RegisteredMemory>>,
    rpc_only_buffer: Mutex<Option<OwnedBuffer>>,
    in_use: Arc<AtomicBool>,
    available: Arc<Notify>,
}

impl StagingBuffer {
    pub(crate) fn registered(registration: RegisteredMemory) -> Self {
        Self {
            registration: Mutex::new(Some(registration)),
            rpc_only_buffer: Mutex::new(None),
            in_use: Arc::new(AtomicBool::new(false)),
            available: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn rpc_only(buffer: OwnedBuffer) -> Self {
        Self {
            registration: Mutex::new(None),
            rpc_only_buffer: Mutex::new(Some(buffer)),
            in_use: Arc::new(AtomicBool::new(false)),
            available: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.registration
            .lock()
            .as_ref()
            .and_then(|registration| registration.len().ok())
            .or_else(|| self.rpc_only_buffer.lock().as_ref().map(OwnedBuffer::len))
            .unwrap_or(0)
    }

    pub(crate) fn base_ptr(&self) -> *const u8 {
        self.registration
            .lock()
            .as_ref()
            .and_then(|registration| registration.id().ok())
            .map_or_else(
                || {
                    self.rpc_only_buffer
                        .lock()
                        .as_ref()
                        .map_or(std::ptr::null(), OwnedBuffer::as_ptr)
                },
                |id| id.base_address() as *const u8,
            )
    }

    pub(crate) fn take_registration(&self) -> Option<RegisteredMemory> {
        self.registration.lock().take()
    }

    pub(crate) async fn wait_until_available(&self) {
        loop {
            if !self.in_use.load(Ordering::Acquire) {
                return;
            }
            let notified = self.available.notified();
            if !self.in_use.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn validate_lease(&self, lease: &StagingBufferLease) -> StoreResult<()> {
        if !Arc::ptr_eq(&self.in_use, &lease.in_use) || !self.in_use.load(Ordering::Acquire) {
            return Err(StoreError::OperationFailed(-1));
        }
        Ok(())
    }

    pub(crate) fn copy_from_slice(
        &self,
        lease: &StagingBufferLease,
        offset: usize,
        source: &[u8],
    ) -> StoreResult<()> {
        self.validate_lease(lease)?;
        if let Some(registration) = self.registration.lock().as_mut() {
            registration.copy_from_slice(offset, source)?;
            return Ok(());
        }
        self.rpc_only_buffer
            .lock()
            .as_ref()
            .ok_or_else(|| StoreError::Internal("staging buffer is unavailable".to_string()))?
            .copy_from_slice(offset, source)
    }

    pub(crate) fn copy_to_vec(
        &self,
        lease: &StagingBufferLease,
        offset: usize,
        len: usize,
    ) -> StoreResult<Vec<u8>> {
        self.validate_lease(lease)?;
        if let Some(registration) = self.registration.lock().as_mut() {
            return Ok(registration.copy_to_vec(offset, len)?);
        }
        self.rpc_only_buffer
            .lock()
            .as_ref()
            .ok_or_else(|| StoreError::Internal("staging buffer is unavailable".to_string()))?
            .copy_to_vec(offset, len)
    }

    pub(crate) fn readable_region(
        &self,
        lease: &StagingBufferLease,
        offset: usize,
        len: usize,
    ) -> StoreResult<ReadableRegisteredMemoryRegion> {
        self.validate_lease(lease)?;
        Ok(self
            .registration
            .lock()
            .as_ref()
            .ok_or_else(|| StoreError::OperationFailed(-1))?
            .readable_region(offset, len)?)
    }

    pub(crate) fn writable_region(
        &self,
        lease: &StagingBufferLease,
        offset: usize,
        len: usize,
    ) -> StoreResult<WritableRegisteredMemoryRegion> {
        self.validate_lease(lease)?;
        Ok(self
            .registration
            .lock()
            .as_ref()
            .ok_or_else(|| StoreError::OperationFailed(-1))?
            .writable_region(offset, len)?)
    }

    pub(crate) fn lease(&self) -> StoreResult<StagingBufferLease> {
        self.in_use
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| StoreError::OperationFailed(-1))?;
        Ok(StagingBufferLease {
            in_use: Arc::clone(&self.in_use),
            available: Arc::clone(&self.available),
        })
    }
}

/// Transferable ownership proof for native access to a staging allocation.
///
/// This type is intentionally non-`Clone`. A cancellation guard can move it
/// into a background reaper; the client cannot mutate or reuse the allocation
/// until that owner drops the lease after native quiescence.
pub(crate) struct StagingBufferLease {
    in_use: Arc<AtomicBool>,
    available: Arc<Notify>,
}

impl Drop for StagingBufferLease {
    fn drop(&mut self) {
        self.in_use.store(false, Ordering::Release);
        // Wake every registered waiter; retain one permit as well so a waiter
        // between its state check and first poll cannot miss availability.
        self.available.notify_waiters();
        self.available.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_copy_requires_the_current_exclusive_lease() {
        let staging = StagingBuffer::rpc_only(OwnedBuffer::allocate(8));
        let lease = staging.lease().unwrap();
        assert!(staging.lease().is_err());
        staging.copy_from_slice(&lease, 2, &[1_u8, 2, 3]).unwrap();
        assert_eq!(staging.copy_to_vec(&lease, 2, 3).unwrap(), [1, 2, 3]);

        let unrelated = StagingBuffer::rpc_only(OwnedBuffer::allocate(8));
        let unrelated_lease = unrelated.lease().unwrap();
        assert!(staging.copy_from_slice(&unrelated_lease, 0, &[9]).is_err());
        drop(unrelated_lease);
        drop(lease);
        assert!(staging.lease().is_ok());
    }
}

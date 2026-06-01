//! Offload batch tracking — maps batch_id to allocated buffers for lifecycle management.
//! C++ equivalent: `FileStorage::client_buffer_allocated_batches_` map.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks an active offload read batch.
/// Each batch holds registered TE buffers that peers can RDMA-read from.
pub(crate) struct OffloadBatch {
    /// Raw host memory buffers (one per key).
    pub buffers: Vec<Vec<u8>>,
    /// TE-registered buffer pointers (one per key).
    pub pointers: Vec<u64>,
}

/// Thread-safe registry of active offload batches.
/// C++ equivalent: `std::unordered_map<uint64_t, AllocatedBatch>` in FileStorage.
pub(crate) struct OffloadBufferPool {
    next_batch_id: AtomicU64,
    batches: Mutex<HashMap<u64, OffloadBatch>>,
}

impl OffloadBufferPool {
    pub fn new() -> Self {
        Self {
            next_batch_id: AtomicU64::new(1),
            batches: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate a batch ID and register the batch.
    /// Returns the assigned batch_id.
    pub fn register(&self, batch: OffloadBatch) -> u64 {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);
        self.batches.lock().insert(batch_id, batch);
        batch_id
    }

    /// Release a batch and return its buffers (which will be dropped on the caller side).
    /// The caller is responsible for unregistering the TE buffers before calling this.
    /// C++ equivalent: `FileStorage::ReleaseBuffer`
    pub fn release(&self, batch_id: u64) -> Option<OffloadBatch> {
        self.batches.lock().remove(&batch_id)
    }
}

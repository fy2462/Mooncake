use parking_lot::Mutex;
use std::sync::Arc;

/// Offset-allocator-based sub-allocation within a buffer.
/// Manages free regions and coalesces adjacent free blocks on deallocation.
#[derive(Debug)]
pub struct ClientBufferAllocator {
    #[allow(dead_code, reason = "backing memory for allocations")]
    buffer: Vec<u8>,
    /// Sorted list of free regions (offset, size)
    free_regions: Vec<(usize, usize)>,
    total_size: usize,
    allocated: usize,
}

/// RAII handle for an allocated buffer region.
/// On drop, the region is returned to the allocator's free list.
#[derive(Debug)]
pub struct BufferHandle {
    pub offset: usize,
    pub size: usize,
    allocator: Option<Arc<Mutex<ClientBufferAllocator>>>,
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        if let Some(ref allocator) = self.allocator {
            allocator.lock().deallocate(self.offset, self.size);
        }
    }
}

impl ClientBufferAllocator {
    pub fn new(total_size: usize) -> Arc<Mutex<Self>> {
        let buffer = vec![0u8; total_size];
        Arc::new(Mutex::new(Self {
            buffer,
            free_regions: vec![(0, total_size)],
            total_size,
            allocated: 0,
        }))
    }

    pub fn allocate(self_: &Arc<Mutex<Self>>, size: usize) -> Option<BufferHandle> {
        let mut me = self_.lock();
        // Simple first-fit with 4K alignment
        let aligned_size = (size + 4095) & !4095;
        for i in 0..me.free_regions.len() {
            let (offset, free_size) = me.free_regions[i];
            if free_size >= aligned_size {
                me.free_regions.remove(i);
                if free_size > aligned_size {
                    me.free_regions
                        .insert(i, (offset + aligned_size, free_size - aligned_size));
                }
                me.allocated += aligned_size;
                return Some(BufferHandle {
                    offset,
                    size: aligned_size,
                    allocator: Some(Arc::clone(self_)),
                });
            }
        }
        None
    }

    fn deallocate(&mut self, offset: usize, size: usize) {
        let aligned_size = (size + 4095) & !4095;
        self.allocated = self.allocated.saturating_sub(aligned_size);

        // Insert in sorted position
        let mut insert_idx = 0;
        for (i, &(o, _)) in self.free_regions.iter().enumerate() {
            if o > offset {
                break;
            }
            insert_idx = i + 1;
        }
        self.free_regions.insert(insert_idx, (offset, aligned_size));
        self.coalesce();
    }

    fn coalesce(&mut self) {
        let mut i = 0;
        while i + 1 < self.free_regions.len() {
            if self.free_regions[i].0 + self.free_regions[i].1 == self.free_regions[i + 1].0 {
                let merged_size = self.free_regions[i].1 + self.free_regions[i + 1].1;
                self.free_regions[i].1 = merged_size;
                self.free_regions.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }

    pub fn allocated(&self) -> usize {
        self.allocated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_allocate_deallocate() {
        let allocator = ClientBufferAllocator::new(65536);
        let handle = ClientBufferAllocator::allocate(&allocator, 8192).unwrap();
        assert_eq!(handle.offset, 0);
        assert_eq!(handle.size, 8192); // 8K fits in one 4K block
        drop(handle);
        assert_eq!(allocator.lock().allocated(), 0);
    }

    #[test]
    fn test_multiple_allocations() {
        let allocator = ClientBufferAllocator::new(65536);
        let h1 = ClientBufferAllocator::allocate(&allocator, 4096).unwrap();
        let h2 = ClientBufferAllocator::allocate(&allocator, 4096).unwrap();
        assert_eq!(h1.offset, 0);
        assert_eq!(h2.offset, 4096);
        assert_eq!(allocator.lock().allocated(), 8192);
        drop(h1);
        drop(h2);
        assert_eq!(allocator.lock().allocated(), 0);
    }

    #[test]
    fn test_allocation_exhaustion() {
        let allocator = ClientBufferAllocator::new(4096);
        let h1 = ClientBufferAllocator::allocate(&allocator, 4096).unwrap();
        assert!(ClientBufferAllocator::allocate(&allocator, 1).is_none());
        drop(h1);
        assert!(ClientBufferAllocator::allocate(&allocator, 4096).is_some());
    }

    #[test]
    fn test_coalescing() {
        let allocator = ClientBufferAllocator::new(65536);
        let h1 = ClientBufferAllocator::allocate(&allocator, 4096).unwrap(); // offset 0
        let h2 = ClientBufferAllocator::allocate(&allocator, 4096).unwrap(); // offset 4096
        drop(h1);
        drop(h2);
        // Both regions should coalesce into one 8K region
        let h3 = ClientBufferAllocator::allocate(&allocator, 8192).unwrap();
        assert_eq!(h3.offset, 0);
        assert_eq!(h3.size, 8192);
    }

    #[test]
    fn test_4k_alignment() {
        let allocator = ClientBufferAllocator::new(65536);
        let h = ClientBufferAllocator::allocate(&allocator, 100).unwrap();
        assert_eq!(h.size, 4096); // aligned up to 4K
    }
}

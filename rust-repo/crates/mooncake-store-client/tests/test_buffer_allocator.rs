use mooncake_store_client::buffer_allocator::ClientBufferAllocator;

#[test]
fn test_basic_allocate_deallocate() {
    let allocator = ClientBufferAllocator::new(65536);
    let handle = ClientBufferAllocator::allocate(&allocator, 8192).unwrap();
    assert_eq!(handle.offset, 0);
    assert_eq!(handle.size, 8192);
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
    let allocator = ClientBufferAllocator::new(8 * 4096);
    let handles = (0..8)
        .map(|_| ClientBufferAllocator::allocate(&allocator, 4096).unwrap())
        .collect::<Vec<_>>();
    assert!(ClientBufferAllocator::allocate(&allocator, 1).is_none());
    drop(handles);
    assert!(ClientBufferAllocator::allocate(&allocator, 4096).is_some());
}

#[test]
fn test_coalescing() {
    let allocator = ClientBufferAllocator::new(65536);
    let h1 = ClientBufferAllocator::allocate(&allocator, 4096).unwrap();
    let h2 = ClientBufferAllocator::allocate(&allocator, 4096).unwrap();
    drop(h1);
    drop(h2);
    let h3 = ClientBufferAllocator::allocate(&allocator, 8192).unwrap();
    assert_eq!(h3.offset, 0);
    assert_eq!(h3.size, 8192);
}

#[test]
fn test_4k_alignment() {
    let allocator = ClientBufferAllocator::new(65536);
    let h = ClientBufferAllocator::allocate(&allocator, 100).unwrap();
    assert_eq!(h.size, 100);
    assert_eq!(allocator.lock().allocated(), 4096);
}

#[test]
fn test_zero_and_overflowing_allocations_are_rejected() {
    let allocator = ClientBufferAllocator::new(65536);
    assert!(ClientBufferAllocator::allocate(&allocator, 0).is_none());
    assert!(ClientBufferAllocator::allocate(&allocator, 65537).is_none());
    assert!(ClientBufferAllocator::allocate(&allocator, usize::MAX).is_none());
    assert_eq!(allocator.lock().allocated(), 0);
}

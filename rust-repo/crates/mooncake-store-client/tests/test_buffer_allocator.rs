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

#[test]
fn owned_allocations_are_aligned_writable_and_independent() {
    let allocator = ClientBufferAllocator::new(64 * 4096);
    let first = ClientBufferAllocator::allocate(&allocator, 13).unwrap();
    let second = ClientBufferAllocator::allocate(&allocator, 17).unwrap();
    assert_eq!(first.offset % 4096, 0);
    assert_eq!(second.offset % 4096, 0);
    first.write(b"hello, buffer").unwrap();
    second.write(b"independent bytes").unwrap();
    assert_eq!(first.read().unwrap(), b"hello, buffer");
    assert_eq!(second.read().unwrap(), b"independent bytes");
    assert!(first.write(&[0; 14]).is_err());
    drop(second);
    drop(first);
    assert_eq!(allocator.lock().allocated(), 0);
}

#[test]
fn varying_size_allocations_survive_reverse_order_release() {
    let allocator = ClientBufferAllocator::new(128 * 4096);
    let mut handles = Vec::new();
    for index in 0..100 {
        let size = index % 4096 + 1;
        let handle = ClientBufferAllocator::allocate(&allocator, size).unwrap();
        handle.write(&vec![index as u8; size]).unwrap();
        handles.push(handle);
    }
    for (index, handle) in handles.iter().enumerate() {
        assert_eq!(handle.read().unwrap(), vec![index as u8; handle.size]);
    }
    while let Some(handle) = handles.pop() {
        drop(handle);
    }
    assert_eq!(allocator.lock().allocated(), 0);
    assert!(ClientBufferAllocator::allocate(&allocator, 100 * 4096).is_some());
}

#[test]
fn cpp_parity_client_buffer_test_cpp_clientbuffertest_multipleallocations_fe5945cf() {
    const BUFFER_SIZE: usize = 1024 * 1024;
    const ALLOCATION_SIZE: usize = 64 * 1024;
    const ALLOCATION_COUNT: usize = 8;

    let allocator = ClientBufferAllocator::new(BUFFER_SIZE);
    let handles = (0..ALLOCATION_COUNT)
        .map(|index| {
            let handle = ClientBufferAllocator::allocate(&allocator, ALLOCATION_SIZE)
                .unwrap_or_else(|| panic!("allocation {index} must succeed"));
            assert_eq!(handle.size, ALLOCATION_SIZE);
            handle.write(&vec![index as u8; ALLOCATION_SIZE]).unwrap();
            handle
        })
        .collect::<Vec<_>>();

    assert_eq!(handles.len(), ALLOCATION_COUNT);
    assert_eq!(
        allocator.lock().allocated(),
        ALLOCATION_COUNT * ALLOCATION_SIZE
    );
    for (index, handle) in handles.iter().enumerate() {
        assert_eq!(handle.size, ALLOCATION_SIZE);
        assert_eq!(handle.read().unwrap(), vec![index as u8; ALLOCATION_SIZE]);
    }

    drop(handles);
    assert_eq!(allocator.lock().allocated(), 0);
}

#[test]
fn cpp_parity_client_buffer_test_cpp_clientbuffertest_smallallocation_a47348f7() {
    let allocator = ClientBufferAllocator::new(1024 * 1024);
    let handle = ClientBufferAllocator::allocate(&allocator, 1).unwrap();

    assert_eq!(handle.size, 1);
    handle.write(&[0xff]).unwrap();
    assert_eq!(handle.read().unwrap(), vec![0xff]);

    drop(handle);
    assert_eq!(allocator.lock().allocated(), 0);
}

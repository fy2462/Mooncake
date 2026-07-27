use super::*;

#[test]
fn test_cachelib_like_allocator_rounds_to_size_class_usage() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 2, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);

    let repls = a.allocate("k", 64, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].offset, 0);
    assert_eq!(a.used_bytes(&seg_id).unwrap(), 72);
}

#[test]
fn test_cachelib_like_allocator_reuses_freed_slot() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    a.add_segment(
        make_seg("cachelib:1", CACHELIB_SLAB_SIZE * 2),
        0,
        Uuid::new_v4(),
    );

    let first = a.allocate("k1", 128, 1, &ReplicateConfig::default());
    let second = a.allocate("k2", 128, 1, &ReplicateConfig::default());
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first[0].offset, second[0].offset);

    a.release(&first).unwrap();
    let reused = a.allocate("k3", 128, 1, &ReplicateConfig::default());
    assert_eq!(reused.len(), 1);
    assert_eq!(reused[0].offset, first[0].offset);
}

#[test]
fn test_cachelib_like_allocator_pool_lifecycle() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 4, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);

    let pool_ids = a.pool_ids(&seg_id).unwrap();
    assert_eq!(pool_ids.len(), 1);
    let main_pool = pool_ids[0];
    assert_eq!(a.pool_name(&seg_id, main_pool).as_deref(), Some("main"));

    assert!(
        a.shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE)
            .unwrap()
    );
    let extra_pool = a
        .add_pool(&seg_id, "secondary", CACHELIB_SLAB_SIZE)
        .unwrap();
    assert_eq!(
        a.pool_name(&seg_id, extra_pool).as_deref(),
        Some("secondary")
    );

    assert!(
        a.resize_pools(&seg_id, main_pool, extra_pool, CACHELIB_SLAB_SIZE)
            .unwrap()
    );
    assert!(
        a.shrink_pool(&seg_id, extra_pool, CACHELIB_SLAB_SIZE)
            .unwrap()
    );
    assert!(a.grow_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE).unwrap());

    let pool_ids = a.pool_ids(&seg_id).unwrap();
    assert_eq!(pool_ids, vec![main_pool, extra_pool]);
}

#[test]
fn test_cachelib_like_slab_release_resize_requires_freeing_active_allocations() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 2, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];

    let replica = a.allocate("k1", 128, 1, &ReplicateConfig::default());
    assert_eq!(replica.len(), 1);
    let class_size = a.used_bytes(&seg_id).unwrap();
    let ctx = a
        .start_slab_release(
            &seg_id,
            main_pool,
            Some(class_size),
            None,
            SlabReleaseMode::Resize,
        )
        .unwrap();
    assert!(!ctx.is_released);
    assert_eq!(ctx.active_offsets, vec![replica[0].offset]);

    assert!(a.complete_slab_release(&seg_id, &ctx).is_err());
    a.release(&replica).unwrap();
    a.complete_slab_release(&seg_id, &ctx).unwrap();

    assert!(
        a.shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE)
            .unwrap()
    );
    let secondary = a
        .add_pool(&seg_id, "secondary", CACHELIB_SLAB_SIZE)
        .unwrap();
    assert_eq!(
        a.pool_name(&seg_id, secondary).as_deref(),
        Some("secondary")
    );
}

#[test]
fn test_cachelib_like_slab_release_abort_restores_free_slots() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 2, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];

    let replica = a.allocate("k1", 128, 1, &ReplicateConfig::default());
    assert_eq!(replica.len(), 1);
    let class_size = a.used_bytes(&seg_id).unwrap();
    let ctx = a
        .start_slab_release(
            &seg_id,
            main_pool,
            Some(class_size),
            None,
            SlabReleaseMode::Resize,
        )
        .unwrap();
    a.release(&replica).unwrap();
    a.abort_slab_release(&seg_id, &ctx).unwrap();

    let reused = a.allocate("k2", 128, 1, &ReplicateConfig::default());
    assert_eq!(reused.len(), 1);
    assert_eq!(reused[0].offset, replica[0].offset);
}

#[test]
fn test_cachelib_like_pool_over_limit_and_helper_queries() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 2, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];

    let replica = a.allocate("k1", 128, 1, &ReplicateConfig::default());
    assert_eq!(replica.len(), 1);
    assert_eq!(a.bytes_unreserved(&seg_id), Some(0));

    assert!(
        a.shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE * 2)
            .unwrap()
    );
    assert_eq!(a.pool_is_over_limit(&seg_id, main_pool), Some(true));
    assert_eq!(a.pools_over_limit(&seg_id).unwrap(), vec![main_pool]);
}

#[test]
fn test_cachelib_like_add_pool_with_ensure_provisionable() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 8, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];

    assert!(
        a.shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE * 4)
            .unwrap()
    );
    let err = a
        .add_pool_with_options(&seg_id, "tiny", CACHELIB_SLAB_SIZE, true)
        .unwrap_err();
    assert!(err.contains("not provisionable"));
}

#[test]
fn test_cachelib_like_release_helpers_and_rebalance() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 2, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];
    let receiver_class_size = cachelib_allocation_class_size_for_request(256).unwrap();

    let replica = a.allocate("k1", 128, 1, &ReplicateConfig::default());
    assert_eq!(replica.len(), 1);
    let class_size = a.used_bytes(&seg_id).unwrap();
    let callback_hit = Cell::new(false);
    let ctx = a
        .start_slab_release(
            &seg_id,
            main_pool,
            Some(class_size),
            Some(receiver_class_size),
            SlabReleaseMode::Rebalance,
        )
        .unwrap();

    assert_eq!(a.all_allocs_freed(&seg_id, &ctx).unwrap(), false);
    assert_eq!(
        a.is_alloc_freed(&seg_id, &ctx, replica[0].offset).unwrap(),
        false
    );
    a.process_alloc_for_release(&seg_id, &ctx, replica[0].offset, |_| {
        callback_hit.set(true);
    })
    .unwrap();
    assert!(callback_hit.get());

    a.release(&replica).unwrap();
    assert_eq!(
        a.is_alloc_freed(&seg_id, &ctx, replica[0].offset).unwrap(),
        true
    );
    assert_eq!(a.all_allocs_freed(&seg_id, &ctx).unwrap(), true);
    a.complete_slab_release(&seg_id, &ctx).unwrap();

    let rebalanced = a.allocate("k2", 256, 1, &ReplicateConfig::default());
    assert_eq!(rebalanced.len(), 1);
    assert_eq!(rebalanced[0].offset, 0);
}

#[test]
fn test_cachelib_like_query_interfaces_match_allocations() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 3, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];

    assert_eq!(a.pool_name(&seg_id, main_pool).as_deref(), Some("main"));
    assert_eq!(a.pool_id(&seg_id, "main"), Some(main_pool));
    assert_eq!(a.memory_size(&seg_id), Some(CACHELIB_SLAB_SIZE * 3));
    assert_eq!(a.all_slabs_allocated(&seg_id), Some(true));
    assert_eq!(
        a.all_slabs_allocated_for_pool(&seg_id, main_pool),
        Some(false)
    );

    let replica = a.allocate("k1", 256, 1, &ReplicateConfig::default());
    assert_eq!(replica.len(), 1);
    let expected_class_id = cachelib_allocation_class_id_for_request(256).unwrap();
    let expected_class_size = cachelib_allocation_class_size_for_request(256).unwrap();

    assert_eq!(
        a.allocation_class_id(&seg_id, main_pool, 256).unwrap(),
        expected_class_id
    );
    assert_eq!(
        a.alloc_size_by_class_id(&seg_id, main_pool, expected_class_id)
            .unwrap(),
        expected_class_size
    );

    let info = a.alloc_info(&seg_id, replica[0].offset).unwrap();
    assert_eq!(info.pool_id, main_pool);
    assert_eq!(info.class_id, expected_class_id);
    assert_eq!(info.alloc_size, expected_class_size);
}

#[test]
fn test_cachelib_like_start_slab_release_with_hint_and_abort() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 3, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];

    let first = a.allocate("k1", 128, 1, &ReplicateConfig::default());
    let second = a.allocate("k2", 128, 1, &ReplicateConfig::default());
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    let class_size = cachelib_allocation_class_size_for_request(128).unwrap();

    let err = a
        .start_slab_release_with_options(
            &seg_id,
            main_pool,
            Some(class_size),
            None,
            SlabReleaseMode::Resize,
            Some(first[0].offset),
            || true,
        )
        .unwrap_err();
    assert!(err.contains("aborted"));

    let ctx = a
        .start_slab_release_with_options(
            &seg_id,
            main_pool,
            Some(class_size),
            None,
            SlabReleaseMode::Resize,
            Some(second[0].offset),
            || false,
        )
        .unwrap();
    assert_eq!(ctx.slab_index, 0);
    assert_eq!(ctx.active_offsets.len(), 2);
}

#[test]
fn test_cachelib_like_for_each_allocation_reports_slots_and_skips_releasing_slab() {
    let mut a = SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let (seg, used_u, cid_u) = make_seg_with_usage("cachelib:1", CACHELIB_SLAB_SIZE * 2, 0);
    let seg_id = seg.id;
    a.add_segment(seg, used_u, cid_u);
    let main_pool = a.pool_ids(&seg_id).unwrap()[0];

    let replica = a.allocate("k1", 128, 1, &ReplicateConfig::default());
    assert_eq!(replica.len(), 1);

    let mut seen_allocated = false;
    let mut seen_free = false;
    let skipped_before = a
        .for_each_allocation(&seg_id, |visit| {
            if visit.offset == replica[0].offset && visit.allocated {
                seen_allocated = true;
            }
            if visit.info.alloc_size == cachelib_allocation_class_size_for_request(128).unwrap()
                && !visit.allocated
            {
                seen_free = true;
            }
            !(seen_allocated && seen_free)
        })
        .unwrap();
    assert_eq!(skipped_before, 0);
    assert!(seen_allocated);
    assert!(seen_free);

    let class_size = cachelib_allocation_class_size_for_request(128).unwrap();
    let ctx = a
        .start_slab_release_with_options(
            &seg_id,
            main_pool,
            Some(class_size),
            None,
            SlabReleaseMode::Resize,
            Some(replica[0].offset),
            || false,
        )
        .unwrap();

    let skipped_after = a.for_each_allocation(&seg_id, |_| true).unwrap();
    assert_eq!(skipped_after, 1);
    a.abort_slab_release(&seg_id, &ctx).unwrap();
}

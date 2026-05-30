use std::cell::Cell;

mod common;
use common::{make_seg, make_seg_with_usage};

use mooncake_store_core::{ReplicateConfig, Segment};
use mooncake_store_master::allocator::{
    cachelib_allocation_class_id_for_request, cachelib_allocation_class_size_for_request,
    AllocationStrategy, MemoryAllocatorKind, SegmentAllocator, SlabReleaseMode, CACHELIB_SLAB_SIZE,
};
use uuid::Uuid;

#[test]
fn test_single_segment_single_replica() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 10000), 0, Uuid::new_v4());
    let repls = a.allocate("k", 100, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "n:1");
}

#[test]
fn test_multiple_segments_partially_filled() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    a.add_segment(make_seg("n:1", 10000), 9000, Uuid::new_v4());
    a.add_segment(make_seg("n:2", 10000), 1000, Uuid::new_v4());
    a.add_segment(make_seg("n:3", 10000), 0, Uuid::new_v4());

    let repls = a.allocate("k", 500, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "n:3");
}

#[test]
fn test_no_segment_has_enough_space() {
    let mut a = SegmentAllocator::new();
    let repls = a.allocate("k", 1000, 1, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_all_segments_too_full() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 100), 90, Uuid::new_v4());
    a.add_segment(make_seg("n:2", 100), 95, Uuid::new_v4());
    let repls = a.allocate("k", 20, 1, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_exact_fit() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 100), 50, Uuid::new_v4());
    let repls = a.allocate("k", 50, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].offset, 50);
}

#[test]
fn test_replica_count_zero() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 1000), 0, Uuid::new_v4());
    let repls = a.allocate("k", 100, 0, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_not_enough_segments_for_replicas() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 1000), 0, Uuid::new_v4());
    a.add_segment(make_seg("n:2", 1000), 0, Uuid::new_v4());
    let repls = a.allocate("k", 100, 5, &ReplicateConfig::default());
    assert_eq!(repls.len(), 2);
}

#[test]
fn test_preferred_segment_nonexistent() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("real:1", 1000), 0, Uuid::new_v4());
    let config = ReplicateConfig {
        preferred_segment: "ghost:1".into(),
        ..Default::default()
    };
    let repls = a.allocate("k", 100, 1, &config);
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "real:1");
}

#[test]
fn test_preferred_segment_no_space() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("pref:1", 100), 100, Uuid::new_v4());
    a.add_segment(make_seg("fallback:1", 1000), 0, Uuid::new_v4());
    let config = ReplicateConfig {
        preferred_segment: "pref:1".into(),
        ..Default::default()
    };
    let repls = a.allocate("k", 50, 1, &config);
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "fallback:1");
}

#[test]
fn test_random_strategy_uses_available_segments() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::Random);
    for i in 1..=5 {
        a.add_segment(make_seg(&format!("n:{}", i), 10000), 0, Uuid::new_v4());
    }
    let repls = a.allocate("k", 100, 3, &ReplicateConfig::default());
    assert_eq!(repls.len(), 3);
    let mut names: Vec<&str> = repls.iter().map(|r| r.segment_name.as_str()).collect();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 3);
}

#[test]
fn test_remove_segment_and_reallocate() {
    let mut a = SegmentAllocator::new();
    let sid = Uuid::new_v4();
    a.add_segment(
        Segment {
            id: sid,
            name: "to-remove:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
        },
        0,
        Uuid::new_v4(),
    );
    let repls = a.allocate("k", 100, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);

    a.remove_segment(&sid);
    let repls = a.allocate("k", 100, 1, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_allocator_new_is_empty() {
    let mut a = SegmentAllocator::new();
    let repls = a.allocate("k", 1, 1, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_many_small_segments() {
    let mut a = SegmentAllocator::new();
    for i in 0..20 {
        a.add_segment(make_seg(&format!("s{}:1", i), 100), 0, Uuid::new_v4());
    }
    let repls = a.allocate("k", 50, 10, &ReplicateConfig::default());
    assert_eq!(repls.len(), 10);
}

#[test]
fn test_large_object_no_segment() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("big:1", 1_000_000), 0, Uuid::new_v4());
    let repls = a.allocate("huge_key", 2_000_000, 1, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_free_ratio_sort_order() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    a.add_segment(make_seg("most_free:1", 10000), 0, Uuid::new_v4());
    a.add_segment(make_seg("some_free:1", 10000), 5000, Uuid::new_v4());
    a.add_segment(make_seg("little_free:1", 10000), 9000, Uuid::new_v4());

    let repls = a.allocate("k", 500, 2, &ReplicateConfig::default());
    assert_eq!(repls.len(), 2);
    assert_eq!(repls[0].segment_name, "most_free:1");
    assert_eq!(repls[1].segment_name, "some_free:1");
}

#[test]
fn test_replicas_on_different_segments() {
    let mut a = SegmentAllocator::new();
    for i in 0..4 {
        a.add_segment(make_seg(&format!("n{}:1", i), 10000), 0, Uuid::new_v4());
    }
    let repls = a.allocate("k", 100, 4, &ReplicateConfig::default());
    assert_eq!(repls.len(), 4);
    let segments: Vec<&str> = repls.iter().map(|r| r.segment_name.as_str()).collect();
    for (i, s1) in segments.iter().enumerate() {
        for (j, s2) in segments.iter().enumerate() {
            if i != j {
                assert_ne!(s1, s2);
            }
        }
    }
}

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

    a.release(&first);
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

    assert!(a
        .shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE)
        .unwrap());
    let extra_pool = a
        .add_pool(&seg_id, "secondary", CACHELIB_SLAB_SIZE)
        .unwrap();
    assert_eq!(
        a.pool_name(&seg_id, extra_pool).as_deref(),
        Some("secondary")
    );

    assert!(a
        .resize_pools(&seg_id, main_pool, extra_pool, CACHELIB_SLAB_SIZE)
        .unwrap());
    assert!(a
        .shrink_pool(&seg_id, extra_pool, CACHELIB_SLAB_SIZE)
        .unwrap());
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
    a.release(&replica);
    a.complete_slab_release(&seg_id, &ctx).unwrap();

    assert!(a
        .shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE)
        .unwrap());
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
    a.release(&replica);
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

    assert!(a
        .shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE * 2)
        .unwrap());
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

    assert!(a
        .shrink_pool(&seg_id, main_pool, CACHELIB_SLAB_SIZE * 4)
        .unwrap());
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

    a.release(&replica);
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

#[test]
fn test_composite_sort_same_node_before_preferred_then_free_ratio() {
    let client = Uuid::new_v4();
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);

    // Layout (priority: same_node > free_ratio):
    //   seg-B-90:  host "B" (other), 90% free  → 2nd (same host, highest free)
    //   seg-B-50:  host "B" (other), 50% free  → 3rd (same host, less free)
    //   seg-A-90:  host "A" (different), 90% free → 1st (because "B" is same host, not "A"
    //              wait... client is `other`, so "B" is same host. "A" is different host.
    //              Let me re-think.)
    //
    // Actually: client_id determines preferred host. The segment's "host" is derived
    // from segment_name via `segment_host()` which splits on ':'. So "A:10001" → host "A".
    //
    // Scenario: client has host "A", preferred_segment="pref-seg"(host "B").
    // Priority from original C++: same_node > preferred > free_ratio.
    // After same_node sort: "A:*" segments first, "B:*" second.
    // Within each group: preferred first, then by free_ratio.
    // Result: "A:*" come first (same host), then "pref-seg" (different host, preferred),
    // then remaining "B:*" by free_ratio.
    let sid_same_90 = Uuid::new_v4();
    a.add_segment(
        Segment {
            id: sid_same_90,
            name: "A:10001".into(),
            size: 10000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
        },
        1000,
        Uuid::new_v4(),
    );

    let sid_same_10 = Uuid::new_v4();
    a.add_segment(
        Segment {
            id: sid_same_10,
            name: "A:10002".into(),
            size: 10000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
        },
        9000,
        Uuid::new_v4(),
    );

    let sid_pref = Uuid::new_v4();
    a.add_segment(
        Segment {
            id: sid_pref,
            name: "pref-seg".into(),
            size: 10000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
        },
        1000,
        Uuid::new_v4(),
    ); // host "pref-seg" → different from "A"

    let sid_other = Uuid::new_v4();
    a.add_segment(
        Segment {
            id: sid_other,
            name: "B:10003".into(),
            size: 10000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
        },
        5000,
        Uuid::new_v4(),
    ); // host "B" → different

    // Client is on "A" (segments A:10001 and A:10002 are linked to any client on "A")
    // Set client_id to match the host "A" behavior by linking it to a segment on "A".
    let config = ReplicateConfig {
        preferred_segment: "pref-seg".into(),
        prefer_alloc_in_same_node: true,
        ..Default::default()
    };
    // allocate_for_client uses client_id to find a segment with matching client_id to
    // determine preferred_host. We need client to match a segment on host "A".
    // The original code: `self.segments.values().find(|state| state.client_id == client_id)`
    // → finds segment with matching client_id → gets its host → that's preferred_host.
    //
    // So we need a segment owned by `client` that is on host "A".
    let sid_owned_by_client = Uuid::new_v4();
    a.add_segment(
        Segment {
            id: sid_owned_by_client,
            name: "A:own".into(),
            size: 100,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
        },
        100,
        client,
    ); // ← this segment maps client → host "A"
       // But this segment can't allocate (100 used out of 100), so it won't be in candidates.

    let repls = a.allocate_for_client("k", Some(client), 500, 4, &config);
    assert_eq!(repls.len(), 4);
    // Priority (C++ behaviour): same_node > preferred > free_ratio
    assert_eq!(repls[0].segment_name, "A:10001", "same host, most free");
    assert_eq!(repls[1].segment_name, "A:10002", "same host, less free");
    assert_eq!(
        repls[2].segment_name, "pref-seg",
        "preferred, different host"
    );
    assert_eq!(
        repls[3].segment_name, "B:10003",
        "different host, non-preferred"
    );
}

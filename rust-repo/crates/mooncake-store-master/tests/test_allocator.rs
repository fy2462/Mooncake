use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, RwLock};

mod common;
use common::{make_seg, make_seg_with_usage};

use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment,
};
use mooncake_store_master::allocator::{
    AllocationStrategy, CACHELIB_SLAB_SIZE, MemoryAllocatorKind, SegmentAllocationError,
    SegmentAllocator, SlabReleaseMode, SsdUsageMetrics, cachelib_allocation_class_id_for_request,
    cachelib_allocation_class_size_for_request,
};
use uuid::Uuid;

fn restored_replica(segment: &Segment, offset: u64, size: u64) -> ReplicaDescriptor {
    ReplicaDescriptor {
        segment_id: segment.id,
        segment_name: segment.name.clone(),
        offset,
        size,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        refcnt: 0,
        handle_valid: true,
        base_addr: segment.base,
        protocol: segment.protocol.clone(),
    }
}

#[test]
fn test_offset_restore_rebuilds_internal_holes_from_live_replicas() {
    let segment = make_seg("restore-offset:1", 1_000);
    let client_id = Uuid::new_v4();
    let replicas = vec![
        restored_replica(&segment, 0, 100),
        restored_replica(&segment, 300, 100),
    ];
    let mut allocator = SegmentAllocator::new();
    assert_eq!(
        allocator
            .restore_segment(segment.clone(), client_id, &replicas)
            .unwrap(),
        200
    );

    let restored = allocator.allocate("hole", 150, 1, &ReplicateConfig::default());
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].offset, 100);
    let tail = allocator.allocate("tail", 250, 1, &ReplicateConfig::default());
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].offset, 400);
}

#[test]
fn test_offset_restore_rejects_overlap_and_out_of_bounds() {
    let segment = make_seg("restore-invalid:1", 1_000);
    let client_id = Uuid::new_v4();
    let mut overlap_allocator = SegmentAllocator::new();
    let overlap = vec![
        restored_replica(&segment, 100, 100),
        restored_replica(&segment, 150, 100),
    ];
    assert!(
        overlap_allocator
            .restore_segment(segment.clone(), client_id, &overlap)
            .unwrap_err()
            .contains("overlapping")
    );
    assert!(
        overlap_allocator
            .allocate("missing", 1, 1, &ReplicateConfig::default())
            .is_empty()
    );

    let mut bounds_allocator = SegmentAllocator::new();
    let out_of_bounds = vec![restored_replica(&segment, 950, 100)];
    assert!(
        bounds_allocator
            .restore_segment(segment, client_id, &out_of_bounds)
            .unwrap_err()
            .contains("exceeds")
    );
}

#[test]
fn restore_rejects_replica_segment_name_mismatch_before_installing_allocator() {
    let segment = make_seg("restore-identity:1", 1_000);
    let client_id = Uuid::new_v4();
    let mut mismatched = restored_replica(&segment, 0, 100);
    mismatched.segment_name = "other-segment:1".to_string();
    let mut allocator = SegmentAllocator::new();

    let error = allocator
        .restore_segment(segment, client_id, &[mismatched])
        .unwrap_err();
    assert!(error.contains("segment name"));
    assert!(
        allocator
            .allocate("must-not-install", 1, 1, &ReplicateConfig::default())
            .is_empty()
    );
}

#[test]
fn offset_release_rejects_stale_or_mismatched_descriptor_without_freeing_space() {
    let segment = make_seg("release-offset:1", 200);
    let segment_id = segment.id;
    let mut allocator = SegmentAllocator::new();
    allocator.add_segment(segment, 0, Uuid::new_v4());
    let allocation = allocator.allocate("first", 100, 1, &ReplicateConfig::default());
    assert_eq!(allocator.used_bytes(&segment_id), Some(100));

    let mut wrong_size = allocation[0].clone();
    wrong_size.size = 99;
    assert!(allocator.release(&[wrong_size]).is_err());
    assert_eq!(allocator.used_bytes(&segment_id), Some(100));
    assert!(
        allocator
            .allocate("must-not-overlap", 101, 1, &ReplicateConfig::default())
            .is_empty()
    );

    allocator.release(&allocation).unwrap();
    assert_eq!(allocator.used_bytes(&segment_id), Some(0));
    assert!(allocator.release(&allocation).is_err());
    assert_eq!(allocator.used_bytes(&segment_id), Some(0));
}

#[test]
fn offset_release_batch_is_validated_before_any_range_is_freed() {
    let segment = make_seg("release-batch:1", 200);
    let segment_id = segment.id;
    let mut allocator = SegmentAllocator::new();
    allocator.add_segment(segment, 0, Uuid::new_v4());
    let first = allocator.allocate("first", 100, 1, &ReplicateConfig::default());
    let second = allocator.allocate("second", 100, 1, &ReplicateConfig::default());
    let mut invalid_second = second[0].clone();
    invalid_second.offset = first[0].offset;

    assert!(
        allocator
            .release(&[first[0].clone(), invalid_second])
            .is_err()
    );
    assert_eq!(allocator.used_bytes(&segment_id), Some(200));
    assert!(
        allocator
            .allocate("still-full", 1, 1, &ReplicateConfig::default())
            .is_empty()
    );

    allocator
        .release(&[first[0].clone(), second[0].clone()])
        .unwrap();
    assert_eq!(allocator.used_bytes(&segment_id), Some(0));
}

#[test]
fn test_cachelib_restore_rebuilds_slots_without_reusing_live_offset() {
    let segment = make_seg("restore-cachelib:1", CACHELIB_SLAB_SIZE * 2);
    let segment_id = segment.id;
    let client_id = Uuid::new_v4();
    let mut original =
        SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    original.add_segment(segment.clone(), 0, client_id);
    let first = original.allocate("first", 128, 1, &ReplicateConfig::default());
    let second = original.allocate("second", 128, 1, &ReplicateConfig::default());
    assert_eq!(first[0].offset, 0);
    assert_ne!(second[0].offset, first[0].offset);

    let mut restored =
        SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let rebuilt_used = restored
        .restore_segment(segment, client_id, &second)
        .unwrap();
    assert_eq!(rebuilt_used, original.used_bytes(&segment_id).unwrap() / 2);
    let reused_hole = restored.allocate("third", 128, 1, &ReplicateConfig::default());
    assert_eq!(reused_hole.len(), 1);
    assert_eq!(reused_hole[0].offset, first[0].offset);
    assert_ne!(reused_hole[0].offset, second[0].offset);
}

#[test]
fn cachelib_restore_rejects_capacity_outside_u32_slab_index_space() {
    let oversized_capacity = (u64::from(u32::MAX) + 1)
        .checked_mul(CACHELIB_SLAB_SIZE)
        .unwrap();
    let segment = make_seg("oversized-cachelib:1", oversized_capacity);
    let mut allocator =
        SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
    let error = allocator
        .restore_segment(segment, Uuid::new_v4(), &[])
        .unwrap_err();
    assert!(error.contains("u32 slab index space"));

    let alias = cxl_alias("oversized-cxl:1", oversized_capacity);
    let mut cxl = SegmentAllocator::new()
        .with_strategy(AllocationStrategy::Cxl)
        .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
        .with_cxl_capacity(oversized_capacity);
    cxl.restore_cxl_alias(alias, Uuid::new_v4()).unwrap();
    let error = cxl.restore_cxl_allocations(&[]).unwrap_err();
    assert!(error.contains("u32 slab index space"));
}

#[test]
fn test_single_segment_single_replica() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 10000), 0, Uuid::new_v4());
    let repls = a.allocate("k", 100, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "n:1");
    assert_eq!(repls[0].protocol, "tcp");
}

#[test]
fn test_exact_segment_allocation_does_not_follow_same_name_peer() {
    let mut allocator = SegmentAllocator::new();
    let first = make_seg("same-name:1", 10_000);
    let second = make_seg("same-name:1", 10_000);
    assert_ne!(first.id, second.id);
    allocator.add_segment(first.clone(), 0, Uuid::new_v4());
    allocator.add_segment(second.clone(), 0, Uuid::new_v4());

    let replica = allocator.allocate_from_segment_id(second.id, 100).unwrap();

    assert_eq!(replica.segment_id, second.id);
    assert_ne!(replica.segment_id, first.id);
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
fn checked_allocation_reports_cpp_parameter_and_capacity_errors() {
    let mut empty = SegmentAllocator::new();
    let preferred = ReplicateConfig {
        preferred_segment: "preferred:1".into(),
        ..Default::default()
    };
    assert_eq!(
        empty
            .allocate_checked("key", 100, 1, &preferred)
            .unwrap_err(),
        SegmentAllocationError::NoAvailableHandle
    );

    let mut allocator = SegmentAllocator::new();
    allocator.add_segment(make_seg("segment:1", 1024), 0, Uuid::new_v4());
    assert_eq!(
        allocator
            .allocate_checked("key", 0, 1, &ReplicateConfig::default())
            .unwrap_err(),
        SegmentAllocationError::InvalidParams
    );
    assert_eq!(
        allocator
            .allocate_checked("key", 100, 0, &ReplicateConfig::default())
            .unwrap_err(),
        SegmentAllocationError::InvalidParams
    );
}

#[test]
fn plural_preference_and_exclusion_match_cpp_candidate_selection() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    for name in ["fallback:1", "preferred-a:1", "preferred-b:1", "excluded:1"] {
        allocator.add_segment(make_seg(name, 4096), 0, Uuid::new_v4());
    }
    let preferred = ReplicateConfig {
        preferred_segments: vec!["preferred-a:1".into(), "preferred-b:1".into()],
        ..Default::default()
    };
    let replicas = allocator.allocate("plural", 128, 2, &preferred);
    assert_eq!(
        replicas
            .iter()
            .map(|replica| replica.segment_name.as_str())
            .collect::<Vec<_>>(),
        vec!["preferred-a:1", "preferred-b:1"]
    );
    allocator.release(&replicas).unwrap();

    let preferred = ReplicateConfig {
        preferred_segment: "preferred-a:1".into(),
        ..Default::default()
    };
    let replicas =
        allocator.allocate_with_exclusions("excluded", 128, 3, &preferred, &["excluded:1".into()]);
    assert_eq!(replicas.len(), 3);
    assert_eq!(replicas[0].segment_name, "preferred-a:1");
    assert!(
        replicas
            .iter()
            .all(|replica| replica.segment_name != "excluded:1")
    );
}

#[test]
fn ssd_strategy_falls_back_filters_and_clamps_cpp_metrics_cases() {
    let mut fallback = SegmentAllocator::new().with_strategy(AllocationStrategy::SsdFreeRatioFirst);
    fallback.add_segment(make_seg("fallback:1", 4096), 0, Uuid::new_v4());
    assert_eq!(
        fallback
            .allocate("fallback", 128, 1, &ReplicateConfig::default())
            .len(),
        1
    );

    let mut allocator =
        SegmentAllocator::new().with_strategy(AllocationStrategy::SsdFreeRatioFirst);
    let over_capacity = Uuid::new_v4();
    let normal = Uuid::new_v4();
    let excluded = Uuid::new_v4();
    allocator.add_segment(make_seg("over:1", 4096), 0, over_capacity);
    allocator.add_segment(make_seg("normal:1", 4096), 0, normal);
    allocator.add_segment(make_seg("excluded:1", 4096), 0, excluded);
    let metrics = HashMap::from([
        (
            over_capacity,
            SsdUsageMetrics {
                total_capacity_bytes: 1000,
                used_bytes: 1500,
            },
        ),
        (
            normal,
            SsdUsageMetrics {
                total_capacity_bytes: 1000,
                used_bytes: 100,
            },
        ),
        (
            excluded,
            SsdUsageMetrics {
                total_capacity_bytes: 1000,
                used_bytes: 0,
            },
        ),
    ]);
    let replicas = allocator.allocate_for_client_with_exclusions(
        "metrics",
        None,
        128,
        1,
        &ReplicateConfig::default(),
        &["excluded:1".into()],
        Some(&metrics),
    );
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "normal:1");
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
            host_id: String::new(),
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
fn cpp_parity_allocation_strategy_test_cpp_allocationstrategyparameterizedtest_preferredsegmentinsufficientspace_26230ac5()
 {
    const MIB: u64 = 1024 * 1024;
    for strategy in [
        AllocationStrategy::Random,
        AllocationStrategy::FreeRatioFirst,
    ] {
        for allocator_kind in [
            MemoryAllocatorKind::Offset,
            MemoryAllocatorKind::CachelibLike,
        ] {
            let mut allocator = SegmentAllocator::new()
                .with_strategy(strategy)
                .with_memory_allocator(allocator_kind);
            allocator.add_segment(make_seg("segment1", 64 * MIB), 0, Uuid::new_v4());
            allocator.add_segment(make_seg("preferred", 64 * MIB), 0, Uuid::new_v4());
            let preferred = ReplicateConfig {
                preferred_segment: "preferred".into(),
                ..Default::default()
            };

            for index in 0..4 {
                let replicas = allocator
                    .allocate_checked(&format!("preferred-{index}"), 15 * MIB, 1, &preferred)
                    .unwrap();
                assert_eq!(replicas.len(), 1);
                assert_eq!(replicas[0].segment_name, "preferred");
                assert_eq!(replicas[0].size, 15 * MIB);
                assert_eq!(replicas[0].replica_type, ReplicaType::Memory);
            }

            let fallback = allocator
                .allocate_checked("fallback", 5 * MIB, 1, &preferred)
                .unwrap();
            assert_eq!(fallback.len(), 1);
            assert_eq!(fallback[0].segment_name, "segment1");
            assert_eq!(fallback[0].size, 5 * MIB);
            assert_eq!(fallback[0].replica_type, ReplicaType::Memory);
        }
    }
}

#[test]
fn cpp_parity_allocation_strategy_test_cpp_allocationstrategyparameterizedtest_allallocatorsfull_963d7115()
 {
    const MIB: u64 = 1024 * 1024;
    for strategy in [
        AllocationStrategy::Random,
        AllocationStrategy::FreeRatioFirst,
    ] {
        for allocator_kind in [
            MemoryAllocatorKind::Offset,
            MemoryAllocatorKind::CachelibLike,
        ] {
            let mut allocator = SegmentAllocator::new()
                .with_strategy(strategy)
                .with_memory_allocator(allocator_kind);
            allocator.add_segment(make_seg("segment1", 64 * MIB), 0, Uuid::new_v4());
            allocator.add_segment(make_seg("segment2", 64 * MIB), 0, Uuid::new_v4());

            for index in 0..8 {
                let replicas = allocator
                    .allocate_checked(
                        &format!("fill-{index}"),
                        15 * MIB,
                        1,
                        &ReplicateConfig::default(),
                    )
                    .unwrap();
                assert_eq!(replicas.len(), 1);
                assert_eq!(replicas[0].size, 15 * MIB);
                assert_eq!(replicas[0].replica_type, ReplicaType::Memory);
            }

            assert_eq!(
                allocator
                    .allocate_checked("impossible", 5 * MIB, 1, &ReplicateConfig::default())
                    .unwrap_err(),
                SegmentAllocationError::NoAvailableHandle
            );
        }
    }
}

#[test]
fn cpp_parity_allocation_strategy_test_cpp_allocationstrategyparameterizedtest_verylargesizeallocation_8ebc4bfe()
 {
    const MIB: u64 = 1024 * 1024;
    for strategy in [
        AllocationStrategy::Random,
        AllocationStrategy::FreeRatioFirst,
    ] {
        for allocator_kind in [
            MemoryAllocatorKind::Offset,
            MemoryAllocatorKind::CachelibLike,
        ] {
            let mut allocator = SegmentAllocator::new()
                .with_strategy(strategy)
                .with_memory_allocator(allocator_kind);
            allocator.add_segment(make_seg("segment1", 64 * MIB), 0, Uuid::new_v4());

            assert_eq!(
                allocator
                    .allocate_checked("huge", 100 * MIB, 1, &ReplicateConfig::default())
                    .unwrap_err(),
                SegmentAllocationError::NoAvailableHandle
            );
        }
    }
}

#[test]
fn offset_and_cachelib_allocators_reject_requests_larger_than_capacity() {
    for kind in [
        MemoryAllocatorKind::Offset,
        MemoryAllocatorKind::CachelibLike,
    ] {
        let capacity = CACHELIB_SLAB_SIZE * 2;
        let mut allocator = SegmentAllocator::new().with_memory_allocator(kind);
        allocator.add_segment(make_seg("oversized:1", capacity), 0, Uuid::new_v4());
        assert!(
            allocator
                .allocate("oversized", capacity + 1, 1, &ReplicateConfig::default())
                .is_empty()
        );
    }
}

#[test]
fn offset_and_cachelib_allocators_support_parallel_allocate_release() {
    for kind in [
        MemoryAllocatorKind::Offset,
        MemoryAllocatorKind::CachelibLike,
    ] {
        let mut allocator = SegmentAllocator::new().with_memory_allocator(kind);
        allocator.add_segment(
            make_seg("parallel:1", CACHELIB_SLAB_SIZE * 4),
            0,
            Uuid::new_v4(),
        );
        let allocator = Arc::new(Mutex::new(allocator));
        let barrier = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|worker| {
                let allocator = Arc::clone(&allocator);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for iteration in 0..500 {
                        let mut allocator = allocator.lock().unwrap();
                        let replicas = allocator.allocate(
                            &format!("worker-{worker}-{iteration}"),
                            128,
                            1,
                            &ReplicateConfig::default(),
                        );
                        assert_eq!(replicas.len(), 1);
                        assert_eq!(replicas[0].size, 128);
                        allocator.release(&replicas).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(allocator.lock().unwrap().usage_totals().1, 0);
    }
}

#[test]
fn cpp_parity_buffer_allocator_test_cpp_bufferallocatortest_parallelallocation_c8e3e399() {
    const ALLOCATION_SIZE: u64 = 477;
    const WORKERS: usize = 4;

    for kind in [
        MemoryAllocatorKind::Offset,
        MemoryAllocatorKind::CachelibLike,
    ] {
        let mut segment = make_seg("test", 32 * 1024 * 1024);
        segment.base = 0x1_2000_0000;
        segment.te_endpoint = "test".to_string();
        let segment_id = segment.id;

        let mut allocator = SegmentAllocator::new().with_memory_allocator(kind);
        allocator.add_segment(segment, 0, Uuid::new_v4());
        let allocator = Arc::new(RwLock::new(allocator));
        let barrier = Arc::new(Barrier::new(WORKERS));
        let success_count = Arc::new(AtomicUsize::new(0));
        let saw_invalid_descriptor = Arc::new(AtomicBool::new(false));

        let workers = (0..WORKERS)
            .map(|worker| {
                let allocator = Arc::clone(&allocator);
                let barrier = Arc::clone(&barrier);
                let success_count = Arc::clone(&success_count);
                let saw_invalid_descriptor = Arc::clone(&saw_invalid_descriptor);
                std::thread::spawn(move || {
                    barrier.wait();
                    for iteration in 0..1_000 {
                        let replicas = allocator.write().unwrap().allocate(
                            &format!("parallel-{worker}-{iteration}"),
                            ALLOCATION_SIZE,
                            1,
                            &ReplicateConfig::default(),
                        );
                        let Some(replica) = replicas.first() else {
                            std::thread::yield_now();
                            continue;
                        };
                        let endpoint = allocator
                            .read()
                            .unwrap()
                            .transport_endpoint_for(replica)
                            .map(str::to_owned);
                        if replica.segment_id != segment_id
                            || replica.segment_name != "test"
                            || endpoint.as_deref() != Some("test")
                            || replica.size != ALLOCATION_SIZE
                            || matches!(
                                replica.base_addr.checked_add(replica.offset),
                                None | Some(0)
                            )
                        {
                            saw_invalid_descriptor.store(true, Ordering::Relaxed);
                        }
                        success_count.fetch_add(1, Ordering::Relaxed);
                        std::thread::yield_now();
                        allocator.write().unwrap().release(&replicas).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();

        for worker in workers {
            worker.join().unwrap();
        }
        assert!(!saw_invalid_descriptor.load(Ordering::Relaxed));
        assert!(success_count.load(Ordering::Relaxed) > 0);
        assert_eq!(allocator.read().unwrap().usage_totals().1, 0);
    }
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
fn free_ratio_first_balances_differently_sized_segments_within_cpp_tolerance() {
    const MIB: u64 = 1024 * 1024;
    const SLICE_SIZE: u64 = 64 * 1024;
    let capacities = [32 * MIB, 64 * MIB, 128 * MIB];
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    let mut segment_ids = Vec::new();
    for (index, capacity) in capacities.into_iter().enumerate() {
        let segment = make_seg(&format!("{index}-segment"), capacity);
        segment_ids.push(segment.id);
        allocator.add_segment(segment, 0, Uuid::new_v4());
    }

    for index in 0..3000 {
        assert_eq!(
            allocator
                .allocate(
                    &format!("balanced-{index}"),
                    SLICE_SIZE,
                    1,
                    &ReplicateConfig::default(),
                )
                .len(),
            1
        );
    }

    let utilization = segment_ids
        .iter()
        .zip(capacities)
        .map(|(segment_id, capacity)| {
            allocator.used_bytes(segment_id).unwrap() as f64 * 100.0 / capacity as f64
        })
        .collect::<Vec<_>>();
    let minimum = utilization.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = utilization
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    assert!(
        maximum - minimum < 15.0,
        "utilization {utilization:?} exceeds the C++ 15% balance tolerance"
    );
}

#[test]
fn test_ssd_free_ratio_first_uses_owner_ssd_metrics() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::SsdFreeRatioFirst);
    let nearly_full_ssd = Uuid::new_v4();
    let most_free_ssd = Uuid::new_v4();
    let some_free_ssd = Uuid::new_v4();
    a.add_segment(make_seg("nearly_full_ssd:1", 10000), 0, nearly_full_ssd);
    a.add_segment(make_seg("most_free_ssd:1", 10000), 9000, most_free_ssd);
    a.add_segment(make_seg("some_free_ssd:1", 10000), 0, some_free_ssd);

    let metrics = HashMap::from([
        (
            nearly_full_ssd,
            SsdUsageMetrics {
                total_capacity_bytes: 1000,
                used_bytes: 950,
            },
        ),
        (
            most_free_ssd,
            SsdUsageMetrics {
                total_capacity_bytes: 1000,
                used_bytes: 100,
            },
        ),
        (
            some_free_ssd,
            SsdUsageMetrics {
                total_capacity_bytes: 1000,
                used_bytes: 400,
            },
        ),
    ]);

    let repls = a.allocate_for_client_with_ssd_metrics(
        "k",
        Some(Uuid::new_v4()),
        500,
        2,
        &ReplicateConfig::default(),
        &metrics,
    );
    assert_eq!(repls.len(), 2);
    assert_eq!(repls[0].segment_name, "most_free_ssd:1");
    assert_eq!(repls[1].segment_name, "some_free_ssd:1");
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

#[path = "test_allocator/cachelib.rs"]
mod cachelib;

#[test]
fn test_preferred_segment_takes_precedence_over_preferred_segments() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    a.add_segment(make_seg("singular:1", 10000), 9000, Uuid::new_v4());
    a.add_segment(make_seg("plural:1", 10000), 0, Uuid::new_v4());
    a.add_segment(make_seg("fallback:1", 10000), 0, Uuid::new_v4());

    let config = ReplicateConfig {
        preferred_segment: "singular:1".into(),
        preferred_segments: vec!["plural:1".into()],
        ..Default::default()
    };

    let repls = a.allocate_for_client("k", Some(Uuid::new_v4()), 500, 2, &config);
    assert_eq!(repls.len(), 2);
    assert_eq!(repls[0].segment_name, "singular:1");
}

#[test]
fn test_allocate_with_exclusions_skips_preferred_and_fallback_segments() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    a.add_segment(make_seg("preferred:1", 10000), 0, Uuid::new_v4());
    a.add_segment(make_seg("excluded:1", 10000), 0, Uuid::new_v4());
    a.add_segment(make_seg("allowed:1", 10000), 0, Uuid::new_v4());

    let config = ReplicateConfig {
        preferred_segment: "preferred:1".into(),
        ..Default::default()
    };
    let excluded = vec!["preferred:1".to_string(), "excluded:1".to_string()];

    let repls = a.allocate_with_exclusions("k", 128, 2, &config, &excluded);
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "allowed:1");
}

#[test]
fn test_allocate_from_segment_reports_cpp_style_errors() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("target:1", 256), 0, Uuid::new_v4());
    a.add_segment(make_seg("full:1", 256), 256, Uuid::new_v4());

    let replica = a.allocate_from_segment("target:1", 128).unwrap();
    assert_eq!(replica.segment_name, "target:1");
    assert_eq!(replica.offset, 0);

    assert_eq!(
        a.allocate_from_segment("target:1", 0).unwrap_err(),
        SegmentAllocationError::InvalidParams
    );
    assert_eq!(
        a.allocate_from_segment("ghost:1", 128).unwrap_err(),
        SegmentAllocationError::SegmentNotFound
    );
    assert_eq!(
        a.allocate_from_segment("full:1", 128).unwrap_err(),
        SegmentAllocationError::NoAvailableHandle
    );
}

#[test]
fn test_free_ratio_first_aggregates_allocators_with_same_segment_name() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    a.add_segment(make_seg("aggregated:1", 1000), 1000, Uuid::new_v4());
    a.add_segment(make_seg("aggregated:1", 1000), 0, Uuid::new_v4());
    a.add_segment(make_seg("other:1", 1000), 400, Uuid::new_v4());

    let repls = a.allocate("k", 100, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "other:1");
}

#[test]
fn test_local_first_prefers_writer_client_host_for_single_replica() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::LocalFirst);
    let writer = Uuid::new_v4();
    let remote = Uuid::new_v4();
    a.add_segment(make_seg("host-a:1", 1000), 0, remote);
    a.add_segment(make_seg("host-b:1", 1000), 0, writer);
    a.add_segment(make_seg("host-c:1", 1000), 0, remote);

    let repls = a.allocate_for_client("k", Some(writer), 100, 1, &ReplicateConfig::default());

    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "host-b:1");
}

#[test]
fn test_local_first_prefers_explicit_request_host_over_client_segment_name() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::LocalFirst);
    let mut host_a = make_seg("logical-segment-a", 1000);
    host_a.host_id = "physical-a".to_string();
    let mut host_b = make_seg("logical-segment-b", 1000);
    host_b.host_id = "physical-b".to_string();
    allocator.add_segment(host_a, 0, Uuid::new_v4());
    allocator.add_segment(host_b, 0, Uuid::new_v4());

    let config = ReplicateConfig {
        host_id: "physical-b".to_string(),
        ..Default::default()
    };
    let replicas = allocator.allocate_for_client("key", None, 100, 1, &config);

    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "logical-segment-b");
}

#[test]
fn test_local_first_falls_back_when_writer_host_is_full() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::LocalFirst);
    let writer = Uuid::new_v4();
    let remote = Uuid::new_v4();
    a.add_segment(make_seg("host-a:1", 1000), 0, remote);
    a.add_segment(make_seg("host-b:1", 1000), 1000, writer);
    a.add_segment(make_seg("host-c:1", 1000), 0, remote);

    let repls = a.allocate_for_client("k", Some(writer), 100, 1, &ReplicateConfig::default());

    assert_eq!(repls.len(), 1);
    assert_ne!(repls[0].segment_name, "host-b:1");
}

fn cxl_alias(name: &str, size: u64) -> Segment {
    let mut segment = make_seg(name, size);
    segment.protocol = "cxl".to_string();
    segment
}

#[test]
fn test_cxl_aliases_share_one_global_capacity_and_require_preferred_alias() {
    let capacity = CACHELIB_SLAB_SIZE * 2;
    let mut allocator = SegmentAllocator::new()
        .with_strategy(AllocationStrategy::Cxl)
        .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
        .with_cxl_capacity(capacity);
    let alias_a = cxl_alias("cxl-a:1", capacity);
    let alias_b = cxl_alias("cxl-b:1", capacity);
    allocator.add_segment(alias_a.clone(), 0, Uuid::new_v4());
    allocator.add_segment(alias_b, 0, Uuid::new_v4());

    assert_eq!(allocator.usage_totals(), (capacity, 0));
    assert!(
        allocator
            .allocate("missing-preference", 128, 1, &ReplicateConfig::default())
            .is_empty()
    );

    let config = ReplicateConfig {
        preferred_segment: alias_a.name.clone(),
        ..Default::default()
    };
    let replica = allocator.allocate("key", 128, 2, &config);
    assert_eq!(replica.len(), 1);
    assert_eq!(replica[0].segment_id, alias_a.id);
    assert_eq!(replica[0].segment_name, alias_a.name);
    assert_eq!(replica[0].base_addr, 0);
    assert_eq!(replica[0].protocol, "cxl");
    assert!(allocator.usage_totals().1 > 0);

    let offset = replica[0].offset;
    allocator.release(&replica).unwrap();
    let reused = allocator.allocate("reused", 128, 1, &config);
    assert_eq!(reused[0].offset, offset);
}

#[test]
fn test_duplicate_cxl_alias_restore_is_rejected_without_unbinding_live_alias() {
    let capacity = CACHELIB_SLAB_SIZE * 2;
    let mut allocator = SegmentAllocator::new()
        .with_strategy(AllocationStrategy::Cxl)
        .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
        .with_cxl_capacity(capacity);
    let alias = cxl_alias("cxl-duplicate:1", capacity);
    allocator.add_segment(alias.clone(), 0, Uuid::new_v4());

    assert!(
        allocator
            .restore_cxl_alias(alias.clone(), Uuid::new_v4())
            .is_err()
    );
    let replicas = allocator.allocate(
        "still-bound",
        128,
        1,
        &ReplicateConfig {
            preferred_segment: alias.name,
            ..Default::default()
        },
    );
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_id, alias.id);
}

#[test]
fn test_cxl_restore_rebuilds_global_layout_once_across_aliases() {
    let capacity = CACHELIB_SLAB_SIZE * 2;
    let alias_a = cxl_alias("restore-cxl-a:1", capacity);
    let alias_b = cxl_alias("restore-cxl-b:1", capacity);
    let mut original = SegmentAllocator::new()
        .with_strategy(AllocationStrategy::Cxl)
        .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
        .with_cxl_capacity(capacity);
    original.add_segment(alias_a.clone(), 0, Uuid::new_v4());
    original.add_segment(alias_b.clone(), 0, Uuid::new_v4());
    let first = original.allocate(
        "a",
        128,
        1,
        &ReplicateConfig {
            preferred_segment: alias_a.name.clone(),
            ..Default::default()
        },
    );
    let second = original.allocate(
        "b",
        128,
        1,
        &ReplicateConfig {
            preferred_segment: alias_b.name.clone(),
            ..Default::default()
        },
    );
    let live = first.into_iter().chain(second).collect::<Vec<_>>();

    let mut restored = SegmentAllocator::new()
        .with_strategy(AllocationStrategy::Cxl)
        .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
        .with_cxl_capacity(capacity);
    restored
        .restore_cxl_alias(alias_a.clone(), Uuid::new_v4())
        .unwrap();
    restored.restore_cxl_alias(alias_b, Uuid::new_v4()).unwrap();
    let used = restored.restore_cxl_allocations(&live).unwrap();

    assert_eq!(restored.usage_totals(), (capacity, used));
    assert!(
        restored
            .allocate(
                "before-remount",
                128,
                1,
                &ReplicateConfig {
                    preferred_segment: alias_a.name,
                    ..Default::default()
                },
            )
            .is_empty()
    );
}

#[test]
fn cxl_restore_rejects_replica_alias_identity_mismatch() {
    let capacity = CACHELIB_SLAB_SIZE * 2;
    let alias = cxl_alias("restore-cxl-identity:1", capacity);
    let mut restored = SegmentAllocator::new()
        .with_strategy(AllocationStrategy::Cxl)
        .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
        .with_cxl_capacity(capacity);
    restored
        .restore_cxl_alias(alias.clone(), Uuid::new_v4())
        .unwrap();

    let mut replica = restored_replica(&alias, 0, 128);
    replica.segment_name = "wrong-cxl-alias:1".to_string();
    let error = restored.restore_cxl_allocations(&[replica]).unwrap_err();
    assert!(error.contains("identity mismatch"));
    assert_eq!(restored.usage_totals(), (capacity, 0));
}

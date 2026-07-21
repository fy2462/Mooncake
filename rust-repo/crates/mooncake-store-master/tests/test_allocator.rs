use std::cell::Cell;
use std::collections::HashMap;

mod common;
use common::{make_seg, make_seg_with_usage};

use mooncake_store_core::{ReplicateConfig, Segment};
use mooncake_store_master::allocator::{
    cachelib_allocation_class_id_for_request, cachelib_allocation_class_size_for_request,
    AllocationStrategy, MemoryAllocatorKind, SegmentAllocationError, SegmentAllocator,
    SlabReleaseMode, SsdUsageMetrics, CACHELIB_SLAB_SIZE,
};
use uuid::Uuid;

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

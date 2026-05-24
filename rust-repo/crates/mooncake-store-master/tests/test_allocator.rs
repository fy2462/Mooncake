use mooncake_store_core::{ReplicateConfig, Segment};
use mooncake_store_master::allocator::{AllocationStrategy, SegmentAllocator};
use uuid::Uuid;

fn make_seg(name: &str, size: u64, used: u64) -> Segment {
    Segment {
        id: Uuid::new_v4(),
        name: name.into(),
        size,
        used,
        client_id: Uuid::new_v4(),
    }
}

#[test]
fn test_single_segment_single_replica() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 10000, 0));
    let repls = a.allocate("k", 100, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].segment_name, "n:1");
}

#[test]
fn test_multiple_segments_partially_filled() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    a.add_segment(make_seg("n:1", 10000, 9000));
    a.add_segment(make_seg("n:2", 10000, 1000));
    a.add_segment(make_seg("n:3", 10000, 0));

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
    a.add_segment(make_seg("n:1", 100, 90));
    a.add_segment(make_seg("n:2", 100, 95));
    let repls = a.allocate("k", 20, 1, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_exact_fit() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 100, 50));
    let repls = a.allocate("k", 50, 1, &ReplicateConfig::default());
    assert_eq!(repls.len(), 1);
    assert_eq!(repls[0].offset, 50);
}

#[test]
fn test_replica_count_zero() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 1000, 0));
    let repls = a.allocate("k", 100, 0, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_not_enough_segments_for_replicas() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("n:1", 1000, 0));
    a.add_segment(make_seg("n:2", 1000, 0));
    let repls = a.allocate("k", 100, 5, &ReplicateConfig::default());
    assert_eq!(repls.len(), 2);
}

#[test]
fn test_preferred_segment_nonexistent() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("real:1", 1000, 0));
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
    a.add_segment(make_seg("pref:1", 100, 100));
    a.add_segment(make_seg("fallback:1", 1000, 0));
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
        a.add_segment(make_seg(&format!("n:{}", i), 10000, 0));
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
    let cid = Uuid::new_v4();
    a.add_segment(Segment {
        id: sid,
        name: "to-remove:1".into(),
        size: 1000,
        used: 0,
        client_id: cid,
    });
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
        a.add_segment(make_seg(&format!("s{}:1", i), 100, 0));
    }
    let repls = a.allocate("k", 50, 10, &ReplicateConfig::default());
    assert_eq!(repls.len(), 10);
}

#[test]
fn test_large_object_no_segment() {
    let mut a = SegmentAllocator::new();
    a.add_segment(make_seg("big:1", 1_000_000, 0));
    let repls = a.allocate("huge_key", 2_000_000, 1, &ReplicateConfig::default());
    assert!(repls.is_empty());
}

#[test]
fn test_free_ratio_sort_order() {
    let mut a = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    a.add_segment(make_seg("most_free:1", 10000, 0));
    a.add_segment(make_seg("some_free:1", 10000, 5000));
    a.add_segment(make_seg("little_free:1", 10000, 9000));

    let repls = a.allocate("k", 500, 2, &ReplicateConfig::default());
    assert_eq!(repls.len(), 2);
    assert_eq!(repls[0].segment_name, "most_free:1");
    assert_eq!(repls[1].segment_name, "some_free:1");
}

#[test]
fn test_replicas_on_different_segments() {
    let mut a = SegmentAllocator::new();
    for i in 0..4 {
        a.add_segment(make_seg(&format!("n{}:1", i), 10000, 0));
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

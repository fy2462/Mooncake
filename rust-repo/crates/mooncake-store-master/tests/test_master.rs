use mooncake_store_master::allocator::{AllocationStrategy, SegmentAllocator};
use mooncake_store_master::eviction::EvictionManager;
use mooncake_store_core::{ReplicateConfig, Segment};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

#[test]
fn test_allocator_random_strategy() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::Random);

    let cid = Uuid::new_v4();
    allocator.add_segment(Segment {
        id: Uuid::new_v4(), name: "n1:1".into(), size: 1000, used: 0, client_id: cid,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(), name: "n2:1".into(), size: 1000, used: 0, client_id: cid,
    });

    let replicas = allocator.allocate("key1", 100, 2, &ReplicateConfig::default());
    assert_eq!(replicas.len(), 2);
    assert_ne!(replicas[0].segment_name, replicas[1].segment_name);
}

#[test]
fn test_allocator_insufficient_space() {
    let allocator = SegmentAllocator::new();
    let replicas = allocator.allocate("k", 1000, 3, &ReplicateConfig::default());
    assert!(replicas.is_empty());
}

#[test]
fn test_allocator_preferred_segment() {
    let mut allocator = SegmentAllocator::new();
    let cid = Uuid::new_v4();
    allocator.add_segment(Segment {
        id: Uuid::new_v4(), name: "far:1".into(), size: 1000, used: 0, client_id: cid,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(), name: "preferred:1".into(), size: 1000, used: 0, client_id: cid,
    });

    let config = ReplicateConfig {
        preferred_segment: "preferred:1".into(),
        ..Default::default()
    };
    let replicas = allocator.allocate("k", 100, 1, &config);
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "preferred:1");
}

#[test]
fn test_allocator_free_ratio_first() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    let cid = Uuid::new_v4();
    allocator.add_segment(Segment {
        id: Uuid::new_v4(), name: "fuller:1".into(), size: 1000, used: 800, client_id: cid,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(), name: "emptier:1".into(), size: 1000, used: 100, client_id: cid,
    });

    let replicas = allocator.allocate("k", 100, 1, &ReplicateConfig::default());
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "emptier:1");
}

#[test]
fn test_eviction_selects_oldest() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(3600));
    let now = SystemTime::now();
    let very_old = now - Duration::from_secs(10000);
    let recent = now - Duration::from_secs(10);

    let candidates: Vec<(&str, &[mooncake_store_core::ReplicaDescriptor], bool, SystemTime)> = vec![
        ("old_key", &[], false, very_old),
        ("new_key", &[], false, recent),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "old_key");
}

#[test]
fn test_lease_not_expired_is_skipped() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(3600));
    let fresh = SystemTime::now() - Duration::from_secs(100);

    let candidates: Vec<(&str, &[mooncake_store_core::ReplicaDescriptor], bool, SystemTime)> = vec![
        ("still_live", &[], false, fresh),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert!(evicted.is_empty());
}

#[test]
fn test_eviction_skips_soft_pinned() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(5000));
    let now = SystemTime::now();
    let old = now - Duration::from_secs(1000);

    let candidates: Vec<(&str, &[mooncake_store_core::ReplicaDescriptor], bool, SystemTime)> = vec![
        ("pinned_key", &[], true, old),
        ("normal_key", &[], false, old),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "normal_key");
}

#[test]
fn test_soft_pin_expiry() {
    let mgr = EvictionManager::new(Duration::from_millis(1), Duration::from_millis(5000));
    let old = SystemTime::now() - Duration::from_secs(10);
    assert!(mgr.soft_pin_expired(old));
}

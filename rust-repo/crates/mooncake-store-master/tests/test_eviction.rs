use mooncake_store_core::ReplicaDescriptor;
use mooncake_store_master::eviction::EvictionManager;
use std::time::{Duration, SystemTime};

#[test]
fn test_lease_not_expired_is_skipped() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(3600));
    let fresh = SystemTime::now() - Duration::from_secs(100);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> =
        vec![("still_live", &[], false, fresh)];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert!(evicted.is_empty());
}

#[test]
fn test_lease_expired_is_evicted() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(1));
    let old = SystemTime::now() - Duration::from_secs(10);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> =
        vec![("old_key", &[], false, old)];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "old_key");
}

#[test]
fn test_eviction_selects_oldest_first() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(1));
    let now = SystemTime::now();
    let very_old = now - Duration::from_secs(1000);
    let somewhat_old = now - Duration::from_secs(500);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> = vec![
        ("key_middle", &[], false, somewhat_old),
        ("key_oldest", &[], false, very_old),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "key_oldest");
}

#[test]
fn test_eviction_skips_soft_pinned() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(1));
    let now = SystemTime::now();
    let old = now - Duration::from_secs(1000);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> = vec![
        ("pinned_old", &[], true, old),
        ("normal_old", &[], false, old),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "normal_old");
}

#[test]
fn test_eviction_all_soft_pinned_no_eviction() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(1));
    let now = SystemTime::now();
    let old = now - Duration::from_secs(1000);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> =
        vec![("pinned_a", &[], true, old), ("pinned_b", &[], true, old)];

    let evicted = mgr.select_for_eviction(&candidates, 2);
    assert!(evicted.is_empty());
}

#[test]
fn test_eviction_target_count_zero() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(3600));
    let now = SystemTime::now();
    let old = now - Duration::from_secs(100000);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> =
        vec![("k", &[], false, old)];

    let evicted = mgr.select_for_eviction(&candidates, 0);
    assert!(evicted.is_empty());
}

#[test]
fn test_eviction_empty_candidates() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(3600));
    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> = vec![];
    let evicted = mgr.select_for_eviction(&candidates, 5);
    assert!(evicted.is_empty());
}

#[test]
fn test_eviction_target_exceeds_candidates() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(1));
    let old = SystemTime::now() - Duration::from_secs(100);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> =
        vec![("a", &[], false, old), ("b", &[], false, old)];

    let evicted = mgr.select_for_eviction(&candidates, 10);
    assert_eq!(evicted.len(), 2);
}

#[test]
fn test_eviction_returns_lru_order() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(1));
    let now = SystemTime::now();
    let t0 = now - Duration::from_secs(10000);
    let t1 = now - Duration::from_secs(5000);
    let t2 = now - Duration::from_secs(1000);

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> = vec![
        ("key_t2", &[], false, t2),
        ("key_t0", &[], false, t0),
        ("key_t1", &[], false, t1),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 3);
    assert_eq!(evicted.len(), 3);
    assert_eq!(evicted[0], "key_t0");
    assert_eq!(evicted[1], "key_t1");
    assert_eq!(evicted[2], "key_t2");
}

#[test]
fn cpp_parity_eviction_strategy_test_cpp_evictionstrategytest_evictkey_57455bfb() {
    let mgr = EvictionManager::new(Duration::ZERO, Duration::ZERO);
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
    let mut candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> = vec![
        ("key1", &[], false, base),
        ("key2", &[], false, base + Duration::from_secs(1)),
    ];

    let first = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(first, ["key1"]);
    candidates.retain(|(key, ..)| *key != first[0]);

    candidates.push(("key3", &[], false, base + Duration::from_secs(2)));
    candidates.push(("key4", &[], false, base + Duration::from_secs(3)));
    candidates
        .iter_mut()
        .find(|(key, ..)| *key == "key2")
        .unwrap()
        .3 = base + Duration::from_secs(4);
    candidates
        .iter_mut()
        .find(|(key, ..)| *key == "key3")
        .unwrap()
        .3 = base + Duration::from_secs(5);

    let second = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(second, ["key4"]);
    candidates.retain(|(key, ..)| *key != second[0]);
    assert_eq!(candidates.len(), 2);
}

#[test]
fn test_eviction_filters_lease_plus_soft_pin() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(1));
    let now = SystemTime::now();
    let old = now - Duration::from_secs(100);
    let fresh = now;

    let candidates: Vec<(&str, &[ReplicaDescriptor], bool, SystemTime)> = vec![
        ("old_normal", &[], false, old),
        ("old_pinned", &[], true, old),
        ("fresh_normal", &[], false, fresh),
        ("fresh_pinned", &[], true, fresh),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 4);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "old_normal");
}

#[test]
fn test_soft_pin_expired_milliseconds() {
    let mgr = EvictionManager::new(Duration::from_millis(100), Duration::from_secs(3600));
    let old = SystemTime::now() - Duration::from_secs(5);
    let fresh = SystemTime::now();
    assert!(mgr.soft_pin_expired(old));
    assert!(!mgr.soft_pin_expired(fresh));
}

#[test]
fn test_soft_pin_expired_zero_ttl() {
    let mgr = EvictionManager::new(Duration::from_millis(0), Duration::from_secs(3600));
    std::thread::sleep(Duration::from_millis(1));
    let then = SystemTime::now();
    std::thread::sleep(Duration::from_millis(1));
    assert!(mgr.soft_pin_expired(then));
}

#[test]
fn test_soft_pin_expired_large_ttl() {
    let mgr = EvictionManager::new(Duration::from_secs(86400), Duration::from_secs(3600));
    let fresh = SystemTime::now();
    assert!(!mgr.soft_pin_expired(fresh));
}

#[test]
fn test_is_lease_expired_edge_cases() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(60));
    let now = SystemTime::now();
    assert!(!mgr.is_lease_expired(now, now));
    assert!(mgr.is_lease_expired(now - Duration::from_secs(61), now));
    assert!(!mgr.is_lease_expired(now - Duration::from_secs(59), now));
}

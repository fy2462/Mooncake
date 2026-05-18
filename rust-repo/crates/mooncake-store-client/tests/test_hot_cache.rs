use mooncake_store_client::LocalHotCache;

#[test]
fn test_hot_cache_basic_put_get() {
    let cache = LocalHotCache::new(1024 * 1024, 100);
    cache.put("key1", b"hello world");
    let result = cache.get("key1");
    assert!(result.is_some());
    assert_eq!(&result.unwrap(), b"hello world");
}

#[test]
fn test_hot_cache_miss() {
    let cache = LocalHotCache::new(1024 * 1024, 100);
    assert!(cache.get("nonexistent").is_none());
}

#[test]
fn test_hot_cache_overwrite() {
    let cache = LocalHotCache::new(1024 * 1024, 100);
    cache.put("key1", b"first");
    cache.put("key1", b"second");
    let result = cache.get("key1");
    assert_eq!(result.as_deref(), Some(&b"second"[..]));
}

#[test]
fn test_hot_cache_remove() {
    let cache = LocalHotCache::new(1024 * 1024, 100);
    cache.put("key1", b"data");
    cache.remove("key1");
    assert!(cache.get("key1").is_none());
}

#[test]
fn test_hot_cache_clear() {
    let cache = LocalHotCache::new(1024 * 1024, 100);
    cache.put("key1", b"a");
    cache.put("key2", b"b");
    cache.clear();
    assert!(cache.get("key1").is_none());
    assert!(cache.get("key2").is_none());
}

#[test]
fn test_hot_cache_evicts_when_full() {
    let cache = LocalHotCache::new(4096, 100);
    // Fill with many 100-byte entries
    for i in 0..50 {
        let val = format!("{:>99}", i); // 99 bytes + 1 char = 100 bytes
        cache.put(&format!("key_{}", i), val.as_bytes());
    }
    // At least some entries should survive
    assert!(cache.get("key_0").is_some() || cache.get("key_49").is_some());
}

#[test]
fn test_hot_cache_rejects_oversized() {
    let cache = LocalHotCache::new(4096, 10);
    let big_value = vec![0u8; 3000]; // > max_size/2 (4096/2=2048)
    cache.put("big", &big_value);
    assert!(cache.get("big").is_none());
}

#[test]
fn test_hot_cache_multiple_entries() {
    let cache = LocalHotCache::new(10 * 1024, 1000);
    for i in 0..50 {
        let val = format!("value_{}", i);
        cache.put(&format!("key_{}", i), val.as_bytes());
    }
    for i in 0..50 {
        let expected = format!("value_{}", i);
        let result = cache.get(&format!("key_{}", i));
        assert_eq!(result.as_deref(), Some(expected.as_bytes()));
    }
}

#[test]
fn test_hot_cache_default_constructor() {
    let cache = LocalHotCache::default();
    cache.put("test", b"default");
    let result = cache.get("test");
    assert_eq!(result.as_deref(), Some(&b"default"[..]));
}

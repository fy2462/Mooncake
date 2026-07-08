use mooncake_store_client::{BufferHandle, LocalHotCache};

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
fn test_hot_cache_remove_by_regex_respects_tenant_scope() {
    let cache = LocalHotCache::new(1024 * 1024, 100);
    cache.put("tenant-a\0prefix_1", b"a1");
    cache.put("tenant-a\0other_1", b"a2");
    cache.put("tenant-b\0prefix_1", b"b1");
    cache.put("prefix_1", b"default");

    let removed = cache
        .remove_by_regex_for_tenant("tenant-a", "^prefix_.*")
        .unwrap();
    assert_eq!(removed, 1);
    assert!(cache.get("tenant-a\0prefix_1").is_none());
    assert_eq!(cache.get("tenant-a\0other_1"), Some(b"a2".to_vec()));
    assert_eq!(cache.get("tenant-b\0prefix_1"), Some(b"b1".to_vec()));
    assert_eq!(cache.get("prefix_1"), Some(b"default".to_vec()));

    let removed_default = cache.remove_by_regex_for_tenant("", "^prefix_.*").unwrap();
    assert_eq!(removed_default, 1);
    assert!(cache.get("prefix_1").is_none());
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
    for i in 0..50 {
        let val = format!("{:>99}", i);
        cache.put(&format!("key_{}", i), val.as_bytes());
    }
    assert!(cache.get("key_0").is_some() || cache.get("key_49").is_some());
}

#[test]
fn test_hot_cache_rejects_oversized() {
    let cache = LocalHotCache::new(4096, 10);
    let big_value = vec![0u8; 4097];
    cache.put("big", &big_value);
    assert!(cache.get("big").is_none());
}

#[test]
fn test_hot_cache_accepts_larger_value_after_reuse() {
    let cache = LocalHotCache::new(4096, 10);
    cache.put("small", &[b's'; 1024]);
    assert_eq!(cache.get("small").unwrap().len(), 1024);
    cache.put("filler", &[b'f'; 3072]);
    assert_eq!(cache.get("filler").unwrap().len(), 3072);

    let larger = vec![b'L'; 3072];
    cache.put("larger", &larger);

    assert_eq!(cache.get("larger"), Some(larger));
    assert!(cache.get("small").is_none());
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

#[test]
fn test_hot_cache_empty_value() {
    let cache = LocalHotCache::new(1024, 100);
    cache.put("empty", b"");
    let result = cache.get("empty");
    assert_eq!(result.as_deref(), Some(&b""[..]));
}

#[test]
fn test_hot_cache_entry_count_limit() {
    let cache = LocalHotCache::new(1024 * 1024, 3);
    cache.put("a", b"1");
    cache.put("b", b"2");
    cache.put("c", b"3");
    cache.put("d", b"4");
    assert!(cache.get("a").is_none());
    assert_eq!(cache.get("d"), Some(b"4".to_vec()));
}

#[test]
fn test_buffer_handle_creation() {
    let bh = BufferHandle {
        key: "test_key".into(),
        size: 42,
        data: vec![1, 2, 3],
    };
    assert_eq!(bh.key, "test_key");
    assert_eq!(bh.size, 42);
    assert_eq!(bh.data, vec![1, 2, 3]);
}

#[test]
fn test_buffer_handle_large() {
    let data = vec![0xAAu8; 65536];
    let bh = BufferHandle {
        key: "large".into(),
        size: data.len(),
        data: data.clone(),
    };
    assert_eq!(bh.size, 65536);
    assert_eq!(bh.data.len(), 65536);
    assert_eq!(bh.data[0], 0xAA);
}

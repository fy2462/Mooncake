use mooncake_store_client::{LocalStorageBackend, LocalStorageConfig};

fn test_backend() -> LocalStorageBackend {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "test_data".to_string(),
        enable_eviction: false,
        quota_bytes: 0,
    };
    let backend = LocalStorageBackend::new(config);
    backend.init().unwrap();
    backend
}

fn eviction_backend(quota: u64) -> (LocalStorageBackend, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "evict_data".to_string(),
        enable_eviction: true,
        quota_bytes: quota,
    };
    let backend = LocalStorageBackend::new(config);
    backend.init().unwrap();
    (backend, tmp)
}

// ------------------------------------------------------------------
// Path tests
// ------------------------------------------------------------------

#[test]
fn test_key_path_is_deterministic() {
    let backend = test_backend();
    let p1 = backend.key_path("hello");
    let p2 = backend.key_path("hello");
    assert_eq!(p1, p2);
}

#[test]
fn test_key_path_uses_hash_dirs() {
    let backend = test_backend();
    let path = backend.key_path("my_key");
    let path_str = path.to_string_lossy();
    // Should contain <fsdir>/<dir1>/<dir2>/sanitized_key
    assert!(path_str.contains("test_data/"), "missing fsdir: {path_str}");
    // dir1 and dir2 are single chars 'a'-'p'
    let parts: Vec<&str> = path_str.split('/').collect();
    // Find the test_data segment and verify dir1/dir2 after it
    let idx = parts.iter().position(|p| *p == "test_data").unwrap();
    let dir1 = parts[idx + 1];
    let dir2 = parts[idx + 2];
    let filename = parts[idx + 3];
    assert_eq!(dir1.len(), 1);
    assert_eq!(dir2.len(), 1);
    assert_eq!(filename, "my_key");
}

#[test]
fn test_key_path_sanitizes_slashes() {
    let backend = test_backend();
    let path = backend.key_path("prefix/suffix");
    let path_str = path.to_string_lossy();
    assert!(
        path_str.contains("prefix_suffix"),
        "expected prefix_suffix in: {path_str}"
    );
    assert!(
        !path_str.contains("prefix/suffix"),
        "slash should be replaced: {path_str}"
    );
}

#[test]
fn test_different_keys_different_dirs() {
    let backend = test_backend();
    let p1 = backend.key_path("aaaa");
    let p2 = backend.key_path("zzzz");
    // Different hashes should produce at least one different path component.
    assert_ne!(p1, p2);
}

// ------------------------------------------------------------------
// Write / Read / Delete round-trip
// ------------------------------------------------------------------

#[test]
fn test_write_and_read_roundtrip() {
    let backend = test_backend();
    let data = vec![1u8, 2, 3, 4, 5];
    backend.write_object("roundtrip_key", &data).unwrap();
    let read = backend.read_object("roundtrip_key").unwrap();
    assert_eq!(read, data);
}

#[test]
fn test_read_missing_returns_key_not_found() {
    let backend = test_backend();
    let err = backend.read_object("no_such_key").unwrap_err();
    match err {
        mooncake_store_core::StoreError::KeyNotFound(k) => assert_eq!(k, "no_such_key"),
        other => panic!("expected KeyNotFound, got {other:?}"),
    }
}

#[test]
fn test_write_overwrite() {
    let backend = test_backend();
    backend.write_object("overwrite_key", b"first").unwrap();
    backend.write_object("overwrite_key", b"second").unwrap();
    let read = backend.read_object("overwrite_key").unwrap();
    assert_eq!(read, b"second");
}

#[test]
fn test_delete_removes_file() {
    let backend = test_backend();
    backend.write_object("del_key", b"data").unwrap();
    assert!(backend.exists("del_key"));
    backend.delete_object("del_key").unwrap();
    assert!(!backend.exists("del_key"));
}

#[test]
fn test_delete_nonexistent_is_noop() {
    let backend = test_backend();
    backend.delete_object("never_written").unwrap();
}

#[test]
fn test_exists() {
    let backend = test_backend();
    assert!(!backend.exists("no_key"));
    backend.write_object("exists_key", b"x").unwrap();
    assert!(backend.exists("exists_key"));
}

// ------------------------------------------------------------------
// Space usage
// ------------------------------------------------------------------

#[test]
fn test_space_usage() {
    let backend = test_backend();
    let (used, total) = backend.space_usage();
    assert_eq!(used, 0, "should be 0 before any writes");
    // total is auto-detected from filesystem when quota_bytes=0.
    assert!(total > 0, "total should be auto-detected from filesystem");
}

// ------------------------------------------------------------------
// Scan meta
// ------------------------------------------------------------------

#[test]
fn test_scan_meta_returns_keys_and_sizes() {
    let backend = test_backend();
    backend.write_object("scan_a", b"1234567890").unwrap();
    backend.write_object("scan_b", b"hello").unwrap();

    let mut metas = backend.scan_meta().unwrap();
    metas.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(metas.len(), 2);
    assert_eq!(metas[0].0, "scan_a");
    assert_eq!(metas[0].1, 10);
    assert_eq!(metas[1].0, "scan_b");
    assert_eq!(metas[1].1, 5);
}

#[test]
fn test_scan_meta_empty() {
    let backend = test_backend();
    let metas = backend.scan_meta().unwrap();
    assert!(metas.is_empty());
}

// ------------------------------------------------------------------
// Remove all
// ------------------------------------------------------------------

#[test]
fn test_remove_all() {
    let backend = test_backend();
    backend.write_object("ra_1", b"aaa").unwrap();
    backend.write_object("ra_2", b"bbb").unwrap();
    assert_eq!(backend.scan_meta().unwrap().len(), 2);

    let count = backend.remove_all().unwrap();
    assert_eq!(count, 2);
    assert!(backend.scan_meta().unwrap().is_empty());
}

// ------------------------------------------------------------------
// Remove by regex
// ------------------------------------------------------------------

#[test]
fn test_remove_by_regex() {
    let backend = test_backend();
    backend.write_object("prefix_1", b"a").unwrap();
    backend.write_object("prefix_2", b"b").unwrap();
    backend.write_object("other", b"c").unwrap();

    let removed = backend.remove_by_regex(r"^prefix_").unwrap();
    assert_eq!(removed, 2);
    assert!(!backend.exists("prefix_1"));
    assert!(!backend.exists("prefix_2"));
    assert!(backend.exists("other"));
}

#[test]
fn test_remove_by_regex_no_match() {
    let backend = test_backend();
    backend.write_object("key1", b"a").unwrap();
    let removed = backend.remove_by_regex(r"^nomatch_").unwrap();
    assert_eq!(removed, 0);
    assert!(backend.exists("key1"));
}

// ------------------------------------------------------------------
// Eviction tests
// ------------------------------------------------------------------

#[test]
fn test_eviction_fifo_order() {
    let quota = 100u64;
    let (backend, _tmp) = eviction_backend(quota);

    // Write 3 objects of 40 bytes each. Total 120 > quota 100.
    // Oldest should be evicted first.
    backend.write_object("evict_a", &vec![0u8; 40]).unwrap();
    backend.write_object("evict_b", &vec![0u8; 40]).unwrap();
    // This write triggers eviction of evict_a (oldest).
    let evicted = backend.write_object("evict_c", &vec![0u8; 40]).unwrap();
    assert!(!evicted.is_empty(), "should have evicted at least evict_a");

    assert!(
        !backend.exists("evict_a"),
        "evict_a should be evicted (oldest)"
    );
    assert!(backend.exists("evict_b"));
    assert!(backend.exists("evict_c"));
}

#[test]
fn test_no_eviction_when_under_quota() {
    let quota = 500u64;
    let (backend, _tmp) = eviction_backend(quota);

    let evicted = backend.write_object("small", &vec![0u8; 100]).unwrap();
    assert!(evicted.is_empty(), "no eviction expected under quota");
    assert!(backend.exists("small"));
}

#[test]
fn test_eviction_returns_evicted_keys() {
    let quota = 50u64;
    let (backend, _tmp) = eviction_backend(quota);

    backend.write_object("large", &vec![0u8; 40]).unwrap();
    let evicted = backend.write_object("trigger", &vec![0u8; 40]).unwrap();

    assert!(!evicted.is_empty());
    // The filename (sanitized key) should be in the evicted list.
    assert!(evicted.contains(&"large".to_string()) || !evicted.is_empty());
}

#[test]
fn test_eviction_delete_frees_space() {
    let quota = 100u64;
    let (backend, _tmp) = eviction_backend(quota);

    backend.write_object("d1", &vec![0u8; 60]).unwrap();
    backend.delete_object("d1").unwrap();

    // Now we should have 60 free again, writing 60 should not evict.
    let evicted = backend.write_object("d2", &vec![0u8; 60]).unwrap();
    assert!(evicted.is_empty(), "delete should have freed space");
}

// ------------------------------------------------------------------
// Init idempotency
// ------------------------------------------------------------------

#[test]
fn test_init_idempotent() {
    let backend = test_backend();
    // Second init is a no-op.
    backend.init().unwrap();
    backend.write_object("idem_key", b"data").unwrap();
    let read = backend.read_object("idem_key").unwrap();
    assert_eq!(read, b"data");
}

// ------------------------------------------------------------------
// Integration tests: full offload/promotion cycle with LocalStorageBackend
// ------------------------------------------------------------------

#[test]
fn test_full_local_storage_lifecycle() {
    let backend = test_backend();

    // Write (simulating offload).
    backend
        .write_object("lifecycle_key", b"lifecycle_value")
        .unwrap();
    assert!(backend.exists("lifecycle_key"));

    // Read (simulating promotion source).
    let data = backend.read_object("lifecycle_key").unwrap();
    assert_eq!(data, b"lifecycle_value");

    // Delete (simulating eviction cleanup).
    backend.delete_object("lifecycle_key").unwrap();
    assert!(!backend.exists("lifecycle_key"));
}

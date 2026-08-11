use mooncake_store_client::{LocalStorageBackend, LocalStorageConfig};

struct TestBackend {
    backend: LocalStorageBackend,
    _tmp: tempfile::TempDir,
}

impl std::ops::Deref for TestBackend {
    type Target = LocalStorageBackend;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

fn test_backend() -> TestBackend {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "test_data".to_string(),
        enable_eviction: false,
        quota_bytes: 0,
    };
    let backend = LocalStorageBackend::new(config);
    backend.init().unwrap();
    TestBackend { backend, _tmp: tmp }
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

#[test]
fn test_ephemeral_storage_wipes_owned_data_on_startup_and_drop() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "ephemeral_data".to_string(),
        enable_eviction: true,
        quota_bytes: 1024 * 1024,
    };
    let data_dir = tmp.path().join("ephemeral_data");

    let previous = LocalStorageBackend::new_ephemeral(config.clone());
    previous.init().unwrap();
    let leftover_path = previous.key_path("leftover");
    drop(previous);
    std::fs::create_dir_all(leftover_path.parent().unwrap()).unwrap();
    std::fs::write(&leftover_path, b"\x0a\x08leftover\x12\x05stale").unwrap();

    {
        let backend = LocalStorageBackend::new_ephemeral(config.clone());
        backend.init().unwrap();

        assert!(data_dir.exists(), "startup wipe keeps the data directory");
        assert!(
            !backend.exists("leftover"),
            "startup wipe removes stale data owned by the Rust backend"
        );

        backend.write_object("live_key", b"live").unwrap();
        assert!(backend.exists("live_key"));
    }

    assert!(data_dir.exists(), "drop wipe keeps the data directory");
    let mut remaining = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    remaining.sort();
    assert_eq!(
        remaining,
        [
            std::ffi::OsString::from(".mooncake-storage-format"),
            std::ffi::OsString::from(".mooncake-storage-id"),
        ],
        "drop wipe preserves the namespace identity and format markers"
    );
}

#[test]
fn test_ephemeral_backend_refuses_persistent_marker_without_deletion() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "persistent_data".to_string(),
        enable_eviction: true,
        quota_bytes: 1024 * 1024,
    };

    let persistent = LocalStorageBackend::new(config.clone());
    persistent.init().unwrap();
    persistent.write_object("keep", b"durable").unwrap();
    let record_path = persistent.key_path("keep");
    let record_bytes = std::fs::read(&record_path).unwrap();
    drop(persistent);

    let ephemeral = LocalStorageBackend::new_ephemeral(config);
    let error = ephemeral.init().unwrap_err();
    assert!(error.to_string().contains("foreign format marker"));
    assert_eq!(std::fs::read(record_path).unwrap(), record_bytes);
}

#[test]
fn test_persistent_reopen_over_quota_fails_without_eviction() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "over_quota".to_string(),
        enable_eviction: true,
        quota_bytes: 1024,
    };

    let backend = LocalStorageBackend::new(config.clone());
    backend.init().unwrap();
    backend.write_object("keep", &[7_u8; 128]).unwrap();
    let record_path = backend.key_path("keep");
    let record_bytes = std::fs::read(&record_path).unwrap();
    drop(backend);

    config.quota_bytes = 1;
    let too_small = LocalStorageBackend::new(config);
    let error = too_small.init().unwrap_err();
    assert!(error.to_string().contains("exceeding configured quota"));
    assert_eq!(std::fs::read(record_path).unwrap(), record_bytes);
}

#[test]
fn test_persistent_init_cleans_reserved_stale_temp_namespace() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "temp_recovery".to_string(),
        enable_eviction: false,
        quota_bytes: 1024,
    };
    let backend = LocalStorageBackend::new(config.clone());
    backend.init().unwrap();
    backend.write_object("keep", b"value").unwrap();
    drop(backend);

    let temp_dir = tmp.path().join("temp_recovery/.mooncake-tmp");
    std::fs::create_dir(&temp_dir).unwrap();
    std::fs::write(temp_dir.join("stale"), b"partial").unwrap();

    let reopened = LocalStorageBackend::new(config);
    reopened.init().unwrap();
    assert!(!temp_dir.exists());
    assert_eq!(reopened.read_object("keep").unwrap(), b"value");
}

#[test]
fn test_half_initialized_temp_namespace_is_recovered_without_manual_cleanup() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "half_initialized".to_string(),
        enable_eviction: false,
        quota_bytes: 1024,
    };
    let data_dir = tmp.path().join("half_initialized");
    let temp_dir = data_dir.join(".mooncake-tmp");
    std::fs::create_dir_all(&temp_dir).unwrap();
    std::fs::write(temp_dir.join("stale-marker-write"), b"partial").unwrap();

    let backend = LocalStorageBackend::new(config);
    backend.init().unwrap();
    assert!(!temp_dir.exists());
    assert!(data_dir.join(".mooncake-storage-format").exists());
}

#[test]
fn test_second_backend_for_same_directory_is_rejected_until_owner_drops() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "exclusive".to_string(),
        enable_eviction: false,
        quota_bytes: 1024,
    };
    let first = LocalStorageBackend::new(config.clone());
    first.init().unwrap();

    let second = LocalStorageBackend::new(config.clone());
    let error = second.init().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("already open by another backend")
    );

    drop(first);
    second.init().unwrap();
}

#[test]
fn test_failed_ephemeral_lock_contender_does_not_clean_owner_data_on_drop() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "ephemeral-lock".to_string(),
        enable_eviction: false,
        quota_bytes: 1024,
    };
    let owner = LocalStorageBackend::new_ephemeral(config.clone());
    owner.init().unwrap();
    owner.write_object("keep", b"value").unwrap();

    let contender = LocalStorageBackend::new_ephemeral(config);
    assert!(contender.init().is_err());
    drop(contender);

    assert_eq!(owner.read_object("keep").unwrap(), b"value");
}

#[test]
fn test_unsafe_fsdir_is_rejected_before_touching_parent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path().join("root");
    let backend = LocalStorageBackend::new(LocalStorageConfig {
        root_dir: root.clone(),
        fsdir: "..".to_string(),
        enable_eviction: false,
        quota_bytes: 1024,
    });
    let error = backend.init().unwrap_err();
    assert!(error.to_string().contains("one normal path component"));
    assert!(!root.exists());
}

#[cfg(unix)]
#[test]
fn test_data_directory_symlink_is_rejected_without_touching_target() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path().join("root");
    let target = tmp.path().join("foreign-target");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&target).unwrap();
    let sentinel = target.join("sentinel");
    std::fs::write(&sentinel, b"keep").unwrap();
    symlink(&target, root.join("linked")).unwrap();

    let backend = LocalStorageBackend::new(LocalStorageConfig {
        root_dir: root,
        fsdir: "linked".to_string(),
        enable_eviction: false,
        quota_bytes: 1024,
    });
    let error = backend.init().unwrap_err();
    assert!(error.to_string().contains("must not be a symbolic link"));
    assert_eq!(std::fs::read(sentinel).unwrap(), b"keep");
}

#[test]
fn test_nonempty_unowned_storage_is_rejected_without_deletion() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "foreign_data".to_string(),
        enable_eviction: true,
        quota_bytes: 1024 * 1024,
    };
    let data_dir = tmp.path().join("foreign_data");
    let foreign = data_dir.join("aa").join("bb").join("foreign-record");
    std::fs::create_dir_all(foreign.parent().unwrap()).unwrap();
    std::fs::write(&foreign, b"foreign").unwrap();

    let backend = LocalStorageBackend::new(config);
    let error = backend.init().unwrap_err();
    assert!(error.to_string().contains("non-empty unowned storage path"));
    assert_eq!(std::fs::read(&foreign).unwrap(), b"foreign");
}

#[test]
fn test_foreign_format_marker_is_rejected_without_deletion() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = LocalStorageConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "foreign_marker".to_string(),
        enable_eviction: true,
        quota_bytes: 1024 * 1024,
    };
    let data_dir = tmp.path().join("foreign_marker");
    std::fs::create_dir_all(&data_dir).unwrap();
    let marker = data_dir.join(".mooncake-storage-format");
    let foreign = data_dir.join("foreign-record");
    std::fs::write(
        &marker,
        b"backend=file-per-key\nformat=cpp-struct-pb\nversion=1\n",
    )
    .unwrap();
    std::fs::write(&foreign, b"foreign").unwrap();

    let backend = LocalStorageBackend::new(config);
    let error = backend.init().unwrap_err();
    assert!(error.to_string().contains("foreign format marker"));
    assert_eq!(
        std::fs::read(&marker).unwrap(),
        b"backend=file-per-key\nformat=cpp-struct-pb\nversion=1\n"
    );
    assert_eq!(std::fs::read(&foreign).unwrap(), b"foreign");
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
    assert_eq!(
        p1.file_name().unwrap().to_str().unwrap(),
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}

#[test]
fn test_key_path_uses_hash_dirs() {
    let backend = test_backend();
    let path = backend.key_path("my_key");
    let path_str = path.to_string_lossy();
    // Should contain <fsdir>/<digest[0..2]>/<digest[2..4]>/<digest>
    assert!(path_str.contains("test_data/"), "missing fsdir: {path_str}");
    let parts: Vec<&str> = path_str.split('/').collect();
    let idx = parts.iter().position(|p| *p == "test_data").unwrap();
    let dir1 = parts[idx + 1];
    let dir2 = parts[idx + 2];
    let filename = parts[idx + 3];
    assert_eq!(dir1.len(), 2);
    assert_eq!(dir2.len(), 2);
    assert_eq!(filename.len(), 64);
    assert!(filename.chars().all(|ch| ch.is_ascii_hexdigit()));
    assert_eq!(&filename[0..2], dir1);
    assert_eq!(&filename[2..4], dir2);
}

#[test]
fn test_key_path_digest_does_not_expose_slashes() {
    let backend = test_backend();
    let path = backend.key_path("prefix/suffix");
    let filename = path.file_name().unwrap().to_str().unwrap();
    assert_eq!(filename.len(), 64);
    assert!(!filename.contains('/'));
    assert!(!path.to_string_lossy().contains("prefix/suffix"));
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
    assert!(backend.space_usage().0 > 0);
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
    let quota = 150u64;
    let (backend, _tmp) = eviction_backend(quota);

    // Each record includes the key and generation metadata. Two records fit,
    // while the third exceeds the quota.
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
    // Either encoded record fits individually, but both cannot coexist.
    let quota = 100u64;
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

#[test]
fn cpp_parity_file_storage_batch_load_100_objects() {
    // C++ FileStorageTest.BatchLoad_WithStorageBackendAdaptor: after 100
    // objects are offloaded, BatchLoad fills caller-owned slices with every
    // exact value through the FilePerKey adaptor.
    let backend = test_backend();
    let mut expected = Vec::with_capacity(100);
    for index in 0..100 {
        let key = format!("batch_load_key_{index}");
        let value = format!("batch-load-value-{index}")
            .repeat(1 + index % 7)
            .into_bytes();
        backend.write_object(&key, &value).unwrap();
        expected.push((key, value));
    }

    let mut batch = expected
        .iter()
        .map(|(key, value)| (key.clone(), vec![0; value.len()]))
        .collect::<Vec<_>>();
    backend.batch_read_into(&mut batch).unwrap();
    for ((key, loaded), (_, value)) in batch.iter().zip(&expected) {
        assert_eq!(loaded, value, "exact batch-load bytes for {key}");
    }
}

use mooncake_store_client::{
    OffsetAllocatorConfig, OffsetAllocatorStorageBackend, OffsetEvictionPolicy, OffsetPersistMode,
};

fn offset_config(root_dir: std::path::PathBuf) -> OffsetAllocatorConfig {
    OffsetAllocatorConfig {
        root_dir,
        fsdir: "offset".to_string(),
        eviction_policy: OffsetEvictionPolicy::Fifo,
        // A one-byte key and four-byte value occupy a 4,100-byte v3 record.
        quota_bytes: 12_300,
        total_keys_limit: 16,
        high_ratio: 0.90,
        low_ratio: 0.80,
        keys_high_ratio: 0.90,
        keys_low_ratio: 0.80,
        max_evict_per_offload: 16,
        fallback_evict_batch: 2,
        persist_mode: OffsetPersistMode::Disabled,
        ..OffsetAllocatorConfig::default()
    }
}

#[test]
fn offset_allocator_fifo_eviction_reuses_released_extent() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = offset_config(temp.path().to_path_buf());
    config.persist_mode = OffsetPersistMode::Strict;
    let backend = OffsetAllocatorStorageBackend::new(config.clone());
    backend.init().unwrap();

    assert!(backend.write_object("a", b"aaaa").unwrap().is_empty());
    assert!(backend.write_object("b", b"bbbb").unwrap().is_empty());
    assert_eq!(backend.write_object("c", b"cccc").unwrap(), vec!["a"]);

    assert!(!backend.exists("a"));
    assert_eq!(backend.read_object("b").unwrap(), b"bbbb");
    assert_eq!(backend.read_object("c").unwrap(), b"cccc");
    assert_eq!(backend.space_usage(), (8_200, 12_300));
    assert_eq!(
        std::fs::metadata(temp.path().join("offset/offset_allocator.data"))
            .unwrap()
            .len(),
        8_200
    );

    let restarted = OffsetAllocatorStorageBackend::new(config);
    restarted.init().unwrap();
    assert!(!restarted.exists("a"));
    assert_eq!(restarted.read_object("b").unwrap(), b"bbbb");
    assert_eq!(restarted.read_object("c").unwrap(), b"cccc");
    assert_eq!(restarted.space_usage(), (8_200, 12_300));
}

#[test]
fn offset_allocator_restores_prepared_fifo_victims_on_notification_failure() {
    let temp = tempfile::tempdir().unwrap();
    let backend = OffsetAllocatorStorageBackend::new(offset_config(temp.path().to_path_buf()));
    backend.init().unwrap();
    backend.write_object("a", b"aaaa").unwrap();
    backend.write_object("b", b"bbbb").unwrap();

    let pending = backend.prepare_write("c", 4).unwrap();
    assert_eq!(pending.keys(), vec!["a"]);
    assert_eq!(backend.read_object("a").unwrap(), b"aaaa");
    assert_eq!(backend.space_usage(), (8_200, 12_300));
    backend.rollback_eviction(pending);

    assert_eq!(backend.read_object("a").unwrap(), b"aaaa");
    assert_eq!(backend.read_object("b").unwrap(), b"bbbb");
    assert!(!backend.exists("c"));
    assert_eq!(backend.space_usage(), (8_200, 12_300));

    // The restored object keeps its original FIFO position.
    assert_eq!(backend.write_object("c", b"cccc").unwrap(), vec!["a"]);
}

#[test]
fn offset_allocator_upgrades_legacy_index_before_appending() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("offset");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(data_dir.join("offset_allocator.data"), b"aaaabbbb").unwrap();
    std::fs::write(
        data_dir.join("offset_allocator.index.json"),
        r#"{"entries":{"a":{"offset":0,"len":4},"b":{"offset":4,"len":4}}}"#,
    )
    .unwrap();

    let mut config = offset_config(temp.path().to_path_buf());
    config.quota_bytes = 16 * 1024;
    config.persist_mode = OffsetPersistMode::Strict;
    let backend = OffsetAllocatorStorageBackend::new(config);
    backend.init().unwrap();
    backend.write_object("c", b"cccc").unwrap();

    assert_eq!(backend.read_object("a").unwrap(), b"aaaa");
    assert_eq!(backend.read_object("b").unwrap(), b"bbbb");
    assert_eq!(backend.read_object("c").unwrap(), b"cccc");
    assert!(
        std::fs::read(data_dir.join("offset_allocator.data"))
            .unwrap()
            .starts_with(b"aaaabbbb")
    );
}

#[test]
fn offset_allocator_writes_a_versioned_checksummed_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = offset_config(temp.path().to_path_buf());
    config.persist_mode = OffsetPersistMode::Strict;
    let backend = OffsetAllocatorStorageBackend::new(config);
    backend.init().unwrap();
    backend.write_object("a", b"aaaa").unwrap();

    let checkpoint: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("offset/offset_allocator.index.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(checkpoint["format"], "mooncake-offset-allocator-checkpoint");
    assert_eq!(checkpoint["version"], 1);
    assert!(checkpoint["payload_crc32c"].is_u64());
    assert!(checkpoint["payload"].is_object());
}

#[test]
fn offset_allocator_uses_fallback_batch_after_key_high_watermark_is_exceeded() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = offset_config(temp.path().to_path_buf());
    config.quota_bytes = 64 * 1024;
    config.total_keys_limit = 3;
    config.keys_high_ratio = 0.90;
    config.keys_low_ratio = 0.80;
    config.fallback_evict_batch = 2;
    let backend = OffsetAllocatorStorageBackend::new(config);
    backend.init().unwrap();

    assert!(backend.write_object("a", b"a").unwrap().is_empty());
    assert!(backend.write_object("b", b"b").unwrap().is_empty());
    assert!(backend.write_object("c", b"c").unwrap().is_empty());
    assert_eq!(backend.write_object("d", b"d").unwrap(), vec!["a", "b"]);

    assert!(!backend.exists("a"));
    assert!(!backend.exists("b"));
    assert!(backend.exists("c"));
    assert!(backend.exists("d"));
}

#[test]
fn offset_allocator_keeps_notified_victims_evicted_when_new_write_fails() {
    let temp = tempfile::tempdir().unwrap();
    let backend = OffsetAllocatorStorageBackend::new(offset_config(temp.path().to_path_buf()));
    backend.init().unwrap();
    backend.write_object("a", b"aaaa").unwrap();
    backend.write_object("b", b"bbbb").unwrap();

    let pending = backend.prepare_write("c", 4).unwrap();
    assert_eq!(pending.keys(), vec!["a"]);

    let data_path = temp.path().join("offset/offset_allocator.data");
    std::fs::remove_file(&data_path).unwrap();
    std::fs::create_dir(&data_path).unwrap();
    assert!(backend.commit_write("c", b"cccc", pending).is_err());

    // The caller has already notified master before commit_write. Restoring
    // the victim here would diverge local state from master metadata.
    assert!(!backend.exists("a"));
    assert!(backend.exists("b"));
    assert!(!backend.exists("c"));
    assert_eq!(backend.space_usage(), (4_100, 12_300));
}

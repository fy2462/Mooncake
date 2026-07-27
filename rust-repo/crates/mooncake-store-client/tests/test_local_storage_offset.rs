use mooncake_store_client::{
    OffsetAllocatorConfig, OffsetAllocatorStorageBackend, OffsetEvictionPolicy, OffsetPersistMode,
    OffsetPersistenceConfig, migrate_cpp_offset_allocator_layout,
};

#[test]
fn cpp_offset_layout_is_rejected_without_mutation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().join("offset");
    std::fs::create_dir_all(&data_dir).unwrap();
    let cpp_data = data_dir.join("kv_cache.data");
    let cpp_meta = data_dir.join("kv_cache.meta");
    std::fs::write(&cpp_data, b"cpp-data").unwrap();
    std::fs::write(&cpp_meta, b"cpp-meta").unwrap();

    let backend = OffsetAllocatorStorageBackend::new(OffsetAllocatorConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "offset".to_string(),
        quota_bytes: 1024 * 1024,
        ..OffsetAllocatorConfig::default()
    });
    let error = backend.init().unwrap_err();

    assert!(error.to_string().contains("C++ storage layout"));
    assert_eq!(std::fs::read(&cpp_data).unwrap(), b"cpp-data");
    assert_eq!(std::fs::read(&cpp_meta).unwrap(), b"cpp-meta");
    assert!(!data_dir.join("offset_allocator.data").exists());
    assert!(!data_dir.join("offset_allocator.index.json").exists());
    assert!(!data_dir.join(".offset_allocator.lock").exists());
}

#[test]
fn mixed_cpp_and_rust_offset_layout_is_rejected_without_mutation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().join("offset");
    std::fs::create_dir_all(&data_dir).unwrap();
    let cpp_data = data_dir.join("kv_cache.data");
    let rust_data = data_dir.join("offset_allocator.data");
    std::fs::write(&cpp_data, b"cpp-data").unwrap();
    std::fs::write(&rust_data, b"rust-data").unwrap();

    let backend = OffsetAllocatorStorageBackend::new(OffsetAllocatorConfig {
        root_dir: tmp.path().to_path_buf(),
        fsdir: "offset".to_string(),
        quota_bytes: 1024 * 1024,
        ..OffsetAllocatorConfig::default()
    });
    let error = backend.init().unwrap_err();

    assert!(error.to_string().contains("mixed C++ and Rust"));
    assert_eq!(std::fs::read(&cpp_data).unwrap(), b"cpp-data");
    assert_eq!(std::fs::read(&rust_data).unwrap(), b"rust-data");
    assert!(!data_dir.join(".offset_allocator.lock").exists());
}

fn persistence(mode: OffsetPersistMode) -> OffsetPersistenceConfig {
    OffsetPersistenceConfig {
        persist_mode: mode,
        ..OffsetPersistenceConfig::default()
    }
}

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
    }
}

#[test]
fn offset_allocator_fifo_eviction_reuses_released_extent() {
    let temp = tempfile::tempdir().unwrap();
    let config = offset_config(temp.path().to_path_buf());
    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        config.clone(),
        persistence(OffsetPersistMode::Strict),
    );
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
    let checkpoint: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("offset/offset_allocator.index.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        checkpoint["payload"]["tombstones"],
        serde_json::json!(["a"])
    );
    drop(backend);

    let restarted = OffsetAllocatorStorageBackend::new_with_persistence(
        config,
        persistence(OffsetPersistMode::Strict),
    );
    restarted.init().unwrap();
    assert!(!restarted.exists("a"));
    assert_eq!(restarted.read_object("b").unwrap(), b"bbbb");
    assert_eq!(restarted.read_object("c").unwrap(), b"cccc");
    assert_eq!(restarted.space_usage(), (8_200, 12_300));
}

#[test]
fn offset_allocator_scan_meta_survives_restart_with_value_sizes() {
    let temp = tempfile::tempdir().unwrap();
    let config = offset_config(temp.path().to_path_buf());
    {
        let backend = OffsetAllocatorStorageBackend::new_with_persistence(
            config.clone(),
            persistence(OffsetPersistMode::Strict),
        );
        backend.init().unwrap();
        backend.write_object("v1:8:tenant-bb", b"bbbb").unwrap();
        backend.write_object("v1:8:tenant-aa", b"a").unwrap();
        assert_eq!(
            backend.scan_meta().unwrap(),
            vec![
                ("v1:8:tenant-aa".to_string(), 1),
                ("v1:8:tenant-bb".to_string(), 4),
            ]
        );
    }

    let restarted = OffsetAllocatorStorageBackend::new_with_persistence(
        config,
        persistence(OffsetPersistMode::Strict),
    );
    restarted.init().unwrap();
    assert_eq!(
        restarted.scan_meta().unwrap(),
        vec![
            ("v1:8:tenant-aa".to_string(), 1),
            ("v1:8:tenant-bb".to_string(), 4),
        ]
    );
}

#[test]
fn offset_allocator_restores_prepared_fifo_victims_on_notification_failure() {
    let temp = tempfile::tempdir().unwrap();
    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        offset_config(temp.path().to_path_buf()),
        OffsetPersistenceConfig::default(),
    );
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
    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        config,
        persistence(OffsetPersistMode::Strict),
    );
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
    let config = offset_config(temp.path().to_path_buf());
    let expected_quota_bytes = config.quota_bytes;
    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        config,
        persistence(OffsetPersistMode::Strict),
    );
    backend.init().unwrap();
    backend.write_object("a", b"aaaa").unwrap();

    let checkpoint: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("offset/offset_allocator.index.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(checkpoint["format"], "mooncake-offset-allocator-checkpoint");
    assert_eq!(checkpoint["version"], 4);
    assert!(checkpoint["payload_crc32c"].is_u64());
    assert!(checkpoint["payload"].is_object());
    assert_eq!(checkpoint["payload"]["quota_bytes"], expected_quota_bytes);
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
    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        config,
        OffsetPersistenceConfig::default(),
    );
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
    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        offset_config(temp.path().to_path_buf()),
        OffsetPersistenceConfig::default(),
    );
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

#[test]
fn synthetic_cpp_v3_shape_migrates_without_a_fifo_index() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("cpp-offset");
    std::fs::create_dir(&source).unwrap();
    let (data, metadata) = synthetic_cpp_v3_single_record_bytes();
    std::fs::write(source.join("kv_cache.data"), &data).unwrap();
    std::fs::write(source.join("kv_cache.meta"), &metadata).unwrap();

    let target_root = temp.path().join("rust-offset");
    let config = OffsetAllocatorConfig {
        root_dir: target_root.clone(),
        fsdir: "imported".to_string(),
        eviction_policy: OffsetEvictionPolicy::None,
        quota_bytes: 8192,
        total_keys_limit: 8,
        ..OffsetAllocatorConfig::default()
    };
    let report = migrate_cpp_offset_allocator_layout(&source, config.clone(), "default").unwrap();

    assert_eq!(report.object_count, 1);
    assert_eq!(report.skipped_record_count, 0);
    assert_eq!(report.unverified_object_count, 1);
    assert_eq!(report.value_bytes, 1);
    assert_eq!(report.source_capacity_bytes, 8192);
    assert_eq!(std::fs::read(source.join("kv_cache.data")).unwrap(), data);
    assert_eq!(
        std::fs::read(source.join("kv_cache.meta")).unwrap(),
        metadata
    );

    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        config,
        persistence(OffsetPersistMode::Strict),
    );
    backend.init().unwrap();
    assert_eq!(backend.read_object("v1:7:defaultk").unwrap(), b"v");
}

/// Hand-assembled bytes matching the audited C++ v3 layout. This is decoder
/// coverage, not a C++-produced golden fixture.
fn synthetic_cpp_v3_single_record_bytes() -> (Vec<u8>, Vec<u8>) {
    const UNUSED: u32 = u32::MAX;
    let mut allocator = vec![0_u8; 1184];
    put_u64(&mut allocator, 0, 0); // base
    put_u64(&mut allocator, 8, 0); // multiplier_bits
    put_u64(&mut allocator, 16, 8192); // capacity
    put_u64(&mut allocator, 24, 4097); // allocated_size
    put_u64(&mut allocator, 32, 1); // allocated_num
    put_u32(&mut allocator, 40, 8192); // internal size
    put_u32(&mut allocator, 44, 2); // current_capacity
    put_u32(&mut allocator, 48, 2); // max_capacity
    put_u32(&mut allocator, 52, 3584); // free storage
    put_u32(&mut allocator, 1116, 2); // freeOffset: both nodes are active

    let used_node = 1120;
    put_u32(&mut allocator, used_node, 0);
    put_u32(&mut allocator, used_node + 4, 4608);
    put_u32(&mut allocator, used_node + 8, UNUSED);
    put_u32(&mut allocator, used_node + 12, UNUSED);
    put_u32(&mut allocator, used_node + 16, UNUSED);
    put_u32(&mut allocator, used_node + 20, 1);
    allocator[used_node + 24] = 1;

    let free_node = used_node + 28;
    put_u32(&mut allocator, free_node, 4608);
    put_u32(&mut allocator, free_node + 4, 3584);
    put_u32(&mut allocator, free_node + 8, UNUSED);
    put_u32(&mut allocator, free_node + 12, UNUSED);
    put_u32(&mut allocator, free_node + 16, 0);
    put_u32(&mut allocator, free_node + 20, UNUSED);
    put_u32(&mut allocator, 1176, 0);
    put_u32(&mut allocator, 1180, 1);

    let mut data = vec![0_u8; 8192];
    put_u32(&mut data, 0, 1); // key length
    put_u32(&mut data, 4, 1); // value length
    put_u64(&mut data, 8, 0); // checkpointed write sequence
    put_u32(&mut data, 16, 0); // valid v3 record without CRC
    put_u32(&mut data, 20, 0);
    data[24] = b'k';
    data[4096] = b'v';

    let mut metadata = vec![0x08, 0x03, 0x12];
    put_varint(&mut metadata, allocator.len() as u64);
    metadata.extend_from_slice(&allocator);
    metadata.extend_from_slice(&[0x18, 0x01]); // insert_seq = 1
    (data, metadata)
}

fn put_u32(buffer: &mut [u8], offset: usize, value: u32) {
    buffer[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(buffer: &mut [u8], offset: usize, value: u64) {
    buffer[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_varint(buffer: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

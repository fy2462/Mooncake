use super::*;

#[test]
fn test_distributed_storage_backend_offload_load_scan_and_remove() {
    let tmp = temp_dir();
    let config = DistributedStorageConfig::default()
        .with_root(&tmp)
        .with_fs_adapter_type("posix")
        .with_hash_bucket_count(8)
        .with_health_check(true);
    let backend = StorageBackend::new_distributed(config).unwrap();

    let key_a = "tenant@a:key/with\\chars%".to_string();
    let key_b = "plain-key".to_string();
    backend
        .batch_offload(&[
            (key_a.clone(), b"hello distributed".to_vec()),
            (key_b.clone(), b"world".to_vec()),
        ])
        .unwrap();

    assert!(backend.is_enable_offloading());
    assert!(backend.is_exist(&key_a).unwrap());

    let loaded = backend.batch_load(&[key_a.clone(), key_b.clone()]).unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0], (key_a.clone(), b"hello distributed".to_vec()));
    assert_eq!(loaded[1], (key_b.clone(), b"world".to_vec()));

    let mut meta = backend.scan_meta().unwrap();
    meta.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        meta,
        vec![(key_b.clone(), 5), (key_a.clone(), 17)]
            .into_iter()
            .collect::<Vec<_>>()
    );

    assert_eq!(backend.remove_by_regex("tenant@a:.*").unwrap(), 1);
    assert!(!backend.is_exist(&key_a).unwrap());
    assert!(backend.is_exist(&key_b).unwrap());
    assert_eq!(backend.remove_all().unwrap(), 1);
    assert!(backend.scan_meta().unwrap().is_empty());
}

#[test]
fn test_distributed_storage_filename_codec_matches_cpp_rules() {
    let key = "a@b:c/d\\e%f\n\u{2603}";
    let escaped = StorageBackend::escape_distributed_filename(key);
    assert_eq!(escaped, "a%40b%3ac%2fd%5ce%25f%0a%e2%98%83");
    assert_eq!(StorageBackend::unescape_distributed_filename(&escaped), key);
}

#[test]
fn test_bucket_storage_backend_offload_load_scan_and_remove() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::Bucket, &tmp);

    backend
        .batch_offload(&[
            ("alpha".to_string(), b"one".to_vec()),
            ("beta".to_string(), b"two-two".to_vec()),
        ])
        .unwrap();
    // C++ BatchOffload rejects a duplicate key and preserves the original
    // bytes.
    assert!(
        backend
            .batch_offload(&[("alpha".to_string(), b"updated".to_vec())])
            .is_err()
    );

    assert!(backend.is_enable_offloading());
    assert!(backend.is_exist("alpha").unwrap());
    assert!(!backend.is_exist("missing").unwrap());

    let loaded = backend
        .batch_load(&["alpha".to_string(), "beta".to_string()])
        .unwrap();
    assert_eq!(
        loaded,
        vec![
            ("alpha".to_string(), b"one".to_vec()),
            ("beta".to_string(), b"two-two".to_vec()),
        ]
    );

    let mut meta = backend.scan_meta().unwrap();
    meta.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        meta,
        vec![("alpha".to_string(), 3), ("beta".to_string(), 7)]
    );

    assert_eq!(backend.remove_by_regex("alp.*").unwrap(), 1);
    assert!(!backend.is_exist("alpha").unwrap());
    assert_eq!(backend.remove_all().unwrap(), 1);
}

#[test]
fn cpp_parity_storage_backend_test_storagebackendtest_storagebackendall() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::Bucket, &tmp);

    assert!(backend.init());
    assert!(std::fs::read_dir(&tmp).unwrap().next().is_none());
    assert!(!backend.init());

    let expected = vec![
        ("key-a".to_string(), b"alpha".to_vec()),
        ("key-b".to_string(), b"beta-beta".to_vec()),
        ("key-c".to_string(), vec![b'c'; 1024]),
    ];
    backend.batch_offload(&expected).unwrap();

    let mut metadata = backend.scan_meta().unwrap();
    metadata.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        metadata,
        expected
            .iter()
            .map(|(key, bytes)| (key.clone(), bytes.len() as u64))
            .collect::<Vec<_>>()
    );

    let keys = expected
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let loaded = backend.batch_load(&keys).unwrap();
    assert_eq!(loaded, expected);
    for (key, bytes) in &loaded {
        assert!(backend.is_exist(key).unwrap());
        assert_eq!(
            bytes.len(),
            metadata.iter().find(|(name, _)| name == key).unwrap().1 as usize
        );
    }

    assert_eq!(backend.remove_all().unwrap(), 3);
    assert!(backend.scan_meta().unwrap().is_empty());
}

#[test]
fn cpp_parity_storage_backend_test_adaptorscanmetaandisenableoffloading() {
    let tmp = temp_dir();
    let data = tmp.join("file-per-key-readiness");
    let backend = StorageBackend::new_file_per_key_adaptor(&data, 10, 1024 * 1024);
    assert!(backend.init());
    assert!(backend.offloading_readiness().is_err());
    assert!(
        backend
            .batch_offload(&[("before-scan".into(), b"rejected".to_vec())])
            .is_err()
    );
    assert!(!backend.is_exist("before-scan").unwrap());
    assert!(backend.scan_meta().unwrap().is_empty());
    assert_eq!(backend.offloading_readiness().unwrap(), true);

    let expected = vec![
        ("k1".to_string(), vec![b'a'; 128]),
        ("k2".to_string(), vec![b'b'; 256]),
    ];
    backend.batch_offload(&expected).unwrap();
    assert_eq!(backend.offloading_readiness().unwrap(), true);

    let restarted = StorageBackend::new_file_per_key_adaptor(&data, 10, 1024 * 1024);
    assert!(restarted.init());
    let mut scanned = restarted.scan_meta().unwrap();
    scanned.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(scanned, vec![("k1".into(), 128), ("k2".into(), 256)]);
    assert_eq!(restarted.offloading_readiness().unwrap(), true);
    assert_eq!(
        restarted.batch_load(&["k1".into(), "k2".into()]).unwrap(),
        expected
    );

    let strict = StorageBackend::new_file_per_key_adaptor(&data, 1, 1);
    assert!(strict.init());
    assert_eq!(strict.scan_meta().unwrap().len(), 2);
    assert_eq!(strict.offloading_readiness().unwrap(), false);
    assert_eq!(restarted.remove_all().unwrap(), 2);
}

#[test]
fn cpp_parity_bucket_storage_backend_concurrent_read_write_delete() {
    // C++ StorageBackendTest.BucketStorageBackend_ConcurrentReadWriteDelete:
    // under concurrent unique-key writes, exact reads, and deletion attempts
    // no successful read returns corrupted bytes and both write and read
    // counts are positive.
    let tmp = temp_dir();
    let backend = Arc::new(StorageBackend::new(StorageBackendType::Bucket, &tmp));
    const WRITERS: usize = 4;
    const KEYS_PER_WRITER: usize = 200;

    let mut handles = Vec::new();
    for writer in 0..WRITERS {
        let backend = Arc::clone(&backend);
        handles.push(std::thread::spawn(move || {
            let mut writes = 0usize;
            let mut reads = 0usize;
            for index in 0..KEYS_PER_WRITER {
                let key = format!("concurrent-{writer}-{index}");
                let value = format!("value-{writer}-{index}").into_bytes();
                if backend
                    .batch_offload(&[(key.clone(), value.clone())])
                    .is_ok()
                {
                    writes += 1;
                }
                if let Ok(loaded) = backend.batch_load(&[key.clone()])
                    && let Some((_, bytes)) = loaded.into_iter().next()
                {
                    assert_eq!(bytes, value, "corrupted read for key {key}");
                    reads += 1;
                }
                // Concurrent deletion attempt against this writer's own
                // previous key; absence after removal is expected.
                if index > 0 {
                    let _ = backend.remove_by_regex(&format!("concurrent-{writer}-{}", index - 1));
                }
            }
            (writes, reads)
        }));
    }

    let mut total_writes = 0usize;
    let mut total_reads = 0usize;
    for handle in handles {
        let (writes, reads) = handle.join().unwrap();
        total_writes += writes;
        total_reads += reads;
    }
    assert!(total_writes > 0, "expected positive writes");
    assert!(total_reads > 0, "expected positive exact reads");

    let removed = backend.remove_all().unwrap();
    assert!(removed > 0);
    assert!(backend.scan_meta().unwrap().is_empty());
}

#[test]
fn cpp_parity_offset_allocator_empty_batch_is_invalid() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::OffsetAllocator, &tmp);
    let data_path = tmp.join("offset_allocator.data");
    let index_path = tmp.join("offset_allocator.index.msgpack");

    assert!(backend.batch_offload(&[]).is_err());
    assert!(!data_path.exists());
    assert!(!index_path.exists());
}

#[test]
fn cpp_parity_adaptor_empty_batch_is_invalid() {
    let tmp = temp_dir();
    let config = DistributedStorageConfig::default()
        .with_root(&tmp)
        .with_fs_adapter_type("posix")
        .with_hash_bucket_count(8)
        .with_health_check(true);
    let backend = StorageBackend::new_distributed(config).unwrap();

    assert!(backend.batch_offload(&[]).is_err());
    assert!(backend.scan_meta().unwrap().is_empty());
}

#[test]
fn cpp_parity_offset_allocator_empty_value_is_skipped() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::OffsetAllocator, &tmp);

    backend
        .batch_offload(&[("empty_key".to_string(), Vec::new())])
        .unwrap();
    assert!(!backend.is_exist("empty_key").unwrap());
    assert!(backend.scan_meta().unwrap().is_empty());
}

#[test]
fn cpp_parity_bucket_duplicate_key_rejected_preserves_original() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::Bucket, &tmp);
    backend
        .batch_offload(&[("dup-key".to_string(), b"original".to_vec())])
        .unwrap();

    assert!(
        backend
            .batch_offload(&[("dup-key".to_string(), b"replacement".to_vec())])
            .is_err()
    );
    let loaded = backend.batch_load(&["dup-key".to_string()]).unwrap();
    assert_eq!(loaded, vec![("dup-key".to_string(), b"original".to_vec())]);
}

#[test]
fn cpp_parity_bucket_duplicate_batch_rejects_atomically() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::Bucket, &tmp);
    backend
        .batch_offload(&[("key-a".to_string(), b"original-a".to_vec())])
        .unwrap();

    assert!(
        backend
            .batch_offload(&[
                ("key-a".to_string(), b"replacement-a".to_vec()),
                ("key-b".to_string(), b"new-b".to_vec()),
            ])
            .is_err()
    );
    assert!(!backend.is_exist("key-b").unwrap());
    let loaded = backend.batch_load(&["key-a".to_string()]).unwrap();
    assert_eq!(loaded, vec![("key-a".to_string(), b"original-a".to_vec())]);
}

#[test]
fn cpp_parity_bucket_duplicate_rejection_leaves_no_extra_durable_state() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::Bucket, &tmp);
    backend
        .batch_offload(&[("keep-key".to_string(), b"value".to_vec())])
        .unwrap();
    let before = backend.scan_meta().unwrap();

    assert!(
        backend
            .batch_offload(&[("keep-key".to_string(), b"other".to_vec())])
            .is_err()
    );
    let after = backend.scan_meta().unwrap();
    assert_eq!(after, before);
    let leftover = std::fs::read_dir(tmp.join("buckets"))
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp"))
        .count();
    assert_eq!(leftover, 0);
}

#[test]
fn test_bucket_storage_backend_skips_non_numeric_bucket_files() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::Bucket, &tmp);

    backend
        .batch_offload(&[("alpha".to_string(), b"one".to_vec())])
        .unwrap();
    let bucket_dir = tmp.join("buckets");
    std::fs::write(bucket_dir.join("backup.bucket"), b"not-a-bucket").unwrap();

    assert!(backend.is_exist("alpha").unwrap());
    assert_eq!(
        backend.batch_load(&["alpha".to_string()]).unwrap(),
        vec![("alpha".to_string(), b"one".to_vec())]
    );
    assert_eq!(backend.scan_meta().unwrap(), vec![("alpha".to_string(), 3)]);
    assert_eq!(backend.remove_all().unwrap(), 1);
    assert!(bucket_dir.join("backup.bucket").exists());
}

#[test]
fn test_offset_allocator_storage_backend_offload_load_scan_and_remove() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::OffsetAllocator, &tmp);

    backend
        .batch_offload(&[
            ("k1".to_string(), b"payload-one".to_vec()),
            ("k2".to_string(), b"payload-two".to_vec()),
        ])
        .unwrap();
    backend
        .batch_offload(&[("k1".to_string(), b"replacement".to_vec())])
        .unwrap();

    assert!(backend.is_enable_offloading());
    assert!(backend.is_exist("k1").unwrap());

    let loaded = backend
        .batch_load(&["k1".to_string(), "k2".to_string(), "missing".to_string()])
        .unwrap();
    assert_eq!(
        loaded,
        vec![
            ("k1".to_string(), b"replacement".to_vec()),
            ("k2".to_string(), b"payload-two".to_vec()),
        ]
    );

    let mut meta = backend.scan_meta().unwrap();
    meta.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(meta, vec![("k1".to_string(), 11), ("k2".to_string(), 11)]);

    backend.remove_keys(&["k2".to_string()]).unwrap();
    assert!(!backend.is_exist("k2").unwrap());
    assert_eq!(backend.remove_all().unwrap(), 1);
}

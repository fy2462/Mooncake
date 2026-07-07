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
    backend
        .batch_offload(&[("alpha".to_string(), b"updated".to_vec())])
        .unwrap();

    assert!(backend.is_enable_offloading());
    assert!(backend.is_exist("alpha").unwrap());
    assert!(!backend.is_exist("missing").unwrap());

    let loaded = backend
        .batch_load(&["alpha".to_string(), "beta".to_string()])
        .unwrap();
    assert_eq!(
        loaded,
        vec![
            ("beta".to_string(), b"two-two".to_vec()),
            ("alpha".to_string(), b"updated".to_vec()),
        ]
    );

    let mut meta = backend.scan_meta().unwrap();
    meta.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        meta,
        vec![("alpha".to_string(), 7), ("beta".to_string(), 7)]
    );

    assert_eq!(backend.remove_by_regex("alp.*").unwrap(), 1);
    assert!(!backend.is_exist("alpha").unwrap());
    assert_eq!(backend.remove_all().unwrap(), 1);
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

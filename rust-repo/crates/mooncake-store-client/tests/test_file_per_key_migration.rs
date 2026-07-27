use mooncake_store_client::{
    LocalStorageBackend, LocalStorageConfig, migrate_cpp_file_per_key_layout,
};

fn append_varint(output: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn cpp_kv_entry(key: &str, value: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    output.push(0x0a);
    append_varint(&mut output, key.len());
    output.extend_from_slice(key.as_bytes());
    if !value.is_empty() {
        output.push(0x12);
        append_varint(&mut output, value.len());
        output.extend_from_slice(value);
    }
    output
}

fn target_config(root: &std::path::Path) -> LocalStorageConfig {
    LocalStorageConfig {
        root_dir: root.to_path_buf(),
        fsdir: "migrated".to_string(),
        enable_eviction: false,
        quota_bytes: 1024 * 1024,
    }
}

#[test]
fn cpp_file_per_key_migration_preserves_values_tenants_and_source() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let first = source_root.path().join("a/b/cpp-record-a");
    let second = source_root.path().join("c/d/cpp-record-b");
    std::fs::create_dir_all(first.parent().unwrap()).unwrap();
    std::fs::create_dir_all(second.parent().unwrap()).unwrap();
    let first_bytes = cpp_kv_entry("key/a", b"\0binary/value");
    let second_bytes = cpp_kv_entry("tenant-b\0key-b", b"");
    std::fs::write(&first, &first_bytes).unwrap();
    std::fs::write(&second, &second_bytes).unwrap();

    let config = target_config(target_root.path());
    let report =
        migrate_cpp_file_per_key_layout(source_root.path(), config.clone(), "default").unwrap();

    assert_eq!(report.object_count, 2);
    assert_eq!(report.value_bytes, 13);
    assert_eq!(report.target_data_dir, target_root.path().join("migrated"));
    assert_eq!(std::fs::read(&first).unwrap(), first_bytes);
    assert_eq!(std::fs::read(&second).unwrap(), second_bytes);

    let backend = LocalStorageBackend::new_persistent(config);
    backend.init().unwrap();
    assert_eq!(
        backend.read_object("v1:7:defaultkey/a").unwrap(),
        b"\0binary/value"
    );
    assert_eq!(backend.read_object("v1:8:tenant-bkey-b").unwrap(), b"");
    let mut metadata = backend.scan_meta().unwrap();
    metadata.sort();
    assert_eq!(
        metadata,
        [
            ("v1:7:defaultkey/a".to_string(), 13),
            ("v1:8:tenant-bkey-b".to_string(), 0),
        ]
    );
}

#[test]
fn corrupt_source_fails_before_target_publication() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let source = source_root.path().join("a/b/corrupt");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, b"\x0a\x80").unwrap();

    let error = migrate_cpp_file_per_key_layout(
        source_root.path(),
        target_config(target_root.path()),
        "default",
    )
    .unwrap_err();

    assert!(error.to_string().contains("invalid C++ FilePerKey record"));
    assert_eq!(std::fs::read(&source).unwrap(), b"\x0a\x80");
    assert!(!target_root.path().join("migrated").exists());
    assert_eq!(std::fs::read_dir(target_root.path()).unwrap().count(), 0);
}

#[test]
fn duplicate_scoped_keys_fail_before_target_publication() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let first = source_root.path().join("a/b/one");
    let second = source_root.path().join("c/d/two");
    std::fs::create_dir_all(first.parent().unwrap()).unwrap();
    std::fs::create_dir_all(second.parent().unwrap()).unwrap();
    std::fs::write(&first, cpp_kv_entry("same", b"one")).unwrap();
    std::fs::write(&second, cpp_kv_entry("same", b"two")).unwrap();

    let error = migrate_cpp_file_per_key_layout(
        source_root.path(),
        target_config(target_root.path()),
        "default",
    )
    .unwrap_err();

    assert!(error.to_string().contains("duplicate FilePerKey key"));
    assert!(!target_root.path().join("migrated").exists());
    assert_eq!(std::fs::read_dir(target_root.path()).unwrap().count(), 0);
}

#[test]
fn insufficient_target_quota_fails_before_target_publication() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let source = source_root.path().join("a/b/large");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, cpp_kv_entry("large", &[0u8; 128])).unwrap();
    let mut config = target_config(target_root.path());
    config.quota_bytes = 64;

    let error = migrate_cpp_file_per_key_layout(source_root.path(), config, "default").unwrap_err();

    assert!(matches!(
        error,
        mooncake_store_core::StoreError::NoAvailableHandle
    ));
    assert!(!target_root.path().join("migrated").exists());
    assert_eq!(std::fs::read_dir(target_root.path()).unwrap().count(), 0);
}

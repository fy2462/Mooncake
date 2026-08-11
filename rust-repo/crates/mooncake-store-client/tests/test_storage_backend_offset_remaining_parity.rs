use mooncake_store_client::local_storage_backend::{
    OffsetAllocatorConfig, OffsetAllocatorStorageBackend, OffsetEvictionPolicy,
};
use mooncake_store_core::StoreError;

fn config(root: &std::path::Path) -> OffsetAllocatorConfig {
    config_with_key_limit(root, 1_000)
}

fn config_with_key_limit(root: &std::path::Path, total_keys_limit: usize) -> OffsetAllocatorConfig {
    OffsetAllocatorConfig {
        root_dir: root.to_path_buf(),
        fsdir: "offset-double-init".to_string(),
        eviction_policy: OffsetEvictionPolicy::None,
        quota_bytes: 10 * 1024 * 1024,
        total_keys_limit,
        high_ratio: 0.90,
        low_ratio: 0.80,
        keys_high_ratio: 0.90,
        keys_low_ratio: 0.80,
        max_evict_per_offload: 16,
        fallback_evict_batch: 2,
    }
}

#[test]
fn cpp_parity_offset_allocator_second_init_returns_internal_error() {
    let root = tempfile::tempdir().unwrap();
    let backend = OffsetAllocatorStorageBackend::new(config(root.path()));

    backend.init().unwrap();
    assert!(matches!(backend.init(), Err(StoreError::Internal(_))));
}

#[test]
fn cpp_parity_offset_allocator_enablement_tracks_key_limit() {
    let root = tempfile::tempdir().unwrap();
    let backend = OffsetAllocatorStorageBackend::new(config_with_key_limit(root.path(), 5));
    backend.init().unwrap();

    assert!(backend.is_enable_offloading().unwrap());
    for index in 0..5 {
        backend
            .write_object(&format!("key-{index}"), b"value")
            .unwrap();
    }
    assert!(!backend.is_enable_offloading().unwrap());

    // A one-byte key and one-byte value occupy a complete 4,097-byte record:
    // 24-byte header + key + alignment padding + value. Readiness follows the
    // full record accounting, not only the payload byte.
    let exact_root = tempfile::tempdir().unwrap();
    let mut exact_config = config(exact_root.path());
    exact_config.quota_bytes = 4_097;
    let exact_backend = OffsetAllocatorStorageBackend::new(exact_config);
    exact_backend.init().unwrap();
    assert!(exact_backend.is_enable_offloading().unwrap());
    exact_backend.write_object("k", b"v").unwrap();
    assert_eq!(exact_backend.space_usage(), (4_097, 4_097));
    assert!(!exact_backend.is_enable_offloading().unwrap());
}

#[test]
fn cpp_parity_offset_allocator_operations_fail_before_init() {
    let root = tempfile::tempdir().unwrap();
    let backend = OffsetAllocatorStorageBackend::new(config(root.path()));

    assert!(matches!(
        backend.is_exist("key"),
        Err(StoreError::Internal(_))
    ));
    assert!(matches!(
        backend.is_enable_offloading(),
        Err(StoreError::Internal(_))
    ));
    assert!(matches!(backend.scan_meta(), Err(StoreError::Internal(_))));
    assert!(matches!(
        backend.read_object("key"),
        Err(StoreError::Internal(_))
    ));
    assert!(matches!(
        backend.write_object("key", b"value"),
        Err(StoreError::Internal(_))
    ));
}

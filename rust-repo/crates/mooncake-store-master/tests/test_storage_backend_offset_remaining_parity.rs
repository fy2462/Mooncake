use mooncake_store_master::storage_backend::{StorageBackend, StorageBackendType};

#[test]
fn cpp_parity_offset_persist_skips_oversized_key_and_accepts_boundary() {
    let root = tempfile::tempdir().unwrap();
    let backend = StorageBackend::new(StorageBackendType::OffsetAllocator, root.path());
    let value = b"value".to_vec();
    let oversized_key = "k".repeat(1024 * 1024 + 1);

    backend
        .batch_offload(&[(oversized_key.clone(), value.clone())])
        .unwrap();
    assert!(!backend.is_exist(&oversized_key).unwrap());

    let boundary_key = "m".repeat(1024 * 1024);
    backend
        .batch_offload(&[(boundary_key.clone(), value.clone())])
        .unwrap();
    assert!(backend.is_exist(&boundary_key).unwrap());
    assert_eq!(
        backend
            .batch_load(std::slice::from_ref(&boundary_key))
            .unwrap(),
        vec![(boundary_key, value)]
    );
}

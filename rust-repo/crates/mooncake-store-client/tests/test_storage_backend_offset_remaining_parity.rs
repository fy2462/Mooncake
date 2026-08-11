use mooncake_store_client::local_storage_backend::{
    OffsetAllocatorConfig, OffsetAllocatorStorageBackend, OffsetEvictionPolicy,
};
use mooncake_store_core::StoreError;

fn config(root: &std::path::Path) -> OffsetAllocatorConfig {
    OffsetAllocatorConfig {
        root_dir: root.to_path_buf(),
        fsdir: "offset-double-init".to_string(),
        eviction_policy: OffsetEvictionPolicy::None,
        quota_bytes: 10 * 1024 * 1024,
        total_keys_limit: 1_000,
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

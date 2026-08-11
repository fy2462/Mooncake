use mooncake_store_client::local_storage_backend::{
    OffsetAllocatorConfig, OffsetAllocatorStorageBackend, OffsetEvictionPolicy, OffsetPersistMode,
    OffsetPersistenceConfig,
};
use mooncake_store_core::StoreError;
use std::path::Path;
use std::sync::{Arc, Barrier};

fn offset_config(
    root: &Path,
    eviction_policy: OffsetEvictionPolicy,
    quota_bytes: u64,
) -> OffsetAllocatorConfig {
    OffsetAllocatorConfig {
        root_dir: root.to_path_buf(),
        fsdir: "offset".to_string(),
        eviction_policy,
        quota_bytes,
        total_keys_limit: 10_000,
        high_ratio: 0.90,
        low_ratio: 0.80,
        keys_high_ratio: 0.90,
        keys_low_ratio: 0.80,
        max_evict_per_offload: 16,
        fallback_evict_batch: 2,
    }
}

fn persistence() -> OffsetPersistenceConfig {
    OffsetPersistenceConfig {
        persist_mode: OffsetPersistMode::Relaxed,
        persist_interval_seconds: 60,
        enable_record_crc: true,
    }
}

fn backend(
    root: &Path,
    eviction_policy: OffsetEvictionPolicy,
    quota_bytes: u64,
) -> OffsetAllocatorStorageBackend {
    let backend = OffsetAllocatorStorageBackend::new_with_persistence(
        offset_config(root, eviction_policy, quota_bytes),
        persistence(),
    );
    backend.init().unwrap();
    backend
}

// OffsetAllocatorStorageBackend_ScanMetaEmpty: a fresh backend scans to an
// empty vector with zero key/byte accounting and unchanged durable inventory.
#[test]
fn cpp_parity_offset_allocator_empty_scan_returns_empty() {
    let root = tempfile::tempdir().unwrap();
    let backend = backend(root.path(), OffsetEvictionPolicy::None, 12_300);

    assert!(backend.scan_meta().unwrap().is_empty());
    assert_eq!(backend.space_usage(), (0, 12_300));
    assert!(!backend.exists("never-written"));
}

// OffsetAllocatorStorageBackend_OutOfSpace: a sustained fresh-key fill under a
// 50-KiB quota succeeds at least once and then terminates on the
// replacement-boundary capacity result (NoAvailableHandle) without eviction.
#[test]
fn cpp_parity_offset_allocator_out_of_space() {
    let root = tempfile::tempdir().unwrap();
    let backend = backend(root.path(), OffsetEvictionPolicy::None, 50_000);

    let mut successes = 0;
    let mut exhausted = false;
    for i in 0..100 {
        match backend.write_object(&format!("key_{i}"), &[b'x'; 1024]) {
            Ok(evicted) => {
                assert!(evicted.is_empty());
                successes += 1;
            }
            Err(StoreError::NoAvailableHandle) => {
                exhausted = true;
                break;
            }
            Err(error) => panic!("unexpected write error: {error}"),
        }
    }
    assert!(successes >= 1);
    assert!(exhausted, "bounded fill must terminate on capacity");
    assert!(successes < 100);
}

// OffsetAllocatorStorageBackend_Eviction_NoEvictionWhenNONE: under policy NONE
// every successful write returns an empty victim list and capacity exhaustion
// surfaces NoAvailableHandle instead of evicting.
#[test]
fn cpp_parity_offset_allocator_no_eviction_when_none() {
    let root = tempfile::tempdir().unwrap();
    let backend = backend(root.path(), OffsetEvictionPolicy::None, 12_300);

    let mut successes = 0;
    for i in 0..4 {
        match backend.write_object(&format!("k{i}"), b"data") {
            Ok(evicted) => {
                assert!(evicted.is_empty());
                successes += 1;
            }
            Err(StoreError::NoAvailableHandle) => {}
            Err(error) => panic!("unexpected write error: {error}"),
        }
    }
    assert!(successes >= 1);
    assert!(successes < 4);
}

// OffsetAllocatorStorageBackend_Concurrency: four writer threads each perform
// ten single-key offloads while reads run concurrently; all 40 writes succeed
// and every key reads back exact bytes after join.
#[test]
fn cpp_parity_offset_allocator_concurrent_writes_and_reads() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(backend(
        root.path(),
        OffsetEvictionPolicy::None,
        4 * 1024 * 1024,
    ));
    let barrier = Arc::new(Barrier::new(8));

    let mut writers = Vec::new();
    for writer in 0..4u8 {
        let backend = Arc::clone(&backend);
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            for i in 0..10 {
                let key = format!("w{writer}-{i}");
                let data = vec![b'a' + writer; i + 1];
                let evicted = backend.write_object(&key, &data).unwrap();
                assert!(evicted.is_empty());
            }
        }));
    }
    let mut readers = Vec::new();
    for _ in 0..4 {
        let backend = Arc::clone(&backend);
        let barrier = Arc::clone(&barrier);
        readers.push(std::thread::spawn(move || {
            barrier.wait();
            for writer in 0..4u8 {
                for i in 0..10 {
                    let key = format!("w{writer}-{i}");
                    if let Ok(data) = backend.read_object(&key) {
                        assert_eq!(data, vec![b'a' + writer; i + 1]);
                    }
                }
            }
        }));
    }
    for writer in writers {
        writer.join().unwrap();
    }
    for reader in readers {
        reader.join().unwrap();
    }
    for writer in 0..4u8 {
        for i in 0..10 {
            let key = format!("w{writer}-{i}");
            assert_eq!(
                backend.read_object(&key).unwrap(),
                vec![b'a' + writer; i + 1]
            );
        }
    }
}

//! Integration tests for prefix_index module.
//!
//! These test the public API of the prefix cache table: hash computation,
//! prefix hash generation, cache hit computation, store/remove events,
//! DP rank registration, and global view.

use mooncake_conductor::prefix_index::{ModelContext, PrefixCacheTable};
use mooncake_conductor::types::{RemovedEvent, StoredEvent};

#[test]
fn test_compute_hash_deterministic() {
    let parent_hash = 42u64;
    let tokens = vec![1i32, 2, 3, 4];

    let hash1 = PrefixCacheTable::compute_hash(parent_hash, &tokens);
    let hash2 = PrefixCacheTable::compute_hash(parent_hash, &tokens);

    assert_eq!(hash1, hash2, "Hash should be deterministic");
}

#[test]
fn test_compute_hash_different_inputs() {
    let hash1 = PrefixCacheTable::compute_hash(0, &[1, 2, 3]);
    let hash2 = PrefixCacheTable::compute_hash(0, &[1, 2, 4]);
    assert_ne!(
        hash1, hash2,
        "Different inputs should produce different hashes"
    );
}

#[test]
fn test_compute_prefix_hash_basic() {
    let table = PrefixCacheTable::new();
    let ctx = ModelContext {
        model_name: "test-model".into(),
        lora_name: "".into(),
        block_size: 2,
        additional_salt: "".into(),
        tenant_id: "default".into(),
    };

    let token_ids = vec![1i32, 2, 3, 4, 5, 6];
    let hashes = table.compute_prefix_hash(&ctx, &token_ids, 0);

    assert_eq!(
        hashes.len(),
        3,
        "6 tokens with block_size=2 should give 3 prefixes"
    );
}

#[test]
fn test_cache_hit_no_context() {
    let table = PrefixCacheTable::new();
    let ctx = ModelContext {
        model_name: "nonexistent".into(),
        lora_name: "".into(),
        block_size: 2,
        additional_salt: "".into(),
        tenant_id: "default".into(),
    };

    let result = table.cache_hit_compute(&ctx, &[1, 2, 3, 4], "instance-1");
    assert_eq!(result.longest_match_tokens, 0);
    assert_eq!(result.gpu, 0);
    assert_eq!(result.cpu, 0);
}

#[test]
fn test_store_and_hit() {
    let table = PrefixCacheTable::new();

    // Store an event to register the hash
    let event = StoredEvent {
        block_hashes: vec![100u64, 200u64],
        block_size: 2,
        model_name: "test-model".into(),
        lora_name: "".into(),
        instance_id: "instance-1".into(),
        parent_block_hash: 0,
        token_ids: vec![1i32, 2, 3, 4],
        medium: "GPU".into(),
    };

    let result = table.process_store_event(&event, 0, "instance-1");
    assert!(
        result.is_ok(),
        "process_store_event failed: {:?}",
        result.err()
    );

    // Now check cache hit
    let ctx = ModelContext {
        model_name: "test-model".into(),
        lora_name: "".into(),
        block_size: 2,
        additional_salt: "".into(),
        tenant_id: "default".into(),
    };
    let hit = table.cache_hit_compute(&ctx, &[1, 2, 3, 4], "instance-1");
    assert!(hit.longest_match_tokens > 0, "Should find a match");
}

#[test]
fn test_remove_event() {
    let table = PrefixCacheTable::new();

    let event = StoredEvent {
        block_hashes: vec![100u64],
        block_size: 2,
        model_name: "test-model".into(),
        lora_name: "".into(),
        instance_id: "instance-1".into(),
        parent_block_hash: 0,
        token_ids: vec![1i32, 2],
        medium: "GPU".into(),
    };

    table.process_store_event(&event, 0, "instance-1").unwrap();

    let remove = RemovedEvent {
        block_hashes: vec![100u64],
        model_name: "test-model".into(),
        lora_name: "".into(),
        instance_id: "instance-1".into(),
        block_size: 2,
        medium: "GPU".into(),
    };

    let result = table.process_remove_event(&remove, 0, "instance-1");
    assert!(result.is_ok());
}

#[test]
fn test_add_dp_size() {
    let table = PrefixCacheTable::new();
    let ctx = ModelContext {
        model_name: "test-model".into(),
        lora_name: "".into(),
        block_size: 2,
        additional_salt: "".into(),
        tenant_id: "default".into(),
    };

    table.add_dp_size(&ctx, "instance-1", 0);
    table.add_dp_size(&ctx, "instance-1", 1);

    let view = table.get_global_view();
    assert!(view.context_count >= 1);
}

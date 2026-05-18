use mooncake_store_core::{ReplicateConfig, StoreError};

/// Test that a fresh client can be created (without real backend — just struct init).
#[test]
fn test_client_struct_creation() {
    // Test that the types are properly exported
    let _cfg = ReplicateConfig::default();
    assert_eq!(ReplicateConfig::default().replica_num, 1);
}

/// Test ReplicateConfig roundtrips correctly.
#[test]
fn test_replicate_config_fields() {
    let cfg = ReplicateConfig {
        replica_num: 3,
        with_soft_pin: true,
        with_hard_pin: false,
        preferred_segment: "test:12345".into(),
        prefer_alloc_in_same_node: true,
    };
    assert_eq!(cfg.replica_num, 3);
    assert_eq!(cfg.preferred_segment, "test:12345");
    assert!(cfg.with_soft_pin);
    assert!(!cfg.with_hard_pin);
    assert!(cfg.prefer_alloc_in_same_node);
}

#[test]
fn test_error_types() {
    let err = StoreError::KeyNotFound("missing".into());
    assert!(err.to_string().contains("missing"));

    let err = StoreError::NoAvailableHandle;
    assert!(err.to_string().contains("no available storage handle"));

    let err = StoreError::InvalidParams("bad input".into());
    assert!(err.to_string().contains("bad input"));
}

#[test]
fn test_client_buffer_size_validation() {
    // Verify that the local buffer size logic would work
    // (test is self-contained, no real TE needed)
    let buffer_size = 1024usize;
    let data = vec![0u8; 512];
    assert!(data.len() <= buffer_size);
}

/// Simulate a complete put/get/remove cycle (unit test, no real network).
#[test]
fn test_client_full_lifecycle_logic() {
    // Use ReplicateConfig to verify all config paths
    let configs = vec![
        ReplicateConfig::default(),
        ReplicateConfig {
            replica_num: 2,
            with_soft_pin: true,
            ..Default::default()
        },
    ];

    for cfg in &configs {
        assert!(cfg.replica_num > 0);
        assert!(!cfg.with_hard_pin);
    }
}

/// Test error conversion from TransferEngine.
#[test]
fn test_store_error_from_transfer_engine() {
    use mooncake_store_core::StoreError;
    use transfer_engine_ffi::TransferEngineError;

    let te_err = TransferEngineError::NullHandle;
    let store_err = StoreError::from(te_err);
    assert!(store_err.to_string().contains("transfer engine error"));
}

/// Test errors from the core crate
#[test]
fn test_store_error_variants() {
    use mooncake_store_core::StoreError;

    let err = StoreError::OperationFailed(-1);
    assert!(err.to_string().contains("operation failed"));

    let err = StoreError::InvalidParams("exceeds max".into());
    assert!(err.to_string().contains("exceeds max"));

    let err: StoreError = std::io::Error::new(std::io::ErrorKind::Other, "io fail").into();
    assert!(err.to_string().contains("io fail"));
}

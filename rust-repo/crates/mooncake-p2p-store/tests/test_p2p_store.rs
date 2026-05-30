use mooncake_p2p_store::{error::P2pStoreError, metadata::*, Buffer, PayloadInfo, MAX_CHUNK_SIZE};

// =========================================================================
// Location
// =========================================================================

#[test]
fn test_location_equality() {
    let a = Location {
        segment_name: "s1".into(),
        offset: 100,
    };
    let b = Location {
        segment_name: "s1".into(),
        offset: 100,
    };
    assert_eq!(a, b);

    let c = Location {
        segment_name: "s1".into(),
        offset: 200,
    };
    assert_ne!(a, c);
}

#[test]
fn test_location_clone() {
    let loc = Location {
        segment_name: "n1:12345".into(),
        offset: 0xDEAD,
    };
    assert_eq!(loc, loc.clone());
}

// =========================================================================
// Shard
// =========================================================================

#[test]
fn test_shard_location_random() {
    let shard = Shard {
        length: 128,
        gold: vec![Location {
            segment_name: "s1".into(),
            offset: 0,
        }],
        replica_list: vec![Location {
            segment_name: "r1".into(),
            offset: 100,
        }],
    };
    let loc = shard.get_random_location();
    assert!(loc.is_some());
    let segment = &loc.unwrap().segment_name;
    assert!(segment == "s1" || segment == "r1");
}

#[test]
fn test_shard_location_random_empty() {
    let shard = Shard {
        length: 0,
        gold: vec![],
        replica_list: vec![],
    };
    assert!(shard.get_random_location().is_none());
}

#[test]
fn test_shard_location_random_only_gold() {
    let locs = vec![Location {
        segment_name: "g".into(),
        offset: 0,
    }];
    let shard = Shard {
        length: 64,
        gold: locs,
        replica_list: vec![],
    };
    let result = shard.get_random_location();
    assert_eq!(result.unwrap().segment_name, "g");
}

#[test]
fn test_shard_location_random_only_replica() {
    let locs = vec![Location {
        segment_name: "r".into(),
        offset: 0,
    }];
    let shard = Shard {
        length: 64,
        gold: vec![],
        replica_list: locs,
    };
    let result = shard.get_random_location();
    assert_eq!(result.unwrap().segment_name, "r");
}

#[test]
fn test_shard_is_empty() {
    let empty = Shard {
        length: 0,
        gold: vec![],
        replica_list: vec![],
    };
    assert!(empty.is_empty());

    let non_empty = Shard {
        length: 128,
        gold: vec![Location {
            segment_name: "x".into(),
            offset: 0,
        }],
        replica_list: vec![],
    };
    assert!(!non_empty.is_empty());

    let non_empty_replica = Shard {
        length: 128,
        gold: vec![],
        replica_list: vec![Location {
            segment_name: "x".into(),
            offset: 0,
        }],
    };
    assert!(!non_empty_replica.is_empty());
}

#[test]
fn test_shard_get_retry_location() {
    let shard = Shard {
        length: 64,
        gold: vec![
            Location {
                segment_name: "g0".into(),
                offset: 0,
            },
            Location {
                segment_name: "g1".into(),
                offset: 64,
            },
        ],
        replica_list: vec![Location {
            segment_name: "r0".into(),
            offset: 128,
        }],
    };

    assert_eq!(shard.get_retry_location(0).unwrap().segment_name, "r0");
    assert_eq!(shard.get_retry_location(1).unwrap().segment_name, "g0");
    assert_eq!(shard.get_retry_location(2).unwrap().segment_name, "g1");
    assert!(shard.get_retry_location(3).is_none());
}

#[test]
fn test_shard_get_location() {
    let shard = Shard {
        length: 64,
        gold: vec![
            Location {
                segment_name: "g0".into(),
                offset: 0,
            },
            Location {
                segment_name: "g1".into(),
                offset: 64,
            },
        ],
        replica_list: vec![Location {
            segment_name: "r0".into(),
            offset: 128,
        }],
    };

    assert_eq!(shard.get_location(1).unwrap().segment_name, "r0");
    assert_eq!(shard.get_location(2).unwrap().segment_name, "g0");
    assert_eq!(shard.get_location(3).unwrap().segment_name, "g1");
    assert!(shard.get_location(4).is_none());
}

// =========================================================================
// Payload
// =========================================================================

#[test]
fn test_payload_serialization() {
    let payload = Payload {
        name: "test".into(),
        size: 1024,
        size_list: vec![512, 512],
        max_shard_size: 256,
        shards: vec![
            Shard {
                length: 256,
                gold: vec![Location {
                    segment_name: "node1:12345".into(),
                    offset: 0x1000,
                }],
                replica_list: vec![],
            },
            Shard {
                length: 256,
                gold: vec![Location {
                    segment_name: "node1:12345".into(),
                    offset: 0x1100,
                }],
                replica_list: vec![],
            },
        ],
    };

    let json = serde_json::to_vec(&payload).unwrap();
    let restored: Payload = serde_json::from_slice(&json).unwrap();

    assert_eq!(restored.name, "test");
    assert_eq!(restored.size, 1024);
    assert_eq!(restored.shards.len(), 2);
    assert_eq!(restored.shards[0].gold[0].segment_name, "node1:12345");
}

#[test]
fn test_payload_is_empty() {
    let mut payload = Payload {
        name: "p".into(),
        size: 0,
        size_list: vec![],
        max_shard_size: 64,
        shards: vec![Shard {
            length: 0,
            gold: vec![],
            replica_list: vec![],
        }],
    };
    assert!(payload.is_empty());

    payload.shards[0].gold.push(Location {
        segment_name: "s".into(),
        offset: 0,
    });
    assert!(!payload.is_empty());
}

#[test]
fn test_payload_empty_no_shards() {
    let payload = Payload {
        name: "empty".into(),
        size: 0,
        size_list: vec![],
        max_shard_size: 64,
        shards: vec![],
    };
    assert!(payload.is_empty());
}

#[test]
fn test_payload_multiple_shards() {
    let payload = Payload {
        name: "multi".into(),
        size: 1024,
        size_list: vec![512, 512],
        max_shard_size: 128,
        shards: (0..8)
            .map(|i| Shard {
                length: 128,
                gold: vec![Location {
                    segment_name: format!("s{}", i),
                    offset: (i * 128) as u64,
                }],
                replica_list: vec![],
            })
            .collect(),
    };
    assert_eq!(payload.shards.len(), 8);
    assert!(!payload.is_empty());
}

// =========================================================================
// PayloadInfo
// =========================================================================

#[test]
fn test_payload_info_creation() {
    let info = PayloadInfo {
        name: "test-payload".into(),
        max_shard_size: 256,
        total_size: 4096,
        size_list: vec![2048, 2048],
    };
    assert_eq!(info.name, "test-payload");
    assert_eq!(info.max_shard_size, 256);
    assert_eq!(info.total_size, 4096);
    assert_eq!(info.size_list, vec![2048, 2048]);
}

#[test]
fn test_payload_info_empty() {
    let info = PayloadInfo {
        name: String::new(),
        max_shard_size: 0,
        total_size: 0,
        size_list: vec![],
    };
    assert!(info.name.is_empty());
    assert_eq!(info.total_size, 0);
    assert!(info.size_list.is_empty());
}

// =========================================================================
// Buffer
// =========================================================================

#[test]
fn test_buffer_creation() {
    let buf = Buffer {
        addr: 0xDEAD_BEEF,
        size: 4096,
    };
    assert_eq!(buf.addr, 0xDEAD_BEEF);
    assert_eq!(buf.size, 4096);
}

#[test]
fn test_buffer_clone() {
    let buf = Buffer {
        addr: 0x1000,
        size: 1024,
    };
    let cloned = buf.clone();
    assert_eq!(cloned.addr, buf.addr);
    assert_eq!(cloned.size, buf.size);
}

#[test]
fn test_buffer_zero_size() {
    let buf = Buffer { addr: 0, size: 0 };
    assert_eq!(buf.size, 0);
}

// =========================================================================
// MAX_CHUNK_SIZE
// =========================================================================

#[test]
fn test_max_chunk_size_value() {
    assert_eq!(MAX_CHUNK_SIZE, 4096 * 1024 * 1024);
    assert_eq!(MAX_CHUNK_SIZE, 4u64 * 1024 * 1024 * 1024);
}

// =========================================================================
// METADATA_KEY_PREFIX
// =========================================================================

#[test]
fn test_metadata_key_prefix() {
    assert_eq!(METADATA_KEY_PREFIX, "mooncake/checkpoint/");
}

// =========================================================================
// P2pStoreError
// =========================================================================

#[test]
fn test_p2p_store_error_display_all() {
    let cases: Vec<(&str, P2pStoreError)> = vec![
        ("invalid arguments", P2pStoreError::InvalidArgument),
        ("payload already opened", P2pStoreError::PayloadOpened),
        ("payload not opened", P2pStoreError::PayloadNotOpened),
        ("payload not found", P2pStoreError::PayloadNotFound),
        ("transfer engine error", P2pStoreError::TransferEngine),
        (
            "metadata store error: etcd timeout",
            P2pStoreError::MetadataError("etcd timeout".into()),
        ),
    ];
    for (expected, err) in &cases {
        assert_eq!(err.to_string(), *expected);
    }
}

#[test]
fn test_p2p_store_error_debug() {
    assert!(format!("{:?}", P2pStoreError::InvalidArgument).contains("InvalidArgument"));
    assert!(format!("{:?}", P2pStoreError::PayloadNotFound).contains("PayloadNotFound"));
}

#[test]
fn test_p2p_store_error_from_serde() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let err: P2pStoreError = json_err.into();
    assert!(err.to_string().contains("serialization error"));
}

#[test]
fn test_p2p_store_error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::Other, "disk full");
    let err: P2pStoreError = io_err.into();
    assert!(err.to_string().contains("disk full"));
}

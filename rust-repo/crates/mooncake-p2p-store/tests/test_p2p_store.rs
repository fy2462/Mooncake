use mooncake_p2p_store::{
    error::P2pStoreError, metadata::*,
};

#[tokio::test]
async fn test_payload_serialization() {
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

    // retry=0 → random (any location)
    // retry=1 → get_retry_location(0) → replica_list[0] = "r0"
    assert_eq!(shard.get_location(1).unwrap().segment_name, "r0");
    // retry=2 → get_retry_location(1) → gold[0] = "g0"
    assert_eq!(shard.get_location(2).unwrap().segment_name, "g0");
    // retry=3 → get_retry_location(2) → gold[1] = "g1"
    assert_eq!(shard.get_location(3).unwrap().segment_name, "g1");
    // retry=4 → get_retry_location(3) → no more → None
    assert!(shard.get_location(4).is_none());
}

#[test]
fn test_error_display() {
    assert_eq!(
        P2pStoreError::InvalidArgument.to_string(),
        "invalid arguments"
    );
    assert_eq!(
        P2pStoreError::PayloadNotFound.to_string(),
        "payload not found"
    );
}

use mooncake_p2p_store::{Location, MetadataStore, Payload, Shard, METADATA_KEY_PREFIX};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{}", prefix, nanos)
}

fn live_etcd_endpoint() -> Option<String> {
    std::env::var("MOONCAKE_P2P_LIVE_ETCD")
        .ok()
        .filter(|value| !value.is_empty())
}

fn sample_payload(name: &str) -> Payload {
    Payload {
        name: name.to_string(),
        size: 8,
        size_list: vec![8],
        max_shard_size: 4,
        shards: vec![
            Shard {
                length: 4,
                gold: vec![Location {
                    segment_name: "node-a:19001".into(),
                    offset: 100,
                }],
                replica_list: Vec::new(),
            },
            Shard {
                length: 4,
                gold: vec![Location {
                    segment_name: "node-a:19001".into(),
                    offset: 104,
                }],
                replica_list: Vec::new(),
            },
        ],
    }
}

#[tokio::test]
async fn test_live_metadata_store_create_update_list_delete() {
    let Some(endpoint) = live_etcd_endpoint() else {
        return;
    };

    let prefix = format!("{}live-test/", METADATA_KEY_PREFIX);
    let name = unique_name("metadata");
    let mut store = MetadataStore::new(&endpoint, &prefix).await.unwrap();
    let mut payload = sample_payload(&name);

    store.create(&name, &payload).await.unwrap();
    assert!(store.create(&name, &payload).await.is_err());

    let (stored, revision) = store.get(&name).await.unwrap();
    assert_eq!(stored.unwrap().name, name);

    payload.shards[0].replica_list.push(Location {
        segment_name: "node-b:19002".into(),
        offset: 200,
    });
    assert!(store.update(&name, &payload, revision).await.unwrap());
    assert!(!store.update(&name, &payload, revision).await.unwrap());

    let listed = store.list("metadata").await.unwrap();
    assert!(listed.iter().any(|payload| payload.name == name));

    let (_, revision) = store.get(&name).await.unwrap();
    for shard in &mut payload.shards {
        shard.gold.clear();
        shard.replica_list.clear();
    }
    assert!(store.update(&name, &payload, revision).await.unwrap());
    assert!(store.get(&name).await.unwrap().0.is_none());
}

#[tokio::test]
#[cfg(feature = "link-native")]
async fn test_live_p2p_store_register_replicate_and_cleanup() {
    use mooncake_p2p_store::P2pStore;

    let Some(endpoint) = live_etcd_endpoint() else {
        return;
    };
    if std::env::var("MOONCAKE_P2P_LIVE_TRANSFER").ok().as_deref() != Some("1") {
        return;
    }

    let name = unique_name("payload");
    let mut source = b"abcdefgh".to_vec();
    let mut destination = vec![0u8; source.len()];
    let source_addr = source.as_mut_ptr() as usize;
    let destination_addr = destination.as_mut_ptr() as usize;

    let server_a = std::env::var("MOONCAKE_P2P_LIVE_SERVER_A")
        .unwrap_or_else(|_| "127.0.0.1:19001".to_string());
    let server_b = std::env::var("MOONCAKE_P2P_LIVE_SERVER_B")
        .unwrap_or_else(|_| "127.0.0.1:19002".to_string());

    let store_a = P2pStore::new(&endpoint, &server_a, "").await.unwrap();
    let store_b = P2pStore::new(&endpoint, &server_b, "").await.unwrap();

    store_a
        .register(
            &name,
            &[source_addr],
            &[source.len() as u64],
            4,
            "cpu:0",
            false,
        )
        .await
        .unwrap();
    let listed = store_a.list("payload").await.unwrap();
    assert!(listed.iter().any(|payload| payload.name == name));

    store_b
        .get_replica(&name, &[destination_addr], &[destination.len() as u64])
        .await
        .unwrap();
    assert_eq!(destination, source);

    store_b.delete_replica(&name).await.unwrap();
    store_a.unregister(&name).await.unwrap();
}

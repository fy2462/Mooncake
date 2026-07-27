#![cfg(feature = "link-native")]

use mooncake_store_client::{ClientBackgroundConfig, ClientHealthStatus, MooncakeClient};
use mooncake_store_core::ReplicateConfig;
use mooncake_store_master::MasterRuntimeConfig;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::proto::master_service_server::MasterServiceServer;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

async fn start_master() -> (String, oneshot::Sender<()>) {
    start_master_with_config(MasterRuntimeConfig::default()).await
}

async fn start_master_with_config(config: MasterRuntimeConfig) -> (String, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = Arc::new(MasterServiceImpl::with_runtime_config(config));
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MasterServiceServer::from_arc(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    (address.to_string(), shutdown_tx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_lifecycle_and_real_transfer_metrics_match_cpp_codes() {
    assert_eq!(
        MooncakeClient::health_status_for(None),
        ClientHealthStatus::NotInitialized
    );
    let (master, shutdown) = start_master().await;
    let mut client = create_tcp_client(&master).await;

    let payload = vec![b'A'; 1024];
    client
        .put(
            "health-metrics",
            &payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: client.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(client.get("health-metrics").await.unwrap(), payload);
    client.health_check().await.unwrap();
    assert_eq!(client.health_status(), ClientHealthStatus::Healthy);
    let metrics = client.serialize_metrics().unwrap();
    for metric in [
        "mooncake_transfer_write_bytes",
        "mooncake_transfer_read_bytes",
        "mooncake_transfer_put_latency_count",
        "mooncake_transfer_get_latency_count",
    ] {
        assert!(metrics.contains(metric), "missing {metric} in {metrics}");
    }
    let summary = client.summary_metrics().unwrap();
    assert!(summary.contains("Put:"), "{summary}");
    assert!(summary.contains("Get:"), "{summary}");

    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        ClientBackgroundConfig {
            health_interval: std::time::Duration::from_millis(20),
            enable_offloading: false,
            enable_promotion: false,
            enable_task_poll: false,
            report_ssd_capacity: false,
            enable_disk_watermark_eviction: false,
            ..Default::default()
        },
    );
    let _ = shutdown.send(());
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let unreachable = slot.lock().await.as_ref().is_some_and(|client| {
                client.health_status() == ClientHealthStatus::MasterUnreachable
            });
            if unreachable {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("background health worker did not observe stopped master");
    background.shutdown().await;

    let mut client = slot.lock().await.take().unwrap();
    let _ = client.tear_down_all().await;
    assert_eq!(client.health_status(), ClientHealthStatus::NotInitialized);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_duplicate_keys_and_mixed_group_ids_preserve_cpp_client_results() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;

    let duplicate_keys = vec!["duplicate-key".to_string(); 2];
    assert_eq!(
        writer
            .batch_put(
                &duplicate_keys,
                &[b"same".as_slice(), b"same".as_slice()],
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: writer.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0, 0]
    );
    assert_eq!(reader.get("duplicate-key").await.unwrap(), b"same");

    let keys = vec![
        "grouped-a".to_string(),
        "ungrouped".to_string(),
        "grouped-b".to_string(),
    ];
    let values = [
        b"value-a".as_slice(),
        b"value-u".as_slice(),
        b"value-b".as_slice(),
    ];
    assert_eq!(
        writer
            .batch_put(
                &keys,
                &values,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: writer.get_hostname(),
                    group_ids: vec!["group-a".to_string(), String::new(), "group-b".to_string()],
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0, 0, 0]
    );
    assert_eq!(
        reader.batch_get(&keys).await.unwrap(),
        values
            .iter()
            .map(|value| Some(value.to_vec()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        writer.batch_remove(&keys, true).await.unwrap(),
        vec![0, 0, 0]
    );

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_remove_preserves_cpp_order_errors_duplicates_and_key_identity() {
    let (master, shutdown) = start_master().await;
    let mut client = create_tcp_client(&master).await;
    let existing = vec![
        "plain-a".to_string(),
        "key with spaces".to_string(),
        "key/with/slashes".to_string(),
        "key:with:colons".to_string(),
        "key#hash".to_string(),
        "key@at".to_string(),
        "unicode_中文".to_string(),
    ];
    let values = existing
        .iter()
        .map(|_| b"value".as_slice())
        .collect::<Vec<_>>();
    assert_eq!(
        client
            .batch_put(
                &existing,
                &values,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0; existing.len()]
    );

    let missing = "missing-key".to_string();
    let mixed = vec![existing[0].clone(), missing.clone(), existing[1].clone()];
    assert_eq!(
        client.batch_remove(&mixed, true).await.unwrap(),
        vec![0, -1, 0]
    );
    assert!(client.remove(&missing, true).await.is_err());
    assert_eq!(
        client.batch_remove(&[missing.clone()], true).await.unwrap(),
        vec![-1]
    );
    assert!(client.batch_remove(&[], true).await.unwrap().is_empty());

    let duplicate = existing[2].clone();
    assert_eq!(
        client
            .batch_remove(&[duplicate.clone(), duplicate.clone(), duplicate], true)
            .await
            .unwrap(),
        vec![0, -1, -1]
    );
    assert_eq!(
        client.batch_remove(&existing[3..], true).await.unwrap(),
        vec![0; existing.len() - 3]
    );
    for key in &existing {
        assert!(!client.exists(key).await.unwrap());
    }

    let single_key = "single-remove";
    let batch_key = "batch-remove";
    for key in [single_key, batch_key] {
        client
            .put(
                key,
                b"value",
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }
    client.remove(single_key, true).await.unwrap();
    assert_eq!(
        client
            .batch_remove(&[batch_key.to_string()], true)
            .await
            .unwrap(),
        vec![0]
    );

    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_remove_thousand_keys_completes_within_cpp_budget() {
    let (master, shutdown) = start_master().await;
    let mut client = create_tcp_client(&master).await;
    let keys = (0..1000)
        .map(|index| format!("large-batch-remove-{index}"))
        .collect::<Vec<_>>();
    let value = vec![7_u8; 1024];
    let values = keys.iter().map(|_| value.as_slice()).collect::<Vec<_>>();
    assert_eq!(
        client
            .batch_put(
                &keys,
                &values,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0; keys.len()]
    );

    let started = std::time::Instant::now();
    assert_eq!(
        client.batch_remove(&keys, true).await.unwrap(),
        vec![0; keys.len()]
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "1000-key batch remove exceeded the C++ five-second budget"
    );

    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_replica_clear_handles_single_multiple_empty_and_missing_keys() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let mut client = create_tcp_client(&master).await;
    let keys = vec![
        "clear-a".to_string(),
        "clear-b".to_string(),
        "clear-c".to_string(),
    ];
    assert_eq!(
        client
            .batch_put(
                &keys,
                &[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()],
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0, 0, 0]
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(
        client
            .batch_replica_clear(&keys[..1], client.client_id(), "", "")
            .await
            .unwrap(),
        keys[..1]
    );
    assert_eq!(
        client
            .batch_replica_clear(&keys[1..], client.client_id(), "", "")
            .await
            .unwrap(),
        keys[1..]
    );
    assert_eq!(client.batch_is_exist(&keys).await.unwrap(), vec![false; 3]);
    assert!(
        client
            .batch_replica_clear(&[], client.client_id(), "", "")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .batch_replica_clear(
                &["missing-a".to_string(), "missing-b".to_string()],
                client.client_id(),
                "",
                "",
            )
            .await
            .unwrap()
            .is_empty()
    );

    drop(client);
    let _ = shutdown.send(());
}

async fn create_tcp_client(master: &str) -> MooncakeClient {
    create_tcp_client_with_segment_size(master, 16 * 1024 * 1024).await
}

async fn create_tcp_client_with_segment_size(master: &str, segment_size: u64) -> MooncakeClient {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_host = probe.local_addr().unwrap().to_string();
    drop(probe);
    MooncakeClient::create(
        master,
        "P2PHANDSHAKE",
        &local_host,
        "tcp",
        "",
        segment_size,
        8 * 1024 * 1024,
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_owned_segment_mount_routes_data_and_unmounts_by_canonical_id() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client_with_segment_size(&master, 0).await;
    let mut reader = create_tcp_client(&master).await;
    let segment_ids = writer
        .allocate_and_mount_segments(16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(segment_ids.len(), 1);

    writer
        .put(
            "dynamic-segment-object",
            b"dynamic-segment-bytes",
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        reader.get("dynamic-segment-object").await.unwrap(),
        b"dynamic-segment-bytes"
    );

    writer
        .unmount_and_free_segments(&segment_ids, 0)
        .await
        .unwrap();
    assert!(
        writer
            .get_segments_detail()
            .await
            .unwrap()
            .iter()
            .all(|segment| segment.segment_id != segment_ids[0])
    );

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_tcp_clients_roundtrip_exact_bytes_through_one_master() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;
    let payload = b"rust-client-inproc-e2e";

    writer
        .put(
            "cross-client-roundtrip",
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.get("cross-client-roundtrip").await.unwrap(), payload);

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_integration_basic_remove_batch_upsert_and_large_payload_parity() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;
    let config = ReplicateConfig {
        replica_num: 1,
        preferred_segment: writer.get_hostname(),
        ..Default::default()
    };

    let large = vec![0x5a; 5 * 1024 * 1024];
    writer
        .put("large-object", &large, Some(config.clone()))
        .await
        .unwrap();
    assert_eq!(reader.get("large-object").await.unwrap(), large);

    let keys = vec!["batch-a".to_string(), "batch-b".to_string()];
    let first = b"first".as_slice();
    let second = b"second".as_slice();
    assert_eq!(
        writer
            .batch_put(&keys, &[first, second], Some(config.clone()))
            .await
            .unwrap(),
        vec![0, 0]
    );
    assert_eq!(
        reader.batch_is_exist(&keys).await.unwrap(),
        vec![true, true]
    );
    assert_eq!(
        reader.batch_get(&keys).await.unwrap(),
        vec![Some(first.to_vec()), Some(second.to_vec())]
    );

    writer
        .upsert("upsert-new", b"created", Some(config.clone()))
        .await
        .unwrap();
    assert_eq!(reader.get("upsert-new").await.unwrap(), b"created");
    writer
        .upsert("batch-a", b"other", Some(config.clone()))
        .await
        .unwrap();
    assert_eq!(reader.get("batch-a").await.unwrap(), b"other");
    let replacement = b"different-sized-replacement".as_slice();
    assert_eq!(
        writer
            .batch_upsert(&keys, &[replacement, b"B"], Some(config))
            .await
            .unwrap(),
        vec![0, 0]
    );
    assert_eq!(reader.get("batch-a").await.unwrap(), replacement);
    assert_eq!(reader.get("batch-b").await.unwrap(), b"B");

    let addresses = reader
        .batch_query_ip(&[writer.client_id(), reader.client_id()])
        .await
        .unwrap();
    assert_eq!(addresses.len(), 2);
    writer.remove("batch-a", true).await.unwrap();
    assert_eq!(
        reader.batch_is_exist(&keys).await.unwrap(),
        vec![false, true]
    );

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_tcp_clients_copy_and_move_preserve_bytes_across_distinct_segments() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        put_start_release_timeout: std::time::Duration::from_millis(20),
        reaper_interval: std::time::Duration::from_millis(5),
        lease_ttl: std::time::Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let mut source = create_tcp_client(&master).await;
    let mut copy_target = create_tcp_client(&master).await;
    let mut move_target = create_tcp_client(&master).await;
    let source_name = source.get_hostname();
    let copy_target_name = copy_target.get_hostname();
    let move_target_name = move_target.get_hostname();
    let payload = b"three-node-copy-move-payload";

    source
        .put(
            "copy-move-object",
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: source_name.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let filler = vec![0x33; 1024 * 1024];
    let mut filler_keys = Vec::new();
    for index in 0..32 {
        let key = format!("copy-target-filler-{index}");
        if copy_target
            .put(
                &key,
                &filler,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: copy_target_name.clone(),
                    ..Default::default()
                }),
            )
            .await
            .is_err()
        {
            break;
        }
        filler_keys.push(key);
    }
    assert!(!filler_keys.is_empty());
    assert!(
        source
            .copy(
                "copy-move-object",
                &source_name,
                std::slice::from_ref(&copy_target_name),
            )
            .await
            .is_err()
    );
    let used_before_remove = copy_target
        .get_segments_detail()
        .await
        .unwrap()
        .into_iter()
        .find(|segment| segment.segment_name == copy_target_name)
        .unwrap()
        .allocator_used_bytes;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(
        copy_target
            .batch_replica_clear(&filler_keys, copy_target.client_id(), "", "",)
            .await
            .unwrap(),
        filler_keys
    );
    let release_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let used = copy_target
            .get_segments_detail()
            .await
            .unwrap()
            .into_iter()
            .find(|segment| segment.segment_name == copy_target_name)
            .unwrap()
            .allocator_used_bytes;
        if used < used_before_remove {
            break;
        }
        assert!(
            tokio::time::Instant::now() < release_deadline,
            "removed target allocation was not released before retry"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    source
        .copy(
            "copy-move-object",
            &source_name,
            std::slice::from_ref(&copy_target_name),
        )
        .await
        .unwrap();
    assert_eq!(copy_target.get("copy-move-object").await.unwrap(), payload);

    source
        .move_object("copy-move-object", &source_name, &move_target_name)
        .await
        .unwrap();
    assert_eq!(copy_target.get("copy-move-object").await.unwrap(), payload);
    assert_eq!(move_target.get("copy-move-object").await.unwrap(), payload);

    drop(source);
    drop(copy_target);
    drop(move_target);
    let _ = shutdown.send(());
}

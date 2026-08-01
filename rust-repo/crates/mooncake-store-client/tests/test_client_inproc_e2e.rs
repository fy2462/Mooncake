#![cfg(feature = "link-native")]

use mooncake_store_client::{
    ClientBackgroundConfig, ClientHealthStatus, LocalHotCache, LocalStorageBackend,
    LocalStorageConfig, MooncakeClient, proto,
};
use mooncake_store_core::{ReplicaType, ReplicateConfig, StoreError};
use mooncake_store_master::MasterRuntimeConfig;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::allocator::{
    AllocationStrategy, CACHELIB_SLAB_SIZE, MemoryAllocatorKind,
};
use mooncake_store_master::http_metadata::serve_metadata_listener_with_service_gate;
use mooncake_store_master::proto::master_service_server::MasterServiceServer;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

async fn start_master() -> (String, oneshot::Sender<()>) {
    start_master_with_config(MasterRuntimeConfig::default()).await
}

async fn start_master_with_config(config: MasterRuntimeConfig) -> (String, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, _server) = spawn_master(listener, config);
    (address.to_string(), shutdown_tx)
}

fn run_cxl_subprocess(mode: &str, cxl_size: u64) {
    let device = tempfile::NamedTempFile::new().unwrap();
    device.as_file().set_len(cxl_size).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("cxl_client_integration_subprocess_helper")
        .arg("--exact")
        .arg("--nocapture")
        .env("MOONCAKE_CXL_PARITY_MODE", mode)
        .env("MC_CXL_DEV_PATH", device.path())
        .env("MC_CXL_DEV_SIZE", cxl_size.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "CXL {mode} helper failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cxl_client_integration_subprocess_helper() {
    let Ok(mode) = std::env::var("MOONCAKE_CXL_PARITY_MODE") else {
        return;
    };
    let cxl_path = std::env::var("MC_CXL_DEV_PATH").unwrap();
    let cxl_size = std::env::var("MC_CXL_DEV_SIZE")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let config = MasterRuntimeConfig {
        allocation_strategy: AllocationStrategy::Cxl,
        memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
        enable_cxl: true,
        cxl_path,
        cxl_size,
        eviction_interval: std::time::Duration::from_millis(5),
        eviction_high_watermark_ratio: 0.25,
        eviction_ratio: 0.25,
        soft_pin_ttl: std::time::Duration::ZERO,
        lease_ttl: std::time::Duration::from_millis(20),
        allow_evict_soft_pinned_objects: true,
        ..Default::default()
    };
    let (master, shutdown) = start_master_with_config(config).await;
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_host = probe.local_addr().unwrap().to_string();
    drop(probe);
    let mut client = MooncakeClient::create(
        &master,
        "P2PHANDSHAKE",
        &local_host,
        "cxl",
        "",
        0,
        8 * 1024 * 1024,
    )
    .await
    .unwrap();
    let replication = ReplicateConfig {
        replica_num: 1,
        ..Default::default()
    };

    match mode.as_str() {
        "basic" => {
            let payload = b"Hello, World!";
            client
                .put("test_key", payload, Some(replication.clone()))
                .await
                .unwrap();
            assert_eq!(client.get("test_key").await.unwrap(), payload);
            client
                .put("test_key", payload, Some(replication))
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            client.remove("test_key", false).await.unwrap();
            assert!(!client.exists("test_key").await.unwrap());
        }
        "batch" => {
            let keys = (0..10)
                .map(|index| format!("test_key_batch_put_{index}"))
                .collect::<Vec<_>>();
            let values = (0..10)
                .map(|index| format!("test_data_{index}").into_bytes())
                .collect::<Vec<_>>();
            let borrowed = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
            assert_eq!(
                client
                    .batch_put(&keys, &borrowed, Some(replication))
                    .await
                    .unwrap(),
                vec![0; 10]
            );
            for (key, value) in keys.iter().zip(&values) {
                assert_eq!(client.get(key).await.unwrap(), *value);
            }
            assert_eq!(
                client.batch_get(&keys).await.unwrap(),
                values.into_iter().map(Some).collect::<Vec<_>>()
            );
        }
        "evict" => {
            let payload = vec![b'T'; 1024 * 1024];
            let mut keys = Vec::new();
            for index in 0..48 {
                let key = format!("evict_key_{index}");
                client
                    .put(&key, &payload, Some(replication.clone()))
                    .await
                    .unwrap();
                keys.push(key);
                if index % 4 == 3 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    for key in &keys {
                        if !client.exists(key).await.unwrap() {
                            return;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("CXL pressure did not produce a client-visible eviction");
        }
        other => panic!("unknown CXL parity mode {other}"),
    }

    client.tear_down_all().await.unwrap();
    let _ = shutdown.send(());
}

#[test]
fn cpp_parity_cxl_client_integration_test_cpp_clientintegrationtestcxl_basicputgetoperations_88ca6678()
 {
    run_cxl_subprocess("basic", CACHELIB_SLAB_SIZE * 4);
}

#[test]
fn cpp_parity_cxl_client_integration_test_cpp_clientintegrationtestcxl_batchputgetoperations_0423569f()
 {
    run_cxl_subprocess("batch", CACHELIB_SLAB_SIZE * 4);
}

#[test]
fn cpp_parity_cxl_client_integration_test_cpp_clientintegrationtestcxl_evictoperation_a8cef4ce() {
    run_cxl_subprocess("evict", CACHELIB_SLAB_SIZE * 4);
}

fn spawn_master(
    listener: tokio::net::TcpListener,
    config: MasterRuntimeConfig,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = Arc::new(MasterServiceImpl::with_runtime_config(config));
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MasterServiceServer::from_arc(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    (shutdown_tx, server)
}

async fn start_master_with_http_metadata() -> (
    String,
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_address = grpc_listener.local_addr().unwrap();
    let metadata_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metadata_address = metadata_listener.local_addr().unwrap();
    let service = Arc::new(MasterServiceImpl::with_runtime_config(
        MasterRuntimeConfig::default(),
    ));
    service
        .metadata_state()
        .set_master_addr(format!("http://{grpc_address}"))
        .await;
    let metadata_state = service.metadata_state();
    let metadata_service = Arc::clone(&service);
    let metadata_server = tokio::spawn(async move {
        serve_metadata_listener_with_service_gate(
            metadata_listener,
            metadata_state,
            metadata_service,
        )
        .await
        .unwrap();
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MasterServiceServer::from_arc(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(grpc_listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    (
        grpc_address.to_string(),
        format!("http://{metadata_address}/metadata"),
        shutdown_tx,
        metadata_server,
    )
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
    client
        .put(
            "after-health-remount",
            &payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: client.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(client.get("after-health-remount").await.unwrap(), payload);
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

fn health_only_background_config() -> ClientBackgroundConfig {
    ClientBackgroundConfig {
        health_interval: std::time::Duration::from_millis(20),
        enable_offloading: false,
        enable_promotion: false,
        enable_task_poll: false,
        report_ssd_capacity: false,
        enable_disk_watermark_eviction: false,
        ..Default::default()
    }
}

async fn wait_for_healthy(slot: &Arc<tokio::sync::Mutex<Option<MooncakeClient>>>) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if slot
                .lock()
                .await
                .as_ref()
                .is_some_and(|client| client.health_status() == ClientHealthStatus::Healthy)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("storage heartbeat did not become healthy");
}

fn assert_prometheus_counter(metrics: &str, sample: &str, expected: u64) {
    let line = metrics
        .lines()
        .find(|line| line.starts_with(sample))
        .unwrap_or_else(|| {
            let strategy_lines = metrics
                .lines()
                .filter(|line| line.contains("strategy"))
                .collect::<Vec<_>>();
            panic!("missing Prometheus sample {sample:?}; strategy lines: {strategy_lines:?}")
        });
    assert_eq!(line, format!("{sample} {expected}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_local_and_remote_reads_report_the_actual_transfer_strategy() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;
    let payload = b"locality-strategy-exact-bytes";
    writer
        .put(
            "locality-strategy",
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    assert_eq!(writer.get("locality-strategy").await.unwrap(), payload);
    assert_prometheus_counter(
        &writer.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"local_memcpy\"}",
        1,
    );

    assert_eq!(reader.get("locality-strategy").await.unwrap(), payload);
    assert_prometheus_counter(
        &reader.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"transfer_engine\"}",
        1,
    );

    drop(reader);
    drop(writer);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_cache_hit_bypasses_missing_master_metadata_in_both_handshake_modes() {
    let (master, shutdown) = start_master().await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    cache.put("p2p-hot-cache-only", b"p2p-hot-cache-exact-bytes");
    let mut client = create_tcp_client(&master).await.with_hot_cache(cache);

    assert_eq!(
        client.get("p2p-hot-cache-only").await.unwrap(),
        b"p2p-hot-cache-exact-bytes"
    );
    assert_prometheus_counter(
        &client.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"local_memcpy\"}",
        1,
    );

    drop(client);
    let _ = shutdown.send(());

    let (master, metadata_url, shutdown, metadata_server) = start_master_with_http_metadata().await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    cache.put("metadata-hot-cache-only", b"metadata-hot-cache-exact-bytes");
    let mut client = create_tcp_client_with_metadata(&master, &metadata_url, 16 * 1024 * 1024)
        .await
        .with_hot_cache(cache);
    assert_eq!(
        client.get("metadata-hot-cache-only").await.unwrap(),
        b"metadata-hot-cache-exact-bytes"
    );
    assert_prometheus_counter(
        &client.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"local_memcpy\"}",
        1,
    );

    drop(client);
    let _ = shutdown.send(());
    metadata_server.abort();
    let _ = metadata_server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_segment_heartbeat_activates_after_first_memory_mount() {
    let (master, shutdown) = start_master().await;
    let client = create_tcp_client_with_segment_size(&master, 0).await;
    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        health_only_background_config(),
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        slot.lock().await.as_ref().unwrap().health_status(),
        ClientHealthStatus::MasterUnreachable,
        "compute-only clients must not send storage heartbeat"
    );
    slot.lock()
        .await
        .as_mut()
        .unwrap()
        .allocate_and_mount_segments(16 * 1024 * 1024)
        .await
        .unwrap();
    wait_for_healthy(&slot).await;

    background.shutdown().await;
    drop(slot.lock().await.take());
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_segment_heartbeat_activates_after_local_disk_mount() {
    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        enable_offload: true,
        ..Default::default()
    })
    .await;
    let backend = Arc::new(LocalStorageBackend::new_ephemeral(LocalStorageConfig {
        root_dir: root.path().join("client"),
        fsdir: "local-disk".into(),
        enable_eviction: false,
        quota_bytes: 16 * 1024 * 1024,
    }));
    backend.init().unwrap();
    let client = create_tcp_client_with_segment_size(&master, 0)
        .await
        .with_local_storage_backend(backend);
    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        health_only_background_config(),
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        slot.lock().await.as_ref().unwrap().health_status(),
        ClientHealthStatus::MasterUnreachable
    );
    slot.lock()
        .await
        .as_mut()
        .unwrap()
        .mount_local_disk_segment(false)
        .await
        .unwrap();
    wait_for_healthy(&slot).await;

    background.shutdown().await;
    drop(slot.lock().await.take());
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_ha_master_restart_remounts_owned_segments_on_the_same_endpoint() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, server) = spawn_master(listener, MasterRuntimeConfig::default());
    let client = create_tcp_client(&address.to_string()).await;
    let hostname = client.get_hostname();
    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        health_only_background_config(),
    );
    wait_for_healthy(&slot).await;

    shutdown.send(()).unwrap();
    server.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if slot.lock().await.as_ref().is_some_and(|client| {
                client.health_status() == ClientHealthStatus::MasterUnreachable
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("client did not observe the stopped master");

    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (shutdown, server) = spawn_master(listener, MasterRuntimeConfig::default());
    wait_for_healthy(&slot).await;
    let payload = vec![0x5a; 4096];
    {
        let mut client = slot.lock().await;
        let client = client.as_mut().unwrap();
        client
            .put(
                "after-master-restart",
                &payload,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: hostname,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        assert_eq!(client.get("after-master-restart").await.unwrap(), payload);
    }

    background.shutdown().await;
    drop(slot.lock().await.take());
    shutdown.send(()).unwrap();
    server.await.unwrap();
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

async fn create_tcp_client_with_metadata(
    master: &str,
    metadata_url: &str,
    segment_size: u64,
) -> MooncakeClient {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_host = probe.local_addr().unwrap().to_string();
    drop(probe);
    MooncakeClient::create(
        master,
        metadata_url,
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
async fn http_metadata_local_and_remote_endpoints_choose_exact_transfer_strategy() {
    let (master, metadata_url, shutdown, metadata_server) = start_master_with_http_metadata().await;
    let mut writer =
        create_tcp_client_with_metadata(&master, &metadata_url, 16 * 1024 * 1024).await;
    let mut reader =
        create_tcp_client_with_metadata(&master, &metadata_url, 16 * 1024 * 1024).await;
    let payload = b"http-metadata-locality-exact-bytes";
    writer
        .put(
            "http-metadata-locality",
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    assert_eq!(writer.get("http-metadata-locality").await.unwrap(), payload);
    assert_prometheus_counter(
        &writer.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"local_memcpy\"}",
        1,
    );
    assert_eq!(reader.get("http-metadata-locality").await.unwrap(), payload);
    assert_prometheus_counter(
        &reader.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"transfer_engine\"}",
        1,
    );

    drop(reader);
    drop(writer);
    let _ = shutdown.send(());
    metadata_server.abort();
    let _ = metadata_server.await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permanently_full_copy_target_reaches_failed_task_after_bounded_retries() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        max_task_retry_attempts: 2,
        put_start_release_timeout: std::time::Duration::ZERO,
        ..Default::default()
    })
    .await;
    let mut source = create_tcp_client_with_segment_size(&master, 64 * 1024 * 1024).await;
    let target = create_tcp_client(&master).await;
    let target_name = target.get_hostname();
    let filler = vec![0x41; 1024 * 1024];
    for index in 0..20 {
        source
            .put(
                &format!("terminal-task-filler-{index}"),
                &filler,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: target_name.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }

    source
        .put(
            "terminal-copy-source",
            &filler,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: source.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let task_id = source
        .create_copy_task("terminal-copy-source", std::slice::from_ref(&target_name))
        .await
        .unwrap();
    let mut assignments = source.fetch_tasks(1).await.unwrap();
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].max_retry_attempts, 2);
    assert!(matches!(
        source
            .execute_task_assignment(assignments.remove(0))
            .await
            .unwrap_err(),
        StoreError::NoAvailableHandle
    ));

    let terminal = source.query_task(task_id).await.unwrap();
    assert_eq!(terminal.status, proto::TaskStatus::TaskFailed as i32);
    assert!(terminal.message.contains("max retries reached: 2"));

    drop(target);
    drop(source);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_disk_fifo_eviction_removes_only_the_oldest_master_replica() {
    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        enable_disk_eviction: true,
        quota_bytes: 3 * 1024,
        ..Default::default()
    })
    .await;
    let mut client = create_tcp_client(&master).await;
    let segment = client.get_hostname();
    let keys = (0..4)
        .map(|index| format!("global-disk-fifo-{index}"))
        .collect::<Vec<_>>();

    for (index, key) in keys.iter().enumerate() {
        client
            .put(
                key,
                &vec![b'A' + index as u8; 1024],
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: segment.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }

    let mut disk_presence = Vec::new();
    for key in &keys {
        disk_presence.push(
            client
                .query(key)
                .await
                .unwrap()
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::Disk),
        );
    }
    assert_eq!(disk_presence, [false, true, true, true]);
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            client.get(key).await.unwrap(),
            vec![b'A' + index as u8; 1024]
        );
    }

    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn seeded_two_client_large_object_delete_put_get_never_returns_stale_bytes() {
    const SEGMENT_SIZE: u64 = 32 * 1024 * 1024;
    const VALUE_SIZE: usize = 3 * 1024 * 1024;
    const KEY_COUNT: usize = 42;
    const ROUNDS: usize = 4;
    const SEED: u64 = 0x4d4f_4f4e_4341_4b45;

    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        put_start_release_timeout: std::time::Duration::ZERO,
        reaper_interval: std::time::Duration::from_millis(5),
        eviction_interval: std::time::Duration::from_millis(5),
        lease_ttl: std::time::Duration::from_millis(5),
        eviction_high_watermark_ratio: 0.90,
        eviction_ratio: 0.20,
        ..Default::default()
    })
    .await;
    let mut clients = vec![
        create_tcp_client_with_segment_size(&master, SEGMENT_SIZE).await,
        create_tcp_client_with_segment_size(&master, SEGMENT_SIZE).await,
    ];
    let mut rng = SEED;
    let mut selected = 0_usize;
    let mut successful_reads = 0_usize;

    for _ in 0..ROUNDS {
        for key_index in 0..KEY_COUNT {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            if (rng >> 32) % 100 < 50 {
                continue;
            }
            selected += 1;
            let key = format!("seeded-pressure-{key_index}");
            let value = vec![(key_index as u8).wrapping_mul(17); VALUE_SIZE];

            rng = rng.rotate_left(13);
            let delete_client = ((rng >> 32) as usize) % clients.len();
            let _ = clients[delete_client].remove(&key, false).await;
            rng = rng.rotate_left(17);
            let put_client = ((rng >> 32) as usize) % clients.len();
            let _ = clients[put_client].put(&key, &value, None).await;
            rng = rng.rotate_left(23);
            let get_client = ((rng >> 32) as usize) % clients.len();
            if let Ok(actual) = clients[get_client].get(&key).await {
                assert_eq!(actual, value, "seed={SEED:#x}, key={key}");
                successful_reads += 1;
            }
        }
    }

    assert!(selected >= KEY_COUNT, "seed did not create enough pressure");
    assert!(
        successful_reads > 0,
        "pressure run produced no successful reads"
    );
    drop(clients);
    let _ = shutdown.send(());
}

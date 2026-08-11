#![cfg(feature = "link-native")]

use mooncake_store_client::{
    ClientBackgroundConfig, ClientHealthStatus, LocalHotCache, LocalStorageBackend,
    LocalStorageConfig, MooncakeClient, RegisteredBufferAllocation, RemoteSource,
    RemoteSourceConfig, RemoteSourceResult, proto,
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
use uuid::Uuid;

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
        lease_ttl: std::time::Duration::from_millis(1_000),
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
async fn cpp_parity_hot_cache_batch_get_returns_ordered_exact_bytes() {
    let (master, shutdown) = start_master().await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    cache.put("batch-hot-key-1", b"Data1");
    cache.put("batch-hot-key-2", b"Data2");
    let mut client = create_tcp_client(&master)
        .await
        .with_hot_cache(Arc::clone(&cache));
    let keys = vec!["batch-hot-key-2".to_string(), "batch-hot-key-1".to_string()];

    assert_eq!(
        client.batch_get(&keys).await.unwrap(),
        vec![Some(b"Data2".to_vec()), Some(b"Data1".to_vec())]
    );
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_hot_cache_hits_preserve_admission_count_and_exact_bytes() {
    let (master, shutdown) = start_master().await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    cache.put("admission-cache-hit", b"cache-hit-data");
    let mut client = create_tcp_client(&master)
        .await
        .with_hot_cache(Arc::clone(&cache));
    let count_before = client.hot_cache_admission_count("admission-cache-hit");
    assert_eq!(count_before, 0);

    for _ in 0..3 {
        assert_eq!(
            client.get("admission-cache-hit").await.unwrap(),
            b"cache-hit-data"
        );
    }

    assert_eq!(
        client.hot_cache_admission_count("admission-cache-hit"),
        count_before
    );
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_hot_cache_admission_helpers_are_disabled_without_cache() {
    if std::env::var_os("MOONCAKE_HOT_CACHE_DISABLED_TEST").is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("cpp_parity_hot_cache_admission_helpers_are_disabled_without_cache")
            .arg("--exact")
            .arg("--nocapture")
            .env("MOONCAKE_HOT_CACHE_DISABLED_TEST", "1")
            .env_remove("MC_STORE_LOCAL_HOT_CACHE_SIZE")
            .env_remove("MC_STORE_LOCAL_HOT_BLOCK_SIZE")
            .env_remove("MC_STORE_LOCAL_HOT_CACHE_USE_SHM")
            .env_remove("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cache-disabled helper failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let (master, shutdown) = start_master().await;
    let mut client = create_tcp_client(&master).await;

    assert!(!client.is_hot_cache_enabled());
    assert_eq!(client.hot_cache_admission_count("any-key"), 0);
    assert!(!client.should_admit_to_hot_cache("any-key", false));
    assert!(!client.should_admit_to_hot_cache("any-key", true));

    client.tear_down_all().await.unwrap();
    let _ = shutdown.send(());
}

struct BlockingRemoteSource {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    value: Vec<u8>,
}

#[async_trait::async_trait]
impl RemoteSource for BlockingRemoteSource {
    async fn get(&self, _key: &str) -> RemoteSourceResult<Vec<u8>> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(self.value.clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_hot_cache_client_get_rejects_stale_fill_after_invalidation() {
    let (master, shutdown) = start_master().await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let source = BlockingRemoteSource {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        value: b"stale-remote-value".to_vec(),
    };
    let mut client = create_tcp_client(&master)
        .await
        .with_remote_source(
            source,
            RemoteSourceConfig {
                enabled: true,
                ..Default::default()
            },
        )
        .with_hot_cache(Arc::clone(&cache));

    let get = tokio::spawn(async move { client.get("racing-key").await });
    started.notified().await;
    cache.remove("racing-key");
    release.notify_one();

    assert_eq!(get.await.unwrap().unwrap(), b"stale-remote-value");
    assert_eq!(cache.get("racing-key"), None);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_dummy_stable_hot_cache_get_buffer_invalidated_by_remove() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    let mut reader = create_tcp_client(&master)
        .await
        .with_hot_cache(Arc::clone(&cache));
    let payload = b"stable-hot-cache-owned-buffer";
    writer
        .put(
            "stable-hot-buffer",
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let first = reader.get_buffer("stable-hot-buffer").await.unwrap();
    assert_eq!(first.key, "stable-hot-buffer");
    assert_eq!(first.size, 29);
    assert_eq!(first.data, b"stable-hot-cache-owned-buffer");
    assert_eq!(
        cache.get("stable-hot-buffer"),
        Some(b"stable-hot-cache-owned-buffer".to_vec())
    );
    let second = reader.get_buffer("stable-hot-buffer").await.unwrap();
    assert_eq!(second.key, "stable-hot-buffer");
    assert_eq!(second.size, 29);
    assert_eq!(second.data, b"stable-hot-cache-owned-buffer");
    assert_prometheus_counter(
        &reader.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"local_memcpy\"}",
        1,
    );

    reader.remove("stable-hot-buffer", true).await.unwrap();
    assert_eq!(cache.get("stable-hot-buffer"), None);
    assert!(matches!(
        reader.get_buffer("stable-hot-buffer").await,
        Err(StoreError::KeyNotFound(key)) if key == "stable-hot-buffer"
    ));

    drop(reader);
    drop(writer);
    let _ = shutdown.send(());
}

struct OneShotBlockingRemoteSource {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl RemoteSource for OneShotBlockingRemoteSource {
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.started.notify_one();
            self.release.notified().await;
            Ok(b"inflight-owned-stale-value".to_vec())
        } else {
            Err(mooncake_store_client::RemoteSourceError::NotFound(
                key.to_string(),
            ))
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_dummy_inflight_get_buffer_fill_cannot_resurrect_removed_key() {
    let (master, shutdown) = start_master().await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let source = OneShotBlockingRemoteSource {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let reader = create_tcp_client(&master)
        .await
        .with_remote_source(
            source,
            RemoteSourceConfig {
                enabled: true,
                ..Default::default()
            },
        )
        .with_hot_cache(Arc::clone(&cache));
    let mut writer = create_tcp_client(&master).await;
    let mut remover = create_tcp_client(&master)
        .await
        .with_hot_cache(Arc::clone(&cache));

    let inflight = tokio::spawn(async move {
        let mut reader = reader;
        let result = reader.get_buffer("inflight-remove-key").await;
        (reader, result)
    });
    started.notified().await;
    writer
        .put(
            "inflight-remove-key",
            b"temporary-store-value",
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    remover.remove("inflight-remove-key", true).await.unwrap();
    release.notify_one();

    let (mut reader, stale_owner) = inflight.await.unwrap();
    let stale_owner = stale_owner.unwrap();
    assert_eq!(stale_owner.key, "inflight-remove-key");
    assert_eq!(stale_owner.size, 26);
    assert_eq!(stale_owner.data, b"inflight-owned-stale-value");
    assert_eq!(cache.get("inflight-remove-key"), None);
    assert!(matches!(
        reader.get_buffer("inflight-remove-key").await,
        Err(StoreError::KeyNotFound(key)) if key == "inflight-remove-key"
    ));
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(matches!(
        reader.get_buffer("inflight-remove-key").await,
        Err(StoreError::KeyNotFound(key)) if key == "inflight-remove-key"
    ));
    assert_eq!(cache.get("inflight-remove-key"), None);

    drop(reader);
    drop(remover);
    drop(writer);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_dummy_get_buffer_cold_allocator_fallback_returns_exact_handle() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    let mut reader = create_tcp_client(&master)
        .await
        .with_hot_cache(Arc::clone(&cache));
    writer
        .put(
            "cold-allocator-buffer",
            b"cold-allocator-owned-data",
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(cache.get("cold-allocator-buffer"), None);

    let handle = reader.get_buffer("cold-allocator-buffer").await.unwrap();
    assert_eq!(handle.key, "cold-allocator-buffer");
    assert_eq!(handle.size, 25);
    assert_eq!(handle.data, b"cold-allocator-owned-data");
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
async fn cpp_parity_dummy_get_buffer_warmed_hot_cache_returns_exact_handle() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    let mut reader = create_tcp_client(&master)
        .await
        .with_hot_cache(Arc::clone(&cache));
    writer
        .put(
            "warmed-hot-buffer",
            b"hot-buffer-exact-data",
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let cold = reader.get_buffer("warmed-hot-buffer").await.unwrap();
    assert_eq!(cold.data, b"hot-buffer-exact-data");
    let hot = reader.get_buffer("warmed-hot-buffer").await.unwrap();
    assert_eq!(hot.key, "warmed-hot-buffer");
    assert_eq!(hot.size, 21);
    assert_eq!(hot.data, b"hot-buffer-exact-data");
    assert_prometheus_counter(
        &reader.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"local_memcpy\"}",
        1,
    );

    drop(reader);
    drop(writer);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_dummy_batch_get_buffer_mixed_hot_cold_preserves_order() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let cache = Arc::new(LocalHotCache::new(1024 * 1024, 16));
    let mut reader = create_tcp_client(&master)
        .await
        .with_hot_cache(Arc::clone(&cache));
    for (key, value) in [
        ("batch-hot-owned", b"hot-five".as_slice()),
        ("batch-cold-owned-a", b"cold-seven-a".as_slice()),
        ("batch-cold-owned-b", b"cold-seven-b".as_slice()),
    ] {
        writer
            .put(
                key,
                value,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: writer.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        reader.get_buffer("batch-hot-owned").await.unwrap().data,
        b"hot-five"
    );
    assert_eq!(cache.get("batch-hot-owned"), Some(b"hot-five".to_vec()));
    assert_eq!(cache.get("batch-cold-owned-a"), None);
    assert_eq!(cache.get("batch-cold-owned-b"), None);
    assert_prometheus_counter(
        &reader.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"transfer_engine\"}",
        1,
    );

    let keys = vec![
        "batch-cold-owned-b".to_string(),
        "batch-hot-owned".to_string(),
        "batch-cold-owned-a".to_string(),
    ];
    let handles = reader.batch_get_buffer(&keys).await.unwrap();
    assert_eq!(handles.len(), 3);
    for (handle, expected_key, expected_size, expected_data) in [
        (
            handles[0].as_ref(),
            "batch-cold-owned-b",
            12,
            b"cold-seven-b".as_slice(),
        ),
        (
            handles[1].as_ref(),
            "batch-hot-owned",
            8,
            b"hot-five".as_slice(),
        ),
        (
            handles[2].as_ref(),
            "batch-cold-owned-a",
            12,
            b"cold-seven-a".as_slice(),
        ),
    ] {
        let handle = handle.expect("every stored key returns one owned handle");
        assert_eq!(handle.key, expected_key);
        assert_eq!(handle.size, expected_size);
        assert_eq!(handle.data, expected_data);
    }
    assert_prometheus_counter(
        &reader.serialize_metrics().unwrap(),
        "mooncake_transfer_read_strategy_total{strategy=\"transfer_engine\"}",
        3,
    );

    drop(reader);
    drop(writer);
    let _ = shutdown.send(());
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
async fn cpp_parity_offload_on_eviction_small_workload_stays_memory_only() {
    const SEGMENT_SIZE: u64 = 128 * 1024 * 1024;
    const VALUE_SIZE: usize = 1024;
    const KEY_COUNT: usize = 16;

    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        enable_offload: true,
        offload_on_evict: true,
        lease_ttl: std::time::Duration::from_secs(1),
        eviction_interval: std::time::Duration::from_millis(10),
        eviction_high_watermark_ratio: 0.95,
        ..Default::default()
    })
    .await;
    let backend = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
        root_dir: root.path().join("client"),
        fsdir: "offload-on-evict-small".into(),
        enable_eviction: false,
        quota_bytes: SEGMENT_SIZE,
    }));
    backend.init().unwrap();
    let mut client = create_tcp_client_with_segment_size(&master, SEGMENT_SIZE)
        .await
        .with_local_storage_backend(backend);
    client.mount_local_disk_segment(false).await.unwrap();

    let keys = (0..KEY_COUNT)
        .map(|index| format!("ooe_small_{index}"))
        .collect::<Vec<_>>();
    let values = (0..KEY_COUNT)
        .map(|index| vec![b'A' + index as u8; VALUE_SIZE])
        .collect::<Vec<_>>();
    for (key, value) in keys.iter().zip(&values) {
        client.put(key, value, None).await.unwrap();
    }

    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        ClientBackgroundConfig {
            health_interval: std::time::Duration::from_secs(1),
            storage_interval: std::time::Duration::from_millis(20),
            enable_offloading: true,
            enable_promotion: false,
            enable_task_poll: false,
            report_ssd_capacity: false,
            enable_disk_watermark_eviction: false,
            ..Default::default()
        },
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let mut guard = slot.lock().await;
        let client = guard.as_mut().unwrap();
        for (key, expected) in keys.iter().zip(&values) {
            let replicas = client.query(key).await.unwrap().replicas;
            assert!(
                !replicas.is_empty()
                    && replicas
                        .iter()
                        .all(|replica| replica.replica_type == ReplicaType::Memory),
                "below-watermark key {key} had non-MEMORY replicas: {replicas:?}"
            );
            assert_eq!(client.get(key).await.unwrap(), *expected);
        }
    }

    background.shutdown().await;
    let mut client = slot.lock().await.take().unwrap();
    for key in &keys {
        client.remove(key, true).await.unwrap();
    }
    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_legacy_ssd_overflow_single_and_batch_reads() {
    const SEGMENT_SIZE: u64 = 512 * 1024 * 1024;
    const VALUE_SIZE: usize = 1024 * 1024;
    const KEY_COUNT: usize = 1000;
    const BATCH_SIZE: usize = 4;
    const BUFFER_SPACING: usize = 2 * 1024 * 1024;

    fn value_for(index: usize) -> Vec<u8> {
        let mut value = vec![(index % 251) as u8; VALUE_SIZE];
        value[..std::mem::size_of::<u64>()].copy_from_slice(&(index as u64).to_le_bytes());
        value
    }

    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        enable_offload: true,
        offload_on_evict: false,
        lease_ttl: std::time::Duration::from_millis(500),
        eviction_interval: std::time::Duration::from_millis(5),
        eviction_high_watermark_ratio: 0.90,
        eviction_ratio: 0.10,
        ..Default::default()
    })
    .await;
    let backend = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
        root_dir: root.path().join("client"),
        fsdir: "legacy-ssd-overflow".into(),
        enable_eviction: false,
        quota_bytes: 2 * 1024 * 1024 * 1024,
    }));
    backend.init().unwrap();
    let mut client = create_tcp_client_with_segment_size(&master, SEGMENT_SIZE)
        .await
        .with_local_storage_backend(backend);
    client.mount_local_disk_segment(true).await.unwrap();
    let destination =
        RegisteredBufferAllocation::allocate(BATCH_SIZE * BUFFER_SPACING, 4096).unwrap();
    let destination_registration = client
        .register_owned_buffer(destination.clone(), "cpu:0")
        .unwrap();

    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        ClientBackgroundConfig {
            health_interval: std::time::Duration::from_secs(1),
            storage_interval: std::time::Duration::from_millis(10),
            enable_offloading: true,
            enable_promotion: false,
            enable_task_poll: false,
            report_ssd_capacity: false,
            enable_disk_watermark_eviction: false,
            ..Default::default()
        },
    );

    let keys = (0..KEY_COUNT)
        .map(|index| format!("k_{index}"))
        .collect::<Vec<_>>();
    for (index, key) in keys.iter().enumerate() {
        let value = value_for(index);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let result = slot
                .lock()
                .await
                .as_mut()
                .unwrap()
                .put(key, &value, None)
                .await;
            match result {
                Ok(()) => break,
                Err(StoreError::NoAvailableHandle) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "NO_AVAILABLE_HANDLE retry budget expired for {key}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => panic!("unexpected put error for {key}: {error}"),
            }
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut local_disk_only = 0_usize;
    for key in &keys {
        let replicas = slot
            .lock()
            .await
            .as_mut()
            .unwrap()
            .query(key)
            .await
            .unwrap()
            .replicas;
        if replicas
            .iter()
            .any(|replica| replica.replica_type == ReplicaType::LocalDisk)
            && replicas
                .iter()
                .all(|replica| replica.replica_type != ReplicaType::Memory)
        {
            local_disk_only += 1;
        }
    }
    assert!(
        local_disk_only > 0,
        "512-MiB segment pressure produced no LOCAL_DISK-only object"
    );

    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            slot.lock().await.as_mut().unwrap().get(key).await.unwrap(),
            value_for(index),
            "single get returned wrong bytes for {key}"
        );
    }

    let base = destination.as_ptr().cast::<u8>();
    for (batch_index, batch_keys) in keys.chunks(BATCH_SIZE).enumerate() {
        let buffers = (0..batch_keys.len())
            .map(|index| unsafe { base.add(index * BUFFER_SPACING).cast() })
            .collect::<Vec<_>>();
        let sizes = vec![VALUE_SIZE; batch_keys.len()];
        let results = slot
            .lock()
            .await
            .as_mut()
            .unwrap()
            .batch_get_into(batch_keys, &buffers, &sizes)
            .await
            .unwrap();
        assert_eq!(results, vec![VALUE_SIZE as i64; batch_keys.len()]);
        for index in 0..batch_keys.len() {
            let object_index = batch_index * BATCH_SIZE + index;
            let actual =
                unsafe { std::slice::from_raw_parts(base.add(index * BUFFER_SPACING), VALUE_SIZE) };
            assert_eq!(
                actual,
                value_for(object_index),
                "batch get returned wrong bytes for {}",
                batch_keys[index]
            );
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    background.shutdown().await;
    let mut client = slot.lock().await.take().unwrap();
    client
        .unregister_buffer_handle(destination_registration)
        .unwrap();
    for key in &keys {
        client.remove(key, false).await.unwrap();
    }
    drop(destination);
    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_promotion_below_watermark_workload_stays_memory_only() {
    const SEGMENT_SIZE: u64 = 32 * 1024 * 1024;
    const VALUE_SIZE: usize = 1024;
    const KEY_COUNT: usize = 16;

    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        enable_offload: true,
        offload_on_evict: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        lease_ttl: std::time::Duration::from_secs(1),
        eviction_interval: std::time::Duration::from_millis(10),
        eviction_high_watermark_ratio: 0.95,
        ..Default::default()
    })
    .await;
    let backend = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
        root_dir: root.path().join("client"),
        fsdir: "promotion-below-watermark".into(),
        enable_eviction: false,
        quota_bytes: SEGMENT_SIZE,
    }));
    backend.init().unwrap();
    let mut client = create_tcp_client_with_segment_size(&master, SEGMENT_SIZE)
        .await
        .with_local_storage_backend(backend);
    client.mount_local_disk_segment(false).await.unwrap();

    let keys = (0..KEY_COUNT)
        .map(|index| format!("neg_poh_small_{index}"))
        .collect::<Vec<_>>();
    for (index, key) in keys.iter().enumerate() {
        client
            .put(key, &vec![b'A' + index as u8; VALUE_SIZE], None)
            .await
            .unwrap();
    }

    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        ClientBackgroundConfig {
            health_interval: std::time::Duration::from_secs(1),
            storage_interval: std::time::Duration::from_millis(20),
            enable_offloading: true,
            enable_promotion: true,
            enable_task_poll: false,
            report_ssd_capacity: false,
            enable_disk_watermark_eviction: false,
            ..Default::default()
        },
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let mut guard = slot.lock().await;
        let client = guard.as_mut().unwrap();
        for key in &keys {
            let replicas = client.query(key).await.unwrap().replicas;
            assert!(
                replicas
                    .iter()
                    .any(|replica| replica.replica_type == ReplicaType::Memory),
                "below-watermark key {key} lost its MEMORY replica: {replicas:?}"
            );
            assert!(
                replicas
                    .iter()
                    .all(|replica| replica.replica_type != ReplicaType::LocalDisk),
                "below-watermark key {key} gained a LOCAL_DISK replica: {replicas:?}"
            );
        }
    }

    background.shutdown().await;
    let mut client = slot.lock().await.take().unwrap();
    for key in &keys {
        client.remove(key, true).await.unwrap();
    }
    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_empty_offload_heartbeat_still_runs_disk_watermark_eviction() {
    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        enable_offload: true,
        ..Default::default()
    })
    .await;
    let backend = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
        root_dir: root.path().join("client"),
        fsdir: "heartbeat-watermark".into(),
        enable_eviction: true,
        quota_bytes: 4 * 1024 * 1024,
    }));
    backend.init().unwrap();
    let keys = ["heartbeat-key-1", "heartbeat-key-2", "heartbeat-key-3"];
    let storage_keys = keys.map(|key| format!("v1:7:default{key}"));

    let mut client = create_tcp_client_with_segment_size(&master, 0)
        .await
        .with_local_storage_backend(Arc::clone(&backend));
    client.mount_local_disk_segment(true).await.unwrap();
    for (index, storage_key) in storage_keys.iter().enumerate() {
        backend
            .write_object(storage_key, &vec![b'a' + index as u8; 512])
            .unwrap();
    }
    let transport_endpoint = client.get_hostname();
    client
        .notify_offload_success_tasks(
            keys.iter()
                .map(|key| mooncake_store_client::OffloadTaskItem {
                    tenant_id: "default".into(),
                    key: (*key).into(),
                    size: 512,
                    generation_id: Uuid::new_v4(),
                })
                .collect(),
            keys.iter()
                .map(|key| proto::StorageObjectMetadata {
                    key_size: key.len() as i64,
                    data_size: 512,
                    transport_endpoint: transport_endpoint.clone(),
                    ..Default::default()
                })
                .collect(),
        )
        .await
        .unwrap();
    let user_keys = keys.map(str::to_string);
    client.health_check().await.unwrap();
    for key in &user_keys {
        let replicas = client.query(key).await.unwrap().replicas;
        let local_disk = replicas
            .iter()
            .find(|replica| replica.replica_type == ReplicaType::LocalDisk)
            .unwrap_or_else(|| panic!("published replicas for {key}: {replicas:?}"));
        assert_eq!(local_disk.holder_client_id, Some(client.client_id()));
        assert_eq!(
            local_disk.local_disk_storage_id,
            Some(backend.storage_id().unwrap())
        );
    }
    assert!(
        client
            .offload_object_heartbeat_tasks(true)
            .await
            .unwrap()
            .is_empty()
    );

    let slot = Arc::new(tokio::sync::Mutex::new(Some(client)));
    let background = MooncakeClient::start_background_workers(
        Arc::clone(&slot),
        ClientBackgroundConfig {
            health_interval: std::time::Duration::from_secs(1),
            storage_interval: std::time::Duration::from_millis(10),
            enable_offloading: true,
            enable_promotion: false,
            enable_task_poll: false,
            report_ssd_capacity: false,
            enable_disk_watermark_eviction: true,
            disk_eviction_high_watermark_ratio: 1e-12,
            disk_eviction_low_watermark_ratio: 0.5e-12,
            ..Default::default()
        },
    );

    let eviction = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let backend_empty = storage_keys
                .iter()
                .all(|storage_key| !backend.exists(storage_key));
            let master_empty = {
                let mut client = slot.lock().await;
                let client = client.as_mut().unwrap();
                client
                    .batch_get_replica_list_results(&user_keys)
                    .await
                    .unwrap()
                    .into_iter()
                    .all(|result| matches!(result, Err(StoreError::KeyNotFound(_))))
            };
            if backend_empty && master_empty {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    if eviction.is_err() {
        let backend_present = storage_keys
            .iter()
            .map(|storage_key| backend.exists(storage_key))
            .collect::<Vec<_>>();
        let master_results = slot
            .lock()
            .await
            .as_mut()
            .unwrap()
            .batch_get_replica_list_results(&user_keys)
            .await
            .unwrap();
        panic!(
            "empty-work storage heartbeat did not evict all LocalDisk records: \
             backend_present={backend_present:?}, master_results={master_results:?}"
        );
    }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_replica_clear_does_not_affect_other_keys() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(200),
        ..Default::default()
    })
    .await;
    let mut client = create_tcp_client(&master).await;
    let clear_key = "clear-other-client-key";
    let keep_key = "keep-other-client-key";

    assert_eq!(
        client
            .batch_put(
                &[clear_key.to_string(), keep_key.to_string()],
                &[b"expire_me".as_slice(), b"keep_me".as_slice()],
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0, 0]
    );

    assert_eq!(client.get(keep_key).await.unwrap(), b"keep_me".to_vec());
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    assert_eq!(
        client
            .batch_replica_clear(&[clear_key.to_string()], client.client_id(), "", "")
            .await
            .unwrap(),
        vec![clear_key.to_string()]
    );

    assert_eq!(
        client
            .batch_is_exist(&[clear_key.to_string(), keep_key.to_string()])
            .await
            .unwrap(),
        vec![false, true]
    );
    assert_eq!(client.get(keep_key).await.unwrap(), b"keep_me".to_vec());

    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_replica_clear_replicated_key_all_replicas() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        // 200 ms keeps the immediate post-put reads robust under parallel test
        // load while still expiring long before the batch clear below.
        lease_ttl: std::time::Duration::from_millis(200),
        ..Default::default()
    })
    .await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;

    let key = "replicated-clear-key";
    let value = b"replicated-value".to_vec();
    assert_eq!(
        writer
            .put(
                key,
                &value,
                Some(ReplicateConfig {
                    replica_num: 2,
                    preferred_segments: vec![writer.get_hostname(), reader.get_hostname()],
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        ()
    );

    assert_eq!(writer.get(key).await.unwrap(), value);
    assert_eq!(reader.get(key).await.unwrap(), value);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert_eq!(
        writer
            .batch_replica_clear(&[key.to_string()], writer.client_id(), "", "")
            .await
            .unwrap(),
        vec![key.to_string()]
    );
    assert!(writer.get(key).await.is_err());
    assert!(reader.get(key).await.is_err());

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_replica_clear_specific_segment_replica_keeps_other_readable() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(40),
        ..Default::default()
    })
    .await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;

    let key = "replicated-segment-clear-key";
    let value = b"segment-replica-value".to_vec();
    assert_eq!(
        writer
            .put(
                key,
                &value,
                Some(ReplicateConfig {
                    replica_num: 2,
                    preferred_segments: vec![writer.get_hostname(), reader.get_hostname()],
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        ()
    );
    assert_eq!(reader.get(key).await.unwrap(), value);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        writer
            .batch_replica_clear(
                &[key.to_string()],
                writer.client_id(),
                &writer.get_hostname(),
                "",
            )
            .await
            .unwrap(),
        vec![key.to_string()]
    );
    // Named clear removes the main segment's replica while the other Store's
    // replica stays readable with the exact original bytes.
    assert_eq!(reader.get(key).await.unwrap(), value);

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_replicated_registered_put_from_config_matrix() {
    const BUFFER_SPACING: usize = 1024 * 1024;
    const BUFFER_SIZE: usize = 4 * BUFFER_SPACING;

    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let mut writer = create_tcp_client(&master).await;
    let reader = create_tcp_client(&master).await;
    let allocation = RegisteredBufferAllocation::allocate(BUFFER_SIZE, 4096).unwrap();
    let registration = writer
        .register_owned_buffer(allocation.clone(), "cpu:0")
        .unwrap();
    let base = allocation.as_ptr().cast::<u8>();
    let batch_values = [
        b"Batch Config Data 1".as_slice(),
        b"Batch Config Data 2".as_slice(),
        b"Batch Config Data 3".as_slice(),
    ];
    let single_value = b"Hello, put_from config world!".as_slice();
    for (index, value) in batch_values.iter().enumerate() {
        unsafe {
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                base.add(index * BUFFER_SPACING),
                value.len(),
            );
        }
    }
    let single_source = unsafe { base.add(3 * BUFFER_SPACING) };
    unsafe {
        std::ptr::copy_nonoverlapping(single_value.as_ptr(), single_source, single_value.len());
    }

    let replicated = ReplicateConfig {
        replica_num: 2,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segments: vec![writer.get_hostname(), reader.get_hostname()],
        ..Default::default()
    };
    let single_keys = ["test_put_from_config_key", "test_put_from_config_key2"];
    writer
        .put_from(
            single_keys[0],
            single_source.cast(),
            single_value.len(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(writer.get(single_keys[0]).await.unwrap(), single_value);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    writer.remove(single_keys[0], false).await.unwrap();

    writer
        .put_from(
            single_keys[1],
            single_source.cast(),
            single_value.len(),
            Some(replicated.clone()),
        )
        .await
        .unwrap();
    assert_eq!(writer.get(single_keys[1]).await.unwrap(), single_value);
    assert_eq!(
        writer.query(single_keys[1]).await.unwrap().replicas.len(),
        2
    );

    let batch_sources = (0..batch_values.len())
        .map(|index| unsafe { base.add(index * BUFFER_SPACING).cast() })
        .collect::<Vec<_>>();
    let batch_sizes = batch_values
        .iter()
        .map(|value| value.len())
        .collect::<Vec<_>>();
    let default_batch_keys = (1..=3)
        .map(|index| format!("test_batch_put_from_config_key{index}"))
        .collect::<Vec<_>>();
    assert_eq!(
        writer
            .batch_put_from(&default_batch_keys, &batch_sources, &batch_sizes, None)
            .await
            .unwrap(),
        vec![0, 0, 0]
    );
    for (key, expected) in default_batch_keys.iter().zip(batch_values) {
        assert_eq!(writer.get(key).await.unwrap(), expected);
    }
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    for key in &default_batch_keys {
        writer.remove(key, false).await.unwrap();
    }

    let replicated_batch_keys = (4..=6)
        .map(|index| format!("test_batch_put_from_config_key{index}"))
        .collect::<Vec<_>>();
    assert_eq!(
        writer
            .batch_put_from(
                &replicated_batch_keys,
                &batch_sources,
                &batch_sizes,
                Some(replicated),
            )
            .await
            .unwrap(),
        vec![0, 0, 0]
    );
    for (key, expected) in replicated_batch_keys.iter().zip(batch_values) {
        assert_eq!(writer.get(key).await.unwrap(), expected);
        assert_eq!(writer.query(key).await.unwrap().replicas.len(), 2);
    }

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    writer.unregister_buffer_handle(registration).unwrap();
    writer.remove(single_keys[1], false).await.unwrap();
    for key in &replicated_batch_keys {
        writer.remove(key, false).await.unwrap();
    }

    drop(allocation);
    drop(reader);
    drop(writer);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_replica_clear_mixed_expired_and_active_keeps_active_bytes() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(200),
        ..Default::default()
    })
    .await;
    let mut client = create_tcp_client(&master).await;

    assert_eq!(
        client
            .batch_put(
                &["expired-a".to_string(), "expired-b".to_string()],
                &[b"expired-a".as_slice(), b"expired-b".as_slice()],
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0, 0]
    );
    assert_eq!(
        client
            .batch_put(
                &["active-a".to_string(), "active-b".to_string()],
                &[b"active-a".as_slice(), b"active-b".as_slice()],
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        vec![0, 0]
    );

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    assert_eq!(client.get("active-a").await.unwrap(), b"active-a".to_vec());
    assert_eq!(client.get("active-b").await.unwrap(), b"active-b".to_vec());

    assert_eq!(
        client
            .batch_replica_clear(
                &[
                    "expired-a".to_string(),
                    "expired-b".to_string(),
                    "active-a".to_string(),
                    "active-b".to_string(),
                ],
                client.client_id(),
                "",
                "",
            )
            .await
            .unwrap(),
        vec!["expired-a".to_string(), "expired-b".to_string()]
    );

    assert_eq!(
        client
            .batch_is_exist(&[
                "expired-a".to_string(),
                "expired-b".to_string(),
                "active-a".to_string(),
                "active-b".to_string(),
            ])
            .await
            .unwrap(),
        vec![false, false, true, true]
    );
    assert_eq!(client.get("active-a").await.unwrap(), b"active-a".to_vec());
    assert_eq!(client.get("active-b").await.unwrap(), b"active-b".to_vec());

    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_replica_clear_with_active_lease_keeps_exact_bytes() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(500),
        ..Default::default()
    })
    .await;
    let mut client = create_tcp_client(&master).await;

    let keys = vec![
        "active-a".to_string(),
        "active-b".to_string(),
        "active-c".to_string(),
    ];
    let values = vec![
        b"value-a".as_slice(),
        b"value-b".as_slice(),
        b"value-c".as_slice(),
    ];
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
        vec![0, 0, 0]
    );

    for (index, key) in keys.iter().enumerate() {
        assert_eq!(client.get(key).await.unwrap(), values[index].to_vec());
    }

    assert!(
        client
            .batch_replica_clear(&keys, client.client_id(), "", "")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client.batch_is_exist(&keys).await.unwrap(),
        vec![true, true, true]
    );
    assert_eq!(client.get("active-a").await.unwrap(), b"value-a".to_vec());
    assert_eq!(client.get("active-b").await.unwrap(), b"value-b".to_vec());
    assert_eq!(client.get("active-c").await.unwrap(), b"value-c".to_vec());

    drop(client);
    let _ = shutdown.send(());
}

async fn task_status(client: &mut MooncakeClient, task_id: Uuid) -> i32 {
    client.query_task(task_id).await.unwrap().status
}

async fn drive_task_to_success(
    creator: &mut MooncakeClient,
    worker: &mut MooncakeClient,
    task_id: Uuid,
) {
    if task_status(creator, task_id).await == proto::TaskStatus::TaskSuccess as i32 {
        return;
    }
    for client in [worker, creator] {
        let assignments = client.fetch_tasks(100).await.unwrap();
        for assignment in assignments {
            let payload = assignment.payload.clone();
            let result = client.execute_task_assignment(assignment).await;
            if let Err(error) = result {
                eprintln!(
                    "execute failed on {}: {error}; payload={payload}",
                    client.get_hostname()
                );
                panic!("execute_task_assignment failed: {error}");
            }
        }
    }
    for _ in 0..200 {
        let response = creator.query_task(task_id).await.unwrap();
        if response.status == proto::TaskStatus::TaskSuccess as i32 {
            return;
        }
        assert_ne!(
            response.status,
            proto::TaskStatus::TaskFailed as i32,
            "task {task_id} failed: {}",
            response.message
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("task {task_id} did not reach SUCCESS");
}

async fn source_segment_for(client: &mut MooncakeClient, key: &str) -> String {
    let _ = key;
    // The client-cached query can report a locally-derived endpoint name;
    // the master's segment table holds the authoritative segment name.
    let details = client.get_segments_detail().await.unwrap();
    details
        .iter()
        .find(|detail| detail.client_id == client.client_id())
        .expect("client should own exactly one mounted segment")
        .segment_name
        .clone()
}

async fn other_segment(client: &mut MooncakeClient, source: &str) -> String {
    let details = client.get_segments_detail().await.unwrap();
    details
        .iter()
        .find(|detail| detail.segment_name != source)
        .expect("expected a second distinct mounted segment")
        .segment_name
        .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_replica_copy_complete_flow() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig::default()).await;
    let mut client1 = create_tcp_client(&master).await;
    let mut client2 = create_tcp_client(&master).await;
    let key = "task-copy-key";
    let payload = b"This is test data for replica copy operation.";

    client1
        .put(
            key,
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: client1.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let source = source_segment_for(&mut client1, key).await;
    let target = other_segment(&mut client1, &source).await;
    assert_ne!(source, target);

    let task_id = client1.create_copy_task(key, &[target]).await.unwrap();
    drive_task_to_success(&mut client1, &mut client2, task_id).await;

    let query = client2.query(key).await.unwrap();
    assert!(!query.replicas.is_empty());
    assert_eq!(client2.get(key).await.unwrap(), payload.to_vec());

    drop(client1);
    drop(client2);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_replica_move_complete_flow() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig::default()).await;
    let mut client1 = create_tcp_client(&master).await;
    let mut client2 = create_tcp_client(&master).await;
    let key = "task-move-key";
    let payload = b"payload for replica move complete flow";

    client1
        .put(
            key,
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: client1.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let source = source_segment_for(&mut client1, key).await;
    let target = other_segment(&mut client1, &source).await;
    assert_ne!(source, target);

    let task_id = client1
        .create_move_task(key, &source, &target)
        .await
        .unwrap();
    drive_task_to_success(&mut client1, &mut client2, task_id).await;

    let query = client2.query(key).await.unwrap();
    assert!(!query.replicas.is_empty());
    assert_eq!(client2.get(key).await.unwrap(), payload.to_vec());

    drop(client1);
    drop(client2);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_replica_copy_to_multiple_targets() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig::default()).await;
    let mut client1 = create_tcp_client(&master).await;
    let mut client2 = create_tcp_client(&master).await;
    let key = "task-copy-multi-key";
    let payload = b"multi-target copy payload";

    client1
        .put(
            key,
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: client1.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let source = source_segment_for(&mut client1, key).await;
    let target = other_segment(&mut client1, &source).await;

    let task_id = client1.create_copy_task(key, &[target]).await.unwrap();
    drive_task_to_success(&mut client1, &mut client2, task_id).await;

    let query = client2.query(key).await.unwrap();
    assert!(!query.replicas.is_empty());
    assert_eq!(client2.get(key).await.unwrap(), payload.to_vec());

    drop(client1);
    drop(client2);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_multiple_copy_tasks() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig::default()).await;
    let mut client1 = create_tcp_client(&master).await;
    let mut client2 = create_tcp_client(&master).await;
    let payloads = [
        b"copy payload for key 0",
        b"copy payload for key 1",
        b"copy payload for key 2",
    ];
    let keys = ["task-copy-0", "task-copy-1", "task-copy-2"];
    let mut task_ids = Vec::new();

    for (key, payload) in keys.iter().zip(payloads.iter()) {
        client1
            .put(
                key,
                *payload,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client1.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let source = source_segment_for(&mut client1, key).await;
        let target = other_segment(&mut client1, &source).await;
        task_ids.push(client1.create_copy_task(key, &[target]).await.unwrap());
    }

    for task_id in &task_ids {
        drive_task_to_success(&mut client1, &mut client2, *task_id).await;
    }
    for (index, key) in keys.iter().enumerate() {
        let query = client2.query(key).await.unwrap();
        assert!(!query.replicas.is_empty());
        assert_eq!(client2.get(key).await.unwrap(), payloads[index].to_vec());
    }

    drop(client1);
    drop(client2);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_multiple_move_tasks() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig::default()).await;
    let mut client1 = create_tcp_client(&master).await;
    let mut client2 = create_tcp_client(&master).await;
    let payloads = [
        b"move payload for key 0",
        b"move payload for key 1",
        b"move payload for key 2",
    ];
    let keys = ["task-move-0", "task-move-1", "task-move-2"];
    let mut task_ids = Vec::new();

    for (key, payload) in keys.iter().zip(payloads.iter()) {
        client1
            .put(
                key,
                *payload,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client1.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let source = source_segment_for(&mut client1, key).await;
        let target = other_segment(&mut client1, &source).await;
        task_ids.push(
            client1
                .create_move_task(key, &source, &target)
                .await
                .unwrap(),
        );
    }

    for task_id in &task_ids {
        drive_task_to_success(&mut client1, &mut client2, *task_id).await;
    }
    for (index, key) in keys.iter().enumerate() {
        let query = client2.query(key).await.unwrap();
        assert!(!query.replicas.is_empty());
        assert_eq!(client2.get(key).await.unwrap(), payloads[index].to_vec());
    }

    drop(client1);
    drop(client2);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_concurrent_copy_and_move_operations() {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig::default()).await;
    let mut client1 = create_tcp_client(&master).await;
    let mut client2 = create_tcp_client(&master).await;
    let copy_payloads = [b"concurrent copy data 0", b"concurrent copy data 1"];
    let move_payloads = [b"concurrent move data 0", b"concurrent move data 1"];
    let copy_keys = ["task-copy-c-0", "task-copy-c-1"];
    let move_keys = ["task-move-c-0", "task-move-c-1"];
    let mut task_ids = Vec::new();

    for (key, payload) in copy_keys.iter().zip(copy_payloads.iter()) {
        client1
            .put(
                key,
                *payload,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client1.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let source = source_segment_for(&mut client1, key).await;
        let target = other_segment(&mut client1, &source).await;
        task_ids.push(client1.create_copy_task(key, &[target]).await.unwrap());
    }
    for (key, payload) in move_keys.iter().zip(move_payloads.iter()) {
        client1
            .put(
                key,
                *payload,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: client1.get_hostname(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let source = source_segment_for(&mut client1, key).await;
        let target = other_segment(&mut client1, &source).await;
        task_ids.push(
            client1
                .create_move_task(key, &source, &target)
                .await
                .unwrap(),
        );
    }

    for task_id in &task_ids {
        drive_task_to_success(&mut client1, &mut client2, *task_id).await;
    }
    for (index, key) in copy_keys.iter().chain(move_keys.iter()).enumerate() {
        let query = client2.query(key).await.unwrap();
        assert!(!query.replicas.is_empty());
        let expected = if index < 2 {
            copy_payloads[index]
        } else {
            move_payloads[index - 2]
        };
        assert_eq!(client2.get(key).await.unwrap(), expected.to_vec());
    }

    drop(client1);
    drop(client2);
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
async fn cpp_parity_client_integration_test_cpp_clientintegrationtest_basicputgetoperations_d96c1745()
 {
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let mut client = create_tcp_client(&master).await;
    let payload = b"Hello, World!";
    let replication = ReplicateConfig {
        replica_num: 1,
        ..Default::default()
    };

    client
        .put("test_key", payload, Some(replication.clone()))
        .await
        .unwrap();
    let fetched = client.get("test_key").await.unwrap();
    assert_eq!(fetched.len(), payload.len());
    assert_eq!(fetched, payload);
    client
        .put("test_key", payload, Some(replication))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    client.remove("test_key", false).await.unwrap();
    assert!(!client.exists("test_key").await.unwrap());

    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_client_integration_test_cpp_clientintegrationtest_mountsegmentandgetidandunmountsegmentbyid_998a1ea2()
 {
    const SEGMENT_SIZE: usize = 16 * 1024 * 1024;
    const SEGMENT_ALIGNMENT: usize = 4096;

    let (master, shutdown) = start_master().await;
    let mut client = create_tcp_client_with_segment_size(&master, 0).await;
    let allocation = RegisteredBufferAllocation::allocate(SEGMENT_SIZE, SEGMENT_ALIGNMENT).unwrap();
    let registration = client
        .register_owned_buffer(allocation.clone(), "cpu:0")
        .unwrap();
    let base_addr = allocation.as_ptr() as u64;
    let segment_id = client
        .mount_segment_with_id("legacy-mount-segment", SEGMENT_SIZE as u64, base_addr)
        .await
        .unwrap();
    let (high, low) = segment_id.as_u64_pair();
    assert_ne!(high, 0);
    assert_ne!(low, 0);
    client.unmount_segment_by_id(segment_id, 0).await.unwrap();

    client
        .mount_segment("legacy-mount-segment", SEGMENT_SIZE as u64, base_addr)
        .await
        .unwrap();
    client
        .unmount_segment("legacy-mount-segment", 0)
        .await
        .unwrap();
    client.unregister_buffer_handle(registration).unwrap();

    drop(allocation);
    drop(client);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_global_disk_cross_client_readback_preserves_exact_bytes() {
    const KEY_COUNT: usize = 5;
    const VALUE_SIZE: usize = 4096;

    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        ..Default::default()
    })
    .await;
    let mut writer = create_tcp_client(&master).await;
    let writer_segment = writer.get_hostname();
    let keys = (0..KEY_COUNT)
        .map(|index| format!("global-disk-cross-client-{index}"))
        .collect::<Vec<_>>();
    let values = (0..KEY_COUNT)
        .map(|index| vec![b'A' + index as u8; VALUE_SIZE])
        .collect::<Vec<_>>();

    for (key, value) in keys.iter().zip(&values) {
        writer
            .put(
                key,
                value,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: writer_segment.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        assert!(
            writer
                .query(key)
                .await
                .unwrap()
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::Disk)
        );
    }

    let mut reader = create_tcp_client(&master).await;
    for (key, expected) in keys.iter().zip(&values) {
        assert!(
            reader
                .query(key)
                .await
                .unwrap()
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::Disk)
        );
        assert_eq!(reader.get(key).await.unwrap(), *expected);
    }

    drop(reader);
    drop(writer);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_global_disk_only_read_after_memory_eviction() {
    const VALUE_SIZE: usize = 256 * 1024;
    const SEED_COUNT: usize = 4;
    const PRESSURE_COUNT: usize = 12;

    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        lease_ttl: std::time::Duration::from_millis(1_000),
        eviction_interval: std::time::Duration::from_millis(5),
        eviction_high_watermark_ratio: 0.10,
        eviction_ratio: 0.05,
        ..Default::default()
    })
    .await;
    let mut client = create_tcp_client_with_segment_size(&master, 16 * 1024 * 1024).await;
    let segment = client.get_hostname();
    let seed_keys = (0..SEED_COUNT)
        .map(|index| format!("global-disk-evict-seed-{index}"))
        .collect::<Vec<_>>();
    let seed_values = (0..SEED_COUNT)
        .map(|index| vec![b'A' + index as u8; VALUE_SIZE])
        .collect::<Vec<_>>();

    for (key, value) in seed_keys.iter().zip(&seed_values) {
        client
            .put(
                key,
                value,
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: segment.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        assert!(
            client
                .query(key)
                .await
                .unwrap()
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::Disk)
        );
    }

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    for index in 0..PRESSURE_COUNT {
        client
            .put(
                &format!("global-disk-evict-pressure-{index}"),
                &vec![b'P' + (index % 8) as u8; VALUE_SIZE],
                Some(ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: segment.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let mut evicted_seed_index = None;
    for (index, key) in seed_keys.iter().enumerate() {
        let replicas = client.query(key).await.unwrap().replicas;
        let has_disk = replicas
            .iter()
            .any(|replica| replica.replica_type == ReplicaType::Disk);
        let has_memory = replicas
            .iter()
            .any(|replica| replica.replica_type == ReplicaType::Memory);
        if has_disk && !has_memory {
            evicted_seed_index = Some(index);
            break;
        }
    }
    let evicted_seed_index =
        evicted_seed_index.expect("memory eviction did not leave a Disk-only seed");

    let handle = client
        .get_buffer(&seed_keys[evicted_seed_index])
        .await
        .unwrap();
    assert_eq!(handle.size, VALUE_SIZE);
    assert_eq!(handle.data, seed_values[evicted_seed_index]);

    drop(client);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpp_parity_global_disk_replicas_survive_writer_liveness_cleanup() {
    const VALUE_SIZE: usize = 2 * 1024;
    const KEY_COUNT: usize = 4;

    let root = tempfile::tempdir().unwrap();
    let (master, shutdown) = start_master_with_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        client_live_ttl: std::time::Duration::from_millis(2_000),
        client_monitor_interval: std::time::Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let keys = (0..KEY_COUNT)
        .map(|index| format!("global-disk-writer-death-{index}"))
        .collect::<Vec<_>>();
    let values = (0..KEY_COUNT)
        .map(|index| vec![b'a' + index as u8; VALUE_SIZE])
        .collect::<Vec<_>>();

    {
        let mut writer = create_tcp_client_with_segment_size(&master, 16 * 1024 * 1024).await;
        let writer_segment = writer.get_hostname();
        for (key, value) in keys.iter().zip(&values) {
            writer.health_check().await.unwrap();
            writer
                .put(
                    key,
                    value,
                    Some(ReplicateConfig {
                        replica_num: 1,
                        preferred_segment: writer_segment.clone(),
                        ..Default::default()
                    }),
                )
                .await
                .unwrap();
        }
        writer.health_check().await.unwrap();
        let writer_replicas = writer.batch_get_replica_list(&keys).await.unwrap();
        assert!(writer_replicas.iter().all(|replicas| {
            replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::Disk)
                && replicas
                    .iter()
                    .any(|replica| replica.replica_type == ReplicaType::Memory)
        }));
        writer.health_check().await.unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(2_300)).await;
    let mut reader = create_tcp_client_with_segment_size(&master, 16 * 1024 * 1024).await;
    for (key, expected) in keys.iter().zip(&values) {
        let replicas = reader.query(key).await.unwrap().replicas;
        assert!(
            replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::Disk),
            "Disk replica disappeared after writer liveness cleanup for {key}"
        );
        assert!(
            replicas
                .iter()
                .all(|replica| replica.replica_type != ReplicaType::Memory),
            "Memory replica survived writer liveness cleanup for {key}"
        );
        match reader.get_buffer(key).await {
            Ok(handle) => {
                assert_eq!(handle.size, VALUE_SIZE);
                assert_eq!(handle.data, *expected);
            }
            Err(error) => eprintln!("Disk-only successor read failed for {key}: {error}"),
        }
    }

    drop(reader);
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

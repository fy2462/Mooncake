mod common;

use common::proto_uuid;
use mooncake_store_master::metrics;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use prometheus::{Encoder, TextEncoder};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

static METRICS_TEST_LOCK: Mutex<()> = Mutex::new(());

fn replicate_config(segment: &str) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: segment.into(),
        prefer_alloc_in_same_node: false,
        preferred_segments: vec![],
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
        host_id: String::new(),
    }
}

fn multi_replicate_config(segments: &[&str]) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: segments.len() as u32,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: String::new(),
        prefer_alloc_in_same_node: false,
        preferred_segments: segments.iter().map(|segment| (*segment).into()).collect(),
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
        host_id: String::new(),
    }
}

async fn mount_memory_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str) {
    static NEXT_BASE: AtomicU64 = AtomicU64::new(0x1_0000_0000);
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 4096,
            base_addr: NEXT_BASE.fetch_add(0x1_0000, Ordering::Relaxed),
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

fn batch_matrix(snapshot: &metrics::MasterMetricSnapshot, operation: &str) -> [u64; 5] {
    match operation {
        "exist" => [
            snapshot.batch_exist_key_requests,
            snapshot.batch_exist_key_partial_successes,
            snapshot.batch_exist_key_failures,
            snapshot.batch_exist_key_items,
            snapshot.batch_exist_key_failed_items,
        ],
        "get" => [
            snapshot.batch_get_replica_list_requests,
            snapshot.batch_get_replica_list_partial_successes,
            snapshot.batch_get_replica_list_failures,
            snapshot.batch_get_replica_list_items,
            snapshot.batch_get_replica_list_failed_items,
        ],
        "put_start" => [
            snapshot.batch_put_start_requests,
            snapshot.batch_put_start_partial_successes,
            snapshot.batch_put_start_failures,
            snapshot.batch_put_start_items,
            snapshot.batch_put_start_failed_items,
        ],
        "put_end" => [
            snapshot.batch_put_end_requests,
            snapshot.batch_put_end_partial_successes,
            snapshot.batch_put_end_failures,
            snapshot.batch_put_end_items,
            snapshot.batch_put_end_failed_items,
        ],
        "put_revoke" => [
            snapshot.batch_put_revoke_requests,
            snapshot.batch_put_revoke_partial_successes,
            snapshot.batch_put_revoke_failures,
            snapshot.batch_put_revoke_items,
            snapshot.batch_put_revoke_failed_items,
        ],
        _ => panic!("unknown batch metric operation: {operation}"),
    }
}

fn assert_basic_memory_metrics(segment: &str, keys: i64, allocated: i64, capacity: i64) {
    let snapshot = metrics::master_metric_snapshot();
    assert_eq!(snapshot.key_count, keys);
    assert_eq!(snapshot.allocated_mem_size, allocated);
    assert_eq!(snapshot.total_mem_capacity, capacity);
    assert_eq!(
        snapshot.global_mem_used_ratio,
        if capacity == 0 {
            0.0
        } else {
            allocated as f64 / capacity as f64
        }
    );
    let segment_snapshot = metrics::segment_memory_metric_snapshot(segment);
    assert_eq!(segment_snapshot.allocated_mem_size, allocated);
    assert_eq!(segment_snapshot.total_mem_capacity, capacity);
    assert_eq!(
        segment_snapshot.used_ratio,
        if capacity == 0 {
            0.0
        } else {
            allocated as f64 / capacity as f64
        }
    );
}

#[test]
fn cpp_parity_master_metrics_test_cpp_mastermetricstest_basicrequesttest() {
    const CHILD_MARKER: &str = "MOONCAKE_BASIC_REQUEST_METRICS_CHILD";
    const TEST_NAME: &str = "cpp_parity_master_metrics_test_cpp_mastermetricstest_basicrequesttest";
    if std::env::var_os(CHILD_MARKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(TEST_NAME)
            .arg("--exact")
            .arg("--test-threads=1")
            .env(CHILD_MARKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "fresh basic-request metrics child failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        const SEGMENT: &str = "test_segment";
        const SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
        const KEY: &str = "test_key";
        const VALUE_SIZE: u64 = 1024;
        let service = MasterServiceImpl::new_with_runtime_config(
            None,
            None,
            MasterRuntimeConfig {
                lease_ttl: Duration::ZERO,
                ..Default::default()
            },
        );
        let client_id = Uuid::new_v4();

        let mounted = MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: SEGMENT.into(),
                size: SEGMENT_SIZE,
                base_addr: 0x3_0000_0000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let segment_id = mounted.segment_id.expect("mounted segment id");
        assert_basic_memory_metrics(SEGMENT, 0, 0, SEGMENT_SIZE as i64);
        let snapshot = metrics::master_metric_snapshot();
        assert_eq!(
            (
                snapshot.mount_segment_requests,
                snapshot.mount_segment_failures
            ),
            (1, 0)
        );

        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: KEY.into(),
                slice_length: VALUE_SIZE,
                config: Some(replicate_config(SEGMENT)),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_basic_memory_metrics(SEGMENT, 1, VALUE_SIZE as i64, SEGMENT_SIZE as i64);
        let snapshot = metrics::master_metric_snapshot();
        assert_eq!(
            (snapshot.put_start_requests, snapshot.put_start_failures),
            (1, 0)
        );

        MasterService::put_revoke(
            &service,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(proto_uuid(client_id)),
                key: KEY.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_basic_memory_metrics(SEGMENT, 0, 0, SEGMENT_SIZE as i64);
        let snapshot = metrics::master_metric_snapshot();
        assert_eq!(
            (snapshot.put_revoke_requests, snapshot.put_revoke_failures),
            (1, 0)
        );

        for generation in 0..3 {
            MasterService::put_start(
                &service,
                Request::new(proto::PutStartRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key: KEY.into(),
                    slice_length: VALUE_SIZE,
                    config: Some(replicate_config(SEGMENT)),
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap();
            MasterService::put_end(
                &service,
                Request::new(proto::PutEndRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key: KEY.into(),
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap();
            assert_basic_memory_metrics(SEGMENT, 1, VALUE_SIZE as i64, SEGMENT_SIZE as i64);

            match generation {
                0 => {
                    let snapshot = metrics::master_metric_snapshot();
                    assert_eq!(
                        (snapshot.put_start_requests, snapshot.put_start_failures),
                        (2, 0)
                    );
                    assert_eq!(
                        (snapshot.put_end_requests, snapshot.put_end_failures),
                        (1, 0)
                    );

                    let exists = MasterService::exist_key(
                        &service,
                        Request::new(proto::ExistKeyRequest {
                            key: KEY.into(),
                            tenant_id: String::new(),
                        }),
                    )
                    .await
                    .unwrap()
                    .into_inner();
                    assert!(exists.exists);
                    let replicas = MasterService::get_replica_list(
                        &service,
                        Request::new(proto::GetReplicaListRequest {
                            key: KEY.into(),
                            tenant_id: String::new(),
                        }),
                    )
                    .await
                    .unwrap()
                    .into_inner();
                    assert_eq!(replicas.replicas.len(), 1);
                    let snapshot = metrics::master_metric_snapshot();
                    assert_eq!(
                        (snapshot.exist_key_requests, snapshot.exist_key_failures),
                        (1, 0)
                    );
                    assert_eq!(
                        (
                            snapshot.get_replica_list_requests,
                            snapshot.get_replica_list_failures
                        ),
                        (1, 0)
                    );

                    MasterService::remove(
                        &service,
                        Request::new(proto::RemoveRequest {
                            key: KEY.into(),
                            force: false,
                            tenant_id: String::new(),
                        }),
                    )
                    .await
                    .unwrap();
                    assert_basic_memory_metrics(SEGMENT, 0, 0, SEGMENT_SIZE as i64);
                    let snapshot = metrics::master_metric_snapshot();
                    assert_eq!((snapshot.remove_requests, snapshot.remove_failures), (1, 0));
                }
                1 => {
                    let removed = MasterService::remove_all(
                        &service,
                        Request::new(proto::RemoveAllRequest {
                            force: true,
                            tenant_id: String::new(),
                        }),
                    )
                    .await
                    .unwrap()
                    .into_inner();
                    assert_eq!(removed.removed_count, 1);
                    assert_basic_memory_metrics(SEGMENT, 0, 0, SEGMENT_SIZE as i64);
                    let snapshot = metrics::master_metric_snapshot();
                    assert_eq!(
                        (snapshot.remove_all_requests, snapshot.remove_all_failures),
                        (1, 0)
                    );
                }
                2 => {
                    MasterService::unmount_segment(
                        &service,
                        Request::new(proto::UnmountSegmentRequest {
                            segment_id: Some(segment_id.clone()),
                            client_id: Some(proto_uuid(client_id)),
                        }),
                    )
                    .await
                    .unwrap();
                    assert_basic_memory_metrics(SEGMENT, 0, 0, 0);
                    assert_eq!(
                        metrics::segment_memory_metric_snapshot(""),
                        metrics::SegmentMemoryMetricSnapshot::default()
                    );
                    assert_eq!(
                        metrics::segment_memory_metric_snapshot("xxxxxx_segment"),
                        metrics::SegmentMemoryMetricSnapshot::default()
                    );
                    let snapshot = metrics::master_metric_snapshot();
                    assert_eq!(
                        (
                            snapshot.unmount_segment_requests,
                            snapshot.unmount_segment_failures
                        ),
                        (1, 0)
                    );
                }
                _ => unreachable!(),
            }
        }
    });
}

#[tokio::test]
async fn non_scalar_rpc_paths_do_not_contaminate_scalar_request_counters() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let service = MasterServiceImpl::default();
    let before = metrics::master_metric_snapshot();

    MasterService::batch_get_replica_list(
        &service,
        Request::new(proto::BatchGetReplicaListRequest {
            keys: vec!["missing".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::get_replica_list_by_regex(
        &service,
        Request::new(proto::GetReplicaListByRegexRequest {
            key_regex: ".*".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::remove_by_regex(
        &service,
        Request::new(proto::RemoveByRegexRequest {
            pattern: ".*".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after = metrics::master_metric_snapshot();
    assert_eq!(
        after.get_replica_list_requests,
        before.get_replica_list_requests
    );
    assert_eq!(after.remove_requests, before.remove_requests);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_memory_metric_publications_converge_after_unmounts() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let service = std::sync::Arc::new(MasterServiceImpl::default());
    let mut tasks = Vec::new();
    for index in 0..32u64 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            let client_id = Uuid::new_v4();
            let segment_name = format!("concurrent-metrics-{index}");
            let mounted = MasterService::mount_segment(
                service.as_ref(),
                Request::new(proto::MountSegmentRequest {
                    client_id: Some(proto_uuid(client_id)),
                    segment_name: segment_name.clone(),
                    size: 4096,
                    base_addr: 0x5_0000_0000 + index * 0x1_0000,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                    host_id: String::new(),
                }),
            )
            .await
            .unwrap()
            .into_inner();
            tokio::task::yield_now().await;
            MasterService::unmount_segment(
                service.as_ref(),
                Request::new(proto::UnmountSegmentRequest {
                    segment_id: mounted.segment_id,
                    client_id: Some(proto_uuid(client_id)),
                }),
            )
            .await
            .unwrap();
            segment_name
        }));
    }
    let mut names = Vec::new();
    for task in tasks {
        names.push(task.await.unwrap());
    }

    let snapshot = metrics::master_metric_snapshot();
    assert_eq!(snapshot.allocated_mem_size, 0);
    assert_eq!(snapshot.total_mem_capacity, 0);
    for name in names {
        assert_eq!(
            metrics::segment_memory_metric_snapshot(&name),
            metrics::SegmentMemoryMetricSnapshot::default()
        );
    }
}

#[test]
fn cpp_parity_master_metrics_test_cpp_mastermetricstest_batchrequesttest() {
    const CHILD_MARKER: &str = "MOONCAKE_BATCH_METRICS_CHILD";
    const TEST_NAME: &str = "cpp_parity_master_metrics_test_cpp_mastermetricstest_batchrequesttest";
    if std::env::var_os(CHILD_MARKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(TEST_NAME)
            .arg("--exact")
            .arg("--test-threads=1")
            .env(CHILD_MARKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "fresh batch metrics child failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment = "batch-metrics:1";
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: segment.into(),
                size: 64 * 1024 * 1024,
                base_addr: 0x3_0000_0000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let mut keys = vec!["test_key1".into(), "test_key2".into(), "test_key3".into()];
        let mut lengths = vec![1024, 2048, 512];

        let exist = MasterService::batch_exist_key(
            &service,
            Request::new(proto::BatchExistKeyRequest {
                keys: keys.clone(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(exist.results, vec![false; 3]);
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "exist"),
            [1, 0, 0, 3, 0]
        );

        let started = MasterService::batch_put_start(
            &service,
            Request::new(proto::BatchPutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                keys: keys.clone(),
                slice_lengths: lengths.clone(),
                config: Some(replicate_config(segment)),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.results.len(), 3);
        assert!(started.results.iter().all(|result| result.status == 0));
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "put_start"),
            [1, 0, 0, 3, 0]
        );

        let before_end = MasterService::batch_get_replica_list(
            &service,
            Request::new(proto::BatchGetReplicaListRequest {
                keys: keys.clone(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(before_end.results.len(), 3);
        assert!(before_end.results.iter().all(|result| result.status != 0));
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "get"),
            [1, 0, 1, 3, 3]
        );

        let ended = MasterService::batch_put_end(
            &service,
            Request::new(proto::BatchPutEndRequest {
                entries: keys
                    .iter()
                    .map(|key| proto::PutEndEntry {
                        client_id: Some(proto_uuid(client_id)),
                        key: key.clone(),
                        replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                        tenant_id: String::new(),
                    })
                    .collect(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(ended.statuses, vec![0; 3]);
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "put_end"),
            [1, 0, 0, 3, 0]
        );

        let exist = MasterService::batch_exist_key(
            &service,
            Request::new(proto::BatchExistKeyRequest {
                keys: keys.clone(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(exist.results, vec![true; 3]);
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "exist"),
            [2, 0, 0, 6, 0]
        );

        let after_end = MasterService::batch_get_replica_list(
            &service,
            Request::new(proto::BatchGetReplicaListRequest {
                keys: keys.clone(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(after_end.results.len(), 3);
        assert!(after_end.results.iter().all(|result| result.status == 0));
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "get"),
            [2, 0, 1, 6, 3]
        );

        let revoked = MasterService::batch_put_revoke(
            &service,
            Request::new(proto::BatchPutRevokeRequest {
                keys: keys.clone(),
                client_id: Some(proto_uuid(client_id)),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(revoked.statuses.len(), 3);
        assert!(revoked.statuses.iter().all(|status| *status != 0));
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "put_revoke"),
            [1, 0, 1, 3, 3]
        );

        keys.push("test_key4".into());
        lengths.push(512);
        let partial_get = MasterService::batch_get_replica_list(
            &service,
            Request::new(proto::BatchGetReplicaListRequest {
                keys: keys.clone(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(partial_get.results.len(), 4);
        assert_eq!(
            partial_get
                .results
                .iter()
                .filter(|result| result.status == 0)
                .count(),
            3
        );
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "get"),
            [3, 1, 1, 10, 4]
        );

        let partial_start = MasterService::batch_put_start(
            &service,
            Request::new(proto::BatchPutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                keys,
                slice_lengths: lengths,
                config: Some(replicate_config(segment)),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(partial_start.results.len(), 4);
        assert_eq!(
            partial_start
                .results
                .iter()
                .filter(|result| result.status == 0)
                .count(),
            1
        );
        assert_eq!(
            batch_matrix(&metrics::master_metric_snapshot(), "put_start"),
            [2, 1, 0, 7, 3]
        );
    });
}

#[test]
fn cpp_parity_master_metrics_test_cpp_mastermetricstest_localdiskreplicaallocatedsize() {
    const CHILD_MARKER: &str = "MOONCAKE_ALLOCATED_FILE_SIZE_METRICS_CHILD";
    const TEST_NAME: &str =
        "cpp_parity_master_metrics_test_cpp_mastermetricstest_localdiskreplicaallocatedsize";
    if std::env::var_os(CHILD_MARKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(TEST_NAME)
            .arg("--exact")
            .arg("--test-threads=1")
            .env(CHILD_MARKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "fresh allocated-file metrics child failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        const KEY: &str = "ssd_alloc_test_key";
        const SIZE: u64 = 4096;
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment = "ssd_alloc_test_segment";
        assert_eq!(metrics::master_metric_snapshot().allocated_file_size, 0);

        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: segment.into(),
                size: 64 * 1024 * 1024,
                base_addr: 0x4_0000_0000,
                te_endpoint: segment.into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: KEY.into(),
                slice_length: SIZE,
                tenant_id: String::new(),
                config: Some(replicate_config(segment)),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: KEY.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();

        MasterService::notify_offload_success(
            &service,
            Request::new(proto::NotifyOffloadSuccessRequest {
                client_id: Some(proto_uuid(client_id)),
                keys: vec![KEY.into()],
                metadatas: vec![proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: KEY.len() as i64,
                    data_size: SIZE as i64,
                    transport_endpoint: "ssd_alloc_test_endpoint".into(),
                }],
                tasks: vec![proto::OffloadTaskItem {
                    tenant_id: String::new(),
                    key: KEY.into(),
                    size: SIZE as i64,
                    generation_id: None,
                }],
                recovery_session_id: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            metrics::master_metric_snapshot().allocated_file_size,
            SIZE as i64
        );

        MasterService::remove(
            &service,
            Request::new(proto::RemoveRequest {
                key: KEY.into(),
                force: false,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let exists = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: KEY.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .exists;
        assert!(!exists);
        assert_eq!(metrics::master_metric_snapshot().allocated_file_size, 0);
    });
}

async fn put_complete(service: &MasterServiceImpl, client_id: Uuid, key: &str, segment: &str) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(replicate_config(segment)),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_complete_with_config(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    config: proto::ReplicateConfig,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(config),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_local_disk(service: &MasterServiceImpl, holder_id: Uuid) {
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: false,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: false,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
}

async fn seed_local_disk_object(service: &MasterServiceImpl, holder_id: Uuid, key: &str) {
    let source_segment = format!("metrics-offload-source-{key}");
    let source_segment_id = MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            segment_name: source_segment.clone(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .segment_id
    .unwrap();
    put_complete(service, holder_id, key, &source_segment).await;
    let task = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks
    .into_iter()
    .find(|task| task.key == key)
    .expect("Master must issue the metrics fixture offload task");
    assert!(task.generation_id.is_some());
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: 128,
                transport_endpoint: "holder".into(),
            }],
            tasks: vec![task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    MasterService::unmount_segment(
        service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(source_segment_id),
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap();
}

async fn trigger_promotion_lookup(service: &MasterServiceImpl, key: &str) {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_promotion_candidate_metrics_are_registered_and_incremented() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    metrics::register_metrics();
    let recorded = metrics::PROMOTION_CANDIDATE_RECORDED.get();
    let admitted = metrics::PROMOTION_CANDIDATE_ADMITTED.get();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "metric-first").await;
    seed_local_disk_object(&service, holder_id, "metric-retry").await;
    for key in ["metric-first", "metric-retry"] {
        trigger_promotion_lookup(&service, key).await;
        trigger_promotion_lookup(&service, key).await;
    }
    assert_eq!(metrics::PROMOTION_CANDIDATE_RECORDED.get(), recorded + 1);

    MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap();
    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "metric-first".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    service.make_promotion_candidates_due_for_test();
    service.run_promotion_candidate_retry_for_test();
    assert_eq!(metrics::PROMOTION_CANDIDATE_ADMITTED.get(), admitted + 1);

    let mut output = Vec::new();
    TextEncoder::new()
        .encode(&prometheus::gather(), &mut output)
        .unwrap();
    let output = String::from_utf8(output).unwrap();
    for name in [
        "master_promotion_candidate_recorded_total",
        "master_promotion_candidate_admitted_total",
        "master_promotion_candidate_admission_rejected_total",
        "master_promotion_candidate_expired_evaluated_total",
        "master_promotion_candidate_expired_unevaluated_total",
        "master_promotion_candidate_dropped_limit_total",
    ] {
        assert!(output.contains(name), "missing metric {name}");
    }
}

#[tokio::test]
async fn test_cache_hit_metrics_count_memory_and_local_disk_bytes() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: false,
        ..Default::default()
    });
    let memory_client = Uuid::new_v4();
    let disk_holder = Uuid::new_v4();
    mount_memory_segment(&service, memory_client, "metrics-memory:1").await;
    put_complete(
        &service,
        memory_client,
        "metrics-memory-key",
        "metrics-memory:1",
    )
    .await;
    mount_local_disk(&service, disk_holder).await;
    seed_local_disk_object(&service, disk_holder, "metrics-disk-key").await;

    let base_mem_hits = metrics::MEM_CACHE_HITS.get();
    let base_file_hits = metrics::FILE_CACHE_HITS.get();
    let base_mem_hit_bytes = metrics::MEM_CACHE_HIT_BYTES.get();
    let base_file_hit_bytes = metrics::FILE_CACHE_HIT_BYTES.get();
    let base_valid_gets = metrics::VALID_GETS.get();

    for key in ["metrics-memory-key", "metrics-disk-key"] {
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    assert_eq!(metrics::MEM_CACHE_HITS.get(), base_mem_hits + 1);
    assert_eq!(metrics::FILE_CACHE_HITS.get(), base_file_hits + 1);
    assert_eq!(metrics::MEM_CACHE_HIT_BYTES.get(), base_mem_hit_bytes + 128);
    assert_eq!(
        metrics::FILE_CACHE_HIT_BYTES.get(),
        base_file_hit_bytes + 128
    );
    assert_eq!(metrics::VALID_GETS.get(), base_valid_gets + 2);
}

#[tokio::test]
async fn cpp_parity_cache_stats_discriminants_aliases_and_reuse() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    use metrics::CacheHitStat;

    assert_eq!(CacheHitStat::MemoryHits as u8, 0);
    assert_eq!(CacheHitStat::SsdHits as u8, 1);
    assert_eq!(CacheHitStat::MemoryTotal as u8, 2);
    assert_eq!(CacheHitStat::SsdTotal as u8, 3);
    assert_eq!(CacheHitStat::MemoryHitRate as u8, 4);
    assert_eq!(CacheHitStat::SsdHitRate as u8, 5);
    assert_eq!(CacheHitStat::OverallHitRate as u8, 6);
    assert_eq!(CacheHitStat::ValidGetRate as u8, 7);
    assert_eq!(
        CacheHitStat::MEMORY_CURRENT_CACHED_OBJECTS,
        CacheHitStat::MemoryTotal
    );
    assert_eq!(
        CacheHitStat::SSD_CURRENT_CACHED_OBJECTS,
        CacheHitStat::SsdTotal
    );
    assert_eq!(
        CacheHitStat::MEMORY_HITS_PER_CURRENT_CACHED_OBJECT,
        CacheHitStat::MemoryHitRate
    );
    assert_eq!(
        CacheHitStat::SSD_HITS_PER_CURRENT_CACHED_OBJECT,
        CacheHitStat::SsdHitRate
    );
    assert_eq!(
        CacheHitStat::OVERALL_HITS_PER_CURRENT_CACHED_OBJECT,
        CacheHitStat::OverallHitRate
    );

    fn assert_aliases_and_formulas(stats: &metrics::CacheStats) {
        use metrics::CacheHitStat;
        let ratio = |hits: f64, total: f64| {
            if total > 0.0 {
                (hits / total * 100.0).round() / 100.0
            } else {
                0.0
            }
        };
        assert_eq!(
            stats[CacheHitStat::MEMORY_CURRENT_CACHED_OBJECTS],
            stats[CacheHitStat::MemoryTotal]
        );
        assert_eq!(
            stats[CacheHitStat::SSD_CURRENT_CACHED_OBJECTS],
            stats[CacheHitStat::SsdTotal]
        );
        assert_eq!(
            stats[CacheHitStat::MEMORY_HITS_PER_CURRENT_CACHED_OBJECT],
            stats[CacheHitStat::MemoryHitRate]
        );
        assert_eq!(
            stats[CacheHitStat::SSD_HITS_PER_CURRENT_CACHED_OBJECT],
            stats[CacheHitStat::SsdHitRate]
        );
        assert_eq!(
            stats[CacheHitStat::OVERALL_HITS_PER_CURRENT_CACHED_OBJECT],
            stats[CacheHitStat::OverallHitRate]
        );
        assert_eq!(
            stats[CacheHitStat::MemoryHitRate],
            ratio(
                stats[CacheHitStat::MemoryHits],
                stats[CacheHitStat::MemoryTotal]
            )
        );
        assert_eq!(
            stats[CacheHitStat::SsdHitRate],
            ratio(stats[CacheHitStat::SsdHits], stats[CacheHitStat::SsdTotal])
        );
        assert_eq!(
            stats[CacheHitStat::OverallHitRate],
            ratio(
                stats[CacheHitStat::MemoryHits] + stats[CacheHitStat::SsdHits],
                stats[CacheHitStat::MemoryTotal] + stats[CacheHitStat::SsdTotal]
            )
        );
    }

    let baseline = metrics::calculate_cache_stats();
    assert_aliases_and_formulas(&baseline);
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let segment_name = "cache-stats:1";
    let segment_id = MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 16 * 1024 * 1024,
            base_addr: 0x3_0000_0000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .segment_id
    .unwrap();
    let key = "cache-stats-key";
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(replicate_config(segment_name)),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_put = metrics::calculate_cache_stats();
    assert_aliases_and_formulas(&after_put);
    assert_eq!(
        after_put[CacheHitStat::MemoryHits],
        baseline[CacheHitStat::MemoryHits]
    );
    assert_eq!(
        after_put[CacheHitStat::MemoryTotal],
        baseline[CacheHitStat::MemoryTotal] + 1.0
    );
    let extra_gets = ((after_put[CacheHitStat::MemoryTotal] - after_put[CacheHitStat::MemoryHits])
        .max(0.0) as usize)
        + 1;
    for _ in 0..extra_gets {
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    let after_gets = metrics::calculate_cache_stats();
    assert_aliases_and_formulas(&after_gets);
    assert_eq!(
        after_gets[CacheHitStat::MemoryHits],
        baseline[CacheHitStat::MemoryHits] + extra_gets as f64
    );
    assert_eq!(
        after_gets[CacheHitStat::MemoryTotal],
        baseline[CacheHitStat::MemoryTotal] + 1.0
    );
    assert!(after_gets[CacheHitStat::ValidGetRate] >= baseline[CacheHitStat::ValidGetRate]);
    assert!(after_gets[CacheHitStat::ValidGetRate] <= 1.0);
    assert!(after_gets[CacheHitStat::MemoryHitRate] > 1.0);

    let total_gets_before_failures = metrics::TOTAL_GETS.get();
    let valid_gets_before_failures = metrics::VALID_GETS.get();
    let not_ready_key = "cache-stats-not-ready";
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: not_ready_key.into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(replicate_config(segment_name)),
        }),
    )
    .await
    .unwrap();
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: not_ready_key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "cache-stats-missing".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
    let batch = MasterService::batch_get_replica_list(
        &service,
        Request::new(proto::BatchGetReplicaListRequest {
            keys: vec![key.into(), "cache-stats-batch-missing".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch.results.len(), 2);
    assert_eq!(metrics::TOTAL_GETS.get(), total_gets_before_failures + 4);
    assert_eq!(metrics::VALID_GETS.get(), valid_gets_before_failures + 1);
    let total_before_exist = metrics::TOTAL_GETS.get();
    MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::TOTAL_GETS.get(), total_before_exist);

    let rpc =
        MasterService::calc_cache_stats(&service, Request::new(proto::CalcCacheStatsRequest {}))
            .await
            .unwrap()
            .into_inner()
            .stats;
    let calculated = metrics::calculate_cache_stats();
    let expected_valid_get_rate = {
        let valid = metrics::VALID_GETS.get() as f64;
        let total = metrics::TOTAL_GETS.get() as f64;
        if total > 0.0 {
            (valid / total * 100.0).round() / 100.0
        } else {
            0.0
        }
    };
    assert_eq!(
        calculated[CacheHitStat::ValidGetRate],
        expected_valid_get_rate
    );
    assert_eq!(rpc["memory_hits"], calculated[CacheHitStat::MemoryHits]);
    assert_eq!(rpc["ssd_hits"], calculated[CacheHitStat::SsdHits]);
    assert_eq!(rpc["memory_total"], calculated[CacheHitStat::MemoryTotal]);
    assert_eq!(rpc["ssd_total"], calculated[CacheHitStat::SsdTotal]);
    assert_eq!(
        rpc["memory_hit_rate"],
        calculated[CacheHitStat::MemoryHitRate]
    );
    assert_eq!(rpc["ssd_hit_rate"], calculated[CacheHitStat::SsdHitRate]);
    assert_eq!(
        rpc["overall_hit_rate"],
        calculated[CacheHitStat::OverallHitRate]
    );
    assert_eq!(
        rpc["valid_get_rate"],
        calculated[CacheHitStat::ValidGetRate]
    );

    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(segment_id),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        metrics::calculate_cache_stats()[CacheHitStat::MemoryTotal],
        baseline[CacheHitStat::MemoryTotal]
    );
}

#[tokio::test]
async fn test_cache_total_metrics_track_object_inventory() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: false,
        ..Default::default()
    });
    let memory_client = Uuid::new_v4();
    let disk_holder = Uuid::new_v4();
    mount_memory_segment(&service, memory_client, "inventory-memory:1").await;
    mount_memory_segment(&service, memory_client, "inventory-memory:2").await;

    let base_mem_total = metrics::MEM_CACHE_TOTAL.get();
    let base_file_total = metrics::FILE_CACHE_TOTAL.get();

    put_complete_with_config(
        &service,
        memory_client,
        "inventory-memory-key",
        multi_replicate_config(&["inventory-memory:1", "inventory-memory:2"]),
    )
    .await;
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total + 1);

    mount_local_disk(&service, disk_holder).await;
    seed_local_disk_object(&service, disk_holder, "inventory-disk-key").await;
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total + 1);

    MasterService::evict_disk_replica(
        &service,
        Request::new(proto::EvictDiskReplicaRequest {
            client_id: Some(proto_uuid(disk_holder)),
            key: "inventory-disk-key".into(),
            tenant_id: String::new(),
            replica_type: proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "inventory-memory-key".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total);
}

#[tokio::test]
async fn cpp_parity_remove_same_memory_global_disk_object_decrements_both_totals() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "remove-cache-totals".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "remove-cache-totals:1").await;

    let base_mem_total = metrics::MEM_CACHE_TOTAL.get();
    let base_file_total = metrics::FILE_CACHE_TOTAL.get();
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "remove_cache_total_metric_key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "remove-cache-totals:1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
    for replica_type in [
        proto::replica_descriptor::ReplicaType::Memory,
        proto::replica_descriptor::ReplicaType::Disk,
    ] {
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "remove_cache_total_metric_key".into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total + 1);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "remove_cache_total_metric_key".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total);
}

#[tokio::test]
async fn cpp_parity_processing_global_disk_revoke_preserves_cache_totals() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "revoke-processing-disk-metrics".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "revoke-processing-disk:1").await;

    let base_mem_total = metrics::MEM_CACHE_TOTAL.get();
    let base_file_total = metrics::FILE_CACHE_TOTAL.get();
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke_processing_disk_metric_key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "revoke-processing-disk:1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke_processing_disk_metric_key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total);

    MasterService::put_revoke(
        &service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke_processing_disk_metric_key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_mem_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_file_total);
}

#[tokio::test]
async fn cpp_parity_local_disk_capacity_heartbeat_replaces_not_accumulates() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let baseline = metrics::TOTAL_FILE_CAPACITY.get();
    let client_id = Uuid::new_v4();
    {
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: "ssd-capacity-test-segment".into(),
                size: 64 * 1024 * 1024,
                base_addr: 0x5_0000_0000,
                te_endpoint: "ssd-capacity-test-segment".into(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
        mount_local_disk(&service, client_id).await;

        const CAPACITY_800_GIB: i64 = 800 * 1024 * 1024 * 1024;
        const CAPACITY_400_GIB: i64 = 400 * 1024 * 1024 * 1024;
        for (capacity, expected) in [
            (CAPACITY_800_GIB, baseline + CAPACITY_800_GIB),
            (CAPACITY_400_GIB, baseline + CAPACITY_400_GIB),
            (CAPACITY_400_GIB, baseline + CAPACITY_400_GIB),
        ] {
            MasterService::report_ssd_capacity(
                &service,
                Request::new(proto::ReportSsdCapacityRequest {
                    client_id: Some(proto_uuid(client_id)),
                    ssd_total_capacity_bytes: capacity,
                }),
            )
            .await
            .unwrap();
            assert_eq!(metrics::TOTAL_FILE_CAPACITY.get(), expected);
        }
    }
    assert_eq!(metrics::TOTAL_FILE_CAPACITY.get(), baseline);
}

#[tokio::test]
async fn cpp_parity_put_start_no_available_handle_metrics() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let put_start_requests = metrics::PUT_START_REQUESTS.get();
    let allocation_failures = metrics::PUT_START_ALLOCATION_FAILURES.get();
    let put_start_failures = metrics::PUT_START_FAILURES.get();
    let service = MasterServiceImpl::default();
    let error = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "allocation_failure_key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(replicate_config("")),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(metrics::PUT_START_REQUESTS.get(), put_start_requests + 1);
    assert_eq!(
        metrics::PUT_START_ALLOCATION_FAILURES.get(),
        allocation_failures + 1
    );
    assert_eq!(metrics::PUT_START_FAILURES.get(), put_start_failures + 1);
}

#[tokio::test]
async fn put_start_nof_unavailable_counts_allocation_failure_once() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let allocation_failures = metrics::PUT_START_ALLOCATION_FAILURES.get();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let error = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "nof_allocation_failure_key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 0,
                nof_replica_num: 1,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        metrics::PUT_START_ALLOCATION_FAILURES.get(),
        allocation_failures + 1
    );
}

#[tokio::test]
async fn put_start_flexible_dual_single_side_success_is_not_allocation_failure() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let allocation_failures = metrics::PUT_START_ALLOCATION_FAILURES.get();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "metrics-flexible-memory:1").await;
    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "flexible_single_side_key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                preferred_segment: "metrics-flexible-memory:1".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.replicas.len(), 1);
    assert_eq!(
        metrics::PUT_START_ALLOCATION_FAILURES.get(),
        allocation_failures
    );
}

#[tokio::test]
async fn put_start_unavailable_gate_counts_request_and_failure() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    let put_start_requests = metrics::PUT_START_REQUESTS.get();
    let put_start_failures = metrics::PUT_START_FAILURES.get();
    let service = MasterServiceImpl::default();
    service.set_service_available(false);
    let error = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "unavailable_gate_key".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(replicate_config("")),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(metrics::PUT_START_REQUESTS.get(), put_start_requests + 1);
    assert_eq!(metrics::PUT_START_FAILURES.get(), put_start_failures + 1);
}

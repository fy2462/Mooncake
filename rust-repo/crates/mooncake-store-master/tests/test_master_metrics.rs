mod common;

use common::proto_uuid;
use mooncake_store_master::metrics;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use prometheus::{Encoder, TextEncoder};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
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

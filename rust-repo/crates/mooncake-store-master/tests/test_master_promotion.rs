use mooncake_store_core::ReplicaType;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

fn uuid_proto(id: Uuid) -> proto::Uuid {
    proto::Uuid {
        high: id.as_u64_pair().0,
        low: id.as_u64_pair().1,
    }
}

async fn mount_local_disk(service: &MasterServiceImpl, holder_id: Uuid) {
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(uuid_proto(holder_id)),
            enable_offloading: false,
            storage_id: Some(uuid_proto(storage_id)),
            recovery_complete: false,
            recovery_session_id: Some(uuid_proto(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(uuid_proto(holder_id)),
            enable_offloading: true,
            storage_id: Some(uuid_proto(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(uuid_proto(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
}

async fn seed_local_disk_object(
    service: &MasterServiceImpl,
    holder_id: Uuid,
    key: &str,
    size: u64,
) {
    static NEXT_SOURCE_BASE: AtomicU64 = AtomicU64::new(0x2_0000_0000);
    let source_segment = format!("promotion-source-{key}");
    let source_segment_id = MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(uuid_proto(holder_id)),
            segment_name: source_segment.clone(),
            size: 4096,
            base_addr: NEXT_SOURCE_BASE.fetch_add(0x1_0000, Ordering::Relaxed),
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
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(uuid_proto(holder_id)),
            key: key.into(),
            slice_length: size,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: source_segment,
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(uuid_proto(holder_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let task = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(uuid_proto(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks
    .into_iter()
    .find(|task| task.key == key)
    .expect("Master must issue the promotion fixture offload task");
    assert!(task.generation_id.is_some());
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(uuid_proto(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: size as i64,
                transport_endpoint: "holder-endpoint".into(),
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
            client_id: Some(uuid_proto(holder_id)),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_promotion_flow_success_and_failure() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    let dram_client = Uuid::new_v4();

    mount_local_disk(&service, holder_id).await;
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: dram_client.as_u64_pair().0,
                low: dram_client.as_u64_pair().1,
            }),
            segment_name: "dram-a".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for key in ["promo-ok", "promo-fail"] {
        seed_local_disk_object(&service, holder_id, key, 256).await;

        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            service.replica_refcnts_for_test(key, ReplicaType::LocalDisk, ""),
            vec![1]
        );
    }

    let first_heartbeat = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first_heartbeat.objects.len(), 1);
    let first_key = first_heartbeat.objects.keys().next().unwrap().clone();

    let second_heartbeat = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second_heartbeat.objects.len(), 1);
    let second_key = second_heartbeat.objects.keys().next().unwrap().clone();
    assert_ne!(first_key, second_key);

    let alloc = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: first_key.clone(),
            size: 256,
            preferred_segments: vec!["dram-a".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        alloc.memory_descriptor.as_ref().unwrap().segment_name,
        "dram-a"
    );

    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: first_key.clone(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let promoted = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: first_key.clone(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(promoted.replicas.iter().any(|replica| replica.replica_type
        == proto::replica_descriptor::ReplicaType::Memory as i32
        && replica.status == proto::replica_descriptor::ReplicaStatus::Complete as i32));
    assert_eq!(
        service.replica_refcnts_for_test(&first_key, ReplicaType::LocalDisk, ""),
        vec![0]
    );

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: second_key.clone(),
            size: 256,
            preferred_segments: vec!["dram-a".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: second_key.clone(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service.replica_refcnts_for_test(&second_key, ReplicaType::LocalDisk, ""),
        vec![0]
    );

    let failed = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: second_key.clone(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        failed
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        0
    );
}

#[tokio::test]
async fn test_promotion_admission_threshold_requires_multiple_reads() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 2,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    mount_local_disk(&service, holder_id).await;
    seed_local_disk_object(&service, holder_id, "threshold-key", 256).await;

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "threshold-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let first = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(first.objects.is_empty());

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "threshold-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let second = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second.objects.get("threshold-key"), Some(&256));
}

#[tokio::test]
async fn test_promotion_queue_limit_released_after_success() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    mount_local_disk(&service, holder_id).await;
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            segment_name: "limit-dram".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for key in ["limit-a", "limit-b"] {
        seed_local_disk_object(&service, holder_id, key, 128).await;
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

    let first_tick = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first_tick.objects.len(), 1);
    assert!(first_tick.objects.contains_key("limit-a"));

    let second_tick = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(second_tick.objects.is_empty());

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "limit-a".into(),
            size: 128,
            preferred_segments: vec!["limit-dram".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "limit-a".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "limit-b".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let third_tick = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(third_tick.objects.get("limit-b"), Some(&128));
}

#[tokio::test]
async fn test_promotion_reaper_resets_deadline_and_releases_staged_buffer() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        put_start_release_timeout: Duration::from_millis(120),
        reaper_interval: Duration::from_millis(20),
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    mount_local_disk(&service, holder_id).await;
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            segment_name: "reaper-dram".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    seed_local_disk_object(&service, holder_id, "reaper-key", 256).await;

    let baseline = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "reaper-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;
    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "reaper-key".into(),
            size: 256,
            preferred_segments: vec!["reaper-dram".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_alloc = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;
    assert!(after_alloc > baseline);

    tokio::time::sleep(Duration::from_millis(70)).await;
    let still_allocated = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;
    assert!(still_allocated > baseline);

    tokio::time::sleep(Duration::from_millis(120)).await;
    let after_reap = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;
    assert_eq!(after_reap, baseline);

    let alloc_after_reap = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "reaper-key".into(),
            size: 256,
            preferred_segments: vec!["reaper-dram".into()],
            tenant_id: String::new(),
        }),
    )
    .await;
    assert!(alloc_after_reap.is_err());
}

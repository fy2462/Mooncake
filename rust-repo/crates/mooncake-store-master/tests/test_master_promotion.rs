use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn test_promotion_flow_success_and_failure() {
    let service = MasterServiceImpl::default();
    let holder_id = Uuid::new_v4();
    let dram_client = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: dram_client.as_u64_pair().0,
                low: dram_client.as_u64_pair().1,
            }),
            segment_name: "dram-a".into(),
            size: 4096,
            base_addr: 0,
        }),
    )
    .await
    .unwrap();

    for key in ["promo-ok", "promo-fail"] {
        MasterService::notify_offload_success(
            &service,
            Request::new(proto::NotifyOffloadSuccessRequest {
                client_id: Some(proto::Uuid {
                    high: holder_id.as_u64_pair().0,
                    low: holder_id.as_u64_pair().1,
                }),
                keys: vec![key.into()],
                metadatas: vec![proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: key.len() as i64,
                    data_size: 256,
                    transport_endpoint: "holder-endpoint".into(),
                }],
            }),
        )
        .await
        .unwrap();

        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest { key: key.into() }),
        )
        .await
        .unwrap();
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
        }),
    )
    .await
    .unwrap();

    let promoted = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest { key: first_key }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(promoted.replicas.iter().any(|replica| replica.replica_type
        == proto::replica_descriptor::ReplicaType::Memory as i32
        && replica.status == proto::replica_descriptor::ReplicaStatus::Complete as i32));

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
        }),
    )
    .await
    .unwrap();

    let failed = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest { key: second_key }),
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
        promotion_admission_threshold: 2,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            keys: vec!["threshold-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 13,
                data_size: 256,
                transport_endpoint: "holder-threshold".into(),
            }],
        }),
    )
    .await
    .unwrap();

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "threshold-key".into(),
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
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            segment_name: "limit-dram".into(),
            size: 4096,
            base_addr: 0,
        }),
    )
    .await
    .unwrap();

    for key in ["limit-a", "limit-b"] {
        MasterService::notify_offload_success(
            &service,
            Request::new(proto::NotifyOffloadSuccessRequest {
                client_id: Some(proto::Uuid {
                    high: holder_id.as_u64_pair().0,
                    low: holder_id.as_u64_pair().1,
                }),
                keys: vec![key.into()],
                metadatas: vec![proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: key.len() as i64,
                    data_size: 128,
                    transport_endpoint: "holder-limit".into(),
                }],
            }),
        )
        .await
        .unwrap();
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest { key: key.into() }),
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
        }),
    )
    .await
    .unwrap();

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "limit-b".into(),
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
        put_start_release_timeout: Duration::from_millis(120),
        reaper_interval: Duration::from_millis(20),
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            segment_name: "reaper-dram".into(),
            size: 4096,
            base_addr: 0,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            keys: vec!["reaper-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 10,
                data_size: 256,
                transport_endpoint: "holder-reaper".into(),
            }],
        }),
    )
    .await
    .unwrap();

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
        }),
    )
    .await;
    assert!(alloc_after_reap.is_err());
}

use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn test_offload_on_evict_keeps_one_memory_replica_and_queues_local_disk_work() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    for segment_name in ["evict-a", "evict-b"] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: segment_name.into(),
                size: 4096,
            }),
        )
        .await
        .unwrap();
    }
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "evict-offload".into(),
            slice_length: 256,
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
                prefer_alloc_in_same_node: false, preferred_segments: vec![], preferred_nof_segments: vec![], data_type: proto::ObjectDataType::Unknown as i32, 
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(put.replicas.len(), 2);
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "evict-offload".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["evict-offload".to_string()]);

    let offload = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(offload.objects.get("evict-offload"), Some(&256));

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "evict-offload".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_offload_on_evict_drops_memory_when_local_disk_already_exists() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "evict-localdisk".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "already-offloaded".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "evict-localdisk".into(),
                prefer_alloc_in_same_node: false, preferred_segments: vec![], preferred_nof_segments: vec![], data_type: proto::ObjectDataType::Unknown as i32, 
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "already-offloaded".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            keys: vec!["already-offloaded".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 17,
                data_size: 128,
                transport_endpoint: "holder-existing".into(),
            }],
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["already-offloaded".to_string()]);

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "already-offloaded".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        0
    );
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::LocalDisk as i32)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_background_eviction_worker_triggers_offload_on_high_watermark() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        eviction_interval: Duration::from_millis(10),
        eviction_high_watermark_ratio: 0.1,
        eviction_ratio: 0.05,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    for segment_name in ["bg-evict-a", "bg-evict-b"] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: segment_name.into(),
                size: 4096,
            }),
        )
        .await
        .unwrap();
    }
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "bg-evict-offload".into(),
            slice_length: 512,
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
                prefer_alloc_in_same_node: false, preferred_segments: vec![], preferred_nof_segments: vec![], data_type: proto::ObjectDataType::Unknown as i32, 
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "bg-evict-offload".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    let offload = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(offload.objects.get("bg-evict-offload"), Some(&512));

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "bg-evict-offload".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        1
    );
}

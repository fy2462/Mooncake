use mooncake_store_core::ReplicaType;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::Request;
use uuid::Uuid;

fn uuid_proto(id: Uuid) -> proto::Uuid {
    proto::Uuid {
        high: id.as_u64_pair().0,
        low: id.as_u64_pair().1,
    }
}

fn replicate_config(preferred_segment: &str) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: preferred_segment.into(),
        prefer_alloc_in_same_node: false,
        preferred_segments: vec![],
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
    }
}

async fn mount_memory_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
    size: u64,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(uuid_proto(client_id)),
            segment_name: segment_name.into(),
            size,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_local_disk(service: &MasterServiceImpl, client_id: Uuid) {
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
}

async fn put_complete(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    preferred_segment: &str,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(uuid_proto(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(replicate_config(preferred_segment)),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(uuid_proto(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_offload_object_heartbeat_and_notify_offload_success() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
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
            segment_name: "mem-a".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
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
            key: "offload-key".into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "mem-a".into(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
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
            key: "offload-key".into(),
            replica_type: 0,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service.replica_refcnts_for_test("offload-key", ReplicaType::Memory, ""),
        vec![1]
    );

    let heartbeat = MasterService::offload_object_heartbeat(
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
    assert_eq!(heartbeat.objects.get("offload-key"), Some(&256));
    assert_eq!(
        heartbeat.tasks,
        vec![proto::OffloadTaskItem {
            tenant_id: "default".into(),
            key: "offload-key".into(),
            size: 256,
        }]
    );

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            keys: vec!["offload-key".into(), "disk-only-key".into()],
            metadatas: vec![
                proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: "offload-key".len() as i64,
                    data_size: 256,
                    transport_endpoint: "holder-a".into(),
                },
                proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: "disk-only-key".len() as i64,
                    data_size: 512,
                    transport_endpoint: "holder-a".into(),
                },
            ],
            tasks: vec![],
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service.replica_refcnts_for_test("offload-key", ReplicaType::Memory, ""),
        vec![0]
    );

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            keys: vec!["tenant-disk-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: "tenant-disk-key".len() as i64,
                data_size: 1024,
                transport_endpoint: "holder-a".into(),
            }],
            tasks: vec![proto::OffloadTaskItem {
                tenant_id: "tenant-a".into(),
                key: "tenant-disk-key".into(),
                size: 1024,
            }],
        }),
    )
    .await
    .unwrap();

    let tenant_replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "tenant-disk-key".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(tenant_replicas.replicas.len(), 1);

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "disk-only-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);
    assert_eq!(
        replicas.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::LocalDisk as i32
    );
    assert_eq!(
        replicas.replicas[0].holder_client_id.as_ref().unwrap().high,
        client_id.as_u64_pair().0
    );
}

#[tokio::test]
async fn test_notify_offload_negative_size_is_nack_without_local_disk_replica() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "nack-mem", 4096).await;
    mount_local_disk(&service, client_id).await;
    put_complete(&service, client_id, "nack-key", 256, "nack-mem").await;

    assert_eq!(
        service.replica_refcnts_for_test("nack-key", ReplicaType::Memory, ""),
        vec![1]
    );
    let heartbeat = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(heartbeat.tasks.len(), 1);

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(uuid_proto(client_id)),
            keys: vec![],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: "nack-key".len() as i64,
                data_size: -1,
                transport_endpoint: "holder-a".into(),
            }],
            tasks: heartbeat.tasks,
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        service.replica_refcnts_for_test("nack-key", ReplicaType::Memory, ""),
        vec![0]
    );
    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "nack-key".into(),
            tenant_id: String::new(),
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
                == proto::replica_descriptor::ReplicaType::LocalDisk as i32)
            .count(),
        0
    );
}

#[tokio::test]
async fn test_offload_on_evict_respects_queue_limit_and_cap_ratio() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        offload_on_evict: true,
        offloading_queue_limit: 2,
        offload_cap_ratio: 0.5,
        lease_ttl: std::time::Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "cap-mem", 4096).await;
    mount_local_disk(&service, client_id).await;
    for key in ["cap-a", "cap-b"] {
        put_complete(&service, client_id, key, 128, "cap-mem").await;
    }

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let evicted = service.run_eviction_cycle_for_test(2);
    assert_eq!(evicted.len(), 0);

    let heartbeat = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(heartbeat.tasks.len(), 1);
}

#[tokio::test]
async fn test_automatic_eviction_excludes_disk_only_objects_from_target_base() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        eviction_high_watermark_ratio: 0.49,
        eviction_ratio: 0.10,
        lease_ttl: std::time::Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "auto-evict-mem", 1024).await;
    mount_local_disk(&service, client_id).await;

    for key in ["auto-mem-a", "auto-mem-b"] {
        put_complete(&service, client_id, key, 256, "auto-evict-mem").await;
    }

    let memory_keys = vec!["auto-mem-a".to_string(), "auto-mem-b".to_string()];
    let disk_keys = (0..8)
        .map(|idx| format!("auto-disk-only-{idx}"))
        .collect::<Vec<_>>();
    let mut offload_keys = memory_keys.clone();
    offload_keys.extend(disk_keys.clone());
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(uuid_proto(client_id)),
            keys: offload_keys.clone(),
            metadatas: offload_keys
                .iter()
                .map(|key| proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: key.len() as i64,
                    data_size: if key.starts_with("auto-mem-") {
                        256
                    } else {
                        128
                    },
                    transport_endpoint: "holder-a".into(),
                })
                .collect(),
            tasks: vec![],
        }),
    )
    .await
    .unwrap();
    for key in &memory_keys {
        assert_eq!(
            service.replica_refcnts_for_test(key, ReplicaType::Memory, ""),
            vec![0]
        );
    }

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let evicted = service.run_automatic_eviction_once_for_test();
    assert_eq!(evicted.len(), 1);

    let mut memory_replicas = 0;
    for key in memory_keys {
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas;
        memory_replicas += replicas
            .iter()
            .filter(|replica| {
                replica.replica_type == proto::replica_descriptor::ReplicaType::Memory as i32
            })
            .count();
    }
    assert_eq!(memory_replicas, 1);

    for key in disk_keys {
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas;
        assert_eq!(replicas.len(), 1);
        assert_eq!(
            replicas[0].replica_type,
            proto::replica_descriptor::ReplicaType::LocalDisk as i32
        );
    }
}

use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

fn uuid_proto(id: Uuid) -> proto::Uuid {
    proto::Uuid {
        high: id.as_u64_pair().0,
        low: id.as_u64_pair().1,
    }
}

async fn mount_local_disk(service: &MasterServiceImpl, client_id: Uuid) -> Uuid {
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    assert_ne!(storage_id, client_id);
    assert_ne!(recovery_session_id, client_id);
    assert_ne!(recovery_session_id, storage_id);
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(uuid_proto(client_id)),
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
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: true,
            storage_id: Some(uuid_proto(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(uuid_proto(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
    storage_id
}

async fn take_offload_tasks(
    service: &MasterServiceImpl,
    client_id: Uuid,
) -> Vec<proto::OffloadTaskItem> {
    MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks
}

async fn notify_offload_tasks(
    service: &MasterServiceImpl,
    client_id: Uuid,
    tasks: Vec<proto::OffloadTaskItem>,
    endpoint: &str,
) -> proto::NotifyOffloadSuccessResponse {
    let metadatas = tasks
        .iter()
        .map(|task| proto::StorageObjectMetadata {
            bucket_id: 0,
            offset: 0,
            key_size: task.key.len() as i64,
            data_size: task.size,
            transport_endpoint: endpoint.to_owned(),
        })
        .collect();
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(uuid_proto(client_id)),
            keys: tasks.iter().map(|task| task.key.clone()).collect(),
            metadatas,
            tasks,
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap()
    .into_inner()
}

#[tokio::test]
async fn test_offload_on_evict_keeps_one_memory_replica_and_queues_local_disk_work() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    let tenant_id = "tenant-a";

    for (index, segment_name) in ["evict-a", "evict-b"].into_iter().enumerate() {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_proto(client_id)),
                segment_name: segment_name.into(),
                size: 4096,
                base_addr: 0x100000000 + (index as u64 * 0x10000),
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    let _storage_id = mount_local_disk(&service, client_id).await;

    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "evict-offload".into(),
            slice_length: 256,
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(put.replicas.len(), 2);
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "evict-offload".into(),
            replica_type: 0,
            tenant_id: tenant_id.into(),
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
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(offload.objects.get("evict-offload"), Some(&256));
    assert_eq!(offload.tasks.len(), 1);
    assert_eq!(offload.tasks[0].tenant_id, "default");
    assert_eq!(offload.tasks[0].key, "evict-offload");
    assert_eq!(offload.tasks[0].size, 256);
    assert!(
        offload.tasks[0]
            .generation_id
            .as_ref()
            .is_some_and(|generation_id| generation_id.high != 0 || generation_id.low != 0)
    );

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "evict-offload".into(),
            tenant_id: tenant_id.into(),
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
        enable_offload: true,
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(uuid_proto(client_id)),
            segment_name: "evict-localdisk".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let storage_id = mount_local_disk(&service, client_id).await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "already-offloaded".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "evict-localdisk".into(),
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
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "already-offloaded".into(),
            replica_type: 0,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    // The first cycle admits the authoritative offload task but preserves the
    // only Memory replica until LocalDisk completion is reported.
    assert!(service.run_eviction_cycle_for_test(1).is_empty());
    let tasks = take_offload_tasks(&service, client_id).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].key, "already-offloaded");
    let generation_id = tasks[0]
        .generation_id
        .clone()
        .filter(|generation_id| generation_id.high != 0 || generation_id.low != 0)
        .expect("Master must issue a non-nil LocalDisk generation");
    let response = notify_offload_tasks(&service, client_id, tasks, "holder-existing").await;
    assert!(response.stale_recovery_tasks.is_empty());

    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["already-offloaded".to_string()]);

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "already-offloaded".into(),
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
    let local_disk = replicas
        .replicas
        .iter()
        .find(|replica| {
            replica.replica_type == proto::replica_descriptor::ReplicaType::LocalDisk as i32
        })
        .unwrap();
    assert_eq!(
        local_disk.local_disk_storage_id,
        Some(uuid_proto(storage_id))
    );
    assert_eq!(local_disk.local_disk_generation_id, Some(generation_id));
    assert_eq!(local_disk.holder_client_id, Some(uuid_proto(client_id)));
}

#[tokio::test]
async fn test_background_eviction_worker_triggers_offload_on_high_watermark() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        eviction_interval: Duration::from_millis(10),
        eviction_high_watermark_ratio: 0.1,
        eviction_ratio: 0.05,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    for (index, segment_name) in ["bg-evict-a", "bg-evict-b"].into_iter().enumerate() {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(uuid_proto(client_id)),
                segment_name: segment_name.into(),
                size: 4096,
                base_addr: 0x100000000 + (index as u64 * 0x10000),
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    let _storage_id = mount_local_disk(&service, client_id).await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "bg-evict-offload".into(),
            slice_length: 512,
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
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
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "bg-evict-offload".into(),
            replica_type: 0,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    let offload = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(offload.objects.get("bg-evict-offload"), Some(&512));
    assert_eq!(offload.tasks.len(), 1);
    assert_eq!(offload.tasks[0].key, "bg-evict-offload");
    assert_eq!(offload.tasks[0].size, 512);
    assert!(
        offload.tasks[0]
            .generation_id
            .as_ref()
            .is_some_and(|generation_id| generation_id.high != 0 || generation_id.low != 0)
    );

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "bg-evict-offload".into(),
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
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_processing_keys_excluded_from_eviction() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        offload_on_evict: false,
        lease_ttl: Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(uuid_proto(client_id)),
            segment_name: "proc-key-seg".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    // Complete PutEnd for an evictable key.
    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "evictable".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
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
    .unwrap()
    .into_inner();
    assert_eq!(put.replicas.len(), 1);
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "evictable".into(),
            replica_type: 0,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    // PutStart but never PutEnd — key stays in processing_keys.
    let put2 = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(uuid_proto(client_id)),
            key: "still-processing".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
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
    .unwrap()
    .into_inner();
    assert_eq!(put2.replicas.len(), 1);

    tokio::time::sleep(Duration::from_millis(5)).await;

    // Eviction should only evict "evictable"; "still-processing" is in processing_keys.
    let evicted = service.run_eviction_cycle_for_test(10);
    assert_eq!(evicted, vec!["evictable".to_string()]);

    // "still-processing" still exists — replicas are Allocating, not Complete.
    let err = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "still-processing".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("replica is not ready"));
}

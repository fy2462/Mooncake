use mooncake_store_core::ReplicaType;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::Arc;
use tonic::{Code, Request};
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
        host_id: String::new(),
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
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn begin_local_disk_recovery(
    service: &MasterServiceImpl,
    client_id: Uuid,
    storage_id: Uuid,
    recovery_session_id: Uuid,
) {
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
}

async fn commit_local_disk_recovery(
    service: &MasterServiceImpl,
    client_id: Uuid,
    storage_id: Uuid,
    recovery_session_id: Uuid,
) {
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
}

async fn mount_local_disk(service: &MasterServiceImpl, client_id: Uuid) -> Uuid {
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    begin_local_disk_recovery(service, client_id, storage_id, recovery_session_id).await;
    commit_local_disk_recovery(service, client_id, storage_id, recovery_session_id).await;
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
) {
    let metadatas = tasks
        .iter()
        .map(|task| proto::StorageObjectMetadata {
            bucket_id: 0,
            offset: 0,
            key_size: task.key.len() as i64,
            data_size: task.size,
            transport_endpoint: endpoint.to_string(),
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
async fn concurrent_local_disk_mounts_parity() {
    let service = Arc::new(MasterServiceImpl::with_runtime_config(
        MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        },
    ));

    let mut tasks = Vec::new();
    for _ in 0..100 {
        let service = Arc::clone(&service);
        tasks.push(tokio::spawn(async move {
            let client_id = Uuid::new_v4();
            let storage_id = Uuid::new_v4();
            let recovery_session_id = Uuid::new_v4();
            begin_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
            commit_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
        }));
    }

    let mut success = 0usize;
    for task in tasks {
        task.await.unwrap();
        success += 1;
    }
    assert_eq!(success, 100);
}

#[tokio::test]
async fn cpp_parity_segment_test_cpp_segmenttest_mountlocaldisksegmentsuccess() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();

    begin_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
    commit_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
    // A completed retry is accepted only while this exact client and recovery
    // session remain the active binding; persisted ownership alone is not enough.
    commit_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;

    let snapshot = service.capture_loaded_snapshot("local-disk-mount");
    assert_eq!(snapshot.local_disk_segments.len(), 1);
    let mounted = &snapshot.local_disk_segments[0];
    assert_eq!(mounted.storage_id, storage_id);
    assert_eq!(mounted.client_id, client_id);
    assert!(mounted.enable_offloading);
    assert!(mounted.offloading_objects.is_empty());
}

#[tokio::test]
async fn heartbeat_batches_three_thousand_new_objects_parity() {
    const KEY_COUNT: usize = 3000;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "hb-mem", 16 * 1024 * 1024).await;

    // C++ mounts the local disk segment with offloading disabled, then the
    // first heartbeat enables offloading and observes an empty queue.
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    begin_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(uuid_proto(client_id)),
            enable_offloading: false,
            storage_id: Some(uuid_proto(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(uuid_proto(recovery_session_id)),
        }),
    )
    .await
    .unwrap();

    async fn put_phase(service: &MasterServiceImpl, client_id: Uuid, prefix: &str) -> Vec<String> {
        let keys = (0..KEY_COUNT)
            .map(|index| format!("{prefix}-{index}"))
            .collect::<Vec<_>>();
        for key in &keys {
            put_complete(service, client_id, key, 1024, "hb-mem").await;
        }
        keys
    }

    // Phase 1: objects created while offloading is disabled; heartbeat
    // enables offloading and returns an empty batch.
    put_phase(&service, client_id, "hb-p1").await;
    let first = take_offload_tasks(&service, client_id).await;
    assert_eq!(first.len(), 0);

    // Phase 2: offloading is now enabled, so all 3000 new objects are queued.
    let second_keys = put_phase(&service, client_id, "hb-p2").await;
    let second = take_offload_tasks(&service, client_id).await;
    assert_eq!(second.len(), second_keys.len());
    for task in &second {
        assert!(second_keys.contains(&task.key));
        assert_eq!(task.size, 1024);
    }

    // Phase 3: a fresh 3000-object batch is queued and returned again.
    let third_keys = put_phase(&service, client_id, "hb-p3").await;
    let third = take_offload_tasks(&service, client_id).await;
    assert_eq!(third.len(), third_keys.len());
    for task in &third {
        assert!(third_keys.contains(&task.key));
        assert_eq!(task.size, 1024);
    }
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
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let storage_id = mount_local_disk(&service, client_id).await;

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
                host_id: String::new(),
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
    assert_eq!(heartbeat.tasks.len(), 1);
    assert_eq!(heartbeat.tasks[0].tenant_id, "default");
    assert_eq!(heartbeat.tasks[0].key, "offload-key");
    assert_eq!(heartbeat.tasks[0].size, 256);
    assert!(heartbeat.tasks[0].generation_id.is_some());
    let offload_task = heartbeat.tasks[0].clone();

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            keys: vec!["offload-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: "offload-key".len() as i64,
                data_size: 256,
                transport_endpoint: "holder-a".into(),
            }],
            tasks: vec![offload_task.clone()],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service.replica_refcnts_for_test("offload-key", ReplicaType::Memory, ""),
        vec![0]
    );

    // C++ classic path accepts a registered tenant's unsolicited completion
    // (create-on-missing) without an admitted task; the object is then a
    // readable LocalDisk-only entry.
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
                generation_id: Some(uuid_proto(Uuid::new_v4())),
            }],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    let disk_only = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "tenant-disk-key".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(disk_only.len(), 1);
    assert_eq!(disk_only[0].replica_type, ReplicaType::LocalDisk as i32);

    // Reattachment is permitted only inside an explicit recovery transaction
    // and must present the exact Master-issued byte generation.
    let recovery_session_id = Uuid::new_v4();
    begin_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(uuid_proto(client_id)),
            keys: vec!["offload-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: "offload-key".len() as i64,
                data_size: 256,
                transport_endpoint: "holder-after-restart".into(),
            }],
            tasks: vec![offload_task.clone()],
            recovery_session_id: Some(uuid_proto(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
    commit_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "offload-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let local_disk = replicas
        .replicas
        .iter()
        .find(|replica| {
            replica.replica_type == proto::replica_descriptor::ReplicaType::LocalDisk as i32
        })
        .unwrap();
    assert_eq!(local_disk.size, 256);
    assert_eq!(local_disk.segment_name, "holder-after-restart");
    assert_eq!(
        local_disk.holder_client_id.as_ref().unwrap().high,
        client_id.as_u64_pair().0
    );

    let wrong_size = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(uuid_proto(client_id)),
            keys: vec!["offload-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: "offload-key".len() as i64,
                data_size: 512,
                transport_endpoint: "stale-holder".into(),
            }],
            tasks: vec![offload_task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(wrong_size.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn stale_local_disk_report_cannot_resurrect_removed_object() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "remove-mem", 4096).await;
    let storage_id = mount_local_disk(&service, client_id).await;
    put_complete(&service, client_id, "removed-key", 256, "remove-mem").await;
    let tasks = take_offload_tasks(&service, client_id).await;
    assert_eq!(tasks.len(), 1);
    notify_offload_tasks(&service, client_id, tasks.clone(), "holder-before-remove").await;

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "removed-key".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let recovery_session_id = Uuid::new_v4();
    begin_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;
    let stale = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(uuid_proto(client_id)),
            keys: vec!["removed-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: "removed-key".len() as i64,
                data_size: 256,
                transport_endpoint: "holder-after-restart".into(),
            }],
            tasks,
            recovery_session_id: Some(uuid_proto(recovery_session_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(stale.stale_recovery_tasks.len(), 1);
    commit_local_disk_recovery(&service, client_id, storage_id, recovery_session_id).await;

    let lookup = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "removed-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(lookup.code(), Code::NotFound);
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
            recovery_session_id: None,
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

    let disk_keys = (0..8)
        .map(|idx| format!("auto-disk-only-{idx}"))
        .collect::<Vec<_>>();
    for key in &disk_keys {
        put_complete(&service, client_id, key, 128, "auto-evict-mem").await;
    }
    let disk_tasks = take_offload_tasks(&service, client_id).await;
    assert_eq!(disk_tasks.len(), disk_keys.len());
    notify_offload_tasks(&service, client_id, disk_tasks, "holder-a").await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert_eq!(
        service.run_eviction_cycle_for_test(disk_keys.len()).len(),
        disk_keys.len()
    );

    let memory_keys = vec!["auto-mem-a".to_string(), "auto-mem-b".to_string()];
    for key in &memory_keys {
        put_complete(&service, client_id, key, 256, "auto-evict-mem").await;
    }
    let memory_tasks = take_offload_tasks(&service, client_id).await;
    assert_eq!(memory_tasks.len(), memory_keys.len());
    notify_offload_tasks(&service, client_id, memory_tasks, "holder-a").await;
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

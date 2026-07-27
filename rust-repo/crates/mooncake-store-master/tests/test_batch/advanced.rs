use super::*;

async fn put_grouped_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    group_id: &str,
    hard_pinned: bool,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                with_hard_pin: hard_pinned,
                group_ids: vec![group_id.into()],
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();
    put_end_one(service, client_id, key).await;
}

#[tokio::test]
async fn test_upsert_and_batch_upsert_follow_two_phase_semantics() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "upsert-two-phase:1").await;

    let config = proto::ReplicateConfig {
        preferred_segment: "upsert-two-phase:1".into(),
        ..replicate_config()
    };
    let upsert = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert-key".into(),
            slice_length: 128,
            config: Some(config.clone()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(upsert.replicas.len(), 1);
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "upsert-key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );

    let end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "upsert-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(end.statuses, vec![0]);
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "upsert-key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_ok()
    );

    MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert-key".into(),
            slice_length: 128,
            config: Some(config.clone()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "upsert-key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
    let same_size_end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "upsert-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(same_size_end.statuses, vec![0]);

    let batch_start = MasterService::batch_upsert_start(
        &service,
        Request::new(proto::BatchUpsertStartRequest {
            entries: vec![proto::UpsertEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "batch-upsert-key".into(),
                slice_length: 128,
                config: Some(config),
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_start.statuses, vec![0]);
    assert_eq!(batch_start.replicas.len(), 1);

    let batch_end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "batch-upsert-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_end.statuses, vec![0]);
}

#[tokio::test]
async fn test_upsert_all_does_not_complete_stale_local_disk_replica() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "upsert-local-disk:1").await;
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: false,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: false,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();

    let config = proto::ReplicateConfig {
        preferred_segment: "upsert-local-disk:1".into(),
        ..replicate_config()
    };
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert-local-disk-key".into(),
            slice_length: 128,
            config: Some(config.clone()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert-local-disk-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let task = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks
    .into_iter()
    .find(|task| task.key == "upsert-local-disk-key")
    .expect("Master must issue the upsert fixture offload task");
    assert!(task.generation_id.is_some());
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["upsert-local-disk-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 21,
                data_size: 128,
                transport_endpoint: "disk-holder:50051".into(),
            }],
            tasks: vec![task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();

    let start = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert-local-disk-key".into(),
            slice_length: 128,
            config: Some(config),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(start.replicas.len(), 1);
    assert_eq!(
        start.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );

    MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "upsert-local-disk-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::All as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap();

    let readable = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "upsert-local-disk-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(readable.replicas.len(), 1);
    assert_eq!(
        readable.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
}

#[tokio::test]
async fn test_upsert_start_preempts_inflight_buffer_without_reuse() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "upsert-preempt:1").await;
    let config = proto::ReplicateConfig {
        preferred_segment: "upsert-preempt:1".into(),
        ..replicate_config()
    };

    let first = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "preempt-key".into(),
            slice_length: 128,
            config: Some(config.clone()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let second = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "preempt-key".into(),
            slice_length: 128,
            config: Some(config),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(first.replicas.len(), 1);
    assert_eq!(second.replicas.len(), 1);
    assert_ne!(first.replicas[0].offset, second.replicas[0].offset);
}

#[tokio::test]
async fn test_put_end_and_batch_put_end_start_without_hard_lease() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-lease:1").await;

    put_start_one(&service, client_id, "single-no-lease").await;
    put_end_one(&service, client_id, "single-no-lease").await;
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "single-no-lease".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    put_start_one(&service, client_id, "batch-no-lease").await;
    let batch_put = MasterService::batch_put_end(
        &service,
        Request::new(proto::BatchPutEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "batch-no-lease".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_put.statuses, vec![0]);

    let batch_remove = MasterService::batch_remove(
        &service,
        Request::new(proto::BatchRemoveRequest {
            keys: vec!["batch-no-lease".into()],
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_remove.statuses, vec![0]);
}

#[tokio::test]
async fn test_regex_get_grants_lease_and_batch_remove_respects_it() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "regex-lease:1").await;
    put_start_one(&service, client_id, "regex-lease-key").await;
    put_end_one(&service, client_id, "regex-lease-key").await;

    let regex_result = MasterService::get_replica_list_by_regex(
        &service,
        Request::new(proto::GetReplicaListByRegexRequest {
            key_regex: "regex-lease-.*".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(regex_result.entries.len(), 1);

    let batch_remove = MasterService::batch_remove(
        &service,
        Request::new(proto::BatchRemoveRequest {
            keys: vec!["regex-lease-key".into()],
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_ne!(batch_remove.statuses, vec![0]);

    let still_readable = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "regex-lease-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await;
    assert!(still_readable.is_ok());
}

#[tokio::test]
async fn test_eviction_uses_explicit_lease_timeout_not_last_access_ttl() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "evict-explicit-lease:1").await;
    put_start_one(&service, client_id, "evict-after-put-end").await;
    put_end_one(&service, client_id, "evict-after-put-end").await;

    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["evict-after-put-end".to_string()]);
}

#[tokio::test]
async fn test_reaper_removes_expired_processing_put_start() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        put_start_release_timeout: Duration::from_millis(20),
        reaper_interval: Duration::from_millis(5),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "reaper-processing:1").await;
    put_start_one(&service, client_id, "stale-processing").await;

    tokio::time::sleep(Duration::from_millis(80)).await;
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "stale-processing".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!exists.exists);
}

#[tokio::test]
async fn test_soft_pinned_eviction_uses_configured_second_pass() {
    for (allow_soft_pinned, should_evict) in [(false, false), (true, true)] {
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: Duration::from_secs(3600),
            soft_pin_ttl: Duration::from_secs(3600),
            allow_evict_soft_pinned_objects: allow_soft_pinned,
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_memory_segment(
            &service,
            client_id,
            if allow_soft_pinned {
                "soft-force:1"
            } else {
                "soft-protect:1"
            },
        )
        .await;

        let key = if allow_soft_pinned {
            "soft-force-key"
        } else {
            "soft-protected-key"
        };
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    with_soft_pin: true,
                    ..replicate_config()
                }),
            }),
        )
        .await
        .unwrap();
        put_end_one(&service, client_id, key).await;

        let evicted = service.run_eviction_cycle_for_test(1);
        assert_eq!(
            evicted.iter().any(|evicted_key| evicted_key == key),
            should_evict
        );
    }
}

#[tokio::test]
async fn test_automatic_eviction_expands_an_expired_group_atomically() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-eviction:1").await;
    put_grouped_object(
        &service,
        client_id,
        "group-expired-a",
        "expired-group",
        false,
    )
    .await;
    put_grouped_object(
        &service,
        client_id,
        "group-expired-b",
        "expired-group",
        false,
    )
    .await;

    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted.len(), 2);
    assert!(evicted.iter().any(|key| key == "group-expired-a"));
    assert!(evicted.iter().any(|key| key == "group-expired-b"));
}

#[tokio::test]
async fn test_automatic_eviction_live_group_lease_protects_every_member() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-live-lease:1").await;
    put_grouped_object(&service, client_id, "group-expired", "lease-group", false).await;
    put_grouped_object(&service, client_id, "group-live", "lease-group", false).await;
    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "group-live".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let evicted = service.run_eviction_cycle_for_test(1);
    assert!(evicted.is_empty());
}

#[tokio::test]
async fn test_automatic_eviction_keeps_hard_pinned_group_member() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-hard-pin:1").await;
    put_grouped_object(&service, client_id, "group-safe", "hard-pin-group", false).await;
    put_grouped_object(&service, client_id, "group-hard", "hard-pin-group", true).await;

    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["group-safe".to_string()]);
    assert!(
        MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "group-hard".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .exists
    );
}

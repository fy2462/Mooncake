mod common;
use common::proto_uuid;
use mooncake_store_master::oplog::{InMemoryOpLog, OpLogManager};
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tonic::Request;
use uuid::Uuid;

async fn mount_batch_clear_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
    index: u64,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024 * 1024,
            base_addr: 0x100000000 + index * 0x200000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_complete_batch_clear_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    replica_num: usize,
    preferred_segment: &str,
) -> Vec<proto::ReplicaDescriptor> {
    let started = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: replica_num as u32,
                preferred_segment: preferred_segment.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), replica_num);

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
    started.replicas
}

async fn batch_clear(
    service: &MasterServiceImpl,
    client_id: Uuid,
    keys: &[&str],
    segment_name: &str,
) -> Vec<String> {
    MasterService::batch_replica_clear(
        service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: keys.iter().map(|key| (*key).to_string()).collect(),
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .cleared_keys
}

async fn object_exists(service: &MasterServiceImpl, key: &str) -> bool {
    MasterService::exist_key(
        service,
        Request::new(proto::ExistKeyRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .exists
}

async fn mount_put_start_parity_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 16 * 1024 * 1024,
            base_addr: 0x300000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_put_start_parity_nof_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
) {
    MasterService::mount_no_f_segment(
        service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(Uuid::new_v4())),
                name: segment_name.into(),
                base: 0x400000000,
                size: 16 * 1024 * 1024,
                te_endpoint: format!("nof://{segment_name}"),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn put_start_invalid_parameter_matrix_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_segment(&service, client_id, "put-invalid:3333").await;
    let request = |slice_length, config| proto::PutStartRequest {
        client_id: Some(proto_uuid(client_id)),
        key: "test_key".into(),
        slice_length,
        tenant_id: String::new(),
        config: Some(config),
    };

    for (case, request) in [
        (
            "zero total replicas",
            request(
                1024,
                proto::ReplicateConfig {
                    replica_num: 0,
                    nof_replica_num: 0,
                    ..Default::default()
                },
            ),
        ),
        (
            "zero slice length",
            request(
                0,
                proto::ReplicateConfig {
                    replica_num: 1,
                    ..Default::default()
                },
            ),
        ),
        (
            "same-node preference with NoF",
            request(
                1024,
                proto::ReplicateConfig {
                    replica_num: 1,
                    nof_replica_num: 1,
                    prefer_alloc_in_same_node: true,
                    ..Default::default()
                },
            ),
        ),
    ] {
        let error = MasterService::put_start(&service, Request::new(request))
            .await
            .expect_err(case);
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{case}");
    }
}

#[tokio::test]
async fn one_plus_one_allows_available_memory_only_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_segment(&service, client_id, "one-plus-one:3333").await;

    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_one_plus_one".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.replicas.len(), 1);
    assert_eq!(
        response.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
}

#[tokio::test]
async fn one_plus_one_flexible_mode_also_allows_available_nof_only() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_put_start_parity_nof_segment(&service, client_id, "one-plus-one-nof:3333").await;

    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_one_plus_one_nof".into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.replicas.len(), 1);
    assert_eq!(
        response.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::NofSsd as i32
    );
}

#[tokio::test]
async fn batch_replica_clear_empty_input_parity() {
    let service = MasterServiceImpl::new(None, None);
    let cleared = batch_clear(&service, Uuid::new_v4(), &[], "").await;
    assert!(cleared.is_empty());
}

#[tokio::test]
async fn batch_replica_clear_missing_keys_parity() {
    let service = MasterServiceImpl::new(None, None);
    let cleared = batch_clear(
        &service,
        Uuid::new_v4(),
        &["missing_key1", "missing_key2"],
        "",
    )
    .await;
    assert!(cleared.is_empty());
}

#[tokio::test]
async fn batch_replica_clear_skips_active_lease_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(2000),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "active-lease:1", 0).await;
    put_complete_batch_clear_object(&service, client_id, "active-lease-key", 1, "active-lease:1")
        .await;

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "active-lease-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);

    assert!(
        batch_clear(&service, client_id, &["active-lease-key"], "")
            .await
            .is_empty()
    );
    assert!(object_exists(&service, "active-lease-key").await);
}

#[tokio::test]
async fn batch_replica_clear_rejects_nonowner_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let owner_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, owner_id, "owner:1", 0).await;
    put_complete_batch_clear_object(&service, owner_id, "owner-key", 1, "owner:1").await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert!(
        batch_clear(&service, Uuid::new_v4(), &["owner-key"], "")
            .await
            .is_empty()
    );
    assert!(object_exists(&service, "owner-key").await);
}

#[tokio::test]
async fn batch_replica_clear_all_segments_five_keys_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "all-segments:1", 0).await;
    let keys = [
        "clear-key-1",
        "clear-key-2",
        "clear-key-3",
        "clear-key-4",
        "clear-key-5",
    ];
    for key in keys {
        put_complete_batch_clear_object(&service, client_id, key, 1, "all-segments:1").await;
        assert!(object_exists(&service, key).await);
    }
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(batch_clear(&service, client_id, &keys, "").await, keys);
    for key in keys {
        assert!(!object_exists(&service, key).await);
    }
}

#[tokio::test]
async fn batch_replica_clear_specific_segment_polling_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "specific:1", 0).await;
    mount_batch_clear_segment(&service, client_id, "specific:2", 1).await;
    let replicas =
        put_complete_batch_clear_object(&service, client_id, "specific-key", 1, "specific:1").await;
    assert_eq!(replicas[0].segment_name, "specific:1");

    tokio::time::sleep(Duration::from_millis(10)).await;
    let cleared = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cleared = batch_clear(&service, client_id, &["specific-key"], "specific:1").await;
            if !cleared.is_empty() {
                break cleared;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("named-segment replica did not become clearable within five seconds");

    assert_eq!(cleared, ["specific-key"]);
    assert!(!object_exists(&service, "specific-key").await);
}

#[tokio::test]
async fn batch_replica_clear_skips_empty_and_missing_strings_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "empty-strings:1", 0).await;
    put_complete_batch_clear_object(&service, client_id, "valid_key", 1, "empty-strings:1").await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(
        batch_clear(
            &service,
            client_id,
            &["", "valid_key", "", "another_empty"],
            "",
        )
        .await,
        ["valid_key"]
    );
}

#[tokio::test]
async fn batch_replica_clear_mixed_owner_missing_empty_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    let other_client_id = Uuid::new_v4();
    mount_batch_clear_segment(&service, client_id, "mixed-owner:1", 0).await;
    mount_batch_clear_segment(&service, other_client_id, "mixed-owner:2", 1).await;
    for key in ["key1", "key2"] {
        put_complete_batch_clear_object(&service, client_id, key, 1, "mixed-owner:1").await;
    }
    put_complete_batch_clear_object(&service, other_client_id, "key3", 1, "mixed-owner:2").await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(
        batch_clear(
            &service,
            client_id,
            &["key1", "key2", "key3", "nonexistent", ""],
            "",
        )
        .await,
        ["key1", "key2"]
    );
    assert!(!object_exists(&service, "key1").await);
    assert!(!object_exists(&service, "key2").await);
    assert!(object_exists(&service, "key3").await);
}

#[tokio::test]
async fn put_start_preserves_default_and_non_default_object_data_types() {
    let service = MasterServiceImpl::new(None, None);
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "data-type:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for (index, data_type) in [
        proto::ObjectDataType::Unknown,
        proto::ObjectDataType::Weight,
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("data-type-{index}");
        let started = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.clone(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    data_type: data_type as i32,
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 1);

        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.clone(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(replicas.replicas.len(), 1);
    }
}

#[tokio::test]
async fn test_batch_replica_clear_respects_client_and_segment_name() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig {
            lease_ttl: Duration::ZERO,
            ..Default::default()
        },
        Some(OpLogManager::new(Some(Box::new(InMemoryOpLog::new(64))), 0)),
    );
    let client_id = Uuid::new_v4();
    let other_client_id = Uuid::new_v4();

    for (index, (cid, name)) in [(client_id, "node-a:1"), (client_id, "node-b:1")]
        .into_iter()
        .enumerate()
    {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(cid)),
                segment_name: name.into(),
                size: 1024,
                base_addr: 0x100000000 + (index as u64 * 0x10000),
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch-clear-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
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
    .unwrap()
    .into_inner();
    assert_eq!(put.replicas.len(), 2);

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch-clear-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(other_client_id)),
            segment_name: "node-c:1".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let sequence_before_clear = service.oplog_manager().latest_sequence();
    let cleared = MasterService::batch_replica_clear(
        &service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: vec!["batch-clear-key".into()],
            client_id: Some(proto_uuid(client_id)),
            segment_name: put.replicas[0].segment_name.clone(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(cleared.cleared_keys, vec!["batch-clear-key".to_string()]);
    assert_eq!(
        service.oplog_manager().latest_sequence(),
        sequence_before_clear + 1,
        "successful replica clear must publish one durable object image"
    );

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "batch-clear-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);
    assert_ne!(
        replicas.replicas[0].segment_name,
        put.replicas[0].segment_name
    );

    let denied = MasterService::batch_replica_clear(
        &service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: vec!["batch-clear-key".into()],
            client_id: Some(proto_uuid(other_client_id)),
            segment_name: String::new(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(denied.cleared_keys.is_empty());

    let still_exists = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "batch-clear-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(still_exists.replicas.len(), 1);
}

#[tokio::test]
async fn test_timed_out_put_start_releases_dashmap_guard_before_remove() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        put_start_discard_timeout: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "stale-put:1".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let request = || {
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "stale-put-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "stale-put:1".into(),
                ..Default::default()
            }),
        })
    };
    MasterService::put_start(&service, request()).await.unwrap();

    let replacement = tokio::time::timeout(
        Duration::from_secs(1),
        MasterService::put_start(&service, request()),
    )
    .await
    .expect("stale PutStart cleanup must not deadlock")
    .unwrap()
    .into_inner();
    assert_eq!(replacement.replicas.len(), 1);
}

#[tokio::test]
async fn test_put_start_discard_timeout_removes_incomplete_memory_and_disk_replicas() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "discard-incomplete-replicas".into(),
        put_start_discard_timeout: Duration::from_millis(20),
        put_start_release_timeout: Duration::from_secs(5),
        reaper_interval: Duration::from_millis(5),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "discard-incomplete:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for (key, completed_type, discarded_type) in [
        (
            "discard-disk",
            proto::replica_descriptor::ReplicaType::Memory,
            proto::replica_descriptor::ReplicaType::Disk,
        ),
        (
            "discard-memory",
            proto::replica_descriptor::ReplicaType::Disk,
            proto::replica_descriptor::ReplicaType::Memory,
        ),
    ] {
        let started = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "discard-incomplete:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(started.replicas.len(), 2);

        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: completed_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(replicas.replicas.len(), 1);
        assert_eq!(replicas.replicas[0].replica_type, completed_type as i32);

        // Match C++ discarded_replicas_: a late completion is accepted as a
        // no-op and must not resurrect the discarded replica.
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: discarded_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let after_late_end = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(after_late_end.replicas.len(), 1);
        assert_eq!(
            after_late_end.replicas[0].replica_type,
            completed_type as i32
        );
    }
}

#[tokio::test]
async fn test_memory_and_global_disk_put_revoke_remove_parity() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "memory-disk-lifecycle".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "memory-disk-lifecycle:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    async fn start(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
        let response = MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "memory-disk-lifecycle:1".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(response.replicas.len(), 2);
    }

    async fn end(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        replica_type: proto::replica_descriptor::ReplicaType,
    ) {
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn revoke(
        service: &MasterServiceImpl,
        client_id: Uuid,
        key: &str,
        replica_type: proto::replica_descriptor::ReplicaType,
    ) {
        MasterService::put_revoke(
            service,
            Request::new(proto::PutRevokeRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    async fn replicas(service: &MasterServiceImpl, key: &str) -> Vec<proto::ReplicaDescriptor> {
        MasterService::get_replica_list(
            service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas
    }

    use proto::replica_descriptor::{ReplicaStatus as Status, ReplicaType as Type};

    start(&service, client_id, "complete-both").await;
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "complete-both".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
    end(&service, client_id, "complete-both", Type::Memory).await;
    end(&service, client_id, "complete-both", Type::Disk).await;
    let complete = replicas(&service, "complete-both").await;
    assert_eq!(complete.len(), 2);
    assert!(
        complete
            .iter()
            .all(|replica| replica.status == Status::Complete as i32)
    );

    start(&service, client_id, "revoke-disk").await;
    end(&service, client_id, "revoke-disk", Type::Memory).await;
    revoke(&service, client_id, "revoke-disk", Type::Disk).await;
    let memory_only = replicas(&service, "revoke-disk").await;
    assert_eq!(memory_only.len(), 1);
    assert_eq!(memory_only[0].replica_type, Type::Memory as i32);

    start(&service, client_id, "revoke-memory").await;
    revoke(&service, client_id, "revoke-memory", Type::Memory).await;
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "revoke-memory".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
    end(&service, client_id, "revoke-memory", Type::Disk).await;
    let disk_only = replicas(&service, "revoke-memory").await;
    assert_eq!(disk_only.len(), 1);
    assert_eq!(disk_only[0].replica_type, Type::Disk as i32);

    start(&service, client_id, "revoke-both").await;
    revoke(&service, client_id, "revoke-both", Type::Disk).await;
    revoke(&service, client_id, "revoke-both", Type::Memory).await;
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "revoke-both".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );

    start(&service, client_id, "remove-both").await;
    end(&service, client_id, "remove-both", Type::Memory).await;
    end(&service, client_id, "remove-both", Type::Disk).await;
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "remove-both".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "remove-both".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn test_global_disk_keeps_object_readable_after_memory_capacity_eviction() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "global-disk-eviction".into(),
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "global-disk-eviction:1".into(),
            size: 128,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let put = |key: &str| {
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "global-disk-eviction:1".into(),
                ..Default::default()
            }),
        })
    };
    MasterService::put_start(&service, put("evicted-to-disk"))
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
                key: "evicted-to-disk".into(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        service.run_eviction_cycle_for_test(1),
        vec!["evicted-to-disk".to_string()]
    );
    let retained = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "evicted-to-disk".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(retained.replicas.len(), 1);
    assert_eq!(
        retained.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::Disk as i32
    );

    let replacement = MasterService::put_start(&service, put("reuses-memory-capacity"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replacement.replicas.len(), 2);
}

#[tokio::test]
async fn test_concurrent_initial_put_start_has_single_winner() {
    const WRITERS: usize = 32;

    let service = Arc::new(MasterServiceImpl::default());
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        service.as_ref(),
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "concurrent-put:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let barrier = Arc::new(Barrier::new(WRITERS));
    let mut writers = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        let service = Arc::clone(&service);
        let barrier = Arc::clone(&barrier);
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            MasterService::put_start(
                service.as_ref(),
                Request::new(proto::PutStartRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key: "single-winner".into(),
                    slice_length: 128,
                    tenant_id: String::new(),
                    config: Some(proto::ReplicateConfig {
                        replica_num: 1,
                        preferred_segment: "concurrent-put:1".into(),
                        ..Default::default()
                    }),
                }),
            )
            .await
        }));
    }

    let mut successes = 0;
    let mut already_exists = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(response) => {
                successes += 1;
                assert_eq!(response.into_inner().replicas.len(), 1);
            }
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::AlreadyExists);
                already_exists += 1;
            }
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(already_exists, WRITERS - 1);
}

#[tokio::test]
async fn test_overlapping_batch_put_start_uses_stable_lock_order() {
    let service = Arc::new(MasterServiceImpl::default());
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        service.as_ref(),
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "batch-lock-order:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let mut batches = Vec::new();
    for keys in [
        vec!["batch-a".to_string(), "batch-b".to_string()],
        vec!["batch-b".to_string(), "batch-a".to_string()],
    ] {
        let service = Arc::clone(&service);
        let barrier = Arc::clone(&barrier);
        batches.push(tokio::spawn(async move {
            barrier.wait().await;
            MasterService::batch_put_start(
                service.as_ref(),
                Request::new(proto::BatchPutStartRequest {
                    client_id: Some(proto_uuid(client_id)),
                    keys,
                    slice_lengths: vec![128, 128],
                    config: Some(proto::ReplicateConfig {
                        replica_num: 1,
                        preferred_segment: "batch-lock-order:1".into(),
                        ..Default::default()
                    }),
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap()
            .into_inner()
        }));
    }

    let responses = tokio::time::timeout(Duration::from_secs(2), async {
        let mut responses = Vec::new();
        for batch in batches {
            responses.push(batch.await.unwrap());
        }
        responses
    })
    .await
    .expect("overlapping batches must not deadlock");

    let statuses = responses
        .iter()
        .flat_map(|response| response.results.iter().map(|result| result.status))
        .collect::<Vec<_>>();
    assert_eq!(statuses.iter().filter(|&&status| status == 0).count(), 2);
    assert_eq!(statuses.iter().filter(|&&status| status == -7).count(), 2);
}

#[tokio::test]
async fn test_hard_pinned_object_survives_eviction_cycle() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "hardpin:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for (key, with_hard_pin) in [("hard-key", true), ("normal-key", false)] {
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 512,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    nof_replica_num: 0,
                    with_soft_pin: false,
                    with_hard_pin,
                    preferred_segment: String::new(),
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
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    let evicted = service.run_eviction_cycle_for_test(2);
    assert!(evicted.iter().any(|key| key == "normal-key"));
    assert!(!evicted.iter().any(|key| key == "hard-key"));

    let hard_key = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "hard-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(hard_key.replicas.len(), 1);

    let normal_key = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "normal-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await;
    assert!(normal_key.is_err());
}

#[tokio::test]
async fn test_copy_move_and_revoke_workflow() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    for (index, name) in ["copy-src:1", "copy-dst:1", "move-dst:1"]
        .into_iter()
        .enumerate()
    {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: name.into(),
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

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "copy-src:1".into(),
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
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let copy_started = MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-src:1".into(),
            targets: vec!["copy-dst:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(copy_started.targets.len(), 1);
    assert_eq!(copy_started.targets[0].segment_name, "copy-dst:1");

    MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_copy = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_copy.replicas.len(), 2);

    let move_started = MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-src:1".into(),
            target: "move-dst:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(move_started.target.unwrap().segment_name, "move-dst:1");

    MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_move = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_move.replicas.len(), 2);
    assert!(
        after_move
            .replicas
            .iter()
            .any(|r| r.segment_name == "copy-dst:1")
    );
    assert!(
        after_move
            .replicas
            .iter()
            .any(|r| r.segment_name == "move-dst:1")
    );
    assert!(
        !after_move
            .replicas
            .iter()
            .any(|r| r.segment_name == "copy-src:1")
    );

    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-dst:1".into(),
            targets: vec!["copy-src:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::copy_revoke(
        &service,
        Request::new(proto::CopyRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let after_revoke = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_revoke.replicas.len(), 2);
    assert!(
        !after_revoke
            .replicas
            .iter()
            .any(|r| r.segment_name == "copy-src:1")
    );
}

#[tokio::test]
async fn test_put_revoke_remove_all_and_storage_config() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: "/tmp/mooncake-root".into(),
        cluster_id: "cluster-a".into(),
        enable_disk_eviction: true,
        quota_bytes: 4096,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "revoke:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for key in ["remove-all-a", "remove-all-b"] {
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    nof_replica_num: 0,
                    with_soft_pin: false,
                    with_hard_pin: false,
                    preferred_segment: "revoke:1".into(),
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
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "remove-all-tenant".into(),
            slice_length: 128,
            tenant_id: "tenant-a".into(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "revoke:1".into(),
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
            client_id: Some(proto_uuid(client_id)),
            key: "remove-all-tenant".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "remove-all-tenant".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    // PutStart revoke-key without PutEnd so it stays in Allocating state for PutRevoke
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "revoke:1".into(),
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

    MasterService::put_revoke(
        &service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::All as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "revoke-key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );

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
    assert_eq!(removed.removed_count, 3);
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "remove-all-tenant".into(),
                tenant_id: "tenant-a".into(),
            }),
        )
        .await
        .is_err()
    );

    let storage = MasterService::get_storage_config(
        &service,
        Request::new(proto::GetStorageConfigRequest {}),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(storage.fs_dir, "/tmp/mooncake-root/cluster-a");
    assert!(storage.enable_disk_eviction);
    assert_eq!(storage.quota_bytes, 4096);
    assert!(!storage.enable_tenant_scope);
    assert_eq!(storage.memory_allocator, "offset");
    assert_eq!(storage.memory_segment_alignment, 1);
}

#[tokio::test]
async fn test_global_disk_only_put_lifecycle_uses_shared_file_descriptor() {
    let root = tempfile::tempdir().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: root.path().to_string_lossy().into_owned(),
        cluster_id: "cluster-disk".into(),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    let start = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "disk-only".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 0,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
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
    .unwrap()
    .into_inner();

    assert_eq!(start.replicas.len(), 1);
    let disk = &start.replicas[0];
    assert_eq!(
        disk.replica_type,
        proto::replica_descriptor::ReplicaType::Disk as i32
    );
    assert_eq!(disk.file_path, disk.segment_name);
    assert!(
        std::path::Path::new(&disk.file_path)
            .starts_with(root.path().join("cluster-disk/global-disk"))
    );
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "disk-only".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "disk-only".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let complete = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "disk-only".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(complete.replicas.len(), 1);
    assert_eq!(
        complete.replicas[0].status,
        proto::replica_descriptor::ReplicaStatus::Complete as i32
    );

    let eviction = MasterService::batch_evict_disk_replica(
        &service,
        Request::new(proto::BatchEvictDiskReplicaRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["disk-only".into(), "already-missing".into()],
            replica_type: proto::replica_descriptor::ReplicaType::Disk as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(eviction.statuses, [0, -1]);
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "disk-only".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
}

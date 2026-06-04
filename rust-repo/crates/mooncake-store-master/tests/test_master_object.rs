mod common;
use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn test_batch_replica_clear_respects_client_and_segment_name() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    let other_client_id = Uuid::new_v4();

    for (cid, name) in [(client_id, "node-a:1"), (client_id, "node-b:1")] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(cid)),
                segment_name: name.into(),
                size: 1024,
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
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
        }),
    )
    .await
    .unwrap();

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
    for name in ["copy-src:1", "copy-dst:1", "move-dst:1"] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: name.into(),
                size: 4096,
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
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
    assert!(after_move
        .replicas
        .iter()
        .any(|r| r.segment_name == "copy-dst:1"));
    assert!(after_move
        .replicas
        .iter()
        .any(|r| r.segment_name == "move-dst:1"));
    assert!(!after_move
        .replicas
        .iter()
        .any(|r| r.segment_name == "copy-src:1"));

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
    assert!(!after_revoke
        .replicas
        .iter()
        .any(|r| r.segment_name == "copy-src:1"));
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
    assert!(MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "revoke-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .is_err());

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
    assert!(MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "remove-all-tenant".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .is_err());

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
}

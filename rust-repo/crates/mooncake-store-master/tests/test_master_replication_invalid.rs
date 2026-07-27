mod common;

use common::proto_uuid;
use mooncake_store_core::ReplicaType;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use tonic::Request;
use uuid::Uuid;

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_complete_on_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    segment_name: &str,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 256,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: segment_name.into(),
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

#[tokio::test]
async fn test_move_end_invalid_source_releases_refcnt_and_keeps_source() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let key = "move-invalid-source-key";

    for segment in ["move-src:1", "move-keep:1", "move-dst:1"] {
        mount_segment(&service, client_id, segment).await;
    }
    put_complete_on_segment(&service, client_id, key, "move-src:1").await;

    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            source: "move-src:1".into(),
            targets: vec!["move-keep:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            source: "move-src:1".into(),
            target: "move-dst:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(
        service
            .replica_refcnts_for_test(key, ReplicaType::Memory, "")
            .contains(&1)
    );
    assert!(service.set_replica_handle_valid_for_test(key, "move-src:1", "", false));

    let err = MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        service
            .replica_refcnts_for_test(key, ReplicaType::Memory, "")
            .iter()
            .all(|refcnt| *refcnt == 0)
    );

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert!(
        replicas
            .iter()
            .any(|replica| replica.segment_name == "move-src:1")
    );
    assert!(
        replicas
            .iter()
            .any(|replica| replica.segment_name == "move-keep:1")
    );
    assert!(
        !replicas
            .iter()
            .any(|replica| replica.segment_name == "move-dst:1")
    );
}

#[tokio::test]
async fn test_move_start_rejects_client_that_does_not_own_source_segment() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let other_client = Uuid::new_v4();
    mount_segment(&service, owner, "owner-source:1").await;
    mount_segment(&service, owner, "owner-target:1").await;
    put_complete_on_segment(&service, owner, "owner-key", "owner-source:1").await;

    let error = MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(other_client)),
            key: "owner-key".into(),
            source: "owner-source:1".into(),
            target: "owner-target:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_copy_start_rejects_client_that_does_not_own_source_segment() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let other_client = Uuid::new_v4();
    mount_segment(&service, owner, "copy-owner-source:1").await;
    mount_segment(&service, owner, "copy-owner-target:1").await;
    put_complete_on_segment(&service, owner, "copy-owner-key", "copy-owner-source:1").await;

    let error = MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(other_client)),
            key: "copy-owner-key".into(),
            source: "copy-owner-source:1".into(),
            targets: vec!["copy-owner-target:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_copy_and_task_creation_reject_ambiguous_target_name() {
    let service = MasterServiceImpl::default();
    let source_owner = Uuid::new_v4();
    mount_segment(&service, source_owner, "ambiguous-source:1").await;
    mount_segment(&service, Uuid::new_v4(), "ambiguous-target:1").await;
    mount_segment(&service, Uuid::new_v4(), "ambiguous-target:1").await;
    put_complete_on_segment(
        &service,
        source_owner,
        "ambiguous-key",
        "ambiguous-source:1",
    )
    .await;

    let copy_error = MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(source_owner)),
            key: "ambiguous-key".into(),
            source: "ambiguous-source:1".into(),
            targets: vec!["ambiguous-target:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(copy_error.code(), tonic::Code::FailedPrecondition);

    let copy_task_error = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "ambiguous-key".into(),
            targets: vec!["ambiguous-target:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(copy_task_error.code(), tonic::Code::FailedPrecondition);

    let move_task_error = MasterService::create_move_task(
        &service,
        Request::new(proto::CreateMoveTaskRequest {
            key: "ambiguous-key".into(),
            source: "ambiguous-source:1".into(),
            target: "ambiguous-target:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(move_task_error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_copy_with_no_new_targets_completes_as_noop() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let key = "copy-no-new-target-key";
    mount_segment(&service, owner, "copy-noop-source:1").await;
    put_complete_on_segment(&service, owner, key, "copy-noop-source:1").await;

    let started = MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            source: "copy-noop-source:1".into(),
            targets: Vec::new(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(started.targets.is_empty());

    MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service.replica_refcnts_for_test(key, ReplicaType::Memory, ""),
        vec![0]
    );
}

#[tokio::test]
async fn test_move_to_existing_complete_target_is_durable_and_removes_only_source() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let key = "move-existing-target-key";
    mount_segment(&service, owner, "existing-source:1").await;
    mount_segment(&service, owner, "existing-target:1").await;
    put_complete_on_segment(&service, owner, key, "existing-source:1").await;

    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            source: "existing-source:1".into(),
            targets: vec!["existing-target:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let started = MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            source: "existing-source:1".into(),
            target: "existing-target:1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(
        started.target.is_none(),
        "an existing Move target must tell the thin client to skip transfer"
    );
    MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "existing-target:1");
}

#[tokio::test]
async fn test_copy_end_invalid_target_detaches_it_before_retry() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let key = "copy-invalid-target-key";
    mount_segment(&service, owner, "copy-valid-source:1").await;
    mount_segment(&service, owner, "copy-invalid-target:1").await;
    put_complete_on_segment(&service, owner, key, "copy-valid-source:1").await;

    let request = || proto::CopyStartRequest {
        client_id: Some(proto_uuid(owner)),
        key: key.into(),
        source: "copy-valid-source:1".into(),
        targets: vec!["copy-invalid-target:1".into()],
        tenant_id: String::new(),
    };
    MasterService::copy_start(&service, Request::new(request()))
        .await
        .unwrap();
    assert!(service.set_replica_handle_valid_for_test(key, "copy-invalid-target:1", "", false,));

    let error = MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let retry = MasterService::copy_start(&service, Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry.targets.len(), 1);
}

#[tokio::test]
async fn test_move_end_invalid_target_keeps_source_and_allows_retry() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let key = "move-invalid-target-key";
    mount_segment(&service, owner, "move-valid-source:1").await;
    mount_segment(&service, owner, "move-invalid-target:1").await;
    put_complete_on_segment(&service, owner, key, "move-valid-source:1").await;

    let request = || proto::MoveStartRequest {
        client_id: Some(proto_uuid(owner)),
        key: key.into(),
        source: "move-valid-source:1".into(),
        target: "move-invalid-target:1".into(),
        tenant_id: String::new(),
    };
    MasterService::move_start(&service, Request::new(request()))
        .await
        .unwrap();
    assert!(service.set_replica_handle_valid_for_test(key, "move-invalid-target:1", "", false,));

    let error = MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(owner)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "move-valid-source:1");

    MasterService::move_start(&service, Request::new(request()))
        .await
        .unwrap();
}

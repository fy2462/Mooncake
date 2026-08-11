//! C++ `MasterServiceTest.CopyStart` replication parity.

mod common;
use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::{Code, Request};
use uuid::Uuid;

async fn mount_repl_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
    index: u64,
) -> Uuid {
    let mount = MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 16 * 1024 * 1024,
            base_addr: 0x300000000 + index * 0x1000000,
            te_endpoint: segment_name.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let id = mount.segment_id.unwrap();
    Uuid::from_u64_pair(id.high, id.low)
}

async fn put_repl_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    preferred_segment: &str,
) {
    put_repl_object_sized(service, client_id, key, 1024, preferred_segment).await;
}

async fn put_repl_object_sized(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    preferred_segment: &str,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: preferred_segment.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
}

async fn try_put_repl_object_sized(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    preferred_segment: &str,
) -> bool {
    let Ok(response) = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: preferred_segment.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    else {
        return false;
    };
    !response.into_inner().replicas.is_empty()
}

async fn put_end_repl_object(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
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

async fn copy_start(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    source: &str,
    targets: &[&str],
) -> Result<proto::CopyStartResponse, tonic::Status> {
    MasterService::copy_start(
        service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            source: source.into(),
            targets: targets.iter().map(|target| target.to_string()).collect(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|response| response.into_inner())
}

async fn copy_end(service: &MasterServiceImpl, client_id: Uuid, key: &str) -> Result<(), Code> {
    MasterService::copy_end(
        service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|_| ())
    .map_err(|status| status.code())
}

async fn copy_revoke(service: &MasterServiceImpl, client_id: Uuid, key: &str) -> Result<(), Code> {
    MasterService::copy_revoke(
        service,
        Request::new(proto::CopyRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|_| ())
    .map_err(|status| status.code())
}

async fn move_start(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    source: &str,
    target: &str,
) -> Result<proto::MoveStartResponse, tonic::Status> {
    MasterService::move_start(
        service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            source: source.into(),
            target: target.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|response| response.into_inner())
}

async fn move_end(service: &MasterServiceImpl, client_id: Uuid, key: &str) -> Result<(), Code> {
    MasterService::move_end(
        service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|_| ())
    .map_err(|status| status.code())
}

async fn move_revoke(service: &MasterServiceImpl, client_id: Uuid, key: &str) -> Result<(), Code> {
    MasterService::move_revoke(
        service,
        Request::new(proto::MoveRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|_| ())
    .map_err(|status| status.code())
}

async fn remove_repl(service: &MasterServiceImpl, key: &str) -> Result<(), Code> {
    MasterService::remove(
        service,
        Request::new(proto::RemoveRequest {
            key: key.into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|_| ())
    .map_err(|status| status.code())
}

async fn replica_list(service: &MasterServiceImpl, key: &str) -> Vec<proto::ReplicaDescriptor> {
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

#[tokio::test]
async fn copy_start_full_error_skip_and_cleanup_matrix_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    for (index, name) in ["segment_1", "segment_2", "segment_3", "segment_4"]
        .into_iter()
        .enumerate()
    {
        mount_repl_segment(&service, client_id, name, index as u64).await;
    }
    let key = "test_key";

    // Case 1: missing key -> OBJECT_NOT_FOUND.
    let missing = copy_start(
        &service,
        client_id,
        "non_existent_key",
        "segment_1",
        &["segment_2"],
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);

    // Case 2: incomplete source replica -> REPLICA_NOT_FOUND equivalent.
    put_repl_object(&service, client_id, key, "segment_1").await;
    let incomplete = copy_start(
        &service,
        client_id,
        key,
        "segment_1",
        &["segment_2", "segment_3"],
    )
    .await
    .unwrap_err();
    assert_eq!(incomplete.code(), Code::InvalidArgument);

    // Case 3: complete source -> two targets.
    put_end_repl_object(&service, client_id, key).await;
    let started = copy_start(
        &service,
        client_id,
        key,
        "segment_1",
        &["segment_2", "segment_3"],
    )
    .await
    .unwrap();
    assert_eq!(started.source.unwrap().segment_name, "segment_1");
    assert_eq!(started.targets.len(), 2);

    // Case 4: remove blocked by ongoing copy.
    assert_eq!(
        remove_repl(&service, key).await,
        Err(Code::FailedPrecondition)
    );

    // Case 5: second copy blocked by the ongoing task.
    let task_conflict = copy_start(&service, client_id, key, "segment_1", &["segment_4"])
        .await
        .unwrap_err();
    assert_eq!(task_conflict.code(), Code::FailedPrecondition);

    // Case 6: CopyEnd -> three replicas.
    copy_end(&service, client_id, key).await.unwrap();
    assert_eq!(replica_list(&service, key).await.len(), 3);

    // Case 7: missing source -> REPLICA_NOT_FOUND equivalent.
    let bad_source = copy_start(
        &service,
        client_id,
        key,
        "non_existent_segment",
        &["segment_3", "segment_4"],
    )
    .await
    .unwrap_err();
    assert_eq!(bad_source.code(), Code::InvalidArgument);

    // Case 8: missing target -> SEGMENT_NOT_FOUND equivalent.
    let bad_target = copy_start(
        &service,
        client_id,
        key,
        "segment_1",
        &["segment_4", "non_existent_segment"],
    )
    .await
    .unwrap_err();
    assert_eq!(bad_target.code(), Code::FailedPrecondition);

    // Case 9: segment_3 already used, segment_4 new -> one target.
    let skip_started = copy_start(
        &service,
        client_id,
        key,
        "segment_1",
        &["segment_3", "segment_4"],
    )
    .await
    .unwrap();
    assert_eq!(skip_started.source.unwrap().segment_name, "segment_1");
    assert_eq!(skip_started.targets.len(), 1);
    assert_eq!(skip_started.targets[0].segment_name, "segment_4");
    copy_end(&service, client_id, key).await.unwrap();
    assert_eq!(replica_list(&service, key).await.len(), 4);

    // Case 10: already-used target -> zero targets.
    let used = copy_start(&service, client_id, key, "segment_1", &["segment_4"])
        .await
        .unwrap();
    assert_eq!(used.source.unwrap().segment_name, "segment_1");
    assert_eq!(used.targets.len(), 0);

    // Cases 11-12: after lease expiry the ongoing task still blocks removal.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        remove_repl(&service, key).await,
        Err(Code::FailedPrecondition)
    );

    // Case 13: cleanup the task, then removal succeeds after lease expiry.
    copy_end(&service, client_id, key).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(remove_repl(&service, key).await, Ok(()));
}

#[tokio::test]
async fn copy_end_owner_type_and_gone_replica_matrix_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let invalid_client_id = Uuid::new_v4();
    let segment_1 = mount_repl_segment(&service, client_id, "segment_1", 0).await;
    mount_repl_segment(&service, client_id, "segment_2", 1).await;
    let segment_3 = mount_repl_segment(&service, client_id, "segment_3", 2).await;
    let key = "test_key";

    // Case 1: missing key -> OBJECT_NOT_FOUND.
    let missing = MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "non_existent_key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);

    put_repl_object(&service, client_id, key, "segment_1").await;
    put_end_repl_object(&service, client_id, key).await;

    // Case 2: no ongoing task -> OBJECT_NO_REPLICATION_TASK.
    let no_task = MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(no_task.code(), Code::FailedPrecondition);

    copy_start(&service, client_id, key, "segment_1", &["segment_2"])
        .await
        .unwrap();

    // Case 3: wrong client -> ILLEGAL_CLIENT.
    let wrong_client = MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(invalid_client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(wrong_client.code(), Code::PermissionDenied);

    // Case 4: MoveEnd while the task is Copy -> INVALID_PARAMS.
    let move_wrong = MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(move_wrong.code(), Code::PermissionDenied);

    // Case 5: successful CopyEnd -> two replicas.
    copy_end(&service, client_id, key).await;
    assert_eq!(replica_list(&service, key).await.len(), 2);

    // Case 6: source gone -> REPLICA_IS_GONE, surviving segment_2 replica.
    copy_start(&service, client_id, key, "segment_1", &["segment_3"])
        .await
        .unwrap();
    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_1)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
    let source_gone = MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(source_gone.code(), Code::FailedPrecondition);
    let replicas = replica_list(&service, key).await;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "segment_2");

    // Case 7: target gone -> REPLICA_IS_GONE, surviving segment_2 replica.
    copy_start(&service, client_id, key, "segment_2", &["segment_3"])
        .await
        .unwrap();
    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_3)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
    let target_gone = MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(target_gone.code(), Code::FailedPrecondition);
    let replicas = replica_list(&service, key).await;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "segment_2");
}

#[tokio::test]
async fn copy_revoke_full_error_and_source_loss_cleanup_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let invalid_client_id = Uuid::new_v4();
    let segment_1 = mount_repl_segment(&service, client_id, "segment_1", 0).await;
    mount_repl_segment(&service, client_id, "segment_2", 1).await;
    let key = "test_key";

    assert_eq!(
        copy_revoke(&service, client_id, "non_existent_key").await,
        Err(Code::NotFound)
    );

    put_repl_object(&service, client_id, key, "segment_1").await;
    put_end_repl_object(&service, client_id, key).await;
    assert_eq!(
        copy_revoke(&service, client_id, key).await,
        Err(Code::FailedPrecondition)
    );

    copy_start(&service, client_id, key, "segment_1", &["segment_2"])
        .await
        .unwrap();
    assert_eq!(
        copy_revoke(&service, invalid_client_id, key).await,
        Err(Code::PermissionDenied)
    );
    assert_eq!(
        move_revoke(&service, client_id, key).await,
        Err(Code::PermissionDenied)
    );
    assert_eq!(copy_revoke(&service, client_id, key).await, Ok(()));
    assert_eq!(replica_list(&service, key).await.len(), 1);

    copy_start(&service, client_id, key, "segment_1", &["segment_2"])
        .await
        .unwrap();
    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_1)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
    let revoke = copy_revoke(&service, client_id, key).await;
    assert!(revoke.is_ok() || revoke == Err(Code::NotFound));
    assert_eq!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err()
        .code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn move_start_full_error_existing_target_and_cleanup_matrix_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(50),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    for (index, name) in ["segment_1", "segment_2", "segment_3"]
        .into_iter()
        .enumerate()
    {
        mount_repl_segment(&service, client_id, name, index as u64).await;
    }
    let key = "test_key";

    // Case 1: missing key -> OBJECT_NOT_FOUND.
    let missing = move_start(
        &service,
        client_id,
        "non_existent_key",
        "segment_1",
        "segment_2",
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);

    // Case 2: incomplete source -> REPLICA_NOT_FOUND equivalent.
    put_repl_object(&service, client_id, key, "segment_1").await;
    let incomplete = move_start(&service, client_id, key, "segment_1", "segment_2")
        .await
        .unwrap_err();
    assert_eq!(incomplete.code(), Code::InvalidArgument);

    // Case 3: same source and target -> INVALID_PARAMS.
    put_end_repl_object(&service, client_id, key).await;
    let same = move_start(&service, client_id, key, "segment_1", "segment_1")
        .await
        .unwrap_err();
    assert_eq!(same.code(), Code::InvalidArgument);

    // Add a second replica on segment_3 via copy for later cases.
    copy_start(&service, client_id, key, "segment_1", &["segment_3"])
        .await
        .unwrap();
    copy_end(&service, client_id, key).await;

    // Case 4: successful move -> source segment_1, target segment_2.
    let started = move_start(&service, client_id, key, "segment_1", "segment_2")
        .await
        .unwrap();
    assert_eq!(started.source.unwrap().segment_name, "segment_1");
    assert_eq!(started.target.unwrap().segment_name, "segment_2");

    // Case 5: remove blocked by the ongoing move.
    assert_eq!(
        remove_repl(&service, key).await,
        Err(Code::FailedPrecondition)
    );

    // Case 6: second move blocked by the ongoing task.
    let conflict = move_start(&service, client_id, key, "segment_1", "segment_3")
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), Code::FailedPrecondition);

    // Case 7: MoveEnd -> two replicas (segment_2 target + segment_3).
    assert_eq!(move_end(&service, client_id, key).await, Ok(()));
    assert_eq!(replica_list(&service, key).await.len(), 2);

    // Case 8: missing source -> REPLICA_NOT_FOUND equivalent.
    let bad_source = move_start(
        &service,
        client_id,
        key,
        "non_existent_segment",
        "segment_1",
    )
    .await
    .unwrap_err();
    assert_eq!(bad_source.code(), Code::InvalidArgument);

    // Case 8.5: missing target -> SEGMENT_NOT_FOUND equivalent.
    let bad_target = move_start(
        &service,
        client_id,
        key,
        "segment_2",
        "non_existent_segment",
    )
    .await
    .unwrap_err();
    assert_eq!(bad_target.code(), Code::FailedPrecondition);

    // Case 9: existing target -> succeeds with a null target.
    let existing = move_start(&service, client_id, key, "segment_2", "segment_3")
        .await
        .unwrap();
    assert_eq!(existing.source.unwrap().segment_name, "segment_2");
    assert!(existing.target.is_none());

    // Case 10: after lease expiry the ongoing move still blocks removal.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        remove_repl(&service, key).await,
        Err(Code::FailedPrecondition)
    );

    // Case 11: MoveEnd -> one replica on segment_3; removal succeeds after TTL.
    assert_eq!(move_end(&service, client_id, key).await, Ok(()));
    let replicas = replica_list(&service, key).await;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "segment_3");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(remove_repl(&service, key).await, Ok(()));
}

#[tokio::test]
async fn move_end_owner_type_and_source_loss_matrix_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let invalid_client_id = Uuid::new_v4();
    mount_repl_segment(&service, client_id, "segment_1", 0).await;
    let segment_2 = mount_repl_segment(&service, client_id, "segment_2", 1).await;
    let key = "test_key";

    assert_eq!(
        move_end(&service, client_id, "non_existent_key").await,
        Err(Code::NotFound)
    );

    put_repl_object(&service, client_id, key, "segment_1").await;
    put_end_repl_object(&service, client_id, key).await;
    assert_eq!(
        move_end(&service, client_id, key).await,
        Err(Code::FailedPrecondition)
    );

    move_start(&service, client_id, key, "segment_1", "segment_2")
        .await
        .unwrap();
    assert_eq!(
        move_end(&service, invalid_client_id, key).await,
        Err(Code::PermissionDenied)
    );
    assert_eq!(
        copy_end(&service, client_id, key).await,
        Err(Code::PermissionDenied)
    );
    assert_eq!(move_end(&service, client_id, key).await, Ok(()));
    assert_eq!(replica_list(&service, key).await.len(), 1);

    move_start(&service, client_id, key, "segment_2", "segment_1")
        .await
        .unwrap();
    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_2)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
    let ended = move_end(&service, client_id, key).await;
    assert!(ended.is_ok() || ended == Err(Code::NotFound));
    assert_eq!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err()
        .code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn move_revoke_full_error_and_source_loss_cleanup_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let invalid_client_id = Uuid::new_v4();
    let segment_1 = mount_repl_segment(&service, client_id, "segment_1", 0).await;
    mount_repl_segment(&service, client_id, "segment_2", 1).await;
    let key = "test_key";

    assert_eq!(
        move_revoke(&service, client_id, "non_existent_key").await,
        Err(Code::NotFound)
    );

    put_repl_object(&service, client_id, key, "segment_1").await;
    put_end_repl_object(&service, client_id, key).await;
    assert_eq!(
        move_revoke(&service, client_id, key).await,
        Err(Code::FailedPrecondition)
    );

    move_start(&service, client_id, key, "segment_1", "segment_2")
        .await
        .unwrap();
    assert_eq!(
        move_revoke(&service, invalid_client_id, key).await,
        Err(Code::PermissionDenied)
    );
    assert_eq!(
        copy_revoke(&service, client_id, key).await,
        Err(Code::PermissionDenied)
    );
    assert_eq!(move_revoke(&service, client_id, key).await, Ok(()));
    let replicas = replica_list(&service, key).await;
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "segment_1");

    move_start(&service, client_id, key, "segment_1", "segment_2")
        .await
        .unwrap();
    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_1)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
    let revoke = move_revoke(&service, client_id, key).await;
    assert!(revoke.is_ok() || revoke == Err(Code::NotFound));
    assert_eq!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap_err()
        .code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn active_copy_move_sources_survive_eviction_pressure_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(100),
        client_live_ttl: Duration::from_secs(600),
        eviction_interval: Duration::from_millis(5),
        reaper_interval: Duration::from_millis(10),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_repl_segment(&service, client_id, "segment_1", 0).await;
    mount_repl_segment(&service, client_id, "segment_2", 1).await;

    put_repl_object_sized(&service, client_id, "copy_key", 1024 * 1024, "segment_1").await;
    put_end_repl_object(&service, client_id, "copy_key").await;
    put_repl_object_sized(&service, client_id, "move_key", 1024 * 1024, "segment_1").await;
    put_end_repl_object(&service, client_id, "move_key").await;

    copy_start(&service, client_id, "copy_key", "segment_1", &["segment_2"])
        .await
        .unwrap();
    move_start(&service, client_id, "move_key", "segment_1", "segment_2")
        .await
        .unwrap();

    for index in 0..4096 {
        let key = format!("test_key_{index}");
        if try_put_repl_object_sized(&service, client_id, &key, 1024 * 1024, "").await {
            let end = MasterService::put_end(
                &service,
                Request::new(proto::PutEndRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key: key.clone(),
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                }),
            )
            .await;
            if let Err(error) = end {
                panic!("admitted put {key} failed to complete: {error}");
            }
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let removed = MasterService::remove_all(
        &service,
        Request::new(proto::RemoveAllRequest {
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .removed_count;
    assert!(removed > 0);

    assert_eq!(copy_end(&service, client_id, "copy_key").await, Ok(()));
    assert_eq!(move_end(&service, client_id, "move_key").await, Ok(()));
}

#[tokio::test]
async fn timed_out_copy_move_are_discarded_under_pressure_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(100),
        client_live_ttl: Duration::from_secs(600),
        put_start_discard_timeout: Duration::from_secs(1),
        put_start_release_timeout: Duration::from_secs(2),
        eviction_interval: Duration::from_millis(5),
        reaper_interval: Duration::from_millis(10),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_repl_segment(&service, client_id, "segment_1", 0).await;
    mount_repl_segment(&service, client_id, "segment_2", 1).await;

    put_repl_object_sized(&service, client_id, "copy_key", 1024 * 1024, "segment_1").await;
    put_end_repl_object(&service, client_id, "copy_key").await;
    put_repl_object_sized(&service, client_id, "move_key", 1024 * 1024, "segment_1").await;
    put_end_repl_object(&service, client_id, "move_key").await;

    copy_start(&service, client_id, "copy_key", "segment_1", &["segment_2"])
        .await
        .unwrap();
    move_start(&service, client_id, "move_key", "segment_1", "segment_2")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;
    for index in 0..4096 {
        let key = format!("test_key_{index}");
        if try_put_repl_object_sized(&service, client_id, &key, 1024 * 1024, "").await {
            MasterService::put_end(
                &service,
                Request::new(proto::PutEndRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key,
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap();
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    assert_eq!(
        copy_end(&service, client_id, "copy_key").await,
        Err(Code::NotFound)
    );
    assert_eq!(
        move_end(&service, client_id, "move_key").await,
        Err(Code::NotFound)
    );
}

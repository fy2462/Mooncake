mod common;

use common::proto_uuid;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use tonic::{Code, Request};
use uuid::Uuid;

const SEGMENT: &str = "segment_0";

async fn mount_segment(service: &MasterServiceImpl, owner: Uuid) {
    mount_named_segment(service, owner, SEGMENT, 0x300000000).await;
}

async fn mount_named_segment(service: &MasterServiceImpl, owner: Uuid, name: &str, base_addr: u64) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(owner)),
            segment_name: name.into(),
            size: 16 * 1024 * 1024,
            base_addr,
            te_endpoint: name.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

fn config() -> proto::ReplicateConfig {
    config_for(SEGMENT)
}

fn config_for(segment: &str) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        preferred_segment: segment.into(),
        ..Default::default()
    }
}

async fn put_start(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
) -> proto::ReplicaDescriptor {
    let response = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            config: Some(config()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    response.replicas.into_iter().next().unwrap()
}

async fn upsert_start(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
) -> proto::ReplicaDescriptor {
    let response = MasterService::upsert(
        service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            config: Some(config()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    response.replicas.into_iter().next().unwrap()
}

async fn end(service: &MasterServiceImpl, client_id: Uuid, key: &str) -> Result<(), tonic::Status> {
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
    .map(|_| ())
}

async fn get_replica(
    service: &MasterServiceImpl,
    key: &str,
) -> Result<proto::ReplicaDescriptor, tonic::Status> {
    let response = MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await?
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    Ok(response.replicas.into_iter().next().unwrap())
}

async fn exists(service: &MasterServiceImpl, key: &str) -> bool {
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

async fn revoke(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
    MasterService::put_revoke(
        service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

fn buffer_address(replica: &proto::ReplicaDescriptor) -> u64 {
    replica.base_addr.checked_add(replica.offset).unwrap()
}

#[tokio::test]
async fn upsert_new_key_two_phase_state_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id).await;

    let processing = upsert_start(&service, client_id, "upsert_new_key", 1024).await;
    assert_eq!(
        processing.status,
        proto::replica_descriptor::ReplicaStatus::Allocating as i32
    );
    let not_ready = get_replica(&service, "upsert_new_key").await.unwrap_err();
    assert_eq!(not_ready.code(), Code::FailedPrecondition);
    assert_eq!(not_ready.message(), "replica is not ready");

    end(&service, client_id, "upsert_new_key").await.unwrap();
    let complete = get_replica(&service, "upsert_new_key").await.unwrap();
    assert_eq!(
        complete.status,
        proto::replica_descriptor::ReplicaStatus::Complete as i32
    );
}

#[tokio::test]
async fn same_size_upsert_reuses_buffer_and_completes_parity() {
    let service = MasterServiceImpl::default();
    let original_client = Uuid::new_v4();
    let replacement_client = Uuid::new_v4();
    mount_segment(&service, original_client).await;

    let original = put_start(&service, original_client, "upsert_same_size", 1024).await;
    end(&service, original_client, "upsert_same_size")
        .await
        .unwrap();
    let processing = upsert_start(&service, replacement_client, "upsert_same_size", 1024).await;
    assert_eq!(
        processing.status,
        proto::replica_descriptor::ReplicaStatus::Allocating as i32
    );
    assert_eq!(buffer_address(&processing), buffer_address(&original));

    end(&service, replacement_client, "upsert_same_size")
        .await
        .unwrap();
    let complete = get_replica(&service, "upsert_same_size").await.unwrap();
    assert_eq!(
        complete.status,
        proto::replica_descriptor::ReplicaStatus::Complete as i32
    );
}

#[tokio::test]
async fn same_size_upsert_refreshes_client_metadata_parity() {
    let service = MasterServiceImpl::default();
    let client_a = Uuid::new_v4();
    let client_b = Uuid::new_v4();
    mount_segment(&service, client_a).await;
    put_start(&service, client_a, "upsert_refresh_metadata", 1024).await;
    end(&service, client_a, "upsert_refresh_metadata")
        .await
        .unwrap();

    upsert_start(&service, client_b, "upsert_refresh_metadata", 1024).await;
    let former_owner = end(&service, client_a, "upsert_refresh_metadata")
        .await
        .unwrap_err();
    assert_eq!(former_owner.code(), Code::PermissionDenied);
    assert_eq!(former_owner.message(), "illegal client");
    end(&service, client_b, "upsert_refresh_metadata")
        .await
        .unwrap();
}

#[tokio::test]
async fn different_size_upsert_reallocates_and_completes_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id).await;
    let original = put_start(&service, client_id, "upsert_diff_size", 1024).await;
    end(&service, client_id, "upsert_diff_size").await.unwrap();

    let processing = upsert_start(&service, client_id, "upsert_diff_size", 2048).await;
    assert_eq!(
        processing.status,
        proto::replica_descriptor::ReplicaStatus::Allocating as i32
    );
    assert_ne!(buffer_address(&processing), buffer_address(&original));
    end(&service, client_id, "upsert_diff_size").await.unwrap();
    let complete = get_replica(&service, "upsert_diff_size").await.unwrap();
    assert_eq!(
        complete.status,
        proto::replica_descriptor::ReplicaStatus::Complete as i32
    );
}

#[tokio::test]
async fn upsert_rejects_active_replication_task_parity() {
    let service = MasterServiceImpl::default();
    let source_owner = Uuid::new_v4();
    let target_owner = Uuid::new_v4();
    let writer = Uuid::new_v4();
    mount_named_segment(&service, source_owner, "segment_1", 0x300000000).await;
    mount_named_segment(&service, target_owner, "segment_2", 0x400000000).await;

    let started = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(writer)),
            key: "upsert_conflict_copy".into(),
            slice_length: 1024,
            config: Some(config_for("segment_1")),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), 1);
    end(&service, writer, "upsert_conflict_copy").await.unwrap();
    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(source_owner)),
            key: "upsert_conflict_copy".into(),
            source: "segment_1".into(),
            targets: vec!["segment_2".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let conflict = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(writer)),
            key: "upsert_conflict_copy".into(),
            slice_length: 1024,
            config: Some(config_for("segment_1")),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(conflict.code(), Code::FailedPrecondition);
    assert_eq!(conflict.message(), "object has replication task");
}

#[tokio::test]
async fn upsert_preempts_in_progress_put_parity() {
    let service = MasterServiceImpl::default();
    let client_a = Uuid::new_v4();
    let client_b = Uuid::new_v4();
    mount_segment(&service, Uuid::new_v4()).await;
    put_start(&service, client_a, "upsert_preempt", 1024).await;

    let processing = upsert_start(&service, client_b, "upsert_preempt", 1024).await;
    assert_eq!(
        processing.status,
        proto::replica_descriptor::ReplicaStatus::Allocating as i32
    );
    assert!(end(&service, client_a, "upsert_preempt").await.is_err());
    end(&service, client_b, "upsert_preempt").await.unwrap();
    let complete = get_replica(&service, "upsert_preempt").await.unwrap();
    assert_eq!(
        complete.status,
        proto::replica_descriptor::ReplicaStatus::Complete as i32
    );
}

#[tokio::test]
async fn new_key_upsert_revoke_removes_object_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment(&service, Uuid::new_v4()).await;
    upsert_start(&service, client_id, "upsert_revoke", 1024).await;
    revoke(&service, client_id, "upsert_revoke").await;
    assert!(!exists(&service, "upsert_revoke").await);
}

#[tokio::test]
async fn in_place_upsert_revoke_removes_object_parity() {
    let service = MasterServiceImpl::default();
    let original_client = Uuid::new_v4();
    let replacement_client = Uuid::new_v4();
    mount_segment(&service, Uuid::new_v4()).await;
    put_start(&service, original_client, "upsert_inplace_revoke", 1024).await;
    end(&service, original_client, "upsert_inplace_revoke")
        .await
        .unwrap();
    upsert_start(&service, replacement_client, "upsert_inplace_revoke", 1024).await;
    revoke(&service, replacement_client, "upsert_inplace_revoke").await;
    assert!(!exists(&service, "upsert_inplace_revoke").await);
}

#[tokio::test]
async fn mixed_batch_upsert_preserves_order_and_success_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment(&service, Uuid::new_v4()).await;
    put_start(&service, client_id, "key_1", 1024).await;
    end(&service, client_id, "key_1").await.unwrap();

    let start = MasterService::batch_upsert_start(
        &service,
        Request::new(proto::BatchUpsertStartRequest {
            entries: vec![
                proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: "key_1".into(),
                    slice_length: 1024,
                    config: Some(config()),
                    tenant_id: String::new(),
                },
                proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: "key_2".into(),
                    slice_length: 2048,
                    config: Some(config()),
                    tenant_id: String::new(),
                },
            ],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(start.statuses, [0, 0]);
    assert_eq!(start.results.len(), 2);
    assert_eq!(start.results[0].key, "key_1");
    assert_eq!(start.results[0].status, 0);
    assert_eq!(start.results[0].replicas.len(), 1);
    assert_eq!(start.results[1].key, "key_2");
    assert_eq!(start.results[1].status, 0);
    assert_eq!(start.results[1].replicas.len(), 1);

    let finish = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: ["key_1", "key_2"]
                .into_iter()
                .map(|key| proto::PutEndEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: key.into(),
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                })
                .collect(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(finish.statuses, [0, 0]);
}

#[tokio::test]
async fn upsert_preempts_in_progress_upsert_parity() {
    let service = MasterServiceImpl::default();
    let client_a = Uuid::new_v4();
    let client_b = Uuid::new_v4();
    let client_c = Uuid::new_v4();
    mount_segment(&service, Uuid::new_v4()).await;
    put_start(&service, client_a, "upsert_preempt_upsert", 1024).await;
    end(&service, client_a, "upsert_preempt_upsert")
        .await
        .unwrap();

    upsert_start(&service, client_b, "upsert_preempt_upsert", 1024).await;
    let not_ready = get_replica(&service, "upsert_preempt_upsert")
        .await
        .unwrap_err();
    assert_eq!(not_ready.code(), Code::FailedPrecondition);
    assert_eq!(not_ready.message(), "replica is not ready");
    let replacement = upsert_start(&service, client_c, "upsert_preempt_upsert", 1024).await;
    assert_eq!(
        replacement.status,
        proto::replica_descriptor::ReplicaStatus::Allocating as i32
    );
    assert!(
        end(&service, client_b, "upsert_preempt_upsert")
            .await
            .is_err()
    );
    end(&service, client_c, "upsert_preempt_upsert")
        .await
        .unwrap();
    let complete = get_replica(&service, "upsert_preempt_upsert")
        .await
        .unwrap();
    assert_eq!(
        complete.status,
        proto::replica_descriptor::ReplicaStatus::Complete as i32
    );
}

#[tokio::test]
async fn different_size_upsert_revoke_removes_old_and_new_object_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment(&service, Uuid::new_v4()).await;
    put_start(&service, client_id, "upsert_diff_revoke", 1024).await;
    end(&service, client_id, "upsert_diff_revoke")
        .await
        .unwrap();
    assert!(exists(&service, "upsert_diff_revoke").await);
    upsert_start(&service, client_id, "upsert_diff_revoke", 2048).await;
    revoke(&service, client_id, "upsert_diff_revoke").await;
    assert!(!exists(&service, "upsert_diff_revoke").await);
}

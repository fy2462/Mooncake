mod common;

use common::proto_uuid;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use tonic::{Code, Request};
use uuid::Uuid;

const SEGMENT: &str = "segment_0";

async fn mount_segment(service: &MasterServiceImpl, owner: Uuid) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(owner)),
            segment_name: SEGMENT.into(),
            size: 16 * 1024 * 1024,
            base_addr: 0x300000000,
            te_endpoint: SEGMENT.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

fn config() -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        preferred_segment: SEGMENT.into(),
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

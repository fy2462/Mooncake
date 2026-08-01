mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::MasterServiceImpl;
use tonic::Request;
use uuid::Uuid;

async fn mount_memory_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    name: &str,
    base_addr: u64,
    size: u64,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size,
            base_addr,
            te_endpoint: name.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn start_memory_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    replica_num: u32,
) -> Vec<proto::ReplicaDescriptor> {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num,
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas
}

async fn end_memory_object(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
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

async fn get_replicas(
    service: &MasterServiceImpl,
    key: &str,
) -> Result<Vec<proto::ReplicaDescriptor>, tonic::Status> {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|response| response.into_inner().replicas)
}

#[tokio::test]
async fn get_replica_list_missing_and_completed_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let missing = get_replicas(&service, "non_existent").await.unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    mount_memory_segment(
        &service,
        client_id,
        "basic-get:1",
        0x300000000,
        16 * 1024 * 1024,
    )
    .await;
    start_memory_object(&service, client_id, "test_key", 1024, 1).await;
    end_memory_object(&service, client_id, "test_key").await;

    assert!(!get_replicas(&service, "test_key").await.unwrap().is_empty());
}

#[tokio::test]
async fn remove_completed_and_missing_object_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(
        &service,
        client_id,
        "basic-remove:1",
        0x300000000,
        16 * 1024 * 1024,
    )
    .await;
    start_memory_object(&service, client_id, "test_key", 1024, 1).await;
    end_memory_object(&service, client_id, "test_key").await;

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "test_key".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        get_replicas(&service, "test_key").await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    let missing_remove = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "non_existent".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing_remove.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn single_slice_three_replica_descriptor_lifecycle_parity() {
    use proto::replica_descriptor::ReplicaStatus;

    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    const SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
    const SLICE_SIZE: u64 = 5 * 1024 * 1024;
    for index in 0..3 {
        mount_memory_segment(
            &service,
            client_id,
            &format!("segment_{index}"),
            0x300000000 + index * SEGMENT_SIZE,
            SEGMENT_SIZE,
        )
        .await;
    }

    let started =
        start_memory_object(&service, client_id, "multi_slice_object", SLICE_SIZE, 3).await;
    assert_eq!(started.len(), 3);
    assert!(started.iter().all(|replica| {
        replica.status == ReplicaStatus::Allocating as i32 && replica.size == SLICE_SIZE
    }));
    assert_eq!(
        get_replicas(&service, "multi_slice_object")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    end_memory_object(&service, client_id, "multi_slice_object").await;
    let completed = get_replicas(&service, "multi_slice_object").await.unwrap();
    assert_eq!(completed.len(), 3);
    assert!(completed.iter().all(|replica| {
        replica.status == ReplicaStatus::Complete as i32 && replica.size == SLICE_SIZE
    }));
}

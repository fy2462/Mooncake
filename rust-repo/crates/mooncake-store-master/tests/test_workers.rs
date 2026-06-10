mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::MasterServiceImpl;
use tonic::Request;
use uuid::Uuid;

fn proto_nof_segment(id: Uuid, client_id: Uuid, name: &str) -> proto::NoFSegment {
    proto::NoFSegment {
        id: Some(proto_uuid(id)),
        name: name.into(),
        base: 0,
        size: 4096,
        te_endpoint: format!("transport://{name}"),
        client_id: Some(proto_uuid(client_id)),
    }
}

fn nof_replicate_config(segment_name: &str) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 0,
        nof_replica_num: 1,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: String::new(),
        prefer_alloc_in_same_node: false,
        preferred_segments: vec![],
        preferred_nof_segments: vec![segment_name.to_string()],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
    }
}

async fn mount_nof_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_id: Uuid,
    name: &str,
) {
    MasterService::mount_no_f_segment(
        service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto_nof_segment(segment_id, client_id, name)),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_nof_segment_mount_query_and_unmount_lifecycle() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();
    let segment_name = "nof-worker-lifecycle:8000";

    mount_nof_segment(&service, client_id, segment_id, segment_name).await;

    let status = MasterService::query_segment_status(
        &service,
        Request::new(proto::QuerySegmentStatusRequest {
            segment_name: segment_name.into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(status.status, proto::SegmentStatus::Active as i32);

    let status_by_id = MasterService::query_segment_status_by_id(
        &service,
        Request::new(proto::QuerySegmentStatusByIdRequest {
            segment_id: Some(proto_uuid(segment_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(status_by_id.status, proto::SegmentStatus::Active as i32);

    MasterService::unmount_no_f_segment(
        &service,
        Request::new(proto::UnmountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_id: Some(proto_uuid(segment_id)),
        }),
    )
    .await
    .unwrap();

    let missing = MasterService::query_segment_status(
        &service,
        Request::new(proto::QuerySegmentStatusRequest {
            segment_name: segment_name.into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_nof_unmount_clears_nof_only_object_handles() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();
    let segment_name = "nof-worker-cleanup:8000";
    let key = "nof-only-worker-key";

    mount_nof_segment(&service, client_id, segment_id, segment_name).await;

    let put_start = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(nof_replicate_config(segment_name)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(put_start.replicas.len(), 1);
    assert_eq!(
        put_start.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::NofSsd as i32
    );

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::NofSsd as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let before_unmount = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(before_unmount.replicas.len(), 1);

    MasterService::unmount_no_f_segment(
        &service,
        Request::new(proto::UnmountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_id: Some(proto_uuid(segment_id)),
        }),
    )
    .await
    .unwrap();

    let after_unmount = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(after_unmount.code(), tonic::Code::NotFound);
}

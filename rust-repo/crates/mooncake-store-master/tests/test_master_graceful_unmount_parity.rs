mod common;

use common::proto_uuid;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use std::time::Duration;
use tonic::{Code, Request};
use uuid::Uuid;

async fn mount(service: &MasterServiceImpl, owner: Uuid, name: &str, base_addr: u64) -> Uuid {
    let response = MasterService::mount_segment(
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
    .unwrap()
    .into_inner();
    let segment_id = response.segment_id.unwrap();
    Uuid::from_u64_pair(segment_id.high, segment_id.low)
}

async fn graceful(
    service: &MasterServiceImpl,
    segment_id: Uuid,
    client_id: Uuid,
    grace_period_ms: u64,
) -> Result<(), tonic::Status> {
    MasterService::graceful_unmount_segment(
        service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms,
        }),
    )
    .await
    .map(|_| ())
}

async fn status_by_name(service: &MasterServiceImpl, name: &str) -> Result<i32, tonic::Status> {
    MasterService::query_segment_status(
        service,
        Request::new(proto::QuerySegmentStatusRequest {
            segment_name: name.into(),
        }),
    )
    .await
    .map(|response| response.into_inner().status)
}

async fn status_by_id(service: &MasterServiceImpl, id: Uuid) -> Result<i32, tonic::Status> {
    MasterService::query_segment_status_by_id(
        service,
        Request::new(proto::QuerySegmentStatusByIdRequest {
            segment_id: Some(proto_uuid(id)),
        }),
    )
    .await
    .map(|response| response.into_inner().status)
}

#[tokio::test]
async fn graceful_unmount_sets_correct_status_parity() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let name = "graceful_test_segment";
    let segment_id = mount(&service, owner, name, 0x300000000).await;

    assert_eq!(
        status_by_name(&service, name).await.unwrap(),
        proto::SegmentStatus::Active as i32
    );
    graceful(&service, segment_id, owner, 1000).await.unwrap();
    assert_eq!(
        status_by_name(&service, name).await.unwrap(),
        proto::SegmentStatus::GracefullyUnmounting as i32
    );
}

#[tokio::test]
async fn graceful_unmount_rejects_wrong_client_parity() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let wrong_client = Uuid::new_v4();
    let segment_id = mount(&service, owner, "graceful_owner_segment", 0x300000000).await;

    let error = graceful(&service, segment_id, wrong_client, 1000)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
    graceful(&service, segment_id, owner, 1000).await.unwrap();
}

#[tokio::test]
async fn graceful_unmount_timer_expires_and_unmounts_parity() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let name = "graceful_timer_segment";
    let segment_id = mount(&service, owner, name, 0x300000000).await;

    graceful(&service, segment_id, owner, 50).await.unwrap();
    assert_eq!(
        status_by_name(&service, name).await.unwrap(),
        proto::SegmentStatus::GracefullyUnmounting as i32
    );
    tokio::time::sleep(Duration::from_millis(300)).await;

    let after = status_by_name(&service, name).await;
    assert!(
        after.is_err()
            || matches!(after, Ok(status) if status == proto::SegmentStatus::Undefined as i32),
        "segment should be absent or undefined, got {after:?}"
    );
}

#[tokio::test]
async fn graceful_unmount_status_by_id_with_reused_name_parity() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let name = "graceful_reused_name_segment";
    let old_id = mount(&service, owner, name, 0x300000000).await;
    graceful(&service, old_id, owner, 50).await.unwrap();
    let new_id = mount(&service, owner, name, 0x400000000).await;
    assert_ne!(old_id, new_id);

    assert_eq!(
        status_by_id(&service, old_id).await.unwrap(),
        proto::SegmentStatus::GracefullyUnmounting as i32
    );
    assert_eq!(
        status_by_id(&service, new_id).await.unwrap(),
        proto::SegmentStatus::Active as i32
    );
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(status_by_id(&service, old_id).await.is_err());
    assert_eq!(
        status_by_id(&service, new_id).await.unwrap(),
        proto::SegmentStatus::Active as i32
    );
    assert_eq!(
        status_by_name(&service, name).await.unwrap(),
        proto::SegmentStatus::Active as i32
    );
}

#[tokio::test]
async fn graceful_unmount_earlier_timer_preempts_wait_parity() {
    let service = MasterServiceImpl::default();
    let owner = Uuid::new_v4();
    let long_name = "graceful_long_timer_segment";
    let short_name = "graceful_short_timer_segment";
    let long_id = mount(&service, owner, long_name, 0x300000000).await;
    let short_id = mount(&service, owner, short_name, 0x400000000).await;

    graceful(&service, long_id, owner, 1000).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    graceful(&service, short_id, owner, 50).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let short_status = status_by_name(&service, short_name).await;
    assert!(
        short_status.is_err()
            || matches!(short_status, Ok(status) if status == proto::SegmentStatus::Undefined as i32),
        "short segment should be absent or undefined, got {short_status:?}"
    );
    assert_eq!(
        status_by_name(&service, long_name).await.unwrap(),
        proto::SegmentStatus::GracefullyUnmounting as i32
    );
}

#[tokio::test]
async fn graceful_unmount_prevents_new_allocation_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let segment1 = "graceful_seg1";
    let segment2 = "graceful_seg2";
    let segment1_id = mount(&service, client_id, segment1, 0x300000000).await;
    mount(&service, client_id, segment2, 0x400000000).await;

    let first = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_prevent_alloc".into(),
            slice_length: 1024,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: segment1.into(),
                ..Default::default()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first.replicas.len(), 1);
    assert_eq!(first.replicas[0].segment_name, segment1);
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_prevent_alloc".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    graceful(&service, segment1_id, client_id, 1000)
        .await
        .unwrap();
    assert_eq!(
        status_by_name(&service, segment1).await.unwrap(),
        proto::SegmentStatus::GracefullyUnmounting as i32
    );
    let existing = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "test_key_prevent_alloc".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(existing.replicas.len(), 1);
    assert_eq!(existing.replicas[0].segment_name, segment1);
    assert_eq!(
        status_by_name(&service, segment2).await.unwrap(),
        proto::SegmentStatus::Active as i32
    );

    let second = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "test_key_after_graceful".into(),
            slice_length: 1024,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                ..Default::default()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second.replicas.len(), 1);
    assert_eq!(second.replicas[0].segment_name, segment2);
}

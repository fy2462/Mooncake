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

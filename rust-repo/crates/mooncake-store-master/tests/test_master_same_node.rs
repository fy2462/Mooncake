mod common;
use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
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

#[tokio::test]
async fn test_put_start_rejects_same_node_preference_with_nof_replicas() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let remote_client_id = Uuid::new_v4();
    let same_host_nof_owner = Uuid::new_v4();
    let remote_nof_owner = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "same-host:1001".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(remote_client_id)),
            segment_name: "remote-host:1001".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_no_f_segment(
        &service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(same_host_nof_owner)),
            segment: Some(proto_nof_segment(
                Uuid::new_v4(),
                same_host_nof_owner,
                "same-host:nof-1",
            )),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_no_f_segment(
        &service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(remote_nof_owner)),
            segment: Some(proto_nof_segment(
                Uuid::new_v4(),
                remote_nof_owner,
                "remote-host:nof-1",
            )),
        }),
    )
    .await
    .unwrap();

    let err = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "same-node-nof-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: true,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
            }),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message()
            .contains("prefer_alloc_in_same_node is not supported with NoF replicas")
    );
}

#[tokio::test]
async fn test_put_start_same_node_nof_requires_matching_host() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let nof_owner = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "same-host:1001".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_no_f_segment(
        &service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(nof_owner)),
            segment: Some(proto_nof_segment(
                Uuid::new_v4(),
                nof_owner,
                "remote-host:nof-1",
            )),
        }),
    )
    .await
    .unwrap();

    let err = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "same-node-nof-miss".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: true,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
            }),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message()
            .contains("prefer_alloc_in_same_node is not supported with NoF replicas")
    );
}

#[tokio::test]
async fn test_client_monitor_reaps_expired_clients() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        client_live_ttl: Duration::from_millis(20),
        client_monitor_interval: Duration::from_millis(5),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "ttl:1".into(),
            size: 2048,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
    let metadata_state = service.metadata_state();
    assert!(metadata_state.nodes.read().await.contains_key("ttl"));

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "ttl-key".into(),
            slice_length: 64,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "ttl:1".into(),
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
            key: "ttl-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    for _ in 0..20 {
        if MasterService::query_ip(
            &service,
            Request::new(proto::QueryIpRequest {
                client_id: Some(proto_uuid(client_id)),
            }),
        )
        .await
        .is_err()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        MasterService::query_ip(
            &service,
            Request::new(proto::QueryIpRequest {
                client_id: Some(proto_uuid(client_id)),
            }),
        )
        .await
        .is_err()
    );
    assert!(!metadata_state.nodes.read().await.contains_key("ttl"));
    assert!(
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "ttl-key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .is_err()
    );
}

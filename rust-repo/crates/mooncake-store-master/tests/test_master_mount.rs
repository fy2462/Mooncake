use mooncake_store_master::allocator::{MemoryAllocatorKind, CACHELIB_SLAB_SIZE};
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn test_ping_requires_remount_before_ok_status() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let client_proto = proto::Uuid {
        high: client_id.as_u64_pair().0,
        low: client_id.as_u64_pair().1,
    };

    let first_ping = MasterService::ping(
        &service,
        Request::new(proto::PingRequest {
            client_id: Some(client_proto.clone()),
            mounted_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        first_ping.client_status,
        proto::ClientStatus::NeedRemount as i32
    );

    service.set_view_version(77);

    let second_ping = MasterService::ping(
        &service,
        Request::new(proto::PingRequest {
            client_id: Some(client_proto.clone()),
            mounted_segments: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        second_ping.client_status,
        proto::ClientStatus::NeedRemount as i32
    );

    MasterService::re_mount_segment(
        &service,
        Request::new(proto::ReMountSegmentRequest {
            client_id: Some(client_proto.clone()),
            segment_names: vec!["ping-host:1234".into()],
            segment_sizes: vec![2048],
            base_addrs: vec![0x100000000],
            te_endpoints: vec!["tcp://ping-host".into()],
            protocols: vec!["tcp".into()],
        }),
    )
    .await
    .unwrap();

    let resp = MasterService::ping(
        &service,
        Request::new(proto::PingRequest {
            client_id: Some(client_proto),
            mounted_segments: vec!["ping-host:1234".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(resp.view_version_id, 77);
    assert_eq!(resp.client_status, proto::ClientStatus::Ok as i32);
}

#[tokio::test]
async fn test_query_ip_derives_address_from_mounted_segment() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "10.0.0.1:1234".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();

    let resp = MasterService::query_ip(
        &service,
        Request::new(proto::QueryIpRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(resp.addresses, vec!["10.0.0.1"]);
}

#[tokio::test]
async fn test_mount_segment_updates_http_metadata_state() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "10.0.0.2:4321".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();

    let metadata_state = service.metadata_state();
    let nodes = metadata_state.nodes.read().await;
    let node = nodes.get("10.0.0.2").unwrap();
    assert_eq!(node.rpc_port, 4321);
}

#[tokio::test]
async fn test_mount_segment_rejects_zero_base_or_size() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    let err = MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "bad:1".into(),
            size: 1024,
            base_addr: 0,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_mount_segment_cachelib_requires_slab_alignment() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    let err = MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "cachelib-bad:1".into(),
            size: CACHELIB_SLAB_SIZE - 1,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_remount_segment_is_idempotent_per_client_and_name() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    let req = proto::ReMountSegmentRequest {
        client_id: Some(proto::Uuid {
            high: client_id.as_u64_pair().0,
            low: client_id.as_u64_pair().1,
        }),
        segment_names: vec!["host-a:1111".into()],
        segment_sizes: vec![2048],
        base_addrs: vec![0x100000000],
        te_endpoints: vec![String::new()],
        protocols: vec![String::new()],
    };

    MasterService::re_mount_segment(&service, Request::new(req.clone()))
        .await
        .unwrap();
    MasterService::re_mount_segment(&service, Request::new(req))
        .await
        .unwrap();

    let segments =
        MasterService::get_all_segments(&service, Request::new(proto::GetAllSegmentsRequest {}))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(segments.segments, vec!["host-a:1111"]);
}

#[tokio::test]
async fn test_graceful_unmount_segment_removes_after_delay() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "host-g:3333".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();

    let mounted_segment_id = service.segment_id_by_name("host-g:3333").unwrap();

    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto::Uuid {
                high: mounted_segment_id.as_u64_pair().0,
                low: mounted_segment_id.as_u64_pair().1,
            }),
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            grace_period_ms: 20,
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;

    let segments =
        MasterService::get_all_segments(&service, Request::new(proto::GetAllSegmentsRequest {}))
            .await
            .unwrap()
            .into_inner();

    assert!(!segments
        .segments
        .iter()
        .any(|segment| segment == "host-g:3333"));
}

use mooncake_store_master::allocator::{MemoryAllocatorKind, CACHELIB_SLAB_SIZE};
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::Request;
use uuid::Uuid;

fn proto_uuid(id: Uuid) -> proto::Uuid {
    proto::Uuid {
        high: id.as_u64_pair().0,
        low: id.as_u64_pair().1,
    }
}

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
async fn test_mount_segment_returns_id_usable_for_unmount() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    let mount = MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "return-id:1234".into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let segment_id = mount.segment_id.expect("mount should return segment id");

    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(segment_id),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();

    let status = MasterService::query_segment_status(
        &service,
        Request::new(proto::QuerySegmentStatusRequest {
            segment_name: "return-id:1234".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
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
async fn test_get_segments_detail_includes_memory_and_nof_segments() {
    let service = MasterServiceImpl::default();
    let memory_client_id = Uuid::new_v4();
    let nof_client_id = Uuid::new_v4();
    let nof_segment_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(memory_client_id)),
            segment_name: "detail-host:1234".into(),
            size: 8192,
            base_addr: 0x100000000,
            te_endpoint: "tcp://detail-host".into(),
            protocol: "tcp".into(),
        }),
    )
    .await
    .unwrap();

    MasterService::mount_no_f_segment(
        &service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(nof_client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(nof_segment_id)),
                name: "traddr:10.1.1.8 trsvcid:4420 subnqn:nqn.test trtype:TCP adrfam:IPv4 ns:1"
                    .into(),
                base: 0x200000000,
                size: 16384,
                te_endpoint:
                    "traddr:10.1.1.8 trsvcid:4420 subnqn:nqn.test trtype:TCP adrfam:IPv4 ns:1"
                        .into(),
                client_id: Some(proto_uuid(nof_client_id)),
            }),
        }),
    )
    .await
    .unwrap();

    let response = MasterService::get_segments_detail(
        &service,
        Request::new(proto::GetSegmentsDetailRequest {}),
    )
    .await
    .unwrap()
    .into_inner();

    let memory = response
        .segments
        .iter()
        .find(|segment| segment.segment_name == "detail-host:1234")
        .expect("memory segment detail");
    assert!(!memory.nof);
    assert_eq!(memory.protocol, "tcp");
    assert_eq!(memory.size_bytes, 8192);
    assert_eq!(memory.allocator_capacity_bytes, 8192);
    assert_eq!(memory.status, proto::SegmentStatus::Active as i32);

    let nof = response
        .segments
        .iter()
        .find(|segment| segment.segment_id == Some(proto_uuid(nof_segment_id)))
        .expect("NoF segment detail");
    assert!(nof.nof);
    assert_eq!(nof.protocol, "nof");
    assert_eq!(nof.client_id, Some(proto_uuid(nof_client_id)));
    assert_eq!(nof.base_address, 0x200000000);
    assert_eq!(nof.allocator_capacity_bytes, 16384);
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

#[tokio::test]
async fn test_unmount_segment_missing_is_idempotent() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(Uuid::new_v4())),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_nof_disabled_rejects_nof_operations() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_nof: false,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();

    let mount_err = MasterService::mount_no_f_segment(
        &service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(segment_id)),
                name: "nof-disabled:1".into(),
                base: 0x100000000,
                size: 4096,
                te_endpoint: String::new(),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(mount_err.code(), tonic::Code::Unavailable);

    let unmount_err = MasterService::unmount_no_f_segment(
        &service,
        Request::new(proto::UnmountNoFSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(unmount_err.code(), tonic::Code::Unavailable);
}

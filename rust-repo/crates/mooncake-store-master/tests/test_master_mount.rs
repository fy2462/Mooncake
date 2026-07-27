use mooncake_store_master::allocator::{
    AllocationStrategy, CACHELIB_SLAB_SIZE, MemoryAllocatorKind,
};
use mooncake_store_master::ha::{HaError, OpLogPollResult, OpLogRecord};
use mooncake_store_master::oplog::{InMemoryOpLog, OpLogManager, OpLogStore};
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::Request;
use uuid::Uuid;

struct FailingFlushOpLog {
    inner: InMemoryOpLog,
    flush_count: usize,
    fail_on_flush: usize,
}

impl FailingFlushOpLog {
    fn new(fail_on_flush: usize) -> Self {
        Self {
            inner: InMemoryOpLog::new(32),
            flush_count: 0,
            fail_on_flush,
        }
    }
}

impl OpLogStore for FailingFlushOpLog {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.inner.append(entry)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        self.inner.read_since(since_seq, max_count)
    }

    fn latest_sequence(&self) -> u64 {
        self.inner.latest_sequence()
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        self.inner.max_sequence_id()
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.inner.update_latest_sequence_id(sequence_id)
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        self.inner
            .record_snapshot_sequence_id(snapshot_id, sequence_id)
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        self.inner.get_snapshot_sequence_id(snapshot_id)
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        self.inner.cleanup_before(before_sequence_id)
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        self.flush_count += 1;
        if self.flush_count == self.fail_on_flush {
            return Err(HaError::InvalidBackend(
                "injected durable flush failure".into(),
            ));
        }
        self.inner.flush_durable()
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        self.inner.poll_from(since_seq, max_count)
    }
}

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
            segment_ids: vec![],
            host_ids: vec![],
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
            host_id: String::new(),
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
            host_id: String::new(),
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
            host_id: String::new(),
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
            host_id: String::new(),
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
            host_id: String::new(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_mount_cxl_segment_uses_configured_shared_capacity() {
    let cxl_size = CACHELIB_SLAB_SIZE * 2;
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        allocation_strategy: AllocationStrategy::Cxl,
        memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
        enable_cxl: true,
        cxl_size,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    let response = MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "cxl-host:1234".into(),
            size: cxl_size,
            // CXL descriptors use device-relative offsets; Master does not
            // require a process address for allocation.
            base_addr: 0,
            te_endpoint: "cxl-host:1234".into(),
            protocol: "cxl".into(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.segment_id.is_some());
}

#[tokio::test]
async fn test_mount_cxl_segment_fails_closed_when_disabled_or_size_mismatches() {
    let client_id = Uuid::new_v4();
    let request = |size| proto::MountSegmentRequest {
        client_id: Some(proto_uuid(client_id)),
        segment_name: "cxl-host:1234".into(),
        size,
        base_addr: 0,
        te_endpoint: "cxl-host:1234".into(),
        protocol: "cxl".into(),
        host_id: String::new(),
    };

    let disabled = MasterServiceImpl::default();
    let error = MasterService::mount_segment(&disabled, Request::new(request(CACHELIB_SLAB_SIZE)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);

    let enabled = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        allocation_strategy: AllocationStrategy::Cxl,
        memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
        enable_cxl: true,
        cxl_size: CACHELIB_SLAB_SIZE * 2,
        ..Default::default()
    });
    let error = MasterService::mount_segment(&enabled, Request::new(request(CACHELIB_SLAB_SIZE)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
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
        te_endpoints: vec!["tcp://host-a".into()],
        protocols: vec!["tcp".into()],
        segment_ids: vec![],
        host_ids: vec![],
    };

    MasterService::re_mount_segment(&service, Request::new(req.clone()))
        .await
        .unwrap();
    let original_id = service.segment_id_by_name("host-a:1111").unwrap();
    let mut rebound = req;
    rebound.base_addrs = vec![0x200000000];
    rebound.te_endpoints = vec!["tcp://host-a-new-term".into()];
    MasterService::re_mount_segment(&service, Request::new(rebound))
        .await
        .unwrap();

    let segments =
        MasterService::get_all_segments(&service, Request::new(proto::GetAllSegmentsRequest {}))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(segments.segments, vec!["host-a:1111"]);
    assert_eq!(
        service.segment_id_by_name("host-a:1111"),
        Some(original_id),
        "ReMount must preserve durable segment identity"
    );
    let detail = service
        .segments_detail_snapshot()
        .into_iter()
        .find(|detail| detail.segment_name == "host-a:1111")
        .unwrap();
    assert_eq!(detail.base_address, 0x200000000);
    assert_eq!(detail.te_endpoint, "tcp://host-a-new-term");
}

#[tokio::test]
async fn test_mount_retry_reuses_memory_segment_identity_without_new_oplog() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(Some(Box::new(InMemoryOpLog::new(32))), 0)),
    );
    let client_id = Uuid::new_v4();
    let request = || proto::MountSegmentRequest {
        client_id: Some(proto_uuid(client_id)),
        segment_name: "mount-retry:3333".into(),
        size: 4096,
        base_addr: 0x100000000,
        te_endpoint: "tcp://mount-retry".into(),
        protocol: "tcp".into(),
        host_id: "mount-retry".into(),
    };

    let first = MasterService::mount_segment(&service, Request::new(request()))
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();
    let second = MasterService::mount_segment(&service, Request::new(request()))
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(service.oplog_manager().lock().latest_sequence(), 1);
    assert_eq!(service.segments_detail_snapshot().len(), 1);
}

#[tokio::test]
async fn test_memory_mount_identity_is_stable_across_master_instances() {
    let first_service = MasterServiceImpl::default();
    let second_service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let request = || proto::MountSegmentRequest {
        client_id: Some(proto_uuid(client_id)),
        segment_name: "mount-cross-leader-retry:3333".into(),
        size: 4096,
        base_addr: 0x100000000,
        te_endpoint: "tcp://mount-cross-leader-retry".into(),
        protocol: "tcp".into(),
        host_id: "mount-cross-leader-retry".into(),
    };

    let first = MasterService::mount_segment(&first_service, Request::new(request()))
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();
    let second = MasterService::mount_segment(&second_service, Request::new(request()))
        .await
        .unwrap()
        .into_inner()
        .segment_id
        .unwrap();

    assert_eq!(
        first, second,
        "the same client mount request must address one UUID on every leader"
    );
}

#[tokio::test]
async fn test_mount_retry_reuses_nof_id_and_rejects_endpoint_alias() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig {
            enable_nof: true,
            ..Default::default()
        },
        Some(OpLogManager::new(Some(Box::new(InMemoryOpLog::new(32))), 0)),
    );
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();
    let request = |id| proto::MountNoFSegmentRequest {
        client_id: Some(proto_uuid(client_id)),
        segment: Some(proto::NoFSegment {
            id: Some(proto_uuid(id)),
            name: "nof-mount-retry:3333".into(),
            base: 0,
            size: 4096,
            te_endpoint: "nof://mount-retry".into(),
            client_id: Some(proto_uuid(client_id)),
        }),
    };

    MasterService::mount_no_f_segment(&service, Request::new(request(segment_id)))
        .await
        .unwrap();
    MasterService::mount_no_f_segment(&service, Request::new(request(segment_id)))
        .await
        .unwrap();
    let alias_error =
        MasterService::mount_no_f_segment(&service, Request::new(request(Uuid::new_v4())))
            .await
            .unwrap_err();

    assert_eq!(alias_error.code(), tonic::Code::AlreadyExists);
    assert_eq!(service.oplog_manager().lock().latest_sequence(), 1);
    assert_eq!(
        service
            .capture_loaded_snapshot("nof-mount-retry")
            .nof_segments
            .len(),
        1
    );
}

#[tokio::test]
async fn test_identity_aware_remount_accepts_multiple_mrs_with_same_name() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let request = |first_base, second_base| proto::ReMountSegmentRequest {
        client_id: Some(proto_uuid(client_id)),
        segment_names: vec!["multi-mr:1234".into(), "multi-mr:1234".into()],
        segment_sizes: vec![4096, 2048],
        base_addrs: vec![first_base, second_base],
        te_endpoints: vec!["tcp://multi-mr".into(), "tcp://multi-mr".into()],
        protocols: vec!["tcp".into(), "tcp".into()],
        segment_ids: vec![proto_uuid(first_id), proto_uuid(second_id)],
        host_ids: vec![],
    };

    MasterService::re_mount_segment(&service, Request::new(request(0x100000000, 0x200000000)))
        .await
        .unwrap();
    MasterService::re_mount_segment(&service, Request::new(request(0x300000000, 0x400000000)))
        .await
        .unwrap();

    let mut details = service
        .segments_detail_snapshot()
        .into_iter()
        .filter(|detail| detail.segment_name == "multi-mr:1234")
        .collect::<Vec<_>>();
    details.sort_by_key(|detail| detail.base_address);
    assert_eq!(details.len(), 2);
    assert_eq!(
        details
            .iter()
            .map(|detail| detail.base_address)
            .collect::<Vec<_>>(),
        vec![0x300000000, 0x400000000]
    );
    let ids = details
        .iter()
        .map(|detail| {
            let id = detail.segment_id.as_ref().unwrap();
            Uuid::from_u64_pair(id.high, id.low)
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids, std::collections::HashSet::from([first_id, second_id]));
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
            host_id: String::new(),
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

    MasterService::re_mount_no_f_segment(
        &service,
        Request::new(proto::ReMountNoFSegmentRequest {
            client_id: Some(proto_uuid(nof_client_id)),
            segments: vec![proto::NoFSegment {
                id: Some(proto_uuid(nof_segment_id)),
                name: "traddr:10.1.1.8 trsvcid:4420 subnqn:nqn.test trtype:TCP adrfam:IPv4 ns:1"
                    .into(),
                base: 0x300000000,
                size: 16384,
                te_endpoint:
                    "traddr:10.1.1.9 trsvcid:4420 subnqn:nqn.test trtype:TCP adrfam:IPv4 ns:1"
                        .into(),
                client_id: Some(proto_uuid(nof_client_id)),
            }],
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
    assert_eq!(nof.base_address, 0x300000000);
    assert!(nof.te_endpoint.contains("10.1.1.9"));
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
            host_id: String::new(),
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

    let status = MasterService::query_segment_status_by_id(
        &service,
        Request::new(proto::QuerySegmentStatusByIdRequest {
            segment_id: Some(proto_uuid(mounted_segment_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        status.status,
        proto::SegmentStatus::GracefullyUnmounting as i32
    );

    let allocation = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "must-not-use-graceful-segment".into(),
            slice_length: 64,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "host-g:3333".into(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
        }),
    )
    .await;
    assert!(allocation.is_err());

    tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;

    let segments =
        MasterService::get_all_segments(&service, Request::new(proto::GetAllSegmentsRequest {}))
            .await
            .unwrap()
            .into_inner();

    assert!(
        !segments
            .segments
            .iter()
            .any(|segment| segment == "host-g:3333")
    );
}

#[tokio::test]
async fn test_graceful_unmount_pauses_on_standby_and_completes_after_promotion() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(Some(Box::new(InMemoryOpLog::new(32))), 0)),
    );
    let client_id = Uuid::new_v4();
    let segment_name = "host-graceful-ha:3333";
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();
    assert_eq!(service.oplog_manager().lock().latest_sequence(), 1);

    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 80,
        }),
    )
    .await
    .unwrap();
    assert_eq!(service.oplog_manager().lock().latest_sequence(), 2);

    service.set_service_available(false);
    tokio::time::sleep(std::time::Duration::from_millis(130)).await;
    assert!(
        service.segment_id_by_name(segment_name).is_some(),
        "standby must not execute an expired leader-side timer"
    );
    assert_eq!(
        service
            .capture_loaded_snapshot("standby")
            .graceful_unmounts
            .len(),
        1
    );
    assert_eq!(service.oplog_manager().lock().latest_sequence(), 2);

    service.set_service_available(true);
    for _ in 0..100 {
        if service.segment_id_by_name(segment_name).is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(
        service.segment_id_by_name(segment_name).is_none(),
        "promotion must immediately execute an already-expired deadline"
    );
    assert_eq!(
        service.oplog_manager().lock().latest_sequence(),
        3,
        "scheduler completion must append the final unmount oplog"
    );

    let ping = MasterService::ping(
        &service,
        Request::new(proto::PingRequest {
            client_id: Some(proto_uuid(client_id)),
            mounted_segments: Vec::new(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        ping.view_version_id, 3,
        "mount, graceful-start, and completion each bump the topology view"
    );
}

#[tokio::test]
async fn test_mount_flush_failure_does_not_publish_memory_segment() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(
            Some(Box::new(FailingFlushOpLog::new(1))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_name = "host-mount-flush-fail:3333";

    let error = MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());
    assert_eq!(service.segment_id_by_name(segment_name), None);
    assert!(
        service
            .capture_loaded_snapshot("mount-flush-failed")
            .segments
            .is_empty(),
        "a non-durable Memory mount must not become snapshot-visible"
    );
}

#[tokio::test]
async fn test_mount_flush_failure_does_not_publish_nof_segment() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig {
            enable_nof: true,
            ..Default::default()
        },
        Some(OpLogManager::new(
            Some(Box::new(FailingFlushOpLog::new(1))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();

    let error = MasterService::mount_no_f_segment(
        &service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(segment_id)),
                name: "nof-mount-flush-fail:3333".into(),
                base: 0x100000000,
                size: 4096,
                te_endpoint: "nof://mount-flush-fail".into(),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());
    assert!(
        service
            .capture_loaded_snapshot("nof-mount-flush-failed")
            .nof_segments
            .is_empty(),
        "a non-durable NoF mount must not become snapshot-visible"
    );
}

#[tokio::test]
async fn test_remount_new_flush_failure_does_not_publish_segment() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(
            Some(Box::new(FailingFlushOpLog::new(1))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();
    let segment_name = "host-remount-flush-fail:3333";

    let error = MasterService::re_mount_segment(
        &service,
        Request::new(proto::ReMountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_names: vec![segment_name.into()],
            segment_sizes: vec![4096],
            base_addrs: vec![0x100000000],
            te_endpoints: vec!["tcp://remount-flush-fail".into()],
            protocols: vec!["tcp".into()],
            segment_ids: vec![proto_uuid(segment_id)],
            host_ids: vec!["host-remount-flush-fail".into()],
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());
    assert_eq!(service.segment_id_by_name(segment_name), None);
    assert!(
        service
            .capture_loaded_snapshot("remount-flush-failed")
            .segments
            .is_empty()
    );
}

#[tokio::test]
async fn test_remount_new_flush_failure_does_not_publish_nof_segment() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig {
            enable_nof: true,
            ..Default::default()
        },
        Some(OpLogManager::new(
            Some(Box::new(FailingFlushOpLog::new(1))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();

    let error = MasterService::re_mount_no_f_segment(
        &service,
        Request::new(proto::ReMountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segments: vec![proto::NoFSegment {
                id: Some(proto_uuid(segment_id)),
                name: "nof-remount-flush-fail:3333".into(),
                base: 0x100000000,
                size: 4096,
                te_endpoint: "nof://remount-flush-fail".into(),
                client_id: Some(proto_uuid(client_id)),
            }],
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());
    assert!(
        service
            .capture_loaded_snapshot("nof-remount-flush-failed")
            .nof_segments
            .is_empty()
    );
}

#[tokio::test]
async fn test_graceful_unmount_flush_failure_keeps_new_intent_unpublished_and_fences() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(
            // Mount is flush #1; the graceful intent must fail on flush #2.
            Some(Box::new(FailingFlushOpLog::new(2))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_name = "host-graceful-flush-fail:3333";
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();

    let error = MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 60_000,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());
    service.set_service_available(true);
    assert!(
        !service.is_service_available(),
        "a leadership callback must not reopen a durability-fenced service"
    );

    let snapshot = service.capture_loaded_snapshot("flush-failed");
    assert!(snapshot.graceful_unmounts.is_empty());
    assert_eq!(
        snapshot
            .segments
            .iter()
            .find(|segment| segment.segment.id == segment_id)
            .unwrap()
            .status,
        proto::SegmentStatus::Active
    );
}

#[tokio::test]
async fn test_failed_earlier_graceful_deadline_preserves_previously_durable_intent() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(
            // Mount and the first intent are flushes #1/#2.
            Some(Box::new(FailingFlushOpLog::new(3))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_name = "host-graceful-earlier-flush-fail:3333";
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();

    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 60_000,
        }),
    )
    .await
    .unwrap();
    let before = service.capture_loaded_snapshot("before-earlier-failure");
    let durable_deadline = before.graceful_unmounts[0].deadline_epoch_ms;

    let error = MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 1,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());

    let after = service.capture_loaded_snapshot("after-earlier-failure");
    assert_eq!(after.graceful_unmounts.len(), 1);
    assert_eq!(
        after.graceful_unmounts[0].deadline_epoch_ms, durable_deadline,
        "a failed earlier intent must not roll back the previously durable deadline"
    );
    assert_eq!(
        after
            .segments
            .iter()
            .find(|segment| segment.segment.id == segment_id)
            .unwrap()
            .status,
        proto::SegmentStatus::GracefullyUnmounting
    );
}

#[tokio::test]
async fn test_repeated_later_graceful_deadline_reuses_durable_intent_without_append() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(
            // A later idempotent request must not attempt flush #3.
            Some(Box::new(FailingFlushOpLog::new(3))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_name = "host-graceful-idempotent:3333";
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();

    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 30_000,
        }),
    )
    .await
    .unwrap();
    let first_deadline =
        service.capture_loaded_snapshot("first").graceful_unmounts[0].deadline_epoch_ms;

    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 60_000,
        }),
    )
    .await
    .unwrap();

    assert!(service.is_service_available());
    assert_eq!(
        service
            .capture_loaded_snapshot("repeated")
            .graceful_unmounts[0]
            .deadline_epoch_ms,
        first_deadline
    );
}

#[tokio::test]
async fn test_unmount_flush_failure_preserves_memory_segment_and_fences() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(
            Some(Box::new(FailingFlushOpLog::new(2))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_name = "host-unmount-flush-fail:3333";
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();

    let error = MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());
    assert_eq!(
        service.segment_id_by_name(segment_name),
        Some(segment_id),
        "a non-durable tombstone must not release the authoritative segment"
    );
    assert!(
        service
            .capture_loaded_snapshot("unmount-flush-failed")
            .segments
            .iter()
            .any(|segment| segment.segment.id == segment_id)
    );
}

#[tokio::test]
async fn test_graceful_completion_flush_failure_preserves_segment_and_fences() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig::default(),
        Some(OpLogManager::new(
            // Mount and graceful-start are durable; completion is flush #3.
            Some(Box::new(FailingFlushOpLog::new(3))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_name = "host-graceful-completion-flush-fail:3333";
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();

    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 0,
        }),
    )
    .await
    .unwrap();
    for _ in 0..100 {
        if !service.is_service_available() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    assert!(!service.is_service_available());
    assert_eq!(
        service.segment_id_by_name(segment_name),
        Some(segment_id),
        "a non-durable graceful completion must not release the segment"
    );
    let snapshot = service.capture_loaded_snapshot("graceful-completion-flush-failed");
    let segment = snapshot
        .segments
        .iter()
        .find(|segment| segment.segment.id == segment_id)
        .unwrap();
    assert_eq!(segment.status, proto::SegmentStatus::GracefullyUnmounting);
    assert_eq!(snapshot.graceful_unmounts.len(), 1);
}

#[tokio::test]
async fn test_unmount_flush_failure_preserves_nof_segment_and_fences() {
    let service = MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        MasterRuntimeConfig {
            enable_nof: true,
            ..Default::default()
        },
        Some(OpLogManager::new(
            Some(Box::new(FailingFlushOpLog::new(2))),
            0,
        )),
    );
    let client_id = Uuid::new_v4();
    let segment_id = Uuid::new_v4();
    MasterService::mount_no_f_segment(
        &service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(segment_id)),
                name: "nof-unmount-flush-fail:3333".into(),
                base: 0x100000000,
                size: 4096,
                te_endpoint: "nof://flush-fail".into(),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap();

    let error = MasterService::unmount_no_f_segment(
        &service,
        Request::new(proto::UnmountNoFSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!service.is_service_available());
    assert!(
        service
            .capture_loaded_snapshot("nof-unmount-flush-failed")
            .nof_segments
            .iter()
            .any(|segment| segment.segment.id == segment_id),
        "a non-durable NoF tombstone must not release the authoritative segment"
    );
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

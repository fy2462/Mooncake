mod common;

use common::proto_uuid;
use mooncake_store_core::{ReplicateConfig, Segment};
use mooncake_store_master::allocator::{AllocationStrategy, SegmentAllocator};
use mooncake_store_master::eviction::EvictionManager;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::{Duration, SystemTime};
use tonic::Request;
use uuid::Uuid;

async fn mount_nof_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, size: u64) {
    MasterService::mount_no_f_segment(
        service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(Uuid::new_v4())),
                name: name.to_string(),
                base: 0,
                size,
                te_endpoint: format!("transport://{name}"),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap();
}

async fn mount_memory_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, size: u64) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.to_string(),
            size,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_object(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    size: u64,
    memory_replicas: u32,
    nof_replicas: u32,
    hard_pinned: bool,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_string(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: memory_replicas,
                nof_replica_num: nof_replicas,
                with_soft_pin: false,
                with_hard_pin: hard_pinned,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
        }),
    )
    .await
    .unwrap();
    for replica_type in [
        (memory_replicas > 0).then_some(proto::replica_descriptor::ReplicaType::Memory),
        (nof_replicas > 0).then_some(proto::replica_descriptor::ReplicaType::NofSsd),
    ]
    .into_iter()
    .flatten()
    {
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.to_string(),
                replica_type: replica_type as i32,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
}

#[test]
fn test_allocator_random_strategy() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::Random);

    let cid = Uuid::new_v4();
    allocator.add_segment(
        Segment {
            id: Uuid::new_v4(),
            name: "n1:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        },
        0,
        cid,
    );
    allocator.add_segment(
        Segment {
            id: Uuid::new_v4(),
            name: "n2:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        },
        0,
        cid,
    );

    let replicas = allocator.allocate("key1", 100, 2, &ReplicateConfig::default());
    assert_eq!(replicas.len(), 2);
    assert_ne!(replicas[0].segment_name, replicas[1].segment_name);
}

#[test]
fn test_allocator_insufficient_space() {
    let mut allocator = SegmentAllocator::new();
    let replicas = allocator.allocate("k", 1000, 3, &ReplicateConfig::default());
    assert!(replicas.is_empty());
}

#[test]
fn test_allocator_preferred_segment() {
    let mut allocator = SegmentAllocator::new();
    let cid = Uuid::new_v4();
    allocator.add_segment(
        Segment {
            id: Uuid::new_v4(),
            name: "far:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        },
        0,
        cid,
    );
    allocator.add_segment(
        Segment {
            id: Uuid::new_v4(),
            name: "preferred:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        },
        0,
        cid,
    );

    let config = ReplicateConfig {
        preferred_segment: "preferred:1".into(),
        ..Default::default()
    };
    let replicas = allocator.allocate("k", 100, 1, &config);
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "preferred:1");
}

#[test]
fn test_allocator_free_ratio_first() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    let cid = Uuid::new_v4();
    allocator.add_segment(
        Segment {
            id: Uuid::new_v4(),
            name: "fuller:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        },
        800,
        cid,
    );
    allocator.add_segment(
        Segment {
            id: Uuid::new_v4(),
            name: "emptier:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        },
        100,
        cid,
    );

    let replicas = allocator.allocate("k", 100, 1, &ReplicateConfig::default());
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "emptier:1");
}

#[tokio::test]
async fn test_runtime_config_applies_allocator_strategy() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        allocation_strategy: AllocationStrategy::FreeRatioFirst,
        ..Default::default()
    });
    let fuller_client = Uuid::new_v4();
    let emptier_client = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: fuller_client.as_u64_pair().0,
                low: fuller_client.as_u64_pair().1,
            }),
            segment_name: "fuller:1".into(),
            size: 1000,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: emptier_client.as_u64_pair().0,
                low: emptier_client.as_u64_pair().1,
            }),
            segment_name: "emptier:1".into(),
            size: 1000,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let _ = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: fuller_client.as_u64_pair().0,
                low: fuller_client.as_u64_pair().1,
            }),
            key: "prefill".into(),
            slice_length: 800,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "fuller:1".into(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
        }),
    )
    .await
    .unwrap();

    let response = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: fuller_client.as_u64_pair().0,
                low: fuller_client.as_u64_pair().1,
            }),
            key: "strategy-key".into(),
            slice_length: 100,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.replicas.len(), 1);
    assert_eq!(response.replicas[0].segment_name, "emptier:1");
}

#[test]
fn test_allocator_advances_offsets_and_reuses_freed_space() {
    let mut allocator = SegmentAllocator::new();
    let cid = Uuid::new_v4();
    let sid = Uuid::new_v4();
    allocator.add_segment(
        Segment {
            id: sid,
            name: "solo:1".into(),
            size: 1000,
            base: 0,
            te_endpoint: String::new(),
            protocol: "tcp".into(),
            host_id: String::new(),
        },
        0,
        cid,
    );

    let first = allocator.allocate("k1", 100, 1, &ReplicateConfig::default());
    let second = allocator.allocate("k2", 100, 1, &ReplicateConfig::default());

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].offset, 0);
    assert_eq!(second[0].offset, 100);

    allocator.release(&first).unwrap();
    let reused = allocator.allocate("k3", 100, 1, &ReplicateConfig::default());
    assert_eq!(reused.len(), 1);
    assert_eq!(reused[0].offset, 0);
}

#[test]
fn test_eviction_selects_oldest() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(3600));
    let now = SystemTime::now();
    let very_old = now - Duration::from_secs(10000);
    let recent = now - Duration::from_secs(10);

    let candidates: Vec<(
        &str,
        &[mooncake_store_core::ReplicaDescriptor],
        bool,
        SystemTime,
    )> = vec![
        ("old_key", &[], false, very_old),
        ("new_key", &[], false, recent),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "old_key");
}

#[test]
fn test_lease_not_expired_is_skipped() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_secs(3600));
    let fresh = SystemTime::now() - Duration::from_secs(100);

    let candidates: Vec<(
        &str,
        &[mooncake_store_core::ReplicaDescriptor],
        bool,
        SystemTime,
    )> = vec![("still_live", &[], false, fresh)];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert!(evicted.is_empty());
}

#[test]
fn test_eviction_skips_soft_pinned() {
    let mgr = EvictionManager::new(Duration::from_secs(1800), Duration::from_millis(5000));
    let now = SystemTime::now();
    let old = now - Duration::from_secs(1000);

    let candidates: Vec<(
        &str,
        &[mooncake_store_core::ReplicaDescriptor],
        bool,
        SystemTime,
    )> = vec![
        ("pinned_key", &[], true, old),
        ("normal_key", &[], false, old),
    ];

    let evicted = mgr.select_for_eviction(&candidates, 1);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0], "normal_key");
}

#[test]
fn test_soft_pin_expiry() {
    let mgr = EvictionManager::new(Duration::from_millis(1), Duration::from_millis(5000));
    let old = SystemTime::now() - Duration::from_secs(10);
    assert!(mgr.soft_pin_expired(old));
}

#[tokio::test]
async fn test_nof_watermark_eviction_is_independent_from_memory() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        enable_nof: true,
        nof_eviction_high_watermark_ratio: 0.50,
        nof_eviction_ratio: 0.05,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_nof_segment(&service, client_id, "nof-watermark:1", 1_000).await;
    put_object(&service, client_id, "nof-only", 900, 0, 1, false).await;

    assert_eq!(
        service.run_automatic_nof_eviction_once_for_test(),
        vec!["nof-only".to_string()]
    );
    let error = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "nof-only".to_string(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_nof_allocation_failure_triggers_eviction_below_watermark() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        enable_nof: true,
        nof_eviction_high_watermark_ratio: 0.99,
        nof_eviction_ratio: 0.05,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_nof_segment(&service, client_id, "nof-pressure:1", 1_000).await;
    put_object(&service, client_id, "pressure-victim", 900, 0, 1, false).await;

    let allocation_error = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "cannot-fit".to_string(),
            slice_length: 200,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 0,
                nof_replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(allocation_error.code(), tonic::Code::ResourceExhausted);

    assert_eq!(
        service.run_automatic_nof_eviction_once_for_test(),
        vec!["pressure-victim".to_string()]
    );
}

#[tokio::test]
async fn test_nof_eviction_preserves_memory_replica() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "memory-survivor:1", 1_000).await;
    mount_nof_segment(&service, client_id, "nof-removable:1", 1_000).await;
    put_object(&service, client_id, "mixed", 600, 1, 1, false).await;

    assert_eq!(
        service.run_nof_eviction_cycle_for_test(1),
        vec!["mixed".to_string()]
    );
    let response = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "mixed".to_string(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    assert_eq!(
        response.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::Memory as i32
    );
}

#[tokio::test]
async fn test_nof_eviction_respects_hard_pin() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        enable_nof: true,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_nof_segment(&service, client_id, "nof-hard-pin:1", 1_000).await;
    put_object(&service, client_id, "hard-pinned", 600, 0, 1, true).await;

    assert!(service.run_nof_eviction_cycle_for_test(1).is_empty());
    let response = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "hard-pinned".to_string(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.replicas.len(), 1);
    assert_eq!(
        response.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::NofSsd as i32
    );
}

use mooncake_store_core::{ReplicateConfig, Segment};
use mooncake_store_master::allocator::{AllocationStrategy, SegmentAllocator};
use mooncake_store_master::eviction::EvictionManager;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::{Duration, SystemTime};
use tonic::Request;
use uuid::Uuid;

fn proto_uuid(id: Uuid) -> proto::Uuid {
    let (high, low) = id.as_u64_pair();
    proto::Uuid { high, low }
}

#[test]
fn test_allocator_random_strategy() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::Random);

    let cid = Uuid::new_v4();
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "n1:1".into(),
        size: 1000,
        used: 0,
        client_id: cid,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "n2:1".into(),
        size: 1000,
        used: 0,
        client_id: cid,
    });

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
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "far:1".into(),
        size: 1000,
        used: 0,
        client_id: cid,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "preferred:1".into(),
        size: 1000,
        used: 0,
        client_id: cid,
    });

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
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "fuller:1".into(),
        size: 1000,
        used: 800,
        client_id: cid,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "emptier:1".into(),
        size: 1000,
        used: 100,
        client_id: cid,
    });

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
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "fuller:1".into(),
                prefer_alloc_in_same_node: false,
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
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: false,
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
    allocator.add_segment(Segment {
        id: sid,
        name: "solo:1".into(),
        size: 1000,
        used: 0,
        client_id: cid,
    });

    let first = allocator.allocate("k1", 100, 1, &ReplicateConfig::default());
    let second = allocator.allocate("k2", 100, 1, &ReplicateConfig::default());

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].offset, 0);
    assert_eq!(second[0].offset, 100);

    allocator.release(&first);
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
async fn test_create_and_query_task_returns_real_state() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let target_client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "host-b:2222".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: target_client_id.as_u64_pair().0,
                low: target_client_id.as_u64_pair().1,
            }),
            segment_name: "target-a".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "task-key".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap();

    let task_id = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "task-key".into(),
            targets: vec!["target-a".into()],
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap();

    let task = MasterService::query_task(
        &service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(task_id),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(task.task_type, proto::TaskType::ReplicaCopy as i32);
    assert_eq!(task.status, proto::TaskStatus::TaskPending as i32);
    assert!(task.created_at_ms_epoch > 0);
    assert!(task.message.contains("task-key"));
    assert!(task.assigned_client.is_some());
}

#[tokio::test]
async fn test_fetch_tasks_marks_processing_and_respects_batch_size() {
    let service = MasterServiceImpl::default();
    let source_client_id = Uuid::new_v4();
    let target_client_id = Uuid::new_v4();

    for (client_id, segment_name) in [
        (source_client_id, "segment_0"),
        (target_client_id, "segment_1"),
    ] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: segment_name.into(),
                size: 4096,
            }),
        )
        .await
        .unwrap();
    }

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            key: "fetch-task-key".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "segment_0".into(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            key: "fetch-task-key".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();

    let copy_task_id = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "fetch-task-key".into(),
            targets: vec!["segment_1".into()],
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap();

    let move_task_id = MasterService::create_move_task(
        &service,
        Request::new(proto::CreateMoveTaskRequest {
            key: "fetch-task-key".into(),
            source: "segment_0".into(),
            target: "segment_1".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap();

    let first_batch = MasterService::fetch_tasks(
        &service,
        Request::new(proto::FetchTasksRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            batch_size: 1,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first_batch.tasks.len(), 1);
    let first_id = first_batch.tasks[0].id.clone().unwrap();

    let second_batch = MasterService::fetch_tasks(
        &service,
        Request::new(proto::FetchTasksRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            batch_size: 1,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second_batch.tasks.len(), 1);
    let second_id = second_batch.tasks[0].id.clone().unwrap();

    let ids = vec![
        Uuid::from_u64_pair(first_id.high, first_id.low),
        Uuid::from_u64_pair(second_id.high, second_id.low),
    ];
    assert!(ids.contains(&Uuid::from_u64_pair(copy_task_id.high, copy_task_id.low)));
    assert!(ids.contains(&Uuid::from_u64_pair(move_task_id.high, move_task_id.low)));

    let third_batch = MasterService::fetch_tasks(
        &service,
        Request::new(proto::FetchTasksRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            batch_size: 1,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(third_batch.tasks.is_empty());

    let task = MasterService::query_task(
        &service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(first_batch.tasks[0].id.clone().unwrap()),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(task.status, proto::TaskStatus::TaskProcessing as i32);
}

#[tokio::test]
async fn test_mark_task_to_complete_updates_state_and_rejects_wrong_client() {
    let service = MasterServiceImpl::default();
    let source_client_id = Uuid::new_v4();
    let other_client_id = Uuid::new_v4();

    for (client_id, segment_name) in [
        (source_client_id, "segment_a"),
        (other_client_id, "segment_b"),
    ] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: segment_name.into(),
                size: 4096,
            }),
        )
        .await
        .unwrap();
    }

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            key: "complete-task-key".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "segment_a".into(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            key: "complete-task-key".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();

    let task_id = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "complete-task-key".into(),
            targets: vec!["segment_b".into()],
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap();

    MasterService::fetch_tasks(
        &service,
        Request::new(proto::FetchTasksRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            batch_size: 16,
        }),
    )
    .await
    .unwrap();

    let wrong_client = MasterService::mark_task_to_complete(
        &service,
        Request::new(proto::MarkTaskToCompleteRequest {
            client_id: Some(proto::Uuid {
                high: other_client_id.as_u64_pair().0,
                low: other_client_id.as_u64_pair().1,
            }),
            request: Some(proto::TaskCompleteRequest {
                id: Some(task_id.clone()),
                status: proto::TaskStatus::TaskSuccess as i32,
                message: "should-fail".into(),
            }),
        }),
    )
    .await;
    assert!(wrong_client.is_err());

    MasterService::mark_task_to_complete(
        &service,
        Request::new(proto::MarkTaskToCompleteRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            request: Some(proto::TaskCompleteRequest {
                id: Some(task_id.clone()),
                status: proto::TaskStatus::TaskSuccess as i32,
                message: "done".into(),
            }),
        }),
    )
    .await
    .unwrap();

    let task = MasterService::query_task(
        &service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(task_id),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(task.status, proto::TaskStatus::TaskSuccess as i32);
    assert_eq!(task.message, "done");
}

#[tokio::test]
async fn test_offload_object_heartbeat_and_notify_offload_success() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "mem-a".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "offload-key".into(),
            slice_length: 256,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "mem-a".into(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "offload-key".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();

    let heartbeat = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(heartbeat.objects.get("offload-key"), Some(&256));

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            keys: vec!["disk-only-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: "disk-only-key".len() as i64,
                data_size: 512,
                transport_endpoint: "holder-a".into(),
            }],
        }),
    )
    .await
    .unwrap();

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "disk-only-key".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);
    assert_eq!(
        replicas.replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::LocalDisk as i32
    );
    assert_eq!(
        replicas.replicas[0].holder_client_id.as_ref().unwrap().high,
        client_id.as_u64_pair().0
    );
}

#[tokio::test]
async fn test_promotion_flow_success_and_failure() {
    let service = MasterServiceImpl::default();
    let holder_id = Uuid::new_v4();
    let dram_client = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: dram_client.as_u64_pair().0,
                low: dram_client.as_u64_pair().1,
            }),
            segment_name: "dram-a".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();

    for key in ["promo-ok", "promo-fail"] {
        MasterService::notify_offload_success(
            &service,
            Request::new(proto::NotifyOffloadSuccessRequest {
                client_id: Some(proto::Uuid {
                    high: holder_id.as_u64_pair().0,
                    low: holder_id.as_u64_pair().1,
                }),
                keys: vec![key.into()],
                metadatas: vec![proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: key.len() as i64,
                    data_size: 256,
                    transport_endpoint: "holder-endpoint".into(),
                }],
            }),
        )
        .await
        .unwrap();

        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest { key: key.into() }),
        )
        .await
        .unwrap();
    }

    let first_heartbeat = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first_heartbeat.objects.len(), 1);
    let first_key = first_heartbeat.objects.keys().next().unwrap().clone();

    let second_heartbeat = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second_heartbeat.objects.len(), 1);
    let second_key = second_heartbeat.objects.keys().next().unwrap().clone();
    assert_ne!(first_key, second_key);

    let alloc = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: first_key.clone(),
            size: 256,
            preferred_segments: vec!["dram-a".into()],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        alloc.memory_descriptor.as_ref().unwrap().segment_name,
        "dram-a"
    );

    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: first_key.clone(),
        }),
    )
    .await
    .unwrap();

    let promoted = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest { key: first_key }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(promoted.replicas.iter().any(|replica| replica.replica_type
        == proto::replica_descriptor::ReplicaType::Memory as i32
        && replica.status == proto::replica_descriptor::ReplicaStatus::Complete as i32));

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: second_key.clone(),
            size: 256,
            preferred_segments: vec!["dram-a".into()],
        }),
    )
    .await
    .unwrap();
    MasterService::notify_promotion_failure(
        &service,
        Request::new(proto::NotifyPromotionFailureRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: second_key.clone(),
        }),
    )
    .await
    .unwrap();

    let failed = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest { key: second_key }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        failed
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        0
    );
}

#[tokio::test]
async fn test_promotion_admission_threshold_requires_multiple_reads() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        promotion_admission_threshold: 2,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            keys: vec!["threshold-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 13,
                data_size: 256,
                transport_endpoint: "holder-threshold".into(),
            }],
        }),
    )
    .await
    .unwrap();

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "threshold-key".into(),
        }),
    )
    .await
    .unwrap();
    let first = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(first.objects.is_empty());

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "threshold-key".into(),
        }),
    )
    .await
    .unwrap();
    let second = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(second.objects.get("threshold-key"), Some(&256));
}

#[tokio::test]
async fn test_promotion_queue_limit_released_after_success() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        promotion_queue_limit: 1,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            segment_name: "limit-dram".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();

    for key in ["limit-a", "limit-b"] {
        MasterService::notify_offload_success(
            &service,
            Request::new(proto::NotifyOffloadSuccessRequest {
                client_id: Some(proto::Uuid {
                    high: holder_id.as_u64_pair().0,
                    low: holder_id.as_u64_pair().1,
                }),
                keys: vec![key.into()],
                metadatas: vec![proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: key.len() as i64,
                    data_size: 128,
                    transport_endpoint: "holder-limit".into(),
                }],
            }),
        )
        .await
        .unwrap();
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest { key: key.into() }),
        )
        .await
        .unwrap();
    }

    let first_tick = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(first_tick.objects.len(), 1);
    assert!(first_tick.objects.contains_key("limit-a"));

    let second_tick = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(second_tick.objects.is_empty());

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "limit-a".into(),
            size: 128,
            preferred_segments: vec!["limit-dram".into()],
        }),
    )
    .await
    .unwrap();
    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "limit-a".into(),
        }),
    )
    .await
    .unwrap();

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "limit-b".into(),
        }),
    )
    .await
    .unwrap();
    let third_tick = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(third_tick.objects.get("limit-b"), Some(&128));
}

#[tokio::test]
async fn test_promotion_reaper_resets_deadline_and_releases_staged_buffer() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        put_start_release_timeout: Duration::from_millis(120),
        reaper_interval: Duration::from_millis(20),
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();

    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            segment_name: "reaper-dram".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            keys: vec!["reaper-key".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 10,
                data_size: 256,
                transport_endpoint: "holder-reaper".into(),
            }],
        }),
    )
    .await
    .unwrap();

    let baseline = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "reaper-key".into(),
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;
    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "reaper-key".into(),
            size: 256,
            preferred_segments: vec!["reaper-dram".into()],
        }),
    )
    .await
    .unwrap();

    let after_alloc = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;
    assert!(after_alloc > baseline);

    tokio::time::sleep(Duration::from_millis(70)).await;
    let still_allocated = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;
    assert!(still_allocated > baseline);

    tokio::time::sleep(Duration::from_millis(120)).await;
    let after_reap = MasterService::query_segments(
        &service,
        Request::new(proto::QuerySegmentsRequest {
            segment_name: "reaper-dram".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .used_size;
    assert_eq!(after_reap, baseline);

    let alloc_after_reap = MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto::Uuid {
                high: holder_id.as_u64_pair().0,
                low: holder_id.as_u64_pair().1,
            }),
            key: "reaper-key".into(),
            size: 256,
            preferred_segments: vec!["reaper-dram".into()],
        }),
    )
    .await;
    assert!(alloc_after_reap.is_err());
}

#[tokio::test]
async fn test_offload_on_evict_keeps_one_memory_replica_and_queues_local_disk_work() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    for segment_name in ["evict-a", "evict-b"] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: segment_name.into(),
                size: 4096,
            }),
        )
        .await
        .unwrap();
    }
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "evict-offload".into(),
            slice_length: 256,
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(put.replicas.len(), 2);
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "evict-offload".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["evict-offload".to_string()]);

    let offload = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(offload.objects.get("evict-offload"), Some(&256));

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "evict-offload".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_offload_on_evict_drops_memory_when_local_disk_already_exists() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            segment_name: "evict-localdisk".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "already-offloaded".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "evict-localdisk".into(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "already-offloaded".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            keys: vec!["already-offloaded".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 17,
                data_size: 128,
                transport_endpoint: "holder-existing".into(),
            }],
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["already-offloaded".to_string()]);

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "already-offloaded".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        0
    );
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::LocalDisk as i32)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_background_eviction_worker_triggers_offload_on_high_watermark() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        offload_on_evict: true,
        lease_ttl: Duration::from_millis(1),
        eviction_interval: Duration::from_millis(10),
        eviction_high_watermark_ratio: 0.1,
        eviction_ratio: 0.05,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    for segment_name in ["bg-evict-a", "bg-evict-b"] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: segment_name.into(),
                size: 4096,
            }),
        )
        .await
        .unwrap();
    }
    MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "bg-evict-offload".into(),
            slice_length: 512,
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "".into(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            key: "bg-evict-offload".into(),
            replica_type: 0,
        }),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    let offload = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto::Uuid {
                high: client_id.as_u64_pair().0,
                low: client_id.as_u64_pair().1,
            }),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(offload.objects.get("bg-evict-offload"), Some(&512));

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "bg-evict-offload".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        replicas
            .replicas
            .iter()
            .filter(|replica| replica.replica_type
                == proto::replica_descriptor::ReplicaType::Memory as i32)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_batch_replica_clear_respects_client_and_segment_name() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    let other_client_id = Uuid::new_v4();

    for (cid, name) in [(client_id, "node-a:1"), (client_id, "node-b:1")] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(cid)),
                segment_name: name.into(),
                size: 1024,
            }),
        )
        .await
        .unwrap();
    }

    let put = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch-clear-key".into(),
            slice_length: 128,
            config: Some(proto::ReplicateConfig {
                replica_num: 2,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: String::new(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(put.replicas.len(), 2);

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch-clear-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
        }),
    )
    .await
    .unwrap();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(other_client_id)),
            segment_name: "node-c:1".into(),
            size: 1024,
        }),
    )
    .await
    .unwrap();

    let cleared = MasterService::batch_replica_clear(
        &service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: vec!["batch-clear-key".into()],
            client_id: Some(proto_uuid(client_id)),
            segment_name: put.replicas[0].segment_name.clone(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(cleared.cleared_keys, vec!["batch-clear-key".to_string()]);

    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "batch-clear-key".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(replicas.replicas.len(), 1);
    assert_ne!(
        replicas.replicas[0].segment_name,
        put.replicas[0].segment_name
    );

    let denied = MasterService::batch_replica_clear(
        &service,
        Request::new(proto::BatchReplicaClearRequest {
            object_keys: vec!["batch-clear-key".into()],
            client_id: Some(proto_uuid(other_client_id)),
            segment_name: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(denied.cleared_keys.is_empty());

    let still_exists = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "batch-clear-key".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(still_exists.replicas.len(), 1);
}

#[tokio::test]
async fn test_hard_pinned_object_survives_eviction_cycle() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "hardpin:1".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();

    for (key, with_hard_pin) in [("hard-key", true), ("normal-key", false)] {
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 512,
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    with_soft_pin: false,
                    with_hard_pin,
                    preferred_segment: String::new(),
                    prefer_alloc_in_same_node: false,
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            }),
        )
        .await
        .unwrap();
    }

    let evicted = service.run_eviction_cycle_for_test(2);
    assert!(evicted.iter().any(|key| key == "normal-key"));
    assert!(!evicted.iter().any(|key| key == "hard-key"));

    let hard_key = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "hard-key".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(hard_key.replicas.len(), 1);

    let normal_key = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "normal-key".into(),
        }),
    )
    .await;
    assert!(normal_key.is_err());
}

#[tokio::test]
async fn test_copy_move_and_revoke_workflow() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    for name in ["copy-src:1", "copy-dst:1", "move-dst:1"] {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: name.into(),
                size: 4096,
            }),
        )
        .await
        .unwrap();
    }

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            slice_length: 256,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "copy-src:1".into(),
                prefer_alloc_in_same_node: false,
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
        }),
    )
    .await
    .unwrap();

    let copy_started = MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-src:1".into(),
            targets: vec!["copy-dst:1".into()],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(copy_started.targets.len(), 1);
    assert_eq!(copy_started.targets[0].segment_name, "copy-dst:1");

    MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
        }),
    )
    .await
    .unwrap();

    let after_copy = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_copy.replicas.len(), 2);

    let move_started = MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-src:1".into(),
            target: "move-dst:1".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(move_started.target.unwrap().segment_name, "move-dst:1");

    MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
        }),
    )
    .await
    .unwrap();

    let after_move = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_move.replicas.len(), 2);
    assert!(after_move
        .replicas
        .iter()
        .any(|r| r.segment_name == "copy-dst:1"));
    assert!(after_move
        .replicas
        .iter()
        .any(|r| r.segment_name == "move-dst:1"));
    assert!(!after_move
        .replicas
        .iter()
        .any(|r| r.segment_name == "copy-src:1"));

    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
            source: "copy-dst:1".into(),
            targets: vec!["copy-src:1".into()],
        }),
    )
    .await
    .unwrap();
    MasterService::copy_revoke(
        &service,
        Request::new(proto::CopyRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "copy-move-key".into(),
        }),
    )
    .await
    .unwrap();

    let after_revoke = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "copy-move-key".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(after_revoke.replicas.len(), 2);
    assert!(!after_revoke
        .replicas
        .iter()
        .any(|r| r.segment_name == "copy-src:1"));
}

#[tokio::test]
async fn test_put_revoke_remove_all_and_storage_config() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        storage_fs_dir: "/tmp/mooncake".into(),
        enable_disk_eviction: true,
        quota_bytes: 4096,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();

    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "revoke:1".into(),
            size: 4096,
        }),
    )
    .await
    .unwrap();

    for key in ["revoke-key", "remove-all-a", "remove-all-b"] {
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    with_soft_pin: false,
                    with_hard_pin: false,
                    preferred_segment: "revoke:1".into(),
                    prefer_alloc_in_same_node: false,
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            }),
        )
        .await
        .unwrap();
    }

    MasterService::put_revoke(
        &service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "revoke-key".into(),
        }),
    )
    .await
    .unwrap();
    assert!(MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "revoke-key".into(),
        }),
    )
    .await
    .is_err());

    let removed = MasterService::remove_all(&service, Request::new(proto::RemoveAllRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(removed.removed_count, 2);

    let storage = MasterService::get_storage_config(
        &service,
        Request::new(proto::GetStorageConfigRequest {}),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(storage.fs_dir, "/tmp/mooncake");
    assert!(storage.enable_disk_eviction);
    assert_eq!(storage.quota_bytes, 4096);
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
        }),
    )
    .await
    .unwrap();

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "ttl-key".into(),
            slice_length: 64,
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "ttl:1".into(),
                prefer_alloc_in_same_node: false,
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

    assert!(MasterService::query_ip(
        &service,
        Request::new(proto::QueryIpRequest {
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .is_err());
    assert!(MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "ttl-key".into(),
        }),
    )
    .await
    .is_err());
}

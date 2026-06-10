use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn test_create_and_query_task_returns_real_state() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        pending_task_timeout: Duration::from_millis(50),
        reaper_interval: Duration::from_millis(5),
        ..Default::default()
    });
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
            client_id: Some(proto::Uuid {
                high: target_client_id.as_u64_pair().0,
                low: target_client_id.as_u64_pair().1,
            }),
            segment_name: "target-a".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
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
            tenant_id: String::new(),
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
            task_id: Some(task_id.clone()),
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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let expired = MasterService::query_task(
        &service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(task_id),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(expired.status, proto::TaskStatus::TaskFailed as i32);
    assert_eq!(expired.message, "pending timeout");
}

#[tokio::test]
async fn test_fetch_tasks_marks_processing_and_respects_batch_size() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        max_total_processing_tasks: 1,
        ..Default::default()
    });
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
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
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
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "segment_0".into(),
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
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            key: "fetch-task-key".into(),
            replica_type: 0,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let copy_task_id = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "fetch-task-key".into(),
            targets: vec!["segment_1".into()],
            tenant_id: String::new(),
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
            tenant_id: String::new(),
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

    let blocked_batch = MasterService::fetch_tasks(
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
    assert!(blocked_batch.tasks.is_empty());

    MasterService::mark_task_to_complete(
        &service,
        Request::new(proto::MarkTaskToCompleteRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            request: Some(proto::TaskCompleteRequest {
                id: Some(first_id.clone()),
                status: proto::TaskStatus::TaskSuccess as i32,
                message: "done".into(),
            }),
        }),
    )
    .await
    .unwrap();

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
    assert_eq!(task.status, proto::TaskStatus::TaskSuccess as i32);
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
                base_addr: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
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
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "segment_a".into(),
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
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            key: "complete-task-key".into(),
            replica_type: 0,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let task_id = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "complete-task-key".into(),
            targets: vec!["segment_b".into()],
            tenant_id: String::new(),
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

    let nonterminal_status = MasterService::mark_task_to_complete(
        &service,
        Request::new(proto::MarkTaskToCompleteRequest {
            client_id: Some(proto::Uuid {
                high: source_client_id.as_u64_pair().0,
                low: source_client_id.as_u64_pair().1,
            }),
            request: Some(proto::TaskCompleteRequest {
                id: Some(task_id.clone()),
                status: proto::TaskStatus::TaskProcessing as i32,
                message: "still-processing".into(),
            }),
        }),
    )
    .await;
    assert!(nonterminal_status.is_err());

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

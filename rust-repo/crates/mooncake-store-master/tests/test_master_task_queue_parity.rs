mod common;

use common::proto_uuid;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use std::collections::HashSet;
use tonic::Request;
use uuid::Uuid;

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, base_addr: u64) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 16 * 1024 * 1024,
            base_addr,
            te_endpoint: name.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_source_and_target(
    service: &MasterServiceImpl,
    source_client: Uuid,
    target_client: Uuid,
) {
    mount_segment(service, source_client, "segment_0", 0x300000000).await;
    mount_segment(service, target_client, "segment_1", 0x400000000).await;
}

async fn complete_source_object(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: "segment_0".into(),
                ..Default::default()
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

async fn create_copy_task(service: &MasterServiceImpl, key: &str) -> proto::Uuid {
    MasterService::create_copy_task(
        service,
        Request::new(proto::CreateCopyTaskRequest {
            key: key.into(),
            targets: vec!["segment_1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap()
}

async fn create_move_task(service: &MasterServiceImpl, key: &str) -> proto::Uuid {
    MasterService::create_move_task(
        service,
        Request::new(proto::CreateMoveTaskRequest {
            key: key.into(),
            source: "segment_0".into(),
            target: "segment_1".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap()
}

async fn fetch_tasks(
    service: &MasterServiceImpl,
    client_id: Uuid,
    batch_size: u32,
) -> Vec<proto::TaskAssignment> {
    MasterService::fetch_tasks(
        service,
        Request::new(proto::FetchTasksRequest {
            client_id: Some(proto_uuid(client_id)),
            batch_size,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks
}

fn task_id(task: &proto::TaskAssignment) -> Uuid {
    let id = task.id.as_ref().unwrap();
    Uuid::from_u64_pair(id.high, id.low)
}

fn proto_id(id: &proto::Uuid) -> Uuid {
    Uuid::from_u64_pair(id.high, id.low)
}

async fn mark_task(
    service: &MasterServiceImpl,
    client_id: Uuid,
    id: proto::Uuid,
    status: proto::TaskStatus,
    message: &str,
) -> Result<(), tonic::Status> {
    MasterService::mark_task_to_complete(
        service,
        Request::new(proto::MarkTaskToCompleteRequest {
            client_id: Some(proto_uuid(client_id)),
            request: Some(proto::TaskCompleteRequest {
                id: Some(id),
                status: status as i32,
                message: message.into(),
            }),
        }),
    )
    .await
    .map(|_| ())
}

#[tokio::test]
async fn empty_task_queue_fetch_succeeds_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "segment_0", 0x300000000).await;

    assert!(fetch_tasks(&service, client_id, 16).await.is_empty());
}

#[tokio::test]
async fn fetch_returns_only_assigned_tasks_and_drains_queue_parity() {
    let service = MasterServiceImpl::default();
    let source_client = Uuid::new_v4();
    let target_client = Uuid::new_v4();
    let writer_client = Uuid::new_v4();
    mount_source_and_target(&service, source_client, target_client).await;
    complete_source_object(&service, writer_client, "fetch_tasks_key_0").await;
    let copy_id = create_copy_task(&service, "fetch_tasks_key_0").await;
    let move_id = create_move_task(&service, "fetch_tasks_key_0").await;

    let source_tasks = fetch_tasks(&service, source_client, 16).await;
    assert_eq!(source_tasks.len(), 2);
    let fetched_ids = source_tasks.iter().map(task_id).collect::<HashSet<_>>();
    assert_eq!(
        fetched_ids,
        HashSet::from([proto_id(&copy_id), proto_id(&move_id)])
    );
    assert!(fetch_tasks(&service, target_client, 16).await.is_empty());
    assert!(fetch_tasks(&service, source_client, 16).await.is_empty());
}

#[tokio::test]
async fn fetch_tasks_respects_batch_size_and_exhausts_queue_parity() {
    let service = MasterServiceImpl::default();
    let source_client = Uuid::new_v4();
    let target_client = Uuid::new_v4();
    let writer_client = Uuid::new_v4();
    mount_source_and_target(&service, source_client, target_client).await;
    complete_source_object(&service, writer_client, "fetch_tasks_key_1").await;
    let copy_id = create_copy_task(&service, "fetch_tasks_key_1").await;
    let move_id = create_move_task(&service, "fetch_tasks_key_1").await;

    let first = fetch_tasks(&service, source_client, 1).await;
    let second = fetch_tasks(&service, source_client, 1).await;
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(
        HashSet::from([task_id(&first[0]), task_id(&second[0])]),
        HashSet::from([proto_id(&copy_id), proto_id(&move_id)])
    );
    assert!(fetch_tasks(&service, source_client, 1).await.is_empty());
}

#[tokio::test]
async fn successful_task_update_persists_state_and_drains_queue_parity() {
    let service = MasterServiceImpl::default();
    let source_client = Uuid::new_v4();
    let target_client = Uuid::new_v4();
    let writer_client = Uuid::new_v4();
    mount_source_and_target(&service, source_client, target_client).await;
    complete_source_object(&service, writer_client, "update_task_key_success").await;
    let created_id = create_copy_task(&service, "update_task_key_success").await;

    let fetched = fetch_tasks(&service, source_client, 16).await;
    assert_eq!(fetched.len(), 1);
    assert_eq!(task_id(&fetched[0]), proto_id(&created_id));
    mark_task(
        &service,
        source_client,
        created_id.clone(),
        proto::TaskStatus::TaskSuccess,
        "done",
    )
    .await
    .unwrap();

    let queried = MasterService::query_task(
        &service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(created_id.clone()),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        proto_id(queried.id.as_ref().unwrap()),
        proto_id(&created_id)
    );
    assert_eq!(queried.status, proto::TaskStatus::TaskSuccess as i32);
    assert_eq!(
        proto_id(queried.assigned_client.as_ref().unwrap()),
        source_client
    );
    assert_eq!(queried.message, "done");
    assert!(fetch_tasks(&service, source_client, 16).await.is_empty());
}

#[tokio::test]
async fn task_update_rejects_wrong_client_with_permission_denied_parity() {
    let service = MasterServiceImpl::default();
    let source_client = Uuid::new_v4();
    let target_client = Uuid::new_v4();
    let writer_client = Uuid::new_v4();
    mount_source_and_target(&service, source_client, target_client).await;
    complete_source_object(&service, writer_client, "update_task_wrong_client").await;
    let created_id = create_move_task(&service, "update_task_wrong_client").await;

    let fetched = fetch_tasks(&service, source_client, 16).await;
    assert_eq!(fetched.len(), 1);
    assert_eq!(task_id(&fetched[0]), proto_id(&created_id));
    let error = mark_task(
        &service,
        target_client,
        created_id,
        proto::TaskStatus::TaskSuccess,
        "should_not_work",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn task_update_missing_id_returns_not_found_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "segment_0", 0x300000000).await;

    let error = mark_task(
        &service,
        client_id,
        proto_uuid(Uuid::new_v4()),
        proto::TaskStatus::TaskFailed,
        "not_found",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
}

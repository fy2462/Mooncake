mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::MasterServiceImpl;
use tonic::Request;
use uuid::Uuid;

async fn mount_three_segments(service: &MasterServiceImpl, owners: [Uuid; 3]) {
    for (index, owner) in owners.into_iter().enumerate() {
        MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(owner)),
                segment_name: format!("segment_{index}"),
                size: 16 * 1024 * 1024,
                base_addr: 0x300000000 + index as u64 * 16 * 1024 * 1024,
                te_endpoint: format!("segment_{index}"),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
}

async fn complete_source_object(service: &MasterServiceImpl, writer: Uuid, key: &str) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(writer)),
            key: key.into(),
            slice_length: 6 * 1024 * 1024,
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
            client_id: Some(proto_uuid(writer)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

fn id(id: &proto::Uuid) -> Uuid {
    Uuid::from_u64_pair(id.high, id.low)
}

async fn query_task(
    service: &MasterServiceImpl,
    task_id: proto::Uuid,
) -> Result<proto::QueryTaskResponse, tonic::Status> {
    MasterService::query_task(
        service,
        Request::new(proto::QueryTaskRequest {
            task_id: Some(task_id),
        }),
    )
    .await
    .map(|response| response.into_inner())
}

async fn create_move(
    service: &MasterServiceImpl,
    key: &str,
    source: &str,
    target: &str,
) -> Result<proto::Uuid, tonic::Status> {
    MasterService::create_move_task(
        service,
        Request::new(proto::CreateMoveTaskRequest {
            key: key.into(),
            source: source.into(),
            target: target.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .map(|response| response.into_inner().task_id.unwrap())
}

#[tokio::test]
async fn create_copy_task_validation_matrix_parity() {
    let service = MasterServiceImpl::default();
    let owners = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let writer = Uuid::new_v4();
    mount_three_segments(&service, owners).await;
    complete_source_object(&service, writer, "test_key_1").await;

    let created = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "test_key_1".into(),
            targets: vec!["segment_1".into(), "segment_2".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap();
    let task = query_task(&service, created).await.unwrap();
    assert_eq!(task.task_type, proto::TaskType::ReplicaCopy as i32);
    assert_eq!(id(task.assigned_client.as_ref().unwrap()), owners[0]);

    let empty_targets = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "test_key_1".into(),
            targets: vec![],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(empty_targets.code(), tonic::Code::InvalidArgument);

    let unknown_key = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "not_exist_key".into(),
            targets: vec!["segment_1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(unknown_key.code(), tonic::Code::NotFound);

    let unmounted_target = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "test_key_1".into(),
            targets: vec!["not_mounted_segment".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(unmounted_target.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn create_move_task_validation_matrix_parity() {
    let service = MasterServiceImpl::default();
    let owners = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let writer = Uuid::new_v4();
    mount_three_segments(&service, owners).await;
    complete_source_object(&service, writer, "test_key_1").await;

    let created = create_move(&service, "test_key_1", "segment_0", "segment_1")
        .await
        .unwrap();
    let task = query_task(&service, created).await.unwrap();
    assert_eq!(task.task_type, proto::TaskType::ReplicaMove as i32);
    assert_eq!(id(task.assigned_client.as_ref().unwrap()), owners[0]);

    let cases = [
        (
            "not_exist_key",
            "segment_0",
            "segment_1",
            tonic::Code::NotFound,
        ),
        (
            "test_key_1",
            "segment_1",
            "segment_1",
            tonic::Code::InvalidArgument,
        ),
        (
            "test_key_1",
            "segment_0",
            "not_mounted_segment",
            tonic::Code::InvalidArgument,
        ),
        (
            "test_key_1",
            "segment_2",
            "segment_1",
            tonic::Code::InvalidArgument,
        ),
        (
            "test_key_1",
            "not_mounted_segment",
            "segment_1",
            tonic::Code::InvalidArgument,
        ),
    ];
    for (key, source, target, expected) in cases {
        assert_eq!(
            create_move(&service, key, source, target)
                .await
                .unwrap_err()
                .code(),
            expected,
            "key={key} source={source} target={target}"
        );
    }
}

#[tokio::test]
async fn query_missing_and_live_move_task_parity() {
    let service = MasterServiceImpl::default();
    let owners = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let writer = Uuid::new_v4();
    mount_three_segments(&service, owners).await;
    complete_source_object(&service, writer, "test_key_1").await;
    let created = create_move(&service, "test_key_1", "segment_0", "segment_1")
        .await
        .unwrap();

    let missing = query_task(&service, proto_uuid(Uuid::nil()))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);
    let task = query_task(&service, created.clone()).await.unwrap();
    assert_eq!(id(task.id.as_ref().unwrap()), id(&created));
    assert_eq!(task.task_type, proto::TaskType::ReplicaMove as i32);
    assert_eq!(id(task.assigned_client.as_ref().unwrap()), owners[0]);
}

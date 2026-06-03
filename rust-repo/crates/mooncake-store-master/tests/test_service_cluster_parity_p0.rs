mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

fn replicate_config() -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: String::new(),
        prefer_alloc_in_same_node: false,
        preferred_segments: vec![],
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
    }
}

async fn mount_memory_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, size: u64) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_start_one(service: &MasterServiceImpl, client_id: Uuid, key: &str, segment: &str) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: segment.into(),
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();
}

async fn put_end_one(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
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
async fn test_exist_key_requires_complete_replica_and_grants_lease() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "exist-complete:1", 4096).await;

    put_start_one(&service, client_id, "exist-key", "exist-complete:1").await;
    let incomplete = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "exist-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!incomplete.exists);

    put_end_one(&service, client_id, "exist-key").await;
    let complete = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "exist-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(complete.exists);

    let remove = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "exist-key".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(remove.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_batch_exist_key_is_tenant_scoped_and_requires_complete() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-exist-tenant:1", 4096).await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "shared".into(),
            slice_length: 128,
            tenant_id: "tenant-a".into(),
            config: Some(replicate_config()),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "shared".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "pending".into(),
            slice_length: 128,
            tenant_id: "tenant-a".into(),
            config: Some(replicate_config()),
        }),
    )
    .await
    .unwrap();

    let tenant_a = MasterService::batch_exist_key(
        &service,
        Request::new(proto::BatchExistKeyRequest {
            keys: vec!["shared".into(), "pending".into()],
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(tenant_a.results, vec![true, false]);

    let tenant_b = MasterService::batch_exist_key(
        &service,
        Request::new(proto::BatchExistKeyRequest {
            keys: vec!["shared".into()],
            tenant_id: "tenant-b".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(tenant_b.results, vec![false]);
}

#[tokio::test]
async fn test_batch_put_start_records_owner_and_revoke_uses_replica_type() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-owner:1", 4096).await;

    let started = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["batch-owned".into(), "batch-revoke".into()],
            slice_lengths: vec![128, 128],
            config: Some(proto::ReplicateConfig {
                preferred_segment: "batch-owner:1".into(),
                ..replicate_config()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), 2);

    let end = MasterService::batch_put_end(
        &service,
        Request::new(proto::BatchPutEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "batch-owned".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(end.statuses, vec![0]);

    let revoke = MasterService::batch_put_revoke(
        &service,
        Request::new(proto::BatchPutRevokeRequest {
            keys: vec!["batch-revoke".into()],
            client_id: Some(proto_uuid(client_id)),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(revoke.statuses, vec![0]);

    let missing = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "batch-revoke".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!missing.exists);
}

#[tokio::test]
async fn test_remove_and_put_revoke_missing_key_return_not_found() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    let remove = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "missing-remove".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(remove.code(), tonic::Code::NotFound);

    let revoke = MasterService::put_revoke(
        &service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "missing-revoke".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(revoke.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_drain_empty_targets_choose_lowest_usage_move_target() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "drain-src:1", 4096).await;
    mount_memory_segment(&service, client_id, "drain-fuller:1", 256).await;
    mount_memory_segment(&service, client_id, "drain-emptier:1", 4096).await;

    put_start_one(&service, client_id, "filler", "drain-fuller:1").await;
    put_end_one(&service, client_id, "filler").await;
    put_start_one(&service, client_id, "drain-key", "drain-src:1").await;
    put_end_one(&service, client_id, "drain-key").await;

    let create = MasterService::create_drain_job(
        &service,
        Request::new(proto::CreateDrainJobRequest {
            segments: vec!["drain-src:1".into()],
            target_segments: vec![],
            max_concurrency: 1,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let job_proto = create.job_id.unwrap();
    let job_id = Uuid::from_u64_pair(job_proto.high, job_proto.low);
    let task = service.drain_task_for_test(job_id).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&task.payload).unwrap();

    assert_eq!(
        task.info.task_type,
        mooncake_store_core::TaskType::ReplicaMove
    );
    assert_eq!(payload["source"], "drain-src:1");
    assert_eq!(payload["target"], "drain-emptier:1");
}

mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::{Code, Request};
use uuid::Uuid;

fn one_replica_config(segment: &str) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: segment.to_string(),
        prefer_alloc_in_same_node: false,
        preferred_segments: vec![],
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
    }
}

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, size: u64) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.to_string(),
            size,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_tenant_quota_admission_commit_and_release() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        default_tenant_quota_bytes: 512,
        tenant_quota_pool_capacity_bytes: 512,
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "quota-node:1", 4096).await;
    service
        .upsert_tenant_quota_policy("tenant-a", 512)
        .expect("tenant policy");

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-key".into(),
            slice_length: 400,
            tenant_id: "tenant-a".into(),
            config: Some(one_replica_config("quota-node:1")),
        }),
    )
    .await
    .unwrap();

    let reserved = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(reserved.reserved_bytes, 400);
    assert_eq!(reserved.used_bytes, 0);

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    let committed = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(committed.used_bytes, 400);
    assert_eq!(committed.reserved_bytes, 0);
    assert_eq!(committed.committed_count, 1);
    assert_eq!(committed.metadata_object_count, 1);

    let rejected = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "too-large".into(),
            slice_length: 200,
            tenant_id: "tenant-a".into(),
            config: Some(one_replica_config("quota-node:1")),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(rejected.code(), Code::ResourceExhausted);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "quota-key".into(),
            force: true,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    let released = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(released.used_bytes, 0);
    assert_eq!(released.reserved_bytes, 0);
    assert_eq!(released.committed_count, 0);
    assert_eq!(released.metadata_object_count, 0);
}

#[test]
fn test_tenant_quota_admin_policy_lifecycle_methods() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp.path().to_string_lossy().into_owned(),
        tenant_quota_pool_capacity_bytes: 2000,
        ..Default::default()
    });

    let snapshot = service.upsert_tenant_quota_policy("tenant-a", 800).unwrap();
    assert_eq!(snapshot.tenant_id, "tenant-a");
    assert_eq!(snapshot.requested_quota_bytes, 800);
    assert!(snapshot.has_explicit_policy);

    let list = service.list_tenant_quota_snapshots().unwrap();
    assert_eq!(list.len(), 1);

    let deleted = service.delete_tenant_quota_policy("tenant-a").unwrap();
    assert!(deleted.is_none());
}

#[tokio::test]
async fn test_tenant_quota_rejects_unregistered_tenant() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_pool_capacity_bytes: 512,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "strict-quota-node:1", 4096).await;

    let rejected = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-key".into(),
            slice_length: 100,
            tenant_id: "tenant-missing".into(),
            config: Some(one_replica_config("strict-quota-node:1")),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(rejected.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn test_tenant_quota_rejects_delete_for_non_empty_tenant() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_pool_capacity_bytes: 512,
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "quota-non-empty-node:1", 4096).await;
    service
        .upsert_tenant_quota_policy("tenant-a", 512)
        .expect("tenant policy");

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-key".into(),
            slice_length: 100,
            tenant_id: "tenant-a".into(),
            config: Some(one_replica_config("quota-non-empty-node:1")),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    let err = service.delete_tenant_quota_policy("tenant-a").unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("tenant not empty"));
}

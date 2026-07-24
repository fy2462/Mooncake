mod common;

use common::proto_uuid;
use mooncake_store_core::ReplicaType;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::{Code, Request};
use uuid::Uuid;

fn replica_config(segment: &str) -> proto::ReplicateConfig {
    proto::ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: segment.to_owned(),
        prefer_alloc_in_same_node: false,
        preferred_segments: vec![],
        preferred_nof_segments: vec![],
        data_type: proto::ObjectDataType::Unknown as i32,
        group_ids: vec![],
    }
}

fn strict_service(enable_offload: bool) -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        enable_offload,
        tenant_quota_connector_uri: tempfile::NamedTempFile::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .into_owned(),
        tenant_quota_pool_capacity_bytes: 16 * 1024,
        default_tenant_quota_bytes: 16 * 1024,
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    })
}

fn non_strict_service() -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    })
}

async fn mount_memory(service: &MasterServiceImpl, client_id: Uuid, segment: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment.to_owned(),
            size: 16 * 1024,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_local_disk(service: &MasterServiceImpl, client_id: Uuid) {
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
}

async fn put_start(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment: &str,
    tenant_id: &str,
    key: &str,
) -> Result<(), tonic::Status> {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_owned(),
            slice_length: 128,
            config: Some(replica_config(segment)),
            tenant_id: tenant_id.to_owned(),
        }),
    )
    .await
    .map(|_| ())
}

async fn put_complete(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment: &str,
    start_tenant: &str,
    completion_tenant: &str,
    key: &str,
) {
    put_start(service, client_id, segment, start_tenant, key)
        .await
        .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.to_owned(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: completion_tenant.to_owned(),
        }),
    )
    .await
    .unwrap();
}

async fn exists(service: &MasterServiceImpl, tenant_id: &str, key: &str) -> bool {
    MasterService::exist_key(
        service,
        Request::new(proto::ExistKeyRequest {
            key: key.to_owned(),
            tenant_id: tenant_id.to_owned(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .exists
}

async fn object_count(service: &MasterServiceImpl) -> usize {
    MasterService::get_all_keys_for_admin(
        service,
        Request::new(proto::GetAllKeysForAdminRequest {}),
    )
    .await
    .unwrap()
    .into_inner()
    .keys
    .len()
}

#[tokio::test]
async fn strict_put_rejects_empty_and_invalid_tenants_before_mutation() {
    let service = strict_service(false);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "strict-put:1").await;

    for (index, tenant_id) in ["", "_reserved", "bad\nname", "bad\u{7f}"]
        .into_iter()
        .enumerate()
    {
        let error = put_start(
            &service,
            client_id,
            "strict-put:1",
            tenant_id,
            &format!("invalid-{index}"),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(object_count(&service).await, 0);
    }
}

#[tokio::test]
async fn non_strict_requests_ignore_tenant_and_use_default() {
    let service = non_strict_service();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "non-strict:1").await;

    put_complete(
        &service,
        client_id,
        "non-strict:1",
        "_ignored-at-start",
        "bad\ncompletion-tenant",
        "key",
    )
    .await;

    assert!(exists(&service, "another-ignored-value", "key").await);
    assert!(exists(&service, "", "key").await);
}

#[tokio::test]
async fn invalid_batch_put_tenant_is_rejected_before_any_item_changes_state() {
    let service = strict_service(false);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "strict-batch:1").await;

    let error = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["a".into(), "b".into()],
            slice_lengths: vec![128, 128],
            config: Some(replica_config("strict-batch:1")),
            tenant_id: "_reserved".into(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(object_count(&service).await, 0);
}

#[tokio::test]
async fn invalid_ordinary_tenants_fail_reads_and_remove_before_mutation() {
    let service = strict_service(false);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "strict-read:1").await;
    service
        .upsert_tenant_quota_policy("tenant:with:colon", 4096)
        .unwrap();
    put_complete(
        &service,
        client_id,
        "strict-read:1",
        "tenant:with:colon",
        "tenant:with:colon",
        "isolated-key",
    )
    .await;

    let exist_error = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "isolated-key".into(),
            tenant_id: "_reserved".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(exist_error.code(), Code::InvalidArgument);

    let get_error = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "isolated-key".into(),
            tenant_id: "bad\nname".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(get_error.code(), Code::InvalidArgument);

    let remove_error = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "isolated-key".into(),
            force: true,
            tenant_id: "bad\u{7f}".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(remove_error.code(), Code::InvalidArgument);
    assert!(exists(&service, "tenant:with:colon", "isolated-key").await);
    assert!(!exists(&service, "tenant", "isolated-key").await);
}

#[tokio::test]
async fn batch_upsert_prevalidates_all_tenants_before_object_or_quota_mutation() {
    let service = strict_service(false);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "strict-upsert:1").await;
    service
        .upsert_tenant_quota_policy("tenant:with:colon", 4096)
        .unwrap();

    let error = MasterService::batch_upsert_start(
        &service,
        Request::new(proto::BatchUpsertStartRequest {
            entries: vec![
                proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: "would-mutate".into(),
                    slice_length: 128,
                    config: Some(replica_config("strict-upsert:1")),
                    tenant_id: "tenant:with:colon".into(),
                },
                proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: "invalid".into(),
                    slice_length: 128,
                    config: Some(replica_config("strict-upsert:1")),
                    tenant_id: "_reserved".into(),
                },
            ],
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(object_count(&service).await, 0);
    let quota = service
        .get_tenant_quota_snapshot("tenant:with:colon")
        .unwrap()
        .unwrap();
    assert_eq!(quota.reserved_bytes, 0);
    assert_eq!(quota.used_bytes, 0);
}

#[tokio::test]
async fn notify_offload_prevalidates_all_task_tenants_before_clearing_tasks() {
    let service = strict_service(true);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "strict-offload:1").await;
    mount_local_disk(&service, client_id).await;
    service
        .upsert_tenant_quota_policy("tenant:with:colon", 4096)
        .unwrap();
    put_complete(
        &service,
        client_id,
        "strict-offload:1",
        "tenant:with:colon",
        "tenant:with:colon",
        "offload-key",
    )
    .await;
    assert_eq!(
        service.replica_refcnts_for_test("offload-key", ReplicaType::Memory, "tenant:with:colon"),
        vec![1]
    );

    let error = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![],
            tasks: vec![
                proto::OffloadTaskItem {
                    tenant_id: "tenant:with:colon".into(),
                    key: "offload-key".into(),
                    size: 128,
                },
                proto::OffloadTaskItem {
                    tenant_id: "_reserved".into(),
                    key: "invalid".into(),
                    size: 128,
                },
            ],
            metadatas: vec![
                proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: 11,
                    data_size: 128,
                    transport_endpoint: "disk-holder".into(),
                },
                proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: 7,
                    data_size: 128,
                    transport_endpoint: "disk-holder".into(),
                },
            ],
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        service.replica_refcnts_for_test("offload-key", ReplicaType::Memory, "tenant:with:colon"),
        vec![1]
    );
    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "offload-key".into(),
            tenant_id: "tenant:with:colon".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert!(
        replicas.iter().all(|replica| replica.replica_type
            != proto::replica_descriptor::ReplicaType::LocalDisk as i32)
    );
}

#[test]
fn admin_quota_mutations_reject_empty_and_invalid_tenants() {
    let service = strict_service(false);

    for tenant_id in ["", "_reserved", "bad\nname", "bad\u{7f}"] {
        let error = service
            .upsert_tenant_quota_policy(tenant_id, 128)
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
    assert!(service.list_tenant_quota_snapshots().unwrap().is_empty());

    let snapshot = service
        .upsert_tenant_quota_policy("tenant:with:colon", 128)
        .unwrap();
    assert_eq!(snapshot.tenant_id.as_str(), "tenant:with:colon");
}

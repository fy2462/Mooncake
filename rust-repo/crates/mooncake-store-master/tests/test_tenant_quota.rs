mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::tenant_quota::{TenantQuotaError, TenantQuotaTable};
use mooncake_store_master::tenant_quota_policy_store::{
    TenantQuotaPolicySnapshot, load_tenant_quota_policy, save_tenant_quota_policy,
};
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl, TenantId};
use std::os::unix::fs::PermissionsExt;
use tonic::{Code, Request};
use uuid::Uuid;

static NEXT_SEGMENT_BASE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0x7_0000_0000);

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
        host_id: String::new(),
    }
}

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, size: u64) {
    let base_addr = NEXT_SEGMENT_BASE.fetch_add(0x10000, std::sync::atomic::Ordering::Relaxed);
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.to_string(),
            size,
            base_addr,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn mount_nof_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_id: Uuid,
    name: &str,
) {
    MasterService::mount_no_f_segment(
        service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(proto_uuid(segment_id)),
                name: name.into(),
                base: 0,
                size: 4096,
                te_endpoint: format!("transport://{name}"),
                client_id: Some(proto_uuid(client_id)),
            }),
        }),
    )
    .await
    .unwrap();
}

async fn put_complete(
    service: &MasterServiceImpl,
    client_id: Uuid,
    tenant_id: &str,
    key: &str,
    size: u64,
    config: proto::ReplicateConfig,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: tenant_id.into(),
            config: Some(config),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::All as i32,
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn expired_partial_put_settles_surviving_nof_object_quota() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        default_tenant_quota_bytes: 512,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 512,
        put_start_release_timeout: std::time::Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "partial-expiry-memory:1", 4096).await;
    mount_nof_segment(&service, client_id, Uuid::new_v4(), "partial-expiry-nof:1").await;
    service
        .upsert_tenant_quota_policy("tenant-a", 512)
        .expect("tenant policy");

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "partial-expiry".into(),
            slice_length: 128,
            tenant_id: "tenant-a".into(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                preferred_segment: "partial-expiry-memory:1".into(),
                preferred_nof_segments: vec!["partial-expiry-nof:1".into()],
                ..one_replica_config("partial-expiry-memory:1")
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "partial-expiry".into(),
            replica_type: proto::replica_descriptor::ReplicaType::NofSsd as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap()
            .reserved_bytes,
        128
    );

    service.reap_expired_background_tasks_for_test();

    let quota = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(quota.reserved_bytes, 0);
    assert_eq!(quota.used_bytes, 0);
    assert_eq!(quota.committed_count, 0);
    assert_eq!(quota.metadata_object_count, 1);
    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "partial-expiry".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(replicas.len(), 1);
    assert_eq!(
        replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::NofSsd as i32
    );
}

async fn mount_local_disk(service: &MasterServiceImpl, client_id: Uuid) {
    let storage_id = Uuid::new_v4();
    let recovery_session_id = Uuid::new_v4();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: false,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: false,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
            storage_id: Some(proto_uuid(storage_id)),
            recovery_complete: true,
            recovery_session_id: Some(proto_uuid(recovery_session_id)),
        }),
    )
    .await
    .unwrap();
}

async fn seed_local_disk_object(
    service: &MasterServiceImpl,
    holder_id: Uuid,
    tenant_id: &str,
    key: &str,
    size: u64,
) {
    let source_segment = format!("quota-offload-source-{key}");
    let source_segment_id = MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            segment_name: source_segment.clone(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .segment_id
    .unwrap();
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: tenant_id.into(),
            config: Some(one_replica_config(&source_segment)),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap();
    let task = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks
    .into_iter()
    .find(|task| task.tenant_id == tenant_id && task.key == key)
    .expect("Master must issue the quota fixture offload task");
    assert!(task.generation_id.is_some());
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: size as i64,
                transport_endpoint: "disk-holder:1".into(),
            }],
            tasks: vec![task],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    MasterService::unmount_segment(
        service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(source_segment_id),
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap();
}

fn temp_policy_uri() -> String {
    tempfile::NamedTempFile::new()
        .unwrap()
        .path()
        .to_string_lossy()
        .into_owned()
}

#[test]
fn test_tenant_quota_table_uses_typed_ids_for_deterministic_assignment() {
    let alpha = TenantId::new("alpha".into()).unwrap();
    let beta = TenantId::new("beta".into()).unwrap();
    let mut table = TenantQuotaTable::new(0);
    table.upsert_policy(&alpha, 2, 3).unwrap();
    table.upsert_policy(&beta, 2, 3).unwrap();
    assert_eq!(table.get_snapshot(&alpha).unwrap().effective_quota_bytes, 2);
    assert_eq!(table.get_snapshot(&beta).unwrap().effective_quota_bytes, 1);
}

#[test]
fn test_tenant_quota_settle_tracks_zero_and_partial_memory_charge() {
    let tenant = TenantId::new("tenant-a".into()).unwrap();
    let mut table = TenantQuotaTable::new(0);
    table.upsert_policy(&tenant, 512, 512).unwrap();

    table.reserve(&tenant, 200).unwrap();
    table.register_object(&tenant);
    table.settle(&tenant, 200, 100, true).unwrap();
    let partial = table.get_snapshot(&tenant).unwrap();
    assert_eq!(partial.used_bytes, 100);
    assert_eq!(partial.reserved_bytes, 0);
    assert_eq!(partial.committed_count, 1);
    assert_eq!(partial.metadata_object_count, 1);

    table.reserve(&tenant, 0).unwrap();
    table.register_object(&tenant);
    table.settle(&tenant, 0, 0, true).unwrap();
    let nof_only = table.get_snapshot(&tenant).unwrap();
    assert_eq!(nof_only.used_bytes, 100);
    assert_eq!(nof_only.committed_count, 1);
    assert_eq!(nof_only.metadata_object_count, 2);

    table.remove_object(&tenant, 0).unwrap();
    table.remove_object(&tenant, 100).unwrap();
    let empty = table.get_snapshot(&tenant).unwrap();
    assert_eq!(empty.used_bytes, 0);
    assert_eq!(empty.metadata_object_count, 0);
}

#[test]
fn test_tenant_quota_deficit_includes_used_and_reserved_bytes() {
    let tenant = TenantId::new("tenant-a".into()).unwrap();
    let mut table = TenantQuotaTable::new(0);
    table.upsert_policy(&tenant, 500, 500).unwrap();
    table.restore_object_checked(&tenant, 300).unwrap();
    table.reserve(&tenant, 100).unwrap();

    assert_eq!(table.compute_deficit(&tenant, 50), 0);
    assert_eq!(table.compute_deficit(&tenant, 150), 50);
    assert_eq!(
        table.compute_deficit(&TenantId::new("missing".into()).unwrap(), 25),
        25
    );
}

#[test]
fn checked_restore_rejects_combined_used_and_reserved_overflow_atomically() {
    let tenant = TenantId::new("tenant-a".into()).unwrap();
    let mut table = TenantQuotaTable::new(0);
    table.restore_object_checked(&tenant, u64::MAX).unwrap();
    let before = table.get_snapshot(&tenant).unwrap();

    assert_eq!(
        table.restore_reservation_checked(&tenant, 1),
        Err(TenantQuotaError::AccountingMismatch)
    );
    assert_eq!(table.get_snapshot(&tenant).unwrap(), before);

    let mut replacement = TenantQuotaTable::new(0);
    assert_eq!(
        replacement.restore_replacement_checked(&tenant, u64::MAX, 1),
        Err(TenantQuotaError::AccountingMismatch)
    );
    assert!(replacement.list_snapshots().is_empty());
}

#[tokio::test]
async fn test_tenant_quota_admission_commit_and_release() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        default_tenant_quota_bytes: 512,
        tenant_quota_connector_uri: temp_policy_uri(),
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
    assert_eq!(reserved.committed_count, 0);
    assert_eq!(reserved.metadata_object_count, 1);

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

    MasterService::put_start(
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
    .expect("quota admission should evict the expired same-tenant object");

    let admitted = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(admitted.used_bytes, 0);
    assert_eq!(admitted.reserved_bytes, 200);
    assert_eq!(admitted.committed_count, 0);
    assert_eq!(admitted.metadata_object_count, 1);
    let old = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "quota-key".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!old.exists);

    MasterService::put_revoke(
        &service,
        Request::new(proto::PutRevokeRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "too-large".into(),
            replica_type: proto::replica_descriptor::ReplicaType::All as i32,
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

#[tokio::test]
async fn test_tenant_quota_admission_evicts_only_the_requesting_tenant() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 800,
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "quota-isolation:1", 4096).await;
    service.upsert_tenant_quota_policy("tenant-a", 400).unwrap();
    service.upsert_tenant_quota_policy("tenant-b", 400).unwrap();

    put_complete(
        &service,
        client_id,
        "tenant-b",
        "tenant-b-object",
        100,
        one_replica_config("quota-isolation:1"),
    )
    .await;
    put_complete(
        &service,
        client_id,
        "tenant-a",
        "tenant-a-object",
        400,
        one_replica_config("quota-isolation:1"),
    )
    .await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "tenant-a-replacement".into(),
            slice_length: 1,
            tenant_id: "tenant-a".into(),
            config: Some(one_replica_config("quota-isolation:1")),
        }),
    )
    .await
    .expect("tenant-a admission should evict only tenant-a bytes");

    let tenant_a = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(tenant_a.used_bytes, 0);
    assert_eq!(tenant_a.reserved_bytes, 1);
    let tenant_b = service
        .get_tenant_quota_snapshot("tenant-b")
        .unwrap()
        .unwrap();
    assert_eq!(tenant_b.used_bytes, 100);
    assert_eq!(tenant_b.metadata_object_count, 1);
    assert!(
        MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "tenant-b-object".into(),
                tenant_id: "tenant-b".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .exists
    );
}

#[tokio::test]
async fn test_tenant_quota_admission_respects_hard_and_soft_pin_policy() {
    for (hard_pin, soft_pin, allow_soft, should_admit) in [
        (true, false, true, false),
        (false, true, false, false),
        (false, true, true, true),
    ] {
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_tenant_quota: true,
            tenant_quota_connector_uri: temp_policy_uri(),
            tenant_quota_pool_capacity_bytes: 100,
            lease_ttl: std::time::Duration::ZERO,
            soft_pin_ttl: std::time::Duration::from_secs(3600),
            allow_evict_soft_pinned_objects: allow_soft,
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        let segment = format!("quota-pin-{hard_pin}-{soft_pin}-{allow_soft}:1");
        mount_segment(&service, client_id, &segment, 4096).await;
        service.upsert_tenant_quota_policy("tenant-a", 100).unwrap();
        let mut config = one_replica_config(&segment);
        config.with_hard_pin = hard_pin;
        config.with_soft_pin = soft_pin;
        put_complete(&service, client_id, "tenant-a", "protected", 100, config).await;

        let admission = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "next".into(),
                slice_length: 1,
                tenant_id: "tenant-a".into(),
                config: Some(one_replica_config(&segment)),
            }),
        )
        .await;
        assert_eq!(admission.is_ok(), should_admit);
        if let Err(status) = admission {
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert_eq!(status.message(), "tenant quota exceeded");
        }
    }
}

#[tokio::test]
async fn test_size_changing_upsert_quota_failure_preserves_the_protected_old_object() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 200,
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "quota-upsert-protected:1", 4096).await;
    service.upsert_tenant_quota_policy("tenant-a", 200).unwrap();
    put_complete(
        &service,
        client_id,
        "tenant-a",
        "protected-old",
        100,
        one_replica_config("quota-upsert-protected:1"),
    )
    .await;

    let rejected = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "protected-old".into(),
            slice_length: 150,
            tenant_id: "tenant-a".into(),
            config: Some(one_replica_config("quota-upsert-protected:1")),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(rejected.code(), Code::ResourceExhausted);
    assert_eq!(rejected.message(), "tenant quota exceeded");
    assert!(
        MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "protected-old".into(),
                tenant_id: "tenant-a".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .exists
    );
    let quota = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(quota.used_bytes, 100);
    assert_eq!(quota.reserved_bytes, 0);
    assert_eq!(quota.committed_count, 1);
    assert_eq!(quota.metadata_object_count, 1);
}

#[tokio::test]
async fn test_tenant_quota_group_live_lease_blocks_partial_group_eviction() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 200,
        lease_ttl: std::time::Duration::from_millis(30),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "quota-group:1", 4096).await;
    service.upsert_tenant_quota_policy("tenant-a", 200).unwrap();
    let mut grouped = one_replica_config("quota-group:1");
    grouped.group_ids = vec!["group-a".into()];
    put_complete(
        &service,
        client_id,
        "tenant-a",
        "expired-member",
        100,
        grouped.clone(),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    put_complete(&service, client_id, "tenant-a", "live-member", 100, grouped).await;
    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "live-member".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    let rejected = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "would-split-group".into(),
            slice_length: 1,
            tenant_id: "tenant-a".into(),
            config: Some(one_replica_config("quota-group:1")),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(rejected.code(), Code::ResourceExhausted);
    assert_eq!(rejected.message(), "tenant quota exceeded");
}

#[tokio::test]
async fn test_tenant_quota_charges_each_completed_memory_replica() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 300,
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "quota-replica-a:1", 4096).await;
    mount_segment(&service, client_id, "quota-replica-b:1", 4096).await;
    service
        .upsert_tenant_quota_policy("tenant-a", 300)
        .expect("tenant policy");

    let config = proto::ReplicateConfig {
        replica_num: 2,
        preferred_segments: vec!["quota-replica-a:1".into(), "quota-replica-b:1".into()],
        ..one_replica_config("")
    };
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "replicated".into(),
            slice_length: 100,
            tenant_id: "tenant-a".into(),
            config: Some(config),
        }),
    )
    .await
    .unwrap();
    let reserved = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(reserved.reserved_bytes, 200);

    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "replicated".into(),
            replica_type: proto::replica_descriptor::ReplicaType::All as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let committed = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(committed.used_bytes, 200);
    assert_eq!(committed.reserved_bytes, 0);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "replicated".into(),
            force: true,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap()
            .used_bytes,
        0
    );
}

#[tokio::test]
async fn test_tenant_quota_tracks_copy_move_and_full_memory_eviction() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 400,
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    for segment in ["quota-flow-a:1", "quota-flow-b:1", "quota-flow-c:1"] {
        mount_segment(&service, client_id, segment, 4096).await;
    }
    service
        .upsert_tenant_quota_policy("tenant-a", 400)
        .expect("tenant policy");

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-flow".into(),
            slice_length: 100,
            tenant_id: "tenant-a".into(),
            config: Some(one_replica_config("quota-flow-a:1")),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-flow".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-flow".into(),
            source: "quota-flow-a:1".into(),
            targets: vec!["quota-flow-b:1".into()],
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let copying = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(copying.used_bytes, 100);
    assert_eq!(copying.reserved_bytes, 100);

    MasterService::copy_end(
        &service,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-flow".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let copied = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(copied.used_bytes, 200);
    assert_eq!(copied.reserved_bytes, 0);
    assert_eq!(copied.committed_count, 1);
    assert_eq!(copied.metadata_object_count, 1);

    MasterService::move_start(
        &service,
        Request::new(proto::MoveStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-flow".into(),
            source: "quota-flow-a:1".into(),
            target: "quota-flow-c:1".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let moving = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(moving.used_bytes, 200);
    assert_eq!(moving.reserved_bytes, 100);

    MasterService::move_end(
        &service,
        Request::new(proto::MoveEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "quota-flow".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let moved = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(moved.used_bytes, 200);
    assert_eq!(moved.reserved_bytes, 0);
    assert_eq!(moved.committed_count, 1);

    assert_eq!(
        service.run_eviction_cycle_for_test(1),
        vec!["quota-flow".to_string()]
    );
    let evicted = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(evicted.used_bytes, 0);
    assert_eq!(evicted.reserved_bytes, 0);
    assert_eq!(evicted.committed_count, 0);
    assert_eq!(evicted.metadata_object_count, 0);
}

#[tokio::test]
async fn test_add_replica_rejects_injection_and_offload_counts_zero_charge_metadata() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        enable_offload: true,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 400,
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    service
        .upsert_tenant_quota_policy("tenant-a", 400)
        .expect("tenant policy");

    let invalid = MasterService::add_replica(
        &service,
        Request::new(proto::AddReplicaRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "disk-only".into(),
            replica: Some(proto::ReplicaDescriptor {
                segment_name: "memory-bypass:1".into(),
                status: proto::replica_descriptor::ReplicaStatus::Complete as i32,
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                size: 100,
                holder_client_id: Some(proto_uuid(client_id)),
                ..Default::default()
            }),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);
    let before_disk = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(before_disk.metadata_object_count, 0);

    mount_local_disk(&service, client_id).await;
    seed_local_disk_object(&service, client_id, "tenant-a", "disk-only", 100).await;
    let disk_only = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(disk_only.used_bytes, 0);
    assert_eq!(disk_only.reserved_bytes, 0);
    assert_eq!(disk_only.committed_count, 0);
    assert_eq!(disk_only.metadata_object_count, 1);

    let delete = service.delete_tenant_quota_policy("tenant-a").unwrap_err();
    assert_eq!(delete.code(), Code::FailedPrecondition);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "disk-only".into(),
            force: true,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap()
            .metadata_object_count,
        0
    );
}

#[tokio::test]
async fn test_promotion_registers_first_physical_memory_charge() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        tenant_quota_connector_uri: temp_policy_uri(),
        tenant_quota_pool_capacity_bytes: 400,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    let dram_client_id = Uuid::new_v4();
    service
        .upsert_tenant_quota_policy("tenant-a", 400)
        .expect("tenant policy");
    mount_local_disk(&service, holder_id).await;
    mount_segment(&service, dram_client_id, "quota-promotion:1", 4096).await;

    seed_local_disk_object(&service, holder_id, "tenant-a", "promotion", 100).await;
    let disk_only = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(disk_only.used_bytes, 0);
    assert_eq!(disk_only.reserved_bytes, 0);
    assert_eq!(disk_only.committed_count, 0);
    assert_eq!(disk_only.metadata_object_count, 1);
    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "promotion".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let heartbeat = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(heartbeat.objects.get("promotion"), Some(&100));

    MasterService::promotion_alloc_start(
        &service,
        Request::new(proto::PromotionAllocStartRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "promotion".into(),
            size: 100,
            preferred_segments: vec!["quota-promotion:1".into()],
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let reserved = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(reserved.used_bytes, 0);
    assert_eq!(reserved.reserved_bytes, 100);
    assert_eq!(reserved.committed_count, 0);
    assert_eq!(reserved.metadata_object_count, 1);

    MasterService::notify_promotion_success(
        &service,
        Request::new(proto::NotifyPromotionSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            key: "promotion".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    let promoted = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(promoted.used_bytes, 100);
    assert_eq!(promoted.reserved_bytes, 0);
    assert_eq!(promoted.committed_count, 1);
    assert_eq!(promoted.metadata_object_count, 1);
}

#[test]
fn test_tenant_quota_policy_connectors_require_uri() {
    for connector_type in ["file", "etcd"] {
        let load_err = load_tenant_quota_policy(connector_type, " ", "cluster-a").unwrap_err();
        assert!(load_err.contains("non-empty uri"));

        let save_err = save_tenant_quota_policy(
            connector_type,
            "",
            "cluster-a",
            &TenantQuotaPolicySnapshot::default(),
        )
        .unwrap_err();
        assert!(save_err.contains("non-empty uri"));
    }
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
    service.set_leadership_view_version(17);
    service.set_view_version(999);

    let snapshot = service.upsert_tenant_quota_policy("tenant-a", 800).unwrap();
    assert_eq!(
        snapshot.tenant_id,
        TenantId::new("tenant-a".to_string()).unwrap()
    );
    assert_eq!(snapshot.requested_quota_bytes, 800);
    assert!(snapshot.has_explicit_policy);
    let persisted =
        load_tenant_quota_policy("file", &temp.path().to_string_lossy(), "mooncake_cluster")
            .unwrap();
    assert_eq!(persisted.producer_view_version, 17);

    let list = service.list_tenant_quota_snapshots().unwrap();
    assert_eq!(list.len(), 1);

    let deleted = service.delete_tenant_quota_policy("tenant-a").unwrap();
    assert!(deleted.is_none());
}

#[test]
fn test_leader_term_barrier_absorbs_prior_policy_write_and_fences_late_writer() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let uri = temp.path().to_string_lossy().into_owned();
    save_tenant_quota_policy(
        "file",
        &uri,
        "mooncake_cluster",
        &TenantQuotaPolicySnapshot {
            producer_view_version: 11,
            tenant_quotas: std::collections::BTreeMap::from([("tenant-a".to_string(), 800)]),
        },
    )
    .unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: uri.clone(),
        tenant_quota_pool_capacity_bytes: 2000,
        ..Default::default()
    });

    let prior_term_winner = TenantQuotaPolicySnapshot {
        producer_view_version: 11,
        tenant_quotas: std::collections::BTreeMap::from([("tenant-b".to_string(), 600)]),
    };
    save_tenant_quota_policy("file", &uri, "mooncake_cluster", &prior_term_winner).unwrap();

    service.set_leadership_view_version(12);
    service.prepare_tenant_quota_leadership_term(12).unwrap();

    assert!(
        service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        service
            .get_tenant_quota_snapshot("tenant-b")
            .unwrap()
            .unwrap()
            .requested_quota_bytes,
        600
    );
    let persisted = load_tenant_quota_policy("file", &uri, "mooncake_cluster").unwrap();
    assert_eq!(persisted.producer_view_version, 12);
    assert_eq!(persisted.tenant_quotas, prior_term_winner.tenant_quotas);
    assert!(
        save_tenant_quota_policy(
            "file",
            &uri,
            "mooncake_cluster",
            &TenantQuotaPolicySnapshot {
                producer_view_version: 11,
                tenant_quotas: std::collections::BTreeMap::new(),
            },
        )
        .is_err()
    );
}

#[test]
fn test_tenant_quota_admin_policy_mutation_is_fenced_after_demotion() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp.path().to_string_lossy().into_owned(),
        tenant_quota_pool_capacity_bytes: 2000,
        ..Default::default()
    });
    service.upsert_tenant_quota_policy("tenant-a", 800).unwrap();

    service.set_service_available(false);

    let upsert = service
        .upsert_tenant_quota_policy("tenant-b", 600)
        .unwrap_err();
    assert_eq!(upsert.code(), Code::Unavailable);
    let delete = service.delete_tenant_quota_policy("tenant-a").unwrap_err();
    assert_eq!(delete.code(), Code::Unavailable);
    assert!(
        service
            .get_tenant_quota_snapshot("tenant-b")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .unwrap()
            .requested_quota_bytes,
        800
    );
}

#[test]
fn test_tenant_quota_policy_save_failure_does_not_mutate_memory_state() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let policy_path = temp_dir.path().join("tenant-quota.yaml");
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: policy_path.to_string_lossy().into_owned(),
        tenant_quota_pool_capacity_bytes: 2000,
        ..Default::default()
    });

    service
        .upsert_tenant_quota_policy("tenant-a", 800)
        .expect("initial tenant policy");
    let original_permissions = std::fs::metadata(temp_dir.path())
        .unwrap()
        .permissions()
        .mode();
    let mut read_only = std::fs::metadata(temp_dir.path()).unwrap().permissions();
    read_only.set_mode(0o500);
    std::fs::set_permissions(temp_dir.path(), read_only).unwrap();

    let upsert_err = service
        .upsert_tenant_quota_policy("tenant-b", 600)
        .unwrap_err();
    assert_eq!(upsert_err.code(), Code::Unavailable);
    assert!(service.is_service_fenced());
    assert!(
        service
            .get_tenant_quota_snapshot("tenant-b")
            .unwrap()
            .is_none()
    );

    let delete_err = service.delete_tenant_quota_policy("tenant-a").unwrap_err();
    assert_eq!(delete_err.code(), Code::Unavailable);
    let tenant_a = service
        .get_tenant_quota_snapshot("tenant-a")
        .unwrap()
        .expect("tenant-a policy should remain in memory after save failure");
    assert_eq!(tenant_a.requested_quota_bytes, 800);
    assert!(tenant_a.has_explicit_policy);

    let mut restored = std::fs::metadata(temp_dir.path()).unwrap().permissions();
    restored.set_mode(original_permissions);
    std::fs::set_permissions(temp_dir.path(), restored).unwrap();
}

#[tokio::test]
async fn test_tenant_quota_rejects_unregistered_tenant() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp_policy_uri(),
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
    assert!(rejected.message().contains("tenant not registered"));
}

#[tokio::test]
async fn test_tenant_quota_rejects_delete_for_non_empty_tenant() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: temp_policy_uri(),
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

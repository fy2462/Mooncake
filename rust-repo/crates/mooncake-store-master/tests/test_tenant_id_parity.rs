mod common;

use common::proto_uuid;
use dashmap::DashMap;
use mooncake_store_core::ReplicaType;
use mooncake_store_master::TenantId;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::service::ObjectEntry;
use mooncake_store_master::storage_backend::{StorageBackend, StorageBackendType};
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::SystemTime;
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
        host_id: String::new(),
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

fn strict_remote_pull_service() -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        remote_source_enabled: true,
        tenant_quota_connector_uri: tempfile::NamedTempFile::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .into_owned(),
        ..Default::default()
    })
}

fn non_strict_remote_pull_service() -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        remote_source_enabled: true,
        ..Default::default()
    })
}

fn non_strict_service() -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::ZERO,
        ..Default::default()
    })
}

fn strict_service_with_unregistered_object(tenant_id: &str, key: &str) -> MasterServiceImpl {
    let snapshot_dir = tempfile::tempdir().unwrap();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, snapshot_dir.path());
    let objects = DashMap::new();
    objects.insert(
        format!("{tenant_id}\0{key}"),
        ObjectEntry {
            replicas: vec![mooncake_store_core::ReplicaDescriptor {
                segment_id: Uuid::new_v4(),
                segment_name: "disk://fixture".into(),
                offset: 0,
                size: 128,
                status: mooncake_store_core::ReplicaStatus::Complete,
                replica_type: ReplicaType::Disk,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 0,
                protocol: String::new(),
            }],
            size: 128,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::nil(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: TenantId::new(tenant_id.to_owned()).unwrap(),
            group_id: String::new(),
            quota_committed: false,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            disk_allocated_bytes_accounted: 0,
            user_key: key.to_owned(),
        },
    );
    backend
        .save(&DashMap::new(), &DashMap::new(), &objects, &DashMap::new())
        .unwrap();

    let policy = tempfile::NamedTempFile::new().unwrap();
    MasterServiceImpl::new_with_runtime_config(
        Some(StorageBackendType::LocalDisk),
        Some(snapshot_dir.keep()),
        MasterRuntimeConfig {
            enable_tenant_quota: true,
            enable_offload: true,
            tenant_quota_connector_uri: policy.path().to_string_lossy().into_owned(),
            tenant_quota_pool_capacity_bytes: 16 * 1024,
            default_tenant_quota_bytes: 16 * 1024,
            lease_ttl: std::time::Duration::ZERO,
            ..Default::default()
        },
    )
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
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
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

async fn take_offload_task(
    service: &MasterServiceImpl,
    client_id: Uuid,
    tenant_id: &str,
    key: &str,
) -> proto::OffloadTaskItem {
    let tasks = MasterService::offload_object_heartbeat(
        service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks;
    let task = tasks
        .into_iter()
        .find(|task| task.tenant_id == tenant_id && task.key == key)
        .expect("Master must issue the admitted offload task");
    assert!(
        task.generation_id
            .as_ref()
            .is_some_and(|generation_id| generation_id.high != 0 || generation_id.low != 0)
    );
    task
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

fn local_disk_replica(client_id: Uuid, size: u64) -> proto::ReplicaDescriptor {
    proto::ReplicaDescriptor {
        status: proto::replica_descriptor::ReplicaStatus::Complete as i32,
        replica_type: proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        size,
        holder_client_id: Some(proto_uuid(client_id)),
        ..Default::default()
    }
}

fn offload_metadata(key: &str, size: i64) -> proto::StorageObjectMetadata {
    proto::StorageObjectMetadata {
        key_size: key.len() as i64,
        data_size: size,
        transport_endpoint: "disk-holder".into(),
        ..Default::default()
    }
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
async fn strict_add_replica_rejects_unregistered_tenant_without_mutation() {
    let service = strict_service(false);
    let client_id = Uuid::new_v4();

    let error = MasterService::add_replica(
        &service,
        Request::new(proto::AddReplicaRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "must-not-exist".into(),
            replica: Some(local_disk_replica(client_id, 128)),
            tenant_id: "unregistered".into(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(object_count(&service).await, 0);
}

#[tokio::test]
async fn admitted_offload_task_can_complete_after_tenant_registration_is_removed() {
    let service = strict_service(true);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "orphan-offload:1").await;
    mount_local_disk(&service, client_id).await;
    service.upsert_tenant_quota_policy("orphan", 4096).unwrap();
    put_complete(
        &service,
        client_id,
        "orphan-offload:1",
        "orphan",
        "orphan",
        "in-flight",
    )
    .await;
    assert_eq!(
        service.replica_refcnts_for_test("in-flight", ReplicaType::Memory, "orphan"),
        vec![1]
    );
    let task = take_offload_task(&service, client_id, "orphan", "in-flight").await;

    service.remove_tenant_registration_for_test("orphan");
    assert!(
        !service
            .get_tenant_quota_snapshot("orphan")
            .unwrap()
            .unwrap()
            .has_explicit_policy
    );

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![],
            tasks: vec![task],
            metadatas: vec![offload_metadata("in-flight", 128)],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        service.replica_refcnts_for_test("in-flight", ReplicaType::Memory, "orphan"),
        vec![0]
    );
    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "in-flight".into(),
            tenant_id: "orphan".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert!(replicas.iter().any(|replica| {
        replica.replica_type == proto::replica_descriptor::ReplicaType::LocalDisk as i32
    }));
}

#[tokio::test]
async fn cpp_parity_notify_evicted_disk_replicas_removes_same_key_for_both_tenants() {
    let service = strict_service(true);
    let client_id = Uuid::new_v4();
    let segment_id = MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "tenant-eviction:1".into(),
            size: 16 * 1024,
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
    .expect("mounted memory segment has an id");
    mount_local_disk(&service, client_id).await;
    for tenant_id in ["tenant-a", "tenant-b"] {
        service.upsert_tenant_quota_policy(tenant_id, 4096).unwrap();
        put_complete(
            &service,
            client_id,
            "tenant-eviction:1",
            tenant_id,
            tenant_id,
            "shared-key",
        )
        .await;
    }

    let mut tasks = MasterService::offload_object_heartbeat(
        &service,
        Request::new(proto::OffloadObjectHeartbeatRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks;
    tasks.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
    assert_eq!(
        tasks
            .iter()
            .map(|task| (task.tenant_id.as_str(), task.key.as_str()))
            .collect::<Vec<_>>(),
        [("tenant-a", "shared-key"), ("tenant-b", "shared-key")]
    );
    assert!(tasks.iter().all(|task| {
        task.generation_id
            .as_ref()
            .is_some_and(|generation_id| generation_id.high != 0 || generation_id.low != 0)
    }));

    MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![],
            metadatas: tasks
                .iter()
                .map(|_| offload_metadata("shared-key", 128))
                .collect(),
            tasks,
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap();
    MasterService::unmount_segment(
        &service,
        Request::new(proto::UnmountSegmentRequest {
            segment_id: Some(segment_id),
            client_id: Some(proto_uuid(client_id)),
        }),
    )
    .await
    .unwrap();

    for tenant_id in ["tenant-a", "tenant-b"] {
        let replicas = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "shared-key".into(),
                tenant_id: tenant_id.into(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .replicas;
        assert_eq!(replicas.len(), 1);
        assert_eq!(
            replicas[0].replica_type,
            proto::replica_descriptor::ReplicaType::LocalDisk as i32
        );

        let statuses = MasterService::batch_evict_disk_replica(
            &service,
            Request::new(proto::BatchEvictDiskReplicaRequest {
                client_id: Some(proto_uuid(client_id)),
                keys: vec!["shared-key".into()],
                replica_type: proto::replica_descriptor::ReplicaType::LocalDisk as i32,
                tenant_id: tenant_id.into(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .statuses;
        assert_eq!(statuses, [0]);

        let error = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "shared-key".into(),
                tenant_id: tenant_id.into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::NotFound);
    }
}

#[tokio::test]
async fn unsolicited_offload_success_rejects_unregistered_tenant_without_mutation() {
    let service = strict_service_with_unregistered_object("unregistered", "unsolicited");
    let client_id = Uuid::new_v4();
    mount_local_disk(&service, client_id).await;
    assert_eq!(object_count(&service).await, 1);

    let error = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![],
            tasks: vec![proto::OffloadTaskItem {
                tenant_id: "unregistered".into(),
                key: "unsolicited".into(),
                size: 128,
                generation_id: Some(proto_uuid(Uuid::new_v4())),
            }],
            metadatas: vec![offload_metadata("unsolicited", 128)],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(object_count(&service).await, 1);
    let snapshot = service.capture_loaded_snapshot("after-rejection");
    let object = snapshot
        .objects
        .iter()
        .find(|(key, _)| key == "unregistered\0unsolicited")
        .unwrap();
    assert_eq!(object.1.replicas.len(), 1);
}

#[tokio::test]
async fn mixed_offload_batch_rejection_does_not_clear_or_mutate_earlier_task() {
    let service = strict_service(true);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "atomic-offload:1").await;
    mount_local_disk(&service, client_id).await;
    service
        .upsert_tenant_quota_policy("registered", 4096)
        .unwrap();
    put_complete(
        &service,
        client_id,
        "atomic-offload:1",
        "registered",
        "registered",
        "admitted",
    )
    .await;
    assert_eq!(
        service.replica_refcnts_for_test("admitted", ReplicaType::Memory, "registered"),
        vec![1]
    );
    let admitted_task = take_offload_task(&service, client_id, "registered", "admitted").await;

    let error = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![],
            tasks: vec![
                admitted_task,
                proto::OffloadTaskItem {
                    tenant_id: "unregistered".into(),
                    key: "unsolicited".into(),
                    size: 128,
                    generation_id: Some(proto_uuid(Uuid::new_v4())),
                },
            ],
            metadatas: vec![
                offload_metadata("admitted", 128),
                offload_metadata("unsolicited", 128),
            ],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        service.replica_refcnts_for_test("admitted", ReplicaType::Memory, "registered"),
        vec![1]
    );
    let replicas = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "admitted".into(),
            tenant_id: "registered".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert!(replicas.iter().all(|replica| {
        replica.replica_type != proto::replica_descriptor::ReplicaType::LocalDisk as i32
    }));
    assert!(!exists(&service, "unregistered", "unsolicited").await);
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
async fn cpp_parity_single_tenant_collapses_duplicate_remove_and_has_no_quota_snapshot() {
    let service = non_strict_service();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "single-tenant:1").await;

    // C++ SingleTenantModeCollapsesTenantsAndDisablesQuota: an 800-byte object
    // completed as tenant-a is visible as tenant-b.
    put_start(
        &service,
        client_id,
        "single-tenant:1",
        "tenant-a",
        "shared-key",
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "shared-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();

    assert!(exists(&service, "tenant-b", "shared-key").await);

    // A duplicate PutStart as tenant-b fails OBJECT_ALREADY_EXISTS.
    let duplicate = put_start(
        &service,
        client_id,
        "single-tenant:1",
        "tenant-b",
        "shared-key",
    )
    .await
    .unwrap_err();
    assert_eq!(duplicate.code(), Code::AlreadyExists);

    // Forced removal as tenant-b succeeds and leaves tenant-a with no snapshot
    // because multi-tenancy (and therefore quota accounting) is disabled.
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "shared-key".into(),
            force: true,
            tenant_id: "tenant-b".into(),
        }),
    )
    .await
    .unwrap();

    assert!(
        service
            .get_tenant_quota_snapshot("tenant-a")
            .unwrap()
            .is_none()
    );
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
    let admitted_task =
        take_offload_task(&service, client_id, "tenant:with:colon", "offload-key").await;

    let error = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec![],
            tasks: vec![
                admitted_task,
                proto::OffloadTaskItem {
                    tenant_id: "_reserved".into(),
                    key: "invalid".into(),
                    size: 128,
                    generation_id: Some(proto_uuid(Uuid::new_v4())),
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
            recovery_session_id: None,
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

#[tokio::test]
async fn strict_upsert_rejects_unregistered_tenant_before_removing_existing_object() {
    let service = strict_service_with_unregistered_object("unregistered", "existing");
    assert!(
        service
            .capture_loaded_snapshot("before")
            .objects
            .iter()
            .any(|(key, _)| key == "unregistered\0existing")
    );

    let error = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(Uuid::new_v4())),
            key: "existing".into(),
            slice_length: 256,
            config: Some(replica_config("missing-segment")),
            tenant_id: "unregistered".into(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        service
            .capture_loaded_snapshot("after")
            .objects
            .iter()
            .any(|(key, _)| key == "unregistered\0existing")
    );
}

#[tokio::test]
async fn batch_upsert_rejects_later_unregistered_tenant_before_any_entry_mutates() {
    let service = strict_service(false);
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "strict-registered-batch:1").await;
    service
        .upsert_tenant_quota_policy("registered", 4096)
        .unwrap();

    let error = MasterService::batch_upsert_start(
        &service,
        Request::new(proto::BatchUpsertStartRequest {
            entries: vec![
                proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: "would-mutate".into(),
                    slice_length: 128,
                    config: Some(replica_config("strict-registered-batch:1")),
                    tenant_id: "registered".into(),
                },
                proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: "unregistered".into(),
                    slice_length: 128,
                    config: Some(replica_config("strict-registered-batch:1")),
                    tenant_id: "unregistered".into(),
                },
            ],
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(object_count(&service).await, 0);
    let quota = service
        .get_tenant_quota_snapshot("registered")
        .unwrap()
        .unwrap();
    assert_eq!(quota.reserved_bytes, 0);
}

#[tokio::test]
async fn strict_remote_pull_coordination_is_tenant_scoped_and_validated() {
    let service = strict_remote_pull_service();
    let client_id = Uuid::new_v4();

    let acquire = |tenant_id: &str| {
        MasterService::acquire_remote_pull(
            &service,
            Request::new(proto::AcquireRemotePullRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "shared-key".into(),
                tenant_id: tenant_id.into(),
            }),
        )
    };

    assert_eq!(
        acquire("tenant-a").await.unwrap().into_inner().action,
        proto::RemotePullAction::Pull as i32
    );
    assert_eq!(
        acquire("tenant-a").await.unwrap().into_inner().action,
        proto::RemotePullAction::Wait as i32
    );
    assert_eq!(
        acquire("tenant-b").await.unwrap().into_inner().action,
        proto::RemotePullAction::Pull as i32
    );

    let error = acquire("_reserved").await.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);

    let other_client = Uuid::new_v4();
    let error = MasterService::complete_remote_pull(
        &service,
        Request::new(proto::CompleteRemotePullRequest {
            client_id: Some(proto_uuid(other_client)),
            key: "shared-key".into(),
            success: true,
            data_size: 128,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    assert_eq!(
        acquire("tenant-a").await.unwrap().into_inner().action,
        proto::RemotePullAction::Wait as i32
    );

    let error = MasterService::release_remote_pull(
        &service,
        Request::new(proto::ReleaseRemotePullRequest {
            client_id: Some(proto_uuid(other_client)),
            key: "shared-key".into(),
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);

    MasterService::complete_remote_pull(
        &service,
        Request::new(proto::CompleteRemotePullRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "shared-key".into(),
            success: true,
            data_size: 128,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        acquire("tenant-a").await.unwrap().into_inner().action,
        proto::RemotePullAction::Pull as i32
    );
    assert_eq!(
        acquire("tenant-b").await.unwrap().into_inner().action,
        proto::RemotePullAction::Wait as i32
    );
}

#[tokio::test]
async fn non_strict_remote_pull_coordination_uses_one_default_tenant_key() {
    let service = non_strict_remote_pull_service();
    let client_id = Uuid::new_v4();

    let acquire = |tenant_id: &str| {
        MasterService::acquire_remote_pull(
            &service,
            Request::new(proto::AcquireRemotePullRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "shared-key".into(),
                tenant_id: tenant_id.into(),
            }),
        )
    };

    assert_eq!(
        acquire("_reserved").await.unwrap().into_inner().action,
        proto::RemotePullAction::Pull as i32
    );
    assert_eq!(
        acquire("tenant-a").await.unwrap().into_inner().action,
        proto::RemotePullAction::Wait as i32
    );

    MasterService::complete_remote_pull(
        &service,
        Request::new(proto::CompleteRemotePullRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "shared-key".into(),
            success: true,
            data_size: 128,
            tenant_id: "bad\ncompletion-tenant".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        acquire("tenant-b").await.unwrap().into_inner().action,
        proto::RemotePullAction::Pull as i32
    );

    MasterService::release_remote_pull(
        &service,
        Request::new(proto::ReleaseRemotePullRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "shared-key".into(),
            tenant_id: "_another-reserved-value".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        acquire("tenant-c").await.unwrap().into_inner().action,
        proto::RemotePullAction::Pull as i32
    );
}

#[tokio::test]
async fn wrapped_put_start_rejects_empty_and_nul_tenants_parity() {
    let service = strict_service(false);
    service
        .upsert_tenant_quota_policy("registered-tenant", 4096)
        .unwrap();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "wrapped-write:1").await;

    for (index, tenant_id) in ["", "tenant\0bad"].into_iter().enumerate() {
        let error = put_start(
            &service,
            client_id,
            "wrapped-write:1",
            tenant_id,
            &format!("invalid-{index}"),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
    }
    assert_eq!(object_count(&service).await, 0);
}

#[tokio::test]
async fn wrapped_read_control_invalid_tenant_matrix_parity() {
    let service = strict_service(true);
    service.upsert_tenant_quota_policy("default", 4096).unwrap();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "wrapped-read:1").await;
    mount_local_disk(&service, client_id).await;

    let get_error = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "missing-key".into(),
            tenant_id: "_invalid-tenant".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(get_error.code(), Code::InvalidArgument);

    let batch_error = MasterService::batch_get_replica_list(
        &service,
        Request::new(proto::BatchGetReplicaListRequest {
            keys: vec!["key-a".into(), "key-b".into()],
            tenant_id: "tenant\0bad".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(batch_error.code(), Code::InvalidArgument);

    let offload_error = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["key".into()],
            tasks: vec![proto::OffloadTaskItem {
                tenant_id: "_invalid-tenant".into(),
                key: "key".into(),
                size: 1,
                generation_id: Some(proto_uuid(Uuid::new_v4())),
            }],
            metadatas: vec![offload_metadata("key", 1)],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(offload_error.code(), Code::InvalidArgument);

    let removed = MasterService::remove_all(
        &service,
        Request::new(proto::RemoveAllRequest {
            force: false,
            tenant_id: "_invalid-tenant".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .removed_count;
    assert_eq!(removed, 0);
}

#[tokio::test]
async fn wrapped_exist_normalizes_empty_and_ignores_tenant_when_disabled_parity() {
    let strict = strict_service(false);
    strict.upsert_tenant_quota_policy("default", 4096).unwrap();
    let client_id = Uuid::new_v4();
    mount_memory(&strict, client_id, "wrapped-exist:1").await;
    assert!(!exists(&strict, "", "missing-key").await);

    let single = non_strict_service();
    let client_id = Uuid::new_v4();
    mount_memory(&single, client_id, "wrapped-exist:2").await;
    assert!(!exists(&single, "_invalid-tenant", "missing-key").await);
}

#[tokio::test]
async fn cpp_parity_strict_mode_requires_explicit_default_and_named_registration() {
    let service = strict_service(false);
    service
        .upsert_tenant_quota_policy("tenant-a", 1000)
        .unwrap();
    let client_id = Uuid::new_v4();
    mount_memory(&service, client_id, "strict-mode:1").await;

    for (index, tenant_id) in ["tenant-b", "default"].into_iter().enumerate() {
        let error = put_start(
            &service,
            client_id,
            "strict-mode:1",
            tenant_id,
            &format!("implicit-{index}"),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
    }

    assert!(TenantId::new("tenant\0bad".to_owned()).is_err());

    service.upsert_tenant_quota_policy("default", 100).unwrap();
    async fn put_small(
        service: &MasterServiceImpl,
        client_id: Uuid,
        segment: &str,
        tenant_id: &str,
        key: &str,
    ) {
        MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.to_owned(),
                slice_length: 10,
                config: Some(replica_config(segment)),
                tenant_id: tenant_id.to_owned(),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.to_owned(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: tenant_id.to_owned(),
            }),
        )
        .await
        .unwrap();
    }
    put_small(
        &service,
        client_id,
        "strict-mode:1",
        "default",
        "default-key",
    )
    .await;
    put_small(
        &service,
        client_id,
        "strict-mode:1",
        "tenant-a",
        "tenant-a-key",
    )
    .await;
    assert!(exists(&service, "default", "default-key").await);
    assert!(exists(&service, "tenant-a", "tenant-a-key").await);
}

#[tokio::test]
async fn ping_ignores_tenant_like_the_cpp_control_plane() {
    let service = strict_service(false);
    let client_id = Uuid::new_v4();

    let response = MasterService::ping(
        &service,
        Request::new(proto::PingRequest {
            client_id: Some(proto_uuid(client_id)),
            mounted_segments: vec![],
            tenant_id: "_not-a-store-identity".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(
        response.client_status,
        proto::ClientStatus::NeedRemount as i32
    );
}

#[tokio::test]
async fn create_copy_task_rejects_unregistered_tenant_before_object_lookup() {
    let service = strict_service(false);

    let error = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: "missing".into(),
            targets: vec!["missing-target".into()],
            tenant_id: "unregistered".into(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn tenant_tasks_carry_tenant_in_payload() {
    let service = strict_service(false);
    let source_owner = Uuid::new_v4();
    let target_owner = Uuid::new_v4();
    let writer = Uuid::new_v4();
    let tenant_id = "tenant_for_async_task";
    let key = "tenant_task_key";

    service
        .upsert_tenant_quota_policy(tenant_id, 16 * 1024)
        .unwrap();
    mount_memory(&service, source_owner, "segment_0").await;
    mount_memory(&service, target_owner, "segment_1").await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(writer)),
            key: key.into(),
            slice_length: 1024,
            config: Some(replica_config("segment_0")),
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(writer)),
            key: key.into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap();

    let copy_task_id = MasterService::create_copy_task(
        &service,
        Request::new(proto::CreateCopyTaskRequest {
            key: key.into(),
            targets: vec!["segment_1".into()],
            tenant_id: tenant_id.into(),
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
            key: key.into(),
            source: "segment_0".into(),
            target: "segment_1".into(),
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .task_id
    .unwrap();

    let assignments = MasterService::fetch_tasks(
        &service,
        Request::new(proto::FetchTasksRequest {
            client_id: Some(proto_uuid(source_owner)),
            batch_size: 16,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .tasks;
    assert_eq!(assignments.len(), 2);

    let copy_task_id = Uuid::from_u64_pair(copy_task_id.high, copy_task_id.low);
    let move_task_id = Uuid::from_u64_pair(move_task_id.high, move_task_id.low);
    let mut saw_copy = false;
    let mut saw_move = false;
    for assignment in assignments {
        let assignment_id = assignment.id.as_ref().unwrap();
        let assignment_id = Uuid::from_u64_pair(assignment_id.high, assignment_id.low);
        let payload: serde_json::Value = serde_json::from_str(&assignment.payload).unwrap();
        if assignment_id == copy_task_id {
            assert_eq!(payload["tenant_id"], tenant_id);
            assert_eq!(payload["key"], key);
            saw_copy = true;
        } else if assignment_id == move_task_id {
            assert_eq!(payload["tenant_id"], tenant_id);
            assert_eq!(payload["key"], key);
            saw_move = true;
        }
    }
    assert!(saw_copy);
    assert!(saw_move);
}

// MultiTenantModeRejectsUnregisteredOffloadSuccess: an unsolicited
// NotifyOffloadSuccess for an unregistered tenant fails with
// TENANT_NOT_REGISTERED (ResourceExhausted) even when no Rust-only LocalDisk
// session is mounted, and no object is created.
#[tokio::test]
async fn cpp_parity_single_unregistered_unsolicited_offload_without_disk_session_does_not_create_object()
 {
    let service = strict_service(false);
    service
        .upsert_tenant_quota_policy("tenant-a", 1000)
        .expect("tenant policy");
    let client_id = Uuid::new_v4();
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: "unregistered-offload:1".into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let error = MasterService::notify_offload_success(
        &service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["ghost".into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: 5,
                data_size: 128,
                transport_endpoint: "disk-endpoint".into(),
            }],
            tasks: vec![],
            recovery_session_id: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);

    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "ghost".into(),
            tenant_id: "tenant-b".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .exists;
    assert!(!exists);
}

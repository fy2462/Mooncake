mod common;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use common::proto_uuid;
use mooncake_store_master::oplog::{InMemoryOpLog, OpLogManager};
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::{Code, Request};
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
        group_ids: vec![],
    }
}

fn service_with_oplog(config: MasterRuntimeConfig) -> MasterServiceImpl {
    MasterServiceImpl::new_with_runtime_config_and_oplog(
        None,
        None,
        config,
        Some(OpLogManager::new(
            Some(Box::new(InMemoryOpLog::new(1000))),
            0,
        )),
    )
}

fn decode_oplog_payload(payload: &str) -> serde_json::Value {
    if let Ok(value) = serde_json::from_str(payload) {
        return value;
    }
    let encoded = payload
        .strip_prefix("msgpack:")
        .expect("expected legacy JSON or msgpack oplog payload");
    let bytes = BASE64_STANDARD.decode(encoded).unwrap();
    rmp_serde::from_slice(&bytes).unwrap()
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

async fn mount_local_disk_and_notify_success(
    service: &MasterServiceImpl,
    holder_id: Uuid,
    key: &str,
) {
    MasterService::mount_local_disk_segment(
        service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(holder_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap();
    MasterService::notify_offload_success(
        service,
        Request::new(proto::NotifyOffloadSuccessRequest {
            client_id: Some(proto_uuid(holder_id)),
            keys: vec![key.into()],
            metadatas: vec![proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: 128,
                transport_endpoint: "holder".into(),
            }],
            tasks: vec![],
        }),
    )
    .await
    .unwrap();
}

async fn put_complete(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    tenant_id: &str,
    segment: &str,
) {
    put_complete_with_config(
        service,
        client_id,
        key,
        tenant_id,
        proto::ReplicateConfig {
            preferred_segment: segment.into(),
            ..replicate_config()
        },
    )
    .await;
}

async fn put_complete_with_config(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    tenant_id: &str,
    config: proto::ReplicateConfig,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
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
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_get_all_keys_ignores_tenant_when_quota_is_disabled() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "keys-default:1", 4096).await;

    put_complete(&service, client_id, "default-key", "", "keys-default:1").await;
    put_complete(
        &service,
        client_id,
        "tenant-key",
        "tenant-a",
        "keys-default:1",
    )
    .await;

    let mut keys = MasterService::get_all_keys(
        &service,
        Request::new(proto::GetAllKeysRequest {
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .keys;
    keys.sort();
    assert_eq!(keys, vec!["default-key", "tenant-key"]);
}

#[tokio::test]
async fn test_remove_by_regex_records_remove_oplog() {
    let service = service_with_oplog(MasterRuntimeConfig::default());
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "regex-oplog:1", 4096).await;
    put_complete(&service, client_id, "regex-a", "", "regex-oplog:1").await;

    let removed = MasterService::remove_by_regex(
        &service,
        Request::new(proto::RemoveByRegexRequest {
            pattern: "regex-.*".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(removed.removed_count, 1);

    let guard = service.oplog_manager().lock();
    let records = guard.store().unwrap().read_since(1, 10).unwrap();
    let payloads = records
        .iter()
        .map(|record| record.payload.clone())
        .collect::<Vec<_>>();
    assert!(
        records.iter().any(|record| {
            let payload = decode_oplog_payload(&record.payload);
            payload["op"] == "remove"
                && payload["key"]
                    .as_str()
                    .is_some_and(|key| key.ends_with("regex-a"))
        }),
        "payloads: {payloads:?}"
    );
}

#[tokio::test]
async fn test_mount_local_disk_segment_requires_global_offload() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();

    let err = MasterService::mount_local_disk_segment(
        &service,
        Request::new(proto::MountLocalDiskSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            enable_offloading: true,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn test_promotion_queue_respects_memory_high_watermark() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        eviction_high_watermark_ratio: 0.0,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    let dram_id = Uuid::new_v4();
    mount_memory_segment(&service, dram_id, "promo-watermark:1", 4096).await;
    mount_local_disk_and_notify_success(&service, holder_id, "disk-hot").await;

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "disk-hot".into(),
            tenant_id: String::new(),
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
    assert!(heartbeat.objects.is_empty());
}

#[tokio::test]
async fn test_admin_batch_get_replica_list_does_not_trigger_promotion() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        promotion_admission_threshold: 1,
        promotion_max_per_heartbeat: 2,
        ..Default::default()
    });
    let holder_id = Uuid::new_v4();
    mount_local_disk_and_notify_success(&service, holder_id, "admin-disk-hot").await;

    let admin_results =
        service.batch_get_replica_list_for_admin(&["admin-disk-hot".to_string()], "");
    assert_eq!(admin_results.len(), 1);
    assert_eq!(admin_results[0].status, 0);
    assert_eq!(
        admin_results[0].response.as_ref().unwrap().replicas[0].replica_type,
        proto::replica_descriptor::ReplicaType::LocalDisk as i32
    );

    let admin_heartbeat = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(admin_heartbeat.tasks.is_empty());
    assert!(admin_heartbeat.objects.is_empty());

    MasterService::batch_get_replica_list(
        &service,
        Request::new(proto::BatchGetReplicaListRequest {
            keys: vec!["admin-disk-hot".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let client_heartbeat = MasterService::promotion_object_heartbeat(
        &service,
        Request::new(proto::PromotionObjectHeartbeatRequest {
            client_id: Some(proto_uuid(holder_id)),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(client_heartbeat.tasks.len(), 1);
    assert_eq!(client_heartbeat.tasks[0].key, "admin-disk-hot");
}

#[tokio::test]
async fn test_batch_put_start_returns_per_key_results() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-results:1", 4096).await;

    let response = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["batch-a".into(), "batch-zero".into()],
            slice_lengths: vec![128, 0],
            config: Some(proto::ReplicateConfig {
                preferred_segment: "batch-results:1".into(),
                ..replicate_config()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].key, "batch-a");
    assert_eq!(response.results[0].status, 0);
    assert_eq!(response.results[0].replicas.len(), 1);
    assert_eq!(response.results[1].key, "batch-zero");
    assert!(response.results[1].status < 0);
}

#[tokio::test]
async fn test_group_lookup_grants_lease_to_all_group_members() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-lease:1", 4096).await;

    for key in ["group-a", "group-b"] {
        put_complete_with_config(
            &service,
            client_id,
            key,
            "",
            proto::ReplicateConfig {
                preferred_segment: "group-lease:1".into(),
                group_ids: vec!["shared-group".into()],
                ..replicate_config()
            },
        )
        .await;
    }

    MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "group-a".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let remove_other_group_member = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "group-b".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(remove_other_group_member.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn test_batch_exist_grants_lease_to_all_group_members() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-exist-group:1", 4096).await;

    for key in ["batch-exist-group-a", "batch-exist-group-b"] {
        put_complete_with_config(
            &service,
            client_id,
            key,
            "",
            proto::ReplicateConfig {
                preferred_segment: "batch-exist-group:1".into(),
                group_ids: vec!["batch-exist-shared".into()],
                ..replicate_config()
            },
        )
        .await;
    }

    let exist = MasterService::batch_exist_key(
        &service,
        Request::new(proto::BatchExistKeyRequest {
            keys: vec!["batch-exist-group-a".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(exist.results, vec![true]);

    let remove_other_group_member = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "batch-exist-group-b".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(remove_other_group_member.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn test_upsert_group_membership_is_immutable_when_explicit() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-upsert:1", 4096).await;

    put_complete_with_config(
        &service,
        client_id,
        "group-upsert-key",
        "",
        proto::ReplicateConfig {
            preferred_segment: "group-upsert:1".into(),
            group_ids: vec!["group-one".into()],
            ..replicate_config()
        },
    )
    .await;

    MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "group-upsert-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "group-upsert:1".into(),
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();

    let changed_group = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "group-upsert-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "group-upsert:1".into(),
                group_ids: vec!["group-two".into()],
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(changed_group.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn test_batch_put_start_rejects_mismatched_group_ids_per_key() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-group:1", 4096).await;

    let response = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["batch-group-a".into(), "batch-group-b".into()],
            slice_lengths: vec![128, 128],
            config: Some(proto::ReplicateConfig {
                preferred_segment: "batch-group:1".into(),
                group_ids: vec!["only-one-group".into()],
                ..replicate_config()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].key, "batch-group-a");
    assert_eq!(response.results[1].key, "batch-group-b");
    assert!(response.results.iter().all(|result| result.status < 0));
    assert!(
        response
            .results
            .iter()
            .all(|result| result.replicas.is_empty())
    );
}

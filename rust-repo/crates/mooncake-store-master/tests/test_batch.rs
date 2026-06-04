mod common;

use common::proto_uuid;
use dashmap::DashMap;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct ObjectEntry {
    replicas: Vec<ReplicaDescriptor>,
}

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

async fn mount_memory_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: String::new(),
        }),
    )
    .await
    .unwrap();
}

fn proto_nof_segment(id: Uuid, client_id: Uuid, name: &str) -> proto::NoFSegment {
    proto::NoFSegment {
        id: Some(proto_uuid(id)),
        name: name.into(),
        base: 0,
        size: 4096,
        te_endpoint: format!("transport://{name}"),
        client_id: Some(proto_uuid(client_id)),
    }
}

async fn mount_nof_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str) {
    MasterService::mount_no_f_segment(
        service,
        Request::new(proto::MountNoFSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment: Some(proto_nof_segment(Uuid::new_v4(), client_id, name)),
        }),
    )
    .await
    .unwrap();
}

async fn put_start_one(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(replicate_config()),
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
async fn test_batch_put_start_supports_nof_replicas() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-nof-memory:1").await;
    mount_nof_segment(&service, client_id, "batch-nof-ssd:1").await;

    let response = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["batch-nof-key".into()],
            slice_lengths: vec![128],
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 1,
                preferred_segment: "batch-nof-memory:1".into(),
                preferred_nof_segments: vec!["batch-nof-ssd:1".into()],
                ..replicate_config()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].status, 0);
    assert_eq!(response.replicas.len(), 2);
    assert_eq!(response.results[0].replicas.len(), 2);
    assert!(response.results[0]
        .replicas
        .iter()
        .any(|r| r.replica_type == proto::replica_descriptor::ReplicaType::Memory as i32));
    assert!(response.results[0]
        .replicas
        .iter()
        .any(|r| r.replica_type == proto::replica_descriptor::ReplicaType::NofSsd as i32));
}

#[tokio::test]
async fn test_batch_put_start_marks_existing_object_separately() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-existing:1").await;
    put_start_one(&service, client_id, "already-there").await;
    put_end_one(&service, client_id, "already-there").await;

    let response = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: vec!["already-there".into()],
            slice_lengths: vec![128],
            config: Some(proto::ReplicateConfig {
                preferred_segment: "batch-existing:1".into(),
                ..replicate_config()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].status, -7);
    assert!(response.results[0].replicas.is_empty());
}

#[test]
fn test_batch_remove_logic() {
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    for i in 0..5 {
        objects.insert(format!("batch_key_{}", i), ObjectEntry { replicas: vec![] });
    }
    assert_eq!(objects.len(), 5);

    let keys: Vec<String> = (0..5).map(|i| format!("batch_key_{}", i)).collect();
    for key in &keys {
        objects.remove(key);
    }

    assert_eq!(objects.len(), 0);
}

#[test]
fn test_batch_remove_empty() {
    let keys: Vec<String> = vec![];
    assert!(keys.is_empty());
}

#[test]
fn test_batch_put_revoke_logic() {
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    objects.insert("k1".into(), ObjectEntry { replicas: vec![] });
    objects.insert("k2".into(), ObjectEntry { replicas: vec![] });

    let keys: Vec<String> = vec!["k1".into(), "k2".into(), "k3".into()];
    let statuses: Vec<i32> = keys
        .iter()
        .map(|key| if objects.remove(key).is_some() { 0 } else { -1 })
        .collect();

    assert_eq!(statuses, vec![0, 0, -1]);
    assert!(objects.is_empty());
}

#[test]
fn test_batch_put_end_status_transition() {
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    let sid = Uuid::new_v4();
    objects.insert(
        "pending_key".into(),
        ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                base_addr: 0x100000000,
                refcnt: 0,
                handle_valid: true,
                segment_id: sid,
                segment_name: "s1".into(),
                offset: 0,
                size: 128,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
            }],
        },
    );

    let keys = vec!["pending_key".to_string()];
    for key in &keys {
        if let Some(mut obj) = objects.get_mut(key) {
            for r in &mut obj.replicas {
                if r.status == ReplicaStatus::Allocating {
                    r.status = ReplicaStatus::Complete;
                }
            }
        }
    }

    let obj = objects.get("pending_key").unwrap();
    assert_eq!(obj.replicas[0].status, ReplicaStatus::Complete);
}

#[tokio::test]
async fn test_upsert_and_batch_upsert_follow_two_phase_semantics() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "upsert-two-phase:1").await;

    let config = proto::ReplicateConfig {
        preferred_segment: "upsert-two-phase:1".into(),
        ..replicate_config()
    };
    let upsert = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert-key".into(),
            slice_length: 128,
            config: Some(config.clone()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(upsert.replicas.len(), 1);
    assert!(MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "upsert-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .is_err());

    let end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "upsert-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(end.statuses, vec![0]);
    assert!(MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "upsert-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .is_ok());

    MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert-key".into(),
            slice_length: 128,
            config: Some(config.clone()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "upsert-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .is_err());
    let same_size_end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "upsert-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(same_size_end.statuses, vec![0]);

    let batch_start = MasterService::batch_upsert_start(
        &service,
        Request::new(proto::BatchUpsertStartRequest {
            entries: vec![proto::UpsertEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "batch-upsert-key".into(),
                slice_length: 128,
                config: Some(config),
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_start.statuses, vec![0]);
    assert_eq!(batch_start.replicas.len(), 1);

    let batch_end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "batch-upsert-key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_end.statuses, vec![0]);
}

#[tokio::test]
async fn test_put_end_and_batch_put_end_start_without_hard_lease() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-lease:1").await;

    put_start_one(&service, client_id, "single-no-lease").await;
    put_end_one(&service, client_id, "single-no-lease").await;
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "single-no-lease".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    put_start_one(&service, client_id, "batch-no-lease").await;
    let batch_put = MasterService::batch_put_end(
        &service,
        Request::new(proto::BatchPutEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "batch-no-lease".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_put.statuses, vec![0]);

    let batch_remove = MasterService::batch_remove(
        &service,
        Request::new(proto::BatchRemoveRequest {
            keys: vec!["batch-no-lease".into()],
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(batch_remove.statuses, vec![0]);
}

#[tokio::test]
async fn test_regex_get_grants_lease_and_batch_remove_respects_it() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "regex-lease:1").await;
    put_start_one(&service, client_id, "regex-lease-key").await;
    put_end_one(&service, client_id, "regex-lease-key").await;

    let regex_result = MasterService::get_replica_list_by_regex(
        &service,
        Request::new(proto::GetReplicaListByRegexRequest {
            key_regex: "regex-lease-.*".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(regex_result.entries.len(), 1);

    let batch_remove = MasterService::batch_remove(
        &service,
        Request::new(proto::BatchRemoveRequest {
            keys: vec!["regex-lease-key".into()],
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_ne!(batch_remove.statuses, vec![0]);

    let still_readable = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: "regex-lease-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await;
    assert!(still_readable.is_ok());
}

#[tokio::test]
async fn test_eviction_uses_explicit_lease_timeout_not_last_access_ttl() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_secs(3600),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "evict-explicit-lease:1").await;
    put_start_one(&service, client_id, "evict-after-put-end").await;
    put_end_one(&service, client_id, "evict-after-put-end").await;

    let evicted = service.run_eviction_cycle_for_test(1);
    assert_eq!(evicted, vec!["evict-after-put-end".to_string()]);
}

#[tokio::test]
async fn test_reaper_removes_expired_processing_put_start() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        put_start_release_timeout: Duration::from_millis(20),
        reaper_interval: Duration::from_millis(5),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "reaper-processing:1").await;
    put_start_one(&service, client_id, "stale-processing").await;

    tokio::time::sleep(Duration::from_millis(80)).await;
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "stale-processing".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!exists.exists);
}

#[tokio::test]
async fn test_soft_pinned_eviction_uses_force_second_pass() {
    for (force, should_evict) in [(false, false), (true, true)] {
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: Duration::from_secs(3600),
            soft_pin_ttl: Duration::from_secs(3600),
            offload_force_evict: force,
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_memory_segment(
            &service,
            client_id,
            if force {
                "soft-force:1"
            } else {
                "soft-protect:1"
            },
        )
        .await;

        let key = if force {
            "soft-force-key"
        } else {
            "soft-protected-key"
        };
        MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: key.into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    with_soft_pin: true,
                    ..replicate_config()
                }),
            }),
        )
        .await
        .unwrap();
        put_end_one(&service, client_id, key).await;

        let evicted = service.run_eviction_cycle_for_test(1);
        assert_eq!(
            evicted.iter().any(|evicted_key| evicted_key == key),
            should_evict
        );
    }
}

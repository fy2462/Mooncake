mod common;

use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use tonic::{Code, Request};
use uuid::Uuid;

static NEXT_SEGMENT_BASE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0x6_0000_0000);

fn next_segment_base() -> u64 {
    NEXT_SEGMENT_BASE.fetch_add(0x10000, std::sync::atomic::Ordering::Relaxed)
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
        host_id: String::new(),
    }
}

async fn mount_memory_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 4096,
            base_addr: next_segment_base(),
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
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

async fn lookup(service: &MasterServiceImpl, key: &str, tenant_id: &str) {
    MasterService::get_replica_list(
        service,
        Request::new(proto::GetReplicaListRequest {
            key: key.into(),
            tenant_id: tenant_id.into(),
        }),
    )
    .await
    .unwrap();
}

// PutStartGroupIdsValidation: a two-ID group_ids config is INVALID_PARAMS and a
// single empty-string config completes an existing object. (The C++ explicit
// empty-vector case is unrepresentable in proto3, where the absent repeated
// field and an empty list are identical; Rust treats empty as the ungrouped
// default like the C++ absent-optional case.)
#[tokio::test]
async fn put_start_group_id_shape_validation_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-shape:1").await;

    let too_many = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "too_many_group_ids".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "group-shape:1".into(),
                group_ids: vec!["g0".into(), "g1".into()],
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(too_many.code(), Code::InvalidArgument);

    put_complete_with_config(
        &service,
        client_id,
        "explicit_ungrouped",
        "",
        proto::ReplicateConfig {
            preferred_segment: "group-shape:1".into(),
            group_ids: vec![String::new()],
            ..replicate_config()
        },
    )
    .await;
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "explicit_ungrouped".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(exists.exists);
}

// GroupedObjectRoutesKeyLevelLookupAndRemove: a grouped object is found by
// key-level lookup and force removal removes it.
#[tokio::test]
async fn grouped_object_key_lookup_and_forced_remove_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-route:1").await;
    put_complete_with_config(
        &service,
        client_id,
        "grouped_route_key",
        "",
        proto::ReplicateConfig {
            preferred_segment: "group-route:1".into(),
            group_ids: vec!["route-group".into()],
            ..replicate_config()
        },
    )
    .await;

    lookup(&service, "grouped_route_key", "").await;
    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "grouped_route_key".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "grouped_route_key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!exists.exists);
}

// GroupRoutingIsTenantScopedForSameUserKey: the same user key in two tenants
// routes to distinct groups and removing tenant A leaves tenant B intact.
#[tokio::test]
async fn group_routes_are_tenant_scoped_parity() {
    let policy = tempfile::NamedTempFile::new().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: policy.path().to_string_lossy().into_owned(),
        ..Default::default()
    });
    service
        .upsert_tenant_quota_policy("tenant-a", 1024)
        .unwrap();
    service
        .upsert_tenant_quota_policy("tenant-b", 2048)
        .unwrap();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-tenant:1").await;
    put_complete_with_config(
        &service,
        client_id,
        "shared_hot_key",
        "tenant-a",
        proto::ReplicateConfig {
            preferred_segment: "group-tenant:1".into(),
            group_ids: vec!["group-a".into()],
            ..replicate_config()
        },
    )
    .await;
    put_complete_with_config(
        &service,
        client_id,
        "shared_hot_key",
        "tenant-b",
        proto::ReplicateConfig {
            preferred_segment: "group-tenant:1".into(),
            group_ids: vec!["group-b".into()],
            ..replicate_config()
        },
    )
    .await;

    lookup(&service, "shared_hot_key", "tenant-a").await;
    lookup(&service, "shared_hot_key", "tenant-b").await;

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "shared_hot_key".into(),
            force: true,
            tenant_id: "tenant-a".into(),
        }),
    )
    .await
    .unwrap();
    lookup(&service, "shared_hot_key", "tenant-b").await;
}

// BatchRemoveUnregistersGroupedRoute: forced batch removal clears the group
// route so an ungrouped replacement under the same key is readable.
#[tokio::test]
async fn batch_remove_unregisters_group_route_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-batch-remove:1").await;
    put_complete_with_config(
        &service,
        client_id,
        "grouped-batch-key",
        "",
        proto::ReplicateConfig {
            preferred_segment: "group-batch-remove:1".into(),
            group_ids: vec!["batch-remove-group".into()],
            ..replicate_config()
        },
    )
    .await;

    let response = MasterService::batch_remove(
        &service,
        Request::new(proto::BatchRemoveRequest {
            keys: vec!["grouped-batch-key".into()],
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.statuses.len(), 1);
    assert_eq!(response.statuses[0], 0);

    put_complete_with_config(
        &service,
        client_id,
        "grouped-batch-key",
        "",
        replicate_config(),
    )
    .await;
    lookup(&service, "grouped-batch-key", "").await;
}

// RemoveByRegexUnregistersGroupedRoute: an anchored regex removal of a grouped
// object frees the key for an ungrouped replacement.
#[tokio::test]
async fn regex_remove_unregisters_group_route_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-regex:1").await;
    put_complete_with_config(
        &service,
        client_id,
        "regex_grouped_key",
        "",
        proto::ReplicateConfig {
            preferred_segment: "group-regex:1".into(),
            group_ids: vec!["regex-group".into()],
            ..replicate_config()
        },
    )
    .await;

    let response = MasterService::remove_by_regex(
        &service,
        Request::new(proto::RemoveByRegexRequest {
            pattern: "^regex_grouped_key$".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.removed_count, 1);

    put_complete_with_config(
        &service,
        client_id,
        "regex_grouped_key",
        "",
        replicate_config(),
    )
    .await;
    lookup(&service, "regex_grouped_key", "").await;
}

// RemoveGroupedMemberPreservesOtherMembers: removing one member of a group
// leaves the peer's route intact, and removing the last member empties the
// group.
#[tokio::test]
async fn remove_group_member_preserves_peers_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-members:1").await;
    for key in ["group-member-a", "group-member-b"] {
        put_complete_with_config(
            &service,
            client_id,
            key,
            "",
            proto::ReplicateConfig {
                preferred_segment: "group-members:1".into(),
                group_ids: vec!["members-group".into()],
                ..replicate_config()
            },
        )
        .await;
    }

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "group-member-a".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    lookup(&service, "group-member-b", "").await;

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "group-member-b".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "group-member-b".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!exists.exists);
}

// UpsertPreservesGroupMembership: an unset-group upsert preserves the group,
// while a different group and explicit ungrouping are both INVALID_PARAMS.
#[tokio::test]
async fn upsert_preserves_and_rejects_group_membership_changes_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-upsert:1").await;
    put_complete_with_config(
        &service,
        client_id,
        "upsert_group_key",
        "",
        proto::ReplicateConfig {
            preferred_segment: "group-upsert:1".into(),
            group_ids: vec!["upsert-group".into()],
            ..replicate_config()
        },
    )
    .await;

    MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert_group_key".into(),
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
    MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "upsert_group_key".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap();
    lookup(&service, "upsert_group_key", "").await;

    for group_ids in [vec!["other-group".to_string()], vec![String::new()]] {
        let error = MasterService::upsert(
            &service,
            Request::new(proto::UpsertRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "upsert_group_key".into(),
                slice_length: 128,
                tenant_id: String::new(),
                config: Some(proto::ReplicateConfig {
                    preferred_segment: "group-upsert:1".into(),
                    group_ids,
                    ..replicate_config()
                }),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
}

// UpsertRejectsExistingUngroupedToGrouped: grouping an existing ungrouped
// object is INVALID_PARAMS and the original stays readable.
#[tokio::test]
async fn upsert_rejects_existing_ungrouped_to_grouped_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "group-ungrouped:1").await;
    put_complete_with_config(
        &service,
        client_id,
        "upsert_ungrouped_to_grouped",
        "",
        proto::ReplicateConfig {
            preferred_segment: "group-ungrouped:1".into(),
            ..replicate_config()
        },
    )
    .await;

    let error = MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "upsert_ungrouped_to_grouped".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "group-ungrouped:1".into(),
                group_ids: vec!["different-shard-group".into()],
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    lookup(&service, "upsert_ungrouped_to_grouped", "").await;
}

// BatchExistKeyGroupedAndIncompletePreservesOrder: a grouped ready member plus
// ungrouped ready/processing/missing states produce the exact boolean vector
// in input order.
#[tokio::test]
async fn batch_exist_grouped_ready_incomplete_missing_order_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-exist-group:1").await;

    for key in ["batch_grouped_key_a", "batch_grouped_key_b"] {
        put_complete_with_config(
            &service,
            client_id,
            key,
            "",
            proto::ReplicateConfig {
                preferred_segment: "batch-exist-group:1".into(),
                group_ids: vec!["exist-group".into()],
                ..replicate_config()
            },
        )
        .await;
    }
    put_complete_with_config(
        &service,
        client_id,
        "batch_completed_key",
        "",
        replicate_config(),
    )
    .await;
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch_incomplete_key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "batch-exist-group:1".into(),
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();

    let exist = MasterService::batch_exist_key(
        &service,
        Request::new(proto::BatchExistKeyRequest {
            keys: vec![
                "batch_grouped_key_a".into(),
                "batch_completed_key".into(),
                "batch_incomplete_key".into(),
                "batch_missing_key".into(),
                "batch_grouped_key_b".into(),
            ],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(exist.results, vec![true, true, false, false, true]);
}

// BatchGetReplicaListPreservesOrderWithGroupedKeys: the five exact states
// return in input order with nonempty descriptors, OBJECT_NOT_FOUND at index
// 1, and REPLICA_IS_NOT_READY at index 4.
#[tokio::test]
async fn batch_get_grouped_mixed_results_preserve_order_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-get-group:1").await;

    put_complete_with_config(
        &service,
        client_id,
        "batch_get_grouped_a",
        "",
        proto::ReplicateConfig {
            preferred_segment: "batch-get-group:1".into(),
            group_ids: vec!["get-group-a".into()],
            ..replicate_config()
        },
    )
    .await;
    put_complete_with_config(
        &service,
        client_id,
        "batch_get_ungrouped",
        "",
        replicate_config(),
    )
    .await;
    put_complete_with_config(
        &service,
        client_id,
        "batch_get_grouped_b",
        "",
        proto::ReplicateConfig {
            preferred_segment: "batch-get-group:1".into(),
            group_ids: vec!["get-group-b".into()],
            ..replicate_config()
        },
    )
    .await;
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "batch_get_pending".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "batch-get-group:1".into(),
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();

    let results = MasterService::batch_get_replica_list(
        &service,
        Request::new(proto::BatchGetReplicaListRequest {
            keys: vec![
                "batch_get_grouped_a".into(),
                "batch_get_missing".into(),
                "batch_get_ungrouped".into(),
                "batch_get_grouped_b".into(),
                "batch_get_pending".into(),
            ],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .results;
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].status, 0);
    assert!(results[0].response.as_ref().unwrap().replicas.len() >= 1);
    assert_eq!(results[1].status, -1); // OBJECT_NOT_FOUND
    assert_eq!(results[2].status, 0);
    assert!(results[2].response.as_ref().unwrap().replicas.len() >= 1);
    assert_eq!(results[3].status, 0);
    assert!(results[3].response.as_ref().unwrap().replicas.len() >= 1);
    assert_eq!(results[4].status, -5); // REPLICA_IS_NOT_READY
}

// ExpiredGroupedPutCanBeReplacedByUngroupedPut: an incomplete grouped put can
// be replaced by an ungrouped put once the discard deadline passes.
#[tokio::test]
async fn expired_grouped_put_can_be_replaced_ungrouped_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        put_start_discard_timeout: std::time::Duration::ZERO,
        put_start_release_timeout: std::time::Duration::from_secs(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "expired-group-put:1").await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "expired_grouped_put_to_ungrouped".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "expired-group-put:1".into(),
                group_ids: vec!["expired-group".into()],
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();

    put_complete_with_config(
        &service,
        client_id,
        "expired_grouped_put_to_ungrouped",
        "",
        replicate_config(),
    )
    .await;
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "expired_grouped_put_to_ungrouped".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(exists.exists);
}

// IncompleteGroupedUpsertCanBecomeUngrouped: a started-but-unfinished grouped
// put can be completed by an ungrouped two-phase upsert.
#[tokio::test]
async fn incomplete_grouped_put_can_be_upserted_ungrouped_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        put_start_discard_timeout: std::time::Duration::ZERO,
        put_start_release_timeout: std::time::Duration::from_secs(1),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "incomplete-group-upsert:1").await;

    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "incomplete_grouped_upsert_to_ungrouped".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "incomplete-group-upsert:1".into(),
                group_ids: vec!["incomplete-group".into()],
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();

    MasterService::upsert(
        &service,
        Request::new(proto::UpsertRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "incomplete_grouped_upsert_to_ungrouped".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "incomplete-group-upsert:1".into(),
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: vec![proto::PutEndEntry {
                client_id: Some(proto_uuid(client_id)),
                key: "incomplete_grouped_upsert_to_ungrouped".into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }],
        }),
    )
    .await
    .unwrap();
    let exists = MasterService::exist_key(
        &service,
        Request::new(proto::ExistKeyRequest {
            key: "incomplete_grouped_upsert_to_ungrouped".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(exists.exists);
}

// BatchUpsertStartMixedGroupIdsPreservesOrder: three starts, three ends, and
// three lookups succeed in order, and a one-ID mismatch makes every position
// INVALID_PARAMS.
#[tokio::test]
async fn batch_upsert_mixed_group_ids_preserves_order_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "batch-upsert-group:1").await;
    let keys = ["batch_grouped_a", "batch_ungrouped", "batch_grouped_b"];

    let start = MasterService::batch_upsert_start(
        &service,
        Request::new(proto::BatchUpsertStartRequest {
            entries: keys
                .iter()
                .map(|key| proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: (*key).into(),
                    slice_length: 128,
                    config: Some(proto::ReplicateConfig {
                        preferred_segment: "batch-upsert-group:1".into(),
                        group_ids: vec![match *key {
                            "batch_ungrouped" => String::new(),
                            _ => format!("batch-group-{key}"),
                        }],
                        ..replicate_config()
                    }),
                    tenant_id: String::new(),
                })
                .collect(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(start.statuses.len(), 3);
    assert!(start.statuses.iter().all(|status| *status == 0));

    let end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: keys
                .iter()
                .map(|key| proto::PutEndEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: (*key).into(),
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                })
                .collect(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(end.statuses.len(), 3);
    assert!(end.statuses.iter().all(|status| *status == 0));
    for key in keys {
        lookup(&service, key, "").await;
    }

    let invalid = MasterService::batch_upsert_start(
        &service,
        Request::new(proto::BatchUpsertStartRequest {
            entries: keys
                .iter()
                .map(|key| proto::UpsertEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: (*key).into(),
                    slice_length: 128,
                    config: Some(proto::ReplicateConfig {
                        preferred_segment: "batch-upsert-group:1".into(),
                        group_ids: vec!["only_one".into(), "extra".into()],
                        ..replicate_config()
                    }),
                    tenant_id: String::new(),
                })
                .collect(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(invalid.statuses.len(), 3);
    assert!(invalid.statuses.iter().all(|status| *status == -6));
}

// WrappedBatchPutStartMixedGroupIdsPreservesOrder: three mixed-group starts
// succeed in order, and group-count and size-count mismatches both produce an
// invalid per-key result for every position.
#[tokio::test]
async fn wrapped_batch_put_mixed_groups_and_shape_errors_parity() {
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "wrapped-batch-group:1").await;
    let keys = [
        "wrapped_batch_grouped_a",
        "wrapped_batch_ungrouped",
        "wrapped_batch_grouped_b",
    ];
    let sizes = vec![128, 256, 512];

    let mixed_config = proto::ReplicateConfig {
        preferred_segment: "wrapped-batch-group:1".into(),
        group_ids: vec![
            "wrapped-group-a".into(),
            String::new(),
            "wrapped-group-b".into(),
        ],
        ..replicate_config()
    };
    let valid = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: keys.iter().map(|key| (*key).into()).collect(),
            slice_lengths: sizes.clone(),
            config: Some(mixed_config.clone()),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(valid.results.len(), 3);
    assert!(valid.results.iter().all(|result| result.status == 0));

    let end = MasterService::batch_upsert_end(
        &service,
        Request::new(proto::BatchUpsertEndRequest {
            entries: keys
                .iter()
                .map(|key| proto::PutEndEntry {
                    client_id: Some(proto_uuid(client_id)),
                    key: (*key).into(),
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                })
                .collect(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(end.statuses, vec![0, 0, 0]);
    for key in keys {
        lookup(&service, key, "").await;
    }

    let invalid_group = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: keys.iter().map(|key| (*key).into()).collect(),
            slice_lengths: sizes.clone(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "wrapped-batch-group:1".into(),
                group_ids: vec!["only_one".into()],
                ..replicate_config()
            }),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(invalid_group.results.len(), 3);
    assert!(
        invalid_group
            .results
            .iter()
            .all(|result| result.status == -6)
    );

    let invalid_size = MasterService::batch_put_start(
        &service,
        Request::new(proto::BatchPutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            keys: keys.iter().map(|key| (*key).into()).collect(),
            slice_lengths: vec![128],
            config: Some(mixed_config),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(invalid_size.results.len(), 3);
    assert!(
        invalid_size
            .results
            .iter()
            .all(|result| result.status == -6)
    );
}

// ConcurrentGroupedAndUngroupedFirstCreateDoesNotDuplicateMetadata: sixteen
// barrier-started alternating grouped/ungrouped PutStarts produce exactly one
// winning start and one winning end, one metadata entry, and a readable final
// object.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_grouped_and_ungrouped_first_create_single_winner_parity() {
    let service = std::sync::Arc::new(MasterServiceImpl::default());
    let client_id = Uuid::new_v4();
    mount_memory_segment(service.as_ref(), client_id, "concurrent-first:1").await;
    let key = "concurrent_grouped_ungrouped_first_create";
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));

    let mut workers = Vec::new();
    for index in 0..16 {
        let service = std::sync::Arc::clone(&service);
        let barrier = std::sync::Arc::clone(&barrier);
        let key = key.to_string();
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            let config = if index % 2 == 0 {
                proto::ReplicateConfig {
                    preferred_segment: "concurrent-first:1".into(),
                    group_ids: vec!["concurrent-group".into()],
                    ..replicate_config()
                }
            } else {
                replicate_config()
            };
            let start = MasterService::put_start(
                service.as_ref(),
                Request::new(proto::PutStartRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key: key.clone(),
                    slice_length: 128,
                    tenant_id: String::new(),
                    config: Some(config),
                }),
            )
            .await;
            if start.is_err() {
                return 0_u32;
            }
            MasterService::put_end(
                service.as_ref(),
                Request::new(proto::PutEndRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key,
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap();
            1
        }));
    }
    let mut winners = 0;
    for worker in workers {
        winners += worker.await.unwrap();
    }
    assert_eq!(winners, 1);
    lookup(
        service.as_ref(),
        "concurrent_grouped_ungrouped_first_create",
        "",
    )
    .await;
}

// ConcurrentDifferentGroupedFirstCreateDoesNotDuplicateMetadata: sixteen
// barrier-started PutStarts over two distinct group IDs yield exactly one
// winner, one metadata entry, and a readable final object.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_distinct_group_first_create_single_winner_parity() {
    let service = std::sync::Arc::new(MasterServiceImpl::default());
    let client_id = Uuid::new_v4();
    mount_memory_segment(service.as_ref(), client_id, "concurrent-groups:1").await;
    let key = "concurrent_different_grouped_first_create";
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));

    let mut workers = Vec::new();
    for index in 0..16 {
        let service = std::sync::Arc::clone(&service);
        let barrier = std::sync::Arc::clone(&barrier);
        let key = key.to_string();
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            let group = if index % 2 == 0 {
                "group-one"
            } else {
                "group-two"
            };
            let start = MasterService::put_start(
                service.as_ref(),
                Request::new(proto::PutStartRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key: key.clone(),
                    slice_length: 128,
                    tenant_id: String::new(),
                    config: Some(proto::ReplicateConfig {
                        preferred_segment: "concurrent-groups:1".into(),
                        group_ids: vec![group.into()],
                        ..replicate_config()
                    }),
                }),
            )
            .await;
            if start.is_err() {
                return 0_u32;
            }
            MasterService::put_end(
                service.as_ref(),
                Request::new(proto::PutEndRequest {
                    client_id: Some(proto_uuid(client_id)),
                    key,
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: String::new(),
                }),
            )
            .await
            .unwrap();
            1
        }));
    }
    let mut winners = 0;
    for worker in workers {
        winners += worker.await.unwrap();
    }
    assert_eq!(winners, 1);
    lookup(
        service.as_ref(),
        "concurrent_different_grouped_first_create",
        "",
    )
    .await;
}

async fn exist_key(service: &MasterServiceImpl, key: &str) -> bool {
    MasterService::exist_key(
        service,
        Request::new(proto::ExistKeyRequest {
            key: key.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .exists
}

// GroupedLeaseRefreshNearExpiryProtectsCurrentMembers: a lookup near expiry
// refreshes every current member so a normal peer removal still reports
// OBJECT_HAS_LEASE, and forced cleanup of both succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn near_expiry_group_lookup_refreshes_all_members_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(200),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "lease-group:1").await;
    for key in ["lease_group_key_a", "lease_group_key_b"] {
        put_complete_with_config(
            &service,
            client_id,
            key,
            "",
            proto::ReplicateConfig {
                preferred_segment: "lease-group:1".into(),
                group_ids: vec!["lease-group".into()],
                ..replicate_config()
            },
        )
        .await;
    }

    assert!(exist_key(&service, "lease_group_key_a").await);
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    assert!(exist_key(&service, "lease_group_key_a").await);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let error = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "lease_group_key_b".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);

    for key in ["lease_group_key_a", "lease_group_key_b"] {
        MasterService::remove(
            &service,
            Request::new(proto::RemoveRequest {
                key: key.into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
}

// GroupedLeaseRefreshAfterMembershipChangeDoesNotWaitForTriggerExpiry: a
// refresh after a peer was added grants the new peer the lease without waiting
// for the original trigger to expire.
#[tokio::test(flavor = "multi_thread")]
async fn group_refresh_after_membership_change_updates_new_peer_parity() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: std::time::Duration::from_millis(500),
        ..Default::default()
    });
    let client_id = Uuid::new_v4();
    mount_memory_segment(&service, client_id, "lease-membership:1").await;
    put_complete_with_config(
        &service,
        client_id,
        "lease_group_dirty_key_a",
        "",
        proto::ReplicateConfig {
            preferred_segment: "lease-membership:1".into(),
            group_ids: vec!["lease-membership".into()],
            ..replicate_config()
        },
    )
    .await;
    assert!(exist_key(&service, "lease_group_dirty_key_a").await);
    put_complete_with_config(
        &service,
        client_id,
        "lease_group_dirty_key_b",
        "",
        proto::ReplicateConfig {
            preferred_segment: "lease-membership:1".into(),
            group_ids: vec!["lease-membership".into()],
            ..replicate_config()
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(exist_key(&service, "lease_group_dirty_key_a").await);
    tokio::time::sleep(std::time::Duration::from_millis(390)).await;

    let error = MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "lease_group_dirty_key_b".into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);

    for key in ["lease_group_dirty_key_a", "lease_group_dirty_key_b"] {
        MasterService::remove(
            &service,
            Request::new(proto::RemoveRequest {
                key: key.into(),
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
}

async fn mount_segment_with_size(
    service: &MasterServiceImpl,
    client_id: Uuid,
    name: &str,
    size: u64,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size,
            base_addr: next_segment_base(),
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_grouped_size(
    service: &MasterServiceImpl,
    client_id: Uuid,
    key: &str,
    group_id: &str,
    size: u64,
) {
    MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: size,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "evict-segment:1".into(),
                group_ids: vec![group_id.into()],
                ..replicate_config()
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

// GroupedEvictionExpandsSafeMembersAndSkipsLeasedGroup: under 4 MiB pressure
// an expired grouped pair is evicted as a unit, while a leased grouped pair is
// protected and stays readable.
#[tokio::test]
async fn pressure_eviction_expands_safe_group_and_skips_leased_group_parity() {
    const SEGMENT_SIZE: u64 = 4 * 1024 * 1024;
    const OBJECT_SIZE: u64 = 2 * 1024 * 1024;

    // Expired group: evicted together under pressure.
    {
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(1000),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_segment_with_size(&service, client_id, "evict-segment:1", SEGMENT_SIZE).await;
        put_grouped_size(
            &service,
            client_id,
            "grouped_evict_key_a",
            "evict-group",
            OBJECT_SIZE,
        )
        .await;
        put_grouped_size(
            &service,
            client_id,
            "grouped_evict_key_b",
            "evict-group",
            OBJECT_SIZE,
        )
        .await;

        let trigger = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "trigger_grouped_eviction".into(),
                slice_length: OBJECT_SIZE,
                tenant_id: String::new(),
                config: Some(replicate_config()),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(trigger.code(), Code::ResourceExhausted);

        let evicted = service.run_eviction_cycle_for_test(1);
        assert!(evicted.iter().any(|key| key == "grouped_evict_key_a"));
        assert!(evicted.iter().any(|key| key == "grouped_evict_key_b"));
        assert!(!exist_key(&service, "grouped_evict_key_a").await);
        assert!(!exist_key(&service, "grouped_evict_key_b").await);
    }

    // Leased group: both members stay readable after pressure eviction.
    {
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            lease_ttl: std::time::Duration::from_millis(1000),
            ..Default::default()
        });
        let client_id = Uuid::new_v4();
        mount_segment_with_size(&service, client_id, "evict-segment:1", SEGMENT_SIZE).await;
        put_grouped_size(
            &service,
            client_id,
            "grouped_leased_key_a",
            "leased-group",
            OBJECT_SIZE,
        )
        .await;
        put_grouped_size(
            &service,
            client_id,
            "grouped_leased_key_b",
            "leased-group",
            OBJECT_SIZE,
        )
        .await;
        assert!(exist_key(&service, "grouped_leased_key_a").await);

        let trigger = MasterService::put_start(
            &service,
            Request::new(proto::PutStartRequest {
                client_id: Some(proto_uuid(client_id)),
                key: "trigger_leased_group_eviction".into(),
                slice_length: OBJECT_SIZE,
                tenant_id: String::new(),
                config: Some(replicate_config()),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(trigger.code(), Code::ResourceExhausted);

        let evicted = service.run_eviction_cycle_for_test(1);
        assert!(evicted.is_empty());
        lookup(&service, "grouped_leased_key_a", "").await;
        lookup(&service, "grouped_leased_key_b", "").await;
    }
}

// GroupedEvictionSkipsUnsafeMembersAndEvictsSafePeers: under pressure the safe
// member is evicted while the hard-pinned member stays readable, and the
// survivor is force-removable.
#[tokio::test]
async fn pressure_eviction_skips_hard_pinned_member_parity() {
    const SEGMENT_SIZE: u64 = 4 * 1024 * 1024;
    const OBJECT_SIZE: u64 = 2 * 1024 * 1024;
    let service = MasterServiceImpl::default();
    let client_id = Uuid::new_v4();
    mount_segment_with_size(&service, client_id, "evict-segment:1", SEGMENT_SIZE).await;
    put_grouped_size(
        &service,
        client_id,
        "grouped_mixed_safe_key",
        "mixed-group",
        OBJECT_SIZE,
    )
    .await;
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "grouped_mixed_hard_key".into(),
            slice_length: OBJECT_SIZE,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                preferred_segment: "evict-segment:1".into(),
                group_ids: vec!["mixed-group".into()],
                with_hard_pin: true,
                ..replicate_config()
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "grouped_mixed_hard_key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    let trigger = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "trigger_mixed_group_eviction".into(),
            slice_length: OBJECT_SIZE,
            tenant_id: String::new(),
            config: Some(replicate_config()),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(trigger.code(), Code::ResourceExhausted);

    let evicted = service.run_eviction_cycle_for_test(1);
    assert!(evicted.iter().any(|key| key == "grouped_mixed_safe_key"));
    assert!(exist_key(&service, "grouped_mixed_hard_key").await);

    MasterService::remove(
        &service,
        Request::new(proto::RemoveRequest {
            key: "grouped_mixed_hard_key".into(),
            force: true,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

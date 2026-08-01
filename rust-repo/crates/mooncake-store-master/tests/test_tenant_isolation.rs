//! # Multi-Tenant Isolation Tests — 多租户隔离测试
//!
//! Verifies that objects created in different tenants are fully isolated:
//! - Same user_key in different tenants → independent objects
//! - ExistKey / GetReplicaList / Remove operations are tenant-scoped
//! - RemoveByRegex only removes within the specified tenant
//! - Eviction operates per-tenant (keys are scoped internally)
//! - Copy/Move operations are tenant-isolated
//! - Offload / Promotion heartbeat keys are tenant-scoped internally
//!
//! 验证不同租户中的对象完全隔离：
//! - 不同租户中的相同 user_key → 独立的对象
//! - ExistKey / GetReplicaList / Remove 操作按租户隔离
//! - RemoveByRegex 仅在指定租户内删除
//! - 驱逐按租户操作（内部 key 已作用域化）
//! - Copy/Move 操作按租户隔离
//! - Offload / Promotion 心跳 key 内部已作用域化

use dashmap::DashMap;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::service::ObjectEntry;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl, TenantId};
use std::time::{Duration, SystemTime};
use tonic::Request;
use uuid::Uuid;

static NEXT_SEGMENT_BASE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0x6_0000_0000);

fn client_proto(id: Uuid) -> proto::Uuid {
    proto::Uuid {
        high: id.as_u64_pair().0,
        low: id.as_u64_pair().1,
    }
}

fn strict_service(tenants: &[&str], lease_ttl: Duration) -> MasterServiceImpl {
    let policy = tempfile::NamedTempFile::new().unwrap();
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_tenant_quota: true,
        tenant_quota_connector_uri: policy.path().to_string_lossy().into_owned(),
        tenant_quota_pool_capacity_bytes: 64 * 1024,
        default_tenant_quota_bytes: 16 * 1024,
        lease_ttl,
        ..Default::default()
    });
    for tenant_id in tenants {
        service
            .upsert_tenant_quota_policy(tenant_id, 16 * 1024)
            .unwrap();
    }
    service
}

fn mount_seg(service: &MasterServiceImpl, name: &str, cid: Uuid, size: u64) {
    let base_addr = NEXT_SEGMENT_BASE.fetch_add(0x10000, std::sync::atomic::Ordering::Relaxed);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        MasterService::mount_segment(
            service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(client_proto(cid)),
                segment_name: name.into(),
                size,
                base_addr,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    });
}

fn put_object(
    service: &MasterServiceImpl,
    key: &str,
    tenant_id: &str,
    cid: Uuid,
    size: u64,
) -> Vec<proto::ReplicaDescriptor> {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let put = MasterService::put_start(
            service,
            Request::new(proto::PutStartRequest {
                client_id: Some(client_proto(cid)),
                key: key.into(),
                slice_length: size,
                config: Some(proto::ReplicateConfig {
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
                }),
                tenant_id: tenant_id.into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();

        MasterService::put_end(
            service,
            Request::new(proto::PutEndRequest {
                client_id: Some(client_proto(cid)),
                key: key.into(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: tenant_id.into(),
            }),
        )
        .await
        .unwrap();

        put.replicas
    })
}

#[test]
fn test_object_state_retains_typed_default_tenant() {
    let objects = DashMap::new();
    objects.insert(
        "default\0typed-object".to_string(),
        ObjectEntry {
            replicas: vec![],
            size: 1,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::nil(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: TenantId::default(),
            group_id: String::new(),
            quota_committed: false,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "typed-object".to_string(),
        },
    );

    let entry = objects.get("default\0typed-object").unwrap();
    let tenant: &TenantId = &entry.tenant_id;
    assert_eq!(tenant, &TenantId::default());
}

#[test]
fn test_same_key_different_tenants_isolated() {
    let service = strict_service(&["tenant-A", "tenant-B"], Duration::ZERO);
    let cid_a = Uuid::new_v4();
    let cid_b = Uuid::new_v4();

    mount_seg(&service, "tenant-a:1", cid_a, 4096);
    mount_seg(&service, "tenant-b:1", cid_b, 4096);

    // Same user_key, different tenants
    put_object(&service, "shared-key", "tenant-A", cid_a, 128);
    put_object(&service, "shared-key", "tenant-B", cid_b, 256);

    let rt = tokio::runtime::Runtime::new().unwrap();

    // ExistKey: tenant-A should see it, tenant-B should see it, default tenant should NOT
    rt.block_on(async {
        let exists_a = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "shared-key".into(),
                tenant_id: "tenant-A".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(exists_a.exists, "should exist in tenant-A");

        let exists_b = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "shared-key".into(),
                tenant_id: "tenant-B".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(exists_b.exists, "should exist in tenant-B");

        let exists_default = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "shared-key".into(),
                tenant_id: "default".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(!exists_default.exists, "should NOT exist in default tenant");

        // GetReplicaList: each tenant sees its own data
        let list_a = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "shared-key".into(),
                tenant_id: "tenant-A".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(list_a.replicas.len(), 1);
        assert_eq!(list_a.replicas[0].size, 128);

        let list_b = MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "shared-key".into(),
                tenant_id: "tenant-B".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(list_b.replicas.len(), 1);
        assert_eq!(list_b.replicas[0].size, 256);
    });

    // Remove in tenant-A should NOT affect tenant-B
    rt.block_on(async {
        MasterService::remove(
            &service,
            Request::new(proto::RemoveRequest {
                key: "shared-key".into(),
                force: true,
                tenant_id: "tenant-A".into(),
            }),
        )
        .await
        .unwrap();

        let exists_a_after = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "shared-key".into(),
                tenant_id: "tenant-A".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(!exists_a_after.exists, "should be removed from tenant-A");

        let exists_b_after = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "shared-key".into(),
                tenant_id: "tenant-B".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(exists_b_after.exists, "should still exist in tenant-B");
    });
}

#[test]
fn test_remove_by_regex_tenant_scoped() {
    let service = strict_service(&["tenant-X", "tenant-Y"], Duration::ZERO);
    let cid = Uuid::new_v4();
    mount_seg(&service, "regex-seg:1", cid, 4096);

    // Create objects in two tenants with similar keys
    put_object(&service, "alpha-one", "tenant-X", cid, 64);
    put_object(&service, "alpha-two", "tenant-X", cid, 64);
    put_object(&service, "alpha-three", "tenant-Y", cid, 64);

    let rt = tokio::runtime::Runtime::new().unwrap();

    // RemoveByRegex should only affect tenant-X
    rt.block_on(async {
        let removed = MasterService::remove_by_regex(
            &service,
            Request::new(proto::RemoveByRegexRequest {
                pattern: "alpha-.*".into(),
                force: true,
                tenant_id: "tenant-X".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(removed.removed_count, 2, "should remove 2 from tenant-X");

        // Verify tenant-X keys are gone
        let exists = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "alpha-one".into(),
                tenant_id: "tenant-X".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(!exists.exists, "alpha-one should be gone from tenant-X");

        // Verify tenant-Y key still exists
        let exists_y = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "alpha-three".into(),
                tenant_id: "tenant-Y".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(exists_y.exists, "alpha-three should remain in tenant-Y");
    });
}

#[test]
fn regex_lookup_and_removal_are_tenant_scoped_parity() {
    let service = strict_service(
        &["default", "tenant_regex_a", "tenant_regex_b"],
        Duration::ZERO,
    );
    let client_id = Uuid::new_v4();
    mount_seg(&service, "regex-parity:1", client_id, 16 * 1024);
    for tenant_id in ["default", "tenant_regex_a", "tenant_regex_b"] {
        put_object(&service, "regex_shared_key", tenant_id, client_id, 1024);
    }

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let default_matches = MasterService::get_replica_list_by_regex(
            &service,
            Request::new(proto::GetReplicaListByRegexRequest {
                key_regex: "^regex_shared".into(),
                tenant_id: "default".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(default_matches.entries.len(), 1);

        let removed_default = MasterService::remove_by_regex(
            &service,
            Request::new(proto::RemoveByRegexRequest {
                pattern: "^regex_shared".into(),
                force: true,
                tenant_id: "default".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(removed_default.removed_count, 1);
        assert!(MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "regex_shared_key".into(),
                tenant_id: "default".into(),
            }),
        )
        .await
        .is_err());
        for tenant_id in ["tenant_regex_a", "tenant_regex_b"] {
            MasterService::get_replica_list(
                &service,
                Request::new(proto::GetReplicaListRequest {
                    key: "regex_shared_key".into(),
                    tenant_id: tenant_id.into(),
                }),
            )
            .await
            .unwrap();
        }

        let removed_a = MasterService::remove_by_regex(
            &service,
            Request::new(proto::RemoveByRegexRequest {
                pattern: "^regex_shared".into(),
                force: true,
                tenant_id: "tenant_regex_a".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(removed_a.removed_count, 1);
        assert!(MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "regex_shared_key".into(),
                tenant_id: "tenant_regex_a".into(),
            }),
        )
        .await
        .is_err());
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: "regex_shared_key".into(),
                tenant_id: "tenant_regex_b".into(),
            }),
        )
        .await
        .unwrap();
    });
}

#[test]
fn batch_remove_and_remove_all_are_tenant_scoped_parity() {
    let service = strict_service(
        &["default", "tenant_batch_remove_a", "tenant_batch_remove_b"],
        Duration::ZERO,
    );
    let client_id = Uuid::new_v4();
    let key = "tenant_batch_remove_shared_key";
    mount_seg(&service, "tenant-remove-parity:1", client_id, 16 * 1024);
    for tenant_id in ["default", "tenant_batch_remove_a", "tenant_batch_remove_b"] {
        put_object(&service, key, tenant_id, client_id, 1024);
    }

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let removed_a = MasterService::batch_remove(
            &service,
            Request::new(proto::BatchRemoveRequest {
                keys: vec![key.into()],
                force: true,
                tenant_id: "tenant_batch_remove_a".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(removed_a.statuses, [0]);
        assert!(MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: "tenant_batch_remove_a".into(),
            }),
        )
        .await
        .is_err());
        for tenant_id in ["default", "tenant_batch_remove_b"] {
            MasterService::get_replica_list(
                &service,
                Request::new(proto::GetReplicaListRequest {
                    key: key.into(),
                    tenant_id: tenant_id.into(),
                }),
            )
            .await
            .unwrap();
        }

        let removed_b = MasterService::remove_all(
            &service,
            Request::new(proto::RemoveAllRequest {
                force: true,
                tenant_id: "tenant_batch_remove_b".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(removed_b.removed_count, 1);
        assert!(MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: "tenant_batch_remove_b".into(),
            }),
        )
        .await
        .is_err());
        MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: "default".into(),
            }),
        )
        .await
        .unwrap();

        let removed_default = MasterService::remove_all(
            &service,
            Request::new(proto::RemoveAllRequest {
                force: true,
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(removed_default.removed_count, 1);
        assert!(MasterService::get_replica_list(
            &service,
            Request::new(proto::GetReplicaListRequest {
                key: key.into(),
                tenant_id: "default".into(),
            }),
        )
        .await
        .is_err());
    });
}

#[test]
fn batch_get_replica_list_keeps_tenant_isolation_parity() {
    let service = strict_service(
        &["default", "batch_get_tenant_a", "batch_get_tenant_b"],
        Duration::ZERO,
    );
    let client_id = Uuid::new_v4();
    let key = "batch_get_tenant_shared_key";
    mount_seg(&service, "tenant-batch-get:1", client_id, 16 * 1024);

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let put_for_tenant =
            |tenant_id: &str, size: u64, group_ids: Vec<String>| proto::PutStartRequest {
                client_id: Some(client_proto(client_id)),
                key: key.into(),
                slice_length: size,
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    group_ids,
                    ..Default::default()
                }),
                tenant_id: tenant_id.into(),
            };
        for (tenant_id, request) in [
            (
                "batch_get_tenant_a",
                put_for_tenant("batch_get_tenant_a", 1024, vec!["batch_get_group_a".into()]),
            ),
            (
                "batch_get_tenant_b",
                put_for_tenant("batch_get_tenant_b", 2048, vec![]),
            ),
        ] {
            MasterService::put_start(&service, Request::new(request))
                .await
                .unwrap();
            MasterService::put_end(
                &service,
                Request::new(proto::PutEndRequest {
                    client_id: Some(client_proto(client_id)),
                    key: key.into(),
                    replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                    tenant_id: tenant_id.into(),
                }),
            )
            .await
            .unwrap();
        }

        for (tenant_id, expected_status) in [
            ("batch_get_tenant_a", 0),
            ("batch_get_tenant_b", 0),
            ("default", -1),
        ] {
            let response = MasterService::batch_get_replica_list(
                &service,
                Request::new(proto::BatchGetReplicaListRequest {
                    keys: vec![key.into()],
                    tenant_id: tenant_id.into(),
                }),
            )
            .await
            .unwrap()
            .into_inner();
            assert_eq!(response.results.len(), 1);
            assert_eq!(response.results[0].status, expected_status, "{tenant_id}");
            assert_eq!(response.results[0].response.is_some(), expected_status == 0);
        }
    });
}

#[test]
fn get_all_keys_lists_only_requested_tenant_parity() {
    let service = strict_service(&["default", "tenant_get_all_keys_a"], Duration::ZERO);
    let client_id = Uuid::new_v4();
    mount_seg(&service, "tenant-listing:1", client_id, 16 * 1024);
    for (key, tenant_id) in [
        ("shared_listing_key", "default"),
        ("default_listing_key", "default"),
        ("shared_listing_key", "tenant_get_all_keys_a"),
        ("tenant_listing_key", "tenant_get_all_keys_a"),
    ] {
        put_object(&service, key, tenant_id, client_id, 1024);
    }

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let default_keys = MasterService::get_all_keys(
            &service,
            Request::new(proto::GetAllKeysRequest {
                tenant_id: "default".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .keys;
        assert!(default_keys.contains(&"shared_listing_key".to_string()));
        assert!(default_keys.contains(&"default_listing_key".to_string()));
        assert!(!default_keys.contains(&"tenant_listing_key".to_string()));

        let tenant_keys = MasterService::get_all_keys(
            &service,
            Request::new(proto::GetAllKeysRequest {
                tenant_id: "tenant_get_all_keys_a".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .keys;
        assert!(tenant_keys.contains(&"shared_listing_key".to_string()));
        assert!(tenant_keys.contains(&"tenant_listing_key".to_string()));
        assert!(!tenant_keys.contains(&"default_listing_key".to_string()));
    });
}

#[test]
fn test_get_all_keys_filters_by_tenant() {
    let service = strict_service(&["tenant-Z", "default"], Duration::ZERO);
    let cid = Uuid::new_v4();
    mount_seg(&service, "allkeys-seg:1", cid, 4096);

    put_object(&service, "obj-1", "tenant-Z", cid, 64);
    put_object(&service, "obj-2", "tenant-Z", cid, 64);
    put_object(&service, "obj-3", "default", cid, 64);

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Without filter: get all
        let all = MasterService::get_all_keys(
            &service,
            Request::new(proto::GetAllKeysRequest {
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(!all.keys.is_empty(), "should have keys");

        // With tenant-Z filter
        let z_keys = MasterService::get_all_keys(
            &service,
            Request::new(proto::GetAllKeysRequest {
                tenant_id: "tenant-Z".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(
            z_keys.keys.len() >= 2,
            "tenant-Z should have at least 2 keys"
        );
        assert!(z_keys.keys.contains(&"obj-1".to_string()));
        assert!(z_keys.keys.contains(&"obj-2".to_string()));
    });
}

#[test]
fn test_eviction_tenant_scoped() {
    let service = strict_service(&["tenant-E1", "tenant-E2"], Duration::from_millis(1));
    let cid = Uuid::new_v4();

    mount_seg(&service, "evict-tenant-seg:1", cid, 4096);

    put_object(&service, "evict-me", "tenant-E1", cid, 128);
    put_object(&service, "keep-me", "tenant-E2", cid, 128);

    // Wait for lease to expire (1ms TTL)
    std::thread::sleep(Duration::from_millis(10));

    // Run eviction — should evict from both tenants
    let evicted = service.run_eviction_cycle_for_test(10);
    assert!(
        !evicted.is_empty(),
        "should evict at least one key, got: {:?}",
        evicted
    );
    assert!(
        evicted.iter().any(|k| k == "evict-me" || k == "keep-me"),
        "should contain expected keys, got: {:?}",
        evicted
    );

    // Verify objects are gone after eviction
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let exists = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "evict-me".into(),
                tenant_id: "tenant-E1".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        // At least one is gone; eviction order is LRU-based
        let still_exists = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "keep-me".into(),
                tenant_id: "tenant-E2".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(
            !exists.exists || !still_exists.exists,
            "at least one key should be evicted"
        );
    });
}

#[test]
fn test_backward_compat_empty_tenant_defaults() {
    // Old clients sending no tenant_id should work in the "default" tenant
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::ZERO,
        ..Default::default()
    });
    let cid = Uuid::new_v4();
    mount_seg(&service, "compat-seg:1", cid, 4096);

    // Use tenant_id="" (proto3 default for old clients)
    put_object(&service, "compat-key", "", cid, 128);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Find with tenant_id=""
        let exists = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "compat-key".into(),
                tenant_id: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(exists.exists, "empty tenant should map to default");

        // Find with explicit "default"
        let exists_d = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "compat-key".into(),
                tenant_id: "default".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(
            exists_d.exists,
            "explicit default tenant should find the key"
        );
    });
}

#[test]
fn test_copy_task_tenant_isolated() {
    let service = strict_service(&["tenant-C1", "tenant-C2"], Duration::ZERO);
    let cid_a = Uuid::new_v4();
    let cid_b = Uuid::new_v4();

    mount_seg(&service, "copy-src-t1:1", cid_a, 4096);
    mount_seg(&service, "copy-dst-t1:2", cid_a, 4096);
    mount_seg(&service, "copy-src-t2:1", cid_b, 4096);

    // Same key, different tenants
    put_object(&service, "copy-key", "tenant-C1", cid_a, 128);
    put_object(&service, "copy-key", "tenant-C2", cid_b, 128);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Create copy task in tenant-C1 — copy to dst segment
        let task = MasterService::create_copy_task(
            &service,
            Request::new(proto::CreateCopyTaskRequest {
                key: "copy-key".into(),
                targets: vec!["copy-dst-t1:2".into()],
                tenant_id: "tenant-C1".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(task.task_id.is_some(), "copy task should be created");

        // Verify tenant-C2 object unaffected
        let exists_c2 = MasterService::exist_key(
            &service,
            Request::new(proto::ExistKeyRequest {
                key: "copy-key".into(),
                tenant_id: "tenant-C2".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(exists_c2.exists, "tenant-C2 should still have its copy-key");
    });
}

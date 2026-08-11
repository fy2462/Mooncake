//! C++ `MasterServiceTest.GetReplicaListByRegex*` / `RemoveByRegex*` parity.

mod common;
use common::proto_uuid;
use mooncake_store_master::proto;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
use std::time::Duration;
use tonic::{Code, Request};
use uuid::Uuid;

const KEY_SET: [&str; 13] = [
    "test_key_01",
    "test_key_02",
    "test_key_10",
    "prod_key_alpha",
    "prod_key_beta",
    "data_part_1_chunk_a",
    "data_part_2_chunk_b",
    "config/user/settings.json",
    "logs/app-2025-08-13.log",
    "short",
    "a_very_very_very_long_key_that_tests_length_limits",
    "test-key-extra",
    "another_key",
];

fn new_service(lease_ttl_ms: u64) -> MasterServiceImpl {
    MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        lease_ttl: Duration::from_millis(lease_ttl_ms),
        ..Default::default()
    })
}

async fn mount_segment(service: &MasterServiceImpl, client_id: Uuid, name: &str, index: u64) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: name.into(),
            size: 1024 * 1024,
            base_addr: 0x100000000 + index * 0x200000,
            te_endpoint: String::new(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn put_object(service: &MasterServiceImpl, client_id: Uuid, key: &str, segment: &str) {
    let started = MasterService::put_start(
        service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: key.into(),
            slice_length: 1024,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: segment.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(started.replicas.len(), 1);

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

async fn populate(service: &MasterServiceImpl, client_id: Uuid, segment: &str) {
    for key in KEY_SET {
        put_object(service, client_id, key, segment).await;
    }
}

async fn regex_keys(service: &MasterServiceImpl, pattern: &str) -> Vec<String> {
    MasterService::get_replica_list_by_regex(
        service,
        Request::new(proto::GetReplicaListByRegexRequest {
            key_regex: pattern.into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .entries
    .into_iter()
    .map(|entry| entry.user_key)
    .collect()
}

async fn exists(service: &MasterServiceImpl, key: &str) -> bool {
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

async fn remove_by_regex(service: &MasterServiceImpl, pattern: &str) -> i64 {
    MasterService::remove_by_regex(
        service,
        Request::new(proto::RemoveByRegexRequest {
            pattern: pattern.into(),
            force: false,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .removed_count
}

#[tokio::test]
async fn regex_replica_lookup_count_and_missing_literal_parity() {
    let service = new_service(50);
    let client_id = Uuid::new_v4();

    let missing = MasterService::get_replica_list(
        &service,
        Request::new(proto::GetReplicaListRequest {
            key: ".*non_existent.*".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);

    mount_segment(&service, client_id, "regex:1", 0).await;
    for times in 0..10 {
        let key = format!("test_key{times}");
        put_object(&service, client_id, &key, "regex:1").await;
        assert!(exists(&service, &key).await);
    }

    tokio::time::sleep(Duration::from_millis(60)).await;

    let keys = regex_keys(&service, "^test_key").await;
    assert_eq!(keys.len(), 10);
}

#[tokio::test]
async fn complex_regex_replica_lookup_matrix_parity() {
    let service = new_service(100);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "regex-complex:1", 0).await;
    populate(&service, client_id, "regex-complex:1").await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    let prefix = regex_keys(&service, "^test_key_").await;
    assert_eq!(prefix.len(), 3);

    let digits = regex_keys(&service, "^test_key_\\d+$").await;
    assert_eq!(digits.len(), 3);

    let data_chunk = regex_keys(&service, "^data_part_\\d_chunk_.$").await;
    assert_eq!(data_chunk.len(), 2);

    let substring = regex_keys(&service, "key").await;
    assert_eq!(substring.len(), 8);

    let log_files = regex_keys(&service, "\\.log$").await;
    assert_eq!(log_files, vec!["logs/app-2025-08-13.log"]);

    let or_pattern = regex_keys(&service, "^prod|\\.json$").await;
    assert_eq!(or_pattern.len(), 3);

    let no_match = regex_keys(&service, "^non_existent_prefix_").await;
    assert!(no_match.is_empty());

    let exact = regex_keys(&service, "^short$").await;
    assert_eq!(exact, vec!["short"]);

    let absent = regex_keys(&service, ".*absolutely_non_existent.*").await;
    assert!(absent.is_empty());
}

#[tokio::test]
async fn remove_ten_keys_by_prefix_parity() {
    let service = new_service(50);
    let client_id = Uuid::new_v4();
    mount_segment(&service, client_id, "regex-remove:1", 0).await;

    for times in 0..10 {
        let key = format!("test_key{times}");
        put_object(&service, client_id, &key, "regex-remove:1").await;
        assert!(exists(&service, &key).await);
    }

    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(remove_by_regex(&service, "^test_key").await, 10);
    for times in 0..10 {
        assert!(!exists(&service, &format!("test_key{times}")).await);
    }
}

#[tokio::test]
async fn complex_regex_remove_count_and_survivor_matrix_parity() {
    async fn fresh(lease_ttl_ms: u64, segment: &str) -> (MasterServiceImpl, Uuid) {
        let service = new_service(lease_ttl_ms);
        let client_id = Uuid::new_v4();
        mount_segment(&service, client_id, segment, 0).await;
        populate(&service, client_id, segment).await;
        tokio::time::sleep(Duration::from_millis(lease_ttl_ms + 20)).await;
        (service, client_id)
    }

    // Case 1: prefix removal leaves survivors.
    {
        let (service, _client_id) = fresh(100, "regex-remove-complex:1").await;
        assert_eq!(remove_by_regex(&service, "^test_key_").await, 3);
        for key in ["test_key_01", "test_key_02", "test_key_10"] {
            assert!(!exists(&service, key).await, "{key} should be removed");
        }
        for key in ["prod_key_alpha", "short", "test-key-extra"] {
            assert!(exists(&service, key).await, "{key} should survive");
        }
    }

    // Case 2: remove everything.
    {
        let (service, _client_id) = fresh(100, "regex-remove-complex:2").await;
        assert_eq!(remove_by_regex(&service, ".*").await, 13);
        assert!(regex_keys(&service, ".*").await.is_empty());
    }

    // Case 3: non-matching pattern removes nothing.
    {
        let (service, _client_id) = fresh(100, "regex-remove-complex:3").await;
        assert_eq!(remove_by_regex(&service, "^nonexistent-pattern-").await, 0);
        assert_eq!(regex_keys(&service, ".*").await.len(), 13);
    }

    // Case 4a: slash or trailing digit removes five keys.
    {
        let (service, _client_id) = fresh(100, "regex-remove-complex:4a").await;
        assert_eq!(remove_by_regex(&service, "/|\\d$").await, 5);
    }

    // Case 4b: chunk-or-config removal with exact survivors.
    {
        let (service, _client_id) = fresh(100, "regex-remove-complex:4b").await;
        assert_eq!(remove_by_regex(&service, "chunk|config").await, 3);
        assert!(!exists(&service, "data_part_1_chunk_a").await);
        assert!(!exists(&service, "config/user/settings.json").await);
        assert!(exists(&service, "prod_key_alpha").await);
    }
}

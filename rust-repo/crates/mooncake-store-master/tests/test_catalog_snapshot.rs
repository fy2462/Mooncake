use chrono::{TimeZone, Utc};
use mooncake_store_core::{
    ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment, TaskInfo, TaskStatus,
    TaskType,
};
use mooncake_store_master::TenantId;
use mooncake_store_master::allocator::{
    AllocationStrategy, AllocatorSnapshotConfig, MemoryAllocatorKind,
};
use mooncake_store_master::ha::{
    CatalogBackedSnapshotProvider, EmbeddedSnapshotCatalogStore, HaError, LoadedSnapshot,
    LocalFileSnapshotObjectStore, SnapshotCatalogStore, SnapshotCatalogStoreType,
    SnapshotDescriptor, SnapshotObjectStore, SnapshotObjectStoreType, SnapshotProvider,
    create_catalog_backed_snapshot_provider,
};
use mooncake_store_master::proto::SegmentStatus;
use mooncake_store_master::service::{
    DelayedReplicaReleaseEntry, GracefulUnmountSnapshotEntry, ObjectEntry, ReplicationTaskKind,
    ReplicationTaskSnapshotEntry, SegmentEntry, TaskEntry,
};
use mooncake_store_master::storage_backend::LocalDiskSnapshotEntry;
use rmpv::Value;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use uuid::Uuid;

fn encode(value: &Value) -> Vec<u8> {
    let mut data = Vec::new();
    rmpv::encode::write_value(&mut data, value).unwrap();
    data
}

fn compress(value: &Value) -> Vec<u8> {
    zstd::stream::encode_all(Cursor::new(encode(value)), 3).unwrap()
}

#[test]
fn cpp_parity_embedded_catalog_list_rejects_missing_object_store() {
    let catalog = EmbeddedSnapshotCatalogStore::without_object_store("");
    assert!(matches!(catalog.list(0), Err(HaError::InvalidParams(_))));
}

fn empty_loaded_snapshot(snapshot_id: &str, snapshot_sequence_id: u64) -> LoadedSnapshot {
    LoadedSnapshot {
        snapshot_id: snapshot_id.to_string(),
        snapshot_sequence_id,
        allocator_config: None,
        segments: Vec::new(),
        nof_segments: Vec::new(),
        objects: Vec::new(),
        tasks: Vec::new(),
        replication_tasks: Vec::new(),
        graceful_unmounts: Vec::new(),
        delayed_replica_releases: Vec::new(),
        local_disk_segments: Vec::new(),
    }
}

struct DownloadFailureSnapshotObjectStore {
    inner: Arc<LocalFileSnapshotObjectStore>,
    failing_key: String,
}

impl SnapshotObjectStore for DownloadFailureSnapshotObjectStore {
    fn upload_buffer(&self, key: &str, buffer: &[u8]) -> Result<(), HaError> {
        self.inner.upload_buffer(key, buffer)
    }

    fn download_buffer(&self, key: &str) -> Result<Vec<u8>, HaError> {
        if key == self.failing_key {
            return Err(HaError::Snapshot("permission denied".into()));
        }
        self.inner.download_buffer(key)
    }

    fn delete_objects_with_prefix(&self, prefix: &str) -> Result<(), HaError> {
        self.inner.delete_objects_with_prefix(prefix)
    }

    fn list_objects_with_prefix(&self, prefix: &str) -> Result<Vec<String>, HaError> {
        self.inner.list_objects_with_prefix(prefix)
    }

    fn is_not_found_error(&self, error: &str) -> bool {
        self.inner.is_not_found_error(error)
    }

    fn connection_info(&self) -> String {
        self.inner.connection_info()
    }
}

struct UnexpectedCatalogAccess;

impl SnapshotCatalogStore for UnexpectedCatalogAccess {
    fn publish(&self, _: &SnapshotDescriptor) -> Result<(), HaError> {
        panic!("cluster mismatch must be rejected before catalog publish")
    }

    fn get_latest(&self) -> Result<Option<SnapshotDescriptor>, HaError> {
        panic!("cluster mismatch must be rejected before catalog read")
    }

    fn list(&self, _: usize) -> Result<Vec<SnapshotDescriptor>, HaError> {
        panic!("cluster mismatch must be rejected before catalog list")
    }

    fn delete(&self, _: &str) -> Result<(), HaError> {
        panic!("cluster mismatch must be rejected before catalog delete")
    }

    fn get_snapshot_root(&self) -> &str {
        "mooncake_master_snapshot/"
    }
}

#[test]
fn cpp_parity_snapshot_child_generated_timestamp_matches_expected_format() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store_and_cluster_id(
        object_store.clone(),
        "cluster-a",
    );
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);

    let descriptor = provider
        .publish_loaded_snapshot(&empty_loaded_snapshot("", 0), 0)
        .unwrap();
    let bytes = descriptor.snapshot_id.as_bytes();

    assert_eq!(bytes.len(), 19);
    assert_eq!(bytes[8], b'_');
    assert_eq!(bytes[15], b'_');
    assert!(
        bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 8 || index == 15 || byte.is_ascii_digit())
    );
}

#[test]
fn cpp_parity_ha_snapshot_catalog_backed_snapshot_provider_test_cpp_catalogbackedsnapshotprovidertest_loadlatestsnapshotreturnsemptywhencatalogmissing()
 {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store_and_cluster_id(
        object_store.clone(),
        "empty-cluster",
    );
    let provider =
        CatalogBackedSnapshotProvider::new("empty-cluster", Box::new(catalog), object_store);

    assert!(
        provider
            .load_latest_snapshot("empty-cluster")
            .unwrap()
            .is_none()
    );
}

#[test]
fn cpp_parity_snapshot_child_persist_state_publishes_descriptor() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store_and_cluster_id(
        object_store.clone(),
        "cluster-a",
    );
    let provider =
        CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store.clone());
    let snapshot_id = "20240601_120000_123";
    let before_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let descriptor = provider
        .publish_loaded_snapshot(&empty_loaded_snapshot(snapshot_id, 0), 37)
        .unwrap();
    let after_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    assert_eq!(descriptor.snapshot_id, snapshot_id);
    assert_eq!(
        descriptor.manifest_key,
        format!("mooncake_master_snapshot/cluster-a/{snapshot_id}/manifest.txt")
    );
    assert_eq!(
        descriptor.object_prefix,
        format!("mooncake_master_snapshot/cluster-a/{snapshot_id}/")
    );
    assert_eq!(descriptor.last_included_seq, 0);
    assert_eq!(descriptor.producer_view_version, 37);
    assert!(descriptor.created_at_ms >= before_ms);
    assert!(descriptor.created_at_ms <= after_ms);

    let catalog_reader =
        EmbeddedSnapshotCatalogStore::with_object_store_and_cluster_id(object_store, "cluster-a");
    assert_eq!(catalog_reader.get_latest().unwrap(), Some(descriptor));
}

#[test]
fn cpp_parity_snapshot_child_persist_state_uses_frozen_descriptor() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog =
        EmbeddedSnapshotCatalogStore::with_object_store_and_cluster_id(object_store, "cluster-a");
    let mut descriptor = SnapshotDescriptor::new_with_snapshot_root(
        catalog.get_snapshot_root(),
        "20240601_120000_124",
    );
    descriptor.last_included_seq = 123;
    descriptor.producer_view_version = 41;
    descriptor.created_at_ms = 1_717_243_200_123;

    catalog.publish(&descriptor).unwrap();

    assert_eq!(catalog.get_latest().unwrap(), Some(descriptor));
}

#[test]
fn cpp_parity_embedded_catalog_trims_ascii_whitespace_marker() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let snapshot_id = "20240301_120000_002";
    let mut descriptor = SnapshotDescriptor::new(snapshot_id);
    descriptor.last_included_seq = 42;
    catalog.publish(&descriptor).unwrap();
    object_store
        .upload_string(
            "mooncake_master_snapshot/latest.txt",
            &format!("  \n{snapshot_id}\t\r\n"),
        )
        .unwrap();

    assert_eq!(catalog.get_latest().unwrap(), Some(descriptor));
}

#[test]
fn cpp_parity_embedded_catalog_lists_newest_snapshots_with_limit() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    for snapshot_id in [
        "20240301_120000_001",
        "20240303_120000_001",
        "20240302_120000_001",
    ] {
        catalog
            .publish(&SnapshotDescriptor::new(snapshot_id))
            .unwrap();
    }
    object_store
        .upload_string("mooncake_master_snapshot/not-a-snapshot/file.txt", "ignore")
        .unwrap();

    let snapshots = catalog.list(2).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].snapshot_id, "20240303_120000_001");
    assert_eq!(snapshots[1].snapshot_id, "20240302_120000_001");
}

#[test]
fn cpp_parity_embedded_catalog_rejects_invalid_snapshot_ids() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());

    assert!(matches!(
        catalog.publish(&SnapshotDescriptor::new("invalid-id")),
        Err(HaError::InvalidParams(_))
    ));
    assert!(matches!(
        catalog.delete("invalid-id"),
        Err(HaError::InvalidParams(_))
    ));
    object_store
        .upload_string("mooncake_master_snapshot/latest.txt", "invalid-id")
        .unwrap();
    assert!(matches!(
        catalog.get_latest(),
        Err(HaError::InvalidParams(_))
    ));
}

#[test]
fn cpp_parity_embedded_catalog_skips_missing_descriptor() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    object_store
        .upload_string(
            "mooncake_master_snapshot/20240303_120000_001/manifest.txt",
            "m3",
        )
        .unwrap();

    assert!(catalog.list(0).unwrap().is_empty());
}

#[test]
fn cpp_parity_embedded_catalog_skips_unreadable_newest_descriptor() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    for snapshot_id in ["20240301_120000_001", "20240303_120000_001"] {
        catalog
            .publish(&SnapshotDescriptor::new(snapshot_id))
            .unwrap();
    }
    object_store
        .delete_objects_with_prefix("mooncake_master_snapshot/20240303_120000_001/descriptor.txt")
        .unwrap();

    let snapshots = catalog.list(0).unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].snapshot_id, "20240301_120000_001");
}

#[test]
fn cpp_parity_embedded_catalog_skips_non_not_found_descriptor_error() {
    let root = tempdir().unwrap();
    let inner = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let object_store = Arc::new(DownloadFailureSnapshotObjectStore {
        inner,
        failing_key: "mooncake_master_snapshot/20240303_120000_001/descriptor.txt".to_string(),
    });
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store);
    for snapshot_id in ["20240301_120000_001", "20240303_120000_001"] {
        catalog
            .publish(&SnapshotDescriptor::new(snapshot_id))
            .unwrap();
    }

    let snapshots = catalog.list(1).unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].snapshot_id, "20240301_120000_001");
}

#[test]
fn cpp_parity_embedded_catalog_propagates_latest_marker_read_error() {
    let root = tempdir().unwrap();
    let inner = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let object_store = Arc::new(DownloadFailureSnapshotObjectStore {
        inner,
        failing_key: "mooncake_master_snapshot/latest.txt".to_string(),
    });
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store);

    let error = catalog.get_latest().unwrap_err();
    assert!(matches!(error, HaError::Snapshot(_)));
    assert!(error.to_string().contains("permission denied"));
}

#[test]
fn cpp_parity_embedded_catalog_delete_latest_falls_back_to_previous() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store);
    for snapshot_id in ["20240301_120000_001", "20240302_120000_001"] {
        catalog
            .publish(&SnapshotDescriptor::new(snapshot_id))
            .unwrap();
    }

    catalog.delete("20240302_120000_001").unwrap();

    assert_eq!(
        catalog.get_latest().unwrap().unwrap().snapshot_id,
        "20240301_120000_001"
    );
}

#[test]
fn cpp_parity_task_snapshot_round_trip_preserves_four_states() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    let client1 = Uuid::new_v4();
    let client2 = Uuid::new_v4();
    let created_at = Utc.with_ymd_and_hms(2026, 8, 3, 2, 0, 0).unwrap();
    let updated_at = Utc.with_ymd_and_hms(2026, 8, 3, 2, 0, 1).unwrap();
    let definitions = [
        (
            TaskType::ReplicaCopy,
            TaskStatus::Success,
            client1,
            "copy-success",
            r#"{"tenant_id":"default","key":"copy-success","source":"seg1","targets":["seg2"]}"#,
        ),
        (
            TaskType::ReplicaCopy,
            TaskStatus::Failed,
            client2,
            "copy-failed",
            r#"{"tenant_id":"default","key":"copy-failed","source":"seg1","targets":["seg2"]}"#,
        ),
        (
            TaskType::ReplicaMove,
            TaskStatus::Pending,
            client1,
            "move-pending",
            r#"{"tenant_id":"default","key":"move-pending","source":"seg1","target":"seg2"}"#,
        ),
        (
            TaskType::ReplicaMove,
            TaskStatus::Processing,
            client2,
            "move-processing",
            r#"{"tenant_id":"default","key":"move-processing","source":"seg1","target":"seg2"}"#,
        ),
    ];
    let tasks = definitions
        .into_iter()
        .map(
            |(task_type, status, assigned_client, key, payload)| TaskEntry {
                info: TaskInfo {
                    id: Uuid::new_v4(),
                    task_type,
                    status,
                    created_at,
                    last_updated_at: updated_at,
                    assigned_client: Some(assigned_client),
                    message: String::new(),
                },
                key: TenantId::default().make_scoped_key(key),
                payload: payload.into(),
                max_retry_attempts: 3,
            },
        )
        .collect::<Vec<_>>();
    let mut snapshot = empty_loaded_snapshot("20260803_020001_001", 81);
    snapshot.tasks = tasks.clone();

    provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(loaded.tasks.len(), 4);
    for (original, restored) in tasks.iter().zip(&loaded.tasks) {
        assert_eq!(restored.info.id, original.info.id);
        assert_eq!(restored.info.task_type, original.info.task_type);
        assert_eq!(restored.info.status, original.info.status);
        assert_eq!(restored.info.assigned_client, original.info.assigned_client);
        assert!(!restored.payload.is_empty());
        assert!(
            (restored.info.created_at - original.info.created_at)
                .num_seconds()
                .abs()
                <= 1
        );
        assert!(
            (restored.info.last_updated_at - original.info.last_updated_at)
                .num_seconds()
                .abs()
                <= 1
        );
    }
}

#[test]
fn cpp_parity_empty_task_catalog_round_trip() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    let snapshot = empty_loaded_snapshot("20260803_020002_002", 82);

    provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(loaded.tasks.len(), 0);
}

// These helpers synthesize audited C++ wire shapes with Rust encoders. They
// exercise decoder branches but are not C++-produced golden fixtures.
fn synthetic_cpp_segments(segment_id: Uuid, client_id: Uuid) -> Vec<u8> {
    let allocator = Value::Array(vec![
        "segment-a".into(),
        0x1000_u64.into(),
        4096_u64.into(),
        512_u64.into(),
        "tcp://node-a".into(),
        Value::Nil,
    ]);
    let mounted = Value::Array(vec![
        segment_id.to_string().into(),
        "segment-a".into(),
        0x1000_u64.into(),
        4096_u64.into(),
        "tcp://node-a".into(),
        1.into(),
        true.into(),
        allocator,
        "node-a".into(),
    ]);
    compress(&Value::Map(vec![
        ("ma".into(), 0.into()),
        ("an".into(), Value::Array(vec!["segment-a".into()])),
        (
            "ms".into(),
            Value::Map(vec![(segment_id.to_string().into(), mounted)]),
        ),
        (
            "cs".into(),
            Value::Map(vec![(
                client_id.to_string().into(),
                Value::Array(vec![segment_id.to_string().into()]),
            )]),
        ),
        (
            "ld".into(),
            Value::Map(vec![(
                client_id.to_string().into(),
                Value::Array(vec![
                    true.into(),
                    1_u64.into(),
                    "tenant-a\0key-a".into(),
                    Value::Array(vec!["tenant-a".into(), "key-a".into(), 128_i64.into()]),
                    4096_i64.into(),
                ]),
            )]),
        ),
    ]))
}

#[derive(Clone, Copy)]
enum SyntheticCppMetadataShape {
    V1,
    V2DataType,
    V2HardPinned,
    V3DataTypeHardPinned,
    CurrentV4,
}

fn synthetic_cpp_metadata_with_shape(
    segment_id: Uuid,
    client_id: Uuid,
    tenant_id: Option<&str>,
    object_key: &str,
    lease_timeout_ms: u64,
    shape: SyntheticCppMetadataShape,
) -> Vec<u8> {
    let replica = Value::Array(vec![
        7_u64.into(),
        3.into(),
        0.into(),
        Value::Array(vec![
            128_u64.into(),
            0x1100_u64.into(),
            segment_id.to_string().into(),
            false.into(),
            Value::Nil,
        ]),
    ]);
    let mut metadata = vec![
        client_id.to_string().into(),
        1000_u64.into(),
        128_u64.into(),
        lease_timeout_ms.into(),
        false.into(),
        0_u64.into(),
        1_u64.into(),
    ];
    if matches!(
        shape,
        SyntheticCppMetadataShape::V2DataType
            | SyntheticCppMetadataShape::V3DataTypeHardPinned
            | SyntheticCppMetadataShape::CurrentV4
    ) {
        metadata.push((ObjectDataType::Kvcache as u64).into());
    }
    metadata.push(replica);
    if matches!(
        shape,
        SyntheticCppMetadataShape::V2HardPinned
            | SyntheticCppMetadataShape::V3DataTypeHardPinned
            | SyntheticCppMetadataShape::CurrentV4
    ) {
        metadata.push(true.into());
    }
    if matches!(shape, SyntheticCppMetadataShape::CurrentV4) {
        metadata.push("group-a".into());
    }
    let metadata = Value::Array(metadata);
    let item = match tenant_id {
        Some(tenant_id) => Value::Array(vec![tenant_id.into(), object_key.into(), metadata]),
        None => Value::Array(vec![object_key.into(), metadata]),
    };
    let shard = Value::Map(vec![("metadata".into(), Value::Array(vec![item]))]);
    encode(&Value::Map(vec![(
        "shards".into(),
        Value::Map(vec![(0.into(), Value::Binary(compress(&shard)))]),
    )]))
}

fn synthetic_cpp_metadata_with_discarded_replica(
    segment_id: Uuid,
    client_id: Uuid,
    lease_timeout_ms: u64,
    release_deadline_ms: u64,
) -> Vec<u8> {
    let encoded = synthetic_cpp_metadata_with_shape(
        segment_id,
        client_id,
        Some("tenant-a"),
        "key-a",
        lease_timeout_ms,
        SyntheticCppMetadataShape::CurrentV4,
    );
    let mut cursor = Cursor::new(encoded);
    let mut root = rmpv::decode::read_value(&mut cursor).unwrap();
    let Value::Map(fields) = &mut root else {
        panic!("synthetic C++ metadata root must be a map");
    };
    fields.push((
        "discarded_replicas".into(),
        Value::Array(vec![Value::Array(vec![
            release_deadline_ms.into(),
            128_u64.into(),
            1_u64.into(),
            Value::Array(vec![
                99_u64.into(),
                2_i64.into(),
                0_i64.into(),
                Value::Array(vec![
                    128_u64.into(),
                    0x1200_u64.into(),
                    segment_id.to_string().into(),
                    false.into(),
                    Value::Nil,
                ]),
            ]),
        ])]),
    ));
    fields.push(("replica_next_id".into(), 100_u64.into()));
    encode(&root)
}

fn synthetic_cpp_local_disk_metadata(client_id: Uuid, lease_timeout_ms: u64) -> Vec<u8> {
    let replica = Value::Array(vec![
        0_u64.into(),
        (ReplicaStatus::Complete as i32 as i64).into(),
        (ReplicaType::LocalDisk as i32 as i64).into(),
        Value::Array(vec![
            client_id.to_string().into(),
            128_u64.into(),
            "local://legacy-disk".into(),
        ]),
    ]);
    let metadata = Value::Array(vec![
        client_id.to_string().into(),
        1000_u64.into(),
        128_u64.into(),
        lease_timeout_ms.into(),
        false.into(),
        0_u64.into(),
        1_u64.into(),
        (ObjectDataType::Kvcache as u64).into(),
        replica,
        true.into(),
        "group-a".into(),
    ]);
    let shard = Value::Map(vec![(
        "metadata".into(),
        Value::Array(vec![Value::Array(vec![
            "tenant-a".into(),
            "legacy-local-disk".into(),
            metadata,
        ])]),
    )]);
    encode(&Value::Map(vec![(
        "shards".into(),
        Value::Map(vec![(0.into(), Value::Binary(compress(&shard)))]),
    )]))
}

fn synthetic_cpp_tasks(task_id: Uuid, client_id: Uuid) -> Vec<u8> {
    compress(&Value::Array(vec![Value::Array(vec![
        task_id.to_string().into(),
        1.into(),
        0.into(),
        r#"{"tenant_id":"tenant-a","key":"key-a","source":"a","target":"b"}"#.into(),
        1_000_i64.into(),
        2_000_i64.into(),
        "pending move".into(),
        client_id.to_string().into(),
    ])]))
}

fn publish_fixture(
    lease_timeout_ms: u64,
) -> (
    tempfile::TempDir,
    CatalogBackedSnapshotProvider,
    Uuid,
    Uuid,
    Uuid,
) {
    publish_fixture_with_identity(Some("tenant-a"), "key-a", lease_timeout_ms)
}

fn publish_fixture_with_identity(
    tenant_id: Option<&str>,
    object_key: &str,
    lease_timeout_ms: u64,
) -> (
    tempfile::TempDir,
    CatalogBackedSnapshotProvider,
    Uuid,
    Uuid,
    Uuid,
) {
    publish_fixture_with_identity_and_shape(
        tenant_id,
        object_key,
        lease_timeout_ms,
        SyntheticCppMetadataShape::CurrentV4,
    )
}

fn publish_fixture_with_identity_and_shape(
    tenant_id: Option<&str>,
    object_key: &str,
    lease_timeout_ms: u64,
    shape: SyntheticCppMetadataShape,
) -> (
    tempfile::TempDir,
    CatalogBackedSnapshotProvider,
    Uuid,
    Uuid,
    Uuid,
) {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let mut descriptor = SnapshotDescriptor::new("20260610_120000_001");
    descriptor.last_included_seq = 42;
    catalog.publish(&descriptor).unwrap();
    object_store
        .upload_string(
            &descriptor.manifest_key,
            &format!("messagepack|1.0.0|{}", descriptor.snapshot_id),
        )
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}segments", descriptor.object_prefix),
            &synthetic_cpp_segments(segment_id, client_id),
        )
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}metadata", descriptor.object_prefix),
            &synthetic_cpp_metadata_with_shape(
                segment_id,
                client_id,
                tenant_id,
                object_key,
                lease_timeout_ms,
                shape,
            ),
        )
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}task_manager", descriptor.object_prefix),
            &synthetic_cpp_tasks(task_id, client_id),
        )
        .unwrap();
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    (root, provider, segment_id, client_id, task_id)
}

/// Builds the C++ default-test metadata payload from
/// `ha/snapshot/snapshot_test_utils.h`: a single default object owned by
/// `UUID{1, 2}` with one complete DISK replica whose file path and object size
/// must survive the format-agnostic decode. The client UUID is written in the
/// C++ `{high}-{low}` decimal-pair shape, and the replica is the exact
/// `PackDiskReplica` array.
fn synthetic_cpp_metadata_with_disk_replica(
    lease_timeout_ms: u64,
    shape: SyntheticCppMetadataShape,
) -> Vec<u8> {
    let include_data_type = matches!(
        shape,
        SyntheticCppMetadataShape::V2DataType
            | SyntheticCppMetadataShape::V3DataTypeHardPinned
            | SyntheticCppMetadataShape::CurrentV4
    );
    let include_hard_pinned = matches!(
        shape,
        SyntheticCppMetadataShape::V2HardPinned
            | SyntheticCppMetadataShape::V3DataTypeHardPinned
            | SyntheticCppMetadataShape::CurrentV4
    );
    let include_group_id = matches!(shape, SyntheticCppMetadataShape::CurrentV4);
    let mut metadata = vec![
        "1-2".into(),
        1_700_000_000_000_u64.into(),
        4096_u64.into(),
        lease_timeout_ms.into(),
        false.into(),
        0_u64.into(),
        1_u64.into(),
    ];
    if include_data_type {
        metadata.push((ObjectDataType::Tensor as u64).into());
    }
    metadata.push(Value::Array(vec![
        1_u64.into(),
        (ReplicaStatus::Complete as i32 as i64).into(),
        (ReplicaType::Disk as i32 as i64).into(),
        Value::Array(vec![
            "/tmp/mooncake_snapshot_disk.data".into(),
            4096_u64.into(),
        ]),
    ]));
    if include_hard_pinned {
        metadata.push(true.into());
    }
    if include_group_id {
        metadata.push("test-group".into());
    }
    let item = Value::Array(vec!["key-1".into(), Value::Array(metadata)]);
    let shard = Value::Map(vec![("metadata".into(), Value::Array(vec![item]))]);
    encode(&Value::Map(vec![(
        "shards".into(),
        Value::Map(vec![(0.into(), Value::Binary(compress(&shard)))]),
    )]))
}

fn synthetic_cpp_metadata_with_declared_replica_count(replica_count: u64) -> Vec<u8> {
    let metadata = Value::Array(vec![
        "1-2".into(),
        1_700_000_000_000_u64.into(),
        4096_u64.into(),
        4_102_444_800_000_u64.into(),
        false.into(),
        0_u64.into(),
        replica_count.into(),
    ]);
    let item = Value::Array(vec!["key-1".into(), metadata]);
    let shard = Value::Map(vec![("metadata".into(), Value::Array(vec![item]))]);
    encode(&Value::Map(vec![(
        "shards".into(),
        Value::Map(vec![("0".into(), Value::Binary(compress(&shard)))]),
    )]))
}

fn publish_disk_replica_fixture(
    lease_timeout_ms: u64,
    shape: SyntheticCppMetadataShape,
) -> (tempfile::TempDir, CatalogBackedSnapshotProvider) {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let mut descriptor = SnapshotDescriptor::new("20260610_120000_001");
    descriptor.last_included_seq = 42;
    catalog.publish(&descriptor).unwrap();
    object_store
        .upload_string(
            &descriptor.manifest_key,
            &format!("messagepack|1.0.0|{}", descriptor.snapshot_id),
        )
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}segments", descriptor.object_prefix),
            &synthetic_cpp_segments(segment_id, client_id),
        )
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}metadata", descriptor.object_prefix),
            &synthetic_cpp_metadata_with_disk_replica(lease_timeout_ms, shape),
        )
        .unwrap();
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    (root, provider)
}

/// C++ CatalogBackedSnapshotProviderTest.LoadLatestSnapshotWith* pins every
/// historical metadata layout and requires the default object plus its
/// complete DISK replica to round-trip intact after a catalog snapshot load.
#[test]
fn cpp_parity_catalog_provider_loads_default_disk_object_for_each_metadata_shape() {
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    for (shape, expected_data_type, expected_hard_pinned, expected_group_id) in [
        (
            SyntheticCppMetadataShape::V2DataType,
            ObjectDataType::Tensor,
            false,
            "",
        ),
        (
            SyntheticCppMetadataShape::V2HardPinned,
            ObjectDataType::Unknown,
            true,
            "",
        ),
        (
            SyntheticCppMetadataShape::V3DataTypeHardPinned,
            ObjectDataType::Tensor,
            true,
            "",
        ),
        (
            SyntheticCppMetadataShape::CurrentV4,
            ObjectDataType::Tensor,
            true,
            "test-group",
        ),
    ] {
        let (_root, provider) = publish_disk_replica_fixture(future_ms, shape);
        let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
        assert_eq!(snapshot.objects.len(), 1);
        let (scoped_key, object) = &snapshot.objects[0];
        assert_eq!(scoped_key, "default\0key-1");
        assert_eq!(object.user_key, "key-1");
        assert_eq!(object.tenant_id, TenantId::default());
        assert_eq!(object.client_id, Uuid::from_u64_pair(1, 2));
        assert_eq!(object.size, 4096);
        assert_eq!(object.data_type, expected_data_type);
        assert_eq!(object.hard_pinned, expected_hard_pinned);
        assert_eq!(object.group_id, expected_group_id);
        assert_eq!(object.replicas.len(), 1);
        let replica = &object.replicas[0];
        assert_eq!(replica.status, ReplicaStatus::Complete);
        assert_eq!(replica.replica_type, ReplicaType::Disk);
        assert_eq!(replica.segment_name, "/tmp/mooncake_snapshot_disk.data");
        assert_eq!(replica.size, 4096);
    }
}

#[test]
fn cpp_parity_catalog_provider_rejects_overflowing_replica_count() {
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let (root, provider) =
        publish_disk_replica_fixture(future_ms, SyntheticCppMetadataShape::CurrentV4);
    std::fs::write(
        root.path()
            .join("mooncake_master_snapshot/20260610_120000_001/metadata"),
        synthetic_cpp_metadata_with_declared_replica_count(u32::MAX as u64),
    )
    .unwrap();

    let error = provider.load_latest_snapshot("cluster-a").unwrap_err();
    assert!(matches!(error, HaError::Snapshot(_)));
    assert!(error.to_string().contains("replica count mismatch"));
}

#[test]
fn test_catalog_provider_loads_synthetic_cpp_wire_shapes() {
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let (_root, provider, segment_id, client_id, task_id) = publish_fixture(future_ms);

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(snapshot.snapshot_sequence_id, 42);
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(snapshot.segments[0].segment.id, segment_id);
    assert_eq!(snapshot.segments[0].segment.host_id, "node-a");
    assert_eq!(snapshot.segments[0].client_id, client_id);
    assert_eq!(snapshot.segments[0].used, 512);
    assert_eq!(snapshot.segments[0].status, SegmentStatus::Active);
    assert_eq!(snapshot.objects.len(), 1);
    assert_eq!(snapshot.objects[0].0, "tenant-a\0key-a");
    let object = &snapshot.objects[0].1;
    assert_eq!(object.data_type, ObjectDataType::Kvcache);
    assert!(object.hard_pinned);
    assert_eq!(object.group_id, "group-a");
    assert_eq!(object.replicas[0].replica_type, ReplicaType::Memory);
    assert_eq!(object.replicas[0].offset, 0x100);
    assert_eq!(object.replicas[0].base_addr, 0x1000);
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].info.id, task_id);
    assert!(
        snapshot.graceful_unmounts.is_empty(),
        "C++/legacy snapshots without the Rust sidecar remain readable"
    );
    assert_eq!(
        snapshot.tasks[0].info.task_type,
        mooncake_store_core::TaskType::ReplicaMove
    );
    assert_eq!(
        snapshot.tasks[0].info.status,
        mooncake_store_core::TaskStatus::Pending
    );
    assert_eq!(snapshot.tasks[0].key, "tenant-a\0key-a");
    assert_eq!(snapshot.local_disk_segments.len(), 1);
    assert_eq!(snapshot.local_disk_segments[0].client_id, client_id);
    assert_eq!(
        snapshot.local_disk_segments[0].offloading_objects,
        std::collections::HashMap::from([("tenant-a\0key-a".to_string(), 128)])
    );
    assert_eq!(
        snapshot.local_disk_segments[0].ssd_total_capacity_bytes,
        4096
    );
}

#[test]
fn test_catalog_provider_restores_synthetic_cpp_discarded_replica() {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let (root, provider, segment_id, client_id, _) = publish_fixture(now_ms + 60_000);
    let metadata_path = root
        .path()
        .join("mooncake_master_snapshot/20260610_120000_001/metadata");
    std::fs::write(
        metadata_path,
        synthetic_cpp_metadata_with_discarded_replica(
            segment_id,
            client_id,
            now_ms + 60_000,
            now_ms + 600_000,
        ),
    )
    .unwrap();

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(snapshot.delayed_replica_releases.len(), 1);
    let delayed = &snapshot.delayed_replica_releases[0];
    assert_eq!(delayed.deadline_epoch_ms, now_ms + 600_000);
    assert_eq!(delayed.replicas[0].segment_id, segment_id);
    assert_eq!(delayed.replicas[0].offset, 0x200);
    assert_eq!(delayed.replicas[0].status, ReplicaStatus::Allocating);
}

#[test]
fn test_catalog_provider_decodes_all_cxx_metadata_shapes() {
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    for (shape, expected_data_type, expected_hard_pinned, expected_group_id) in [
        (
            SyntheticCppMetadataShape::V1,
            ObjectDataType::Unknown,
            false,
            "",
        ),
        (
            SyntheticCppMetadataShape::V2DataType,
            ObjectDataType::Kvcache,
            false,
            "",
        ),
        (
            SyntheticCppMetadataShape::V2HardPinned,
            ObjectDataType::Unknown,
            true,
            "",
        ),
        (
            SyntheticCppMetadataShape::V3DataTypeHardPinned,
            ObjectDataType::Kvcache,
            true,
            "",
        ),
        (
            SyntheticCppMetadataShape::CurrentV4,
            ObjectDataType::Kvcache,
            true,
            "group-a",
        ),
    ] {
        let (_root, provider, _, _, _) =
            publish_fixture_with_identity_and_shape(Some("tenant-a"), "key-a", future_ms, shape);
        let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
        let object = &snapshot.objects[0].1;
        assert_eq!(object.data_type, expected_data_type);
        assert_eq!(object.hard_pinned, expected_hard_pinned);
        assert_eq!(object.group_id, expected_group_id);
    }
}

#[test]
fn test_catalog_provider_keeps_legacy_local_disk_generation_missing() {
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let (root, provider, _, client_id, _) = publish_fixture(future_ms);
    std::fs::write(
        root.path()
            .join("mooncake_master_snapshot/20260610_120000_001/metadata"),
        synthetic_cpp_local_disk_metadata(client_id, future_ms),
    )
    .unwrap();

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(snapshot.objects.len(), 1);
    let replica = &snapshot.objects[0].1.replicas[0];
    assert_eq!(replica.replica_type, ReplicaType::LocalDisk);
    assert_eq!(replica.holder_client_id, Some(client_id));
    assert_eq!(replica.local_disk_storage_id, None);
    assert_eq!(
        replica.local_disk_generation_id, None,
        "legacy/C++ snapshots must not invent a byte generation"
    );
}

#[test]
fn test_catalog_provider_normalizes_empty_tenant_metadata() {
    let (_root, provider, _, _, _) =
        publish_fixture_with_identity(Some(""), "legacy-key", u64::MAX / 2);

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(snapshot.objects[0].0, "default\0legacy-key");
    assert_eq!(snapshot.objects[0].1.tenant_id, TenantId::default());
}

#[test]
fn test_catalog_provider_rejects_invalid_explicit_tenant_metadata() {
    let (_root, provider, _, _, _) =
        publish_fixture_with_identity(Some("_reserved"), "key-a", u64::MAX / 2);

    let error = provider.load_latest_snapshot("cluster-a").unwrap_err();

    assert!(error.to_string().contains("tenant"), "{error}");
}

#[test]
fn test_catalog_provider_skips_invalid_client_id_metadata_record() {
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let (root, provider, segment_id, client_id, _) =
        publish_fixture_with_identity(Some("tenant-a"), "seed-key", future_ms);

    let replica = Value::Array(vec![
        7_u64.into(),
        3.into(),
        0.into(),
        Value::Array(vec![
            128_u64.into(),
            0x1100_u64.into(),
            segment_id.to_string().into(),
            false.into(),
            Value::Nil,
        ]),
    ]);
    let invalid_item = Value::Array(vec![
        "tenant-a".into(),
        "invalid-client-id".into(),
        Value::Array(vec![
            "not-a-valid-client-uuid".into(),
            1000_u64.into(),
            128_u64.into(),
            future_ms.into(),
            false.into(),
            0_u64.into(),
            1_u64.into(),
            (ObjectDataType::Kvcache as u64).into(),
            replica.clone(),
        ]),
    ]);
    let valid_item = Value::Array(vec![
        "tenant-a".into(),
        "valid-object-key".into(),
        Value::Array(vec![
            client_id.to_string().into(),
            1000_u64.into(),
            128_u64.into(),
            future_ms.into(),
            false.into(),
            0_u64.into(),
            1_u64.into(),
            (ObjectDataType::Kvcache as u64).into(),
            replica,
        ]),
    ]);
    let shard = Value::Map(vec![(
        "metadata".into(),
        Value::Array(vec![invalid_item, valid_item]),
    )]);
    let metadata_path = root
        .path()
        .join("mooncake_master_snapshot/20260610_120000_001/metadata");
    std::fs::write(
        metadata_path,
        encode(&Value::Map(vec![(
            "shards".into(),
            Value::Map(vec![(0.into(), Value::Binary(compress(&shard)))]),
        )])),
    )
    .unwrap();

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(snapshot.objects.len(), 1);
    assert_eq!(snapshot.objects[0].0, "tenant-a\0valid-object-key");
}

#[test]
fn test_catalog_provider_parses_legacy_scoped_key_identity() {
    let (_root, provider, _, _, _) =
        publish_fixture_with_identity(None, "tenant-a\0key-a", u64::MAX / 2);

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(snapshot.objects[0].0, "tenant-a\0key-a");
    assert_eq!(snapshot.objects[0].1.tenant_id.as_str(), "tenant-a");
    assert_eq!(snapshot.objects[0].1.user_key, "key-a");
}

#[test]
fn test_catalog_provider_skips_expired_unpinned_objects() {
    let (_root, provider, _, _, _) = publish_fixture_with_identity_and_shape(
        Some("tenant-a"),
        "expired-unpinned",
        1,
        SyntheticCppMetadataShape::V2DataType,
    );

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert!(snapshot.objects.is_empty());
}

#[test]
fn test_catalog_provider_preserves_expired_hard_pinned_objects() {
    let (_root, provider, _, _, _) = publish_fixture_with_identity_and_shape(
        Some("tenant-a"),
        "hard-pinned",
        1,
        SyntheticCppMetadataShape::V2HardPinned,
    );

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(snapshot.objects.len(), 1);
    assert_eq!(snapshot.objects[0].0, "tenant-a\0hard-pinned");
    assert!(snapshot.objects[0].1.hard_pinned);
}

#[test]
fn cpp_parity_catalog_provider_rejects_cluster_mismatch() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let provider = CatalogBackedSnapshotProvider::new(
        "cluster-a",
        Box::new(UnexpectedCatalogAccess),
        object_store,
    );

    assert!(matches!(
        provider.load_latest_snapshot("cluster-b"),
        Err(HaError::InvalidParams(_))
    ));
}

#[test]
fn test_catalog_provider_accepts_legacy_snapshot_without_tasks() {
    let (root, provider, _, _, _) = publish_fixture(u64::MAX / 2);
    std::fs::remove_file(
        root.path()
            .join("mooncake_master_snapshot/20260610_120000_001/task_manager"),
    )
    .unwrap();

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert!(snapshot.tasks.is_empty());
}

#[test]
fn test_catalog_provider_round_trips_three_field_user_key_with_embedded_nul() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    let tenant_id = TenantId::new("tenant-a".to_string()).unwrap();
    let user_key = "part1\0part2";
    let now = SystemTime::now();
    let snapshot = LoadedSnapshot {
        snapshot_id: "20260610_120002_003".to_string(),
        snapshot_sequence_id: 77,
        allocator_config: None,
        segments: Vec::new(),
        nof_segments: Vec::new(),
        objects: vec![(
            tenant_id.make_scoped_key(user_key),
            ObjectEntry {
                replicas: vec![ReplicaDescriptor {
                    segment_id: Uuid::nil(),
                    segment_name: "disk-a".to_string(),
                    offset: 0,
                    size: 128,
                    status: ReplicaStatus::Complete,
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
                last_access: now,
                hard_pinned: true,
                data_type: ObjectDataType::Kvcache,
                client_id: Uuid::new_v4(),
                put_start_time: Some(now),
                lease_timeout: Some(now + std::time::Duration::from_secs(60)),
                soft_pin_timeout: None,
                tenant_id: tenant_id.clone(),
                group_id: "group-a".to_string(),
                quota_committed: true,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: user_key.to_string(),
            },
        )],
        tasks: Vec::new(),
        replication_tasks: Vec::new(),
        graceful_unmounts: Vec::new(),
        delayed_replica_releases: Vec::new(),
        local_disk_segments: Vec::new(),
    };

    provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(loaded.objects[0].0, "tenant-a\0part1\0part2");
    assert_eq!(loaded.objects[0].1.tenant_id, tenant_id);
    assert_eq!(loaded.objects[0].1.user_key, user_key);
}

#[test]
fn test_catalog_provider_publishes_cpp_compatible_snapshot_payloads() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider =
        CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store.clone());
    let segment_id = Uuid::new_v4();
    let target_segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let local_disk_storage_id = Uuid::new_v4();
    let local_disk_generation_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let now = SystemTime::now();
    let replication_source = ReplicaDescriptor {
        segment_id,
        segment_name: "segment-a".to_string(),
        offset: 0x100,
        size: 128,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
        holder_client_id: Some(client_id),
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        refcnt: 0,
        handle_valid: true,
        base_addr: 0x1000,
        protocol: "tcp".to_string(),
    };
    let replication_existing_target = ReplicaDescriptor {
        segment_id: target_segment_id,
        segment_name: "segment-b".to_string(),
        offset: 0,
        size: 128,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
        holder_client_id: Some(client_id),
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        refcnt: 0,
        handle_valid: true,
        base_addr: 0x2000,
        protocol: "tcp".to_string(),
    };
    let snapshot = LoadedSnapshot {
        snapshot_id: "20260610_120001_002".to_string(),
        snapshot_sequence_id: 77,
        allocator_config: Some(AllocatorSnapshotConfig {
            allocation_strategy: AllocationStrategy::FreeRatioFirst,
            memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
            offset_max_allocation_nodes: None,
        }),
        segments: vec![
            SegmentEntry {
                segment: Segment {
                    id: segment_id,
                    name: "segment-a".to_string(),
                    base: 0x1000,
                    size: 4096,
                    te_endpoint: "tcp://node-a".to_string(),
                    protocol: "tcp".to_string(),
                    host_id: String::new(),
                },
                used: 512,
                client_id,
                status: SegmentStatus::GracefullyUnmounting,
            },
            SegmentEntry {
                segment: Segment {
                    id: target_segment_id,
                    name: "segment-b".to_string(),
                    base: 0x2000,
                    size: 4096,
                    te_endpoint: "tcp://node-b".to_string(),
                    protocol: "tcp".to_string(),
                    host_id: String::new(),
                },
                used: 128,
                client_id,
                status: SegmentStatus::Active,
            },
        ],
        nof_segments: Vec::new(),
        objects: vec![(
            "tenant-a\0key-a".to_string(),
            ObjectEntry {
                replicas: vec![
                    replication_source.clone(),
                    replication_existing_target.clone(),
                    ReplicaDescriptor {
                        segment_id: Uuid::nil(),
                        segment_name: "local://disk-a".to_string(),
                        offset: 0,
                        size: 128,
                        status: ReplicaStatus::Complete,
                        replica_type: ReplicaType::LocalDisk,
                        holder_client_id: Some(client_id),
                        local_disk_storage_id: Some(local_disk_storage_id),
                        local_disk_generation_id: Some(local_disk_generation_id),
                        refcnt: 0,
                        handle_valid: true,
                        base_addr: 0,
                        protocol: String::new(),
                    },
                ],
                size: 128,
                last_access: now,
                hard_pinned: true,
                data_type: ObjectDataType::Kvcache,
                client_id,
                put_start_time: Some(now),
                lease_timeout: Some(now + std::time::Duration::from_secs(60)),
                soft_pin_timeout: None,
                tenant_id: TenantId::new("tenant-a".to_string()).unwrap(),
                group_id: "group-a".to_string(),
                quota_committed: true,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "key-a".to_string(),
            },
        )],
        tasks: vec![TaskEntry {
            info: TaskInfo {
                id: task_id,
                task_type: TaskType::ReplicaMove,
                status: TaskStatus::Failed,
                created_at: Utc::now(),
                last_updated_at: Utc::now(),
                assigned_client: Some(client_id),
                message: "failed move".to_string(),
            },
            key: "tenant-a\0key-a".to_string(),
            payload: r#"{"key":"tenant-a\u0000key-a","source":"a","target":"b"}"#.to_string(),
            max_retry_attempts: 3,
        }],
        replication_tasks: vec![ReplicationTaskSnapshotEntry {
            key: "tenant-a\0key-a".to_string(),
            client_id,
            start_age_millis: 25,
            kind: ReplicationTaskKind::Move,
            source: replication_source,
            targets: Vec::new(),
            existing_move_target: Some(replication_existing_target),
            reserved_quota_charge_bytes: 0,
        }],
        graceful_unmounts: vec![GracefulUnmountSnapshotEntry {
            segment_id,
            client_id,
            deadline_epoch_ms: 1_900_000_000_000,
        }],
        delayed_replica_releases: vec![DelayedReplicaReleaseEntry {
            id: Uuid::new_v4(),
            scoped_key: "tenant-a\0retired-key".to_string(),
            deadline_epoch_ms: 1_900_000_000_500,
            replicas: vec![ReplicaDescriptor {
                segment_id,
                segment_name: "segment-a".to_string(),
                offset: 0x300,
                size: 128,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
                holder_client_id: Some(client_id),
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 0x1000,
                protocol: "tcp".to_string(),
            }],
        }],
        local_disk_segments: vec![LocalDiskSnapshotEntry {
            storage_id: local_disk_storage_id,
            client_id,
            enable_offloading: true,
            offloading_objects: std::collections::HashMap::from([(
                "tenant-a\0key-a".to_string(),
                128,
            )]),
            ssd_total_capacity_bytes: 8 * 1024 * 1024,
        }],
    };

    let descriptor = provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    assert_eq!(descriptor.snapshot_id, "20260610_120001_002");
    assert_eq!(descriptor.last_included_seq, 77);
    assert_eq!(descriptor.producer_view_version, 9);
    assert_eq!(
        object_store
            .download_string(&descriptor.manifest_key)
            .unwrap(),
        "messagepack|1.0.0|20260610_120001_002|rust_allocator_config_v1"
    );

    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(loaded.snapshot_id, "20260610_120001_002");
    assert_eq!(loaded.local_disk_segments.len(), 1);
    assert_eq!(loaded.local_disk_segments[0].client_id, client_id);
    assert_eq!(
        loaded.local_disk_segments[0].ssd_total_capacity_bytes,
        8 * 1024 * 1024
    );
    assert_eq!(loaded.snapshot_sequence_id, 77);
    assert_eq!(loaded.allocator_config, snapshot.allocator_config);
    let loaded_segment = loaded
        .segments
        .iter()
        .find(|entry| entry.segment.id == segment_id)
        .expect("published Memory segment must round-trip by durable UUID");
    assert_eq!(loaded_segment.used, 512);
    assert_eq!(loaded.objects.len(), 1);
    assert_eq!(loaded.objects[0].0, "tenant-a\0key-a");
    assert_eq!(loaded.objects[0].1.replicas[0].offset, 0x100);
    let local_disk = loaded.objects[0]
        .1
        .replicas
        .iter()
        .find(|replica| replica.replica_type == ReplicaType::LocalDisk)
        .unwrap();
    assert_eq!(local_disk.replica_type, ReplicaType::LocalDisk);
    assert_eq!(
        local_disk.local_disk_storage_id,
        Some(local_disk_storage_id)
    );
    assert_eq!(
        local_disk.local_disk_generation_id,
        Some(local_disk_generation_id)
    );
    assert_eq!(loaded.objects[0].1.group_id, "group-a");
    assert_eq!(loaded.tasks.len(), 1);
    assert_eq!(loaded.tasks[0].info.id, task_id);
    assert_eq!(loaded.tasks[0].info.status, TaskStatus::Failed);
    assert_eq!(loaded.replication_tasks.len(), 1);
    assert_eq!(loaded.replication_tasks[0].kind, ReplicationTaskKind::Move);
    assert_eq!(
        loaded.replication_tasks[0]
            .existing_move_target
            .as_ref()
            .map(|target| target.segment_id),
        Some(target_segment_id)
    );
    assert_eq!(
        loaded.graceful_unmounts,
        vec![GracefulUnmountSnapshotEntry {
            segment_id,
            client_id,
            deadline_epoch_ms: 1_900_000_000_000,
        }]
    );
    assert_eq!(loaded.delayed_replica_releases.len(), 1);
    assert_eq!(loaded.delayed_replica_releases[0].replicas[0].offset, 0x300);
}

#[test]
fn test_catalog_provider_restores_cxl_protocol_from_allocator_extension() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    let segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let snapshot = LoadedSnapshot {
        snapshot_id: "20260610_120002_003".to_string(),
        snapshot_sequence_id: 78,
        allocator_config: Some(AllocatorSnapshotConfig {
            allocation_strategy: AllocationStrategy::Cxl,
            memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
            offset_max_allocation_nodes: None,
        }),
        segments: vec![SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: "cxl-alias-a".to_string(),
                base: 0,
                size: 4 * 1024 * 1024,
                te_endpoint: "cxl://node-a".to_string(),
                protocol: "cxl".to_string(),
                host_id: "node-a".to_string(),
            },
            used: 0,
            client_id,
            status: SegmentStatus::Active,
        }],
        nof_segments: Vec::new(),
        objects: Vec::new(),
        tasks: Vec::new(),
        replication_tasks: Vec::new(),
        graceful_unmounts: Vec::new(),
        delayed_replica_releases: Vec::new(),
        local_disk_segments: Vec::new(),
    };

    provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(loaded.allocator_config, snapshot.allocator_config);
    assert_eq!(loaded.segments.len(), 1);
    assert_eq!(loaded.segments[0].segment.id, segment_id);
    assert_eq!(loaded.segments[0].segment.protocol, "cxl");
}

#[test]
fn test_catalog_provider_round_trips_offset_node_limit() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    let mut snapshot = empty_loaded_snapshot("20260610_120002_004", 79);
    snapshot.allocator_config = Some(AllocatorSnapshotConfig {
        allocation_strategy: AllocationStrategy::Random,
        memory_allocator_kind: MemoryAllocatorKind::Offset,
        offset_max_allocation_nodes: Some(4),
    });

    provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert_eq!(loaded.allocator_config, snapshot.allocator_config);
}

#[test]
fn test_catalog_provider_rejects_rust_snapshot_missing_required_allocator_extension() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider =
        CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store.clone());
    let mut snapshot = empty_loaded_snapshot("20260610_120002_005", 80);
    snapshot.allocator_config = Some(AllocatorSnapshotConfig {
        allocation_strategy: AllocationStrategy::Random,
        memory_allocator_kind: MemoryAllocatorKind::Offset,
        offset_max_allocation_nodes: Some(4),
    });
    let descriptor = provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    std::fs::remove_file(root.path().join(format!(
        "{}rust_allocator_config_v1",
        descriptor.object_prefix
    )))
    .unwrap();

    let error = provider.load_latest_snapshot("cluster-a").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("required Rust allocator configuration extension is missing"),
        "unexpected error: {error}"
    );
}

#[test]
fn test_catalog_provider_falls_back_when_latest_manifest_is_corrupt() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider =
        CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store.clone());
    let older = empty_loaded_snapshot("20260610_120002_003", 77);
    let latest = empty_loaded_snapshot("20260610_120003_004", 78);

    provider.publish_loaded_snapshot(&older, 9).unwrap();
    let latest_descriptor = provider.publish_loaded_snapshot(&latest, 9).unwrap();
    object_store
        .upload_string(
            &latest_descriptor.manifest_key,
            "messagepack|1.0.0|wrong-snapshot",
        )
        .unwrap();

    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(loaded.snapshot_id, older.snapshot_id);
    assert_eq!(loaded.snapshot_sequence_id, older.snapshot_sequence_id);
}

#[test]
fn test_catalog_provider_rejects_late_old_term_latest_marker_for_restore_and_retention() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    let successor = empty_loaded_snapshot("20260610_120001_001", 80);
    let stale_predecessor = empty_loaded_snapshot("20260610_120002_002", 79);

    provider.publish_loaded_snapshot(&successor, 10).unwrap();
    // Simulate a predecessor whose synchronous object-store publication
    // completes after the successor and overwrites the marker.
    provider
        .publish_loaded_snapshot(&stale_predecessor, 9)
        .unwrap();

    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(loaded.snapshot_id, successor.snapshot_id);
    assert_eq!(loaded.snapshot_sequence_id, successor.snapshot_sequence_id);

    provider.prune_snapshots(1).unwrap();
    let loaded_after_prune = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(loaded_after_prune.snapshot_id, successor.snapshot_id);
    assert!(
        !root
            .path()
            .join(format!(
                "mooncake_master_snapshot/{}/manifest.txt",
                stale_predecessor.snapshot_id
            ))
            .exists()
    );
}

#[test]
fn test_embedded_catalog_scopes_object_keys_by_cluster_id() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog =
        EmbeddedSnapshotCatalogStore::with_object_store_and_cluster_id(object_store, "cluster-a");
    let mut descriptor = SnapshotDescriptor::new_with_snapshot_root(
        catalog.get_snapshot_root(),
        "20260610_120001_002",
    );
    descriptor.last_included_seq = 77;

    catalog.publish(&descriptor).unwrap();

    assert_eq!(
        catalog.get_snapshot_root(),
        "mooncake_master_snapshot/cluster-a/"
    );
    assert!(
        root.path()
            .join("mooncake_master_snapshot/cluster-a/latest.txt")
            .exists()
    );
    assert!(
        root.path()
            .join("mooncake_master_snapshot/cluster-a/20260610_120001_002/descriptor.txt")
            .exists()
    );
    assert!(
        !root
            .path()
            .join("mooncake_master_snapshot/latest.txt")
            .exists()
    );
}

#[test]
fn test_catalog_provider_factory_publishes_cluster_scoped_snapshot_objects() {
    let root = tempdir().unwrap();
    let provider = create_catalog_backed_snapshot_provider(
        "cluster-a",
        SnapshotObjectStoreType::Local,
        SnapshotCatalogStoreType::Embedded,
        Some(root.path().to_path_buf()),
        None,
    )
    .unwrap();
    let snapshot = LoadedSnapshot {
        snapshot_id: "20260610_120003_004".to_string(),
        snapshot_sequence_id: 11,
        allocator_config: None,
        segments: Vec::new(),
        nof_segments: Vec::new(),
        objects: Vec::new(),
        tasks: Vec::new(),
        replication_tasks: Vec::new(),
        graceful_unmounts: Vec::new(),
        delayed_replica_releases: Vec::new(),
        local_disk_segments: Vec::new(),
    };

    let descriptor = provider.publish_loaded_snapshot(&snapshot, 1).unwrap();

    assert_eq!(
        descriptor.object_prefix,
        "mooncake_master_snapshot/cluster-a/20260610_120003_004/"
    );
    assert!(
        root.path()
            .join("mooncake_master_snapshot/cluster-a/latest.txt")
            .exists()
    );
    assert!(
        root.path()
            .join("mooncake_master_snapshot/cluster-a/20260610_120003_004/manifest.txt")
            .exists()
    );
    assert!(
        !root
            .path()
            .join("mooncake_master_snapshot/20260610_120003_004/manifest.txt")
            .exists()
    );
}

#[test]
fn test_catalog_provider_prunes_old_snapshots() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    for snapshot_id in [
        "20260610_120000_001",
        "20260610_120001_002",
        "20260610_120002_003",
    ] {
        let snapshot = LoadedSnapshot {
            snapshot_id: snapshot_id.to_string(),
            snapshot_sequence_id: 1,
            allocator_config: None,
            segments: Vec::new(),
            nof_segments: Vec::new(),
            objects: Vec::new(),
            tasks: Vec::new(),
            replication_tasks: Vec::new(),
            graceful_unmounts: Vec::new(),
            delayed_replica_releases: Vec::new(),
            local_disk_segments: Vec::new(),
        };
        provider.publish_loaded_snapshot(&snapshot, 1).unwrap();
    }

    provider.prune_snapshots(2).unwrap();

    let objects = std::fs::read_dir(root.path().join("mooncake_master_snapshot"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(!objects.contains(&"20260610_120000_001".to_string()));
    assert!(objects.contains(&"20260610_120001_002".to_string()));
    assert!(objects.contains(&"20260610_120002_003".to_string()));
    let latest = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(latest.snapshot_id, "20260610_120002_003");
}

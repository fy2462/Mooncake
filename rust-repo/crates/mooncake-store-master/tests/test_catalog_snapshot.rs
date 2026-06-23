use chrono::Utc;
use mooncake_store_core::{
    ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment, TaskInfo, TaskStatus,
    TaskType,
};
use mooncake_store_master::ha::{
    CatalogBackedSnapshotProvider, EmbeddedSnapshotCatalogStore, LoadedSnapshot,
    LocalFileSnapshotObjectStore, SnapshotCatalogStore, SnapshotDescriptor, SnapshotObjectStore,
    SnapshotProvider,
};
use mooncake_store_master::proto::SegmentStatus;
use mooncake_store_master::service::{ObjectEntry, SegmentEntry, TaskEntry};
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

fn cxx_segments(segment_id: Uuid, client_id: Uuid) -> Vec<u8> {
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
        ("ld".into(), Value::Map(vec![])),
    ]))
}

fn cxx_metadata(segment_id: Uuid, client_id: Uuid, lease_timeout_ms: u64) -> Vec<u8> {
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
            "key-a".into(),
            metadata,
        ])]),
    )]);
    encode(&Value::Map(vec![(
        "shards".into(),
        Value::Map(vec![(0.into(), Value::Binary(compress(&shard)))]),
    )]))
}

fn cxx_tasks(task_id: Uuid, client_id: Uuid) -> Vec<u8> {
    compress(&Value::Array(vec![Value::Array(vec![
        task_id.to_string().into(),
        1.into(),
        0.into(),
        r#"{"key":"tenant-a\u0000key-a","source":"a","target":"b"}"#.into(),
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
        .upload_string(&descriptor.manifest_key, "messagepack|1.0.0|ignored")
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}segments", descriptor.object_prefix),
            &cxx_segments(segment_id, client_id),
        )
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}metadata", descriptor.object_prefix),
            &cxx_metadata(segment_id, client_id, lease_timeout_ms),
        )
        .unwrap();
    object_store
        .upload_buffer(
            &format!("{}task_manager", descriptor.object_prefix),
            &cxx_tasks(task_id, client_id),
        )
        .unwrap();
    let provider = CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store);
    (root, provider, segment_id, client_id, task_id)
}

#[test]
fn test_catalog_provider_loads_cxx_snapshot_payloads() {
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
    assert_eq!(
        snapshot.tasks[0].info.task_type,
        mooncake_store_core::TaskType::ReplicaMove
    );
    assert_eq!(
        snapshot.tasks[0].info.status,
        mooncake_store_core::TaskStatus::Pending
    );
    assert_eq!(snapshot.tasks[0].key, "tenant-a\0key-a");
}

#[test]
fn test_catalog_provider_skips_expired_unpinned_objects() {
    let (_root, provider, _, _, _) = publish_fixture(1);

    let snapshot = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();

    assert!(snapshot.objects.is_empty());
}

#[test]
fn test_catalog_provider_rejects_cluster_mismatch() {
    let (_root, provider, _, _, _) = publish_fixture(u64::MAX / 2);

    assert!(provider.load_latest_snapshot("cluster-b").is_err());
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
fn test_catalog_provider_publishes_cpp_compatible_snapshot_payloads() {
    let root = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSnapshotObjectStore::new(root.path().to_path_buf()));
    let catalog = EmbeddedSnapshotCatalogStore::with_object_store(object_store.clone());
    let provider =
        CatalogBackedSnapshotProvider::new("cluster-a", Box::new(catalog), object_store.clone());
    let segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let now = SystemTime::now();
    let snapshot = LoadedSnapshot {
        snapshot_id: "20260610_120001_002".to_string(),
        snapshot_sequence_id: 77,
        segments: vec![SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: "segment-a".to_string(),
                base: 0x1000,
                size: 4096,
                te_endpoint: "tcp://node-a".to_string(),
                protocol: "tcp".to_string(),
            },
            used: 512,
            client_id,
            status: SegmentStatus::Active,
        }],
        nof_segments: Vec::new(),
        objects: vec![(
            "tenant-a\0key-a".to_string(),
            ObjectEntry {
                replicas: vec![ReplicaDescriptor {
                    segment_id,
                    segment_name: "segment-a".to_string(),
                    offset: 0x100,
                    size: 128,
                    status: ReplicaStatus::Complete,
                    replica_type: ReplicaType::Memory,
                    holder_client_id: Some(client_id),
                    refcnt: 0,
                    handle_valid: true,
                    base_addr: 0x1000,
                }],
                size: 128,
                last_access: now,
                hard_pinned: true,
                data_type: ObjectDataType::Kvcache,
                client_id,
                put_start_time: Some(now),
                lease_timeout: Some(now + std::time::Duration::from_secs(60)),
                soft_pin_timeout: None,
                tenant_id: "tenant-a".to_string(),
                group_id: "group-a".to_string(),
                quota_committed: true,
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
    };

    let descriptor = provider.publish_loaded_snapshot(&snapshot, 9).unwrap();
    assert_eq!(descriptor.snapshot_id, "20260610_120001_002");
    assert_eq!(descriptor.last_included_seq, 77);
    assert_eq!(descriptor.producer_view_version, 9);

    let loaded = provider.load_latest_snapshot("cluster-a").unwrap().unwrap();
    assert_eq!(loaded.snapshot_id, "20260610_120001_002");
    assert_eq!(loaded.snapshot_sequence_id, 77);
    assert_eq!(loaded.segments[0].segment.id, segment_id);
    assert_eq!(loaded.segments[0].used, 512);
    assert_eq!(loaded.objects.len(), 1);
    assert_eq!(loaded.objects[0].0, "tenant-a\0key-a");
    assert_eq!(loaded.objects[0].1.replicas[0].offset, 0x100);
    assert_eq!(loaded.objects[0].1.group_id, "group-a");
    assert_eq!(loaded.tasks.len(), 1);
    assert_eq!(loaded.tasks[0].info.id, task_id);
    assert_eq!(loaded.tasks[0].info.status, TaskStatus::Failed);
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
            segments: Vec::new(),
            nof_segments: Vec::new(),
            objects: Vec::new(),
            tasks: Vec::new(),
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

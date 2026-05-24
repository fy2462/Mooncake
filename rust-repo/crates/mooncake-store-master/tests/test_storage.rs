use mooncake_store_master::storage_backend::{StorageBackend, StorageBackendType};
use dashmap::DashMap;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment};
use mooncake_store_master::service::{ObjectEntry, SegmentEntry};
use std::time::SystemTime;
use uuid::Uuid;

fn temp_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mooncake_test_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_storage_backend_save_and_load() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let sid = Uuid::new_v4();
    let cid = Uuid::new_v4();
    segments.insert(
        sid,
        SegmentEntry {
            segment: Segment {
                id: sid,
                name: "node1:12345".into(),
                size: 1024 * 1024,
                used: 100,
                client_id: cid,
            },
        },
    );

    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    objects.insert(
        "key1".into(),
        ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: sid,
                segment_name: "node1:12345".into(),
                offset: 0x1000,
                size: 256,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
            }],
            size: 256,
            last_access: SystemTime::now(),
            soft_pinned: false,
        },
    );

    backend.save(&segments, &objects).unwrap();

    let (loaded_segs, loaded_objs) = backend.load().unwrap().unwrap();
    assert_eq!(loaded_segs.len(), 1);
    assert_eq!(loaded_segs[0].name, "node1:12345");
    assert_eq!(loaded_segs[0].size, 1024 * 1024);
    assert_eq!(loaded_objs.len(), 1);
    assert_eq!(loaded_objs[0].0, "key1");
    assert_eq!(loaded_objs[0].1.replicas.len(), 1);
    assert_eq!(loaded_objs[0].1.replicas[0].segment_name, "node1:12345");
    assert_eq!(loaded_objs[0].1.replicas[0].offset, 0x1000);
    assert_eq!(loaded_objs[0].1.size, 256);
}

#[test]
fn test_storage_backend_load_empty() {
    let tmp = temp_dir();
    let subdir = tmp.join("empty_snapshots");
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &subdir);
    assert!(backend.load().unwrap().is_none());
}

#[test]
fn test_storage_backend_multiple_objects() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let sid = Uuid::new_v4();
    let cid = Uuid::new_v4();
    segments.insert(sid, SegmentEntry {
        segment: Segment { id: sid, name: "s1".into(), size: 1000, used: 0, client_id: cid },
    });

    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    for i in 0..5u64 {
        let key = format!("key_{}", i);
        objects.insert(key.clone(), ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: sid,
                segment_name: "s1".into(),
                offset: i * 100,
                size: 100,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
            }],
            size: 100,
            last_access: SystemTime::now(),
            soft_pinned: false,
        });
    }

    backend.save(&segments, &objects).unwrap();
    let (_, loaded_objs) = backend.load().unwrap().unwrap();
    assert_eq!(loaded_objs.len(), 5);
}

#[test]
fn test_storage_backend_clear() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    backend.save(&segments, &objects).unwrap();

    assert!(backend.load().unwrap().is_some());
    backend.clear().unwrap();
    assert!(backend.load().unwrap().is_none());
}

#[test]
fn test_serialize_replica_status_roundtrip() {
    let rd = ReplicaDescriptor {
        segment_id: Uuid::new_v4(),
        segment_name: "node1:12345".into(),
        offset: 0x2000,
        size: 512,
        status: ReplicaStatus::Written,
        replica_type: ReplicaType::Disk,
        holder_client_id: None,
    };
    assert_eq!(rd.segment_name, "node1:12345");
    assert_eq!(rd.offset, 0x2000);
    assert_eq!(rd.size, 512);
    assert_eq!(rd.status, ReplicaStatus::Written);
    assert_eq!(rd.replica_type, ReplicaType::Disk);
}

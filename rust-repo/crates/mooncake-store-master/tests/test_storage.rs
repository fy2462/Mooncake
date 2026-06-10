use dashmap::DashMap;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment};
mod common;
use common::temp_dir;

use mooncake_store_master::hf3fs::{self, Hf3fsApi};
use mooncake_store_master::proto::SegmentStatus as ProtoSegmentStatus;
use mooncake_store_master::service::{NoFSegmentEntry, ObjectEntry, SegmentEntry, TaskEntry};
use mooncake_store_master::storage_backend::{
    DistributedStorageConfig, StorageBackend, StorageBackendType,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;
use uuid::Uuid;

fn make_entry(replicas: Vec<ReplicaDescriptor>, size: u64) -> ObjectEntry {
    ObjectEntry {
        replicas,
        size,
        last_access: SystemTime::now(),
        hard_pinned: false,
        data_type: Default::default(),
        client_id: Uuid::nil(),
        put_start_time: None,
        lease_timeout: None,
        soft_pin_timeout: None,
        tenant_id: "default".to_string(),
        user_key: String::new(),
        group_id: String::new(),
    }
}

fn hf3fs_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct MockHf3fsApi {
    reg_calls: AtomicUsize,
    dereg_calls: AtomicUsize,
}

impl MockHf3fsApi {
    fn new() -> Self {
        Self {
            reg_calls: AtomicUsize::new(0),
            dereg_calls: AtomicUsize::new(0),
        }
    }
}

impl Hf3fsApi for MockHf3fsApi {
    fn reg_fd(&self, _fd: std::os::fd::RawFd, _flags: i32) -> Result<i32, std::io::Error> {
        self.reg_calls.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }

    fn dereg_fd(&self, _fd: std::os::fd::RawFd) -> Result<i32, std::io::Error> {
        self.dereg_calls.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }
}

fn make_mem_replica(sid: Uuid, seg_name: &str, off: u64, sz: u64) -> ReplicaDescriptor {
    ReplicaDescriptor {
        base_addr: 0x100000000,
        refcnt: 0,
        handle_valid: true,
        segment_id: sid,
        segment_name: seg_name.into(),
        offset: off,
        size: sz,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
    }
}

fn make_disk_replica(sid: Uuid, seg_name: &str, off: u64, sz: u64) -> ReplicaDescriptor {
    ReplicaDescriptor {
        base_addr: 0x100000000,
        refcnt: 0,
        handle_valid: true,
        segment_id: sid,
        segment_name: seg_name.into(),
        offset: off,
        size: sz,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Disk,
        holder_client_id: None,
    }
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
                base: 0x200000000,
                te_endpoint: "node1:12346".into(),
                protocol: "rdma".into(),
            },
            used: 4096,
            client_id: cid,
            status: ProtoSegmentStatus::Active,
        },
    );

    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    objects.insert(
        "key1".into(),
        make_entry(vec![make_mem_replica(sid, "node1:12345", 0x1000, 256)], 256),
    );

    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();

    let (loaded_segs, loaded_nof_segs, loaded_objs, _loaded_tasks) =
        backend.load().unwrap().unwrap();
    assert_eq!(loaded_segs.len(), 1);
    assert!(loaded_nof_segs.is_empty());
    assert_eq!(loaded_segs[0].segment.name, "node1:12345");
    assert_eq!(loaded_segs[0].segment.base, 0x200000000);
    assert_eq!(loaded_segs[0].segment.size, 1024 * 1024);
    assert_eq!(loaded_segs[0].segment.te_endpoint, "node1:12346");
    assert_eq!(loaded_segs[0].segment.protocol, "rdma");
    assert_eq!(loaded_segs[0].used, 4096);
    assert_eq!(loaded_segs[0].client_id, cid);
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
    segments.insert(
        sid,
        SegmentEntry {
            segment: Segment {
                id: sid,
                name: "s1".into(),
                size: 1000,
                base: 0,
                te_endpoint: String::new(),
                protocol: "tcp".into(),
            },
            used: 0,
            client_id: cid,
            status: ProtoSegmentStatus::Active,
        },
    );

    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    for i in 0..5u64 {
        let key = format!("key_{}", i);
        objects.insert(
            key.clone(),
            make_entry(vec![make_mem_replica(sid, "s1", i * 100, 100)], 100),
        );
    }

    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();
    let (_, loaded_nof_segs, loaded_objs, _loaded_tasks) = backend.load().unwrap().unwrap();
    assert!(loaded_nof_segs.is_empty());
    assert_eq!(loaded_objs.len(), 5);
}

#[test]
fn test_storage_backend_clear() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();

    assert!(backend.load().unwrap().is_some());
    backend.clear().unwrap();
    assert!(backend.load().unwrap().is_none());
}

#[test]
fn test_storage_backend_hf3fs_uses_fd_registration() {
    let _guard = hf3fs_test_lock().lock().unwrap();
    let api = Arc::new(MockHf3fsApi::new());
    hf3fs::set_api_override_for_test(Some(api.clone()));

    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::Hf3fs, &tmp);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let sid = Uuid::new_v4();
    let cid = Uuid::new_v4();
    segments.insert(
        sid,
        SegmentEntry {
            segment: Segment {
                id: sid,
                name: "hf3fs-node".into(),
                size: 4096,
                base: 0,
                te_endpoint: String::new(),
                protocol: "tcp".into(),
            },
            used: 0,
            client_id: cid,
            status: ProtoSegmentStatus::Active,
        },
    );

    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    objects.insert(
        "hf3fs-key".into(),
        make_entry(vec![make_disk_replica(sid, "hf3fs-node", 64, 128)], 128),
    );

    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();
    let loaded = backend.load().unwrap().unwrap();
    assert_eq!(loaded.0.len(), 1);
    assert!(loaded.1.is_empty());
    assert_eq!(loaded.2.len(), 1);
    assert!(api.reg_calls.load(Ordering::Relaxed) >= 2);
    assert!(api.dereg_calls.load(Ordering::Relaxed) >= 2);

    hf3fs::set_api_override_for_test(None);
}

#[test]
fn test_serialize_replica_status_roundtrip() {
    let rd = ReplicaDescriptor {
        base_addr: 0x100000000,
        refcnt: 0,
        handle_valid: true,
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

/// Verify that `load()` falls back to legacy JSON when no msgpack file exists.
#[test]
fn test_storage_backend_load_falls_back_to_json() {
    use std::fs;

    let tmp = temp_dir();
    let subdir = tmp.join("json_fallback");
    fs::create_dir_all(&subdir).unwrap();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &subdir);

    let json_path = subdir.join("master_snapshot.json");
    let snap = serde_json::json!({
        "segments": [{
            "id": "00000000-0000-0000-0000-000000000001",
            "name": "legacy-seg",
            "size": 4096u64,
            "used": 0u64,
            "client_id": "00000000-0000-0000-0000-000000000002",
            "status": 1i32
        }],
        "nof_segments": [],
        "objects": [],
        "tasks": []
    });
    fs::write(&json_path, serde_json::to_string_pretty(&snap).unwrap()).unwrap();

    let (segments, nof_segs, objects, _tasks) = backend.load().unwrap().unwrap();
    assert!(nof_segs.is_empty());
    assert!(objects.is_empty());
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].segment.name, "legacy-seg");
    assert_eq!(segments[0].segment.size, 4096);
}

#[test]
fn test_storage_backend_rejects_invalid_snapshot_uuid() {
    use std::fs;

    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    let snap = serde_json::json!({
        "segments": [{
            "id": "not-a-uuid",
            "name": "corrupt-seg",
            "size": 4096u64,
            "used": 0u64,
            "client_id": "00000000-0000-0000-0000-000000000002",
            "status": 1i32
        }],
        "nof_segments": [],
        "objects": [],
        "tasks": []
    });
    fs::write(
        tmp.join("master_snapshot.json"),
        serde_json::to_string_pretty(&snap).unwrap(),
    )
    .unwrap();

    let error = backend.load().unwrap_err().to_string();
    assert!(error.contains("invalid memory segment id"));
}

/// Verify `clear()` removes both msgpack and legacy JSON files.
#[test]
fn test_storage_backend_clear_removes_both_formats() {
    use std::fs;

    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();

    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();
    let json_path = tmp.join("master_snapshot.json");
    fs::write(&json_path, "{}").unwrap();

    assert!(tmp.join("master_snapshot.msgpack").exists());
    assert!(json_path.exists());

    backend.clear().unwrap();

    assert!(!tmp.join("master_snapshot.msgpack").exists());
    assert!(!json_path.exists());
}

#[test]
fn test_storage_backend_retains_bounded_snapshot_history() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);

    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let nof_segments: DashMap<Uuid, NoFSegmentEntry> = DashMap::new();
    let tasks: DashMap<Uuid, TaskEntry> = DashMap::new();
    let objects: DashMap<String, ObjectEntry> = DashMap::new();

    for _ in 0..3 {
        backend
            .save(&segments, &nof_segments, &objects, &tasks)
            .unwrap();
        backend.retain_latest_snapshot(2).unwrap();
    }

    assert!(tmp.join("master_snapshot.msgpack").exists());
    let history_count = std::fs::read_dir(tmp.join("snapshots"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("master_snapshot_"))
        })
        .count();
    assert_eq!(history_count, 2);
}

#[path = "test_storage/backends.rs"]
mod backends;

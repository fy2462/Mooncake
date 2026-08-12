use dashmap::DashMap;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment};
mod common;
use common::{proto_uuid, temp_dir};

use mooncake_store_master::TenantId;
use mooncake_store_master::hf3fs::{self, Hf3fsApi};
use mooncake_store_master::metrics;
use mooncake_store_master::proto;
use mooncake_store_master::proto::SegmentStatus as ProtoSegmentStatus;
use mooncake_store_master::proto::master_service_server::MasterService;
use mooncake_store_master::service::{
    MasterRuntimeConfig, MasterServiceImpl, NoFSegmentEntry, ObjectEntry, SegmentEntry, TaskEntry,
};
use mooncake_store_master::storage_backend::{
    DistributedStorageConfig, LocalDiskSnapshotEntry, StorageBackend, StorageBackendType,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;
use tonic::Request;
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
        tenant_id: TenantId::default(),
        user_key: String::new(),
        group_id: String::new(),
        quota_committed: false,
        reserved_quota_charge_bytes: 0,
        committed_quota_charge_bytes: 0,
        pending_replaced_quota_charge_bytes: 0,
        memory_cache_total_accounted: false,
        disk_cache_total_accounted: false,
        disk_allocated_bytes_accounted: 0,
    }
}

fn hf3fs_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn cache_total_metrics_test_lock() -> &'static Mutex<()> {
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
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        protocol: "rdma".into(),
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
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        protocol: String::new(),
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
                host_id: String::new(),
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
fn test_snapshot_restore_rebuilds_memory_and_disk_cache_total_metrics() {
    let _guard = cache_total_metrics_test_lock().lock().unwrap();
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    let segments = DashMap::new();
    let nof_segments = DashMap::new();
    let objects = DashMap::new();
    let tasks = DashMap::new();
    let segment_id = Uuid::new_v4();
    segments.insert(
        segment_id,
        SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: "snapshot-cache:1".into(),
                size: 4096,
                base: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            used: 128,
            client_id: Uuid::new_v4(),
            status: ProtoSegmentStatus::Active,
        },
    );
    objects.insert(
        "snapshot-cache-totals".into(),
        make_entry(
            vec![
                make_mem_replica(segment_id, "snapshot-cache:1", 0, 128),
                make_disk_replica(segment_id, "/snapshot-cache/object", 0, 128),
            ],
            128,
        ),
    );
    backend
        .save(&segments, &nof_segments, &objects, &tasks)
        .unwrap();

    let base_memory_total = metrics::MEM_CACHE_TOTAL.get();
    let base_disk_total = metrics::FILE_CACHE_TOTAL.get();
    let restored = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp));

    assert_eq!(metrics::MEM_CACHE_TOTAL.get(), base_memory_total + 1);
    assert_eq!(metrics::FILE_CACHE_TOTAL.get(), base_disk_total + 1);
    let snapshot = restored.capture_loaded_snapshot("cache-total-metrics");
    assert_eq!(snapshot.objects.len(), 1);
    assert_eq!(snapshot.objects[0].1.replicas.len(), 2);

    // Cache-total gauges are process-global, while dropping a service does not
    // mean its durable inventory was removed. Isolate this integration test's
    // synthetic restore from the remaining tests in this process.
    metrics::MEM_CACHE_TOTAL.set(base_memory_total);
    metrics::FILE_CACHE_TOTAL.set(base_disk_total);
}

#[test]
fn test_storage_backend_local_disk_state_roundtrip() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    let segments = DashMap::new();
    let nof_segments = DashMap::new();
    let objects = DashMap::new();
    let tasks = DashMap::new();
    let local_disk_segments = DashMap::new();
    let client_id = Uuid::new_v4();
    local_disk_segments.insert(
        client_id,
        LocalDiskSnapshotEntry {
            storage_id: client_id,
            client_id,
            enable_offloading: true,
            offloading_objects: HashMap::from([("tenant\0key".to_string(), 4096)]),
            ssd_total_capacity_bytes: 1 << 30,
        },
    );

    backend
        .save_with_local_disk(
            &segments,
            &nof_segments,
            &objects,
            &tasks,
            &local_disk_segments,
        )
        .unwrap();

    let (_, _, _, _, loaded_local_disk) = backend.load_with_local_disk().unwrap().unwrap();
    assert_eq!(loaded_local_disk.len(), 1);
    assert_eq!(loaded_local_disk[0].client_id, client_id);
    assert!(loaded_local_disk[0].enable_offloading);
    assert_eq!(loaded_local_disk[0].offloading_objects["tenant\0key"], 4096);
    assert_eq!(loaded_local_disk[0].ssd_total_capacity_bytes, 1 << 30);
}

#[test]
fn test_storage_backend_loads_legacy_snapshot_without_local_disk_state() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    backend
        .save(
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
            &DashMap::new(),
        )
        .unwrap();

    let snapshot_path = tmp.join("master_snapshot.msgpack");
    let mut snapshot: serde_json::Value =
        rmp_serde::from_slice(&std::fs::read(&snapshot_path).unwrap()).unwrap();
    snapshot
        .as_object_mut()
        .unwrap()
        .remove("local_disk_segments");
    std::fs::write(&snapshot_path, rmp_serde::to_vec_named(&snapshot).unwrap()).unwrap();

    let (_, _, _, _, local_disk_segments) = backend.load_with_local_disk().unwrap().unwrap();
    assert!(local_disk_segments.is_empty());
}

#[tokio::test]
async fn test_master_service_restores_dormant_local_disk_state_from_snapshot() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    let segments = DashMap::new();
    let nof_segments = DashMap::new();
    let objects = DashMap::new();
    let tasks = DashMap::new();
    let local_disk_segments = DashMap::new();
    let client_id = Uuid::new_v4();
    local_disk_segments.insert(
        client_id,
        LocalDiskSnapshotEntry {
            storage_id: client_id,
            client_id,
            enable_offloading: true,
            offloading_objects: HashMap::from([("tenant\0key".to_string(), 4096)]),
            ssd_total_capacity_bytes: 1 << 30,
        },
    );
    backend
        .save_with_local_disk(
            &segments,
            &nof_segments,
            &objects,
            &tasks,
            &local_disk_segments,
        )
        .unwrap();

    let service = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp));
    let restored = service.capture_loaded_snapshot("restored");

    assert_eq!(restored.local_disk_segments.len(), 1);
    let local_disk = &restored.local_disk_segments[0];
    assert_eq!(local_disk.storage_id, client_id);
    assert_eq!(local_disk.client_id, client_id);
    assert!(local_disk.enable_offloading);
    assert_eq!(
        local_disk.offloading_objects,
        HashMap::from([("tenant\0key".to_string(), 4096)])
    );
    // Capacity remains process-session state even though durable identity and
    // policy survive for second-save continuity.
    assert_eq!(local_disk.ssd_total_capacity_bytes, 0);
}

#[tokio::test]
async fn test_master_service_restores_future_graceful_unmount_deadline() {
    let tmp = temp_dir();
    let client_id = Uuid::new_v4();
    let segment_name = "snapshot-graceful-future:1";
    let service = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: "rdma".into(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();
    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 800,
        }),
    )
    .await
    .unwrap();
    let original_deadline = service
        .capture_loaded_snapshot("before-native-save")
        .graceful_unmounts[0]
        .deadline_epoch_ms;

    service.save_snapshot();
    let snapshot_path = tmp.join("master_snapshot.msgpack");
    for _ in 0..200 {
        if snapshot_path
            .metadata()
            .is_ok_and(|metadata| metadata.len() > 0)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(snapshot_path.exists(), "snapshot save did not finish");
    drop(service);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let restored = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));
    let restored_snapshot = restored.capture_loaded_snapshot("after-native-restore");
    assert_eq!(restored_snapshot.graceful_unmounts.len(), 1);
    assert_eq!(
        restored_snapshot.graceful_unmounts[0].deadline_epoch_ms, original_deadline,
        "restore must retain the absolute deadline instead of restarting grace"
    );
    assert!(restored.segment_id_by_name(segment_name).is_some());

    for _ in 0..250 {
        if restored.segment_id_by_name(segment_name).is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        restored.segment_id_by_name(segment_name).is_none(),
        "future restored deadline was not rescheduled"
    );
    assert!(
        restored
            .capture_loaded_snapshot("after-expiry")
            .graceful_unmounts
            .is_empty()
    );
}

#[tokio::test]
async fn test_master_service_executes_expired_graceful_unmount_after_restore() {
    let tmp = temp_dir();
    let client_id = Uuid::new_v4();
    let segment_name = "snapshot-graceful-expired:1";
    let service = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 4096,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: "rdma".into(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let segment_id = service.segment_id_by_name(segment_name).unwrap();
    MasterService::graceful_unmount_segment(
        &service,
        Request::new(proto::GracefulUnmountSegmentRequest {
            segment_id: Some(proto_uuid(segment_id)),
            client_id: Some(proto_uuid(client_id)),
            grace_period_ms: 200,
        }),
    )
    .await
    .unwrap();
    service.save_snapshot();
    let snapshot_path = tmp.join("master_snapshot.msgpack");
    for _ in 0..200 {
        if snapshot_path
            .metadata()
            .is_ok_and(|metadata| metadata.len() > 0)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(snapshot_path.exists(), "snapshot save did not finish");
    drop(service);
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let restored = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));
    for _ in 0..100 {
        if restored.segment_id_by_name(segment_name).is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(
        restored.segment_id_by_name(segment_name).is_none(),
        "expired restored deadline must execute immediately"
    );
    assert!(
        restored
            .capture_loaded_snapshot("expired")
            .graceful_unmounts
            .is_empty()
    );
}

#[tokio::test]
async fn test_legacy_snapshot_status_without_deadline_completes_immediately() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    let segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let segment_name = "snapshot-graceful-legacy:1";
    let segments = DashMap::new();
    segments.insert(
        segment_id,
        SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: segment_name.into(),
                size: 4096,
                base: 0x100000000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            used: 0,
            client_id,
            status: ProtoSegmentStatus::GracefullyUnmounting,
        },
    );
    backend
        .save(&segments, &DashMap::new(), &DashMap::new(), &DashMap::new())
        .unwrap();
    let snapshot_path = tmp.join("master_snapshot.msgpack");
    let mut legacy: serde_json::Value =
        rmp_serde::from_slice(&std::fs::read(&snapshot_path).unwrap()).unwrap();
    let object = legacy.as_object_mut().unwrap();
    object.insert("format_version".into(), serde_json::json!(3));
    object.remove("graceful_unmounts");
    std::fs::write(&snapshot_path, rmp_serde::to_vec_named(&legacy).unwrap()).unwrap();

    let restored = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));
    for _ in 0..100 {
        if restored.segment_id_by_name(segment_name).is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(
        restored.segment_id_by_name(segment_name).is_none(),
        "legacy status=GracefullyUnmounting without a deadline must not remain stuck"
    );
}

#[tokio::test]
async fn test_master_service_restores_inflight_native_copy_from_snapshot() {
    let tmp = temp_dir();
    let client_id = Uuid::new_v4();
    let service = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));
    for (index, segment_name) in ["snapshot-copy-src:1", "snapshot-copy-dst:1"]
        .into_iter()
        .enumerate()
    {
        MasterService::mount_segment(
            &service,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: segment_name.into(),
                size: 4096,
                base_addr: 0x100000000 + index as u64 * 0x10000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "snapshot-copy-key".into(),
            slice_length: 128,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                nof_replica_num: 0,
                with_soft_pin: false,
                with_hard_pin: false,
                preferred_segment: "snapshot-copy-src:1".into(),
                prefer_alloc_in_same_node: false,
                preferred_segments: vec![],
                preferred_nof_segments: vec![],
                data_type: proto::ObjectDataType::Unknown as i32,
                group_ids: vec![],
                host_id: String::new(),
            }),
        }),
    )
    .await
    .unwrap();
    MasterService::put_end(
        &service,
        Request::new(proto::PutEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "snapshot-copy-key".into(),
            replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    MasterService::copy_start(
        &service,
        Request::new(proto::CopyStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "snapshot-copy-key".into(),
            source: "snapshot-copy-src:1".into(),
            targets: vec!["snapshot-copy-dst:1".into()],
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();

    service.save_snapshot();
    let snapshot_path = tmp.join("master_snapshot.msgpack");
    for _ in 0..100 {
        if snapshot_path
            .metadata()
            .is_ok_and(|metadata| metadata.len() > 0)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(snapshot_path.exists(), "snapshot save did not finish");
    drop(service);

    let restored = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));
    assert_eq!(
        restored
            .capture_loaded_snapshot("restored-copy")
            .replication_tasks
            .len(),
        1
    );
    for (index, segment_name) in ["snapshot-copy-src:1", "snapshot-copy-dst:1"]
        .into_iter()
        .enumerate()
    {
        MasterService::mount_segment(
            &restored,
            Request::new(proto::MountSegmentRequest {
                client_id: Some(proto_uuid(client_id)),
                segment_name: segment_name.into(),
                size: 4096,
                base_addr: 0x100000000 + index as u64 * 0x10000,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
    }
    MasterService::copy_end(
        &restored,
        Request::new(proto::CopyEndRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "snapshot-copy-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let replicas = MasterService::get_replica_list(
        &restored,
        Request::new(proto::GetReplicaListRequest {
            key: "snapshot-copy-key".into(),
            tenant_id: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .replicas;
    assert_eq!(replicas.len(), 2);
    assert!(
        replicas
            .iter()
            .any(|replica| replica.segment_name == "snapshot-copy-dst:1")
    );
}

#[tokio::test]
async fn test_master_service_rebuilds_allocator_holes_from_snapshot_replicas() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    let client_id = Uuid::new_v4();
    let segment_name = "snapshot-hole:1";
    let segment_id = mooncake_store_core::stable_memory_segment_id(
        client_id,
        segment_name,
        0x100000000,
        1_000,
        "",
        "rdma",
        "",
    );
    let segments = DashMap::new();
    segments.insert(
        segment_id,
        SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: segment_name.into(),
                size: 1_000,
                base: 0x100000000,
                te_endpoint: String::new(),
                protocol: "rdma".into(),
                host_id: String::new(),
            },
            // The legacy aggregate cannot describe the hole [100, 300).
            used: 200,
            client_id,
            status: ProtoSegmentStatus::Active,
        },
    );
    let objects = DashMap::new();
    for (key, offset) in [("left", 0), ("right", 300)] {
        let mut object = make_entry(
            vec![make_mem_replica(segment_id, segment_name, offset, 100)],
            100,
        );
        object.user_key = key.into();
        objects.insert(TenantId::default().make_scoped_key(key), object);
    }
    backend
        .save(&segments, &DashMap::new(), &objects, &DashMap::new())
        .unwrap();

    let service = MasterServiceImpl::new_with_runtime_config(
        Some(StorageBackendType::LocalDisk),
        Some(tmp),
        MasterRuntimeConfig {
            lease_ttl: std::time::Duration::ZERO,
            ..Default::default()
        },
    );
    MasterService::mount_segment(
        &service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 1_000,
            base_addr: 0x100000000,
            te_endpoint: String::new(),
            protocol: "rdma".into(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
    let allocated = MasterService::put_start(
        &service,
        Request::new(proto::PutStartRequest {
            client_id: Some(proto_uuid(client_id)),
            key: "fills-hole".into(),
            slice_length: 150,
            tenant_id: String::new(),
            config: Some(proto::ReplicateConfig {
                replica_num: 1,
                preferred_segment: segment_name.into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(allocated.replicas.len(), 1);
    assert_eq!(allocated.replicas[0].offset, 100);
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
                host_id: String::new(),
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
                host_id: String::new(),
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
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        protocol: String::new(),
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

#[test]
fn test_master_service_try_constructor_rejects_corrupt_native_snapshot() {
    let tmp = temp_dir();
    std::fs::write(tmp.join("master_snapshot.msgpack"), b"not valid msgpack").unwrap();

    let error = match MasterServiceImpl::try_new_with_runtime_config(
        Some(StorageBackendType::LocalDisk),
        Some(tmp),
        MasterRuntimeConfig::default(),
    ) {
        Ok(service) => {
            drop(service);
            panic!("corrupt native snapshot must not produce a service")
        }
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("failed to load native master snapshot")
    );
}

#[test]
fn test_master_service_try_constructor_rejects_future_native_snapshot() {
    let tmp = temp_dir();
    let snapshot = serde_json::json!({
        "format_version": 999u32,
        "segments": [],
        "nof_segments": [],
        "objects": [],
        "tasks": []
    });
    std::fs::write(
        tmp.join("master_snapshot.json"),
        serde_json::to_vec(&snapshot).unwrap(),
    )
    .unwrap();

    let error = match MasterServiceImpl::try_new_with_runtime_config(
        Some(StorageBackendType::LocalDisk),
        Some(tmp),
        MasterRuntimeConfig::default(),
    ) {
        Ok(service) => {
            drop(service);
            panic!("future native snapshot must not produce a service")
        }
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("unsupported master snapshot format version")
    );
}

#[test]
fn test_master_service_try_constructor_rejects_logically_invalid_native_snapshot() {
    let tmp = temp_dir();
    let backend = StorageBackend::new(StorageBackendType::LocalDisk, &tmp);
    let segment_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let segments = DashMap::new();
    segments.insert(
        segment_id,
        SegmentEntry {
            segment: Segment {
                id: segment_id,
                name: "duplicate-graceful-intent:1".into(),
                size: 4096,
                base: 0x100000000,
                te_endpoint: String::new(),
                protocol: "rdma".into(),
                host_id: String::new(),
            },
            used: 0,
            client_id,
            status: ProtoSegmentStatus::GracefullyUnmounting,
        },
    );
    backend
        .save(&segments, &DashMap::new(), &DashMap::new(), &DashMap::new())
        .unwrap();

    let snapshot_path = tmp.join("master_snapshot.msgpack");
    #[derive(serde::Serialize)]
    struct GracefulIntent {
        segment_id: Uuid,
        client_id: Uuid,
        deadline_epoch_ms: u64,
    }
    let intent_bytes = rmp_serde::to_vec_named(&GracefulIntent {
        segment_id,
        client_id,
        deadline_epoch_ms: 1234,
    })
    .unwrap();
    let intent: rmpv::Value = rmpv::decode::read_value(&mut intent_bytes.as_slice()).unwrap();
    let snapshot_bytes = std::fs::read(&snapshot_path).unwrap();
    let mut snapshot: rmpv::Value =
        rmpv::decode::read_value(&mut snapshot_bytes.as_slice()).unwrap();
    let rmpv::Value::Map(fields) = &mut snapshot else {
        panic!("native snapshot must be a map")
    };
    let graceful_unmounts = fields
        .iter_mut()
        .find(|(key, _)| key.as_str() == Some("graceful_unmounts"))
        .expect("snapshot must contain graceful_unmounts");
    graceful_unmounts.1 = rmpv::Value::Array(vec![intent.clone(), intent]);
    let mut encoded = Vec::new();
    rmpv::encode::write_value(&mut encoded, &snapshot).unwrap();
    std::fs::write(&snapshot_path, encoded).unwrap();

    let error = match MasterServiceImpl::try_new_with_runtime_config(
        Some(StorageBackendType::LocalDisk),
        Some(tmp),
        MasterRuntimeConfig::default(),
    ) {
        Ok(service) => {
            drop(service);
            panic!("logically invalid native snapshot must not produce a service")
        }
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("duplicate graceful unmount"),
        "{error}"
    );
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

#[tokio::test]
async fn test_native_snapshot_save_is_rejected_after_service_gate_closes() {
    let tmp = temp_dir();
    let service = MasterServiceImpl::new(Some(StorageBackendType::LocalDisk), Some(tmp.clone()));

    service.set_service_available(false);
    service.save_snapshot();
    tokio::task::yield_now().await;

    assert!(
        !tmp.join("master_snapshot.msgpack").exists(),
        "a non-serving master must not publish a native snapshot"
    );
}

#[path = "test_storage/backends.rs"]
mod backends;

use dashmap::DashMap;
use mooncake_store_core::{
    ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment,
};
use mooncake_store_master::TenantId;
use mooncake_store_master::allocator::{AllocationStrategy, SegmentAllocator};
use mooncake_store_master::proto::SegmentStatus as ProtoSegmentStatus;
use mooncake_store_master::service::{ObjectEntry, SegmentEntry};
use std::time::SystemTime;
use uuid::Uuid;

fn make_test_seg(id: Uuid, name: &str, size: u64) -> Segment {
    Segment {
        id,
        name: name.into(),
        size,
        base: 0,
        te_endpoint: String::new(),
        protocol: "tcp".into(),
        host_id: String::new(),
    }
}

fn make_test_entry(id: Uuid, name: &str, size: u64, used: u64, client_id: Uuid) -> SegmentEntry {
    SegmentEntry {
        segment: make_test_seg(id, name, size),
        used,
        client_id,
        status: ProtoSegmentStatus::Active,
    }
}

#[test]
fn test_segment_creation_all_fields() {
    let id = Uuid::new_v4();
    let segment = make_test_seg(id, "node1:12345", 1024 * 1024 * 100);
    assert_eq!(segment.name, "node1:12345");
    assert_eq!(segment.size, 104857600);
    assert_eq!(segment.base, 0);
    assert_eq!(segment.id, id);
}

#[test]
fn test_segment_entry_wrapper() {
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let entry = make_test_entry(id, "n1:1", 5000, 100, cid);
    assert_eq!(entry.segment.name, "n1:1");
    assert_eq!(entry.used, 100);
    assert_eq!(entry.client_id, cid);
}

#[test]
fn test_segment_dashmap_ops() {
    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let cid = Uuid::new_v4();

    segments.insert(id1, make_test_entry(id1, "s1", 1000, 0, cid));
    segments.insert(id2, make_test_entry(id2, "s2", 2000, 100, cid));

    assert_eq!(segments.len(), 2);
    assert!(segments.contains_key(&id1));
    assert!(!segments.contains_key(&Uuid::new_v4()));

    segments.remove(&id1);
    assert_eq!(segments.len(), 1);
    assert!(segments.contains_key(&id2));
}

#[test]
fn test_replica_status_enum_values() {
    assert_eq!(ReplicaStatus::Undefined as i32, 0);
    assert_eq!(ReplicaStatus::Allocating as i32, 1);
    assert_eq!(ReplicaStatus::Written as i32, 2);
    assert_eq!(ReplicaStatus::Complete as i32, 3);
    assert_eq!(ReplicaStatus::Failed as i32, 4);
}

#[test]
fn test_replica_type_enum_values() {
    assert_eq!(ReplicaType::Memory as i32, 0);
    assert_eq!(ReplicaType::Disk as i32, 1);
    assert_eq!(ReplicaType::LocalDisk as i32, 2);
    assert_eq!(ReplicaType::NoFSsd as i32, 3);
    assert_eq!(ReplicaType::All as i32, 4);
}

#[test]
fn test_replica_status_serde_roundtrip() {
    let statuses = vec![
        ReplicaStatus::Undefined,
        ReplicaStatus::Allocating,
        ReplicaStatus::Written,
        ReplicaStatus::Complete,
        ReplicaStatus::Failed,
    ];

    for status in &statuses {
        let json = serde_json::to_string(status).unwrap();
        let restored: ReplicaStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(*status, restored);
    }
}

#[test]
fn test_replica_type_serde_roundtrip() {
    for ty in &[
        ReplicaType::Memory,
        ReplicaType::Disk,
        ReplicaType::LocalDisk,
        ReplicaType::NoFSsd,
        ReplicaType::All,
    ] {
        let json = serde_json::to_string(ty).unwrap();
        let restored: ReplicaType = serde_json::from_str(&json).unwrap();
        assert_eq!(*ty, restored);
    }
}

#[test]
fn test_replica_descriptor_full() {
    let sid = Uuid::new_v4();
    let rd = ReplicaDescriptor {
        base_addr: 0x100000000,
        refcnt: 0,
        handle_valid: true,
        segment_id: sid,
        segment_name: "node1:12345".into(),
        offset: 0xDEAD,
        size: 128,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        protocol: "rdma".into(),
    };

    assert_eq!(rd.segment_id, sid);
    assert_eq!(rd.segment_name, "node1:12345");
    assert_eq!(rd.offset, 0xDEAD);
    assert_eq!(rd.status, ReplicaStatus::Complete);
    assert_eq!(rd.replica_type, ReplicaType::Memory);
}

#[test]
fn test_replica_descriptor_clone() {
    let rd = ReplicaDescriptor {
        base_addr: 0x100000000,
        refcnt: 0,
        handle_valid: true,
        segment_id: Uuid::new_v4(),
        segment_name: "s1".into(),
        offset: 100,
        size: 64,
        status: ReplicaStatus::Allocating,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        protocol: "tcp".into(),
    };
    let cloned = rd.clone();
    assert_eq!(rd.segment_id, cloned.segment_id);
    assert_eq!(rd.offset, cloned.offset);
    assert_eq!(rd.status, cloned.status);
}

#[test]
fn test_allocator_strategy_enum() {
    let a = AllocationStrategy::Random;
    let b = AllocationStrategy::FreeRatioFirst;
    assert_ne!(a, b);
    assert_eq!(a, AllocationStrategy::Random);
    assert_eq!(
        AllocationStrategy::parse("ssd_free_ratio_first"),
        Some(AllocationStrategy::SsdFreeRatioFirst)
    );
}

#[test]
fn test_allocator_add_and_remove_segment() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::Random);
    let cid = Uuid::new_v4();
    let sid = Uuid::new_v4();
    allocator.add_segment(make_test_seg(sid, "node1:1", 1024 * 1024), 0, cid);

    let replicas = allocator.allocate("k", 100, 1, &Default::default());
    assert_eq!(replicas.len(), 1);
    allocator.remove_segment(&sid);
    let replicas = allocator.allocate("k", 100, 1, &Default::default());
    assert!(replicas.is_empty());
}

#[test]
fn test_object_entry_creation() {
    let sid = Uuid::new_v4();
    let entry = ObjectEntry {
        replicas: vec![
            ReplicaDescriptor {
                base_addr: 0x100000000,
                refcnt: 0,
                handle_valid: true,
                segment_id: sid,
                segment_name: "s1".into(),
                offset: 0,
                size: 128,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                protocol: "rdma".into(),
            },
            ReplicaDescriptor {
                base_addr: 0x100000000,
                refcnt: 0,
                handle_valid: true,
                segment_id: sid,
                segment_name: "s2".into(),
                offset: 128,
                size: 128,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                protocol: "tcp".into(),
            },
        ],
        size: 256,
        last_access: SystemTime::now(),
        hard_pinned: false,
        data_type: ObjectDataType::Unknown,
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
    };
    assert_eq!(entry.replicas.len(), 2);
    assert_eq!(entry.replicas[0].segment_name, "s1");
    assert_eq!(entry.replicas[1].segment_name, "s2");
}

#[test]
fn test_allocator_ignores_same_node_preference_for_memory_only() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::FreeRatioFirst);
    let cid_same = Uuid::new_v4();
    let cid_other = Uuid::new_v4();
    let cid_third = Uuid::new_v4();
    allocator.add_segment(
        make_test_seg(Uuid::new_v4(), "same:1", 10000),
        9000,
        cid_same,
    );
    allocator.add_segment(
        make_test_seg(Uuid::new_v4(), "other:1", 10000),
        0,
        cid_other,
    );
    allocator.add_segment(
        make_test_seg(Uuid::new_v4(), "third:1", 10000),
        0,
        cid_third,
    );
    let config = ReplicateConfig {
        prefer_alloc_in_same_node: true,
        replica_num: 1,
        nof_replica_num: 0,
        ..Default::default()
    };
    let replicas = allocator.allocate_for_client("k", Some(cid_same), 100, 1, &config);
    assert_eq!(replicas.len(), 1);
    assert_ne!(replicas[0].segment_name, "same:1");
}

#[test]
fn test_allocator_no_space() {
    let mut allocator = SegmentAllocator::new();
    let replicas = allocator.allocate("k", 1000000000, 1, &Default::default());
    assert!(replicas.is_empty());
}

#[test]
fn test_multi_replica_different_segments() {
    let mut allocator = SegmentAllocator::new();
    let cid = Uuid::new_v4();
    for i in 0..4 {
        allocator.add_segment(
            make_test_seg(Uuid::new_v4(), &format!("n{}:1", i), 10000),
            0,
            cid,
        );
    }
    let replicas = allocator.allocate("k", 100, 3, &Default::default());
    if replicas.len() >= 2 {
        let mut names: Vec<&str> = replicas.iter().map(|r| r.segment_name.as_str()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), replicas.len());
    }
}

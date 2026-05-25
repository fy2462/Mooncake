use dashmap::DashMap;
use mooncake_store_core::{ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment};
use mooncake_store_master::allocator::{AllocationStrategy, SegmentAllocator};
use mooncake_store_master::service::{ObjectEntry, SegmentEntry};
use std::time::SystemTime;
use uuid::Uuid;

#[test]
fn test_segment_creation_all_fields() {
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let segment = Segment {
        id,
        name: "node1:12345".into(),
        size: 1024 * 1024 * 100,
        used: 0,
        client_id: cid,
    };

    assert_eq!(segment.name, "node1:12345");
    assert_eq!(segment.size, 104857600);
    assert_eq!(segment.used, 0);
    assert_eq!(segment.client_id, cid);
    assert_eq!(segment.id, id);
}

#[test]
fn test_segment_entry_wrapper() {
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let entry = SegmentEntry {
        segment: Segment {
            id,
            name: "n1:1".into(),
            size: 5000,
            used: 100,
            client_id: cid,
        },
    };

    assert_eq!(entry.segment.name, "n1:1");
    assert_eq!(entry.segment.used, 100);
}

#[test]
fn test_segment_dashmap_ops() {
    let segments: DashMap<Uuid, SegmentEntry> = DashMap::new();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let cid = Uuid::new_v4();

    segments.insert(
        id1,
        SegmentEntry {
            segment: Segment {
                id: id1,
                name: "s1".into(),
                size: 1000,
                used: 0,
                client_id: cid,
            },
        },
    );
    segments.insert(
        id2,
        SegmentEntry {
            segment: Segment {
                id: id2,
                name: "s2".into(),
                size: 2000,
                used: 100,
                client_id: cid,
            },
        },
    );

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
    for ty in &[ReplicaType::Memory, ReplicaType::Disk] {
        let json = serde_json::to_string(ty).unwrap();
        let restored: ReplicaType = serde_json::from_str(&json).unwrap();
        assert_eq!(*ty, restored);
    }
}

#[test]
fn test_replica_descriptor_full() {
    let sid = Uuid::new_v4();
    let rd = ReplicaDescriptor {
        segment_id: sid,
        segment_name: "node1:12345".into(),
        offset: 0xDEAD,
        size: 128,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
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
        segment_id: Uuid::new_v4(),
        segment_name: "s1".into(),
        offset: 100,
        size: 64,
        status: ReplicaStatus::Allocating,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
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
}

#[test]
fn test_allocator_add_and_remove_segment() {
    let mut allocator = SegmentAllocator::new().with_strategy(AllocationStrategy::Random);
    let cid = Uuid::new_v4();
    let sid = Uuid::new_v4();

    allocator.add_segment(Segment {
        id: sid,
        name: "node1:1".into(),
        size: 1024 * 1024,
        used: 0,
        client_id: cid,
    });

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
                segment_id: sid,
                segment_name: "s1".into(),
                offset: 0,
                size: 128,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
            },
            ReplicaDescriptor {
                segment_id: sid,
                segment_name: "s2".into(),
                offset: 128,
                size: 128,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
            },
        ],
        size: 256,
        last_access: SystemTime::now(),
        soft_pinned: false,
        hard_pinned: false,
        data_type: ObjectDataType::Unknown,
    };

    assert_eq!(entry.replicas.len(), 2);
    assert_eq!(entry.replicas[0].segment_name, "s1");
    assert_eq!(entry.replicas[1].segment_name, "s2");
}

#[test]
fn test_allocator_prefers_same_node() {
    let mut allocator = SegmentAllocator::new();
    let cid_same = Uuid::new_v4();
    let cid_other = Uuid::new_v4();
    let cid_third = Uuid::new_v4();

    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "same:1".into(),
        size: 10000,
        used: 0,
        client_id: cid_same,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "other:1".into(),
        size: 10000,
        used: 0,
        client_id: cid_other,
    });
    allocator.add_segment(Segment {
        id: Uuid::new_v4(),
        name: "third:1".into(),
        size: 10000,
        used: 0,
        client_id: cid_third,
    });

    let config = ReplicateConfig {
        prefer_alloc_in_same_node: true, preferred_segments: vec![], preferred_nof_segments: vec![], data_type: mooncake_store_core::ObjectDataType::Unknown, 
        replica_num: 1,
        nof_replica_num: 0,
        ..Default::default()
    };

    let replicas = allocator.allocate_for_client("k", Some(cid_same), 100, 1, &config);
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].segment_name, "same:1");
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
        allocator.add_segment(Segment {
            id: Uuid::new_v4(),
            name: format!("n{}:1", i),
            size: 10000,
            used: 0,
            client_id: cid,
        });
    }

    let replicas = allocator.allocate("k", 100, 3, &Default::default());
    if replicas.len() >= 2 {
        let mut names: Vec<&str> = replicas.iter().map(|r| r.segment_name.as_str()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), replicas.len());
    }
}

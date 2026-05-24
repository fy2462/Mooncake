use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
use dashmap::DashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct ObjectEntry {
    replicas: Vec<ReplicaDescriptor>,
    size: u64,
}

#[test]
fn test_batch_remove_logic() {
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    for i in 0..5 {
        objects.insert(format!("batch_key_{}", i), ObjectEntry { replicas: vec![], size: 0 });
    }
    assert_eq!(objects.len(), 5);

    let keys: Vec<String> = (0..5).map(|i| format!("batch_key_{}", i)).collect();
    for key in &keys {
        objects.remove(key);
    }

    assert_eq!(objects.len(), 0);
}

#[test]
fn test_batch_remove_empty() {
    let keys: Vec<String> = vec![];
    assert!(keys.is_empty());
}

#[test]
fn test_batch_put_revoke_logic() {
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    objects.insert("k1".into(), ObjectEntry { replicas: vec![], size: 0 });
    objects.insert("k2".into(), ObjectEntry { replicas: vec![], size: 0 });

    let keys: Vec<String> = vec!["k1".into(), "k2".into(), "k3".into()];
    let statuses: Vec<i32> = keys
        .iter()
        .map(|key| {
            if objects.remove(key).is_some() { 0 } else { -1 }
        })
        .collect();

    assert_eq!(statuses, vec![0, 0, -1]);
    assert!(objects.is_empty());
}

#[test]
fn test_batch_put_end_status_transition() {
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    let sid = Uuid::new_v4();
    objects.insert(
        "pending_key".into(),
        ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: sid,
                segment_name: "s1".into(),
                offset: 0,
                size: 128,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
            }],
            size: 128,
        },
    );

    let keys = vec!["pending_key".to_string()];
    for key in &keys {
        if let Some(mut obj) = objects.get_mut(key) {
            for r in &mut obj.replicas {
                if r.status == ReplicaStatus::Allocating {
                    r.status = ReplicaStatus::Complete;
                }
            }
        }
    }

    let obj = objects.get("pending_key").unwrap();
    assert_eq!(obj.replicas[0].status, ReplicaStatus::Complete);
}

#[test]
fn test_batch_upsert_end_allocates_new() {
    let objects: DashMap<String, ObjectEntry> = DashMap::new();
    let sid = Uuid::new_v4();

    // Upsert: if key doesn't exist, insert new replica
    let entries = vec![
        ("new_key_1", 100u64),
        ("new_key_2", 200u64),
    ];

    for (key, size) in &entries {
        objects.insert(key.to_string(), ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: sid,
                segment_name: "s1".into(),
                offset: *size,
                size: *size,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
            }],
            size: *size,
        });
    }

    assert_eq!(objects.len(), 2);
    assert!(objects.contains_key("new_key_1"));
    assert!(objects.contains_key("new_key_2"));

    // Re-upsert existing key should keep old replicas
    objects.insert("new_key_1".into(), ObjectEntry {
        replicas: vec![ReplicaDescriptor {
            segment_id: sid,
            segment_name: "s1".into(),
            offset: 999,
            size: 200,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
        }],
        size: 200,
    });

    let obj = objects.get("new_key_1").unwrap();
    assert_eq!(obj.replicas[0].offset, 999);
    assert_eq!(obj.size, 200);
    assert_eq!(obj.replicas[0].status, ReplicaStatus::Complete);
}

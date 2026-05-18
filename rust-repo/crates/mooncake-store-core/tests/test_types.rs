use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment, TaskInfo,
    TaskStatus, TaskType,
};
use uuid::Uuid;

#[test]
fn test_replicate_config_default() {
    let cfg = ReplicateConfig::default();
    assert_eq!(cfg.replica_num, 1);
    assert!(!cfg.with_soft_pin);
    assert!(!cfg.with_hard_pin);
    assert!(cfg.preferred_segment.is_empty());
    assert!(!cfg.prefer_alloc_in_same_node);
}

#[test]
fn test_replicate_config_custom() {
    let cfg = ReplicateConfig {
        replica_num: 3,
        with_soft_pin: true,
        with_hard_pin: false,
        preferred_segment: "node1:12345".into(),
        prefer_alloc_in_same_node: true,
    };
    assert_eq!(cfg.replica_num, 3);
    assert!(cfg.with_soft_pin);
    assert!(!cfg.with_hard_pin);
    assert_eq!(cfg.preferred_segment, "node1:12345");
    assert!(cfg.prefer_alloc_in_same_node);
}

#[test]
fn test_segment_creation() {
    let id = Uuid::new_v4();
    let seg = Segment {
        id,
        name: "node1:12345".into(),
        size: 1024 * 1024 * 100,
        used: 1024,
        client_id: Uuid::new_v4(),
    };
    assert_eq!(seg.name, "node1:12345");
    assert_eq!(seg.size, 104857600);
    assert_eq!(seg.used, 1024);
}

#[test]
fn test_replica_descriptor() {
    let rd = ReplicaDescriptor {
        segment_id: Uuid::new_v4(),
        segment_name: "node1:12345".into(),
        offset: 0x1000,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
    };
    assert_eq!(rd.offset, 0x1000);
}

#[test]
fn test_replica_status_values() {
    assert_eq!(ReplicaStatus::Undefined as i32, 0);
    assert_eq!(ReplicaStatus::Allocating as i32, 1);
    assert_eq!(ReplicaStatus::Written as i32, 2);
    assert_eq!(ReplicaStatus::Complete as i32, 3);
    assert_eq!(ReplicaStatus::Failed as i32, 4);
}

#[test]
fn test_task_info() {
    let id = Uuid::new_v4();
    let task = TaskInfo {
        id,
        task_type: TaskType::ReplicaCopy,
        status: TaskStatus::Pending,
        created_at: chrono::Utc::now(),
        last_updated_at: chrono::Utc::now(),
        assigned_client: None,
        message: "waiting".into(),
    };
    assert_eq!(task.status, TaskStatus::Pending);
    assert_eq!(task.message, "waiting");
}

#[test]
fn test_error_conversion() {
    use mooncake_store_core::StoreError;
    // Test basic error creation
    let err = StoreError::KeyNotFound("test-key".into());
    assert_eq!(err.to_string(), "key not found: test-key");

    let err = StoreError::NoAvailableHandle;
    assert_eq!(err.to_string(), "no available storage handle");

    let err = StoreError::Internal("custom msg".into());
    assert!(err.to_string().contains("custom msg"));
}

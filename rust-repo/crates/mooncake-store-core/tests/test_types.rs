use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment, StorageObjectMetadata,
    TaskAssignment, TaskCompleteRequest, TaskInfo, TaskStatus, TaskType,
};
use uuid::Uuid;

// =========================================================================
// ReplicateConfig
// =========================================================================

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
        nof_replica_num: 0,
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
fn test_replicate_config_clone() {
    let cfg = ReplicateConfig {
        replica_num: 5,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: true,
        preferred_segment: "seg-x".into(),
        prefer_alloc_in_same_node: false,
    };
    let cloned = cfg.clone();
    assert_eq!(cfg.replica_num, cloned.replica_num);
    assert_eq!(cfg.with_hard_pin, cloned.with_hard_pin);
    assert_eq!(cfg.preferred_segment, cloned.preferred_segment);
}

#[test]
fn test_replicate_config_all_fields_false() {
    let cfg = ReplicateConfig {
        replica_num: 0,
        nof_replica_num: 0,
        with_soft_pin: false,
        with_hard_pin: false,
        preferred_segment: String::new(),
        prefer_alloc_in_same_node: false,
    };
    assert_eq!(cfg.replica_num, 0);
    assert!(!cfg.with_soft_pin);
    assert!(!cfg.with_hard_pin);
    assert!(cfg.preferred_segment.is_empty());
    assert!(!cfg.prefer_alloc_in_same_node);
}

#[test]
fn test_replicate_config_all_fields_true() {
    let cfg = ReplicateConfig {
        replica_num: 10,
        nof_replica_num: 0,
        with_soft_pin: true,
        with_hard_pin: true,
        preferred_segment: "host:9999".into(),
        prefer_alloc_in_same_node: true,
    };
    assert_eq!(cfg.replica_num, 10);
    assert!(cfg.with_soft_pin);
    assert!(cfg.with_hard_pin);
    assert_eq!(cfg.preferred_segment, "host:9999");
    assert!(cfg.prefer_alloc_in_same_node);
}

// =========================================================================
// Segment
// =========================================================================

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
fn test_segment_empty() {
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let seg = Segment {
        id,
        name: String::new(),
        size: 0,
        used: 0,
        client_id: cid,
    };
    assert!(seg.name.is_empty());
    assert_eq!(seg.size, 0);
    assert_eq!(seg.used, 0);
    assert_eq!(seg.client_id, cid);
}

#[test]
fn test_segment_fully_used() {
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let seg = Segment {
        id,
        name: "full-seg:1".into(),
        size: 4096,
        used: 4096,
        client_id: cid,
    };
    assert_eq!(seg.size, seg.used);
}

#[test]
fn test_segment_clone() {
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let seg = Segment {
        id,
        name: "clone-me:1".into(),
        size: 999,
        used: 111,
        client_id: cid,
    };
    let cloned = seg.clone();
    assert_eq!(seg.id, cloned.id);
    assert_eq!(seg.name, cloned.name);
    assert_eq!(seg.size, cloned.size);
    assert_eq!(seg.used, cloned.used);
    assert_eq!(seg.client_id, cloned.client_id);
}

#[test]
fn test_segment_serde_roundtrip() {
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let seg = Segment {
        id,
        name: "serde-seg:1".into(),
        size: 123456,
        used: 7890,
        client_id: cid,
    };
    let json = serde_json::to_string(&seg).unwrap();
    let restored: Segment = serde_json::from_str(&json).unwrap();
    assert_eq!(seg.id, restored.id);
    assert_eq!(seg.name, restored.name);
    assert_eq!(seg.size, restored.size);
    assert_eq!(seg.used, restored.used);
    assert_eq!(seg.client_id, restored.client_id);
}

#[test]
fn test_segment_serde_json_keys() {
    let cid = Uuid::new_v4();
    let seg = Segment {
        id: cid,
        name: "k:1".into(),
        size: 2048,
        used: 512,
        client_id: cid,
    };
    let json = serde_json::to_value(&seg).unwrap();
    assert!(json.get("id").is_some());
    assert!(json.get("name").is_some());
    assert!(json.get("size").is_some());
    assert!(json.get("used").is_some());
    assert!(json.get("client_id").is_some());
    assert_eq!(json["size"], 2048);
    assert_eq!(json["used"], 512);
}

// =========================================================================
// ReplicaDescriptor
// =========================================================================

#[test]
fn test_replica_descriptor() {
    let rd = ReplicaDescriptor {
        segment_id: Uuid::new_v4(),
        segment_name: "node1:12345".into(),
        offset: 0x1000,
        size: 256,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
    };
    assert_eq!(rd.offset, 0x1000);
    assert_eq!(rd.size, 256);
}

#[test]
fn test_replica_descriptor_disk() {
    let rd = ReplicaDescriptor {
        segment_id: Uuid::new_v4(),
        segment_name: "disk-node:1".into(),
        offset: 65536,
        size: 4096,
        status: ReplicaStatus::Written,
        replica_type: ReplicaType::Disk,
        holder_client_id: None,
    };
    assert_eq!(rd.replica_type, ReplicaType::Disk);
    assert_eq!(rd.status, ReplicaStatus::Written);
    assert_eq!(rd.offset, 65536);
}

#[test]
fn test_replica_descriptor_all_statuses() {
    for status in &[
        ReplicaStatus::Undefined,
        ReplicaStatus::Allocating,
        ReplicaStatus::Written,
        ReplicaStatus::Complete,
        ReplicaStatus::Failed,
    ] {
        let rd = ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: "s1".into(),
            offset: 0,
            size: 1,
            status: *status,
            replica_type: ReplicaType::Memory,
            holder_client_id: None,
        };
        assert_eq!(rd.status, *status);
    }
}

#[test]
fn test_replica_descriptor_clone() {
    let rd = ReplicaDescriptor {
        segment_id: Uuid::new_v4(),
        segment_name: "cl".into(),
        offset: 777,
        size: 64,
        status: ReplicaStatus::Allocating,
        replica_type: ReplicaType::Memory,
        holder_client_id: None,
    };
    let cloned = rd.clone();
    assert_eq!(rd.segment_id, cloned.segment_id);
    assert_eq!(rd.offset, cloned.offset);
    assert_eq!(rd.status, cloned.status);
    assert_eq!(rd.replica_type, cloned.replica_type);
}

#[test]
fn test_replica_descriptor_serde_roundtrip() {
    let sid = Uuid::new_v4();
    let rd = ReplicaDescriptor {
        segment_id: sid,
        segment_name: "serde-rep:1".into(),
        offset: 0xFACE,
        size: 1024,
        status: ReplicaStatus::Complete,
        replica_type: ReplicaType::Disk,
        holder_client_id: None,
    };
    let json = serde_json::to_string(&rd).unwrap();
    let restored: ReplicaDescriptor = serde_json::from_str(&json).unwrap();
    assert_eq!(rd.segment_id, restored.segment_id);
    assert_eq!(rd.segment_name, restored.segment_name);
    assert_eq!(rd.offset, restored.offset);
    assert_eq!(rd.size, restored.size);
    assert_eq!(rd.status, restored.status);
    assert_eq!(rd.replica_type, restored.replica_type);
}

// =========================================================================
// ReplicaStatus / ReplicaType
// =========================================================================

#[test]
fn test_replica_status_values() {
    assert_eq!(ReplicaStatus::Undefined as i32, 0);
    assert_eq!(ReplicaStatus::Allocating as i32, 1);
    assert_eq!(ReplicaStatus::Written as i32, 2);
    assert_eq!(ReplicaStatus::Complete as i32, 3);
    assert_eq!(ReplicaStatus::Failed as i32, 4);
}

#[test]
fn test_replica_type_values() {
    assert_eq!(ReplicaType::Memory as i32, 0);
    assert_eq!(ReplicaType::Disk as i32, 1);
}

#[test]
fn test_replica_status_serde_all_variants() {
    for status in &[
        ReplicaStatus::Undefined,
        ReplicaStatus::Allocating,
        ReplicaStatus::Written,
        ReplicaStatus::Complete,
        ReplicaStatus::Failed,
    ] {
        let json = serde_json::to_string(status).unwrap();
        let restored: ReplicaStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(*status, restored);
    }
}

#[test]
fn test_replica_type_serde_all_variants() {
    for ty in &[ReplicaType::Memory, ReplicaType::Disk] {
        let json = serde_json::to_string(ty).unwrap();
        let restored: ReplicaType = serde_json::from_str(&json).unwrap();
        assert_eq!(*ty, restored);
    }
}

#[test]
fn test_replica_status_display() {
    let s = format!("{:?}", ReplicaStatus::Failed);
    assert!(s.contains("Failed"));
    let s = format!("{:?}", ReplicaStatus::Undefined);
    assert!(s.contains("Undefined"));
}

// =========================================================================
// TaskInfo / TaskType / TaskStatus
// =========================================================================

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
fn test_task_info_with_client() {
    let cid = Uuid::new_v4();
    let task = TaskInfo {
        id: Uuid::new_v4(),
        task_type: TaskType::ReplicaMove,
        status: TaskStatus::Processing,
        created_at: chrono::Utc::now(),
        last_updated_at: chrono::Utc::now(),
        assigned_client: Some(cid),
        message: "moving".into(),
    };
    assert_eq!(task.task_type, TaskType::ReplicaMove);
    assert_eq!(task.status, TaskStatus::Processing);
    assert_eq!(task.assigned_client, Some(cid));
}

#[test]
fn test_task_info_clone() {
    let cid = Some(Uuid::new_v4());
    let task = TaskInfo {
        id: Uuid::new_v4(),
        task_type: TaskType::ReplicaMove,
        status: TaskStatus::Success,
        created_at: chrono::Utc::now(),
        last_updated_at: chrono::Utc::now(),
        assigned_client: cid,
        message: "ok".into(),
    };
    let cloned = task.clone();
    assert_eq!(task.id, cloned.id);
    assert_eq!(task.status, cloned.status);
    assert_eq!(task.assigned_client, cloned.assigned_client);
}

#[test]
fn test_task_type_values() {
    assert_eq!(TaskType::ReplicaCopy as i32, 0);
    assert_eq!(TaskType::ReplicaMove as i32, 1);
}

#[test]
fn test_task_status_values() {
    assert_eq!(TaskStatus::Pending as i32, 0);
    assert_eq!(TaskStatus::Processing as i32, 1);
    assert_eq!(TaskStatus::Success as i32, 2);
    assert_eq!(TaskStatus::Failed as i32, 3);
}

#[test]
fn test_task_info_serde_roundtrip() {
    let cid = Some(Uuid::new_v4());
    let task = TaskInfo {
        id: Uuid::new_v4(),
        task_type: TaskType::ReplicaCopy,
        status: TaskStatus::Success,
        created_at: chrono::Utc::now(),
        last_updated_at: chrono::Utc::now(),
        assigned_client: cid,
        message: "done".into(),
    };
    let json = serde_json::to_string(&task).unwrap();
    let restored: TaskInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(task.id, restored.id);
    assert_eq!(task.task_type, restored.task_type);
    assert_eq!(task.status, restored.status);
    assert_eq!(task.assigned_client, restored.assigned_client);
    assert_eq!(task.message, restored.message);
}

#[test]
fn test_task_info_serde_no_assigned_client() {
    let task = TaskInfo {
        id: Uuid::new_v4(),
        task_type: TaskType::ReplicaMove,
        status: TaskStatus::Failed,
        created_at: chrono::Utc::now(),
        last_updated_at: chrono::Utc::now(),
        assigned_client: None,
        message: "error".into(),
    };
    let json = serde_json::to_string(&task).unwrap();
    let restored: TaskInfo = serde_json::from_str(&json).unwrap();
    assert!(restored.assigned_client.is_none());
    assert_eq!(restored.message, "error");
    assert_eq!(restored.status, TaskStatus::Failed);
}

#[test]
fn test_task_assignment_roundtrip() {
    let assignment = TaskAssignment {
        id: Uuid::new_v4(),
        task_type: TaskType::ReplicaCopy,
        payload: r#"{"key":"k","source":"s0","targets":["s1"]}"#.into(),
        created_at_ms_epoch: 123456,
        max_retry_attempts: 3,
    };
    let json = serde_json::to_string(&assignment).unwrap();
    let restored: TaskAssignment = serde_json::from_str(&json).unwrap();
    assert_eq!(assignment.id, restored.id);
    assert_eq!(assignment.task_type, restored.task_type);
    assert_eq!(assignment.payload, restored.payload);
    assert_eq!(assignment.max_retry_attempts, restored.max_retry_attempts);
}

#[test]
fn test_task_complete_request_roundtrip() {
    let request = TaskCompleteRequest {
        id: Uuid::new_v4(),
        status: TaskStatus::Success,
        message: "done".into(),
    };
    let json = serde_json::to_string(&request).unwrap();
    let restored: TaskCompleteRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(request.id, restored.id);
    assert_eq!(request.status, restored.status);
    assert_eq!(request.message, restored.message);
}

#[test]
fn test_storage_object_metadata_roundtrip() {
    let metadata = StorageObjectMetadata {
        bucket_id: 1,
        offset: 64,
        key_size: 8,
        data_size: 1024,
        transport_endpoint: "holder-a".into(),
    };
    let json = serde_json::to_string(&metadata).unwrap();
    let restored: StorageObjectMetadata = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.data_size, 1024);
    assert_eq!(restored.transport_endpoint, "holder-a");
}

// =========================================================================
// StoreError
// =========================================================================

#[test]
fn test_error_conversion() {
    use mooncake_store_core::StoreError;
    let err = StoreError::KeyNotFound("test-key".into());
    assert_eq!(err.to_string(), "key not found: test-key");

    let err = StoreError::NoAvailableHandle;
    assert_eq!(err.to_string(), "no available storage handle");

    let err = StoreError::Internal("custom msg".into());
    assert!(err.to_string().contains("custom msg"));
}

#[test]
fn test_error_all_variants() {
    use mooncake_store_core::StoreError;

    let cases: Vec<(&str, StoreError)> = vec![
        ("operation failed with code 42", StoreError::OperationFailed(42)),
        ("null handle returned", StoreError::NullHandle),
        ("object already exists: dup", StoreError::ObjectExists("dup".into())),
        ("replica is not ready", StoreError::ReplicaNotReady),
        ("invalid parameters: bad", StoreError::InvalidParams("bad".into())),
        ("client not found: c1", StoreError::ClientNotFound("c1".into())),
        ("segment not found: s1", StoreError::SegmentNotFound("s1".into())),
        ("service unavailable", StoreError::ServiceUnavailable),
        ("etcd error: etcd-down", StoreError::EtcdError("etcd-down".into())),
        ("redis error: redis-down", StoreError::RedisError("redis-down".into())),
        ("K8s error: k8s-down", StoreError::K8sError("k8s-down".into())),
        ("S3 error: s3-down", StoreError::S3Error("s3-down".into())),
    ];

    for (expected, err) in &cases {
        assert_eq!(err.to_string(), *expected);
    }
}

#[test]
fn test_error_from_std_io() {
    use mooncake_store_core::StoreError;
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let store_err: StoreError = io_err.into();
    assert!(store_err.to_string().contains("file missing"));
}

#[test]
fn test_error_from_serde() {
    use mooncake_store_core::StoreError;
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let store_err: StoreError = json_err.into();
    assert!(store_err.to_string().contains("serialization error"));
}

#[test]
fn test_error_debug_format() {
    use mooncake_store_core::StoreError;
    let err = StoreError::OperationFailed(-5);
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("OperationFailed"));
    assert!(debug_str.contains("-5"));
}

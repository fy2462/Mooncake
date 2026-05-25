use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Segment
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: Uuid,
    pub name: String,
    pub size: u64,
    pub used: u64,
    pub client_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoFSegment {
    pub id: Uuid,
    pub name: String,
    pub base: u64,
    pub size: u64,
    pub te_endpoint: String,
    pub client_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoFSegmentOwnerInfo {
    pub segment_id: Uuid,
    pub client_id: Uuid,
}

// ---------------------------------------------------------------------------
// Replica
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaStatus {
    Undefined = 0,
    Allocating = 1,
    Written = 2,
    Complete = 3,
    Failed = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaType {
    Memory = 0,
    Disk = 1,
    LocalDisk = 2,
    NoFSsd = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectDataType {
    Unknown = 0,
    Kvcache = 1,
    Tensor = 2,
    Weight = 3,
    Sample = 4,
    Activation = 5,
    Gradient = 6,
    OptimizerState = 7,
    Metadata = 8,
    General = 9,
}

impl Default for ObjectDataType {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaDescriptor {
    pub segment_id: Uuid,
    pub segment_name: String,
    pub offset: u64,
    pub size: u64,
    pub status: ReplicaStatus,
    pub replica_type: ReplicaType,
    pub holder_client_id: Option<Uuid>,
}

// ---------------------------------------------------------------------------
// ReplicateConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateConfig {
    pub replica_num: u32,
    pub nof_replica_num: u32,
    pub with_soft_pin: bool,
    pub with_hard_pin: bool,
    pub preferred_segment: String,
    pub preferred_segments: Vec<String>,
    pub preferred_nof_segments: Vec<String>,
    pub prefer_alloc_in_same_node: bool,
    pub data_type: ObjectDataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageObjectMetadata {
    pub bucket_id: i64,
    pub offset: i64,
    pub key_size: i64,
    pub data_size: i64,
    pub transport_endpoint: String,
}

impl Default for ReplicateConfig {
    fn default() -> Self {
        Self {
            replica_num: 1,
            nof_replica_num: 0,
            with_soft_pin: false,
            with_hard_pin: false,
            preferred_segment: String::new(),
            preferred_segments: vec![],
            preferred_nof_segments: vec![],
            prefer_alloc_in_same_node: false,
            data_type: ObjectDataType::Unknown,
        }
    }
}

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskType {
    ReplicaCopy = 0,
    ReplicaMove = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending = 0,
    Processing = 1,
    Success = 2,
    Failed = 3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: Uuid,
    pub task_type: TaskType,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    pub assigned_client: Option<Uuid>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub id: Uuid,
    pub task_type: TaskType,
    pub payload: String,
    pub created_at_ms_epoch: i64,
    pub max_retry_attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleteRequest {
    pub id: Uuid,
    pub status: TaskStatus,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Client info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub id: Uuid,
    pub addresses: Vec<String>,
    pub segments: Vec<Segment>,
    pub last_seen: DateTime<Utc>,
}

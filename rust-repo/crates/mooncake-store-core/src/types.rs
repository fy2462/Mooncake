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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaDescriptor {
    pub segment_id: Uuid,
    pub segment_name: String,
    pub offset: u64,
    pub size: u64,
    pub status: ReplicaStatus,
    pub replica_type: ReplicaType,
}

// ---------------------------------------------------------------------------
// ReplicateConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateConfig {
    pub replica_num: u32,
    pub with_soft_pin: bool,
    pub with_hard_pin: bool,
    pub preferred_segment: String,
    pub prefer_alloc_in_same_node: bool,
}

impl Default for ReplicateConfig {
    fn default() -> Self {
        Self {
            replica_num: 1,
            with_soft_pin: false,
            with_hard_pin: false,
            preferred_segment: String::new(),
            prefer_alloc_in_same_node: false,
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

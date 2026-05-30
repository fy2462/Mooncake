//! Core types for the Mooncake Conductor.
//!
//! Ported from Go: mooncake-conductor/conductor-ctrl/common/types.go
//! and mooncake-conductor/conductor-ctrl/zmq/event_type.go

use serde::{Deserialize, Serialize};

// --- Service type constants ---

pub const SERVICE_TYPE_VLLM: &str = "vLLM";
pub const SERVICE_TYPE_MOONCAKE: &str = "Mooncake";

// --- Service configuration ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    /// KV publisher endpoint
    #[serde(default)]
    pub endpoint: String,
    /// Optional replay endpoint
    #[serde(default)]
    pub replay_endpoint: String,
    /// KV publisher type: vLLM or Mooncake
    #[serde(rename = "type", default)]
    pub service_type: String,
    /// Model name hosted by the service
    #[serde(default)]
    pub model_name: String,
    /// LoRA adapter name
    #[serde(default)]
    pub lora_name: String,
    /// Tenant ID (optional, defaults to "default")
    #[serde(default = "default_tenant")]
    pub tenant_id: String,
    /// Required instance ID
    #[serde(default)]
    pub instance_id: String,
    /// Block size
    #[serde(default)]
    pub block_size: i64,
    /// Data parallel rank
    #[serde(default)]
    pub dp_rank: i64,
    /// Additional salt for hash separation
    #[serde(default)]
    pub additional_salt: String,
}

fn default_tenant() -> String {
    "default".to_string()
}

// --- KV Events (conductor internal) ---

#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub block_hashes: Vec<u64>,
    pub block_size: i64,
    pub model_name: String,
    pub lora_name: String,
    pub instance_id: String,
    pub parent_block_hash: u64,
    pub token_ids: Vec<i32>,
    pub medium: String,
}

#[derive(Debug, Clone)]
pub struct RemovedEvent {
    pub block_hashes: Vec<u64>,
    pub model_name: String,
    pub lora_name: String,
    pub instance_id: String,
    pub block_size: i64,
    pub medium: String,
}

// --- Event types (from ZMQ layer) ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    BlockStored,
    BlockRemoved,
    BlockUpdate,
    AllBlocksCleared,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::BlockStored => write!(f, "BlockStored"),
            EventType::BlockRemoved => write!(f, "BlockRemoved"),
            EventType::BlockUpdate => write!(f, "BlockUpdate"),
            EventType::AllBlocksCleared => write!(f, "AllBlocksCleared"),
        }
    }
}

// --- ZMQ Event structs ---

#[derive(Debug, Clone)]
pub struct BlockStoredEvent {
    pub block_hashes: Vec<u64>,
    pub token_ids: Vec<i32>,
    pub parent_block_hash: u64,
    pub block_size: i64,
    pub mooncake_key: String,
    pub replica_list: Vec<Vec<String>>,
    pub model_name: String,
    pub lora_id: i64,
    pub lora_name: String,
    pub pod_name: String,
    pub medium: String,
}

#[derive(Debug, Clone)]
pub struct BlockRemovedEvent {
    pub block_hashes: Vec<u64>,
    pub model_name: String,
    pub pod_name: String,
}

#[derive(Debug, Clone)]
pub struct AllBlocksClearedEvent {
    pub model_name: String,
    pub pod_name: String,
}

#[derive(Debug, Clone)]
pub struct BlockUpdateEvent {
    pub block_hashes: Vec<u64>,
    pub token_ids: Vec<i32>,
    pub parent_block_hash: u64,
    pub model_name: String,
    pub pod_name: String,
    pub block_size: i64,
}

// --- Serializable event enum (replaces Box<dyn KVEvent> in EventBatch) ---

#[derive(Debug, Clone)]
pub enum KVEventData {
    BlockStored(BlockStoredEvent),
    BlockRemoved(BlockRemovedEvent),
    AllBlocksCleared(AllBlocksClearedEvent),
    BlockUpdate(BlockUpdateEvent),
}

impl KVEventData {
    pub fn event_type(&self) -> EventType {
        match self {
            KVEventData::BlockStored(_) => EventType::BlockStored,
            KVEventData::BlockRemoved(_) => EventType::BlockRemoved,
            KVEventData::AllBlocksCleared(_) => EventType::AllBlocksCleared,
            KVEventData::BlockUpdate(_) => EventType::BlockUpdate,
        }
    }
}

// --- Event batch ---

pub const SOURCE_MOONCAKE: &str = "mooncake";
pub const SOURCE_VLLM: &str = "vllm";

#[derive(Debug, Clone)]
pub struct EventBatch {
    /// Indicates the origin of the event batch
    pub source: String,
    pub events: Vec<KVEventData>,
    pub data_parallel_rank: i64,
}

// ModelContext, CacheHitResult, GlobalView are defined in prefix_index module.
// Re-export for convenience.
pub use crate::prefix_index::{CacheHitResult, GlobalView, ModelContext, ModelContextView};

// --- HTTP API request/response types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub replay_endpoint: String,
    #[serde(rename = "type")]
    pub service_type: String,
    #[serde(default)]
    pub modelname: String,
    #[serde(default)]
    pub lora_name: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub instance_id: String,
    #[serde(default)]
    pub block_size: i64,
    #[serde(default)]
    pub dp_rank: i64,
    #[serde(default)]
    pub additionalsalt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterRequest {
    #[serde(rename = "type")]
    pub service_type: String,
    #[serde(default)]
    pub modelname: String,
    #[serde(default)]
    pub lora_name: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub instance_id: String,
    #[serde(default)]
    pub block_size: i64,
    #[serde(default)]
    pub dp_rank: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub model: String,
    #[serde(default)]
    pub lora_name: Option<String>,
    #[serde(default)]
    pub lora_id: Option<i64>,
    pub token_ids: Vec<i32>,
    #[serde(default)]
    pub instance_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub block_size: i64,
    #[serde(default)]
    pub cache_salt: Option<String>,
}

// --- Helper ---

/// Build a composite service key: "{instance_id}|{tenant_id}|{dp_rank}"
pub fn make_service_key(instance_id: &str, tenant_id: &str, dp_rank: i64) -> String {
    format!("{}|{}|{}", instance_id, tenant_id, dp_rank)
}

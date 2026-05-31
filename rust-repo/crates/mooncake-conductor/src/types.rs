// ============================================================================
// Core types for the Mooncake Conductor.
// Mooncake Conductor 的核心类型定义。
//
// Ported from Go: mooncake-conductor/conductor-ctrl/common/types.go
// and mooncake-conductor/conductor-ctrl/zmq/event_type.go
// ============================================================================

use serde::{Deserialize, Serialize};

// ----------------------------------------------------------------------------
// Service type constants / 服务类型常量
// ----------------------------------------------------------------------------

pub const SERVICE_TYPE_VLLM: &str = "vLLM";
pub const SERVICE_TYPE_MOONCAKE: &str = "Mooncake";

// ----------------------------------------------------------------------------
// Service configuration / 服务配置
// ----------------------------------------------------------------------------

/// Per-instance service configuration, parsed from JSON config file.
/// 每个实例的服务配置，从 JSON 配置文件解析。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    /// KV publisher endpoint (ZMQ PUB socket address).
    /// KV 发布者端点（ZMQ PUB socket 地址）。
    #[serde(default)]
    pub endpoint: String,
    /// Optional replay endpoint for gap recovery (ZMQ DEALER socket).
    /// 可选的重放端点，用于断点恢复（ZMQ DEALER socket）。
    #[serde(default)]
    pub replay_endpoint: String,
    /// KV publisher type: "vLLM" or "Mooncake".
    /// KV 发布者类型："vLLM" 或 "Mooncake"。
    #[serde(rename = "type", default)]
    pub service_type: String,
    /// Model name hosted by this service instance.
    /// 此服务实例托管的模型名称。
    #[serde(default)]
    pub model_name: String,
    /// LoRA adapter name (empty if not using LoRA).
    /// LoRA 适配器名称（不使用时为空）。
    #[serde(default)]
    pub lora_name: String,
    /// Tenant ID for multi-tenant isolation (defaults to "default").
    /// 租户 ID，用于多租户隔离（默认为 "default"）。
    #[serde(default = "default_tenant")]
    pub tenant_id: String,
    /// Unique instance identifier (from config key or explicit field).
    /// 唯一实例标识符（来自配置 key 或显式字段）。
    #[serde(default)]
    pub instance_id: String,
    /// Block size (number of tokens per cache block).
    /// 块大小（每个缓存块的 token 数）。
    #[serde(default)]
    pub block_size: i64,
    /// Data parallel rank within the model replica group.
    /// 模型副本组内的数据并行 rank。
    #[serde(default)]
    pub dp_rank: i64,
    /// Additional salt for hash separation across deployments.
    /// 额外的盐值，用于跨部署的哈希隔离。
    #[serde(default)]
    pub additional_salt: String,
}

fn default_tenant() -> String {
    "default".to_string()
}

// ----------------------------------------------------------------------------
// Conductor-internal event types / Conductor 内部事件类型
// ----------------------------------------------------------------------------

/// Event representing a block being stored in the KV cache.
/// BlockStored 经过 event_handler 转换后，传给 prefix_index 的内部事件。
#[derive(Debug, Clone)]
pub struct StoredEvent {
    /// Engine-side block hashes (one per block).
    /// 引擎侧的块哈希值（每块一个）。
    pub block_hashes: Vec<u64>,
    /// Block size in tokens. / 块大小（token 数）。
    pub block_size: i64,
    /// Model name. / 模型名称。
    pub model_name: String,
    /// LoRA adapter name. / LoRA 适配器名称。
    pub lora_name: String,
    /// Source instance ID. / 来源实例 ID。
    pub instance_id: String,
    /// Parent block hash for chain computation (0 = root block).
    /// 父块哈希，用于链式计算（0 = 根块）。
    pub parent_block_hash: u64,
    /// Token IDs contained in the stored blocks.
    /// 存储块中包含的 token ID 序列。
    pub token_ids: Vec<i32>,
    /// Storage medium: "GPU", "cpu", etc. / 存储介质："GPU"、"cpu" 等。
    pub medium: String,
}

/// Event representing blocks being removed from the KV cache.
/// BlockRemoved 转换后的内部事件，传给 prefix_index。
#[derive(Debug, Clone)]
pub struct RemovedEvent {
    /// Engine-side block hashes to remove. / 要移除的引擎侧块哈希。
    pub block_hashes: Vec<u64>,
    /// Model name. / 模型名称。
    pub model_name: String,
    /// LoRA adapter name. / LoRA 适配器名称。
    pub lora_name: String,
    /// Source instance ID. / 来源实例 ID。
    pub instance_id: String,
    /// Block size in tokens. / 块大小（token 数）。
    pub block_size: i64,
    /// Storage medium. / 存储介质。
    pub medium: String,
}

// ----------------------------------------------------------------------------
// ZMQ-layer event types / ZMQ 层事件类型
// ----------------------------------------------------------------------------

/// Event type enum matching the Go conductor's event classification.
/// 事件类型枚举，匹配 Go conductor 的事件分类。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// A new block was stored. / 新块已存储。
    BlockStored,
    /// A block was removed. / 块已移除。
    BlockRemoved,
    /// An existing block was updated (token delta).
    /// 已有块被更新（token 增量变更）。
    BlockUpdate,
    /// All blocks for a model were cleared. / 模型的所有块已清除。
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

// ----------------------------------------------------------------------------
// ZMQ event payload structs / ZMQ 事件负载结构体
// ----------------------------------------------------------------------------

/// Raw BlockStored event decoded from ZMQ MessagePack payload.
/// 从 ZMQ MessagePack 负载解码的原始 BlockStored 事件。
#[derive(Debug, Clone)]
pub struct BlockStoredEvent {
    /// Engine-side block hashes. / 引擎侧块哈希。
    pub block_hashes: Vec<u64>,
    /// Token IDs in the stored blocks. / 存储块中的 token ID。
    pub token_ids: Vec<i32>,
    /// Parent block hash (0 = root). / 父块哈希（0 = 根）。
    pub parent_block_hash: u64,
    /// Block size in tokens. / 块大小。
    pub block_size: i64,
    /// Mooncake store key (Mooncake events only). / Mooncake store key（仅 Mooncake 事件）。
    pub mooncake_key: String,
    /// Replica list from Mooncake store (Mooncake events only).
    /// Mooncake store 副本列表（仅 Mooncake 事件）。
    pub replica_list: Vec<Vec<String>>,
    /// Model name — populated from service config during handling.
    /// 模型名称 —— 处理期间从服务配置填充。
    pub model_name: String,
    /// LoRA ID. / LoRA 标识符。
    pub lora_id: i64,
    /// LoRA adapter name — populated from service config.
    /// LoRA 适配器名称 —— 从服务配置填充。
    pub lora_name: String,
    /// Pod/instance name — set to cache_pool_key during consume.
    /// Pod/实例名称 —— 消费期间设置为 cache_pool_key。
    pub pod_name: String,
    /// Storage medium: "GPU", "cpu", "DRAM", etc. / 存储介质。
    pub medium: String,
}

/// Raw BlockRemoved event from ZMQ.
/// 从 ZMQ 解码的原始 BlockRemoved 事件。
#[derive(Debug, Clone)]
pub struct BlockRemovedEvent {
    /// Block hashes to remove. / 要移除的块哈希。
    pub block_hashes: Vec<u64>,
    /// Model name. / 模型名称。
    pub model_name: String,
    /// Pod/instance name. / Pod/实例名称。
    pub pod_name: String,
}

/// All-blocks-cleared event (full cache flush for a model).
/// 全部块清除事件（模型的完整缓存刷新）。
#[derive(Debug, Clone)]
pub struct AllBlocksClearedEvent {
    /// Model name whose blocks were cleared. / 块被清除的模型名称。
    pub model_name: String,
    /// Pod/instance name. / Pod/实例名称。
    pub pod_name: String,
}

/// Block update event (token delta for an existing block).
/// 块更新事件（已有块的 token 增量变更）。
#[derive(Debug, Clone)]
pub struct BlockUpdateEvent {
    /// Block hashes to update. / 要更新的块哈希。
    pub block_hashes: Vec<u64>,
    /// Updated token IDs. / 更新后的 token ID。
    pub token_ids: Vec<i32>,
    /// Parent block hash. / 父块哈希。
    pub parent_block_hash: u64,
    /// Model name. / 模型名称。
    pub model_name: String,
    /// Pod/instance name. / Pod/实例名称。
    pub pod_name: String,
    /// Block size. / 块大小。
    pub block_size: i64,
}

// ----------------------------------------------------------------------------
// Serializable event enum / 可序列化的事件枚举
// ----------------------------------------------------------------------------

/// Tagged union of all KV event variants — replaces `Box<dyn KVEvent>` from Go.
/// 所有 KV 事件变体的标记联合 —— 替代 Go 中的 `Box<dyn KVEvent>`。
#[derive(Debug, Clone)]
pub enum KVEventData {
    BlockStored(BlockStoredEvent),
    BlockRemoved(BlockRemovedEvent),
    AllBlocksCleared(AllBlocksClearedEvent),
    BlockUpdate(BlockUpdateEvent),
}

impl KVEventData {
    /// Return the event type discriminator for routing.
    /// 返回事件类型鉴别器，用于路由。
    pub fn event_type(&self) -> EventType {
        match self {
            KVEventData::BlockStored(_) => EventType::BlockStored,
            KVEventData::BlockRemoved(_) => EventType::BlockRemoved,
            KVEventData::AllBlocksCleared(_) => EventType::AllBlocksCleared,
            KVEventData::BlockUpdate(_) => EventType::BlockUpdate,
        }
    }
}

// ----------------------------------------------------------------------------
// Event batch / 事件批次
// ----------------------------------------------------------------------------

/// Source identifier for Mooncake events. / Mooncake 事件来源标识符。
pub const SOURCE_MOONCAKE: &str = "mooncake";
/// Source identifier for vLLM events. / vLLM 事件来源标识符。
pub const SOURCE_VLLM: &str = "vllm";

/// A batch of KV events decoded from a single ZMQ message.
/// 从单个 ZMQ 消息解码的 KV 事件批次。
#[derive(Debug, Clone)]
pub struct EventBatch {
    /// Origin of the event batch: "mooncake" or "vllm".
    /// 事件批次来源："mooncake" 或 "vllm"。
    pub source: String,
    /// All events in this batch. / 此批次中的所有事件。
    pub events: Vec<KVEventData>,
    /// Data parallel rank of the publishing engine.
    /// 发布引擎的数据并行 rank。
    pub data_parallel_rank: i64,
}

// ModelContext, CacheHitResult, GlobalView are defined in prefix_index module.
// Re-export for convenience.
// ModelContext, CacheHitResult, GlobalView 定义在 prefix_index 模块中，在此重新导出。
pub use crate::prefix_index::{CacheHitResult, GlobalView, ModelContext, ModelContextView};

// ----------------------------------------------------------------------------
// HTTP API request/response types / HTTP API 请求/响应类型
// ----------------------------------------------------------------------------

/// POST /register request body. / POST /register 请求体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// ZMQ PUB endpoint. / ZMQ PUB 端点。
    #[serde(default)]
    pub endpoint: String,
    /// ZMQ DEALER replay endpoint. / ZMQ DEALER 重放端点。
    #[serde(default)]
    pub replay_endpoint: String,
    /// Service type: "vLLM" or "Mooncake". / 服务类型。
    #[serde(rename = "type")]
    pub service_type: String,
    /// Model name. / 模型名称。
    #[serde(default)]
    pub modelname: String,
    /// Optional LoRA name. / 可选的 LoRA 名称。
    #[serde(default)]
    pub lora_name: Option<String>,
    /// Optional tenant ID. / 可选的租户 ID。
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Unique instance ID. / 唯一实例 ID。
    #[serde(default)]
    pub instance_id: String,
    /// Block size in tokens. / 块大小。
    #[serde(default)]
    pub block_size: i64,
    /// Data parallel rank. / 数据并行 rank。
    #[serde(default)]
    pub dp_rank: i64,
    /// Additional salt for hash computation. / 哈希计算的额外盐值。
    #[serde(default)]
    pub additionalsalt: Option<String>,
}

/// POST /unregister request body. / POST /unregister 请求体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterRequest {
    /// Service type. / 服务类型。
    #[serde(rename = "type")]
    pub service_type: String,
    /// Model name. / 模型名称。
    #[serde(default)]
    pub modelname: String,
    /// Optional LoRA name. / 可选的 LoRA 名称。
    #[serde(default)]
    pub lora_name: Option<String>,
    /// Optional tenant ID. / 可选的租户 ID。
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Instance ID to unregister. / 要注销的实例 ID。
    #[serde(default)]
    pub instance_id: String,
    /// Block size. / 块大小。
    #[serde(default)]
    pub block_size: i64,
    /// Data parallel rank. / 数据并行 rank。
    #[serde(default)]
    pub dp_rank: i64,
}

/// POST /query request body. / POST /query 请求体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Model name to query. / 要查询的模型名称。
    pub model: String,
    /// Optional LoRA name filter. / 可选的 LoRA 名称过滤。
    #[serde(default)]
    pub lora_name: Option<String>,
    /// Optional LoRA ID filter. / 可选的 LoRA ID 过滤。
    #[serde(default)]
    pub lora_id: Option<i64>,
    /// Token sequence to compute prefix hashes for.
    /// 用于计算前缀哈希的 token 序列。
    pub token_ids: Vec<i32>,
    /// Optional specific instance to query. / 可选的特定实例查询。
    #[serde(default)]
    pub instance_id: Option<String>,
    /// Optional tenant filter. / 可选的租户过滤。
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Block size for hash computation. / 哈希计算的块大小。
    #[serde(default)]
    pub block_size: i64,
    /// Optional cache salt. / 可选的缓存盐值。
    #[serde(default)]
    pub cache_salt: Option<String>,
}

// ----------------------------------------------------------------------------
// Helper / 辅助函数
// ----------------------------------------------------------------------------

/// Build a composite service key for deduplication and routing.
/// 构建复合服务 key，用于去重和路由。
///
/// Format: `"{instance_id}|{tenant_id}|{dp_rank}"`
pub fn make_service_key(instance_id: &str, tenant_id: &str, dp_rank: i64) -> String {
    format!("{}|{}|{}", instance_id, tenant_id, dp_rank)
}

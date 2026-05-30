//! Metadata types and etcd-backed metadata store.
//! 元数据类型和基于 etcd 的元数据存储。
//!
//! # Metadata Architecture / 元数据架构
//!
//! Metadata is stored in etcd under keys prefixed with `METADATA_KEY_PREFIX`
//! (`"mooncake/checkpoint/"`). Each key maps a payload name to its `Payload`
//! struct serialized as JSON.
//! 元数据存储在 etcd 中，键前缀为 `METADATA_KEY_PREFIX`（`"mooncake/checkpoint/"`）。
//! 每个键将负载名称映射到序列化为 JSON 的 `Payload` 结构体。
//!
//! # Data Model / 数据模型
//!
//! ```text
//! Payload                    Shard                     Location
//! ┌───────────┐            ┌───────────┐            ┌─────────────┐
//! │ name      │         1:N│ length    │         1:N│ segment_name│
//! │ size      │──────────►│ gold[]    │──────────►│ offset      │
//! │ size_list │           │ replica[] │           └─────────────┘
//! │ max_shard │           └───────────┘
//! │ shards[]  │
//! └───────────┘
//! ```
//!
//! - **Location**: Points to a specific byte range within a remote segment.
//!   指向远程段内特定字节范围。
//! - **Shard**: A chunk of data with a primary location (gold) and backup
//!   locations (replica_list) for fault tolerance.
//!   数据块，带有主位置（gold）和容错备份位置（replica_list）。
//! - **Payload**: The complete metadata for a named dataset, composed of
//!   multiple shards spread across nodes.
//!   命名数据集的完整元数据，由分布在多个节点上的多个分片组成。
//!
//! # Concurrency / 并发控制
//!
//! MetadataStore uses etcd transactions (compare-and-swap on mod_revision)
//! for atomic updates. This prevents lost updates when multiple nodes
//! concurrently modify the same payload's metadata.
//! MetadataStore 使用 etcd 事务（基于 mod_revision 的比较并交换）
//! 进行原子更新。这防止了多节点并发修改同一负载元数据时的更新丢失。

use crate::error::P2pStoreError;
use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp};
use serde::{Deserialize, Serialize};

/// Key prefix for all metadata stored in etcd.
/// etcd 中存储的所有元数据的键前缀。
///
/// Full key format: `mooncake/checkpoint/{payload_name}`
/// 完整键格式：`mooncake/checkpoint/{payload_name}`
pub const METADATA_KEY_PREFIX: &str = "mooncake/checkpoint/";

/// Location of a shard within a remote segment.
/// 远程段内分片的位置。
///
/// Identifies which node holds the data and at what byte offset.
/// 标识哪个节点持有数据以及在哪个字节偏移处。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Location {
    /// Segment name (typically `IP:port`) of the node holding the data.
    /// 持有数据的节点的段名称（通常为 `IP:port`）。
    pub segment_name: String,
    /// Byte offset within the segment where the shard data begins.
    /// 段内分片数据开始的字节偏移量。
    pub offset: u64,
}

/// A shard represents a contiguous chunk of data within a payload.
/// 分片表示负载中的连续数据块。
///
/// Each shard has a primary ("gold") location and a list of replica
/// locations. The replicas provide fault tolerance: if the primary
/// is unavailable, a replica can be tried.
/// 每个分片有一个主（"gold"）位置和一个副本位置列表。
/// 副本提供容错能力：如果主位置不可用，可以尝试副本。
///
/// # Location Selection / 位置选择
///
/// `get_location(retry)` implements a fallback strategy:
/// - `retry == 0`: Pick a random location from replicas or gold.
/// - `retry > 0`: Try replicas in order, then gold in order.
/// `get_location(retry)` 实现了回退策略：
/// - `retry == 0`：从副本或 gold 中随机选择一个位置。
/// - `retry > 0`：按顺序尝试副本，然后按顺序尝试 gold。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    /// Size of this shard in bytes.
    /// 此分片的大小（字节）。
    #[serde(rename = "size")]
    pub length: u64,
    /// Primary location list (the original data holders).
    /// 主位置列表（原始数据持有者）。
    pub gold: Vec<Location>,
    /// Replica location list (backup data holders).
    /// 副本位置列表（备份数据持有者）。
    pub replica_list: Vec<Location>,
}

impl Shard {
    /// Get a location for this shard, with retry-based fallback.
    /// 获取此分片的位置，支持基于重试的回退。
    ///
    /// On first attempt (retry=0), picks randomly for load balancing.
    /// On subsequent retries, picks deterministically for reliability.
    /// 首次尝试（retry=0）时随机选择以实现负载均衡。
    /// 后续重试时确定性选择以提高可靠性。
    pub fn get_location(&self, retry: usize) -> Option<&Location> {
        if retry == 0 {
            self.get_random_location()
        } else {
            self.get_retry_location(retry - 1)
        }
    }

    /// Pick a random location, preferring replicas over gold for load distribution.
    /// 随机选择一个位置，优先选择副本（而非 gold）以实现负载分布。
    pub fn get_random_location(&self) -> Option<&Location> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        if !self.replica_list.is_empty() {
            let idx = rng.gen_range(0..self.replica_list.len());
            Some(&self.replica_list[idx])
        } else if !self.gold.is_empty() {
            let idx = rng.gen_range(0..self.gold.len());
            Some(&self.gold[idx])
        } else {
            None
        }
    }

    /// Pick a deterministic location based on the retry index.
    /// Iterates through replicas first, then gold.
    /// 根据重试索引确定性选择位置。
    /// 首先遍历副本，然后遍历 gold。
    pub fn get_retry_location(&self, retry: usize) -> Option<&Location> {
        if self.replica_list.len() > retry {
            return Some(&self.replica_list[retry]);
        }
        let remain = retry - self.replica_list.len();
        if self.gold.len() > remain {
            return Some(&self.gold[remain]);
        }
        None
    }

    /// Returns true if this shard has no locations at all.
    /// 如果此分片没有任何位置，返回 true。
    pub fn is_empty(&self) -> bool {
        self.gold.is_empty() && self.replica_list.is_empty()
    }
}

/// Complete metadata for a named payload distributed across the cluster.
/// 分布在集群中的命名负载的完整元数据。
///
/// A Payload describes a logical dataset that has been split into fixed-size
/// shards (up to `max_shard_size`) and distributed across cluster nodes.
/// Each shard tracks where its data resides.
/// Payload 描述一个逻辑数据集，该数据集已被分割为固定大小的分片
/// （最多 `max_shard_size`）并分布在集群节点上。每个分片跟踪其数据所在位置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    /// Unique name of this payload (used as the etcd key).
    /// 此负载的唯一名称（用作 etcd 键）。
    pub name: String,
    /// Total size of the payload in bytes (sum of all size_list entries).
    /// 负载的总大小（字节）（所有 size_list 条目的总和）。
    pub size: u64,
    /// Sizes of individual memory buffers that make up this payload.
    /// 组成此负载的各个内存缓冲区的大小。
    pub size_list: Vec<u64>,
    /// Maximum size of each shard when splitting the payload.
    /// 分割负载时每个分片的最大大小。
    pub max_shard_size: u64,
    /// List of shards, each describing a chunk with its locations.
    /// 分片列表，每个分片描述一个数据块及其位置。
    pub shards: Vec<Shard>,
}

impl Payload {
    /// Returns true if all shards in this payload have empty location lists.
    /// This can indicate that all replicas have been removed (cleanup state).
    /// 如果此负载中的所有分片位置列表都为空，返回 true。
    /// 这可能表示所有副本已被移除（清理状态）。
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }
}

/// Lightweight summary of a payload for listing operations.
/// 负载的轻量级摘要，用于列出操作。
///
/// Unlike `Payload`, this does not include the full shard/location details.
/// Used for discovery (e.g., listing available checkpoints).
/// 与 `Payload` 不同，此结构不包含完整的分片/位置详情。
/// 用于发现（如列出可用的检查点）。
#[derive(Debug, Clone)]
pub struct PayloadInfo {
    pub name: String,
    pub max_shard_size: u64,
    pub total_size: u64,
    pub size_list: Vec<u64>,
}

// ---------------------------------------------------------------------------
// MetadataStore — etcd-backed metadata persistence
// MetadataStore — 基于 etcd 的元数据持久化
// ---------------------------------------------------------------------------

/// An etcd-backed metadata store for payload metadata.
/// 基于 etcd 的负载元数据存储。
///
/// Provides atomic CRUD operations on payload metadata using etcd
/// transactions. The store uses a configurable key prefix to namespace
/// all keys, allowing multiple stores to share the same etcd cluster.
/// 使用 etcd 事务提供负载元数据的原子 CRUD 操作。
/// 存储使用可配置的键前缀为所有键命名空间化，允许多个存储共享同一个 etcd 集群。
///
/// # Consistency / 一致性
///
/// - `create`: Uses etcd transaction with version=0 check (key must not exist).
///   使用 etcd 事务，通过 version=0 检查（键必须不存在）。
/// - `update`: Uses compare-and-swap on mod_revision to prevent lost updates.
///   使用基于 mod_revision 的比较并交换，防止更新丢失。
/// - `put`: Blind overwrite (for force_create scenarios).
///   盲写覆盖（用于 force_create 场景）。
pub struct MetadataStore {
    client: Client,
    key_prefix: String,
}

impl MetadataStore {
    /// Create a new MetadataStore connected to the given etcd endpoints.
    /// 创建连接到给定 etcd 端点的新 MetadataStore。
    ///
    /// `endpoints` can be a semicolon-separated list of etcd addresses.
    /// `key_prefix` is prepended to all keys stored in etcd.
    /// `endpoints` 可以是分号分隔的 etcd 地址列表。
    /// `key_prefix` 会添加到所有存储在 etcd 中的键前面。
    pub async fn new(endpoints: &str, key_prefix: &str) -> Result<Self, P2pStoreError> {
        let endpoints: Vec<&str> = endpoints.split(';').filter(|s| !s.is_empty()).collect();
        let client = Client::connect(endpoints, None)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.to_string(),
        })
    }

    /// Build the full etcd key for a payload name.
    /// 为负载名称构建完整的 etcd 键。
    fn full_key(&self, name: &str) -> Vec<u8> {
        format!("{}{}", self.key_prefix, name).into_bytes()
    }

    /// Create a new payload entry. Fails if the key already exists.
    /// 创建新的负载条目。如果键已存在则失败。
    ///
    /// Uses an etcd transaction: only writes if the key's version is 0
    /// (meaning it does not exist yet).
    /// 使用 etcd 事务：仅在键的版本为 0（即尚不存在）时写入。
    pub async fn create(&mut self, name: &str, payload: &Payload) -> Result<(), P2pStoreError> {
        let key = self.full_key(name);
        let json = serde_json::to_vec(payload)?;

        let txn = Txn::new()
            .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key, json, None)]);

        let resp = self
            .client
            .txn(txn)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;

        if !resp.succeeded() {
            return Err(P2pStoreError::MetadataError(format!(
                "key '{}' already exists",
                name
            )));
        }
        Ok(())
    }

    /// Blindly write (create or overwrite) a payload entry.
    /// 无条件写入（创建或覆盖）负载条目。
    ///
    /// Unlike `create`, this does not check for prior existence.
    /// Used for force_create scenarios where overwriting is desired.
    /// 与 `create` 不同，此方法不检查是否已存在。
    /// 用于需要覆盖的 force_create 场景。
    pub async fn put(&mut self, name: &str, payload: &Payload) -> Result<(), P2pStoreError> {
        let key = self.full_key(name);
        let json = serde_json::to_vec(payload)?;
        self.client
            .put(key, json, None)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
        Ok(())
    }

    /// Retrieve a payload by name.
    /// 按名称检索负载。
    ///
    /// Returns `(Option<Payload>, mod_revision)`.
    /// The `mod_revision` is needed for subsequent atomic `update` calls.
    /// 返回 `(Option<Payload>, mod_revision)`。
    /// `mod_revision` 需要用于后续的原子 `update` 调用。
    pub async fn get(&mut self, name: &str) -> Result<(Option<Payload>, i64), P2pStoreError> {
        let key = self.full_key(name);
        let resp = self
            .client
            .get(key, None)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;

        if let Some(kv) = resp.kvs().first() {
            let payload: Payload = serde_json::from_slice(kv.value())?;
            Ok((Some(payload), kv.mod_revision()))
        } else {
            Ok((None, -1))
        }
    }

    /// Atomically update a payload using compare-and-swap on mod_revision.
    /// 使用 mod_revision 的比较并交换原子更新负载。
    ///
    /// Only succeeds if the current mod_revision matches `revision`,
    /// preventing lost updates from concurrent modifications.
    /// 仅当当前 mod_revision 与 `revision` 匹配时成功，
    /// 防止并发修改导致的更新丢失。
    ///
    /// If the updated payload is empty (all shards cleared), the key is
    /// deleted instead of written — this signals that the payload has been
    /// fully removed from the cluster.
    /// 如果更新后的负载为空（所有分片已清除），则删除键而非写入——
    /// 这表示负载已从集群中完全移除。
    ///
    /// Returns `true` if the CAS succeeded, `false` if the revision changed.
    /// 返回 `true` 表示 CAS 成功，`false` 表示版本已变化。
    pub async fn update(
        &mut self,
        name: &str,
        payload: &Payload,
        revision: i64,
    ) -> Result<bool, P2pStoreError> {
        let key = self.full_key(name);

        if payload.is_empty() {
            let txn = Txn::new()
                .when([Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    revision,
                )])
                .and_then([TxnOp::delete(key, None)]);

            let resp = self
                .client
                .txn(txn)
                .await
                .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
            Ok(resp.succeeded())
        } else {
            let json = serde_json::to_vec(payload)?;
            let txn = Txn::new()
                .when([Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    revision,
                )])
                .and_then([TxnOp::put(key, json, None)]);

            let resp = self
                .client
                .txn(txn)
                .await
                .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
            Ok(resp.succeeded())
        }
    }

    /// List all payloads whose names start with the given prefix.
    /// 列出名称以给定前缀开头的所有负载。
    ///
    /// Uses etcd's prefix range query. Returns full `Payload` objects
    /// (deserialized from JSON).
    /// 使用 etcd 的前缀范围查询。返回完整的 `Payload` 对象（从 JSON 反序列化）。
    pub async fn list(&mut self, prefix: &str) -> Result<Vec<Payload>, P2pStoreError> {
        let search_key = format!("{}{}", self.key_prefix, prefix).into_bytes();
        let opts = GetOptions::new().with_prefix();
        let resp = self
            .client
            .get(search_key, Some(opts))
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;

        let mut results = Vec::new();
        for kv in resp.kvs() {
            let payload: Payload = serde_json::from_slice(kv.value())?;
            results.push(payload);
        }
        Ok(results)
    }
}

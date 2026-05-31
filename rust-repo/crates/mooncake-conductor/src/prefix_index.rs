// ============================================================================
// Prefix Cache Index — 前缀缓存索引
//
// Core data structure for the Conductor service. Maintains an in-memory
// prefix-hash table that maps token-sequence prefixes to cache locations.
// Inference engines query this index to discover which blocks already exist
// in the distributed KV cache, avoiding redundant computation.
//
// Conductor 服务的核心数据结构。维护内存中的前缀哈希表，
// 将 token 序列前缀映射到缓存位置。推理引擎查询此索引以发现
// 哪些块已存在于分布式 KV 缓存中，避免冗余计算。
//
// Key concepts / 核心概念:
//
//   1. ModelContext: unique key for a model variant (model + lora + block_size
//      + salt + tenant). Each context has its own prefix store with independent
//      hash seeds.
//      ModelContext：模型变体的唯一 key。每个上下文有独立的前缀存储和哈希种子。
//
//   2. Chained hashing: blocks are hashed as a chain: H_i = xxh64(H_{i-1} ||
//      token_ids_i). This means every block's hash depends on all preceding
//      blocks in the sequence — enabling prefix matching.
//      链式哈希：每个块的哈希依赖所有前驱块 —— 实现前缀匹配。
//
//   3. Proxy hash mapping: engine-side block hashes are mapped to conductor-side
//      prefix hashes. This indirection allows the conductor to maintain its own
//      hash namespace independent of engine implementations.
//      代理哈希映射：引擎侧块哈希 → conductor 侧前缀哈希。
//      这一间接层使 conductor 的哈希空间独立于引擎实现。
//
//   4. DP-aware hit counting: cache hits are counted per data-parallel rank,
//      enabling load-aware request routing by the frontend.
//      DP 感知的命中计数：按数据并行 rank 统计命中，前端据此做负载感知路由。
//
// Architecture / 架构:
//
//   PrefixCacheTable
//   └── DashMap<ModelContext, ContextData>
//       └── ContextData
//           ├── prefix_store: HashMapStore
//           │   └── HashMap<u64, CacheStoreInfo>
//           │       └── CacheStoreInfo
//           │           ├── engine_last_access_time (per-instance LRU)
//           │           ├── total_replica_nums (eviction guard: skip if zero)
//           │           ├── medium_set (GPU, cpu, etc.)
//           │           └── dp_rank_set (which DP ranks hold this block)
//           ├── seed: u64 (from xxh64(additional_salt))
//           ├── dp_size: HashSet<i64>
//           └── proxy_hash_mapping: HashMap<u64, u64> (engine_hash → conductor_hash)
//
// Ported from Go: mooncake-conductor/conductor-ctrl/kvevent/prefix_index.go
// ============================================================================

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Serialize;
use tracing::{debug, error, warn};
use xxhash_rust::xxh64::xxh64;

use crate::types::{RemovedEvent, StoredEvent};

// ----------------------------------------------------------------------------
// ModelContext — unique key for a model variant
// ModelContext —— 模型变体的唯一标识
// ----------------------------------------------------------------------------

/// Identifies a unique model variant for prefix indexing.
/// Model name + LoRA name + block size + salt + tenant together form the key.
/// 标识前缀索引的唯一模型变体。模型名 + LoRA 名 + 块大小 + 盐值 + 租户共同构成 key。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ModelContext {
    pub model_name: String,
    pub lora_name: String,
    pub block_size: i64,
    pub additional_salt: String,
    pub tenant_id: String,
}

// ----------------------------------------------------------------------------
// CacheStoreInfo — per-prefix-hash cache metadata
// CacheStoreInfo —— 每个前缀哈希的缓存元数据
// ----------------------------------------------------------------------------

/// Stores information about which engines hold a given prefix block.
/// 存储哪些引擎持有给定前缀块的信息。
#[derive(Debug)]
struct CacheStoreInfo {
    /// Per-instance last access timestamps (epoch seconds), for LRU eviction.
    /// 每个实例的最后访问时间戳（epoch 秒），用于 LRU 驱逐。
    engine_last_access_time: HashMap<String, AtomicI64>,
    /// Total number of replicas for this prefix. Cache hit is skipped when zero.
    /// 此前缀的副本总数。为零时跳过缓存命中。
    total_replica_nums: AtomicI64,
    /// Set of storage media types holding this block (e.g. "GPU", "cpu").
    /// 持有此块的存储介质类型集合（如 "GPU"、"cpu"）。
    medium_set: HashSet<String>,
    /// Set of data-parallel ranks that hold this block.
    /// 持有此块的数据并行 rank 集合。
    dp_rank_set: HashSet<i64>,
}

impl CacheStoreInfo {
    fn new() -> Self {
        Self {
            engine_last_access_time: HashMap::new(),
            total_replica_nums: AtomicI64::new(0),
            medium_set: HashSet::new(),
            dp_rank_set: HashSet::new(),
        }
    }
}

// ----------------------------------------------------------------------------
// HashMapStore — the per-context prefix → cache-info map
// HashMapStore —— 每个上下文的 prefix → cache-info 映射
// ----------------------------------------------------------------------------

/// The core prefix store for a single ModelContext.
/// Maps conductor-side prefix hashes to CacheStoreInfo.
/// 单个 ModelContext 的核心前缀存储。将 conductor 侧前缀哈希映射到 CacheStoreInfo。
#[derive(Debug)]
struct HashMapStore {
    /// Hash map: conductor hash → cache metadata.
    /// 哈希映射：conductor 哈希 → 缓存元数据。
    prefix_map: HashMap<u64, CacheStoreInfo>,
    /// Last access time for LRU eviction of stale contexts.
    /// 最后访问时间，用于驱逐过期上下文。
    last_access: AtomicI64,
    /// Total number of unique prefix entries in this store.
    /// 此存储中唯一前缀条目的总数。
    total_prefixes: i64,
}

impl HashMapStore {
    fn new() -> Self {
        Self {
            prefix_map: HashMap::new(),
            last_access: AtomicI64::new(now_unix()),
            total_prefixes: 0,
        }
    }
}

/// Return current Unix timestamp in seconds. / 返回当前 Unix 时间戳（秒）。
fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ----------------------------------------------------------------------------
// ContextData — per-ModelContext state
// ContextData —— 每个 ModelContext 的状态
// ----------------------------------------------------------------------------

/// All state associated with a single ModelContext.
/// 与单个 ModelContext 关联的全部状态。
#[derive(Debug)]
struct ContextData {
    /// The prefix → cache-info map (RWLock for concurrent read/write).
    /// 前缀 → 缓存信息映射（RWLock 支持并发读写）。
    prefix_store: RwLock<HashMapStore>,
    /// Hash seed derived from `additional_salt`. / 由 additional_salt 派生的哈希种子。
    seed: u64,
    /// Set of DP ranks registered for this context.
    /// 为此上下文注册的 DP rank 集合。
    dp_size: RwLock<HashSet<i64>>,
    /// Maps engine-side block hashes to conductor-side prefix hashes.
    /// Go: proxyHashMap. / 将引擎侧块哈希映射到 conductor 侧前缀哈希。
    proxy_hash_mapping: RwLock<HashMap<u64, u64>>,
}

impl ContextData {
    fn new(seed: u64) -> Self {
        Self {
            prefix_store: RwLock::new(HashMapStore::new()),
            seed,
            dp_size: RwLock::new(HashSet::new()),
            proxy_hash_mapping: RwLock::new(HashMap::new()),
        }
    }
}

// ----------------------------------------------------------------------------
// CacheHitResult — query result / 缓存命中查询结果
// ----------------------------------------------------------------------------

/// Result of a cache-hit query: how many tokens matched, and where the blocks live.
/// 缓存命中查询结果：匹配了多少 token，以及块的位置分布。
#[derive(Debug, Clone, Serialize)]
pub struct CacheHitResult {
    /// Number of consecutive tokens from the start that are cached.
    /// 从开头起连续命中的 token 数量。
    #[serde(rename = "longest_matched")]
    pub longest_match_tokens: i64,
    /// Token count per DP rank for load-aware routing.
    /// 每个 DP rank 的 token 数量，用于负载感知路由。
    #[serde(rename = "DP")]
    pub dp: HashMap<i64, i64>,
    /// Token count on GPU. / GPU 上的 token 数。
    #[serde(rename = "GPU")]
    pub gpu: i64,
    /// Token count on CPU. / CPU 上的 token 数。
    #[serde(rename = "CPU")]
    pub cpu: i64,
    /// Token count on disk. / 磁盘上的 token 数。
    #[serde(rename = "DISK")]
    pub disk: i64,
}

impl CacheHitResult {
    pub fn new() -> Self {
        Self {
            longest_match_tokens: 0,
            dp: HashMap::new(),
            gpu: 0,
            cpu: 0,
            disk: 0,
        }
    }
}

// ----------------------------------------------------------------------------
// GlobalView — diagnostic snapshot / 诊断快照
// ----------------------------------------------------------------------------

/// Lightweight context info for the global view endpoint.
/// 全局视图端点的轻量上下文信息。
#[derive(Debug, Clone, Serialize)]
pub struct ModelContextView {
    pub model_name: String,
    pub lora_name: String,
    pub block_size: i64,
    pub additional_salt: String,
    pub tenant_id: String,
}

/// Complete snapshot of all contexts for debugging (/global_view).
/// 所有上下文的完整快照，用于调试（/global_view 端点）。
#[derive(Debug, Clone, Serialize)]
pub struct GlobalView {
    /// Number of active model contexts. / 活跃的模型上下文数量。
    pub context_count: i32,
    /// Metadata for each context. / 每个上下文的元数据。
    pub model_contexts: Vec<ModelContextView>,
    /// Snapshot of all proxy hash maps. / 所有代理哈希映射的快照。
    pub proxy_hashmap: Vec<HashMap<u64, u64>>,
}

// ============================================================================
// PrefixCacheTable — the main index / 主索引
// ============================================================================

/// The central in-memory prefix cache index.
/// Thread-safe via DashMap (sharded concurrent map) + per-context RwLock.
///
/// 中央内存前缀缓存索引。通过 DashMap（分段并发 map）+ 每上下文 RwLock 实现线程安全。
pub struct PrefixCacheTable {
    /// All model contexts and their associated state.
    /// 所有模型上下文及其关联状态。
    context_map: DashMap<ModelContext, ContextData>,
    /// Atomic counter for the number of active contexts.
    /// 活跃上下文数量的原子计数器。
    context_count: AtomicI32,
}

impl PrefixCacheTable {
    pub fn new() -> Self {
        Self {
            context_map: DashMap::new(),
            context_count: AtomicI32::new(0),
        }
    }

    // ------------------------------------------------------------------------
    // Seed generation / 种子生成
    // ------------------------------------------------------------------------

    /// Generate a seed for hash computation. Uses `CONDUCTOR_SEED` env var if
    /// set to a non-negative value; otherwise generates a random seed.
    ///
    /// 生成哈希计算的种子。如果设置了非负的 `CONDUCTOR_SEED` 则使用环境变量；
    /// 否则生成随机种子。
    pub fn generate_seed() -> u64 {
        use rand::Rng;
        let env_seed = std::env::var("CONDUCTOR_SEED")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(-1);

        if env_seed != -1 {
            env_seed as u64
        } else {
            rand::thread_rng().gen()
        }
    }

    // ------------------------------------------------------------------------
    // Context management / 上下文管理
    // ------------------------------------------------------------------------

    /// Lazily create context data for a ModelContext if it doesn't exist.
    /// The hash seed is derived from `xxh64(additional_salt, 0)`, matching Go.
    ///
    /// 如果 ModelContext 不存在则惰性创建。哈希种子从 `xxh64(additional_salt, 0)` 派生，
    /// 与 Go 实现一致。
    fn get_context_data(&self, model_context: &ModelContext) {
        if self.context_map.contains_key(model_context) {
            return;
        }

        let seed = xxh64(model_context.additional_salt.as_bytes(), 0);
        let context_data = ContextData::new(seed);
        context_data
            .prefix_store
            .read()
            .last_access
            .store(now_unix(), Ordering::SeqCst);

        debug!("in get_context_data, model_context={:?}", model_context);
        self.context_count.fetch_add(1, Ordering::SeqCst);
        self.context_map.insert(model_context.clone(), context_data);
    }

    /// Get a reference to existing context data. / 获取已有上下文数据的引用。
    fn get_context(
        &self,
        model_context: &ModelContext,
    ) -> Option<dashmap::mapref::one::Ref<'_, ModelContext, ContextData>> {
        self.context_map.get(model_context)
    }

    // ------------------------------------------------------------------------
    // DP registration / DP 注册
    // ------------------------------------------------------------------------

    /// Register a data-parallel rank for a model context.
    /// Called during service registration (both static and dynamic).
    ///
    /// 为模型上下文注册数据并行 rank。在服务注册时调用（静态和动态注册均调用）。
    pub fn add_dp_size(&self, model_context: &ModelContext, _instance_id: &str, dp_rank: i64) {
        self.get_context_data(model_context);
        if let Some(ctx) = self.get_context(model_context) {
            ctx.dp_size.write().insert(dp_rank);
        }
    }

    // ------------------------------------------------------------------------
    // Hash computation / 哈希计算
    // ------------------------------------------------------------------------

    /// Core hash function matching Go's `computeHash`.
    ///
    /// Go writes parent_hash as 8 bytes (little-endian uint64), then each
    /// token_id as 8 bytes (little-endian uint32 in the first 4 bytes,
    /// 4 zero bytes remaining since Go uses a `[8]byte` buffer).
    /// This function replicates that exact byte layout for hash compatibility.
    ///
    /// 核心哈希函数，匹配 Go 的 `computeHash`。
    /// Go 将 parent_hash 写为 8 字节（little-endian uint64），
    /// 然后将每个 token_id 写为 8 字节（前 4 字节为 little-endian uint32，
    /// 后 4 字节为零，匹配 Go 的 `[8]byte` 缓冲区）。
    /// 本函数完全复制该字节布局以保证哈希兼容性。
    pub fn compute_hash(parent_hash: u64, block_token_ids: &[i32]) -> u64 {
        // 8 bytes for parent_hash + 8 bytes per token_id
        let cap = 8 + block_token_ids.len() * 8;
        let mut buf = Vec::with_capacity(cap);
        buf.extend_from_slice(&parent_hash.to_le_bytes());
        for &token_id in block_token_ids {
            // Go: put uint32 into first 4 bytes of [8]byte, remaining 4 bytes stay zero
            // Go 风格：uint32 放入 [8]byte 的前 4 字节，后 4 字节保持为零
            buf.extend_from_slice(&(token_id as u32).to_le_bytes());
            buf.extend_from_slice(&[0u8; 4]);
        }
        xxh64(&buf, 0)
    }

    /// Compute prefix hashes for a sequence of token IDs using chained xxhash64.
    /// Each block of `block_size` tokens is hashed with the previous block's hash
    /// as parent, forming a hash chain. Returns one hash per block.
    ///
    /// 使用链式 xxhash64 计算 token ID 序列的前缀哈希。
    /// 每个 `block_size` 大小的 token 块与前一块的哈希值作为 parent 一起哈希，
    /// 形成哈希链。每块返回一个哈希值。
    pub fn compute_prefix_hash(
        &self,
        model_context: &ModelContext,
        token_ids: &[i32],
        cache_salt: u64,
    ) -> Vec<u64> {
        let block_size = model_context.block_size as usize;
        let num_blocks = token_ids.len() / block_size;
        let mut prefix_hashes = Vec::with_capacity(num_blocks);

        let mut parent_hash = cache_salt;

        for i in 0..num_blocks {
            let start = i * block_size;
            let end = start + block_size;
            if end > token_ids.len() {
                break;
            }
            let hash_value = Self::compute_hash(parent_hash, &token_ids[start..end]);
            prefix_hashes.push(hash_value);
            parent_hash = hash_value;
        }

        prefix_hashes
    }

    // ------------------------------------------------------------------------
    // Cache hit computation / 缓存命中计算
    // ------------------------------------------------------------------------

    /// Compute cache hit statistics for a token sequence.
    /// Walks prefix hashes sequentially; stops at the first hash not found
    /// in the prefix store. Aggregates hit counts by medium (GPU/CPU/DISK)
    /// and by DP rank for load-aware scheduling.
    ///
    /// 计算 token 序列的缓存命中统计。顺序遍历前缀哈希；
    /// 在第一个未找到的哈希处停止。按介质（GPU/CPU/DISK）
    /// 和 DP rank 聚合命中数，用于负载感知调度。
    pub fn cache_hit_compute(
        &self,
        model_context: &ModelContext,
        token_ids: &[i32],
        _instance_id: &str,
    ) -> CacheHitResult {
        let mut result = CacheHitResult::new();

        let Some(context_data) = self.context_map.get(model_context) else {
            error!("In CacheHitCompute, contextData not found");
            return result;
        };

        let cache_salt = xxh64(model_context.additional_salt.as_bytes(), 0);
        let prefix_hashes = self.compute_prefix_hash(model_context, token_ids, cache_salt);

        debug!("In CacheHitCompute, prefix_hashes={:?}", prefix_hashes);

        let prefix_store = context_data.prefix_store.read();

        for prefix_hash in &prefix_hashes {
            let Some(cache_store_info) = prefix_store.prefix_map.get(prefix_hash) else {
                // First miss: stop walking (no further blocks can match).
                // 首次未命中：停止遍历（后续块也不可能匹配）。
                break;
            };

            // Skip if all replicas have been evicted (guard against stale entries).
            // 如果所有副本已被驱逐则跳过（防止过期条目）。
            if cache_store_info.total_replica_nums.load(Ordering::SeqCst) == 0 {
                break;
            }

            let mut cache_hit = false;

            for medium in &cache_store_info.medium_set {
                debug!("In CacheHitCompute, medium={}", medium);
                match medium.as_str() {
                    "cpu" => {
                        result.cpu += model_context.block_size;
                        cache_hit = true;
                    }
                    "GPU" => {
                        result.gpu += model_context.block_size;
                        cache_hit = true;
                    }
                    _ => {
                        warn!("In CacheHitCompute, unknown medium type: {}", medium);
                    }
                }
            }

            if cache_hit {
                result.longest_match_tokens += model_context.block_size;
                for &dp_rank in &cache_store_info.dp_rank_set {
                    *result.dp.entry(dp_rank).or_insert(0) += model_context.block_size;
                }
            }
        }

        drop(prefix_store);
        // Update last access time for LRU eviction of stale contexts.
        // 更新最后访问时间，用于驱逐过期上下文。
        context_data
            .prefix_store
            .read()
            .last_access
            .store(now_unix(), Ordering::SeqCst);

        result
    }

    // ------------------------------------------------------------------------
    // Store event processing / 存储事件处理
    // ------------------------------------------------------------------------

    /// Process a store event: computes conductor hashes for engine-side block
    /// hashes and registers them in the prefix store.
    ///
    /// Lock order (matching Go): proxy_hash_mapping (write) → prefix_store (write)
    ///
    /// 处理存储事件：计算引擎侧块哈希对应的 conductor 哈希并注册到前缀存储。
    /// 锁顺序（匹配 Go）：proxy_hash_mapping (写) → prefix_store (写)
    pub fn process_store_event(
        &self,
        event: &StoredEvent,
        dp_rank: i64,
        instance_id: &str,
    ) -> Result<(), String> {
        if event.block_hashes.is_empty() {
            return Ok(());
        }

        let tenant_id = "default";

        debug!(
            "In ProcessStoreEvent, model_name={}, instance_id={}, dp_rank={}",
            event.model_name, instance_id, dp_rank
        );

        let model_context = ModelContext {
            model_name: event.model_name.clone(),
            lora_name: event.lora_name.clone(),
            block_size: event.block_size,
            tenant_id: tenant_id.to_string(),
            additional_salt: String::new(),
        };

        self.get_context_data(&model_context);
        let context_data = self.get_context(&model_context).unwrap();
        let mut proxy_hash_map = context_data.proxy_hash_mapping.write();

        // Mooncake events may have a single block hash but variable-length tokens.
        // Skip for now; only process events where tokens align with block_hashes.
        // Mooncake 事件可能单块哈希但 token 长度可变，暂时跳过此类情况。
        if event.block_hashes.len() * event.block_size as usize != event.token_ids.len() {
            if event.block_hashes.len() != 1 {
                return Err("block hashes and tokens length mismatch".to_string());
            }
            return Ok(());
        }

        #[derive(Debug)]
        struct NewPrefix {
            hash_value: u64,
            engine_id: String,
        }

        let mut new_prefix_store: Vec<NewPrefix> = Vec::new();
        let mut parent_hash = context_data.seed;

        debug!("In ProcessStoreEvent, seed={}", parent_hash);

        // If this block has a parent, resolve it through the proxy hash map.
        // 如果此块有父块，通过代理哈希映射解析。
        if event.parent_block_hash != 0 {
            debug!("parent Block HASH is not None.");
            if let Some(&pbh) = proxy_hash_map.get(&event.parent_block_hash) {
                parent_hash = pbh;
            }
        }

        for (i, &block_hash) in event.block_hashes.iter().enumerate() {
            // Cache already exists: add engine info and continue.
            // 缓存已存在：添加引擎信息并继续。
            if let Some(&existing_hash) = proxy_hash_map.get(&block_hash) {
                new_prefix_store.push(NewPrefix {
                    hash_value: existing_hash,
                    engine_id: event.instance_id.clone(),
                });
                continue;
            }

            // Compute new conductor hash using the parent hash chain.
            // 使用父哈希链计算新的 conductor 哈希。
            let hash_value = Self::compute_hash(
                parent_hash,
                &event.token_ids
                    [i * event.block_size as usize..(i + 1) * event.block_size as usize],
            );
            parent_hash = hash_value;

            proxy_hash_map.insert(block_hash, hash_value);

            new_prefix_store.push(NewPrefix {
                hash_value,
                engine_id: event.instance_id.clone(),
            });
        }

        if !new_prefix_store.is_empty() {
            let mut prefix_store = context_data.prefix_store.write();

            for new_prefix in &new_prefix_store {
                debug!("show new prefix data, new_prefix={:?}", new_prefix);
                Self::add_new_prefix_store(
                    &mut prefix_store,
                    new_prefix.hash_value,
                    &new_prefix.engine_id,
                    &event.medium,
                    dp_rank,
                );
            }
        }

        Ok(())
    }

    // ------------------------------------------------------------------------
    // Remove event processing / 移除事件处理
    // ------------------------------------------------------------------------

    /// Process a remove event: removes entries from the prefix store and cleans
    /// up proxy hash mappings.
    ///
    /// Lock order (matching Go): proxy_hash_mapping (write) → prefix_store (write)
    ///
    /// 处理移除事件：从前缀存储中删除条目并清理代理哈希映射。
    /// 锁顺序（匹配 Go）：proxy_hash_mapping (写) → prefix_store (写)
    pub fn process_remove_event(
        &self,
        event: &RemovedEvent,
        _dp_rank: i64,
        _instance_id: &str,
    ) -> Result<(), String> {
        if event.block_hashes.is_empty() {
            return Ok(());
        }

        let model_context = ModelContext {
            model_name: event.model_name.clone(),
            lora_name: event.lora_name.clone(),
            block_size: event.block_size,
            tenant_id: "default".to_string(),
            additional_salt: String::new(),
        };

        self.get_context_data(&model_context);
        let context_data = self.get_context(&model_context).unwrap();
        let mut proxy_hash_map = context_data.proxy_hash_mapping.write();

        let mut remove_conductor_hashes: Vec<u64> = Vec::with_capacity(event.block_hashes.len());

        // Delete from proxy hash mapping: engine_hash → conductor_hash.
        // 从代理哈希映射中删除：engine_hash → conductor_hash。
        for &block_hash in &event.block_hashes {
            if let Some(conductor_hash) = proxy_hash_map.remove(&block_hash) {
                remove_conductor_hashes.push(conductor_hash);
            }
        }

        let mut prefix_store = context_data.prefix_store.write();

        for conductor_hash in &remove_conductor_hashes {
            prefix_store.prefix_map.remove(conductor_hash);
            prefix_store.total_prefixes -= 1;
        }

        Ok(())
    }

    // ------------------------------------------------------------------------
    // Internal helpers / 内部辅助函数
    // ------------------------------------------------------------------------

    /// Add a new prefix entry to the HashMapStore, updating all metadata.
    /// 向 HashMapStore 添加新的前缀条目，更新所有元数据。
    fn add_new_prefix_store(
        prefix_store: &mut HashMapStore,
        hash_value: u64,
        instance_id: &str,
        medium: &str,
        dp_rank: i64,
    ) {
        let now = now_unix();

        let cache_store_info = prefix_store
            .prefix_map
            .entry(hash_value)
            .or_insert_with(|| {
                prefix_store.total_prefixes += 1;
                CacheStoreInfo::new()
            });

        // Update per-instance last access time. / 更新单实例最后访问时间。
        cache_store_info
            .engine_last_access_time
            .entry(instance_id.to_string())
            .or_insert_with(|| AtomicI64::new(now))
            .store(now, Ordering::SeqCst);

        cache_store_info
            .total_replica_nums
            .fetch_add(1, Ordering::SeqCst);
        cache_store_info.medium_set.insert(medium.to_string());
        cache_store_info.dp_rank_set.insert(dp_rank);

        debug!(
            "in add_new_prefix_store, conductor_hash={}, current_medium={}",
            hash_value, medium
        );
    }

    // ------------------------------------------------------------------------
    // Global view / 全局视图
    // ------------------------------------------------------------------------

    /// Get a global view of all contexts for diagnostics (/global_view endpoint).
    /// 获取所有上下文的全局视图，用于诊断（/global_view 端点）。
    pub fn get_global_view(&self) -> GlobalView {
        let context_count = self.context_count.load(Ordering::SeqCst);
        let mut model_contexts = Vec::new();
        let mut proxy_hashes = Vec::new();

        for entry in self.context_map.iter() {
            let ctx = entry.key();
            let context_data = entry.value();

            let ctx_view = ModelContextView {
                model_name: ctx.model_name.clone(),
                lora_name: ctx.lora_name.clone(),
                block_size: ctx.block_size,
                additional_salt: ctx.additional_salt.clone(),
                tenant_id: ctx.tenant_id.clone(),
            };

            let hashmap_clone = context_data.proxy_hash_mapping.read().clone();
            proxy_hashes.push(hashmap_clone);
            model_contexts.push(ctx_view);
        }

        GlobalView {
            context_count,
            model_contexts,
            proxy_hashmap: proxy_hashes,
        }
    }
}

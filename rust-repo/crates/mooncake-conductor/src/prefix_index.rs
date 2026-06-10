use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Serialize;
use tracing::{debug, error, warn};
use xxhash_rust::xxh64::xxh64;

use crate::types::{RemovedEvent, StoredEvent};
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ModelContext {
    pub model_name: String,
    pub lora_name: String,
    pub block_size: i64,
    pub additional_salt: String,
    pub tenant_id: String,
}
#[derive(Debug)]
struct CacheStoreInfo {
    engine_last_access_time: HashMap<String, AtomicI64>,
    total_replica_nums: AtomicI64,
    medium_set: HashSet<String>,
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
#[derive(Debug)]
struct HashMapStore {
    prefix_map: HashMap<u64, CacheStoreInfo>,
    last_access: AtomicI64,
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
fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
#[derive(Debug)]
struct ContextData {
    prefix_store: RwLock<HashMapStore>,
    seed: u64,
    dp_size: RwLock<HashSet<i64>>,
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
#[derive(Debug, Clone, Serialize)]
pub struct CacheHitResult {
    #[serde(rename = "longest_matched")]
    pub longest_match_tokens: i64,
    #[serde(rename = "DP")]
    pub dp: HashMap<i64, i64>,
    #[serde(rename = "GPU")]
    pub gpu: i64,
    #[serde(rename = "CPU")]
    pub cpu: i64,
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
#[derive(Debug, Clone, Serialize)]
pub struct ModelContextView {
    pub model_name: String,
    pub lora_name: String,
    pub block_size: i64,
    pub additional_salt: String,
    pub tenant_id: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct GlobalView {
    pub context_count: i32,
    pub model_contexts: Vec<ModelContextView>,
    pub proxy_hashmap: Vec<HashMap<u64, u64>>,
}
pub struct PrefixCacheTable {
    context_map: DashMap<ModelContext, ContextData>,
    context_count: AtomicI32,
}

impl PrefixCacheTable {
    pub fn new() -> Self {
        Self {
            context_map: DashMap::new(),
            context_count: AtomicI32::new(0),
        }
    }
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
    fn get_context(
        &self,
        model_context: &ModelContext,
    ) -> Option<dashmap::mapref::one::Ref<'_, ModelContext, ContextData>> {
        self.context_map.get(model_context)
    }
    pub fn add_dp_size(&self, model_context: &ModelContext, _instance_id: &str, dp_rank: i64) {
        self.get_context_data(model_context);
        if let Some(ctx) = self.get_context(model_context) {
            ctx.dp_size.write().insert(dp_rank);
        }
    }
    pub fn compute_hash(parent_hash: u64, block_token_ids: &[i32]) -> u64 {
        let cap = 8 + block_token_ids.len() * 8;
        let mut buf = Vec::with_capacity(cap);
        buf.extend_from_slice(&parent_hash.to_le_bytes());
        for &token_id in block_token_ids {
            buf.extend_from_slice(&(token_id as u32).to_le_bytes());
            buf.extend_from_slice(&[0u8; 4]);
        }
        xxh64(&buf, 0)
    }
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
                break;
            };
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
        context_data
            .prefix_store
            .read()
            .last_access
            .store(now_unix(), Ordering::SeqCst);

        result
    }
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
        if event.parent_block_hash != 0 {
            debug!("parent Block HASH is not None.");
            if let Some(&pbh) = proxy_hash_map.get(&event.parent_block_hash) {
                parent_hash = pbh;
            }
        }

        for (i, &block_hash) in event.block_hashes.iter().enumerate() {
            if let Some(&existing_hash) = proxy_hash_map.get(&block_hash) {
                new_prefix_store.push(NewPrefix {
                    hash_value: existing_hash,
                    engine_id: event.instance_id.clone(),
                });
                continue;
            }
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

    pub fn clear_model_context(
        &self,
        model_name: &str,
        lora_name: &str,
        block_size: i64,
        additional_salt: &str,
        tenant_id: &str,
    ) {
        let model_context = ModelContext {
            model_name: model_name.to_string(),
            lora_name: lora_name.to_string(),
            block_size,
            additional_salt: additional_salt.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        if self.context_map.remove(&model_context).is_some() {
            self.context_count.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

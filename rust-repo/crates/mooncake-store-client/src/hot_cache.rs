//! # Local Hot Cache — 本地热缓存
//!
//! 基于单一环形缓冲区 + HashMap 索引的本地 LRU 缓存。
//! (A local LRU cache backed by a single ring-buffer with a HashMap index.)
//!
//! ## 设计 (Design)
//!
//! 使用一个固定大小的 `Vec<u8>` 作为环形存储 (`data`)，一个 `tail` 指针跟踪写入位置，
//! 一个 `HashMap<String, (offset, len)>` 记录每个 key 的数据位置。
//!
//! ### 驱逐策略 (Eviction Policy)
//!
//! 当空间不足或条目数超过 `max_entries` 时，驱逐 offset 最小的条目（最早写入的）。
//! 这不是严格的 FIFO（因为已有条目的 offset 不会变），但近似于 LRU 语义。
//!
//! ### 线程安全 (Thread Safety)
//!
//! 使用三个独立的 `parking_lot::Mutex`（`data`, `tail`, `entries`），减少锁竞争：
//! - `get` 先锁 entries（查找 offset）再锁 data（读取数据）
//! - `put` 按顺序锁 entries → tail → data，避免死锁
//!
//! ### 限制 (Limitations)
//!
//! - 不支持并发读写同一数据区域
//! - 单个 value 超过 `max_size` 时直接丢弃
//! - 绕回时清空整个缓存（简化实现）

use parking_lot::Mutex;
use regex::Regex;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, Ordering},
};

/// 默认热缓存大小: 256 MiB
/// (Default hot cache size: 256 MiB)
const DEFAULT_HOT_CACHE_SIZE: usize = 256 * 1024 * 1024;
/// 默认最大条目数: 10000
/// (Default maximum number of entries: 10,000)
const DEFAULT_MAX_ENTRIES: usize = 10000;
pub(crate) const DEFAULT_HOT_CACHE_BLOCK_SIZE: usize = 16 * 1024 * 1024;
const ADMISSION_SKETCH_WIDTH: usize = 4096;
const ADMISSION_SKETCH_DEPTH: usize = 4;
const DEFAULT_ADMISSION_THRESHOLD: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LocalHotCacheSettings {
    pub(crate) total_size: usize,
    pub(crate) block_size: usize,
    pub(crate) max_entries: usize,
    pub(crate) admission_threshold: u8,
}

impl LocalHotCacheSettings {
    pub(crate) fn from_environment() -> Result<Option<Self>, String> {
        Self::from_values(
            std::env::var("MC_STORE_LOCAL_HOT_CACHE_SIZE")
                .ok()
                .as_deref(),
            std::env::var("MC_STORE_LOCAL_HOT_BLOCK_SIZE")
                .ok()
                .as_deref(),
            std::env::var("MC_STORE_LOCAL_HOT_CACHE_USE_SHM")
                .ok()
                .as_deref(),
            std::env::var("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD")
                .ok()
                .as_deref(),
        )
    }

    pub(crate) fn from_values(
        total_size: Option<&str>,
        block_size: Option<&str>,
        use_shm: Option<&str>,
        admission_threshold: Option<&str>,
    ) -> Result<Option<Self>, String> {
        let Some(total_size) = parse_positive_usize(total_size) else {
            return Ok(None);
        };
        if use_shm == Some("1") {
            return Err(
                "MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1 is not supported by the Rust Store client"
                    .to_string(),
            );
        }

        let block_size = parse_positive_usize(block_size).unwrap_or(DEFAULT_HOT_CACHE_BLOCK_SIZE);
        let max_entries = total_size / block_size;
        if max_entries == 0 {
            return Ok(None);
        }
        let admission_threshold = admission_threshold
            .and_then(|value| value.parse::<u8>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_ADMISSION_THRESHOLD);

        Ok(Some(Self {
            total_size,
            block_size,
            max_entries,
            admission_threshold,
        }))
    }
}

fn parse_positive_usize(value: Option<&str>) -> Option<usize> {
    value?.parse::<usize>().ok().filter(|value| *value > 0)
}

/// Fixed-memory Count-Min Sketch used by the C++-compatible hot-cache
/// frequency admission policy.
pub(crate) struct HotCacheAdmission {
    counters: Mutex<Vec<u8>>,
    threshold: u8,
}

impl HotCacheAdmission {
    pub(crate) fn new(threshold: u8) -> Self {
        Self {
            counters: Mutex::new(vec![0; ADMISSION_SKETCH_WIDTH * ADMISSION_SKETCH_DEPTH]),
            threshold: threshold.max(1),
        }
    }

    pub(crate) fn should_admit(&self, key: &str) -> bool {
        let mut counters = self.counters.lock();
        let mut estimate = u8::MAX;
        for row in 0..ADMISSION_SKETCH_DEPTH {
            let column = admission_column(key, row);
            let counter = &mut counters[row * ADMISSION_SKETCH_WIDTH + column];
            *counter = counter.saturating_add(1);
            estimate = estimate.min(*counter);
        }
        estimate >= self.threshold
    }

    pub(crate) fn count(&self, key: &str) -> u8 {
        let counters = self.counters.lock();
        (0..ADMISSION_SKETCH_DEPTH)
            .map(|row| counters[row * ADMISSION_SKETCH_WIDTH + admission_column(key, row)])
            .min()
            .unwrap_or(0)
    }
}

fn admission_column(key: &str, row: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.hash(&mut hasher);
    key.hash(&mut hasher);
    hasher.finish() as usize % ADMISSION_SKETCH_WIDTH
}

struct HotCacheEntry {
    value: Vec<u8>,
    last_access: u64,
}

/// Captures the invalidation generation for one in-flight cache fill.
pub struct HotCachePutToken {
    key: String,
    key_generation: Arc<AtomicU64>,
    key_generation_value: u64,
    clear_generation: u64,
}

#[derive(Default)]
struct HotCacheState {
    entries: HashMap<String, HotCacheEntry>,
    key_generations: HashMap<String, Weak<AtomicU64>>,
    clear_generation: u64,
    used_size: usize,
    next_access: u64,
}

impl HotCacheState {
    fn take_access_sequence(&mut self) -> u64 {
        let sequence = self.next_access;
        self.next_access = self.next_access.saturating_add(1);
        sequence
    }

    fn prune_inactive_generations(&mut self) {
        self.key_generations
            .retain(|_, generation| generation.strong_count() > 0);
    }
}

/// A bounded, thread-safe local LRU cache that returns owned values.
pub struct LocalHotCache {
    state: Mutex<HotCacheState>,
    max_size: usize,
    max_value_size: usize,
    max_entries: usize,
}

impl LocalHotCache {
    pub fn new(max_size: usize, max_entries: usize) -> Self {
        Self::new_with_block_size(max_size, max_size.max(1), max_entries)
    }

    /// Create a cache whose individual values may not exceed `block_size`.
    pub fn new_with_block_size(max_size: usize, block_size: usize, max_entries: usize) -> Self {
        Self {
            state: Mutex::new(HotCacheState::default()),
            max_size,
            max_value_size: block_size.max(1),
            max_entries,
        }
    }

    /// Return an owned copy and atomically refresh the key's LRU position.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut state = self.state.lock();
        let sequence = state.take_access_sequence();
        let entry = state.entries.get_mut(key)?;
        entry.last_access = sequence;
        Some(entry.value.clone())
    }

    /// Insert a new value. Duplicate keys are LRU touches and retain their
    /// original bytes, matching the C++ LocalHotCache contract.
    pub fn put(&self, key: &str, value: &[u8]) {
        let mut state = self.state.lock();
        self.put_locked(&mut state, key, value);
    }

    /// Capture the current invalidation generation before starting a fill.
    pub fn acquire_put_token(&self, key: &str) -> HotCachePutToken {
        let mut state = self.state.lock();
        state.prune_inactive_generations();
        let key_generation = state
            .key_generations
            .get(key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let generation = Arc::new(AtomicU64::new(0));
                state
                    .key_generations
                    .insert(key.to_string(), Arc::downgrade(&generation));
                generation
            });
        HotCachePutToken {
            key: key.to_string(),
            key_generation_value: key_generation.load(Ordering::Relaxed),
            key_generation,
            clear_generation: state.clear_generation,
        }
    }

    /// Publish a fill only if no invalidation raced after token acquisition.
    pub fn put_with_token(&self, token: HotCachePutToken, value: &[u8]) -> bool {
        let mut state = self.state.lock();
        let is_current_generation = state
            .key_generations
            .get(&token.key)
            .and_then(Weak::upgrade)
            .is_some_and(|generation| {
                Arc::ptr_eq(&generation, &token.key_generation)
                    && generation.load(Ordering::Relaxed) == token.key_generation_value
            });
        if state.clear_generation != token.clear_generation || !is_current_generation {
            return false;
        }
        self.put_locked(&mut state, &token.key, value)
    }

    fn put_locked(&self, state: &mut HotCacheState, key: &str, value: &[u8]) -> bool {
        if value.is_empty()
            || value.len() > self.max_value_size
            || value.len() > self.max_size
            || self.max_entries == 0
        {
            return false;
        }

        let sequence = state.take_access_sequence();
        if let Some(entry) = state.entries.get_mut(key) {
            entry.last_access = sequence;
            return true;
        }

        while state.used_size + value.len() > self.max_size
            || state.entries.len() >= self.max_entries
        {
            let oldest_key = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| key.clone());
            let Some(oldest_key) = oldest_key else {
                return false;
            };
            if let Some(removed) = state.entries.remove(&oldest_key) {
                state.used_size -= removed.value.len();
            }
        }

        state.used_size += value.len();
        state.entries.insert(
            key.to_string(),
            HotCacheEntry {
                value: value.to_vec(),
                last_access: sequence,
            },
        );
        true
    }

    pub fn remove(&self, key: &str) {
        let mut state = self.state.lock();
        state.prune_inactive_generations();
        if let Some(generation) = state.key_generations.get(key).and_then(Weak::upgrade) {
            generation.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(entry) = state.entries.remove(key) {
            state.used_size -= entry.value.len();
        }
    }

    pub fn remove_by_regex_for_tenant(
        &self,
        tenant_id: &str,
        pattern: &str,
    ) -> Result<usize, regex::Error> {
        let re = Regex::new(pattern)?;
        let prefix = (!tenant_id.is_empty()).then(|| format!("{tenant_id}\0"));
        let mut state = self.state.lock();
        state.clear_generation = state.clear_generation.wrapping_add(1);
        state.key_generations.clear();
        let matching_keys = state
            .entries
            .keys()
            .filter(|key| {
                let user_key = match prefix.as_deref() {
                    Some(prefix) => match key.strip_prefix(prefix) {
                        Some(user_key) => user_key,
                        None => return false,
                    },
                    None => key.as_str(),
                };
                re.is_match(user_key)
            })
            .cloned()
            .collect::<Vec<_>>();
        for key in &matching_keys {
            if let Some(entry) = state.entries.remove(key) {
                state.used_size -= entry.value.len();
            }
        }
        Ok(matching_keys.len())
    }

    pub fn clear(&self) {
        let mut state = self.state.lock();
        state.clear_generation = state.clear_generation.wrapping_add(1);
        state.entries.clear();
        state.key_generations.clear();
        state.used_size = 0;
    }

    pub fn block_count(&self) -> usize {
        self.max_size / self.max_value_size
    }

    pub fn block_size(&self) -> usize {
        self.max_value_size
    }
}

impl Default for LocalHotCache {
    /// 使用默认参数创建缓存：256 MiB 容量，最多 10000 条目。
    /// (Create cache with defaults: 256 MiB capacity, max 10,000 entries.)
    fn default() -> Self {
        Self::new(DEFAULT_HOT_CACHE_SIZE, DEFAULT_MAX_ENTRIES)
    }
}

#[cfg(test)]
mod tests {
    use super::{HotCacheAdmission, LocalHotCache, LocalHotCacheSettings};

    #[test]
    fn environment_settings_default_to_cpp_block_and_admission_values() {
        let settings = LocalHotCacheSettings::from_values(Some("33554432"), None, None, None)
            .unwrap()
            .unwrap();

        assert_eq!(settings.block_size, 16 * 1024 * 1024);
        assert_eq!(settings.max_entries, 2);
        assert_eq!(settings.admission_threshold, 2);
    }

    #[test]
    fn invalid_or_missing_total_size_disables_cache() {
        assert_eq!(
            LocalHotCacheSettings::from_values(None, None, None, None).unwrap(),
            None
        );
        assert_eq!(
            LocalHotCacheSettings::from_values(Some("-1"), None, None, None).unwrap(),
            None
        );
        assert_eq!(
            LocalHotCacheSettings::from_values(Some("invalid"), None, None, None).unwrap(),
            None
        );
    }

    #[test]
    fn shared_memory_mode_fails_closed() {
        let error = LocalHotCacheSettings::from_values(Some("33554432"), None, Some("1"), None)
            .unwrap_err();
        assert!(error.contains("not supported"));
    }

    #[test]
    fn cache_rejects_values_larger_than_cpp_compatible_block() {
        let cache = LocalHotCache::new_with_block_size(8192, 4096, 2);
        cache.put("too-large", &[0; 4097]);
        cache.put("fits", &[0; 4096]);

        assert!(cache.get("too-large").is_none());
        assert_eq!(cache.get("fits").unwrap().len(), 4096);
    }

    #[test]
    fn invalidating_uncached_unique_keys_does_not_retain_generation_metadata() {
        let cache = LocalHotCache::new(1024, 4);
        for index in 0..1024 {
            cache.remove(&format!("absent-{index}"));
        }

        assert!(cache.state.lock().key_generations.is_empty());
    }

    #[test]
    fn admission_uses_saturating_frequency_threshold() {
        let admission = HotCacheAdmission::new(2);
        assert_eq!(admission.count("key"), 0);
        assert!(!admission.should_admit("key"));
        assert_eq!(admission.count("key"), 1);
        assert!(admission.should_admit("key"));
        assert_eq!(admission.count("key"), 2);
        assert!(admission.should_admit("key"));
    }

    #[test]
    fn cpp_parity_zero_hot_cache_size_disables_cache() {
        assert_eq!(
            LocalHotCacheSettings::from_values(Some("0"), None, None, None).unwrap(),
            None
        );
    }

    #[test]
    fn cpp_parity_sub_default_block_disables_cache_without_failing_client() {
        assert_eq!(
            LocalHotCacheSettings::from_values(Some("8388608"), None, None, None).unwrap(),
            None
        );
    }

    #[test]
    fn cpp_parity_sub_custom_block_disables_cache_without_failing_client() {
        assert_eq!(
            LocalHotCacheSettings::from_values(Some("2097152"), Some("4194304"), None, None,)
                .unwrap(),
            None
        );
    }
}

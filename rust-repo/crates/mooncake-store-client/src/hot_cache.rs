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
            return Err(format!(
                "local hot cache size {total_size} is smaller than block size {block_size}"
            ));
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

/// 本地热缓存，减少远程存储访问的延迟。
/// (Local hot cache to reduce latency of remote storage accesses.)
///
/// ## 字段 (Fields)
/// - `data`: 环形缓冲区，存储实际的缓存数据 (ring buffer storing cached data)
/// - `tail`: 环形缓冲区的下一个写入位置 (next write position in the ring buffer)
/// - `entries`: key -> (offset, len) 映射，定位缓存数据 (key-to-location index)
/// - `max_size`: 缓冲区最大字节数 (maximum buffer size in bytes)
/// - `max_entries`: 最大缓存条目数 (maximum number of cached entries)
pub struct LocalHotCache {
    /// 环形存储：数据按 offset 顺序写入，到达末尾时绕回 (ring buffer storage)
    data: Mutex<Vec<u8>>,
    /// 写入指针：下一个 value 将从 data[tail] 开始写入 (write cursor)
    tail: Mutex<usize>,
    /// key → (offset, len) 索引，用于 O(1) 查找 (key-to-location index)
    entries: Mutex<HashMap<String, (usize, usize)>>,
    max_size: usize,
    max_value_size: usize,
    max_entries: usize,
}

impl LocalHotCache {
    /// 创建一个新的热缓存。
    /// (Create a new hot cache with given max_size and max_entries.)
    ///
    /// `max_size` 至少为 4096 字节（单页大小）。
    pub fn new(max_size: usize, max_entries: usize) -> Self {
        let size = max_size.max(4096);
        Self::new_with_block_size(size, size, max_entries)
    }

    /// Create a cache whose individual values may not exceed `block_size`.
    ///
    /// The C++ cache allocates fixed-size physical blocks and therefore cannot
    /// admit an object larger than one block.
    pub fn new_with_block_size(max_size: usize, block_size: usize, max_entries: usize) -> Self {
        let size = max_size.max(4096);
        Self {
            data: Mutex::new(vec![0u8; size]),
            tail: Mutex::new(0),
            entries: Mutex::new(HashMap::new()),
            max_size: size,
            max_value_size: block_size.max(1).min(size),
            max_entries,
        }
    }

    /// 从缓存中获取 key 对应的数据。
    /// (Get cached data for a key. Returns `None` on cache miss.)
    ///
    /// 查找流程：先查 entries 索引 → 找到 offset/len → 从 data 中拷贝对应字节。
    /// 返回的是数据的拷贝（`Vec<u8>`），避免借用冲突。
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let entries = self.entries.lock();
        if let Some(&(offset, len)) = entries.get(key) {
            let data = self.data.lock();
            Some(data[offset..offset + len].to_vec())
        } else {
            None
        }
    }

    /// 将 key/value 写入缓存。
    /// (Put a key/value pair into the cache.)
    ///
    /// ## 行为 (Behavior)
    /// 1. 若 value 超过 `max_size`，直接丢弃
    /// 2. 循环驱逐：当空间不足或条目数超过 `max_entries` 时，驱逐 offset 最小的条目
    /// 3. 若写入位置 + value 超出缓冲区末尾，绕回到开头并清空所有条目
    /// 4. 将 value 拷贝到环形缓冲区，更新 entries 索引和 tail 指针
    pub fn put(&self, key: &str, value: &[u8]) {
        if value.len() > self.max_value_size {
            return;
        }

        let mut entries = self.entries.lock();
        let mut tail = self.tail.lock();
        let mut data = self.data.lock();

        // evict if needed
        // 驱逐：空间不足 或 条目数超限
        while *tail + value.len() > self.max_size || entries.len() >= self.max_entries {
            if entries.is_empty() {
                *tail = 0;
                break;
            }
            // 驱逐 offset 最小的条目（最早写入的）
            // Evict the entry with the smallest offset (oldest write)
            let oldest_key = entries
                .iter()
                .min_by_key(|&(_, &(off, _))| off)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest_key {
                entries.remove(&k);
            }
        }

        let offset = *tail;
        // 缓冲区末尾空间不足 → 绕回 (wrap around)
        if offset + value.len() > data.len() {
            *tail = 0;
            entries.clear();
        }

        let offset = *tail;
        data[offset..offset + value.len()].copy_from_slice(value);
        entries.insert(key.to_string(), (offset, value.len()));
        *tail = offset + value.len();
    }

    /// 从缓存中移除指定 key。
    /// (Remove a key from the cache. No-op if the key is not present.)
    pub fn remove(&self, key: &str) {
        self.entries.lock().remove(key);
    }

    /// Remove cached entries whose user key matches `pattern`.
    ///
    /// When `tenant_id` is non-empty, cache keys use the same scoped format as
    /// the client (`tenant_id + '\0' + key`) and the regex is applied only to
    /// the user-key suffix.
    pub fn remove_by_regex_for_tenant(
        &self,
        tenant_id: &str,
        pattern: &str,
    ) -> Result<usize, regex::Error> {
        let re = Regex::new(pattern)?;
        let prefix = (!tenant_id.is_empty()).then(|| format!("{tenant_id}\0"));
        let mut removed = 0usize;
        self.entries.lock().retain(|key, _| {
            let user_key = match prefix.as_deref() {
                Some(prefix) => match key.strip_prefix(prefix) {
                    Some(user_key) => user_key,
                    None => return true,
                },
                None => key.as_str(),
            };
            let keep = !re.is_match(user_key);
            if !keep {
                removed += 1;
            }
            keep
        });
        Ok(removed)
    }

    /// 清空所有缓存条目，重置 tail 指针。
    /// (Clear all cached entries and reset the tail pointer.)
    pub fn clear(&self) {
        self.entries.lock().clear();
        *self.tail.lock() = 0;
    }

    pub fn block_count(&self) -> usize {
        self.max_size / self.max_value_size
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
    fn admission_uses_saturating_frequency_threshold() {
        let admission = HotCacheAdmission::new(2);
        assert_eq!(admission.count("key"), 0);
        assert!(!admission.should_admit("key"));
        assert_eq!(admission.count("key"), 1);
        assert!(admission.should_admit("key"));
        assert_eq!(admission.count("key"), 2);
        assert!(admission.should_admit("key"));
    }
}

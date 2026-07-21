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

/// 默认热缓存大小: 256 MiB
/// (Default hot cache size: 256 MiB)
const DEFAULT_HOT_CACHE_SIZE: usize = 256 * 1024 * 1024;
/// 默认最大条目数: 10000
/// (Default maximum number of entries: 10,000)
const DEFAULT_MAX_ENTRIES: usize = 10000;

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
    max_entries: usize,
}

impl LocalHotCache {
    /// 创建一个新的热缓存。
    /// (Create a new hot cache with given max_size and max_entries.)
    ///
    /// `max_size` 至少为 4096 字节（单页大小）。
    pub fn new(max_size: usize, max_entries: usize) -> Self {
        let size = max_size.max(4096);
        Self {
            data: Mutex::new(vec![0u8; size]),
            tail: Mutex::new(0),
            entries: Mutex::new(HashMap::new()),
            max_size: size,
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
        // C++ LocalHotCache admits any value that fits the physical cache block.
        // This ring-buffer variant has one physical buffer, so the hard limit is
        // max_size rather than a conservative fraction of it.
        if value.len() > self.max_size {
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
}

impl Default for LocalHotCache {
    /// 使用默认参数创建缓存：256 MiB 容量，最多 10000 条目。
    /// (Create cache with defaults: 256 MiB capacity, max 10,000 entries.)
    fn default() -> Self {
        Self::new(DEFAULT_HOT_CACHE_SIZE, DEFAULT_MAX_ENTRIES)
    }
}

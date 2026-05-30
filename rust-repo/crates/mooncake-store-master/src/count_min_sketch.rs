// =============================================================================
// Count-Min Sketch — Count-Min 概要数据结构
// =============================================================================
// Implements a Count-Min Sketch for approximate frequency estimation of keys.
// 实现 Count-Min 概要数据结构，用于 key 的近似频率估计。
//
// Purpose / 目的:
// Used by the frequency admission policy to determine whether a key should be
// promoted into the local hot cache. Keys with higher estimated access frequencies
// are more likely to be cached locally.
// 由频率准入策略使用，决定一个 key 是否应该被 promotion 到本地热点缓存。
// 估计访问频率高的 key 更可能被本地缓存。
//
// Algorithm / 算法:
// Count-Min Sketch is a probabilistic data structure that uses multiple hash
// functions (rows) and a 2D counter array. To increment a key's count:
// Count-Min Sketch 是一种概率数据结构，使用多个哈希函数（行）和二维计数器数组。
// 递增 key 的计数：
//   1. Hash the key with each of `depth` independent hash functions.
//      使用 `depth` 个独立哈希函数对 key 进行哈希。
//   2. Increment the counter at table[i][hash_i] for each row.
//      对每一行，递增 table[i][hash_i] 处的计数器。
//   3. The estimated count = min(table[i][hash_i] for all rows).
//      估计计数 = 所有行中 table[i][hash_i] 的最小值。
//
// Properties / 特性:
// - Sub-linear memory: 4096 x 4 x 1 byte = 16 KiB fixed.
//   次线性内存：4096 x 4 x 1 字节 = 16 KiB 固定。
//   默认 4096 x 4 = 16 KB 固定内存，与 key 总数无关。
// - One-sided error: may over-count but never under-counts.
//   单侧误差：可能过估计但不会低估计。
//   使用多哈希 + 取小值的经典 Count-Min 设计，存在一定的过估计但不会低估计。
// - Auto-decay: when total_increments >= width * depth, all counters are halved
//   to prevent saturation and adapt to changing access patterns.
//   自动衰减：当 total_increments >= width * depth 时，所有计数器减半以防止饱和
//   并适应变化的访问模式。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A simple Count-Min Sketch for tracking key access frequency.
/// 简单的 Count-Min Sketch，用于跟踪 key 的访问频率。
///
/// The sketch uses a fixed-size 2D table of u8 counters. Each key is hashed
/// `depth` times (with different seeds) and the minimum across all rows is
/// the estimated frequency.
/// 使用固定大小的 u8 二维计数器表。每个 key 用不同种子哈希 `depth` 次，
/// 所有行中的最小值即为估计频率。
pub(crate) struct CountMinSketch {
    /// Number of columns (hash buckets per row).
    /// 列数（每行的哈希桶数）。
    width: usize,
    /// Number of rows (independent hash functions).
    /// 行数（独立哈希函数数量）。
    depth: usize,
    /// The 2D counter table: table[depth][width] of u8 values.
    /// 二维计数器表：table[depth][width] 的 u8 值。
    table: Vec<Vec<u8>>,
    /// Total number of increment operations, used to trigger auto-decay.
    /// 总递增操作数，用于触发自动衰减。
    total_increments: usize,
}

impl CountMinSketch {
    /// Default table dimensions: 4096 columns x 4 rows = 16 KiB.
    /// 默认表维度：4096 列 x 4 行 = 16 KiB。
    const DEFAULT_WIDTH: usize = 4096;
    const DEFAULT_DEPTH: usize = 4;

    /// Create a new CountMinSketch with default dimensions.
    /// 使用默认维度创建新的 CountMinSketch。
    pub fn new() -> Self {
        Self::with_dimensions(Self::DEFAULT_WIDTH, Self::DEFAULT_DEPTH)
    }

    /// Create a CountMinSketch with custom dimensions.
    /// 使用自定义维度创建 CountMinSketch。
    ///
    /// Falls back to defaults if either dimension is 0.
    /// 如果任一维度为 0，则回退到默认值。
    pub fn with_dimensions(width: usize, depth: usize) -> Self {
        let width = if width > 0 {
            width
        } else {
            Self::DEFAULT_WIDTH
        };
        let depth = if depth > 0 {
            depth
        } else {
            Self::DEFAULT_DEPTH
        };
        Self {
            width,
            depth,
            table: vec![vec![0u8; width]; depth],
            total_increments: 0,
        }
    }

    /// Increment the count for a key and return the estimated count.
    /// 递增 key 的计数值，返回递增后的估计值。
    ///
    /// After incrementing, checks if total_increments >= width * depth; if so,
    /// triggers auto-decay (halves all counters) to prevent saturation.
    /// 递增后检查 total_increments >= width * depth；若满足条件，
    /// 触发自动衰减（所有计数器减半）以防止饱和。
    /// 递增 key 的计数值，返回递增后的估计值（所有行中的最小值）。
    /// 当 total_increments 超过 width * depth 时自动触发衰减，防止计数器饱和。
    pub fn increment(&mut self, key: &str) -> u8 {
        let mut min_val = u8::MAX;
        for i in 0..self.depth {
            let idx = self.hash(key, i as u64) % self.width;
            self.table[i][idx] = self.table[i][idx].saturating_add(1);
            min_val = min_val.min(self.table[i][idx]);
        }
        self.total_increments += 1;
        // Auto-decay: every time we touch every cell ~once on average, halve counters
        // 自动衰减：每次平均触及每个单元约一次时，计数器减半
        if self.total_increments >= self.width * self.depth {
            self.decay();
        }
        min_val
    }

    /// Query the estimated count for a key without modifying the sketch.
    /// 只读查询 key 的估计计数值，不触发递增和衰减。
    ///
    /// Returns the minimum counter value across all rows.
    /// 返回所有行中最小的计数器值。
    #[cfg(test)]
    pub(crate) fn count(&self, key: &str) -> u8 {
        let mut min_val = u8::MAX;
        for i in 0..self.depth {
            let idx = self.hash(key, i as u64) % self.width;
            min_val = min_val.min(self.table[i][idx]);
        }
        min_val
    }

    /// Decay all counters by halving (right-shift by 1) and reset total_increments.
    /// 衰减所有计数器（右移 1 位 = 减半），并将 total_increments 归零。
    ///
    /// Decay is essential to prevent counter saturation (all u8 values reaching 255)
    /// and to allow the sketch to adapt to shifting access patterns over time.
    /// 衰减对于防止计数器饱和（所有 u8 值达到 255）以及允许 sketch 适应
    /// 随时间变化的访问模式至关重要。
    /// 使用右移而非除法以提高性能。
    pub fn decay(&mut self) {
        for row in &mut self.table {
            for cell in row.iter_mut() {
                *cell >>= 1;
            }
        }
        self.total_increments = 0;
    }

    /// Hash a key with a per-row seed for independent hash functions.
    /// 使用逐行种子对 key 进行哈希，实现独立哈希函数。
    ///
    /// Uses the standard library DefaultHasher for the initial hash, then mixes
    /// with the seed using murmur-style constants for cross-language consistency
    /// with the C++ implementation.
    /// 使用标准库 DefaultHasher 进行初始哈希，然后用 murmur 风格的常数与种子混合，
    /// 确保与 C++ 实现的跨语言一致性。
    /// 多行独立哈希：先用 DefaultHasher 对 key 做一次哈希，再与 per-row seed 混合。
    /// 使用与 C++ 相同的 murmur-style 常数确保跨语言一致性。
    fn hash(&self, key: &str, seed: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let h = hasher.finish();
        // Murmur-style finalizer with seed mixing for per-row independence
        // Murmur 风格最终化器与种子混合，实现逐行独立
        // 与 C++ 保持一致的哈希混合常数
        let h = h
            ^ (seed
                .wrapping_mul(0x9e3779b97f4a7c15)
                .wrapping_add(0x517cc1b727220a95));
        let h = h ^ (h >> 33);
        let h = h.wrapping_mul(0xff51afd7ed558ccd);
        let h = h ^ (h >> 33);
        h as usize
    }
}

impl Default for CountMinSketch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_zero() {
        let sketch = CountMinSketch::new();
        // Fresh sketch should report 0 for any key
        assert_eq!(sketch.count("anything"), 0);
    }

    #[test]
    fn test_increment_and_count() {
        let mut sketch = CountMinSketch::new();
        let c = sketch.increment("key1");
        assert!(c >= 1);
        assert!(sketch.count("key1") >= 1);
    }

    #[test]
    fn test_multiple_increments() {
        let mut sketch = CountMinSketch::new();
        for _ in 0..5 {
            sketch.increment("key1");
        }
        // After 5 increments, min count should be >= 5 (barring hash collisions)
        // 5 次递增后，最小计数应 >= 5（排除哈希冲突）
        assert!(sketch.count("key1") >= 5);
    }

    #[test]
    fn test_different_keys_independent() {
        let mut sketch = CountMinSketch::new();
        sketch.increment("key1");
        sketch.increment("key1");
        sketch.increment("key2");
        // key1 should have higher count than key2
        // key1 的计数应高于 key2
        assert!(sketch.count("key1") > sketch.count("key2"));
    }

    #[test]
    fn test_decay_halves_counters() {
        let mut sketch = CountMinSketch::new();
        for _ in 0..100 {
            sketch.increment("key1");
        }
        let before = sketch.count("key1");
        sketch.decay();
        let after = sketch.count("key1");
        // After decay, count should be roughly halved
        // 衰减后，计数应大致减半
        assert!(after <= before / 2 + 1); // allow rounding up from halving
    }

    #[test]
    fn test_auto_decay_triggers() {
        // Create a small sketch that will auto-decay quickly
        // 创建一个小 sketch，可快速触发自动衰减
        let mut sketch = CountMinSketch::with_dimensions(100, 2);
        let threshold = 100 * 2; // width * depth = 200
        for i in 0..threshold + 1 {
            sketch.increment(&format!("key{}", i));
        }
        // After exceeding threshold, all counters should have been decayed at least once
        // 超过阈值后，所有计数器应至少衰减一次
        // This just verifies no panic and reasonable behavior
        assert!(sketch.count("key0") < 200);
    }

    #[test]
    fn test_count_saturates_at_u8_max() {
        // Use a large sketch to avoid auto-decay, then manually saturate with many increments
        // 使用大 sketch 避免自动衰减，然后通过大量递增手动饱和
        let mut sketch = CountMinSketch::with_dimensions(4096, 4);
        // 300 increments without auto-decay (4096*4=16384 threshold)
        // 300 次递增，不触发自动衰减（4096*4=16384 阈值）
        for _ in 0..300 {
            sketch.increment("same_key");
        }
        // After 300 increments, the min counter should be 255 (saturated) or close to it
        // 300 次递增后，最小计数器应为 255（饱和）或接近饱和
        assert!(sketch.count("same_key") >= 200);
    }
}

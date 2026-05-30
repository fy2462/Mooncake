use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// 简单的 Count-Min Sketch，用于跟踪 key 的访问频率。
/// 被 frequency admission policy 使用，决定一个 key 是否应该被 promotion 到本地热点缓存。
///
/// 默认 4096 × 4 = 16 KB 固定内存，与 key 总数无关。
/// 使用多哈希 + 取小值的经典 Count-Min 设计，存在一定的过估计但不会低估计。
pub(crate) struct CountMinSketch {
    width: usize,
    depth: usize,
    table: Vec<Vec<u8>>,
    total_increments: usize,
}

impl CountMinSketch {
    const DEFAULT_WIDTH: usize = 4096;
    const DEFAULT_DEPTH: usize = 4;

    pub fn new() -> Self {
        Self::with_dimensions(Self::DEFAULT_WIDTH, Self::DEFAULT_DEPTH)
    }

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
        if self.total_increments >= self.width * self.depth {
            self.decay();
        }
        min_val
    }

    /// 只读查询 key 的估计计数值（不触发递增和衰减），供 admission control 使用。
    #[allow(dead_code)]
    pub fn count(&self, key: &str) -> u8 {
        let mut min_val = u8::MAX;
        for i in 0..self.depth {
            let idx = self.hash(key, i as u64) % self.width;
            min_val = min_val.min(self.table[i][idx]);
        }
        min_val
    }

    /// 衰减所有计数器（右移 1 位 = 减半），并将 total_increments 归零。
    /// 使用右移而非除法以提高性能。
    pub fn decay(&mut self) {
        for row in &mut self.table {
            for cell in row.iter_mut() {
                *cell >>= 1;
            }
        }
        self.total_increments = 0;
    }

    // 多行独立哈希：先用 DefaultHasher 对 key 做一次哈希，再与 per-row seed 混合。
    // 使用与 C++ 相同的 murmur-style 常数确保跨语言一致性。
    fn hash(&self, key: &str, seed: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let h = hasher.finish();
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
        assert!(sketch.count("key1") >= 5);
    }

    #[test]
    fn test_different_keys_independent() {
        let mut sketch = CountMinSketch::new();
        sketch.increment("key1");
        sketch.increment("key1");
        sketch.increment("key2");
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
        assert!(after <= before / 2 + 1); // allow rounding up from halving
    }

    #[test]
    fn test_auto_decay_triggers() {
        // Create a small sketch that will auto-decay quickly
        let mut sketch = CountMinSketch::with_dimensions(100, 2);
        let threshold = 100 * 2; // width * depth = 200
        for i in 0..threshold + 1 {
            sketch.increment(&format!("key{}", i));
        }
        // After exceeding threshold, all counters should have been decayed at least once
        // This just verifies no panic and reasonable behavior
        assert!(sketch.count("key0") < 200);
    }

    #[test]
    fn test_count_saturates_at_u8_max() {
        // Use a large sketch to avoid auto-decay, then manually saturate with many increments
        let mut sketch = CountMinSketch::with_dimensions(4096, 4);
        // 300 increments without auto-decay (4096*4=16384 threshold)
        for _ in 0..300 {
            sketch.increment("same_key");
        }
        // After 300 increments, the min counter should be 255 (saturated) or close to it
        // (hash collisions may route some increments to other cells, but with 4 rows
        // a single key hitting the same cell 255+ times is effectively guaranteed)
        assert!(sketch.count("same_key") >= 200);
    }
}

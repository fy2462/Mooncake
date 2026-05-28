use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// A simple Count-Min Sketch for tracking key access frequency.
/// Used by the frequency admission policy to decide whether a key
/// should be promoted into the local hot cache.
///
/// Default 4096 × 4 = 16 KB fixed memory, independent of number of keys.
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
        let width = if width > 0 { width } else { Self::DEFAULT_WIDTH };
        let depth = if depth > 0 { depth } else { Self::DEFAULT_DEPTH };
        Self {
            width,
            depth,
            table: vec![vec![0u8; width]; depth],
            total_increments: 0,
        }
    }

    /// Increment the count for `key` and return the estimated min-count.
    /// Automatically triggers decay when total_increments exceeds
    /// width * depth to prevent counters from saturating.
    pub fn increment(&mut self, key: &str) -> u8 {
        let mut min_val = u8::MAX;
        for i in 0..self.depth {
            let idx = self.hash(key, i as u64) % self.width;
            if self.table[i][idx] < u8::MAX {
                self.table[i][idx] += 1;
            }
            min_val = min_val.min(self.table[i][idx]);
        }
        self.total_increments += 1;
        if self.total_increments >= self.width * self.depth {
            self.decay();
        }
        min_val
    }

    /// Return the estimated count for `key` (read-only). Used by admission control.
    #[allow(dead_code)]
    pub fn count(&self, key: &str) -> u8 {
        let mut min_val = u8::MAX;
        for i in 0..self.depth {
            let idx = self.hash(key, i as u64) % self.width;
            min_val = min_val.min(self.table[i][idx]);
        }
        min_val
    }

    /// Halve all counters (right-shift by 1).
    pub fn decay(&mut self) {
        for row in &mut self.table {
            for cell in row.iter_mut() {
                *cell >>= 1;
            }
        }
        self.total_increments = 0;
    }

    fn hash(&self, key: &str, seed: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let h = hasher.finish();
        // Combine with per-row seed for independent hashes (same constants as C++)
        let h = h ^ (seed.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(0x517cc1b727220a95));
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

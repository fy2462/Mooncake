use std::time::{Duration, SystemTime};

pub struct EvictionManager {
    soft_pin_ttl: Duration,
    lease_ttl: Duration,
}

impl EvictionManager {
    pub fn new(soft_pin_ttl: Duration, lease_ttl: Duration) -> Self {
        Self {
            soft_pin_ttl,
            lease_ttl,
        }
    }

    /// Public API kept for backward compatibility with existing tests.
    /// Internally converts to owned data and delegates to `select_for_eviction_with_hard_pin`.
    pub fn select_for_eviction(
        &self,
        candidates: &[(&str, &[mooncake_store_core::ReplicaDescriptor], bool, SystemTime)],
        target_count: usize,
    ) -> Vec<String> {
        let mut owned: Vec<(String, bool, bool, SystemTime)> = candidates
            .iter()
            .map(|(key, _replicas, soft_pinned, last_access)| {
                (key.to_string(), *soft_pinned, false, *last_access)
            })
            .collect();
        self.select_for_eviction_with_hard_pin(&mut owned, target_count)
    }

    /// Select up to `target_count` keys for eviction.
    ///
    /// Uses O(N) partial sort (`select_nth_unstable_by_key`) to find the top-K
    /// oldest entries, avoiding a full O(N log N) sort across all candidates.
    /// Only the selected K entries are sorted for deterministic LRU order (O(K log K)).
    pub fn select_for_eviction_with_hard_pin(
        &self,
        candidates: &mut [(String, bool, bool, SystemTime)],
        target_count: usize,
    ) -> Vec<String> {
        if target_count == 0 {
            return vec![];
        }

        let now = SystemTime::now();
        let mut valid: Vec<usize> = candidates
            .iter()
            .enumerate()
            .filter(|(_, (_, soft_pinned, hard_pinned, last_access))| {
                !*soft_pinned && !*hard_pinned && self.is_lease_expired(*last_access, now)
            })
            .map(|(i, _)| i)
            .collect();

        if valid.is_empty() {
            return vec![];
        }

        if valid.len() <= target_count {
            valid.sort_by_key(|&i| &candidates[i].3);
            return valid
                .into_iter()
                .map(|i| std::mem::take(&mut candidates[i].0))
                .collect();
        }

        // O(N): partition so that indices [0..target_count) contain the
        // target_count oldest entries (by last_access).
        let nth = target_count - 1;
        valid.select_nth_unstable_by_key(nth, |&i| &candidates[i].3);

        // O(K log K): sort only the selected K for stable LRU order.
        valid[..target_count].sort_by_key(|&i| &candidates[i].3);

        valid
            .into_iter()
            .take(target_count)
            .map(|i| std::mem::take(&mut candidates[i].0))
            .collect()
    }

    pub fn soft_pin_expired(&self, created_at: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(created_at)
            .map(|d| d > self.soft_pin_ttl)
            .unwrap_or(true)
    }

    pub fn is_lease_expired(&self, last_access: SystemTime, now: SystemTime) -> bool {
        now.duration_since(last_access)
            .map(|d| d > self.lease_ttl)
            .unwrap_or(true)
    }
}

use std::time::{Duration, SystemTime};

/// 驱逐管理器：基于 LRU（最近最少使用）策略选择驱逐候选对象。
/// 提供两层保护：soft_pin（软固定，TTL 内不会被驱逐）和 lease（租约，过期后允许驱逐）。
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

    /// 向后兼容的公开 API，内部转换为自有数据后委托给 select_for_eviction_with_hard_pin。
    /// 仅用于已有测试；生产路径应使用下面的 select_for_eviction_with_hard_pin。
    pub fn select_for_eviction(
        &self,
        candidates: &[(
            &str,
            &[mooncake_store_core::ReplicaDescriptor],
            bool,
            SystemTime,
        )],
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

        // 过滤：跳过软/硬固定的对象，以及租约未过期的对象
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

    /// 检查软固定是否已过期：创建时间超过 soft_pin_ttl 后允许驱逐。
    pub fn soft_pin_expired(&self, created_at: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(created_at)
            .map(|d| d > self.soft_pin_ttl)
            .unwrap_or(true)
    }

    /// 检查租约是否过期：最近访问时间距今超过 lease_ttl 后允许驱逐。
    /// 租约机制保证最近被访问的对象不会被误驱逐。
    pub fn is_lease_expired(&self, last_access: SystemTime, now: SystemTime) -> bool {
        now.duration_since(last_access)
            .map(|d| d > self.lease_ttl)
            .unwrap_or(true)
    }
}

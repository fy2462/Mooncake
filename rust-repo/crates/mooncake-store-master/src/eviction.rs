// =============================================================================
// Eviction Manager — 驱逐管理器
// =============================================================================
// Implements LRU (Least Recently Used) based eviction policy for the cache.
// 实现基于 LRU（最近最少使用）的缓存驱逐策略。
//
// Architecture / 架构:
// ┌─────────────────────────────────────────────────────┐
// │  EvictionManager                                     │
// │  ┌──────────────┐  ┌──────────────────────────────┐  │
// │  │ soft_pin_ttl  │  │ Eviction Decision Logic     │  │
// │  │ (软固定 TTL)   │  │ soft_pin > lease > LRU order│  │
// │  ├──────────────┤  ├──────────────────────────────┤  │
// │  │ lease_ttl    │  │ select_for_eviction()        │  │
// │  │ (租约 TTL)    │  │ select_nth_unstable O(N)    │  │
// │  └──────────────┘  └──────────────────────────────┘  │
// └─────────────────────────────────────────────────────┘
//
// Eviction Decision Flow / 驱逐决策流程:
// 1. Filter out soft-pinned keys (TTL not expired).
//    过滤掉软固定的 key（TTL 未过期）。
// 2. Filter out hard-pinned keys (always protected).
//    过滤掉硬固定的 key（始终保护）。
// 3. Filter out keys with active lease (recently accessed).
//    过滤掉租约未过期的 key（最近被访问过）。
// 4. Among remaining candidates, select oldest (LRU) keys.
//    在剩余候选中选择最旧的（LRU）key。
//
// Two-level protection / 两层保护:
// - soft_pin（软固定）：TTL 内不会被驱逐，过期后允许被选中。
// - lease（租约）：最近访问时间在 lease_ttl 内的对象不会被驱逐。

use std::time::{Duration, SystemTime};

pub type LeaseCandidate = (
    String,
    Option<SystemTime>,
    bool,
    Option<SystemTime>,
    SystemTime,
);

fn take_sorted_lease_candidates(
    candidates: &mut [LeaseCandidate],
    indexes: &mut Vec<usize>,
    count: usize,
) -> Vec<String> {
    if count == 0 || indexes.is_empty() {
        return vec![];
    }
    let sort_key =
        |candidate: &LeaseCandidate| (candidate.3.unwrap_or(SystemTime::UNIX_EPOCH), candidate.4);
    if indexes.len() <= count {
        indexes.sort_by_key(|&i| sort_key(&candidates[i]));
        return indexes
            .drain(..)
            .map(|i| std::mem::take(&mut candidates[i].0))
            .collect();
    }

    let nth = count - 1;
    indexes.select_nth_unstable_by_key(nth, |&i| sort_key(&candidates[i]));
    indexes[..count].sort_by_key(|&i| sort_key(&candidates[i]));
    indexes
        .drain(..count)
        .map(|i| std::mem::take(&mut candidates[i].0))
        .collect()
}

/// The eviction manager selects eviction candidates based on LRU strategy.
/// 驱逐管理器：基于 LRU（最近最少使用）策略选择驱逐候选对象。
///
/// Provides two levels of protection against premature eviction:
/// 提供两层保护防止过早驱逐：
/// - `soft_pin_ttl`: Objects created within this TTL are immune to eviction.
///   soft_pin_ttl：在此 TTL 内创建的对象不会被驱逐（软固定）。
/// - `lease_ttl`: Objects accessed within this TTL are immune to eviction.
///   lease_ttl：在此 TTL 内被访问的对象不会被驱逐（租约）。
///
/// 使用 O(N) 部分排序（select_nth_unstable_by_key）而非完整排序，提高性能。
pub struct EvictionManager {
    /// Duration after creation before a soft-pinned object becomes evictable.
    /// 软固定对象的创建时间超过此值后允许驱逐。
    soft_pin_ttl: Duration,
    /// Duration after last access before a leased object becomes evictable.
    /// 租约对象的最近访问时间距今超过此值后允许驱逐。
    lease_ttl: Duration,
}

impl EvictionManager {
    /// Create a new EvictionManager with the given TTLs.
    /// 使用指定的 TTL 创建新的 EvictionManager。
    pub fn new(soft_pin_ttl: Duration, lease_ttl: Duration) -> Self {
        Self {
            soft_pin_ttl,
            lease_ttl,
        }
    }

    /// Backward-compatible public API: converts borrowed slices to owned tuples
    /// and delegates to select_for_eviction_with_hard_pin.
    /// 向后兼容的公开 API：将借用的 slice 转换为自有数据后委托给 select_for_eviction_with_hard_pin。
    ///
    /// Used by existing tests; production paths should use select_for_eviction_with_hard_pin.
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
        let now = SystemTime::now();
        let mut owned: Vec<(String, Option<SystemTime>, bool, SystemTime)> = candidates
            .iter()
            .map(|(key, _replicas, soft_pinned, last_access)| {
                let timeout = soft_pinned.then(|| now + self.soft_pin_ttl);
                (key.to_string(), timeout, false, *last_access)
            })
            .collect();
        self.select_for_eviction_with_hard_pin(&mut owned, target_count)
    }

    /// Select up to `target_count` keys for eviction.
    /// 选择最多 `target_count` 个 key 进行驱逐。
    ///
    /// Algorithm / 算法:
    ///
    /// 1. Filter out protected entries:
    ///    过滤掉受保护的条目：
    ///    - soft_pinned = true → skip (still within soft_pin_ttl).
    ///      soft_pinned = true → 跳过（仍在软固定 TTL 内）。
    ///    - hard_pinned = true → skip (permanently protected).
    ///      hard_pinned = true → 跳过（永久保护）。
    ///    - lease not expired → skip (recently accessed).
    ///      lease 未过期 → 跳过（最近被访问过）。
    ///
    /// 2. If valid candidates <= target_count, sort all and return all.
    ///    如果有效候选数 <= target_count，排序全部并返回全部。
    ///
    /// 3. Otherwise, use O(N) partial sort:
    ///    否则使用 O(N) 部分排序：
    ///    - `select_nth_unstable_by_key` to partition: indices [0..target_count) contain
    ///      the target_count oldest entries by last_access.
    ///      `select_nth_unstable_by_key` 分区：索引 [0..target_count) 包含
    ///      target_count 个最旧的条目（按 last_access）。
    ///
    /// 4. Sort only the selected K entries for deterministic LRU order (O(K log K)).
    ///    仅排序选中的 K 个条目以获得确定性的 LRU 顺序（O(K log K)）。
    ///
    /// This avoids a full O(N log N) sort across all candidates — a significant
    /// optimization when the number of candidates is large but target_count is small.
    /// 这避免了所有候选的完整 O(N log N) 排序 —— 当候选数大而 target_count 小时，
    /// 这是显著的性能优化。
    pub fn select_for_eviction_with_hard_pin(
        &self,
        candidates: &mut [(String, Option<SystemTime>, bool, SystemTime)],
        target_count: usize,
    ) -> Vec<String> {
        if target_count == 0 {
            return vec![];
        }

        // Step 1: filter — skip soft/hard pinned and lease-protected entries
        // 过滤：跳过软/硬固定的对象，以及租约未过期的对象
        let now = SystemTime::now();
        let mut valid: Vec<usize> = candidates
            .iter()
            .enumerate()
            .filter(|(_, (_, soft_pin_timeout, hard_pinned, last_access))| {
                let is_soft_pinned = soft_pin_timeout.map_or(false, |t| now < t);
                !is_soft_pinned && !*hard_pinned && self.is_lease_expired(*last_access, now)
            })
            .map(|(i, _)| i)
            .collect();

        if valid.is_empty() {
            return vec![];
        }

        // Step 2: all valid entries fit within target_count
        if valid.len() <= target_count {
            valid.sort_by_key(|&i| &candidates[i].3);
            return valid
                .into_iter()
                .map(|i| std::mem::take(&mut candidates[i].0))
                .collect();
        }

        // Step 3: O(N) partition — indices [0..target_count) = oldest K
        // O(N): partition so that indices [0..target_count) contain the
        // target_count oldest entries (by last_access).
        // O(N): 分区使索引 [0..target_count) 包含 target_count 个最旧的条目。
        let nth = target_count - 1;
        valid.select_nth_unstable_by_key(nth, |&i| &candidates[i].3);

        // Step 4: O(K log K) — sort only the selected K for stable LRU order
        // O(K log K): sort only the selected K for stable LRU order.
        // O(K log K): 仅排序选中的 K 个以保证 LRU 顺序确定性。
        valid[..target_count].sort_by_key(|&i| &candidates[i].3);

        valid
            .into_iter()
            .take(target_count)
            .map(|i| std::mem::take(&mut candidates[i].0))
            .collect()
    }

    /// Select eviction candidates using explicit lease timeout timestamps.
    /// Production master metadata stores C++-style absolute `lease_timeout`
    /// values; `None` is treated like the default C++ epoch and is evictable.
    pub fn select_for_eviction_with_lease_timeout(
        &self,
        candidates: &mut [LeaseCandidate],
        target_count: usize,
    ) -> Vec<String> {
        self.select_for_eviction_with_lease_timeout_policy(candidates, target_count, false)
    }

    /// Select eviction candidates with C++-style two-pass soft-pin handling.
    /// First pass always prefers non-soft-pinned, lease-expired objects. When
    /// `allow_soft_pinned` is true and the first pass cannot fill `target_count`,
    /// a second pass may choose soft-pinned objects ordered by lease timeout.
    pub fn select_for_eviction_with_lease_timeout_policy(
        &self,
        candidates: &mut [LeaseCandidate],
        target_count: usize,
        allow_soft_pinned: bool,
    ) -> Vec<String> {
        if target_count == 0 {
            return vec![];
        }

        let now = SystemTime::now();
        let mut normal: Vec<usize> = candidates
            .iter()
            .enumerate()
            .filter(
                |(_, (_, soft_pin_timeout, hard_pinned, lease_timeout, _last_access))| {
                    let is_soft_pinned = soft_pin_timeout.map_or(false, |t| now < t);
                    let lease_expired = lease_timeout.is_none_or(|timeout| now >= timeout);
                    !*hard_pinned && lease_expired && !is_soft_pinned
                },
            )
            .map(|(i, _)| i)
            .collect();

        let mut selected = take_sorted_lease_candidates(candidates, &mut normal, target_count);
        if selected.len() >= target_count || !allow_soft_pinned {
            return selected;
        }

        let mut soft_pinned: Vec<usize> = candidates
            .iter()
            .enumerate()
            .filter(
                |(_, (key, soft_pin_timeout, hard_pinned, lease_timeout, _last_access))| {
                    if key.is_empty() {
                        return false;
                    }
                    let is_soft_pinned = soft_pin_timeout.map_or(false, |t| now < t);
                    let lease_expired = lease_timeout.is_none_or(|timeout| now >= timeout);
                    !*hard_pinned && lease_expired && is_soft_pinned
                },
            )
            .map(|(i, _)| i)
            .collect();

        selected.extend(take_sorted_lease_candidates(
            candidates,
            &mut soft_pinned,
            target_count.saturating_sub(selected.len()),
        ));
        selected
    }

    /// Check if a soft pin has expired.
    /// 检查软固定是否已过期。
    ///
    /// Returns true if the creation time is older than soft_pin_ttl,
    /// meaning the object is now eligible for eviction.
    /// 创建时间超过 soft_pin_ttl 后返回 true，表示该对象现在可以被驱逐。
    /// 软固定过期意味着对象不再受初始保护。
    pub fn soft_pin_expired(&self, created_at: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(created_at)
            .map(|d| d > self.soft_pin_ttl)
            .unwrap_or(true)
    }

    /// Check if the lease on an object has expired.
    /// 检查租约是否过期。
    ///
    /// A lease is active (not expired) if the last_access is within lease_ttl
    /// from `now`. Once expired, the object can be evicted.
    /// 如果 last_access 在 now 的 lease_ttl 范围内，租约有效（未过期）。
    /// 过期后对象可被驱逐。
    /// 租约机制保证最近被访问的对象不会被误驱逐。
    pub fn is_lease_expired(&self, last_access: SystemTime, now: SystemTime) -> bool {
        now.duration_since(last_access)
            .map(|d| d > self.lease_ttl)
            .unwrap_or(true)
    }
}

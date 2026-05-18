use mooncake_store_core::ReplicaDescriptor;
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

    pub fn select_for_eviction(
        &self,
        candidates: &[(&str, &[ReplicaDescriptor], bool, SystemTime)],
        target_count: usize,
    ) -> Vec<String> {
        if target_count == 0 {
            return vec![];
        }

        let now = SystemTime::now();
        let mut sorted: Vec<_> = candidates
            .iter()
            .filter(|(_, _, soft_pinned, _)| !*soft_pinned)
            .filter(|(_, _, _, last_access)| self.is_lease_expired(*last_access, now))
            .collect();

        sorted.sort_by_key(|(_, _, _, last_access)| *last_access);

        sorted
            .into_iter()
            .take(target_count)
            .map(|(key, _, _, _)| (*key).to_string())
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

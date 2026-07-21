use super::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

pub(super) struct AllocationPlan {
    target_count: usize,
    replicas: Vec<ReplicaDescriptor>,
    used_segment_names: HashSet<String>,
}

impl AllocationPlan {
    pub(super) fn with_capacity(target_count: usize) -> Self {
        Self {
            target_count,
            replicas: Vec::with_capacity(target_count),
            used_segment_names: HashSet::new(),
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.replicas.len() >= self.target_count
    }

    pub(super) fn remaining(&self) -> usize {
        self.target_count.saturating_sub(self.replicas.len())
    }

    pub(super) fn contains_segment(&self, segment_name: &str) -> bool {
        self.used_segment_names.contains(segment_name)
    }

    pub(super) fn push(&mut self, replica: ReplicaDescriptor) {
        self.used_segment_names.insert(replica.segment_name.clone());
        self.replicas.push(replica);
    }

    pub(super) fn into_replicas(self) -> Vec<ReplicaDescriptor> {
        self.replicas
    }
}

#[cfg(test)]
mod tests {
    use super::AllocationPlan;

    #[test]
    fn allocation_plan_tracks_target_and_partial_progress() {
        let plan = AllocationPlan::with_capacity(3);

        assert_eq!(plan.remaining(), 3);
        assert!(!plan.is_complete());
        assert!(plan.into_replicas().is_empty());
    }
}

impl SegmentAllocator {
    pub(super) fn allocate_random_remaining(
        &mut self,
        slice_size: u64,
        plan: &mut AllocationPlan,
        excluded_segments: &HashSet<String>,
    ) {
        let names = self.segment_names();
        if names.is_empty() {
            return;
        }

        let max_retry = RANDOM_MAX_RETRY_LIMIT.min(names.len());
        let mut rng = thread_rng();
        let mut start_idx = rng.gen_range(0..names.len());

        for _ in 0..max_retry {
            if plan.is_complete() {
                return;
            }
            let segment_name = names[start_idx % names.len()].clone();
            start_idx += 1;

            if excluded_segments.contains(&segment_name) || plan.contains_segment(&segment_name) {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                plan.push(replica);
            }
        }
    }

    pub(super) fn allocate_free_ratio_remaining(
        &mut self,
        slice_size: u64,
        plan: &mut AllocationPlan,
        excluded_segments: &HashSet<String>,
    ) {
        let names = self.segment_names();
        if names.is_empty() {
            return;
        }

        let remaining = plan.remaining();
        let sample_count = (FREE_RATIO_CANDIDATE_MULTIPLIER * remaining).min(names.len());
        let mut rng = thread_rng();
        let mut start_idx = rng.gen_range(0..names.len());
        let mut candidates = Vec::with_capacity(sample_count);

        for _ in 0..sample_count {
            let segment_name = names[start_idx % names.len()].clone();
            start_idx += 1;
            candidates.push((
                segment_name.clone(),
                self.free_ratio_for_name(&segment_name),
            ));
        }

        candidates.sort_by(|(_, ratio_a), (_, ratio_b)| {
            ratio_b.partial_cmp(ratio_a).unwrap_or(Ordering::Equal)
        });

        for (segment_name, _) in candidates {
            if plan.is_complete() {
                return;
            }
            if excluded_segments.contains(&segment_name) || plan.contains_segment(&segment_name) {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                plan.push(replica);
            }
        }

        if !plan.is_complete() {
            self.allocate_random_remaining(slice_size, plan, excluded_segments);
        }
    }

    pub(super) fn allocate_ssd_free_ratio_remaining(
        &mut self,
        slice_size: u64,
        plan: &mut AllocationPlan,
        excluded_segments: &HashSet<String>,
        ssd_metrics: Option<&HashMap<Uuid, SsdUsageMetrics>>,
    ) {
        let names = self.segment_names();
        if names.is_empty() {
            return;
        }

        let remaining = plan.remaining();
        let sample_count = (FREE_RATIO_CANDIDATE_MULTIPLIER * remaining).min(names.len());
        let mut rng = thread_rng();
        let mut start_idx = rng.gen_range(0..names.len());
        let mut candidates = Vec::with_capacity(sample_count);

        for _ in 0..sample_count {
            let segment_name = names[start_idx % names.len()].clone();
            start_idx += 1;
            candidates.push((
                segment_name.clone(),
                self.ssd_free_ratio_for_name(&segment_name, ssd_metrics),
                self.free_ratio_for_name(&segment_name),
            ));
        }

        candidates.sort_by(|(_, ssd_a, mem_a), (_, ssd_b, mem_b)| {
            ssd_b
                .partial_cmp(ssd_a)
                .unwrap_or(Ordering::Equal)
                .then_with(|| mem_b.partial_cmp(mem_a).unwrap_or(Ordering::Equal))
        });

        for (segment_name, _, _) in candidates {
            if plan.is_complete() {
                return;
            }
            if excluded_segments.contains(&segment_name) || plan.contains_segment(&segment_name) {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                plan.push(replica);
            }
        }

        if !plan.is_complete() {
            self.allocate_random_remaining(slice_size, plan, excluded_segments);
        }
    }

    pub(super) fn allocate_local_first_remaining(
        &mut self,
        key: &str,
        client_id: Option<Uuid>,
        slice_size: u64,
        plan: &mut AllocationPlan,
        excluded_segments: &HashSet<String>,
    ) {
        if plan.target_count != 1 {
            self.allocate_random_remaining(slice_size, plan, excluded_segments);
            return;
        }

        for segment_name in self.host_ordered_segment_names(client_id, key) {
            if plan.is_complete() {
                return;
            }
            if excluded_segments.contains(&segment_name) || plan.contains_segment(&segment_name) {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                plan.push(replica);
                return;
            }
        }

        if !plan.is_complete() {
            self.allocate_random_remaining(slice_size, plan, excluded_segments);
        }
    }

    fn segment_names(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.segments
            .values()
            .filter_map(|state| {
                if seen.insert(state.segment.name.clone()) {
                    Some(state.segment.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn free_ratio_for_name(&self, segment_name: &str) -> f64 {
        let (total, used) = self
            .segments
            .values()
            .filter(|state| state.segment.name == segment_name)
            .fold((0_u64, 0_u64), |(total, used), state| {
                (
                    total.saturating_add(state.segment.size),
                    used.saturating_add(state.used),
                )
            });
        offset_layout::free_ratio(total, used)
    }

    fn ssd_free_ratio_for_name(
        &self,
        segment_name: &str,
        ssd_metrics: Option<&HashMap<Uuid, SsdUsageMetrics>>,
    ) -> f64 {
        let Some(ssd_metrics) = ssd_metrics else {
            return self.free_ratio_for_name(segment_name);
        };
        let mut seen_clients = HashSet::new();
        let (total, used) = self
            .segments
            .values()
            .filter(|state| state.segment.name == segment_name)
            .filter(|state| seen_clients.insert(state.client_id))
            .filter_map(|state| ssd_metrics.get(&state.client_id))
            .fold((0_u64, 0_u64), |(total, used), metrics| {
                (
                    total.saturating_add(metrics.total_capacity_bytes),
                    used.saturating_add(metrics.used_bytes),
                )
            });
        if total == 0 {
            return self.free_ratio_for_name(segment_name);
        }
        offset_layout::free_ratio(total, used)
    }

    fn host_ordered_segment_names(&self, client_id: Option<Uuid>, key: &str) -> Vec<String> {
        let Some(client_id) = client_id else {
            return Vec::new();
        };
        let Some(writer_host) = self.host_for_client(client_id) else {
            return Vec::new();
        };

        let mut by_host: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for name in self.segment_names() {
            by_host
                .entry(host_from_segment_name(&name))
                .or_default()
                .push(name);
        }
        if by_host.is_empty() {
            return Vec::new();
        }

        let hosts = by_host.keys().cloned().collect::<Vec<_>>();
        let start_idx = hosts
            .iter()
            .position(|host| host == &writer_host)
            .unwrap_or_else(|| {
                hosts
                    .iter()
                    .position(|host| host >= &writer_host)
                    .unwrap_or(0)
            });

        let mut ordered = Vec::new();
        for idx in 0..hosts.len() {
            let host = &hosts[(start_idx + idx) % hosts.len()];
            let Some(names) = by_host.get_mut(host) else {
                continue;
            };
            names.sort();
            let start = stable_key_hash(key) % names.len();
            for name_idx in 0..names.len() {
                ordered.push(names[(start + name_idx) % names.len()].clone());
            }
        }
        ordered
    }

    fn host_for_client(&self, client_id: Uuid) -> Option<String> {
        self.segments
            .values()
            .find(|state| state.client_id == client_id)
            .map(|state| host_from_segment_name(&state.segment.name))
    }

    pub(super) fn allocate_from_segment_name(
        &mut self,
        segment_name: &str,
        slice_size: u64,
    ) -> Option<ReplicaDescriptor> {
        let mut ids = self
            .segments
            .iter()
            .filter(|(_, state)| state.segment.name == segment_name)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.shuffle(&mut thread_rng());
        for segment_id in ids {
            if let Some(replica) = self.allocate_from_segment_id(segment_id, slice_size) {
                return Some(replica);
            }
        }
        None
    }

    fn allocate_from_segment_id(
        &mut self,
        segment_id: Uuid,
        slice_size: u64,
    ) -> Option<ReplicaDescriptor> {
        let state = self.segments.get_mut(&segment_id)?;
        let (offset, accounted_size) = state.allocate(slice_size)?;
        state.used = state.used.saturating_add(accounted_size);
        Some(ReplicaDescriptor {
            refcnt: 0,
            handle_valid: true,
            segment_id: state.segment.id,
            segment_name: state.segment.name.clone(),
            offset,
            size: slice_size,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: None,
            base_addr: state.segment.base,
            protocol: state.segment.protocol.clone(),
        })
    }
}

fn host_from_segment_name(name: &str) -> String {
    name.split(':').next().unwrap_or(name).to_string()
}

fn stable_key_hash(key: &str) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize
}

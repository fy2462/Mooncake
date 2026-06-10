use super::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use std::cmp::Ordering;

impl SegmentAllocator {
    pub(super) fn allocate_random_remaining(
        &mut self,
        slice_size: u64,
        replica_count: usize,
        replicas: &mut Vec<ReplicaDescriptor>,
        used_segment_names: &mut HashSet<String>,
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
            if replicas.len() >= replica_count {
                return;
            }
            let segment_name = names[start_idx % names.len()].clone();
            start_idx += 1;

            if excluded_segments.contains(&segment_name)
                || used_segment_names.contains(&segment_name)
            {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                used_segment_names.insert(replica.segment_name.clone());
                replicas.push(replica);
            }
        }
    }

    pub(super) fn allocate_free_ratio_remaining(
        &mut self,
        slice_size: u64,
        replica_count: usize,
        replicas: &mut Vec<ReplicaDescriptor>,
        used_segment_names: &mut HashSet<String>,
        excluded_segments: &HashSet<String>,
    ) {
        let names = self.segment_names();
        if names.is_empty() {
            return;
        }

        let remaining = replica_count.saturating_sub(replicas.len());
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
            if replicas.len() >= replica_count {
                return;
            }
            if excluded_segments.contains(&segment_name)
                || used_segment_names.contains(&segment_name)
            {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                used_segment_names.insert(replica.segment_name.clone());
                replicas.push(replica);
            }
        }

        if replicas.len() < replica_count {
            self.allocate_random_remaining(
                slice_size,
                replica_count,
                replicas,
                used_segment_names,
                excluded_segments,
            );
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
        })
    }
}

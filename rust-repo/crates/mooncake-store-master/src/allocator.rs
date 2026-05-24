use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct SegmentState {
    segment: Segment,
    free_ranges: Vec<(u64, u64)>,
}

/// Allocation strategy for replica placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationStrategy {
    Random,
    FreeRatioFirst,
}

/// Manages allocation of replicas across available segments.
pub struct SegmentAllocator {
    segments: HashMap<Uuid, SegmentState>,
    strategy: AllocationStrategy,
}

impl SegmentAllocator {
    pub fn new() -> Self {
        Self {
            segments: HashMap::new(),
            strategy: AllocationStrategy::Random,
        }
    }

    pub fn with_strategy(mut self, strategy: AllocationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn add_segment(&mut self, segment: Segment) {
        let tail_free = segment.size.saturating_sub(segment.used);
        let free_ranges = if tail_free > 0 {
            vec![(segment.used, tail_free)]
        } else {
            Vec::new()
        };
        self.segments.insert(segment.id, SegmentState {
            segment,
            free_ranges,
        });
    }

    pub fn remove_segment(&mut self, segment_id: &Uuid) {
        self.segments.remove(segment_id);
    }

    /// Allocate replicas for an object slice.
    ///
    /// Returns a list of `ReplicaDescriptor` describing where each replica
    /// should be placed.  Guarantees that each replica lands on a different
    /// segment, and respects the preferred segment when provided.
    pub fn allocate(
        &mut self,
        _key: &str,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Vec<ReplicaDescriptor> {
        if self.segments.is_empty() || replica_count == 0 {
            return vec![];
        }

        let mut candidates: Vec<Uuid> = self
            .segments
            .iter()
            .filter(|(_, state)| {
                state.free_ranges.iter().any(|(_, len)| *len >= slice_size)
            })
            .map(|(id, _)| *id)
            .collect();

        match self.strategy {
            AllocationStrategy::Random => {
                candidates.shuffle(&mut thread_rng());
            }
            AllocationStrategy::FreeRatioFirst => {
                candidates.sort_by(|a, b| {
                    let seg_a = &self.segments[a].segment;
                    let seg_b = &self.segments[b].segment;
                    let ratio_a = seg_a.size.saturating_sub(seg_a.used) as f64 / seg_a.size as f64;
                    let ratio_b = seg_b.size.saturating_sub(seg_b.used) as f64 / seg_b.size as f64;
                    ratio_b.partial_cmp(&ratio_a).unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }

        // If a preferred segment is specified, move it to front.
        if !config.preferred_segment.is_empty() {
            candidates.sort_by_key(|segment_id| {
                if self.segments[segment_id].segment.name == config.preferred_segment {
                    0
                } else {
                    1
                }
            });
        }

        let count = replica_count.min(candidates.len());
        let mut replicas = Vec::with_capacity(count);
        for segment_id in candidates.into_iter().take(count) {
            let state = match self.segments.get_mut(&segment_id) {
                Some(state) => state,
                None => continue,
            };
            let Some(offset) = reserve_range(&mut state.free_ranges, slice_size) else {
                continue;
            };
            state.segment.used = state.segment.used.saturating_add(slice_size);
            replicas.push(ReplicaDescriptor {
                segment_id: state.segment.id,
                segment_name: state.segment.name.clone(),
                offset,
                size: slice_size,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
            });
        }
        replicas
    }

    pub fn release(&mut self, replicas: &[ReplicaDescriptor]) {
        for replica in replicas {
            let Some(state) = self.segments.get_mut(&replica.segment_id) else {
                continue;
            };
            if replica.size == 0 || replica.offset >= state.segment.size {
                continue;
            }
            let releasable = replica.size.min(state.segment.size - replica.offset);
            insert_free_range(&mut state.free_ranges, replica.offset, releasable);
            state.segment.used = state.segment.used.saturating_sub(releasable);
        }
    }

    pub fn used_bytes(&self, segment_id: &Uuid) -> Option<u64> {
        self.segments.get(segment_id).map(|state| state.segment.used)
    }
}

fn reserve_range(free_ranges: &mut Vec<(u64, u64)>, size: u64) -> Option<u64> {
    let idx = free_ranges.iter().position(|(_, len)| *len >= size)?;
    let (offset, len) = free_ranges[idx];
    if len == size {
        free_ranges.remove(idx);
    } else {
        free_ranges[idx] = (offset + size, len - size);
    }
    Some(offset)
}

fn insert_free_range(free_ranges: &mut Vec<(u64, u64)>, offset: u64, len: u64) {
    free_ranges.push((offset, len));
    free_ranges.sort_by_key(|(start, _)| *start);

    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(free_ranges.len());
    for (start, size) in free_ranges.drain(..) {
        if let Some((prev_start, prev_size)) = merged.last_mut() {
            let prev_end = *prev_start + *prev_size;
            let current_end = start + size;
            if start <= prev_end {
                *prev_size = (*prev_size).max(current_end.saturating_sub(*prev_start));
                continue;
            }
        }
        merged.push((start, size));
    }
    *free_ranges = merged;
}

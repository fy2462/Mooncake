use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use uuid::Uuid;

/// Allocation strategy for replica placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationStrategy {
    Random,
    FreeRatioFirst,
}

/// Manages allocation of replicas across available segments.
pub struct SegmentAllocator {
    segments: HashMap<Uuid, Segment>,
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
        self.segments.insert(segment.id, segment);
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
        &self,
        _key: &str,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Vec<ReplicaDescriptor> {
        if self.segments.is_empty() || replica_count == 0 {
            return vec![];
        }

        let mut candidates: Vec<&Segment> = self
            .segments
            .values()
            .filter(|s| s.size - s.used >= slice_size)
            .collect();

        match self.strategy {
            AllocationStrategy::Random => {
                candidates.shuffle(&mut thread_rng());
            }
            AllocationStrategy::FreeRatioFirst => {
                candidates.sort_by(|a, b| {
                    let ratio_a = (a.size - a.used) as f64 / a.size as f64;
                    let ratio_b = (b.size - b.used) as f64 / b.size as f64;
                    ratio_b.partial_cmp(&ratio_a).unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }

        // If a preferred segment is specified, move it to front.
        if !config.preferred_segment.is_empty() {
            candidates.sort_by_key(|s| if s.name == config.preferred_segment { 0 } else { 1 });
        }

        let count = replica_count.min(candidates.len());
        candidates[..count]
            .iter()
            .map(|seg| ReplicaDescriptor {
                segment_id: seg.id,
                segment_name: seg.name.clone(),
                offset: seg.used, // simplified: always at current used boundary
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
            })
            .collect()
    }
}

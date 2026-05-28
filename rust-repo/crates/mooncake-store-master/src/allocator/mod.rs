mod cachelib;
mod types;

use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment,
};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::cmp::Ordering;
use std::collections::HashMap;
use uuid::Uuid;

use self::cachelib::{
    align_up, allocate_cachelib, generate_cachelib_class_sizes, release_cachelib,
    CachelibSegmentState,
};
use self::types::DEFAULT_CACHELIB_POOL_NAME;
pub use self::types::{
    cachelib_allocation_class_id_for_request, cachelib_allocation_class_size_for_request,
    AllocationStrategy, CachelibAllocInfo, CachelibAllocationVisit, ClassId, MemoryAllocatorKind,
    PoolId, SlabReleaseContext, SlabReleaseMode, CACHELIB_MIN_ALLOC_SIZE, CACHELIB_SLAB_SIZE,
};

#[derive(Debug, Clone)]
struct OffsetSegmentState {
    free_ranges: Vec<(u64, u64)>,
}

#[derive(Debug, Clone)]
enum SegmentLayout {
    Offset(OffsetSegmentState),
    Cachelib(CachelibSegmentState),
}

#[derive(Debug, Clone)]
struct SegmentState {
    segment: Segment,
    used: u64,
    client_id: Uuid,
    layout: SegmentLayout,
}

pub struct SegmentAllocator {
    segments: HashMap<Uuid, SegmentState>,
    strategy: AllocationStrategy,
    memory_allocator_kind: MemoryAllocatorKind,
}

impl SegmentAllocator {
    pub fn new() -> Self {
        Self {
            segments: HashMap::new(),
            strategy: AllocationStrategy::Random,
            memory_allocator_kind: MemoryAllocatorKind::Offset,
        }
    }

    pub fn with_strategy(mut self, strategy: AllocationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_memory_allocator(mut self, memory_allocator_kind: MemoryAllocatorKind) -> Self {
        self.memory_allocator_kind = memory_allocator_kind;
        self
    }

    pub fn memory_allocator_kind(&self) -> MemoryAllocatorKind {
        self.memory_allocator_kind
    }

    pub fn add_segment(&mut self, segment: Segment, used: u64, client_id: Uuid) {
        let (layout, effective_used) = match self.memory_allocator_kind {
            MemoryAllocatorKind::Offset => {
                let tail_free = segment.size.saturating_sub(used);
                let free_ranges = if tail_free > 0 {
                    vec![(used, tail_free)]
                } else {
                    Vec::new()
                };
                (SegmentLayout::Offset(OffsetSegmentState { free_ranges }), used)
            }
            MemoryAllocatorKind::CachelibLike => {
                let reserved_bytes = align_up(used, CACHELIB_SLAB_SIZE).min(segment.size);
                let total_slabs = (segment.size / CACHELIB_SLAB_SIZE) as u32;
                let reserved_slabs = (reserved_bytes / CACHELIB_SLAB_SIZE) as u32;
                let total_capacity_bytes =
                    (total_slabs.saturating_sub(reserved_slabs)) as u64 * CACHELIB_SLAB_SIZE;
                let mut cachelib = CachelibSegmentState {
                    total_capacity_bytes,
                    class_sizes: generate_cachelib_class_sizes(),
                    pools: HashMap::new(),
                    pool_names: HashMap::new(),
                    next_pool_id: 0,
                    default_pool_id: 0,
                    unreserved_slab_indices: (reserved_slabs..total_slabs)
                        .rev()
                        .collect::<Vec<_>>(),
                    allocations: HashMap::new(),
                    pending_releases: HashMap::new(),
                    next_release_token: 1,
                    n_slab_resize: 0,
                    n_slab_rebalance: 0,
                    n_slab_release_aborted: 0,
                };
                let main_pool_id = cachelib
                    .create_pool(DEFAULT_CACHELIB_POOL_NAME.to_string(), total_capacity_bytes)
                    .expect("default cachelib pool should be provisionable");
                cachelib.default_pool_id = main_pool_id;
                (SegmentLayout::Cachelib(cachelib), reserved_bytes)
            }
        };
        self.segments
            .insert(segment.id, SegmentState { segment, used: effective_used, client_id, layout });
    }

    pub fn remove_segment(&mut self, segment_id: &Uuid) {
        self.segments.remove(segment_id);
    }

    pub fn allocate(
        &mut self,
        key: &str,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Vec<ReplicaDescriptor> {
        self.allocate_for_client(key, None, slice_size, replica_count, config)
    }

    pub fn allocate_for_client(
        &mut self,
        _key: &str,
        client_id: Option<Uuid>,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Vec<ReplicaDescriptor> {
        if self.segments.is_empty() || replica_count == 0 || slice_size == 0 {
            return vec![];
        }

        let mut candidates: Vec<Uuid> = self
            .segments
            .iter()
            .filter(|(_, state)| state.can_allocate(slice_size))
            .map(|(id, _)| *id)
            .collect();

        match self.strategy {
            AllocationStrategy::Random => {
                candidates.shuffle(&mut thread_rng());
            }
            AllocationStrategy::FreeRatioFirst => {
                candidates.sort_by(|a, b| {
                    let state_a = &self.segments[a];
                    let state_b = &self.segments[b];
                    let ratio_a = free_ratio(state_a.segment.size, state_a.used);
                    let ratio_b = free_ratio(state_b.segment.size, state_b.used);
                    ratio_b.partial_cmp(&ratio_a).unwrap_or(Ordering::Equal)
                });
            }
        }

        if !config.preferred_segment.is_empty() {
            candidates.sort_by_key(|segment_id| {
                if self.segments[segment_id].segment.name == config.preferred_segment {
                    0
                } else {
                    1
                }
            });
        }

        if config.prefer_alloc_in_same_node {
            let preferred_host = client_id.and_then(|client_id| {
                self.segments
                    .values()
                    .find(|state| state.client_id == client_id)
                    .map(|state| segment_host(&state.segment.name))
            });
            if let Some(preferred_host) = preferred_host {
                candidates.sort_by_key(|segment_id| {
                    let host = segment_host(&self.segments[segment_id].segment.name);
                    if host == preferred_host { 0 } else { 1 }
                });
            }
        }

        let count = replica_count.min(candidates.len());
        let mut replicas = Vec::with_capacity(count);
        for segment_id in candidates.into_iter().take(count) {
            let Some(state) = self.segments.get_mut(&segment_id) else {
                continue;
            };
            let Some((offset, accounted_size)) = state.allocate(slice_size) else {
                continue;
            };
            state.used = state.used.saturating_add(accounted_size);
            replicas.push(ReplicaDescriptor {
                refcnt: 0,
                handle_valid: true,
                segment_id: state.segment.id,
                segment_name: state.segment.name.clone(),
                offset,
                size: slice_size,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
                holder_client_id: None,
            });
        }
        replicas
    }

    pub fn release(&mut self, replicas: &[ReplicaDescriptor]) {
        for replica in replicas {
            let Some(state) = self.segments.get_mut(&replica.segment_id) else {
                continue;
            };
            let Some(released_size) = state.release(replica) else {
                continue;
            };
            state.used = state.used.saturating_sub(released_size);
        }
    }

    pub fn used_bytes(&self, segment_id: &Uuid) -> Option<u64> {
        self.segments
            .get(segment_id)
            .map(|state| state.used)
    }

    pub fn usage_totals(&self) -> (u64, u64) {
        self.segments.values().fold((0, 0), |(total, used), state| {
            (
                total.saturating_add(state.segment.size),
                used.saturating_add(state.used),
            )
        })
    }

    pub fn add_pool(
        &mut self,
        segment_id: &Uuid,
        name: impl Into<String>,
        size_bytes: u64,
    ) -> Result<PoolId, String> {
        self.add_pool_with_options(segment_id, name, size_bytes, false)
    }

    pub fn add_pool_with_options(
        &mut self,
        segment_id: &Uuid,
        name: impl Into<String>,
        size_bytes: u64,
        ensure_provisionable: bool,
    ) -> Result<PoolId, String> {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("pool operations require cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.create_pool_with_options(name.into(), size_bytes, ensure_provisionable)
            }
        }
    }

    pub fn grow_pool(
        &mut self,
        segment_id: &Uuid,
        pool_id: PoolId,
        size_bytes: u64,
    ) -> Result<bool, String> {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("pool operations require cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.grow_pool(pool_id, size_bytes),
        }
    }

    pub fn shrink_pool(
        &mut self,
        segment_id: &Uuid,
        pool_id: PoolId,
        size_bytes: u64,
    ) -> Result<bool, String> {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("pool operations require cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.shrink_pool(pool_id, size_bytes),
        }
    }

    pub fn resize_pools(
        &mut self,
        segment_id: &Uuid,
        src_pool_id: PoolId,
        dest_pool_id: PoolId,
        size_bytes: u64,
    ) -> Result<bool, String> {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("pool operations require cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.resize_pools(src_pool_id, dest_pool_id, size_bytes)
            }
        }
    }

    pub fn pool_ids(&self, segment_id: &Uuid) -> Option<Vec<PoolId>> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.pool_ids()),
        }
    }

    pub fn pool_name(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<String> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.pools.get(&pool_id).map(|pool| pool.name.clone())
            }
        }
    }

    pub fn pool_id(&self, segment_id: &Uuid, name: &str) -> Option<PoolId> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_names.get(name).copied(),
        }
    }

    pub fn memory_size(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.total_capacity_bytes),
        }
    }

    pub fn all_slabs_allocated(&self, segment_id: &Uuid) -> Option<bool> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.bytes_unreserved() == 0),
        }
    }

    pub fn all_slabs_allocated_for_pool(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<bool> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.all_slabs_allocated_for_pool(pool_id).ok()
            }
        }
    }

    pub fn pool_is_over_limit(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<bool> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_over_limit(pool_id).ok(),
        }
    }

    pub fn pools_over_limit(&self, segment_id: &Uuid) -> Option<Vec<PoolId>> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.pools_over_limit()),
        }
    }

    pub fn bytes_unreserved(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.bytes_unreserved()),
        }
    }

    pub fn pool_advised_size(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_advised_size(pool_id),
        }
    }

    pub fn pool_usable_size(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_usable_size(pool_id),
        }
    }

    pub fn pool_current_alloc_size(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => {
                Some(cachelib.current_alloc_size_for_pool(pool_id))
            }
        }
    }

    pub fn pool_unallocated_slab_memory(
        &self,
        segment_id: &Uuid,
        pool_id: PoolId,
    ) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_unallocated_slab_memory(pool_id),
        }
    }

    pub fn advised_memory_size(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.advised_memory_size()),
        }
    }

    pub fn n_slab_resize(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.n_slab_resize),
        }
    }

    pub fn n_slab_rebalance(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.n_slab_rebalance),
        }
    }

    pub fn n_slab_release_aborted(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.n_slab_release_aborted),
        }
    }

    pub fn is_alloc_freed(
        &self,
        segment_id: &Uuid,
        context: &SlabReleaseContext,
        offset: u64,
    ) -> Result<bool, String> {
        let Some(state) = self.segments.get(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &state.layout {
            SegmentLayout::Offset(_) => {
                Err("slab release requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.is_alloc_freed(context, offset),
        }
    }

    pub fn all_allocs_freed(
        &self,
        segment_id: &Uuid,
        context: &SlabReleaseContext,
    ) -> Result<bool, String> {
        let Some(state) = self.segments.get(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &state.layout {
            SegmentLayout::Offset(_) => {
                Err("slab release requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.all_allocs_freed(context),
        }
    }

    pub fn process_alloc_for_release<F>(
        &self,
        segment_id: &Uuid,
        context: &SlabReleaseContext,
        offset: u64,
        callback: F,
    ) -> Result<(), String>
    where
        F: FnOnce(u64),
    {
        let Some(state) = self.segments.get(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &state.layout {
            SegmentLayout::Offset(_) => {
                Err("slab release requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.process_alloc_for_release(context, offset, callback)
            }
        }
    }

    pub fn allocation_class_id(
        &self,
        segment_id: &Uuid,
        pool_id: PoolId,
        requested_size: u64,
    ) -> Result<ClassId, String> {
        let Some(state) = self.segments.get(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &state.layout {
            SegmentLayout::Offset(_) => {
                Err("allocation classes require cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.allocation_class_id(pool_id, requested_size)
            }
        }
    }

    pub fn alloc_size_by_class_id(
        &self,
        segment_id: &Uuid,
        pool_id: PoolId,
        class_id: ClassId,
    ) -> Result<u64, String> {
        let Some(state) = self.segments.get(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &state.layout {
            SegmentLayout::Offset(_) => {
                Err("allocation classes require cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.alloc_size_by_class_id(pool_id, class_id),
        }
    }

    pub fn alloc_info(&self, segment_id: &Uuid, offset: u64) -> Result<CachelibAllocInfo, String> {
        let Some(state) = self.segments.get(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &state.layout {
            SegmentLayout::Offset(_) => {
                Err("alloc info requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.alloc_info(offset),
        }
    }

    pub fn for_each_allocation<F>(&self, segment_id: &Uuid, mut callback: F) -> Result<u64, String>
    where
        F: FnMut(CachelibAllocationVisit) -> bool,
    {
        let Some(state) = self.segments.get(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &state.layout {
            SegmentLayout::Offset(_) => {
                Err("allocation traversal requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.for_each_allocation(&mut callback),
        }
    }

    pub fn start_slab_release(
        &mut self,
        segment_id: &Uuid,
        pool_id: PoolId,
        victim_class_size: Option<u64>,
        receiver_class_size: Option<u64>,
        mode: SlabReleaseMode,
    ) -> Result<SlabReleaseContext, String> {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("slab release requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.start_slab_release(pool_id, victim_class_size, receiver_class_size, mode)
            }
        }
    }

    pub fn start_slab_release_with_options<F>(
        &mut self,
        segment_id: &Uuid,
        pool_id: PoolId,
        victim_class_size: Option<u64>,
        receiver_class_size: Option<u64>,
        mode: SlabReleaseMode,
        hint_offset: Option<u64>,
        should_abort: F,
    ) -> Result<SlabReleaseContext, String>
    where
        F: Fn() -> bool,
    {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("slab release requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.start_slab_release_with_options(
                pool_id,
                victim_class_size,
                receiver_class_size,
                mode,
                hint_offset,
                should_abort,
            ),
        }
    }

    pub fn complete_slab_release(
        &mut self,
        segment_id: &Uuid,
        context: &SlabReleaseContext,
    ) -> Result<(), String> {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("slab release requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.complete_slab_release(context),
        }
    }

    pub fn abort_slab_release(
        &mut self,
        segment_id: &Uuid,
        context: &SlabReleaseContext,
    ) -> Result<(), String> {
        let Some(state) = self.segments.get_mut(segment_id) else {
            return Err(format!("segment {segment_id} not found"));
        };
        match &mut state.layout {
            SegmentLayout::Offset(_) => {
                Err("slab release requires cachelib-like allocator".to_string())
            }
            SegmentLayout::Cachelib(cachelib) => cachelib.abort_slab_release(context),
        }
    }
}

fn segment_host(name: &str) -> &str {
    name.split(':').next().unwrap_or(name)
}

impl SegmentState {
    fn can_allocate(&self, size: u64) -> bool {
        match &self.layout {
            SegmentLayout::Offset(offset) => offset.free_ranges.iter().any(|(_, len)| *len >= size),
            SegmentLayout::Cachelib(cachelib) => {
                let Some(class_size) = cachelib::cachelib_class_size(&cachelib.class_sizes, size)
                else {
                    return false;
                };
                cachelib.pool_can_allocate(cachelib.default_pool_id, class_size)
            }
        }
    }

    fn allocate(&mut self, size: u64) -> Option<(u64, u64)> {
        match &mut self.layout {
            SegmentLayout::Offset(offset) => {
                let start = reserve_range(&mut offset.free_ranges, size)?;
                Some((start, size))
            }
            SegmentLayout::Cachelib(cachelib) => allocate_cachelib(cachelib, size),
        }
    }

    fn release(&mut self, replica: &ReplicaDescriptor) -> Option<u64> {
        match &mut self.layout {
            SegmentLayout::Offset(offset) => {
                if replica.size == 0 || replica.offset >= self.segment.size {
                    return None;
                }
                let releasable = replica.size.min(self.segment.size - replica.offset);
                insert_free_range(&mut offset.free_ranges, replica.offset, releasable);
                Some(releasable)
            }
            SegmentLayout::Cachelib(cachelib) => release_cachelib(cachelib, replica.offset),
        }
    }
}

fn free_ratio(size: u64, used: u64) -> f64 {
    if size == 0 {
        return 0.0;
    }
    size.saturating_sub(used) as f64 / size as f64
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

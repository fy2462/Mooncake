use std::collections::HashMap;

use super::types::{
    CachelibAllocInfo, CachelibAllocationVisit, ClassId, PoolId, SlabReleaseContext,
    SlabReleaseMode, CACHELIB_ALIGNMENT, CACHELIB_CLASS_MIN_SIZE, CACHELIB_MAX_ALLOC_SIZE,
    CACHELIB_MIN_ALLOC_SIZE, CACHELIB_SLAB_SIZE,
};

#[derive(Debug, Clone)]
pub(super) struct CachelibAllocation {
    pub(super) pool_id: PoolId,
    pub(super) class_size: u64,
    pub(super) slab_index: u32,
    pub(super) slot_index: u32,
}

#[derive(Debug, Clone)]
pub(super) struct SlabClassState {
    pub(super) slab_index: u32,
    pub(super) capacity: u32,
    pub(super) free_slots: Vec<u32>,
    pub(super) pending_release_token: Option<u64>,
}

#[derive(Debug, Clone)]
pub(super) struct CachelibPoolState {
    pub(super) name: String,
    pub(super) configured_size_bytes: u64,
    pub(super) reserved_slabs: Vec<u32>,
    pub(super) class_slabs: HashMap<u64, Vec<SlabClassState>>,
    pub(super) advised_slabs: u64,
}

#[derive(Debug, Clone)]
pub(super) struct CachelibSegmentState {
    pub(super) total_capacity_bytes: u64,
    pub(super) class_sizes: Vec<u64>,
    pub(super) pools: HashMap<PoolId, CachelibPoolState>,
    pub(super) pool_names: HashMap<String, PoolId>,
    pub(super) next_pool_id: PoolId,
    pub(super) default_pool_id: PoolId,
    pub(super) unreserved_slab_indices: Vec<u32>,
    pub(super) allocations: HashMap<u64, CachelibAllocation>,
    pub(super) pending_releases: HashMap<u64, PendingSlabRelease>,
    pub(super) next_release_token: u64,
    pub(super) n_slab_resize: u64,
    pub(super) n_slab_rebalance: u64,
    pub(super) n_slab_release_aborted: u64,
}

#[derive(Debug, Clone)]
pub(super) struct PendingSlabRelease {
    pub(super) pool_id: PoolId,
    pub(super) victim_class_size: Option<u64>,
    pub(super) receiver_class_size: Option<u64>,
    pub(super) slab_index: u32,
    pub(super) mode: SlabReleaseMode,
    pub(super) active_allocations: HashMap<u64, u32>,
    pub(super) freed_slots: Vec<u32>,
}

pub(super) fn cachelib_class_size(class_sizes: &[u64], requested_size: u64) -> Option<u64> {
    let padded = align_up(
        requested_size.max(CACHELIB_MIN_ALLOC_SIZE),
        CACHELIB_ALIGNMENT,
    );
    class_sizes
        .iter()
        .copied()
        .find(|class_size| *class_size >= padded)
}

pub(super) fn allocate_cachelib(
    state: &mut CachelibSegmentState,
    requested_size: u64,
) -> Option<(u64, u64)> {
    let class_size = cachelib_class_size(&state.class_sizes, requested_size)?;
    let default_pool_id = state.default_pool_id;
    let pool = state.pools.get_mut(&default_pool_id)?;
    if let Some(slot) = pool.class_slabs.get_mut(&class_size).and_then(|slabs| {
        slabs
            .iter_mut()
            .find(|slab| slab.pending_release_token.is_none() && !slab.free_slots.is_empty())
            .and_then(|slab| {
                slab.free_slots
                    .pop()
                    .map(|slot_index| (slab.slab_index, slot_index))
            })
    }) {
        let (slab_index, slot_index) = slot;
        let offset = slab_index as u64 * CACHELIB_SLAB_SIZE + slot_index as u64 * class_size;
        state.allocations.insert(
            offset,
            CachelibAllocation {
                pool_id: default_pool_id,
                class_size,
                slab_index,
                slot_index,
            },
        );
        return Some((offset, class_size));
    }

    let slab_index = if let Some(existing) = pool.releasable_slabs().into_iter().min() {
        existing
    } else {
        let can_provision =
            pool.current_used_size() + CACHELIB_SLAB_SIZE <= pool.configured_size_bytes;
        if !can_provision {
            return None;
        }
        let slab_index = state.unreserved_slab_indices.pop()?;
        pool.reserved_slabs.push(slab_index);
        pool.reserved_slabs.sort_unstable_by(|a, b| b.cmp(a));
        slab_index
    };
    let slabs = pool.class_slabs.entry(class_size).or_default();
    let capacity = (CACHELIB_SLAB_SIZE / class_size) as u32;
    if capacity == 0 {
        return None;
    }
    let mut free_slots = (0..capacity).collect::<Vec<_>>();
    free_slots.reverse();
    let slot_index = free_slots.pop()?;
    slabs.push(SlabClassState {
        slab_index,
        capacity,
        free_slots,
        pending_release_token: None,
    });
    let offset = slab_index as u64 * CACHELIB_SLAB_SIZE + slot_index as u64 * class_size;
    state.allocations.insert(
        offset,
        CachelibAllocation {
            pool_id: default_pool_id,
            class_size,
            slab_index,
            slot_index,
        },
    );
    Some((offset, class_size))
}

pub(super) fn release_cachelib(state: &mut CachelibSegmentState, offset: u64) -> Option<u64> {
    let allocation = state.allocations.remove(&offset)?;
    let pool = state.pools.get_mut(&allocation.pool_id)?;
    let slabs = pool.class_slabs.get_mut(&allocation.class_size)?;
    let slab_pos = slabs
        .iter()
        .position(|slab| slab.slab_index == allocation.slab_index)?;
    let slab = &mut slabs[slab_pos];
    if let Some(token) = slab.pending_release_token {
        let pending = state.pending_releases.get_mut(&token)?;
        pending.active_allocations.remove(&offset);
        pending.freed_slots.push(allocation.slot_index);
    } else {
        slab.free_slots.push(allocation.slot_index);
        if slab.free_slots.len() as u32 == slab.capacity {
            let _slab = slabs.remove(slab_pos);
        }
    }
    if slabs.is_empty() {
        pool.class_slabs.remove(&allocation.class_size);
    }
    Some(allocation.class_size)
}

pub(super) fn align_up(value: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        return value;
    }
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value.saturating_add(alignment - remainder)
    }
}

pub(super) fn generate_cachelib_class_sizes() -> Vec<u64> {
    let mut sizes = Vec::new();
    let mut current = CACHELIB_CLASS_MIN_SIZE;
    while current < CACHELIB_MAX_ALLOC_SIZE {
        if CACHELIB_SLAB_SIZE / current <= 1 {
            break;
        }
        sizes.push(current);
        let next = align_up(((current as f64) * 1.25).ceil() as u64, CACHELIB_ALIGNMENT);
        if next <= current {
            current = current.saturating_add(CACHELIB_ALIGNMENT);
        } else {
            current = next;
        }
    }
    sizes.push(align_up(CACHELIB_MAX_ALLOC_SIZE, CACHELIB_ALIGNMENT));
    sizes
}

impl CachelibSegmentState {
    pub(super) fn start_slab_release(
        &mut self,
        pool_id: PoolId,
        victim_class_size: Option<u64>,
        receiver_class_size: Option<u64>,
        mode: SlabReleaseMode,
    ) -> Result<SlabReleaseContext, String> {
        self.start_slab_release_with_options(
            pool_id,
            victim_class_size,
            receiver_class_size,
            mode,
            None,
            || false,
        )
    }

    pub(super) fn start_slab_release_with_options<F>(
        &mut self,
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
        if receiver_class_size.is_some() && mode != SlabReleaseMode::Rebalance {
            return Err("receiver_class_size requires rebalance mode".to_string());
        }
        if victim_class_size.is_none() && mode != SlabReleaseMode::Resize {
            return Err("victim_class_size can be omitted only in resize mode".to_string());
        }

        if victim_class_size.is_none() {
            let pool = self
                .pools
                .get_mut(&pool_id)
                .ok_or_else(|| format!("invalid pool id {pool_id}"))?;
            let slab_index = pool
                .releasable_slabs()
                .into_iter()
                .min()
                .ok_or_else(|| "no releasable free slab available".to_string())?;
            pool.reserved_slabs.retain(|slab| *slab != slab_index);
            self.unreserved_slab_indices.push(slab_index);
            self.unreserved_slab_indices
                .sort_unstable_by(|a, b| b.cmp(a));
            return Ok(SlabReleaseContext {
                token: 0,
                pool_id,
                victim_class_size,
                receiver_class_size,
                slab_index,
                mode,
                active_offsets: Vec::new(),
                is_released: true,
            });
        }

        let victim_class_size = victim_class_size.expect("checked above");
        let hinted_slab_index = if let Some(offset) = hint_offset {
            let hint = self
                .allocations
                .get(&offset)
                .ok_or_else(|| format!("invalid slab release hint offset {offset}"))?;
            if hint.pool_id != pool_id || hint.class_size != victim_class_size {
                return Err("slab release hint does not match victim pool/class".to_string());
            }
            Some(hint.slab_index)
        } else {
            None
        };
        let (token, slab_index) = {
            let pool = self
                .pools
                .get_mut(&pool_id)
                .ok_or_else(|| format!("invalid pool id {pool_id}"))?;
            let slabs = pool
                .class_slabs
                .get_mut(&victim_class_size)
                .ok_or_else(|| format!("victim class size {victim_class_size} not found"))?;
            let victim = slabs
                .iter_mut()
                .filter(|slab| slab.pending_release_token.is_none())
                .filter(|slab| {
                    hinted_slab_index
                        .map(|hinted| slab.slab_index == hinted)
                        .unwrap_or(true)
                })
                .min_by_key(|slab| slab.capacity.saturating_sub(slab.free_slots.len() as u32))
                .ok_or_else(|| "no releasable slab available in victim class".to_string())?;
            let token = self.next_release_token;
            self.next_release_token = self.next_release_token.saturating_add(1);
            victim.pending_release_token = Some(token);
            (token, victim.slab_index)
        };

        let active_allocations = self
            .allocations
            .iter()
            .filter(|(_, alloc)| {
                alloc.pool_id == pool_id
                    && alloc.class_size == victim_class_size
                    && alloc.slab_index == slab_index
            })
            .map(|(offset, alloc)| (*offset, alloc.slot_index))
            .collect::<HashMap<_, _>>();
        if should_abort() {
            let pool = self
                .pools
                .get_mut(&pool_id)
                .ok_or_else(|| format!("invalid pool id {pool_id}"))?;
            if let Some(slabs) = pool.class_slabs.get_mut(&victim_class_size) {
                if let Some(victim) = slabs.iter_mut().find(|slab| slab.slab_index == slab_index) {
                    victim.pending_release_token = None;
                }
            }
            return Err(format!(
                "slab release aborted before creating context for slab {}",
                slab_index
            ));
        }
        let active_offsets = active_allocations.keys().copied().collect::<Vec<_>>();
        self.pending_releases.insert(
            token,
            PendingSlabRelease {
                pool_id,
                victim_class_size: Some(victim_class_size),
                receiver_class_size,
                slab_index,
                mode,
                active_allocations,
                freed_slots: Vec::new(),
            },
        );

        Ok(SlabReleaseContext {
            token,
            pool_id,
            victim_class_size: Some(victim_class_size),
            receiver_class_size,
            slab_index,
            mode,
            active_offsets,
            is_released: false,
        })
    }

    pub(super) fn complete_slab_release(
        &mut self,
        context: &SlabReleaseContext,
    ) -> Result<(), String> {
        if context.is_released {
            return Ok(());
        }
        let pending = self
            .pending_releases
            .remove(&context.token)
            .ok_or_else(|| format!("invalid slab release token {}", context.token))?;
        if !pending.active_allocations.is_empty() {
            self.pending_releases.insert(context.token, pending);
            return Err("slab release still has active allocations".to_string());
        }

        let pool = self
            .pools
            .get_mut(&pending.pool_id)
            .ok_or_else(|| format!("invalid pool id {}", pending.pool_id))?;
        let victim_class_size = pending
            .victim_class_size
            .ok_or_else(|| "complete requires victim class size".to_string())?;
        let mut removed = None;
        if let Some(slabs) = pool.class_slabs.get_mut(&victim_class_size) {
            if let Some(pos) = slabs.iter().position(|slab| {
                slab.slab_index == pending.slab_index
                    && slab.pending_release_token == Some(context.token)
            }) {
                removed = Some(slabs.remove(pos));
            }
            if slabs.is_empty() {
                pool.class_slabs.remove(&victim_class_size);
            }
        }
        if removed.is_none() {
            return Err(format!(
                "slab {} not found for completion",
                pending.slab_index
            ));
        }

        match pending.mode {
            SlabReleaseMode::Resize => {
                self.n_slab_resize = self.n_slab_resize.saturating_add(1);
                pool.reserved_slabs
                    .retain(|slab| *slab != pending.slab_index);
                self.unreserved_slab_indices.push(pending.slab_index);
                self.unreserved_slab_indices
                    .sort_unstable_by(|a, b| b.cmp(a));
            }
            SlabReleaseMode::Rebalance => {
                self.n_slab_rebalance = self.n_slab_rebalance.saturating_add(1);
                if let Some(receiver_class_size) = pending.receiver_class_size {
                    let capacity = (CACHELIB_SLAB_SIZE / receiver_class_size) as u32;
                    let free_slots = (0..capacity).rev().collect::<Vec<_>>();
                    pool.class_slabs
                        .entry(receiver_class_size)
                        .or_default()
                        .push(SlabClassState {
                            slab_index: pending.slab_index,
                            capacity,
                            free_slots,
                            pending_release_token: None,
                        });
                }
            }
        }
        Ok(())
    }

    pub(super) fn for_each_allocation<F>(&self, callback: &mut F) -> Result<u64, String>
    where
        F: FnMut(CachelibAllocationVisit) -> bool,
    {
        let mut skipped = 0;
        for (pool_id, pool) in &self.pools {
            for (class_id, class_size) in self.class_sizes.iter().enumerate() {
                let Some(slabs) = pool.class_slabs.get(class_size) else {
                    continue;
                };
                for slab in slabs {
                    if slab.pending_release_token.is_some() {
                        skipped += 1;
                        continue;
                    }
                    for slot_index in 0..slab.capacity {
                        let offset = slab.slab_index as u64 * CACHELIB_SLAB_SIZE
                            + slot_index as u64 * *class_size;
                        let allocated = self.allocations.contains_key(&offset);
                        let visit = CachelibAllocationVisit {
                            offset,
                            info: CachelibAllocInfo {
                                pool_id: *pool_id,
                                class_id: class_id as ClassId,
                                alloc_size: *class_size,
                            },
                            allocated,
                        };
                        if !callback(visit) {
                            return Ok(skipped);
                        }
                    }
                }
            }
        }
        Ok(skipped)
    }

    pub(super) fn abort_slab_release(
        &mut self,
        context: &SlabReleaseContext,
    ) -> Result<(), String> {
        if context.is_released {
            return Ok(());
        }
        self.n_slab_release_aborted = self.n_slab_release_aborted.saturating_add(1);
        let pending = self
            .pending_releases
            .remove(&context.token)
            .ok_or_else(|| format!("invalid slab release token {}", context.token))?;
        let pool = self
            .pools
            .get_mut(&pending.pool_id)
            .ok_or_else(|| format!("invalid pool id {}", pending.pool_id))?;
        let victim_class_size = pending
            .victim_class_size
            .ok_or_else(|| "abort requires victim class size".to_string())?;
        let slabs = pool
            .class_slabs
            .get_mut(&victim_class_size)
            .ok_or_else(|| format!("victim class size {victim_class_size} not found"))?;
        let slab = slabs
            .iter_mut()
            .find(|slab| slab.slab_index == pending.slab_index)
            .ok_or_else(|| format!("slab {} not found", pending.slab_index))?;
        slab.pending_release_token = None;
        slab.free_slots.extend(pending.freed_slots);
        slab.free_slots.sort_unstable_by(|a, b| b.cmp(a));
        slab.free_slots.dedup_by(|a, b| a == b);
        Ok(())
    }

    pub(super) fn create_pool(&mut self, name: String, size_bytes: u64) -> Result<PoolId, String> {
        self.create_pool_with_options(name, size_bytes, false)
    }

    pub(super) fn create_pool_with_options(
        &mut self,
        name: String,
        size_bytes: u64,
        ensure_provisionable: bool,
    ) -> Result<PoolId, String> {
        if self.pool_names.contains_key(&name) {
            return Err(format!("duplicate pool name {name}"));
        }
        if self.bytes_unreserved() < size_bytes {
            return Err(format!(
                "not enough unreserved bytes to create pool {name}: need {}, have {}",
                size_bytes,
                self.bytes_unreserved()
            ));
        }
        if ensure_provisionable {
            let minimum = self.class_sizes.len() as u64 * CACHELIB_SLAB_SIZE;
            if size_bytes < minimum {
                return Err(format!(
                    "pool {name} size {} is not provisionable; need at least {}",
                    size_bytes, minimum
                ));
            }
        }
        let pool_id = self.next_pool_id;
        self.next_pool_id = self.next_pool_id.saturating_add(1);
        self.pool_names.insert(name.clone(), pool_id);
        self.pools.insert(
            pool_id,
            CachelibPoolState {
                name,
                configured_size_bytes: size_bytes,
                reserved_slabs: Vec::new(),
                class_slabs: HashMap::new(),
                advised_slabs: 0,
            },
        );
        Ok(pool_id)
    }

    pub(super) fn pool_ids(&self) -> Vec<PoolId> {
        let mut ids = self.pools.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub(super) fn pool_can_allocate(&self, pool_id: PoolId, class_size: u64) -> bool {
        let Some(pool) = self.pools.get(&pool_id) else {
            return false;
        };
        pool.class_slabs
            .get(&class_size)
            .map(|slabs| slabs.iter().any(|slab| !slab.free_slots.is_empty()))
            .unwrap_or(false)
            || count_unassigned_reserved_slabs(pool) > 0
            || (pool.current_used_size() + CACHELIB_SLAB_SIZE <= pool.configured_size_bytes
                && !self.unreserved_slab_indices.is_empty())
    }

    pub(super) fn grow_pool(&mut self, pool_id: PoolId, size_bytes: u64) -> Result<bool, String> {
        if self.bytes_unreserved() < size_bytes {
            return Ok(false);
        }
        let Some(pool) = self.pools.get_mut(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        pool.configured_size_bytes = pool.configured_size_bytes.saturating_add(size_bytes);
        Ok(true)
    }

    pub(super) fn shrink_pool(&mut self, pool_id: PoolId, size_bytes: u64) -> Result<bool, String> {
        let Some(pool) = self.pools.get_mut(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        if pool.configured_size_bytes < size_bytes {
            return Ok(false);
        }
        pool.configured_size_bytes -= size_bytes;
        Ok(true)
    }

    pub(super) fn resize_pools(
        &mut self,
        src_pool_id: PoolId,
        dest_pool_id: PoolId,
        size_bytes: u64,
    ) -> Result<bool, String> {
        if src_pool_id == dest_pool_id {
            return Ok(true);
        }
        let Some(src_pool) = self.pools.get(&src_pool_id) else {
            return Err(format!("invalid source pool id {src_pool_id}"));
        };
        if !self.pools.contains_key(&dest_pool_id) {
            return Err(format!("invalid destination pool id {dest_pool_id}"));
        }
        if src_pool.configured_size_bytes < size_bytes {
            return Ok(false);
        }
        {
            let src_pool = self
                .pools
                .get_mut(&src_pool_id)
                .expect("source pool checked");
            src_pool.configured_size_bytes -= size_bytes;
        }
        {
            let dest_pool = self
                .pools
                .get_mut(&dest_pool_id)
                .expect("destination pool checked");
            dest_pool.configured_size_bytes =
                dest_pool.configured_size_bytes.saturating_add(size_bytes);
        }
        Ok(true)
    }

    pub(super) fn bytes_unreserved(&self) -> u64 {
        self.total_capacity_bytes.saturating_sub(
            self.pools
                .values()
                .map(|pool| pool.configured_size_bytes)
                .sum::<u64>(),
        )
    }

    pub(super) fn pool_over_limit(&self, pool_id: PoolId) -> Result<bool, String> {
        let Some(pool) = self.pools.get(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        Ok(pool.current_used_size() > pool.configured_size_bytes)
    }

    pub(super) fn pools_over_limit(&self) -> Vec<PoolId> {
        let mut ids = self
            .pools
            .iter()
            .filter_map(|(id, pool)| {
                (pool.current_used_size() > pool.configured_size_bytes).then_some(*id)
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub(super) fn all_slabs_allocated_for_pool(&self, pool_id: PoolId) -> Result<bool, String> {
        let Some(pool) = self.pools.get(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        Ok(
            pool.current_used_size().saturating_add(CACHELIB_SLAB_SIZE)
                > pool.configured_size_bytes,
        )
    }

    pub(super) fn is_alloc_freed(
        &self,
        context: &SlabReleaseContext,
        offset: u64,
    ) -> Result<bool, String> {
        let pending = self
            .pending_releases
            .get(&context.token)
            .ok_or_else(|| format!("invalid slab release token {}", context.token))?;
        self.validate_release_context(context, pending)?;
        Ok(!pending.active_allocations.contains_key(&offset))
    }

    pub(super) fn all_allocs_freed(&self, context: &SlabReleaseContext) -> Result<bool, String> {
        let pending = self
            .pending_releases
            .get(&context.token)
            .ok_or_else(|| format!("invalid slab release token {}", context.token))?;
        self.validate_release_context(context, pending)?;
        Ok(pending.active_allocations.is_empty())
    }

    pub(super) fn process_alloc_for_release<F>(
        &self,
        context: &SlabReleaseContext,
        offset: u64,
        callback: F,
    ) -> Result<(), String>
    where
        F: FnOnce(u64),
    {
        let pending = self
            .pending_releases
            .get(&context.token)
            .ok_or_else(|| format!("invalid slab release token {}", context.token))?;
        self.validate_release_context(context, pending)?;
        if pending.active_allocations.contains_key(&offset) {
            callback(offset);
        }
        Ok(())
    }

    pub(super) fn validate_release_context(
        &self,
        context: &SlabReleaseContext,
        pending: &PendingSlabRelease,
    ) -> Result<(), String> {
        if context.pool_id != pending.pool_id
            || context.slab_index != pending.slab_index
            || context.victim_class_size != pending.victim_class_size
        {
            return Err("slab release context does not match pending release".to_string());
        }
        Ok(())
    }

    pub(super) fn allocation_class_id(
        &self,
        pool_id: PoolId,
        requested_size: u64,
    ) -> Result<ClassId, String> {
        if !self.pools.contains_key(&pool_id) {
            return Err(format!("invalid pool id {pool_id}"));
        }
        let class_size = cachelib_class_size(&self.class_sizes, requested_size)
            .ok_or_else(|| format!("invalid allocation size {requested_size}"))?;
        self.class_sizes
            .iter()
            .position(|size| *size == class_size)
            .map(|idx| idx as ClassId)
            .ok_or_else(|| format!("class size {class_size} not found"))
    }

    pub(super) fn alloc_size_by_class_id(
        &self,
        pool_id: PoolId,
        class_id: ClassId,
    ) -> Result<u64, String> {
        if !self.pools.contains_key(&pool_id) {
            return Err(format!("invalid pool id {pool_id}"));
        }
        self.class_sizes
            .get(class_id as usize)
            .copied()
            .ok_or_else(|| format!("invalid class id {class_id}"))
    }

    pub(super) fn alloc_info(&self, offset: u64) -> Result<CachelibAllocInfo, String> {
        let allocation = self
            .allocations
            .get(&offset)
            .ok_or_else(|| format!("allocation at offset {offset} not found"))?;
        let class_id = self
            .class_sizes
            .iter()
            .position(|size| *size == allocation.class_size)
            .ok_or_else(|| format!("class size {} not found", allocation.class_size))?
            as ClassId;
        Ok(CachelibAllocInfo {
            pool_id: allocation.pool_id,
            class_id,
            alloc_size: allocation.class_size,
        })
    }

    pub(super) fn current_alloc_size_for_pool(&self, pool_id: PoolId) -> u64 {
        self.allocations
            .values()
            .filter(|alloc| alloc.pool_id == pool_id)
            .map(|alloc| alloc.class_size)
            .sum()
    }

    pub(super) fn pool_advised_size(&self, pool_id: PoolId) -> Option<u64> {
        self.pools.get(&pool_id).map(|p| p.get_pool_advised_size())
    }

    pub(super) fn pool_usable_size(&self, pool_id: PoolId) -> Option<u64> {
        self.pools.get(&pool_id).map(|p| p.get_pool_usable_size())
    }

    pub(super) fn pool_unallocated_slab_memory(&self, pool_id: PoolId) -> Option<u64> {
        self.pools.get(&pool_id).map(|p| p.get_unallocated_slab_memory())
    }

    pub(super) fn advised_memory_size(&self) -> u64 {
        self.pools.values().map(|p| p.get_pool_advised_size()).sum()
    }
}

impl CachelibPoolState {
    pub(super) fn current_used_size(&self) -> u64 {
        self.reserved_slabs.len() as u64 * CACHELIB_SLAB_SIZE
    }

    pub(super) fn get_pool_advised_size(&self) -> u64 {
        self.advised_slabs * CACHELIB_SLAB_SIZE
    }

    pub(super) fn get_pool_usable_size(&self) -> u64 {
        let advised = self.get_pool_advised_size();
        if self.configured_size_bytes <= advised {
            0
        } else {
            self.configured_size_bytes - advised
        }
    }

    pub(super) fn get_unallocated_slab_memory(&self) -> u64 {
        let total = self.current_used_size() + self.get_pool_advised_size();
        if total >= self.configured_size_bytes {
            0
        } else {
            self.configured_size_bytes - total
        }
    }

    pub(super) fn releasable_slabs(&self) -> Vec<u32> {
        let active_slabs = self
            .class_slabs
            .values()
            .flat_map(|slabs| slabs.iter().map(|slab| slab.slab_index))
            .collect::<Vec<_>>();
        self.reserved_slabs
            .iter()
            .copied()
            .filter(|slab| !active_slabs.contains(slab))
            .collect::<Vec<_>>()
    }
}

pub(super) fn count_unassigned_reserved_slabs(pool: &CachelibPoolState) -> usize {
    let active = pool
        .class_slabs
        .values()
        .map(|slabs| slabs.len())
        .sum::<usize>();
    pool.reserved_slabs.len().saturating_sub(active)
}

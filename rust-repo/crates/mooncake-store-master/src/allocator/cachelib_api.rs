use super::*;

impl SegmentAllocator {
    pub fn add_pool(
        &mut self,
        segment_id: &Uuid,
        name: impl Into<String>,
        size_bytes: u64,
    ) -> Result<PoolId, String> {
        self.add_pool_with_options(segment_id, name, size_bytes, false)
    }

    /// Create a pool with provisionability check.
    /// 创建池并检查可提供性。
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

    /// Grow a pool's configured size.
    /// 增加池的配置大小。
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

    /// Shrink a pool's configured size.
    /// 减少池的配置大小。
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

    /// Resize pools: move capacity from source pool to destination pool.
    /// 调整池大小：将容量从源池转移到目标池。
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

    /// Get all pool IDs for a segment.
    /// 获取 segment 的所有池 ID。
    pub fn pool_ids(&self, segment_id: &Uuid) -> Option<Vec<PoolId>> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.pool_ids()),
        }
    }

    /// Get pool name by pool ID.
    /// 通过池 ID 获取池名称。
    pub fn pool_name(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<String> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.pools.get(&pool_id).map(|pool| pool.name.clone())
            }
        }
    }

    /// Get pool ID by name.
    /// 通过名称获取池 ID。
    pub fn pool_id(&self, segment_id: &Uuid, name: &str) -> Option<PoolId> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_names.get(name).copied(),
        }
    }

    /// Get total memory capacity for a segment.
    /// 获取 segment 的总内存容量。
    pub fn memory_size(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.total_capacity_bytes),
        }
    }

    /// Check if all slabs in a segment are allocated.
    /// 检查 segment 中所有 slab 是否已分配。
    pub fn all_slabs_allocated(&self, segment_id: &Uuid) -> Option<bool> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.bytes_unreserved() == 0),
        }
    }

    /// Check if all slabs for a pool are allocated.
    /// 检查池的所有 slab 是否已分配。
    pub fn all_slabs_allocated_for_pool(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<bool> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => {
                cachelib.all_slabs_allocated_for_pool(pool_id).ok()
            }
        }
    }

    /// Check if a pool exceeds its configured size.
    /// 检查池是否超过其配置大小。
    pub fn pool_is_over_limit(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<bool> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_over_limit(pool_id).ok(),
        }
    }

    /// Get all pools that exceed their configured limits.
    /// 获取所有超过配置限制的池。
    pub fn pools_over_limit(&self, segment_id: &Uuid) -> Option<Vec<PoolId>> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.pools_over_limit()),
        }
    }

    /// Get unreserved bytes for a segment.
    /// 获取 segment 的未预留字节数。
    pub fn bytes_unreserved(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.bytes_unreserved()),
        }
    }

    /// Get advised pool size.
    /// 获取建议的池大小。
    pub fn pool_advised_size(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_advised_size(pool_id),
        }
    }

    /// Get usable pool size (configured - advised).
    /// 获取可用的池大小（配置大小 - 建议大小）。
    pub fn pool_usable_size(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_usable_size(pool_id),
        }
    }

    /// Get current allocation size for a pool.
    /// 获取池的当前分配大小。
    pub fn pool_current_alloc_size(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => {
                Some(cachelib.current_alloc_size_for_pool(pool_id))
            }
        }
    }

    /// Get unallocated slab memory for a pool.
    /// 获取池的未分配 slab 内存。
    pub fn pool_unallocated_slab_memory(&self, segment_id: &Uuid, pool_id: PoolId) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => cachelib.pool_unallocated_slab_memory(pool_id),
        }
    }

    /// Get advised memory size for a segment (sum of all pool advised sizes).
    /// 获取 segment 的建议内存大小（所有池建议大小之和）。
    pub fn advised_memory_size(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.advised_memory_size()),
        }
    }

    // --- Slab Statistics / Slab 统计 ---

    /// Number of slab resize operations completed.
    /// 已完成的 slab resize 操作数。
    pub fn n_slab_resize(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.n_slab_resize),
        }
    }

    /// Number of slab rebalance operations completed.
    /// 已完成的 slab rebalance 操作数。
    pub fn n_slab_rebalance(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.n_slab_rebalance),
        }
    }

    /// Number of slab release operations aborted.
    /// 已中止的 slab 释放操作数。
    pub fn n_slab_release_aborted(&self, segment_id: &Uuid) -> Option<u64> {
        let state = self.segments.get(segment_id)?;
        match &state.layout {
            SegmentLayout::Offset(_) => None,
            SegmentLayout::Cachelib(cachelib) => Some(cachelib.n_slab_release_aborted),
        }
    }

    // --- Slab Release Protocol Methods / Slab 释放协议方法 ---

    /// Check if an allocation has been freed within a slab release.
    /// 检查 slab 释放中的分配是否已释放。
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

    /// Check if all allocations in a slab release are freed.
    /// 检查 slab 释放中所有分配是否已释放。
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

    /// Process an allocation within a slab release.
    /// 处理 slab 释放中的分配。
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

    /// Get the allocation class ID for a given pool and size.
    /// 获取给定池和大小的分配 class ID。
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

    /// Get the allocation size by class ID.
    /// 通过 class ID 获取分配大小。
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

    /// Get allocation metadata for an offset.
    /// 获取偏移量的分配元数据。
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

    /// Iterate over all allocations in a segment.
    /// 遍历 segment 中的所有分配。
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

    /// Start a slab release operation.
    /// 开始 slab 释放操作。
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

    /// Start slab release with hint offset and abort callback.
    /// 带 hint 偏移量和终止回调开始 slab 释放。
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

    /// Complete a slab release.
    /// 完成 slab 释放。
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

    /// Abort a slab release.
    /// 中止 slab 释放。
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

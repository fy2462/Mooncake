use super::*;

impl CachelibSegmentState {
    // --- Pool Management / 池管理 ---

    /// Create a new pool with the given name and size.
    /// 用给定名称和大小创建新池。
    pub(crate) fn create_pool(&mut self, name: String, size_bytes: u64) -> Result<PoolId, String> {
        self.create_pool_with_options(name, size_bytes, false)
    }

    /// Create a pool with options (duplicate check, capacity validation, provisionability check).
    /// 带选项（重复检查、容量验证、可提供性检查）创建池。
    pub(crate) fn create_pool_with_options(
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
        // Ensure at least one slab per size class can be provisioned
        // 确保每个 size class 至少能分配一个 slab
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

    /// Get sorted list of all pool IDs.
    /// 获取所有池 ID 的排序列表。
    pub(crate) fn pool_ids(&self) -> Vec<PoolId> {
        let mut ids = self.pools.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Grow a pool's configured size.
    /// 增加池的配置大小。
    pub(crate) fn grow_pool(&mut self, pool_id: PoolId, size_bytes: u64) -> Result<bool, String> {
        if self.bytes_unreserved() < size_bytes {
            return Ok(false);
        }
        let Some(pool) = self.pools.get_mut(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        pool.configured_size_bytes = pool.configured_size_bytes.saturating_add(size_bytes);
        Ok(true)
    }

    /// Shrink a pool's configured size.
    /// 减少池的配置大小。
    pub(crate) fn shrink_pool(&mut self, pool_id: PoolId, size_bytes: u64) -> Result<bool, String> {
        let Some(pool) = self.pools.get_mut(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        if pool.configured_size_bytes < size_bytes {
            return Ok(false);
        }
        pool.configured_size_bytes -= size_bytes;
        Ok(true)
    }

    /// Resize pools: move capacity from src_pool to dest_pool.
    /// 调整池大小：将容量从 src_pool 转移到 dest_pool。
    pub(crate) fn resize_pools(
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

    /// Compute unreserved bytes: total_capacity - sum(all pool configured sizes).
    /// 计算未预留字节数：总容量 - 所有池配置大小之和。
    pub(crate) fn bytes_unreserved(&self) -> u64 {
        self.total_capacity_bytes.saturating_sub(
            self.pools
                .values()
                .map(|pool| pool.configured_size_bytes)
                .sum::<u64>(),
        )
    }

    /// Check if a pool is over its configured size limit.
    /// 检查池是否超过其配置大小限制。
    pub(crate) fn pool_over_limit(&self, pool_id: PoolId) -> Result<bool, String> {
        let Some(pool) = self.pools.get(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        Ok(pool.current_used_size() > pool.configured_size_bytes)
    }

    /// Get all pool IDs that are over their configured limits.
    /// 获取所有超过配置限制的池 ID。
    pub(crate) fn pools_over_limit(&self) -> Vec<PoolId> {
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

    /// Check if all slabs for a pool are already allocated (cannot provision more).
    /// 检查池的所有 slab 是否已全部分配（无法再分配更多）。
    pub(crate) fn all_slabs_allocated_for_pool(&self, pool_id: PoolId) -> Result<bool, String> {
        let Some(pool) = self.pools.get(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        Ok(
            pool.current_used_size().saturating_add(CACHELIB_SLAB_SIZE)
                > pool.configured_size_bytes,
        )
    }

    // --- Slab Release Query Methods / Slab 释放查询方法 ---

    /// Check if a specific allocation has been freed within a pending release.
    /// 检查特定分配在待处理的释放中是否已被释放。
    pub(crate) fn is_alloc_freed(
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

    /// Check if all allocations in a pending release have been freed.
    /// 检查待处理释放中的所有分配是否已全部释放。
    pub(crate) fn all_allocs_freed(&self, context: &SlabReleaseContext) -> Result<bool, String> {
        let pending = self
            .pending_releases
            .get(&context.token)
            .ok_or_else(|| format!("invalid slab release token {}", context.token))?;
        self.validate_release_context(context, pending)?;
        Ok(pending.active_allocations.is_empty())
    }

    /// Process an allocation for release: call callback if the offset is still active.
    /// 处理释放中的分配：若偏移量仍活跃则调用回调。
    pub(crate) fn process_alloc_for_release<F>(
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

    /// Validate that a release context matches the pending release state.
    /// 验证释放上下文是否与待处理释放状态匹配。
    pub(crate) fn validate_release_context(
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

    // --- Allocation Class Queries / 分配类查询 ---

    /// Get the class ID for a given pool and requested size.
    /// 获取给定池和请求尺寸的 class ID。
    pub(crate) fn allocation_class_id(
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

    /// Get the allocation size for a given pool and class ID.
    /// 获取给定池和 class ID 的分配大小。
    pub(crate) fn alloc_size_by_class_id(
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

    /// Get allocation metadata for a given offset.
    /// 获取给定偏移量的分配元数据。
    pub(crate) fn alloc_info(&self, offset: u64) -> Result<CachelibAllocInfo, String> {
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

    // --- Pool Statistics / 池统计 ---

    /// Get current total allocated size for a pool.
    /// 获取池当前的已分配总大小。
    pub(crate) fn current_alloc_size_for_pool(&self, pool_id: PoolId) -> u64 {
        self.allocations
            .values()
            .filter(|alloc| alloc.pool_id == pool_id)
            .map(|alloc| alloc.class_size)
            .sum()
    }

    /// Get advised memory size for a pool.
    /// 获取池的建议内存大小。
    pub(crate) fn pool_advised_size(&self, pool_id: PoolId) -> Option<u64> {
        self.pools.get(&pool_id).map(|p| p.get_pool_advised_size())
    }

    /// Get usable size for a pool (configured - advised).
    /// 获取池的可用大小（配置大小 - 建议大小）。
    pub(crate) fn pool_usable_size(&self, pool_id: PoolId) -> Option<u64> {
        self.pools.get(&pool_id).map(|p| p.get_pool_usable_size())
    }

    /// Get unallocated slab memory for a pool.
    /// 获取池的未分配 slab 内存量。
    pub(crate) fn pool_unallocated_slab_memory(&self, pool_id: PoolId) -> Option<u64> {
        self.pools
            .get(&pool_id)
            .map(|p| p.get_unallocated_slab_memory())
    }

    /// Get total advised memory size across all pools.
    /// 获取所有池的建议内存总大小。
    pub(crate) fn advised_memory_size(&self) -> u64 {
        self.pools.values().map(|p| p.get_pool_advised_size()).sum()
    }
}

// =============================================================================
// CachelibPoolState Methods / CachelibPoolState 方法
// =============================================================================

impl CachelibPoolState {
    /// Get current actual used size: reserved_slabs count * SLAB_SIZE.
    /// 获取当前实际使用大小：reserved_slabs 数量 * SLAB_SIZE。
    pub(crate) fn current_used_size(&self) -> u64 {
        self.reserved_slabs.len() as u64 * CACHELIB_SLAB_SIZE
    }

    /// Get advised size: advised_slabs * SLAB_SIZE.
    /// 获取建议大小：advised_slabs * SLAB_SIZE。
    pub(crate) fn get_pool_advised_size(&self) -> u64 {
        self.advised_slabs * CACHELIB_SLAB_SIZE
    }

    /// Get usable size: configured_size - advised_size.
    /// 获取可用大小：配置大小 - 建议大小。
    pub(crate) fn get_pool_usable_size(&self) -> u64 {
        let advised = self.get_pool_advised_size();
        self.configured_size_bytes.saturating_sub(advised)
    }

    /// Get unallocated slab memory: configured - (current + advised).
    /// 获取未分配的 slab 内存：配置大小 - (当前使用 + 建议)。
    pub(crate) fn get_unallocated_slab_memory(&self) -> u64 {
        let total = self.current_used_size() + self.get_pool_advised_size();
        self.configured_size_bytes.saturating_sub(total)
    }

    /// Get slabs that are reserved but not assigned to any class (releasable).
    /// 获取已预留但未分配给任何 class 的 slab（可释放的）。
    pub(crate) fn releasable_slabs(&self) -> Vec<u32> {
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

use super::*;

impl CachelibSegmentState {
    // --- Slab Release (Two-Phase Protocol) / Slab 释放（两阶段协议） ---

    /// Start a slab release operation.
    /// 开始 slab 释放操作。
    ///
    /// Two modes / 两种模式:
    /// - Resize (no victim_class_size): release an empty reserved slab back to unreserved.
    ///   Resize（未指定 victim_class_size）：将空的已预留 slab 归还给未预留池。
    /// - Rebalance (with victim + receiver): move a slab between size classes.
    ///   Rebalance（指定 victim + receiver）：在 size class 之间迁移 slab。
    pub(crate) fn start_slab_release(
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

    /// Start slab release with additional options (hint and abort check).
    /// 带额外选项（提示和终止检查）开始 slab 释放。
    ///
    /// - hint_offset: prefer releasing the slab containing this offset.
    ///   hint_offset：优先释放包含此偏移量的 slab。
    /// - should_abort: callback to check if release should be cancelled mid-operation.
    ///   should_abort：在操作中途检查是否应取消释放的回调。
    pub(crate) fn start_slab_release_with_options<F>(
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
        // Validate parameters
        if receiver_class_size.is_some() && mode != SlabReleaseMode::Rebalance {
            return Err("receiver_class_size requires rebalance mode".to_string());
        }
        if victim_class_size.is_none() && mode != SlabReleaseMode::Resize {
            return Err("victim_class_size can be omitted only in resize mode".to_string());
        }

        // Resize mode with no victim: release an empty reserved slab
        // Resize 模式且无 victim：释放一个空的已预留 slab
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
            // Remove from reserved, return to unreserved pool
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
                is_released: true, // immediately complete — no active allocs
            });
        }

        let victim_class_size = victim_class_size.expect("checked above");

        // Resolve hinted slab index if provided
        // 如果提供了提示偏移量，解析提示的 slab 索引
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

        // Select the victim slab: prefer the hint, then the most-allocated slab
        // 选择受害 slab：优先使用提示，然后选择分配最多的 slab
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
                // Choose slab with fewest free slots (most allocated) to minimize disruption
                // 选择空闲 slot 最少的 slab（分配最多的），以最小化搬迁影响
                .min_by_key(|slab| slab.capacity.saturating_sub(slab.free_slots.len() as u32))
                .ok_or_else(|| "no releasable slab available in victim class".to_string())?;
            let token = self.next_release_token;
            self.next_release_token = self.next_release_token.saturating_add(1);
            victim.pending_release_token = Some(token);
            (token, victim.slab_index)
        };

        // Collect active allocations on the victim slab
        // 收集受害 slab 上的活跃分配
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

        // Check abort condition after collecting allocations
        // 在收集分配后检查终止条件
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

    /// Complete a slab release operation.
    /// 完成 slab 释放操作。
    ///
    /// Verifies all active allocations have been freed, then:
    /// - Resize: returns the slab to the unreserved pool.
    ///   Resize：将 slab 归还给未预留池。
    /// - Rebalance: reassigns the slab to the receiver size class.
    ///   Rebalance：将 slab 重新分配给接收方 size class。
    pub(crate) fn complete_slab_release(
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

        // Guard: all active allocations must be freed first
        // 保护：所有活跃分配必须先被释放
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

        // Remove the victim slab from its class
        // 从 class 中移除受害 slab
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

        // Finalize based on mode
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
                    // Create new slab state for the receiver class
                    // 为接收方 class 创建新的 slab 状态
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

    /// Iterate over all allocation slots in the segment.
    /// 遍历 segment 中的所有分配槽位。
    ///
    /// Calls `callback` for each slot (both allocated and free).
    /// Skips slabs with pending releases.
    /// Returns the count of skipped slabs.
    /// 对每个槽位（已分配和空闲均包括）调用 `callback`。
    /// 跳过正在释放的 slab。返回跳过的 slab 计数。
    pub(crate) fn for_each_allocation<F>(&self, callback: &mut F) -> Result<u64, String>
    where
        F: FnMut(CachelibAllocationVisit) -> bool,
    {
        let mut skipped = 0;
        // Iterate pools → class_sizes → slabs → slots
        // 遍历 池 → class_size → slab → slot
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

    /// Abort a slab release operation (restore slab to normal state).
    /// 中止 slab 释放操作（将 slab 恢复到正常状态）。
    ///
    /// Removes the pending release, clears the pending_release_token on the slab,
    /// and returns freed slots back to the slab's free list.
    /// 移除待处理的释放，清除 slab 上的 pending_release_token，
    /// 并将已释放的 slot 归还到 slab 的空闲列表。
    pub(crate) fn abort_slab_release(
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
        // Restore slab and return freed slots
        // 恢复 slab 并归还已释放的 slot
        slab.pending_release_token = None;
        slab.free_slots.extend(pending.freed_slots);
        slab.free_slots.sort_unstable_by(|a, b| b.cmp(a));
        slab.free_slots.dedup_by(|a, b| a == b);
        Ok(())
    }
}

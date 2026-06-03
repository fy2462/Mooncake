// =============================================================================
// Cachelib-like Slab Allocator — 类 Cachelib 的 Slab 分配器实现
// =============================================================================
// Implements a slab-based memory allocator inspired by Facebook's CacheLib.
// 实现了受 Facebook CacheLib 启发的基于 slab 的内存分配器。
//
// Architecture / 架构概述:
// ┌──────────────────────────────────────────────────┐
// │  Segment (memory region)                         │
// │  ┌──────┬──────┬──────┬──────┬──────┬──────┐     │
// │  │ Slab0│ Slab1│ Slab2│ ...  │ SlabN│      │     │
// │  └──────┴──────┴──────┴──────┴──────┴──────┘     │
// │  Each slab = CACHELIB_SLAB_SIZE (16 MiB)          │
// │  每个 slab = 16 MiB                               │
// └──────────────────────────────────────────────────┘
//
// Key concepts / 关键概念:
// 1. Size Classes: pre-computed allocation sizes (72 → ~16 MiB, 1.25x growth)
//    Size class：预计算的分配尺寸（72 → ~16 MiB，1.25 倍增长）
// 2. Slabs: fixed-size memory chunks (16 MiB each) assigned to a specific size class
//    Slab：分配给特定 size class 的固定大小内存块（每个 16 MiB）
// 3. Pools: logical groups of slabs with configurable size limits
//    Pool：具有可配置大小限制的 slab 逻辑分组
// 4. Slab Release: two-phase protocol for safely freeing/repurposing slabs
//    Slab 释放：安全释放/重分配 slab 的两阶段协议
// 5. Slots: equal-sized units within a slab (slab_size / class_size)
//    Slot：slab 内等大小的单元（slab_size / class_size）

use std::collections::HashMap;

use super::types::{
    CachelibAllocInfo, CachelibAllocationVisit, ClassId, PoolId, SlabReleaseContext,
    SlabReleaseMode, CACHELIB_ALIGNMENT, CACHELIB_CLASS_MIN_SIZE, CACHELIB_MAX_ALLOC_SIZE,
    CACHELIB_MIN_ALLOC_SIZE, CACHELIB_SLAB_SIZE,
};

// =============================================================================
// Core Data Structures / 核心数据结构
// =============================================================================

/// Metadata for a single allocation within the Cachelib-like allocator.
/// Cachelib 类分配器中单次分配的元数据。
/// Records which pool, class, slab, and slot this allocation occupies.
/// 记录该分配占用的池、size class、slab 和 slot。
#[derive(Debug, Clone)]
pub(super) struct CachelibAllocation {
    pub(super) pool_id: PoolId,
    pub(super) class_size: u64,
    pub(super) slab_index: u32,
    pub(super) slot_index: u32,
}

/// State of one slab within a size class.
/// 一个 size class 内某个 slab 的状态。
/// Tracks capacity (number of slots), free slots, and whether a release is pending.
/// 跟踪容量（slot 数量）、空闲 slot 以及是否正在进行释放。
#[derive(Debug, Clone)]
pub(super) struct SlabClassState {
    /// Global slab index within the segment.
    /// segment 内的全局 slab 索引。
    pub(super) slab_index: u32,
    /// Maximum number of slots this slab can hold.
    /// 该 slab 能容纳的最大 slot 数量。
    pub(super) capacity: u32,
    /// Stack of free slot indices (LIFO for cache locality).
    /// 空闲 slot 索引的栈（LIFO，有利于缓存局部性）。
    pub(super) free_slots: Vec<u32>,
    /// Non-None if a slab release is pending, holds the release token.
    /// 若 slab 释放正在进行中则非 None，包含释放令牌。
    pub(super) pending_release_token: Option<u64>,
}

/// State for a logical pool of slabs.
/// 逻辑 slab 池的状态。
/// Pools allow partitioning memory within a segment for different purposes.
/// 池允许在 segment 内为不同目的划分内存。
#[derive(Debug, Clone)]
pub(super) struct CachelibPoolState {
    /// Human-readable pool name.
    /// 人类可读的池名称。
    pub(super) name: String,
    /// Configured maximum size in bytes.
    /// 配置的最大字节大小。
    pub(super) configured_size_bytes: u64,
    /// Slab indices reserved for this pool.
    /// 为该池保留的 slab 索引列表。
    pub(super) reserved_slabs: Vec<u32>,
    /// Slab state grouped by class size.
    /// 按 class size 分组的 slab 状态。
    pub(super) class_slabs: HashMap<u64, Vec<SlabClassState>>,
    /// Recommended number of slabs for this pool.
    /// 为该池推荐的 slab 数量。
    pub(super) advised_slabs: u64,
}

/// Top-level state for the Cachelib-like allocator within a single segment.
/// 单个 segment 内 Cachelib 类分配器的顶层状态。
///
/// Manages: pools, slabs, allocations, pending releases, and statistics.
/// 管理：池、slab、分配、待处理释放和统计信息。
#[derive(Debug, Clone)]
pub(super) struct CachelibSegmentState {
    /// Total unreserved capacity in bytes.
    /// 未预留的总容量（字节）。
    pub(super) total_capacity_bytes: u64,
    /// Ordered list of size classes.
    /// 有序的 size class 列表。
    pub(super) class_sizes: Vec<u64>,
    /// Pools by PoolId.
    /// 按 PoolId 索引的池。
    pub(super) pools: HashMap<PoolId, CachelibPoolState>,
    /// Pool name → PoolId lookup.
    /// 池名称 → PoolId 的查找映射。
    pub(super) pool_names: HashMap<String, PoolId>,
    /// Monotonically increasing pool ID counter.
    /// 单调递增的池 ID 计数器。
    pub(super) next_pool_id: PoolId,
    /// Pool ID for the default main pool.
    /// 默认主池的 Pool ID。
    pub(super) default_pool_id: PoolId,
    /// Stack of unassigned slab indices (LIFO: pop from end for newest slabs).
    /// 未分配 slab 索引的栈（LIFO：从末尾弹出最新 slab）。
    pub(super) unreserved_slab_indices: Vec<u32>,
    /// All active allocations keyed by offset.
    /// 所有活跃分配，以偏移量为 key。
    pub(super) allocations: HashMap<u64, CachelibAllocation>,
    /// Pending slab releases by token.
    /// 按令牌索引的待处理 slab 释放。
    pub(super) pending_releases: HashMap<u64, PendingSlabRelease>,
    /// Monotonically increasing release token counter.
    /// 单调递增的释放令牌计数器。
    pub(super) next_release_token: u64,
    /// Count of completed slab resize operations.
    /// 已完成的 slab 调整大小操作计数。
    pub(super) n_slab_resize: u64,
    /// Count of completed slab rebalance operations.
    /// 已完成的 slab 重平衡操作计数。
    pub(super) n_slab_rebalance: u64,
    /// Count of aborted slab release operations.
    /// 已中止的 slab 释放操作计数。
    pub(super) n_slab_release_aborted: u64,
}

/// Pending slab release state — tracks the two-phase release protocol.
/// 待处理的 slab 释放状态 —— 跟踪两阶段释放协议。
///
/// Phase 1 (start_slab_release): mark slab as releasing, collect active allocations.
/// Phase 2 (complete_slab_release): after all allocations are freed, finalize release.
/// 阶段 1（start_slab_release）：标记 slab 为释放中，收集活跃分配。
/// 阶段 2（complete_slab_release）：等待所有分配释放后，完成释放。
#[derive(Debug, Clone)]
pub(super) struct PendingSlabRelease {
    pub(super) pool_id: PoolId,
    pub(super) victim_class_size: Option<u64>,
    pub(super) receiver_class_size: Option<u64>,
    pub(super) slab_index: u32,
    pub(super) mode: SlabReleaseMode,
    /// Active allocations that need to be freed before release can complete.
    /// 释放完成前需要被释放的活跃分配（offset → slot_index）。
    pub(super) active_allocations: HashMap<u64, u32>,
    /// Slots that have been freed since the release started.
    /// 自释放开始以来已被释放的 slot 列表。
    pub(super) freed_slots: Vec<u32>,
}

// =============================================================================
// Size Class Generation / Size Class 生成
// =============================================================================

/// Find the smallest size class that can fit the requested size.
/// 找到能容纳请求大小的最小 size class。
///
/// Pads the request to at least CACHELIB_MIN_ALLOC_SIZE and aligns to CACHELIB_ALIGNMENT,
/// then finds the first class_size >= padded request.
/// 将请求向上取整到至少 CACHELIB_MIN_ALLOC_SIZE 并对齐到 CACHELIB_ALIGNMENT，
/// 然后找到第一个 >= 调整后请求的 class_size。
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

/// Allocate a slot from the Cachelib-like allocator.
/// 从 Cachelib 类分配器中分配一个 slot。
///
/// Algorithm / 算法:
/// 1. Determine the appropriate size class for the request.
///    确定请求对应的 size class。
/// 2. Try to find a free slot in an existing slab for that class.
///    尝试在该 class 的现有 slab 中找到空闲 slot。
/// 3. If no free slots, check for a releasable (empty but reserved) slab to reuse.
///    若无空闲 slot，检查是否有可释放（空但已预留）的 slab 可复用。
/// 4. If none, provision a new slab from the unreserved pool.
///    若无，从未预留池中分配一个新的 slab。
/// 5. Create a CachelibAllocation record and return (offset, class_size).
///    创建 CachelibAllocation 记录并返回 (offset, class_size)。
///
/// Returns None if no space is available.
/// 若无可用空间则返回 None。
pub(super) fn allocate_cachelib(
    state: &mut CachelibSegmentState,
    requested_size: u64,
) -> Option<(u64, u64)> {
    // Step 1: determine size class
    let class_size = cachelib_class_size(&state.class_sizes, requested_size)?;
    let default_pool_id = state.default_pool_id;
    let pool = state.pools.get_mut(&default_pool_id)?;

    // Step 2: try existing slabs for free slot
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

    // Step 3: find a releasable (empty, reserved) slab to repurpose
    // Step 4: or provision a new slab from unreserved_pool
    let slab_index = if let Some(existing) = pool.releasable_slabs().into_iter().min() {
        // Reuse an empty reserved slab (repurpose for this class)
        // 复用一个空的已预留 slab（重新用于此 class）
        existing
    } else {
        // Provision a new slab — check pool limits
        // 分配新 slab —— 检查池限制
        let can_provision =
            pool.current_used_size() + CACHELIB_SLAB_SIZE <= pool.configured_size_bytes;
        if !can_provision {
            return None;
        }
        let slab_index = state.unreserved_slab_indices.pop()?;
        pool.reserved_slabs.push(slab_index);
        // Keep reserved_slabs sorted descending for efficient pop
        // 保持 reserved_slabs 降序排列以便高效弹出
        pool.reserved_slabs.sort_unstable_by(|a, b| b.cmp(a));
        slab_index
    };

    // Step 5: create the slab state and allocate first slot
    // 创建 slab 状态并分配第一个 slot
    let slabs = pool.class_slabs.entry(class_size).or_default();
    let capacity = (CACHELIB_SLAB_SIZE / class_size) as u32;
    if capacity == 0 {
        return None;
    }
    // Pre-fill free_slots stack (0..capacity), reversed for LIFO pop
    // 预填充 free_slots 栈（0..capacity），反转以实现 LIFO 弹出
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

/// Release a Cachelib allocation at the given offset.
/// 释放给定偏移量处的 Cachelib 分配。
///
/// Behavior depends on slab state:
/// - If slab has a pending release: move to freed_slots (for completion tracking)
///   若 slab 有待处理的释放：移入 freed_slots（用于完成追踪）
/// - Otherwise: return slot to free_slots; if slab becomes fully empty, deallocate it
///   否则：归还 slot 到 free_slots；若 slab 变为全空，则回收之
///
/// Returns the class_size of the freed allocation.
/// 返回已释放分配的 class_size。
pub(super) fn release_cachelib(state: &mut CachelibSegmentState, offset: u64) -> Option<u64> {
    let allocation = state.allocations.remove(&offset)?;
    let pool = state.pools.get_mut(&allocation.pool_id)?;
    let slabs = pool.class_slabs.get_mut(&allocation.class_size)?;
    let slab_pos = slabs
        .iter()
        .position(|slab| slab.slab_index == allocation.slab_index)?;
    let slab = &mut slabs[slab_pos];

    if let Some(token) = slab.pending_release_token {
        // Slab is being released — track freed slots for completion
        // Slab 正在释放中 —— 跟踪已释放 slot 以完成释放
        let pending = state.pending_releases.get_mut(&token)?;
        pending.active_allocations.remove(&offset);
        pending.freed_slots.push(allocation.slot_index);
    } else {
        // Normal release: return slot to free list
        // 正常释放：归还 slot 到空闲列表
        slab.free_slots.push(allocation.slot_index);
        // If slab is now entirely free, remove it (slab can be reused)
        // 若 slab 现在完全空闲，则移除之（slab 可被复用）
        if slab.free_slots.len() as u32 == slab.capacity {
            let _slab = slabs.remove(slab_pos);
        }
    }
    // Clean up empty class_slabs entries
    // 清理空的 class_slabs 条目
    if slabs.is_empty() {
        pool.class_slabs.remove(&allocation.class_size);
    }
    Some(allocation.class_size)
}

// =============================================================================
// Utilities / 工具函数
// =============================================================================

/// Align a value up to the nearest multiple of `alignment`.
/// 将值向上对齐到 `alignment` 的最近倍数。
///
/// Examples: align_up(73, 8) → 80, align_up(64, 8) → 64.
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

/// Generate the ordered list of Cachelib size classes.
/// 生成有序的 Cachelib size class 列表。
///
/// Algorithm / 算法:
/// - Start at CACHELIB_CLASS_MIN_SIZE (72 bytes).
///   从 CACHELIB_CLASS_MIN_SIZE（72 字节）开始。
/// - Each subsequent size = ceil(prev * 1.25) aligned up to 8 bytes.
///   每个后续尺寸 = ceil(前一个 * 1.25) 对齐到 8 字节。
/// - Stop when next size exceeds CACHELIB_MAX_ALLOC_SIZE or slab_size/class_size <= 1.
///   当下一个尺寸超过 CACHELIB_MAX_ALLOC_SIZE 或 slab_size/class_size <= 1 时停止。
/// - Append the aligned MAX_ALLOC_SIZE as the final class.
///   追加对齐的 MAX_ALLOC_SIZE 作为最后一个 class。
///
/// This 1.25x growth factor balances internal fragmentation (~12.5%) against
/// the number of size classes (and thus memory overhead).
/// 1.25 倍增长因子在内部碎片（约 12.5%）与 size class 数量（以及内存开销）之间取得平衡。
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
            // Handle overflow or rounding edge case
            // 处理溢出或舍入边界情况
            current = current.saturating_add(CACHELIB_ALIGNMENT);
        } else {
            current = next;
        }
    }
    sizes.push(align_up(CACHELIB_MAX_ALLOC_SIZE, CACHELIB_ALIGNMENT));
    sizes
}

// =============================================================================
// CachelibSegmentState Methods / CachelibSegmentState 方法
// =============================================================================

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

    /// Start slab release with additional options (hint and abort check).
    /// 带额外选项（提示和终止检查）开始 slab 释放。
    ///
    /// - hint_offset: prefer releasing the slab containing this offset.
    ///   hint_offset：优先释放包含此偏移量的 slab。
    /// - should_abort: callback to check if release should be cancelled mid-operation.
    ///   should_abort：在操作中途检查是否应取消释放的回调。
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
    pub(super) fn for_each_allocation<F>(&self, callback: &mut F) -> Result<u64, String>
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
        // Restore slab and return freed slots
        // 恢复 slab 并归还已释放的 slot
        slab.pending_release_token = None;
        slab.free_slots.extend(pending.freed_slots);
        slab.free_slots.sort_unstable_by(|a, b| b.cmp(a));
        slab.free_slots.dedup_by(|a, b| a == b);
        Ok(())
    }

    // --- Pool Management / 池管理 ---

    /// Create a new pool with the given name and size.
    /// 用给定名称和大小创建新池。
    pub(super) fn create_pool(&mut self, name: String, size_bytes: u64) -> Result<PoolId, String> {
        self.create_pool_with_options(name, size_bytes, false)
    }

    /// Create a pool with options (duplicate check, capacity validation, provisionability check).
    /// 带选项（重复检查、容量验证、可提供性检查）创建池。
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
    pub(super) fn pool_ids(&self) -> Vec<PoolId> {
        let mut ids = self.pools.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Grow a pool's configured size.
    /// 增加池的配置大小。
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

    /// Shrink a pool's configured size.
    /// 减少池的配置大小。
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

    /// Resize pools: move capacity from src_pool to dest_pool.
    /// 调整池大小：将容量从 src_pool 转移到 dest_pool。
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

    /// Compute unreserved bytes: total_capacity - sum(all pool configured sizes).
    /// 计算未预留字节数：总容量 - 所有池配置大小之和。
    pub(super) fn bytes_unreserved(&self) -> u64 {
        self.total_capacity_bytes.saturating_sub(
            self.pools
                .values()
                .map(|pool| pool.configured_size_bytes)
                .sum::<u64>(),
        )
    }

    /// Check if a pool is over its configured size limit.
    /// 检查池是否超过其配置大小限制。
    pub(super) fn pool_over_limit(&self, pool_id: PoolId) -> Result<bool, String> {
        let Some(pool) = self.pools.get(&pool_id) else {
            return Err(format!("invalid pool id {pool_id}"));
        };
        Ok(pool.current_used_size() > pool.configured_size_bytes)
    }

    /// Get all pool IDs that are over their configured limits.
    /// 获取所有超过配置限制的池 ID。
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

    /// Check if all slabs for a pool are already allocated (cannot provision more).
    /// 检查池的所有 slab 是否已全部分配（无法再分配更多）。
    pub(super) fn all_slabs_allocated_for_pool(&self, pool_id: PoolId) -> Result<bool, String> {
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

    /// Check if all allocations in a pending release have been freed.
    /// 检查待处理释放中的所有分配是否已全部释放。
    pub(super) fn all_allocs_freed(&self, context: &SlabReleaseContext) -> Result<bool, String> {
        let pending = self
            .pending_releases
            .get(&context.token)
            .ok_or_else(|| format!("invalid slab release token {}", context.token))?;
        self.validate_release_context(context, pending)?;
        Ok(pending.active_allocations.is_empty())
    }

    /// Process an allocation for release: call callback if the offset is still active.
    /// 处理释放中的分配：若偏移量仍活跃则调用回调。
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

    /// Validate that a release context matches the pending release state.
    /// 验证释放上下文是否与待处理释放状态匹配。
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

    // --- Allocation Class Queries / 分配类查询 ---

    /// Get the class ID for a given pool and requested size.
    /// 获取给定池和请求尺寸的 class ID。
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

    /// Get the allocation size for a given pool and class ID.
    /// 获取给定池和 class ID 的分配大小。
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

    /// Get allocation metadata for a given offset.
    /// 获取给定偏移量的分配元数据。
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

    // --- Pool Statistics / 池统计 ---

    /// Get current total allocated size for a pool.
    /// 获取池当前的已分配总大小。
    pub(super) fn current_alloc_size_for_pool(&self, pool_id: PoolId) -> u64 {
        self.allocations
            .values()
            .filter(|alloc| alloc.pool_id == pool_id)
            .map(|alloc| alloc.class_size)
            .sum()
    }

    /// Get advised memory size for a pool.
    /// 获取池的建议内存大小。
    pub(super) fn pool_advised_size(&self, pool_id: PoolId) -> Option<u64> {
        self.pools.get(&pool_id).map(|p| p.get_pool_advised_size())
    }

    /// Get usable size for a pool (configured - advised).
    /// 获取池的可用大小（配置大小 - 建议大小）。
    pub(super) fn pool_usable_size(&self, pool_id: PoolId) -> Option<u64> {
        self.pools.get(&pool_id).map(|p| p.get_pool_usable_size())
    }

    /// Get unallocated slab memory for a pool.
    /// 获取池的未分配 slab 内存量。
    pub(super) fn pool_unallocated_slab_memory(&self, pool_id: PoolId) -> Option<u64> {
        self.pools
            .get(&pool_id)
            .map(|p| p.get_unallocated_slab_memory())
    }

    /// Get total advised memory size across all pools.
    /// 获取所有池的建议内存总大小。
    pub(super) fn advised_memory_size(&self) -> u64 {
        self.pools.values().map(|p| p.get_pool_advised_size()).sum()
    }
}

// =============================================================================
// CachelibPoolState Methods / CachelibPoolState 方法
// =============================================================================

impl CachelibPoolState {
    /// Get current actual used size: reserved_slabs count * SLAB_SIZE.
    /// 获取当前实际使用大小：reserved_slabs 数量 * SLAB_SIZE。
    pub(super) fn current_used_size(&self) -> u64 {
        self.reserved_slabs.len() as u64 * CACHELIB_SLAB_SIZE
    }

    /// Get advised size: advised_slabs * SLAB_SIZE.
    /// 获取建议大小：advised_slabs * SLAB_SIZE。
    pub(super) fn get_pool_advised_size(&self) -> u64 {
        self.advised_slabs * CACHELIB_SLAB_SIZE
    }

    /// Get usable size: configured_size - advised_size.
    /// 获取可用大小：配置大小 - 建议大小。
    pub(super) fn get_pool_usable_size(&self) -> u64 {
        let advised = self.get_pool_advised_size();
        self.configured_size_bytes.saturating_sub(advised)
    }

    /// Get unallocated slab memory: configured - (current + advised).
    /// 获取未分配的 slab 内存：配置大小 - (当前使用 + 建议)。
    pub(super) fn get_unallocated_slab_memory(&self) -> u64 {
        let total = self.current_used_size() + self.get_pool_advised_size();
        self.configured_size_bytes.saturating_sub(total)
    }

    /// Get slabs that are reserved but not assigned to any class (releasable).
    /// 获取已预留但未分配给任何 class 的 slab（可释放的）。
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

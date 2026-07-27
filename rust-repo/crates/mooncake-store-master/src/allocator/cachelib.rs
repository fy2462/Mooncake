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

mod cachelib_pools;
mod cachelib_slab;

use super::types::{
    CACHELIB_ALIGNMENT, CACHELIB_CLASS_MIN_SIZE, CACHELIB_MAX_ALLOC_SIZE, CACHELIB_MIN_ALLOC_SIZE,
    CACHELIB_SLAB_SIZE, CachelibAllocInfo, CachelibAllocationVisit, ClassId, PoolId,
    SlabReleaseContext, SlabReleaseMode,
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
    pub(super) requested_size: u64,
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
                requested_size,
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
            requested_size,
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
    let allocation = state.allocations.get(&offset)?.clone();
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
        state.allocations.remove(&offset)?;
        pending.active_allocations.remove(&offset);
        pending.freed_slots.push(allocation.slot_index);
    } else {
        state.allocations.remove(&offset)?;
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

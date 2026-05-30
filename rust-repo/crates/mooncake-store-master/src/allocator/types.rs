// =============================================================================
// Allocator Types — 分配器类型定义
// =============================================================================
// Defines the core types, constants, and enums for the memory allocator subsystem.
// 定义内存分配器子系统的核心类型、常量和枚举。
// Key concepts:
// - CachelibLike: slab-based allocator with size classes (inspired by Facebook CacheLib)
// - Offset: simple offset-based allocation for large contiguous blocks
// - AllocationStrategy: how candidate segments are ordered during allocation
// 核心概念：
// - CachelibLike: 基于 slab 和 size class 的分配器（灵感来自 Facebook CacheLib）
// - Offset: 基于偏移的简单分配器，适合大块连续内存
// - AllocationStrategy: 分配时如何对候选 segment 进行排序

use super::cachelib::{cachelib_class_size, generate_cachelib_class_sizes};

// --- Constants / 常量 ---

/// Size of each slab in the Cachelib-like allocator (16 MiB).
/// Slab = 最小管理单元，每次向 segment 申请空间的粒度。
/// Cachelib 类分配器中每个 slab 的大小（16 MiB）。
pub const CACHELIB_SLAB_SIZE: u64 = 1 << 24;

/// Minimum size for a Cachelib allocation (64 bytes).
/// 低于此值的请求会被自动向上舍入。
/// Cachelib 分配的最小请求尺寸（64 字节）。
pub const CACHELIB_MIN_ALLOC_SIZE: u64 = 1 << 6;

/// Internal allocation alignment in bytes.
/// 内部分配对齐（字节）。
pub(super) const CACHELIB_ALIGNMENT: u64 = 8;

/// Smallest size class produced by the class-size generator (72 bytes).
/// 1.25x growth factor from 64, aligned to 8.
/// 由 class size 生成器产生的最小 size class（72 字节）。
pub(super) const CACHELIB_CLASS_MIN_SIZE: u64 = 72;

/// Largest allocation size before the final class size (SLAB_SIZE - 16).
/// 最大分配尺寸，最终 class size = SLAB_SIZE - 16。
pub(super) const CACHELIB_MAX_ALLOC_SIZE: u64 = CACHELIB_SLAB_SIZE - 16;

/// Type alias for pool identifiers (u8, up to 256 pools per segment).
/// 池标识符类型别名（u8，每个 segment 最多 256 个池）。
pub type PoolId = u8;

/// Type alias for allocation class identifiers (u16).
/// 分配类标识符类型别名（u16）。
pub type ClassId = u16;

/// Default pool name for the main allocation pool.
/// 默认主分配池的名称。
pub(super) const DEFAULT_CACHELIB_POOL_NAME: &str = "main";

// --- Slab Release Types / Slab 释放类型 ---

/// Mode for slab release operations: resize (shrink/grow) or rebalance (move between classes).
/// Slab 释放操作模式：Resize = 扩容/缩容，Rebalance = 在不同 size class 之间迁移 slab。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlabReleaseMode {
    /// Shrink the pool by returning a slab to the unreserved pool.
    /// 缩容：将 slab 归还给未预留池。
    Resize,
    /// Rebalance: move a slab from one size class to another within the same pool.
    /// 重平衡：在同一池内将 slab 从一个 size class 转移到另一个。
    Rebalance,
}

/// Context for an ongoing slab release operation.
/// Tracks which slab is being released and which allocations are still active on it.
/// 正在进行的 slab 释放操作的上下文。
/// 跟踪被释放的 slab 以及其上仍活跃的分配。
#[derive(Debug, Clone)]
pub struct SlabReleaseContext {
    /// Unique token identifying this release operation.
    /// 标识本次释放操作的唯一令牌。
    pub token: u64,
    /// Pool the slab belongs to.
    /// 该 slab 所属的池。
    pub pool_id: PoolId,
    /// Victim class size (the class being released).
    /// 受害 class size（被释放的 size class）。
    pub victim_class_size: Option<u64>,
    /// Receiver class size (target class for rebalance).
    /// 接收方 class size（重平衡的目标 size class）。
    pub receiver_class_size: Option<u64>,
    /// Index of the slab being released.
    /// 被释放 slab 的索引。
    pub slab_index: u32,
    /// Mode of this release.
    /// 本次释放的模式。
    pub mode: SlabReleaseMode,
    /// Offsets of allocations that are still active on the slab.
    /// 该 slab 上仍然活跃的分配的偏移量列表。
    pub active_offsets: Vec<u64>,
    /// Whether the release is already completed.
    /// 释放是否已经完成。
    pub is_released: bool,
}

// --- Allocation Metadata Types / 分配元数据类型 ---

/// Metadata about a single allocation in the Cachelib-like allocator.
/// Cachelib 类分配器中单个分配的元数据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachelibAllocInfo {
    /// Pool this allocation belongs to.
    /// 该分配所属的池。
    pub pool_id: PoolId,
    /// Size class ID for this allocation.
    /// 该分配的 size class ID。
    pub class_id: ClassId,
    /// Actual allocated size in bytes.
    /// 实际分配的字节数。
    pub alloc_size: u64,
}

/// A visit record during allocation traversal (iterator pattern).
/// 分配遍历过程中的访问记录（迭代器模式）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachelibAllocationVisit {
    /// Byte offset within the segment.
    /// segment 内的字节偏移量。
    pub offset: u64,
    /// Metadata about this allocation.
    /// 该分配的元数据。
    pub info: CachelibAllocInfo,
    /// Whether the slot is currently allocated (false = free slot).
    /// 该槽位是否已被分配（false = 空闲槽位）。
    pub allocated: bool,
}

// --- Strategy & Allocator Kind / 分配策略与分配器类型 ---

/// Strategy for selecting which segments to allocate from.
/// 选择哪些 segment 进行分配的策略。
///
/// - Random: Randomly shuffle candidates, then stable-sort by affinity (same node > preferred segment).
///   随机打乱候选 segment，然后用稳定排序按亲和性（同节点 > 首选 segment）排序。
/// - FreeRatioFirst: Composite sort: same node > preferred segment > highest free ratio.
///   复合排序：同节点 > 首选 segment > 空闲率从高到低。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationStrategy {
    /// Randomize candidates with affinity boosting (same node / preferred segment).
    /// 随机化候选并提升亲和性（同节点 / 首选 segment）。
    Random,
    /// Deterministic sort: same node > preferred > highest free ratio first.
    /// 确定排序：同节点 > 首选 segment > 空闲率高者优先。
    FreeRatioFirst,
}

impl AllocationStrategy {
    /// Parse allocation strategy from a string value.
    /// 从字符串解析分配策略。
    /// "random" -> Random, "free_ratio_first" -> FreeRatioFirst.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "random" => Some(Self::Random),
            "free_ratio_first" => Some(Self::FreeRatioFirst),
            _ => None,
        }
    }
}

/// Kind of memory allocator used within a segment.
/// segment 内使用的内存分配器类型。
///
/// - Offset: Simple offset-based allocator with free-range merging.
///   基于偏移量的简单分配器，带空闲区间合并。
/// - CachelibLike: Slab-based allocator with size classes and pool management.
///   基于 slab 的分配器，具有 size class 和池管理功能。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAllocatorKind {
    /// Simple offset-based allocation for large contiguous blocks.
    /// 基于偏移的简单分配，适合大块连续内存。
    Offset,
    /// Slab-based allocation with size classes, inspired by Facebook CacheLib.
    /// 基于 slab 的分配，具有 size class 机制（灵感来自 Facebook CacheLib）。
    CachelibLike,
}

impl MemoryAllocatorKind {
    /// Parse memory allocator kind from a string value.
    /// 从字符串解析内存分配器类型。
    /// "offset" -> Offset, "cachelib" / "cachelib-like" -> CachelibLike.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "offset" => Some(Self::Offset),
            "cachelib" | "cachelib-like" => Some(Self::CachelibLike),
            _ => None,
        }
    }
}

// --- Convenience Functions / 便捷函数 ---

/// Get the Cachelib allocation class size for a given requested size.
/// 获取给定请求尺寸对应的 Cachelib 分配 class size。
/// Rounds up to the nearest size class that can accommodate the request.
/// 向上舍入到能容纳该请求的最小 size class。
pub fn cachelib_allocation_class_size_for_request(requested_size: u64) -> Option<u64> {
    cachelib_class_size(&generate_cachelib_class_sizes(), requested_size)
}

/// Get the Cachelib allocation class ID (index) for a given requested size.
/// 获取给定请求尺寸对应的 Cachelib 分配 class ID（索引）。
pub fn cachelib_allocation_class_id_for_request(requested_size: u64) -> Option<ClassId> {
    let class_sizes = generate_cachelib_class_sizes();
    let class_size = cachelib_class_size(&class_sizes, requested_size)?;
    class_sizes
        .iter()
        .position(|size| *size == class_size)
        .map(|idx| idx as ClassId)
}

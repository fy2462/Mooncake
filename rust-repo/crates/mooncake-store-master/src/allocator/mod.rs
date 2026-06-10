// =============================================================================
// Memory Allocator Module — 内存分配器模块
// =============================================================================
// This module implements the core memory allocation logic for the master service.
// 本模块实现 master 服务的核心内存分配逻辑。
//
// Architecture / 架构:
// ┌─────────────────────────────────────────────────────────┐
// │  SegmentAllocator (facade)                              │
// │  ┌──────────────────────┐  ┌─────────────────────────┐  │
// │  │ AllocationStrategy   │  │ Segment → SegmentState   │  │
// │  │ Random / FreeRatioFirst│  │   ├─ Offset (simple)    │  │
// │  └──────────────────────┘  │   └─ Cachelib (slab)     │  │
// │                            └─────────────────────────┘  │
// └─────────────────────────────────────────────────────────┘
//
// Allocation Flow / 分配流程:
// 1. Client requests N replicas of size S for key K.
//    客户端为 key K 请求大小为 S 的 N 个副本。
// 2. SegmentAllocator filters segments with enough free space.
//    SegmentAllocator 过滤出有足够空闲空间的 segment。
// 3. Candidates are sorted per AllocationStrategy.
//    候选 segment 按 AllocationStrategy 排序。
// 4. Top N candidates each allocate space via their SegmentLayout.
//    前 N 个候选各自通过其 SegmentLayout 分配空间。
// 5. ReplicaDescriptors are returned to the caller.
//    ReplicaDescriptor 返回给调用方。

mod cachelib;
mod cachelib_api;
mod offset_layout;
mod strategies;
mod types;

use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment,
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use self::cachelib::{
    align_up, allocate_cachelib, generate_cachelib_class_sizes, release_cachelib,
    CachelibSegmentState,
};
use self::offset_layout::preferred_segment_names;
use self::types::DEFAULT_CACHELIB_POOL_NAME;
pub use self::types::{
    cachelib_allocation_class_id_for_request, cachelib_allocation_class_size_for_request,
    AllocationStrategy, CachelibAllocInfo, CachelibAllocationVisit, ClassId, MemoryAllocatorKind,
    PoolId, SlabReleaseContext, SlabReleaseMode, CACHELIB_MIN_ALLOC_SIZE, CACHELIB_SLAB_SIZE,
};

const RANDOM_MAX_RETRY_LIMIT: usize = 100;
const FREE_RATIO_CANDIDATE_MULTIPLIER: usize = 6;

/// Error returned by C++-style single-segment allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentAllocationError {
    InvalidParams,
    SegmentNotFound,
    NoAvailableHandle,
}

// =============================================================================
// Internal State Structures / 内部状态结构
// =============================================================================

/// State for the simple offset-based allocator.
/// 基于 Offset 的简单分配器状态。
///
/// Maintains a list of free ranges (offset, size) that are merged after release.
/// 维护释放后合并的空闲区间列表（偏移量, 大小）。
/// Best suited for large contiguous allocations where slab overhead is undesirable.
/// 最适合不适合 slab 开销的大块连续分配场景。
#[derive(Debug, Clone)]
struct OffsetSegmentState {
    free_ranges: Vec<(u64, u64)>, // (offset, size) 空闲区间列表
}

/// The two possible memory layout modes within a segment.
/// segment 内两种可能的内存布局模式。
///
/// - Offset: simple free-range management for large blocks.
///   Offset：适合大块分配的简单空闲区间管理。
/// - Cachelib: slab-based, size-class allocation with pool management.
///   Cachelib：基于 slab、size class 分配，带池管理。
#[derive(Debug, Clone)]
enum SegmentLayout {
    Offset(OffsetSegmentState),
    Cachelib(CachelibSegmentState),
}

/// Runtime state for a single memory segment.
/// 单个内存 segment 的运行时状态。
///
/// Tracks the segment handle, bytes used, and allocator layout.
/// 跟踪 segment 句柄、已用字节和分配器布局。
#[derive(Debug, Clone)]
struct SegmentState {
    segment: Segment,
    used: u64,
    layout: SegmentLayout,
}

// =============================================================================
// SegmentAllocator — the main allocator facade / 主分配器门面
// =============================================================================

/// The segment allocator manages space allocation and reclamation across multiple
/// memory segments.
/// 段分配器：管理多个 memory segment 的空间分配和回收。
///
/// Supports two allocation strategies (Random / FreeRatioFirst) and two memory
/// allocator kinds (Offset / CachelibLike).
/// 支持两种分配策略（Random / FreeRatioFirst）和两种内存分配器（Offset / CachelibLike）。
///
/// Thread safety: SegmentAllocator is NOT internally synchronized; the caller
/// (typically MasterState) wraps it in a RwLock.
/// 线程安全：SegmentAllocator 内部不同步；调用方（通常为 MasterState）用 RwLock 包装。
pub struct SegmentAllocator {
    segments: HashMap<Uuid, SegmentState>,
    strategy: AllocationStrategy,
    memory_allocator_kind: MemoryAllocatorKind,
}

impl Default for SegmentAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentAllocator {
    /// Create a new SegmentAllocator with default settings (Random strategy, Offset allocator).
    /// 创建使用默认设置的新 SegmentAllocator（Random 策略、Offset 分配器）。
    pub fn new() -> Self {
        Self {
            segments: HashMap::new(),
            strategy: AllocationStrategy::Random,
            memory_allocator_kind: MemoryAllocatorKind::Offset,
        }
    }

    /// Builder method: set the allocation strategy.
    /// 构建器方法：设置分配策略。
    pub fn with_strategy(mut self, strategy: AllocationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Builder method: set the memory allocator kind.
    /// 构建器方法：设置内存分配器类型。
    pub fn with_memory_allocator(mut self, memory_allocator_kind: MemoryAllocatorKind) -> Self {
        self.memory_allocator_kind = memory_allocator_kind;
        self
    }

    /// Get the current memory allocator kind.
    /// 获取当前内存分配器类型。
    pub fn memory_allocator_kind(&self) -> MemoryAllocatorKind {
        self.memory_allocator_kind
    }

    /// Register a new memory segment with the allocator.
    /// 注册一个新的 memory segment 到分配器。
    ///
    /// Initializes the layout based on memory_allocator_kind:
    /// 根据 memory_allocator_kind 初始化不同的布局：
    ///
    /// - Offset: creates a single free range from (used, size - used).
    ///   Offset：创建单个空闲区间 (used, size - used)。
    ///   空闲区 = (已用, 大小 - 已用)。
    ///
    /// - CachelibLike: reserves used space as slabs (aligned up to SLAB_SIZE),
    ///   creates remaining unreserved slabs, and provisions a default "main" pool.
    ///   CachelibLike：按 slab 粒度分片，预留已用空间对应的 slab，
    ///   创建剩余未预留 slab，并分配默认 "main" 池。
    ///   剩余归入默认池。
    pub fn add_segment(&mut self, segment: Segment, used: u64, _client_id: Uuid) {
        let (layout, effective_used) = match self.memory_allocator_kind {
            MemoryAllocatorKind::Offset => {
                let tail_free = segment.size.saturating_sub(used);
                let free_ranges = if tail_free > 0 {
                    vec![(used, tail_free)]
                } else {
                    Vec::new()
                };
                (
                    SegmentLayout::Offset(OffsetSegmentState { free_ranges }),
                    used,
                )
            }
            MemoryAllocatorKind::CachelibLike => {
                // Reserve space for pre-existing data: align used up to slab boundary
                // 为已有数据预留空间：将已用量向上对齐到 slab 边界
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
                    // Remaining slab indices in reverse order (LIFO stack)
                    // 剩余 slab 索引按逆序排列（LIFO 栈）
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
                // Provision the default main pool with all unreserved capacity
                // 用全部未预留容量分配默认 main 池
                let main_pool_id = cachelib
                    .create_pool(DEFAULT_CACHELIB_POOL_NAME.to_string(), total_capacity_bytes)
                    .expect("default cachelib pool should be provisionable");
                cachelib.default_pool_id = main_pool_id;
                (SegmentLayout::Cachelib(cachelib), reserved_bytes)
            }
        };
        self.segments.insert(
            segment.id,
            SegmentState {
                segment,
                used: effective_used,
                layout,
            },
        );
    }

    /// Remove a segment from the allocator.
    /// 从分配器中移除一个 segment。
    pub fn remove_segment(&mut self, segment_id: &Uuid) {
        self.segments.remove(segment_id);
    }

    /// Allocate replicas for a key (convenience wrapper, no preferred client).
    /// 为 key 分配副本（便捷包装器，无指定客户端偏好）。
    pub fn allocate(
        &mut self,
        key: &str,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Vec<ReplicaDescriptor> {
        self.allocate_for_client(key, None, slice_size, replica_count, config)
    }

    /// Allocate replica_count memory replicas for a specific client.
    /// 为指定 client 分配 replica_count 个内存副本。
    ///
    /// Algorithm / 分配策略:
    ///
    /// 1. Filter segments that can accommodate `slice_size`.
    ///    过滤出能容纳 `slice_size` 的 segment。
    ///
    /// 2. Sort/shuffle candidates per strategy:
    ///    按策略排序/打乱候选：
    ///    - Random: shuffle + stable-sort by affinity (same node, preferred segment).
    ///      Random：随机打乱候选 segment 后稳定排序（优先同节点、优先首选 segment）。
    ///      注意：stable sort 保持相同 key 的原有顺序，从而保留 shuffle 的随机性。
    ///    - FreeRatioFirst: composite sort: same node > preferred segment > free ratio desc.
    ///      FreeRatioFirst：复合排序（同节点 > 首选 segment > 空闲率从高到低）。
    ///      三级复合排序：同节点 > 首选 segment > 空闲率从高到低。
    ///
    /// 3. Allocate from top-N candidates, creating Allocating-status ReplicaDescriptors.
    ///    从 top-N 候选分配，创建 Allocating 状态的 ReplicaDescriptor。
    ///    每个副本从候选 segment 中按顺序取空闲空间，创建 Allocating 状态的 ReplicaDescriptor。
    pub fn allocate_for_client(
        &mut self,
        _key: &str,
        _client_id: Option<Uuid>,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Vec<ReplicaDescriptor> {
        self.allocate_for_client_excluding(
            _key,
            _client_id,
            slice_size,
            replica_count,
            config,
            &HashSet::new(),
        )
    }

    /// Allocate replicas while excluding named segments, matching the C++ allocation strategy API.
    pub fn allocate_with_exclusions(
        &mut self,
        key: &str,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
        excluded_segments: &[String],
    ) -> Vec<ReplicaDescriptor> {
        let excluded_segments = excluded_segments
            .iter()
            .filter(|name| !name.is_empty())
            .cloned()
            .collect::<HashSet<_>>();
        self.allocate_for_client_excluding(
            key,
            None,
            slice_size,
            replica_count,
            config,
            &excluded_segments,
        )
    }

    /// Allocate a single replica from the specified segment name.
    pub fn allocate_from_segment(
        &mut self,
        segment_name: &str,
        slice_size: u64,
    ) -> Result<ReplicaDescriptor, SegmentAllocationError> {
        if slice_size == 0 {
            return Err(SegmentAllocationError::InvalidParams);
        }
        if !self
            .segments
            .values()
            .any(|state| state.segment.name == segment_name)
        {
            return Err(SegmentAllocationError::SegmentNotFound);
        }
        self.allocate_from_segment_name(segment_name, slice_size)
            .ok_or(SegmentAllocationError::NoAvailableHandle)
    }

    fn allocate_for_client_excluding(
        &mut self,
        _key: &str,
        _client_id: Option<Uuid>,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
        excluded_segments: &HashSet<String>,
    ) -> Vec<ReplicaDescriptor> {
        if self.segments.is_empty() || replica_count == 0 || slice_size == 0 {
            return vec![];
        }

        let preferred_names = preferred_segment_names(config);
        let mut replicas = Vec::with_capacity(replica_count);
        let mut used_segment_names = HashSet::new();

        // C++ tries preferred_segment(s) first. If preferred_segment is set, it
        // wins over preferred_segments; the plural list is used only as fallback
        // when the singular field is empty.
        for preferred_name in preferred_names {
            if replicas.len() >= replica_count
                || excluded_segments.contains(preferred_name)
                || used_segment_names.contains(preferred_name)
            {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(preferred_name, slice_size) {
                used_segment_names.insert(replica.segment_name.clone());
                replicas.push(replica);
            }
        }

        if replicas.len() >= replica_count {
            return replicas;
        }

        match self.strategy {
            AllocationStrategy::Random => {
                self.allocate_random_remaining(
                    slice_size,
                    replica_count,
                    &mut replicas,
                    &mut used_segment_names,
                    excluded_segments,
                );
            }
            AllocationStrategy::FreeRatioFirst => {
                self.allocate_free_ratio_remaining(
                    slice_size,
                    replica_count,
                    &mut replicas,
                    &mut used_segment_names,
                    excluded_segments,
                );
            }
        }
        replicas
    }

    /// Release a set of replicas, returning their space to the respective segments.
    /// 释放一组副本，将占用的空间归还给各自的 segment。
    ///
    /// - Offset mode: triggers free range insertion and merging (insert_free_range).
    ///   对于 Offset 模式，归还会触发空闲区间合并（insert_free_range）。
    /// - Cachelib mode: marks the allocation as freed via release_cachelib.
    ///   对于 Cachelib 模式，通过 release_cachelib 标记分配为已释放。
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

    /// Get used bytes for a specific segment.
    /// 获取特定 segment 的已用字节数。
    pub fn used_bytes(&self, segment_id: &Uuid) -> Option<u64> {
        self.segments.get(segment_id).map(|state| state.used)
    }

    /// Get total memory usage across all segments: (total_capacity, total_used).
    /// 获取所有 segment 的内存使用总计：(总容量, 总已用)。
    pub fn usage_totals(&self) -> (u64, u64) {
        self.segments.values().fold((0, 0), |(total, used), state| {
            (
                total.saturating_add(state.segment.size),
                used.saturating_add(state.used),
            )
        })
    }
}

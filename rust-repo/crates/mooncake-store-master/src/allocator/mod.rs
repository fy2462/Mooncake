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
mod types;

use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, Segment,
};
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
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

    fn allocate_random_remaining(
        &mut self,
        slice_size: u64,
        replica_count: usize,
        replicas: &mut Vec<ReplicaDescriptor>,
        used_segment_names: &mut HashSet<String>,
        excluded_segments: &HashSet<String>,
    ) {
        let names = self.segment_names();
        if names.is_empty() {
            return;
        }

        let max_retry = RANDOM_MAX_RETRY_LIMIT.min(names.len());
        let mut rng = thread_rng();
        let mut start_idx = rng.gen_range(0..names.len());

        for _ in 0..max_retry {
            if replicas.len() >= replica_count {
                return;
            }
            let segment_name = names[start_idx % names.len()].clone();
            start_idx += 1;

            if excluded_segments.contains(&segment_name)
                || used_segment_names.contains(&segment_name)
            {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                used_segment_names.insert(replica.segment_name.clone());
                replicas.push(replica);
            }
        }
    }

    fn allocate_free_ratio_remaining(
        &mut self,
        slice_size: u64,
        replica_count: usize,
        replicas: &mut Vec<ReplicaDescriptor>,
        used_segment_names: &mut HashSet<String>,
        excluded_segments: &HashSet<String>,
    ) {
        let names = self.segment_names();
        if names.is_empty() {
            return;
        }

        let remaining = replica_count.saturating_sub(replicas.len());
        let sample_count = (FREE_RATIO_CANDIDATE_MULTIPLIER * remaining).min(names.len());
        let mut rng = thread_rng();
        let mut start_idx = rng.gen_range(0..names.len());
        let mut candidates = Vec::with_capacity(sample_count);

        for _ in 0..sample_count {
            let segment_name = names[start_idx % names.len()].clone();
            start_idx += 1;
            candidates.push((
                segment_name.clone(),
                self.free_ratio_for_name(&segment_name),
            ));
        }

        candidates.sort_by(|(_, ratio_a), (_, ratio_b)| {
            ratio_b.partial_cmp(ratio_a).unwrap_or(Ordering::Equal)
        });

        for (segment_name, _) in candidates {
            if replicas.len() >= replica_count {
                return;
            }
            if excluded_segments.contains(&segment_name)
                || used_segment_names.contains(&segment_name)
            {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(&segment_name, slice_size) {
                used_segment_names.insert(replica.segment_name.clone());
                replicas.push(replica);
            }
        }

        if replicas.len() < replica_count {
            self.allocate_random_remaining(
                slice_size,
                replica_count,
                replicas,
                used_segment_names,
                excluded_segments,
            );
        }
    }

    fn segment_names(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.segments
            .values()
            .filter_map(|state| {
                if seen.insert(state.segment.name.clone()) {
                    Some(state.segment.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn free_ratio_for_name(&self, segment_name: &str) -> f64 {
        let (total, used) = self
            .segments
            .values()
            .filter(|state| state.segment.name == segment_name)
            .fold((0_u64, 0_u64), |(total, used), state| {
                (
                    total.saturating_add(state.segment.size),
                    used.saturating_add(state.used),
                )
            });
        free_ratio(total, used)
    }

    fn allocate_from_segment_name(
        &mut self,
        segment_name: &str,
        slice_size: u64,
    ) -> Option<ReplicaDescriptor> {
        let mut ids = self
            .segments
            .iter()
            .filter(|(_, state)| state.segment.name == segment_name)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.shuffle(&mut thread_rng());
        for segment_id in ids {
            if let Some(replica) = self.allocate_from_segment_id(segment_id, slice_size) {
                return Some(replica);
            }
        }
        None
    }

    fn allocate_from_segment_id(
        &mut self,
        segment_id: Uuid,
        slice_size: u64,
    ) -> Option<ReplicaDescriptor> {
        let state = self.segments.get_mut(&segment_id)?;
        let (offset, accounted_size) = state.allocate(slice_size)?;
        state.used = state.used.saturating_add(accounted_size);
        Some(ReplicaDescriptor {
            refcnt: 0,
            handle_valid: true,
            segment_id: state.segment.id,
            segment_name: state.segment.name.clone(),
            offset,
            size: slice_size,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: None,
            base_addr: state.segment.base,
        })
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

    // --- Pool Management Delegates / 池管理委托 ---
    // These delegate to the CachelibLayout; Offset layout returns errors.
    // 以下方法委托给 CachelibLayout；Offset 布局返回错误。

    /// Create a new pool within a segment (Cachelib only).
    /// 在 segment 内创建新池（仅 Cachelib）。
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

    // --- Query Methods / 查询方法 ---

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

// =============================================================================
// Free-standing Helpers / 独立辅助函数
// =============================================================================

/// Resolve C++-compatible preferred segment precedence.
/// 解析与 C++ 一致的 preferred segment 优先级。
fn preferred_segment_names(config: &ReplicateConfig) -> Vec<&str> {
    if !config.preferred_segment.is_empty() {
        return vec![config.preferred_segment.as_str()];
    }
    config
        .preferred_segments
        .iter()
        .filter(|name| !name.is_empty())
        .map(String::as_str)
        .collect()
}

// =============================================================================
// SegmentState Methods / SegmentState 方法
// =============================================================================

impl SegmentState {
    /// Attempt to allocate `size` bytes from this segment.
    /// 尝试从此 segment 分配 `size` 字节。
    ///
    /// Returns Some((offset, accounted_size)) on success.
    /// Note: accounted_size may be larger than size due to size class rounding (Cachelib).
    /// 成功时返回 Some((偏移量, 计入大小))。
    /// 注意：由于 size class 取整（Cachelib），计入大小可能大于请求大小。
    fn allocate(&mut self, size: u64) -> Option<(u64, u64)> {
        match &mut self.layout {
            SegmentLayout::Offset(offset) => {
                let start = reserve_range(&mut offset.free_ranges, size)?;
                Some((start, size))
            }
            SegmentLayout::Cachelib(cachelib) => allocate_cachelib(cachelib, size),
        }
    }

    /// Release the space occupied by a replica.
    /// 释放副本占用的空间。
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

/// Compute the free ratio of a segment: free / total.
/// 计算 segment 的空闲率：free / total。
/// Used by FreeRatioFirst strategy for segment sorting.
/// 由 FreeRatioFirst 策略用于 segment 排序。
fn free_ratio(size: u64, used: u64) -> f64 {
    if size == 0 {
        return 0.0;
    }
    size.saturating_sub(used) as f64 / size as f64
}

/// Reserve `size` bytes from the free ranges list, returning the starting offset.
/// 从空闲区间列表中预留 size 字节，返回起始偏移量。
///
/// Uses the first sufficiently large range; removes the range if exact fit,
/// otherwise shrinks it from the left.
/// 优先使用首个足够大的区间；若区间刚好吃完则移除，否则从左侧收缩。
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

/// Insert a free range into the list, automatically merging adjacent/overlapping ranges.
/// 向空闲区间列表插入一个新区间，并自动合并相邻/重叠区间。
///
/// Algorithm: sort by start offset, then one-pass merge: if current range
/// overlaps or touches the previous, extend the previous range.
/// 先排序后遍历合并：若新区间与前一区间重叠或相邻，则扩展前一区间。
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

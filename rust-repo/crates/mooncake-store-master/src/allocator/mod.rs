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
// │  │ AllocationStrategy     │  │   ├─ Offset (simple)    │  │
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
    CachelibAllocation, CachelibSegmentState, SlabClassState, align_up, allocate_cachelib,
    cachelib_class_size, generate_cachelib_class_sizes, release_cachelib,
};
use self::offset_layout::preferred_segment_names;
use self::strategies::AllocationPlan;
use self::types::DEFAULT_CACHELIB_POOL_NAME;
pub use self::types::{
    AllocationStrategy, AllocatorSnapshotConfig, CACHELIB_MIN_ALLOC_SIZE, CACHELIB_SLAB_SIZE,
    CachelibAllocInfo, CachelibAllocationVisit, ClassId, MemoryAllocatorKind, PoolId,
    SlabReleaseContext, SlabReleaseMode, SsdUsageMetrics, cachelib_allocation_class_id_for_request,
    cachelib_allocation_class_size_for_request,
};

const RANDOM_MAX_RETRY_LIMIT: usize = 100;
const FREE_RATIO_CANDIDATE_MULTIPLIER: usize = 6;
/// Largest slab-aligned capacity representable by Cachelib's u32 slab IDs.
pub const CACHELIB_MAX_SEGMENT_SIZE: u64 = u32::MAX as u64 * CACHELIB_SLAB_SIZE;

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
    /// Exact live allocation size by starting offset. This is the Rust
    /// equivalent of C++ `AllocatedBuffer` ownership and prevents a stale or
    /// duplicate descriptor from manufacturing free space.
    allocations: HashMap<u64, u64>,
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
    client_id: Uuid,
    /// False after Master snapshot restore until the owning process supplies
    /// current base/endpoint coordinates through ReMount.
    runtime_bound: bool,
}

// =============================================================================
// SegmentAllocator — the main allocator facade / 主分配器门面
// =============================================================================

/// The segment allocator manages space allocation and reclamation across multiple
/// memory segments.
/// 段分配器：管理多个 memory segment 的空间分配和回收。
///
/// Supports multiple allocation strategies and two memory
/// allocator kinds (Offset / CachelibLike).
/// 支持多种分配策略和两种内存分配器（Offset / CachelibLike）。
///
/// Thread safety: SegmentAllocator is NOT internally synchronized; the caller
/// (typically MasterState) wraps it in a RwLock.
/// 线程安全：SegmentAllocator 内部不同步；调用方（通常为 MasterState）用 RwLock 包装。
pub struct SegmentAllocator {
    segments: HashMap<Uuid, SegmentState>,
    strategy: AllocationStrategy,
    memory_allocator_kind: MemoryAllocatorKind,
    /// C++ mounts every client-visible `protocol=cxl` segment against one
    /// global allocator. Keeping a single state here prevents each client
    /// alias from multiplying the physical CXL capacity.
    cxl_global: Option<SegmentState>,
}

const CXL_GLOBAL_SEGMENT_NAME: &str = "__mooncake_cxl_global__";

fn new_cachelib_layout(size: u64, used: u64) -> (SegmentLayout, u64) {
    let reserved_bytes = align_up(used, CACHELIB_SLAB_SIZE).min(size);
    let total_slabs = (size / CACHELIB_SLAB_SIZE) as u32;
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
        unreserved_slab_indices: (reserved_slabs..total_slabs).rev().collect::<Vec<_>>(),
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

fn restore_cachelib_layout(
    cachelib: &mut CachelibSegmentState,
    segment: &Segment,
    replicas: &[ReplicaDescriptor],
) -> Result<u64, String> {
    let total_slabs_u64 = segment.size / CACHELIB_SLAB_SIZE;
    let total_slabs = u32::try_from(total_slabs_u64).map_err(|_| {
        format!(
            "segment {} has {total_slabs_u64} cachelib slabs, exceeding the u32 slab index space",
            segment.id
        )
    })?;
    let default_pool_id = cachelib.default_pool_id;
    let mut slab_slots: HashMap<u32, (u64, HashSet<u32>)> = HashMap::new();
    let mut allocations = Vec::with_capacity(replicas.len());
    let mut used = 0u64;

    for replica in replicas {
        let class_size =
            cachelib_class_size(&cachelib.class_sizes, replica.size).ok_or_else(|| {
                format!(
                    "replica size {} has no cachelib class in segment {}",
                    replica.size, segment.id
                )
            })?;
        let slab_index_u64 = replica.offset / CACHELIB_SLAB_SIZE;
        if slab_index_u64 >= u64::from(total_slabs) {
            return Err(format!(
                "replica offset {} is outside usable cachelib slabs in segment {}",
                replica.offset, segment.id
            ));
        }
        let slab_index = u32::try_from(slab_index_u64).map_err(|_| {
            format!(
                "replica offset {} exceeds cachelib slab index space in segment {}",
                replica.offset, segment.id
            )
        })?;
        let within_slab = replica.offset % CACHELIB_SLAB_SIZE;
        if within_slab % class_size != 0 {
            return Err(format!(
                "replica offset {} is not aligned to cachelib class {} in segment {}",
                replica.offset, class_size, segment.id
            ));
        }
        let capacity = CACHELIB_SLAB_SIZE / class_size;
        let slot_index = within_slab / class_size;
        if slot_index >= capacity
            || within_slab
                .checked_add(class_size)
                .is_none_or(|end| end > CACHELIB_SLAB_SIZE)
        {
            return Err(format!(
                "replica offset {} crosses cachelib slab boundary in segment {}",
                replica.offset, segment.id
            ));
        }

        let (existing_class, occupied_slots) = slab_slots
            .entry(slab_index)
            .or_insert_with(|| (class_size, HashSet::new()));
        if *existing_class != class_size {
            return Err(format!(
                "cachelib slab {slab_index} mixes classes {} and {} in segment {}",
                *existing_class, class_size, segment.id
            ));
        }
        if !occupied_slots.insert(slot_index as u32) {
            return Err(format!(
                "duplicate cachelib slot {slot_index} in slab {slab_index} for segment {}",
                segment.id
            ));
        }
        used = used
            .checked_add(class_size)
            .ok_or_else(|| format!("restored usage overflow in segment {}", segment.id))?;
        allocations.push((
            replica.offset,
            CachelibAllocation {
                pool_id: default_pool_id,
                requested_size: replica.size,
                class_size,
                slab_index,
                slot_index: slot_index as u32,
            },
        ));
    }

    cachelib.allocations.clear();
    cachelib.pending_releases.clear();
    cachelib.next_release_token = 1;
    cachelib
        .unreserved_slab_indices
        .retain(|slab| !slab_slots.contains_key(slab));

    {
        let pool = cachelib
            .pools
            .get_mut(&default_pool_id)
            .ok_or_else(|| format!("default cachelib pool missing in segment {}", segment.id))?;
        pool.reserved_slabs = slab_slots.keys().copied().collect();
        pool.reserved_slabs
            .sort_unstable_by(|left, right| right.cmp(left));
        pool.class_slabs.clear();

        let mut restored_slabs = slab_slots.into_iter().collect::<Vec<_>>();
        restored_slabs.sort_unstable_by_key(|(slab_index, _)| *slab_index);
        for (slab_index, (class_size, occupied_slots)) in restored_slabs {
            let capacity = (CACHELIB_SLAB_SIZE / class_size) as u32;
            let mut free_slots = (0..capacity)
                .filter(|slot| !occupied_slots.contains(slot))
                .collect::<Vec<_>>();
            free_slots.reverse();
            pool.class_slabs
                .entry(class_size)
                .or_default()
                .push(SlabClassState {
                    slab_index,
                    capacity,
                    free_slots,
                    pending_release_token: None,
                });
        }
    }
    cachelib.allocations.extend(allocations);
    Ok(used)
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
            cxl_global: None,
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

    /// Configure the single physical CXL address space. Client mounts are
    /// aliases only and therefore do not create additional capacity.
    pub fn with_cxl_capacity(mut self, size: u64) -> Self {
        if size == 0 {
            return self;
        }
        let (layout, effective_used) = new_cachelib_layout(size, 0);
        self.cxl_global = Some(SegmentState {
            segment: Segment {
                id: Uuid::nil(),
                name: CXL_GLOBAL_SEGMENT_NAME.to_string(),
                base: 0,
                size,
                te_endpoint: String::new(),
                protocol: "cxl".to_string(),
                host_id: String::new(),
            },
            used: effective_used,
            layout,
            client_id: Uuid::nil(),
            runtime_bound: true,
        });
        self
    }

    /// Restore a durable CXL segment identity as an unbound process-local
    /// alias. The owning client must later supply its current endpoint through
    /// ReMount before this alias can receive allocations.
    pub fn restore_cxl_alias(&mut self, segment: Segment, client_id: Uuid) -> Result<(), String> {
        let global_size = self
            .cxl_global
            .as_ref()
            .map(|state| state.segment.size)
            .ok_or_else(|| "CXL allocator is not configured".to_string())?;
        if segment.protocol != "cxl" {
            return Err(format!(
                "restored CXL alias {} has protocol {:?}",
                segment.id, segment.protocol
            ));
        }
        if segment.size != global_size {
            return Err(format!(
                "restored CXL alias {} size {} does not match configured CXL size {}",
                segment.id, segment.size, global_size
            ));
        }
        self.add_segment(segment.clone(), 0, client_id);
        self.invalidate_segment_runtime(&segment.id)
    }

    /// Rebuild the single global CXL allocator from every live CXL replica.
    /// Alias IDs intentionally do not participate in the physical layout:
    /// multiple clients name the same device-relative address space.
    pub fn restore_cxl_allocations(
        &mut self,
        replicas: &[ReplicaDescriptor],
    ) -> Result<u64, String> {
        for replica in replicas {
            let alias = self.segments.get(&replica.segment_id).ok_or_else(|| {
                format!(
                    "restored CXL replica references missing alias segment {}",
                    replica.segment_id
                )
            })?;
            if alias.segment.protocol != "cxl" || alias.segment.name != replica.segment_name {
                return Err(format!(
                    "restored CXL replica identity mismatch for alias {}: descriptor={:?}, allocator={:?}",
                    replica.segment_id, replica.segment_name, alias.segment.name
                ));
            }
        }
        let global = self
            .cxl_global
            .as_mut()
            .ok_or_else(|| "CXL allocator is not configured".to_string())?;
        let SegmentLayout::Cachelib(cachelib) = &mut global.layout else {
            return Err("CXL allocator is not cachelib-backed".to_string());
        };
        let used = restore_cachelib_layout(cachelib, &global.segment, replicas)?;
        global.used = used;
        Ok(used)
    }

    /// Get the current memory allocator kind.
    /// 获取当前内存分配器类型。
    pub fn memory_allocator_kind(&self) -> MemoryAllocatorKind {
        self.memory_allocator_kind
    }

    /// Get the current allocation strategy.
    /// 获取当前分配策略。
    pub fn allocation_strategy(&self) -> AllocationStrategy {
        self.strategy
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
    pub fn add_segment(&mut self, segment: Segment, used: u64, client_id: Uuid) {
        if segment.protocol == "cxl" {
            self.segments.insert(
                segment.id,
                SegmentState {
                    segment,
                    used: 0,
                    // A CXL segment is only a process-local routing alias. Its
                    // empty layout deliberately carries no allocatable
                    // capacity; all allocations go through `cxl_global`.
                    layout: SegmentLayout::Offset(OffsetSegmentState {
                        free_ranges: Vec::new(),
                        allocations: HashMap::new(),
                    }),
                    client_id,
                    runtime_bound: true,
                },
            );
            return;
        }
        let (layout, effective_used) = match self.memory_allocator_kind {
            MemoryAllocatorKind::Offset => {
                let tail_free = segment.size.saturating_sub(used);
                let free_ranges = if tail_free > 0 {
                    vec![(used, tail_free)]
                } else {
                    Vec::new()
                };
                (
                    SegmentLayout::Offset(OffsetSegmentState {
                        free_ranges,
                        allocations: HashMap::new(),
                    }),
                    used,
                )
            }
            MemoryAllocatorKind::CachelibLike => new_cachelib_layout(segment.size, used),
        };
        self.segments.insert(
            segment.id,
            SegmentState {
                segment,
                used: effective_used,
                layout,
                client_id,
                runtime_bound: true,
            },
        );
    }

    /// Restore one segment from the exact set of live replica descriptors.
    ///
    /// This is deliberately descriptor-driven instead of trusting the legacy
    /// aggregate `used` counter: an aggregate cannot represent holes and may
    /// make a recovered allocator overlap a live replica. Every descriptor is
    /// validated for segment identity, bounds, alignment, and overlap before
    /// the recovered segment becomes visible.
    pub fn restore_segment(
        &mut self,
        segment: Segment,
        client_id: Uuid,
        replicas: &[ReplicaDescriptor],
    ) -> Result<u64, String> {
        if self.segments.contains_key(&segment.id) {
            return Err(format!("segment {} already exists", segment.id));
        }
        if segment.protocol == "cxl" {
            return Err("CXL aliases must be restored through restore_cxl_global".to_string());
        }
        for replica in replicas {
            if replica.segment_id != segment.id {
                return Err(format!(
                    "replica segment {} does not match restored segment {}",
                    replica.segment_id, segment.id
                ));
            }
            if replica.segment_name != segment.name {
                return Err(format!(
                    "replica segment name {:?} does not match restored segment {:?} ({})",
                    replica.segment_name, segment.name, segment.id
                ));
            }
            if replica.size == 0 {
                return Err(format!(
                    "zero-sized replica at offset {} in segment {}",
                    replica.offset, segment.id
                ));
            }
            let end = replica
                .offset
                .checked_add(replica.size)
                .ok_or_else(|| format!("replica range overflow in segment {}", segment.id))?;
            if end > segment.size {
                return Err(format!(
                    "replica range [{}, {}) exceeds segment {} size {}",
                    replica.offset, end, segment.id, segment.size
                ));
            }
        }
        if self.memory_allocator_kind == MemoryAllocatorKind::CachelibLike {
            if segment.size % CACHELIB_SLAB_SIZE != 0 {
                return Err(format!(
                    "segment {} size {} is not aligned to Cachelib slab size {}",
                    segment.id, segment.size, CACHELIB_SLAB_SIZE
                ));
            }
            let total_slabs_u64 = segment.size / CACHELIB_SLAB_SIZE;
            u32::try_from(total_slabs_u64).map_err(|_| {
                format!(
                    "segment {} has {total_slabs_u64} cachelib slabs, exceeding the u32 slab index space",
                    segment.id
                )
            })?;
        }

        self.add_segment(segment.clone(), 0, client_id);
        let restore_result = {
            let state = self
                .segments
                .get_mut(&segment.id)
                .expect("segment inserted immediately above");
            match &mut state.layout {
                SegmentLayout::Offset(offset) => (|| {
                    let mut allocated = replicas
                        .iter()
                        .map(|replica| (replica.offset, replica.size))
                        .collect::<Vec<_>>();
                    allocated.sort_unstable_by_key(|(start, _)| *start);

                    let mut free_ranges = Vec::with_capacity(allocated.len() + 1);
                    let mut cursor = 0u64;
                    let mut used = 0u64;
                    for (start, size) in allocated {
                        if start < cursor {
                            return Err(format!(
                                "overlapping replica range starts at {start} before {cursor} in segment {}",
                                segment.id
                            ));
                        }
                        if start > cursor {
                            free_ranges.push((cursor, start - cursor));
                        }
                        cursor = start + size;
                        used = used.checked_add(size).ok_or_else(|| {
                            format!("restored usage overflow in segment {}", segment.id)
                        })?;
                    }
                    if cursor < segment.size {
                        free_ranges.push((cursor, segment.size - cursor));
                    }
                    offset.free_ranges = free_ranges;
                    offset.allocations = replicas
                        .iter()
                        .map(|replica| (replica.offset, replica.size))
                        .collect();
                    state.used = used;
                    Ok(used)
                })(),
                SegmentLayout::Cachelib(cachelib) => {
                    restore_cachelib_layout(cachelib, &segment, replicas).map(|used| {
                        state.used = used;
                        used
                    })
                }
            }
        };
        if restore_result.is_err() {
            self.segments.remove(&segment.id);
        }
        restore_result
    }

    /// Replace only the process-local routing fields of a restored segment.
    ///
    /// The segment UUID, logical name, capacity, allocation layout and used
    /// ranges are durable identities and must remain unchanged. Base address,
    /// transfer endpoint, protocol and owning process session are supplied by
    /// ReMount after a Master restart.
    pub fn validate_segment_rebind(&self, segment: &Segment) -> Result<(), String> {
        let state = self
            .segments
            .get(&segment.id)
            .ok_or_else(|| format!("segment {} does not exist", segment.id))?;
        if state.segment.name != segment.name {
            return Err(format!(
                "segment {} name mismatch: restored {:?}, remounted {:?}",
                segment.id, state.segment.name, segment.name
            ));
        }
        if state.segment.size != segment.size {
            return Err(format!(
                "segment {} size mismatch: restored {}, remounted {}",
                segment.id, state.segment.size, segment.size
            ));
        }
        if state.segment.protocol == "cxl" && segment.protocol != "cxl" {
            return Err(format!(
                "restored CXL segment {} cannot be remounted with protocol {:?}",
                segment.id, segment.protocol
            ));
        }
        Ok(())
    }

    pub fn invalidate_segment_runtime(&mut self, segment_id: &Uuid) -> Result<(), String> {
        let state = self
            .segments
            .get_mut(segment_id)
            .ok_or_else(|| format!("segment {segment_id} does not exist"))?;
        state.runtime_bound = false;
        Ok(())
    }

    pub fn rebind_segment(&mut self, segment: Segment, client_id: Uuid) -> Result<(), String> {
        self.validate_segment_rebind(&segment)?;
        let state = self
            .segments
            .get_mut(&segment.id)
            .expect("segment identity was validated immediately above");
        state.segment.base = segment.base;
        state.segment.te_endpoint = segment.te_endpoint;
        state.segment.protocol = segment.protocol;
        state.client_id = client_id;
        state.runtime_bound = true;
        Ok(())
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

    /// C++-compatible checked allocation boundary.
    ///
    /// Internal placement callers retain the best-effort `Vec` API because a
    /// request for more replicas than available segments legitimately returns
    /// a partial result. Callers that need the public strategy error contract
    /// use this method to distinguish invalid parameters from exhausted or
    /// absent allocators.
    pub fn allocate_checked(
        &mut self,
        key: &str,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Result<Vec<ReplicaDescriptor>, SegmentAllocationError> {
        if slice_size == 0 || replica_count == 0 {
            return Err(SegmentAllocationError::InvalidParams);
        }
        let replicas = self.allocate(key, slice_size, replica_count, config);
        if replicas.is_empty() {
            Err(SegmentAllocationError::NoAvailableHandle)
        } else {
            Ok(replicas)
        }
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
        key: &str,
        client_id: Option<Uuid>,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
    ) -> Vec<ReplicaDescriptor> {
        self.allocate_for_client_excluding(
            key,
            client_id,
            slice_size,
            replica_count,
            config,
            &HashSet::new(),
            None,
        )
    }

    /// Allocate replicas with a snapshot of local SSD usage for SsdFreeRatioFirst.
    pub fn allocate_for_client_with_ssd_metrics(
        &mut self,
        key: &str,
        client_id: Option<Uuid>,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
        ssd_metrics: &HashMap<Uuid, SsdUsageMetrics>,
    ) -> Vec<ReplicaDescriptor> {
        self.allocate_for_client_excluding(
            key,
            client_id,
            slice_size,
            replica_count,
            config,
            &HashSet::new(),
            Some(ssd_metrics),
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
        self.allocate_for_client_with_exclusions(
            key,
            None,
            slice_size,
            replica_count,
            config,
            excluded_segments,
            None,
        )
    }

    pub fn allocate_for_client_with_exclusions(
        &mut self,
        key: &str,
        client_id: Option<Uuid>,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
        excluded_segments: &[String],
        ssd_metrics: Option<&HashMap<Uuid, SsdUsageMetrics>>,
    ) -> Vec<ReplicaDescriptor> {
        let excluded_segments = excluded_segments
            .iter()
            .filter(|name| !name.is_empty())
            .cloned()
            .collect::<HashSet<_>>();
        self.allocate_for_client_excluding(
            key,
            client_id,
            slice_size,
            replica_count,
            config,
            &excluded_segments,
            ssd_metrics,
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

    /// Allocate a single replica from one exact durable segment identity.
    ///
    /// Internal jobs use this after capturing a UUID so a same-name mount
    /// cannot inherit an older task's allocation.
    pub fn allocate_from_segment_id(
        &mut self,
        segment_id: Uuid,
        slice_size: u64,
    ) -> Result<ReplicaDescriptor, SegmentAllocationError> {
        if slice_size == 0 {
            return Err(SegmentAllocationError::InvalidParams);
        }
        if !self.segments.contains_key(&segment_id) {
            return Err(SegmentAllocationError::SegmentNotFound);
        }
        self.allocate_from_segment_id_inner(segment_id, slice_size)
            .ok_or(SegmentAllocationError::NoAvailableHandle)
    }

    fn allocate_for_client_excluding(
        &mut self,
        key: &str,
        client_id: Option<Uuid>,
        slice_size: u64,
        replica_count: usize,
        config: &ReplicateConfig,
        excluded_segments: &HashSet<String>,
        ssd_metrics: Option<&HashMap<Uuid, SsdUsageMetrics>>,
    ) -> Vec<ReplicaDescriptor> {
        if self.segments.is_empty() || replica_count == 0 || slice_size == 0 {
            return vec![];
        }

        let preferred_names = preferred_segment_names(config);
        if self.strategy == AllocationStrategy::Cxl {
            let Some(preferred_name) = preferred_names.first().copied() else {
                return vec![];
            };
            if excluded_segments.contains(preferred_name) {
                return vec![];
            }
            return self
                .allocate_cxl(preferred_name, slice_size)
                .into_iter()
                .collect();
        }
        let mut plan = AllocationPlan::with_capacity(replica_count);

        // C++ tries preferred_segment(s) first. If preferred_segment is set, it
        // wins over preferred_segments; the plural list is used only as fallback
        // when the singular field is empty.
        for preferred_name in preferred_names {
            if plan.is_complete()
                || excluded_segments.contains(preferred_name)
                || plan.contains_segment(preferred_name)
            {
                continue;
            }
            if let Some(replica) = self.allocate_from_segment_name(preferred_name, slice_size) {
                plan.push(replica);
            }
        }

        if plan.is_complete() {
            return plan.into_replicas();
        }

        match self.strategy {
            AllocationStrategy::Random => {
                self.allocate_random_remaining(slice_size, &mut plan, excluded_segments);
            }
            AllocationStrategy::FreeRatioFirst => {
                self.allocate_free_ratio_remaining(slice_size, &mut plan, excluded_segments);
            }
            AllocationStrategy::SsdFreeRatioFirst => {
                self.allocate_ssd_free_ratio_remaining(
                    slice_size,
                    &mut plan,
                    excluded_segments,
                    ssd_metrics,
                );
            }
            AllocationStrategy::LocalFirst => {
                self.allocate_local_first_remaining(
                    key,
                    client_id,
                    &config.host_id,
                    slice_size,
                    &mut plan,
                    excluded_segments,
                );
            }
            AllocationStrategy::Cxl => unreachable!("CXL allocation returned above"),
        }
        plan.into_replicas()
    }

    fn allocate_cxl(
        &mut self,
        preferred_segment_name: &str,
        slice_size: u64,
    ) -> Option<ReplicaDescriptor> {
        let alias = self
            .segments
            .values()
            .find(|state| {
                state.runtime_bound
                    && state.segment.protocol == "cxl"
                    && state.segment.name == preferred_segment_name
            })
            .map(|state| state.segment.clone())?;
        let global = self.cxl_global.as_mut()?;
        let (offset, accounted_size) = global.allocate(slice_size)?;
        global.used = global.used.saturating_add(accounted_size);
        Some(ReplicaDescriptor {
            refcnt: 0,
            handle_valid: true,
            segment_id: alias.id,
            segment_name: alias.name,
            offset,
            size: slice_size,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: None,
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            // CXL target offsets are device-relative. The native CXL transport
            // adds its process-local mmap base internally.
            base_addr: 0,
            protocol: "cxl".to_string(),
        })
    }

    /// Release a set of replicas, returning their space to the respective segments.
    /// 释放一组副本，将占用的空间归还给各自的 segment。
    ///
    /// - Offset mode: triggers free range insertion and merging (insert_free_range).
    ///   对于 Offset 模式，归还会触发空闲区间合并（insert_free_range）。
    /// - Cachelib mode: marks the allocation as freed via release_cachelib.
    ///   对于 Cachelib 模式，通过 release_cachelib 标记分配为已释放。
    pub fn validate_release(&self, replicas: &[ReplicaDescriptor]) -> Result<(), String> {
        let mut targets = HashSet::with_capacity(replicas.len());
        for replica in replicas {
            let target = if replica.protocol == "cxl" {
                let alias = self.segments.get(&replica.segment_id).ok_or_else(|| {
                    format!(
                        "CXL release references missing alias segment {}",
                        replica.segment_id
                    )
                })?;
                if alias.segment.protocol != "cxl"
                    || alias.segment.name != replica.segment_name
                    || !alias.runtime_bound
                {
                    return Err(format!(
                        "CXL release descriptor does not match a bound alias: segment={} name={:?}",
                        replica.segment_id, replica.segment_name
                    ));
                }
                self.cxl_global
                    .as_ref()
                    .ok_or_else(|| "CXL release has no global allocator".to_string())?
                    .validate_release(replica)?;
                (true, Uuid::nil(), replica.offset)
            } else {
                let state = self.segments.get(&replica.segment_id).ok_or_else(|| {
                    format!(
                        "release references missing segment {} at offset {}",
                        replica.segment_id, replica.offset
                    )
                })?;
                if state.segment.name != replica.segment_name {
                    return Err(format!(
                        "release segment name mismatch for {}: descriptor={:?}, allocator={:?}",
                        replica.segment_id, replica.segment_name, state.segment.name
                    ));
                }
                state.validate_release(replica)?;
                (false, replica.segment_id, replica.offset)
            };
            if !targets.insert(target) {
                return Err(format!(
                    "release batch contains duplicate allocation: segment={} offset={}",
                    replica.segment_id, replica.offset
                ));
            }
        }
        Ok(())
    }

    pub fn release(&mut self, replicas: &[ReplicaDescriptor]) -> Result<(), String> {
        self.validate_release(replicas)?;
        for replica in replicas {
            if replica.protocol == "cxl" {
                let global = self
                    .cxl_global
                    .as_mut()
                    .expect("CXL allocator validated before release");
                let released_size = global
                    .release(replica)
                    .expect("CXL allocation validated before release");
                global.used = global
                    .used
                    .checked_sub(released_size)
                    .expect("validated CXL usage must cover released allocation");
                continue;
            }
            let state = self
                .segments
                .get_mut(&replica.segment_id)
                .expect("segment validated before release");
            let released_size = state
                .release(replica)
                .expect("allocation validated before release");
            state.used = state
                .used
                .checked_sub(released_size)
                .expect("validated segment usage must cover released allocation");
        }
        Ok(())
    }

    /// Get used bytes for a specific segment.
    /// 获取特定 segment 的已用字节数。
    pub fn used_bytes(&self, segment_id: &Uuid) -> Option<u64> {
        self.segments.get(segment_id).map(|state| {
            if state.segment.protocol == "cxl" {
                self.cxl_global.as_ref().map_or(0, |global| global.used)
            } else {
                state.used
            }
        })
    }

    /// Get total memory usage across all segments: (total_capacity, total_used).
    /// 获取所有 segment 的内存使用总计：(总容量, 总已用)。
    pub fn usage_totals(&self) -> (u64, u64) {
        let regular = self
            .segments
            .values()
            .filter(|state| state.segment.protocol != "cxl")
            .fold((0_u64, 0_u64), |(total, used), state| {
                (
                    total.saturating_add(state.segment.size),
                    used.saturating_add(state.used),
                )
            });
        self.cxl_global.as_ref().map_or(regular, |global| {
            (
                regular.0.saturating_add(global.segment.size),
                regular.1.saturating_add(global.used),
            )
        })
    }
}

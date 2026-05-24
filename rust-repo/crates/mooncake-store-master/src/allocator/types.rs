use super::cachelib::{cachelib_class_size, generate_cachelib_class_sizes};

pub const CACHELIB_SLAB_SIZE: u64 = 1 << 24;
pub const CACHELIB_MIN_ALLOC_SIZE: u64 = 1 << 6;
pub(super) const CACHELIB_ALIGNMENT: u64 = 8;
pub(super) const CACHELIB_CLASS_MIN_SIZE: u64 = 72;
pub(super) const CACHELIB_MAX_ALLOC_SIZE: u64 = CACHELIB_SLAB_SIZE - 16;
pub type PoolId = u8;
pub type ClassId = u16;
pub(super) const DEFAULT_CACHELIB_POOL_NAME: &str = "main";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlabReleaseMode {
    Resize,
    Rebalance,
}

#[derive(Debug, Clone)]
pub struct SlabReleaseContext {
    pub token: u64,
    pub pool_id: PoolId,
    pub victim_class_size: Option<u64>,
    pub receiver_class_size: Option<u64>,
    pub slab_index: u32,
    pub mode: SlabReleaseMode,
    pub active_offsets: Vec<u64>,
    pub is_released: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachelibAllocInfo {
    pub pool_id: PoolId,
    pub class_id: ClassId,
    pub alloc_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachelibAllocationVisit {
    pub offset: u64,
    pub info: CachelibAllocInfo,
    pub allocated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationStrategy {
    Random,
    FreeRatioFirst,
}

impl AllocationStrategy {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "random" => Some(Self::Random),
            "free_ratio_first" => Some(Self::FreeRatioFirst),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAllocatorKind {
    Offset,
    CachelibLike,
}

impl MemoryAllocatorKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "offset" => Some(Self::Offset),
            "cachelib" | "cachelib-like" => Some(Self::CachelibLike),
            _ => None,
        }
    }
}

pub fn cachelib_allocation_class_size_for_request(requested_size: u64) -> Option<u64> {
    cachelib_class_size(&generate_cachelib_class_sizes(), requested_size)
}

pub fn cachelib_allocation_class_id_for_request(requested_size: u64) -> Option<ClassId> {
    let class_sizes = generate_cachelib_class_sizes();
    let class_size = cachelib_class_size(&class_sizes, requested_size)?;
    class_sizes
        .iter()
        .position(|size| *size == class_size)
        .map(|idx| idx as ClassId)
}

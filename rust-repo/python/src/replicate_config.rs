// =============================================================================
// ReplicateConfig Python bindings — 副本/分配策略配置的 Python 绑定
// Python wrapper for replication and allocation strategy configuration
// =============================================================================
//
// ReplicateConfig controls how objects are replicated and where they are
// placed across the Mooncake cluster. It is the per-operation configuration
// (passed to put/upsert/etc.) that governs data durability and affinity.
//
// ReplicateConfig 控制对象如何在 Mooncake 集群中复制和放置。它是每次操作
// 级别的配置（传递给 put/upsert 等），管理数据持久性和亲和性。
//
// Key concepts (关键概念):
// - replica_num:     How many total copies (primary + replicas). 总副本数。
// - nof_replica_num: Number of "no-fault" replicas (placed on different fault
//                    domains). 跨故障域的副本数量。
// - soft_pin / hard_pin: Soft-pinned objects can be evicted under memory
//                    pressure; hard-pinned objects never are. Soft=可被驱逐；
//                    Hard=永久驻留。
// - preferred_segment / preferred_segments: Hint for which memory segments to
//                    allocate on. 指定优先分配的内存段。
// - data_type:       Semantic type of data (Kvcache, Weight, etc.) used by
//                    the eviction policy to make smarter decisions.
//                    数据的语义类型，用于驱逐策略做智能决策。
// - group_ids:       Optional per-key group routing ids for C++ parity.
//                    分组路由 ID；batch 操作时数量必须与 keys 数量一致。

use mooncake_store_core::{ObjectDataType, ReplicateConfig};
use pyo3::prelude::*;

/// Python-visible replication and allocation configuration.
///
/// Python 侧的复制与分配策略配置。
///
/// Fields (字段说明):
/// - replica_num:    Number of replicas to create. 默认 1。
///                   创建的副本数量（主副本 + 额外副本）。
/// - nof_replica_num: Number of replicas on different fault domains.
///                    跨不同故障域的副本数量。默认 0。
/// - with_soft_pin:  Soft-pin this object (may be evicted under pressure).
///                   Soft-pin 标记（内存压力下可被驱逐）。默认 false。
/// - with_hard_pin:  Hard-pin this object (never evicted).
///                   Hard-pin 标记（永不被驱逐）。默认 false。
/// - preferred_segment: Preferred segment for allocation (single).
///                      优先分配的单个段名称。
/// - prefer_alloc_in_same_node: Allocate all replicas on the same node.
///                              所有副本分配在同一节点。默认 false。
/// - preferred_segments:    Preferred segments for allocation (multiple).
///                          优先分配的多个段名称。
/// - preferred_nof_segments: Preferred no-fault segments.
///                           优先分配的无故障段。
/// - data_type:  Object data type enum value (ObjectDataType):
///   数据对象的语义类型枚举值:
///     0 = Unknown (未知)
///     1 = Kvcache (KV 缓存)
///     2 = Tensor (张量)
///     3 = Weight (模型权重)
///     4 = Sample (训练样本)
///     5 = Activation (激活值)
///     6 = Gradient (梯度)
///     7 = OptimizerState (优化器状态)
///     8 = Metadata (元数据)
///     9 = General (通用)
/// - group_ids: Optional per-key group ids for group routing / leases.
#[pyclass(name = "ReplicateConfig", from_py_object)]
#[derive(Clone)]
pub(crate) struct ReplicateConfigPy {
    #[pyo3(get, set)]
    pub replica_num: u32,
    #[pyo3(get, set)]
    pub nof_replica_num: u32,
    #[pyo3(get, set)]
    pub with_soft_pin: bool,
    #[pyo3(get, set)]
    pub with_hard_pin: bool,
    #[pyo3(get, set)]
    pub preferred_segment: String,
    #[pyo3(get, set)]
    pub prefer_alloc_in_same_node: bool,
    #[pyo3(get, set)]
    pub preferred_segments: Vec<String>,
    #[pyo3(get, set)]
    pub preferred_nof_segments: Vec<String>,
    /// ObjectDataType enum value:
    ///   0=Unknown, 1=Kvcache, 2=Tensor, 3=Weight, 4=Sample,
    ///   5=Activation, 6=Gradient, 7=OptimizerState, 8=Metadata, 9=General
    /// 对象数据类型枚举值（见上方说明）
    #[pyo3(get, set)]
    pub data_type: i32,
    #[pyo3(get, set)]
    pub group_ids: Vec<String>,
}

#[pymethods]
impl ReplicateConfigPy {
    /// Create a new ReplicateConfig with sensible defaults:
    /// 1 replica, no pins, no preferred segments, Unknown data type.
    /// 创建新的 ReplicateConfig，默认：1 个副本，无 pin，无首选段，Unknown 类型。
    #[new]
    #[pyo3(signature = (
        replica_num = 1,
        nof_replica_num = 0,
        with_soft_pin = false,
        with_hard_pin = false,
        preferred_segment = String::new(),
        prefer_alloc_in_same_node = false,
        preferred_segments = vec![],
        preferred_nof_segments = vec![],
        data_type = 0,
        group_ids = vec![],
    ))]
    fn new(
        replica_num: u32,
        nof_replica_num: u32,
        with_soft_pin: bool,
        with_hard_pin: bool,
        preferred_segment: String,
        prefer_alloc_in_same_node: bool,
        preferred_segments: Vec<String>,
        preferred_nof_segments: Vec<String>,
        data_type: i32,
        group_ids: Vec<String>,
    ) -> Self {
        Self {
            replica_num,
            nof_replica_num,
            with_soft_pin,
            with_hard_pin,
            preferred_segment,
            prefer_alloc_in_same_node,
            preferred_segments,
            preferred_nof_segments,
            data_type,
            group_ids,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ReplicateConfig(replica_num={}, nof_replica_num={}, data_type={}, preferred_segment='{}', group_ids={:?})",
            self.replica_num,
            self.nof_replica_num,
            self.data_type,
            self.preferred_segment,
            self.group_ids
        )
    }
}

impl ReplicateConfigPy {
    /// Convert Python-side ReplicateConfig to the Rust core ReplicateConfig.
    ///
    /// 将 Python 侧 ReplicateConfig 转换为 Rust 核心 ReplicateConfig。
    /// The data_type i32 is mapped to the ObjectDataType enum via match.
    /// Unknown values default to ObjectDataType::Unknown.
    /// data_type i32 通过 match 映射到 ObjectDataType 枚举。
    /// 未知值默认为 ObjectDataType::Unknown。
    pub(crate) fn to_core(&self) -> ReplicateConfig {
        ReplicateConfig {
            replica_num: self.replica_num,
            nof_replica_num: self.nof_replica_num,
            with_soft_pin: self.with_soft_pin,
            with_hard_pin: self.with_hard_pin,
            preferred_segment: self.preferred_segment.clone(),
            preferred_segments: self.preferred_segments.clone(),
            preferred_nof_segments: self.preferred_nof_segments.clone(),
            prefer_alloc_in_same_node: self.prefer_alloc_in_same_node,
            data_type: match self.data_type {
                1 => ObjectDataType::Kvcache,
                2 => ObjectDataType::Tensor,
                3 => ObjectDataType::Weight,
                4 => ObjectDataType::Sample,
                5 => ObjectDataType::Activation,
                6 => ObjectDataType::Gradient,
                7 => ObjectDataType::OptimizerState,
                8 => ObjectDataType::Metadata,
                9 => ObjectDataType::General,
                _ => ObjectDataType::Unknown,
            },
            group_ids: self.group_ids.clone(),
        }
    }
}

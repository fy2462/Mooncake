use mooncake_store_core::{ObjectDataType, ReplicateConfig};
use pyo3::prelude::*;

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
    #[pyo3(get, set)]
    pub data_type: i32,
}

#[pymethods]
impl ReplicateConfigPy {
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
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ReplicateConfig(replica_num={}, nof_replica_num={}, data_type={}, preferred_segment='{}')",
            self.replica_num, self.nof_replica_num, self.data_type, self.preferred_segment
        )
    }
}

impl ReplicateConfigPy {
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
        }
    }
}

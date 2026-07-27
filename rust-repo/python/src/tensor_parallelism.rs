//! Python-facing tensor-parallel request types and C++ wire/key compatibility.
//!
//! This module is an adapter: it owns no Store state. Tensor objects and
//! manifests are persisted through `MooncakeClient`, while this layer validates
//! Python requests and reproduces the C++ integration key/wire conventions.

use crate::to_py_err;
use pyo3::prelude::*;
use std::collections::HashSet;

const WRITER_MANIFEST_MAGIC: u32 = 0x574d_414e;
const WRITER_MANIFEST_VERSION: u16 = 1;
const WRITER_MANIFEST_SIZE: usize = 96;
const MAX_TENSOR_DIMS: usize = 8;
const MAX_LAYOUT_AXES: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AxisKind {
    Dp,
    Tp,
    Ep,
    Pp,
}

impl AxisKind {
    fn parse(value: &str) -> PyResult<Self> {
        match value.to_ascii_uppercase().as_str() {
            "DP" => Ok(Self::Dp),
            "TP" => Ok(Self::Tp),
            "EP" => Ok(Self::Ep),
            "PP" => Ok(Self::Pp),
            _ => Err(to_py_err(format!(
                "unsupported tensor parallel axis kind {value}"
            ))),
        }
    }

    fn canonical_name(self) -> &'static str {
        match self {
            Self::Dp => "dp",
            Self::Tp => "tp",
            Self::Ep => "ep",
            Self::Pp => "pp",
        }
    }

    fn canonical_order(self) -> u8 {
        match self {
            Self::Dp => 0,
            Self::Tp => 1,
            Self::Ep => 2,
            Self::Pp => 3,
        }
    }

    pub(crate) fn wire_value(self) -> i32 {
        match self {
            Self::Dp => 0,
            Self::Tp => 1,
            Self::Ep => 2,
            Self::Pp => 3,
        }
    }
}

#[pyclass(name = "ParallelAxis", from_py_object)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParallelAxisPy {
    #[pyo3(get, set)]
    pub kind: String,
    #[pyo3(get, set)]
    pub rank: i32,
    #[pyo3(get, set)]
    pub size: i32,
    #[pyo3(get, set)]
    pub split_dim: Option<i32>,
    #[pyo3(get, set)]
    pub expert_id: Option<i32>,
    #[pyo3(get, set)]
    pub stage_id: Option<i32>,
}

#[pymethods]
impl ParallelAxisPy {
    #[new]
    #[pyo3(signature = (
        kind = String::new(),
        rank = 0,
        size = 1,
        split_dim = None,
        expert_id = None,
        stage_id = None
    ))]
    fn new(
        kind: String,
        rank: i32,
        size: i32,
        split_dim: Option<i32>,
        expert_id: Option<i32>,
        stage_id: Option<i32>,
    ) -> Self {
        Self {
            kind,
            rank,
            size,
            split_dim,
            expert_id,
            stage_id,
        }
    }
}

impl ParallelAxisPy {
    pub(crate) fn parsed_kind(&self) -> PyResult<AxisKind> {
        AxisKind::parse(&self.kind)
    }

    fn validate(&self) -> PyResult<AxisKind> {
        let kind = self.parsed_kind()?;
        if self.size <= 0 || self.rank < 0 || self.rank >= self.size {
            return Err(to_py_err(format!(
                "invalid rank/size for tensor parallel axis {}",
                self.kind
            )));
        }
        match kind {
            AxisKind::Tp if self.split_dim.is_none() => {
                return Err(to_py_err("TP axis requires split_dim"));
            }
            AxisKind::Tp => {}
            AxisKind::Dp if self.split_dim.is_some() => {
                return Err(to_py_err("DP axis must not provide split_dim"));
            }
            AxisKind::Dp => {}
            AxisKind::Ep if self.expert_id.is_none() => {
                return Err(to_py_err("EP axis requires expert_id"));
            }
            AxisKind::Ep if self.split_dim.is_some() => {
                return Err(to_py_err("EP axis must not provide split_dim"));
            }
            AxisKind::Ep => {}
            AxisKind::Pp if self.stage_id.is_none() => {
                return Err(to_py_err("PP axis requires stage_id"));
            }
            AxisKind::Pp if self.split_dim.is_some() => {
                return Err(to_py_err("PP axis must not provide split_dim"));
            }
            AxisKind::Pp => {}
        }
        Ok(kind)
    }
}

#[pyclass(name = "TensorParallelism", from_py_object)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TensorParallelismPy {
    #[pyo3(get, set)]
    pub axes: Vec<ParallelAxisPy>,
}

#[pymethods]
impl TensorParallelismPy {
    #[new]
    #[pyo3(signature = (axes = Vec::new()))]
    fn new(axes: Vec<ParallelAxisPy>) -> Self {
        Self { axes }
    }
}

impl TensorParallelismPy {
    pub(crate) fn validated(&self, allow_empty: bool) -> PyResult<Self> {
        if self.axes.is_empty() && !allow_empty {
            return Err(to_py_err(
                "TensorParallelism must contain at least one axis",
            ));
        }
        if self.axes.len() > MAX_LAYOUT_AXES {
            return Err(to_py_err(format!(
                "TensorParallelism supports at most {MAX_LAYOUT_AXES} axes"
            )));
        }
        let mut seen = HashSet::new();
        let mut axes = self.axes.clone();
        for axis in &mut axes {
            let kind = axis.validate()?;
            if !seen.insert(kind) {
                return Err(to_py_err(
                    "duplicate tensor parallel axis kinds are unsupported",
                ));
            }
            axis.kind = kind.canonical_name().to_string();
        }
        Ok(Self { axes })
    }

    pub(crate) fn validated_canonical(&self, allow_empty: bool) -> PyResult<Self> {
        let mut axes = self.validated(allow_empty)?.axes;
        axes.sort_by_key(|axis| {
            axis.parsed_kind()
                .map(AxisKind::canonical_order)
                .unwrap_or(u8::MAX)
        });
        Ok(Self { axes })
    }

    pub(crate) fn tp_axis_index(&self) -> PyResult<Option<usize>> {
        self.axes
            .iter()
            .enumerate()
            .find_map(|(index, axis)| match axis.parsed_kind() {
                Ok(AxisKind::Tp) => Some(Ok(index)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .transpose()
    }
}

#[pyclass(name = "ReadTarget", from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct ReadTargetPy {
    mode: String,
    #[pyo3(get, set)]
    pub parallelism: Option<TensorParallelismPy>,
}

#[pymethods]
impl ReadTargetPy {
    #[new]
    #[pyo3(signature = (mode = "as_stored", parallelism = None))]
    fn new(mode: &str, parallelism: Option<TensorParallelismPy>) -> PyResult<Self> {
        let mut target = Self {
            mode: String::new(),
            parallelism,
        };
        target.set_mode(mode)?;
        Ok(target)
    }

    #[getter]
    fn mode(&self) -> &str {
        &self.mode
    }

    #[setter]
    fn set_mode(&mut self, mode: &str) -> PyResult<()> {
        self.mode = match mode.to_ascii_uppercase().as_str() {
            "AS_STORED" => "as_stored",
            "SHARD" => "shard",
            "FULL" => "full",
            _ => return Err(to_py_err(format!("unsupported ReadTarget mode {mode}"))),
        }
        .to_string();
        Ok(())
    }
}

impl ReadTargetPy {
    pub(crate) fn mode_name(&self) -> &str {
        &self.mode
    }
}

#[pyclass(name = "WriterPartition", from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct WriterPartitionPy {
    #[pyo3(get, set)]
    pub rank: i32,
    #[pyo3(get, set)]
    pub size: i32,
    #[pyo3(get, set)]
    pub split_dim: i32,
}

#[pymethods]
impl WriterPartitionPy {
    #[new]
    #[pyo3(signature = (rank = 0, size = 1, split_dim = 0))]
    fn new(rank: i32, size: i32, split_dim: i32) -> Self {
        Self {
            rank,
            size,
            split_dim,
        }
    }
}

impl WriterPartitionPy {
    pub(crate) fn validate(&self, ndim: usize) -> PyResult<()> {
        if self.size <= 0 || self.rank < 0 || self.rank >= self.size {
            return Err(to_py_err("invalid writer partition rank/size"));
        }
        let split_dim = usize::try_from(self.split_dim)
            .map_err(|_| to_py_err("writer partition split_dim must be non-negative"))?;
        if split_dim >= ndim {
            return Err(to_py_err(
                "writer partition split_dim is outside tensor rank",
            ));
        }
        Ok(())
    }
}

pub(crate) fn parallelism_key(
    base_key: &str,
    parallelism: &TensorParallelismPy,
) -> PyResult<String> {
    let parallelism = parallelism.validated_canonical(false)?;
    if parallelism.axes.len() == 1 && parallelism.axes[0].parsed_kind()? == AxisKind::Tp {
        return Ok(format!("{base_key}_tp_{}", parallelism.axes[0].rank));
    }
    let mut key = base_key.to_string();
    for axis in &parallelism.axes {
        let kind = axis.parsed_kind()?;
        key.push_str("__");
        key.push_str(kind.canonical_name());
        key.push('_');
        key.push_str(&format!("{}of{}", axis.rank, axis.size));
        if let Some(split_dim) = axis.split_dim {
            key.push_str(&format!("_sd{split_dim}"));
        }
        if let Some(expert_id) = axis.expert_id {
            key.push_str(&format!("_eid{expert_id}"));
        }
        if let Some(stage_id) = axis.stage_id {
            key.push_str(&format!("_sid{stage_id}"));
        }
    }
    Ok(key)
}

pub(crate) fn writer_manifest_key(base_key: &str) -> String {
    format!("{base_key}__writer_manifest")
}

pub(crate) fn parallelism_manifest_key(base_key: &str) -> String {
    format!("{base_key}__parallelism_manifest")
}

pub(crate) fn writer_shard_key(base_key: &str, writer: &WriterPartitionPy) -> String {
    format!(
        "{base_key}__writer_{}of{}_sd{}",
        writer.rank, writer.size, writer.split_dim
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShardManifest {
    pub dtype: i32,
    pub global_shape: Vec<i64>,
    pub split_dim: usize,
    pub shard_count: usize,
}

impl ShardManifest {
    pub(crate) fn encode(&self) -> PyResult<[u8; WRITER_MANIFEST_SIZE]> {
        if self.global_shape.len() > MAX_TENSOR_DIMS
            || self.shard_count == 0
            || self.split_dim >= self.global_shape.len()
            || self.global_shape.iter().any(|dimension| *dimension < 0)
        {
            return Err(to_py_err("invalid tensor shard manifest"));
        }
        let ndim = i32::try_from(self.global_shape.len())
            .map_err(|_| to_py_err("manifest ndim exceeds i32"))?;
        let split_dim =
            i32::try_from(self.split_dim).map_err(|_| to_py_err("split_dim exceeds i32"))?;
        let shard_count =
            i32::try_from(self.shard_count).map_err(|_| to_py_err("shard_count exceeds i32"))?;
        let mut bytes = [0_u8; WRITER_MANIFEST_SIZE];
        bytes[0..4].copy_from_slice(&WRITER_MANIFEST_MAGIC.to_le_bytes());
        bytes[4..6].copy_from_slice(&WRITER_MANIFEST_VERSION.to_le_bytes());
        bytes[6..8].copy_from_slice(&(WRITER_MANIFEST_SIZE as u16).to_le_bytes());
        bytes[8..12].copy_from_slice(&self.dtype.to_le_bytes());
        bytes[12..16].copy_from_slice(&ndim.to_le_bytes());
        bytes[16..20].copy_from_slice(&split_dim.to_le_bytes());
        bytes[20..24].copy_from_slice(&shard_count.to_le_bytes());
        for index in 0..MAX_TENSOR_DIMS {
            let dimension = self.global_shape.get(index).copied().unwrap_or(-1);
            let offset = 32 + index * 8;
            bytes[offset..offset + 8].copy_from_slice(&dimension.to_le_bytes());
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> PyResult<Self> {
        if bytes.len() < WRITER_MANIFEST_SIZE
            || u32::from_le_bytes(bytes[0..4].try_into().expect("fixed slice"))
                != WRITER_MANIFEST_MAGIC
            || u16::from_le_bytes(bytes[4..6].try_into().expect("fixed slice"))
                != WRITER_MANIFEST_VERSION
            || usize::from(u16::from_le_bytes(
                bytes[6..8].try_into().expect("fixed slice"),
            )) != WRITER_MANIFEST_SIZE
        {
            return Err(to_py_err("invalid tensor shard manifest header"));
        }
        let dtype = i32::from_le_bytes(bytes[8..12].try_into().expect("fixed slice"));
        let ndim = usize::try_from(i32::from_le_bytes(
            bytes[12..16].try_into().expect("fixed slice"),
        ))
        .map_err(|_| to_py_err("manifest ndim is negative"))?;
        let split_dim = usize::try_from(i32::from_le_bytes(
            bytes[16..20].try_into().expect("fixed slice"),
        ))
        .map_err(|_| to_py_err("manifest split_dim is negative"))?;
        let shard_count = usize::try_from(i32::from_le_bytes(
            bytes[20..24].try_into().expect("fixed slice"),
        ))
        .map_err(|_| to_py_err("manifest shard_count is negative"))?;
        if ndim > MAX_TENSOR_DIMS || shard_count == 0 || split_dim >= ndim {
            return Err(to_py_err("invalid tensor shard manifest dimensions"));
        }
        let mut global_shape = Vec::with_capacity(ndim);
        for index in 0..MAX_TENSOR_DIMS {
            let offset = 32 + index * 8;
            let dimension =
                i64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed slice"));
            if index < ndim {
                if dimension < 0 {
                    return Err(to_py_err("manifest contains a negative active dimension"));
                }
                global_shape.push(dimension);
            } else if dimension != -1 {
                return Err(to_py_err("manifest unused dimensions must be -1"));
            }
        }
        Ok(Self {
            dtype,
            global_shape,
            split_dim,
            shard_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_parallelism_key_matches_cpp_axis_order() {
        let parallelism = TensorParallelismPy {
            axes: vec![
                ParallelAxisPy {
                    kind: "pp".to_string(),
                    rank: 1,
                    size: 2,
                    split_dim: None,
                    expert_id: None,
                    stage_id: Some(7),
                },
                ParallelAxisPy {
                    kind: "dp".to_string(),
                    rank: 3,
                    size: 4,
                    split_dim: None,
                    expert_id: None,
                    stage_id: None,
                },
            ],
        };
        assert_eq!(
            parallelism_key("tensor", &parallelism).unwrap(),
            "tensor__dp_3of4__pp_1of2_sid7"
        );
    }

    #[test]
    fn shard_manifest_round_trip_preserves_cpp_layout() {
        let manifest = ShardManifest {
            dtype: 10,
            global_shape: vec![16, 8],
            split_dim: 0,
            shard_count: 4,
        };
        let bytes = manifest.encode().unwrap();
        assert_eq!(bytes.len(), WRITER_MANIFEST_SIZE);
        assert_eq!(&bytes[28..32], &[0; 4]);
        assert_eq!(ShardManifest::decode(&bytes).unwrap(), manifest);
    }
}

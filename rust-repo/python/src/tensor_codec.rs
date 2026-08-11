use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyTuple};

use crate::tensor_parallelism::{
    AxisKind, ParallelAxisPy, ShardManifest, TensorParallelismPy, WriterPartitionPy,
};

const TENSOR_OBJECT_MAGIC: u32 = 0x4d4f4f4e;
const TENSOR_OBJECT_VERSION: u16 = 1;
const TENSOR_METADATA_SIZE: usize = 304;
const MAX_TENSOR_DIMS: usize = 8;
const MAX_LAYOUT_AXES: usize = 4;
const FULL_LAYOUT: u32 = 0;
const SHARD_LAYOUT: u32 = 1;
const TP_AXIS_KIND: i32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TensorDtype {
    Float32 = 0,
    Float64 = 1,
    Int8 = 2,
    Uint8 = 3,
    Int16 = 4,
    Uint16 = 5,
    Int32 = 6,
    Uint32 = 7,
    Int64 = 8,
    Uint64 = 9,
    Bool = 10,
    Float16 = 11,
    Bfloat16 = 12,
    Float8E4m3 = 13,
    Float8E5m2 = 14,
}

impl TensorDtype {
    fn from_torch_name(name: &str) -> Option<Self> {
        Some(match name {
            "torch.float32" => Self::Float32,
            "torch.float64" => Self::Float64,
            "torch.int8" => Self::Int8,
            "torch.uint8" => Self::Uint8,
            "torch.int16" => Self::Int16,
            "torch.uint16" => Self::Uint16,
            "torch.int32" => Self::Int32,
            "torch.uint32" => Self::Uint32,
            "torch.int64" => Self::Int64,
            "torch.uint64" => Self::Uint64,
            "torch.bool" => Self::Bool,
            "torch.float16" => Self::Float16,
            "torch.bfloat16" => Self::Bfloat16,
            "torch.float8_e4m3fn" => Self::Float8E4m3,
            "torch.float8_e5m2" => Self::Float8E5m2,
            _ => return None,
        })
    }

    fn from_wire(value: i32) -> Option<Self> {
        Some(match value {
            0 => Self::Float32,
            1 => Self::Float64,
            2 => Self::Int8,
            3 => Self::Uint8,
            4 => Self::Int16,
            5 => Self::Uint16,
            6 => Self::Int32,
            7 => Self::Uint32,
            8 => Self::Int64,
            9 => Self::Uint64,
            10 => Self::Bool,
            11 => Self::Float16,
            12 => Self::Bfloat16,
            13 => Self::Float8E4m3,
            14 => Self::Float8E5m2,
            _ => return None,
        })
    }

    fn torch_attr(self) -> &'static str {
        match self {
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Int8 => "int8",
            Self::Uint8 => "uint8",
            Self::Int16 => "int16",
            Self::Uint16 => "uint16",
            Self::Int32 => "int32",
            Self::Uint32 => "uint32",
            Self::Int64 => "int64",
            Self::Uint64 => "uint64",
            Self::Bool => "bool",
            Self::Float16 => "float16",
            Self::Bfloat16 => "bfloat16",
            Self::Float8E4m3 => "float8_e4m3fn",
            Self::Float8E5m2 => "float8_e5m2",
        }
    }

    fn element_size(self) -> usize {
        match self {
            Self::Float64 | Self::Int64 | Self::Uint64 => 8,
            Self::Float32 | Self::Int32 | Self::Uint32 => 4,
            Self::Int16 | Self::Uint16 | Self::Float16 | Self::Bfloat16 => 2,
            Self::Int8 | Self::Uint8 | Self::Bool | Self::Float8E4m3 | Self::Float8E5m2 => 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedTensorMetadata {
    dtype: TensorDtype,
    shape: Vec<i64>,
    data_offset: usize,
    data_bytes: usize,
}

struct SerializedTensorParts {
    metadata: [u8; TENSOR_METADATA_SIZE],
    data_ptr: usize,
    data_bytes: usize,
    owner: Py<PyAny>,
}

fn invalid_metadata(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_i32(bytes: &mut [u8], offset: usize, value: i32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_i64(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("fixed slice"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed slice"))
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed slice"))
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed slice"))
}

fn checked_tensor_bytes(shape: &[i64], element_size: usize) -> PyResult<usize> {
    let mut numel = 1usize;
    for &dim in shape {
        let dim = usize::try_from(dim)
            .map_err(|_| invalid_metadata("tensor shape dimensions must be non-negative"))?;
        numel = numel
            .checked_mul(dim)
            .ok_or_else(|| invalid_metadata("tensor element count overflows usize"))?;
    }
    numel
        .checked_mul(element_size)
        .ok_or_else(|| invalid_metadata("tensor byte size overflows usize"))
}

fn encode_full_metadata(
    dtype: TensorDtype,
    shape: &[i64],
    data_bytes: usize,
) -> PyResult<[u8; TENSOR_METADATA_SIZE]> {
    if !cfg!(target_endian = "little") {
        return Err(invalid_metadata(
            "C++ TensorMetadata native ABI is only supported on little-endian hosts",
        ));
    }
    if shape.len() > MAX_TENSOR_DIMS {
        return Err(invalid_metadata(format!(
            "tensor has {} dimensions; maximum is {MAX_TENSOR_DIMS}",
            shape.len()
        )));
    }
    let expected = checked_tensor_bytes(shape, dtype.element_size())?;
    if expected != data_bytes {
        return Err(invalid_metadata(format!(
            "tensor payload size {data_bytes} does not match shape/dtype size {expected}"
        )));
    }

    let mut bytes = [0u8; TENSOR_METADATA_SIZE];
    put_u32(&mut bytes, 0, TENSOR_OBJECT_MAGIC);
    put_u16(&mut bytes, 4, TENSOR_OBJECT_VERSION);
    put_u16(&mut bytes, 6, TENSOR_METADATA_SIZE as u16);
    put_i32(&mut bytes, 8, dtype as i32);
    put_i32(&mut bytes, 12, shape.len() as i32);
    put_u32(&mut bytes, 16, FULL_LAYOUT);
    put_u64(&mut bytes, 24, TENSOR_METADATA_SIZE as u64);
    put_u64(&mut bytes, 32, data_bytes as u64);
    for index in 0..MAX_TENSOR_DIMS {
        let dim = shape.get(index).copied().unwrap_or(-1);
        put_i64(&mut bytes, 40 + index * 8, dim);
        put_i64(&mut bytes, 104 + index * 8, dim);
    }
    Ok(bytes)
}

fn encode_tp_shard_metadata(
    dtype: TensorDtype,
    global_shape: &[i64],
    local_shape: &[i64],
    rank: usize,
    shard_count: usize,
    split_dim: usize,
    data_bytes: usize,
) -> PyResult<[u8; TENSOR_METADATA_SIZE]> {
    if global_shape.len() != local_shape.len() || global_shape.len() > MAX_TENSOR_DIMS {
        return Err(invalid_metadata(
            "TP global/local shapes must have the same supported rank",
        ));
    }
    if split_dim >= global_shape.len() || shard_count == 0 || rank >= shard_count {
        return Err(invalid_metadata(
            "invalid TP rank, size, or split dimension",
        ));
    }
    let shard_count_i32 =
        i32::try_from(shard_count).map_err(|_| invalid_metadata("TP size exceeds i32"))?;
    let rank_i32 = i32::try_from(rank).map_err(|_| invalid_metadata("TP rank exceeds i32"))?;
    let split_dim_i32 =
        i32::try_from(split_dim).map_err(|_| invalid_metadata("TP split_dim exceeds i32"))?;
    let expected = checked_tensor_bytes(local_shape, dtype.element_size())?;
    if expected != data_bytes {
        return Err(invalid_metadata(
            "TP shard payload size does not match local shape/dtype",
        ));
    }

    let mut bytes = [0_u8; TENSOR_METADATA_SIZE];
    put_u32(&mut bytes, 0, TENSOR_OBJECT_MAGIC);
    put_u16(&mut bytes, 4, TENSOR_OBJECT_VERSION);
    put_u16(&mut bytes, 6, TENSOR_METADATA_SIZE as u16);
    put_i32(&mut bytes, 8, dtype as i32);
    put_i32(&mut bytes, 12, global_shape.len() as i32);
    put_u32(&mut bytes, 16, SHARD_LAYOUT);
    put_u64(&mut bytes, 24, TENSOR_METADATA_SIZE as u64);
    put_u64(&mut bytes, 32, data_bytes as u64);
    for index in 0..MAX_TENSOR_DIMS {
        put_i64(
            &mut bytes,
            40 + index * 8,
            global_shape.get(index).copied().unwrap_or(-1),
        );
        put_i64(
            &mut bytes,
            104 + index * 8,
            local_shape.get(index).copied().unwrap_or(-1),
        );
    }
    put_u32(&mut bytes, 168, 1);
    put_i32(&mut bytes, 176, TP_AXIS_KIND);
    put_i32(&mut bytes, 180, 0);
    put_i32(&mut bytes, 184, rank_i32);
    put_i32(&mut bytes, 188, shard_count_i32);
    put_i32(&mut bytes, 192, split_dim_i32);
    put_i64(&mut bytes, 200, local_shape[split_dim]);
    Ok(bytes)
}

fn encode_parallel_shard_metadata(
    dtype: TensorDtype,
    global_shape: &[i64],
    local_shape: &[i64],
    axes: &[ParallelAxisPy],
    data_bytes: usize,
) -> PyResult<[u8; TENSOR_METADATA_SIZE]> {
    if axes.is_empty()
        || axes.len() > MAX_LAYOUT_AXES
        || global_shape.len() != local_shape.len()
        || global_shape.len() > MAX_TENSOR_DIMS
    {
        return Err(invalid_metadata(
            "parallel shard axes and global/local shapes are invalid",
        ));
    }
    let expected = checked_tensor_bytes(local_shape, dtype.element_size())?;
    if expected != data_bytes {
        return Err(invalid_metadata(
            "parallel shard payload size does not match local shape/dtype",
        ));
    }

    let mut bytes = [0_u8; TENSOR_METADATA_SIZE];
    put_u32(&mut bytes, 0, TENSOR_OBJECT_MAGIC);
    put_u16(&mut bytes, 4, TENSOR_OBJECT_VERSION);
    put_u16(&mut bytes, 6, TENSOR_METADATA_SIZE as u16);
    put_i32(&mut bytes, 8, dtype as i32);
    put_i32(&mut bytes, 12, global_shape.len() as i32);
    put_u32(&mut bytes, 16, SHARD_LAYOUT);
    put_u64(&mut bytes, 24, TENSOR_METADATA_SIZE as u64);
    put_u64(&mut bytes, 32, data_bytes as u64);
    for index in 0..MAX_TENSOR_DIMS {
        put_i64(
            &mut bytes,
            40 + index * 8,
            global_shape.get(index).copied().unwrap_or(-1),
        );
        put_i64(
            &mut bytes,
            104 + index * 8,
            local_shape.get(index).copied().unwrap_or(-1),
        );
    }
    put_u32(&mut bytes, 168, axes.len() as u32);
    for (axis_index, axis) in axes.iter().enumerate() {
        let kind = axis.parsed_kind()?;
        let offset = 176 + axis_index * 32;
        put_i32(&mut bytes, offset, kind.wire_value());
        put_i32(
            &mut bytes,
            offset + 4,
            i32::try_from(axis_index)
                .map_err(|_| invalid_metadata("parallel axis index exceeds i32"))?,
        );
        put_i32(&mut bytes, offset + 8, axis.rank);
        put_i32(&mut bytes, offset + 12, axis.size);
        put_i32(&mut bytes, offset + 16, axis.split_dim.unwrap_or(-1));
        let reserved0 = match kind {
            AxisKind::Ep => axis.expert_id.unwrap_or(0),
            AxisKind::Pp => axis.stage_id.unwrap_or(0),
            AxisKind::Dp | AxisKind::Tp => 0,
        };
        put_i32(&mut bytes, offset + 20, reserved0);
        let local_split_extent = match axis.split_dim {
            Some(split_dim) => {
                let split_dim = usize::try_from(split_dim)
                    .map_err(|_| invalid_metadata("parallel split_dim is negative"))?;
                *local_shape
                    .get(split_dim)
                    .ok_or_else(|| invalid_metadata("parallel split_dim is outside tensor rank"))?
            }
            None => 0,
        };
        put_i64(&mut bytes, offset + 24, local_split_extent);
    }
    Ok(bytes)
}

fn parse_metadata(payload: &[u8]) -> PyResult<ParsedTensorMetadata> {
    if !cfg!(target_endian = "little") {
        return Err(invalid_metadata(
            "C++ TensorMetadata native ABI is only supported on little-endian hosts",
        ));
    }
    if payload.len() < TENSOR_METADATA_SIZE {
        return Err(invalid_metadata("tensor payload is smaller than metadata"));
    }
    if read_u32(payload, 0) != TENSOR_OBJECT_MAGIC
        || read_u16(payload, 4) != TENSOR_OBJECT_VERSION
        || usize::from(read_u16(payload, 6)) != TENSOR_METADATA_SIZE
    {
        return Err(invalid_metadata(
            "tensor metadata magic, version, or header size is invalid",
        ));
    }
    let dtype = TensorDtype::from_wire(read_i32(payload, 8))
        .ok_or_else(|| invalid_metadata("tensor metadata dtype is unsupported"))?;
    let ndim = usize::try_from(read_i32(payload, 12))
        .map_err(|_| invalid_metadata("tensor metadata ndim is negative"))?;
    if ndim > MAX_TENSOR_DIMS {
        return Err(invalid_metadata("tensor metadata ndim exceeds maximum"));
    }
    if read_u32(payload, 16) > 1 {
        return Err(invalid_metadata("tensor metadata layout kind is invalid"));
    }
    let data_offset = usize::try_from(read_u64(payload, 24))
        .map_err(|_| invalid_metadata("tensor metadata data offset overflows usize"))?;
    let data_bytes = usize::try_from(read_u64(payload, 32))
        .map_err(|_| invalid_metadata("tensor metadata data size overflows usize"))?;
    if data_offset < TENSOR_METADATA_SIZE
        || data_offset > payload.len()
        || data_bytes != payload.len() - data_offset
    {
        return Err(invalid_metadata(
            "tensor metadata data range does not match payload length",
        ));
    }

    let axis_count = read_u32(payload, 168) as usize;
    if axis_count > MAX_LAYOUT_AXES {
        return Err(invalid_metadata(
            "tensor metadata layout axis count exceeds maximum",
        ));
    }
    let mut shape = Vec::with_capacity(ndim);
    for index in 0..MAX_TENSOR_DIMS {
        let global_dim = read_i64(payload, 40 + index * 8);
        let local_dim = read_i64(payload, 104 + index * 8);
        if index < ndim {
            if global_dim < 0 || local_dim < 0 {
                return Err(invalid_metadata(
                    "tensor metadata contains a negative active dimension",
                ));
            }
            shape.push(local_dim);
        } else if global_dim != -1 || local_dim != -1 {
            return Err(invalid_metadata(
                "tensor metadata unused dimensions must be -1",
            ));
        }
    }
    for index in 0..axis_count {
        let offset = 176 + index * 32;
        let kind = read_i32(payload, offset);
        let shard_rank = read_i32(payload, offset + 8);
        let shard_count = read_i32(payload, offset + 12);
        let split_dim = read_i32(payload, offset + 16);
        if !(0..=4).contains(&kind)
            || shard_count <= 0
            || shard_rank < 0
            || shard_rank >= shard_count
            || split_dim < -1
            || split_dim >= ndim as i32
        {
            return Err(invalid_metadata("tensor metadata layout axis is invalid"));
        }
    }
    let expected = checked_tensor_bytes(&shape, dtype.element_size())?;
    if expected != data_bytes {
        return Err(invalid_metadata(
            "tensor metadata shape/dtype does not match data size",
        ));
    }
    Ok(ParsedTensorMetadata {
        dtype,
        shape,
        data_offset,
        data_bytes,
    })
}

#[pyfunction(name = "_tensor_metadata_size")]
pub(crate) fn tensor_metadata_size() -> usize {
    TENSOR_METADATA_SIZE
}

#[pyfunction(name = "_serialize_tensor")]
pub(crate) fn serialize_tensor(
    py: Python<'_>,
    tensor: &Bound<'_, PyAny>,
) -> PyResult<(Py<PyBytes>, usize, usize, Py<PyAny>)> {
    let parts = serialize_tensor_parts(tensor)?;
    let metadata = PyBytes::new(py, &parts.metadata).unbind();
    Ok((metadata, parts.data_ptr, parts.data_bytes, parts.owner))
}

fn serialize_tensor_parts(tensor: &Bound<'_, PyAny>) -> PyResult<SerializedTensorParts> {
    let contiguous = tensor.call_method0("contiguous")?;
    let device_type: String = contiguous.getattr("device")?.getattr("type")?.extract()?;
    if device_type != "cpu" {
        return Err(invalid_metadata(
            "accelerator tensor serialization is outside classic Store parity; move the tensor to CPU first",
        ));
    }
    let dtype_name = contiguous.getattr("dtype")?.str()?.to_str()?.to_string();
    let dtype = TensorDtype::from_torch_name(&dtype_name)
        .ok_or_else(|| invalid_metadata(format!("unsupported tensor dtype {dtype_name}")))?;
    let shape: Vec<i64> = contiguous.getattr("shape")?.extract()?;
    let numel: usize = contiguous.call_method0("numel")?.extract()?;
    let element_size: usize = contiguous.call_method0("element_size")?.extract()?;
    if element_size != dtype.element_size() {
        return Err(invalid_metadata(
            "tensor element_size does not match its dtype",
        ));
    }
    let data_bytes = numel
        .checked_mul(element_size)
        .ok_or_else(|| invalid_metadata("tensor byte size overflows usize"))?;
    let metadata = encode_full_metadata(dtype, &shape, data_bytes)?;
    let data_ptr: usize = contiguous.call_method0("data_ptr")?.extract()?;
    if data_bytes != 0 && data_ptr == 0 {
        return Err(invalid_metadata("non-empty tensor has a null data pointer"));
    }
    Ok(SerializedTensorParts {
        metadata,
        data_ptr,
        data_bytes,
        owner: contiguous.unbind(),
    })
}

pub(crate) fn serialize_tensor_payload(tensor: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let parts = serialize_tensor_parts(tensor)?;
    Ok(payload_from_parts(parts, None))
}

fn payload_from_parts(
    parts: SerializedTensorParts,
    metadata: Option<[u8; TENSOR_METADATA_SIZE]>,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(TENSOR_METADATA_SIZE + parts.data_bytes);
    payload.extend_from_slice(metadata.as_ref().unwrap_or(&parts.metadata));
    if parts.data_bytes != 0 {
        // SAFETY: `serialize_tensor_parts` accepts only a CPU contiguous
        // PyTorch tensor, verifies its byte size and non-null pointer, and
        // returns the owning tensor. `parts.owner` remains live and the GIL is
        // held for this immediate copy.
        let data =
            unsafe { std::slice::from_raw_parts(parts.data_ptr as *const u8, parts.data_bytes) };
        payload.extend_from_slice(data);
    }
    drop(parts.owner);
    payload
}

pub(crate) fn serialize_tp_tensor_payloads(
    tensor: &Bound<'_, PyAny>,
    tp_size: usize,
    split_dim: usize,
) -> PyResult<Vec<Vec<u8>>> {
    if tp_size == 0 {
        return Err(invalid_metadata("tp_size must be positive"));
    }
    if tp_size == 1 {
        return Ok(vec![serialize_tensor_payload(tensor)?]);
    }

    let contiguous = tensor.call_method0("contiguous")?;
    let device_type: String = contiguous.getattr("device")?.getattr("type")?.extract()?;
    if device_type != "cpu" {
        return Err(invalid_metadata(
            "accelerator TP serialization is outside classic Store parity",
        ));
    }
    let dtype_name = contiguous.getattr("dtype")?.str()?.to_str()?.to_string();
    let dtype = TensorDtype::from_torch_name(&dtype_name)
        .ok_or_else(|| invalid_metadata(format!("unsupported tensor dtype {dtype_name}")))?;
    let global_shape: Vec<i64> = contiguous.getattr("shape")?.extract()?;
    if split_dim >= global_shape.len() {
        return Err(invalid_metadata("TP split_dim is outside tensor rank"));
    }
    let split_extent = usize::try_from(global_shape[split_dim])
        .map_err(|_| invalid_metadata("TP split dimension is negative"))?;
    if split_extent % tp_size != 0 {
        return Err(invalid_metadata("only uniform TP sharding is supported"));
    }
    let local_extent = split_extent / tp_size;
    let local_extent_i64 =
        i64::try_from(local_extent).map_err(|_| invalid_metadata("TP shard extent exceeds i64"))?;
    let mut payloads = Vec::with_capacity(tp_size);
    for rank in 0..tp_size {
        let start = rank
            .checked_mul(local_extent)
            .ok_or_else(|| invalid_metadata("TP shard offset overflows usize"))?;
        let shard = contiguous
            .call_method1("narrow", (split_dim, start, local_extent))?
            .call_method0("contiguous")?;
        let parts = serialize_tensor_parts(&shard)?;
        let mut local_shape = global_shape.clone();
        local_shape[split_dim] = local_extent_i64;
        let metadata = encode_tp_shard_metadata(
            dtype,
            &global_shape,
            &local_shape,
            rank,
            tp_size,
            split_dim,
            parts.data_bytes,
        )?;
        payloads.push(payload_from_parts(parts, Some(metadata)));
    }
    Ok(payloads)
}

pub(crate) fn serialize_parallel_tensor_payloads(
    tensor: &Bound<'_, PyAny>,
    parallelism: &TensorParallelismPy,
) -> PyResult<Vec<(TensorParallelismPy, Vec<u8>, Option<ShardManifest>)>> {
    let parallelism = parallelism.validated(false)?;
    let contiguous = tensor.call_method0("contiguous")?;
    let device_type: String = contiguous.getattr("device")?.getattr("type")?.extract()?;
    if device_type != "cpu" {
        return Err(invalid_metadata(
            "accelerator parallel tensor serialization is outside classic Store parity",
        ));
    }
    let dtype_name = contiguous.getattr("dtype")?.str()?.to_str()?.to_string();
    let dtype = TensorDtype::from_torch_name(&dtype_name)
        .ok_or_else(|| invalid_metadata(format!("unsupported tensor dtype {dtype_name}")))?;
    let local_shape: Vec<i64> = contiguous.getattr("shape")?.extract()?;
    let mut inferred_global_shape = local_shape.clone();
    let manifest = if let Some(tp_axis_index) = parallelism.tp_axis_index()? {
        let tp_axis = &parallelism.axes[tp_axis_index];
        let split_dim = usize::try_from(
            tp_axis
                .split_dim
                .ok_or_else(|| invalid_metadata("TP axis requires split_dim"))?,
        )
        .map_err(|_| invalid_metadata("TP split_dim is negative"))?;
        if split_dim >= local_shape.len() {
            return Err(invalid_metadata("TP split_dim is outside tensor rank"));
        }
        inferred_global_shape[split_dim] = local_shape[split_dim]
            .checked_mul(i64::from(tp_axis.size))
            .ok_or_else(|| invalid_metadata("inferred TP global extent overflows i64"))?;
        Some(ShardManifest {
            dtype: dtype as i32,
            global_shape: inferred_global_shape.clone(),
            split_dim,
            shard_count: usize::try_from(tp_axis.size)
                .map_err(|_| invalid_metadata("TP size is negative"))?,
        })
    } else {
        None
    };
    let parts = serialize_tensor_parts(&contiguous)?;
    let metadata = encode_parallel_shard_metadata(
        dtype,
        &inferred_global_shape,
        &local_shape,
        &parallelism.axes,
        parts.data_bytes,
    )?;
    Ok(vec![(
        parallelism,
        payload_from_parts(parts, Some(metadata)),
        manifest,
    )])
}

pub(crate) fn serialize_parallel_full_tensor_payloads(
    tensor: &Bound<'_, PyAny>,
    parallelism: &TensorParallelismPy,
) -> PyResult<(Vec<(TensorParallelismPy, Vec<u8>)>, Option<ShardManifest>)> {
    let parallelism = parallelism.validated(false)?;
    let Some(tp_axis_index) = parallelism.tp_axis_index()? else {
        let mut payloads = serialize_parallel_tensor_payloads(tensor, &parallelism)?;
        let (parallelism, payload, manifest) = payloads
            .pop()
            .ok_or_else(|| invalid_metadata("parallel tensor write produced no payload"))?;
        return Ok((vec![(parallelism, payload)], manifest));
    };

    let contiguous = tensor.call_method0("contiguous")?;
    let device_type: String = contiguous.getattr("device")?.getattr("type")?.extract()?;
    if device_type != "cpu" {
        return Err(invalid_metadata(
            "accelerator parallel tensor serialization is outside classic Store parity",
        ));
    }
    let dtype_name = contiguous.getattr("dtype")?.str()?.to_str()?.to_string();
    let dtype = TensorDtype::from_torch_name(&dtype_name)
        .ok_or_else(|| invalid_metadata(format!("unsupported tensor dtype {dtype_name}")))?;
    let global_shape: Vec<i64> = contiguous.getattr("shape")?.extract()?;
    let tp_axis = &parallelism.axes[tp_axis_index];
    let split_dim = usize::try_from(
        tp_axis
            .split_dim
            .ok_or_else(|| invalid_metadata("TP axis requires split_dim"))?,
    )
    .map_err(|_| invalid_metadata("TP split_dim is negative"))?;
    if split_dim >= global_shape.len() {
        return Err(invalid_metadata("TP split_dim is outside tensor rank"));
    }
    let shard_count =
        usize::try_from(tp_axis.size).map_err(|_| invalid_metadata("TP size is negative"))?;
    let split_extent = usize::try_from(global_shape[split_dim])
        .map_err(|_| invalid_metadata("TP split dimension is negative"))?;
    if shard_count == 0 || split_extent % shard_count != 0 {
        return Err(invalid_metadata(
            "only uniform TP parallel sharding is supported",
        ));
    }
    let local_extent = split_extent / shard_count;
    let mut payloads = Vec::with_capacity(shard_count);
    for rank in 0..shard_count {
        let start = rank
            .checked_mul(local_extent)
            .ok_or_else(|| invalid_metadata("parallel shard offset overflows usize"))?;
        let shard = contiguous
            .call_method1("narrow", (split_dim, start, local_extent))?
            .call_method0("contiguous")?;
        let parts = serialize_tensor_parts(&shard)?;
        let mut local_shape = global_shape.clone();
        local_shape[split_dim] = i64::try_from(local_extent)
            .map_err(|_| invalid_metadata("parallel shard extent exceeds i64"))?;
        let mut shard_parallelism = parallelism.clone();
        shard_parallelism.axes[tp_axis_index].rank =
            i32::try_from(rank).map_err(|_| invalid_metadata("TP rank exceeds i32"))?;
        let metadata = encode_parallel_shard_metadata(
            dtype,
            &global_shape,
            &local_shape,
            &shard_parallelism.axes,
            parts.data_bytes,
        )?;
        payloads.push((shard_parallelism, payload_from_parts(parts, Some(metadata))));
    }
    Ok((
        payloads,
        Some(ShardManifest {
            dtype: dtype as i32,
            global_shape,
            split_dim,
            shard_count,
        }),
    ))
}

pub(crate) fn serialize_writer_partition_payload(
    tensor: &Bound<'_, PyAny>,
    writer: &WriterPartitionPy,
) -> PyResult<(Vec<u8>, ShardManifest)> {
    let contiguous = tensor.call_method0("contiguous")?;
    let global_shape: Vec<i64> = contiguous.getattr("shape")?.extract()?;
    writer.validate(global_shape.len())?;
    let split_dim = usize::try_from(writer.split_dim)
        .map_err(|_| invalid_metadata("writer split_dim is negative"))?;
    let shard_count =
        usize::try_from(writer.size).map_err(|_| invalid_metadata("writer size is negative"))?;
    let rank =
        usize::try_from(writer.rank).map_err(|_| invalid_metadata("writer rank is negative"))?;
    let split_extent = usize::try_from(global_shape[split_dim])
        .map_err(|_| invalid_metadata("writer split dimension is negative"))?;
    if split_extent % shard_count != 0 {
        return Err(invalid_metadata(
            "only uniform writer partition sharding is supported",
        ));
    }
    let local_extent = split_extent / shard_count;
    let start = rank
        .checked_mul(local_extent)
        .ok_or_else(|| invalid_metadata("writer shard offset overflows usize"))?;
    let shard = contiguous
        .call_method1("narrow", (split_dim, start, local_extent))?
        .call_method0("contiguous")?;
    let parts = serialize_tensor_parts(&shard)?;
    let dtype_name = shard.getattr("dtype")?.str()?.to_str()?.to_string();
    let dtype = TensorDtype::from_torch_name(&dtype_name)
        .ok_or_else(|| invalid_metadata(format!("unsupported tensor dtype {dtype_name}")))?;
    let mut local_shape = global_shape.clone();
    local_shape[split_dim] = i64::try_from(local_extent)
        .map_err(|_| invalid_metadata("writer shard extent exceeds i64"))?;
    let axes = vec![ParallelAxisPy {
        kind: "tp".to_string(),
        rank: writer.rank,
        size: writer.size,
        split_dim: Some(writer.split_dim),
        expert_id: None,
        stage_id: None,
    }];
    let metadata = encode_parallel_shard_metadata(
        dtype,
        &global_shape,
        &local_shape,
        &axes,
        parts.data_bytes,
    )?;
    let manifest = ShardManifest {
        dtype: dtype as i32,
        global_shape,
        split_dim,
        shard_count,
    };
    Ok((payload_from_parts(parts, Some(metadata)), manifest))
}

#[pyfunction(name = "_deserialize_tensor")]
pub(crate) fn deserialize_tensor(
    py: Python<'_>,
    payload: &Bound<'_, PyBytes>,
) -> PyResult<Py<PyAny>> {
    deserialize_tensor_bytes(py, payload.as_bytes())
}

pub(crate) fn deserialize_tensor_bytes(py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyAny>> {
    let parsed = parse_metadata(payload)?;
    let torch = py.import("torch")?;
    let dtype = torch.getattr(parsed.dtype.torch_attr())?;
    let shape = PyTuple::new(py, &parsed.shape)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("dtype", dtype)?;
    let tensor = if parsed.data_bytes == 0 {
        torch.call_method("empty", (shape,), Some(&kwargs))?
    } else {
        let owner = PyByteArray::new(
            py,
            &payload[parsed.data_offset..parsed.data_offset + parsed.data_bytes],
        );
        torch
            .call_method("frombuffer", (owner,), Some(&kwargs))?
            .call_method1("reshape", (shape,))?
    };
    Ok(tensor.unbind())
}

pub(crate) fn deserialize_tensor_payloads_concat(
    py: Python<'_>,
    payloads: &[Vec<u8>],
    split_dim: usize,
) -> PyResult<Py<PyAny>> {
    if payloads.is_empty() {
        return Err(invalid_metadata(
            "full tensor reconstruction requires at least one shard",
        ));
    }
    let tensors = payloads
        .iter()
        .map(|payload| deserialize_tensor_bytes(py, payload))
        .collect::<PyResult<Vec<_>>>()?;
    let tensor_refs = tensors
        .iter()
        .map(|tensor| tensor.bind(py))
        .collect::<Vec<_>>();
    let tensor_list = PyList::new(py, tensor_refs)?;
    let torch = py.import("torch")?;
    Ok(torch
        .call_method1("cat", (tensor_list, split_dim))?
        .unbind())
}

pub(crate) fn tensor_payload_matches_parallelism(
    payload: &[u8],
    parallelism: &TensorParallelismPy,
) -> bool {
    let stored =
        tensor_payload_parallelism(payload).and_then(|value| value.validated_canonical(false));
    let requested = parallelism.validated_canonical(false);
    matches!((stored, requested), (Ok(stored), Ok(requested)) if stored == requested)
}

pub(crate) fn tensor_payload_parallelism(payload: &[u8]) -> PyResult<TensorParallelismPy> {
    parse_metadata(payload)?;
    if read_u32(payload, 16) != SHARD_LAYOUT {
        return Err(invalid_metadata("tensor payload is not a shard"));
    }
    let axis_count = read_u32(payload, 168) as usize;
    if axis_count == 0 || axis_count > MAX_LAYOUT_AXES {
        return Err(invalid_metadata(
            "tensor shard has an invalid parallelism axis count",
        ));
    }
    let mut axes = Vec::with_capacity(axis_count);
    for axis_index in 0..axis_count {
        let offset = 176 + axis_index * 32;
        let kind = match read_i32(payload, offset) {
            0 => "dp",
            1 => "tp",
            2 => "ep",
            3 => "pp",
            _ => return Err(invalid_metadata("tensor shard has an invalid axis kind")),
        };
        let reserved0 = read_i32(payload, offset + 20);
        axes.push(ParallelAxisPy {
            kind: kind.to_string(),
            rank: read_i32(payload, offset + 8),
            size: read_i32(payload, offset + 12),
            split_dim: match read_i32(payload, offset + 16) {
                -1 => None,
                value => Some(value),
            },
            expert_id: (kind == "ep").then_some(reserved0),
            stage_id: (kind == "pp").then_some(reserved0),
        });
    }
    TensorParallelismPy { axes }.validated_canonical(false)
}

pub(crate) fn tensor_payload_global_shape(payload: &[u8]) -> PyResult<Vec<i64>> {
    parse_metadata(payload)?;
    let ndim = usize::try_from(read_i32(payload, 12))
        .map_err(|_| invalid_metadata("tensor payload rank is negative"))?;
    if ndim > MAX_TENSOR_DIMS {
        return Err(invalid_metadata("tensor payload rank exceeds limit"));
    }
    Ok((0..ndim)
        .map(|index| read_i64(payload, 40 + index * 8))
        .collect())
}

fn export_tensor_buffer(
    buffer: &Bound<'_, PyAny>,
    size: usize,
    require_writable: bool,
) -> PyResult<crate::buffer_export::ExportedBufferView> {
    let export = crate::buffer_export::ExportedBufferView::get(buffer)?;
    if !export.is_c_contiguous() {
        return Err(invalid_metadata("tensor buffer must be C-contiguous"));
    }
    if require_writable && export.readonly() {
        return Err(invalid_metadata(
            "tensor destination buffer must be writable",
        ));
    }
    if size > export.len_bytes() {
        return Err(invalid_metadata(format!(
            "tensor payload size {size} exceeds buffer capacity {}",
            export.len_bytes()
        )));
    }
    if size != 0 && export.buf_ptr().is_null() {
        return Err(invalid_metadata(
            "non-empty tensor buffer has a null data pointer",
        ));
    }
    Ok(export)
}

pub(crate) fn validate_tensor_buffer_object(
    buffer: &Bound<'_, PyAny>,
    size: usize,
) -> PyResult<()> {
    let export = export_tensor_buffer(buffer, size, false)?;
    // SAFETY: `export` owns a live Py_buffer view; `export_tensor_buffer`
    // checked the requested range against its byte capacity.
    let bytes = unsafe { std::slice::from_raw_parts(export.buf_ptr().cast::<u8>(), size) };
    parse_metadata(bytes).map(|_| ())
}

/// Validate a tensor encoding in caller-managed memory.
///
/// # Safety
///
/// `ptr..ptr + size` must remain readable for the duration of this call.
pub(crate) unsafe fn validate_tensor_raw_buffer(ptr: *const u8, size: usize) -> PyResult<()> {
    // SAFETY: upheld by the caller; raw-address client paths first verify that
    // this entire range belongs to a live registration.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, size) };
    parse_metadata(bytes).map(|_| ())
}

pub(crate) fn deserialize_tensor_buffer_copy(
    py: Python<'_>,
    buffer: &Bound<'_, PyAny>,
    size: usize,
) -> PyResult<Py<PyAny>> {
    let export = export_tensor_buffer(buffer, size, false)?;
    // SAFETY: `export` owns a live Py_buffer view; `export_tensor_buffer`
    // checked the requested range against its byte capacity. The deserializer
    // copies the payload into its own Python owner before this export drops.
    let bytes = unsafe { std::slice::from_raw_parts(export.buf_ptr().cast::<u8>(), size) };
    deserialize_tensor_bytes(py, bytes)
}

pub(crate) fn deserialize_tensor_buffer_object(
    py: Python<'_>,
    buffer: &Bound<'_, PyAny>,
    size: usize,
) -> PyResult<Py<PyAny>> {
    let export = export_tensor_buffer(buffer, size, true)?;
    // SAFETY: `export` owns a live Py_buffer view; `export_tensor_buffer`
    // checked the requested range against its byte capacity.
    let bytes = unsafe { std::slice::from_raw_parts(export.buf_ptr().cast::<u8>(), size) };
    let parsed = parse_metadata(bytes)?;
    let torch = py.import("torch")?;
    let dtype = torch.getattr(parsed.dtype.torch_attr())?;
    let shape = PyTuple::new(py, &parsed.shape)?;
    if parsed.data_bytes == 0 {
        let kwargs = PyDict::new(py);
        kwargs.set_item("dtype", dtype)?;
        return Ok(torch
            .call_method("empty", (shape,), Some(&kwargs))?
            .unbind());
    }

    let kwargs = PyDict::new(py);
    kwargs.set_item("dtype", dtype)?;
    kwargs.set_item("count", parsed.data_bytes / parsed.dtype.element_size())?;
    kwargs.set_item("offset", parsed.data_offset)?;
    let tensor = torch
        .call_method("frombuffer", (buffer,), Some(&kwargs))?
        .call_method1("reshape", (shape,))?;
    Ok(tensor.unbind())
}

pub(crate) fn copy_tensor_payload_into_buffer(
    py: Python<'_>,
    buffer: &Bound<'_, PyAny>,
    capacity: usize,
    payload: &[u8],
) -> PyResult<Py<PyAny>> {
    // Parse before mutating caller-owned memory so invalid Store payloads leave
    // the destination untouched.
    parse_metadata(payload)?;
    let export = export_tensor_buffer(buffer, capacity, true)?;
    if payload.len() > capacity {
        return Err(invalid_metadata(format!(
            "tensor payload size {} exceeds destination capacity {capacity}",
            payload.len()
        )));
    }
    if !payload.is_empty() {
        // SAFETY: `export` holds a writable, C-contiguous Py_buffer whose
        // capacity was checked above. Source and destination cannot overlap:
        // Store payloads are owned Rust Vec bytes, independent of the Python
        // destination allocation.
        unsafe {
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                export.buf_ptr().cast::<u8>(),
                payload.len(),
            );
        }
    }
    deserialize_tensor_buffer_object(py, buffer, payload.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_metadata_matches_cpp_native_offsets_and_size() {
        let metadata = encode_full_metadata(TensorDtype::Float32, &[2, 3], 24).unwrap();
        assert_eq!(metadata.len(), 304);
        assert_eq!(read_u32(&metadata, 0), TENSOR_OBJECT_MAGIC);
        assert_eq!(read_u16(&metadata, 6), 304);
        assert_eq!(read_i32(&metadata, 8), TensorDtype::Float32 as i32);
        assert_eq!(read_i32(&metadata, 12), 2);
        assert_eq!(read_u64(&metadata, 24), 304);
        assert_eq!(read_u64(&metadata, 32), 24);
        assert_eq!(read_i64(&metadata, 40), 2);
        assert_eq!(read_i64(&metadata, 48), 3);
        assert_eq!(read_i64(&metadata, 56), -1);
        assert_eq!(read_i64(&metadata, 104), 2);
        assert_eq!(read_u32(&metadata, 168), 0);
    }

    #[test]
    fn tp_shard_metadata_matches_cpp_axis_layout() {
        let metadata =
            encode_tp_shard_metadata(TensorDtype::Float32, &[8, 4], &[2, 4], 2, 4, 0, 32).unwrap();
        assert_eq!(read_u32(&metadata, 16), SHARD_LAYOUT);
        assert_eq!(read_i64(&metadata, 40), 8);
        assert_eq!(read_i64(&metadata, 104), 2);
        assert_eq!(read_u32(&metadata, 168), 1);
        assert_eq!(read_i32(&metadata, 176), TP_AXIS_KIND);
        assert_eq!(read_i32(&metadata, 180), 0);
        assert_eq!(read_i32(&metadata, 184), 2);
        assert_eq!(read_i32(&metadata, 188), 4);
        assert_eq!(read_i32(&metadata, 192), 0);
        assert_eq!(read_i64(&metadata, 200), 2);
    }

    #[test]
    fn shard_payload_exposes_canonical_parallelism_and_global_shape() {
        let metadata =
            encode_tp_shard_metadata(TensorDtype::Float32, &[8, 4], &[2, 4], 2, 4, 0, 32).unwrap();
        let mut payload = metadata.to_vec();
        payload.extend_from_slice(&[0; 32]);
        assert_eq!(tensor_payload_global_shape(&payload).unwrap(), vec![8, 4]);
        assert_eq!(
            tensor_payload_parallelism(&payload).unwrap(),
            TensorParallelismPy {
                axes: vec![ParallelAxisPy {
                    kind: "tp".to_string(),
                    rank: 2,
                    size: 4,
                    split_dim: Some(0),
                    expert_id: None,
                    stage_id: None,
                }],
            }
        );
    }

    #[test]
    fn parser_rejects_truncated_corrupt_and_shape_mismatched_payloads() {
        assert!(parse_metadata(&[0; TENSOR_METADATA_SIZE - 1]).is_err());

        let metadata = encode_full_metadata(TensorDtype::Int64, &[2], 16).unwrap();
        let mut payload = metadata.to_vec();
        payload.extend_from_slice(&[0; 16]);
        assert_eq!(
            parse_metadata(&payload).unwrap(),
            ParsedTensorMetadata {
                dtype: TensorDtype::Int64,
                shape: vec![2],
                data_offset: 304,
                data_bytes: 16,
            }
        );

        payload[0] ^= 1;
        assert!(parse_metadata(&payload).is_err());
        payload[0] ^= 1;
        put_i64(&mut payload, 104, 3);
        assert!(parse_metadata(&payload).is_err());
    }
}

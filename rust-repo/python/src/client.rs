// =============================================================================
// PythonMooncakeClient — Mooncake 存储客户端的 Python 绑定
// Python bindings for the Mooncake distributed object store client
// =============================================================================
//
// Architectural overview (架构概览):
// ===================================
//
// 1. Why Arc<Mutex<Option<MooncakeClient>>>? (为什么使用三层嵌套?)
//
//    Arc:       Multiple Python objects (client handle, engram store, etc.)
//               may need to hold a reference to the same underlying connection.
//               Arc enables shared ownership without copying the client.
//               Arc 允许多个 Python 对象共享同一个底层连接的所有权。
//
//    Mutex:     Python is single-threaded (GIL), but tokio futures may
//               resume on different threads.  The Mutex serializes all access
//               to the client, preventing data-races when Rust async tasks
//               and Python sync calls interleave.
//               Python 有 GIL 是单线程的，但 tokio future 可能在不同线程上
//               恢复执行。Mutex 序列化所有对 client 的访问，防止竞态。
//
//    Option:    Represents the client lifecycle state. Some = connected and
//               usable; None = closed. take_client() holds an async owned
//               mutex guard and borrows the value in place. Concurrent calls
//               wait, and cancellation cannot lose the client value.
//               Option 代表客户端的生命周期状态。Some = 已连接可用；
//               None = 已关闭。take_client() 在异步操作期间原位借用 client；
//               并发调用等待同一 async mutex，取消 future 不会丢失 client。
//
// 2. Client lifecycle (客户端生命周期):
//
//    create() -> use (put/get/...) -> close()
//
//    After a successful close, all methods return StoreError("client already closed").
//    close() 先释放 client/TE registration，再清空 registered_py_buffers。
//
// 3. Async execution patterns (异步执行模式):
//
//    a) future_into_py: For Python async methods.  Runs the future on the
//       tokio runtime managed by pyo3_async_runtimes.  The future returns
//       Rust types (Vec<u8>, String, tuples, etc.) — never PyObject.
//       future_into_py calls IntoPy::into_py() on the result with the GIL
//       held, converting Rust types to Python types on the Python thread.
//
//       IMPORTANT: With Python 3.14t (free-threaded), Python::assume_attached()
//       is safe to call from any thread.  However, future_into_py still
//       requires returning Rust types because the IntoPy conversion happens
//       on the Python event-loop thread with the GIL properly acquired.
//       You should NEVER call Python::assume_attached() directly inside an
//       async block passed to future_into_py.
//
//       future_into_py: 用于 Python 异步方法。future 在 pyo3_async_runtimes
//       管理的 tokio runtime 上执行。future 返回 Rust 类型（Vec<u8>、String、
//       元组等）—— 绝不返回 PyObject。future_into_py 在持有 GIL 的情况下
//       调用 IntoPy::into_py()，在 Python 线程上将 Rust 类型转换为 Python 类型。
//
//       重要：使用 Python 3.14t (free-threaded) 时，Python::assume_attached()
//       可在任意线程安全调用。但 future_into_py 仍然要求返回 Rust 类型，因为
//       IntoPy 转换在 Python 事件循环线程上执行并正确获取 GIL。绝对不要在传给
//       future_into_py 的 async 块内部直接调用 Python::assume_attached()。
//
//    b) block_on: For Python sync methods (get_into, put_from, upsert_from,
//       register_buffer, etc.).  These block the current OS thread on the
//       tokio runtime, waiting for the async operation to complete.  Used
//       for zero-copy operations where the future is not Send (contains raw
//       pointers captured across await points).
//
//       block_on: 用于 Python 同步方法。阻塞当前线程等待 tokio runtime 上的
//       异步操作完成。用于零拷贝操作，这些操作的 future 不是 Send（跨 await
//       点捕获了裸指针）。
//
// 4. GIL-free design (无 GIL 设计):
//
//    With Python 3.14t, the GIL is optional.  The binding layer is designed
//    to be GIL-free compatible:
//    - All async code returns Rust types, not PyObject
//    - future_into_py handles the GIL acquisition for IntoPy conversion
//    - block_on methods hold the GIL for the entire blocking call
//    - No Python::assume_attached() inside async blocks
//
//    在 Python 3.14t 中 GIL 是可选的。绑定层设计为兼容无 GIL 模式：
//    - 所有异步代码返回 Rust 类型，而非 PyObject
//    - future_into_py 负责获取 GIL 以进行 IntoPy 转换
//    - block_on 方法在整个阻塞调用期间持有 GIL
//    - 不在 async 块内部使用 Python::assume_attached()
// =============================================================================

use crate::dlpack::DLPackMemoryOwner;
use crate::remote_config::PyRemoteSourceConfig;
use crate::replicate_config::ReplicateConfigPy;
use crate::tensor_parallelism::{
    AxisKind, ParallelAxisPy, ReadTargetPy, ShardManifest, TensorParallelismPy, WriterPartitionPy,
    parallelism_key, parallelism_manifest_key, writer_manifest_key, writer_shard_key,
};
use mooncake_store_client::proto::StorageObjectMetadata;
use mooncake_store_client::{
    BufferRegistrationId, ClientBackgroundConfig, ClientBackgroundHandle, LocalStorageBackend,
    LocalStorageConfig, MooncakeClient,
};
use mooncake_store_core::{NoFSegment, ReplicateConfig};
use parking_lot::Mutex;
use pyo3::IntoPyObjectExt;
use pyo3::buffer::PyUntypedBuffer;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::HashMap;
use std::ffi::c_void;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, OwnedMutexGuard};
use tracing::warn;
use transfer_engine_ffi::StableMemoryOwner;
use uuid::Uuid;

use super::to_py_err;

/// Python wrapper for MooncakeClient.
///
/// Python 侧的 Mooncake 存储客户端封装。
///
/// Fields (字段说明):
/// - `inner`: The actual Rust client, guarded by Arc<Mutex<Option<T>>>.
///   Arc 提供多所有者共享，Mutex 序列化访问，Option 表示生命周期状态。
/// - `registered_py_buffers`: Tracks Python buffer objects registered for
///   zero-copy RDMA transfers. Each entry retains the exact registration
///   identity needed by the Python unregister API. The allocation owner itself
///   lives inside the FFI `RegisteredMemory` held by `MooncakeClient`.
///   跟踪 Python 注销 API 所需的精确 registration identity；allocation owner
///   由 MooncakeClient 内的 FFI RegisteredMemory 直接持有。
#[pyclass(name = "MooncakeClient")]
pub(crate) struct PythonMooncakeClient {
    pub(crate) inner: SharedClient,
    pub(crate) background: Arc<AsyncMutex<Option<ClientBackgroundHandle>>>,
    pub(crate) registered_py_buffers: Arc<Mutex<Vec<PythonBufferRegistration>>>,
}

pub(crate) type SharedClient = Arc<AsyncMutex<Option<MooncakeClient>>>;

pub(crate) struct PythonBufferRegistration {
    registration_id: BufferRegistrationId,
    python_object_identity: usize,
    registered_size: usize,
}

#[derive(Debug)]
struct PythonBufferMemoryOwner {
    base: usize,
    len: usize,
    // Retaining the exported Python buffer, rather than only its PyObject, prevents
    // resizable exporters such as bytearray from moving the registered memory.
    _buffer_export: PyUntypedBuffer,
}

unsafe impl StableMemoryOwner for PythonBufferMemoryOwner {
    // PyUntypedBuffer owns a live Py_buffer export whose base, capacity,
    // writability, and contiguity were validated before construction. CPython
    // requires the exporter to keep that allocation stable until release.
    fn base_address(&self) -> NonNull<c_void> {
        NonNull::new(self.base as *mut c_void).expect("validated Python memory address")
    }

    fn length(&self) -> usize {
        self.len
    }
}

#[derive(Debug)]
enum PythonMemoryOwner {
    Buffer(PythonBufferMemoryOwner),
    DLPack(DLPackMemoryOwner),
}

unsafe impl StableMemoryOwner for PythonMemoryOwner {
    fn base_address(&self) -> NonNull<c_void> {
        match self {
            Self::Buffer(owner) => owner.base_address(),
            Self::DLPack(owner) => owner.base_address(),
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Buffer(owner) => owner.length(),
            Self::DLPack(owner) => owner.length(),
        }
    }
}

impl PythonBufferRegistration {
    fn registration_id(&self) -> BufferRegistrationId {
        self.registration_id
    }

    fn matches_python_object(&self, identity: usize) -> bool {
        self.python_object_identity == identity
    }

    fn contains_range(&self, base: usize, size: usize) -> bool {
        let registration_base = self.registration_id.base_address();
        let Some(registration_end) = registration_base.checked_add(self.registered_size) else {
            return false;
        };
        let Some(range_end) = base.checked_add(size) else {
            return false;
        };
        base >= registration_base && range_end <= registration_end
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NormalizedCreateArgs {
    local_hostname: String,
    metadata_server: String,
    master_server_addr: String,
    protocol: String,
}

// =========================================================================
// Internal helpers — 内部辅助函数
// =========================================================================

const KNOWN_PROTOCOLS: &[&str] = &[
    "tcp",
    "rdma",
    "efa",
    "nvmeof",
    "nvlink",
    "nvlink_intra",
    "hip",
    "barex",
    "cxl",
    "ascend",
    "ub",
    "ubshmem",
    "maca",
    "sunrise_link",
    "rpc_only",
];

fn required_non_empty(field: &str, value: String) -> PyResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(to_py_err(format!(
            "Config field '{field}' must be a non-empty string"
        )));
    }
    Ok(trimmed.to_string())
}

fn normalize_create_args(
    local_hostname: String,
    metadata_server: String,
    master_server_addr: String,
    protocol: String,
) -> PyResult<NormalizedCreateArgs> {
    let local_hostname = required_non_empty("local_hostname", local_hostname)?;
    let metadata_server = required_non_empty("metadata_server", metadata_server)?;
    let master_server_addr = required_non_empty("master_server_addr", master_server_addr)?;

    let protocol = if protocol.is_empty() {
        "tcp".to_string()
    } else {
        let trimmed = protocol.trim();
        if trimmed.is_empty() {
            return Err(to_py_err(
                "Invalid protocol: protocol must be a non-empty string",
            ));
        }
        trimmed.to_ascii_lowercase()
    };

    if !KNOWN_PROTOCOLS.contains(&protocol.as_str()) {
        warn!(
            protocol,
            "unrecognised protocol; passing it through to the Mooncake engine"
        );
    }

    Ok(NormalizedCreateArgs {
        local_hostname,
        metadata_server,
        master_server_addr,
        protocol,
    })
}

fn normalize_client_http_config(
    enabled: bool,
    port: u16,
) -> PyResult<mooncake_store_client::ClientHttpConfig> {
    if enabled && port == 0 {
        return Err(to_py_err("client_http_port must be between 1 and 65535"));
    }
    Ok(mooncake_store_client::ClientHttpConfig { enabled, port })
}

/// Extract the raw pointer and size from a Python buffer-like object.
///
/// 从 Python buffer-like 对象（如 bytearray、memoryview、numpy array）中
/// 提取裸指针和字节大小。用于零拷贝 RDMA 读写操作。
///
/// Supports any Python object that implements the buffer protocol:
/// bytearray, memoryview, array.array, numpy arrays, etc.
fn export_c_contiguous_buffer(
    obj: &Bound<'_, PyAny>,
    require_writable: bool,
) -> PyResult<PyUntypedBuffer> {
    let buffer = PyUntypedBuffer::get(obj)?;
    if !buffer.is_c_contiguous() {
        return Err(to_py_err("Python buffer must be C-contiguous"));
    }
    if require_writable && buffer.readonly() {
        return Err(to_py_err("Python destination buffer must be writable"));
    }
    Ok(buffer)
}

fn host_registration_location(requested: Option<&str>) -> PyResult<String> {
    let location = requested.unwrap_or("cpu:0").trim();
    if location.is_empty() {
        return Err(to_py_err("registration location must not be empty"));
    }
    if location != "*" && !location.starts_with("cpu:") {
        return Err(to_py_err(format!(
            "Python buffer-protocol memory is host memory and cannot be registered as {location:?}"
        )));
    }
    Ok(location.to_string())
}

fn checked_python_buffer_owner(
    buffer_export: PyUntypedBuffer,
    expected_base: Option<usize>,
    size: usize,
) -> PyResult<PythonMemoryOwner> {
    if buffer_export.readonly() {
        return Err(to_py_err("registered Python buffer owner must be writable"));
    }
    if !buffer_export.is_c_contiguous() {
        return Err(to_py_err(
            "registered Python buffer owner must be C-contiguous",
        ));
    }
    let base = buffer_export.buf_ptr() as usize;
    if let Some(expected) = expected_base
        && expected != base
    {
        return Err(to_py_err(format!(
            "raw address {expected:#x} does not match its Python buffer owner's base address {base:#x}"
        )));
    }
    if size > buffer_export.len_bytes() {
        return Err(to_py_err(format!(
            "registered size {size} exceeds Python buffer owner capacity {}",
            buffer_export.len_bytes()
        )));
    }
    Ok(PythonMemoryOwner::Buffer(PythonBufferMemoryOwner {
        base,
        len: size,
        _buffer_export: buffer_export,
    }))
}

pub(crate) fn get_buffer_ptr(obj: &Bound<'_, PyAny>) -> PyResult<(*mut c_void, usize)> {
    let buffer = export_c_contiguous_buffer(obj, false)?;
    Ok((buffer.buf_ptr(), buffer.len_bytes()))
}

fn get_dlpack_ptr(obj: &Bound<'_, PyAny>) -> PyResult<(*mut c_void, usize)> {
    let owner = DLPackMemoryOwner::from_python(obj, None, None, None)?;
    Ok((owner.base() as *mut c_void, owner.len()))
}

pub(crate) fn get_pointer(obj: &Bound<'_, PyAny>) -> PyResult<*mut c_void> {
    if let Ok(address) = obj.extract::<usize>() {
        if address == 0 {
            return Err(to_py_err("buffer address must not be zero"));
        }
        return Ok(address as *mut c_void);
    }
    match get_buffer_ptr(obj) {
        Ok((pointer, _)) => Ok(pointer),
        Err(buffer_error) => {
            if obj.hasattr("__dlpack__")? {
                get_dlpack_ptr(obj).map(|(pointer, _)| pointer)
            } else {
                Err(buffer_error)
            }
        }
    }
}

pub(crate) fn get_writable_pointer(obj: &Bound<'_, PyAny>) -> PyResult<*mut c_void> {
    if let Ok(address) = obj.extract::<usize>() {
        if address == 0 {
            return Err(to_py_err("buffer address must not be zero"));
        }
        return Ok(address as *mut c_void);
    }
    match export_c_contiguous_buffer(obj, true) {
        Ok(buffer) => Ok(buffer.buf_ptr()),
        Err(buffer_error) => {
            if obj.hasattr("__dlpack__")? {
                get_dlpack_ptr(obj).map(|(pointer, _)| pointer)
            } else {
                Err(buffer_error)
            }
        }
    }
}

fn get_pointer_and_size(
    obj: &Bound<'_, PyAny>,
    requested_size: Option<usize>,
) -> PyResult<(*mut c_void, usize)> {
    if let Ok(address) = obj.extract::<usize>() {
        let size = requested_size
            .ok_or_else(|| to_py_err("size is required when buffer is a raw integer address"))?;
        if address == 0 {
            return Err(to_py_err("buffer address must not be zero"));
        }
        return Ok((address as *mut c_void, size));
    }
    let (pointer, capacity) = match export_c_contiguous_buffer(obj, true) {
        Ok(buffer) => (buffer.buf_ptr(), buffer.len_bytes()),
        Err(buffer_error) => {
            if obj.hasattr("__dlpack__")? {
                get_dlpack_ptr(obj)?
            } else {
                return Err(buffer_error);
            }
        }
    };
    let size = requested_size.unwrap_or(capacity);
    if size > capacity {
        return Err(to_py_err("requested size exceeds Python buffer capacity"));
    }
    Ok((pointer, size))
}

fn validate_registered_tensor_destination(
    slf: &Bound<'_, PythonMooncakeClient>,
    buffer: &Bound<'_, PyAny>,
    size: usize,
) -> PyResult<()> {
    if buffer.extract::<usize>().is_ok() {
        return Err(to_py_err(
            "tensor destination requires an owner-bearing Python buffer",
        ));
    }
    let export = export_c_contiguous_buffer(buffer, true)?;
    if size > export.len_bytes() {
        return Err(to_py_err(
            "tensor destination size exceeds Python buffer capacity",
        ));
    }
    let base = export.buf_ptr() as usize;
    let registered = slf
        .borrow()
        .registered_py_buffers
        .lock()
        .iter()
        .any(|registration| registration.contains_range(base, size));
    if !registered {
        return Err(to_py_err(
            "tensor destination buffer range is not registered with this client",
        ));
    }
    Ok(())
}

/// Exclusive, cancellation-safe borrow of the shared Rust client.
///
/// The client remains inside its lifecycle slot for the entire operation.
/// Concurrent calls wait on the async mutex; cancelling a future only drops
/// this guard and can never lose the client value.
pub(crate) struct ClientOperationGuard {
    slot: OwnedMutexGuard<Option<MooncakeClient>>,
}

impl Deref for ClientOperationGuard {
    type Target = MooncakeClient;

    fn deref(&self) -> &Self::Target {
        self.slot
            .as_ref()
            .expect("operation guard is only created for a live client")
    }
}

impl DerefMut for ClientOperationGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slot
            .as_mut()
            .expect("operation guard is only created for a live client")
    }
}

pub(crate) async fn take_client(inner: &SharedClient) -> PyResult<ClientOperationGuard> {
    let slot = Arc::clone(inner).lock_owned().await;
    if slot.is_none() {
        return Err(to_py_err("client already closed"));
    }
    Ok(ClientOperationGuard { slot })
}

pub(crate) fn try_client_slot(
    inner: &SharedClient,
) -> PyResult<AsyncMutexGuard<'_, Option<MooncakeClient>>> {
    inner
        .try_lock()
        .map_err(|_| to_py_err("client is busy with another operation"))
}

/// Convert a Vec of ReplicaDescriptor into a Python list of dicts.
///
/// 将 ReplicaDescriptor 的 Vec 转换为 Python 字典列表。
///
/// Each dict has keys: "segment_name" (str), "offset" (int), "segment_id" (str).
/// Called from block_on methods (upsert_from, batch_upsert_from) which hold
/// the GIL, so Python::assume_attached() is safe.
///
/// 每个字典包含键: "segment_name" (str), "offset" (int), "segment_id" (str)。
/// 从 block_on 方法中调用，这些方法持有 GIL，因此 Python::assume_attached() 安全。
pub(crate) fn replicas_to_py(replicas: Vec<mooncake_store_core::ReplicaDescriptor>) -> Py<PyAny> {
    let py = unsafe { Python::assume_attached() };
    let out: Vec<Py<PyAny>> = replicas
        .iter()
        .map(|r| {
            let d = PyDict::new(py);
            d.set_item("segment_name", &r.segment_name).ok();
            d.set_item("offset", r.offset).ok();
            d.set_item("segment_id", r.segment_id.to_string()).ok();
            d.set_item("size", r.size).ok();
            d.set_item("status", format!("{:?}", r.status)).ok();
            d.set_item("replica_type", format!("{:?}", r.replica_type))
                .ok();
            d.set_item(
                "holder_client_id",
                r.holder_client_id.map(|id| id.to_string()),
            )
            .ok();
            d.set_item("handle_valid", r.handle_valid).ok();
            d.set_item("base_addr", r.base_addr).ok();
            d.set_item("protocol", &r.protocol).ok();
            d.into()
        })
        .collect();
    out.into_pyobject(py)
        .expect("replicas_to_py: into_pyobject failed")
        .unbind()
}

pub(crate) fn parse_uuid(id: &str) -> PyResult<Uuid> {
    Uuid::parse_str(id).map_err(|e| to_py_err(format!("invalid UUID: {e}")))
}

fn parse_uuid_list(ids: &[String]) -> PyResult<Vec<Uuid>> {
    ids.iter().map(|id| parse_uuid(id)).collect()
}

fn normalize_client_tenant_id(tenant_id: String) -> String {
    if tenant_id.is_empty() {
        "default".to_string()
    } else {
        tenant_id
    }
}

const TENSOR_INVALID_PARAMS_STATUS: i32 = -600;
const TENSOR_FILE_NOT_FOUND_STATUS: i32 = -1100;

struct ParallelTensorWritePlan {
    object_key: String,
    payload: Vec<u8>,
    manifest: Option<(String, Vec<u8>)>,
}

struct ParallelTensorFullWritePlan {
    objects: Vec<(String, Vec<u8>)>,
    manifest: Option<(String, Vec<u8>)>,
}

fn indexed_batch_config(
    mut config: Option<ReplicateConfig>,
    key_count: usize,
    original_indices: &[usize],
) -> PyResult<Option<ReplicateConfig>> {
    let Some(config) = config.as_mut() else {
        return Ok(None);
    };
    if config.group_ids.is_empty() {
        return Ok(Some(config.clone()));
    }
    if config.group_ids.len() != key_count {
        return Err(to_py_err("group_ids length must match keys length"));
    }
    config.group_ids = original_indices
        .iter()
        .map(|&index| config.group_ids[index].clone())
        .collect();
    Ok(Some(config.clone()))
}

fn repeated_indexed_batch_config(
    mut config: Option<ReplicateConfig>,
    key_count: usize,
    original_indices: &[usize],
    repeat_count: usize,
) -> PyResult<Option<ReplicateConfig>> {
    let Some(config) = config.as_mut() else {
        return Ok(None);
    };
    if config.group_ids.is_empty() {
        return Ok(Some(config.clone()));
    }
    if config.group_ids.len() != key_count {
        return Err(to_py_err("group_ids length must match base keys length"));
    }
    config.group_ids = original_indices
        .iter()
        .flat_map(|&index| std::iter::repeat(config.group_ids[index].clone()).take(repeat_count))
        .collect();
    Ok(Some(config.clone()))
}

fn tp_parameters(tp_rank: i32, tp_size: i32, split_dim: i32) -> PyResult<(usize, usize, usize)> {
    let tp_size = usize::try_from(tp_size).map_err(|_| to_py_err("tp_size must be positive"))?;
    if tp_size == 0 {
        return Err(to_py_err("tp_size must be positive"));
    }
    let tp_rank =
        usize::try_from(tp_rank).map_err(|_| to_py_err("tp_rank must be non-negative"))?;
    if tp_rank >= tp_size {
        return Err(to_py_err("tp_rank must be smaller than tp_size"));
    }
    let split_dim =
        usize::try_from(split_dim).map_err(|_| to_py_err("split_dim must be non-negative"))?;
    Ok((tp_rank, tp_size, split_dim))
}

fn tp_shard_key(base_key: &str, rank: usize) -> String {
    format!("{base_key}_tp_{rank}")
}

fn tensor_publish_config_is_valid(config: &ReplicateConfig) -> bool {
    config.preferred_segments.is_empty()
        || config.preferred_segments.len() == config.replica_num as usize
}

fn parallel_tensor_write_plan(
    key: &str,
    tensor: &Bound<'_, PyAny>,
    parallelism: Option<&TensorParallelismPy>,
    writer_partition: Option<&WriterPartitionPy>,
) -> PyResult<ParallelTensorWritePlan> {
    match (parallelism, writer_partition) {
        (Some(_), Some(_)) => Err(to_py_err(
            "writer_partition cannot be combined with parallelism",
        )),
        (None, None) => Ok(ParallelTensorWritePlan {
            object_key: key.to_string(),
            payload: crate::tensor_codec::serialize_tensor_payload(tensor)?,
            manifest: None,
        }),
        (Some(parallelism), None) => {
            let mut payloads =
                crate::tensor_codec::serialize_parallel_tensor_payloads(tensor, parallelism)?;
            let (resolved_parallelism, payload, manifest) = payloads
                .pop()
                .ok_or_else(|| to_py_err("parallel tensor write produced no payload"))?;
            if !payloads.is_empty() {
                return Err(to_py_err(
                    "parallel tensor write unexpectedly produced multiple payloads",
                ));
            }
            let manifest = manifest
                .map(|manifest| {
                    Ok::<_, PyErr>((parallelism_manifest_key(key), manifest.encode()?.to_vec()))
                })
                .transpose()?;
            Ok(ParallelTensorWritePlan {
                object_key: parallelism_key(key, &resolved_parallelism)?,
                payload,
                manifest,
            })
        }
        (None, Some(writer)) => {
            let (payload, manifest) =
                crate::tensor_codec::serialize_writer_partition_payload(tensor, writer)?;
            Ok(ParallelTensorWritePlan {
                object_key: writer_shard_key(key, writer),
                payload,
                manifest: Some((writer_manifest_key(key), manifest.encode()?.to_vec())),
            })
        }
    }
}

fn parallel_tensor_full_write_plan(
    key: &str,
    tensor: &Bound<'_, PyAny>,
    parallelism: Option<&TensorParallelismPy>,
    writer_partition: Option<&WriterPartitionPy>,
) -> PyResult<ParallelTensorFullWritePlan> {
    match (parallelism, writer_partition) {
        (Some(_), Some(_)) => Err(to_py_err(
            "writer_partition cannot be combined with parallelism",
        )),
        (None, None) => Ok(ParallelTensorFullWritePlan {
            objects: vec![(
                key.to_string(),
                crate::tensor_codec::serialize_tensor_payload(tensor)?,
            )],
            manifest: None,
        }),
        (Some(parallelism), None) => {
            let (payloads, manifest) =
                crate::tensor_codec::serialize_parallel_full_tensor_payloads(tensor, parallelism)?;
            let objects = payloads
                .into_iter()
                .map(|(parallelism, payload)| Ok((parallelism_key(key, &parallelism)?, payload)))
                .collect::<PyResult<Vec<_>>>()?;
            let manifest = manifest
                .map(|manifest| {
                    Ok::<_, PyErr>((parallelism_manifest_key(key), manifest.encode()?.to_vec()))
                })
                .transpose()?;
            Ok(ParallelTensorFullWritePlan { objects, manifest })
        }
        (None, Some(writer)) => {
            let (payload, manifest) =
                crate::tensor_codec::serialize_writer_partition_payload(tensor, writer)?;
            Ok(ParallelTensorFullWritePlan {
                objects: vec![(writer_shard_key(key, writer), payload)],
                manifest: Some((writer_manifest_key(key), manifest.encode()?.to_vec())),
            })
        }
    }
}

fn expanded_single_key_config(
    mut config: Option<ReplicateConfig>,
    object_count: usize,
) -> PyResult<Option<ReplicateConfig>> {
    let Some(config) = config.as_mut() else {
        return Ok(None);
    };
    if config.group_ids.is_empty() {
        return Ok(Some(config.clone()));
    }
    if config.group_ids.len() != 1 {
        return Err(to_py_err(
            "single tensor parallel request accepts exactly one group_id",
        ));
    }
    config.group_ids = std::iter::repeat(config.group_ids[0].clone())
        .take(object_count)
        .collect();
    Ok(Some(config.clone()))
}

fn optional_parallelism(value: &Bound<'_, PyAny>) -> PyResult<Option<TensorParallelismPy>> {
    if value.is_none() {
        Ok(None)
    } else {
        let value: PyRef<'_, TensorParallelismPy> = value.extract()?;
        Ok(Some(TensorParallelismPy::clone(&value)))
    }
}

fn optional_writer_partition(value: &Bound<'_, PyAny>) -> PyResult<Option<WriterPartitionPy>> {
    if value.is_none() {
        Ok(None)
    } else {
        let value: PyRef<'_, WriterPartitionPy> = value.extract()?;
        Ok(Some(WriterPartitionPy::clone(&value)))
    }
}

#[derive(Clone)]
struct ParallelTensorReadCandidate {
    key: String,
    expected_parallelism: Option<TensorParallelismPy>,
}

#[derive(Clone)]
enum ParallelTensorReadPlan {
    Direct {
        base_key: String,
        candidates: Vec<ParallelTensorReadCandidate>,
        reconstruct_shard: Option<TensorParallelismPy>,
    },
    Full {
        base_key: String,
        manifest_key: String,
        parallelism: Option<TensorParallelismPy>,
    },
}

enum ParallelTensorReadMaterialization {
    Single(Vec<u8>),
    Concat {
        payloads: Vec<Vec<u8>>,
        split_dim: usize,
    },
}

struct ParallelFullReadSources {
    payloads: Vec<Vec<u8>>,
    split_dim: usize,
    global_shape: Vec<i64>,
}

fn tp_compatible_parallelism(
    requested: &TensorParallelismPy,
    stored: &TensorParallelismPy,
    expected_tp_rank: Option<i32>,
    expected_tp_size: Option<i32>,
) -> PyResult<bool> {
    let requested = requested.validated_canonical(false)?;
    let stored = stored.validated_canonical(false)?;
    if requested.axes.len() != stored.axes.len() {
        return Ok(false);
    }
    for (requested_axis, stored_axis) in requested.axes.iter().zip(&stored.axes) {
        let requested_kind = requested_axis.parsed_kind()?;
        if requested_kind != stored_axis.parsed_kind()? {
            return Ok(false);
        }
        if requested_kind == AxisKind::Tp {
            if requested_axis.split_dim != stored_axis.split_dim
                || requested_axis.expert_id != stored_axis.expert_id
                || requested_axis.stage_id != stored_axis.stage_id
                || expected_tp_rank.is_some_and(|rank| stored_axis.rank != rank)
                || expected_tp_size.is_some_and(|size| stored_axis.size != size)
            {
                return Ok(false);
            }
        } else if requested_axis != stored_axis {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn load_parallel_full_sources_without_manifest(
    client: &mut MooncakeClient,
    base_key: &str,
    parallelism: &TensorParallelismPy,
) -> PyResult<Option<ParallelFullReadSources>> {
    let parallelism = parallelism.validated_canonical(false)?;
    let Some(tp_axis_index) = parallelism.tp_axis_index()? else {
        return Ok(None);
    };
    let requested_tp = &parallelism.axes[tp_axis_index];
    let mut first_parallelism = parallelism.clone();
    first_parallelism.axes[tp_axis_index].rank = 0;
    let first_key = parallelism_key(base_key, &first_parallelism)?;
    let first_payload = client
        .batch_get(&[first_key])
        .await
        .map_err(to_py_err)?
        .into_iter()
        .next()
        .flatten();
    let Some(first_payload) = first_payload else {
        return Ok(None);
    };
    let first_stored = match crate::tensor_codec::tensor_payload_parallelism(&first_payload) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    if !tp_compatible_parallelism(&parallelism, &first_stored, Some(0), None)? {
        return Ok(None);
    }
    let Some(stored_tp_index) = first_stored.tp_axis_index()? else {
        return Ok(None);
    };
    let stored_tp = &first_stored.axes[stored_tp_index];
    let legacy_single_tp = parallelism.axes.len() == 1;
    let shard_count = if legacy_single_tp {
        stored_tp.size
    } else {
        requested_tp.size
    };
    if shard_count <= 0
        || (!legacy_single_tp && stored_tp.size != shard_count)
        || requested_tp.split_dim != stored_tp.split_dim
    {
        return Ok(None);
    }
    let split_dim = usize::try_from(
        stored_tp
            .split_dim
            .ok_or_else(|| to_py_err("stored TP shard is missing split_dim"))?,
    )
    .map_err(to_py_err)?;
    let global_shape = crate::tensor_codec::tensor_payload_global_shape(&first_payload)?;
    if split_dim >= global_shape.len()
        || global_shape[split_dim] < 0
        || global_shape[split_dim] % i64::from(requested_tp.size) != 0
        || global_shape[split_dim] % i64::from(shard_count) != 0
    {
        return Ok(None);
    }

    let mut shard_keys = Vec::with_capacity(usize::try_from(shard_count).map_err(to_py_err)?);
    for rank in 0..shard_count {
        let mut shard_parallelism = parallelism.clone();
        shard_parallelism.axes[tp_axis_index].rank = rank;
        shard_parallelism.axes[tp_axis_index].size = shard_count;
        shard_keys.push(parallelism_key(base_key, &shard_parallelism)?);
    }
    let payloads = client.batch_get(&shard_keys).await.map_err(to_py_err)?;
    let Some(payloads) = payloads.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    for (rank, payload) in payloads.iter().enumerate() {
        if crate::tensor_codec::tensor_payload_global_shape(payload)? != global_shape {
            return Ok(None);
        }
        let stored = match crate::tensor_codec::tensor_payload_parallelism(payload) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        if !tp_compatible_parallelism(
            &parallelism,
            &stored,
            Some(i32::try_from(rank).map_err(to_py_err)?),
            Some(shard_count),
        )? {
            return Ok(None);
        }
    }
    Ok(Some(ParallelFullReadSources {
        payloads,
        split_dim,
        global_shape,
    }))
}

fn reconstruct_requested_parallel_shard(
    py: Python<'_>,
    sources: ParallelFullReadSources,
    parallelism: &TensorParallelismPy,
) -> PyResult<ParallelTensorReadMaterialization> {
    let parallelism = parallelism.validated_canonical(false)?;
    let tp_axis_index = parallelism
        .tp_axis_index()?
        .ok_or_else(|| to_py_err("requested shard reconstruction requires a TP axis"))?;
    let tp_axis = &parallelism.axes[tp_axis_index];
    let global_extent = usize::try_from(sources.global_shape[sources.split_dim])
        .map_err(|_| to_py_err("parallel tensor global extent is negative"))?;
    let shard_count = usize::try_from(tp_axis.size).map_err(to_py_err)?;
    let rank = usize::try_from(tp_axis.rank).map_err(to_py_err)?;
    if shard_count == 0 || global_extent % shard_count != 0 {
        return Err(to_py_err(
            "requested TP layout does not uniformly divide the stored tensor",
        ));
    }
    let local_extent = global_extent / shard_count;
    let start = rank
        .checked_mul(local_extent)
        .ok_or_else(|| to_py_err("requested TP shard offset overflows usize"))?;
    let full = crate::tensor_codec::deserialize_tensor_payloads_concat(
        py,
        &sources.payloads,
        sources.split_dim,
    )?;
    let shard = full
        .bind(py)
        .call_method1("narrow", (sources.split_dim, start, local_extent))?
        .call_method0("contiguous")?;
    let mut payloads =
        crate::tensor_codec::serialize_parallel_tensor_payloads(&shard, &parallelism)?;
    let (_, payload, _) = payloads
        .pop()
        .ok_or_else(|| to_py_err("requested TP shard serialization produced no payload"))?;
    Ok(ParallelTensorReadMaterialization::Single(payload))
}

async fn load_parallel_full_sources_from_manifest(
    client: &mut MooncakeClient,
    base_key: &str,
    manifest_payload: &[u8],
    parallelism: Option<&TensorParallelismPy>,
) -> PyResult<Option<ParallelFullReadSources>> {
    let Ok(manifest) = ShardManifest::decode(manifest_payload) else {
        return Ok(None);
    };
    let (shard_keys, expected_parallelisms) = if let Some(parallelism) = parallelism {
        let parallelism = parallelism.validated_canonical(false)?;
        let Some(tp_axis_index) = parallelism.tp_axis_index()? else {
            return Ok(None);
        };
        let requested_tp = &parallelism.axes[tp_axis_index];
        if requested_tp
            .split_dim
            .is_some_and(|split_dim| usize::try_from(split_dim).ok() != Some(manifest.split_dim))
            || manifest.split_dim >= manifest.global_shape.len()
            || manifest.global_shape[manifest.split_dim] < 0
            || manifest.global_shape[manifest.split_dim] % i64::from(requested_tp.size) != 0
            || manifest.global_shape[manifest.split_dim]
                % i64::try_from(manifest.shard_count).map_err(to_py_err)?
                != 0
        {
            return Ok(None);
        }
        let manifest_size = i32::try_from(manifest.shard_count).map_err(to_py_err)?;
        let mut keys = Vec::with_capacity(manifest.shard_count);
        let mut expected = Vec::with_capacity(manifest.shard_count);
        for rank in 0..manifest.shard_count {
            let mut shard_parallelism = parallelism.clone();
            shard_parallelism.axes[tp_axis_index].rank = i32::try_from(rank).map_err(to_py_err)?;
            shard_parallelism.axes[tp_axis_index].size = manifest_size;
            keys.push(parallelism_key(base_key, &shard_parallelism)?);
            expected.push(shard_parallelism);
        }
        (keys, expected)
    } else {
        let size = i32::try_from(manifest.shard_count).map_err(to_py_err)?;
        let split_dim = i32::try_from(manifest.split_dim).map_err(to_py_err)?;
        let mut keys = Vec::with_capacity(manifest.shard_count);
        let mut expected = Vec::with_capacity(manifest.shard_count);
        for rank in 0..manifest.shard_count {
            let rank = i32::try_from(rank).map_err(to_py_err)?;
            keys.push(writer_shard_key(
                base_key,
                &WriterPartitionPy {
                    rank,
                    size,
                    split_dim,
                },
            ));
            expected.push(TensorParallelismPy {
                axes: vec![ParallelAxisPy {
                    kind: "tp".to_string(),
                    rank,
                    size,
                    split_dim: Some(split_dim),
                    expert_id: None,
                    stage_id: None,
                }],
            });
        }
        (keys, expected)
    };
    let payloads = client.batch_get(&shard_keys).await.map_err(to_py_err)?;
    let Some(payloads) = payloads.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    if payloads
        .iter()
        .zip(&expected_parallelisms)
        .any(|(payload, expected)| {
            if !crate::tensor_codec::tensor_payload_matches_parallelism(payload, expected) {
                return true;
            }
            match crate::tensor_codec::tensor_payload_global_shape(payload) {
                Ok(shape) => shape != manifest.global_shape,
                Err(_) => true,
            }
        })
    {
        return Ok(None);
    }
    Ok(Some(ParallelFullReadSources {
        payloads,
        split_dim: manifest.split_dim,
        global_shape: manifest.global_shape,
    }))
}

fn parallel_tensor_read_plan(
    key: &str,
    target: Option<&ReadTargetPy>,
) -> PyResult<Option<ParallelTensorReadPlan>> {
    let Some(target) = target else {
        return Ok(Some(ParallelTensorReadPlan::Direct {
            base_key: key.to_string(),
            candidates: vec![ParallelTensorReadCandidate {
                key: key.to_string(),
                expected_parallelism: None,
            }],
            reconstruct_shard: None,
        }));
    };
    match target.mode_name() {
        "as_stored" if target.parallelism.is_none() => Ok(Some(ParallelTensorReadPlan::Direct {
            base_key: key.to_string(),
            candidates: vec![ParallelTensorReadCandidate {
                key: key.to_string(),
                expected_parallelism: None,
            }],
            reconstruct_shard: None,
        })),
        "as_stored" => Ok(None),
        "shard" => {
            let Some(parallelism) = target.parallelism.as_ref() else {
                return Ok(None);
            };
            let parallelism = parallelism.validated(false)?;
            let mut candidates = vec![ParallelTensorReadCandidate {
                key: parallelism_key(key, &parallelism)?,
                expected_parallelism: Some(parallelism.clone()),
            }];
            if parallelism.axes.len() == 1 && parallelism.tp_axis_index()?.is_some() {
                let axis = &parallelism.axes[0];
                candidates.push(ParallelTensorReadCandidate {
                    key: writer_shard_key(
                        key,
                        &WriterPartitionPy {
                            rank: axis.rank,
                            size: axis.size,
                            split_dim: axis.split_dim.unwrap_or(0),
                        },
                    ),
                    expected_parallelism: Some(parallelism.clone()),
                });
            }
            candidates.dedup_by(|left, right| left.key == right.key);
            Ok(Some(ParallelTensorReadPlan::Direct {
                base_key: key.to_string(),
                candidates,
                reconstruct_shard: parallelism
                    .tp_axis_index()?
                    .is_some()
                    .then_some(parallelism),
            }))
        }
        "full" => {
            let parallelism = match target.parallelism.as_ref() {
                Some(parallelism) => {
                    let parallelism = parallelism.validated(false)?;
                    if parallelism.tp_axis_index()?.is_none() {
                        return Ok(None);
                    }
                    Some(parallelism)
                }
                None => None,
            };
            Ok(Some(ParallelTensorReadPlan::Full {
                base_key: key.to_string(),
                manifest_key: if parallelism.is_some() {
                    parallelism_manifest_key(key)
                } else {
                    writer_manifest_key(key)
                },
                parallelism,
            }))
        }
        _ => Ok(None),
    }
}

async fn execute_parallel_tensor_read(
    client: &mut MooncakeClient,
    plan: ParallelTensorReadPlan,
) -> PyResult<Option<ParallelTensorReadMaterialization>> {
    match plan {
        ParallelTensorReadPlan::Direct {
            base_key,
            candidates,
            reconstruct_shard,
        } => {
            let keys = candidates
                .iter()
                .map(|candidate| candidate.key.clone())
                .collect::<Vec<_>>();
            let payloads = client.batch_get(&keys).await.map_err(to_py_err)?;
            for (candidate, payload) in candidates.into_iter().zip(payloads) {
                let Some(payload) = payload else {
                    continue;
                };
                if candidate
                    .expected_parallelism
                    .as_ref()
                    .is_none_or(|expected| {
                        crate::tensor_codec::tensor_payload_matches_parallelism(&payload, expected)
                    })
                {
                    return Ok(Some(ParallelTensorReadMaterialization::Single(payload)));
                }
            }
            if let Some(parallelism) = reconstruct_shard {
                let manifest_payload = client
                    .batch_get(&[parallelism_manifest_key(&base_key)])
                    .await
                    .map_err(to_py_err)?
                    .into_iter()
                    .next()
                    .flatten();
                let mut sources = match manifest_payload {
                    Some(payload) => {
                        load_parallel_full_sources_from_manifest(
                            client,
                            &base_key,
                            &payload,
                            Some(&parallelism),
                        )
                        .await?
                    }
                    None => None,
                };
                if sources.is_none() {
                    sources = load_parallel_full_sources_without_manifest(
                        client,
                        &base_key,
                        &parallelism,
                    )
                    .await?;
                }
                let Some(sources) = sources else {
                    return Ok(None);
                };
                return Python::attach(|py| {
                    reconstruct_requested_parallel_shard(py, sources, &parallelism).map(Some)
                });
            }
            Ok(None)
        }
        ParallelTensorReadPlan::Full {
            base_key,
            manifest_key,
            parallelism,
        } => {
            let manifest_payload = client
                .batch_get(&[manifest_key])
                .await
                .map_err(to_py_err)?
                .into_iter()
                .next()
                .flatten();
            let mut sources = match manifest_payload {
                Some(payload) => {
                    load_parallel_full_sources_from_manifest(
                        client,
                        &base_key,
                        &payload,
                        parallelism.as_ref(),
                    )
                    .await?
                }
                None => None,
            };
            if sources.is_none()
                && let Some(parallelism) = parallelism.as_ref()
            {
                sources =
                    load_parallel_full_sources_without_manifest(client, &base_key, parallelism)
                        .await?;
            }
            Ok(
                sources.map(|sources| ParallelTensorReadMaterialization::Concat {
                    payloads: sources.payloads,
                    split_dim: sources.split_dim,
                }),
            )
        }
    }
}

fn materialize_parallel_tensor_read(
    py: Python<'_>,
    materialization: Option<ParallelTensorReadMaterialization>,
) -> Py<PyAny> {
    let result = match materialization {
        Some(ParallelTensorReadMaterialization::Single(payload)) => {
            crate::tensor_codec::deserialize_tensor_bytes(py, &payload)
        }
        Some(ParallelTensorReadMaterialization::Concat {
            payloads,
            split_dim,
        }) => crate::tensor_codec::deserialize_tensor_payloads_concat(py, &payloads, split_dim),
        None => return py.None(),
    };
    result.unwrap_or_else(|_| py.None())
}

fn parallel_tensor_read_payload(
    py: Python<'_>,
    materialization: ParallelTensorReadMaterialization,
) -> PyResult<Vec<u8>> {
    match materialization {
        ParallelTensorReadMaterialization::Single(payload) => Ok(payload),
        ParallelTensorReadMaterialization::Concat {
            payloads,
            split_dim,
        } => {
            let tensor =
                crate::tensor_codec::deserialize_tensor_payloads_concat(py, &payloads, split_dim)?;
            crate::tensor_codec::serialize_tensor_payload(tensor.bind(py))
        }
    }
}

fn nof_segment_from_dict(d: &Bound<'_, PyDict>) -> PyResult<NoFSegment> {
    let id = d
        .get_item("id")?
        .and_then(|v| v.extract::<String>().ok())
        .map(|s| parse_uuid(&s))
        .transpose()?
        .unwrap_or_else(Uuid::nil);
    let client_id = d
        .get_item("client_id")?
        .and_then(|v| v.extract::<String>().ok())
        .map(|s| parse_uuid(&s))
        .transpose()?
        .unwrap_or_else(Uuid::nil);
    Ok(NoFSegment {
        id,
        name: d
            .get_item("name")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default(),
        base: d
            .get_item("base")?
            .and_then(|v| v.extract::<u64>().ok())
            .unwrap_or(0),
        size: d
            .get_item("size")?
            .and_then(|v| v.extract::<u64>().ok())
            .unwrap_or(0),
        te_endpoint: d
            .get_item("te_endpoint")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default(),
        client_id,
    })
}

// =========================================================================
// Python-exported methods — Python 导出方法
// =========================================================================
//
// Convention for every async method (每个异步方法的约定):
//   1. Extract data from Python objects while GIL is held
//      (在持有 GIL 时从 Python 对象中提取数据)
//   2. Clone the Arc<Mutex<...>> so the future owns its own reference
//      (克隆 Arc<Mutex<...>> 让 future 拥有自己的引用)
//   3. Call future_into_py(py, async move { ... })
//   4. Inside the async block: await take_client(), call the Rust method,
//      then return a Rust type (not PyObject). Dropping the guard releases
//      the client for the next waiter.
//
// Convention for every sync/block_on method (每个 sync/block_on 方法的约定):
//   1. Extract buffer pointers while GIL is held
//      (在持有 GIL 时提取 buffer 指针)
//   2. Clone the Arc<Mutex<...>>
//   3. Call tokio::runtime::Handle::current().block_on(async { ... })
//   4. Use the same cancellation-safe async client guard.

#[pymethods]
impl PythonMooncakeClient {
    // ===================================================================
    // create - 创建客户端
    // ===================================================================

    /// Create a new MooncakeClient and connect to the metadata server.
    ///
    /// 创建新的 MooncakeClient 并连接到元数据服务器。
    ///
    /// Parameters (参数):
    ///   local_hostname:   This node's hostname for RDMA/device identification.
    ///                     本节点用于 RDMA/设备识别的主机名。
    ///   metadata_server:  etcd connection string, e.g. "http://localhost:2379".
    ///                     etcd 连接字符串。
    ///   master_server_addr: Master server address (host:port).
    ///                       主控服务器地址。
    ///   protocol:         Transport protocol, "tcp" or "rdma". 默认 "tcp"。
    ///   device:           RDMA device name, e.g. "mlx5_0". 空字符串表示自动检测。
    ///   global_segment_size: Size of global memory segments. 默认 0 (禁用)。
    ///   local_buffer_size:   Size of local buffer pool. 默认 256 MiB。
    ///   remote_config:    Optional S3 / LocalFS remote source for prefetch.
    ///                     可选的 S3 / LocalFS 远程源配置，用于预取。
    ///   tenant_id:       Default tenant for all convenience KV operations.
    ///                    Empty input is normalized to the canonical default tenant.
    ///   enable_client_http_server: Enable /health and /metrics endpoints.
    ///   client_http_port: Port for the optional client HTTP server. 默认 9300。
    #[staticmethod]
    #[pyo3(signature = (
        local_hostname,
        metadata_server,
        master_server_addr,
        protocol = String::new(),
        device = String::new(),
        global_segment_size = -1,
        local_buffer_size = -1,
        remote_config = None::<PyRemoteSourceConfig>,
        enable_client_http_server = false,
        client_http_port = 9300,
        tenant_id = String::from("default"),
    ))]
    fn create<'py>(
        py: Python<'py>,
        local_hostname: String,
        metadata_server: String,
        master_server_addr: String,
        protocol: String,
        device: String,
        global_segment_size: i64,
        local_buffer_size: i64,
        remote_config: Option<PyRemoteSourceConfig>,
        enable_client_http_server: bool,
        client_http_port: u16,
        tenant_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        use mooncake_store_client::LocalFsSource;

        let normalized = normalize_create_args(
            local_hostname,
            metadata_server,
            master_server_addr,
            protocol,
        )?;
        let gss = if global_segment_size < 0 {
            0u64
        } else {
            global_segment_size as u64
        };
        let lbs = if local_buffer_size < 0 {
            268435456u64
        } else {
            local_buffer_size as u64
        };
        let http_config =
            normalize_client_http_config(enable_client_http_server, client_http_port)?;
        let tenant_id = normalize_client_tenant_id(tenant_id);

        // future_into_py: the async block runs on the tokio runtime, then
        // the returned PythonMooncakeClient is converted into a Python object
        // with the GIL held.
        // future_into_py: async 块在 tokio runtime 上执行，返回的
        // PythonMooncakeClient 在 GIL 持有下转换为 Python 对象。
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = MooncakeClient::create_with_http_config_for_tenant(
                &normalized.master_server_addr,
                &normalized.metadata_server,
                &normalized.local_hostname,
                &normalized.protocol,
                &device,
                gss,
                lbs,
                &tenant_id,
                http_config,
            )
            .await
            .map_err(to_py_err)?;

            // Wire up remote source if configured (可选: 配置远程源)
            if let Some(ref py_cfg) = remote_config {
                let config = py_cfg.to_core();

                if let Some(ref s3_py) = py_cfg.s3_config {
                    #[cfg(feature = "s3")]
                    {
                        use mooncake_store_client::S3RemoteSource;
                        let source = S3RemoteSource::new(&s3_py.to_core())
                            .await
                            .map_err(|e| to_py_err(format!("S3 init failed: {e}")))?;
                        client = client.with_remote_source(source, config);
                    }
                    #[cfg(not(feature = "s3"))]
                    {
                        let _ = s3_py; // used only with s3 feature
                        return Err(to_py_err(
                            "S3 remote source configured but 's3' feature is not enabled. \
                             Rebuild with --features s3",
                        ));
                    }
                } else if let Some(ref root) = py_cfg.local_fs_root {
                    // Local filesystem fallback for dev/test (本地文件系统备选)
                    let source = LocalFsSource::new(root.clone());
                    client = client.with_remote_source(source, config);
                } else if config.enabled {
                    return Err(to_py_err(
                        "RemoteSourceConfig.enabled=true requires s3_config or local_fs_root",
                    ));
                }
            }

            // Return the Rust struct — future_into_py converts it to a Python
            // MooncakeClient object via IntoPy.
            // 返回 Rust 结构体 —— future_into_py 通过 IntoPy 将其转换为
            // Python MooncakeClient 对象。
            let inner = Arc::new(AsyncMutex::new(Some(client)));
            let background_handle = MooncakeClient::start_background_workers(
                Arc::clone(&inner),
                ClientBackgroundConfig::default(),
            );
            Ok(PythonMooncakeClient {
                inner,
                background: Arc::new(AsyncMutex::new(Some(background_handle))),
                registered_py_buffers: Arc::new(Mutex::new(Vec::new())),
            })
        })
    }

    // ===================================================================
    // put / put_parts / get / remove / exists — 基础 KV 操作
    // ===================================================================

    /// Store a single key-value pair.
    /// 存储单个键值对。value 必须是 Python bytes 对象。
    #[pyo3(signature = (key, value, config = None))]
    fn put<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        value: Bound<'py, PyBytes>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Copy out of Python memory while GIL is held
        // 在持有 GIL 时从 Python 内存中拷贝出来
        let data = value.as_bytes().to_vec();
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.put(&key, &data, cfg).await;
            result.map(|()| 0).map_err(to_py_err)
        })
    }

    /// Store a CPU PyTorch tensor using the audited TensorMetadata v1
    /// payload. Accelerator tensors fail closed in the codec.
    #[pyo3(signature = (key, tensor, config = None))]
    fn put_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload = crate::tensor_codec::serialize_tensor_payload(&tensor)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.put(&key, &payload, cfg).await;
            result.map(|()| 0).map_err(to_py_err)
        })
    }

    /// C++ `pub_tensor`: tensor put with an explicit replication config.
    #[pyo3(signature = (key, tensor, config = None))]
    fn pub_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(&value.borrow().to_core()))
        {
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
            });
        }
        Self::put_tensor(slf, py, key, tensor, config)
    }

    /// Write one full tensor, requested parallel shard, or writer partition
    /// using the C++ integration key and manifest conventions.
    #[pyo3(signature = (key, tensor, parallelism = None, config = None, writer_partition = None))]
    fn put_tensor_with_parallelism<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        parallelism: Option<Bound<'py, TensorParallelismPy>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
        writer_partition: Option<Bound<'py, WriterPartitionPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let parallelism = parallelism.map(|value| value.borrow().clone());
        let writer_partition = writer_partition.map(|value| value.borrow().clone());
        let config = config.map(|value| value.borrow().to_core());
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(value))
        {
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
            });
        }
        let plan = match parallel_tensor_write_plan(
            &key,
            &tensor,
            parallelism.as_ref(),
            writer_partition.as_ref(),
        ) {
            Ok(plan) => plan,
            Err(_) => {
                return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
                });
            }
        };
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = async {
                client
                    .put(&plan.object_key, &plan.payload, config.clone())
                    .await?;
                if let Some((manifest_key, manifest)) = plan.manifest {
                    client.put(&manifest_key, &manifest, config).await?;
                }
                Ok::<_, mooncake_store_core::StoreError>(0)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    /// Split a CPU tensor uniformly and store every TP shard under the C++
    /// legacy `{key}_tp_{rank}` naming convention.
    #[pyo3(signature = (key, tensor, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn put_tensor_with_tp<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (_tp_rank, tp_size, split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        let payloads =
            match crate::tensor_codec::serialize_tp_tensor_payloads(&tensor, tp_size, split_dim) {
                Ok(payloads) => payloads,
                Err(_) => {
                    return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                        Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
                    });
                }
            };
        let keys = if tp_size == 1 {
            vec![key]
        } else {
            (0..tp_size).map(|rank| tp_shard_key(&key, rank)).collect()
        };
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let slices = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client.batch_put(&keys, &slices, None).await;
            let statuses = result.map_err(to_py_err)?;
            Ok(statuses
                .into_iter()
                .find(|status| *status != 0)
                .unwrap_or(0))
        })
    }

    /// C++ `pub_tensor_with_tp`: configurable legacy-TP tensor publish.
    #[pyo3(signature = (key, tensor, config = None, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn pub_tensor_with_tp<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (_tp_rank, tp_size, split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        let config = config.map(|value| value.borrow().to_core());
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(value))
        {
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
            });
        }
        let payloads =
            match crate::tensor_codec::serialize_tp_tensor_payloads(&tensor, tp_size, split_dim) {
                Ok(payloads) => payloads,
                Err(_) => {
                    return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                        Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
                    });
                }
            };
        let keys = if tp_size == 1 {
            vec![key]
        } else {
            (0..tp_size).map(|rank| tp_shard_key(&key, rank)).collect()
        };
        if config
            .as_ref()
            .is_some_and(|value| !value.group_ids.is_empty() && value.group_ids.len() != keys.len())
        {
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
            });
        }
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let slices = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client.batch_put(&keys, &slices, config).await;
            let statuses = result.map_err(to_py_err)?;
            Ok(statuses
                .into_iter()
                .find(|status| *status != 0)
                .unwrap_or(0))
        })
    }

    /// Store a value split across multiple byte buffers.
    ///
    /// 将值分片存储 —— 将多个 bytes 对象组合成一个逻辑值。
    /// Useful when the value is naturally fragmented (e.g., multiple page-aligned
    /// RDMA buffers) and you want to avoid an extra concatenation copy.
    /// 当值天然分片时（多个页对齐的 RDMA buffer），避免额外的拼接拷贝。
    #[pyo3(signature = (key, values, config = None))]
    fn put_parts<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config.map(|c| c.borrow().to_core());
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.put_parts(&key, &slices, cfg).await;
            result.map(|()| 0).map_err(to_py_err)
        })
    }

    /// Get the value associated with a key.
    ///
    /// 获取 key 对应的 value，返回 bytes。
    /// Returns Rust Vec<u8> — future_into_py converts it to Python bytes.
    /// 返回 Rust Vec<u8> —— future_into_py 将其转换为 Python bytes。
    fn get<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.get(&key).await;
            // Return Rust type — future_into_py handles IntoPy conversion with GIL
            // 返回 Rust 类型 —— future_into_py 在持有 GIL 时处理 IntoPy 转换
            result.map_err(to_py_err)
        })
    }

    /// Read and materialize a TensorMetadata v1 payload as a CPU PyTorch
    /// tensor. Metadata validation happens after the Store read completes.
    fn get_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.get(&key).await;
            let payload = result.map_err(to_py_err)?;
            Python::attach(|py| crate::tensor_codec::deserialize_tensor_bytes(py, &payload))
        })
    }

    /// Read the current rank's legacy TP shard.
    #[pyo3(signature = (key, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn get_tensor_with_tp<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (tp_rank, tp_size, _split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        let read_key = if tp_size == 1 {
            key
        } else {
            tp_shard_key(&key, tp_rank)
        };
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.get(&read_key).await;
            let payload = result.map_err(to_py_err)?;
            Python::attach(|py| crate::tensor_codec::deserialize_tensor_bytes(py, &payload))
        })
    }

    /// Read an as-stored tensor, one requested shard, or reconstruct a full
    /// tensor from writer/parallelism shards.
    #[pyo3(signature = (key, target = None))]
    fn get_tensor_with_parallelism<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        target: Option<Bound<'py, ReadTargetPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let target = target.map(|value| value.borrow().clone());
        let plan = parallel_tensor_read_plan(&key, target.as_ref())
            .ok()
            .flatten();
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let Some(plan) = plan else {
                return Ok(Python::attach(|py| py.None()));
            };
            let mut client = take_client(&inner).await?;
            let result = execute_parallel_tensor_read(&mut client, plan).await;
            let materialization = result?;
            Ok(Python::attach(|py| {
                materialize_parallel_tensor_read(py, materialization)
            }))
        })
    }

    /// Remove a key. If force=false, only removes soft-pinned objects.
    ///
    /// 删除 key。force=false 仅删除 soft-pinned 对象；
    /// force=true 同时删除 hard-pinned 对象。
    #[pyo3(signature = (key, force = false))]
    fn remove<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.remove(&key, force).await;
            result.map_err(to_py_err)
        })
    }

    /// Check whether a key exists. Returns bool.
    /// 检查 key 是否存在，返回 bool。
    fn exists<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.exists(&key).await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // batch operations — 批量操作
    // ===================================================================

    /// Store multiple key-value pairs in a single RPC batch.
    /// 在单次 RPC 批次中存储多个键值对，减少网络往返。
    #[pyo3(signature = (keys, values, config = None))]
    fn batch_put<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config.map(|c| c.borrow().to_core());
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.batch_put(&keys, &slices, cfg).await;
            result.map_err(to_py_err)
        })
    }

    /// Store multiple CPU PyTorch tensors with one batch Store lifecycle.
    ///
    /// Invalid tensors receive the C++ `INVALID_PARAMS` status while valid
    /// entries continue through the existing Client batch state machine.
    #[pyo3(signature = (keys, tensors, config = None))]
    fn batch_put_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if keys.len() != tensors.len() {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let config = config.map(|value| value.borrow().to_core());
        if config
            .as_ref()
            .is_some_and(|value| !value.group_ids.is_empty() && value.group_ids.len() != keys.len())
        {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let mut statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
        let mut valid_keys = Vec::new();
        let mut valid_payloads = Vec::new();
        let mut original_indices = Vec::new();
        for (index, tensor) in tensors.iter().enumerate() {
            if let Ok(payload) = crate::tensor_codec::serialize_tensor_payload(tensor) {
                valid_keys.push(keys[index].clone());
                valid_payloads.push(payload);
                original_indices.push(index);
            }
        }
        let indexed_config = indexed_batch_config(config, keys.len(), &original_indices)?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if valid_keys.is_empty() {
                return Ok(statuses);
            }
            let mut client = take_client(&inner).await?;
            let slices = valid_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client.batch_put(&valid_keys, &slices, indexed_config).await;
            let valid_statuses = result.map_err(to_py_err)?;
            for (original_index, status) in original_indices.into_iter().zip(valid_statuses) {
                statuses[original_index] = status;
            }
            Ok(statuses)
        })
    }

    /// C++ `batch_pub_tensor`: batch tensor put with replication config.
    #[pyo3(signature = (keys, tensors, config = None))]
    fn batch_pub_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(&value.borrow().to_core()))
        {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }
        Self::batch_put_tensor(slf, py, keys, tensors, config)
    }

    /// Batch variant of `put_tensor_with_parallelism`. C++ executes requested
    /// parallelism/writer entries independently, so each key retains its own
    /// status and group id.
    #[pyo3(signature = (
        keys,
        tensors,
        parallelisms = None,
        config = None,
        writer_partitions = None
    ))]
    fn batch_put_tensor_with_parallelism<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        parallelisms: Option<Vec<Bound<'py, PyAny>>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
        writer_partitions: Option<Vec<Bound<'py, PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if parallelisms.is_none() && writer_partitions.is_none() {
            return Self::batch_put_tensor(slf, py, keys, tensors, config);
        }
        if keys.len() != tensors.len()
            || parallelisms
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || writer_partitions
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || (parallelisms.is_some() && writer_partitions.is_some())
        {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }
        let config = config.map(|value| value.borrow().to_core());
        if config.as_ref().is_some_and(|value| {
            !tensor_publish_config_is_valid(value)
                || (!value.group_ids.is_empty() && value.group_ids.len() != keys.len())
        }) {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let mut statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
        let mut plans = Vec::with_capacity(keys.len());
        for index in 0..keys.len() {
            let parallelism = match parallelisms.as_ref() {
                Some(values) => match optional_parallelism(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let writer = match writer_partitions.as_ref() {
                Some(values) => match optional_writer_partition(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let plan = parallel_tensor_write_plan(
                &keys[index],
                &tensors[index],
                parallelism.as_ref(),
                writer.as_ref(),
            )
            .ok();
            plans.push(plan);
        }
        let per_key_configs = (0..keys.len())
            .map(|index| indexed_batch_config(config.clone(), keys.len(), &[index]))
            .collect::<PyResult<Vec<_>>>()?;
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = async {
                for (index, plan) in plans.into_iter().enumerate() {
                    let Some(plan) = plan else {
                        continue;
                    };
                    let object_keys = [plan.object_key];
                    let object_payloads = [plan.payload];
                    let object_slices = [object_payloads[0].as_slice()];
                    let mut result = client
                        .batch_put(&object_keys, &object_slices, per_key_configs[index].clone())
                        .await?;
                    let mut status = result.pop().unwrap_or(TENSOR_INVALID_PARAMS_STATUS);
                    if status == 0 {
                        if let Some((manifest_key, manifest)) = plan.manifest {
                            let manifest_keys = [manifest_key];
                            let manifests = [manifest];
                            let manifest_slices = [manifests[0].as_slice()];
                            status = client
                                .batch_put(
                                    &manifest_keys,
                                    &manifest_slices,
                                    per_key_configs[index].clone(),
                                )
                                .await?
                                .into_iter()
                                .next()
                                .unwrap_or(TENSOR_INVALID_PARAMS_STATUS);
                        }
                    }
                    statuses[index] = status;
                }
                Ok::<_, mooncake_store_core::StoreError>(statuses)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    /// Batch legacy-TP tensor write. Each base-key status is successful only
    /// when every generated shard commits.
    #[pyo3(signature = (base_keys, tensors, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn batch_put_tensor_with_tp<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        base_keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (_tp_rank, tp_size, split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        if base_keys.len() != tensors.len() {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; base_keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let mut final_statuses = vec![TENSOR_INVALID_PARAMS_STATUS; base_keys.len()];
        let mut shard_keys = Vec::new();
        let mut shard_payloads = Vec::new();
        let mut processed_indices = Vec::new();
        for (index, tensor) in tensors.iter().enumerate() {
            let Ok(payloads) =
                crate::tensor_codec::serialize_tp_tensor_payloads(tensor, tp_size, split_dim)
            else {
                continue;
            };
            processed_indices.push(index);
            for (rank, payload) in payloads.into_iter().enumerate() {
                shard_keys.push(if tp_size == 1 {
                    base_keys[index].clone()
                } else {
                    tp_shard_key(&base_keys[index], rank)
                });
                shard_payloads.push(payload);
            }
        }
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if shard_keys.is_empty() {
                return Ok(final_statuses);
            }
            let mut client = take_client(&inner).await?;
            let slices = shard_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client.batch_put(&shard_keys, &slices, None).await;
            let shard_statuses = result.map_err(to_py_err)?;
            for (processed_offset, original_index) in processed_indices.into_iter().enumerate() {
                let start = processed_offset * tp_size;
                let end = start + tp_size;
                if end > shard_statuses.len() {
                    break;
                }
                final_statuses[original_index] = shard_statuses[start..end]
                    .iter()
                    .copied()
                    .find(|status| *status != 0)
                    .unwrap_or(0);
            }
            Ok(final_statuses)
        })
    }

    /// C++ `batch_pub_tensor_with_tp`: configurable batch legacy-TP publish.
    #[pyo3(signature = (base_keys, tensors, config = None, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn batch_pub_tensor_with_tp<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        base_keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (_tp_rank, tp_size, split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        if base_keys.len() != tensors.len() {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; base_keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }
        let config = config.map(|value| value.borrow().to_core());
        if config.as_ref().is_some_and(|value| {
            !tensor_publish_config_is_valid(value)
                || (!value.group_ids.is_empty() && value.group_ids.len() != base_keys.len())
        }) {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; base_keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let mut final_statuses = vec![TENSOR_INVALID_PARAMS_STATUS; base_keys.len()];
        let mut shard_keys = Vec::new();
        let mut shard_payloads = Vec::new();
        let mut processed_indices = Vec::new();
        for (index, tensor) in tensors.iter().enumerate() {
            let Ok(payloads) =
                crate::tensor_codec::serialize_tp_tensor_payloads(tensor, tp_size, split_dim)
            else {
                continue;
            };
            processed_indices.push(index);
            for (rank, payload) in payloads.into_iter().enumerate() {
                shard_keys.push(if tp_size == 1 {
                    base_keys[index].clone()
                } else {
                    tp_shard_key(&base_keys[index], rank)
                });
                shard_payloads.push(payload);
            }
        }
        let indexed_config =
            repeated_indexed_batch_config(config, base_keys.len(), &processed_indices, tp_size)?;
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if shard_keys.is_empty() {
                return Ok(final_statuses);
            }
            let mut client = take_client(&inner).await?;
            let slices = shard_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client.batch_put(&shard_keys, &slices, indexed_config).await;
            let shard_statuses = result.map_err(to_py_err)?;
            for (processed_offset, original_index) in processed_indices.into_iter().enumerate() {
                let start = processed_offset * tp_size;
                let end = start + tp_size;
                if end > shard_statuses.len() {
                    break;
                }
                final_statuses[original_index] = shard_statuses[start..end]
                    .iter()
                    .copied()
                    .find(|status| *status != 0)
                    .unwrap_or(0);
            }
            Ok(final_statuses)
        })
    }

    /// C++-compatible alias for put_batch: returns one aggregate status.
    #[pyo3(signature = (keys, values, config = None))]
    fn put_batch<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config.map(|c| c.borrow().to_core());
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.batch_put(&keys, &slices, cfg).await;
            let statuses = result.map_err(to_py_err)?;
            Ok(statuses.into_iter().find(|status| *status < 0).unwrap_or(0))
        })
    }

    /// Retrieve multiple values by key. Returns Vec<Vec<u8>>.
    /// 批量获取多个 key 的值，返回 Vec<Vec<u8>>，转为 Python list of bytes。
    fn batch_get<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get(&keys).await;
            // Return Rust type — future_into_py handles IntoPy conversion with GIL
            // 返回 Rust 类型 —— future_into_py 在持有 GIL 时处理 IntoPy 转换
            result.map_err(to_py_err)
        })
    }

    /// Batch-read TensorMetadata payloads and materialize CPU PyTorch tensors.
    ///
    /// Missing or malformed entries produce `None` at the corresponding
    /// position, matching the C++ batch tensor surface.
    fn batch_get_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get(&keys).await;
            let payloads = result.map_err(to_py_err)?;
            Python::attach(|py| {
                let mut tensors = Vec::with_capacity(payloads.len());
                for (index, payload) in payloads.into_iter().enumerate() {
                    let tensor = match payload {
                        Some(payload) => {
                            match crate::tensor_codec::deserialize_tensor_bytes(py, &payload) {
                                Ok(tensor) => tensor,
                                Err(error) => {
                                    warn!(
                                        index,
                                        error = %error,
                                        "batch_get_tensor rejected malformed TensorMetadata payload"
                                    );
                                    py.None()
                                }
                            }
                        }
                        None => py.None(),
                    };
                    tensors.push(tensor);
                }
                Ok(tensors)
            })
        })
    }

    /// Batch ReadTarget variant. Request planning and Store reads remain in
    /// one Rust future; this does not loop through Python-visible single APIs.
    #[pyo3(signature = (keys, targets = None))]
    fn batch_get_tensor_with_parallelism<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        targets: Option<Vec<Bound<'py, PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(targets) = targets else {
            return Self::batch_get_tensor(slf, py, keys);
        };
        if targets.len() != keys.len() {
            let count = keys.len();
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(Python::attach(|py| {
                    (0..count).map(|_| py.None()).collect::<Vec<_>>()
                }))
            });
        }
        let plans = keys
            .iter()
            .zip(&targets)
            .map(|(key, target)| {
                let target = if target.is_none() {
                    None
                } else {
                    match target.extract::<ReadTargetPy>() {
                        Ok(target) => Some(target),
                        Err(_) => return None,
                    }
                };
                parallel_tensor_read_plan(key, target.as_ref())
                    .ok()
                    .flatten()
            })
            .collect::<Vec<_>>();
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = async {
                let mut materializations = Vec::with_capacity(plans.len());
                for plan in plans {
                    materializations.push(match plan {
                        Some(plan) => execute_parallel_tensor_read(&mut client, plan).await?,
                        None => None,
                    });
                }
                Ok::<_, PyErr>(materializations)
            }
            .await;
            let materializations = result?;
            Ok(Python::attach(|py| {
                materializations
                    .into_iter()
                    .map(|materialization| materialize_parallel_tensor_read(py, materialization))
                    .collect::<Vec<_>>()
            }))
        })
    }

    /// Batch-read the current rank's legacy TP shards.
    #[pyo3(signature = (base_keys, tp_rank = 0, tp_size = 1))]
    fn batch_get_tensor_with_tp<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        base_keys: Vec<String>,
        tp_rank: i32,
        tp_size: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (tp_rank, tp_size, _split_dim) = tp_parameters(tp_rank, tp_size, 0)?;
        let keys = if tp_size == 1 {
            base_keys
        } else {
            base_keys
                .iter()
                .map(|key| tp_shard_key(key, tp_rank))
                .collect()
        };
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get(&keys).await;
            let payloads = result.map_err(to_py_err)?;
            Python::attach(|py| {
                let tensors = payloads
                    .into_iter()
                    .map(|payload| match payload {
                        Some(payload) => {
                            crate::tensor_codec::deserialize_tensor_bytes(py, &payload)
                                .unwrap_or_else(|_| py.None())
                        }
                        None => py.None(),
                    })
                    .collect::<Vec<_>>();
                Ok(tensors)
            })
        })
    }

    /// Export a stored tensor to a safetensors file.
    #[pyo3(signature = (key, file_name = None))]
    fn save_tensor_to_safetensor(
        slf: &Bound<'_, Self>,
        key: String,
        file_name: Option<String>,
    ) -> PyResult<i32> {
        let inner = slf.borrow().inner.clone();
        let payload = match pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.get(&key).await;
            result.map_err(to_py_err)
        }) {
            Ok(payload) => payload,
            Err(_) => return Ok(TENSOR_FILE_NOT_FOUND_STATUS),
        };
        let py = slf.py();
        let tensor = match crate::tensor_codec::deserialize_tensor_bytes(py, &payload) {
            Ok(tensor) => tensor,
            Err(_) => return Ok(TENSOR_INVALID_PARAMS_STATUS),
        };
        let resolved_file_name = file_name.unwrap_or_else(|| key.clone());
        let result = (|| -> PyResult<()> {
            let safetensors = py.import("safetensors.torch")?;
            let tensors = PyDict::new(py);
            tensors.set_item(&key, tensor.bind(py))?;
            safetensors.call_method1("save_file", (tensors, resolved_file_name))?;
            Ok(())
        })();
        Ok(if result.is_ok() {
            0
        } else {
            TENSOR_INVALID_PARAMS_STATUS
        })
    }

    /// Load one tensor from a safetensors file, store it, and return it.
    #[pyo3(signature = (key = None, *, file_name))]
    fn load_tensor_from_safetensor(
        slf: &Bound<'_, Self>,
        key: Option<String>,
        file_name: String,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let loaded = match py
            .import("safetensors.torch")
            .and_then(|module| module.call_method1("load_file", (&file_name,)))
        {
            Ok(loaded) => loaded,
            Err(_) => return Ok(py.None()),
        };
        let loaded = match loaded.cast::<PyDict>() {
            Ok(loaded) => loaded,
            Err(_) => return Ok(py.None()),
        };
        let loaded_keys: Vec<String> = match loaded.keys().extract() {
            Ok(keys) => keys,
            Err(_) => return Ok(py.None()),
        };
        let Some(first_key) = loaded_keys.first() else {
            return Ok(py.None());
        };
        let target_store_key = key.clone().unwrap_or_else(|| file_name.clone());
        let selected_key = if key
            .as_ref()
            .is_some_and(|desired| loaded.contains(desired).unwrap_or(false))
        {
            target_store_key.clone()
        } else {
            first_key.clone()
        };
        let tensor = match loaded.get_item(&selected_key) {
            Ok(Some(tensor)) => tensor,
            Ok(None) | Err(_) => return Ok(py.None()),
        };
        let payload = match crate::tensor_codec::serialize_tensor_payload(&tensor) {
            Ok(payload) => payload,
            Err(_) => return Ok(py.None()),
        };
        let inner = slf.borrow().inner.clone();
        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.put(&target_store_key, &payload, None).await;
            result.map_err(to_py_err)
        });
        if result.is_err() {
            return Ok(py.None());
        }
        Ok(tensor.unbind())
    }

    /// Remove multiple keys. force=true removes hard-pinned objects too.
    /// 批量删除多个 key。force=true 也会删除 hard-pinned 对象。
    #[pyo3(signature = (keys, force = false))]
    fn batch_remove<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_remove(&keys, force).await;
            result.map_err(to_py_err)
        })
    }

    /// Check existence of multiple keys. Returns Vec<bool>.
    /// 批量检查多个 key 是否存在，返回 Vec<bool>。
    fn batch_is_exist<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_is_exist(&keys).await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // prefetch — 预取
    // ===================================================================

    /// Prefetch a list of keys from the configured remote source (S3 / local FS).
    /// Fetched data is stored in the local hot cache.
    ///
    /// 从配置的远程源（S3 / 本地文件系统）预取一组 key。
    /// 获取的数据存储在本地热缓存中。
    ///
    /// Use this BEFORE a training batch to warm the cache with keys you know
    /// will be accessed soon.
    /// 在训练批次之前调用，用于预热缓存 —— 提前加载即将访问的 key。
    fn prefetch<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.prefetch(&keys).await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // remove_by_regex / remove_all / get_size — 正则删除/全量删除/大小查询
    // ===================================================================

    /// Remove all keys matching a regex pattern. Returns count of removed keys.
    /// 删除所有匹配正则表达式的 key。返回删除的 key 数量。
    #[pyo3(signature = (pattern, force = false))]
    fn remove_by_regex<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        pattern: String,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let removed = client.remove_by_regex(&pattern, force).await;
            removed.map_err(to_py_err)
        })
    }

    /// Remove all keys from the store. Destructive — careful!
    /// 删除 store 中的所有 key。危险操作，请谨慎使用！
    #[pyo3(signature = (force = false))]
    fn remove_all<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let removed = client.remove_all(force).await;
            removed.map_err(to_py_err)
        })
    }

    /// Get the size in bytes of a stored value. Returns u64.
    /// 获取存储值的字节大小，返回 u64。
    fn get_size<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let size = client.get_size(&key).await;
            size.map_err(to_py_err)
        })
    }

    // ===================================================================
    // health / tear down / close — 健康检查/关闭/生命周期管理
    // ===================================================================

    /// Return the hostname this client registered with.
    /// 返回此客户端注册时使用的主机名。
    fn get_hostname(&self) -> PyResult<String> {
        match try_client_slot(&self.inner)?.as_ref() {
            Some(client) => Ok(client.get_hostname()),
            None => Err(to_py_err("client already closed")),
        }
    }

    /// Check connectivity to the metadata server and master.
    /// 检查与元数据服务器和 master 的连接状态。返回 bool。
    fn health_check<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.health_check().await;
            result.map(|()| true).map_err(to_py_err)
        })
    }

    /// Return the currently connected master address.
    fn current_master_addr(&self) -> PyResult<String> {
        match try_client_slot(&self.inner)?.as_ref() {
            Some(client) => Ok(client.current_master_addr()),
            None => Err(to_py_err("client already closed")),
        }
    }

    /// Replace the client-side master failover candidate list.
    fn set_master_candidates(&self, candidates: Vec<String>) -> PyResult<()> {
        match try_client_slot(&self.inner)?.as_ref() {
            Some(client) => client.set_master_candidates(candidates).map_err(to_py_err),
            None => Err(to_py_err("client already closed")),
        }
    }

    /// Return the configured master failover candidate list.
    fn master_candidates(&self) -> PyResult<Vec<String>> {
        match try_client_slot(&self.inner)?.as_ref() {
            Some(client) => Ok(client.master_candidates()),
            None => Err(to_py_err("client already closed")),
        }
    }

    /// Switch to a specific master address.
    fn switch_master<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        master_addr: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.switch_master(&master_addr).await;
            result.map_err(to_py_err)
        })
    }

    /// Try configured master candidates until one connects. Returns new address.
    fn failover_master<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.failover_master().await;
            result.map_err(to_py_err)
        })
    }

    /// Tear down all resources: buffers, segments, metadata entries.
    /// 销毁所有资源：缓冲区、段、元数据条目。用于整个集群的清理。
    fn tear_down_all<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();
        let background = slf.borrow().background.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(handle) = background.lock().await.take() {
                handle.shutdown().await;
            }
            let mut client = take_client(&inner).await?;
            let result = client.tear_down_all().await;
            result.map_err(to_py_err)
        })
    }

    /// Check whether this client has been closed.
    /// 检查此客户端是否已关闭。
    fn is_closed(&self) -> bool {
        match self.inner.try_lock() {
            Ok(slot) => slot.as_ref().is_none_or(MooncakeClient::is_closed),
            // A contended slot still contains the live client.
            Err(_) => false,
        }
    }

    /// Close the client after completing Store teardown. The returned
    /// awaitable is cancellation-safe with respect to Rust ownership.
    fn close<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();
        let background = slf.borrow().background.clone();
        let registered_py_buffers = slf.borrow().registered_py_buffers.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(handle) = background.lock().await.take() {
                handle.shutdown().await;
            }
            let mut slot = inner.lock().await;
            let Some(client) = slot.as_mut() else {
                registered_py_buffers.lock().clear();
                return Ok(0);
            };
            client.tear_down_all().await.map_err(to_py_err)?;
            drop(slot.take());
            drop(slot);
            registered_py_buffers.lock().clear();
            Ok(0)
        })
    }

    /// String representation: "MooncakeClient(connected)" or
    /// "MooncakeClient(closed)".
    fn __repr__(&self) -> String {
        match self.inner.try_lock() {
            Ok(slot) if slot.is_none() => "MooncakeClient(closed)".to_string(),
            Ok(_) | Err(_) => "MooncakeClient(connected)".to_string(),
        }
    }

    // ===================================================================
    // upsert / upsert_parts — 插入或更新
    // ===================================================================

    /// Insert-or-update a key-value pair. Returns replica locations.
    ///
    /// 插入或更新键值对。返回副本位置列表 List[(segment_name, offset, segment_id)]。
    /// Unlike put(), upsert() can overwrite existing keys and returns
    /// the replica descriptors showing where the data was allocated.
    /// 与 put() 不同，upsert() 可以覆盖已有 key 并返回副本分配信息。
    #[pyo3(signature = (key, value, config = None))]
    fn upsert<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        value: Bound<'py, PyBytes>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data = value.as_bytes().to_vec();
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();

        // Return Rust tuple list — future_into_py handles IntoPy conversion
        // 返回 Rust 元组列表 —— future_into_py 处理 IntoPy 转换
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.upsert(&key, &data, cfg).await;
            let replicas = result.map_err(to_py_err)?;
            let out: Vec<(String, u64, String)> = replicas
                .iter()
                .map(|r| (r.segment_name.clone(), r.offset, r.segment_id.to_string()))
                .collect();
            Ok(out)
        })
    }

    /// Upsert a CPU PyTorch tensor using the same strict TensorMetadata payload
    /// as `put_tensor`.
    #[pyo3(signature = (key, tensor, config = None))]
    fn upsert_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload = crate::tensor_codec::serialize_tensor_payload(&tensor)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.upsert(&key, &payload, cfg).await;
            result.map(|_| 0).map_err(to_py_err)
        })
    }

    /// C++ `upsert_pub_tensor`: tensor upsert with replication config.
    #[pyo3(signature = (key, tensor, config = None))]
    fn upsert_pub_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(&value.borrow().to_core()))
        {
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
            });
        }
        Self::upsert_tensor(slf, py, key, tensor, config)
    }

    /// Upsert one full tensor, requested parallel shard, or writer partition
    /// using the C++ integration key and manifest conventions.
    #[pyo3(signature = (key, tensor, parallelism = None, config = None, writer_partition = None))]
    fn upsert_tensor_with_parallelism<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        tensor: Bound<'py, PyAny>,
        parallelism: Option<Bound<'py, TensorParallelismPy>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
        writer_partition: Option<Bound<'py, WriterPartitionPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let parallelism = parallelism.map(|value| value.borrow().clone());
        let writer_partition = writer_partition.map(|value| value.borrow().clone());
        let config = config.map(|value| value.borrow().to_core());
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(value))
        {
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
            });
        }
        let plan = match parallel_tensor_write_plan(
            &key,
            &tensor,
            parallelism.as_ref(),
            writer_partition.as_ref(),
        ) {
            Ok(plan) => plan,
            Err(_) => {
                return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    Ok::<_, PyErr>(TENSOR_INVALID_PARAMS_STATUS)
                });
            }
        };
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = async {
                client
                    .upsert(&plan.object_key, &plan.payload, config.clone())
                    .await?;
                if let Some((manifest_key, manifest)) = plan.manifest {
                    client.upsert(&manifest_key, &manifest, config).await?;
                }
                Ok::<_, mooncake_store_core::StoreError>(0)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    /// Batch-upsert CPU PyTorch tensors with one status per input key.
    ///
    /// Tensor adaptation remains in Python/Rust binding code; the actual
    /// multi-key mutation and finalize lifecycle is owned by the Client and
    /// Master.
    #[pyo3(signature = (keys, tensors, config = None))]
    fn batch_upsert_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if keys.len() != tensors.len() {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let config = config.map(|value| value.borrow().to_core());
        if config
            .as_ref()
            .is_some_and(|value| !value.group_ids.is_empty() && value.group_ids.len() != keys.len())
        {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let mut statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
        let mut valid_keys = Vec::new();
        let mut valid_payloads = Vec::new();
        let mut original_indices = Vec::new();
        for (index, tensor) in tensors.iter().enumerate() {
            if let Ok(payload) = crate::tensor_codec::serialize_tensor_payload(tensor) {
                valid_keys.push(keys[index].clone());
                valid_payloads.push(payload);
                original_indices.push(index);
            }
        }
        let indexed_config = indexed_batch_config(config, keys.len(), &original_indices)?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if valid_keys.is_empty() {
                return Ok(statuses);
            }
            let mut client = take_client(&inner).await?;
            let slices = valid_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client
                .batch_upsert(&valid_keys, &slices, indexed_config)
                .await;
            let valid_statuses = result.map_err(to_py_err)?;
            for (original_index, status) in original_indices.into_iter().zip(valid_statuses) {
                statuses[original_index] = status;
            }
            Ok(statuses)
        })
    }

    /// C++ `batch_upsert_pub_tensor`: batch tensor upsert with replication
    /// config.
    #[pyo3(signature = (keys, tensors, config = None))]
    fn batch_upsert_pub_tensor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(&value.borrow().to_core()))
        {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }
        Self::batch_upsert_tensor(slf, py, keys, tensors, config)
    }

    /// Batch variant of `upsert_tensor_with_parallelism`, preserving one
    /// result per base key and the C++ per-key routing behavior.
    #[pyo3(signature = (
        keys,
        tensors,
        parallelisms = None,
        config = None,
        writer_partitions = None
    ))]
    fn batch_upsert_tensor_with_parallelism<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        tensors: Vec<Bound<'py, PyAny>>,
        parallelisms: Option<Vec<Bound<'py, PyAny>>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
        writer_partitions: Option<Vec<Bound<'py, PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if parallelisms.is_none() && writer_partitions.is_none() {
            return Self::batch_upsert_tensor(slf, py, keys, tensors, config);
        }
        if keys.len() != tensors.len()
            || parallelisms
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || writer_partitions
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || (parallelisms.is_some() && writer_partitions.is_some())
        {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }
        let config = config.map(|value| value.borrow().to_core());
        if config.as_ref().is_some_and(|value| {
            !tensor_publish_config_is_valid(value)
                || (!value.group_ids.is_empty() && value.group_ids.len() != keys.len())
        }) {
            let statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok::<_, PyErr>(statuses)
            });
        }

        let mut statuses = vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()];
        let mut plans = Vec::with_capacity(keys.len());
        for index in 0..keys.len() {
            let parallelism = match parallelisms.as_ref() {
                Some(values) => match optional_parallelism(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let writer = match writer_partitions.as_ref() {
                Some(values) => match optional_writer_partition(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            plans.push(
                parallel_tensor_write_plan(
                    &keys[index],
                    &tensors[index],
                    parallelism.as_ref(),
                    writer.as_ref(),
                )
                .ok(),
            );
        }
        let per_key_configs = (0..keys.len())
            .map(|index| indexed_batch_config(config.clone(), keys.len(), &[index]))
            .collect::<PyResult<Vec<_>>>()?;
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = async {
                for (index, plan) in plans.into_iter().enumerate() {
                    let Some(plan) = plan else {
                        continue;
                    };
                    let object_keys = [plan.object_key];
                    let object_payloads = [plan.payload];
                    let object_slices = [object_payloads[0].as_slice()];
                    let mut result = client
                        .batch_upsert(&object_keys, &object_slices, per_key_configs[index].clone())
                        .await?;
                    let mut status = result.pop().unwrap_or(TENSOR_INVALID_PARAMS_STATUS);
                    if status == 0 {
                        if let Some((manifest_key, manifest)) = plan.manifest {
                            let manifest_keys = [manifest_key];
                            let manifests = [manifest];
                            let manifest_slices = [manifests[0].as_slice()];
                            status = client
                                .batch_upsert(
                                    &manifest_keys,
                                    &manifest_slices,
                                    per_key_configs[index].clone(),
                                )
                                .await?
                                .into_iter()
                                .next()
                                .unwrap_or(TENSOR_INVALID_PARAMS_STATUS);
                        }
                    }
                    statuses[index] = status;
                }
                Ok::<_, mooncake_store_core::StoreError>(statuses)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    /// Insert-or-update a value split across multiple byte buffers.
    /// 将分片值插入或更新。类似 upsert() 但接受多个 bytes 对象作为分片值。
    #[pyo3(signature = (key, values, config = None))]
    fn upsert_parts<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config.map(|c| c.borrow().to_core());
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let inner = slf.borrow().inner.clone();

        // Return Rust tuple list — future_into_py handles IntoPy conversion
        // 返回 Rust 元组列表 —— future_into_py 处理 IntoPy 转换
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.upsert_parts(&key, &slices, cfg).await;
            let replicas = result.map_err(to_py_err)?;
            let out: Vec<(String, u64, String)> = replicas
                .iter()
                .map(|r| (r.segment_name.clone(), r.offset, r.segment_id.to_string()))
                .collect();
            Ok(out)
        })
    }

    /// C++-compatible batch upsert: returns one aggregate status.
    #[pyo3(signature = (keys, values, config = None))]
    fn upsert_batch<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        values: Vec<Bound<'py, PyBytes>>,
        config: Option<Bound<'py, ReplicateConfigPy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if keys.len() != values.len() {
            return Err(to_py_err("keys and values must have same length"));
        }
        let cfg = config.map(|c| c.borrow().to_core());
        let data: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let mut status = 0;
            for (key, value) in keys.iter().zip(data.iter()) {
                if client.upsert(key, value, cfg.clone()).await.is_err() {
                    status = -1;
                    break;
                }
            }
            Ok(status)
        })
    }

    // ===================================================================
    // Task management — 任务管理（复制/移动/查询/完成）
    // ===================================================================

    /// Create a task to copy a key to the given target nodes.
    /// 创建复制任务：将 key 复制到目标节点。返回任务 UUID 字符串。
    fn create_copy_task<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        targets: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.create_copy_task(&key, &targets).await;
            let task_id = result.map_err(to_py_err)?;
            Ok(task_id.to_string())
        })
    }

    /// Create a task to move a key from source to target node.
    /// 创建移动任务：将 key 从 source 节点移动到 target 节点。
    fn create_move_task<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        source: String,
        target: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.create_move_task(&key, &source, &target).await;
            let task_id = result.map_err(to_py_err)?;
            Ok(task_id.to_string())
        })
    }

    /// Query the status of a task by its UUID.
    /// 根据 UUID 查询任务状态。返回 (task_id, status, message)。
    fn query_task<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        task_id_str: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let task_id =
            Uuid::parse_str(&task_id_str).map_err(|e| to_py_err(format!("invalid UUID: {e}")))?;
        let inner = slf.borrow().inner.clone();

        // Return Rust tuple — future_into_py handles IntoPy conversion
        // 返回 Rust 元组 —— future_into_py 处理 IntoPy 转换
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.query_task(task_id).await;
            let resp = result.map_err(to_py_err)?;
            let task_id_str = resp
                .id
                .map(|id| Uuid::from_u64_pair(id.high, id.low).to_string());
            Ok((task_id_str, resp.status, resp.message))
        })
    }

    /// Fetch up to batch_size pending tasks from the task queue.
    /// 从任务队列中获取最多 batch_size 个待处理任务。
    /// Returns list of (task_id, type, payload, created_at_ms, max_retry_attempts).
    fn fetch_tasks<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        batch_size: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        // Return Rust data — future_into_py handles IntoPy conversion
        // 返回 Rust 数据 —— future_into_py 处理 IntoPy 转换
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let tasks = client.fetch_tasks(batch_size).await.map_err(to_py_err)?;
            let out: Vec<(Option<String>, i32, String, i64, u32)> = tasks
                .iter()
                .map(|t| {
                    let id_str =
                        t.id.map(|id| Uuid::from_u64_pair(id.high, id.low).to_string());
                    (
                        id_str,
                        t.r#type,
                        t.payload.clone(),
                        t.created_at_ms_epoch,
                        t.max_retry_attempts,
                    )
                })
                .collect();
            Ok(out)
        })
    }

    /// Mark a task as completed (or failed). status uses the TaskStatus enum:
    /// 0=Pending, 1=Running, 2=Completed, 3=Failed, etc.
    ///
    /// 将任务标记为已完成（或失败）。status 使用 TaskStatus 枚举值。
    #[pyo3(signature = (task_id_str, status, message = String::new()))]
    fn mark_task_to_complete<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        task_id_str: String,
        status: i32,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let task_id =
            Uuid::parse_str(&task_id_str).map_err(|e| to_py_err(format!("invalid UUID: {e}")))?;
        let inner = slf.borrow().inner.clone();

        let proto_status = mooncake_store_client::proto::TaskStatus::try_from(status)
            .map_err(|_| to_py_err(format!("invalid task status: {status}")))?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client
                .mark_task_to_complete(task_id, proto_status, &message)
                .await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Endpoint management — 端点注册/注销
    // ===================================================================

    /// Register a local transport endpoint (e.g., "127.0.0.1:12345").
    /// 注册本地传输端点（如 "127.0.0.1:12345"），用于 RDMA/TCP 数据传输。
    /// This is a synchronous method — no async needed.
    /// 这是同步方法 —— 无需异步。
    fn register_local_endpoint(&self, endpoint: String) -> PyResult<()> {
        match try_client_slot(&self.inner)?.as_ref() {
            Some(client) => {
                client.register_local_endpoint(&endpoint);
                Ok(())
            }
            None => Err(to_py_err("client already closed")),
        }
    }

    /// Unregister a previously registered local endpoint.
    /// 注销之前注册的本地传输端点。
    fn unregister_local_endpoint(&self, endpoint: String) -> PyResult<()> {
        match try_client_slot(&self.inner)?.as_ref() {
            Some(client) => {
                client.unregister_local_endpoint(&endpoint);
                Ok(())
            }
            None => Err(to_py_err("client already closed")),
        }
    }

    // ===================================================================
    // Zero-copy read — 零拷贝读取
    //
    // These methods use block_on instead of future_into_py because the
    // underlying futures capture raw pointers (ptr) across await points,
    // making them !Send.  future_into_py requires Send futures.
    //
    // block_on 会阻塞当前 OS 线程直到异步操作完成。虽然会阻塞线程，但避免了
    // 额外的内存拷贝 —— 数据直接通过 RDMA 写入 Python buffer 对象。
    // ===================================================================

    /// Read a key directly into a Python buffer (zero-copy via RDMA).
    ///
    /// 零拷贝读取：通过 RDMA 直接将数据读入 Python buffer（如 bytearray、
    /// memoryview、numpy array）。返回实际读取的字节数。
    /// buffer must support the Python buffer protocol and be large enough.
    #[pyo3(signature = (key, buffer, size = None))]
    fn get_into(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: Option<usize>,
    ) -> PyResult<usize> {
        let (ptr, size) = get_pointer_and_size(&buffer, size)?;
        let inner = slf.borrow().inner.clone();
        // block_on: future captures raw ptr, not Send-safe
        // block_on: future 捕获了裸指针，不是 Send 的
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.get_into(&key, ptr, size).await;
            result.map_err(to_py_err)
        })
    }

    /// Read a TensorMetadata payload directly into a registered writable
    /// Python buffer and return a PyTorch tensor view over that owner.
    #[pyo3(signature = (key, buffer, size))]
    fn get_tensor_into(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
    ) -> PyResult<Py<PyAny>> {
        if buffer.extract::<usize>().is_ok() {
            return Err(to_py_err(
                "get_tensor_into requires an owner-bearing Python buffer, not a raw address",
            ));
        }
        let (ptr, capacity) = get_pointer_and_size(&buffer, Some(size))?;
        let inner = slf.borrow().inner.clone();
        let total_length = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.get_into(&key, ptr, capacity).await;
            result.map_err(to_py_err)
        })?;
        crate::tensor_codec::deserialize_tensor_buffer_object(buffer.py(), &buffer, total_length)
    }

    /// TP-key variant of `get_tensor_into`.
    #[pyo3(signature = (key, buffer, size, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn get_tensor_with_tp_into(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<Py<PyAny>> {
        let (tp_rank, tp_size, _split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        if buffer.extract::<usize>().is_ok() {
            return Err(to_py_err(
                "get_tensor_with_tp_into requires an owner-bearing Python buffer",
            ));
        }
        let read_key = if tp_size == 1 {
            key
        } else {
            tp_shard_key(&key, tp_rank)
        };
        let (ptr, capacity) = get_pointer_and_size(&buffer, Some(size))?;
        let inner = slf.borrow().inner.clone();
        let total_length = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.get_into(&read_key, ptr, capacity).await;
            result.map_err(to_py_err)
        })?;
        crate::tensor_codec::deserialize_tensor_buffer_object(buffer.py(), &buffer, total_length)
    }

    /// Read an as-stored tensor, requested shard, or reconstructed full tensor
    /// into an owner-bearing buffer registered with this Client.
    #[pyo3(signature = (key, buffer, size, target = None))]
    fn get_tensor_with_parallelism_into(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        target: Option<Bound<'_, ReadTargetPy>>,
    ) -> PyResult<Py<PyAny>> {
        validate_registered_tensor_destination(slf, &buffer, size)?;
        let target = target.map(|value| value.borrow().clone());
        let Some(plan) = parallel_tensor_read_plan(&key, target.as_ref())
            .ok()
            .flatten()
        else {
            return Ok(buffer.py().None());
        };
        let inner = slf.borrow().inner.clone();
        let materialization = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = execute_parallel_tensor_read(&mut client, plan).await;
            result
        })?;
        let Some(materialization) = materialization else {
            return Ok(buffer.py().None());
        };
        let py = buffer.py();
        let Ok(payload) = parallel_tensor_read_payload(py, materialization) else {
            return Ok(py.None());
        };
        Ok(
            crate::tensor_codec::copy_tensor_payload_into_buffer(py, &buffer, size, &payload)
                .unwrap_or_else(|_| py.None()),
        )
    }

    /// Batch zero-copy read into per-key buffers.
    ///
    /// 批量零拷贝读取：每个 key 对应一个 buffer。
    /// keys, buffers, sizes must have the same length.
    /// 返回每个 key 的读取字节数的 Vec<i64>。
    fn batch_get_into(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
    ) -> PyResult<Vec<i64>> {
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            return Err(to_py_err("keys, buffers, sizes must have same length"));
        }
        let ptrs: Vec<*mut c_void> = buffers
            .iter()
            .map(get_writable_pointer)
            .collect::<PyResult<_>>()?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get_into(&keys, &ptrs, &sizes).await;
            result.map_err(to_py_err)
        })
    }

    /// Batch variant of `get_tensor_into`.
    ///
    /// Each successful slot is a PyTorch tensor view over the corresponding
    /// registered owner; failed slots preserve the Client's negative status.
    fn batch_get_tensor_into(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            let py = slf.py();
            return (0..keys.len())
                .map(|_| TENSOR_INVALID_PARAMS_STATUS.into_py_any(py))
                .collect();
        }
        for (buffer, size) in buffers.iter().zip(&sizes) {
            if buffer.extract::<usize>().is_ok() {
                return Err(to_py_err(
                    "batch_get_tensor_into requires owner-bearing Python buffers, not raw addresses",
                ));
            }
            let export = export_c_contiguous_buffer(buffer, true)?;
            if *size > export.len_bytes() {
                return Err(to_py_err(
                    "tensor destination size exceeds Python buffer capacity",
                ));
            }
        }
        let ptrs = buffers
            .iter()
            .map(get_writable_pointer)
            .collect::<PyResult<Vec<_>>>()?;
        let inner = slf.borrow().inner.clone();
        let lengths = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get_into(&keys, &ptrs, &sizes).await;
            result.map_err(to_py_err)
        })?;

        let py = slf.py();
        lengths
            .into_iter()
            .zip(buffers)
            .map(|(length, buffer)| {
                if length < 0 {
                    return length.into_py_any(py);
                }
                let length = usize::try_from(length)
                    .map_err(|_| to_py_err("tensor payload length exceeds usize"))?;
                crate::tensor_codec::deserialize_tensor_buffer_object(py, &buffer, length)
            })
            .collect()
    }

    /// Batch TP-key variant of `batch_get_tensor_into`.
    #[pyo3(signature = (base_keys, buffers, sizes, tp_rank = 0, tp_size = 1))]
    fn batch_get_tensor_with_tp_into(
        slf: &Bound<'_, Self>,
        base_keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        tp_rank: i32,
        tp_size: i32,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let (tp_rank, tp_size, _split_dim) = tp_parameters(tp_rank, tp_size, 0)?;
        let keys = if tp_size == 1 {
            base_keys
        } else {
            base_keys
                .iter()
                .map(|key| tp_shard_key(key, tp_rank))
                .collect()
        };
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            let py = slf.py();
            return Ok((0..keys.len()).map(|_| py.None()).collect());
        }
        for (buffer, size) in buffers.iter().zip(&sizes) {
            if buffer.extract::<usize>().is_ok() {
                return Err(to_py_err(
                    "batch_get_tensor_with_tp_into requires owner-bearing Python buffers",
                ));
            }
            let export = export_c_contiguous_buffer(buffer, true)?;
            if *size > export.len_bytes() {
                return Err(to_py_err(
                    "TP tensor destination size exceeds Python buffer capacity",
                ));
            }
        }
        let ptrs = buffers
            .iter()
            .map(get_writable_pointer)
            .collect::<PyResult<Vec<_>>>()?;
        let inner = slf.borrow().inner.clone();
        let lengths = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get_into(&keys, &ptrs, &sizes).await;
            result.map_err(to_py_err)
        })?;
        let py = slf.py();
        lengths
            .into_iter()
            .zip(buffers)
            .map(|(length, buffer)| {
                if length < 0 {
                    return Ok(py.None());
                }
                let length = usize::try_from(length)
                    .map_err(|_| to_py_err("TP tensor payload length exceeds usize"))?;
                crate::tensor_codec::deserialize_tensor_buffer_object(py, &buffer, length)
            })
            .collect()
    }

    /// Batch ReadTarget variant of `get_tensor_with_parallelism_into`.
    #[pyo3(signature = (keys, buffers, sizes, targets = None))]
    fn batch_get_tensor_with_parallelism_into(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        targets: Option<Vec<Bound<'_, PyAny>>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let py = slf.py();
        if keys.len() != buffers.len()
            || keys.len() != sizes.len()
            || targets
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
        {
            return Ok((0..keys.len()).map(|_| py.None()).collect());
        }
        for (buffer, size) in buffers.iter().zip(&sizes) {
            validate_registered_tensor_destination(slf, buffer, *size)?;
        }
        let plans = keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                let target = match targets.as_ref() {
                    Some(values) if values[index].is_none() => None,
                    Some(values) => match values[index].extract::<ReadTargetPy>() {
                        Ok(target) => Some(target),
                        Err(_) => return None,
                    },
                    None => None,
                };
                parallel_tensor_read_plan(key, target.as_ref())
                    .ok()
                    .flatten()
            })
            .collect::<Vec<_>>();
        let inner = slf.borrow().inner.clone();
        let materializations = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = async {
                let mut materializations = Vec::with_capacity(plans.len());
                for plan in plans {
                    materializations.push(match plan {
                        Some(plan) => execute_parallel_tensor_read(&mut client, plan).await?,
                        None => None,
                    });
                }
                Ok::<_, PyErr>(materializations)
            }
            .await;
            result
        })?;
        materializations
            .into_iter()
            .zip(buffers)
            .zip(sizes)
            .map(|((materialization, buffer), size)| {
                let Some(materialization) = materialization else {
                    return Ok(py.None());
                };
                let Ok(payload) = parallel_tensor_read_payload(py, materialization) else {
                    return Ok(py.None());
                };
                Ok(crate::tensor_codec::copy_tensor_payload_into_buffer(
                    py, &buffer, size, &payload,
                )
                .unwrap_or_else(|_| py.None()))
            })
            .collect()
    }

    /// Batch zero-copy read with multiple candidate buffers per key.
    ///
    /// 批量零拷贝读取，每个 key 有多个候选 buffer。
    /// For each key, the system picks the best buffer (e.g., local RDMA
    /// segment).  prefer_same_node optimizes for node-local reads.
    /// 每个 key 系统选择最佳 buffer（如本地 RDMA 段）。
    /// prefer_same_node 优先本地节点读取。
    #[pyo3(signature = (keys, all_buffers, all_sizes, prefer_same_node = false))]
    fn batch_get_into_multi_buffers(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        all_buffers: Vec<Vec<Bound<'_, PyAny>>>,
        all_sizes: Vec<Vec<usize>>,
        prefer_same_node: bool,
    ) -> PyResult<Vec<i64>> {
        let ptrs: Vec<Vec<*mut c_void>> = all_buffers
            .iter()
            .map(|bufs| {
                bufs.iter()
                    .map(get_writable_pointer)
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<_>>()?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client
                .batch_get_into_multi_buffers(&keys, &ptrs, &all_sizes, prefer_same_node)
                .await;
            result.map_err(to_py_err)
        })
    }

    /// Zero-copy multi-range read into pre-registered Python buffers.
    fn get_into_ranges(
        slf: &Bound<'_, Self>,
        buffers: Vec<Bound<'_, PyAny>>,
        all_keys: Vec<Vec<String>>,
        all_dst_offsets: Vec<Vec<Vec<usize>>>,
        all_src_offsets: Vec<Vec<Vec<usize>>>,
        all_sizes: Vec<Vec<Vec<usize>>>,
    ) -> PyResult<Vec<Vec<Vec<i64>>>> {
        let ptrs: Vec<*mut c_void> = buffers
            .iter()
            .map(get_writable_pointer)
            .collect::<PyResult<_>>()?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client
                .get_into_ranges(
                    &ptrs,
                    &all_keys,
                    &all_dst_offsets,
                    &all_src_offsets,
                    &all_sizes,
                )
                .await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Zero-copy write — 零拷贝写入
    //
    // Same block_on pattern as zero-copy reads: raw pointer in future makes
    // it !Send, so we block the current thread on the tokio runtime.
    // 与零拷贝读取一样的 block_on 模式：裸指针使 future 不是 Send，所以阻塞
    // 当前线程在 tokio runtime 上。
    // ===================================================================

    /// Write data from a Python buffer to the store (zero-copy via RDMA).
    ///
    /// 零拷贝写入：通过 RDMA 直接从 Python buffer 写入数据。
    /// buffer must support the buffer protocol.  size is the number of bytes
    /// to write (may be smaller than the buffer's total capacity).
    #[pyo3(signature = (key, buffer, size, config = None))]
    fn put_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<()> {
        let ptr = get_pointer(&buffer)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.put_from(&key, ptr, size, cfg).await;
            result.map_err(to_py_err)
        })
    }

    /// Zero-copy TensorMetadata write from an owner-bearing registered buffer.
    #[pyo3(signature = (key, buffer, size, config = None))]
    fn put_tensor_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<i32> {
        if buffer.extract::<usize>().is_ok() {
            return Err(to_py_err(
                "put_tensor_from requires an owner-bearing Python buffer, not a raw address",
            ));
        }
        crate::tensor_codec::validate_tensor_buffer_object(&buffer, size)?;
        let ptr = get_pointer(&buffer)?;
        let config = config.map(|value| value.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.put_from(&key, ptr, size, config).await;
            result.map(|()| 0).map_err(to_py_err)
        })
    }

    /// Owner-bearing general-parallelism write from a full TensorMetadata
    /// buffer. TP requests split the full tensor into every requested rank,
    /// matching the C++ `*_with_parallelism_from` contract.
    #[pyo3(signature = (
        key,
        buffer,
        size,
        parallelism = None,
        config = None,
        writer_partition = None
    ))]
    fn put_tensor_with_parallelism_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        parallelism: Option<Bound<'_, TensorParallelismPy>>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
        writer_partition: Option<Bound<'_, WriterPartitionPy>>,
    ) -> PyResult<i32> {
        if parallelism.is_none() && writer_partition.is_none() {
            return Self::put_tensor_from(slf, key, buffer, size, config);
        }
        if buffer.extract::<usize>().is_ok() {
            return Err(to_py_err(
                "put_tensor_with_parallelism_from requires an owner-bearing Python buffer",
            ));
        }
        let parallelism = parallelism.map(|value| value.borrow().clone());
        let writer_partition = writer_partition.map(|value| value.borrow().clone());
        let config = config.map(|value| value.borrow().to_core());
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(value))
        {
            return Ok(TENSOR_INVALID_PARAMS_STATUS);
        }
        let py = buffer.py();
        let tensor = crate::tensor_codec::deserialize_tensor_buffer_copy(py, &buffer, size)?;
        let plan = match parallel_tensor_full_write_plan(
            &key,
            tensor.bind(py),
            parallelism.as_ref(),
            writer_partition.as_ref(),
        ) {
            Ok(plan) => plan,
            Err(_) => return Ok(TENSOR_INVALID_PARAMS_STATUS),
        };
        let object_config = expanded_single_key_config(config.clone(), plan.objects.len())?;
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = async {
                let object_keys = plan
                    .objects
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                let object_slices = plan
                    .objects
                    .iter()
                    .map(|(_, payload)| payload.as_slice())
                    .collect::<Vec<_>>();
                let statuses = client
                    .batch_put(&object_keys, &object_slices, object_config)
                    .await?;
                let status = statuses
                    .into_iter()
                    .find(|status| *status != 0)
                    .unwrap_or(0);
                if status != 0 {
                    return Ok(status);
                }
                if let Some((manifest_key, manifest)) = plan.manifest {
                    return Ok(client
                        .batch_put(&[manifest_key], &[manifest.as_slice()], config)
                        .await?
                        .into_iter()
                        .next()
                        .unwrap_or(TENSOR_INVALID_PARAMS_STATUS));
                }
                Ok::<_, mooncake_store_core::StoreError>(0)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    /// Decode a full TensorMetadata buffer, split it uniformly, and store all
    /// legacy TP shards.
    #[pyo3(signature = (key, buffer, size, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn put_tensor_with_tp_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<i32> {
        let (_tp_rank, tp_size, split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        if buffer.extract::<usize>().is_ok() {
            return Err(to_py_err(
                "put_tensor_with_tp_from requires an owner-bearing Python buffer",
            ));
        }
        let py = buffer.py();
        let tensor = crate::tensor_codec::deserialize_tensor_buffer_copy(py, &buffer, size)?;
        let payloads =
            crate::tensor_codec::serialize_tp_tensor_payloads(tensor.bind(py), tp_size, split_dim)?;
        let keys = if tp_size == 1 {
            vec![key]
        } else {
            (0..tp_size).map(|rank| tp_shard_key(&key, rank)).collect()
        };
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let slices = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client.batch_put(&keys, &slices, None).await;
            let statuses = result.map_err(to_py_err)?;
            Ok(statuses
                .into_iter()
                .find(|status| *status != 0)
                .unwrap_or(0))
        })
    }

    /// Batch zero-copy write from multiple Python buffers.
    /// 批量零拷贝写入。keys, buffers, sizes 长度必须相同。
    #[pyo3(signature = (keys, buffers, sizes, config = None))]
    fn batch_put_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Vec<i32>> {
        let ptrs: Vec<*mut c_void> = buffers.iter().map(get_pointer).collect::<PyResult<_>>()?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_put_from(&keys, &ptrs, &sizes, cfg).await;
            result.map_err(to_py_err)
        })
    }

    /// Batch zero-copy TensorMetadata write from registered Python buffers.
    #[pyo3(signature = (keys, buffers, sizes, config = None))]
    fn batch_put_tensor_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Vec<i32>> {
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
        }
        for (buffer, size) in buffers.iter().zip(&sizes) {
            if buffer.extract::<usize>().is_ok()
                || crate::tensor_codec::validate_tensor_buffer_object(buffer, *size).is_err()
            {
                return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
            }
        }
        let ptrs = buffers
            .iter()
            .map(get_pointer)
            .collect::<PyResult<Vec<_>>>()?;
        let config = config.map(|value| value.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_put_from(&keys, &ptrs, &sizes, config).await;
            result.map_err(to_py_err)
        })
    }

    /// Batch owner-bearing general-parallelism writes from full
    /// TensorMetadata buffers.
    #[pyo3(signature = (
        keys,
        buffers,
        sizes,
        parallelisms = None,
        config = None,
        writer_partitions = None
    ))]
    fn batch_put_tensor_with_parallelism_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        parallelisms: Option<Vec<Bound<'_, PyAny>>>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
        writer_partitions: Option<Vec<Bound<'_, PyAny>>>,
    ) -> PyResult<Vec<i32>> {
        if parallelisms.is_none() && writer_partitions.is_none() {
            return Self::batch_put_tensor_from(slf, keys, buffers, sizes, config);
        }
        if keys.len() != buffers.len()
            || keys.len() != sizes.len()
            || parallelisms
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || writer_partitions
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || (parallelisms.is_some() && writer_partitions.is_some())
        {
            return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
        }
        let config = config.map(|value| value.borrow().to_core());
        if config.as_ref().is_some_and(|value| {
            !tensor_publish_config_is_valid(value)
                || (!value.group_ids.is_empty() && value.group_ids.len() != keys.len())
        }) {
            return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
        }
        let py = slf.py();
        let mut plans = Vec::with_capacity(keys.len());
        for index in 0..keys.len() {
            if buffers[index].extract::<usize>().is_ok() {
                plans.push(None);
                continue;
            }
            let Ok(tensor) = crate::tensor_codec::deserialize_tensor_buffer_copy(
                py,
                &buffers[index],
                sizes[index],
            ) else {
                plans.push(None);
                continue;
            };
            let parallelism = match parallelisms.as_ref() {
                Some(values) => match optional_parallelism(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let writer = match writer_partitions.as_ref() {
                Some(values) => match optional_writer_partition(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let Ok(plan) = parallel_tensor_full_write_plan(
                &keys[index],
                tensor.bind(py),
                parallelism.as_ref(),
                writer.as_ref(),
            ) else {
                plans.push(None);
                continue;
            };
            let base_config = indexed_batch_config(config.clone(), keys.len(), &[index])?;
            let object_config =
                expanded_single_key_config(base_config.clone(), plan.objects.len())?;
            plans.push(Some((plan, base_config, object_config)));
        }
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = async {
                let mut statuses = vec![TENSOR_INVALID_PARAMS_STATUS; plans.len()];
                for (index, plan) in plans.into_iter().enumerate() {
                    let Some((plan, base_config, object_config)) = plan else {
                        continue;
                    };
                    let object_keys = plan
                        .objects
                        .iter()
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    let object_slices = plan
                        .objects
                        .iter()
                        .map(|(_, payload)| payload.as_slice())
                        .collect::<Vec<_>>();
                    let mut status = client
                        .batch_put(&object_keys, &object_slices, object_config)
                        .await?
                        .into_iter()
                        .find(|status| *status != 0)
                        .unwrap_or(0);
                    if status == 0 {
                        if let Some((manifest_key, manifest)) = plan.manifest {
                            status = client
                                .batch_put(&[manifest_key], &[manifest.as_slice()], base_config)
                                .await?
                                .into_iter()
                                .next()
                                .unwrap_or(TENSOR_INVALID_PARAMS_STATUS);
                        }
                    }
                    statuses[index] = status;
                }
                Ok::<_, mooncake_store_core::StoreError>(statuses)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    /// Batch owner-bearing TP buffer write.
    #[pyo3(signature = (base_keys, buffers, sizes, tp_rank = 0, tp_size = 1, split_dim = 0))]
    fn batch_put_tensor_with_tp_from(
        slf: &Bound<'_, Self>,
        base_keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        tp_rank: i32,
        tp_size: i32,
        split_dim: i32,
    ) -> PyResult<Vec<i32>> {
        let (_tp_rank, tp_size, split_dim) = tp_parameters(tp_rank, tp_size, split_dim)?;
        if base_keys.len() != buffers.len() || base_keys.len() != sizes.len() {
            return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; base_keys.len()]);
        }
        let py = slf.py();
        let mut final_statuses = vec![TENSOR_INVALID_PARAMS_STATUS; base_keys.len()];
        let mut shard_keys = Vec::new();
        let mut shard_payloads = Vec::new();
        let mut processed_indices = Vec::new();
        for (index, (buffer, size)) in buffers.iter().zip(&sizes).enumerate() {
            if buffer.extract::<usize>().is_ok() {
                continue;
            }
            let Ok(tensor) = crate::tensor_codec::deserialize_tensor_buffer_copy(py, buffer, *size)
            else {
                continue;
            };
            let Ok(payloads) = crate::tensor_codec::serialize_tp_tensor_payloads(
                tensor.bind(py),
                tp_size,
                split_dim,
            ) else {
                continue;
            };
            processed_indices.push(index);
            for (rank, payload) in payloads.into_iter().enumerate() {
                shard_keys.push(if tp_size == 1 {
                    base_keys[index].clone()
                } else {
                    tp_shard_key(&base_keys[index], rank)
                });
                shard_payloads.push(payload);
            }
        }
        if shard_keys.is_empty() {
            return Ok(final_statuses);
        }
        let inner = slf.borrow().inner.clone();
        let shard_statuses = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let slices = shard_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let result = client.batch_put(&shard_keys, &slices, None).await;
            result.map_err(to_py_err)
        })?;
        for (processed_offset, original_index) in processed_indices.into_iter().enumerate() {
            let start = processed_offset * tp_size;
            let end = start + tp_size;
            if end > shard_statuses.len() {
                break;
            }
            final_statuses[original_index] = shard_statuses[start..end]
                .iter()
                .copied()
                .find(|status| *status != 0)
                .unwrap_or(0);
        }
        Ok(final_statuses)
    }

    /// Write an object from metadata and data buffers.
    #[pyo3(signature = (key, buffer, metadata_buffer, size, metadata_size, config = None))]
    fn put_from_with_metadata(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        metadata_buffer: Bound<'_, PyAny>,
        size: usize,
        metadata_size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<i32> {
        let ptr = get_pointer(&buffer)?;
        let (metadata_ptr, _) = get_buffer_ptr(&metadata_buffer)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client
                .put_from_with_metadata(&key, ptr, metadata_ptr, size, metadata_size, cfg)
                .await;
            result.map_err(to_py_err)
        })
    }

    /// Batch zero-copy write where each key is assembled from multiple buffers.
    #[pyo3(signature = (keys, all_buffers, all_sizes, config = None))]
    fn batch_put_from_multi_buffers(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        all_buffers: Vec<Vec<Bound<'_, PyAny>>>,
        all_sizes: Vec<Vec<usize>>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Vec<i32>> {
        let ptrs: Vec<Vec<*mut c_void>> = all_buffers
            .iter()
            .map(|bufs| bufs.iter().map(get_pointer).collect::<PyResult<Vec<_>>>())
            .collect::<PyResult<_>>()?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client
                .batch_put_from_multi_buffers(&keys, &ptrs, &all_sizes, cfg)
                .await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Buffer-based get — 基于 buffer 句柄的读取
    //
    // These methods return owned Vec<u8> data (not raw pointers).  The
    // underlying Rust API returns a BufferHandle whose .data field contains
    // the actual bytes.  We extract .data and return it as a Rust Vec<u8>;
    // future_into_py converts to Python bytes.
    //
    // 这些方法返回 owned Vec<u8> 数据（不是裸指针）。底层 Rust API 返回
    // BufferHandle，其 .data 字段包含实际字节。我们提取 .data 作为 Rust
    // Vec<u8> 返回；future_into_py 将其转换为 Python bytes。
    // ===================================================================

    /// Get a value as owned bytes (no copy from RDMA, but owned buffer).
    /// 获取值作为 owned bytes 返回。
    fn get_buffer<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.get_buffer(&key).await;
            let bh = result.map_err(to_py_err)?;
            // Return owned data — future_into_py handles IntoPy conversion with GIL
            // 返回 owned 数据 —— future_into_py 在持有 GIL 时处理 IntoPy 转换
            Ok(bh.data)
        })
    }

    /// Batch get multiple values as owned bytes.
    /// 批量获取多个值作为 owned bytes 返回。不存在的 key 返回 None。
    fn batch_get_buffer<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_get_buffer(&keys).await;
            let results = result.map_err(to_py_err)?;
            // Return owned data — future_into_py handles IntoPy conversion
            // 返回 owned 数据 —— future_into_py 处理 IntoPy 转换
            let out: Vec<Option<Vec<u8>>> = results
                .into_iter()
                .map(|opt| opt.map(|bh| bh.data))
                .collect();
            Ok(out)
        })
    }

    /// Return replica descriptors for one key as dictionaries.
    fn get_replica_desc(slf: &Bound<'_, Self>, key: String) -> PyResult<Py<PyAny>> {
        let inner = slf.borrow().inner.clone();
        let replicas = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.get_replica_list(&key).await;
            result.map_err(to_py_err)
        })?;
        Ok(replicas_to_py(replicas))
    }

    /// Return replica descriptors for multiple keys as a dict keyed by object key.
    fn batch_get_replica_desc(slf: &Bound<'_, Self>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let inner = slf.borrow().inner.clone();
        let results = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let results = client
                .batch_get_replica_list_results(&keys)
                .await
                .map_err(to_py_err)?;
            results
                .into_iter()
                .map(|result| match result {
                    Ok(replicas) => Ok(Some(replicas)),
                    Err(mooncake_store_core::StoreError::KeyNotFound(_)) => Ok(None),
                    Err(error) => Err(to_py_err(error)),
                })
                .collect::<PyResult<Vec<_>>>()
        })?;

        let py = unsafe { Python::assume_attached() };
        let dict = PyDict::new(py);
        for (key, replicas) in keys.into_iter().zip(results.into_iter()) {
            if let Some(replicas) = replicas {
                dict.set_item(key, replicas_to_py(replicas))?;
            }
        }
        Ok(dict.into_any().unbind())
    }

    /// Clear replicas for keys owned by this client.
    #[pyo3(signature = (keys, segment_name = String::new(), tenant_id = String::new()))]
    fn batch_replica_clear(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        segment_name: String,
        tenant_id: String,
    ) -> PyResult<Vec<String>> {
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let client_id = client.client_id();
            let result = client
                .batch_replica_clear(&keys, client_id, &segment_name, &tenant_id)
                .await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Buffer registration — buffer 注册（RDMA 内存注册）
    //
    // register_buffer: Register a Python buffer's memory with the RDMA
    // subsystem so it can be used for zero-copy transfers.  The Python
    // buffer object is kept alive in registered_py_buffers to prevent
    // the underlying memory from being freed or moved by Python's GC.
    //
    // register_buffer: 将 Python buffer 的内存注册到 RDMA 子系统，使其可用
    // 于零拷贝传输。Python buffer 对象保存在 registered_py_buffers 中，防止
    // GC 释放或移动底层内存。
    //
    // Both register_buffer and unregister_buffer are SYNCHRONOUS: they hold
    // the client mutex via as_ref() (not take()) so the client remains
    // available for concurrent async operations.  This is safe because the
    // underlying register/unregister calls are immediate (no await points).
    //
    // register_buffer 和 unregister_buffer 都是同步的：通过 as_ref() 持有
    // client mutex（而非 take()），因此可与并发异步操作共存。这是安全的，
    // 因为底层的 register/unregister 调用是即时的（没有 await 点）。
    // ===================================================================

    /// Register a Python buffer for RDMA zero-copy access.
    /// 为 RDMA 零拷贝访问注册 Python buffer。
    /// location: optional device identifier. Host buffers default to "cpu:0";
    /// DLPack device buffers derive and verify their canonical location.
    #[pyo3(signature = (buffer, size, location = None, owner = None))]
    fn register_buffer(
        slf: &Bound<'_, Self>,
        buffer: Bound<'_, PyAny>,
        size: usize,
        location: Option<String>,
        owner: Option<Bound<'_, PyAny>>,
    ) -> PyResult<i32> {
        if size == 0 {
            return Err(to_py_err("registered buffer size must be non-zero"));
        }
        let (memory_owner, effective_location, python_object_identity) = if let Ok(address) =
            buffer.extract::<usize>()
        {
            if address == 0 {
                return Err(to_py_err("buffer address must not be zero"));
            }
            let owner = owner.ok_or_else(|| {
                to_py_err(
                    "an allocation-owning Python object is required when registering a raw integer address",
                )
            })?;
            let python_object_identity = owner.as_ptr() as usize;
            match PyUntypedBuffer::get(&owner) {
                Ok(buffer_export) => {
                    let memory_owner =
                        checked_python_buffer_owner(buffer_export, Some(address), size)?;
                    (
                        memory_owner,
                        host_registration_location(location.as_deref())?,
                        python_object_identity,
                    )
                }
                Err(buffer_error) => {
                    if !owner.hasattr("__dlpack__")? {
                        return Err(to_py_err(format!(
                            "raw registration owner exposes neither a Python buffer nor DLPack: {buffer_error}"
                        )));
                    }
                    let device_owner = DLPackMemoryOwner::from_python(
                        &owner,
                        Some(address),
                        Some(size),
                        location.as_deref(),
                    )?;
                    let effective_location = device_owner.location().to_string();
                    (
                        PythonMemoryOwner::DLPack(device_owner),
                        effective_location,
                        python_object_identity,
                    )
                }
            }
        } else {
            let python_object_identity = buffer.as_ptr() as usize;
            match PyUntypedBuffer::get(&buffer) {
                Ok(buffer_export) => {
                    let memory_owner = checked_python_buffer_owner(buffer_export, None, size)?;
                    (
                        memory_owner,
                        host_registration_location(location.as_deref())?,
                        python_object_identity,
                    )
                }
                Err(buffer_error) => {
                    if !buffer.hasattr("__dlpack__")? {
                        return Err(to_py_err(format!(
                            "registration object exposes neither a Python buffer nor DLPack: {buffer_error}"
                        )));
                    }
                    let device_owner = DLPackMemoryOwner::from_python(
                        &buffer,
                        None,
                        Some(size),
                        location.as_deref(),
                    )?;
                    let effective_location = device_owner.location().to_string();
                    (
                        PythonMemoryOwner::DLPack(device_owner),
                        effective_location,
                        python_object_identity,
                    )
                }
            }
        };
        let registration_base = memory_owner.base_address().as_ptr() as usize;
        {
            let slf_ref = slf.borrow();
            let registrations = slf_ref.registered_py_buffers.lock();
            if registrations.iter().any(|registration| {
                registration.matches_python_object(python_object_identity)
                    || registration.registration_id().base_address() == registration_base
            }) {
                return Err(to_py_err(
                    "Python buffer object or address is already registered",
                ));
            }
        }
        {
            let slf_ref = slf.borrow();
            let guard = try_client_slot(&slf_ref.inner)?;
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            let registration_id = client
                .register_owned_buffer(memory_owner, &effective_location)
                .map_err(to_py_err)?;
            // Publish the Python lookup identity before releasing the client
            // lock. The actual allocation owner already moved into the FFI
            // registration capability.
            slf_ref
                .registered_py_buffers
                .lock()
                .push(PythonBufferRegistration {
                    registration_id,
                    python_object_identity,
                    registered_size: size,
                });
        }
        Ok(0)
    }

    /// Unregister a previously registered Python buffer.
    /// 注销之前注册的 Python buffer。
    fn unregister_buffer(slf: &Bound<'_, Self>, buffer: Bound<'_, PyAny>) -> PyResult<i32> {
        let raw_address = buffer.extract::<usize>().ok();
        if raw_address == Some(0) {
            return Err(to_py_err("buffer address must not be zero"));
        }
        let python_object_identity = buffer.as_ptr() as usize;
        // DLPack capsules are ownership transfers, not pointer-query handles.
        // Prefer the stable Python identity retained by the registration and
        // never consume a second capsule merely to unregister. For ordinary
        // buffer-protocol aliases, preserve the historical base-address lookup.
        let fallback_base = if raw_address.is_none() && !buffer.hasattr("__dlpack__")? {
            Some(get_pointer(&buffer)? as usize)
        } else {
            raw_address
        };
        {
            let slf_ref = slf.borrow();
            let guard = try_client_slot(&slf_ref.inner)?;
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            let mut registered_py_buffers = slf_ref.registered_py_buffers.lock();
            let registration_id = registered_py_buffers
                .iter()
                .find(|registration| {
                    registration.matches_python_object(python_object_identity)
                        || fallback_base.is_some_and(|base| {
                            registration.registration_id().base_address() == base
                        })
                })
                .map(PythonBufferRegistration::registration_id)
                .ok_or_else(|| to_py_err("Python buffer object or address is not registered"))?;
            client
                .unregister_buffer_handle(registration_id)
                .map_err(to_py_err)?;
            registered_py_buffers
                .retain(|registration| registration.registration_id() != registration_id);
        }
        Ok(0)
    }

    // ===================================================================
    // Zero-copy upsert — 零拷贝插入或更新
    //
    // Same block_on pattern as put_from/get_into: the future captures raw
    // pointers, making it !Send.  Returns replica descriptors as Python dicts.
    // 与 put_from/get_into 相同的 block_on 模式：future 捕获裸指针使其不是
    // Send。返回副本描述信息作为 Python 字典。
    // ===================================================================

    /// Insert-or-update from a Python buffer (zero-copy via RDMA).
    /// 零拷贝插入或更新：从 Python buffer 通过 RDMA 直接写入。
    #[pyo3(signature = (key, buffer, size, config = None))]
    fn upsert_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Py<PyAny>> {
        let ptr = get_pointer(&buffer)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        let replicas = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.upsert_from(&key, ptr, size, cfg).await;
            result.map_err(to_py_err)
        })?;
        Ok(replicas_to_py(replicas))
    }

    /// Zero-copy TensorMetadata upsert from an owner-bearing registered
    /// Python buffer.
    #[pyo3(signature = (key, buffer, size, config = None))]
    fn upsert_tensor_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<i32> {
        if buffer.extract::<usize>().is_ok() {
            return Err(to_py_err(
                "upsert_tensor_from requires an owner-bearing Python buffer, not a raw address",
            ));
        }
        crate::tensor_codec::validate_tensor_buffer_object(&buffer, size)?;
        let ptr = get_pointer(&buffer)?;
        let config = config.map(|value| value.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.upsert_from(&key, ptr, size, config).await;
            result.map(|_| 0).map_err(to_py_err)
        })
    }

    /// Owner-bearing general-parallelism upsert from a full TensorMetadata
    /// buffer.
    #[pyo3(signature = (
        key,
        buffer,
        size,
        parallelism = None,
        config = None,
        writer_partition = None
    ))]
    fn upsert_tensor_with_parallelism_from(
        slf: &Bound<'_, Self>,
        key: String,
        buffer: Bound<'_, PyAny>,
        size: usize,
        parallelism: Option<Bound<'_, TensorParallelismPy>>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
        writer_partition: Option<Bound<'_, WriterPartitionPy>>,
    ) -> PyResult<i32> {
        if parallelism.is_none() && writer_partition.is_none() {
            return Self::upsert_tensor_from(slf, key, buffer, size, config);
        }
        if buffer.extract::<usize>().is_ok() {
            return Err(to_py_err(
                "upsert_tensor_with_parallelism_from requires an owner-bearing Python buffer",
            ));
        }
        let parallelism = parallelism.map(|value| value.borrow().clone());
        let writer_partition = writer_partition.map(|value| value.borrow().clone());
        let config = config.map(|value| value.borrow().to_core());
        if config
            .as_ref()
            .is_some_and(|value| !tensor_publish_config_is_valid(value))
        {
            return Ok(TENSOR_INVALID_PARAMS_STATUS);
        }
        let py = buffer.py();
        let tensor = crate::tensor_codec::deserialize_tensor_buffer_copy(py, &buffer, size)?;
        let plan = match parallel_tensor_full_write_plan(
            &key,
            tensor.bind(py),
            parallelism.as_ref(),
            writer_partition.as_ref(),
        ) {
            Ok(plan) => plan,
            Err(_) => return Ok(TENSOR_INVALID_PARAMS_STATUS),
        };
        let object_config = expanded_single_key_config(config.clone(), plan.objects.len())?;
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = async {
                let object_keys = plan
                    .objects
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                let object_slices = plan
                    .objects
                    .iter()
                    .map(|(_, payload)| payload.as_slice())
                    .collect::<Vec<_>>();
                let statuses = client
                    .batch_upsert(&object_keys, &object_slices, object_config)
                    .await?;
                let status = statuses
                    .into_iter()
                    .find(|status| *status != 0)
                    .unwrap_or(0);
                if status != 0 {
                    return Ok(status);
                }
                if let Some((manifest_key, manifest)) = plan.manifest {
                    return Ok(client
                        .batch_upsert(&[manifest_key], &[manifest.as_slice()], config)
                        .await?
                        .into_iter()
                        .next()
                        .unwrap_or(TENSOR_INVALID_PARAMS_STATUS));
                }
                Ok::<_, mooncake_store_core::StoreError>(0)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    /// Batch zero-copy upsert from multiple Python buffers.
    /// 批量零拷贝插入或更新。
    #[pyo3(signature = (keys, buffers, sizes, config = None))]
    fn batch_upsert_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Py<PyAny>> {
        let ptrs: Vec<*mut c_void> = buffers.iter().map(get_pointer).collect::<PyResult<_>>()?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        let results = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client.batch_upsert_from(&keys, &ptrs, &sizes, cfg).await;
            result.map_err(to_py_err)
        })?;
        // GIL is held throughout block_on, so assume_attached() is safe here
        // GIL 在整个 block_on 期间持有，因此 assume_attached() 在此安全
        let py = unsafe { Python::assume_attached() };
        let out: Vec<Py<PyAny>> = results
            .iter()
            .map(|replicas| replicas_to_py(replicas.clone()))
            .collect();
        Ok(out.into_pyobject(py)?.unbind())
    }

    /// Batch zero-copy TensorMetadata upsert with one C++-compatible status
    /// per key.
    #[pyo3(signature = (keys, buffers, sizes, config = None))]
    fn batch_upsert_tensor_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
    ) -> PyResult<Vec<i32>> {
        if keys.len() != buffers.len() || keys.len() != sizes.len() {
            return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
        }
        for (buffer, size) in buffers.iter().zip(&sizes) {
            if buffer.extract::<usize>().is_ok()
                || crate::tensor_codec::validate_tensor_buffer_object(buffer, *size).is_err()
            {
                return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
            }
        }
        let ptrs = buffers
            .iter()
            .map(get_pointer)
            .collect::<PyResult<Vec<_>>>()?;
        let config = config.map(|value| value.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = client
                .batch_upsert_from_statuses(&keys, &ptrs, &sizes, config)
                .await;
            result.map_err(to_py_err)
        })
    }

    /// Batch owner-bearing general-parallelism upserts from full
    /// TensorMetadata buffers.
    #[pyo3(signature = (
        keys,
        buffers,
        sizes,
        parallelisms = None,
        config = None,
        writer_partitions = None
    ))]
    fn batch_upsert_tensor_with_parallelism_from(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
        buffers: Vec<Bound<'_, PyAny>>,
        sizes: Vec<usize>,
        parallelisms: Option<Vec<Bound<'_, PyAny>>>,
        config: Option<Bound<'_, ReplicateConfigPy>>,
        writer_partitions: Option<Vec<Bound<'_, PyAny>>>,
    ) -> PyResult<Vec<i32>> {
        if parallelisms.is_none() && writer_partitions.is_none() {
            return Self::batch_upsert_tensor_from(slf, keys, buffers, sizes, config);
        }
        if keys.len() != buffers.len()
            || keys.len() != sizes.len()
            || parallelisms
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || writer_partitions
                .as_ref()
                .is_some_and(|values| values.len() != keys.len())
            || (parallelisms.is_some() && writer_partitions.is_some())
        {
            return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
        }
        let config = config.map(|value| value.borrow().to_core());
        if config.as_ref().is_some_and(|value| {
            !tensor_publish_config_is_valid(value)
                || (!value.group_ids.is_empty() && value.group_ids.len() != keys.len())
        }) {
            return Ok(vec![TENSOR_INVALID_PARAMS_STATUS; keys.len()]);
        }
        let py = slf.py();
        let mut plans = Vec::with_capacity(keys.len());
        for index in 0..keys.len() {
            if buffers[index].extract::<usize>().is_ok() {
                plans.push(None);
                continue;
            }
            let Ok(tensor) = crate::tensor_codec::deserialize_tensor_buffer_copy(
                py,
                &buffers[index],
                sizes[index],
            ) else {
                plans.push(None);
                continue;
            };
            let parallelism = match parallelisms.as_ref() {
                Some(values) => match optional_parallelism(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let writer = match writer_partitions.as_ref() {
                Some(values) => match optional_writer_partition(&values[index]) {
                    Ok(value) => value,
                    Err(_) => {
                        plans.push(None);
                        continue;
                    }
                },
                None => None,
            };
            let Ok(plan) = parallel_tensor_full_write_plan(
                &keys[index],
                tensor.bind(py),
                parallelism.as_ref(),
                writer.as_ref(),
            ) else {
                plans.push(None);
                continue;
            };
            let base_config = indexed_batch_config(config.clone(), keys.len(), &[index])?;
            let object_config =
                expanded_single_key_config(base_config.clone(), plan.objects.len())?;
            plans.push(Some((plan, base_config, object_config)));
        }
        let inner = slf.borrow().inner.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut client = take_client(&inner).await?;
            let result = async {
                let mut statuses = vec![TENSOR_INVALID_PARAMS_STATUS; plans.len()];
                for (index, plan) in plans.into_iter().enumerate() {
                    let Some((plan, base_config, object_config)) = plan else {
                        continue;
                    };
                    let object_keys = plan
                        .objects
                        .iter()
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    let object_slices = plan
                        .objects
                        .iter()
                        .map(|(_, payload)| payload.as_slice())
                        .collect::<Vec<_>>();
                    let mut status = client
                        .batch_upsert(&object_keys, &object_slices, object_config)
                        .await?
                        .into_iter()
                        .find(|status| *status != 0)
                        .unwrap_or(0);
                    if status == 0 {
                        if let Some((manifest_key, manifest)) = plan.manifest {
                            status = client
                                .batch_upsert(&[manifest_key], &[manifest.as_slice()], base_config)
                                .await?
                                .into_iter()
                                .next()
                                .unwrap_or(TENSOR_INVALID_PARAMS_STATUS);
                        }
                    }
                    statuses[index] = status;
                }
                Ok::<_, mooncake_store_core::StoreError>(statuses)
            }
            .await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Storage admin / segment queries — 存储管理与 segment 查询
    // ===================================================================

    /// Mount a memory segment by name, size, and base address.
    fn mount_segment<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_name: String,
        size: u64,
        base_addr: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.mount_segment(&segment_name, size, base_addr).await;
            result.map_err(to_py_err)
        })
    }

    /// Unmount a memory segment by name.
    #[pyo3(signature = (segment_name, grace_period_ms = 0))]
    fn unmount_segment<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_name: String,
        grace_period_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.unmount_segment(&segment_name, grace_period_ms).await;
            result.map_err(to_py_err)
        })
    }

    /// Allocate client-owned memory, register it with the Transfer Engine, and
    /// mount every `MC_MAX_MR_SIZE` chunk in Master.
    ///
    /// Returns `(segment_ids, allocated_size)`. The IDs are the authoritative
    /// UUIDs required by `unmount_and_free_segments`; Python never owns or
    /// manipulates the native allocation directly.
    fn allocate_and_mount_segments<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        size: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.allocate_and_mount_segments_with_size(size).await;
            result
                .map(|(segment_ids, allocated_size)| {
                    (
                        segment_ids
                            .into_iter()
                            .map(|id| id.to_string())
                            .collect::<Vec<_>>(),
                        allocated_size,
                    )
                })
                .map_err(to_py_err)
        })
    }

    /// Unmount exact client-owned segment UUIDs and release their registrations
    /// and owners only after Master confirms the requested lifecycle boundary.
    #[pyo3(signature = (segment_ids, grace_period_ms = 0))]
    fn unmount_and_free_segments<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_ids: Vec<String>,
        grace_period_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let segment_ids = parse_uuid_list(&segment_ids)?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client
                .unmount_and_free_segments(&segment_ids, grace_period_ms)
                .await;
            result.map_err(to_py_err)
        })
    }

    /// Mount a NoF segment. The segment dict accepts:
    /// id, name, base, size, te_endpoint, client_id.
    fn mount_nof_segment<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment: Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let segment = nof_segment_from_dict(&segment)?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.mount_nof_segment(&segment).await;
            result.map_err(to_py_err)
        })
    }

    /// Re-mount NoF segments after restart. Each segment is a dict accepted by
    /// mount_nof_segment().
    fn remount_nof_segments<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segments: Vec<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let segments = segments
            .iter()
            .map(nof_segment_from_dict)
            .collect::<PyResult<Vec<_>>>()?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.remount_nof_segments(&segments).await;
            result.map_err(to_py_err)
        })
    }

    /// Unmount a NoF segment by UUID string.
    fn unmount_nof_segment<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let segment_id = parse_uuid(&segment_id)?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.unmount_nof_segment(segment_id).await;
            result.map_err(to_py_err)
        })
    }

    /// Return all NoF segments as tuples:
    /// (id, name, base, size, te_endpoint, client_id).
    fn get_all_nof_segments<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let segments = client.get_all_nof_segments().await.map_err(to_py_err)?;
            let out: Vec<(String, String, u64, u64, String, String)> = segments
                .iter()
                .map(|s| {
                    (
                        s.id.to_string(),
                        s.name.clone(),
                        s.base,
                        s.size,
                        s.te_endpoint.clone(),
                        s.client_id.to_string(),
                    )
                })
                .collect();
            Ok(out)
        })
    }

    /// Return owners for NoF segments with a given name as
    /// (segment_id, client_id) tuples.
    fn get_nof_segments_by_name<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let owners = client
                .get_nof_segments_by_name(&segment_name)
                .await
                .map_err(to_py_err)?;
            let out: Vec<(String, String)> = owners
                .iter()
                .map(|o| (o.segment_id.to_string(), o.client_id.to_string()))
                .collect();
            Ok(out)
        })
    }

    /// Query segment usage, returning (total_size, used_size).
    fn query_segments<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let usage = client
                .query_segments(&segment_name)
                .await
                .map_err(to_py_err)?;
            Ok((usage.total_size, usage.used_size))
        })
    }

    /// Query storage config, returning (fs_dir, enable_disk_eviction, quota_bytes).
    fn get_storage_config<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let config = client.get_storage_config().await.map_err(to_py_err)?;
            Ok((
                config.fs_dir,
                config.enable_disk_eviction,
                config.quota_bytes,
            ))
        })
    }

    /// Query segment status by name. Returns the proto enum value as int.
    fn query_segment_status<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let status = client
                .query_segment_status(&segment_name)
                .await
                .map_err(to_py_err)?;
            Ok(status)
        })
    }

    /// Query segment status by UUID string. Returns the proto enum value as int.
    fn query_segment_status_by_id<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let segment_id = parse_uuid(&segment_id)?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let status = client
                .query_segment_status_by_id(segment_id)
                .await
                .map_err(to_py_err)?;
            Ok(status)
        })
    }

    /// Return the master's configured filesystem directory.
    fn get_fsdir<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let fsdir = client.get_fsdir().await.map_err(to_py_err)?;
            Ok(fsdir)
        })
    }

    /// Check master readiness and return its version string.
    fn service_ready<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let version = client.service_ready().await.map_err(to_py_err)?;
            Ok(version)
        })
    }

    /// Return all keys across tenants for admin/debug use.
    fn get_all_keys_for_admin<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let keys = client.get_all_keys_for_admin().await.map_err(to_py_err)?;
            Ok(keys)
        })
    }

    /// Return all memory segment names for admin/debug use.
    fn get_all_segments_for_admin<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let segments = client
                .get_all_segments_for_admin()
                .await
                .map_err(to_py_err)?;
            Ok(segments)
        })
    }

    /// Query segment usage for admin/debug use, returning (total_size, used_size).
    fn query_segment_for_admin<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        segment_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let usage = client
                .query_segment_for_admin(&segment_name)
                .await
                .map_err(to_py_err)?;
            Ok((usage.total_size, usage.used_size))
        })
    }

    /// Calculate cache stats. Returns a dict-like mapping of metric name to value.
    fn calc_cache_stats<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let stats: HashMap<String, f64> = client.calc_cache_stats().await.map_err(to_py_err)?;
            Ok(stats)
        })
    }

    /// Query client transport addresses for multiple client UUID strings.
    fn batch_query_ip<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        client_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ids = client_ids
            .iter()
            .map(|id| Uuid::parse_str(id).map_err(|e| to_py_err(format!("invalid UUID: {e}"))))
            .collect::<PyResult<Vec<_>>>()?;
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.batch_query_ip(&ids).await;
            result.map_err(to_py_err)
        })
    }

    /// Attach a local FilePerKey storage backend for offload/promotion.
    #[pyo3(signature = (
        root_dir,
        fsdir = String::from("moon_file_per_key_dir"),
        enable_eviction = true,
        quota_bytes = 0,
    ))]
    fn attach_local_storage_backend(
        &self,
        root_dir: String,
        fsdir: String,
        enable_eviction: bool,
        quota_bytes: u64,
    ) -> PyResult<()> {
        let backend = Arc::new(LocalStorageBackend::new(LocalStorageConfig {
            root_dir: PathBuf::from(root_dir),
            fsdir,
            enable_eviction,
            quota_bytes,
        }));
        backend.init().map_err(to_py_err)?;

        let mut guard = try_client_slot(&self.inner)?;
        let client = guard
            .take()
            .ok_or_else(|| to_py_err("client already closed"))?;
        *guard = Some(client.with_local_storage_backend(backend));
        Ok(())
    }

    /// Start the local P2P offload read server. Returns its port.
    fn start_offload_server<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = take_client(&inner).await?;
            let result = client.start_offload_server().await;
            result.map_err(to_py_err)
        })
    }

    /// Return the local P2P offload RPC address if the server is running.
    fn offload_rpc_address(&self) -> PyResult<String> {
        match try_client_slot(&self.inner)?.as_ref() {
            Some(client) => Ok(client.offload_rpc_address()),
            None => Err(to_py_err("client already closed")),
        }
    }

    /// Execute one complete offload heartbeat cycle.
    fn offload_objects<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        enable_offloading: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.offload_objects(enable_offloading).await;
            result.map_err(to_py_err)
        })
    }

    /// Execute one complete promotion heartbeat cycle.
    fn promote_objects<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.promote_objects().await;
            result.map_err(to_py_err)
        })
    }

    /// Evict local SSD objects until usage drops below the low watermark.
    fn run_disk_watermark_eviction<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client
                .run_disk_watermark_eviction(high_watermark_ratio, low_watermark_ratio)
                .await;
            result.map_err(to_py_err)
        })
    }

    /// C++ ClientRequester-compatible P2P offload request.
    #[staticmethod]
    fn batch_get_offload_object<'py>(
        py: Python<'py>,
        peer_addr: String,
        keys: Vec<String>,
        sizes: Vec<i64>,
        tenant_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = mooncake_store_client::offload::client::batch_get_offload_objects(
                &peer_addr,
                &keys,
                &sizes,
                &tenant_ids,
            )
            .await
            .map_err(to_py_err)?;
            Ok((
                result.batch_id,
                result.pointers,
                result.transfer_engine_addr,
            ))
        })
    }

    /// Release a peer offload batch buffer.
    #[staticmethod]
    fn release_offload_buffer<'py>(
        py: Python<'py>,
        peer_addr: String,
        batch_id: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            mooncake_store_client::offload::client::release_offload_buffer(&peer_addr, batch_id)
                .await;
            Ok(())
        })
    }

    // ===================================================================
    // Storage / offload — 存储 / 卸载（SSD 分层存储）
    //
    // These methods implement the SSD-based object offloading feature.
    // Object data can be offloaded from RDMA memory to local SSD and
    // promoted back on demand.  This enables storing data sets larger
    // than available GPU + host memory.
    //
    // 这些方法实现基于 SSD 的对象卸载功能。对象数据可以从 RDMA 内存卸载到
    // 本地 SSD，并在需要时提升回来。这使存储数据集超出 GPU + 主存容量。
    // ===================================================================

    /// Mount a local disk segment for SSD offloading.
    /// 挂载本地磁盘段用于 SSD 卸载存储。
    fn mount_local_disk_segment<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        enable_offloading: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.mount_local_disk_segment(enable_offloading).await;
            result.map_err(to_py_err)
        })
    }

    /// Send a heartbeat to indicate this node is alive for offloading.
    /// 发送心跳表示此节点存活可用于卸载任务。
    fn offload_object_heartbeat<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        enable_offloading: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.offload_object_heartbeat(enable_offloading).await;
            result.map_err(to_py_err)
        })
    }

    /// Report the local SSD capacity to the master for scheduling.
    /// 向 master 上报本地 SSD 容量用于调度决策。
    fn report_ssd_capacity<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        bytes: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.report_ssd_capacity(bytes).await;
            result.map_err(to_py_err)
        })
    }

    /// Notify master that an offload operation succeeded.
    /// 通知 master 卸载操作成功完成。
    /// metadatas: list of dicts with keys bucket_id, offset, key_size,
    /// data_size, transport_endpoint describing where data was stored.
    fn notify_offload_success<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Vec<String>,
        metadatas: Vec<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();
        // Convert Python dicts to Rust StorageObjectMetadata protobuf structs
        // 将 Python dict 转换为 Rust StorageObjectMetadata protobuf 结构体
        let proto_metas: Vec<StorageObjectMetadata> = metadatas
            .iter()
            .map(|d| StorageObjectMetadata {
                bucket_id: d
                    .get_item("bucket_id")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                offset: d
                    .get_item("offset")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                key_size: d
                    .get_item("key_size")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                data_size: d
                    .get_item("data_size")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<i64>().ok())
                    .unwrap_or(0),
                transport_endpoint: d
                    .get_item("transport_endpoint")
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<String>().ok())
                    .unwrap_or_default(),
            })
            .collect();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.notify_offload_success(keys, proto_metas).await;
            result.map_err(to_py_err)
        })
    }

    // ===================================================================
    // Promotion — 数据提升（从 SSD 回到 RDMA 内存）
    //
    // Promotion is the reverse of offloading: moving data from local SSD
    // back into RDMA-accessible memory so it can be served at high speed.
    // The workflow is:
    //   1. promotion_object_heartbeat() — register as a promotion-capable node
    //   2. promotion_alloc_start()    — allocate RDMA memory for the object
    //   3. (transfer data into allocated memory)
    //   4. notify_promotion_success() — tell master the promotion is done
    //
    // 提升是卸载的反向操作：将数据从本地 SSD 移回 RDMA 可访问内存以提供
    // 高速服务。工作流为：
    //   1. promotion_object_heartbeat() — 注册为可执行提升的节点
    //   2. promotion_alloc_start() — 为对象分配 RDMA 内存
    //   3. (将数据传输到分配的内存)
    //   4. notify_promotion_success() — 通知 master 提升完成
    // ===================================================================

    /// Heartbeat for promotion-capable nodes.
    /// 可执行提升节点的心跳信号。
    fn promotion_object_heartbeat<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.promotion_object_heartbeat().await;
            result.map_err(to_py_err)
        })
    }

    /// Start promoting an object: allocate RDMA memory.
    /// 开始提升对象：分配 RDMA 内存。
    /// Returns (segment_name, offset, size, segment_id).
    fn promotion_alloc_start<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
        size: i64,
        preferred_segments: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        // Return Rust tuple — future_into_py handles IntoPy conversion
        // 返回 Rust 元组 —— future_into_py 处理 IntoPy 转换
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client
                .promotion_alloc_start(&key, size as u64, preferred_segments)
                .await;
            let replica = result.map_err(to_py_err)?;
            Ok((
                replica.segment_name,
                replica.offset,
                replica.size,
                replica.segment_id.to_string(),
            ))
        })
    }

    /// Notify master that promotion completed successfully.
    /// 通知 master 提升成功完成。
    fn notify_promotion_success<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.notify_promotion_success(&key).await;
            result.map_err(to_py_err)
        })
    }

    /// Notify master that promotion failed (memory can be reclaimed).
    /// 通知 master 提升失败（内存可被回收）。
    fn notify_promotion_failure<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner).await?;
            let result = client.notify_promotion_failure(&key).await;
            result.map_err(to_py_err)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (nqn, nsid, traddr, trsvcid, trtype = None))]
    fn build_nof_te_endpoint(
        nqn: String,
        nsid: u64,
        traddr: String,
        trsvcid: u64,
        trtype: Option<String>,
    ) -> String {
        crate::client_ext::build_nof_te_endpoint(nqn, nsid, traddr, trsvcid, trtype)
    }

    #[pyo3(signature = (nqn, nsid, traddr, trsvcid, base, size, trtype = None))]
    fn register_nof_ssd<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        nqn: String,
        nsid: u64,
        traddr: String,
        trsvcid: u64,
        base: u64,
        size: u64,
        trtype: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::client_ext::register_nof_ssd(slf, py, nqn, nsid, traddr, trsvcid, base, size, trtype)
    }

    #[pyo3(signature = (nqn, nsid, traddr, trsvcid, trtype = None))]
    fn unregister_nof_ssd_by_endpoint<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        nqn: String,
        nsid: u64,
        traddr: String,
        trsvcid: u64,
        trtype: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::client_ext::unregister_nof_ssd_by_endpoint(
            slf, py, nqn, nsid, traddr, trsvcid, trtype,
        )
    }

    fn batch_get_query_results(
        slf: &Bound<'_, Self>,
        keys: Vec<String>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        crate::client_ext::batch_get_query_results(slf, keys)
    }

    fn get_segments_detail<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::client_ext::get_segments_detail(slf, py)
    }

    fn get_into_ranges_cached(
        slf: &Bound<'_, Self>,
        buffers: Vec<Bound<'_, PyAny>>,
        all_keys: Vec<Vec<String>>,
        all_dst_offsets: Vec<Vec<Vec<usize>>>,
        all_src_offsets: Vec<Vec<Vec<usize>>>,
        all_sizes: Vec<Vec<Vec<usize>>>,
    ) -> PyResult<Vec<Vec<Vec<i64>>>> {
        crate::client_ext::get_into_ranges_cached(
            slf,
            buffers,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_http_arguments_default_and_validate_like_rust_config() {
        assert_eq!(
            normalize_client_http_config(false, 9300).unwrap(),
            mooncake_store_client::ClientHttpConfig::default()
        );
        assert_eq!(
            normalize_client_http_config(true, 19_300).unwrap(),
            mooncake_store_client::ClientHttpConfig {
                enabled: true,
                port: 19_300,
            }
        );
        assert!(normalize_client_http_config(true, 0).is_err());
    }

    #[test]
    fn python_tenant_defaults_match_cpp_tenant_identity() {
        assert_eq!(normalize_client_tenant_id(String::new()), "default");
        assert_eq!(
            normalize_client_tenant_id("tenant-a".to_string()),
            "tenant-a"
        );
    }

    #[test]
    fn dynamic_segment_ids_are_parsed_before_client_mutation() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let parsed =
            parse_uuid_list(&[first.to_string(), second.to_string()]).expect("valid UUIDs");
        assert_eq!(parsed, vec![first, second]);
        assert!(parse_uuid_list(&["not-a-uuid".to_string()]).is_err());
    }

    #[test]
    fn tensor_batch_config_preserves_group_id_indexing_after_filtering() {
        let config = ReplicateConfig {
            group_ids: vec![
                "group-a".to_string(),
                "group-b".to_string(),
                "group-c".to_string(),
            ],
            ..ReplicateConfig::default()
        };
        let indexed = indexed_batch_config(Some(config), 3, &[0, 2])
            .unwrap()
            .unwrap();
        assert_eq!(indexed.group_ids, vec!["group-a", "group-c"]);

        let invalid = ReplicateConfig {
            group_ids: vec!["only-one".to_string()],
            ..ReplicateConfig::default()
        };
        assert!(indexed_batch_config(Some(invalid), 2, &[0]).is_err());

        let repeated = repeated_indexed_batch_config(
            Some(ReplicateConfig {
                group_ids: vec!["group-a".to_string(), "group-b".to_string()],
                ..ReplicateConfig::default()
            }),
            2,
            &[1, 0],
            2,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            repeated.group_ids,
            vec!["group-b", "group-b", "group-a", "group-a"]
        );
    }

    #[test]
    fn parallel_reconstruction_accepts_only_tp_rank_size_remapping() {
        let requested = TensorParallelismPy {
            axes: vec![
                ParallelAxisPy {
                    kind: "dp".to_string(),
                    rank: 1,
                    size: 2,
                    split_dim: None,
                    expert_id: None,
                    stage_id: None,
                },
                ParallelAxisPy {
                    kind: "tp".to_string(),
                    rank: 3,
                    size: 8,
                    split_dim: Some(1),
                    expert_id: None,
                    stage_id: None,
                },
            ],
        };
        let mut stored = requested.clone();
        stored.axes[1].rank = 0;
        stored.axes[1].size = 4;
        assert!(tp_compatible_parallelism(&requested, &stored, Some(0), Some(4)).unwrap());
        assert!(!tp_compatible_parallelism(&requested, &stored, Some(1), Some(4)).unwrap());

        stored.axes[1].split_dim = Some(0);
        assert!(!tp_compatible_parallelism(&requested, &stored, Some(0), Some(4)).unwrap());
        stored.axes[1].split_dim = Some(1);
        stored.axes[0].rank = 0;
        assert!(!tp_compatible_parallelism(&requested, &stored, Some(0), Some(4)).unwrap());
    }

    fn normalize(protocol: &str) -> PyResult<NormalizedCreateArgs> {
        normalize_create_args(
            " host-1 ".to_string(),
            " P2PHANDSHAKE ".to_string(),
            " localhost:50051 ".to_string(),
            protocol.to_string(),
        )
    }

    #[test]
    fn normalize_create_args_trims_required_fields_and_defaults_protocol() {
        let normalized = normalize("").unwrap();
        assert_eq!(
            normalized,
            NormalizedCreateArgs {
                local_hostname: "host-1".to_string(),
                metadata_server: "P2PHANDSHAKE".to_string(),
                master_server_addr: "localhost:50051".to_string(),
                protocol: "tcp".to_string(),
            }
        );
    }

    #[test]
    fn normalize_create_args_lowercases_known_protocols() {
        assert_eq!(normalize(" RDMA ").unwrap().protocol, "rdma");
        assert_eq!(normalize("Tcp").unwrap().protocol, "tcp");
        assert_eq!(normalize("UBSHMEM").unwrap().protocol, "ubshmem");
        assert_eq!(normalize("rpc_only").unwrap().protocol, "rpc_only");
    }

    #[test]
    fn normalize_create_args_warns_but_passes_unknown_protocol() {
        assert_eq!(
            normalize(" FooTransport ").unwrap().protocol,
            "footransport"
        );
    }

    #[test]
    fn normalize_create_args_rejects_blank_protocol_when_explicit() {
        let err = normalize("   ").unwrap_err().to_string();
        assert!(err.contains("Invalid protocol"));
    }

    #[test]
    fn normalize_create_args_rejects_empty_required_fields() {
        for field in [
            ("local_hostname", "", "metadata", "master"),
            ("metadata_server", "host", "   ", "master"),
            ("master_server_addr", "host", "metadata", ""),
        ] {
            let (name, local_hostname, metadata_server, master_server_addr) = field;
            let err = normalize_create_args(
                local_hostname.to_string(),
                metadata_server.to_string(),
                master_server_addr.to_string(),
                "tcp".to_string(),
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains(name), "{err}");
        }
    }
}

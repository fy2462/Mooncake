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
//    Option:    Represents the client lifecycle state.  Some = connected
//               and usable; None = closed/consumed.  The take_client() helper
//               moves the client OUT of the Option temporarily during async
//               operations, then places it back.  This prevents any other
//               thread from using the client concurrently during an operation.
//               Option 代表客户端的生命周期状态。Some = 已连接可用；
//               None = 已关闭/已消费。take_client() 在异步操作期间临时将
//               client 移出，操作完成后再放回，防止并发使用。
//
// 2. Client lifecycle (客户端生命周期):
//
//    create() -> use (put/get/...) -> close()
//
//    After close(), all methods return StoreError("client already closed").
//    close() 设置 inner 为 None 并清空 registered_py_buffers。
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

use crate::remote_config::PyRemoteSourceConfig;
use crate::replicate_config::ReplicateConfigPy;
use mooncake_store_client::proto::StorageObjectMetadata;
use mooncake_store_client::MooncakeClient;
use parking_lot::Mutex;
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::ffi::c_void;
use std::sync::Arc;
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
///   zero-copy RDMA transfers.  Each entry is (pointer_address, PyObject).
///   Keeps the Python objects alive so the underlying memory is not freed
///   while the client holds RDMA registrations on it.
///   跟踪为零拷贝 RDMA 传输注册的 Python buffer 对象。保持 Python 对象
///   存活，防止底层内存在客户端持有 RDMA 注册期间被释放。
#[pyclass(name = "MooncakeClient")]
pub(crate) struct PythonMooncakeClient {
    pub(crate) inner: Arc<Mutex<Option<MooncakeClient>>>,
    pub(crate) registered_py_buffers: Arc<Mutex<Vec<(usize, Py<PyAny>)>>>,
}

// =========================================================================
// Internal helpers — 内部辅助函数
// =========================================================================

/// Convert a byte slice into an unbound Python bytes object.
/// 将字节切片转换为不绑定到特定生命周期的 Python bytes 对象。
fn bytes_to_py(py: Python<'_>, data: &[u8]) -> Py<PyBytes> {
    PyBytes::new(py, data).unbind()
}

/// Extract the raw pointer and size from a Python buffer-like object.
///
/// 从 Python buffer-like 对象（如 bytearray、memoryview、numpy array）中
/// 提取裸指针和字节大小。用于零拷贝 RDMA 读写操作。
///
/// Supports any Python object that implements the buffer protocol:
/// bytearray, memoryview, array.array, numpy arrays, etc.
fn get_buffer_ptr(obj: &Bound<'_, PyAny>) -> PyResult<(*mut c_void, usize)> {
    let buf = PyBuffer::<u8>::get(obj)?;
    Ok((buf.buf_ptr() as *mut c_void, buf.item_count()))
}

/// Take temporary ownership of the MooncakeClient from its Mutex.
///
/// 从 Mutex 中临时取出 MooncakeClient 的所有权。
///
/// This is the core concurrency primitive for every method:
/// 1. Lock the Mutex
/// 2. Extract the client via Option::take() — leaves None behind
/// 3. Perform the async operation with exclusive ownership
/// 4. Place the client back via *inner.lock() = Some(client)
///
/// If the client has already been taken (Option is None), returns an error.
/// This prevents concurrent access: only one operation can hold the client
/// at any given time.
///
/// 这是每个方法使用的核心并发原语：
/// 1. 锁定 Mutex
/// 2. 通过 Option::take() 提取客户端 —— 原位留下 None
/// 3. 以独占所有权执行异步操作
/// 4. 通过 *inner.lock() = Some(client) 将客户端放回
///
/// 如果客户端已被取出（Option 为 None），返回错误。这样可以防止并发访问：
/// 同一时间只有一个操作可以持有客户端。
pub(crate) fn take_client(inner: &Arc<Mutex<Option<MooncakeClient>>>) -> PyResult<MooncakeClient> {
    inner
        .lock()
        .take()
        .ok_or_else(|| to_py_err("client already closed"))
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
            d.into()
        })
        .collect();
    out.into_pyobject(py).expect("replicas_to_py: into_pyobject failed").unbind()
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
//   4. Inside the async block: take_client(), call the Rust method,
//      put the client back, return a Rust type (not PyObject)
//      (在 async 块内: take_client(), 调用 Rust 方法, 放回 client,
//       返回 Rust 类型而非 PyObject)
//
// Convention for every sync/block_on method (每个 sync/block_on 方法的约定):
//   1. Extract buffer pointers while GIL is held
//      (在持有 GIL 时提取 buffer 指针)
//   2. Clone the Arc<Mutex<...>>
//   3. Call tokio::runtime::Handle::current().block_on(async { ... })
//   4. Same take_client / put-back pattern inside the async block
//      (在 async 块内部使用相同的 take_client / 放回模式)

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
    ///                     本节点用于 RDMA/设备识别的主机名。默认 "localhost"。
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
    ) -> PyResult<Bound<'py, PyAny>> {
        use mooncake_store_client::LocalFsSource;

        // Apply defaults for empty/negative sentinel values.
        // 对空值/负数值哨兵应用默认值。
        let local_hostname = if local_hostname.is_empty() {
            "localhost".to_string()
        } else {
            local_hostname
        };
        let protocol = if protocol.is_empty() {
            "tcp".to_string()
        } else {
            protocol
        };
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

        // future_into_py: the async block runs on the tokio runtime, then
        // the returned PythonMooncakeClient is converted into a Python object
        // with the GIL held.
        // future_into_py: async 块在 tokio runtime 上执行，返回的
        // PythonMooncakeClient 在 GIL 持有下转换为 Python 对象。
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = MooncakeClient::create(
                &master_server_addr,
                &metadata_server,
                &local_hostname,
                &protocol,
                &device,
                gss,
                lbs,
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
            Ok(PythonMooncakeClient {
                inner: Arc::new(Mutex::new(Some(client))),
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
            let mut client = take_client(&inner)?;
            let result = client.put(&key, &data, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
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
            let mut client = take_client(&inner)?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.put_parts(&key, &slices, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
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
            let mut client = take_client(&inner)?;
            let result = client.get(&key).await;
            *inner.lock() = Some(client);
            // Return Rust type — future_into_py handles IntoPy conversion with GIL
            // 返回 Rust 类型 —— future_into_py 在持有 GIL 时处理 IntoPy 转换
            result.map_err(to_py_err)
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
            let mut client = take_client(&inner)?;
            let result = client.remove(&key, force).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.exists(&key).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.batch_put(&keys, &slices, cfg).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
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
            let mut client = take_client(&inner)?;
            let result = client.batch_get(&keys).await;
            *inner.lock() = Some(client);
            // Return Rust type — future_into_py handles IntoPy conversion with GIL
            // 返回 Rust 类型 —— future_into_py 在持有 GIL 时处理 IntoPy 转换
            result.map_err(to_py_err)
        })
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
            let mut client = take_client(&inner)?;
            let result = client.batch_remove(&keys, force).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.batch_is_exist(&keys).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.prefetch(&keys).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let removed = client.remove_by_regex(&pattern, force).await;
            *inner.lock() = Some(client);
            removed.map_err(to_py_err)
        })
    }

    /// Remove all keys from the store. Destructive — careful!
    /// 删除 store 中的所有 key。危险操作，请谨慎使用！
    fn remove_all<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let removed = client.remove_all().await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let size = client.get_size(&key).await;
            *inner.lock() = Some(client);
            size.map_err(to_py_err)
        })
    }

    // ===================================================================
    // health / tear down / close — 健康检查/关闭/生命周期管理
    // ===================================================================

    /// Return the hostname this client registered with.
    /// 返回此客户端注册时使用的主机名。
    fn get_hostname(&self) -> PyResult<String> {
        match self.inner.lock().as_ref() {
            Some(client) => Ok(client.get_hostname()),
            None => Err(to_py_err("client already closed")),
        }
    }

    /// Check connectivity to the metadata server and master.
    /// 检查与元数据服务器和 master 的连接状态。返回 bool。
    fn health_check<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.health_check().await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    /// Tear down all resources: buffers, segments, metadata entries.
    /// 销毁所有资源：缓冲区、段、元数据条目。用于整个集群的清理。
    fn tear_down_all<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = slf.borrow().inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut client = take_client(&inner)?;
            let result = client.tear_down_all().await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }

    /// Check whether this client has been closed.
    /// 检查此客户端是否已关闭。
    fn is_closed(&self) -> bool {
        match self.inner.lock().as_ref() {
            Some(client) => client.is_closed(),
            None => true,
        }
    }

    /// Close the client: drop the underlying connection and clear all
    /// registered Python buffer references.
    ///
    /// 关闭客户端：释放底层连接并清除所有已注册的 Python buffer 引用。
    /// After close(), all further operations will return StoreError.
    /// 关闭后，所有后续操作将返回 StoreError。
    fn close(&self) {
        *self.inner.lock() = None;
        self.registered_py_buffers.lock().clear();
    }

    /// String representation: "MooncakeClient(connected)" or
    /// "MooncakeClient(closed)".
    fn __repr__(&self) -> String {
        if self.inner.lock().is_some() {
            "MooncakeClient(connected)".to_string()
        } else {
            "MooncakeClient(closed)".to_string()
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
            let mut client = take_client(&inner)?;
            let result = client.upsert(&key, &data, cfg).await;
            *inner.lock() = Some(client);
            let replicas = result.map_err(to_py_err)?;
            let out: Vec<(String, u64, String)> = replicas
                .iter()
                .map(|r| (r.segment_name.clone(), r.offset, r.segment_id.to_string()))
                .collect();
            Ok(out)
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
            let mut client = take_client(&inner)?;
            let slices: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let result = client.upsert_parts(&key, &slices, cfg).await;
            *inner.lock() = Some(client);
            let replicas = result.map_err(to_py_err)?;
            let out: Vec<(String, u64, String)> = replicas
                .iter()
                .map(|r| (r.segment_name.clone(), r.offset, r.segment_id.to_string()))
                .collect();
            Ok(out)
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
            let mut client = take_client(&inner)?;
            let result = client.create_copy_task(&key, &targets).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.create_move_task(&key, &source, &target).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.query_task(task_id).await;
            *inner.lock() = Some(client);
            let resp = result.map_err(to_py_err)?;
            let task_id_str = resp.id.map(|id| Uuid::from_u64_pair(id.high, id.low).to_string());
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
            let mut client = take_client(&inner)?;
            let tasks = client.fetch_tasks(batch_size).await.map_err(to_py_err)?;
            *inner.lock() = Some(client);
            let out: Vec<(Option<String>, i32, String, i64, u32)> = tasks
                .iter()
                .map(|t| {
                    let id_str = t.id.map(|id| Uuid::from_u64_pair(id.high, id.low).to_string());
                    (id_str, t.r#type, t.payload.clone(), t.created_at_ms_epoch, t.max_retry_attempts)
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
            let mut client = take_client(&inner)?;
            let result = client
                .mark_task_to_complete(task_id, proto_status, &message)
                .await;
            *inner.lock() = Some(client);
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
        match self.inner.lock().as_ref() {
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
        match self.inner.lock().as_ref() {
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
    fn get_into(slf: &Bound<'_, Self>, key: String, buffer: Bound<'_, PyAny>) -> PyResult<usize> {
        let (ptr, size) = get_buffer_ptr(&buffer)?;
        let inner = slf.borrow().inner.clone();
        // block_on: future captures raw ptr, not Send-safe
        // block_on: future 捕获了裸指针，不是 Send 的
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.get_into(&key, ptr, size) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
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
            .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
            .collect::<PyResult<_>>()?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_get_into(&keys, &ptrs, &sizes) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
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
    ) -> PyResult<Vec<Vec<i64>>> {
        let ptrs: Vec<Vec<*mut c_void>> = all_buffers
            .iter()
            .map(|bufs| {
                bufs.iter()
                    .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<_>>()?;
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe {
                client.batch_get_into_multi_buffers(&keys, &ptrs, &all_sizes, prefer_same_node)
            }
            .await;
            *inner.lock() = Some(client);
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
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.put_from(&key, ptr, size, cfg) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
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
        let ptrs: Vec<*mut c_void> = buffers
            .iter()
            .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
            .collect::<PyResult<_>>()?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_put_from(&keys, &ptrs, &sizes, cfg) }.await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.get_buffer(&key).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.batch_get_buffer(&keys).await;
            *inner.lock() = Some(client);
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
    /// location: device identifier (e.g., "cpu:0", "cuda:0").
    fn register_buffer(
        slf: &Bound<'_, Self>,
        buffer: Bound<'_, PyAny>,
        size: usize,
        location: String,
    ) -> PyResult<()> {
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        {
            let slf_ref = slf.borrow();
            let guard = slf_ref.inner.lock();
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            unsafe { client.register_buffer(ptr, size, &location) }.map_err(to_py_err)?;
        }
        // Keep the Python object alive to prevent GC from freeing the memory
        // 保持 Python 对象存活，防止 GC 释放内存
        slf.borrow()
            .registered_py_buffers
            .lock()
            .push((ptr as usize, buffer.into_any().unbind()));
        Ok(())
    }

    /// Unregister a previously registered Python buffer.
    /// 注销之前注册的 Python buffer。
    fn unregister_buffer(slf: &Bound<'_, Self>, buffer: Bound<'_, PyAny>) -> PyResult<()> {
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        {
            let slf_ref = slf.borrow();
            let guard = slf_ref.inner.lock();
            let client = guard
                .as_ref()
                .ok_or_else(|| to_py_err("client already closed"))?;
            unsafe { client.unregister_buffer(ptr) }.map_err(to_py_err)?;
        }
        // Remove the tracking entry so the Python buffer can be GC'd
        // 移除跟踪条目，允许 Python buffer 被 GC
        slf.borrow()
            .registered_py_buffers
            .lock()
            .retain(|(addr, _)| *addr != ptr as usize);
        Ok(())
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
        let (ptr, _) = get_buffer_ptr(&buffer)?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        let replicas = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.upsert_from(&key, ptr, size, cfg) }.await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })?;
        Ok(replicas_to_py(replicas))
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
        let ptrs: Vec<*mut c_void> = buffers
            .iter()
            .map(|b| get_buffer_ptr(b).map(|(p, _)| p))
            .collect::<PyResult<_>>()?;
        let cfg = config.map(|c| c.borrow().to_core());
        let inner = slf.borrow().inner.clone();
        let results = tokio::runtime::Handle::current().block_on(async {
            let mut client = take_client(&inner)?;
            let result = unsafe { client.batch_upsert_from(&keys, &ptrs, &sizes, cfg) }.await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.mount_local_disk_segment(enable_offloading).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.offload_object_heartbeat(enable_offloading).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.report_ssd_capacity(bytes).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.notify_offload_success(keys, proto_metas).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.promotion_object_heartbeat().await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client
                .promotion_alloc_start(&key, size as u64, preferred_segments)
                .await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.notify_promotion_success(&key).await;
            *inner.lock() = Some(client);
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
            let mut client = take_client(&inner)?;
            let result = client.notify_promotion_failure(&key).await;
            *inner.lock() = Some(client);
            result.map_err(to_py_err)
        })
    }
}

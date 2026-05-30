// =============================================================================
// P2pStore Python bindings — 点对点元数据存储的 Python 绑定
// Python wrapper for the peer-to-peer metadata store
// =============================================================================
//
// P2pStore provides a distributed registry for tracking which nodes hold
// which data segments in a Mooncake cluster.  Each node registers its
// available memory regions (address + size) under a logical name.  Other
// nodes query the store to discover where data resides.
//
// P2pStore 提供分布式注册表，用于跟踪 Mooncake 集群中哪些节点持有哪些
// 数据段。每个节点将可用内存区域（地址 + 大小）注册在一个逻辑名称下。
// 其他节点查询该存储以发现数据所在位置。
//
// Lifecycle (生命周期):
//   create() -> register/unregister/get_replica/list -> close()
//
// Uses tokio::runtime::Handle::current().block_on() for all async operations
// because P2pStore methods are not Send (internal RPC connections).

use mooncake_p2p_store::{P2pStore, P2pStoreError, PayloadInfo};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use super::to_py_err;

/// Python wrapper for P2pStore.
///
/// Python 侧的 P2P 元数据存储封装。
/// Uses the same Arc<Mutex<Option<T>>> pattern as PythonMooncakeClient
/// for shared ownership + exclusive access + lifecycle tracking.
/// 与 PythonMooncakeClient 使用相同的 Arc<Mutex<Option<T>>> 模式。
#[pyclass(name = "P2pStore")]
pub(crate) struct P2pStorePy {
    inner: Arc<Mutex<Option<P2pStore>>>,
}

/// Convert a P2pStoreError to a Python StoreErrorPy exception.
/// 将 P2pStoreError 转换为 Python StoreErrorPy 异常。
fn map_p2p_err(e: P2pStoreError) -> PyErr {
    to_py_err(format!("P2P Store error: {e:?}"))
}

#[pymethods]
impl P2pStorePy {
    /// Create a new P2pStore and connect to the metadata backend (etcd).
    ///
    /// 创建新的 P2pStore 并连接到元数据后端（etcd）。
    ///
    /// Parameters (参数):
    ///   metadata_conn_string: etcd connection, e.g. "http://localhost:2379".
    ///   local_server_name:    Unique name for this node in the cluster.
    ///                         本节点在集群中的唯一名称。
    ///   nic_priority_matrix:  NIC priority list for RDMA NIC selection.
    ///                         RDMA 网卡优先级列表。
    #[staticmethod]
    fn create(
        metadata_conn_string: String,
        local_server_name: String,
        nic_priority_matrix: String,
    ) -> PyResult<Self> {
        // block_on: P2pStore::new() future is not Send
        // block_on: P2pStore::new() 的 future 不是 Send
        let store = tokio::runtime::Handle::current()
            .block_on(async {
                P2pStore::new(
                    &metadata_conn_string,
                    &local_server_name,
                    &nic_priority_matrix,
                )
                .await
            })
            .map_err(map_p2p_err)?;

        Ok(P2pStorePy {
            inner: Arc::new(Mutex::new(Some(store))),
        })
    }

    /// Return the local server name registered with the metadata backend.
    /// 返回在元数据后端注册的本地服务器名称。
    fn get_local_server_name(&self) -> PyResult<String> {
        let guard = self.inner.lock();
        let store = guard
            .as_ref()
            .ok_or_else(|| to_py_err("P2pStore already closed"))?;
        store.get_local_server_name().map_err(map_p2p_err)
    }

    /// Register a named memory region in the P2P registry.
    ///
    /// 在 P2P 注册表中注册一个命名的内存区域。
    ///
    /// Parameters (参数):
    ///   name:           Logical name for this memory region.
    ///                   内存区域的逻辑名称。
    ///   addr_list:      List of base addresses (as usize from Python ints).
    ///                   基地址列表（来自 Python int 的 usize）。
    ///   size_list:      Corresponding sizes in bytes. 对应的大小（字节）。
    ///   max_shard_size: Maximum per-shard size for splitting.
    ///                   分片的最大大小。
    ///   location:       Device location, e.g. "cpu:0". 设备位置。
    ///   force_create:   If true, overwrite existing registration.
    ///                   true 时覆盖已有注册。
    #[pyo3(signature = (name, addr_list, size_list, max_shard_size, location, force_create = false))]
    fn register(
        slf: &Bound<'_, Self>,
        name: String,
        addr_list: Vec<usize>,
        size_list: Vec<u64>,
        max_shard_size: u64,
        location: String,
        force_create: bool,
    ) -> PyResult<()> {
        let inner = slf.borrow().inner.clone();
        // block_on: the register future captures internal RPC connections,
        // making it not Send.
        // block_on: register future 捕获了内部 RPC 连接，使其不是 Send。
        tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store
                .register(
                    &name,
                    &addr_list,
                    &size_list,
                    max_shard_size,
                    &location,
                    force_create,
                )
                .await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })
    }

    /// Unregister a named memory region from the P2P registry.
    /// 从 P2P 注册表中注销一个命名的内存区域。
    fn unregister(slf: &Bound<'_, Self>, name: String) -> PyResult<()> {
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store.unregister(&name).await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })
    }

    /// List all registered payloads whose name starts with a prefix.
    ///
    /// 列出以指定前缀开头的所有已注册负载。
    /// Returns a Python list of dicts with keys: name, max_shard_size,
    /// total_size, size_list.
    /// 返回 Python 字典列表，包含 name, max_shard_size, total_size, size_list。
    fn list(slf: &Bound<'_, Self>, prefix: String) -> PyResult<Py<PyAny>> {
        let inner = slf.borrow().inner.clone();
        let payloads = tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store.list(&prefix).await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })?;
        // block_on holds the GIL, so assume_attached() is safe
        // block_on 持有 GIL，因此 assume_attached() 安全
        Ok({
            let py = unsafe { Python::assume_attached() };
            let out: Vec<Py<PyAny>> = payloads.iter().map(|p| payload_info_to_py(py, p)).collect();
            out.into_pyobject(py).unwrap().unbind()
        })
    }

    /// Get a replica of a registered payload (distributes metadata to this node).
    ///
    /// 获取已注册负载的副本（将元数据分发给本节点）。
    /// addr_list and size_list should match the original registration.
    /// addr_list 和 size_list 应与原始注册一致。
    fn get_replica(
        slf: &Bound<'_, Self>,
        name: String,
        addr_list: Vec<usize>,
        size_list: Vec<u64>,
    ) -> PyResult<()> {
        let inner = slf.borrow().inner.clone();
        tokio::runtime::Handle::current().block_on(async {
            let store = inner
                .lock()
                .take()
                .ok_or_else(|| to_py_err("P2pStore already closed"))?;
            let result = store.get_replica(&name, &addr_list, &size_list).await;
            *inner.lock() = Some(store);
            result.map_err(map_p2p_err)
        })
    }

    /// Close this P2pStore, releasing backend connections.
    /// 关闭 P2pStore，释放后端连接。
    fn close(&self) {
        *self.inner.lock() = None;
    }

    fn __repr__(&self) -> String {
        if self.inner.lock().is_some() {
            "P2pStore(connected)".to_string()
        } else {
            "P2pStore(closed)".to_string()
        }
    }
}

/// Convert a PayloadInfo into a Python dict with keys:
/// name, max_shard_size, total_size, size_list.
/// 将 PayloadInfo 转换为 Python 字典。
fn payload_info_to_py(py: Python<'_>, p: &PayloadInfo) -> Py<PyAny> {
    let d = PyDict::new(py);
    d.set_item("name", &p.name).ok();
    d.set_item("max_shard_size", p.max_shard_size).ok();
    d.set_item("total_size", p.total_size).ok();
    d.set_item("size_list", &p.size_list).ok();
    d.into()
}

// =============================================================================
// EngramStore Python bindings — LLM embedding table 存储的 Python 绑定
// Python wrapper for the engram (embedding-based) key-value store
// =============================================================================
//
// EngramStore is a specialized store for LLM embedding tables. It partitions
// the embedding table across multiple "heads" (a form of model parallelism).
// Each head stores a shard of the embedding vocab. Lookups use token indices
// to fetch the corresponding embedding vectors from the distributed store.
//
// EngramStore 是专门用于 LLM 嵌入表的存储。它将嵌入表分区为多个 "head"
// （一种模型并行形式）。每个 head 存储嵌入词汇表的一个分片。查找操作使用
// token 索引从分布式存储中获取对应的嵌入向量。
//
// NOTE: async lookup/populate methods are not yet exposed because the
// EngramClient trait returns non-Send futures (raw pointers captured across
// await points). These will be added when the trait is refactored.
// 注意：异步 lookup/populate 方法尚未暴露，因为 EngramClient trait 返回
// 非 Send 的 future（跨 await 点捕获裸指针）。待 trait 重构后再添加。

use super::to_py_err;
use crate::client::PythonMooncakeClient;
use mooncake_store_client::{EngramStore, EngramStoreConfig, MooncakeClient};
use parking_lot::Mutex;
use pyo3::prelude::*;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// EngramStoreConfigPy — 训练超参配置
// EngramStore configuration (table vocab sizes, embedding dim, buffer location)
// ---------------------------------------------------------------------------

/// Python-visible configuration for EngramStore.
///
/// Python 侧的 EngramStore 训练配置。
///
/// Fields (字段说明):
/// - table_vocab_sizes: Vocab size for each embedding table shard.
///   List length = number of heads. Default: [1024].
///   每个嵌入表分片的词汇量，列表长度 = head 数量。
/// - embedding_dim: Dimensionality of each embedding vector. Default: 64.
///   每个嵌入向量的维度。
/// - buffer_location: Device where buffers are allocated. Default: "cpu:0".
///   缓冲区分配的设备，如 "cpu:0", "cuda:0"。
#[pyclass(name = "EngramStoreConfig", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct EngramStoreConfigPy {
    #[pyo3(get, set)]
    pub table_vocab_sizes: Vec<i64>,
    #[pyo3(get, set)]
    pub embedding_dim: usize,
    #[pyo3(get, set)]
    pub buffer_location: String,
}

#[pymethods]
impl EngramStoreConfigPy {
    /// Create a new config with defaults suitable for a single-head table.
    /// 创建新配置，默认值适用于单 head 表。
    #[new]
    #[pyo3(signature = (
        table_vocab_sizes = vec![1024],
        embedding_dim = 64,
        buffer_location = "cpu:0".to_string(),
    ))]
    fn new(table_vocab_sizes: Vec<i64>, embedding_dim: usize, buffer_location: String) -> Self {
        Self {
            table_vocab_sizes,
            embedding_dim,
            buffer_location,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "EngramStoreConfig(tables={}, dim={}, location='{}')",
            self.table_vocab_sizes.len(),
            self.embedding_dim,
            self.buffer_location
        )
    }
}

impl EngramStoreConfigPy {
    /// Convert Python-side config to the Rust core config.
    /// 将 Python 侧配置转换为 Rust 核心配置。
    fn to_core(&self) -> EngramStoreConfig {
        EngramStoreConfig {
            table_vocab_sizes: self.table_vocab_sizes.clone(),
            embedding_dim: self.embedding_dim,
            buffer_location: self.buffer_location.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// EngramStorePy — 嵌入存储主类
// Main EngramStore wrapper
// ---------------------------------------------------------------------------

/// Python wrapper for EngramStore.
///
/// Python 侧的 EngramStore 封装。
///
/// Lifecycle (生命周期):
///   1. Create a PythonMooncakeClient (connected)
///   2. Create an EngramStorePy from config + client
///      -> Transfers MooncakeClient ownership INTO EngramStore
///      -> Source client's registered_py_buffers is cleared (client consumed)
///      -> 将 MooncakeClient 的所有权转移到 EngramStore 内部
///      -> 源 client 的 registered_py_buffers 被清除（client 已消费）
///   3. Use EngramStore... (currently only accessors exposed)
///   4. Call into_inner() to extract MooncakeClient back (EngramStore consumed)
///      -> 调用 into_inner() 取出 MooncakeClient（EngramStore 被消费）
///
/// Same Arc<Mutex<Option<T>>> pattern as PythonMooncakeClient — see client.rs
/// for the rationale.
/// 与 PythonMooncakeClient 使用相同的 Arc<Mutex<Option<T>>> 模式。
#[pyclass(name = "EngramStore")]
pub(crate) struct EngramStorePy {
    inner: Arc<Mutex<Option<EngramStore<MooncakeClient>>>>,
    /// Number of embedding table shards (heads). 嵌入表分片数。
    num_heads: usize,
    /// Dimensionality of each embedding vector. 嵌入向量维度。
    embedding_dim: usize,
    /// Store keys managed by this engram store. 此存储管理的 store key 列表。
    embed_keys: Vec<String>,
}

#[pymethods]
impl EngramStorePy {
    /// Create a new EngramStore, consuming the MooncakeClient.
    ///
    /// 创建新的 EngramStore，消费 MooncakeClient。
    /// After this call, the Python MooncakeClient object is marked as "consumed"
    /// (its inner is None and buffers are cleared).  You can recover the client
    /// later via into_inner().
    /// 调用后，Python MooncakeClient 对象被标记为"已消费"（inner 为 None，
    /// buffers 被清除）。之后可通过 into_inner() 恢复客户端。
    #[staticmethod]
    #[pyo3(signature = (layer_id, config, client))]
    fn new(
        layer_id: i32,
        config: Bound<'_, EngramStoreConfigPy>,
        client: Bound<'_, PythonMooncakeClient>,
    ) -> PyResult<Self> {
        let cfg = config.borrow().to_core();
        // Take the MooncakeClient out of its Python wrapper.
        // 从 Python 封装中取出 MooncakeClient。
        let inner_client = {
            let client_ref = client.borrow();
            let mut guard = client_ref.inner.lock();
            guard
                .take()
                .ok_or_else(|| to_py_err("MooncakeClient already closed or consumed"))?
        };
        let num_heads = cfg.table_vocab_sizes.len();
        let embedding_dim = cfg.embedding_dim;

        let store = EngramStore::new(layer_id, cfg, inner_client).map_err(to_py_err)?;
        let embed_keys = store.get_store_keys().to_vec();

        // Mark the source client as consumed — no more ops through it.
        // 标记源客户端已消费 —— 不能再通过它操作。
        client.borrow().registered_py_buffers.lock().clear();

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(store))),
            num_heads,
            embedding_dim,
            embed_keys,
        })
    }

    /// Extract the MooncakeClient back out. EngramStore is consumed.
    ///
    /// 取出内部的 MooncakeClient。EngramStore 被消费。
    /// Returns a fresh PythonMooncakeClient wrapping the recovered client.
    /// 返回一个新的 PythonMooncakeClient 封装恢复的客户端。
    fn into_inner(&self) -> PyResult<PythonMooncakeClient> {
        let store = self
            .inner
            .lock()
            .take()
            .ok_or_else(|| to_py_err("EngramStore already closed"))?;
        let client = store.into_inner();
        Ok(PythonMooncakeClient {
            inner: Arc::new(Mutex::new(Some(client))),
            registered_py_buffers: Arc::new(Mutex::new(Vec::new())),
        })
    }

    // -- accessors (只读属性) --

    /// Number of embedding table heads (shards).
    /// 嵌入表 head（分片）数量。
    #[getter]
    fn num_heads(&self) -> usize {
        self.num_heads
    }

    /// Dimensionality of each embedding vector.
    /// 每个嵌入向量的维度。
    #[getter]
    fn embedding_dim(&self) -> usize {
        self.embedding_dim
    }

    /// List of store keys managed by this engram store.
    /// 此 engram store 管理的 store key 列表。
    #[getter]
    fn store_keys(&self) -> Vec<String> {
        self.embed_keys.clone()
    }

    fn __repr__(&self) -> String {
        if self.inner.lock().is_some() {
            format!(
                "EngramStore(heads={}, dim={})",
                self.num_heads, self.embedding_dim
            )
        } else {
            "EngramStore(closed)".to_string()
        }
    }
}

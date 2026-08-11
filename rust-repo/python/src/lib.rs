// =============================================================================
// Rust-repo Python bindings — 模块注册 & 基础设施
// Module registration & infrastructure
// =============================================================================
//
// This file registers every Python-visible type and function exposed by the
// _mooncake_store native extension.  PyO3 automatically generates CPython glue
// from the #[pyclass] / #[pyfunction] / #[pymodule] annotations below.
//
// 本文件注册了 _mooncake_store 原生扩展暴露给 Python 的所有类型和函数。
// PyO3 根据下面的 #[pyclass] / #[pyfunction] / #[pymodule] 注解自动生成
// CPython 胶水代码。

mod buffer_export;
mod buffer_pool;
mod classic_transfer_engine;
mod client;
mod client_ext;
mod dlpack;
mod dummy_client;
mod dummy_ipc;
mod engram;
mod p2p_store;
pub mod remote_config;
mod replicate_config;
mod tensor_codec;
mod tensor_parallelism;
#[cfg(feature = "link-tent-native")]
mod transfer_engine;

use pyo3::prelude::*;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// StoreErrorPy — 自定义 Python 异常类
// Custom Python exception class for all Mooncake Store errors
// ---------------------------------------------------------------------------
//
// Uses PyO3's create_exception! macro to define a new exception type that
// inherits from PyException.  Every Rust error returned by the bindings is
// converted into this single exception type so Python callers can catch it
// uniformly:
//
//   try:
//       client.put("key", b"value")
//   except _mooncake_store.StoreError as e:
//       print(f"Mooncake error: {e}")
//
// 使用 PyO3 的 create_exception! 宏定义一个继承自 PyException 的新异常类型。
// 所有 Rust 错误都统一转换为这个异常，方便 Python 侧统一捕获处理。

pyo3::create_exception!(
    mooncake_store,
    StoreErrorPy,
    pyo3::exceptions::PyException,
    "Mooncake Store error"
);

/// Convert any Display-able Rust error into a Python StoreErrorPy exception.
///
/// 将任意实现了 Display 的 Rust 错误转换为 Python StoreErrorPy 异常。
/// Used throughout all binding modules as the canonical error bridge.
/// 在所有绑定模块中作为统一的错误桥接函数使用。
fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    StoreErrorPy::new_err(e.to_string())
}

// ---------------------------------------------------------------------------
// enable_te_debug_tracing() — 可在 Python 侧调用的调试日志初始化
// Tracing initialization (callable from Python for debugging)
// ---------------------------------------------------------------------------
//
// tracing_subscriber is a global singleton in Rust — you can only initialize
// it once per process.  The OnceLock ensures the first call wins and
// subsequent calls are no-ops, which is important when multiple Python tests
// import _mooncake_store.
//
// tracing_subscriber 是 Rust 中的全局单例 —— 每个进程只能初始化一次。
// OnceLock 确保只有第一次调用生效，后续调用为 no-op，这在多个 Python
// 测试 import _mooncake_store 时尤为重要。
//
// Logs are written to stderr with target name "te_debug", thread ids/names,
// source file paths, and line numbers.  The env filter can be overridden via
// the RUST_LOG environment variable.
//
// 日志写入 stderr，包含 target 名 "te_debug"，线程 id/名称，源文件路径和行号。
// 可通过 RUST_LOG 环境变量覆盖过滤级别。

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Enable TE debug tracing. Call once before any MooncakeClient operations.
/// Logs are written to stderr with the `te_debug` target.
///
/// From Python:
///     import _mooncake_store
///     _mooncake_store.enable_te_debug_tracing()
///
/// 启用 TE 调试日志。在所有 MooncakeClient 操作之前调用一次。
#[pyfunction]
fn enable_te_debug_tracing() {
    TRACING_INIT.get_or_init(|| {
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::fmt::format::FmtSpan;
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("te_debug=info")),
            )
            .init();
    });
}

// ---------------------------------------------------------------------------
// _mooncake_store 模块初始化
// Module init — called once when Python imports _mooncake_store
// ---------------------------------------------------------------------------
//
// This #[pymodule] function is the entry-point that CPython calls on the
// first `import _mooncake_store`.  It adds every #[pyclass] type as a Python
// class on the module, and every #[pyfunction] as a module-level function.
//
// 这个 #[pymodule] 函数是 CPython 在首次 import _mooncake_store 时调用的
// 入口点。它将每个 #[pyclass] 类型作为 Python 类添加到模块上，将每个
// #[pyfunction] 作为模块级函数。
//
// Classes registered (注册的类):
//   MooncakeClient      — core store client (核心存储客户端)
//   ReplicateConfig     — replication parameters (副本配置参数)
//   S3Config            — S3 connection settings (S3 连接设置)
//   RemoteSourceConfig  — remote source master switch (远程源总开关)
//   EngramStoreConfig   — engram training hyper-params (engram 训练超参)
//   EngramStore         — engram embedding store (engram 嵌入存储)
//   P2pStore            — peer-to-peer metadata store (P2P 元数据存储)
//
// Exception registered (注册的异常):
//   StoreError          — unified Mooncake error (统一 Mooncake 错误)
//
// Function registered (注册的函数):
//   enable_te_debug_tracing — one-shot tracing init (一次性日志初始化)

#[pymodule]
fn _mooncake_store(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<client::PythonMooncakeClient>()?;
    m.add_class::<classic_transfer_engine::PyClassicTransferEngine>()?;
    m.add_class::<buffer_pool::BufferPoolPy>()?;
    m.add_class::<buffer_pool::BufferLeasePy>()?;
    m.add("RegisteredBufferPool", m.getattr("BufferPool")?)?;
    m.add("RegisteredBufferLease", m.getattr("BufferLease")?)?;
    m.add_class::<dummy_client::PythonMooncakeDummyClient>()?;
    m.add_class::<dummy_ipc::PythonMooncakeDummyIpcChannel>()?;
    m.add_class::<replicate_config::ReplicateConfigPy>()?;
    m.add_class::<remote_config::PyS3Config>()?;
    m.add_class::<remote_config::PyRemoteSourceConfig>()?;
    m.add_class::<engram::EngramStoreConfigPy>()?;
    m.add_class::<engram::EngramStorePy>()?;
    m.add_class::<p2p_store::P2pStorePy>()?;
    m.add_class::<tensor_parallelism::ParallelAxisPy>()?;
    m.add_class::<tensor_parallelism::TensorParallelismPy>()?;
    m.add_class::<tensor_parallelism::ReadTargetPy>()?;
    m.add_class::<tensor_parallelism::WriterPartitionPy>()?;
    #[cfg(feature = "link-tent-native")]
    m.add_class::<transfer_engine::PyTransferIntent>()?;
    #[cfg(feature = "link-tent-native")]
    m.add_class::<transfer_engine::PyTransferPriority>()?;
    #[cfg(feature = "link-tent-native")]
    m.add_class::<transfer_engine::PyTentMetricsStatus>()?;
    #[cfg(feature = "link-tent-native")]
    m.add_class::<transfer_engine::PyTransferStatus>()?;
    #[cfg(feature = "link-tent-native")]
    m.add_class::<transfer_engine::PyTransferRequest>()?;
    #[cfg(feature = "link-tent-native")]
    m.add_class::<transfer_engine::PyTransferEngine>()?;
    m.add_function(wrap_pyfunction!(tensor_codec::tensor_metadata_size, m)?)?;
    m.add_function(wrap_pyfunction!(tensor_codec::serialize_tensor, m)?)?;
    m.add_function(wrap_pyfunction!(tensor_codec::deserialize_tensor, m)?)?;
    m.add("StoreError", m.py().get_type::<StoreErrorPy>())?;
    m.add_function(wrap_pyfunction!(enable_te_debug_tracing, m)?)?;
    Ok(())
}

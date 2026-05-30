//! # Error types for the Mooncake Store core crate.
//!
//! Defines the unified error type [`StoreError`] used across all store operations
//! (metadata management, replica allocation, transfer-engine interactions, etc.).
//! All fallible public APIs in this crate return [`StoreResult<T>`].
//!
//! # Mooncake Store 核心错误类型模块
//!
//! 定义了统一的错误类型 [`StoreError`]，用于所有存储操作（元数据管理、副本分配、传输引擎交互等）。
//! 本 crate 中所有可能失败的公共 API 都返回 [`StoreResult<T>`]。

use std::ffi::NulError;
use std::str::Utf8Error;

/// Unified error enum for all store-layer operations.
///
/// Wraps errors from FFI (transfer-engine, C-string conversions), I/O, serialization,
/// external services (etcd, Redis, K8s, S3), and operational failures (key not found,
/// replica not ready, etc.).
///
/// 统一的存储层错误枚举。
///
/// 包装了来自 FFI（传输引擎、C 字符串转换）、I/O、序列化、
/// 外部服务（etcd、Redis、K8s、S3）以及操作失败（key 未找到、副本未就绪等）的错误。
/// 对应 C++ 侧的异常体系和错误码返回值。
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A general-purpose operation failure carrying a numeric error code.
    /// 通用操作失败，携带数字错误码。
    /// 对应 C++ 中返回负值或非零错误码的场景。
    #[error("operation failed with code {0}")]
    OperationFailed(i32),

    /// A C/FFI function returned a null pointer when a valid handle was expected.
    /// C/FFI 函数返回了空指针，但期望获得有效句柄。
    /// 通常发生在 transfer engine 初始化或内存注册失败时。
    #[error("null handle returned")]
    NullHandle,

    /// The requested key does not exist in the metadata store.
    /// 请求的 key 在元数据存储中不存在。
    /// 对应 etcd key-not-found 或 Redis GET 返回 nil。
    #[error("key not found: {0}")]
    KeyNotFound(String),

    /// Attempted to create an object that already exists (e.g. duplicate PutStart).
    /// 尝试创建已存在的对象（例如重复调用 PutStart）。
    /// 对应 C++ ObjectExists 异常。
    #[error("object already exists: {0}")]
    ObjectExists(String),

    /// No storage handle is currently available — the handle pool is exhausted.
    /// 当前无可用存储句柄——句柄池已耗尽。
    /// 对应 C++ NO_AVAILABLE_HANDLE 错误。
    #[error("no available storage handle")]
    NoAvailableHandle,

    /// The target replica has not finished allocation / initialization yet.
    /// 目标副本尚未完成分配或初始化。
    /// 调用方应等待并重试。
    #[error("replica is not ready")]
    ReplicaNotReady,

    /// Caller-supplied parameters are malformed or out of range.
    /// 调用方提供的参数格式错误或超出范围。
    #[error("invalid parameters: {0}")]
    InvalidParams(String),

    /// The specified client ID is not registered with the master service.
    /// 指定的客户端 ID 未在 master 服务中注册。
    #[error("client not found: {0}")]
    ClientNotFound(String),

    /// The specified segment ID or name does not exist.
    /// 指定的 segment ID 或名称不存在。
    #[error("segment not found: {0}")]
    SegmentNotFound(String),

    /// The master / metadata service is temporarily unreachable.
    /// master 或元数据服务暂时不可达。
    #[error("service unavailable")]
    ServiceUnavailable,

    /// Error returned by the etcd metadata backend.
    /// etcd 元数据后端返回的错误。
    /// 对应 C++ 中 etcd::Client 操作的异常。
    #[error("etcd error: {0}")]
    EtcdError(String),

    /// Error returned by the Redis metadata backend.
    /// Redis 元数据后端返回的错误。
    #[error("redis error: {0}")]
    RedisError(String),

    /// Error from the Kubernetes API (used for service discovery).
    /// Kubernetes API 返回的错误（用于服务发现）。
    #[error("K8s error: {0}")]
    K8sError(String),

    /// Error from the S3-compatible object storage backend.
    /// S3 兼容对象存储后端返回的错误。
    #[error("S3 error: {0}")]
    S3Error(String),

    /// Error from the transfer-engine FFI layer (only available with the
    /// `transfer-engine` feature flag).
    /// Implements `From<TransferEngineError>` for ergonomic `?` usage.
    ///
    /// 传输引擎 FFI 层的错误（仅在启用 `transfer-engine` feature 时可用）。
    /// 实现了 `From<TransferEngineError>`，方便使用 `?` 运算符。
    #[cfg(feature = "transfer-engine")]
    #[error("transfer engine error: {0}")]
    TransferEngine(#[from] transfer_engine_ffi::TransferEngineError),

    /// A byte buffer that was expected to be valid UTF-8 is not.
    /// 期望为合法 UTF-8 的字节缓冲区实际不是。
    /// 常见于解析从 C++ 侧传回的字符串时。
    #[error("UTF-8 error: {0}")]
    InvalidUtf8(#[from] Utf8Error),

    /// A CString / CStr construction failed because the input contained an
    /// interior NUL byte.
    /// 由于输入包含中间 NUL 字节，CString/CStr 构造失败。
    #[error("NUL byte in C string: {0}")]
    NulError(#[from] NulError),

    /// JSON serialization / deserialization error (serde_json).
    /// JSON 序列化/反序列化错误。
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Standard I/O error from the standard library.
    /// 标准库的 I/O 错误。
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Catch-all variant for errors that do not fit other categories.
    /// 兜底变体，用于不适合其他分类的错误。
    #[error("{0}")]
    Internal(String),
}

/// Convenience type alias — every fallible function in this crate returns this.
/// 便利类型别名——本 crate 中所有可能失败的函数都返回此类型。
pub type StoreResult<T> = Result<T, StoreError>;

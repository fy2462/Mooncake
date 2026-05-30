//! Error types for the P2P Store.
//! P2P Store 的错误类型。
//!
//! # Error Categories / 错误分类
//!
//! | Variant / 变体 | Layer / 层级 | Meaning / 含义 |
//! |---------------|-------------|---------------|
//! | `InvalidArgument` | API | Bad parameters (e.g., mismatched addr/size lists) |
//! | `PayloadOpened` | API | Attempt to register an already-registered payload |
//! | `PayloadNotOpened` | API | Attempt to unregister an unknown payload |
//! | `PayloadNotFound` | Metadata | Payload not found in etcd |
//! | `TransferEngine` | Transport | Underlying Transfer Engine error |
//! | `MetadataError` | Metadata | etcd operation failure |
//! | `Serialization` | Data | JSON serialization/deserialization error |
//! | `Io` | System | Filesystem I/O error |
//!
//! All variants map to `thiserror::Error` for automatic `Display` and `Error`
//! trait implementations. Conversion traits (`From`) are implemented for
//! `serde_json::Error` and `std::io::Error` to allow the `?` operator.
//! 所有变体通过 `thiserror::Error` 自动实现 `Display` 和 `Error` trait。
//! 为 `serde_json::Error` 和 `std::io::Error` 实现了 `From` 转换 trait，
//! 允许使用 `?` 操作符。

#[derive(Debug, thiserror::Error)]
pub enum P2pStoreError {
    /// API call with invalid arguments (e.g., empty addr_list, mismatched lengths).
    /// API 调用参数无效（如空的 addr_list、长度不匹配）。
    #[error("invalid arguments")]
    InvalidArgument,

    /// Attempted to register a payload that is already registered.
    /// A payload cannot be registered twice without first unregistering.
    /// 尝试注册一个已注册的负载。必须先取消注册再重新注册。
    #[error("payload already opened")]
    PayloadOpened,

    /// Attempted to operate on a payload that has not been registered.
    /// 尝试操作一个尚未注册的负载。
    #[error("payload not opened")]
    PayloadNotOpened,

    /// The requested payload was not found in the metadata store (etcd).
    /// 在元数据存储（etcd）中未找到请求的负载。
    #[error("payload not found")]
    PayloadNotFound,

    /// The underlying Transfer Engine returned an error.
    /// This wraps all TransferEngine errors (memory registration, segment
    /// operations, transfer failures, etc.).
    /// 底层 Transfer Engine 返回了错误。
    /// 封装所有 TransferEngine 错误（内存注册、段操作、传输失败等）。
    #[error("transfer engine error")]
    TransferEngine,

    /// An error occurred in the metadata store (etcd operations).
    /// The string contains the etcd error message.
    /// 元数据存储（etcd 操作）发生错误。
    /// 字符串包含 etcd 错误消息。
    #[error("metadata store error: {0}")]
    MetadataError(String),

    /// JSON serialization or deserialization of metadata failed.
    /// Payload metadata is stored as JSON in etcd.
    /// 元数据的 JSON 序列化或反序列化失败。
    /// 负载元数据以 JSON 格式存储在 etcd 中。
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// A filesystem I/O operation failed.
    /// 文件系统 I/O 操作失败。
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

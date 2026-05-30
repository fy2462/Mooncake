//! # Remote Source Error Types — 远程源错误类型
//!
//! 定义远程数据源操作可能产生的所有错误类型。
//! (Defines all error types that remote source operations can produce.)

use std::sync::Arc;
use std::time::Duration;

/// 远程源错误枚举。
/// (Remote source error enumeration.)
///
/// ## 错误变体 (Error Variants)
///
/// | 变体 (Variant) | 含义 (Meaning) |
/// |---|---|
/// | [`NotFound`] | key 在远程源中不存在 (key does not exist in remote source) |
/// | [`Io`] | I/O 错误，包装在 `Arc` 中以支持跨线程传播 (I/O error, wrapped in Arc for cross-thread propagation) |
/// | [`Timeout`] | 请求超时，记录超时时长 (request timed out, duration recorded) |
/// | [`RateLimited`] | 请求被限流 (request was rate-limited) |
/// | [`Internal`] | 内部错误，携带描述信息 (internal error with description string) |
///
/// 使用 `thiserror::Error` derive 宏自动生成 `Display` 和 `Error` trait 实现。
#[derive(Debug, Clone, thiserror::Error)]
pub enum RemoteSourceError {
    /// key 不存在于远程源中
    /// (key not found in remote source)
    #[error("key not found: {0}")]
    NotFound(String),

    /// I/O 错误（使用 Arc 包装以支持 Clone + Send + Sync）
    /// (I/O error, wrapped in Arc for Clone + Send + Sync support)
    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),

    /// 请求超时
    /// (request timed out after the given duration)
    #[error("timeout after {0:?}")]
    Timeout(Duration),

    /// 限流错误：请求频率过高被拒绝
    /// (rate-limited: too many requests)
    #[error("rate limited")]
    RateLimited,

    /// 内部错误（如协议解析失败、配置错误等）
    /// (internal error — protocol deserialization, misconfiguration, etc.)
    #[error("internal error: {0}")]
    Internal(String),
}

/// 自动将 `std::io::Error` 转换为 `RemoteSourceError::Io`。
/// 使用 `Arc` 包装以绕过 `std::io::Error` 的 `Clone` 限制。
impl From<std::io::Error> for RemoteSourceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(Arc::new(e))
    }
}

/// 远程源操作的 Result 类型别名。
/// (Type alias for remote source operation results.)
pub type RemoteSourceResult<T> = Result<T, RemoteSourceError>;

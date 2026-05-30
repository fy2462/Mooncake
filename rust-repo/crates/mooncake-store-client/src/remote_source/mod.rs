//! # Remote Source Module — 远程数据源抽象
//!
//! 定义了缓存未命中时回退到的远程数据源接口及多种实现。
//! (Defines the remote data source abstraction for cache-miss fallback, plus multiple implementations.)
//!
//! ## 架构 (Architecture)
//!
//! ```text
//! MissHandler (cache miss → remote fetch → hot cache populate)
//!     |
//!     +-- RemoteSource trait
//!           |
//!           +-- LocalFsSource     (本地文件系统，测试/开发环境)
//!           +-- S3RemoteSource    (AWS S3 / MinIO，生产环境，需 feature flag "s3")
//!           +-- [custom]          (用户可实现自定义远程源)
//! ```
//!
//! ## 何时触发远程回退 (When Remote Fallback is Triggered)
//!
//! 1. 本地存储 (RDMA / local cache) 未命中
//! 2. [`RemoteSourceConfig::enabled`] = `true`
//! 3. [`MissHandler::handle_miss`] 调用 → 查询 hot cache → 若未命中则调用 `RemoteSource::get`
//! 4. 获取成功后自动写入 hot cache，后续访问直接命中
//!
//! ## 模块列表 (Submodules)
//!
//! | 子模块 | 职责 |
//! |---|---|
//! | [`config`] | RemoteSourceConfig + S3Config 配置结构体 |
//! | [`error`] | RemoteSourceError 错误枚举 |
//! | [`local_fs`] | LocalFsSource — 本地文件系统源 |
//! | [`s3_source`] | S3RemoteSource — S3 远程源 (需 feature `s3`) |
//! | [`miss_handler`] | MissHandler — 缓存未命中处理 + 请求合并 (coalescing) |
//! | [`distributed`] | DistributedMissHandler — 多节点协调的未命中处理 |

pub mod config;
pub mod distributed;
pub mod error;
pub mod local_fs;
pub mod miss_handler;
#[cfg(feature = "s3")]
pub mod s3_source;

use std::sync::Arc;

use async_trait::async_trait;
pub use error::{RemoteSourceError, RemoteSourceResult};

/// 远程数据源抽象：当缓存未命中时从此 trait 实现中获取数据。
/// (Abstraction over a remote data source that backs cache misses.)
///
/// ## 设计 (Design)
///
/// 采用 `async_trait` 宏来实现 `async fn` 在 trait 中的支持。
/// 所有实现必须满足 `Send + Sync`，以便在线程和 async task 间共享。
///
/// ## 内置实现 (Built-in Implementations)
/// - [`LocalFsSource`] — 本地文件系统，test/dev 环境
/// - `S3RemoteSource` — AWS S3（或 MinIO 等兼容存储），生产环境
///
/// ## 自定义实现 (Custom Implementation)
/// 用户可实现此 trait 以支持其他远程存储后端（如 GCS、Azure Blob、HTTP 等）。
///
/// ## Blanket 实现 (Blanket Implementation)
/// 为 `Arc<T>` 提供了通用的委托实现，自动将调用转发到内部 `T`。
#[async_trait]
pub trait RemoteSource: Send + Sync {
    /// 从远程源获取单个 key 的数据。
    /// (Fetch a single key's value from the remote source.)
    ///
    /// ## 参数 (Parameters)
    /// - `key`: 要获取的 key，语义由实现定义（如 S3 object key、文件路径等）
    ///
    /// ## 返回值 (Returns)
    /// - `Ok(Vec<u8>)`: 获取成功
    /// - `Err(RemoteSourceError::NotFound)`: key 不存在
    /// - `Err(RemoteSourceError::Io)`: I/O 错误
    /// - `Err(RemoteSourceError::Timeout)`: 超时
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>>;

    /// 批量预取多个 key，实现可用源优化（如并行 S3 请求）。
    /// (Batch-fetch multiple keys with source-optimized concurrency.)
    ///
    /// 默认实现顺序调用 `get`——各实现可覆盖以利用底层并行能力。
    /// (Default implementation calls `get` sequentially — implementations may override
    /// with parallel fetches for better performance.)
    async fn prefetch_keys(&self, keys: &[String]) -> Vec<RemoteSourceResult<Vec<u8>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await);
        }
        results
    }
}

/// Blanket impl: `Arc<T>` 自动委托给内部 `T`。
/// (Blanket impl: `Arc<T>` delegates to inner `T`.)
///
/// 这样 MissHandler 可以使用 `Arc<dyn RemoteSource>` 来持有动态分发的远程源。
#[async_trait]
impl<T: RemoteSource + ?Sized> RemoteSource for Arc<T> {
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        self.as_ref().get(key).await
    }

    async fn prefetch_keys(&self, keys: &[String]) -> Vec<RemoteSourceResult<Vec<u8>>> {
        self.as_ref().prefetch_keys(keys).await
    }
}

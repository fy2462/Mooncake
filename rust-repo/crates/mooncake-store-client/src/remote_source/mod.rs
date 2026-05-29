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

/// Abstraction over a remote data source that backs cache misses.
///
/// Implementations include:
/// - [`LocalFsSource`] for local filesystem (test/dev)
/// - `S3RemoteSource` for AWS S3 (prod)
#[async_trait]
pub trait RemoteSource: Send + Sync {
    /// Fetch a single key's value from the remote source.
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>>;

    /// Batch-fetch multiple keys with source-optimized concurrency.
    /// Default implementation calls `get` sequentially.
    async fn prefetch_keys(&self, keys: &[String]) -> Vec<RemoteSourceResult<Vec<u8>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await);
        }
        results
    }
}

/// Blanket impl: `Arc<T>` delegates to inner `T`.
#[async_trait]
impl<T: RemoteSource + ?Sized> RemoteSource for Arc<T> {
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        self.as_ref().get(key).await
    }

    async fn prefetch_keys(&self, keys: &[String]) -> Vec<RemoteSourceResult<Vec<u8>>> {
        self.as_ref().prefetch_keys(keys).await
    }
}

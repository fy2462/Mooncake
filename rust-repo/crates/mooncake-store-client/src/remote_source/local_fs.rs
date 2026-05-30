//! # Local FS Source — 本地文件系统远程源
//!
//! 将本地文件系统作为远程数据源，主要用于测试和开发环境。
//! (Uses the local filesystem as a remote data source, primarily for test/dev environments.)
//!
//! ## 存储布局 (Storage Layout)
//!
//! Key → 文件路径 (file path): `<root_dir>/<hash_prefix>/<sanitized_key>`
//!
//! - **hash_prefix**: key 的哈希值的最低 8 位（2 个 hex 字符），用于目录分片，避免单目录文件过多
//! - **sanitized_key**: 将 key 中的 `/` 和 `\` 替换为 `_`，防止路径穿越攻击

use std::path::PathBuf;

use async_trait::async_trait;

use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};

/// 将字符串 key 映射到 `<root_dir>/<hash>/<key>` 路径的本地文件系统源。
/// (Maps string keys to files under `<root_dir>/<hash>/<key>`.)
///
/// ## Key 安全处理 (Key Sanitization)
/// - `/` 和 `\` 替换为 `_` 以防止路径穿越 (path traversal prevention)
/// - hash 前缀（2 位 hex）用于目录分片 (directory sharding)，减少单目录文件数量
///
/// ## 字段 (Fields)
/// - `root`: 文件存储的根目录
pub struct LocalFsSource {
    root: PathBuf,
}

impl LocalFsSource {
    /// 创建以 `root` 为根目录的本地文件系统源。
    /// (Create a LocalFsSource rooted at the given path.)
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// 将逻辑 key 转换为文件系统路径。
    /// (Convert a logical key to a filesystem path.)
    ///
    /// ## 路径生成逻辑 (Path Generation)
    /// 1. 对 key 进行安全替换（`/`、`\` → `_`）
    /// 2. 计算 key 的哈希值，取最低 8 位作为目录前缀（2 hex chars → 256 个分片目录）
    /// 3. 组合: `{root}/{hash_prefix}/{sanitized_key}`
    pub fn key_path(&self, key: &str) -> PathBuf {
        let sanitized = key.replace(['/', '\\'], "_");
        let hash_prefix = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            format!("{:02x}", hasher.finish() & 0xff)
        };
        self.root.join(hash_prefix).join(sanitized)
    }
}

#[async_trait]
impl RemoteSource for LocalFsSource {
    /// 从本地文件系统读取 key 对应的数据。
    /// (Read data for a key from the local filesystem.)
    ///
    /// ## 错误处理 (Error Handling)
    /// - 文件不存在 (`NotFound` kind) → `RemoteSourceError::NotFound(key)`
    /// - 其他 I/O 错误 → `RemoteSourceError::Io(arc_of_error)`
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        let path = self.key_path(key);
        tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                RemoteSourceError::NotFound(key.to_string())
            } else {
                RemoteSourceError::Io(std::sync::Arc::new(e))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_fs_get_missing() {
        let tmp = std::env::temp_dir().join("mooncake_test_local_fs_missing");
        let _ = std::fs::remove_dir_all(&tmp);
        let source = LocalFsSource::new(&tmp);
        let result = source.get("nonexistent_key").await;
        assert!(matches!(result, Err(RemoteSourceError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_local_fs_put_and_get() {
        let tmp = std::env::temp_dir().join("mooncake_test_local_fs_put_get");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let source = LocalFsSource::new(&tmp);
        let path = source.key_path("test_key");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"hello world").unwrap();

        let data = source.get("test_key").await.unwrap();
        assert_eq!(data, b"hello world");
    }
}

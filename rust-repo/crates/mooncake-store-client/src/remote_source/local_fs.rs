use std::path::PathBuf;

use async_trait::async_trait;

use super::{RemoteSource, RemoteSourceError, RemoteSourceResult};

/// Maps string keys to files under `<root_dir>/<hash>/<key>`.
///
/// Key sanitization: replaces `/` and `\` with `_` to prevent path traversal.
/// Hash prefix: first 2 hex chars of key hash for directory sharding.
pub struct LocalFsSource {
    root: PathBuf,
}

impl LocalFsSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

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

// =============================================================================
// Remote source configuration — S3 / 本地文件系统远程源配置
// S3Config and RemoteSourceConfig Python bindings
// =============================================================================
//
// These classes configure the remote "cold" storage backend. When an object
// is not found in the local RDMA cache, Mooncake can fetch it transparently
// from a remote source (S3 or local filesystem).  This enables training on
// datasets much larger than the aggregate cluster memory.
//
// 这些类配置远程"冷"存储后端。当本地 RDMA 缓存中未找到对象时，Mooncake
// 可以透明地从远程源（S3 或本地文件系统）获取。这使得训练数据集可以远超
// 集群总内存容量。

use mooncake_store_client::{RemoteSourceConfig, S3Config};
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// PyS3Config — S3 存储桶/连接设置
// S3Config — Python-visible S3 bucket+connection settings
// ---------------------------------------------------------------------------

/// Python-visible S3 configuration for remote object storage.
///
/// Python 侧的 S3 远程存储配置。
///
/// Fields (字段说明):
/// - bucket:            S3 bucket name. S3 存储桶名称。
/// - region:            AWS region, e.g. "us-east-1". AWS 区域。
/// - endpoint:          Custom S3-compatible endpoint (MinIO, Ceph, etc.).
///                      自定义 S3 兼容端点（MinIO, Ceph 等）。None = use AWS.
/// - prefix:            Object key prefix within the bucket.
///                      存储桶中对象的 key 前缀。
/// - access_key_id:     S3 access key. None = use default credentials chain.
///                      S3 访问密钥。None = 使用默认凭证链。
/// - secret_access_key: S3 secret key. None = use default credentials chain.
///                      S3 秘密密钥。None = 使用默认凭证链。
#[pyclass(name = "S3Config", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyS3Config {
    #[pyo3(get, set)]
    pub bucket: String,
    #[pyo3(get, set)]
    pub region: String,
    #[pyo3(get, set)]
    pub endpoint: Option<String>,
    #[pyo3(get, set)]
    pub prefix: String,
    #[pyo3(get, set)]
    pub access_key_id: Option<String>,
    #[pyo3(get, set)]
    pub secret_access_key: Option<String>,
}

#[pymethods]
impl PyS3Config {
    /// Create a new S3 config. Only bucket is required; all other fields
    /// have sensible defaults.
    /// 创建新的 S3 配置。只有 bucket 是必填的；其他字段有合理默认值。
    #[new]
    #[pyo3(signature = (
        bucket,
        region = String::from("us-east-1"),
        endpoint = None::<String>,
        prefix = String::new(),
        access_key_id = None::<String>,
        secret_access_key = None::<String>,
    ))]
    fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        prefix: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    ) -> Self {
        Self {
            bucket,
            region,
            endpoint,
            prefix,
            access_key_id,
            secret_access_key,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "S3Config(bucket='{}', region='{}', prefix='{}', endpoint={:?})",
            self.bucket, self.region, self.prefix, self.endpoint
        )
    }
}

impl PyS3Config {
    /// Convert Python-side S3Config to the Rust core S3Config.
    /// 将 Python 侧 S3Config 转换为 Rust 核心 S3Config。
    pub fn to_core(&self) -> S3Config {
        S3Config {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            prefix: self.prefix.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// PyRemoteSourceConfig — 远程源总开关
// RemoteSourceConfig — master switch + S3 or LocalFS config
// ---------------------------------------------------------------------------

/// Python wrapper for RemoteSourceConfig.
///
/// Python 侧的远程源总配置。
///
/// This is the top-level config passed to MooncakeClient.create(). It
/// controls whether the remote source feature is enabled and how it behaves.
/// When both s3_config and local_fs_root are set, S3 takes priority.
///
/// 这是传给 MooncakeClient.create() 的顶层配置。控制远程源功能是否启用
/// 及其行为。当同时设置 s3_config 和 local_fs_root 时，S3 优先。
///
/// Fields (字段说明):
/// - enabled:                Master switch for remote fetching. Default: false.
///                           远程获取的主开关。
/// - max_concurrent_fetches: Throttle on concurrent S3/LocalFS fetches.
///                           Default: 16. 并发远程获取的最大数量限制。
/// - s3_config:              S3 configuration. Takes priority when set.
///                           S3 配置。设置后优先使用。
/// - local_fs_root:          Local filesystem root dir. Fallback for dev/test.
///                           本地文件系统根目录。用于开发/测试的备选方案。
#[pyclass(name = "RemoteSourceConfig", from_py_object)]
#[derive(Clone)]
pub struct PyRemoteSourceConfig {
    #[pyo3(get, set)]
    pub enabled: bool,
    /// Maximum concurrent remote fetches. 并发远程获取的最大数量。
    #[pyo3(get, set)]
    pub max_concurrent_fetches: usize,
    /// S3 configuration (takes priority over local_fs_root when both are set).
    /// S3 配置（同时设置时优先于 local_fs_root）。
    #[pyo3(get, set)]
    pub s3_config: Option<PyS3Config>,
    /// Local filesystem root directory for test/dev.
    /// 本地文件系统根目录，用于测试/开发。
    #[pyo3(get, set)]
    pub local_fs_root: Option<String>,
}

#[pymethods]
impl PyRemoteSourceConfig {
    /// Create a new remote source config. By default, remote fetching is
    /// disabled (enabled=false). Set enabled=true and provide an s3_config
    /// or local_fs_root to activate.
    ///
    /// 创建新的远程源配置。默认远程获取禁用（enabled=false）。
    /// 设置 enabled=true 并提供 s3_config 或 local_fs_root 以启用。
    #[new]
    #[pyo3(signature = (
        enabled = false,
        max_concurrent_fetches = 16usize,
        s3_config = None::<PyS3Config>,
        local_fs_root = None::<String>,
    ))]
    fn new(
        enabled: bool,
        max_concurrent_fetches: usize,
        s3_config: Option<PyS3Config>,
        local_fs_root: Option<String>,
    ) -> Self {
        Self {
            enabled,
            max_concurrent_fetches,
            s3_config,
            local_fs_root,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RemoteSourceConfig(enabled={}, s3={:?}, local_fs_root={:?})",
            self.enabled, self.s3_config, self.local_fs_root
        )
    }
}

impl PyRemoteSourceConfig {
    /// Convert Python-side RemoteSourceConfig to the Rust core version.
    /// 将 Python 侧 RemoteSourceConfig 转换为 Rust 核心版本。
    /// Note: local_fs_root is NOT passed to the Rust config here; it is
    /// handled directly in MooncakeClient.create().
    /// 注意：local_fs_root 不在此处传给 Rust config；它在
    /// MooncakeClient.create() 中直接处理。
    pub fn to_core(&self) -> RemoteSourceConfig {
        RemoteSourceConfig {
            enabled: self.enabled,
            max_concurrent_fetches: self.max_concurrent_fetches,
            s3: self.s3_config.as_ref().map(|s| s.to_core()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_client::S3Config as RustS3Config;

    #[test]
    fn s3_config_roundtrip() {
        let py = PyS3Config {
            bucket: "bkt".into(),
            region: "eu-west-1".into(),
            endpoint: Some("http://minio:9000".into()),
            prefix: "p/".into(),
            access_key_id: Some("ak".into()),
            secret_access_key: Some("sk".into()),
        };
        let rust: RustS3Config = py.to_core();
        assert_eq!(rust.bucket, "bkt");
        assert_eq!(rust.region, "eu-west-1");
        assert_eq!(rust.endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(rust.prefix, "p/");
    }

    #[test]
    fn remote_config_enabled_s3() {
        let s3 = PyS3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: None,
            prefix: String::new(),
            access_key_id: None,
            secret_access_key: None,
        };
        let py = PyRemoteSourceConfig {
            enabled: true,
            max_concurrent_fetches: 8,
            s3_config: Some(s3),
            local_fs_root: None,
        };
        let rust = py.to_core();
        assert!(rust.enabled);
        assert_eq!(rust.max_concurrent_fetches, 8);
        assert_eq!(rust.s3.unwrap().bucket, "b");
    }

    #[test]
    fn remote_config_disabled() {
        let py = PyRemoteSourceConfig {
            enabled: false,
            max_concurrent_fetches: 16,
            s3_config: None,
            local_fs_root: None,
        };
        assert!(!py.to_core().enabled);
    }

    #[test]
    fn remote_config_local_fs() {
        let py = PyRemoteSourceConfig {
            enabled: true,
            max_concurrent_fetches: 4,
            s3_config: None,
            local_fs_root: Some("/tmp".into()),
        };
        let rust = py.to_core();
        assert!(rust.enabled);
        assert!(rust.s3.is_none());
    }
}

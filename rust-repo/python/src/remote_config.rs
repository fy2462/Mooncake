use mooncake_store_client::{RemoteSourceConfig, S3Config};
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// S3Config — Python-visible S3 bucket+connection settings
// ---------------------------------------------------------------------------

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
        Self { bucket, region, endpoint, prefix, access_key_id, secret_access_key }
    }

    fn __repr__(&self) -> String {
        format!(
            "S3Config(bucket='{}', region='{}', prefix='{}', endpoint={:?})",
            self.bucket, self.region, self.prefix, self.endpoint
        )
    }
}

impl PyS3Config {
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
// RemoteSourceConfig — master switch + S3 or LocalFS config
// ---------------------------------------------------------------------------

#[pyclass(name = "RemoteSourceConfig", from_py_object)]
#[derive(Clone)]
pub struct PyRemoteSourceConfig {
    #[pyo3(get, set)]
    pub enabled: bool,
    /// Maximum concurrent remote fetches.
    #[pyo3(get, set)]
    pub max_concurrent_fetches: usize,
    /// S3 configuration (takes priority over local_fs_root when both are set).
    #[pyo3(get, set)]
    pub s3_config: Option<PyS3Config>,
    /// Local filesystem root directory for test/dev.
    #[pyo3(get, set)]
    pub local_fs_root: Option<String>,
}

#[pymethods]
impl PyRemoteSourceConfig {
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
        Self { enabled, max_concurrent_fetches, s3_config, local_fs_root }
    }

    fn __repr__(&self) -> String {
        format!(
            "RemoteSourceConfig(enabled={}, s3={:?}, local_fs_root={:?})",
            self.enabled, self.s3_config, self.local_fs_root
        )
    }
}

impl PyRemoteSourceConfig {
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
            bucket: "bkt".into(), region: "eu-west-1".into(),
            endpoint: Some("http://minio:9000".into()), prefix: "p/".into(),
            access_key_id: Some("ak".into()), secret_access_key: Some("sk".into()),
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
            bucket: "b".into(), region: "us-east-1".into(),
            endpoint: None, prefix: String::new(),
            access_key_id: None, secret_access_key: None,
        };
        let py = PyRemoteSourceConfig { enabled: true, max_concurrent_fetches: 8, s3_config: Some(s3), local_fs_root: None };
        let rust = py.to_core();
        assert!(rust.enabled);
        assert_eq!(rust.max_concurrent_fetches, 8);
        assert_eq!(rust.s3.unwrap().bucket, "b");
    }

    #[test]
    fn remote_config_disabled() {
        let py = PyRemoteSourceConfig { enabled: false, max_concurrent_fetches: 16, s3_config: None, local_fs_root: None };
        assert!(!py.to_core().enabled);
    }

    #[test]
    fn remote_config_local_fs() {
        let py = PyRemoteSourceConfig { enabled: true, max_concurrent_fetches: 4, s3_config: None, local_fs_root: Some("/tmp".into()) };
        let rust = py.to_core();
        assert!(rust.enabled);
        assert!(rust.s3.is_none());
    }
}

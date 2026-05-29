use mooncake_store_client::{RemoteSourceConfig, S3Config};
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// S3Config — Python-visible S3 bucket+connection settings
// ---------------------------------------------------------------------------

#[pyclass(name = "S3Config", from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PyS3Config {
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
    pub(crate) fn to_core(&self) -> S3Config {
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
pub(crate) struct PyRemoteSourceConfig {
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
    pub(crate) fn to_core(&self) -> RemoteSourceConfig {
        RemoteSourceConfig {
            enabled: self.enabled,
            max_concurrent_fetches: self.max_concurrent_fetches,
            s3: self.s3_config.as_ref().map(|s| s.to_core()),
        }
    }
}

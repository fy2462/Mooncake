use serde::{Deserialize, Serialize};

/// Controls whether and how the remote source fallback operates.
///
/// When `enabled` is `false`, [`MissHandler::handle_miss`] returns
/// `NotFound` immediately — no remote fetch is attempted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSourceConfig {
    /// Master switch: when false, remote source is never consulted.
    #[serde(default)]
    pub enabled: bool,

    /// Maximum concurrent remote fetches (admission control).
    #[serde(default = "default_max_concurrent_fetches")]
    pub max_concurrent_fetches: usize,

    /// S3-specific configuration. When present and `enabled` is true,
    /// the S3 remote source is used.
    #[serde(default)]
    pub s3: Option<S3Config>,
}

/// S3 bucket and connection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,

    /// AWS region (e.g. "us-east-1").
    #[serde(default = "default_region")]
    pub region: String,

    /// Optional custom endpoint (for MinIO / compatible stores).
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Optional key prefix within the bucket.
    #[serde(default)]
    pub prefix: String,

    /// Optional AWS access key ID (falls back to env / IAM).
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// Optional AWS secret access key (falls back to env / IAM).
    #[serde(default)]
    pub secret_access_key: Option<String>,
}

fn default_max_concurrent_fetches() -> usize {
    16
}

fn default_region() -> String {
    "us-east-1".to_string()
}

impl Default for RemoteSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_concurrent_fetches: default_max_concurrent_fetches(),
            s3: None,
        }
    }
}

impl RemoteSourceConfig {
    /// Load from a TOML string.
    pub fn from_toml(toml: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(toml)
    }

    /// Returns true if the remote source is properly configured and enabled.
    pub fn is_ready(&self) -> bool {
        self.enabled && self.s3.as_ref().is_some_and(|s3| !s3.bucket.is_empty())
    }
}

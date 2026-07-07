//! # Remote Source Configuration — 远程源配置
//!
//! 控制远程数据源回退行为的配置结构体，支持 TOML 反序列化。
//! (Configuration structures controlling remote source fallback behavior, with TOML deserialization.)

use serde::{Deserialize, Serialize};

/// 控制远程源回退的开启/关闭及并发限制。
/// (Controls whether and how the remote source fallback operates.)
///
/// ## 字段语义 (Field Semantics)
///
/// | 字段 | 类型 | 默认值 | 含义 |
/// |---|---|---|---|
/// | `enabled` | `bool` | `false` | 总开关：`false` 时 [`MissHandler::handle_miss`] 直接返回 `NotFound` |
/// | `max_concurrent_fetches` | `usize` | `16` | 最大并发远程获取数（准入控制信号量大小） |
/// | `s3` | `Option<S3Config>` | `None` | S3 配置；非空 + `enabled=true` 时启用 S3 远程源 |
///
/// ## 使用 (Usage)
///
/// ```ignore
/// use mooncake_store_client::RemoteSourceConfig;
/// let config = RemoteSourceConfig {
///     enabled: true,
///     s3: None,  // 只启用 MissHandler 但不使用 S3（需配合其他 RemoteSource 实现）
///     ..Default::default()
/// };
/// ```
///
/// ## TOML 反序列化 (TOML Deserialization)
///
/// ```ignore
/// let toml_str = r#"
/// enabled = true
/// max_concurrent_fetches = 32
/// [s3]
/// bucket = "my-bucket"
/// region = "us-west-2"
/// "#;
/// let config = RemoteSourceConfig::from_toml(toml_str)?;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSourceConfig {
    /// 总开关：`false` 时从不查询远程源。
    /// (Master switch: when false, remote source is never consulted.)
    #[serde(default)]
    pub enabled: bool,

    /// 最大并发远程获取数（准入控制）。
    /// (Maximum concurrent remote fetches — admission control via semaphore.)
    #[serde(default = "default_max_concurrent_fetches")]
    pub max_concurrent_fetches: usize,

    /// S3 专用配置。当此字段非空且 `enabled` 为 `true` 时，使用 S3 远程源。
    /// (S3-specific configuration. When present and `enabled` is true,
    /// the S3 remote source is used.)
    #[serde(default)]
    pub s3: Option<S3Config>,
}

/// S3 存储桶及连接配置。
/// (S3 bucket and connection configuration.)
///
/// ## 字段 (Fields)
/// - `bucket`: 存储桶名称
/// - `region`: AWS 区域（默认 `"us-east-1"`）
/// - `endpoint`: 自定义端点（用于 MinIO / 兼容存储）
/// - `prefix`: 桶内 key 前缀（如 `"cache/"`），不会自动追加 `/`
/// - `access_key_id` / `secret_access_key`: 显式凭证（优先级高于环境变量和 IAM 角色）
///
/// ## 凭证优先级 (Credential Resolution, highest to lowest)
/// 1. `access_key_id` + `secret_access_key` 字段
/// 2. 环境变量: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
/// 3. IAM 实例角色 / `~/.aws/credentials`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 存储桶名称 (S3 bucket name)
    #[serde(default)]
    pub bucket: String,

    /// AWS 区域，如 `"us-east-1"` (AWS region, e.g. "us-east-1")
    #[serde(default = "default_region")]
    pub region: String,

    /// 自定义端点 URL，用于 MinIO 等 S3 兼容存储 (optional custom endpoint for MinIO / compatible stores)
    #[serde(default)]
    pub endpoint: Option<String>,

    /// 桶内 key 前缀，如 `"cache/"` (optional key prefix within the bucket)
    #[serde(default)]
    pub prefix: String,

    /// AWS 访问密钥 ID，缺省时使用默认凭证链 (optional AWS access key ID; falls back to default credential chain)
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// AWS 密钥，缺省时使用默认凭证链 (optional AWS secret access key; falls back to default credential chain)
    #[serde(default)]
    pub secret_access_key: Option<String>,

    /// Whether to use virtual-hosted-style addressing. False forces path-style access.
    #[serde(default = "default_use_virtual_addressing")]
    pub use_virtual_addressing: bool,

    /// Per-request timeout in milliseconds.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// TCP connect timeout in milliseconds. Retained for C++ config parity.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Prefer HTTPS for AWS endpoints. Custom endpoints should include their own scheme.
    #[serde(default = "default_use_https")]
    pub use_https: bool,
}

/// `max_concurrent_fetches` 的默认值。
/// (Default value for max_concurrent_fetches.)
fn default_max_concurrent_fetches() -> usize {
    16
}

/// S3 区域的默认值：`"us-east-1"`。
/// (Default AWS region: "us-east-1".)
fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_use_virtual_addressing() -> bool {
    true
}

fn default_request_timeout_ms() -> u64 {
    30_000
}

fn default_connect_timeout_ms() -> u64 {
    10_000
}

fn default_use_https() -> bool {
    true
}

impl S3Config {
    pub fn with_mooncake_env_fallbacks(&self) -> Self {
        let mut config = self.clone();
        config.region = choose_string(&config.region, "MOONCAKE_AWS_REGION", default_region());
        config.endpoint = choose_option(&config.endpoint, "MOONCAKE_AWS_S3_ENDPOINT");
        config.bucket = choose_string(&config.bucket, "MOONCAKE_AWS_BUCKET_NAME", String::new());
        config.access_key_id = choose_option(&config.access_key_id, "MOONCAKE_AWS_ACCESS_KEY_ID");
        config.secret_access_key =
            choose_option(&config.secret_access_key, "MOONCAKE_AWS_SECRET_ACCESS_KEY");
        config.use_virtual_addressing = choose_bool(
            config.use_virtual_addressing,
            "MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING",
        );
        config.use_https = choose_bool(config.use_https, "MOONCAKE_AWS_USE_HTTPS");
        config.request_timeout_ms =
            choose_u64(config.request_timeout_ms, "MOONCAKE_AWS_REQUEST_TIMEOUT_MS");
        config.connect_timeout_ms =
            choose_u64(config.connect_timeout_ms, "MOONCAKE_AWS_CONNECT_TIMEOUT_MS");
        config
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn choose_string(current: &str, env_name: &str, default_value: String) -> String {
    if current != default_value && !current.is_empty() {
        current.to_string()
    } else {
        env_nonempty(env_name).unwrap_or_else(|| current.to_string())
    }
}

fn choose_option(current: &Option<String>, env_name: &str) -> Option<String> {
    current.clone().or_else(|| env_nonempty(env_name))
}

fn choose_bool(current: bool, env_name: &str) -> bool {
    let Some(value) = env_nonempty(env_name) else {
        return current;
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => current,
    }
}

fn choose_u64(current: u64, env_name: &str) -> u64 {
    env_nonempty(env_name)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(current)
}

impl Default for RemoteSourceConfig {
    /// 默认配置：远程源禁用，最大 16 并发，无 S3。
    /// (Default: remote source disabled, max 16 concurrent fetches, no S3.)
    fn default() -> Self {
        Self {
            enabled: false,
            max_concurrent_fetches: default_max_concurrent_fetches(),
            s3: None,
        }
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: default_region(),
            endpoint: None,
            prefix: String::new(),
            access_key_id: None,
            secret_access_key: None,
            use_virtual_addressing: default_use_virtual_addressing(),
            request_timeout_ms: default_request_timeout_ms(),
            connect_timeout_ms: default_connect_timeout_ms(),
            use_https: default_use_https(),
        }
    }
}

impl RemoteSourceConfig {
    /// 从 TOML 字符串加载配置。
    /// (Load configuration from a TOML string.)
    ///
    /// ## 示例 (Example)
    /// ```ignore
    /// let config = RemoteSourceConfig::from_toml(r#"enabled = true"#)?;
    /// ```
    pub fn from_toml(toml: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(toml)
    }

    /// 返回远程源是否已正确配置并启用。
    /// (Returns true if remote source is properly configured and enabled.)
    ///
    /// 当前判定标准：`enabled == true` 且 S3 配置的 `bucket` 非空。
    /// 若未来添加更多远程源类型，此方法会相应扩展。
    pub fn is_ready(&self) -> bool {
        self.enabled && self.s3.as_ref().is_some_and(|s3| !s3.bucket.is_empty())
    }
}

use std::path::PathBuf;

/// Configuration for the client-side distributed filesystem backend.
///
/// The environment names and defaults match the C++
/// `DistributedStorageConfig`. The production backend deliberately accepts
/// only `hf3fs`; POSIX is available only as an injected test adapter and must
/// never silently stand in for USRBIO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedStorageConfig {
    pub fsdir: PathBuf,
    pub fs_adapter_type: String,
    pub enable_health_check: bool,
    pub hash_bucket_count: usize,
}

impl Default for DistributedStorageConfig {
    fn default() -> Self {
        Self {
            fsdir: PathBuf::from("distributed_dir"),
            fs_adapter_type: "hf3fs".to_string(),
            enable_health_check: false,
            hash_bucket_count: 256,
        }
    }
}

impl DistributedStorageConfig {
    pub fn from_environment() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let mut config = Self::default();
        if let Some(value) = lookup("MOONCAKE_DISTRIBUTED_ROOT_DIR") {
            config.fsdir = PathBuf::from(value);
        }
        if !config.fsdir.is_absolute() {
            config.fsdir = std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(&config.fsdir);
        }
        if let Some(value) = lookup("MOONCAKE_DISTRIBUTED_FS_TYPE") {
            config.fs_adapter_type = value;
        }
        if let Some(value) = lookup("MOONCAKE_DISTRIBUTED_HEALTH_CHECK") {
            config.enable_health_check = parse_bool_env(&value);
        }
        if let Some(value) =
            parse_env::<usize>(&mut lookup, "MOONCAKE_DISTRIBUTED_HASH_BUCKET_COUNT")
        {
            config.hash_bucket_count = value;
        }
        config
    }

    pub fn with_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.fsdir = root.into();
        if !self.fsdir.is_absolute() {
            self.fsdir = std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(&self.fsdir);
        }
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.fsdir.as_os_str().is_empty() {
            return Err("distributed fsdir must not be empty".to_string());
        }
        if !self.fsdir.is_absolute() {
            return Err(format!(
                "distributed fsdir must be absolute: {}",
                self.fsdir.display()
            ));
        }
        if self.fs_adapter_type != "hf3fs" {
            return Err(format!(
                "unsupported distributed fs_adapter_type: {}",
                self.fs_adapter_type
            ));
        }
        if self.hash_bucket_count == 0 {
            return Err("distributed hash_bucket_count must be positive".to_string());
        }
        Ok(())
    }
}

fn parse_bool_env(value: &str) -> bool {
    crate::utils::string_to_bool(value).unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketEvictionPolicy {
    None,
    Fifo,
    Lru,
}

/// Client-side bucket backend configuration.
///
/// C++ equivalent: `FileStorageConfig` + `BucketBackendConfig`. Bucket is the
/// default C++ offload backend, so these defaults intentionally follow the C++
/// values instead of the FilePerKey defaults below.
#[derive(Debug, Clone)]
pub struct BucketStorageConfig {
    pub root_dir: PathBuf,
    pub fsdir: String,
    pub bucket_size_limit: u64,
    pub bucket_keys_limit: usize,
    pub eviction_policy: BucketEvictionPolicy,
    pub quota_bytes: u64,
    pub total_keys_limit: usize,
}

impl Default for BucketStorageConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("/data/file_storage"),
            fsdir: "moon_bucket_storage_backend".to_string(),
            bucket_size_limit: 256 * 1024 * 1024,
            bucket_keys_limit: 500,
            // C++ FromEnvironment uses "fifo" when no policy env is set.
            eviction_policy: BucketEvictionPolicy::Fifo,
            // Zero means 90% of physical filesystem capacity.
            quota_bytes: 0,
            total_keys_limit: 10_000_000,
        }
    }
}

impl BucketStorageConfig {
    pub fn from_environment() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let mut config = Self::default();
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH") {
            config.root_dir = PathBuf::from(value);
        }
        if let Some(value) =
            parse_positive_u64(&mut lookup, "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES")
        {
            config.bucket_size_limit = value;
        }
        if let Some(value) = parse_positive_usize(&mut lookup, "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT")
        {
            config.bucket_keys_limit = value;
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY") {
            config.eviction_policy = match value.to_ascii_lowercase().as_str() {
                "fifo" => BucketEvictionPolicy::Fifo,
                "lru" => BucketEvictionPolicy::Lru,
                _ => BucketEvictionPolicy::None,
            };
        }
        let max_total_size =
            parse_positive_u64(&mut lookup, "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE")
                .or_else(|| parse_positive_u64(&mut lookup, "MOONCAKE_BUCKET_MAX_TOTAL_SIZE"));
        if let Some(value) = max_total_size {
            config.quota_bytes = value;
        }
        if let Some(value) = parse_positive_usize(&mut lookup, "MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT")
        {
            config.total_keys_limit = value;
        }
        config
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.fsdir.is_empty() {
            return Err("bucket fsdir must not be empty".to_string());
        }
        if self.bucket_size_limit == 0 || self.bucket_keys_limit == 0 {
            return Err("bucket size/key limits must be positive".to_string());
        }
        if self.total_keys_limit == 0 {
            return Err("bucket total_keys_limit must be positive".to_string());
        }
        Ok(())
    }
}

/// Client-side FileStorage configuration mirroring the C++
/// `FileStorageConfig` surface (mooncake-store/include/storage_backend.h).
///
/// Environment names, defaults, parsing, and validation follow the C++
/// implementation so the wheel FileStorage configuration oracle is
/// reproducible from Rust.
#[derive(Debug, Clone, PartialEq)]
pub struct FileStorageConfig {
    pub storage_backend_type: FileStorageBackendType,
    pub storage_filepath: PathBuf,
    pub local_buffer_size: u64,
    pub scanmeta_iterator_keys_limit: i64,
    pub total_keys_limit: i64,
    pub total_size_limit: u64,
    pub heartbeat_interval_seconds: u32,
    pub client_buffer_gc_interval_seconds: u32,
    pub client_buffer_gc_ttl_ms: u64,
    pub use_uring: bool,
    pub enable_disk_watermark_eviction: bool,
    pub disk_eviction_high_watermark_ratio: f64,
    pub disk_eviction_low_watermark_ratio: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStorageBackendType {
    Bucket,
    FilePerKey,
    OffsetAllocator,
    Distributed,
}

const DEFAULT_LOCAL_BUFFER_SIZE: u64 = 1280 * 1024 * 1024; // ~1.2 GB
const DEFAULT_SCANMETA_ITERATOR_KEYS_LIMIT: i64 = 20_000;
const DEFAULT_TOTAL_KEYS_LIMIT: i64 = 10_000_000;
const DEFAULT_TOTAL_SIZE_LIMIT: u64 = 2 * 1024 * 1024 * 1024 * 1024; // 2 TB

impl Default for FileStorageConfig {
    fn default() -> Self {
        Self {
            storage_backend_type: FileStorageBackendType::Bucket,
            storage_filepath: PathBuf::from("/data/file_storage"),
            local_buffer_size: DEFAULT_LOCAL_BUFFER_SIZE,
            scanmeta_iterator_keys_limit: DEFAULT_SCANMETA_ITERATOR_KEYS_LIMIT,
            total_keys_limit: DEFAULT_TOTAL_KEYS_LIMIT,
            total_size_limit: DEFAULT_TOTAL_SIZE_LIMIT,
            heartbeat_interval_seconds: 10,
            client_buffer_gc_interval_seconds: 1,
            client_buffer_gc_ttl_ms: 5000,
            use_uring: false,
            enable_disk_watermark_eviction: true,
            disk_eviction_high_watermark_ratio: 0.90,
            disk_eviction_low_watermark_ratio: 0.80,
        }
    }
}

impl FileStorageConfig {
    pub fn from_environment() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let mut config = Self::default();
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR") {
            config.storage_backend_type = match value.as_str() {
                "bucket_storage_backend" => FileStorageBackendType::Bucket,
                "file_per_key_storage_backend" => FileStorageBackendType::FilePerKey,
                "offset_allocator_storage_backend" => FileStorageBackendType::OffsetAllocator,
                "distributed_storage_backend" => FileStorageBackendType::Distributed,
                _ => config.storage_backend_type,
            };
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH") {
            config.storage_filepath = PathBuf::from(value);
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES") {
            if let Ok(size) = value.parse::<i64>()
                && size > 0
            {
                config.local_buffer_size = size as u64;
            }
        }
        let scan_limit = lookup("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT")
            .or_else(|| lookup("MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT"));
        if let Some(value) = scan_limit
            && let Ok(size) = value.parse::<i64>()
            && size > 0
        {
            config.scanmeta_iterator_keys_limit = size;
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT")
            && let Ok(size) = value.parse::<i64>()
            && size > 0
        {
            config.total_keys_limit = size;
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES")
            && let Ok(size) = value.parse::<i64>()
            && size > 0
        {
            config.total_size_limit = size as u64;
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS")
            && let Ok(seconds) = value.parse::<u32>()
            && seconds > 0
        {
            config.heartbeat_interval_seconds = seconds;
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS")
            && let Ok(seconds) = value.parse::<u32>()
            && seconds > 0
        {
            config.client_buffer_gc_interval_seconds = seconds;
        }
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS")
            && let Ok(ms) = value.parse::<i64>()
            && ms >= 0
        {
            config.client_buffer_gc_ttl_ms = ms as u64;
        }
        let use_uring = lookup("MOONCAKE_OFFLOAD_USE_URING")
            .or_else(|| lookup("MOONCAKE_USE_URING"))
            .unwrap_or_default();
        config.use_uring = use_uring == "true" || use_uring == "1";
        if let Some(value) = lookup("MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION") {
            config.enable_disk_watermark_eviction =
                matches!(value.as_str(), "1" | "true" | "TRUE" | "True");
        }
        let preferred_high = lookup("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO");
        let fallback_high = lookup("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO");
        config.disk_eviction_high_watermark_ratio = parse_ratio_or(
            preferred_high
                .as_deref()
                .filter(|value| !value.is_empty())
                .or(fallback_high.as_deref()),
            config.disk_eviction_high_watermark_ratio,
        );
        let preferred_low = lookup("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO");
        let fallback_low = lookup("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO");
        config.disk_eviction_low_watermark_ratio = parse_ratio_or(
            preferred_low
                .as_deref()
                .filter(|value| !value.is_empty())
                .or(fallback_low.as_deref()),
            config.disk_eviction_low_watermark_ratio,
        );
        config
    }

    /// C++ `FileStorageConfig::ValidatePath`: non-empty absolute path without
    /// `..` traversal that exists as a writable, non-symlink directory.
    pub fn validate_path(&self, path: &std::path::Path) -> Result<(), String> {
        if path.as_os_str().is_empty() {
            return Err("storage_filepath is invalid".to_string());
        }
        if !path.is_absolute() {
            return Err(format!(
                "storage_filepath must be an absolute path: {}",
                path.display()
            ));
        }
        if path
            .components()
            .any(|component| component.as_os_str() == "..")
        {
            return Err(format!("path traversal is not allowed: {}", path.display()));
        }
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| format!("storage_filepath does not exist: {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!("symbolic link is not allowed: {}", path.display()));
        }
        if !metadata.is_dir() {
            return Err(format!(
                "storage_filepath is not a directory: {}",
                path.display()
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o222 == 0 {
                return Err(format!(
                    "no write permission on directory: {}",
                    path.display()
                ));
            }
        }
        Ok(())
    }

    /// C++ `FileStorageConfig::Validate`: path plus positive global limits,
    /// heartbeat, and ordered in-range watermark ratios.
    pub fn validate(&self) -> Result<(), String> {
        self.validate_path(&self.storage_filepath)?;
        if self.total_keys_limit <= 0 {
            return Err("total_keys_limit must > 0".to_string());
        }
        if self.total_size_limit == 0 {
            return Err("total_size_limit should not be zero".to_string());
        }
        if self.heartbeat_interval_seconds == 0 {
            return Err("heartbeat_interval_seconds must > 0".to_string());
        }
        let low = self.disk_eviction_low_watermark_ratio;
        let high = self.disk_eviction_high_watermark_ratio;
        if !(low > 0.0 && low <= 1.0) {
            return Err("disk_eviction_low_watermark_ratio must be in (0, 1]".to_string());
        }
        if !(high > 0.0 && high <= 1.0) {
            return Err("disk_eviction_high_watermark_ratio must be in (0, 1]".to_string());
        }
        if low >= high {
            return Err("disk_eviction_low_watermark_ratio must be lower than high".to_string());
        }
        Ok(())
    }
}

/// C++ `ParseEnvRatioOr`: empty or malformed/out-of-range ratios fall back to
/// the default; valid ratios must be in (0.0, 1.0].
fn parse_ratio_or(raw: Option<&str>, default_value: f64) -> f64 {
    let Some(raw) = raw else {
        return default_value;
    };
    let Ok(value) = raw.trim().parse::<f64>() else {
        return default_value;
    };
    if !value.is_finite() || value <= 0.0 || value > 1.0 {
        return default_value;
    }
    value
}

#[cfg(test)]
mod file_storage_config_tests {
    use super::*;

    fn lookup_from<'a>(
        entries: &'a [(&'a str, &'a str)],
    ) -> impl FnMut(&str) -> Option<String> + 'a {
        move |name| {
            entries
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn cpp_parity_file_storage_defaults_when_no_env_set() {
        // C++ FileStorageTest.DefaultValuesWhenNoEnvSet: no overrides keeps
        // path, buffer, scan, bucket/global limits, heartbeat, enabled
        // eviction, and the 0.90/0.80 watermark defaults.
        let config = FileStorageConfig::from_lookup(lookup_from(&[]));
        assert_eq!(config.storage_backend_type, FileStorageBackendType::Bucket);
        assert_eq!(config.storage_filepath, PathBuf::from("/data/file_storage"));
        assert_eq!(config.local_buffer_size, DEFAULT_LOCAL_BUFFER_SIZE);
        assert_eq!(
            config.scanmeta_iterator_keys_limit,
            DEFAULT_SCANMETA_ITERATOR_KEYS_LIMIT
        );
        assert_eq!(config.total_keys_limit, DEFAULT_TOTAL_KEYS_LIMIT);
        assert_eq!(config.total_size_limit, DEFAULT_TOTAL_SIZE_LIMIT);
        assert_eq!(config.heartbeat_interval_seconds, 10);
        assert_eq!(config.client_buffer_gc_interval_seconds, 1);
        assert_eq!(config.client_buffer_gc_ttl_ms, 5000);
        assert!(!config.use_uring);
        assert!(config.enable_disk_watermark_eviction);
        assert_eq!(config.disk_eviction_high_watermark_ratio, 0.90);
        assert_eq!(config.disk_eviction_low_watermark_ratio, 0.80);

        let bucket = BucketStorageConfig::from_lookup(lookup_from(&[]));
        assert_eq!(bucket.bucket_keys_limit, 500);
        assert_eq!(bucket.total_keys_limit, 10_000_000);
    }

    #[test]
    fn cpp_parity_file_storage_read_int64_from_env() {
        // C++ FileStorageTest.ReadInt64FromEnv: local buffer 2 GiB, bucket key
        // limit 1000, and global key limit 5,000,000.
        let config = FileStorageConfig::from_lookup(lookup_from(&[
            ("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "2147483648"),
            ("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT", "5000000"),
        ]));
        assert_eq!(config.local_buffer_size, 2 * 1024 * 1024 * 1024);
        assert_eq!(config.total_keys_limit, 5_000_000);

        let bucket = BucketStorageConfig::from_lookup(lookup_from(&[(
            "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT",
            "1000",
        )]));
        assert_eq!(bucket.bucket_keys_limit, 1000);
    }

    #[test]
    fn cpp_parity_file_storage_read_disk_watermark_config_from_env() {
        // C++ FileStorageTest.ReadDiskWatermarkConfigFromEnv: legacy aliases
        // load, preferred offload variables override them, zero disables
        // eviction, and malformed ratios restore 0.90/0.80 defaults.
        let legacy = FileStorageConfig::from_lookup(lookup_from(&[
            ("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO", "0.75"),
            ("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.60"),
        ]));
        assert_eq!(legacy.disk_eviction_high_watermark_ratio, 0.75);
        assert_eq!(legacy.disk_eviction_low_watermark_ratio, 0.60);

        let preferred = FileStorageConfig::from_lookup(lookup_from(&[
            ("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO", "0.75"),
            (
                "MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO",
                "0.95",
            ),
            ("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.60"),
            ("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.85"),
        ]));
        assert_eq!(preferred.disk_eviction_high_watermark_ratio, 0.95);
        assert_eq!(preferred.disk_eviction_low_watermark_ratio, 0.85);

        let disabled = FileStorageConfig::from_lookup(lookup_from(&[(
            "MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION",
            "false",
        )]));
        assert!(!disabled.enable_disk_watermark_eviction);

        let malformed = FileStorageConfig::from_lookup(lookup_from(&[
            (
                "MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO",
                "not-a-ratio",
            ),
            ("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO", "1.5"),
        ]));
        assert_eq!(malformed.disk_eviction_high_watermark_ratio, 0.90);
        assert_eq!(malformed.disk_eviction_low_watermark_ratio, 0.80);
    }

    #[test]
    fn cpp_parity_file_storage_invalid_int_value_uses_default() {
        // C++ FileStorageTest.InvalidIntValueUsesDefault: malformed
        // bucket/global-size values and a negative heartbeat fall back.
        let config = FileStorageConfig::from_lookup(lookup_from(&[
            ("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "bogus"),
            ("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "-5"),
            ("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "-1"),
        ]));
        assert_eq!(config.local_buffer_size, DEFAULT_LOCAL_BUFFER_SIZE);
        assert_eq!(config.total_size_limit, DEFAULT_TOTAL_SIZE_LIMIT);
        assert_eq!(config.heartbeat_interval_seconds, 10);

        let bucket = BucketStorageConfig::from_lookup(lookup_from(&[(
            "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES",
            "not-a-number",
        )]));
        assert_eq!(bucket.bucket_size_limit, 256 * 1024 * 1024);
    }

    #[test]
    fn file_storage_uint32_intervals_reject_overflow() {
        let config = FileStorageConfig::from_lookup(lookup_from(&[
            ("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "4294967296"),
            (
                "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS",
                "4294967296",
            ),
        ]));

        assert_eq!(config.heartbeat_interval_seconds, 10);
        assert_eq!(config.client_buffer_gc_interval_seconds, 1);
    }

    #[test]
    fn cpp_parity_file_storage_empty_env_value_uses_default() {
        // C++ FileStorageTest.EmptyEnvValueUsesDefault: an empty
        // MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT falls back to the default 500.
        let bucket = BucketStorageConfig::from_lookup(lookup_from(&[(
            "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT",
            "",
        )]));
        assert_eq!(bucket.bucket_keys_limit, 500);
    }

    #[test]
    fn cpp_parity_file_storage_validate_success_with_valid_config() {
        // C++ FileStorageTest.ValidateSuccessWithValidConfig: an existing
        // absolute storage path with positive global limits and heartbeat
        // validates successfully.
        let root = tempfile::tempdir().unwrap();
        let mut config = FileStorageConfig::default();
        config.storage_filepath = root.path().to_path_buf();
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn cpp_parity_file_storage_validate_fails_on_empty_storage_path() {
        // C++ FileStorageTest.ValidateFailsOnEmptyStoragePath: empty,
        // whitespace, relative, traversal, and nonexistent paths fail while an
        // existing fixture path succeeds.
        let root = tempfile::tempdir().unwrap();
        for (path, should_fail) in [
            ("", true),
            ("   ", true),
            ("relative/path", true),
            ("/tmp/../etc", true),
            ("/tmp/definitely_missing_mooncake_dir", true),
            (root.path().to_str().unwrap(), false),
        ] {
            let mut config = FileStorageConfig::default();
            config.storage_filepath = PathBuf::from(path);
            assert_eq!(
                config.validate().is_err(),
                should_fail,
                "path {path:?} validation mismatch"
            );
        }
    }

    #[test]
    fn cpp_parity_file_storage_validate_fails_on_invalid_limits() {
        // C++ FileStorageTest.ValidateFailsOnInvalidLimits: zero global
        // key/size/heartbeat limits and invalid watermark ranges/order fail.
        let root = tempfile::tempdir().unwrap();
        let valid_path = root.path().to_path_buf();

        let mut zero_keys = FileStorageConfig::default();
        zero_keys.storage_filepath = valid_path.clone();
        zero_keys.total_keys_limit = 0;
        assert!(zero_keys.validate().is_err());

        let mut zero_size = FileStorageConfig::default();
        zero_size.storage_filepath = valid_path.clone();
        zero_size.total_size_limit = 0;
        assert!(zero_size.validate().is_err());

        let mut zero_heartbeat = FileStorageConfig::default();
        zero_heartbeat.storage_filepath = valid_path.clone();
        zero_heartbeat.heartbeat_interval_seconds = 0;
        assert!(zero_heartbeat.validate().is_err());

        let mut high_zero = FileStorageConfig::default();
        high_zero.storage_filepath = valid_path.clone();
        high_zero.disk_eviction_high_watermark_ratio = 0.0;
        assert!(high_zero.validate().is_err());

        let mut high_too_large = FileStorageConfig::default();
        high_too_large.storage_filepath = valid_path.clone();
        high_too_large.disk_eviction_high_watermark_ratio = 1.5;
        assert!(high_too_large.validate().is_err());

        let mut reversed = FileStorageConfig::default();
        reversed.storage_filepath = valid_path.clone();
        reversed.disk_eviction_high_watermark_ratio = 0.70;
        reversed.disk_eviction_low_watermark_ratio = 0.90;
        assert!(reversed.validate().is_err());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetEvictionPolicy {
    None,
    Fifo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetPersistMode {
    Disabled,
    Relaxed,
    Strict,
}

#[derive(Debug, Clone)]
pub struct OffsetAllocatorConfig {
    pub root_dir: PathBuf,
    pub fsdir: String,
    pub eviction_policy: OffsetEvictionPolicy,
    pub quota_bytes: u64,
    pub total_keys_limit: usize,
    pub high_ratio: f64,
    pub low_ratio: f64,
    pub keys_high_ratio: f64,
    pub keys_low_ratio: f64,
    pub max_evict_per_offload: usize,
    pub fallback_evict_batch: usize,
}

#[derive(Debug, Clone)]
pub struct OffsetPersistenceConfig {
    pub persist_mode: OffsetPersistMode,
    pub persist_interval_seconds: i64,
    pub enable_record_crc: bool,
}

impl Default for OffsetAllocatorConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("/tmp/mooncake_local_storage"),
            fsdir: "moon_offset_allocator".to_string(),
            eviction_policy: OffsetEvictionPolicy::None,
            quota_bytes: 0,
            total_keys_limit: 10_000_000,
            high_ratio: 0.90,
            low_ratio: 0.80,
            keys_high_ratio: 0.90,
            keys_low_ratio: 0.80,
            max_evict_per_offload: 4096,
            fallback_evict_batch: 16,
        }
    }
}

impl Default for OffsetPersistenceConfig {
    fn default() -> Self {
        Self {
            persist_mode: OffsetPersistMode::Disabled,
            persist_interval_seconds: 60,
            enable_record_crc: true,
        }
    }
}

impl OffsetAllocatorConfig {
    pub fn from_environment() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let mut config = Self::default();

        if lookup("MOONCAKE_OFFSET_EVICTION_POLICY")
            .is_some_and(|value| value.eq_ignore_ascii_case("fifo"))
        {
            config.eviction_policy = OffsetEvictionPolicy::Fifo;
        }
        if let Some(value) = parse_env::<f64>(&mut lookup, "MOONCAKE_OFFSET_HIGH_RATIO") {
            config.high_ratio = value;
        }
        if let Some(value) = parse_env::<f64>(&mut lookup, "MOONCAKE_OFFSET_LOW_RATIO") {
            config.low_ratio = value;
        }
        config.keys_high_ratio = config.high_ratio;
        config.keys_low_ratio = config.low_ratio;

        if let Some(value) =
            parse_positive_u64(&mut lookup, "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES")
        {
            config.quota_bytes = value;
        }
        if let Some(value) = parse_positive_usize(&mut lookup, "MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT")
        {
            config.total_keys_limit = value;
        }

        if let Some(value) =
            parse_positive_usize(&mut lookup, "MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD")
        {
            config.max_evict_per_offload = value;
        }

        config
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.fsdir.is_empty() {
            return Err("offset allocator fsdir must not be empty".to_string());
        }
        if !(0.0 < self.high_ratio && self.high_ratio <= 1.0) {
            return Err("offset allocator high_ratio must be in (0, 1]".to_string());
        }
        if !(0.0 < self.low_ratio && self.low_ratio < self.high_ratio) {
            return Err("offset allocator low_ratio must be in (0, high_ratio)".to_string());
        }
        if !(0.0 < self.keys_high_ratio && self.keys_high_ratio <= 1.0) {
            return Err("offset allocator keys_high_ratio must be in (0, 1]".to_string());
        }
        if !(0.0 < self.keys_low_ratio && self.keys_low_ratio < self.keys_high_ratio) {
            return Err(
                "offset allocator keys_low_ratio must be in (0, keys_high_ratio)".to_string(),
            );
        }
        if self.max_evict_per_offload == 0 || self.fallback_evict_batch == 0 {
            return Err("offset allocator eviction caps must be positive".to_string());
        }
        if self.total_keys_limit == 0 {
            return Err("offset allocator total_keys_limit must be positive".to_string());
        }
        Ok(())
    }
}

impl OffsetPersistenceConfig {
    pub fn from_environment() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let mut config = Self::default();

        if let Some(value) = lookup("MOONCAKE_OFFSET_PERSIST_MODE") {
            config.persist_mode = match value.as_str() {
                "disabled" | "DISABLED" => OffsetPersistMode::Disabled,
                "relaxed" | "RELAXED" => OffsetPersistMode::Relaxed,
                "strict" | "STRICT" => OffsetPersistMode::Strict,
                _ => config.persist_mode,
            };
        }
        if let Some(value) =
            parse_env::<i64>(&mut lookup, "MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS")
        {
            config.persist_interval_seconds = value;
        }
        if lookup("MOONCAKE_OFFSET_RECORD_CRC").is_some_and(|value| {
            matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "off")
        }) {
            config.enable_record_crc = false;
        }

        config
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.persist_mode == OffsetPersistMode::Relaxed && self.persist_interval_seconds < 5 {
            return Err(
                "offset allocator persist_interval_seconds must be at least 5 in relaxed mode"
                    .to_string(),
            );
        }
        Ok(())
    }
}

fn parse_env<T: std::str::FromStr>(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Option<T> {
    lookup(name)?.parse().ok()
}

fn parse_positive_usize(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Option<usize> {
    parse_env::<usize>(lookup, name).filter(|value| *value > 0)
}

fn parse_positive_u64(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Option<u64> {
    parse_env::<u64>(lookup, name).filter(|value| *value > 0)
}

#[cfg(test)]
mod tests {
    use super::{
        BucketEvictionPolicy, BucketStorageConfig, DistributedStorageConfig, OffsetAllocatorConfig,
        OffsetEvictionPolicy, OffsetPersistMode, OffsetPersistenceConfig,
    };
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[test]
    fn bucket_defaults_match_cpp_file_storage_defaults() {
        let config = BucketStorageConfig::default();
        assert_eq!(config.bucket_size_limit, 256 * 1024 * 1024);
        assert_eq!(config.bucket_keys_limit, 500);
        assert_eq!(config.eviction_policy, BucketEvictionPolicy::Fifo);
        assert_eq!(config.quota_bytes, 0);
        assert_eq!(config.total_keys_limit, 10_000_000);
    }

    #[test]
    fn bucket_config_reads_cpp_environment_names() {
        let environment = HashMap::from([
            ("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/var/lib/mooncake"),
            ("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "4096"),
            ("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "8"),
            ("MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY", "LRU"),
            ("MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE", "32768"),
            ("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT", "64"),
        ]);
        let config = BucketStorageConfig::from_lookup(|name| {
            environment.get(name).map(|value| value.to_string())
        });
        assert_eq!(config.root_dir, PathBuf::from("/var/lib/mooncake"));
        assert_eq!(config.bucket_size_limit, 4096);
        assert_eq!(config.bucket_keys_limit, 8);
        assert_eq!(config.eviction_policy, BucketEvictionPolicy::Lru);
        assert_eq!(config.quota_bytes, 32768);
        assert_eq!(config.total_keys_limit, 64);
        config.validate().unwrap();
    }

    #[test]
    fn cpp_parity_bucket_global_size_limit_does_not_set_eviction_quota() {
        let config = BucketStorageConfig::from_lookup(|name| match name {
            "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES" => Some("969".to_string()),
            "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES" => Some("100".to_string()),
            _ => None,
        });

        assert_eq!(config.bucket_size_limit, 969);
        assert_eq!(config.quota_bytes, 0);
    }

    #[test]
    fn distributed_defaults_and_environment_match_cpp() {
        let defaults = DistributedStorageConfig::default();
        assert_eq!(defaults.fsdir, PathBuf::from("distributed_dir"));
        assert_eq!(defaults.fs_adapter_type, "hf3fs");
        assert!(!defaults.enable_health_check);
        assert_eq!(defaults.hash_bucket_count, 256);

        let environment = HashMap::from([
            ("MOONCAKE_DISTRIBUTED_ROOT_DIR", "/mnt/3fs/mooncake"),
            ("MOONCAKE_DISTRIBUTED_FS_TYPE", "hf3fs"),
            ("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", "true"),
            ("MOONCAKE_DISTRIBUTED_HASH_BUCKET_COUNT", "64"),
        ]);
        let config = DistributedStorageConfig::from_lookup(|name| {
            environment.get(name).map(|value| value.to_string())
        });
        assert_eq!(config.fsdir, PathBuf::from("/mnt/3fs/mooncake"));
        assert!(config.enable_health_check);
        assert_eq!(config.hash_bucket_count, 64);
        config.validate().unwrap();
    }

    #[test]
    fn distributed_config_rejects_non_hf3fs_and_zero_buckets() {
        let mut config = DistributedStorageConfig::default().with_root("/mnt/3fs/mooncake");
        config.fs_adapter_type = "posix".to_string();
        assert!(config.validate().is_err());

        config.fs_adapter_type = "hf3fs".to_string();
        config.hash_bucket_count = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn offset_config_reads_supported_environment_values() {
        let environment = HashMap::from([
            ("MOONCAKE_OFFSET_EVICTION_POLICY", "FIFO"),
            ("MOONCAKE_OFFSET_HIGH_RATIO", "0.85"),
            ("MOONCAKE_OFFSET_LOW_RATIO", "0.70"),
            ("MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD", "32"),
            ("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "4096"),
            ("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT", "64"),
        ]);

        let config = OffsetAllocatorConfig::from_lookup(|name| {
            environment.get(name).map(|value| value.to_string())
        });

        assert_eq!(config.eviction_policy, OffsetEvictionPolicy::Fifo);
        assert_eq!(config.high_ratio, 0.85);
        assert_eq!(config.low_ratio, 0.70);
        assert_eq!(config.keys_high_ratio, 0.85);
        assert_eq!(config.keys_low_ratio, 0.70);
        assert_eq!(config.max_evict_per_offload, 32);
        assert_eq!(config.quota_bytes, 4096);
        assert_eq!(config.total_keys_limit, 64);
    }

    #[test]
    fn offset_config_ignores_malformed_or_non_positive_environment_values() {
        let environment = HashMap::from([
            ("MOONCAKE_OFFSET_HIGH_RATIO", "not-a-number"),
            ("MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD", "0"),
        ]);

        let config = OffsetAllocatorConfig::from_lookup(|name| {
            environment.get(name).map(|value| value.to_string())
        });

        assert_eq!(config.high_ratio, 0.90);
        assert_eq!(config.max_evict_per_offload, 4096);
        config.validate().unwrap();
    }

    #[test]
    fn offset_persistence_defaults_match_cpp() {
        let config = OffsetPersistenceConfig::default();

        assert_eq!(config.persist_mode, OffsetPersistMode::Disabled);
        assert_eq!(config.persist_interval_seconds, 60);
        assert!(config.enable_record_crc);
    }

    #[test]
    fn offset_config_reads_cpp_persistence_environment_values() {
        let environment = HashMap::from([
            ("MOONCAKE_OFFSET_PERSIST_MODE", "RELAXED"),
            ("MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS", "7"),
            ("MOONCAKE_OFFSET_RECORD_CRC", "OFF"),
        ]);

        let config = OffsetPersistenceConfig::from_lookup(|name| {
            environment.get(name).map(|value| value.to_string())
        });

        assert_eq!(config.persist_mode, OffsetPersistMode::Relaxed);
        assert_eq!(config.persist_interval_seconds, 7);
        assert!(!config.enable_record_crc);
        config.validate().unwrap();
    }

    #[test]
    fn offset_config_ignores_unknown_or_malformed_persistence_environment_values() {
        let environment = HashMap::from([
            ("MOONCAKE_OFFSET_PERSIST_MODE", "ReLaXeD"),
            ("MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS", "soon"),
            ("MOONCAKE_OFFSET_RECORD_CRC", "sometimes"),
        ]);

        let config = OffsetPersistenceConfig::from_lookup(|name| {
            environment.get(name).map(|value| value.to_string())
        });

        assert_eq!(config.persist_mode, OffsetPersistMode::Disabled);
        assert_eq!(config.persist_interval_seconds, 60);
        assert!(config.enable_record_crc);
    }

    #[test]
    fn offset_record_crc_accepts_all_cpp_disable_spellings() {
        for disabled in ["0", "false", "FALSE", "off", "OFF"] {
            let config = OffsetPersistenceConfig::from_lookup(|name| {
                (name == "MOONCAKE_OFFSET_RECORD_CRC").then(|| disabled.to_string())
            });
            assert!(!config.enable_record_crc, "value={disabled}");
        }
    }

    #[test]
    fn relaxed_persistence_rejects_intervals_below_five_seconds() {
        let config = OffsetPersistenceConfig {
            persist_mode: OffsetPersistMode::Relaxed,
            persist_interval_seconds: 4,
            ..OffsetPersistenceConfig::default()
        };

        assert_eq!(
            config.validate().unwrap_err(),
            "offset allocator persist_interval_seconds must be at least 5 in relaxed mode"
        );
    }

    #[test]
    fn strict_and_disabled_modes_do_not_restrict_the_interval() {
        for persist_mode in [OffsetPersistMode::Strict, OffsetPersistMode::Disabled] {
            let config = OffsetPersistenceConfig {
                persist_mode,
                persist_interval_seconds: 0,
                ..OffsetPersistenceConfig::default()
            };
            config.validate().unwrap();
        }
    }
}

/// Configuration for the local storage backend (FilePerKey).
///
/// Controls the on-disk layout, eviction policy, and quota management.
/// C++ equivalent: `FilePerKeyConfig` + quota/eviction fields from `StorageBackend::Init`.
#[derive(Debug, Clone)]
pub struct LocalStorageConfig {
    /// Root directory for all storage data.
    /// FilePerKey data lives in `<root>/<fsdir>/`.
    pub root_dir: PathBuf,

    /// Subdirectory name under `root_dir`.
    /// C++ equivalent: the `fsdir` parameter (prefixed with "moon_" in C++).
    pub fsdir: String,

    /// Enable FIFO eviction when the used space exceeds the quota.
    /// C++ equivalent: `FilePerKeyConfig::enable_eviction`.
    pub enable_eviction: bool,

    /// Total storage quota in bytes.
    /// When `0`, auto-detect from filesystem capacity (90% of available space).
    /// C++ equivalent: `total_space` derived from `statfs` when `quota_bytes == 0`.
    pub quota_bytes: u64,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("/tmp/mooncake_local_storage"),
            fsdir: "moon_file_per_key_dir".to_string(),
            enable_eviction: true,
            quota_bytes: 0,
        }
    }
}

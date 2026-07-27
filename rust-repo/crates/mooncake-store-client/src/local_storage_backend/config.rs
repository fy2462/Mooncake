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
                .or_else(|| parse_positive_u64(&mut lookup, "MOONCAKE_BUCKET_MAX_TOTAL_SIZE"))
                .or_else(|| {
                    parse_positive_u64(&mut lookup, "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES")
                });
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

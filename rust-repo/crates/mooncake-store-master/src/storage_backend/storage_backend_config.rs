use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Snapshot storage backend types.
/// 快照存储后端类型：
/// - LocalDisk：普通本地磁盘，无需额外注册
/// - Hf3fs：3FS 分布式文件系统，需要通过 hf3fs::register_fd 注册文件描述符
/// - FilePerKey：每个 key 独立文件存储（用于 offload 场景）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendType {
    /// Standard local filesystem — no special handling needed.
    /// 标准本地文件系统 —— 无需特殊处理。
    LocalDisk,
    /// HF3FS (3FS) distributed filesystem — requires fd registration.
    /// HF3FS（3FS）分布式文件系统 —— 需要 fd 注册。
    Hf3fs,
    /// File-per-key mode: each key is stored as a separate file.
    /// 每个 key 独立文件模式：每个 key 存储为单独的文件。
    FilePerKey,
    /// Bucket mode: multiple keys are packed into bucket files.
    /// Bucket 模式：多个 key 聚合写入 bucket 文件。
    Bucket,
    /// Offset allocator mode: values are appended into one data file with a
    /// persistent key -> offset index.
    /// Offset allocator 模式：值写入单个数据文件，并维护持久化 key -> offset 索引。
    OffsetAllocator,
    /// Distributed filesystem mode: bucketed key files under a DFS root.
    /// 分布式文件系统模式：在 DFS 根目录下按 hash bucket 存储 key 文件。
    Distributed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketEvictionPolicy {
    None,
    Fifo,
    Lru,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketBackendConfig {
    pub bucket_size_limit: u64,
    pub bucket_keys_limit: usize,
    pub eviction_policy: BucketEvictionPolicy,
    pub max_total_size: u64,
}

impl Default for BucketBackendConfig {
    fn default() -> Self {
        Self {
            bucket_size_limit: 256 * 1024 * 1024,
            bucket_keys_limit: 500,
            eviction_policy: BucketEvictionPolicy::Fifo,
            max_total_size: 0,
        }
    }
}

impl BucketBackendConfig {
    pub fn from_environment() -> Self {
        let mut config = Self::default();
        if let Some(parsed) = parse_env_u64(&[
            "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES",
            "MOONCAKE_BUCKET_SIZE_LIMIT",
        ]) {
            config.bucket_size_limit = parsed;
        }
        if let Some(parsed) = parse_env_usize(&[
            "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT",
            "MOONCAKE_BUCKET_KEYS_LIMIT",
        ]) {
            config.bucket_keys_limit = parsed;
        }
        if let Some(value) = get_first_env(&[
            "MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY",
            "MOONCAKE_BUCKET_EVICTION_POLICY",
        ]) {
            config.eviction_policy = match value.to_ascii_lowercase().as_str() {
                "fifo" => BucketEvictionPolicy::Fifo,
                "lru" => BucketEvictionPolicy::Lru,
                _ => BucketEvictionPolicy::None,
            };
        }
        if let Some(parsed) = parse_env_u64(&[
            "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE",
            "MOONCAKE_BUCKET_MAX_TOTAL_SIZE",
        ]) {
            config.max_total_size = parsed;
        }
        config
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.bucket_size_limit == 0 {
            return Err("BucketBackendConfig: bucket_size_limit must be > 0".into());
        }
        if self.bucket_keys_limit == 0 {
            return Err("BucketBackendConfig: bucket_keys_limit must be > 0".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct BucketFile {
    pub(super) bucket_id: u64,
    pub(super) created_at_ms: i64,
    pub(super) last_access_ns: i64,
    pub(super) entries: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct OffsetIndexEntry {
    pub(super) offset: u64,
    pub(super) len: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct OffsetAllocatorIndex {
    pub(super) entries: HashMap<String, OffsetIndexEntry>,
    pub(super) next_offset: u64,
}

/// Configuration for the distributed storage backend.
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
    /// Load config from the same environment variables used by the C++ backend.
    pub fn from_environment() -> Self {
        let mut config = Self::default();
        if let Ok(root) = std::env::var("MOONCAKE_DISTRIBUTED_ROOT_DIR") {
            config.fsdir = PathBuf::from(root);
        }
        if !config.fsdir.is_absolute() {
            config.fsdir = std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(&config.fsdir);
        }
        if let Ok(adapter) = std::env::var("MOONCAKE_DISTRIBUTED_FS_TYPE") {
            config.fs_adapter_type = adapter;
        }
        if let Ok(enabled) = std::env::var("MOONCAKE_DISTRIBUTED_HEALTH_CHECK") {
            config.enable_health_check = parse_bool_env(&enabled);
        }
        if let Ok(count) = std::env::var("MOONCAKE_DISTRIBUTED_HASH_BUCKET_COUNT") {
            if let Ok(count) = count.parse::<usize>() {
                config.hash_bucket_count = count;
            }
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

    pub fn with_hash_bucket_count(mut self, hash_bucket_count: usize) -> Self {
        self.hash_bucket_count = hash_bucket_count;
        self
    }

    pub fn with_health_check(mut self, enable_health_check: bool) -> Self {
        self.enable_health_check = enable_health_check;
        self
    }

    pub fn with_fs_adapter_type(mut self, fs_adapter_type: impl Into<String>) -> Self {
        self.fs_adapter_type = fs_adapter_type.into();
        self
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.fsdir.as_os_str().is_empty() {
            return Err("DistributedStorageConfig: fsdir is empty".into());
        }
        if !self.fsdir.is_absolute() {
            return Err(format!(
                "DistributedStorageConfig: fsdir must be absolute: {}",
                self.fsdir.display()
            )
            .into());
        }
        if !matches!(
            self.fs_adapter_type.as_str(),
            "hf3fs" | "posix" | "local" | "local-disk"
        ) {
            return Err(format!(
                "DistributedStorageConfig: unsupported fs_adapter_type: {}",
                self.fs_adapter_type
            )
            .into());
        }
        if self.hash_bucket_count == 0 {
            return Err("DistributedStorageConfig: hash_bucket_count must > 0".into());
        }
        Ok(())
    }
}

fn get_first_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| std::env::var(key).ok())
}

fn parse_env_u64(keys: &[&str]) -> Option<u64> {
    get_first_env(keys).and_then(|value| value.parse::<u64>().ok())
}

fn parse_env_usize(keys: &[&str]) -> Option<usize> {
    get_first_env(keys).and_then(|value| value.parse::<usize>().ok())
}

fn parse_bool_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const BUCKET_ENV_KEYS: &[&str] = &[
        "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES",
        "MOONCAKE_BUCKET_SIZE_LIMIT",
        "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT",
        "MOONCAKE_BUCKET_KEYS_LIMIT",
        "MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY",
        "MOONCAKE_BUCKET_EVICTION_POLICY",
        "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE",
        "MOONCAKE_BUCKET_MAX_TOTAL_SIZE",
    ];

    fn with_clean_bucket_env<T>(f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap();
        let saved: Vec<(&str, Option<String>)> = BUCKET_ENV_KEYS
            .iter()
            .map(|key| (*key, std::env::var(key).ok()))
            .collect();
        for key in BUCKET_ENV_KEYS {
            std::env::remove_var(key);
        }
        let result = f();
        for key in BUCKET_ENV_KEYS {
            std::env::remove_var(key);
        }
        for (key, value) in saved {
            if let Some(value) = value {
                std::env::set_var(key, value);
            }
        }
        result
    }

    #[test]
    fn bucket_config_defaults_to_fifo_eviction() {
        with_clean_bucket_env(|| {
            let config = BucketBackendConfig::from_environment();

            assert_eq!(config.eviction_policy, BucketEvictionPolicy::Fifo);
            assert_eq!(config.bucket_keys_limit, 500);
            assert_eq!(config.bucket_size_limit, 256 * 1024 * 1024);
            assert_eq!(config.max_total_size, 0);
        });
    }

    #[test]
    fn bucket_config_prefers_offload_env_names() {
        with_clean_bucket_env(|| {
            std::env::set_var("MOONCAKE_BUCKET_EVICTION_POLICY", "none");
            std::env::set_var("MOONCAKE_BUCKET_MAX_TOTAL_SIZE", "10");
            std::env::set_var("MOONCAKE_BUCKET_SIZE_LIMIT", "20");
            std::env::set_var("MOONCAKE_BUCKET_KEYS_LIMIT", "30");
            std::env::set_var("MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY", "lru");
            std::env::set_var("MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE", "40");
            std::env::set_var("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "50");
            std::env::set_var("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "60");

            let config = BucketBackendConfig::from_environment();

            assert_eq!(config.eviction_policy, BucketEvictionPolicy::Lru);
            assert_eq!(config.max_total_size, 40);
            assert_eq!(config.bucket_size_limit, 50);
            assert_eq!(config.bucket_keys_limit, 60);
        });
    }

    #[test]
    fn bucket_config_keeps_legacy_env_fallbacks() {
        with_clean_bucket_env(|| {
            std::env::set_var("MOONCAKE_BUCKET_EVICTION_POLICY", "none");
            std::env::set_var("MOONCAKE_BUCKET_MAX_TOTAL_SIZE", "70");
            std::env::set_var("MOONCAKE_BUCKET_SIZE_LIMIT", "80");
            std::env::set_var("MOONCAKE_BUCKET_KEYS_LIMIT", "90");

            let config = BucketBackendConfig::from_environment();

            assert_eq!(config.eviction_policy, BucketEvictionPolicy::None);
            assert_eq!(config.max_total_size, 70);
            assert_eq!(config.bucket_size_limit, 80);
            assert_eq!(config.bucket_keys_limit, 90);
        });
    }
}

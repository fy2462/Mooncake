use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetEvictionPolicy {
    None,
    Fifo,
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
    use super::{OffsetAllocatorConfig, OffsetEvictionPolicy};
    use std::collections::HashMap;

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

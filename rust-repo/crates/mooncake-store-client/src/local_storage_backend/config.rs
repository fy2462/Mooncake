use std::path::PathBuf;

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

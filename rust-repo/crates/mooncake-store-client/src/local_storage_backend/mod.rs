pub mod config;

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::UNIX_EPOCH;

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use parking_lot::RwLock;

pub use config::LocalStorageConfig;

const MIN_FREE_SPACE_BYTES: u64 = 256 * 1024 * 1024;

type AvailableSpaceProbe = dyn Fn(&Path) -> std::io::Result<u64> + Send + Sync;

pub(crate) fn local_storage_key(tenant_id: &str, key: &str) -> String {
    let tenant_id = if tenant_id.is_empty() {
        "default"
    } else {
        tenant_id
    };
    format!("v1:{}:{tenant_id}{key}", tenant_id.len())
}

pub(crate) fn parse_local_storage_key(storage_key: &str) -> (&str, &str) {
    let Some(encoded) = storage_key.strip_prefix("v1:") else {
        return ("default", storage_key);
    };
    let Some((tenant_len, payload)) = encoded.split_once(':') else {
        return ("default", storage_key);
    };
    let Ok(tenant_len) = tenant_len.parse::<usize>() else {
        return ("default", storage_key);
    };
    if tenant_len > payload.len() || !payload.is_char_boundary(tenant_len) {
        return ("default", storage_key);
    }
    payload.split_at(tenant_len)
}

#[derive(Clone, Debug)]
struct FileRecord {
    storage_key: String,
    relative_path: String,
    size: u64,
}

#[derive(Debug, Default)]
pub(crate) struct PendingLocalEviction {
    records: Vec<FileRecord>,
}

impl PendingLocalEviction {
    pub(crate) fn keys(&self) -> Vec<String> {
        self.records
            .iter()
            .map(|record| record.storage_key.clone())
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Local disk storage backend for FilePerKey offload/promotion data.
///
/// Each key-value pair is stored as a single file under a hash-partitioned
/// 2-level directory tree. Optional FIFO eviction enforces a space quota.
///
/// C++ equivalent: `StorageBackendAdaptor` (FilePerKey) + `StorageBackend`
/// (file lifecycle + eviction) in `storage_backend.cpp`.
pub struct LocalStorageBackend {
    config: LocalStorageConfig,

    /// FIFO write queue with both the logical key and relative file path.
    /// Entries are lazily cleaned — stale entries (already deleted) are
    /// skipped during eviction and compacted periodically.
    write_queue: RwLock<VecDeque<FileRecord>>,

    /// Set of currently-valid paths in `write_queue`, for O(1) staleness check.
    queue_set: RwLock<HashMap<String, ()>>,

    /// Total quota in bytes (constant after init).
    total_space: RwLock<u64>,

    /// Currently used bytes on disk.
    used_space: RwLock<u64>,

    /// Whether `init()` has been called successfully.
    initialized: AtomicBool,

    available_space_probe: Box<AvailableSpaceProbe>,
}

impl LocalStorageBackend {
    /// Create a new backend. Does NOT touch the filesystem.
    /// Call [`init`](Self::init) before using any other method.
    pub fn new(config: LocalStorageConfig) -> Self {
        Self {
            config,
            write_queue: RwLock::new(VecDeque::new()),
            queue_set: RwLock::new(HashMap::new()),
            total_space: RwLock::new(0),
            used_space: RwLock::new(0),
            initialized: AtomicBool::new(false),
            available_space_probe: Box::new(|path| fs2::available_space(path)),
        }
    }

    #[cfg(test)]
    fn new_with_available_space_probe(
        config: LocalStorageConfig,
        available_space_probe: Box<AvailableSpaceProbe>,
    ) -> Self {
        Self {
            config,
            write_queue: RwLock::new(VecDeque::new()),
            queue_set: RwLock::new(HashMap::new()),
            total_space: RwLock::new(0),
            used_space: RwLock::new(0),
            initialized: AtomicBool::new(false),
            available_space_probe,
        }
    }

    // ------------------------------------------------------------------
    // Path helpers
    // ------------------------------------------------------------------

    /// Sanitize a key for use as a filename: replace '/' with '_'.
    /// C++ equivalent: `SanitizeKey()` in utils.cpp.
    fn sanitize_key(key: &str) -> String {
        key.replace('/', "_")
    }

    /// Compute the on-disk path for a key using hash-based 2-level directory
    /// spreading (256 leaf directories).
    ///
    /// ```text
    /// dir1 = 'a' + (hash & 0x0F)
    /// dir2 = 'a' + ((hash >> 4) & 0x0F)
    /// full  = <root>/<fsdir>/dir1/dir2/<sanitized_key>
    /// ```
    ///
    /// C++ equivalent: `ResolvePathFromKey()` in utils.cpp.
    pub fn key_path(&self, key: &str) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let dir1 = (b'a' + (hash & 0x0F) as u8) as char;
        let dir2 = (b'a' + ((hash >> 4) & 0x0F) as u8) as char;
        let safe_key = Self::sanitize_key(key);
        self.config
            .root_dir
            .join(&self.config.fsdir)
            .join(dir1.to_string())
            .join(dir2.to_string())
            .join(safe_key)
    }

    /// Return the data directory path (`<root>/<fsdir>`).
    fn data_dir(&self) -> PathBuf {
        self.config.root_dir.join(&self.config.fsdir)
    }

    // ------------------------------------------------------------------
    // Initialization
    // ------------------------------------------------------------------

    /// Initialize the backend: create directories, scan existing files,
    /// rebuild the FIFO queue, and compute space accounting.
    ///
    /// Must be called once before any other operation. Idempotent — subsequent
    /// calls are no-ops.
    ///
    /// C++ equivalent: `StorageBackend::Init(quota_bytes)` +
    /// `StorageBackendAdaptor::Init()`.
    pub fn init(&self) -> StoreResult<()> {
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        let data_dir = self.data_dir();
        self.clean_storage_path()?;
        std::fs::create_dir_all(&data_dir)?;

        // Determine quota.
        let total = if self.config.quota_bytes > 0 {
            self.config.quota_bytes
        } else {
            // Auto-detect: 90% of filesystem capacity.
            let available = fs2::available_space(&data_dir).unwrap_or(0);
            let total = fs2::total_space(&data_dir).unwrap_or(0);
            let capacity = std::cmp::max(available, total);
            (capacity as f64 * 0.9) as u64
        };
        *self.total_space.write() = total;

        // Scan existing files, sorted by creation time (oldest first).
        let mut entries: Vec<(String, u64, std::time::SystemTime)> = Vec::new();
        self.scan_files(&data_dir, &mut entries)?;

        entries.sort_by_key(|(_, _, ct)| *ct);

        let mut used = 0u64;
        let mut queue = self.write_queue.write();
        let mut set = self.queue_set.write();

        for (rel_path, size, _ct) in &entries {
            if used.saturating_add(*size) > total && self.config.enable_eviction {
                // Over quota — delete excess files from disk.
                let abs_path = data_dir.join(rel_path);
                let _ = std::fs::remove_file(&abs_path);
            } else {
                used += size;
                set.insert(rel_path.clone(), ());
                queue.push_back(FileRecord {
                    storage_key: Path::new(rel_path)
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|| rel_path.clone()),
                    relative_path: rel_path.clone(),
                    size: *size,
                });
            }
        }

        *self.used_space.write() = used;
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Recursively scan all files under `dir` and collect (relative_path, size, creation_time).
    fn scan_files(
        &self,
        dir: &Path,
        out: &mut Vec<(String, u64, std::time::SystemTime)>,
    ) -> StoreResult<()> {
        if !dir.exists() {
            return Ok(());
        }
        let data_dir = self.data_dir();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                self.scan_files(&path, out)?;
            } else if path.is_file() {
                let metadata = entry.metadata()?;
                let size = metadata.len();
                let created = metadata.created().unwrap_or(UNIX_EPOCH);
                let rel_path = path
                    .strip_prefix(&data_dir)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();
                out.push((rel_path, size, created));
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Core I/O
    // ------------------------------------------------------------------

    /// Write a key-value pair to disk. Creates parent directories as needed.
    /// Returns a list of keys evicted to make space (empty if no eviction).
    ///
    /// C++ equivalent: `StorageBackendAdaptor::BatchOffload` (single-key path).
    pub fn write_object(&self, key: &str, data: &[u8]) -> StoreResult<Vec<String>> {
        self.ensure_init()?;
        let pending = self.prepare_write(data.len() as u64)?;
        let evicted = pending.keys();
        self.commit_write(key, data, pending)?;
        Ok(evicted)
    }

    pub(crate) fn prepare_write(&self, required: u64) -> StoreResult<PendingLocalEviction> {
        self.ensure_init()?;
        if !self.config.enable_eviction {
            return Ok(PendingLocalEviction::default());
        }
        self.prepare_space_eviction(required)
    }

    pub(crate) fn commit_write(
        &self,
        key: &str,
        data: &[u8],
        pending: PendingLocalEviction,
    ) -> StoreResult<()> {
        self.ensure_init()?;
        self.commit_eviction(pending)?;

        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let previous_size = std::fs::metadata(&path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        std::fs::write(&path, data)?;

        if self.config.enable_eviction {
            let rel_path = path
                .strip_prefix(self.data_dir())
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            self.remove_from_write_queue(&rel_path);
            self.add_to_write_queue(key, &rel_path, data.len() as u64);
            let mut used = self.used_space.write();
            *used = used
                .saturating_sub(previous_size)
                .saturating_add(data.len() as u64);
        }
        Ok(())
    }

    pub(crate) fn rollback_eviction(&self, pending: PendingLocalEviction) {
        if pending.is_empty() {
            return;
        }
        let mut queue = self.write_queue.write();
        for record in pending.records.into_iter().rev() {
            queue.push_front(record);
        }
    }

    pub(crate) fn prepare_watermark_eviction(
        &self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<PendingLocalEviction> {
        self.ensure_init()?;
        validate_watermark_ratios(high_watermark_ratio, low_watermark_ratio)?;
        if !self.config.enable_eviction {
            return Ok(PendingLocalEviction::default());
        }

        let total = *self.total_space.read();
        let used = *self.used_space.read();
        if total == 0 || used <= (total as f64 * high_watermark_ratio) as u64 {
            return Ok(PendingLocalEviction::default());
        }

        let target = (total as f64 * low_watermark_ratio) as u64;
        self.prepare_eviction_bytes(used.saturating_sub(target))
    }

    pub(crate) fn commit_eviction(&self, pending: PendingLocalEviction) -> StoreResult<()> {
        let mut first_error = None;
        for record in pending.records {
            let path = self.data_dir().join(&record.relative_path);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
            self.remove_from_write_queue(&record.relative_path);
            let mut used = self.used_space.write();
            *used = used.saturating_sub(record.size);
        }
        if let Some(error) = first_error {
            return Err(error.into());
        }
        Ok(())
    }

    /// Read the value for a key from disk.
    ///
    /// C++ equivalent: `StorageBackendAdaptor::BatchLoad` (single-key path).
    pub fn read_object(&self, key: &str) -> StoreResult<Vec<u8>> {
        self.ensure_init()?;

        let path = self.key_path(key);
        if !path.exists() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }
        Ok(std::fs::read(&path)?)
    }

    /// Delete a key's file from disk.
    ///
    /// C++ equivalent: `StorageBackend::RemoveFile`.
    pub fn delete_object(&self, key: &str) -> StoreResult<()> {
        self.ensure_init()?;

        let path = self.key_path(key);
        if !path.exists() {
            return Ok(());
        }

        let size = std::fs::metadata(&path)?.len();
        std::fs::remove_file(&path)?;

        if self.config.enable_eviction {
            let rel_path = path
                .strip_prefix(self.data_dir())
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            self.remove_from_write_queue(&rel_path);
            let mut used = self.used_space.write();
            *used = used.saturating_sub(size);
        }

        Ok(())
    }

    /// Check whether a key exists on disk.
    ///
    /// C++ equivalent: `StorageBackendAdaptor::IsExist`.
    pub fn exists(&self, key: &str) -> bool {
        self.key_path(key).exists()
    }

    /// Return `(used_bytes, total_quota)`.
    pub fn space_usage(&self) -> (u64, u64) {
        (*self.used_space.read(), *self.total_space.read())
    }

    // ------------------------------------------------------------------
    // Bulk operations
    // ------------------------------------------------------------------

    /// Scan all keys and return `(key, size_bytes)` pairs.
    ///
    /// C++ equivalent: `StorageBackendAdaptor::ScanMeta`.
    pub fn scan_meta(&self) -> StoreResult<Vec<(String, u64)>> {
        self.ensure_init()?;

        let data_dir = self.data_dir();
        if !data_dir.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut file_entries = Vec::new();
        self.scan_files(&data_dir, &mut file_entries)?;

        for (rel_path, size, _ct) in &file_entries {
            // The relative path encodes the key: <dir1>/<dir2>/<safe_key>
            // Extract the filename (safe_key) as the stored key.
            let p = Path::new(rel_path);
            if let Some(name) = p.file_name() {
                results.push((name.to_string_lossy().to_string(), *size));
            }
        }

        Ok(results)
    }

    /// Remove all files under the data directory.
    /// Returns the count of removed files.
    ///
    /// C++ equivalent: `StorageBackend::RemoveAll`.
    pub fn remove_all(&self) -> StoreResult<usize> {
        self.ensure_init()?;

        let data_dir = self.data_dir();
        let count = self.count_files(&data_dir)?;
        if data_dir.exists() {
            std::fs::remove_dir_all(&data_dir)?;
            std::fs::create_dir_all(&data_dir)?;
        }

        if self.config.enable_eviction {
            self.write_queue.write().clear();
            self.queue_set.write().clear();
            *self.used_space.write() = 0;
        }

        Ok(count)
    }

    /// Wipe every file and subdirectory under the data directory, keeping the
    /// directory itself. This is used by the P2P SSD/offload path, where local
    /// disk data is an ephemeral cache rather than durable FileStorage state.
    ///
    /// C++ equivalent: `StorageBackendInterface::CleanStoragePath()`, invoked
    /// by `StorageTier` on startup and destruction.
    pub fn clean_storage_path(&self) -> StoreResult<usize> {
        let data_dir = self.data_dir();
        self.validate_clean_storage_path(&data_dir)?;

        if !data_dir.exists() {
            return Ok(0);
        }
        if !data_dir.is_dir() {
            tracing::warn!(
                "LocalStorageBackend clean_storage_path skipped non-directory path: {:?}",
                data_dir
            );
            return Ok(0);
        }

        let mut removed = 0usize;
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            std::fs::remove_dir_all(entry.path()).or_else(|err| {
                if err.kind() == std::io::ErrorKind::NotADirectory {
                    std::fs::remove_file(entry.path())
                } else {
                    Err(err)
                }
            })?;
            removed += 1;
        }

        self.write_queue.write().clear();
        self.queue_set.write().clear();
        *self.used_space.write() = 0;

        Ok(removed)
    }

    /// Remove keys whose sanitized filenames match the given regex pattern.
    /// Returns the count of removed files.
    ///
    /// C++ equivalent: `StorageBackend::RemoveByRegex`.
    pub fn remove_by_regex(&self, pattern: &str) -> StoreResult<usize> {
        self.ensure_init()?;

        let re = regex::Regex::new(pattern)
            .map_err(|e| StoreError::Internal(format!("invalid regex: {e}")))?;

        let data_dir = self.data_dir();
        let mut removed = 0usize;
        let mut file_entries = Vec::new();
        self.scan_files(&data_dir, &mut file_entries)?;

        for (rel_path, size, _ct) in &file_entries {
            let p = Path::new(rel_path);
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if re.is_match(name) {
                    let abs_path = data_dir.join(rel_path);
                    std::fs::remove_file(&abs_path)?;
                    if self.config.enable_eviction {
                        self.remove_from_write_queue(rel_path);
                        let mut used = self.used_space.write();
                        *used = used.saturating_sub(*size);
                    }
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }

    // ------------------------------------------------------------------
    // Eviction (FIFO)
    // ------------------------------------------------------------------

    /// Ensure at least `required` bytes are free. Evicts oldest files (FIFO)
    /// if the quota would be exceeded. Returns the list of evicted keys.
    ///
    /// C++ equivalent: `StorageBackend::EnsureDiskSpace`.
    fn prepare_space_eviction(&self, required: u64) -> StoreResult<PendingLocalEviction> {
        let total = *self.total_space.read();
        let used = *self.used_space.read();
        let quota_deficit = used.saturating_add(required).saturating_sub(total);
        let disk_deficit = match (self.available_space_probe)(&self.data_dir()) {
            Ok(available) => required
                .saturating_add(MIN_FREE_SPACE_BYTES)
                .saturating_sub(available),
            Err(error) => {
                tracing::warn!(
                    "failed to query available disk space for {:?}: {error}",
                    self.data_dir()
                );
                0
            }
        };
        self.prepare_eviction_bytes(quota_deficit.max(disk_deficit))
    }

    fn prepare_eviction_bytes(&self, bytes_to_free: u64) -> StoreResult<PendingLocalEviction> {
        if bytes_to_free == 0 {
            return Ok(PendingLocalEviction::default());
        }

        let mut pending = PendingLocalEviction::default();
        let mut selected_bytes = 0u64;
        while selected_bytes < bytes_to_free {
            match self.evict_one() {
                Some(record) => {
                    selected_bytes = selected_bytes.saturating_add(record.size);
                    pending.records.push(record);
                }
                None => {
                    self.rollback_eviction(pending);
                    return Err(StoreError::Internal(
                        "disk full: cannot satisfy quota after eviction".to_string(),
                    ));
                }
            }
        }

        // Compact if too many stale entries.
        let stale_ratio = {
            let queue = self.write_queue.read();
            let set = self.queue_set.read();
            if queue.is_empty() {
                0.0
            } else {
                1.0 - (set.len() as f64 / queue.len() as f64)
            }
        };
        if stale_ratio > 0.5 {
            self.compact_write_queue();
        }

        Ok(pending)
    }

    /// Evict the oldest valid file from the FIFO queue.
    /// Returns `None` if the queue is exhausted.
    fn evict_one(&self) -> Option<FileRecord> {
        let mut queue = self.write_queue.write();
        let set = self.queue_set.read();

        while let Some(record) = queue.pop_front() {
            if set.contains_key(&record.relative_path) {
                return Some(record);
            }
            // Stale entry — already deleted, skip.
        }
        None
    }

    fn add_to_write_queue(&self, storage_key: &str, file_path: &str, size: u64) {
        self.write_queue.write().push_back(FileRecord {
            storage_key: storage_key.to_string(),
            relative_path: file_path.to_string(),
            size,
        });
        self.queue_set.write().insert(file_path.to_string(), ());
    }

    fn remove_from_write_queue(&self, file_path: &str) {
        self.queue_set.write().remove(file_path);
    }

    /// Compact stale entries from the write queue.
    fn compact_write_queue(&self) {
        let set = self.queue_set.read();
        let mut queue = self.write_queue.write();
        queue.retain(|record| set.contains_key(&record.relative_path));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn ensure_init(&self) -> StoreResult<()> {
        if !self.initialized.load(Ordering::SeqCst) {
            return Err(StoreError::Internal(
                "LocalStorageBackend not initialized — call init() first".to_string(),
            ));
        }
        Ok(())
    }

    fn count_files(&self, dir: &Path) -> StoreResult<usize> {
        if !dir.exists() {
            return Ok(0);
        }
        let mut count = 0usize;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                count += self.count_files(&path)?;
            } else if path.is_file() {
                count += 1;
            }
        }
        Ok(count)
    }

    fn validate_clean_storage_path(&self, data_dir: &Path) -> StoreResult<()> {
        if self.config.fsdir.trim().is_empty() {
            return Err(StoreError::InvalidParams(
                "LocalStorageBackend clean_storage_path requires a non-empty fsdir".to_string(),
            ));
        }
        if !data_dir.is_absolute() || data_dir.parent().is_none() {
            return Err(StoreError::InvalidParams(format!(
                "LocalStorageBackend refuses to clean unsafe storage path: {}",
                data_dir.display()
            )));
        }
        Ok(())
    }
}

fn validate_watermark_ratios(high: f64, low: f64) -> StoreResult<()> {
    if !high.is_finite()
        || !low.is_finite()
        || !(0.0..=1.0).contains(&high)
        || high == 0.0
        || !(0.0..=1.0).contains(&low)
        || low == 0.0
        || low >= high
    {
        return Err(StoreError::InvalidParams(
            "disk eviction watermarks must satisfy 0 < low < high <= 1".to_string(),
        ));
    }
    Ok(())
}

impl Drop for LocalStorageBackend {
    fn drop(&mut self) {
        if let Err(err) = self.clean_storage_path() {
            tracing::warn!("LocalStorageBackend clean_storage_path on drop failed: {err}");
        }
    }
}

// Safety: all interior mutability is behind parking_lot::RwLock.
unsafe impl Send for LocalStorageBackend {}
unsafe impl Sync for LocalStorageBackend {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn backend_with_available_space_sequence(
        quota: u64,
        values: Vec<u64>,
    ) -> (LocalStorageBackend, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = LocalStorageConfig {
            root_dir: tmp.path().to_path_buf(),
            fsdir: "test_data".to_string(),
            enable_eviction: true,
            quota_bytes: quota,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = LocalStorageBackend::new_with_available_space_probe(
            config,
            Box::new(move |_| {
                let index = calls.fetch_add(1, Ordering::SeqCst);
                Ok(*values
                    .get(index)
                    .unwrap_or_else(|| values.last().expect("space sequence is empty")))
            }),
        );
        backend.init().unwrap();
        (backend, tmp)
    }

    #[test]
    fn actual_disk_space_shortage_triggers_fifo_eviction() {
        let enough_space = MIN_FREE_SPACE_BYTES + 1024;
        let (backend, _tmp) = backend_with_available_space_sequence(
            500,
            vec![enough_space, MIN_FREE_SPACE_BYTES + 49],
        );

        backend.write_object("old", &[0u8; 100]).unwrap();
        let evicted = backend.write_object("new", &[0u8; 50]).unwrap();

        assert_eq!(evicted, vec!["old".to_string()]);
        assert!(!backend.exists("old"));
        assert!(backend.exists("new"));
    }

    #[test]
    fn tenant_storage_key_round_trips_without_path_delimiters() {
        let storage_key = local_storage_key("租户/a", "模型/key:1");
        assert!(!storage_key.contains('\0'));
        assert_eq!(
            parse_local_storage_key(&storage_key),
            ("租户/a", "模型/key:1")
        );

        let default_key = local_storage_key("", "path/to/key");
        assert_eq!(
            parse_local_storage_key(&default_key),
            ("default", "path/to/key")
        );
    }

    #[test]
    fn watermark_eviction_can_be_rolled_back_without_deleting_files() {
        let (backend, _tmp) = backend_with_available_space_sequence(100, vec![u64::MAX]);
        backend.write_object("tenant/key-a", &[0u8; 60]).unwrap();
        backend.write_object("tenant/key-b", &[0u8; 20]).unwrap();

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(pending.keys(), vec!["tenant/key-a"]);
        assert!(backend.exists("tenant/key-a"));
        assert_eq!(backend.space_usage(), (80, 100));

        backend.rollback_eviction(pending);
        let pending_again = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        assert_eq!(pending_again.keys(), vec!["tenant/key-a"]);
    }

    #[test]
    fn watermark_eviction_commit_deletes_fifo_victims() {
        let (backend, _tmp) = backend_with_available_space_sequence(100, vec![u64::MAX]);
        backend.write_object("tenant/key-a", &[0u8; 60]).unwrap();
        backend.write_object("tenant/key-b", &[0u8; 20]).unwrap();

        let pending = backend.prepare_watermark_eviction(0.70, 0.40).unwrap();
        backend.commit_eviction(pending).unwrap();

        assert!(!backend.exists("tenant/key-a"));
        assert!(backend.exists("tenant/key-b"));
        assert_eq!(backend.space_usage(), (20, 100));
    }
}

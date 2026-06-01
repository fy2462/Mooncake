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

/// Local disk storage backend for FilePerKey offload/promotion data.
///
/// Each key-value pair is stored as a single file under a hash-partitioned
/// 2-level directory tree. Optional FIFO eviction enforces a space quota.
///
/// C++ equivalent: `StorageBackendAdaptor` (FilePerKey) + `StorageBackend`
/// (file lifecycle + eviction) in `storage_backend.cpp`.
pub struct LocalStorageBackend {
    config: LocalStorageConfig,

    /// FIFO write queue: (path_relative_to_fsdir, file_size_bytes).
    /// Entries are lazily cleaned — stale entries (already deleted) are
    /// skipped during eviction and compacted periodically.
    write_queue: RwLock<VecDeque<(String, u64)>>,

    /// Set of currently-valid paths in `write_queue`, for O(1) staleness check.
    queue_set: RwLock<HashMap<String, ()>>,

    /// Total quota in bytes (constant after init).
    total_space: RwLock<u64>,

    /// Currently used bytes on disk.
    used_space: RwLock<u64>,

    /// Whether `init()` has been called successfully.
    initialized: AtomicBool,
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
        if self.initialized.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let data_dir = self.data_dir();
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
            if used + size > total && self.config.enable_eviction {
                // Over quota — delete excess files from disk.
                let abs_path = data_dir.join(rel_path);
                let _ = std::fs::remove_file(&abs_path);
            } else {
                used += size;
                set.insert(rel_path.clone(), ());
                queue.push_back((rel_path.clone(), *size));
            }
        }

        *self.used_space.write() = used;
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

        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let evicted = if self.config.enable_eviction {
            self.ensure_disk_space(data.len() as u64)?
        } else {
            Vec::new()
        };

        std::fs::write(&path, data)?;

        // Track in FIFO queue for eviction.
        if self.config.enable_eviction {
            let rel_path = path
                .strip_prefix(self.data_dir())
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            self.add_to_write_queue(&rel_path, data.len() as u64);
            *self.used_space.write() += data.len() as u64;
        }

        Ok(evicted)
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
    fn ensure_disk_space(&self, required: u64) -> StoreResult<Vec<String>> {
        let mut evicted = Vec::new();
        let total = *self.total_space.read();

        loop {
            let used = *self.used_space.read();
            if used + required <= total {
                break;
            }
            match self.evict_one() {
                Some((file_path, size)) => {
                    // Extract key from path: <dir1>/<dir2>/<sanitized_key>
                    let key = Path::new(&file_path)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| file_path.clone());
                    let _ = std::fs::remove_file(self.data_dir().join(&file_path));
                    let mut used = self.used_space.write();
                    *used = used.saturating_sub(size);
                    self.remove_from_write_queue(&file_path);
                    evicted.push(key);
                }
                None => {
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

        Ok(evicted)
    }

    /// Evict the oldest valid file from the FIFO queue.
    /// Returns `None` if the queue is exhausted.
    fn evict_one(&self) -> Option<(String, u64)> {
        let mut queue = self.write_queue.write();
        let set = self.queue_set.read();

        while let Some((path, size)) = queue.pop_front() {
            if set.contains_key(&path) {
                return Some((path, size));
            }
            // Stale entry — already deleted, skip.
        }
        None
    }

    fn add_to_write_queue(&self, file_path: &str, size: u64) {
        self.write_queue
            .write()
            .push_back((file_path.to_string(), size));
        self.queue_set.write().insert(file_path.to_string(), ());
    }

    fn remove_from_write_queue(&self, file_path: &str) {
        self.queue_set.write().remove(file_path);
    }

    /// Compact stale entries from the write queue.
    fn compact_write_queue(&self) {
        let set = self.queue_set.read();
        let mut queue = self.write_queue.write();
        queue.retain(|(path, _)| set.contains_key(path));
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
}

// Safety: all interior mutability is behind parking_lot::RwLock.
unsafe impl Send for LocalStorageBackend {}
unsafe impl Sync for LocalStorageBackend {}

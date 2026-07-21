// Provides snapshot persistence and per-key file storage with support for
// different storage backends: local disk, HF3FS (3FS distributed filesystem),
// and FilePerKey (offload storage).
// 提供快照持久化和按 key 的文件存储，支持不同的存储后端：
// 本地磁盘、HF3FS（3FS 分布式文件系统）和 FilePerKey（offload 存储）。
//
// Architecture / 架构:
// ┌──────────────────────────────────────────────────────┐
// │  StorageBackend                                       │
// │  ┌──────────────┐  ┌────────────────────────────────┐ │
// │  │ Snapshot      │  │ FilePerKey                     │ │
// │  │ (msgpack/json)│  │ (key → file on disk)           │ │
// │  │ save/load     │  │ batch_offload / batch_load     │ │
// │  │ Segments,     │  │ remove / remove_by_regex       │ │
// │  │ Objects,      │  │ scan_meta                      │ │
// │  │ Tasks         │  │                                │ │
// │  └──────────────┘  └────────────────────────────────┘ │
// └──────────────────────────────────────────────────────┘
//
// Backend Types / 后端类型:
// - LocalDisk: plain local filesystem, no special handling needed.
//   LocalDisk：普通本地磁盘，无需额外注册。
// - Hf3fs: 3FS distributed filesystem, requires fd registration via hf3fs API.
//   Hf3fs：3FS 分布式文件系统，需要通过 hf3fs API 注册文件描述符。
// - FilePerKey: each key stored as a separate file (used for memory offloading).
//   FilePerKey：每个 key 独立文件存储（用于 offload 场景）。

use crate::hf3fs;
use crate::storage_distributed::{create_filesystem_adapter, FileSystemAdapter};
use chrono::Utc;
use dashmap::DashMap;
use mooncake_store_core::{TaskInfo, TaskStatus, TaskType};
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use xxhash_rust::xxh64::xxh64;

mod storage_backend_bucket;
mod storage_backend_config;
mod storage_backend_distributed;
mod storage_backend_file;
mod storage_backend_offset;
mod storage_backend_snapshot;

pub use storage_backend_config::{
    BucketBackendConfig, BucketEvictionPolicy, DistributedStorageConfig, StorageBackendType,
};
pub use storage_backend_snapshot::LocalDiskSnapshotEntry;

const MIN_FREE_SPACE_BYTES: u64 = 256 * 1024 * 1024;

type AvailableSpaceProbe = dyn Fn(&Path) -> std::io::Result<u64> + Send + Sync;

/// The main storage backend for snapshot persistence and key-based file operations.
/// 用于快照持久化和基于 key 的文件操作的主存储后端。
pub struct StorageBackend {
    backend_type: StorageBackendType,
    disk_dir: PathBuf,
    distributed_config: Option<DistributedStorageConfig>,
    distributed_adapter: Option<Box<dyn FileSystemAdapter>>,
    available_space_probe: Box<AvailableSpaceProbe>,
}

// =============================================================================
// StorageBackend Methods / StorageBackend 方法
// =============================================================================

impl StorageBackend {
    /// Create a new StorageBackend, ensuring the base directory exists.
    /// 创建新的 StorageBackend，确保基础目录存在。
    pub fn new(backend_type: StorageBackendType, disk_dir: &Path) -> Self {
        if backend_type == StorageBackendType::Distributed {
            let config = DistributedStorageConfig::from_environment().with_root(disk_dir);
            return Self::new_distributed(config).unwrap_or_else(|err| {
                tracing::warn!("Distributed storage init failed: {}", err);
                fs::create_dir_all(disk_dir).ok();
                Self {
                    backend_type,
                    disk_dir: disk_dir.to_path_buf(),
                    distributed_config: None,
                    distributed_adapter: None,
                    available_space_probe: Box::new(|path| fs2::available_space(path)),
                }
            });
        }

        fs::create_dir_all(disk_dir).ok();
        Self {
            backend_type,
            disk_dir: disk_dir.to_path_buf(),
            distributed_config: None,
            distributed_adapter: None,
            available_space_probe: Box::new(|path| fs2::available_space(path)),
        }
    }

    #[cfg(test)]
    fn new_with_available_space_probe(
        backend_type: StorageBackendType,
        disk_dir: &Path,
        available_space_probe: Box<AvailableSpaceProbe>,
    ) -> Self {
        fs::create_dir_all(disk_dir).ok();
        Self {
            backend_type,
            disk_dir: disk_dir.to_path_buf(),
            distributed_config: None,
            distributed_adapter: None,
            available_space_probe,
        }
    }

    /// Create a distributed backend with explicit C++-compatible configuration.
    pub fn new_distributed(
        config: DistributedStorageConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        config.validate()?;
        let adapter = create_filesystem_adapter(&config.fs_adapter_type)?;
        adapter.init(&config.fsdir)?;
        fs::create_dir_all(&config.fsdir)?;
        if config.enable_health_check {
            Self::run_distributed_health_check(adapter.as_ref(), &config.fsdir)?;
        }
        for bucket in 0..config.hash_bucket_count {
            fs::create_dir_all(config.fsdir.join(format!("{bucket:02x}")))?;
        }
        Ok(Self {
            backend_type: StorageBackendType::Distributed,
            disk_dir: config.fsdir.clone(),
            distributed_config: Some(config),
            distributed_adapter: Some(adapter),
            available_space_probe: Box::new(|path| fs2::available_space(path)),
        })
    }

    fn run_distributed_health_check(
        adapter: &dyn FileSystemAdapter,
        root: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let probe = root.join(format!(".mooncake_health_probe_{}", Uuid::new_v4()));
        let payload = b"health_check";
        adapter.write_file(&probe, payload)?;
        let read_back = adapter.read_file(&probe)?;
        let _ = adapter.delete_file(&probe);
        if read_back != payload {
            return Err("DFS health check failed: read back mismatch".into());
        }
        Ok(())
    }

    // =========================================================================
    // FilePerKey Methods — per-key file operations
    // FilePerKey 方法 —— 按 key 的文件操作
    // =========================================================================

    /// Directory for per-key files ("keys" subdirectory).
    /// 按 key 的文件目录（"keys" 子目录）。
    fn key_dir(&self) -> PathBuf {
        self.disk_dir.join("keys")
    }

    /// Compute the safe file path for a given key.
    /// 计算给定 key 的安全文件路径。
    ///
    /// Replaces '/' with '_' to prevent path traversal attacks.
    /// 编码 key：将 '/' 替换为 '_'，防止路径穿越攻击。
    fn key_path(&self, key: &str) -> PathBuf {
        if self.backend_type == StorageBackendType::Distributed {
            return self.distributed_object_path(key);
        }
        let safe_key = key.replace('/', "_");
        self.key_dir().join(safe_key)
    }

    fn distributed_config(&self) -> Option<&DistributedStorageConfig> {
        self.distributed_config.as_ref()
    }

    fn distributed_adapter(&self) -> Option<&dyn FileSystemAdapter> {
        self.distributed_adapter
            .as_ref()
            .map(|adapter| adapter.as_ref())
    }

    fn distributed_bucket_count(&self) -> usize {
        self.distributed_config()
            .map(|config| config.hash_bucket_count)
            .unwrap_or(256)
    }

    fn distributed_object_path(&self, key: &str) -> PathBuf {
        let bucket_count = self.distributed_bucket_count();
        let bucket = xxh64(key.as_bytes(), 0) % bucket_count as u64;
        self.disk_dir
            .join(format!("{bucket:02x}"))
            .join(Self::escape_distributed_filename(key))
    }

    pub fn escape_distributed_filename(key: &str) -> String {
        let mut result = String::with_capacity(key.len() + 16);
        for byte in key.bytes() {
            if byte == b'@'
                || byte == b':'
                || byte == b'/'
                || byte == b'\\'
                || byte == b'%'
                || !(0x20..=0x7e).contains(&byte)
            {
                result.push_str(&format!("%{byte:02x}"));
            } else {
                result.push(byte as char);
            }
        }
        result
    }

    pub fn unescape_distributed_filename(name: &str) -> String {
        let bytes = name.as_bytes();
        let mut result = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] == b'%'
                && index + 2 < bytes.len()
                && bytes[index + 1].is_ascii_hexdigit()
                && bytes[index + 2].is_ascii_hexdigit()
            {
                if let Ok(hex) = std::str::from_utf8(&bytes[index + 1..index + 3]) {
                    if let Ok(value) = u8::from_str_radix(hex, 16) {
                        result.push(value);
                        index += 3;
                        continue;
                    }
                }
            }
            result.push(bytes[index]);
            index += 1;
        }
        String::from_utf8_lossy(&result).into_owned()
    }

    /// Batch offload: write multiple key-value pairs to disk as individual files.
    /// 批量下沉：将多个 key 的二进制数据写入独立文件。
    ///
    /// Used for offloading hot data from memory to local disk.
    /// 用于将热数据从内存 offload 到本地磁盘。
    pub fn batch_offload(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::Bucket => return self.batch_offload_bucket(entries),
            StorageBackendType::OffsetAllocator => {
                return self.batch_offload_offset_allocator(entries)
            }
            _ => {}
        }
        let dir = self.key_dir();
        if self.backend_type != StorageBackendType::Distributed {
            std::fs::create_dir_all(&dir)?;
        }
        for (key, value) in entries {
            let path = self.key_path(key);
            if let Some(adapter) = self.distributed_adapter() {
                adapter.write_file(&path, value)?;
            } else {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut f = std::fs::File::create(&path)?;
                f.write_all(value)?;
            }
        }
        Ok(())
    }

    /// Batch load: read multiple keys from disk.
    /// 批量加载：从磁盘读取多个 key 的二进制数据。
    ///
    /// Missing keys are silently skipped (no error).
    /// 不存在的 key 直接跳过，不报错。
    pub fn batch_load(
        &self,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::Bucket => return self.batch_load_bucket(keys),
            StorageBackendType::OffsetAllocator => return self.batch_load_offset_allocator(keys),
            _ => {}
        }
        let mut results = Vec::new();
        for key in keys {
            let path = self.key_path(key);
            if let Some(adapter) = self.distributed_adapter() {
                if adapter.file_exists(&path)? {
                    results.push((key.clone(), adapter.read_file(&path)?));
                }
            } else if path.exists() {
                let mut f = std::fs::File::open(&path)?;
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                results.push((key.clone(), buf));
            }
        }
        Ok(results)
    }

    /// Remove specific keys from disk.
    /// 从磁盘删除特定 key。
    pub fn remove_keys(&self, keys: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::Bucket => return self.remove_keys_bucket(keys),
            StorageBackendType::OffsetAllocator => return self.remove_keys_offset_allocator(keys),
            _ => {}
        }
        for key in keys {
            let path = self.key_path(key);
            if let Some(adapter) = self.distributed_adapter() {
                if adapter.file_exists(&path)? {
                    adapter.delete_file(&path)?;
                }
            } else if path.exists() {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    /// Check if a key exists on disk.
    /// 检查 key 是否存在于磁盘上。
    pub fn is_exist(&self, key: &str) -> Result<bool, Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::Bucket => return self.is_exist_bucket(key),
            StorageBackendType::OffsetAllocator => return self.is_exist_offset_allocator(key),
            _ => {}
        }
        let path = self.key_path(key);
        if let Some(adapter) = self.distributed_adapter() {
            return adapter.file_exists(&path);
        }
        Ok(path.exists())
    }

    /// Remove all keys matching a regex pattern.
    /// 删除所有匹配正则表达式的 key。
    pub fn remove_by_regex(&self, pattern: &str) -> Result<usize, Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::Distributed => return self.remove_by_regex_distributed(pattern),
            StorageBackendType::Bucket => return self.remove_by_regex_bucket(pattern),
            StorageBackendType::OffsetAllocator => {
                return self.remove_by_regex_offset_allocator(pattern)
            }
            _ => {}
        }
        let dir = self.key_dir();
        if !dir.exists() {
            return Ok(0);
        }
        let re = regex::Regex::new(pattern).map_err(|e| format!("invalid regex: {e}"))?;
        let mut count = 0;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if re.is_match(&name) {
                std::fs::remove_file(entry.path())?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Remove all per-key files.
    /// 删除所有按 key 的文件。
    pub fn remove_all(&self) -> Result<usize, Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::Distributed => return self.remove_all_distributed(),
            StorageBackendType::Bucket => return self.remove_all_bucket(),
            StorageBackendType::OffsetAllocator => return self.remove_all_offset_allocator(),
            _ => {}
        }
        let dir = self.key_dir();
        if !dir.exists() {
            return Ok(0);
        }
        let count = std::fs::read_dir(&dir)?.count();
        std::fs::remove_dir_all(&dir)?;
        std::fs::create_dir_all(&dir)?;
        Ok(count)
    }

    /// Scan metadata for all per-key files: return (key, size) pairs.
    /// 扫描所有按 key 的文件的元数据：返回 (key, size) 对。
    pub fn scan_meta(&self) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error>> {
        match self.backend_type {
            StorageBackendType::Distributed => return self.scan_meta_distributed(),
            StorageBackendType::Bucket => return self.scan_meta_bucket(),
            StorageBackendType::OffsetAllocator => return self.scan_meta_offset_allocator(),
            _ => {}
        }
        let dir = self.key_dir();
        if !dir.exists() {
            return Ok(vec![]);
        }
        let mut results = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let meta = entry.metadata()?;
            results.push((name, meta.len()));
        }
        Ok(results)
    }

    pub fn is_enable_offloading(&self) -> bool {
        matches!(
            self.backend_type,
            StorageBackendType::FilePerKey
                | StorageBackendType::Bucket
                | StorageBackendType::OffsetAllocator
                | StorageBackendType::Distributed
        )
    }
}

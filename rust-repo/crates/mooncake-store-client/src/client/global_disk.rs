use crate::local_storage_backend::local_storage_key;
use fs2::FileExt;
use mooncake_store_core::{StoreError, error::StoreResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const GLOBAL_DISK_METADATA_VERSION: u32 = 1;
const GLOBAL_DISK_LOCK_FILE: &str = ".mooncake-global-disk.lock";
const GLOBAL_DISK_DATA_SUFFIX: &str = "data";
const GLOBAL_DISK_METADATA_SUFFIX: &str = "meta";
const GLOBAL_DISK_TEMP_SUFFIX: &str = "tmp";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct GlobalDiskRecordMetadata {
    version: u32,
    tenant_id: String,
    key: String,
    data_size: u64,
    created_unix_ns: u64,
}

#[derive(Debug, Clone)]
struct GlobalDiskRecord {
    metadata: GlobalDiskRecordMetadata,
    data_path: PathBuf,
    metadata_path: PathBuf,
}

impl GlobalDiskRecord {
    fn storage_key(&self) -> String {
        local_storage_key(&self.metadata.tenant_id, &self.metadata.key)
    }
}

/// A prepared global-DISK mutation.
///
/// The namespace lock deliberately remains owned while the caller notifies the
/// Master about selected victims. This makes the selection stable across both
/// threads and processes: no writer can replace a victim between Master
/// acceptance and local deletion.
pub(crate) struct PendingGlobalDiskWrite {
    _process_lock: tokio::sync::OwnedMutexGuard<()>,
    _namespace_lock: File,
    target_path: PathBuf,
    target_metadata_path: PathBuf,
    tenant_id: String,
    key: String,
    data_size: u64,
    created_unix_ns: u64,
    victims: Vec<GlobalDiskRecord>,
}

impl PendingGlobalDiskWrite {
    pub(crate) fn eviction_storage_keys(&self) -> Vec<String> {
        self.victims
            .iter()
            .map(GlobalDiskRecord::storage_key)
            .collect()
    }
}

pub(crate) struct PendingGlobalDiskEviction {
    _process_lock: tokio::sync::OwnedMutexGuard<()>,
    _namespace_lock: File,
    victims: Vec<GlobalDiskRecord>,
}

impl PendingGlobalDiskEviction {
    pub(crate) fn eviction_storage_keys(&self) -> Vec<String> {
        self.victims
            .iter()
            .map(GlobalDiskRecord::storage_key)
            .collect()
    }
}

/// Client-side data plane for the shared-filesystem `Disk` replica type.
///
/// The Master is authoritative for the exact data path. Each data file has a
/// durable metadata sidecar containing the tenant and user key, allowing a
/// restarted client to rebuild FIFO/quota state and notify the Master before
/// deleting a victim. Mutations are serialized with a namespace-wide
/// cross-process lock; reads remain lock-free.
#[derive(Debug)]
pub(crate) struct GlobalDiskStorage {
    namespace_root: PathBuf,
    lock_path: PathBuf,
    enable_eviction: bool,
    configured_quota_bytes: u64,
    quota_bytes: u64,
    enable_tenant_scope: bool,
    mutation_lock: Arc<tokio::sync::Mutex<()>>,
}

impl GlobalDiskStorage {
    pub(crate) fn new(
        advertised_root: &str,
        enable_eviction: bool,
        configured_quota_bytes: u64,
        enable_tenant_scope: bool,
    ) -> StoreResult<Self> {
        let advertised_root = PathBuf::from(advertised_root);
        if advertised_root.as_os_str().is_empty() {
            return Err(StoreError::InvalidParams(
                "global DISK root is empty".to_string(),
            ));
        }
        if !advertised_root.is_absolute() {
            return Err(StoreError::InvalidParams(format!(
                "global DISK root must be absolute: {}",
                advertised_root.display()
            )));
        }
        let namespace_root = advertised_root.join("global-disk");
        fs::create_dir_all(&namespace_root).map_err(|error| {
            StoreError::Internal(format!(
                "failed to create global DISK namespace {}: {error}",
                namespace_root.display()
            ))
        })?;
        let namespace_root = namespace_root.canonicalize().map_err(|error| {
            StoreError::Internal(format!(
                "failed to canonicalize global DISK namespace {}: {error}",
                namespace_root.display()
            ))
        })?;
        let quota_bytes = if configured_quota_bytes > 0 {
            configured_quota_bytes
        } else {
            let capacity = fs2::total_space(&namespace_root).map_err(|error| {
                StoreError::Internal(format!(
                    "failed to query global DISK capacity for {}: {error}",
                    namespace_root.display()
                ))
            })?;
            (capacity as f64 * 0.90) as u64
        };
        let storage = Self {
            lock_path: namespace_root.join(GLOBAL_DISK_LOCK_FILE),
            namespace_root,
            enable_eviction,
            configured_quota_bytes,
            quota_bytes,
            enable_tenant_scope,
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        storage.initialize_namespace()?;
        Ok(storage)
    }

    pub(crate) fn validate_advertised_config(
        &self,
        advertised_root: &str,
        enable_eviction: bool,
        configured_quota_bytes: u64,
        enable_tenant_scope: bool,
    ) -> StoreResult<()> {
        let namespace_root = Path::new(advertised_root)
            .join("global-disk")
            .canonicalize()
            .map_err(|error| {
                StoreError::InvalidParams(format!(
                    "failover global DISK namespace is inaccessible: {error}"
                ))
            })?;
        if namespace_root != self.namespace_root
            || enable_eviction != self.enable_eviction
            || configured_quota_bytes != self.configured_quota_bytes
            || enable_tenant_scope != self.enable_tenant_scope
        {
            return Err(StoreError::InvalidParams(format!(
                "failover Master advertises incompatible global DISK config: \
                 root={}, eviction={}, quota={}, tenant_scope={}",
                namespace_root.display(),
                enable_eviction,
                configured_quota_bytes,
                enable_tenant_scope
            )));
        }
        Ok(())
    }

    fn normalized_tenant_id(tenant_id: &str) -> &str {
        if tenant_id.is_empty() {
            "default"
        } else {
            tenant_id
        }
    }

    fn scoped_key(tenant_id: &str, key: &str) -> String {
        let tenant_id = Self::normalized_tenant_id(tenant_id);
        let mut scoped = String::with_capacity(tenant_id.len() + key.len() + 1);
        scoped.push_str(tenant_id);
        scoped.push('\0');
        scoped.push_str(key);
        scoped
    }

    fn expected_data_path(&self, tenant_id: &str, key: &str) -> PathBuf {
        let digest = Sha256::digest(Self::scoped_key(tenant_id, key).as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.namespace_root
            .join(&digest[..2])
            .join(&digest[2..4])
            .join(format!("{digest}.{GLOBAL_DISK_DATA_SUFFIX}"))
    }

    fn metadata_path(data_path: &Path) -> PathBuf {
        data_path.with_extension(GLOBAL_DISK_METADATA_SUFFIX)
    }

    fn reject_parent_components(path: &Path) -> StoreResult<()> {
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(StoreError::InvalidParams(format!(
                "global DISK path contains parent traversal: {}",
                path.display()
            )));
        }
        Ok(())
    }

    fn resolve_path(&self, descriptor_path: &str, create_parent: bool) -> StoreResult<PathBuf> {
        let candidate = PathBuf::from(descriptor_path);
        if !candidate.is_absolute() {
            return Err(StoreError::InvalidParams(format!(
                "global DISK descriptor path must be absolute: {}",
                candidate.display()
            )));
        }
        Self::reject_parent_components(&candidate)?;
        if candidate.extension().and_then(|value| value.to_str()) != Some(GLOBAL_DISK_DATA_SUFFIX) {
            return Err(StoreError::InvalidParams(format!(
                "global DISK descriptor has an invalid suffix: {}",
                candidate.display()
            )));
        }
        let parent = candidate.parent().ok_or_else(|| {
            StoreError::InvalidParams(format!(
                "global DISK descriptor has no parent: {}",
                candidate.display()
            ))
        })?;
        if create_parent {
            self.ensure_parent(parent)?;
        }
        let canonical_parent = parent.canonicalize().map_err(|error| {
            StoreError::Internal(format!(
                "failed to canonicalize global DISK parent {}: {error}",
                parent.display()
            ))
        })?;
        if !canonical_parent.starts_with(&self.namespace_root) {
            return Err(StoreError::InvalidParams(format!(
                "global DISK descriptor escapes advertised namespace: {}",
                candidate.display()
            )));
        }
        let file_name = candidate.file_name().ok_or_else(|| {
            StoreError::InvalidParams("global DISK descriptor has no file name".to_string())
        })?;
        let resolved = canonical_parent.join(file_name);
        if let Ok(metadata) = fs::symlink_metadata(&resolved) {
            if metadata.file_type().is_symlink() {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK descriptor resolves to a symlink: {}",
                    resolved.display()
                )));
            }
        }
        Ok(resolved)
    }

    fn ensure_parent(&self, parent: &Path) -> StoreResult<()> {
        let relative = parent.strip_prefix(&self.namespace_root).map_err(|_| {
            StoreError::InvalidParams(format!(
                "global DISK parent escapes namespace: {}",
                parent.display()
            ))
        })?;
        let mut current = self.namespace_root.clone();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK parent has an unsafe component: {}",
                    parent.display()
                )));
            };
            let next = current.join(component);
            match fs::symlink_metadata(&next) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(StoreError::InvalidParams(format!(
                        "global DISK refuses symbolic-link directory: {}",
                        next.display()
                    )));
                }
                Ok(metadata) if !metadata.is_dir() => {
                    return Err(StoreError::InvalidParams(format!(
                        "global DISK parent is not a directory: {}",
                        next.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&next)?;
                    Self::sync_directory(&current)?;
                }
                Err(error) => return Err(error.into()),
            }
            current = next;
        }
        Ok(())
    }

    fn acquire_namespace_lock(&self) -> StoreResult<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lock_path)?;
        file.lock_exclusive().map_err(|error| {
            StoreError::Internal(format!(
                "failed to lock global DISK namespace {}: {error}",
                self.namespace_root.display()
            ))
        })?;
        Ok(file)
    }

    fn initialize_namespace(&self) -> StoreResult<()> {
        let _lock = self.acquire_namespace_lock()?;
        self.reconcile_namespace()?;
        let records = self.scan_records()?;
        let used = Self::used_bytes(&records)?;
        if self.enable_eviction && used > self.quota_bytes {
            return Err(StoreError::InvalidParams(format!(
                "global DISK uses {used} bytes, exceeding configured quota {}; \
                 startup cannot evict before a Master session is available",
                self.quota_bytes
            )));
        }
        Ok(())
    }

    fn reconcile_namespace(&self) -> StoreResult<()> {
        let mut data_paths = HashSet::new();
        let mut metadata_paths = HashSet::new();
        self.collect_namespace_paths(
            &self.namespace_root,
            &mut data_paths,
            &mut metadata_paths,
            true,
        )?;

        for data_path in &data_paths {
            let metadata_path = Self::metadata_path(data_path);
            if !metadata_paths.contains(&metadata_path) {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK found data without durable tenant/key metadata; \
                     refusing destructive recovery: {}",
                    data_path.display()
                )));
            }
        }
        for metadata_path in &metadata_paths {
            let data_path = metadata_path.with_extension(GLOBAL_DISK_DATA_SUFFIX);
            if !data_paths.contains(&data_path) {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK found metadata without data; refusing destructive recovery: {}",
                    metadata_path.display()
                )));
            }
        }
        Ok(())
    }

    fn collect_namespace_paths(
        &self,
        directory: &Path,
        data_paths: &mut HashSet<PathBuf>,
        metadata_paths: &mut HashSet<PathBuf>,
        remove_temps: bool,
    ) -> StoreResult<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK refuses symbolic links: {}",
                    path.display()
                )));
            }
            if file_type.is_dir() {
                self.collect_namespace_paths(&path, data_paths, metadata_paths, remove_temps)?;
                continue;
            }
            if !file_type.is_file() {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK contains a non-regular entry: {}",
                    path.display()
                )));
            }
            if path == self.lock_path {
                continue;
            }
            match path.extension().and_then(|value| value.to_str()) {
                Some(GLOBAL_DISK_DATA_SUFFIX) => {
                    data_paths.insert(path);
                }
                Some(GLOBAL_DISK_METADATA_SUFFIX) => {
                    metadata_paths.insert(path);
                }
                Some(GLOBAL_DISK_TEMP_SUFFIX) if remove_temps => {
                    fs::remove_file(&path)?;
                    Self::sync_directory(directory)?;
                }
                _ => {
                    return Err(StoreError::InvalidParams(format!(
                        "global DISK contains an unknown file: {}",
                        path.display()
                    )));
                }
            }
        }
        Ok(())
    }

    fn scan_records(&self) -> StoreResult<Vec<GlobalDiskRecord>> {
        let mut data_paths = HashSet::new();
        let mut metadata_paths = HashSet::new();
        self.collect_namespace_paths(
            &self.namespace_root,
            &mut data_paths,
            &mut metadata_paths,
            false,
        )?;
        let mut records = Vec::with_capacity(metadata_paths.len());
        let mut identities = HashMap::with_capacity(metadata_paths.len());
        for metadata_path in metadata_paths {
            let data_path = metadata_path.with_extension(GLOBAL_DISK_DATA_SUFFIX);
            if !data_paths.remove(&data_path) {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK metadata has no data file: {}",
                    metadata_path.display()
                )));
            }
            let metadata = self.load_metadata(&metadata_path)?;
            let expected = self.expected_data_path(&metadata.tenant_id, &metadata.key);
            if data_path != expected {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK record path does not match tenant/key identity: {}",
                    data_path.display()
                )));
            }
            let actual_size = fs::metadata(&data_path)?.len();
            if actual_size != metadata.data_size {
                return Err(StoreError::InvalidParams(format!(
                    "global DISK record size mismatch for {}: metadata={}, data={actual_size}",
                    data_path.display(),
                    metadata.data_size
                )));
            }
            let identity = local_storage_key(&metadata.tenant_id, &metadata.key);
            if let Some(previous) = identities.insert(identity.clone(), data_path.clone()) {
                return Err(StoreError::InvalidParams(format!(
                    "duplicate global DISK identity {identity:?}: {} and {}",
                    previous.display(),
                    data_path.display()
                )));
            }
            records.push(GlobalDiskRecord {
                metadata,
                data_path,
                metadata_path,
            });
        }
        if let Some(orphan) = data_paths.into_iter().next() {
            return Err(StoreError::InvalidParams(format!(
                "global DISK data has no metadata sidecar: {}",
                orphan.display()
            )));
        }
        records.sort_by(|left, right| {
            left.metadata
                .created_unix_ns
                .cmp(&right.metadata.created_unix_ns)
                .then_with(|| left.data_path.cmp(&right.data_path))
        });
        Ok(records)
    }

    fn load_metadata(&self, path: &Path) -> StoreResult<GlobalDiskRecordMetadata> {
        let mut file = Self::open_read(path)?;
        let metadata_len = file.metadata()?.len();
        if metadata_len > 64 * 1024 {
            return Err(StoreError::InvalidParams(format!(
                "global DISK metadata is unreasonably large ({metadata_len} bytes): {}",
                path.display()
            )));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let metadata: GlobalDiskRecordMetadata =
            serde_json::from_slice(&bytes).map_err(|error| {
                StoreError::InvalidParams(format!(
                    "invalid global DISK metadata {}: {error}",
                    path.display()
                ))
            })?;
        if metadata.version != GLOBAL_DISK_METADATA_VERSION {
            return Err(StoreError::InvalidParams(format!(
                "unsupported global DISK metadata version {} in {}",
                metadata.version,
                path.display()
            )));
        }
        if metadata.tenant_id.is_empty() || metadata.key.is_empty() {
            return Err(StoreError::InvalidParams(format!(
                "global DISK metadata has an empty identity: {}",
                path.display()
            )));
        }
        Ok(metadata)
    }

    fn used_bytes(records: &[GlobalDiskRecord]) -> StoreResult<u64> {
        records.iter().try_fold(0_u64, |total, record| {
            total.checked_add(record.metadata.data_size).ok_or_else(|| {
                StoreError::InvalidParams("global DISK used-byte total overflow".to_string())
            })
        })
    }

    fn prepare_write_blocking(
        &self,
        process_lock: tokio::sync::OwnedMutexGuard<()>,
        descriptor_path: &str,
        tenant_id: &str,
        key: &str,
        data_size: u64,
    ) -> StoreResult<PendingGlobalDiskWrite> {
        if key.is_empty() {
            return Err(StoreError::InvalidParams(
                "global DISK key is empty".to_string(),
            ));
        }
        let tenant_id = if self.enable_tenant_scope {
            Self::normalized_tenant_id(tenant_id)
        } else {
            "default"
        }
        .to_string();
        let target_path = self.resolve_path(descriptor_path, true)?;
        let expected_path = self.expected_data_path(&tenant_id, key);
        if target_path != expected_path {
            return Err(StoreError::InvalidParams(format!(
                "global DISK descriptor does not match tenant/key identity: descriptor={}, expected={}",
                target_path.display(),
                expected_path.display()
            )));
        }
        let namespace_lock = self.acquire_namespace_lock()?;
        let records = self.scan_records()?;
        let now_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let created_unix_ns = records
            .iter()
            .map(|record| record.metadata.created_unix_ns)
            .max()
            .map_or(now_unix_ns, |latest| {
                now_unix_ns.max(latest.saturating_add(1))
            });
        let used = Self::used_bytes(&records)?;
        let current_size = records
            .iter()
            .find(|record| record.data_path == target_path)
            .map_or(0, |record| record.metadata.data_size);
        let projected = used
            .checked_sub(current_size)
            .and_then(|value| value.checked_add(data_size))
            .ok_or_else(|| {
                StoreError::InvalidParams("global DISK projected usage overflow".to_string())
            })?;

        let mut victims = Vec::new();
        if self.enable_eviction && projected > self.quota_bytes {
            let mut reclaimed = 0_u64;
            let required = projected - self.quota_bytes;
            for record in records {
                if record.data_path == target_path {
                    continue;
                }
                reclaimed = reclaimed
                    .checked_add(record.metadata.data_size)
                    .ok_or_else(|| {
                        StoreError::InvalidParams(
                            "global DISK eviction byte total overflow".to_string(),
                        )
                    })?;
                victims.push(record);
                if reclaimed >= required {
                    break;
                }
            }
            if reclaimed < required {
                return Err(StoreError::NoAvailableHandle);
            }
        }
        Ok(PendingGlobalDiskWrite {
            _process_lock: process_lock,
            _namespace_lock: namespace_lock,
            target_metadata_path: Self::metadata_path(&target_path),
            target_path,
            tenant_id,
            key: key.to_string(),
            data_size,
            created_unix_ns,
            victims,
        })
    }

    fn commit_victims(
        &self,
        victims: &[GlobalDiskRecord],
        accepted_storage_keys: Option<&HashSet<String>>,
    ) -> StoreResult<usize> {
        let mut changed_directories = HashSet::new();
        let mut committed = 0;
        for victim in victims {
            if accepted_storage_keys
                .is_some_and(|accepted| !accepted.contains(&victim.storage_key()))
            {
                continue;
            }
            match fs::remove_file(&victim.data_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            match fs::remove_file(&victim.metadata_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            if let Some(parent) = victim.data_path.parent() {
                changed_directories.insert(parent.to_path_buf());
            }
            committed += 1;
        }
        for directory in changed_directories {
            Self::sync_directory(&directory)?;
        }
        Ok(committed)
    }

    fn commit_write_blocking(
        &self,
        pending: PendingGlobalDiskWrite,
        data: &[u8],
    ) -> StoreResult<()> {
        if data.len() as u64 != pending.data_size {
            return Err(StoreError::InvalidParams(format!(
                "global DISK prepared size {} does not match payload {}",
                pending.data_size,
                data.len()
            )));
        }
        self.commit_victims(&pending.victims, None)?;
        let metadata = GlobalDiskRecordMetadata {
            version: GLOBAL_DISK_METADATA_VERSION,
            tenant_id: pending.tenant_id,
            key: pending.key,
            data_size: pending.data_size,
            created_unix_ns: pending.created_unix_ns,
        };
        let metadata_bytes = serde_json::to_vec(&metadata).map_err(|error| {
            StoreError::Internal(format!("failed to encode global DISK metadata: {error}"))
        })?;
        self.write_file_atomically(&pending.target_path, data)?;
        if let Err(metadata_error) =
            self.write_file_atomically(&pending.target_metadata_path, &metadata_bytes)
        {
            let data_cleanup = fs::remove_file(&pending.target_path);
            let metadata_cleanup = fs::remove_file(&pending.target_metadata_path);
            if let Some(parent) = pending.target_path.parent() {
                let _ = Self::sync_directory(parent);
            }
            return Err(StoreError::Internal(format!(
                "failed to publish global DISK metadata: {metadata_error}; \
                 data cleanup={data_cleanup:?}; metadata cleanup={metadata_cleanup:?}"
            )));
        }
        Ok(())
    }

    fn finalize_partial_blocking(
        &self,
        pending: PendingGlobalDiskWrite,
        accepted_storage_keys: &HashSet<String>,
    ) -> StoreResult<usize> {
        self.commit_victims(&pending.victims, Some(accepted_storage_keys))
    }

    fn prepare_watermark_eviction_blocking(
        &self,
        process_lock: tokio::sync::OwnedMutexGuard<()>,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<PendingGlobalDiskEviction> {
        if !(0.0 < low_watermark_ratio
            && low_watermark_ratio < high_watermark_ratio
            && high_watermark_ratio <= 1.0)
        {
            return Err(StoreError::InvalidParams(format!(
                "invalid global DISK watermarks: high={high_watermark_ratio}, \
                 low={low_watermark_ratio}"
            )));
        }
        let namespace_lock = self.acquire_namespace_lock()?;
        let records = self.scan_records()?;
        let used = Self::used_bytes(&records)?;
        let high = (self.quota_bytes as f64 * high_watermark_ratio) as u64;
        if !self.enable_eviction || self.quota_bytes == 0 || used <= high {
            return Ok(PendingGlobalDiskEviction {
                _process_lock: process_lock,
                _namespace_lock: namespace_lock,
                victims: Vec::new(),
            });
        }
        let low = (self.quota_bytes as f64 * low_watermark_ratio) as u64;
        let mut projected = used;
        let mut victims = Vec::new();
        for record in records {
            projected = projected.saturating_sub(record.metadata.data_size);
            victims.push(record);
            if projected <= low {
                break;
            }
        }
        if projected > low {
            return Err(StoreError::NoAvailableHandle);
        }
        Ok(PendingGlobalDiskEviction {
            _process_lock: process_lock,
            _namespace_lock: namespace_lock,
            victims,
        })
    }

    fn commit_watermark_eviction_blocking(
        &self,
        pending: PendingGlobalDiskEviction,
    ) -> StoreResult<usize> {
        self.commit_victims(&pending.victims, None)
    }

    fn finalize_partial_watermark_blocking(
        &self,
        pending: PendingGlobalDiskEviction,
        accepted_storage_keys: &HashSet<String>,
    ) -> StoreResult<usize> {
        self.commit_victims(&pending.victims, Some(accepted_storage_keys))
    }

    fn remove_all_for_tenant_blocking(
        &self,
        tenant_id: &str,
        _process_lock: tokio::sync::OwnedMutexGuard<()>,
    ) -> StoreResult<usize> {
        let _namespace_lock = self.acquire_namespace_lock()?;
        let tenant_id = if self.enable_tenant_scope {
            Self::normalized_tenant_id(tenant_id)
        } else {
            "default"
        };
        let victims = self
            .scan_records()?
            .into_iter()
            .filter(|record| record.metadata.tenant_id == tenant_id)
            .collect::<Vec<_>>();
        self.commit_victims(&victims, None)
    }

    fn write_file_atomically(&self, path: &Path, data: &[u8]) -> StoreResult<()> {
        let parent = path.parent().ok_or_else(|| {
            StoreError::InvalidParams(format!(
                "global DISK target has no parent: {}",
                path.display()
            ))
        })?;
        self.ensure_parent(parent)?;
        let temp = parent.join(format!(
            ".{}.{}.{}",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("object"),
            Uuid::new_v4(),
            GLOBAL_DISK_TEMP_SUFFIX
        ));
        let result = (|| -> StoreResult<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp)?;
            file.write_all(data)?;
            file.sync_all()?;
            fs::rename(&temp, path)?;
            Self::sync_directory(parent)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    fn sync_directory(directory: &Path) -> StoreResult<()> {
        OpenOptions::new()
            .read(true)
            .open(directory)?
            .sync_all()
            .map_err(Into::into)
    }

    #[cfg(unix)]
    fn open_read(path: &Path) -> std::io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
    }

    #[cfg(not(unix))]
    fn open_read(path: &Path) -> std::io::Result<File> {
        OpenOptions::new().read(true).open(path)
    }

    fn validate_read_metadata(
        &self,
        data_path: &Path,
        expected_size: u64,
    ) -> StoreResult<GlobalDiskRecordMetadata> {
        let metadata_path = Self::metadata_path(data_path);
        let metadata = self.load_metadata(&metadata_path)?;
        if self.expected_data_path(&metadata.tenant_id, &metadata.key) != data_path {
            return Err(StoreError::InvalidParams(format!(
                "global DISK read path does not match metadata identity: {}",
                data_path.display()
            )));
        }
        if metadata.data_size != expected_size {
            return Err(StoreError::Internal(format!(
                "global DISK metadata size mismatch for {}: expected {expected_size}, got {}",
                data_path.display(),
                metadata.data_size
            )));
        }
        Ok(metadata)
    }

    fn read_blocking(&self, descriptor_path: &str, expected_size: usize) -> StoreResult<Vec<u8>> {
        let path = self.resolve_path(descriptor_path, false)?;
        self.validate_read_metadata(&path, expected_size as u64)?;
        let mut file = Self::open_read(&path).map_err(|error| {
            StoreError::Internal(format!(
                "failed to open global DISK file {}: {error}",
                path.display()
            ))
        })?;
        let actual_size = file.metadata()?.len();
        if actual_size != expected_size as u64 {
            return Err(StoreError::Internal(format!(
                "global DISK size mismatch for {}: expected {expected_size}, got {actual_size}",
                path.display()
            )));
        }
        let mut data = vec![0; expected_size];
        file.read_exact(&mut data)?;
        Ok(data)
    }

    fn read_range_blocking(
        &self,
        descriptor_path: &str,
        object_size: u64,
        offset: u64,
        length: usize,
    ) -> StoreResult<Vec<u8>> {
        let length_u64 = u64::try_from(length).map_err(|_| {
            StoreError::InvalidParams("global DISK range length cannot fit u64".to_string())
        })?;
        let end = offset
            .checked_add(length_u64)
            .ok_or_else(|| StoreError::InvalidParams("global DISK range overflow".to_string()))?;
        if end > object_size {
            return Err(StoreError::InvalidParams(format!(
                "global DISK range [{offset}, {end}) exceeds object size {object_size}"
            )));
        }
        let path = self.resolve_path(descriptor_path, false)?;
        self.validate_read_metadata(&path, object_size)?;
        let mut file = Self::open_read(&path)?;
        if file.metadata()?.len() != object_size {
            return Err(StoreError::Internal(format!(
                "global DISK size mismatch for {}",
                path.display()
            )));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0; length];
        file.read_exact(&mut data)?;
        Ok(data)
    }

    pub(crate) async fn prepare_write(
        self: &Arc<Self>,
        path: String,
        tenant_id: String,
        key: String,
        data_size: u64,
    ) -> StoreResult<PendingGlobalDiskWrite> {
        let storage = Arc::clone(self);
        let process_lock = Arc::clone(&self.mutation_lock).lock_owned().await;
        tokio::task::spawn_blocking(move || {
            storage.prepare_write_blocking(process_lock, &path, &tenant_id, &key, data_size)
        })
        .await
        .map_err(|error| {
            StoreError::Internal(format!("global DISK prepare task failed: {error}"))
        })?
    }

    pub(crate) async fn commit_write(
        self: &Arc<Self>,
        pending: PendingGlobalDiskWrite,
        data: Vec<u8>,
    ) -> StoreResult<()> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || storage.commit_write_blocking(pending, &data))
            .await
            .map_err(|error| {
                StoreError::Internal(format!("global DISK writer task failed: {error}"))
            })?
    }

    pub(crate) async fn finalize_partial(
        self: &Arc<Self>,
        pending: PendingGlobalDiskWrite,
        accepted_storage_keys: HashSet<String>,
    ) -> StoreResult<usize> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            storage.finalize_partial_blocking(pending, &accepted_storage_keys)
        })
        .await
        .map_err(|error| {
            StoreError::Internal(format!(
                "global DISK partial eviction finalizer failed: {error}"
            ))
        })?
    }

    pub(crate) async fn prepare_watermark_eviction(
        self: &Arc<Self>,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<PendingGlobalDiskEviction> {
        let storage = Arc::clone(self);
        let process_lock = Arc::clone(&self.mutation_lock).lock_owned().await;
        tokio::task::spawn_blocking(move || {
            storage.prepare_watermark_eviction_blocking(
                process_lock,
                high_watermark_ratio,
                low_watermark_ratio,
            )
        })
        .await
        .map_err(|error| {
            StoreError::Internal(format!(
                "global DISK watermark prepare task failed: {error}"
            ))
        })?
    }

    pub(crate) async fn commit_watermark_eviction(
        self: &Arc<Self>,
        pending: PendingGlobalDiskEviction,
    ) -> StoreResult<usize> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || storage.commit_watermark_eviction_blocking(pending))
            .await
            .map_err(|error| {
                StoreError::Internal(format!("global DISK watermark commit task failed: {error}"))
            })?
    }

    pub(crate) async fn finalize_partial_watermark(
        self: &Arc<Self>,
        pending: PendingGlobalDiskEviction,
        accepted_storage_keys: HashSet<String>,
    ) -> StoreResult<usize> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            storage.finalize_partial_watermark_blocking(pending, &accepted_storage_keys)
        })
        .await
        .map_err(|error| {
            StoreError::Internal(format!(
                "global DISK partial watermark finalizer failed: {error}"
            ))
        })?
    }

    pub(crate) async fn remove_all_for_tenant(
        self: &Arc<Self>,
        tenant_id: String,
    ) -> StoreResult<usize> {
        let storage = Arc::clone(self);
        let process_lock = Arc::clone(&self.mutation_lock).lock_owned().await;
        tokio::task::spawn_blocking(move || {
            storage.remove_all_for_tenant_blocking(&tenant_id, process_lock)
        })
        .await
        .map_err(|error| {
            StoreError::Internal(format!("global DISK tenant cleanup task failed: {error}"))
        })?
    }

    pub(crate) async fn read(
        self: &Arc<Self>,
        path: String,
        expected_size: usize,
    ) -> StoreResult<Vec<u8>> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || storage.read_blocking(&path, expected_size))
            .await
            .map_err(|error| {
                StoreError::Internal(format!("global DISK reader task failed: {error}"))
            })?
    }

    pub(crate) async fn read_range(
        self: &Arc<Self>,
        path: String,
        object_size: u64,
        offset: u64,
        length: usize,
    ) -> StoreResult<Vec<u8>> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            storage.read_range_blocking(&path, object_size, offset, length)
        })
        .await
        .map_err(|error| StoreError::Internal(format!("global DISK reader task failed: {error}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(root: &Path, enable_eviction: bool, quota_bytes: u64) -> Arc<GlobalDiskStorage> {
        Arc::new(
            GlobalDiskStorage::new(
                root.to_str().expect("UTF-8 temp path"),
                enable_eviction,
                quota_bytes,
                true,
            )
            .unwrap(),
        )
    }

    async fn write_without_eviction(
        storage: &Arc<GlobalDiskStorage>,
        tenant_id: &str,
        key: &str,
        data: &[u8],
    ) {
        let path = storage
            .expected_data_path(tenant_id, key)
            .to_string_lossy()
            .into_owned();
        let pending = storage
            .prepare_write(
                path,
                tenant_id.to_string(),
                key.to_string(),
                data.len() as u64,
            )
            .await
            .unwrap();
        assert!(pending.eviction_storage_keys().is_empty());
        storage.commit_write(pending, data.to_vec()).await.unwrap();
    }

    #[tokio::test]
    async fn atomic_round_trip_and_range_read() {
        let temp = tempfile::tempdir().unwrap();
        let advertised = temp.path().join("cluster-a");
        let storage = storage(&advertised, true, 1024);
        write_without_eviction(&storage, "tenant-a", "object", b"abcdefgh").await;
        let path = storage
            .expected_data_path("tenant-a", "object")
            .to_string_lossy()
            .into_owned();

        assert_eq!(storage.read(path.clone(), 8).await.unwrap(), b"abcdefgh");
        assert_eq!(storage.read_range(path, 8, 2, 3).await.unwrap(), b"cde");
    }

    #[tokio::test]
    async fn quota_prepare_selects_fifo_victims_and_commits_only_after_acceptance() {
        let temp = tempfile::tempdir().unwrap();
        let advertised = temp.path().join("cluster-a");
        let storage = storage(&advertised, true, 10);
        write_without_eviction(&storage, "tenant-a", "oldest", b"123456").await;
        write_without_eviction(&storage, "tenant-b", "newer", b"1234").await;

        let target_path = storage
            .expected_data_path("tenant-c", "target")
            .to_string_lossy()
            .into_owned();
        let pending = storage
            .prepare_write(
                target_path.clone(),
                "tenant-c".to_string(),
                "target".to_string(),
                6,
            )
            .await
            .unwrap();
        assert_eq!(
            pending.eviction_storage_keys(),
            [local_storage_key("tenant-a", "oldest")]
        );
        let accepted = HashSet::from([local_storage_key("tenant-a", "oldest")]);
        storage.finalize_partial(pending, accepted).await.unwrap();

        assert!(
            storage
                .read(
                    storage
                        .expected_data_path("tenant-a", "oldest")
                        .to_string_lossy()
                        .into_owned(),
                    6,
                )
                .await
                .is_err()
        );
        assert!(
            storage
                .read(
                    storage
                        .expected_data_path("tenant-b", "newer")
                        .to_string_lossy()
                        .into_owned(),
                    4,
                )
                .await
                .is_ok()
        );
        assert!(storage.read(target_path, 6).await.is_err());
    }

    #[tokio::test]
    async fn restart_recovers_tenant_identity_and_quota_order() {
        let temp = tempfile::tempdir().unwrap();
        let advertised = temp.path().join("cluster-a");
        {
            let storage = storage(&advertised, true, 10);
            write_without_eviction(&storage, "tenant-a", "oldest", b"123456").await;
            write_without_eviction(&storage, "tenant-b", "newer", b"1234").await;
        }
        let restarted = storage(&advertised, true, 10);
        let pending = restarted
            .prepare_write(
                restarted
                    .expected_data_path("tenant-c", "target")
                    .to_string_lossy()
                    .into_owned(),
                "tenant-c".to_string(),
                "target".to_string(),
                5,
            )
            .await
            .unwrap();
        assert_eq!(
            pending.eviction_storage_keys(),
            [local_storage_key("tenant-a", "oldest")]
        );
    }

    #[tokio::test]
    async fn watermark_and_remove_all_preserve_other_tenants() {
        let temp = tempfile::tempdir().unwrap();
        let advertised = temp.path().join("cluster-a");
        let storage = storage(&advertised, true, 10);
        write_without_eviction(&storage, "tenant-a", "oldest", b"123456").await;
        write_without_eviction(&storage, "tenant-b", "newer", b"123").await;

        let pending = storage
            .prepare_watermark_eviction(0.80, 0.40)
            .await
            .unwrap();
        assert_eq!(
            pending.eviction_storage_keys(),
            [local_storage_key("tenant-a", "oldest")]
        );
        assert_eq!(storage.commit_watermark_eviction(pending).await.unwrap(), 1);

        write_without_eviction(&storage, "tenant-a", "replacement", b"12").await;
        assert_eq!(
            storage
                .remove_all_for_tenant("tenant-a".to_string())
                .await
                .unwrap(),
            1
        );
        assert!(
            storage
                .read(
                    storage
                        .expected_data_path("tenant-b", "newer")
                        .to_string_lossy()
                        .into_owned(),
                    3,
                )
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn disabled_eviction_ignores_quota_and_default_scope_ignores_request_tenant() {
        let temp = tempfile::tempdir().unwrap();
        let advertised = temp.path().join("cluster-a");
        let storage = Arc::new(
            GlobalDiskStorage::new(
                advertised.to_str().expect("UTF-8 temp path"),
                false,
                1,
                false,
            )
            .unwrap(),
        );
        let default_path = storage
            .expected_data_path("default", "object")
            .to_string_lossy()
            .into_owned();
        let pending = storage
            .prepare_write(
                default_path.clone(),
                "ignored-tenant".to_string(),
                "object".to_string(),
                8,
            )
            .await
            .unwrap();
        assert!(pending.eviction_storage_keys().is_empty());
        storage
            .commit_write(pending, b"abcdefgh".to_vec())
            .await
            .unwrap();
        assert_eq!(storage.read(default_path, 8).await.unwrap(), b"abcdefgh");
    }

    #[test]
    fn startup_fails_closed_on_unowned_data_without_identity_sidecar() {
        let temp = tempfile::tempdir().unwrap();
        let advertised = temp.path().join("cluster-a");
        let namespace = advertised.join("global-disk/aa/bb");
        fs::create_dir_all(&namespace).unwrap();
        fs::write(namespace.join("orphan.data"), b"must-not-delete").unwrap();

        let error = GlobalDiskStorage::new(
            advertised.to_str().expect("UTF-8 temp path"),
            true,
            1024,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("refusing destructive recovery"));
        assert_eq!(
            fs::read(namespace.join("orphan.data")).unwrap(),
            b"must-not-delete"
        );
    }

    #[tokio::test]
    async fn rejects_escape_identity_mismatch_and_size_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let advertised = temp.path().join("cluster-a");
        let storage = storage(&advertised, true, 1024);
        let outside = temp
            .path()
            .join("outside.data")
            .to_string_lossy()
            .into_owned();
        assert!(
            storage
                .prepare_write(outside, "tenant-a".to_string(), "object".to_string(), 1,)
                .await
                .is_err()
        );

        let wrong_identity = storage
            .expected_data_path("tenant-a", "object")
            .to_string_lossy()
            .into_owned();
        assert!(
            storage
                .prepare_write(
                    wrong_identity,
                    "tenant-b".to_string(),
                    "object".to_string(),
                    1,
                )
                .await
                .is_err()
        );

        write_without_eviction(&storage, "tenant-a", "object", b"abcd").await;
        let path = storage
            .expected_data_path("tenant-a", "object")
            .to_string_lossy()
            .into_owned();
        assert!(storage.read(path, 3).await.is_err());
    }
}

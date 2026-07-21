use super::{OffsetAllocatorConfig, OffsetEvictionPolicy};
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::StoreError;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OffsetEntry {
    offset: u64,
    len: u64,
    #[serde(default)]
    fifo_seq: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedIndex {
    #[serde(default)]
    entries: HashMap<String, OffsetEntry>,
    #[serde(default)]
    next_offset: u64,
    #[serde(default)]
    next_fifo_seq: u64,
}

#[derive(Debug, Default)]
struct OffsetState {
    index: PersistedIndex,
    free_extents: BTreeMap<u64, u64>,
    used_bytes: u64,
    quota_bytes: u64,
}

#[derive(Debug, Default)]
pub struct PendingOffsetEviction {
    victims: Vec<(String, OffsetEntry)>,
}

impl PendingOffsetEviction {
    pub fn keys(&self) -> Vec<String> {
        self.victims.iter().map(|(key, _)| key.clone()).collect()
    }
}

/// Append/reuse offset allocator used by the client SSD offload path.
///
/// Metadata is persisted separately from the data file. FIFO eviction removes
/// logical entries first and makes their extents available for subsequent
/// writes, keeping the data file bounded by the configured quota.
pub struct OffsetAllocatorStorageBackend {
    config: OffsetAllocatorConfig,
    state: Mutex<OffsetState>,
    initialized: AtomicBool,
}

impl OffsetAllocatorStorageBackend {
    pub fn new(config: OffsetAllocatorConfig) -> Self {
        Self {
            config,
            state: Mutex::new(OffsetState::default()),
            initialized: AtomicBool::new(false),
        }
    }

    fn data_dir(&self) -> PathBuf {
        self.config.root_dir.join(&self.config.fsdir)
    }

    fn data_path(&self) -> PathBuf {
        self.data_dir().join("offset_allocator.data")
    }

    fn index_path(&self) -> PathBuf {
        self.data_dir().join("offset_allocator.index.json")
    }

    pub fn init(&self) -> StoreResult<()> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        self.config.validate().map_err(StoreError::InvalidParams)?;
        std::fs::create_dir_all(self.data_dir())?;

        let mut index = if self.index_path().exists() {
            serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(
                self.index_path(),
            )?))?
        } else {
            PersistedIndex::default()
        };
        let file_len = std::fs::metadata(self.data_path())
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let upgraded = normalize_loaded_index(&mut index, file_len);
        let quota_bytes = if self.config.quota_bytes > 0 {
            self.config.quota_bytes
        } else {
            (fs2::available_space(self.data_dir())? as f64 * 0.90) as u64
        };
        let used_bytes = index.entries.values().map(|entry| entry.len).sum();
        let free_extents = rebuild_free_extents(&index, file_len);
        *self.state.lock() = OffsetState {
            index,
            free_extents,
            used_bytes,
            quota_bytes,
        };
        if upgraded {
            let state = self.state.lock();
            self.persist_index(&state.index)?;
        }
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

    fn ensure_init(&self) -> StoreResult<()> {
        if self.initialized.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(StoreError::Internal(
                "offset allocator storage backend is not initialized".to_string(),
            ))
        }
    }

    pub fn write_object(&self, key: &str, data: &[u8]) -> StoreResult<Vec<String>> {
        let pending = self.prepare_write(key, data.len() as u64)?;
        let evicted = pending.keys();
        self.commit_write(key, data, pending)?;
        Ok(evicted)
    }

    pub fn prepare_write(&self, key: &str, required: u64) -> StoreResult<PendingOffsetEviction> {
        self.ensure_init()?;
        let state = self.state.lock();
        if required > state.quota_bytes {
            return Err(StoreError::NoAvailableHandle);
        }
        let replaced = state.index.entries.get(key).cloned();
        let replaced_len = replaced.as_ref().map(|entry| entry.len).unwrap_or(0);
        let high = (state.quota_bytes as f64 * self.config.high_ratio) as u64;
        let low = (state.quota_bytes as f64 * self.config.low_ratio) as u64;
        let (keys_high, keys_low) = key_watermarks(&self.config);
        let over_bytes = state
            .used_bytes
            .saturating_sub(replaced_len)
            .saturating_add(required)
            > high;
        let over_keys = state.index.entries.len() > keys_high;
        let mut pending = PendingOffsetEviction::default();

        if self.config.eviction_policy == OffsetEvictionPolicy::Fifo && (over_bytes || over_keys) {
            let minimum_victims = if over_keys {
                self.config.fallback_evict_batch
            } else {
                0
            };
            let mut projected_used = state.used_bytes;
            let mut projected_keys = state.index.entries.len();
            while (projected_used
                .saturating_sub(replaced_len)
                .saturating_add(required)
                > low
                || projected_keys > keys_low
                || pending.victims.len() < minimum_victims)
                && pending.victims.len() < self.config.max_evict_per_offload
            {
                let victim = state
                    .index
                    .entries
                    .iter()
                    .filter(|(candidate, _)| candidate.as_str() != key)
                    .filter(|(candidate, _)| {
                        !pending
                            .victims
                            .iter()
                            .any(|(selected, _)| selected == *candidate)
                    })
                    .min_by_key(|(_, entry)| entry.fifo_seq)
                    .map(|(candidate, entry)| (candidate.clone(), entry.clone()));
                let Some((victim, entry)) = victim else {
                    break;
                };
                projected_used = projected_used.saturating_sub(entry.len);
                projected_keys = projected_keys.saturating_sub(1);
                pending.victims.push((victim, entry));
            }
        }
        Ok(pending)
    }

    pub fn rollback_eviction(&self, pending: PendingOffsetEviction) {
        drop(pending);
    }

    pub fn commit_write(
        &self,
        key: &str,
        data: &[u8],
        pending: PendingOffsetEviction,
    ) -> StoreResult<()> {
        self.ensure_init()?;
        let required = data.len() as u64;
        let mut state = self.state.lock();
        let replaced = state.index.entries.get(key).cloned();
        let committed_victims = apply_pending_eviction(&mut state, pending);

        let mut candidate_free = state.free_extents.clone();
        for entry in &committed_victims {
            insert_free_extent(&mut candidate_free, entry.offset, entry.len);
        }
        let Some((offset, candidate_next_offset)) = allocate_extent(
            &mut candidate_free,
            state.index.next_offset,
            state.quota_bytes,
            required,
        ) else {
            self.finalize_evicted_entries(&mut state, committed_victims)?;
            return Err(StoreError::NoAvailableHandle);
        };

        let write_result = (|| -> StoreResult<()> {
            let mut data_file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(self.data_path())?;
            data_file.seek(SeekFrom::Start(offset))?;
            data_file.write_all(data)?;
            data_file.sync_data()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            self.finalize_evicted_entries(&mut state, committed_victims)?;
            return Err(error);
        }

        state.free_extents = candidate_free;
        state.index.next_offset = candidate_next_offset;

        if let Some(old) = replaced {
            state.index.entries.remove(key);
            state.used_bytes = state.used_bytes.saturating_sub(old.len);
            insert_free_extent(&mut state.free_extents, old.offset, old.len);
        }
        let fifo_seq = state.index.next_fifo_seq;
        state.index.next_fifo_seq = state.index.next_fifo_seq.saturating_add(1);
        state.index.entries.insert(
            key.to_string(),
            OffsetEntry {
                offset,
                len: required,
                fifo_seq,
            },
        );
        state.used_bytes = state.used_bytes.saturating_add(required);
        self.persist_index(&state.index)?;
        Ok(())
    }

    pub fn read_object(&self, key: &str) -> StoreResult<Vec<u8>> {
        self.ensure_init()?;
        let entry = self
            .state
            .lock()
            .index
            .entries
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::KeyNotFound(key.to_string()))?;
        let mut file = std::fs::File::open(self.data_path())?;
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut value = vec![0; entry.len as usize];
        file.read_exact(&mut value)?;
        Ok(value)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.initialized.load(Ordering::Acquire)
            && self.state.lock().index.entries.contains_key(key)
    }

    pub fn space_usage(&self) -> (u64, u64) {
        let state = self.state.lock();
        (state.used_bytes, state.quota_bytes)
    }

    pub fn prepare_watermark_eviction(
        &self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<PendingOffsetEviction> {
        self.ensure_init()?;
        if !(0.0 < low_watermark_ratio
            && low_watermark_ratio < high_watermark_ratio
            && high_watermark_ratio <= 1.0)
        {
            return Err(StoreError::InvalidParams(
                "watermarks must satisfy 0 < low < high <= 1".to_string(),
            ));
        }
        let state = self.state.lock();
        let high = (state.quota_bytes as f64 * high_watermark_ratio) as u64;
        if state.used_bytes <= high {
            return Ok(PendingOffsetEviction::default());
        }
        let low = (state.quota_bytes as f64 * low_watermark_ratio) as u64;
        let mut pending = PendingOffsetEviction::default();
        let mut projected_used = state.used_bytes;
        while projected_used > low && pending.victims.len() < self.config.max_evict_per_offload {
            let victim = state
                .index
                .entries
                .iter()
                .filter(|(candidate, _)| {
                    !pending
                        .victims
                        .iter()
                        .any(|(selected, _)| selected == *candidate)
                })
                .min_by_key(|(_, entry)| entry.fifo_seq)
                .map(|(key, entry)| (key.clone(), entry.clone()));
            let Some((victim, entry)) = victim else {
                break;
            };
            projected_used = projected_used.saturating_sub(entry.len);
            pending.victims.push((victim, entry));
        }
        Ok(pending)
    }

    pub fn commit_eviction(&self, pending: PendingOffsetEviction) -> StoreResult<()> {
        self.ensure_init()?;
        let mut state = self.state.lock();
        let committed_victims = apply_pending_eviction(&mut state, pending);
        self.finalize_evicted_entries(&mut state, committed_victims)
    }

    pub fn delete_object(&self, key: &str) -> StoreResult<()> {
        self.ensure_init()?;
        let mut state = self.state.lock();
        if let Some(entry) = state.index.entries.remove(key) {
            state.used_bytes = state.used_bytes.saturating_sub(entry.len);
            insert_free_extent(&mut state.free_extents, entry.offset, entry.len);
            self.persist_index(&state.index)?;
        }
        Ok(())
    }

    pub fn remove_all(&self) -> StoreResult<usize> {
        self.ensure_init()?;
        let mut state = self.state.lock();
        let count = state.index.entries.len();
        state.index = PersistedIndex::default();
        state.free_extents.clear();
        state.used_bytes = 0;
        match std::fs::remove_file(self.data_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        match std::fs::remove_file(self.index_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(count)
    }

    fn persist_index(&self, index: &PersistedIndex) -> StoreResult<()> {
        let path = self.index_path();
        let temporary = path.with_extension("json.tmp");
        let mut writer = std::io::BufWriter::new(std::fs::File::create(&temporary)?);
        serde_json::to_writer(&mut writer, index)?;
        writer.flush()?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }

    fn finalize_evicted_entries(
        &self,
        state: &mut OffsetState,
        entries: Vec<OffsetEntry>,
    ) -> StoreResult<()> {
        for entry in entries {
            insert_free_extent(&mut state.free_extents, entry.offset, entry.len);
        }
        self.persist_index(&state.index)
    }
}

fn key_watermarks(config: &OffsetAllocatorConfig) -> (usize, usize) {
    let high = (config.total_keys_limit as f64 * config.keys_high_ratio) as usize;
    let mut low = (config.total_keys_limit as f64 * config.keys_low_ratio) as usize;
    if high > 0 && low >= high {
        low = 1.max(high - 1);
    }
    (high, low)
}

fn normalize_loaded_index(index: &mut PersistedIndex, file_len: u64) -> bool {
    let occupied_end = index
        .entries
        .values()
        .map(|entry| entry.offset.saturating_add(entry.len))
        .max()
        .unwrap_or(0);
    let safe_next_offset = index.next_offset.max(occupied_end).max(file_len);
    let mut changed = safe_next_offset != index.next_offset;
    index.next_offset = safe_next_offset;

    if !index.entries.is_empty() && index.next_fifo_seq == 0 {
        let mut keys_by_offset: Vec<_> = index.entries.keys().cloned().collect();
        keys_by_offset.sort_by_key(|key| index.entries[key].offset);
        for (fifo_seq, key) in keys_by_offset.into_iter().enumerate() {
            index.entries.get_mut(&key).unwrap().fifo_seq = fifo_seq as u64;
        }
        index.next_fifo_seq = index.entries.len() as u64;
        changed = true;
    }

    changed
}

fn rebuild_free_extents(index: &PersistedIndex, file_len: u64) -> BTreeMap<u64, u64> {
    let mut entries: Vec<_> = index.entries.values().collect();
    entries.sort_by_key(|entry| entry.offset);
    let mut free = BTreeMap::new();
    let mut cursor = 0;
    for entry in entries {
        if cursor < entry.offset {
            free.insert(cursor, entry.offset - cursor);
        }
        cursor = cursor.max(entry.offset.saturating_add(entry.len));
    }
    if cursor < file_len {
        free.insert(cursor, file_len - cursor);
    }
    free
}

fn allocate_extent(
    free_extents: &mut BTreeMap<u64, u64>,
    next_offset: u64,
    quota_bytes: u64,
    required: u64,
) -> Option<(u64, u64)> {
    let reusable = free_extents
        .iter()
        .find(|(_, len)| **len >= required)
        .map(|(offset, len)| (*offset, *len));
    if let Some((offset, len)) = reusable {
        free_extents.remove(&offset);
        if len > required {
            free_extents.insert(offset + required, len - required);
        }
        return Some((offset, next_offset));
    }
    if next_offset.saturating_add(required) > quota_bytes {
        return None;
    }
    Some((next_offset, next_offset.saturating_add(required)))
}

fn apply_pending_eviction(
    state: &mut OffsetState,
    pending: PendingOffsetEviction,
) -> Vec<OffsetEntry> {
    let mut removed = Vec::with_capacity(pending.victims.len());
    for (key, expected) in pending.victims {
        if state.index.entries.get(&key) != Some(&expected) {
            continue;
        }
        let entry = state.index.entries.remove(&key).unwrap();
        state.used_bytes = state.used_bytes.saturating_sub(entry.len);
        removed.push(entry);
    }
    removed
}

fn insert_free_extent(extents: &mut BTreeMap<u64, u64>, offset: u64, len: u64) {
    if len == 0 {
        return;
    }
    let mut start = offset;
    let mut length = len;
    if let Some((&previous_offset, &previous_len)) = extents.range(..offset).next_back() {
        if previous_offset.saturating_add(previous_len) == offset {
            start = previous_offset;
            length = length.saturating_add(previous_len);
            extents.remove(&previous_offset);
        }
    }
    if let Some((&next_offset, &next_len)) = extents.range(start..).next() {
        if start.saturating_add(length) == next_offset {
            length = length.saturating_add(next_len);
            extents.remove(&next_offset);
        }
    }
    extents.insert(start, length);
}

use super::{OffsetAllocatorConfig, OffsetEvictionPolicy};
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

// Task 1 establishes these primitives before Tasks 2-3 wire them into I/O.
#[allow(dead_code)]
const RECORD_HEADER_SIZE: usize = 24;
#[allow(dead_code)]
const RECORD_HEADER_PREFIX_SIZE: usize = 20;
#[allow(dead_code)]
const RECORD_FLAG_HAS_CRC: u32 = 1;
#[allow(dead_code)]
const RECORD_KNOWN_FLAGS: u32 = RECORD_FLAG_HAS_CRC;
#[allow(dead_code)]
const RECORD_VALUE_ALIGNMENT: u64 = 4096;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordHeader {
    key_len: u32,
    value_len: u32,
    write_seq: u64,
    flags: u32,
    crc32: u32,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordFormatError(&'static str);

#[allow(dead_code)]
impl RecordHeader {
    fn encode_prefix(&self) -> [u8; RECORD_HEADER_PREFIX_SIZE] {
        let mut encoded = [0; RECORD_HEADER_PREFIX_SIZE];
        encoded[0..4].copy_from_slice(&self.key_len.to_le_bytes());
        encoded[4..8].copy_from_slice(&self.value_len.to_le_bytes());
        encoded[8..16].copy_from_slice(&self.write_seq.to_le_bytes());
        encoded[16..20].copy_from_slice(&self.flags.to_le_bytes());
        encoded
    }

    fn encode(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut encoded = [0; RECORD_HEADER_SIZE];
        encoded[..RECORD_HEADER_PREFIX_SIZE].copy_from_slice(&self.encode_prefix());
        encoded[RECORD_HEADER_PREFIX_SIZE..].copy_from_slice(&self.crc32.to_le_bytes());
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, RecordFormatError> {
        if encoded.len() < RECORD_HEADER_SIZE {
            return Err(RecordFormatError("truncated record header"));
        }
        let header = Self {
            key_len: u32::from_le_bytes(encoded[0..4].try_into().unwrap()),
            value_len: u32::from_le_bytes(encoded[4..8].try_into().unwrap()),
            write_seq: u64::from_le_bytes(encoded[8..16].try_into().unwrap()),
            flags: u32::from_le_bytes(encoded[16..20].try_into().unwrap()),
            crc32: u32::from_le_bytes(encoded[20..24].try_into().unwrap()),
        };
        if header.flags & !RECORD_KNOWN_FLAGS != 0 {
            return Err(RecordFormatError("record header has unknown flags"));
        }
        header.checked_record_size()?;
        Ok(header)
    }

    fn value_padding(key_len: u64) -> Result<u64, RecordFormatError> {
        if key_len > u32::MAX.into() {
            return Err(RecordFormatError("record key length exceeds u32"));
        }
        let head = (RECORD_HEADER_SIZE as u64)
            .checked_add(key_len)
            .ok_or(RecordFormatError("record size overflow"))?;
        Ok((RECORD_VALUE_ALIGNMENT - head % RECORD_VALUE_ALIGNMENT) % RECORD_VALUE_ALIGNMENT)
    }

    fn value_offset(key_len: u64) -> Result<u64, RecordFormatError> {
        let padding = Self::value_padding(key_len)?;
        (RECORD_HEADER_SIZE as u64)
            .checked_add(key_len)
            .and_then(|head| head.checked_add(padding))
            .ok_or(RecordFormatError("record size overflow"))
    }

    fn record_size(key_len: u64, value_len: u64) -> Result<u64, RecordFormatError> {
        if value_len > u32::MAX.into() {
            return Err(RecordFormatError("record value length exceeds u32"));
        }
        let record_size = Self::value_offset(key_len)?
            .checked_add(value_len)
            .ok_or(RecordFormatError("record size overflow"))?;
        if record_size > u32::MAX.into() {
            return Err(RecordFormatError("record size exceeds u32"));
        }
        Ok(record_size)
    }

    fn checked_record_size(&self) -> Result<u64, RecordFormatError> {
        Self::record_size(self.key_len.into(), self.value_len.into())
    }

    fn validate_extent(&self, record_offset: u64, arena_len: u64) -> Result<(), RecordFormatError> {
        let record_end = record_offset
            .checked_add(self.checked_record_size()?)
            .ok_or(RecordFormatError("record extent overflow"))?;
        if record_end > arena_len {
            return Err(RecordFormatError("record extent is out of bounds"));
        }
        Ok(())
    }

    fn verify_crc(&self, key: &[u8], value: &[u8]) -> Result<(), RecordFormatError> {
        if key.len() != self.key_len as usize || value.len() != self.value_len as usize {
            return Err(RecordFormatError("record lengths do not match header"));
        }
        if self.flags & RECORD_FLAG_HAS_CRC != 0
            && crc32c([self.encode_prefix().as_slice(), key, value]) != self.crc32
        {
            return Err(RecordFormatError("record CRC-32C mismatch"));
        }
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
struct Crc32c(u32);

#[allow(dead_code)]
impl Crc32c {
    fn new() -> Self {
        Self(u32::MAX)
    }

    fn extend(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                self.0 = if self.0 & 1 != 0 {
                    0x82f6_3b78 ^ (self.0 >> 1)
                } else {
                    self.0 >> 1
                };
            }
        }
    }

    fn finish(self) -> u32 {
        self.0 ^ u32::MAX
    }
}

#[allow(dead_code)]
fn crc32c<I, B>(chunks: I) -> u32
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    let mut crc = Crc32c::new();
    for chunk in chunks {
        crc.extend(chunk.as_ref());
    }
    crc.finish()
}

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

const CHECKPOINT_FORMAT: &str = "mooncake-offset-allocator-checkpoint";
const CHECKPOINT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointPayload {
    index: PersistedIndex,
    #[serde(default)]
    next_write_seq: u64,
    #[serde(default)]
    tombstones: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointEnvelope {
    format: String,
    version: u32,
    payload_crc32c: u32,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointBoundary {
    Write,
    FileSync,
    Rename,
    DirectorySync,
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

        remove_stale_checkpoint_tmp(&self.index_path())?;
        let mut index = load_persisted_index(&self.index_path())?;
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
        write_checkpoint(&self.index_path(), index)
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

fn checkpoint_tmp_path(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
}

fn remove_stale_checkpoint_tmp(path: &Path) -> StoreResult<()> {
    match std::fs::remove_file(checkpoint_tmp_path(path)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn canonical_json_bytes(value: &serde_json::Value) -> StoreResult<Vec<u8>> {
    fn canonicalize(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(canonicalize).collect())
            }
            serde_json::Value::Object(values) => {
                let sorted = values
                    .iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect();
                serde_json::Value::Object(sorted)
            }
            other => other.clone(),
        }
    }

    Ok(serde_json::to_vec(&canonicalize(value))?)
}

fn checkpoint_envelope(index: &PersistedIndex) -> StoreResult<CheckpointEnvelope> {
    let payload = serde_json::to_value(CheckpointPayload {
        index: PersistedIndex {
            entries: index.entries.clone(),
            next_offset: index.next_offset,
            next_fifo_seq: index.next_fifo_seq,
        },
        next_write_seq: 0,
        tombstones: Vec::new(),
    })?;
    Ok(CheckpointEnvelope {
        format: CHECKPOINT_FORMAT.to_string(),
        version: CHECKPOINT_VERSION,
        payload_crc32c: crc32c([canonical_json_bytes(&payload)?]),
        payload,
    })
}

fn load_persisted_index(path: &Path) -> StoreResult<PersistedIndex> {
    let mut bytes = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut file) => file.read_to_end(&mut bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PersistedIndex::default());
        }
        Err(error) => return Err(error.into()),
    };

    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Ok(PersistedIndex::default());
    };
    if value.get("format").and_then(serde_json::Value::as_str) != Some(CHECKPOINT_FORMAT) {
        return Ok(serde_json::from_value(value).unwrap_or_default());
    }

    let Ok(envelope) = serde_json::from_value::<CheckpointEnvelope>(value) else {
        return Ok(PersistedIndex::default());
    };
    if envelope.version != CHECKPOINT_VERSION {
        return Ok(PersistedIndex::default());
    }
    let Ok(payload_bytes) = canonical_json_bytes(&envelope.payload) else {
        return Ok(PersistedIndex::default());
    };
    if crc32c([payload_bytes]) != envelope.payload_crc32c {
        return Ok(PersistedIndex::default());
    }
    Ok(
        serde_json::from_value::<CheckpointPayload>(envelope.payload)
            .map(|payload| payload.index)
            .unwrap_or_default(),
    )
}

fn write_checkpoint(path: &Path, index: &PersistedIndex) -> StoreResult<()> {
    write_checkpoint_with_hook(path, index, |_| Ok(()))
}

fn write_checkpoint_with_hook(
    path: &Path,
    index: &PersistedIndex,
    mut boundary_hook: impl FnMut(CheckpointBoundary) -> std::io::Result<()>,
) -> StoreResult<()> {
    let temporary = checkpoint_tmp_path(path);
    let bytes = serde_json::to_vec(&checkpoint_envelope(index)?)?;
    let result = (|| -> StoreResult<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary)?;
        boundary_hook(CheckpointBoundary::Write)?;
        file.write_all(&bytes)?;
        boundary_hook(CheckpointBoundary::FileSync)?;
        file.sync_all()?;
        drop(file);
        boundary_hook(CheckpointBoundary::Rename)?;
        std::fs::rename(&temporary, path)?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let directory = std::fs::File::open(parent)?;
        boundary_hook(CheckpointBoundary::DirectorySync)?;
        directory.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
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

#[cfg(test)]
mod checkpoint_tests {
    use super::{
        CHECKPOINT_VERSION, CheckpointBoundary, OffsetEntry, PersistedIndex, checkpoint_envelope,
        checkpoint_tmp_path, load_persisted_index, remove_stale_checkpoint_tmp, write_checkpoint,
        write_checkpoint_with_hook,
    };
    use std::collections::HashMap;
    use std::io::Error;

    fn index(key: &str, offset: u64) -> PersistedIndex {
        PersistedIndex {
            entries: HashMap::from([(
                key.to_string(),
                OffsetEntry {
                    offset,
                    len: 4,
                    fifo_seq: offset,
                },
            )]),
            next_offset: offset + 4,
            next_fifo_seq: offset + 1,
        }
    }

    #[test]
    fn checkpoint_round_trip_verifies_payload_crc() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let expected = index("a", 8);

        write_checkpoint(&path, &expected).unwrap();

        assert_eq!(
            load_persisted_index(&path).unwrap().entries,
            expected.entries
        );
    }

    #[test]
    fn unsupported_corrupt_and_malformed_checkpoints_fall_back_without_rewriting() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let expected = index("a", 8);

        let mut unsupported =
            serde_json::to_value(checkpoint_envelope(&expected).unwrap()).unwrap();
        unsupported["version"] = serde_json::json!(CHECKPOINT_VERSION + 1);
        let unsupported_bytes = serde_json::to_vec(&unsupported).unwrap();
        std::fs::write(&path, &unsupported_bytes).unwrap();
        assert!(load_persisted_index(&path).unwrap().entries.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), unsupported_bytes);

        let mut corrupt = serde_json::to_value(checkpoint_envelope(&expected).unwrap()).unwrap();
        corrupt["payload"]["index"]["next_offset"] = serde_json::json!(99);
        let corrupt_bytes = serde_json::to_vec(&corrupt).unwrap();
        std::fs::write(&path, &corrupt_bytes).unwrap();
        assert!(load_persisted_index(&path).unwrap().entries.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), corrupt_bytes);

        let malformed = b"{not-json".to_vec();
        std::fs::write(&path, &malformed).unwrap();
        assert!(load_persisted_index(&path).unwrap().entries.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), malformed);
    }

    #[test]
    fn legacy_raw_value_index_loads_without_touching_arena_or_index() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let arena = temp.path().join("offset_allocator.data");
        let legacy = br#"{"entries":{"a":{"offset":0,"len":4,"fifo_seq":3}},"next_offset":4,"next_fifo_seq":4}"#;
        std::fs::write(&path, legacy).unwrap();
        std::fs::write(&arena, b"aaaa").unwrap();

        let loaded = load_persisted_index(&path).unwrap();

        assert_eq!(loaded.entries["a"].offset, 0);
        assert_eq!(loaded.entries["a"].len, 4);
        assert_eq!(std::fs::read(&path).unwrap(), legacy);
        assert_eq!(std::fs::read(&arena).unwrap(), b"aaaa");
    }

    #[test]
    fn stale_checkpoint_tmp_is_removed_without_touching_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let expected = index("a", 0);
        write_checkpoint(&path, &expected).unwrap();
        std::fs::write(checkpoint_tmp_path(&path), b"partial").unwrap();

        remove_stale_checkpoint_tmp(&path).unwrap();

        assert!(!checkpoint_tmp_path(&path).exists());
        assert_eq!(
            load_persisted_index(&path).unwrap().entries,
            expected.entries
        );
    }

    #[test]
    fn checkpoint_boundaries_are_ordered_and_failures_leave_a_usable_checkpoint() {
        for failure in [
            CheckpointBoundary::Write,
            CheckpointBoundary::FileSync,
            CheckpointBoundary::Rename,
            CheckpointBoundary::DirectorySync,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("offset_allocator.index.json");
            let previous = index("previous", 0);
            let replacement = index("replacement", 8);
            write_checkpoint(&path, &previous).unwrap();
            let mut observed = Vec::new();

            let result = write_checkpoint_with_hook(&path, &replacement, |boundary| {
                observed.push(boundary);
                if boundary == failure {
                    Err(Error::other("injected checkpoint failure"))
                } else {
                    Ok(())
                }
            });

            assert!(result.is_err(), "{failure:?}");
            let recovered = load_persisted_index(&path).unwrap();
            if failure == CheckpointBoundary::DirectorySync {
                assert_eq!(recovered.entries, replacement.entries);
            } else {
                assert_eq!(recovered.entries, previous.entries);
            }
            let expected_prefix = [
                CheckpointBoundary::Write,
                CheckpointBoundary::FileSync,
                CheckpointBoundary::Rename,
                CheckpointBoundary::DirectorySync,
            ];
            let failure_index = expected_prefix
                .iter()
                .position(|boundary| *boundary == failure)
                .unwrap();
            assert_eq!(observed, expected_prefix[..=failure_index]);
            assert!(!checkpoint_tmp_path(&path).exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn transient_open_error_is_returned_without_truncating_arena() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let arena = temp.path().join("offset_allocator.data");
        symlink("offset_allocator.index.json", &path).unwrap();
        std::fs::write(&arena, b"recoverable-arena").unwrap();

        assert!(load_persisted_index(&path).is_err());
        assert_eq!(std::fs::read(&arena).unwrap(), b"recoverable-arena");
        assert!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_permission_error_is_returned_without_rewriting_files() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("offset_allocator.index.json");
        let arena = temp.path().join("offset_allocator.data");
        let checkpoint = b"recoverable-checkpoint";
        std::fs::write(&path, checkpoint).unwrap();
        std::fs::write(&arena, b"recoverable-arena").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0)).unwrap();

        let result = load_persisted_index(&path);

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(result.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), checkpoint);
        assert_eq!(std::fs::read(&arena).unwrap(), b"recoverable-arena");
    }
}

#[cfg(test)]
mod record_primitive_tests {
    use super::{
        RECORD_FLAG_HAS_CRC, RECORD_HEADER_PREFIX_SIZE, RECORD_HEADER_SIZE, RecordHeader, crc32c,
    };

    #[test]
    fn crc32c_matches_castagnoli_check_vector_and_streaming() {
        assert_eq!(crc32c([b"123", b"456", b"789"]), 0xe306_9283);
        assert_eq!(crc32c([b"".as_slice()]), 0);
    }

    #[test]
    fn record_header_uses_explicit_little_endian_v3_layout() {
        let header = RecordHeader {
            key_len: 0x0102_0304,
            value_len: 0x1112_1314,
            write_seq: 0x2122_2324_2526_2728,
            flags: RECORD_FLAG_HAS_CRC,
            crc32: 0x3132_3334,
        };

        let encoded = header.encode();
        assert_eq!(RECORD_HEADER_SIZE, 24);
        assert_eq!(RECORD_HEADER_PREFIX_SIZE, 20);
        assert_eq!(
            encoded,
            [
                0x04, 0x03, 0x02, 0x01, 0x14, 0x13, 0x12, 0x11, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23,
                0x22, 0x21, 0x01, 0x00, 0x00, 0x00, 0x34, 0x33, 0x32, 0x31,
            ]
        );
        assert_eq!(RecordHeader::decode(&encoded).unwrap(), header);
    }

    #[test]
    fn record_layout_aligns_values_to_four_kibibytes() {
        assert_eq!(RecordHeader::value_padding(4072).unwrap(), 0);
        assert_eq!(RecordHeader::value_padding(4073).unwrap(), 4095);
        assert_eq!(RecordHeader::value_offset(0).unwrap(), 4096);
        assert_eq!(RecordHeader::record_size(5, 1000).unwrap(), 5096);
        assert_eq!(RecordHeader::value_offset(5).unwrap() % 4096, 0);
    }

    #[test]
    fn record_header_rejects_truncation_unknown_flags_and_size_overflow() {
        assert!(RecordHeader::decode(&[0; RECORD_HEADER_SIZE - 1]).is_err());

        let mut unknown_flags = [0u8; RECORD_HEADER_SIZE];
        unknown_flags[16..20].copy_from_slice(&2u32.to_le_bytes());
        assert!(RecordHeader::decode(&unknown_flags).is_err());

        assert!(RecordHeader::value_offset(u64::MAX).is_err());
        assert!(RecordHeader::record_size(0, u64::MAX).is_err());
    }

    #[test]
    fn record_header_checks_extent_bounds_without_truncating_lengths() {
        let header = RecordHeader {
            key_len: 5,
            value_len: 1000,
            write_seq: 7,
            flags: 0,
            crc32: 0,
        };

        assert_eq!(header.checked_record_size().unwrap(), 5096);
        assert!(header.validate_extent(100, 5196).is_ok());
        assert!(header.validate_extent(100, 5195).is_err());
        assert!(header.validate_extent(u64::MAX - 10, u64::MAX).is_err());
    }

    #[test]
    fn record_header_caps_total_record_size_at_u32_max() {
        let max_value_len = u32::MAX - 4096;
        let max_header = RecordHeader {
            key_len: 0,
            value_len: max_value_len,
            write_seq: 7,
            flags: 0,
            crc32: 0,
        };
        let oversized_header = RecordHeader {
            value_len: max_value_len + 1,
            ..max_header
        };

        assert_eq!(
            RecordHeader::record_size(0, u64::from(max_value_len)).unwrap(),
            u64::from(u32::MAX)
        );
        assert_eq!(
            max_header.checked_record_size().unwrap(),
            u64::from(u32::MAX)
        );
        assert_eq!(
            RecordHeader::decode(&max_header.encode()).unwrap(),
            max_header
        );
        assert!(max_header.validate_extent(0, u64::from(u32::MAX)).is_ok());

        assert!(RecordHeader::record_size(0, u64::from(max_value_len) + 1).is_err());
        assert!(oversized_header.checked_record_size().is_err());
        assert!(RecordHeader::decode(&oversized_header.encode()).is_err());
        assert!(
            oversized_header
                .validate_extent(0, u64::from(u32::MAX) + 1)
                .is_err()
        );
    }

    #[test]
    fn record_crc_covers_prefix_key_and_value_but_not_crc_field_or_padding() {
        let mut header = RecordHeader {
            key_len: 3,
            value_len: 5,
            write_seq: 9,
            flags: RECORD_FLAG_HAS_CRC,
            crc32: 0,
        };
        let prefix = header.encode_prefix();
        let expected = crc32c([prefix.as_slice(), b"key", b"value"]);

        header.crc32 = expected;
        assert!(header.verify_crc(b"key", b"value").is_ok());
        assert!(header.verify_crc(b"key", b"Value").is_err());
    }
}

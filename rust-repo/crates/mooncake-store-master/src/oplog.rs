use crate::ha::{HaError, OpLogPollResult, OpLogRecord};
use serde_json::json;
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use tracing::warn;
use uuid::Uuid;

/// Trait for persistent operation log stores.
pub trait OpLogStore: Send + Sync {
    /// Append an entry and return its assigned sequence number.
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError>;

    /// Read entries starting from `since_seq` (inclusive), up to `max_count`.
    fn read_since(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError>;

    /// Get the latest committed sequence number.
    fn latest_sequence(&self) -> u64;

    /// Poll for entries starting from `since_seq`.
    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult;
}

/// In-memory oplog with bounded capacity — used when no persistence backend is configured.
pub struct InMemoryOpLog {
    buffer: VecDeque<OpLogRecord>,
    last_seq: u64,
    max_entries: usize,
}

impl InMemoryOpLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            buffer: VecDeque::new(),
            last_seq: 0,
            max_entries: max_entries.max(1),
        }
    }
}

impl InMemoryOpLog {
    /// Convenience: append a payload with `producer_view_version` and return the assigned seq.
    pub fn append_payload(
        &mut self,
        producer_view_version: u64,
        payload: impl Into<String>,
    ) -> u64 {
        match self.append(&OpLogRecord {
            seq: 0,
            producer_view_version,
            payload: payload.into(),
        }) {
            Ok(seq) => seq,
            Err(_) => 0,
        }
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

impl OpLogStore for InMemoryOpLog {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq += 1;
        if self.buffer.len() >= self.max_entries {
            self.buffer.pop_front();
        }
        self.buffer.push_back(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        Ok(self.last_seq)
    }

    fn read_since(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError> {
        Ok(self
            .buffer
            .iter()
            .filter(|r| r.seq >= since_seq)
            .take(max_count)
            .cloned()
            .collect())
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records.last().map(|r| r.seq + 1).unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }
}

/// Persistent oplog store backed by segmented files on the local filesystem.
///
/// Each segment is a file `oplog_<start_seq>.bin`. Entries are written with
/// length-prefixed framing: [4-byte seq LE][4-byte payload_len LE][payload].
/// Atomic writes are achieved by writing to a `.tmp` file then renaming
/// (the containing directory is fsync'd after rename for durability).
pub struct LocalFsOpLogStore {
    dir: PathBuf,
    /// Maximum entries per segment file.
    max_entries_per_segment: usize,
    /// Entries accumulated in the current (not yet flushed) segment.
    buffer: Vec<OpLogRecord>,
    last_seq: u64,
    current_segment_seq: u64,
    /// Channel to send full buffers to the background flush thread.
    flush_tx: mpsc::Sender<Vec<OpLogRecord>>,
    /// Background flush thread handle — joined on drop.
    _flush_handle: Option<std::thread::JoinHandle<()>>,
}

impl LocalFsOpLogStore {
    pub fn new(dir: &Path, max_entries_per_segment: usize) -> Result<Self, HaError> {
        fs::create_dir_all(dir).map_err(|e| {
            HaError::InvalidBackend(format!("oplog dir create: {e}"))
        })?;

        let dir_buf = dir.to_path_buf();
        let (flush_tx, flush_rx) = mpsc::channel::<Vec<OpLogRecord>>();

        // Background thread: writes full buffers to disk without blocking callers.
        let flush_dir = dir_buf.clone();
        let flush_handle = std::thread::spawn(move || {
            for entries in flush_rx {
                if let Err(e) = Self::flush_inner(&flush_dir, &entries) {
                    warn!("oplog background flush failed: {e}");
                }
            }
        });

        let mut store = Self {
            dir: dir_buf,
            max_entries_per_segment: max_entries_per_segment.max(1000),
            buffer: Vec::new(),
            last_seq: 0,
            current_segment_seq: 0,
            flush_tx,
            _flush_handle: Some(flush_handle),
        };
        // Recover latest sequence from existing segment files
        store.recover()?;
        Ok(store)
    }

    /// Recover `last_seq` by scanning the highest-numbered segment file.
    fn recover(&mut self) -> Result<(), HaError> {
        let mut segments: Vec<u64> = self.list_segment_files()?;
        segments.sort();
        if let Some(&highest_start) = segments.last() {
            let path = self.segment_path(highest_start);
            let data = fs::read(&path).map_err(|e| {
                HaError::InvalidBackend(format!("oplog read segment: {e}"))
            })?;
            let entries = Self::parse_entries(&data);
            if let Some(last) = entries.last() {
                self.last_seq = last.seq;
                self.current_segment_seq = highest_start;
            }
        }
        Ok(())
    }

    fn list_segment_files(&self) -> Result<Vec<u64>, HaError> {
        let mut segments = Vec::new();
        let dir_entries = fs::read_dir(&self.dir).map_err(|e| {
            HaError::InvalidBackend(format!("oplog read dir: {e}"))
        })?;
        for entry in dir_entries {
            let entry = entry.map_err(|e| {
                HaError::InvalidBackend(format!("oplog dir entry: {e}"))
            })?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) =
                name_str.strip_prefix("oplog_").and_then(|s| s.strip_suffix(".bin"))
            {
                if let Ok(seq) = rest.parse::<u64>() {
                    segments.push(seq);
                }
            }
        }
        Ok(segments)
    }

    fn segment_path(&self, start_seq: u64) -> PathBuf {
        self.dir.join(format!("oplog_{:020}.bin", start_seq))
    }

    /// Write buffer to a segment file atomically (synchronous, used by background
    /// thread and explicit flush() calls).
    fn flush_inner(dir: &Path, entries: &[OpLogRecord]) -> Result<(), HaError> {
        if entries.is_empty() {
            return Ok(());
        }
        let start_seq = entries.first().unwrap().seq;
        let tmp_path = dir.join(format!("oplog_{:020}.tmp", start_seq));
        let final_path = dir.join(format!("oplog_{:020}.bin", start_seq));

        let mut data = Vec::with_capacity(entries.len() * 128);
        for entry in entries {
            let payload = entry.payload.as_bytes();
            data.extend_from_slice(&(entry.seq as u32).to_le_bytes());
            data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            data.extend_from_slice(payload);
        }

        fs::write(&tmp_path, &data).map_err(|e| {
            HaError::InvalidBackend(format!("oplog write segment: {e}"))
        })?;
        fs::rename(&tmp_path, &final_path).map_err(|e| {
            HaError::InvalidBackend(format!("oplog rename segment: {e}"))
        })?;
        // fsync parent directory for durability
        if let Ok(f) = fs::File::open(dir) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    /// Write buffer to a segment file atomically.
    fn flush(&mut self) -> Result<(), HaError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        Self::flush_inner(&self.dir, &self.buffer)?;
        self.current_segment_seq = self.buffer.first().map(|e| e.seq).unwrap_or(0);
        self.buffer.clear();
        Ok(())
    }

    fn parse_entries(data: &[u8]) -> Vec<OpLogRecord> {
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset + 8 <= data.len() {
            let seq = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as u64;
            let payload_len = u32::from_le_bytes([
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]) as usize;
            offset += 8;
            if offset + payload_len > data.len() {
                break;
            }
            let payload =
                String::from_utf8_lossy(&data[offset..offset + payload_len]).into_owned();
            entries.push(OpLogRecord {
                seq,
                producer_view_version: 0,
                payload,
            });
            offset += payload_len;
        }
        entries
    }
}

impl OpLogStore for LocalFsOpLogStore {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq += 1;
        self.buffer.push(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        if self.buffer.len() >= self.max_entries_per_segment {
            let to_flush = std::mem::replace(&mut self.buffer, Vec::new());
            match self.flush_tx.send(to_flush) {
                Ok(()) => {}
                Err(mpsc::SendError(entries)) => {
                    // Channel closed — flush inline for safety.
                    warn!("oplog flush channel closed, falling back to sync flush");
                    self.buffer = entries;
                    self.flush()?;
                }
            }
        }
        Ok(self.last_seq)
    }

    fn read_since(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError> {
        let segments = self.list_segment_files()?;
        let mut all_entries = Vec::new();

        // Read from segment files
        let mut sorted_segs = segments;
        sorted_segs.sort();
        for start_seq in sorted_segs {
            if start_seq > since_seq + 100_000 {
                break; // optimization: don't read far-ahead segments
            }
            let data = fs::read(self.segment_path(start_seq)).map_err(|e| {
                HaError::InvalidBackend(format!("oplog read segment: {e}"))
            })?;
            for entry in Self::parse_entries(&data) {
                if entry.seq >= since_seq {
                    all_entries.push(entry);
                    if all_entries.len() >= max_count {
                        return Ok(all_entries);
                    }
                }
            }
        }

        // Also include buffered (unflushed) entries
        for entry in &self.buffer {
            if entry.seq >= since_seq {
                all_entries.push(entry.clone());
                if all_entries.len() >= max_count {
                    break;
                }
            }
        }

        all_entries.truncate(max_count);
        Ok(all_entries)
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records.last().map(|r| r.seq + 1).unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }
}

// =============================================================================
// Etcd-backed OpLog Store
// =============================================================================

/// Persistent oplog store backed by etcd.
///
/// Each entry is stored as a key `/oplog/<prefix>/seq_<seq:020>`. Zero-padded
/// sequence numbers ensure lexicographic ordering. A separate `/oplog/<prefix>/latest`
/// key stores the most recent sequence number for fast recovery.
pub struct EtcdOpLogStore {
    client: etcd_client::Client,
    key_prefix: String,
    last_seq: u64,
    /// Entries accumulated for batch write.
    buffer: Vec<OpLogRecord>,
    /// Flush the buffer after this many entries.
    #[allow(dead_code)]
    batch_size: usize,
}

impl EtcdOpLogStore {
    pub async fn new(
        client: etcd_client::Client,
        key_prefix: &str,
        batch_size: usize,
    ) -> Result<Self, HaError> {
        let prefix = key_prefix.trim_end_matches('/').to_string();
        let mut store = Self {
            client,
            key_prefix: prefix,
            last_seq: 0,
            buffer: Vec::new(),
            batch_size: batch_size.max(1),
        };
        store.recover().await?;
        Ok(store)
    }

    /// Recover `last_seq` from the `/latest` key.
    async fn recover(&mut self) -> Result<(), HaError> {
        let latest_key = format!("{}/latest", self.key_prefix);
        let c = self.client.clone();
        match c
            .kv_client()
            .get(latest_key.as_bytes(), None)
            .await
        {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    if let Ok(val) = String::from_utf8(kv.value().to_vec()) {
                        self.last_seq = val.parse::<u64>().unwrap_or(0);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read oplog latest key: {}", e);
            }
        }
        Ok(())
    }

    fn entry_key(&self, seq: u64) -> String {
        format!("{}/seq_{:020}", self.key_prefix, seq)
    }

    fn latest_key(&self) -> String {
        format!("{}/latest", self.key_prefix)
    }

    /// Write the buffer to etcd.
    async fn flush(&mut self) -> Result<(), HaError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let c = self.client.clone();
        for entry in &self.buffer {
            let key = self.entry_key(entry.seq);
            let value = serde_json::to_string(entry).map_err(|e| {
                HaError::InvalidBackend(format!("oplog serialize: {e}"))
            })?;
            c.kv_client()
                .put(key.as_bytes(), value.as_bytes(), None)
                .await
                .map_err(|e| {
                    HaError::InvalidBackend(format!("etcd put oplog: {e}"))
                })?;
        }
        // Update latest pointer
        let max_seq = self.buffer.last().unwrap().seq;
        let latest_val = max_seq.to_string();
        c.kv_client()
            .put(
                self.latest_key().as_bytes(),
                latest_val.as_bytes(),
                None,
            )
            .await
            .map_err(|e| {
                HaError::InvalidBackend(format!("etcd put oplog latest: {e}"))
            })?;

        self.buffer.clear();
        Ok(())
    }
}

impl OpLogStore for EtcdOpLogStore {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.last_seq += 1;
        self.buffer.push(OpLogRecord {
            seq: self.last_seq,
            ..entry.clone()
        });
        Ok(self.last_seq)
    }

    fn read_since(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError> {
        // Read from buffered entries first (fast path)
        let mut entries: Vec<OpLogRecord> = self
            .buffer
            .iter()
            .filter(|r| r.seq >= since_seq)
            .take(max_count)
            .cloned()
            .collect();
        if entries.len() >= max_count {
            entries.truncate(max_count);
            return Ok(entries);
        }
        let remaining = max_count - entries.len();

        // We can't easily do async etcd reads from &self (sync context).
        // For now, return buffered entries. Full async reads require an async
        // read_since variant.
        let _ = remaining;
        Ok(entries)
    }

    fn latest_sequence(&self) -> u64 {
        self.last_seq
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        let records = self.read_since(since_seq, max_count).unwrap_or_default();
        let next_seq = records.last().map(|r| r.seq + 1).unwrap_or(since_seq);
        OpLogPollResult {
            records,
            next_seq,
            timed_out: false,
        }
    }
}

/// Async extension for etcd-backed stores that need full range queries.
impl EtcdOpLogStore {
    /// Read entries from etcd starting from `since_seq` (async).
    pub async fn read_since_async(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError> {
        let mut entries = Vec::new();
        let c = self.client.clone();

        // Try reading from etcd
        let range_end = self.entry_key(u64::MAX);
        let range_start = self.entry_key(since_seq);

        match c
            .kv_client()
            .get(
                range_start.as_bytes(),
                Some(etcd_client::GetOptions::new().with_range(range_end.as_bytes())),
            )
            .await
        {
            Ok(resp) => {
                for kv in resp.kvs().iter().take(max_count) {
                    if let Ok(val) = String::from_utf8(kv.value().to_vec()) {
                        if let Ok(entry) = serde_json::from_str::<OpLogRecord>(&val) {
                            entries.push(entry);
                            if entries.len() >= max_count {
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("etcd range query for oplog failed: {}", e);
            }
        }

        // Supplement with buffered entries
        for entry in &self.buffer {
            if entry.seq >= since_seq && entries.len() < max_count {
                if !entries.iter().any(|e| e.seq == entry.seq) {
                    entries.push(entry.clone());
                }
            }
        }

        entries.sort_by_key(|e| e.seq);
        entries.truncate(max_count);
        Ok(entries)
    }

    /// Flush buffered entries to etcd (async). Call periodically or before shutdown.
    pub async fn flush_async(&mut self) -> Result<(), HaError> {
        self.flush().await
    }
}

// =============================================================================
// OpLogManager — high-level wrapper that records mutations into the oplog.
// =============================================================================

/// Manages recording of operations to an optional back-end OpLogStore.
/// Each mutation method serializes the event as a JSON payload and appends
/// it via `store.append()`.  Errors are logged (warn!) but never propagated,
/// so that the oplog is best-effort and never blocks the main write path.
pub struct OpLogManager {
    store: Option<Box<dyn OpLogStore + Send>>,
    view_version: u64,
}

impl OpLogManager {
    pub fn new(store: Option<Box<dyn OpLogStore + Send>>, view_version: u64) -> Self {
        Self { store, view_version }
    }

    pub fn latest_sequence(&self) -> u64 {
        self.store
            .as_ref()
            .map(|s| s.latest_sequence())
            .unwrap_or(0)
    }

    pub fn set_view_version(&mut self, version: u64) {
        self.view_version = version;
    }

    pub fn store(&self) -> Option<&(dyn OpLogStore + Send)> {
        self.store.as_deref()
    }

    pub fn into_store(self) -> Option<Box<dyn OpLogStore + Send>> {
        self.store
    }

    /// Record a put-end mutation: { "op": "put_end", "key": "...", "size": ... }
    pub fn record_put_end(&mut self, key: &str, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = json!({"op": "put_end", "key": key, "size": size}).to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record put_end for key={key}: {e}");
            }
        }
    }

    /// Record a remove mutation: { "op": "remove", "key": "..." }
    pub fn record_remove(&mut self, key: &str) {
        if let Some(store) = &mut self.store {
            let payload = json!({"op": "remove", "key": key}).to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record remove for key={key}: {e}");
            }
        }
    }

    /// Record a mount-segment mutation.
    pub fn record_mount_segment(&mut self, segment_name: &str, segment_id: Uuid, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = json!({
                "op": "mount_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
            })
            .to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record mount_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record an unmount-segment mutation.
    pub fn record_unmount_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = json!({
                "op": "unmount_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })
            .to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record unmount_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record a mount-nof-segment mutation.
    pub fn record_mount_nof_segment(&mut self, segment_name: &str, segment_id: Uuid, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = json!({
                "op": "mount_nof_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
            })
            .to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record mount_nof_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record an unmount-nof-segment mutation.
    pub fn record_unmount_nof_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = json!({
                "op": "unmount_nof_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })
            .to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record unmount_nof_segment for {segment_name}: {e}");
            }
        }
    }

    /// Record a put-start mutation: { "op": "put_start", "key": "...", "client_id": "..." }
    pub fn record_put_start(&mut self, key: &str, client_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = json!({
                "op": "put_start",
                "key": key,
                "client_id": client_id.to_string()
            })
            .to_string();
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record put_start for key={key}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seq: u64) -> OpLogRecord {
        OpLogRecord {
            seq,
            producer_view_version: 1,
            payload: format!("entry-{}", seq),
        }
    }

    #[test]
    fn test_in_memory_append_and_poll() {
        let mut oplog = InMemoryOpLog::new(1000);
        oplog.append(&make_entry(0)).unwrap();
        oplog.append(&make_entry(0)).unwrap();
        oplog.append(&make_entry(0)).unwrap();
        assert_eq!(oplog.latest_sequence(), 3);

        let result = oplog.poll_from(1, 10);
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.next_seq, 4);
    }

    #[test]
    fn test_in_memory_poll_empty() {
        let oplog = InMemoryOpLog::new(1000);
        let result = oplog.poll_from(1, 10);
        assert!(result.records.is_empty());
        assert_eq!(result.next_seq, 1);
    }

    #[test]
    fn test_local_fs_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();

        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap();
        assert_eq!(store.latest_sequence(), 2);

        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 1);
        assert_eq!(entries[1].seq, 2);
    }

    #[test]
    fn test_local_fs_flush_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 2).unwrap();

        // Appending 3 entries with max=2 triggers flush of first 2
        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap();
        store.append(&make_entry(0)).unwrap(); // flush triggered for entries 1-2
        // Manually flush remaining buffer so entry 3 is on disk
        store.flush().unwrap();

        let entries = store.read_since(1, 10).unwrap();
        assert_eq!(entries.len(), 3);

        // Re-open recovers the state
        let store2 = LocalFsOpLogStore::new(dir.path(), 2).unwrap();
        assert_eq!(store2.latest_sequence(), 3);
    }

    #[test]
    fn test_local_fs_poll_from() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();

        for _ in 0..10 {
            store.append(&make_entry(0)).unwrap();
        }
        let result = store.poll_from(5, 3);
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].seq, 5);
        assert_eq!(result.next_seq, 8);
    }
}

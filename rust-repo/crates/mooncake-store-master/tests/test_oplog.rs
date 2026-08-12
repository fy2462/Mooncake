use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use mooncake_store_core::ReplicaDescriptor;
use mooncake_store_master::ha::{HaError, OpLogRecord};
use mooncake_store_master::oplog::test_support::*;
use mooncake_store_master::oplog::{InMemoryOpLog, LocalFsOpLogStore, OpLogManager, OpLogStore};
use serde_json::json;
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::time::Duration;
use uuid::Uuid;

#[derive(Default)]
struct DurableFlushGate {
    state: Mutex<DurableFlushGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct DurableFlushGateState {
    entered: bool,
    released: bool,
}

impl DurableFlushGate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn wait_until_entered(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.entered)
            .unwrap();
        state.entered
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }
}

struct GateableOpLog {
    inner: InMemoryOpLog,
    flush_gate: Arc<DurableFlushGate>,
}

impl OpLogStore for GateableOpLog {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.inner.append(entry)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
        self.inner.read_since(since_seq, max_count)
    }

    fn latest_sequence(&self) -> u64 {
        self.inner.latest_sequence()
    }

    fn max_sequence_id(&self) -> Result<u64, HaError> {
        self.inner.max_sequence_id()
    }

    fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        self.inner.update_latest_sequence_id(sequence_id)
    }

    fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        self.inner
            .record_snapshot_sequence_id(snapshot_id, sequence_id)
    }

    fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        self.inner.get_snapshot_sequence_id(snapshot_id)
    }

    fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        self.inner.cleanup_before(before_sequence_id)
    }

    fn flush_durable(&mut self) -> Result<(), HaError> {
        self.flush_gate.block();
        self.inner.flush_durable()
    }

    fn poll_from(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> mooncake_store_master::ha::OpLogPollResult {
        self.inner.poll_from(since_seq, max_count)
    }
}

fn make_entry(seq: u64) -> OpLogRecord {
    OpLogRecord {
        seq,
        producer_view_version: 1,
        payload: format!("entry-{}", seq),
    }
}

fn cpp_wire_json(
    sequence_id: u64,
    timestamp_ms: u64,
    op_type: u8,
    object_key: &str,
    payload: &[u8],
    checksum: u32,
    prefix_hash: u32,
) -> String {
    serde_json::to_string(&CppWireTestEntry {
        sequence_id,
        timestamp_ms,
        op_type,
        object_key: object_key.to_string(),
        payload: BASE64_STANDARD.encode(payload),
        checksum,
        prefix_hash,
    })
    .unwrap()
}

fn write_legacy_segment(dir: &std::path::Path, start_seq: u64, entries: &[(u32, &str)]) {
    let mut data = Vec::new();
    for (seq, payload) in entries {
        data.extend_from_slice(&seq.to_le_bytes());
        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        data.extend_from_slice(payload.as_bytes());
    }
    std::fs::write(dir.join(format!("oplog_{start_seq:020}.bin")), data).unwrap();
}

fn local_disk_replica(
    holder_client_id: Uuid,
    storage_id: Option<Uuid>,
    generation_id: Option<Uuid>,
) -> ReplicaDescriptor {
    ReplicaDescriptor {
        segment_id: Uuid::nil(),
        segment_name: "local://disk-a".to_string(),
        offset: 0,
        size: 100,
        status: mooncake_store_core::ReplicaStatus::Complete,
        replica_type: mooncake_store_core::ReplicaType::LocalDisk,
        holder_client_id: Some(holder_client_id),
        local_disk_storage_id: storage_id,
        local_disk_generation_id: generation_id,
        refcnt: 0,
        handle_valid: true,
        base_addr: 0,
        protocol: String::new(),
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
fn test_oplog_manager_records_put_revoke() {
    let store = InMemoryOpLog::new(1000);
    let manager = OpLogManager::new(Some(Box::new(store)), 7);

    manager.record_put_revoke("k1");

    let entries = manager.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].producer_view_version, 7);
    assert_eq!(
        decode_record_payload_value_for_test(&entries[0].payload).unwrap(),
        json!({"key":"k1","op":"put_revoke","schema_version":1})
    );
}

#[test]
fn test_oplog_manager_records_legacy_put_end_completion_marker() {
    let store = InMemoryOpLog::new(1000);
    let manager = OpLogManager::new(Some(Box::new(store)), 7);

    manager.record_put_end("tenant-a\0k1", 42);

    let entries = manager.read_since(1, 10).unwrap();
    let payload = decode_record_payload_value_for_test(&entries[0].payload).unwrap();
    assert_eq!(
        payload,
        json!({"op": "put_end", "key": "tenant-a\0k1", "size": 42})
    );
}

#[test]
fn manager_latest_sequence_moves_only_after_durable_flush() {
    let flush_gate = Arc::new(DurableFlushGate::default());
    let manager = Arc::new(OpLogManager::new(
        Some(Box::new(GateableOpLog {
            inner: InMemoryOpLog::new(8),
            flush_gate: Arc::clone(&flush_gate),
        })),
        7,
    ));
    let submit_manager = Arc::clone(&manager);
    let submitter =
        std::thread::spawn(move || submit_manager.record_remove_durable("default\0gated"));

    let entered = flush_gate.wait_until_entered(Duration::from_secs(1));
    let latest_while_flush_is_gated = manager.latest_sequence();
    flush_gate.release();

    assert!(entered, "manager record never reached durable flush");
    assert_eq!(latest_while_flush_is_gated, 0);
    assert_eq!(submitter.join().unwrap(), Ok(1));
    assert_eq!(manager.latest_sequence(), 1);
}

#[test]
fn test_etcd_keeps_versioned_rust_remove_as_generic_record() {
    let entry = OpLogRecord {
        seq: 9,
        producer_view_version: 4,
        payload: json!({
            "op": "remove",
            "schema_version": 1,
            "key": "tenant-a\0key",
        })
        .to_string(),
    };

    let encoded = serialize_etcd_value_for_test(&entry).unwrap();
    let decoded = deserialize_etcd_value_for_test(&encoded).unwrap();

    assert_eq!(decoded.seq, 9);
    assert_eq!(decoded.producer_view_version, 4);
    assert_eq!(
        decode_record_payload_value_for_test(&decoded.payload).unwrap(),
        json!({
            "op": "remove",
            "schema_version": 1,
            "key": "tenant-a\0key",
        })
    );
}

#[test]
fn test_etcd_keeps_legacy_put_end_marker_untyped() {
    let entry = OpLogRecord {
        seq: 10,
        producer_view_version: 4,
        payload: json!({
            "op": "put_end",
            "key": "tenant-a\0key",
            "size": 42,
        })
        .to_string(),
    };

    let encoded = serialize_etcd_value_for_test(&entry).unwrap();
    let decoded = deserialize_etcd_value_for_test(&encoded).unwrap();
    let payload = decode_record_payload_value_for_test(&decoded.payload).unwrap();

    assert_eq!(payload["op"], "put_end");
    assert_eq!(payload["key"], "tenant-a\0key");
    assert!(payload.get("replicas").is_none());
}

#[test]
fn cpp_parity_localfs_init_creates_durable_structure_without_eager_snapshots() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("oplog");
    assert!(!root.exists());

    let store = LocalFsOpLogStore::new(&root, 100).unwrap();

    assert!(root.is_dir());
    assert_eq!(std::fs::read_to_string(root.join("latest")).unwrap(), "0");
    assert!(!root.join("snapshots").exists());
    assert_eq!(store.latest_sequence(), 0);
}

#[test]
fn cpp_parity_localfs_fresh_store_persists_latest_zero() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("oplog");

    let store = LocalFsOpLogStore::new(&root, 100).unwrap();
    assert_eq!(std::fs::read_to_string(root.join("latest")).unwrap(), "0");
    drop(store);

    let reopened = LocalFsOpLogStore::new(&root, 100).unwrap();
    assert_eq!(reopened.latest_sequence(), 0);
    assert_eq!(std::fs::read_to_string(root.join("latest")).unwrap(), "0");
}

#[test]
fn cpp_parity_localfs_init_removes_only_owned_stale_temp_files() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("oplog");
    std::fs::create_dir(&root).unwrap();
    let stale_segment = root.join("oplog_00000000000000000001.tmp");
    let unrelated = root.join("keep.tmp");
    std::fs::write(&stale_segment, b"partial segment").unwrap();
    std::fs::write(&unrelated, b"caller-owned").unwrap();

    LocalFsOpLogStore::new(&root, 100).unwrap();

    assert!(!stale_segment.exists());
    assert_eq!(std::fs::read(&unrelated).unwrap(), b"caller-owned");
    assert_eq!(std::fs::read_to_string(root.join("latest")).unwrap(), "0");
}

#[cfg(unix)]
#[test]
fn localfs_unremovable_stale_temp_does_not_block_recovery() {
    use std::os::unix::fs::PermissionsExt;

    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("oplog");
    std::fs::create_dir(&root).unwrap();
    let stale_segment = root.join("oplog_00000000000000000001.tmp");
    std::fs::write(&stale_segment, b"partial segment").unwrap();
    std::fs::write(root.join("latest"), b"0").unwrap();

    let mut permissions = std::fs::metadata(&root).unwrap().permissions();
    permissions.set_mode(0o555);
    std::fs::set_permissions(&root, permissions).unwrap();
    let write_probe = root.join("write-probe");
    if std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&write_probe)
        .is_ok()
    {
        let mut permissions = std::fs::metadata(&root).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&root, permissions).unwrap();
        std::fs::remove_file(write_probe).unwrap();
        return;
    }
    let result = LocalFsOpLogStore::new(&root, 100);
    let mut permissions = std::fs::metadata(&root).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&root, permissions).unwrap();

    let store = result.expect("stale-temp cleanup failure must not block recovery");
    assert_eq!(store.latest_sequence(), 0);
    assert!(stale_segment.exists());
}

#[test]
fn cpp_parity_localfs_single_record_range_read_returns_exact_entry() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    let sequence = store
        .append(&OpLogRecord {
            seq: 0,
            producer_view_version: 7,
            payload: json!({
                "op": "remove",
                "schema_version": 1,
                "key": "test_key",
            })
            .to_string(),
        })
        .unwrap();

    let entries = store.read_since(sequence, 1).unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, sequence);
    assert_eq!(entries[0].producer_view_version, 7);
    let payload = decode_record_payload_value_for_test(&entries[0].payload).unwrap();
    assert_eq!(payload["op"], "remove");
    assert_eq!(payload["key"], "test_key");
}

#[test]
fn cpp_parity_localfs_range_from_zero_returns_one_through_ten_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 10).unwrap();
    for _ in 0..10 {
        store.append(&make_entry(0)).unwrap();
    }

    let entries = store.read_since(0, 100).unwrap();

    assert_eq!(entries.len(), 10);
    assert_eq!(entries.first().unwrap().seq, 1);
    assert_eq!(entries.last().unwrap().seq, 10);
    assert_eq!(
        entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        (1..=10).collect::<Vec<_>>()
    );
}

#[test]
fn cpp_parity_localfs_unknown_snapshot_id_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFsOpLogStore::new(dir.path(), 10).unwrap();

    let error = store
        .get_snapshot_sequence_id("nonexistent")
        .expect_err("an unknown snapshot id must fail");

    assert!(error.to_string().contains("oplog read snapshot seq"));
}

#[test]
fn cpp_parity_localfs_snapshot_ids_reject_traversal_slash_and_nul() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 10).unwrap();

    for snapshot_id in ["../escape", "path/slash", "null\0byte"] {
        let error = store
            .record_snapshot_sequence_id(snapshot_id, 1)
            .expect_err("unsafe snapshot id must fail validation");
        assert!(
            error.to_string().contains("invalid snapshot id"),
            "snapshot_id={snapshot_id:?}: {error}"
        );
    }
    assert!(!dir.path().join("snapshots").exists());
}

#[test]
fn cpp_parity_localfs_cleanup_empty_store_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 10).unwrap();

    store.cleanup_before(100).unwrap();

    assert_eq!(store.latest_sequence(), 0);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("latest")).unwrap(),
        "0"
    );
    assert!(store.read_since(0, 1).unwrap().is_empty());
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
fn test_local_fs_rejects_invalid_utf8_payload_with_record_context() {
    let dir = tempfile::tempdir().unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(&1_u32.to_le_bytes());
    frame.extend_from_slice(&2_u32.to_le_bytes());
    frame.extend_from_slice(&[0xff, 0xfe]);
    std::fs::write(dir.path().join("oplog_00000000000000000001.bin"), frame).unwrap();

    let error = match LocalFsOpLogStore::new(dir.path(), 100) {
        Ok(_) => panic!("invalid UTF-8 oplog payload must fail recovery"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("invalid UTF-8"), "{message}");
    assert!(message.contains("seq=1"), "{message}");
}

#[test]
fn test_local_fs_flush_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 2).unwrap();

    // The second append synchronously flushes the first full segment.
    store.append(&make_entry(0)).unwrap();
    store.append(&make_entry(0)).unwrap();
    store.append(&make_entry(0)).unwrap();
    // Manually flush the remaining partial segment.
    store.flush_durable().unwrap();

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

#[test]
fn test_local_fs_threshold_flush_is_durable_without_explicit_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 3).unwrap();

    for _ in 0..3 {
        store.append(&make_entry(0)).unwrap();
    }
    assert_eq!(store.latest_sequence(), 3);

    let reopened = LocalFsOpLogStore::new(dir.path(), 3).unwrap();
    let entries = reopened.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].seq, 1);
    assert_eq!(entries[2].seq, 3);
}

#[test]
fn test_local_fs_v2_preserves_u64_sequence_and_producer_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    store.update_latest_sequence_id(u32::MAX as u64).unwrap();
    let entry = OpLogRecord {
        seq: 0,
        producer_view_version: 77,
        payload: "beyond-u32".into(),
    };

    let sequence = store.append(&entry).unwrap();
    assert_eq!(sequence, u32::MAX as u64 + 1);

    let reopened = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    let entries = reopened.read_since(sequence, 1).unwrap();
    assert_eq!(reopened.latest_sequence(), sequence);
    assert_eq!(entries[0].seq, sequence);
    assert_eq!(entries[0].producer_view_version, 77);
    assert_eq!(entries[0].payload, "beyond-u32");
}

#[test]
fn test_local_fs_reads_legacy_v1_segment() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_segment(dir.path(), 1, &[(1, "one"), (2, "two")]);

    let store = LocalFsOpLogStore::new(dir.path(), 10).unwrap();
    let entries = store.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].producer_view_version, 0);
    assert_eq!(entries[1].payload, "two");
    assert_eq!(store.latest_sequence(), 2);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("latest")).unwrap(),
        "2"
    );
}

#[test]
fn legacy_v1_magic_prefix_collision_remains_readable() {
    let dir = tempfile::tempdir().unwrap();
    let sequence = u32::from_le_bytes(*b"MCOP");
    let payload = "x".repeat(u16::from_le_bytes(*b"LG") as usize);
    write_legacy_segment(dir.path(), sequence as u64, &[(sequence, payload.as_str())]);

    let store = LocalFsOpLogStore::new(dir.path(), 10).unwrap();
    let entries = store.read_since(sequence as u64, 1).unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, sequence as u64);
    assert_eq!(entries[0].payload, payload);
}

#[test]
fn test_local_fs_rejects_truncated_legacy_frame() {
    let dir = tempfile::tempdir().unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(&1_u32.to_le_bytes());
    frame.extend_from_slice(&5_u32.to_le_bytes());
    frame.extend_from_slice(b"abc");
    std::fs::write(dir.path().join("oplog_00000000000000000001.bin"), frame).unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("truncated legacy payload must fail recovery");
    assert!(error.to_string().contains("truncated"));
}

#[test]
fn test_local_fs_rejects_truncated_legacy_header() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("oplog_00000000000000000001.bin"),
        1_u32.to_le_bytes(),
    )
    .unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("truncated legacy header must fail recovery");
    assert!(error.to_string().contains("truncated"));
}

#[test]
fn test_local_fs_rejects_v2_checksum_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    store.append(&make_entry(0)).unwrap();
    let path = dir.path().join("oplog_00000000000000000001.bin");
    let mut data = std::fs::read(&path).unwrap();
    let last = data.last_mut().unwrap();
    *last ^= 0xff;
    std::fs::write(path, data).unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("checksum corruption must fail recovery");
    assert!(error.to_string().contains("checksum mismatch"));
}

#[test]
fn cpp_parity_localfs_rejects_corrupted_current_format_magic() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    store.append(&make_entry(0)).unwrap();
    let path = dir.path().join("oplog_00000000000000000001.bin");
    let mut data = std::fs::read(&path).unwrap();
    data[..4].copy_from_slice(b"XXXX");
    std::fs::write(path, data).unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("corrupted current-format magic must fail recovery");
    assert!(error.to_string().contains("corrupted v2 magic"), "{error}");
}

#[test]
fn cpp_parity_localfs_rejects_unknown_current_format_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    store.append(&make_entry(0)).unwrap();
    let path = dir.path().join("oplog_00000000000000000001.bin");
    let mut data = std::fs::read(&path).unwrap();
    data[7] = b'3';
    std::fs::write(path, data).unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("unknown current-format version must fail recovery");
    assert!(
        error
            .to_string()
            .contains("unsupported local oplog format version"),
        "{error}"
    );
}

#[test]
fn cpp_parity_localfs_rejects_truncated_current_format_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    store.append(&make_entry(0)).unwrap();
    let path = dir.path().join("oplog_00000000000000000001.bin");
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(16).unwrap();

    let error = store
        .read_since(1, 1)
        .expect_err("truncated current-format segment must fail a live read");
    assert!(error.to_string().contains("truncated"), "{error}");
}

#[test]
fn test_local_fs_rejects_malformed_latest_pointer() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("latest"), "not-a-sequence").unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("malformed latest pointer must fail recovery");
    assert!(error.to_string().contains("latest sequence is malformed"));
}

#[test]
fn test_local_fs_rejects_v2_segment_ahead_of_commit_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    store.append(&make_entry(0)).unwrap();
    std::fs::write(dir.path().join("latest"), "0").unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("an uncommitted v2 segment must not be replayed");
    assert!(
        error
            .to_string()
            .contains("exceeds committed latest pointer")
    );
}

#[test]
fn test_local_fs_rejects_v2_segment_without_commit_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 1).unwrap();
    store.append(&make_entry(0)).unwrap();
    std::fs::remove_file(dir.path().join("latest")).unwrap();

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("a v2 segment without its commit pointer must not be replayed");
    assert!(error.to_string().contains("without a committed latest"));
}

#[test]
fn test_local_fs_rejects_segment_filename_content_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_segment(dir.path(), 2, &[(1, "one")]);

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("segment filename mismatch must fail recovery");
    assert!(error.to_string().contains("filename/content mismatch"));
}

#[test]
fn test_local_fs_rejects_cross_segment_sequence_gap() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_segment(dir.path(), 1, &[(1, "one")]);
    write_legacy_segment(dir.path(), 3, &[(3, "three")]);

    let error = LocalFsOpLogStore::new(dir.path(), 10)
        .err()
        .expect("cross-segment sequence gap must fail recovery");
    assert!(error.to_string().contains("sequence gap"));
}

#[test]
fn test_local_fs_snapshot_sequence_and_cleanup_parity() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 2).unwrap();

    for _ in 0..5 {
        store.append(&make_entry(0)).unwrap();
    }
    store.flush_durable().unwrap();
    assert_eq!(store.max_sequence_id().unwrap(), 5);

    store.record_snapshot_sequence_id("snap1", 3).unwrap();
    assert_eq!(store.get_snapshot_sequence_id("snap1").unwrap(), 3);
    assert!(store.record_snapshot_sequence_id("../bad", 1).is_err());

    store.cleanup_before(4).unwrap();
    let entries = store.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].seq, 4);

    let reopened = LocalFsOpLogStore::new(dir.path(), 2).unwrap();
    assert_eq!(reopened.latest_sequence(), 5);
    assert_eq!(reopened.get_snapshot_sequence_id("snap1").unwrap(), 3);
}

#[test]
fn test_manager_append_and_persist_flushes_local_fs() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();
    let manager = OpLogManager::new(Some(Box::new(store)), 7);

    let seq = manager.record_remove_durable("k1").unwrap();
    assert_eq!(seq, 1);

    let store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();
    let entries = store.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        decode_record_payload_value_for_test(&entries[0].payload).unwrap(),
        json!({"op": "remove", "schema_version": 1, "key": "k1"})
    );
}

#[test]
fn test_oplog_size_validation_matches_cpp_limits() {
    let long_key = "k".repeat(TEST_MAX_OBJECT_KEY_SIZE + 1);
    let entry = OpLogRecord {
        seq: 1,
        producer_view_version: 1,
        payload: json!({"op": "remove", "key": long_key}).to_string(),
    };
    assert!(validate_record_size_for_test(&entry).is_err());

    let wire = CppWireTestEntry {
        sequence_id: 1,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "k".to_string(),
        payload: "A".repeat((TEST_MAX_PAYLOAD_SIZE.div_ceil(3) * 4) + 1),
        checksum: 0,
        prefix_hash: 0,
    };
    assert!(validate_wire_entry_size_for_test(wire).is_err());
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testchecksumcomputation() {
    let checksum_x1 = compute_cpp_checksum_for_test(b"payload-X");
    let checksum_x2 = compute_cpp_checksum_for_test(b"payload-X");
    let checksum_y = compute_cpp_checksum_for_test(b"payload-Y");

    assert_eq!(checksum_x1, checksum_x2);
    assert_ne!(checksum_x1, checksum_y);
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testprefixhashcomputation() {
    let same_key_v1 = compute_cpp_prefix_hash_for_test("same-key");
    let same_key_v2 = compute_cpp_prefix_hash_for_test("same-key");
    let other_key = compute_cpp_prefix_hash_for_test("other-key");

    assert_eq!(same_key_v1, same_key_v2);
    assert_ne!(same_key_v1, other_key);
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testvalidateentrysize_valid() {
    let wire = CppWireTestEntry {
        sequence_id: 1,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "normal-key".to_string(),
        payload: BASE64_STANDARD.encode(b"small-payload"),
        checksum: compute_cpp_checksum_for_test(b"small-payload"),
        prefix_hash: compute_cpp_prefix_hash_for_test("normal-key"),
    };

    assert!(validate_wire_entry_size_for_test(wire).is_ok());
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testvalidateentrysize_keytoolarge() {
    let oversized_key = "k".repeat(TEST_MAX_OBJECT_KEY_SIZE + 1);
    let wire = CppWireTestEntry {
        sequence_id: 1,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: oversized_key.clone(),
        payload: BASE64_STANDARD.encode(b"payload"),
        checksum: compute_cpp_checksum_for_test(b"payload"),
        prefix_hash: compute_cpp_prefix_hash_for_test(&oversized_key),
    };

    let error = validate_wire_entry_size_for_test(wire).unwrap_err();
    assert!(error.to_string().contains(&format!(
        "oplog object key too large: {}",
        oversized_key.len()
    )));
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testvalidateentrysize_payloadtoolarge()
 {
    let oversized_payload = vec![b'p'; TEST_MAX_PAYLOAD_SIZE + 1];
    let wire = CppWireTestEntry {
        sequence_id: 1,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "key".to_string(),
        payload: BASE64_STANDARD.encode(&oversized_payload),
        checksum: compute_cpp_checksum_for_test(&oversized_payload),
        prefix_hash: compute_cpp_prefix_hash_for_test("key"),
    };

    let error =
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap_err();
    assert!(error.to_string().contains(&format!(
        "oplog payload too large: {}",
        oversized_payload.len()
    )));
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testvalidateentrysize_emptykey() {
    let wire = CppWireTestEntry {
        sequence_id: 1,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: String::new(),
        payload: BASE64_STANDARD.encode(b"ordinary-payload"),
        checksum: compute_cpp_checksum_for_test(b"ordinary-payload"),
        prefix_hash: compute_cpp_prefix_hash_for_test(""),
    };

    assert!(validate_wire_entry_size_for_test(wire).is_ok());
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testlargepayload() {
    let manager = OpLogManager::new(Some(Box::new(InMemoryOpLog::new(2))), 7);
    let payload = "x".repeat(TEST_MAX_PAYLOAD_SIZE - 1);

    let sequence = manager.append_and_persist(payload.clone()).unwrap();

    assert_eq!(sequence, 1);
    assert_eq!(manager.latest_sequence(), 1);
    let entries = manager.read_since(1, 2).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, 1);
    assert_eq!(entries[0].payload, payload);
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testappendentry() {
    let manager = OpLogManager::new(Some(Box::new(InMemoryOpLog::new(2))), 7);

    let sequence = manager.append_and_persist("value1".to_string()).unwrap();

    assert_eq!(sequence, 1);
    assert_eq!(manager.latest_sequence(), sequence);
    let entries = manager.read_since(1, 2).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, sequence);
    assert_eq!(entries[0].payload, "value1");
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testsequenceidincrement() {
    let manager = OpLogManager::new(Some(Box::new(InMemoryOpLog::new(4))), 7);

    let id1 = manager.append_and_persist("value1".to_string()).unwrap();
    let id2 = manager.append_and_persist("value2".to_string()).unwrap();
    let id3 = manager.append_and_persist(String::new()).unwrap();

    assert_eq!(id1, 1);
    assert_eq!(id2, id1 + 1);
    assert_eq!(id3, id2 + 1);
    assert_eq!(manager.latest_sequence(), id3);
    let entries = manager.read_since(1, 4).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(
        entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        [id1, id2, id3]
    );
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.payload.as_str())
            .collect::<Vec<_>>(),
        ["value1", "value2", ""]
    );
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testconcurrentappend() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 1_000;
    const TOTAL: usize = THREADS * PER_THREAD;

    let manager = Arc::new(OpLogManager::new(
        Some(Box::new(InMemoryOpLog::new(TOTAL + 1))),
        7,
    ));
    let start = Arc::new(Barrier::new(THREADS));
    let ids = Arc::new(Mutex::new(Vec::with_capacity(TOTAL)));
    let workers = (0..THREADS)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let start = Arc::clone(&start);
            let ids = Arc::clone(&ids);
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..PER_THREAD {
                    let id = manager.append_and_persist("value".to_string()).unwrap();
                    ids.lock().unwrap().push(id);
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }

    let mut ids = Arc::try_unwrap(ids).unwrap().into_inner().unwrap();
    ids.sort_unstable();
    assert_eq!(ids.len(), 8_000);
    assert_eq!(ids.first(), Some(&1));
    assert_eq!(ids.last(), Some(&8_000));
    for (index, id) in ids.iter().enumerate() {
        assert_eq!(*id, u64::try_from(index).unwrap() + 1);
    }
    assert_eq!(manager.latest_sequence(), 8_000);
    let entries = manager.read_since(1, 8_001).unwrap();
    assert_eq!(entries.len(), 8_000);
    assert_eq!(entries.first().unwrap().seq, 1);
    assert_eq!(entries.last().unwrap().seq, 8_000);
}

#[test]
fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testappendmultipletypes() {
    let manager = OpLogManager::new(Some(Box::new(InMemoryOpLog::new(4))), 7);

    let put_end = manager
        .append_and_persist(json!({"op": "put_end", "key": "k1", "size": 7}).to_string())
        .unwrap();
    let put_revoke = manager.record_put_revoke_durable("k2").unwrap();
    let remove = manager.record_remove_durable("k3").unwrap();

    assert_eq!(put_end + 1, put_revoke);
    assert_eq!(put_revoke + 1, remove);
    assert_eq!(manager.latest_sequence(), 3);
    let entries = manager.read_since(1, 4).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(
        entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        entries
            .iter()
            .map(
                |entry| decode_record_payload_value_for_test(&entry.payload).unwrap()["op"]
                    .as_str()
                    .unwrap()
                    .to_string()
            )
            .collect::<Vec<_>>(),
        ["put_end", "put_revoke", "remove"]
    );
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_deserialize_invalidjson() {
    assert!(deserialize_etcd_value_for_test("{not-json").is_err());
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_deserialize_emptystring() {
    assert!(deserialize_etcd_value_for_test("").is_err());
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_deserialize_missingfields() {
    let outcome =
        std::panic::catch_unwind(|| deserialize_etcd_value_for_test(r#"{"sequence_id":1}"#));

    assert!(outcome.is_ok());
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_deserialize_keytoolarge() {
    let oversized_key = "k".repeat(TEST_MAX_OBJECT_KEY_SIZE + 1);
    let wire = CppWireTestEntry {
        sequence_id: 1,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: oversized_key.clone(),
        payload: BASE64_STANDARD.encode(b"v"),
        checksum: compute_cpp_checksum_for_test(b"v"),
        prefix_hash: compute_cpp_prefix_hash_for_test(&oversized_key),
    };

    let error =
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap_err();
    assert!(error.to_string().contains(&format!(
        "oplog object key too large: {}",
        oversized_key.len()
    )));
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_deserialize_payloadtoolarge() {
    let oversized_payload = vec![b'p'; TEST_MAX_PAYLOAD_SIZE + 1];
    let wire = CppWireTestEntry {
        sequence_id: 1,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "k".to_string(),
        payload: BASE64_STANDARD.encode(&oversized_payload),
        checksum: compute_cpp_checksum_for_test(&oversized_payload),
        prefix_hash: compute_cpp_prefix_hash_for_test("k"),
    };

    let error =
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap_err();
    assert!(error.to_string().contains(&format!(
        "oplog payload too large: {}",
        oversized_payload.len()
    )));
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_roundtrip_putend() {
    let decoded = inspect_cpp_wire_entry_for_test(&cpp_wire_json(
        1,
        1_234_567_890,
        TEST_CPP_OP_PUT_END,
        "key1",
        b"value1",
        2_631_246_273,
        133_378_825,
    ))
    .unwrap();

    assert_eq!(decoded.sequence_id, 1);
    assert_eq!(decoded.timestamp_ms, 1_234_567_890);
    assert_eq!(decoded.op_type, TEST_CPP_OP_PUT_END);
    assert_eq!(decoded.object_key, "key1");
    assert_eq!(decoded.payload, b"value1");
    assert_eq!(decoded.checksum, 2_631_246_273);
    assert_eq!(decoded.prefix_hash, 133_378_825);
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_roundtrip_remove() {
    let decoded = inspect_cpp_wire_entry_for_test(&cpp_wire_json(
        42,
        1_234_567_890,
        TEST_CPP_OP_REMOVE,
        "obj/to/remove",
        b"",
        46_947_589,
        4_262_510_626,
    ))
    .unwrap();

    assert_eq!(decoded.sequence_id, 42);
    assert_eq!(decoded.op_type, TEST_CPP_OP_REMOVE);
    assert_eq!(decoded.object_key, "obj/to/remove");
    assert!(decoded.payload.is_empty());
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_roundtrip_putrevoke() {
    let decoded = inspect_cpp_wire_entry_for_test(&cpp_wire_json(
        99,
        1_234_567_890,
        TEST_CPP_OP_PUT_REVOKE,
        "revoked_key",
        b"meta",
        3_739_924_676,
        1_578_467_929,
    ))
    .unwrap();

    assert_eq!(decoded.op_type, TEST_CPP_OP_PUT_REVOKE);
    assert_eq!(decoded.payload, b"meta");
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_roundtrip_binarypayload() {
    let binary_payload = (0_u8..=255).collect::<Vec<_>>();
    let decoded = inspect_cpp_wire_entry_for_test(&cpp_wire_json(
        7,
        1_234_567_890,
        TEST_CPP_OP_PUT_END,
        "bin_key",
        &binary_payload,
        1_497_633_363,
        3_982_346_134,
    ))
    .unwrap();

    assert_eq!(decoded.payload, binary_payload);
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_roundtrip_emptypayload() {
    let decoded = inspect_cpp_wire_entry_for_test(&cpp_wire_json(
        10,
        1_234_567_890,
        TEST_CPP_OP_REMOVE,
        "key",
        b"",
        46_947_589,
        3_017_358_048,
    ))
    .unwrap();

    assert!(decoded.payload.is_empty());
}

#[test]
fn cpp_parity_ha_oplog_oplog_serializer_test_cpp_oplogserializertest_roundtrip_emptykey() {
    let wire_value = cpp_wire_json(
        11,
        1_234_567_890,
        TEST_CPP_OP_PUT_END,
        "",
        b"payload",
        1_219_833_882,
        0,
    );
    let decoded = inspect_cpp_wire_entry_for_test(&wire_value).unwrap();

    assert!(decoded.object_key.is_empty());
    assert_eq!(decoded.prefix_hash, 0);
}

#[test]
fn test_etcd_oplog_entry_key_matches_cpp_format() {
    let key = format_etcd_entry_key_for_test("/oplog/cluster-a", 42);
    assert_eq!(key, "/oplog/cluster-a/00000000000000000042");
    assert!(!key.contains("seq_"));
}

#[test]
fn test_etcd_append_path_builds_contiguous_buffer_without_flushing() {
    let records = [
        OpLogRecord {
            seq: 0,
            producer_view_version: 7,
            payload: "first".to_string(),
        },
        OpLogRecord {
            seq: 0,
            producer_view_version: 7,
            payload: "second".to_string(),
        },
        OpLogRecord {
            seq: 0,
            producer_view_version: 7,
            payload: "third".to_string(),
        },
    ];

    let buffered = buffered_etcd_records_for_test(40, &records).unwrap();

    assert_eq!(
        buffered.iter().map(|record| record.seq).collect::<Vec<_>>(),
        vec![41, 42, 43]
    );
    assert_eq!(
        buffered
            .iter()
            .map(|record| (record.payload.as_str(), record.producer_view_version))
            .collect::<Vec<_>>(),
        vec![("first", 7), ("second", 7), ("third", 7)]
    );
}

#[test]
fn test_etcd_oplog_value_writes_cpp_outer_json_for_put_end() {
    let replica = ReplicaDescriptor {
        segment_id: Uuid::new_v4(),
        segment_name: "seg-a:1234".to_string(),
        offset: 16,
        size: 100,
        status: mooncake_store_core::ReplicaStatus::Complete,
        replica_type: mooncake_store_core::ReplicaType::Memory,
        holder_client_id: None,
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        refcnt: 0,
        handle_valid: true,
        base_addr: 4096,
        protocol: "rdma".into(),
    };
    let entry = OpLogRecord {
        seq: 12,
        producer_view_version: 7,
        payload: json!({
            "op": "put_end",
            "key": "k1",
            "size": 100,
            "client_id": null,
            "tenant_id": "default",
            "group_id": "",
            "user_key": "k1",
            "replicas": [replica],
        })
        .to_string(),
    };

    let value = serialize_etcd_value_for_test(&entry).unwrap();
    let wire: CppWireTestEntry = serde_json::from_str(&value).unwrap();
    assert_eq!(wire.sequence_id, 12);
    assert_eq!(wire.op_type, TEST_CPP_OP_PUT_END);
    assert_eq!(wire.object_key, "k1");
    let decoded = BASE64_STANDARD.decode(&wire.payload).unwrap();
    assert!(decoded.starts_with(TEST_PUT_END_MSGPACK_MAGIC));
    assert_eq!(wire.checksum, compute_cpp_checksum_for_test(&decoded));
    assert_eq!(wire.prefix_hash, compute_cpp_prefix_hash_for_test("k1"));

    let parsed = deserialize_etcd_value_for_test(&value).unwrap();
    assert_eq!(parsed.seq, 12);
    assert_eq!(
        decode_record_payload_value_for_test(&parsed.payload).unwrap(),
        decode_record_payload_value_for_test(&entry.payload).unwrap()
    );
}

#[test]
fn test_etcd_oplog_v2_round_trips_exact_local_disk_identity() {
    let holder_client_id = Uuid::new_v4();
    let storage_id = Uuid::new_v4();
    let generation_id = Uuid::new_v4();
    let replica = local_disk_replica(holder_client_id, Some(storage_id), Some(generation_id));
    let entry = OpLogRecord {
        seq: 13,
        producer_view_version: 7,
        payload: json!({
            "op": "put_end",
            "key": "default\0local-disk-v2",
            "size": 100,
            "client_id": holder_client_id.to_string(),
            "tenant_id": "default",
            "group_id": "",
            "user_key": "local-disk-v2",
            "replicas": [replica],
        })
        .to_string(),
    };

    let wire_value = serialize_etcd_value_for_test(&entry).unwrap();
    let wire: CppWireTestEntry = serde_json::from_str(&wire_value).unwrap();
    let bytes = BASE64_STANDARD.decode(&wire.payload).unwrap();
    assert!(bytes.starts_with(b"MCOPMETA2"));

    let parsed = deserialize_etcd_value_for_test(&wire_value).unwrap();
    let payload = decode_record_payload_value_for_test(&parsed.payload).unwrap();
    let replicas: Vec<ReplicaDescriptor> =
        serde_json::from_value(payload["replicas"].clone()).unwrap();
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].local_disk_storage_id, Some(storage_id));
    assert_eq!(replicas[0].local_disk_generation_id, Some(generation_id));
}

#[test]
fn test_etcd_oplog_v1_local_disk_does_not_invent_generation() {
    let holder_client_id = Uuid::new_v4();
    #[derive(serde::Serialize)]
    struct LegacyPutEndPayload {
        op: &'static str,
        key: &'static str,
        size: u64,
        client_id: String,
        tenant_id: &'static str,
        group_id: &'static str,
        user_key: &'static str,
        replicas: Vec<ReplicaDescriptor>,
    }
    let payload = LegacyPutEndPayload {
        op: "put_end",
        key: "default\0legacy-local-disk",
        size: 100,
        client_id: holder_client_id.to_string(),
        tenant_id: "default",
        group_id: "",
        user_key: "legacy-local-disk",
        replicas: vec![local_disk_replica(holder_client_id, None, None)],
    };
    let mut bytes = b"MCOPMETA1".to_vec();
    bytes.extend_from_slice(&rmp_serde::to_vec_named(&payload).unwrap());
    let wire = CppWireTestEntry {
        sequence_id: 14,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "default\0legacy-local-disk".to_string(),
        payload: BASE64_STANDARD.encode(&bytes),
        checksum: compute_cpp_checksum_for_test(&bytes),
        prefix_hash: compute_cpp_prefix_hash_for_test("default\0legacy-local-disk"),
    };

    let parsed = deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap();
    let payload = decode_record_payload_value_for_test(&parsed.payload).unwrap();
    let replicas: Vec<ReplicaDescriptor> =
        serde_json::from_value(payload["replicas"].clone()).unwrap();
    assert_eq!(replicas.len(), 1);
    assert_eq!(
        replicas[0].replica_type,
        mooncake_store_core::ReplicaType::LocalDisk
    );
    assert_eq!(replicas[0].local_disk_storage_id, None);
    assert_eq!(
        replicas[0].local_disk_generation_id, None,
        "v1 replay must remain offline until exact generation recovery"
    );
}

#[test]
fn test_etcd_oplog_value_reads_versioned_msgpack_put_end() {
    let payload = json!({
        "op": "put_end",
        "key": "k-msgpack",
        "size": 42,
        "client_id": Uuid::new_v4().to_string(),
        "tenant_id": "tenant-a",
        "group_id": "group-a",
        "user_key": "k-msgpack",
        "replicas": [],
    });
    let bytes = encode_put_end_msgpack_from_json_for_test(&payload).unwrap();
    let wire = CppWireTestEntry {
        sequence_id: 21,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "k-msgpack".to_string(),
        payload: BASE64_STANDARD.encode(&bytes),
        checksum: compute_cpp_checksum_for_test(&bytes),
        prefix_hash: compute_cpp_prefix_hash_for_test("k-msgpack"),
    };

    let parsed = deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap();
    assert_eq!(parsed.seq, 21);
    let value = decode_record_payload_value_for_test(&parsed.payload).unwrap();
    assert_eq!(value["op"], "put_end");
    assert_eq!(value["key"], "k-msgpack");
    assert_eq!(value["size"], 42);
    assert_eq!(value["tenant_id"], "tenant-a");
    assert_eq!(value["group_id"], "group-a");
}

#[test]
fn test_etcd_oplog_value_rejects_future_msgpack_put_end_schema() {
    let bytes = b"MCOPMETA4future-body";
    let wire = CppWireTestEntry {
        sequence_id: 22,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "future-key".to_string(),
        payload: BASE64_STANDARD.encode(bytes),
        checksum: compute_cpp_checksum_for_test(bytes),
        prefix_hash: compute_cpp_prefix_hash_for_test("future-key"),
    };

    let error =
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("unsupported put_end msgpack schema version '4'"),
        "{error}"
    );
}

#[test]
fn test_etcd_oplog_value_rejects_invalid_put_end_tenant() {
    let payload = json!({
        "op": "put_end",
        "key": "bad\nname\0k1",
        "size": 42,
        "tenant_id": "bad\nname",
        "user_key": "k1",
        "replicas": [],
    });
    let bytes = encode_put_end_msgpack_from_json_for_test(&payload).unwrap();
    let wire = CppWireTestEntry {
        sequence_id: 22,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "bad\nname\0k1".to_string(),
        payload: BASE64_STANDARD.encode(&bytes),
        checksum: compute_cpp_checksum_for_test(&bytes),
        prefix_hash: compute_cpp_prefix_hash_for_test("bad\nname\0k1"),
    };

    let error =
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap_err();

    assert!(error.to_string().contains("tenant"), "{error}");
}

#[test]
fn test_etcd_oplog_value_rejects_conflicting_put_end_tenant_identity() {
    let payload = json!({
        "op": "put_end",
        "key": "tenant-b\0k1",
        "size": 42,
        "tenant_id": "tenant-a",
        "user_key": "k1",
        "replicas": [],
    });
    let bytes = encode_put_end_msgpack_from_json_for_test(&payload).unwrap();
    let wire = CppWireTestEntry {
        sequence_id: 23,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "tenant-b\0k1".to_string(),
        payload: BASE64_STANDARD.encode(&bytes),
        checksum: compute_cpp_checksum_for_test(&bytes),
        prefix_hash: compute_cpp_prefix_hash_for_test("tenant-b\0k1"),
    };

    let error =
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap_err();

    assert!(error.to_string().contains("tenant"), "{error}");
    assert!(error.to_string().contains("mismatch"), "{error}");
}

#[test]
fn test_etcd_oplog_value_rejects_non_string_put_end_tenant() {
    let entry = OpLogRecord {
        seq: 24,
        producer_view_version: 1,
        payload: json!({
            "op": "put_end",
            "key": "k1",
            "size": 42,
            "tenant_id": 42,
            "user_key": "k1",
            "replicas": [],
        })
        .to_string(),
    };

    let error =
        deserialize_etcd_value_for_test(&serde_json::to_string(&entry).unwrap()).unwrap_err();

    assert!(error.to_string().contains("tenant"), "{error}");
    assert!(error.to_string().contains("string"), "{error}");
}

#[test]
fn test_etcd_oplog_value_accepts_legacy_empty_tenant_for_scoped_key() {
    let payload = json!({
        "op": "put_end",
        "key": "tenant-a\0k1",
        "size": 42,
        "tenant_id": "",
        "user_key": "",
        "replicas": [],
    });
    let bytes = encode_put_end_msgpack_from_json_for_test(&payload).unwrap();
    let wire = CppWireTestEntry {
        sequence_id: 25,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "tenant-a\0k1".to_string(),
        payload: BASE64_STANDARD.encode(&bytes),
        checksum: compute_cpp_checksum_for_test(&bytes),
        prefix_hash: compute_cpp_prefix_hash_for_test("tenant-a\0k1"),
    };

    let record = deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap();
    let restored = decode_record_payload_value_for_test(&record.payload).unwrap();

    assert_eq!(restored["tenant_id"], "");
    assert_eq!(restored["key"], "tenant-a\0k1");
}

#[test]
fn test_etcd_oplog_value_rejects_cpp_checksum_mismatch() {
    let wire = CppWireTestEntry {
        sequence_id: 8,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_REMOVE,
        object_key: "k-bad".to_string(),
        payload: String::new(),
        checksum: compute_cpp_checksum_for_test(b"not-empty"),
        prefix_hash: compute_cpp_prefix_hash_for_test("k-bad"),
    };
    let value = serde_json::to_string(&wire).unwrap();

    assert!(matches!(
        deserialize_etcd_value_for_test(&value),
        Err(HaError::InvalidBackend(_))
    ));
}

#[test]
fn test_etcd_oplog_value_reads_cpp_binary_put_end_payload() {
    let binary_payload = vec![0, 159, 146, 1, 2, 3, 255];
    let wire = CppWireTestEntry {
        sequence_id: 9,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_PUT_END,
        object_key: "k-binary".to_string(),
        payload: BASE64_STANDARD.encode(&binary_payload),
        checksum: compute_cpp_checksum_for_test(&binary_payload),
        prefix_hash: compute_cpp_prefix_hash_for_test("k-binary"),
    };
    let value = serde_json::to_string(&wire).unwrap();

    let parsed = deserialize_etcd_value_for_test(&value).unwrap();
    assert_eq!(parsed.seq, 9);
    let payload = decode_record_payload_value_for_test(&parsed.payload).unwrap();
    assert_eq!(payload["op"], "put_end");
    assert_eq!(payload["key"], "k-binary");
    assert_eq!(payload["size"], 0);
    assert!(payload.get("metadata_payload_base64").is_none());
}

#[test]
fn test_etcd_oplog_value_reads_cpp_remove_and_put_revoke() {
    let remove_wire = CppWireTestEntry {
        sequence_id: 3,
        timestamp_ms: 1,
        op_type: TEST_CPP_OP_REMOVE,
        object_key: "k-remove".to_string(),
        payload: String::new(),
        checksum: 0,
        prefix_hash: 0,
    };
    let remove_value = serde_json::to_string(&remove_wire).unwrap();
    let remove = deserialize_etcd_value_for_test(&remove_value).unwrap();
    assert_eq!(remove.seq, 3);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&remove.payload).unwrap(),
        json!({"op": "remove", "key": "k-remove"})
    );

    let revoke_wire = CppWireTestEntry {
        sequence_id: 4,
        op_type: TEST_CPP_OP_PUT_REVOKE,
        object_key: "k-revoke".to_string(),
        ..remove_wire
    };
    let revoke_value = serde_json::to_string(&revoke_wire).unwrap();
    let revoke = deserialize_etcd_value_for_test(&revoke_value).unwrap();
    assert_eq!(revoke.seq, 4);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&revoke.payload).unwrap(),
        json!({"op": "put_revoke", "key": "k-revoke"})
    );
}

#[test]
fn test_etcd_oplog_value_rejects_invalid_remove_like_tenant_identity() {
    for (sequence_id, op_type) in [(26, TEST_CPP_OP_REMOVE), (27, TEST_CPP_OP_PUT_REVOKE)] {
        let wire = CppWireTestEntry {
            sequence_id,
            timestamp_ms: 1,
            op_type,
            object_key: "_reserved\0k1".to_string(),
            payload: String::new(),
            checksum: 0,
            prefix_hash: compute_cpp_prefix_hash_for_test("_reserved\0k1"),
        };

        let error =
            deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap_err();

        assert!(error.to_string().contains("tenant"), "{error}");
    }
}

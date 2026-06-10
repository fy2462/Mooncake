use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use mooncake_store_core::ReplicaDescriptor;
use mooncake_store_master::ha::{HaError, OpLogRecord};
use mooncake_store_master::oplog::test_support::*;
use mooncake_store_master::oplog::{InMemoryOpLog, LocalFsOpLogStore, OpLogManager, OpLogStore};
use serde_json::json;
use uuid::Uuid;

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
fn test_oplog_manager_records_put_revoke() {
    let store = InMemoryOpLog::new(1000);
    let mut manager = OpLogManager::new(Some(Box::new(store)), 7);

    manager.record_put_revoke("k1");

    let store = manager.into_store().unwrap();
    let entries = store.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].producer_view_version, 7);
    assert_eq!(
        decode_record_payload_value_for_test(&entries[0].payload).unwrap(),
        json!({"key":"k1","op":"put_revoke"})
    );
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
fn test_local_fs_async_flush_readable_without_explicit_flush() {
    let dir = tempfile::tempdir().unwrap();
    // max_entries_per_segment=3 triggers async flush on every 3rd append.
    let mut store = LocalFsOpLogStore::new(dir.path(), 3).unwrap();

    // Append 7 entries: triggers async flush at append #3 and #6.
    for _ in 0..7 {
        store.append(&make_entry(0)).unwrap();
    }
    assert_eq!(store.latest_sequence(), 7);

    // Give the background thread time to flush segments to disk.
    std::thread::sleep(std::time::Duration::from_millis(50));

    // read_since reads from both on-disk segments and in-memory buffer.
    let entries = store.read_since(1, 10).unwrap();
    assert_eq!(
        entries.len(),
        7,
        "all 7 entries should be readable after async flush"
    );
    assert_eq!(entries[0].seq, 1);
    assert_eq!(entries[6].seq, 7);
}

#[test]
fn test_local_fs_async_flush_fallback_on_channel_close() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalFsOpLogStore::new(dir.path(), 2).unwrap();

    // Drop the store's flush channel receiver by replacing the sender
    // with one whose receiver is immediately dropped.
    let (dead_tx, dead_rx) = std::sync::mpsc::channel::<Vec<OpLogRecord>>();
    drop(dead_rx);
    let _old_tx = replace_local_fs_flush_sender_for_test(&mut store, dead_tx);

    // Append should trigger the fallback: mpsc::send fails → inline sync flush.
    store.append(&make_entry(0)).unwrap();
    store.append(&make_entry(0)).unwrap();
    store.append(&make_entry(0)).unwrap();

    // Verify data was flushed synchronously via the fallback.
    assert_eq!(store.latest_sequence(), 3);
    let entries = store.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 3);
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
    let mut manager = OpLogManager::new(Some(Box::new(store)), 7);

    let seq = manager.record_remove_durable("k1").unwrap();
    assert_eq!(seq, 1);

    let store = LocalFsOpLogStore::new(dir.path(), 100).unwrap();
    let entries = store.read_since(1, 10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        decode_record_payload_value_for_test(&entries[0].payload).unwrap(),
        json!({"op": "remove", "key": "k1"})
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
fn test_etcd_oplog_entry_key_matches_cpp_format() {
    let key = format_etcd_entry_key_for_test("/oplog/cluster-a", 42);
    assert_eq!(key, "/oplog/cluster-a/00000000000000000042");
    assert!(!key.contains("seq_"));
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
        refcnt: 0,
        handle_valid: true,
        base_addr: 4096,
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
    let payload: serde_json::Value = serde_json::from_str(&parsed.payload).unwrap();
    assert_eq!(payload["op"], "put_end");
    assert_eq!(payload["key"], "k-binary");
    assert_eq!(payload["metadata_payload_base64"], wire.payload);
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

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
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
    let mut manager = OpLogManager::new(Some(Box::new(store)), 7);

    manager.record_put_revoke("k1");

    let store = manager.into_store().unwrap();
    let entries = store.read_since(1, 10).unwrap();
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
    let mut manager = OpLogManager::new(Some(Box::new(store)), 7);

    manager.record_put_end("tenant-a\0k1", 42);

    let store = manager.into_store().unwrap();
    let entries = store.read_since(1, 10).unwrap();
    let payload = decode_record_payload_value_for_test(&entries[0].payload).unwrap();
    assert_eq!(
        payload,
        json!({"op": "put_end", "key": "tenant-a\0k1", "size": 42})
    );
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
    let mut replica =
        serde_json::to_value(local_disk_replica(holder_client_id, None, None)).unwrap();
    let replica = replica.as_object_mut().unwrap();
    replica.remove("local_disk_storage_id");
    replica.remove("local_disk_generation_id");
    let payload = json!({
        "op": "put_end",
        "key": "default\0legacy-local-disk",
        "size": 100,
        "client_id": holder_client_id.to_string(),
        "tenant_id": "default",
        "group_id": "",
        "user_key": "legacy-local-disk",
        "replicas": [replica],
    });
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
    let bytes = b"MCOPMETA3future-body";
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
            .contains("unsupported put_end msgpack schema version '3'"),
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

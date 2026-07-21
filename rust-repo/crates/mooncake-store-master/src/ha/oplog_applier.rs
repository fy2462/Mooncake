//! OpLog applier — replays OpLog entries onto a MasterState.
//! C++ equivalent: `OpLogApplier` in oplog_applier.h/cpp.
//!
//! Parses msgpack/base64 or legacy JSON payloads from OpLogRecord entries and applies the
//! corresponding mutations (put_end, remove, segment mount/unmount)
//! to the shared MasterState.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use std::time::SystemTime;
use uuid::Uuid;

use crate::ha::types::OpLogRecord;
use crate::oplog::{decode_record_payload_value, OpLogStore};
use crate::service::helpers::release_object_replicas;
use crate::service::state::{MasterState, ObjectEntry};
use crate::service::sync_cache_total_accounting;

const MAX_OBJECT_KEY_SIZE: usize = 4096;
const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

/// Applies OpLog entries to a MasterState, tracking the expected sequence ID.
pub(crate) struct OpLogApplier {
    state: Arc<MasterState>,
    expected_seq: AtomicU64,
    pending_entries: Mutex<BTreeMap<u64, OpLogRecord>>,
    max_pending_entries: usize,
}

impl OpLogApplier {
    pub fn new(state: Arc<MasterState>) -> Self {
        Self {
            state,
            expected_seq: AtomicU64::new(1),
            pending_entries: Mutex::new(BTreeMap::new()),
            max_pending_entries: 100_000,
        }
    }

    /// Set the expected sequence ID after snapshot restore.
    /// C++ equivalent: `OpLogApplier::Recover(base_seq)`
    pub fn recover(&self, base_seq: u64) {
        self.expected_seq.store(base_seq + 1, Ordering::Release);
        self.pending_entries.lock().clear();
    }

    /// Return the next expected sequence ID.
    /// C++ equivalent: `OpLogApplier::GetExpectedSequenceId()`
    pub fn get_expected_sequence_id(&self) -> u64 {
        self.expected_seq.load(Ordering::Acquire)
    }

    /// Apply a batch of OpLog entries. Returns count of successfully applied entries.
    /// Skips entries with seq < expected (already applied) and gaps (out-of-order).
    ///
    /// C++ equivalent: `OpLogApplier::ApplyOpLogEntries`
    pub fn apply_op_log_entries(&self, entries: &[OpLogRecord]) -> usize {
        let mut applied = 0usize;
        for entry in entries {
            let expected = self.expected_seq.load(Ordering::Acquire);
            if entry.seq < expected {
                continue; // already applied
            }
            if entry.seq > expected {
                self.buffer_pending_entry(entry.clone());
                continue;
            }
            applied += self.apply_contiguous_entry(entry.clone());
        }
        applied
    }

    fn apply_contiguous_entry(&self, entry: OpLogRecord) -> usize {
        let expected = self.expected_seq.load(Ordering::Acquire);
        if entry.seq != expected || !Self::apply_one(&self.state, &entry.payload) {
            return 0;
        }
        self.expected_seq.store(expected + 1, Ordering::Release);
        let mut applied = 1;

        loop {
            let next = self.expected_seq.load(Ordering::Acquire);
            let Some(pending) = self.pending_entries.lock().remove(&next) else {
                break;
            };
            if !Self::apply_one(&self.state, &pending.payload) {
                break;
            }
            self.expected_seq.store(next + 1, Ordering::Release);
            applied += 1;
        }
        applied
    }

    fn buffer_pending_entry(&self, entry: OpLogRecord) {
        let mut pending = self.pending_entries.lock();
        if pending.len() >= self.max_pending_entries {
            return;
        }
        pending.entry(entry.seq).or_insert(entry);
    }

    /// Try to fill gaps by reading from the OpLogStore.
    /// Returns (attempted, fetched) — attempted = entries we needed, fetched = entries we got.
    ///
    /// C++ equivalent: `OpLogApplier::TryResolveGapsOnceForPromotion(max_ids)`
    pub fn try_resolve_gaps_once(&self, store: &dyn OpLogStore, max_ids: usize) -> (usize, usize) {
        let expected = self.expected_seq.load(Ordering::Acquire);
        let latest = store.latest_sequence();
        if latest < expected {
            return (0, 0);
        }
        let needed = (latest - expected + 1).min(max_ids as u64) as usize;
        if needed == 0 {
            return (0, 0);
        }

        let entries = match store.read_since(expected, needed) {
            Ok(e) => e,
            Err(_) => return (needed, 0),
        };

        let fetched = entries.len();
        self.apply_op_log_entries(&entries);
        (needed, fetched)
    }

    /// Apply a single payload to MasterState.
    fn apply_one(state: &MasterState, payload: &str) -> bool {
        if payload.len() > MAX_PAYLOAD_SIZE {
            return false;
        }
        let v = match decode_record_payload_value(payload) {
            Ok(v) => v,
            Err(_) => return false,
        };

        let Some(op) = v["op"].as_str() else {
            return false;
        };

        match op {
            "put_end" => {
                let Some(key) = v["key"].as_str() else {
                    return false;
                };
                if key.len() > MAX_OBJECT_KEY_SIZE {
                    return false;
                }
                let size = v["size"].as_u64().unwrap_or(0);
                // PutEnd: mark Allocating replicas as Complete.
                // The actual size is recorded on the object entry.
                if let Some(mut entry) = state.objects.get_mut(key) {
                    for replica in &mut entry.replicas {
                        if replica.status == mooncake_store_core::ReplicaStatus::Allocating {
                            replica.status = mooncake_store_core::ReplicaStatus::Complete;
                        }
                    }
                    entry.size = entry.size.max(size);
                    sync_cache_total_accounting(&mut entry);
                } else if let Some(replicas_value) = v.get("replicas") {
                    let Ok(replicas) = serde_json::from_value(replicas_value.clone()) else {
                        return false;
                    };
                    let client_id = v["client_id"]
                        .as_str()
                        .and_then(|id| Uuid::parse_str(id).ok())
                        .unwrap_or_else(Uuid::nil);
                    let tenant_id = v["tenant_id"].as_str().unwrap_or("default").to_string();
                    let user_key = v["user_key"].as_str().unwrap_or(key).to_string();
                    let mut object = ObjectEntry {
                        replicas,
                        size,
                        last_access: SystemTime::now(),
                        hard_pinned: false,
                        data_type: mooncake_store_core::ObjectDataType::General,
                        client_id,
                        put_start_time: None,
                        lease_timeout: None,
                        soft_pin_timeout: None,
                        tenant_id,
                        group_id: v["group_id"].as_str().unwrap_or_default().to_string(),
                        quota_committed: true,
                        memory_cache_total_accounted: false,
                        disk_cache_total_accounted: false,
                        user_key,
                    };
                    sync_cache_total_accounting(&mut object);
                    state.objects.insert(key.to_string(), object);
                }
                state.processing_keys.remove(key);
                true
            }
            "remove" | "put_revoke" => {
                let Some(key) = v["key"].as_str() else {
                    return false;
                };
                if key.len() > MAX_OBJECT_KEY_SIZE {
                    return false;
                }
                Self::apply_remove_like(state, key);
                true
            }
            "mount_segment" => {
                // Segment mount/unmount entries are informational for standby;
                // the snapshot bootstrap already restores segments. Skip.
                true
            }
            "unmount_segment" | "mount_nof_segment" | "unmount_nof_segment" => {
                // Informational only for oplog replay on standby.
                true
            }
            "put_start" => {
                // PutStart is a transient state; the standby only needs put_end.
                true
            }
            _ => false,
        }
    }

    fn apply_remove_like(state: &MasterState, key: &str) {
        if let Some((_, object)) = state.objects.remove(key) {
            release_object_replicas(state, key, &object.replicas);
        }
        state.processing_keys.remove(key);
        state.replication_tasks.remove(key);
        for mut entry in state.client_objects.iter_mut() {
            entry.value_mut().remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::OpLogRecord;
    use dashmap::DashMap;
    use parking_lot::RwLock;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;

    fn make_state() -> Arc<MasterState> {
        Arc::new(MasterState {
            clients: DashMap::new(),
            ok_clients: DashMap::new(),
            objects: DashMap::new(),
            processing_keys: DashMap::new(),
            client_objects: DashMap::new(),
            segments: DashMap::new(),
            nof_segments: DashMap::new(),
            local_disk_segments: DashMap::new(),
            tasks: DashMap::new(),
            replication_tasks: DashMap::new(),
            offloading_tasks: DashMap::new(),
            promotion_tasks: DashMap::new(),
            promotion_sketch: RwLock::new(crate::count_min_sketch::CountMinSketch::new()),
            drain_jobs: DashMap::new(),
            allocator: RwLock::new(crate::allocator::SegmentAllocator::new()),
            nof_allocator: RwLock::new(crate::allocator::SegmentAllocator::new()),
            storage_backend: RwLock::new(None),
            promotion_in_flight: AtomicUsize::new(0),
            view_version: std::sync::atomic::AtomicI64::new(0),
            runtime_config: crate::service::state::MasterRuntimeConfig::default(),
            service_available: AtomicBool::new(true),
            tenant_quotas: RwLock::new(crate::tenant_quota::TenantQuotaTable::new(0)),
            pending_remote_pulls: DashMap::new(),
            nof_heartbeat_states: DashMap::new(),
            kv_event_publisher: Arc::new(
                crate::kv_event::KvEventPublisher::new(Default::default()),
            ),
        })
    }

    #[test]
    fn test_apply_put_end_removes_processing_key() {
        let state = make_state();
        state.processing_keys.insert("k1".to_string(), ());
        let applier = OpLogApplier::new(state.clone());

        let payload = r#"{"op":"put_end","key":"k1","size":100}"#;
        let entries = vec![OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: payload.to_string(),
        }];
        let n = applier.apply_op_log_entries(&entries);
        assert_eq!(n, 1);
        assert!(!state.processing_keys.contains_key("k1"));
    }

    #[test]
    fn test_apply_put_end_recreates_object_from_metadata_payload() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = uuid::Uuid::new_v4();
        let client_id = uuid::Uuid::new_v4();
        let replica = mooncake_store_core::ReplicaDescriptor {
            segment_id,
            segment_name: "seg-a".to_string(),
            offset: 128,
            size: 256,
            status: mooncake_store_core::ReplicaStatus::Complete,
            replica_type: mooncake_store_core::ReplicaType::Memory,
            holder_client_id: Some(client_id),
            refcnt: 0,
            handle_valid: true,
            base_addr: 4096,
            protocol: "rdma".to_string(),
        };
        let payload = serde_json::json!({
            "op": "put_end",
            "key": "tenant-a/k1",
            "size": 256,
            "client_id": client_id.to_string(),
            "tenant_id": "tenant-a",
            "group_id": "group-a",
            "user_key": "k1",
            "replicas": [replica],
        })
        .to_string();

        let n = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload,
        }]);

        assert_eq!(n, 1);
        let object = state.objects.get("tenant-a/k1").unwrap();
        assert_eq!(object.size, 256);
        assert_eq!(object.client_id, client_id);
        assert_eq!(object.tenant_id, "tenant-a");
        assert_eq!(object.group_id, "group-a");
        assert_eq!(object.user_key, "k1");
        assert_eq!(object.replicas.len(), 1);
        assert_eq!(object.replicas[0].segment_id, segment_id);
    }

    #[test]
    fn test_rejects_oversized_oplog_key() {
        let state = make_state();
        let applier = OpLogApplier::new(state);
        let payload = serde_json::json!({
            "op": "remove",
            "key": "k".repeat(MAX_OBJECT_KEY_SIZE + 1),
        })
        .to_string();

        let n = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload,
        }]);

        assert_eq!(n, 0);
        assert_eq!(applier.get_expected_sequence_id(), 1);
    }

    #[test]
    fn test_apply_remove() {
        let state = make_state();
        let client_id = uuid::Uuid::new_v4();
        state.objects.insert(
            "k1".to_string(),
            crate::service::state::ObjectEntry {
                replicas: vec![],
                size: 0,
                last_access: std::time::SystemTime::now(),
                hard_pinned: false,
                data_type: mooncake_store_core::ObjectDataType::General,
                client_id,
                put_start_time: None,
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: "default".to_string(),
                group_id: String::new(),
                quota_committed: false,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "k1".to_string(),
            },
        );
        state
            .client_objects
            .insert(client_id, std::iter::once("k1".to_string()).collect());
        state.processing_keys.insert("k1".to_string(), ());
        let replica = mooncake_store_core::ReplicaDescriptor {
            segment_id: uuid::Uuid::new_v4(),
            segment_name: "seg".to_string(),
            offset: 0,
            size: 0,
            status: mooncake_store_core::ReplicaStatus::Complete,
            replica_type: mooncake_store_core::ReplicaType::Memory,
            holder_client_id: Some(client_id),
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };
        state.replication_tasks.insert(
            "k1".to_string(),
            crate::service::state::ReplicationTaskEntry {
                client_id,
                start_time: std::time::Instant::now(),
                kind: crate::service::state::ReplicationTaskKind::Copy,
                source: replica,
                targets: vec![],
            },
        );
        let applier = OpLogApplier::new(state.clone());

        let payload = r#"{"op":"remove","key":"k1"}"#;
        let entries = vec![OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: payload.to_string(),
        }];
        let n = applier.apply_op_log_entries(&entries);
        assert_eq!(n, 1);
        assert!(!state.objects.contains_key("k1"));
        assert!(!state.processing_keys.contains_key("k1"));
        assert!(!state.replication_tasks.contains_key("k1"));
        assert!(!state.client_objects.get(&client_id).unwrap().contains("k1"));
    }

    #[test]
    fn test_apply_put_revoke_removes_object_and_processing_key() {
        let state = make_state();
        state.objects.insert(
            "k1".to_string(),
            crate::service::state::ObjectEntry {
                replicas: vec![],
                size: 0,
                last_access: std::time::SystemTime::now(),
                hard_pinned: false,
                data_type: mooncake_store_core::ObjectDataType::General,
                client_id: uuid::Uuid::nil(),
                put_start_time: None,
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: "default".to_string(),
                group_id: String::new(),
                quota_committed: false,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "k1".to_string(),
            },
        );
        state.processing_keys.insert("k1".to_string(), ());
        let applier = OpLogApplier::new(state.clone());

        let payload = r#"{"op":"put_revoke","key":"k1"}"#;
        let entries = vec![OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: payload.to_string(),
        }];
        let n = applier.apply_op_log_entries(&entries);
        assert_eq!(n, 1);
        assert!(!state.objects.contains_key("k1"));
        assert!(!state.processing_keys.contains_key("k1"));
    }

    #[test]
    fn test_skip_already_applied() {
        let state = make_state();
        let applier = OpLogApplier::new(state);
        applier.recover(5); // expected = 6

        let payload = r#"{"op":"remove","key":"old"}"#;
        let entries = vec![OpLogRecord {
            seq: 3, // seq < expected → skip
            producer_view_version: 1,
            payload: payload.to_string(),
        }];
        let n = applier.apply_op_log_entries(&entries);
        assert_eq!(n, 0); // skipped, already applied
    }

    #[test]
    fn test_skip_gap() {
        let state = make_state();
        let applier = OpLogApplier::new(state);
        // expected = 1, but entry has seq=5 → gap, skip
        let payload = r#"{"op":"remove","key":"k1"}"#;
        let entries = vec![OpLogRecord {
            seq: 5,
            producer_view_version: 1,
            payload: payload.to_string(),
        }];
        let n = applier.apply_op_log_entries(&entries);
        assert_eq!(n, 0); // gap, not applied
    }

    #[test]
    fn test_pending_gap_applies_when_missing_entries_arrive() {
        let state = make_state();
        let applier = OpLogApplier::new(state);

        let future = vec![OpLogRecord {
            seq: 3,
            producer_view_version: 1,
            payload: r#"{"op":"put_start","key":"k3"}"#.to_string(),
        }];
        assert_eq!(applier.apply_op_log_entries(&future), 0);
        assert_eq!(applier.get_expected_sequence_id(), 1);

        let contiguous = vec![
            OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: r#"{"op":"put_start","key":"k1"}"#.to_string(),
            },
            OpLogRecord {
                seq: 2,
                producer_view_version: 1,
                payload: r#"{"op":"put_start","key":"k2"}"#.to_string(),
            },
        ];

        assert_eq!(applier.apply_op_log_entries(&contiguous), 3);
        assert_eq!(applier.get_expected_sequence_id(), 4);
    }
}

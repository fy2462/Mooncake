//! OpLog applier — replays OpLog entries onto a MasterState.
//! C++ equivalent: `OpLogApplier` in oplog_applier.h/cpp.
//!
//! Parses msgpack/base64 or legacy JSON payloads from OpLogRecord entries and applies the
//! corresponding mutations (put_end, remove, segment mount/unmount)
//! to the shared MasterState.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment, TaskStatus};
use parking_lot::Mutex;
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

use crate::allocator::{AllocationStrategy, SegmentAllocator};
use crate::ha::types::OpLogRecord;
use crate::oplog::{
    OpLogStore, decode_record_payload_value, recover_object_identity_from_payload,
    verify_cpp_struct_pack_empty_replica_payload,
};
use crate::service::helpers::{
    account_cache_total_removal, checked_allocating_memory_quota_charge,
    checked_completed_memory_quota_charge, checked_durable_committed_memory_quota_charge,
    clone_object_for_mutation, object_has_inflight_write, remove_object_from_quota_projection,
};
use crate::service::state::{GracefulUnmountSnapshotEntry, MasterState, ObjectEntry};
use crate::service::sync_cache_total_accounting;
use crate::service::{
    ReplicationTaskSnapshotEntry, clear_offloading_task as clear_offloading_task_runtime,
    clear_promotion_task as clear_promotion_task_runtime,
};

const MAX_OBJECT_KEY_SIZE: usize = 4096;
const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

struct ReplayedAllocatorState {
    memory: SegmentAllocator,
    nof: SegmentAllocator,
    memory_used: HashMap<Uuid, u64>,
    nof_used: HashMap<Uuid, u64>,
}

fn system_time_from_epoch_millis(value: i64) -> Option<SystemTime> {
    let duration = Duration::from_millis(value.unsigned_abs());
    if value >= 0 {
        SystemTime::UNIX_EPOCH.checked_add(duration)
    } else {
        SystemTime::UNIX_EPOCH.checked_sub(duration)
    }
}

/// Applies OpLog entries to a MasterState, tracking the expected sequence ID.
pub(crate) struct OpLogApplier {
    state: Arc<MasterState>,
    expected_seq: AtomicU64,
    apply_lock: Mutex<()>,
    pending_entries: Mutex<BTreeMap<u64, OpLogRecord>>,
    max_pending_entries: usize,
}

impl OpLogApplier {
    pub fn new(state: Arc<MasterState>) -> Self {
        Self {
            state,
            expected_seq: AtomicU64::new(1),
            apply_lock: Mutex::new(()),
            pending_entries: Mutex::new(BTreeMap::new()),
            max_pending_entries: 100_000,
        }
    }

    /// Set the expected sequence ID after snapshot restore.
    /// C++ equivalent: `OpLogApplier::Recover(base_seq)`
    pub fn recover(&self, base_seq: u64) {
        let _apply_guard = self.apply_lock.lock();
        self.state.clear_transient_promotion_candidates();
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
        // Sequence advancement, pending-entry draining, and the corresponding
        // state mutations form one ordered replay stream. Serializing batches
        // prevents two callers from both applying the same expected sequence.
        let _apply_guard = self.apply_lock.lock();
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

        // Snapshot capture must observe either the state before this durable
        // record or the complete state after it. This also covers allocator,
        // quota, segment, and secondary-index mutations made by the handlers.
        let _global_mutation_guard = state.key_mutations.lock_snapshot();

        match op {
            "put_end" => {
                let Some(durable_key) = v["key"].as_str() else {
                    return false;
                };
                if durable_key.len() > MAX_OBJECT_KEY_SIZE {
                    return false;
                }
                let recovered_identity = match recover_object_identity_from_payload(&v) {
                    Ok(identity) => identity,
                    Err(_) => return false,
                };
                let key = match recovered_identity.as_ref() {
                    Some(identity) => identity.scoped_key.clone(),
                    None => {
                        let (tenant_id, user_key) =
                            match crate::TenantId::parse_scoped_key(durable_key) {
                                Ok(identity) => identity,
                                Err(_) => return false,
                            };
                        tenant_id.make_scoped_key(&user_key)
                    }
                };
                let schema_version = match v.get("schema_version") {
                    None => None,
                    Some(value) => {
                        let Some(version) = value.as_u64() else {
                            return false;
                        };
                        if version != 3 {
                            return false;
                        }
                        Some(version)
                    }
                };
                let size = match v.get("size") {
                    None | Some(serde_json::Value::Null) => 0,
                    Some(value) => {
                        let Some(size) = value.as_u64() else {
                            return false;
                        };
                        size
                    }
                };
                let mut replayed_processing = false;
                let replayed_object = if schema_version == Some(3) {
                    let Some(identity) = recovered_identity.as_ref() else {
                        return false;
                    };
                    let Some(object) = Self::object_image_v3_from_payload(&v, identity) else {
                        return false;
                    };
                    Some(object)
                } else {
                    match (recovered_identity.as_ref(), v.get("replicas").cloned()) {
                        (Some(identity), Some(replicas)) => {
                            let Ok(mut replicas) = serde_json::from_value::<
                                Vec<mooncake_store_core::ReplicaDescriptor>,
                            >(replicas) else {
                                return false;
                            };
                            let legacy_cpp_empty_replicas =
                                size > 0 && verify_cpp_struct_pack_empty_replica_payload(&v);
                            if !legacy_cpp_empty_replicas
                                && !Self::validate_replayed_object_geometry(size, &replicas)
                            {
                                return false;
                            }
                            if replicas.iter().any(|replica| {
                                replica.status != mooncake_store_core::ReplicaStatus::Complete
                            }) {
                                // v1/v2 carried no reservation/deadline state, so
                                // an in-flight typed image cannot be reconstructed
                                // without inventing authority.
                                return false;
                            }
                            for replica in &mut replicas {
                                match replica.replica_type {
                                    mooncake_store_core::ReplicaType::Memory
                                    | mooncake_store_core::ReplicaType::NoFSsd => {
                                        // Leader process addresses and
                                        // transport coordinates are never
                                        // routable in the standby process.
                                        replica.handle_valid = false;
                                        replica.base_addr = 0;
                                        replica.protocol.clear();
                                    }
                                    mooncake_store_core::ReplicaType::LocalDisk => {
                                        // Process/session routing is never durable
                                        // HA state. A promoted standby may expose
                                        // these bytes only after Begin/Report/Commit
                                        // binds the exact storage + generation.
                                        replica.holder_client_id = None;
                                        replica.handle_valid = false;
                                        replica.segment_name.clear();
                                    }
                                    mooncake_store_core::ReplicaType::Disk
                                    | mooncake_store_core::ReplicaType::All => {}
                                }
                            }
                            let client_id = match v.get("client_id") {
                                None | Some(serde_json::Value::Null) => Uuid::nil(),
                                Some(serde_json::Value::String(id)) => {
                                    let Ok(id) = Uuid::parse_str(id) else {
                                        return false;
                                    };
                                    id
                                }
                                Some(_) => return false,
                            };
                            let group_id = match v.get("group_id") {
                                None | Some(serde_json::Value::Null) => String::new(),
                                Some(serde_json::Value::String(group_id)) => group_id.clone(),
                                Some(_) => return false,
                            };
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
                                tenant_id: identity.tenant_id.clone(),
                                group_id,
                                quota_committed: true,
                                reserved_quota_charge_bytes: 0,
                                committed_quota_charge_bytes: 0,
                                pending_replaced_quota_charge_bytes: 0,
                                memory_cache_total_accounted: false,
                                disk_cache_total_accounted: false,
                                user_key: identity.user_key.clone(),
                            };
                            let Ok(committed_charge) =
                                checked_completed_memory_quota_charge(&object)
                            else {
                                return false;
                            };
                            object.committed_quota_charge_bytes = committed_charge;
                            Some(object)
                        }
                        _ => None,
                    }
                };

                if let Some(mut object) = replayed_object {
                    replayed_processing = object_has_inflight_write(&object);
                    let allocator_state = match Self::build_allocator_state_for_object_change(
                        state,
                        &key,
                        Some(&object),
                    ) {
                        Ok(allocator_state) => allocator_state,
                        Err(error) => {
                            tracing::warn!(
                                key,
                                %error,
                                "rejected oplog object image with invalid allocator state"
                            );
                            return false;
                        }
                    };
                    let projected_quotas = if state.runtime_config.enable_tenant_quota {
                        let mut projected = state.tenant_quotas.read().clone();
                        if let Some(previous_task) = state.replication_tasks.get(&key)
                            && projected
                                .abort(&object.tenant_id, previous_task.reserved_quota_charge_bytes)
                                .is_err()
                        {
                            return false;
                        }
                        if let Some(previous) = state.objects.get(&key) {
                            if remove_object_from_quota_projection(&mut projected, &previous)
                                .is_err()
                            {
                                return false;
                            }
                        }
                        if object.quota_committed {
                            if projected
                                .restore_object_checked(
                                    &object.tenant_id,
                                    object.committed_quota_charge_bytes,
                                )
                                .is_err()
                            {
                                return false;
                            }
                        } else {
                            if projected
                                .restore_replacement_checked(
                                    &object.tenant_id,
                                    object.reserved_quota_charge_bytes,
                                    object.pending_replaced_quota_charge_bytes,
                                )
                                .is_err()
                            {
                                return false;
                            }
                        }
                        Some(projected)
                    } else {
                        None
                    };
                    // The typed payload is a complete durable object image.
                    // Replace any snapshot-era/in-flight image, and rebuild its
                    // quota contribution from the exact replica set.
                    if let Some((_, mut previous)) = state.objects.remove(&key) {
                        account_cache_total_removal(&mut previous);
                        for mut entry in state.client_objects.iter_mut() {
                            entry.value_mut().remove(&key);
                        }
                    }
                    state.replication_tasks.remove(&key);
                    sync_cache_total_accounting(&mut object);
                    if !replayed_processing && !object.client_id.is_nil() {
                        state
                            .client_objects
                            .entry(object.client_id)
                            .or_default()
                            .insert(key.clone());
                    }
                    state.objects.insert(key.clone(), object);
                    Self::install_replayed_allocator_state(state, allocator_state);
                    if let Some(projected) = projected_quotas {
                        *state.tenant_quotas.write() = projected;
                    }
                } else {
                    // Legacy payloads do not contain a replica image and can
                    // therefore only finish an object restored by the snapshot.
                    let Some(mut entry) = state.objects.get_mut(&key) else {
                        return false;
                    };
                    let was_committed = entry.quota_committed;
                    let reserved_charge = entry.reserved_quota_charge_bytes;
                    // A legacy completion marker carries no image, so it may
                    // only complete the snapshot object already under this
                    // key. This includes an in-place Upsert whose physical
                    // Memory charge was already committed before its
                    // descriptors returned to Allocating.
                    let mut completed = clone_object_for_mutation(&entry);
                    for replica in &mut completed.replicas {
                        if replica.status == mooncake_store_core::ReplicaStatus::Allocating {
                            replica.status = mooncake_store_core::ReplicaStatus::Complete;
                        }
                    }
                    if size != 0 && size != completed.size {
                        return false;
                    }
                    if !was_committed {
                        // Project the complete state before touching either the
                        // object or quota ledger. A malformed/inconsistent
                        // legacy record must leave both unchanged for retry.
                        let Ok(committed_charge) =
                            checked_completed_memory_quota_charge(&completed)
                        else {
                            return false;
                        };
                        if state.runtime_config.enable_tenant_quota {
                            let mut quotas = state.tenant_quotas.write();
                            let mut projected = quotas.clone();
                            if projected
                                .settle(
                                    &completed.tenant_id,
                                    reserved_charge,
                                    committed_charge,
                                    true,
                                )
                                .and_then(|()| {
                                    if completed.pending_replaced_quota_charge_bytes == 0 {
                                        Ok(())
                                    } else {
                                        projected.release(
                                            &completed.tenant_id,
                                            completed.pending_replaced_quota_charge_bytes,
                                        )
                                    }
                                })
                                .is_err()
                            {
                                return false;
                            }
                            *quotas = projected;
                        }
                        completed.quota_committed = true;
                        completed.reserved_quota_charge_bytes = 0;
                        completed.committed_quota_charge_bytes = committed_charge;
                        completed.pending_replaced_quota_charge_bytes = 0;
                    } else {
                        let Ok(committed_charge) =
                            checked_completed_memory_quota_charge(&completed)
                        else {
                            return false;
                        };
                        if completed.committed_quota_charge_bytes != committed_charge {
                            return false;
                        }
                    }
                    completed.put_start_time = None;
                    sync_cache_total_accounting(&mut completed);
                    *entry = completed;
                }
                if replayed_processing {
                    state.processing_keys.insert(key.clone(), ());
                } else {
                    state.processing_keys.remove(&key);
                }
                true
            }
            "remove" | "put_revoke" => {
                if !Self::accept_legacy_or_v1_schema(&v) {
                    return false;
                }
                let Some(durable_key) = v["key"].as_str() else {
                    return false;
                };
                if durable_key.len() > MAX_OBJECT_KEY_SIZE {
                    return false;
                }
                let (tenant_id, user_key) = match crate::TenantId::parse_scoped_key(durable_key) {
                    Ok(identity) => identity,
                    Err(_) => return false,
                };
                let key = tenant_id.make_scoped_key(&user_key);
                let allocator_state =
                    match Self::build_allocator_state_for_object_change(state, &key, None) {
                        Ok(allocator_state) => allocator_state,
                        Err(error) => {
                            tracing::warn!(
                                key,
                                %error,
                                "rejected oplog removal with invalid allocator state"
                            );
                            return false;
                        }
                    };
                if !Self::apply_remove_like(state, &key) {
                    return false;
                }
                Self::install_replayed_allocator_state(state, allocator_state);
                true
            }
            "mount_segment" => Self::apply_mount_segment(state, &v),
            "mount_nof_segment" => Self::apply_mount_nof_segment(state, &v),
            "graceful_unmount_segment" => Self::apply_graceful_unmount_segment(state, &v),
            "segment_status_batch" => Self::apply_segment_status_batch(state, &v),
            "lease_refresh_batch" => Self::apply_lease_refresh_batch(state, &v),
            "replication_start" => Self::apply_replication_start(state, &v),
            "task_state_batch" => Self::apply_task_state_batch(state, &v),
            "object_delayed_release_batch" => Self::apply_object_delayed_release_batch(state, &v),
            "unmount_segment" => Self::apply_unmount_segment(state, &v, false),
            "unmount_nof_segment" => Self::apply_unmount_segment(state, &v, true),
            "put_start" => {
                // PutStart is a transient state; the standby only needs put_end.
                Self::accept_legacy_or_v1_schema(&v)
            }
            _ => false,
        }
    }

    fn object_image_v3_from_payload(
        payload: &serde_json::Value,
        identity: &crate::oplog::RecoveredObjectIdentity,
    ) -> Option<ObjectEntry> {
        if payload.get("schema_version")?.as_u64()? != 3 {
            return None;
        }
        let image = payload.get("object")?.as_object()?;
        let required_u64 = |name: &str| image.get(name)?.as_u64();
        let required_time = |name: &str| -> Option<Option<SystemTime>> {
            match image.get(name)? {
                serde_json::Value::Null => Some(None),
                serde_json::Value::Number(value) => {
                    Some(Some(system_time_from_epoch_millis(value.as_i64()?)?))
                }
                _ => None,
            }
        };
        let size = required_u64("size")?;
        if size == 0 {
            return None;
        }
        let client_id = Uuid::parse_str(image.get("client_id")?.as_str()?).ok()?;
        let group_id = image.get("group_id")?.as_str()?.to_string();
        let hard_pinned = image.get("hard_pinned")?.as_bool()?;
        let data_type = serde_json::from_value(image.get("data_type")?.clone()).ok()?;
        let replicas =
            serde_json::from_value::<Vec<ReplicaDescriptor>>(image.get("replicas")?.clone())
                .ok()?;
        if !Self::validate_replayed_object_geometry(size, &replicas) {
            return None;
        }
        let quota_committed = image.get("quota_committed")?.as_bool()?;
        let reserved_quota_charge_bytes = required_u64("reserved_quota_charge_bytes")?;
        let committed_quota_charge_bytes = required_u64("committed_quota_charge_bytes")?;
        let pending_replaced_quota_charge_bytes =
            required_u64("pending_replaced_quota_charge_bytes")?;

        let last_access = match image.get("last_access_ms") {
            None | Some(serde_json::Value::Null) => SystemTime::now(),
            Some(value) => system_time_from_epoch_millis(value.as_i64()?)?,
        };
        let mut object = ObjectEntry {
            replicas,
            size,
            last_access,
            hard_pinned,
            data_type,
            client_id,
            put_start_time: required_time("put_start_time_ms")?,
            lease_timeout: required_time("lease_timeout_ms")?,
            soft_pin_timeout: required_time("soft_pin_timeout_ms")?,
            tenant_id: identity.tenant_id.clone(),
            group_id,
            quota_committed,
            reserved_quota_charge_bytes,
            committed_quota_charge_bytes,
            pending_replaced_quota_charge_bytes,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: identity.user_key.clone(),
        };
        let completed_charge = checked_durable_committed_memory_quota_charge(&object).ok()?;
        let allocating_charge = checked_allocating_memory_quota_charge(&object).ok()?;
        if (object.quota_committed
            && (object.reserved_quota_charge_bytes != 0
                || object.pending_replaced_quota_charge_bytes != 0
                || object.committed_quota_charge_bytes != completed_charge))
            || (!object.quota_committed
                && (object.committed_quota_charge_bytes != 0
                    || object.reserved_quota_charge_bytes != allocating_charge))
        {
            return None;
        }
        for replica in &mut object.replicas {
            match replica.replica_type {
                ReplicaType::Memory | ReplicaType::NoFSsd => {
                    replica.handle_valid = false;
                    replica.base_addr = 0;
                    replica.protocol.clear();
                }
                ReplicaType::LocalDisk => {
                    replica.holder_client_id = None;
                    replica.handle_valid = false;
                    replica.segment_name.clear();
                }
                ReplicaType::Disk => {}
                ReplicaType::All => unreachable!("ALL replicas were rejected above"),
            }
        }
        if !object_has_inflight_write(&object) {
            object.put_start_time = None;
        }
        Some(object)
    }

    fn apply_object_delayed_release_batch(
        state: &MasterState,
        payload: &serde_json::Value,
    ) -> bool {
        let Some(schema_version) = payload
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
        else {
            return false;
        };
        if schema_version != 1 {
            return false;
        }
        let Some(durable_key) = payload.get("key").and_then(serde_json::Value::as_str) else {
            return false;
        };
        let Ok((tenant_id, user_key)) = crate::TenantId::parse_scoped_key(durable_key) else {
            return false;
        };
        let key = tenant_id.make_scoped_key(&user_key);
        if key != durable_key || key.len() > MAX_OBJECT_KEY_SIZE {
            return false;
        }
        if payload.get("tenant_id").and_then(serde_json::Value::as_str) != Some(tenant_id.as_str())
            || payload.get("user_key").and_then(serde_json::Value::as_str)
                != Some(user_key.as_str())
        {
            return false;
        }
        let replacement = match payload.get("object_image") {
            Some(serde_json::Value::Null) => None,
            Some(encoded) => {
                let Some(identity) = recover_object_identity_from_payload(encoded).ok().flatten()
                else {
                    return false;
                };
                if identity.scoped_key != key {
                    return false;
                }
                let Some(object) = Self::object_image_v3_from_payload(encoded, &identity) else {
                    return false;
                };
                Some(object)
            }
            None => return false,
        };
        let Ok(mut upserts) = serde_json::from_value::<
            Vec<crate::service::state::DelayedReplicaReleaseEntry>,
        >(payload.get("upserts").cloned().unwrap_or_default()) else {
            return false;
        };
        let Ok(removes) = serde_json::from_value::<Vec<Uuid>>(
            payload.get("removes").cloned().unwrap_or_default(),
        ) else {
            return false;
        };
        let mut seen = HashSet::with_capacity(upserts.len() + removes.len());
        if upserts.iter().any(|entry| {
            entry.id.is_nil()
                || entry.scoped_key != key
                || entry.deadline_epoch_ms == 0
                || entry.replicas.is_empty()
                || !seen.insert(entry.id)
                || entry.replicas.iter().any(|replica| {
                    !matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd
                    )
                })
        }) || removes
            .iter()
            .any(|release_id| release_id.is_nil() || !seen.insert(*release_id))
        {
            return false;
        }
        for entry in &mut upserts {
            for replica in &mut entry.replicas {
                replica.handle_valid = false;
                replica.base_addr = 0;
                replica.protocol.clear();
            }
        }

        let mut removed_entries = Vec::new();
        for release_id in &removes {
            if let Some((_, entry)) = state.delayed_replica_releases.remove(release_id) {
                removed_entries.push(entry);
            }
        }
        let mut inserted_ids = Vec::new();
        for entry in &upserts {
            if let Some(existing) = state.delayed_replica_releases.get(&entry.id) {
                if !existing.same_durable_reservation(entry) {
                    drop(existing);
                    for id in inserted_ids {
                        state.delayed_replica_releases.remove(&id);
                    }
                    for removed in removed_entries {
                        state.delayed_replica_releases.insert(removed.id, removed);
                    }
                    return false;
                }
                continue;
            }
            state
                .delayed_replica_releases
                .insert(entry.id, entry.clone());
            inserted_ids.push(entry.id);
        }

        let allocator_state =
            Self::build_allocator_state_for_object_change(state, &key, replacement.as_ref());
        let projected_quotas = (|| {
            if !state.runtime_config.enable_tenant_quota {
                return Some(None);
            }
            let mut projected = state.tenant_quotas.read().clone();
            if let Some(task) = state.replication_tasks.get(&key)
                && projected
                    .abort(&tenant_id, task.reserved_quota_charge_bytes)
                    .is_err()
            {
                return None;
            }
            if let Some(previous) = state.objects.get(&key)
                && remove_object_from_quota_projection(&mut projected, &previous).is_err()
            {
                return None;
            }
            if let Some(object) = replacement.as_ref() {
                if object.quota_committed {
                    projected
                        .restore_object_checked(
                            &object.tenant_id,
                            object.committed_quota_charge_bytes,
                        )
                        .ok()?;
                } else {
                    projected
                        .restore_replacement_checked(
                            &object.tenant_id,
                            object.reserved_quota_charge_bytes,
                            object.pending_replaced_quota_charge_bytes,
                        )
                        .ok()?;
                }
            }
            Some(Some(projected))
        })();
        let (Ok(allocator_state), Some(projected_quotas)) = (allocator_state, projected_quotas)
        else {
            for id in inserted_ids {
                state.delayed_replica_releases.remove(&id);
            }
            for removed in removed_entries {
                state.delayed_replica_releases.insert(removed.id, removed);
            }
            return false;
        };

        if let Some((_, mut previous)) = state.objects.remove(&key) {
            account_cache_total_removal(&mut previous);
        }
        for mut entry in state.client_objects.iter_mut() {
            entry.value_mut().remove(&key);
        }
        state.replication_tasks.remove(&key);
        match replacement {
            Some(mut object) => {
                // C++ UpsertStart makes a same-size replacement unreadable
                // until UpsertEnd, even though it keeps the already occupied
                // Memory quota committed. Match the object-image and snapshot
                // recovery paths: quota ownership is not a write-terminal
                // signal.
                let processing = object_has_inflight_write(&object);
                sync_cache_total_accounting(&mut object);
                if !processing && !object.client_id.is_nil() {
                    state
                        .client_objects
                        .entry(object.client_id)
                        .or_default()
                        .insert(key.clone());
                }
                state.objects.insert(key.clone(), object);
                if processing {
                    state.processing_keys.insert(key.clone(), ());
                } else {
                    state.processing_keys.remove(&key);
                }
            }
            None => {
                state.processing_keys.remove(&key);
            }
        }
        Self::install_replayed_allocator_state(state, allocator_state);
        if let Some(projected) = projected_quotas {
            *state.tenant_quotas.write() = projected;
        }
        true
    }

    fn apply_replication_start(state: &MasterState, payload: &serde_json::Value) -> bool {
        let Some(schema_version) = payload
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
        else {
            return false;
        };
        if !matches!(schema_version, 1 | 2) {
            return false;
        }
        let Some(identity) = recover_object_identity_from_payload(payload).ok().flatten() else {
            return false;
        };
        if identity.scoped_key.len() > MAX_OBJECT_KEY_SIZE {
            return false;
        }
        let synthetic_object_payload = serde_json::json!({
            "schema_version": 3,
            "object": payload.get("object"),
        });
        let Some(mut object) =
            Self::object_image_v3_from_payload(&synthetic_object_payload, &identity)
        else {
            return false;
        };
        // Native replication begins from an already committed object. Its new
        // target reservations live on the task, not on the object image.
        if !object.quota_committed {
            return false;
        }

        let Some(encoded_task) = payload.get("task") else {
            return false;
        };
        let Ok(snapshot_task) =
            serde_json::from_value::<ReplicationTaskSnapshotEntry>(encoded_task.clone())
        else {
            return false;
        };
        let (task_key, mut task) = snapshot_task.into_runtime(Instant::now());
        if task_key != identity.scoped_key || task.client_id.is_nil() {
            return false;
        }
        if schema_version == 1 && task.existing_move_target.is_some() {
            return false;
        }
        match task.kind {
            crate::service::ReplicationTaskKind::Copy if task.existing_move_target.is_some() => {
                return false;
            }
            crate::service::ReplicationTaskKind::Move
                if !matches!(
                    (task.targets.as_slice(), task.existing_move_target.as_ref()),
                    ([_], None) | ([], Some(_))
                ) =>
            {
                return false;
            }
            _ => {}
        }

        let same_location = |left: &ReplicaDescriptor, right: &ReplicaDescriptor| {
            left.segment_id == right.segment_id
                && left.offset == right.offset
                && left.size == right.size
                && left.replica_type == right.replica_type
                && left.local_disk_storage_id == right.local_disk_storage_id
                && left.local_disk_generation_id == right.local_disk_generation_id
        };
        let Some(source_index) = object
            .replicas
            .iter()
            .position(|replica| same_location(replica, &task.source))
        else {
            return false;
        };
        if object.replicas[source_index].status != ReplicaStatus::Complete
            || !matches!(
                object.replicas[source_index].replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            )
        {
            return false;
        }

        let mut target_indices = Vec::with_capacity(task.targets.len());
        let mut seen_targets = HashSet::with_capacity(task.targets.len());
        for target in &task.targets {
            let Some(index) = object
                .replicas
                .iter()
                .position(|replica| same_location(replica, target))
            else {
                return false;
            };
            if object.replicas[index].status != ReplicaStatus::Allocating
                || !seen_targets.insert(index)
            {
                return false;
            }
            target_indices.push(index);
        }
        let target_index_set = target_indices.iter().copied().collect::<HashSet<_>>();
        if object.replicas.iter().enumerate().any(|(index, replica)| {
            replica.status != ReplicaStatus::Complete && !target_index_set.contains(&index)
        }) {
            return false;
        }
        let Some(expected_reservation) = object
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, replica)| {
                target_index_set.contains(index) && replica.replica_type == ReplicaType::Memory
            })
            .try_fold(0u64, |total, (_, replica)| total.checked_add(replica.size))
        else {
            return false;
        };
        if task.reserved_quota_charge_bytes != expected_reservation {
            return false;
        }
        let existing_move_target_index = if let Some(existing_target) = &task.existing_move_target {
            let Some(index) = object
                .replicas
                .iter()
                .position(|replica| same_location(replica, existing_target))
            else {
                return false;
            };
            if index == source_index
                || object.replicas[index].status != ReplicaStatus::Complete
                || !matches!(
                    object.replicas[index].replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                )
            {
                return false;
            }
            Some(index)
        } else {
            None
        };

        // Serialized ReplicaDescriptor intentionally excludes refcnt. Restore
        // the source pin from the durable task and canonicalize the task's
        // descriptors to the sanitized standby object image.
        object.replicas[source_index].inc_refcnt();
        task.source = object.replicas[source_index].clone();
        task.targets = target_indices
            .into_iter()
            .map(|index| object.replicas[index].clone())
            .collect();
        task.existing_move_target =
            existing_move_target_index.map(|index| object.replicas[index].clone());

        let allocator_state = match Self::build_allocator_state_for_object_change(
            state,
            &identity.scoped_key,
            Some(&object),
        ) {
            Ok(allocator_state) => allocator_state,
            Err(error) => {
                tracing::warn!(
                    key = %identity.scoped_key,
                    %error,
                    "rejected replication_start with invalid allocator state"
                );
                return false;
            }
        };
        let projected_quotas = if state.runtime_config.enable_tenant_quota {
            let mut projected = state.tenant_quotas.read().clone();
            if let Some(previous_task) = state.replication_tasks.get(&identity.scoped_key) {
                if projected
                    .abort(
                        &identity.tenant_id,
                        previous_task.reserved_quota_charge_bytes,
                    )
                    .is_err()
                {
                    return false;
                }
            }
            if let Some(previous) = state.objects.get(&identity.scoped_key)
                && remove_object_from_quota_projection(&mut projected, &previous).is_err()
            {
                return false;
            }
            if projected
                .restore_object_checked(&object.tenant_id, object.committed_quota_charge_bytes)
                .is_err()
                || projected
                    .restore_reservation_checked(
                        &object.tenant_id,
                        task.reserved_quota_charge_bytes,
                    )
                    .is_err()
            {
                return false;
            }
            Some(projected)
        } else {
            None
        };

        if let Some((_, mut previous)) = state.objects.remove(&identity.scoped_key) {
            account_cache_total_removal(&mut previous);
            for mut entry in state.client_objects.iter_mut() {
                entry.value_mut().remove(&identity.scoped_key);
            }
        }
        state.replication_tasks.remove(&identity.scoped_key);
        state.processing_keys.remove(&identity.scoped_key);
        sync_cache_total_accounting(&mut object);
        if !object.client_id.is_nil() {
            state
                .client_objects
                .entry(object.client_id)
                .or_default()
                .insert(identity.scoped_key.clone());
        }
        state.objects.insert(identity.scoped_key.clone(), object);
        state
            .replication_tasks
            .insert(identity.scoped_key.clone(), task);
        Self::install_replayed_allocator_state(state, allocator_state);
        if let Some(projected) = projected_quotas {
            *state.tenant_quotas.write() = projected;
        }
        true
    }

    fn apply_task_state_batch(state: &MasterState, payload: &serde_json::Value) -> bool {
        if payload
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            != Some(1)
        {
            return false;
        }
        let Some(encoded_upserts) = payload.get("upserts").and_then(serde_json::Value::as_array)
        else {
            return false;
        };
        let Some(encoded_removes) = payload.get("removes").and_then(serde_json::Value::as_array)
        else {
            return false;
        };
        if encoded_upserts.is_empty() && encoded_removes.is_empty() {
            return false;
        }

        let mut seen = HashSet::with_capacity(encoded_upserts.len() + encoded_removes.len());
        let mut upserts = Vec::with_capacity(encoded_upserts.len());
        for encoded in encoded_upserts {
            let Ok(mut task) = serde_json::from_value::<crate::service::TaskEntry>(encoded.clone())
            else {
                return false;
            };
            if task.info.id.is_nil()
                || !seen.insert(task.info.id)
                || task
                    .info
                    .assigned_client
                    .is_some_and(|client_id| client_id.is_nil())
                || task.info.last_updated_at < task.info.created_at
                || task.max_retry_attempts == 0
            {
                return false;
            }
            let active = matches!(
                task.info.status,
                TaskStatus::Pending | TaskStatus::Processing
            );
            if active && task.info.assigned_client.is_none() {
                return false;
            }
            let Ok((tenant_id, user_key)) = crate::TenantId::parse_scoped_key(&task.key) else {
                return false;
            };
            if user_key.contains('\0') {
                return false;
            }
            task.key = tenant_id.make_scoped_key(&user_key);
            let Ok(canonical_payload) =
                crate::service::canonicalize_snapshot_task_payload(&task, &task.key)
            else {
                return false;
            };
            task.payload = canonical_payload;
            if active && !state.objects.contains_key(&task.key) {
                return false;
            }
            if let Some(previous) = state.tasks.get(&task.info.id) {
                let valid_transition = matches!(
                    (previous.info.status, task.info.status),
                    (
                        TaskStatus::Pending,
                        TaskStatus::Pending | TaskStatus::Processing
                    ) | (
                        TaskStatus::Pending | TaskStatus::Processing,
                        TaskStatus::Success | TaskStatus::Failed
                    ) | (TaskStatus::Processing, TaskStatus::Processing)
                        | (TaskStatus::Success, TaskStatus::Success)
                        | (TaskStatus::Failed, TaskStatus::Failed)
                );
                if !valid_transition
                    || previous.key != task.key
                    || previous.payload != task.payload
                    || previous.max_retry_attempts != task.max_retry_attempts
                    || previous.info.task_type != task.info.task_type
                    || previous.info.created_at != task.info.created_at
                    || previous.info.assigned_client != task.info.assigned_client
                    || task.info.last_updated_at < previous.info.last_updated_at
                    || (matches!(
                        previous.info.status,
                        TaskStatus::Success | TaskStatus::Failed
                    ) && (previous.info.status != task.info.status
                        || previous.info.message != task.info.message))
                {
                    return false;
                }
            }
            upserts.push(task);
        }

        let mut removes = Vec::with_capacity(encoded_removes.len());
        for encoded in encoded_removes {
            let Some(task_id) = encoded
                .as_str()
                .and_then(|value| Uuid::parse_str(value).ok())
            else {
                return false;
            };
            if task_id.is_nil() || !seen.insert(task_id) {
                return false;
            }
            removes.push(task_id);
        }
        for task in upserts {
            state.tasks.insert(task.info.id, task);
        }
        for task_id in removes {
            state.tasks.remove(&task_id);
        }
        true
    }

    fn validate_replayed_object_geometry(size: u64, replicas: &[ReplicaDescriptor]) -> bool {
        if size == 0 || replicas.is_empty() {
            return false;
        }
        let mut locations = HashSet::with_capacity(replicas.len());
        replicas.iter().all(|replica| {
            replica.replica_type != ReplicaType::All
                && replica.size == size
                && replica.offset.checked_add(replica.size).is_some()
                && (!matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                ) || !replica.segment_id.is_nil())
                && locations.insert((
                    replica.segment_id,
                    replica.offset,
                    replica.size,
                    replica.replica_type,
                    replica.local_disk_storage_id,
                    replica.local_disk_generation_id,
                ))
        })
    }

    fn accept_legacy_or_v1_schema(payload: &serde_json::Value) -> bool {
        match payload.get("schema_version") {
            None => true,
            Some(version) => version.as_u64() == Some(1),
        }
    }

    fn valid_segment_status_transition(
        current: crate::proto::SegmentStatus,
        target: crate::proto::SegmentStatus,
    ) -> bool {
        matches!(
            (current, target),
            (
                crate::proto::SegmentStatus::Active,
                crate::proto::SegmentStatus::Active | crate::proto::SegmentStatus::Draining
            ) | (
                crate::proto::SegmentStatus::Draining,
                crate::proto::SegmentStatus::Active
                    | crate::proto::SegmentStatus::Draining
                    | crate::proto::SegmentStatus::Unavailable
            ) | (
                crate::proto::SegmentStatus::Unavailable,
                crate::proto::SegmentStatus::Unavailable
            )
        )
    }

    fn apply_remove_like(state: &MasterState, key: &str) -> bool {
        let projected_quotas = if state.runtime_config.enable_tenant_quota {
            let mut projected = state.tenant_quotas.read().clone();
            let Ok((tenant_id, _)) = crate::TenantId::parse_scoped_key(key) else {
                return false;
            };
            if let Some(task) = state.replication_tasks.get(key)
                && projected
                    .abort(&tenant_id, task.reserved_quota_charge_bytes)
                    .is_err()
            {
                return false;
            }
            if let Some(object) = state.objects.get(key) {
                if remove_object_from_quota_projection(&mut projected, &object).is_err() {
                    return false;
                }
            }
            Some(projected)
        } else {
            None
        };
        if let Some((_, mut object)) = state.objects.remove(key) {
            account_cache_total_removal(&mut object);
        }
        // The caller has already built a complete allocator candidate from
        // the post-record authoritative object/delayed-release set. Releasing
        // through the old runtime allocator here would be both redundant and
        // non-transactional: a stale allocator could fail after metadata was
        // removed, making a retry observe a different state. Auxiliary runtime
        // tasks are infallible cleanup; the candidate allocator is installed
        // immediately after this function returns.
        clear_offloading_task_runtime(state, key);
        clear_promotion_task_runtime(state, key);
        state.processing_keys.remove(key);
        state.replication_tasks.remove(key);
        for mut entry in state.client_objects.iter_mut() {
            entry.value_mut().remove(key);
        }
        if let Some(projected) = projected_quotas {
            *state.tenant_quotas.write() = projected;
        }
        true
    }

    fn apply_lease_refresh_batch(state: &MasterState, payload: &serde_json::Value) -> bool {
        if payload
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            != Some(1)
        {
            return false;
        }
        let Some(entries) = payload.get("entries").and_then(serde_json::Value::as_array) else {
            return false;
        };
        if entries.is_empty() {
            return false;
        }

        let mut seen = HashSet::with_capacity(entries.len());
        let mut projected = Vec::with_capacity(entries.len());
        let mut batch_identity: Option<(crate::TenantId, String)> = None;
        for encoded in entries {
            let Some(key) = encoded.get("key").and_then(serde_json::Value::as_str) else {
                return false;
            };
            if key.len() > MAX_OBJECT_KEY_SIZE || !seen.insert(key.to_owned()) {
                return false;
            }
            let Ok((scoped_tenant, user_key)) = crate::TenantId::parse_scoped_key(key) else {
                return false;
            };
            if scoped_tenant.make_scoped_key(&user_key) != key {
                return false;
            }
            let Some(encoded_tenant) = encoded
                .get("tenant_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| crate::TenantId::new(value.to_owned()).ok())
            else {
                return false;
            };
            let Some(group_id) = encoded.get("group_id").and_then(serde_json::Value::as_str) else {
                return false;
            };
            if encoded_tenant != scoped_tenant {
                return false;
            }
            match &batch_identity {
                Some((tenant_id, expected_group))
                    if tenant_id != &encoded_tenant || expected_group.as_str() != group_id =>
                {
                    return false;
                }
                None => batch_identity = Some((encoded_tenant.clone(), group_id.to_owned())),
                _ => {}
            }
            let Some(lease_timeout) = encoded
                .get("lease_timeout_ms")
                .and_then(serde_json::Value::as_i64)
                .and_then(system_time_from_epoch_millis)
            else {
                return false;
            };
            let last_access = match encoded.get("last_access_ms") {
                None => None,
                Some(value) => {
                    let Some(last_access) = value.as_i64().and_then(system_time_from_epoch_millis)
                    else {
                        return false;
                    };
                    Some(last_access)
                }
            };
            let soft_pin_timeout = match encoded.get("soft_pin_timeout_ms") {
                Some(serde_json::Value::Null) => None,
                Some(value) => {
                    let Some(deadline) = value.as_i64().and_then(system_time_from_epoch_millis)
                    else {
                        return false;
                    };
                    Some(deadline)
                }
                None => return false,
            };

            let Some(current) = state.objects.get(key) else {
                return false;
            };
            if current.tenant_id != encoded_tenant
                || current.group_id != group_id
                || current.soft_pin_timeout.is_some() != soft_pin_timeout.is_some()
            {
                return false;
            }
            let mut replacement = clone_object_for_mutation(&current);
            if let Some(last_access) = last_access {
                replacement.last_access = replacement.last_access.max(last_access);
            }
            replacement.lease_timeout = Some(
                replacement
                    .lease_timeout
                    .map(|current| current.max(lease_timeout))
                    .unwrap_or(lease_timeout),
            );
            replacement.soft_pin_timeout = match (replacement.soft_pin_timeout, soft_pin_timeout) {
                (Some(current), Some(replayed)) => Some(current.max(replayed)),
                (None, None) => None,
                _ => return false,
            };
            projected.push((key.to_owned(), replacement));
        }

        let Some((tenant_id, group_id)) = batch_identity else {
            return false;
        };
        if group_id.is_empty() {
            if projected.len() != 1 {
                return false;
            }
        } else {
            let expected_group_members = state
                .objects
                .iter()
                .filter(|object| {
                    object.tenant_id == tenant_id
                        && object.group_id == group_id
                        && object
                            .replicas
                            .iter()
                            .any(|replica| replica.status == ReplicaStatus::Complete)
                })
                .map(|object| object.key().clone())
                .collect::<HashSet<_>>();
            if expected_group_members != seen {
                return false;
            }
        }

        for (key, replacement) in projected {
            let Some(mut current) = state.objects.get_mut(&key) else {
                return false;
            };
            *current = replacement;
        }
        true
    }

    fn apply_mount_segment(state: &MasterState, payload: &serde_json::Value) -> bool {
        let Some(schema_version) = payload.get("schema_version") else {
            // Legacy mount entries did not contain enough data to reconstruct
            // a segment. Treat them as informational only when the snapshot
            // already contains one unambiguous matching topology entry and
            // allocator; otherwise advancing would silently lose the mount.
            return Self::legacy_mount_already_present(state, payload, false);
        };
        if schema_version.as_u64() != Some(1) {
            return false;
        }
        let Some(mut segment) = Self::memory_segment_from_mount_payload(payload) else {
            return false;
        };
        let Some(client_id) = payload["client_id"]
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return false;
        };
        if client_id.is_nil() {
            return false;
        }
        if segment.protocol == "cxl" {
            if !state.runtime_config.enable_cxl || segment.size != state.runtime_config.cxl_size {
                return false;
            }
        } else if state.runtime_config.memory_allocator_kind
            == crate::allocator::MemoryAllocatorKind::CachelibLike
            && segment.size % crate::allocator::CACHELIB_SLAB_SIZE != 0
        {
            return false;
        }
        if state.runtime_config.memory_allocator_kind
            == crate::allocator::MemoryAllocatorKind::CachelibLike
            && segment.size > crate::allocator::CACHELIB_MAX_SEGMENT_SIZE
        {
            return false;
        }
        match payload.get("identity_version") {
            None => {}
            Some(version) if version.as_u64() == Some(1) => {
                if mooncake_store_core::stable_memory_segment_id(
                    client_id,
                    &segment.name,
                    segment.base,
                    segment.size,
                    &segment.te_endpoint,
                    &segment.protocol,
                    &segment.host_id,
                ) != segment.id
                {
                    return false;
                }
            }
            Some(_) => return false,
        }
        if state.nof_segments.contains_key(&segment.id)
            || state.nof_allocator.read().used_bytes(&segment.id).is_some()
        {
            return false;
        }
        if let Some(existing) = state.segments.get(&segment.id) {
            let identity_matches = existing.segment.name == segment.name
                && existing.segment.size == segment.size
                && existing.segment.host_id == segment.host_id
                && existing.client_id == client_id;
            drop(existing);
            let allocator_present = state.allocator.read().used_bytes(&segment.id).is_some();
            return allocator_present && identity_matches;
        }
        segment.base = 0;
        segment.te_endpoint.clear();
        segment.protocol.clear();
        let mut allocator = state.allocator.write();
        if allocator.used_bytes(&segment.id).is_some() {
            // Topology and allocator must be installed atomically. An
            // allocator-only identity is corruption; overwriting it could
            // turn live or quarantined ranges into free capacity.
            return false;
        }
        allocator.add_segment(segment.clone(), 0, client_id);
        if allocator.invalidate_segment_runtime(&segment.id).is_err() {
            allocator.remove_segment(&segment.id);
            return false;
        }
        drop(allocator);
        state.segments.insert(
            segment.id,
            crate::service::SegmentEntry {
                segment,
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        true
    }

    fn apply_mount_nof_segment(state: &MasterState, payload: &serde_json::Value) -> bool {
        if !state.runtime_config.enable_nof {
            return false;
        }
        let Some(schema_version) = payload.get("schema_version") else {
            return Self::legacy_mount_already_present(state, payload, true);
        };
        if schema_version.as_u64() != Some(1) {
            return false;
        }
        let Some(segment_id) = payload["segment_id"]
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return false;
        };
        let Some(client_id) = payload["client_id"]
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return false;
        };
        if segment_id.is_nil() || client_id.is_nil() {
            return false;
        }
        let Some(name) = payload["segment_name"]
            .as_str()
            .filter(|name| !name.is_empty())
        else {
            return false;
        };
        let (Some(base), Some(size), Some(te_endpoint)) = (
            payload["base"].as_u64(),
            payload["size"].as_u64(),
            payload["te_endpoint"].as_str(),
        ) else {
            return false;
        };
        if size == 0 || te_endpoint.is_empty() {
            return false;
        }
        if state.runtime_config.memory_allocator_kind
            == crate::allocator::MemoryAllocatorKind::CachelibLike
            && (base % crate::allocator::CACHELIB_SLAB_SIZE != 0
                || size % crate::allocator::CACHELIB_SLAB_SIZE != 0)
        {
            return false;
        }
        if state.runtime_config.memory_allocator_kind
            == crate::allocator::MemoryAllocatorKind::CachelibLike
            && size > crate::allocator::CACHELIB_MAX_SEGMENT_SIZE
        {
            return false;
        }
        if state.segments.contains_key(&segment_id)
            || state.allocator.read().used_bytes(&segment_id).is_some()
        {
            return false;
        }
        if let Some(existing) = state.nof_segments.get(&segment_id) {
            let identity_matches = existing.segment.name == name
                && existing.segment.size == size
                && existing.segment.client_id == client_id;
            drop(existing);
            let allocator_present = state.nof_allocator.read().used_bytes(&segment_id).is_some();
            return allocator_present && identity_matches;
        }
        let segment = mooncake_store_core::NoFSegment {
            id: segment_id,
            name: name.to_string(),
            base: 0,
            size,
            te_endpoint: String::new(),
            client_id,
        };
        let mut allocator = state.nof_allocator.write();
        if allocator.used_bytes(&segment_id).is_some() {
            return false;
        }
        allocator.add_segment(
            Segment {
                id: segment_id,
                name: name.to_string(),
                base: 0,
                size,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            },
            0,
            client_id,
        );
        if allocator.invalidate_segment_runtime(&segment_id).is_err() {
            allocator.remove_segment(&segment_id);
            return false;
        }
        drop(allocator);
        state.nof_segments.insert(
            segment_id,
            crate::service::NoFSegmentEntry {
                segment,
                used: 0,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        true
    }

    fn legacy_mount_already_present(
        state: &MasterState,
        payload: &serde_json::Value,
        nof: bool,
    ) -> bool {
        let Some(name) = payload["segment_name"]
            .as_str()
            .filter(|name| !name.is_empty())
        else {
            return false;
        };
        let segment_id = if nof {
            let mut matches = state
                .nof_segments
                .iter()
                .filter(|entry| entry.segment.name == name);
            let Some(segment_id) = matches.next().map(|entry| entry.segment.id) else {
                return false;
            };
            if matches.next().is_some() {
                return false;
            }
            segment_id
        } else {
            let mut matches = state
                .segments
                .iter()
                .filter(|entry| entry.segment.name == name);
            let Some(segment_id) = matches.next().map(|entry| entry.segment.id) else {
                return false;
            };
            if matches.next().is_some() {
                return false;
            }
            segment_id
        };
        if nof {
            state.nof_allocator.read().used_bytes(&segment_id).is_some()
        } else {
            state.allocator.read().used_bytes(&segment_id).is_some()
        }
    }

    fn memory_segment_from_mount_payload(payload: &serde_json::Value) -> Option<Segment> {
        let id = payload["segment_id"]
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())?;
        if id.is_nil() {
            return None;
        }
        let name = payload["segment_name"]
            .as_str()
            .filter(|name| !name.is_empty())?
            .to_string();
        let base = payload["base"].as_u64()?;
        let size = payload["size"].as_u64()?;
        let protocol = payload["protocol"].as_str()?.to_string();
        if size == 0 || (protocol != "cxl" && base == 0) {
            return None;
        }
        Some(Segment {
            id,
            name,
            base,
            size,
            te_endpoint: payload["te_endpoint"].as_str()?.to_string(),
            protocol,
            host_id: payload
                .get("host_id")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string(),
        })
    }

    fn apply_graceful_unmount_segment(state: &MasterState, payload: &serde_json::Value) -> bool {
        if payload
            .get("schema_version")
            .and_then(|value| value.as_u64())
            != Some(1)
        {
            return false;
        }
        let Some(segment_id) = payload["segment_id"]
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return false;
        };
        let Some(client_id) = payload["client_id"]
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return false;
        };
        let Some(segment_name) = payload["segment_name"]
            .as_str()
            .filter(|name| !name.is_empty())
        else {
            return false;
        };
        if segment_id.is_nil() || client_id.is_nil() {
            return false;
        }
        let Some(deadline_epoch_ms) = payload["deadline_epoch_ms"]
            .as_u64()
            .filter(|deadline| *deadline != 0)
        else {
            return false;
        };

        let existing_intent = state
            .graceful_unmounts
            .get(&segment_id)
            .map(|entry| entry.value().clone());
        if existing_intent.as_ref().is_some_and(|entry| {
            entry.segment_id != segment_id
                || entry.client_id != client_id
                || entry.deadline_epoch_ms == 0
        }) {
            return false;
        }

        let Some(segment) = state.segments.get(&segment_id) else {
            // Idempotent when a snapshot already includes the later completion.
            state.graceful_unmounts.remove(&segment_id);
            return true;
        };
        if segment.client_id != client_id || segment.segment.name != segment_name {
            return false;
        }
        match segment.status {
            crate::proto::SegmentStatus::Active
            | crate::proto::SegmentStatus::GracefullyUnmounting => {}
            _ => return false,
        }
        drop(segment);

        let effective_deadline = existing_intent
            .map(|entry| entry.deadline_epoch_ms.min(deadline_epoch_ms))
            .unwrap_or(deadline_epoch_ms);
        let Some(mut segment) = state.segments.get_mut(&segment_id) else {
            return false;
        };
        if segment.client_id != client_id
            || segment.segment.name != segment_name
            || !matches!(
                segment.status,
                crate::proto::SegmentStatus::Active
                    | crate::proto::SegmentStatus::GracefullyUnmounting
            )
        {
            return false;
        }
        segment.status = crate::proto::SegmentStatus::GracefullyUnmounting;
        drop(segment);

        state.graceful_unmounts.insert(
            segment_id,
            GracefulUnmountSnapshotEntry {
                segment_id,
                client_id,
                deadline_epoch_ms: effective_deadline,
            },
        );
        true
    }

    fn apply_segment_status_batch(state: &MasterState, payload: &serde_json::Value) -> bool {
        if payload
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            != Some(1)
        {
            return false;
        }
        let Some(entries) = payload.get("entries").and_then(serde_json::Value::as_array) else {
            return false;
        };
        if entries.is_empty() {
            return false;
        }

        let mut projected = Vec::with_capacity(entries.len());
        let mut seen = std::collections::HashSet::with_capacity(entries.len());
        for entry in entries {
            let Some(segment_id) = entry
                .get("segment_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
            else {
                return false;
            };
            if segment_id.is_nil() {
                return false;
            }
            let Some(nof) = entry.get("nof").and_then(serde_json::Value::as_bool) else {
                return false;
            };
            let Some(status) = entry
                .get("status")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .and_then(|value| crate::proto::SegmentStatus::try_from(value).ok())
            else {
                return false;
            };
            if !matches!(
                status,
                crate::proto::SegmentStatus::Active
                    | crate::proto::SegmentStatus::Draining
                    | crate::proto::SegmentStatus::Unavailable
            ) || !seen.insert((nof, segment_id))
            {
                return false;
            }
            let current_status = if nof {
                state
                    .nof_segments
                    .get(&segment_id)
                    .map(|segment| segment.status)
            } else {
                state
                    .segments
                    .get(&segment_id)
                    .map(|segment| segment.status)
            };
            let Some(current_status) = current_status else {
                return false;
            };
            if !Self::valid_segment_status_transition(current_status, status) {
                return false;
            }
            projected.push((segment_id, nof, status));
        }

        for (segment_id, nof, status) in projected {
            if nof {
                let Some(mut segment) = state.nof_segments.get_mut(&segment_id) else {
                    return false;
                };
                segment.status = status;
            } else {
                let Some(mut segment) = state.segments.get_mut(&segment_id) else {
                    return false;
                };
                segment.status = status;
            }
        }
        true
    }

    fn apply_unmount_segment(state: &MasterState, payload: &serde_json::Value, nof: bool) -> bool {
        if !Self::accept_legacy_or_v1_schema(payload) {
            return false;
        }
        let Some(segment_id) = payload["segment_id"]
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return false;
        };
        if segment_id.is_nil() {
            return false;
        }
        let Some(segment_name) = payload["segment_name"].as_str() else {
            return false;
        };
        let identity_matches = if nof {
            state
                .nof_segments
                .get(&segment_id)
                .is_none_or(|segment| segment.segment.name == segment_name)
        } else {
            state
                .segments
                .get(&segment_id)
                .is_none_or(|segment| segment.segment.name == segment_name)
        };
        if !identity_matches {
            return false;
        }
        let replica_type = if nof {
            ReplicaType::NoFSsd
        } else {
            ReplicaType::Memory
        };
        let affected_keys = state
            .objects
            .iter()
            .filter(|object| {
                object.replicas.iter().any(|replica| {
                    replica.segment_id == segment_id && replica.replica_type == replica_type
                })
            })
            .map(|object| object.key().clone())
            .collect::<Vec<_>>();
        let mut projected_quotas = state
            .runtime_config
            .enable_tenant_quota
            .then(|| state.tenant_quotas.read().clone());
        let mut changes = Vec::with_capacity(affected_keys.len());
        for key in affected_keys {
            let Some(previous) = state.objects.get(&key) else {
                return false;
            };
            let replication_task = state.replication_tasks.get(&key).map(|task| task.clone());
            let promotion_task = state.promotion_tasks.get(&key).map(|task| task.clone());
            let offloading_task = state.offloading_tasks.get(&key).map(|task| task.clone());
            let was_processing = state.processing_keys.contains_key(&key);
            let was_client_indexed = !previous.client_id.is_nil()
                && state
                    .client_objects
                    .get(&previous.client_id)
                    .is_some_and(|keys| keys.contains(&key));
            if let Some(quotas) = projected_quotas.as_mut() {
                if remove_object_from_quota_projection(quotas, &previous).is_err() {
                    return false;
                }
            }
            let mut replacement = clone_object_for_mutation(&previous);
            drop(previous);
            replacement.replicas.retain(|replica| {
                replica.segment_id != segment_id || replica.replica_type != replica_type
            });
            replacement.memory_cache_total_accounted = false;
            replacement.disk_cache_total_accounted = false;
            let clear_replication_task = replication_task.as_ref().is_some_and(|task| {
                replacement.replicas.is_empty()
                    || (task.source.segment_id == segment_id
                        && task.source.replica_type == replica_type)
                    || task.targets.iter().any(|target| {
                        target.segment_id == segment_id && target.replica_type == replica_type
                    })
                    || task.existing_move_target.as_ref().is_some_and(|target| {
                        target.segment_id == segment_id && target.replica_type == replica_type
                    })
            });
            let clear_promotion_task = promotion_task.as_ref().is_some_and(|task| {
                replacement.replicas.is_empty()
                    || (replica_type == ReplicaType::Memory
                        && task.staged_segment_id == Some(segment_id))
            });
            let clear_offloading_task = offloading_task.as_ref().is_some_and(|task| {
                replacement.replicas.is_empty()
                    || (task.source.segment_id == segment_id
                        && task.source.replica_type == replica_type)
            });
            if let Some(quotas) = projected_quotas.as_mut() {
                if clear_replication_task
                    && quotas
                        .abort(
                            &replacement.tenant_id,
                            replication_task
                                .as_ref()
                                .expect("clear flag has a replication task")
                                .reserved_quota_charge_bytes,
                        )
                        .is_err()
                {
                    return false;
                }
                if clear_promotion_task
                    && quotas
                        .abort(
                            &replacement.tenant_id,
                            promotion_task
                                .as_ref()
                                .expect("clear flag has a promotion task")
                                .reserved_quota_charge_bytes,
                        )
                        .is_err()
                {
                    return false;
                }
            }
            if clear_replication_task
                && let Some(task) = replication_task.as_ref()
                && let Some(source) = replacement.replicas.iter_mut().find(|replica| {
                    replica.segment_id == task.source.segment_id
                        && replica.offset == task.source.offset
                        && replica.replica_type == task.source.replica_type
                })
            {
                source.dec_refcnt();
            }
            if replacement.replicas.is_empty() {
                changes.push((
                    key,
                    None,
                    false,
                    false,
                    clear_replication_task,
                    clear_promotion_task,
                    clear_offloading_task,
                ));
                continue;
            }
            if replacement.quota_committed {
                replacement.reserved_quota_charge_bytes = 0;
                let Ok(committed_charge) = checked_completed_memory_quota_charge(&replacement)
                else {
                    return false;
                };
                replacement.committed_quota_charge_bytes = committed_charge;
                replacement.pending_replaced_quota_charge_bytes = 0;
                if let Some(quotas) = projected_quotas.as_mut() {
                    if quotas
                        .restore_object_checked(
                            &replacement.tenant_id,
                            replacement.committed_quota_charge_bytes,
                        )
                        .is_err()
                    {
                        return false;
                    }
                }
            } else {
                replacement.committed_quota_charge_bytes = 0;
                let Ok(reserved_charge) = checked_allocating_memory_quota_charge(&replacement)
                else {
                    return false;
                };
                replacement.reserved_quota_charge_bytes = reserved_charge;
                if let Some(quotas) = projected_quotas.as_mut() {
                    if quotas
                        .restore_replacement_checked(
                            &replacement.tenant_id,
                            replacement.reserved_quota_charge_bytes,
                            replacement.pending_replaced_quota_charge_bytes,
                        )
                        .is_err()
                    {
                        return false;
                    }
                }
            }
            changes.push((
                key,
                Some(replacement),
                was_processing,
                was_client_indexed,
                clear_replication_task,
                clear_promotion_task,
                clear_offloading_task,
            ));
        }

        for (
            key,
            replacement,
            processing,
            client_indexed,
            clear_replication_task,
            clear_promotion_task,
            clear_offloading_task,
        ) in changes
        {
            let (_, mut previous) = state
                .objects
                .remove(&key)
                .expect("global oplog mutation guard preserves projected objects");
            account_cache_total_removal(&mut previous);
            for mut entry in state.client_objects.iter_mut() {
                entry.value_mut().remove(&key);
            }
            let Some(mut replacement) = replacement else {
                state.processing_keys.remove(&key);
                if clear_replication_task {
                    state.replication_tasks.remove(&key);
                }
                if clear_offloading_task {
                    clear_offloading_task_runtime(state, &key);
                }
                if clear_promotion_task {
                    clear_promotion_task_runtime(state, &key);
                }
                continue;
            };
            sync_cache_total_accounting(&mut replacement);
            if processing {
                state.processing_keys.insert(key.clone(), ());
            } else {
                state.processing_keys.remove(&key);
            }
            if client_indexed && !replacement.client_id.is_nil() {
                state
                    .client_objects
                    .entry(replacement.client_id)
                    .or_default()
                    .insert(key.clone());
            }
            state.objects.insert(key.clone(), replacement);
            if clear_replication_task {
                state.replication_tasks.remove(&key);
            }
            if clear_promotion_task {
                clear_promotion_task_runtime(state, &key);
            }
            if clear_offloading_task {
                clear_offloading_task_runtime(state, &key);
            }
        }

        state.delayed_replica_releases.retain(|_, entry| {
            entry.replicas.retain(|replica| {
                replica.segment_id != segment_id || replica.replica_type != replica_type
            });
            !entry.replicas.is_empty()
        });

        if nof {
            state.nof_segments.remove(&segment_id);
            state.nof_allocator.write().remove_segment(&segment_id);
            state.nof_heartbeat_states.remove(&segment_id);
        } else {
            state.segments.remove(&segment_id);
            state.graceful_unmounts.remove(&segment_id);
            state.allocator.write().remove_segment(&segment_id);
        }
        if let Some(projected) = projected_quotas {
            *state.tenant_quotas.write() = projected;
        }
        true
    }

    fn build_allocator_state_for_object_change(
        state: &MasterState,
        changed_key: &str,
        replacement: Option<&ObjectEntry>,
    ) -> Result<ReplayedAllocatorState, String> {
        let mut memory_replicas: HashMap<Uuid, Vec<ReplicaDescriptor>> = HashMap::new();
        let mut nof_replicas: HashMap<Uuid, Vec<ReplicaDescriptor>> = HashMap::new();
        let mut collect_object = |object: &ObjectEntry| {
            for replica in &object.replicas {
                match replica.replica_type {
                    ReplicaType::Memory => memory_replicas
                        .entry(replica.segment_id)
                        .or_default()
                        .push(replica.clone()),
                    ReplicaType::NoFSsd => nof_replicas
                        .entry(replica.segment_id)
                        .or_default()
                        .push(replica.clone()),
                    ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => {}
                }
            }
        };
        for object in state.objects.iter() {
            if object.key() != changed_key {
                collect_object(object.value());
            }
        }
        if let Some(object) = replacement {
            collect_object(object);
        }
        for entry in state.delayed_replica_releases.iter() {
            for replica in &entry.replicas {
                match replica.replica_type {
                    ReplicaType::Memory => memory_replicas
                        .entry(replica.segment_id)
                        .or_default()
                        .push(replica.clone()),
                    ReplicaType::NoFSsd => nof_replicas
                        .entry(replica.segment_id)
                        .or_default()
                        .push(replica.clone()),
                    ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => {
                        return Err(format!(
                            "delayed release {} contains non-allocator replica",
                            entry.id
                        ));
                    }
                }
            }
        }

        let memory_segment_ids = state
            .segments
            .iter()
            .map(|entry| entry.segment.id)
            .collect::<HashSet<_>>();
        let nof_segment_ids = state
            .nof_segments
            .iter()
            .map(|entry| entry.segment.id)
            .collect::<HashSet<_>>();
        if let Some(segment_id) = memory_replicas
            .keys()
            .find(|segment_id| !memory_segment_ids.contains(segment_id))
        {
            return Err(format!(
                "Memory replica references missing segment {segment_id}"
            ));
        }
        if let Some(segment_id) = nof_replicas
            .keys()
            .find(|segment_id| !nof_segment_ids.contains(segment_id))
        {
            return Err(format!(
                "NoF replica references missing segment {segment_id}"
            ));
        }

        let mut memory = SegmentAllocator::new()
            .with_strategy(state.runtime_config.allocation_strategy)
            .with_memory_allocator(state.runtime_config.memory_allocator_kind)
            .with_cxl_capacity(if state.runtime_config.enable_cxl {
                state.runtime_config.cxl_size
            } else {
                0
            });
        let mut nof = SegmentAllocator::new()
            .with_strategy(
                if state.runtime_config.allocation_strategy == AllocationStrategy::Cxl {
                    AllocationStrategy::Random
                } else {
                    state.runtime_config.allocation_strategy
                },
            )
            .with_memory_allocator(state.runtime_config.memory_allocator_kind);
        let mut memory_used = HashMap::new();
        let mut nof_used = HashMap::new();
        let cxl_segment_ids = state
            .segments
            .iter()
            .filter(|entry| entry.segment.protocol == "cxl")
            .map(|entry| entry.segment.id)
            .collect::<HashSet<_>>();
        for entry in state.segments.iter() {
            if cxl_segment_ids.contains(&entry.segment.id) {
                memory.add_segment(entry.segment.clone(), 0, entry.client_id);
                memory_used.insert(entry.segment.id, entry.used);
                continue;
            }
            let used = memory.restore_segment(
                entry.segment.clone(),
                entry.client_id,
                memory_replicas
                    .get(&entry.segment.id)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            )?;
            memory_used.insert(entry.segment.id, used);
        }
        if !cxl_segment_ids.is_empty() {
            let cxl_replicas = cxl_segment_ids
                .iter()
                .flat_map(|segment_id| {
                    memory_replicas
                        .get(segment_id)
                        .into_iter()
                        .flat_map(|replicas| replicas.iter().cloned())
                })
                .collect::<Vec<_>>();
            memory.restore_cxl_allocations(&cxl_replicas)?;
        }
        for entry in state.nof_segments.iter() {
            let segment = Segment {
                id: entry.segment.id,
                name: entry.segment.name.clone(),
                base: entry.segment.base,
                size: entry.segment.size,
                te_endpoint: entry.segment.te_endpoint.clone(),
                protocol: String::new(),
                host_id: String::new(),
            };
            let used = nof.restore_segment(
                segment,
                entry.segment.client_id,
                nof_replicas
                    .get(&entry.segment.id)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            )?;
            nof_used.insert(entry.segment.id, used);
        }
        Ok(ReplayedAllocatorState {
            memory,
            nof,
            memory_used,
            nof_used,
        })
    }

    fn install_replayed_allocator_state(
        state: &MasterState,
        allocator_state: ReplayedAllocatorState,
    ) {
        *state.allocator.write() = allocator_state.memory;
        *state.nof_allocator.write() = allocator_state.nof;
        for (segment_id, used) in allocator_state.memory_used {
            if let Some(mut segment) = state.segments.get_mut(&segment_id) {
                segment.used = used;
            }
        }
        for (segment_id, used) in allocator_state.nof_used {
            if let Some(mut segment) = state.nof_segments.get_mut(&segment_id) {
                segment.used = used;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TenantId;
    use crate::ha::OpLogRecord;
    use crate::service::{ReplicationTaskEntry, ReplicationTaskKind};
    use dashmap::DashMap;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    const CPP_STRUCT_PACK_EMPTY_REPLICA_PAYLOAD: &[u8] = &[
        0xcd, 0xe7, 0xf3, 0x0f, 0x04, 0xfd, 0xfd, 0x04, 0x04, 0x89, 0x89, 0xff, 0x04, 0x84, 0xfd,
        0x04, 0x86, 0xfd, 0xfd, 0x04, 0x04, 0x80, 0x0c, 0x80, 0x0c, 0xff, 0xff, 0xfd, 0xfd, 0x04,
        0x04, 0x80, 0x0c, 0x80, 0x0c, 0xff, 0xff, 0xfd, 0x80, 0x0c, 0x04, 0xff, 0xfd, 0xfd, 0x04,
        0x04, 0x89, 0x89, 0xff, 0x04, 0x80, 0x0c, 0xff, 0xff, 0x01, 0xff, 0xff, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    fn cpp_wire_record(sequence_id: u64, op_type: u8) -> OpLogRecord {
        use crate::oplog::test_support::{
            CppWireTestEntry, TEST_CPP_OP_PUT_END, compute_cpp_checksum_for_test,
            compute_cpp_prefix_hash_for_test, deserialize_etcd_value_for_test,
        };
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

        let is_put_end = op_type == TEST_CPP_OP_PUT_END;
        let wire = CppWireTestEntry {
            sequence_id,
            timestamp_ms: 1,
            op_type,
            object_key: "key1".to_string(),
            payload: is_put_end
                .then(|| BASE64_STANDARD.encode(CPP_STRUCT_PACK_EMPTY_REPLICA_PAYLOAD))
                .unwrap_or_default(),
            checksum: is_put_end
                .then(|| compute_cpp_checksum_for_test(CPP_STRUCT_PACK_EMPTY_REPLICA_PAYLOAD))
                .unwrap_or_default(),
            prefix_hash: compute_cpp_prefix_hash_for_test("key1"),
        };
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap()
    }

    fn cpp_put_end_wire_record_with_payload(sequence_id: u64, payload_bytes: &[u8]) -> OpLogRecord {
        use crate::oplog::test_support::{
            CppWireTestEntry, TEST_CPP_OP_PUT_END, compute_cpp_checksum_for_test,
            compute_cpp_prefix_hash_for_test, deserialize_etcd_value_for_test,
        };
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

        let wire = CppWireTestEntry {
            sequence_id,
            timestamp_ms: 1,
            op_type: TEST_CPP_OP_PUT_END,
            object_key: "key1".to_string(),
            payload: BASE64_STANDARD.encode(payload_bytes),
            checksum: compute_cpp_checksum_for_test(payload_bytes),
            prefix_hash: compute_cpp_prefix_hash_for_test("key1"),
        };
        deserialize_etcd_value_for_test(&serde_json::to_string(&wire).unwrap()).unwrap()
    }

    fn make_state_with_tenant_quota(enable_tenant_quota: bool) -> Arc<MasterState> {
        let mut runtime_config = crate::service::state::MasterRuntimeConfig::default();
        runtime_config.enable_tenant_quota = enable_tenant_quota;
        Arc::new(MasterState {
            clients: DashMap::new(),
            ok_clients: DashMap::new(),
            objects: DashMap::new(),
            key_mutations: crate::service::state::KeyMutationCoordinator::default(),
            processing_keys: DashMap::new(),
            client_objects: DashMap::new(),
            segments: DashMap::new(),
            delayed_replica_releases: DashMap::new(),
            graceful_unmounts: DashMap::new(),
            nof_segments: DashMap::new(),
            local_disk_segments: DashMap::new(),
            local_disk_client_sessions: DashMap::new(),
            tasks: DashMap::new(),
            replication_tasks: DashMap::new(),
            offloading_tasks: DashMap::new(),
            promotion_tasks: DashMap::new(),
            promotion_sketch: RwLock::new(crate::count_min_sketch::CountMinSketch::new()),
            promotion_candidates: DashMap::new(),
            drain_jobs: DashMap::new(),
            allocator: RwLock::new(crate::allocator::SegmentAllocator::new()),
            nof_allocator: RwLock::new(crate::allocator::SegmentAllocator::new()),
            nof_eviction_requested: AtomicBool::new(false),
            storage_backend: RwLock::new(None),
            oplog_manager: Arc::new(crate::oplog::OpLogManager::new(None, 0)),
            promotion_in_flight: AtomicUsize::new(0),
            promotion_candidate_count: AtomicUsize::new(0),
            promotion_retry_cursor: AtomicUsize::new(0),
            view_version: std::sync::atomic::AtomicI64::new(0),
            leadership_view_version: std::sync::atomic::AtomicU64::new(0),
            runtime_config,
            service_available: AtomicBool::new(true),
            foreground_request_gate: Arc::new(crate::service::state::ForegroundRequestGate::new()),
            background_mutation_gate: RwLock::new(()),
            service_fenced: AtomicBool::new(false),
            tenant_quotas: RwLock::new(crate::tenant_quota::TenantQuotaTable::new(0)),
            pending_remote_pulls: DashMap::new(),
            nof_heartbeat_states: DashMap::new(),
            kv_event_publisher: Arc::new(
                crate::kv_event::KvEventPublisher::new(Default::default()),
            ),
        })
    }

    fn make_state() -> Arc<MasterState> {
        make_state_with_tenant_quota(false)
    }

    #[test]
    fn cpp_parity_ha_oplog_oplog_applier_test_cpp_oplogappliertest_testapplyputend() {
        use crate::oplog::test_support::TEST_CPP_OP_PUT_END;

        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        assert_eq!(
            applier.apply_op_log_entries(&[cpp_wire_record(1, TEST_CPP_OP_PUT_END)]),
            1
        );
        assert_eq!(applier.get_expected_sequence_id(), 2);
        assert_eq!(state.objects.len(), 1);
        let object = state.objects.get("default\0key1").expect("key1 created");
        assert_eq!(object.user_key, "key1");
        assert_eq!(object.size, 1024);
        assert_eq!(object.client_id, Uuid::from_u64_pair(1, 2));
        assert!(object.replicas.is_empty());
    }

    #[test]
    fn cpp_parity_ha_oplog_oplog_applier_test_cpp_oplogappliertest_testapplyputrevoke() {
        use crate::oplog::test_support::{TEST_CPP_OP_PUT_END, TEST_CPP_OP_PUT_REVOKE};

        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        assert_eq!(
            applier.apply_op_log_entries(&[cpp_wire_record(1, TEST_CPP_OP_PUT_END)]),
            1
        );
        assert!(state.objects.contains_key("default\0key1"));
        assert_eq!(
            applier.apply_op_log_entries(&[cpp_wire_record(2, TEST_CPP_OP_PUT_REVOKE)]),
            1
        );
        assert_eq!(applier.get_expected_sequence_id(), 3);
        assert!(!state.objects.contains_key("default\0key1"));
    }

    #[test]
    fn cpp_parity_ha_oplog_oplog_applier_test_cpp_oplogappliertest_testapplyremove_e681ca3e() {
        use crate::oplog::test_support::{TEST_CPP_OP_PUT_END, TEST_CPP_OP_REMOVE};

        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        assert_eq!(
            applier.apply_op_log_entries(&[cpp_wire_record(1, TEST_CPP_OP_PUT_END)]),
            1
        );
        assert!(state.objects.contains_key("default\0key1"));
        assert_eq!(
            applier.apply_op_log_entries(&[cpp_wire_record(2, TEST_CPP_OP_REMOVE)]),
            1
        );
        assert_eq!(applier.get_expected_sequence_id(), 3);
        assert!(!state.objects.contains_key("default\0key1"));
    }

    #[test]
    fn cpp_struct_pack_empty_replica_bypass_requires_verified_wire_provenance() {
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

        let forged_payloads = [
            serde_json::json!({
                "op": "put_end",
                "key": "key1",
                "size": 1024,
                "client_id": Uuid::from_u64_pair(1, 2).to_string(),
                "tenant_id": "default",
                "user_key": "key1",
                "replicas": [],
                "legacy_cpp_struct_pack_empty_replicas": true
            }),
            serde_json::json!({
                "op": "put_end",
                "key": "key1",
                "size": 1024,
                "client_id": Uuid::nil().to_string(),
                "tenant_id": "default",
                "user_key": "key1",
                "replicas": [],
                "legacy_cpp_struct_pack_payload_base64":
                    BASE64_STANDARD.encode(CPP_STRUCT_PACK_EMPTY_REPLICA_PAYLOAD)
            }),
        ];
        for payload in forged_payloads {
            let state = make_state();
            let applier = OpLogApplier::new(state.clone());
            assert_eq!(
                applier.apply_op_log_entries(&[OpLogRecord {
                    seq: 1,
                    producer_view_version: 1,
                    payload: payload.to_string(),
                }]),
                0
            );
            assert_eq!(applier.get_expected_sequence_id(), 1);
            assert!(state.objects.is_empty());
        }

        let mut mutated_payloads = Vec::new();
        let mut bad_header = CPP_STRUCT_PACK_EMPTY_REPLICA_PAYLOAD.to_vec();
        bad_header[0] ^= 1;
        mutated_payloads.push(bad_header);
        mutated_payloads.push(CPP_STRUCT_PACK_EMPTY_REPLICA_PAYLOAD[..82].to_vec());
        let mut nonempty_vector = CPP_STRUCT_PACK_EMPTY_REPLICA_PAYLOAD.to_vec();
        *nonempty_vector.last_mut().unwrap() = 1;
        mutated_payloads.push(nonempty_vector);
        for payload in mutated_payloads {
            let state = make_state();
            let applier = OpLogApplier::new(state.clone());
            assert_eq!(
                applier.apply_op_log_entries(&[cpp_put_end_wire_record_with_payload(1, &payload)]),
                0
            );
            assert_eq!(applier.get_expected_sequence_id(), 1);
            assert!(state.objects.is_empty());
        }
    }

    #[test]
    fn test_replay_v1_local_disk_without_generation_stays_offline() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let holder_client_id = Uuid::new_v4();
        let storage_id = Uuid::new_v4();
        let replica = ReplicaDescriptor {
            segment_id: Uuid::nil(),
            segment_name: "local://legacy-disk".to_string(),
            offset: 0,
            size: 128,
            status: mooncake_store_core::ReplicaStatus::Complete,
            replica_type: ReplicaType::LocalDisk,
            holder_client_id: Some(holder_client_id),
            local_disk_storage_id: Some(storage_id),
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };
        let mut replica = serde_json::to_value(replica).unwrap();
        replica
            .as_object_mut()
            .unwrap()
            .remove("local_disk_generation_id");
        let payload = serde_json::json!({
            "op": "put_end",
            "key": "default\0legacy-local-disk",
            "size": 128,
            "client_id": holder_client_id.to_string(),
            "tenant_id": "default",
            "group_id": "",
            "user_key": "legacy-local-disk",
            "replicas": [replica],
        });
        let payload = serde_json::to_string(&payload).unwrap();

        let applied = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload,
        }]);

        assert_eq!(applied, 1);
        assert_eq!(applier.get_expected_sequence_id(), 2);
        let object = state.objects.get("default\0legacy-local-disk").unwrap();
        assert_eq!(object.replicas.len(), 1);
        let replica = &object.replicas[0];
        assert_eq!(replica.local_disk_storage_id, Some(storage_id));
        assert_eq!(replica.local_disk_generation_id, None);
        assert_eq!(replica.holder_client_id, None);
        assert!(!replica.handle_valid);
        assert!(replica.segment_name.is_empty());
        assert!(!crate::service::helpers::replica_is_routable(
            &state, replica
        ));
    }

    fn insert_memory_segment(state: &MasterState, segment_id: Uuid, client_id: Uuid, size: u64) {
        let segment = Segment {
            id: segment_id,
            name: "seg-a".to_string(),
            base: 4096,
            size,
            te_endpoint: String::new(),
            protocol: "rdma".to_string(),
            host_id: String::new(),
        };
        state.segments.insert(
            segment_id,
            crate::service::SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        state.allocator.write().add_segment(segment, 0, client_id);
    }

    fn insert_default_tenant_object(state: &MasterState, user_key: &str) {
        let scoped_key = TenantId::default().make_scoped_key(user_key);
        state.objects.insert(
            scoped_key,
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
                tenant_id: TenantId::default(),
                group_id: String::new(),
                quota_committed: false,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: user_key.to_string(),
            },
        );
    }

    fn durable_disk_object(tenant_id: TenantId, user_key: &str) -> ObjectEntry {
        ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: Uuid::nil(),
                segment_name: "global-disk".into(),
                offset: 0,
                size: 128,
                status: mooncake_store_core::ReplicaStatus::Complete,
                replica_type: ReplicaType::Disk,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 0,
                protocol: String::new(),
            }],
            size: 128,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: mooncake_store_core::ObjectDataType::General,
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: user_key.into(),
        }
    }

    #[test]
    fn lease_refresh_batch_replays_group_deadlines_atomically() {
        let tenant_id = TenantId::new("lease-tenant".into()).unwrap();
        let first_key = tenant_id.make_scoped_key("first");
        let second_key = tenant_id.make_scoped_key("second");
        let old_lease = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000);
        let old_soft = SystemTime::UNIX_EPOCH + Duration::from_millis(1_500);
        let new_last_access = SystemTime::UNIX_EPOCH + Duration::from_millis(4_000);
        let new_lease = SystemTime::UNIX_EPOCH + Duration::from_millis(5_000);
        let new_soft = SystemTime::UNIX_EPOCH + Duration::from_millis(6_000);
        let state = make_state();
        for (key, user_key) in [(&first_key, "first"), (&second_key, "second")] {
            let mut object = durable_disk_object(tenant_id.clone(), user_key);
            object.group_id = "lease-group".into();
            object.last_access = old_lease;
            object.lease_timeout = Some(old_lease);
            object.soft_pin_timeout = Some(old_soft);
            state.objects.insert(key.clone(), object);
        }

        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_lease_refresh_batch_durable(&[
                crate::oplog::LeaseRefreshEntry {
                    key: first_key.clone(),
                    tenant_id: tenant_id.clone(),
                    group_id: "lease-group".into(),
                    last_access: new_last_access,
                    lease_timeout: new_lease,
                    soft_pin_timeout: Some(new_soft),
                },
                crate::oplog::LeaseRefreshEntry {
                    key: second_key.clone(),
                    tenant_id,
                    group_id: "lease-group".into(),
                    last_access: new_last_access,
                    lease_timeout: new_lease,
                    soft_pin_timeout: Some(new_soft),
                },
            ])
            .unwrap();
        let records = manager.read_since(1, 1).unwrap();
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(applier.apply_op_log_entries(&records), 1);
        assert_eq!(applier.get_expected_sequence_id(), 2);
        for key in [&first_key, &second_key] {
            let object = state.objects.get(key).unwrap();
            assert_eq!(object.last_access, new_last_access);
            assert_eq!(object.lease_timeout, Some(new_lease));
            assert_eq!(object.soft_pin_timeout, Some(new_soft));
        }
    }

    #[test]
    fn lease_refresh_batch_identity_mismatch_changes_nothing() {
        let tenant_id = TenantId::new("lease-tenant".into()).unwrap();
        let first_key = tenant_id.make_scoped_key("first");
        let second_key = tenant_id.make_scoped_key("second");
        let old_lease = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000);
        let old_soft = SystemTime::UNIX_EPOCH + Duration::from_millis(1_500);
        let state = make_state();
        for (key, user_key) in [(&first_key, "first"), (&second_key, "second")] {
            let mut object = durable_disk_object(tenant_id.clone(), user_key);
            object.group_id = "lease-group".into();
            object.lease_timeout = Some(old_lease);
            object.soft_pin_timeout = Some(old_soft);
            state.objects.insert(key.clone(), object);
        }
        let payload = serde_json::json!({
            "op": "lease_refresh_batch",
            "schema_version": 1,
            "entries": [
                {
                    "key": first_key,
                    "tenant_id": tenant_id.as_str(),
                    "group_id": "lease-group",
                    "lease_timeout_ms": 5_000,
                    "soft_pin_timeout_ms": 6_000,
                },
                {
                    "key": second_key,
                    "tenant_id": tenant_id.as_str(),
                    "group_id": "different-group",
                    "lease_timeout_ms": 5_000,
                    "soft_pin_timeout_ms": 6_000,
                },
            ],
        });
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::to_string(&payload).unwrap(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        for key in [
            tenant_id.make_scoped_key("first"),
            tenant_id.make_scoped_key("second"),
        ] {
            let object = state.objects.get(&key).unwrap();
            assert_eq!(object.lease_timeout, Some(old_lease));
            assert_eq!(object.soft_pin_timeout, Some(old_soft));
        }
    }

    #[test]
    fn replication_start_rejects_overflowing_target_reservation_on_both_sides() {
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("overflowing-replication");
        let owner_id = Uuid::new_v4();
        let object_size = u64::MAX / 2 + 1;
        let source_segment_id = Uuid::new_v4();
        let first_target_segment_id = Uuid::new_v4();
        let second_target_segment_id = Uuid::new_v4();
        let replica = |segment_id, status| ReplicaDescriptor {
            segment_id,
            segment_name: segment_id.to_string(),
            offset: 0,
            size: object_size,
            status,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(owner_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: "rdma".into(),
        };
        let source = replica(source_segment_id, ReplicaStatus::Complete);
        let first_target = replica(first_target_segment_id, ReplicaStatus::Allocating);
        let second_target = replica(second_target_segment_id, ReplicaStatus::Allocating);
        let object = ObjectEntry {
            replicas: vec![source.clone(), first_target.clone(), second_target.clone()],
            size: object_size,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: mooncake_store_core::ObjectDataType::General,
            client_id: owner_id,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: object_size,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "overflowing-replication".into(),
        };
        let task = ReplicationTaskEntry {
            client_id: owner_id,
            start_time: Instant::now(),
            kind: ReplicationTaskKind::Copy,
            source,
            targets: vec![first_target, second_target],
            existing_move_target: None,
            // This is what the old saturating target sum produced.
            reserved_quota_charge_bytes: u64::MAX,
        };

        let producer =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        assert!(
            producer
                .record_replication_start_durable(&key, &object, &task)
                .is_err()
        );
        assert_eq!(producer.latest_sequence(), 0);

        // Build a record through the independently validated object-image
        // encoder, then attach the malformed task to exercise standby input.
        let mut encodable_object = object.clone();
        encodable_object.replicas.truncate(1);
        producer
            .record_object_image_durable(&key, &encodable_object)
            .unwrap();
        let object_record = producer.read_since(1, 1).unwrap();
        let mut object_payload = decode_record_payload_value(&object_record[0].payload).unwrap();
        object_payload["object"]["replicas"] = serde_json::to_value(&object.replicas).unwrap();
        let payload = serde_json::json!({
            "op": "replication_start",
            "schema_version": 1,
            "key": object_payload["key"].clone(),
            "tenant_id": object_payload["tenant_id"].clone(),
            "user_key": object_payload["user_key"].clone(),
            "object": object_payload["object"].clone(),
            "task": ReplicationTaskSnapshotEntry::capture(&key, &task, Instant::now()),
        });

        let state = make_state_with_tenant_quota(false);
        for segment_id in [
            source_segment_id,
            first_target_segment_id,
            second_target_segment_id,
        ] {
            insert_memory_segment(&state, segment_id, owner_id, object_size);
        }
        let applier = OpLogApplier::new(state.clone());
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::to_string(&payload).unwrap(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(!state.objects.contains_key(&key));
        assert!(!state.replication_tasks.contains_key(&key));
        for segment_id in [
            source_segment_id,
            first_target_segment_id,
            second_target_segment_id,
        ] {
            assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));
        }
    }

    #[test]
    fn replication_start_replays_object_task_allocator_and_source_pin_atomically() {
        let state = make_state_with_tenant_quota(true);
        let applier = OpLogApplier::new(state.clone());
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("replication-key");
        let owner_id = Uuid::new_v4();
        let source_segment_id = Uuid::new_v4();
        let target_segment_id = Uuid::new_v4();
        insert_memory_segment(&state, source_segment_id, owner_id, 4096);
        insert_memory_segment(&state, target_segment_id, owner_id, 4096);

        let source = ReplicaDescriptor {
            segment_id: source_segment_id,
            segment_name: "seg-a".into(),
            offset: 0,
            size: 256,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(owner_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 4096,
            protocol: "rdma".into(),
        };
        let target = ReplicaDescriptor {
            segment_id: target_segment_id,
            segment_name: "seg-a".into(),
            offset: 0,
            size: 256,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(owner_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 8192,
            protocol: "rdma".into(),
        };
        let object_client_id = Uuid::new_v4();
        let object = ObjectEntry {
            replicas: vec![source.clone(), target.clone()],
            size: 256,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: mooncake_store_core::ObjectDataType::General,
            client_id: object_client_id,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 256,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "replication-key".into(),
        };
        let task = ReplicationTaskEntry {
            client_id: owner_id,
            start_time: Instant::now(),
            kind: ReplicationTaskKind::Copy,
            source,
            targets: vec![target],
            existing_move_target: None,
            reserved_quota_charge_bytes: 256,
        };
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_replication_start_durable(&key, &object, &task)
            .unwrap();
        let records = manager.read_since(1, 1).unwrap();

        assert_eq!(applier.apply_op_log_entries(&records), 1);
        let replayed = state.objects.get(&key).unwrap();
        let replayed_source = replayed
            .replicas
            .iter()
            .find(|replica| replica.segment_id == source_segment_id)
            .unwrap();
        assert!(replayed_source.is_busy());
        assert!(!replayed_source.handle_valid);
        assert!(
            state
                .client_objects
                .get(&object_client_id)
                .unwrap()
                .contains(&key)
        );
        drop(replayed);
        assert!(state.replication_tasks.contains_key(&key));
        assert!(!state.processing_keys.contains_key(&key));
        assert_eq!(
            state.allocator.read().used_bytes(&source_segment_id),
            Some(256)
        );
        assert_eq!(
            state.allocator.read().used_bytes(&target_segment_id),
            Some(256)
        );
        let quota = state.tenant_quotas.read().get_snapshot(&tenant_id).unwrap();
        assert_eq!(quota.used_bytes, 256);
        assert_eq!(quota.reserved_bytes, 256);
        assert_eq!(quota.committed_count, 1);
        assert_eq!(quota.metadata_object_count, 1);

        let mut completed = object;
        completed.replicas[1].status = ReplicaStatus::Complete;
        completed.committed_quota_charge_bytes = 512;
        manager
            .record_object_image_durable(&key, &completed)
            .unwrap();
        let completion = manager.read_since(2, 1).unwrap();
        assert_eq!(applier.apply_op_log_entries(&completion), 1);
        assert!(!state.replication_tasks.contains_key(&key));
        let quota = state.tenant_quotas.read().get_snapshot(&tenant_id).unwrap();
        assert_eq!(quota.used_bytes, 512);
        assert_eq!(quota.reserved_bytes, 0);
        assert_eq!(quota.committed_count, 1);
        assert_eq!(quota.metadata_object_count, 1);
    }

    #[test]
    fn existing_target_move_replays_exact_target_without_reservation() {
        let state = make_state_with_tenant_quota(true);
        let applier = OpLogApplier::new(state.clone());
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("existing-target-move");
        let owner_id = Uuid::new_v4();
        let source_segment_id = Uuid::new_v4();
        let target_segment_id = Uuid::new_v4();
        insert_memory_segment(&state, source_segment_id, owner_id, 4096);
        insert_memory_segment(&state, target_segment_id, owner_id, 4096);
        let replica = |segment_id, name: &str| ReplicaDescriptor {
            segment_id,
            segment_name: name.into(),
            offset: 0,
            size: 256,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(owner_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 4096,
            protocol: "rdma".into(),
        };
        let source = replica(source_segment_id, "seg-a");
        let target = replica(target_segment_id, "seg-a");
        let object = ObjectEntry {
            replicas: vec![source.clone(), target.clone()],
            size: 256,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: mooncake_store_core::ObjectDataType::General,
            client_id: owner_id,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 512,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "existing-target-move".into(),
        };
        let task = ReplicationTaskEntry {
            client_id: owner_id,
            start_time: Instant::now(),
            kind: ReplicationTaskKind::Move,
            source,
            targets: Vec::new(),
            existing_move_target: Some(target),
            reserved_quota_charge_bytes: 0,
        };
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_replication_start_durable(&key, &object, &task)
            .unwrap();
        let records = manager.read_since(1, 1).unwrap();

        assert_eq!(applier.apply_op_log_entries(&records), 1);
        let replayed_task = state.replication_tasks.get(&key).unwrap();
        assert!(replayed_task.targets.is_empty());
        assert_eq!(
            replayed_task
                .existing_move_target
                .as_ref()
                .unwrap()
                .segment_id,
            target_segment_id
        );
        assert_eq!(replayed_task.reserved_quota_charge_bytes, 0);
        drop(replayed_task);
        let replayed = state.objects.get(&key).unwrap();
        assert!(
            replayed
                .replicas
                .iter()
                .find(|replica| replica.segment_id == source_segment_id)
                .unwrap()
                .is_busy()
        );
        drop(replayed);
        let quota = state.tenant_quotas.read().get_snapshot(&tenant_id).unwrap();
        assert_eq!(quota.used_bytes, 512);
        assert_eq!(quota.reserved_bytes, 0);
    }

    #[test]
    fn delayed_release_batch_replays_reservation_and_tombstone_atomically() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("delayed-release-key");
        let owner_id = Uuid::new_v4();
        let segment_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, owner_id, 4096);

        let delayed_replica = ReplicaDescriptor {
            segment_id,
            segment_name: "seg-a".into(),
            offset: 0,
            size: 256,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(owner_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 4096,
            protocol: "rdma".into(),
        };
        let replacement_replica = ReplicaDescriptor {
            offset: 512,
            ..delayed_replica.clone()
        };
        let object = ObjectEntry {
            replicas: vec![replacement_replica],
            size: 256,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: mooncake_store_core::ObjectDataType::General,
            client_id: owner_id,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id,
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 256,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "delayed-release-key".into(),
        };
        let release = crate::service::state::DelayedReplicaReleaseEntry {
            id: Uuid::new_v4(),
            scoped_key: key.clone(),
            deadline_epoch_ms: 1,
            replicas: vec![delayed_replica],
        };
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_object_delayed_release_batch_durable(
                &key,
                Some(&object),
                std::slice::from_ref(&release),
                &[],
            )
            .unwrap();
        let scheduled = manager.read_since(1, 1).unwrap();

        let mut future_payload =
            crate::oplog::decode_record_payload_value(&scheduled[0].payload).unwrap();
        future_payload["schema_version"] = serde_json::json!(2);
        let future_payload = future_payload.to_string();
        assert!(!OpLogApplier::apply_one(&state, &future_payload));
        assert!(state.objects.get(&key).is_none());
        assert!(state.delayed_replica_releases.is_empty());
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));

        assert_eq!(applier.apply_op_log_entries(&scheduled), 1);
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(512));
        let replayed_release = state.delayed_replica_releases.get(&release.id).unwrap();
        assert_eq!(replayed_release.replicas.len(), 1);
        assert!(!replayed_release.replicas[0].handle_valid);
        assert_eq!(replayed_release.replicas[0].base_addr, 0);
        assert!(replayed_release.replicas[0].protocol.is_empty());
        drop(replayed_release);
        assert_eq!(state.objects.get(&key).unwrap().replicas[0].offset, 512);

        manager
            .record_object_delayed_release_batch_durable(&key, Some(&object), &[], &[release.id])
            .unwrap();
        let tombstone = manager.read_since(2, 1).unwrap();
        assert_eq!(applier.apply_op_log_entries(&tombstone), 1);
        assert!(!state.delayed_replica_releases.contains_key(&release.id));
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(256));
    }

    #[test]
    fn delayed_release_batch_keeps_committed_in_place_upsert_unreadable() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("delayed-release-in-place-upsert");
        let owner_id = Uuid::new_v4();
        let segment_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, owner_id, 4096);

        let object = ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id,
                segment_name: "seg-a".into(),
                offset: 0,
                size: 256,
                status: ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
                holder_client_id: Some(owner_id),
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 4096,
                protocol: "rdma".into(),
            }],
            size: 256,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: mooncake_store_core::ObjectDataType::General,
            client_id: owner_id,
            put_start_time: Some(SystemTime::now()),
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id,
            group_id: String::new(),
            // Same-size Upsert reuses the previously charged allocation.
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 256,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "delayed-release-in-place-upsert".into(),
        };
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_object_delayed_release_batch_durable(&key, Some(&object), &[], &[])
            .unwrap();
        let entries = manager.read_since(1, 1).unwrap();

        assert_eq!(applier.apply_op_log_entries(&entries), 1);
        assert!(state.processing_keys.contains_key(&key));
        assert!(
            !state
                .client_objects
                .get(&owner_id)
                .is_some_and(|keys| keys.contains(&key))
        );
        let replayed = state.objects.get(&key).unwrap();
        assert!(replayed.quota_committed);
        assert_eq!(replayed.replicas[0].status, ReplicaStatus::Allocating);
    }

    #[test]
    fn task_state_batch_replays_claim_completion_and_removal() {
        use mooncake_store_core::{TaskInfo, TaskType};

        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("task-key");
        state.objects.insert(
            key.clone(),
            durable_disk_object(tenant_id.clone(), "task-key"),
        );
        let task_id = Uuid::new_v4();
        let assigned_client = Uuid::new_v4();
        let created_at = chrono::Utc::now();
        let mut task = crate::service::TaskEntry {
            info: TaskInfo {
                id: task_id,
                task_type: TaskType::ReplicaMove,
                status: TaskStatus::Pending,
                created_at,
                last_updated_at: created_at,
                assigned_client: Some(assigned_client),
                message: "queued".into(),
            },
            key: key.clone(),
            payload: serde_json::json!({
                "tenant_id": tenant_id.as_str(),
                "key": "task-key",
                "source": "source",
                "target": "target",
            })
            .to_string(),
            max_retry_attempts: 3,
        };
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_task_state_batch_durable(&[task.clone()], &[])
            .unwrap();
        let created = manager.read_since(1, 1).unwrap();
        assert_eq!(applier.apply_op_log_entries(&created), 1);
        assert_eq!(
            state.tasks.get(&task_id).unwrap().info.status,
            TaskStatus::Pending
        );

        task.info.status = TaskStatus::Processing;
        task.info.last_updated_at += chrono::Duration::milliseconds(1);
        manager
            .record_task_state_batch_durable(&[task.clone()], &[])
            .unwrap();
        let claimed = manager.read_since(2, 1).unwrap();
        assert_eq!(applier.apply_op_log_entries(&claimed), 1);
        assert_eq!(
            state.tasks.get(&task_id).unwrap().info.status,
            TaskStatus::Processing
        );

        task.info.status = TaskStatus::Success;
        task.info.message = "done".into();
        task.info.last_updated_at += chrono::Duration::milliseconds(1);
        manager
            .record_task_state_batch_durable(&[task], &[])
            .unwrap();
        let completed = manager.read_since(3, 1).unwrap();
        assert_eq!(applier.apply_op_log_entries(&completed), 1);
        assert_eq!(
            state.tasks.get(&task_id).unwrap().info.status,
            TaskStatus::Success
        );

        manager
            .record_task_state_batch_durable(&[], &[task_id])
            .unwrap();
        let removed = manager.read_since(4, 1).unwrap();
        assert_eq!(applier.apply_op_log_entries(&removed), 1);
        assert!(!state.tasks.contains_key(&task_id));
    }

    #[test]
    fn segment_status_batch_replays_drain_terminal_state_by_uuid() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, Uuid::new_v4(), 4096);
        let draining = serde_json::json!({
            "op": "segment_status_batch",
            "schema_version": 1,
            "entries": [{
                "segment_id": segment_id.to_string(),
                "nof": false,
                "status": crate::proto::SegmentStatus::Draining as i32,
            }],
        });
        let unavailable = serde_json::json!({
            "op": "segment_status_batch",
            "schema_version": 1,
            "entries": [{
                "segment_id": segment_id.to_string(),
                "nof": false,
                "status": crate::proto::SegmentStatus::Unavailable as i32,
            }],
        });

        assert_eq!(
            applier.apply_op_log_entries(&[
                OpLogRecord {
                    seq: 1,
                    producer_view_version: 1,
                    payload: serde_json::to_string(&draining).unwrap(),
                },
                OpLogRecord {
                    seq: 2,
                    producer_view_version: 1,
                    payload: serde_json::to_string(&unavailable).unwrap(),
                },
            ]),
            2
        );
        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            crate::proto::SegmentStatus::Unavailable
        );
    }

    #[test]
    fn segment_status_batch_rejects_resurrection_from_unavailable() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, Uuid::new_v4(), 4096);
        state.segments.get_mut(&segment_id).unwrap().status =
            crate::proto::SegmentStatus::Unavailable;
        let payload = serde_json::json!({
            "op": "segment_status_batch",
            "schema_version": 1,
            "entries": [{
                "segment_id": segment_id.to_string(),
                "nof": false,
                "status": crate::proto::SegmentStatus::Active as i32,
            }],
        });

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::to_string(&payload).unwrap(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            crate::proto::SegmentStatus::Unavailable
        );
    }

    #[test]
    fn segment_status_batch_rejection_is_mutation_atomic() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, Uuid::new_v4(), 4096);
        let payload = serde_json::json!({
            "op": "segment_status_batch",
            "schema_version": 1,
            "entries": [
                {
                    "segment_id": segment_id.to_string(),
                    "nof": false,
                    "status": crate::proto::SegmentStatus::Draining as i32,
                },
                {
                    "segment_id": segment_id.to_string(),
                    "nof": false,
                    "status": crate::proto::SegmentStatus::Unavailable as i32,
                },
            ],
        });

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::to_string(&payload).unwrap(),
            }]),
            0
        );
        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            crate::proto::SegmentStatus::Active
        );
    }

    #[test]
    fn v3_object_image_preserves_pin_type_deadlines_and_quota_state() {
        let tenant_id = TenantId::new("tenant-v3".to_string()).unwrap();
        let scoped_key = tenant_id.make_scoped_key("object-v3");
        let client_id = Uuid::new_v4();
        let lease_timeout = SystemTime::UNIX_EPOCH + Duration::from_millis(2_000);
        let soft_pin_timeout = SystemTime::UNIX_EPOCH + Duration::from_millis(3_000);
        let mut object = durable_disk_object(tenant_id, "object-v3");
        object.hard_pinned = true;
        object.data_type = mooncake_store_core::ObjectDataType::Tensor;
        object.client_id = client_id;
        object.lease_timeout = Some(lease_timeout);
        object.soft_pin_timeout = Some(soft_pin_timeout);
        object.group_id = "group-v3".into();
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_object_image_durable(&scoped_key, &object)
            .unwrap();
        let records = manager.read_since(1, 1).unwrap();
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(applier.apply_op_log_entries(&records), 1);
        let restored = state.objects.get(&scoped_key).unwrap();
        assert!(restored.hard_pinned);
        assert_eq!(
            restored.data_type,
            mooncake_store_core::ObjectDataType::Tensor
        );
        assert_eq!(restored.client_id, client_id);
        assert_eq!(restored.group_id, "group-v3");
        assert!(restored.put_start_time.is_none());
        assert_eq!(restored.lease_timeout, Some(lease_timeout));
        assert_eq!(restored.soft_pin_timeout, Some(soft_pin_timeout));
        assert!(restored.quota_committed);
    }

    #[test]
    fn v3_writer_rejects_identity_and_quota_drift_before_append() {
        let tenant_id = TenantId::default();
        let mut object = durable_disk_object(tenant_id.clone(), "stable-key");
        let scoped_key = tenant_id.make_scoped_key("stable-key");
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);

        assert!(
            manager
                .record_object_image_durable(&tenant_id.make_scoped_key("different-key"), &object,)
                .is_err()
        );
        assert_eq!(manager.latest_sequence(), 0);

        object.committed_quota_charge_bytes = 1;
        assert!(
            manager
                .record_object_image_durable(&scoped_key, &object)
                .is_err()
        );
        assert_eq!(manager.latest_sequence(), 0);
    }

    #[test]
    fn v3_replay_restores_processing_state_for_inflight_image() {
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("inflight-v3");
        let mut object = durable_disk_object(tenant_id, "inflight-v3");
        object.replicas[0].status = mooncake_store_core::ReplicaStatus::Allocating;
        object.quota_committed = false;
        object.put_start_time = Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1_000));
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_object_image_durable(&scoped_key, &object)
            .unwrap();
        let records = manager.read_since(1, 1).unwrap();
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(applier.apply_op_log_entries(&records), 1);
        assert!(state.processing_keys.contains_key(&scoped_key));
        assert!(
            !state
                .client_objects
                .get(&object.client_id)
                .is_some_and(|keys| keys.contains(&scoped_key))
        );
    }

    #[test]
    fn v3_replay_preserves_committed_in_place_upsert_generation() {
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("in-place-upsert-v3");
        let client_id = Uuid::new_v4();
        let segment_id = Uuid::new_v4();
        let put_start_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000);
        let mut object = durable_disk_object(tenant_id, "in-place-upsert-v3");
        object.client_id = client_id;
        object.put_start_time = Some(put_start_time);
        object.replicas[0] = ReplicaDescriptor {
            segment_id,
            segment_name: "seg-a".into(),
            offset: 0,
            size: object.size,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 4096,
            protocol: "rdma".into(),
        };
        object.committed_quota_charge_bytes = object.size;
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_object_image_durable(&scoped_key, &object)
            .expect("committed in-place Upsert image must be durable");
        let records = manager.read_since(1, 1).unwrap();
        let state = make_state();
        insert_memory_segment(&state, segment_id, client_id, 4096);
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(applier.apply_op_log_entries(&records), 1);
        assert_eq!(applier.get_expected_sequence_id(), 2);
        let restored = state.objects.get(&scoped_key).unwrap();
        assert!(restored.quota_committed);
        assert_eq!(restored.committed_quota_charge_bytes, restored.size);
        assert_eq!(restored.put_start_time, Some(put_start_time));
        assert_eq!(restored.replicas[0].status, ReplicaStatus::Allocating);
        assert!(state.processing_keys.contains_key(&scoped_key));
        assert!(
            !state
                .client_objects
                .get(&client_id)
                .is_some_and(|keys| keys.contains(&scoped_key))
        );
        assert_eq!(
            state.allocator.read().used_bytes(&segment_id),
            Some(restored.size)
        );
    }

    #[test]
    fn remove_quota_mismatch_does_not_mutate_or_advance_sequence() {
        let state = make_state_with_tenant_quota(true);
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("remove-quota-mismatch");
        let object = durable_disk_object(tenant_id, "remove-quota-mismatch");
        state
            .client_objects
            .entry(object.client_id)
            .or_default()
            .insert(scoped_key.clone());
        state.objects.insert(scoped_key.clone(), object);
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::json!({"op": "remove", "key": scoped_key}).to_string(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.objects.contains_key("default\0remove-quota-mismatch"));
        assert!(
            state
                .client_objects
                .iter()
                .any(|entry| entry.value().contains("default\0remove-quota-mismatch"))
        );
    }

    #[test]
    fn v3_replace_quota_mismatch_keeps_previous_object_and_sequence() {
        let state = make_state_with_tenant_quota(true);
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("replace-quota-mismatch");
        let previous = durable_disk_object(tenant_id.clone(), "replace-quota-mismatch");
        let previous_client = previous.client_id;
        state.objects.insert(scoped_key.clone(), previous);
        let mut replacement = durable_disk_object(tenant_id, "replace-quota-mismatch");
        replacement.group_id = "replacement".into();
        let manager =
            crate::oplog::OpLogManager::new(Some(Box::new(crate::oplog::InMemoryOpLog::new(8))), 1);
        manager
            .record_object_image_durable(&scoped_key, &replacement)
            .unwrap();
        let records = manager.read_since(1, 1).unwrap();
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(applier.apply_op_log_entries(&records), 0);
        assert_eq!(applier.get_expected_sequence_id(), 1);
        let restored = state.objects.get(&scoped_key).unwrap();
        assert_eq!(restored.client_id, previous_client);
        assert!(restored.group_id.is_empty());
    }

    #[test]
    fn unmount_quota_mismatch_is_atomic_across_all_affected_objects() {
        let state = make_state_with_tenant_quota(true);
        let tenant_id = TenantId::default();
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, client_id, 4096);
        for (index, user_key) in ["left", "right"].into_iter().enumerate() {
            let scoped_key = tenant_id.make_scoped_key(user_key);
            let mut object = durable_disk_object(tenant_id.clone(), user_key);
            object.replicas[0].replica_type = ReplicaType::Memory;
            object.replicas[0].segment_id = segment_id;
            object.replicas[0].offset = (index as u64) * 128;
            object.replicas[0].holder_client_id = Some(client_id);
            object.committed_quota_charge_bytes = 128;
            state.objects.insert(scoped_key, object);
        }
        // Deliberately account only one of the two objects. Projection may
        // encounter either DashMap iteration order, but must commit neither.
        state
            .tenant_quotas
            .write()
            .restore_object_checked(&tenant_id, 128)
            .unwrap();
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::json!({
                    "op": "unmount_segment",
                    "segment_id": segment_id.to_string(),
                    "segment_name": "seg-a",
                })
                .to_string(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.segments.contains_key(&segment_id));
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));
        assert!(state.objects.contains_key("default\0left"));
        assert!(state.objects.contains_key("default\0right"));
    }

    #[test]
    fn malformed_v3_object_image_does_not_advance_sequence() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let payload = serde_json::json!({
            "op": "put_end",
            "schema_version": 3,
            "key": "default\0bad-v3",
            "tenant_id": "default",
            "user_key": "bad-v3",
            "object": {
                "size": 64,
                "client_id": Uuid::nil().to_string(),
                "group_id": "",
                "replicas": [{
                    "segment_id": Uuid::nil(),
                    "segment_name": "",
                    "offset": 0,
                    "size": 64,
                    "status": mooncake_store_core::ReplicaStatus::Complete,
                    "replica_type": ReplicaType::All,
                    "holder_client_id": null,
                    "local_disk_storage_id": null,
                    "local_disk_generation_id": null,
                    "refcnt": 0,
                    "handle_valid": false,
                    "base_addr": 0,
                    "protocol": ""
                }],
                "hard_pinned": false,
                "data_type": mooncake_store_core::ObjectDataType::General,
                "put_start_time_ms": null,
                "lease_timeout_ms": null,
                "soft_pin_timeout_ms": null,
                "quota_committed": true,
                "reserved_quota_charge_bytes": 0,
                "committed_quota_charge_bytes": 0,
                "pending_replaced_quota_charge_bytes": 0
            }
        });

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::to_string(&payload).unwrap(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.objects.is_empty());
    }

    #[test]
    fn future_put_end_schema_does_not_fall_back_to_legacy_replay() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let payload = serde_json::json!({
            "op": "put_end",
            "schema_version": 4,
            "key": "default\0future-image",
            "tenant_id": "default",
            "user_key": "future-image",
            "size": 1,
            "replicas": [{
                "segment_id": Uuid::nil(),
                "segment_name": "global-disk",
                "offset": 0,
                "size": 1,
                "status": mooncake_store_core::ReplicaStatus::Complete,
                "replica_type": ReplicaType::Disk,
                "holder_client_id": null,
                "local_disk_storage_id": null,
                "local_disk_generation_id": null,
                "refcnt": 0,
                "handle_valid": true,
                "base_addr": 0,
                "protocol": ""
            }]
        });

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::to_string(&payload).unwrap(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.objects.is_empty());
    }

    #[test]
    fn legacy_put_end_without_snapshot_object_does_not_advance_or_clear_processing() {
        let state = make_state();
        let scoped_key = TenantId::default().make_scoped_key("k1");
        state.processing_keys.insert(scoped_key.clone(), ());
        let applier = OpLogApplier::new(state.clone());

        let payload = r#"{"op":"put_end","key":"k1","size":100}"#;
        let entries = vec![OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: payload.to_string(),
        }];
        let n = applier.apply_op_log_entries(&entries);
        assert_eq!(n, 0);
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.processing_keys.contains_key(&scoped_key));
        assert!(state.objects.is_empty());
    }

    #[test]
    fn legacy_put_end_rejects_present_non_numeric_size_without_mutation() {
        let state = make_state();
        let scoped_key = TenantId::default().make_scoped_key("legacy-size-type");
        insert_default_tenant_object(&state, "legacy-size-type");
        state.processing_keys.insert(scoped_key.clone(), ());
        let applier = OpLogApplier::new(state.clone());

        let applied = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: r#"{"op":"put_end","key":"legacy-size-type","size":"128"}"#.to_string(),
        }]);

        assert_eq!(applied, 0);
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.objects.contains_key(&scoped_key));
        assert!(state.processing_keys.contains_key(&scoped_key));
    }

    #[test]
    fn legacy_put_end_quota_failure_leaves_object_and_ledger_unchanged() {
        let state = make_state_with_tenant_quota(true);
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("legacy-atomic");
        insert_default_tenant_object(&state, "legacy-atomic");
        {
            let mut object = state.objects.get_mut(&scoped_key).unwrap();
            object.size = 32;
            object.reserved_quota_charge_bytes = 64;
            object.replicas.push(ReplicaDescriptor {
                segment_id: Uuid::new_v4(),
                segment_name: "legacy-segment".into(),
                offset: 0,
                size: 64,
                status: mooncake_store_core::ReplicaStatus::Allocating,
                replica_type: ReplicaType::Memory,
                holder_client_id: Some(Uuid::new_v4()),
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: false,
                base_addr: 0,
                protocol: String::new(),
            });
        }
        state
            .tenant_quotas
            .write()
            .restore_reservation_checked(&tenant_id, 32)
            .unwrap();
        state.processing_keys.insert(scoped_key.clone(), ());
        let applier = OpLogApplier::new(state.clone());

        let applied = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: r#"{"op":"put_end","key":"legacy-atomic","size":64}"#.to_string(),
        }]);

        assert_eq!(applied, 0);
        assert_eq!(applier.get_expected_sequence_id(), 1);
        let object = state.objects.get(&scoped_key).unwrap();
        assert_eq!(object.size, 32);
        assert!(!object.quota_committed);
        assert_eq!(object.reserved_quota_charge_bytes, 64);
        assert_eq!(
            object.replicas[0].status,
            mooncake_store_core::ReplicaStatus::Allocating
        );
        drop(object);
        let quota = state.tenant_quotas.read().get_snapshot(&tenant_id).unwrap();
        assert_eq!(quota.reserved_bytes, 32);
        assert_eq!(quota.used_bytes, 0);
        assert!(state.processing_keys.contains_key(&scoped_key));
    }

    #[test]
    fn legacy_put_end_completes_committed_in_place_upsert_generation() {
        let state = make_state();
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("legacy-in-place");
        let client_id = Uuid::new_v4();
        let mut object = durable_disk_object(tenant_id, "legacy-in-place");
        object.client_id = client_id;
        object.put_start_time = Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1_000));
        object.replicas[0] = ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: "legacy-in-place-segment".into(),
            offset: 0,
            size: object.size,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: false,
            base_addr: 0,
            protocol: String::new(),
        };
        object.committed_quota_charge_bytes = object.size;
        state.objects.insert(scoped_key.clone(), object);
        state.processing_keys.insert(scoped_key.clone(), ());
        let applier = OpLogApplier::new(state.clone());

        let applied = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: r#"{"op":"put_end","key":"legacy-in-place","size":128}"#.to_string(),
        }]);

        assert_eq!(applied, 1);
        assert_eq!(applier.get_expected_sequence_id(), 2);
        let restored = state.objects.get(&scoped_key).unwrap();
        assert!(restored.quota_committed);
        assert_eq!(restored.committed_quota_charge_bytes, restored.size);
        assert_eq!(restored.replicas[0].status, ReplicaStatus::Complete);
        assert!(restored.put_start_time.is_none());
        assert!(!state.processing_keys.contains_key(&scoped_key));
    }

    #[test]
    fn test_recover_clears_transient_promotion_candidates_only() {
        use crate::service::state::{
            PromotionCandidate, PromotionCandidateReason, PromotionTaskEntry,
        };
        use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
        use std::time::Instant;

        let state = make_state();
        let now = Instant::now();
        state.promotion_candidates.insert(
            "tenant/key".into(),
            PromotionCandidate {
                sketch_score: 2,
                first_seen: now,
                last_seen: now,
                retry_after: now,
                last_reason: PromotionCandidateReason::QueueCap,
                last_error_code: None,
                retry_count: 1,
            },
        );
        state.promotion_candidate_count.store(1, Ordering::Relaxed);
        state.promotion_retry_cursor.store(77, Ordering::Relaxed);
        state.promotion_tasks.insert(
            "tenant/in-flight".into(),
            PromotionTaskEntry {
                holder_id: Uuid::new_v4(),
                storage_id: Uuid::new_v4(),
                object_size: 8,
                source: ReplicaDescriptor {
                    segment_id: Uuid::new_v4(),
                    segment_name: "disk".into(),
                    offset: 0,
                    size: 8,
                    status: ReplicaStatus::Complete,
                    replica_type: ReplicaType::LocalDisk,
                    holder_client_id: Some(Uuid::new_v4()),
                    local_disk_storage_id: Some(Uuid::new_v4()),
                    local_disk_generation_id: Some(Uuid::new_v4()),
                    refcnt: 1,
                    handle_valid: true,
                    base_addr: 0,
                    protocol: String::new(),
                },
                staged_segment_id: None,
                staged_offset: None,
                reserved_quota_charge_bytes: 0,
                start_time: now,
            },
        );

        OpLogApplier::new(state.clone()).recover(12);

        assert!(state.promotion_candidates.is_empty());
        assert_eq!(state.promotion_candidate_count.load(Ordering::Relaxed), 0);
        assert_eq!(state.promotion_retry_cursor.load(Ordering::Relaxed), 0);
        assert!(state.promotion_tasks.contains_key("tenant/in-flight"));
    }

    #[test]
    fn test_apply_put_end_recreates_object_from_metadata_payload() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = uuid::Uuid::new_v4();
        let client_id = uuid::Uuid::new_v4();
        insert_memory_segment(&state, segment_id, client_id, 4096);
        let replica = mooncake_store_core::ReplicaDescriptor {
            segment_id,
            segment_name: "seg-a".to_string(),
            offset: 128,
            size: 256,
            status: mooncake_store_core::ReplicaStatus::Complete,
            replica_type: mooncake_store_core::ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 4096,
            protocol: "rdma".to_string(),
        };
        let payload = serde_json::json!({
            "op": "put_end",
            "key": "tenant-a\0k1",
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
        let object = state.objects.get("tenant-a\0k1").unwrap();
        assert_eq!(object.size, 256);
        assert_eq!(object.client_id, client_id);
        assert_eq!(object.tenant_id.as_str(), "tenant-a");
        assert_eq!(object.group_id, "group-a");
        assert_eq!(object.user_key, "k1");
        assert_eq!(object.replicas.len(), 1);
        assert_eq!(object.replicas[0].segment_id, segment_id);
        assert!(!object.replicas[0].handle_valid);
        assert_eq!(object.replicas[0].base_addr, 0);
        assert!(object.replicas[0].protocol.is_empty());
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(256));
    }

    #[test]
    fn test_replay_rejects_overlapping_object_image_before_commit() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, client_id, 4096);
        let payload = |key: &str, offset: u64| {
            serde_json::json!({
                "op": "put_end",
                "key": format!("tenant-a\0{key}"),
                "size": 128,
                "client_id": client_id.to_string(),
                "tenant_id": "tenant-a",
                "group_id": "",
                "user_key": key,
                "replicas": [{
                    "segment_id": segment_id,
                    "segment_name": "seg-a",
                    "offset": offset,
                    "size": 128,
                    "status": mooncake_store_core::ReplicaStatus::Complete,
                    "replica_type": mooncake_store_core::ReplicaType::Memory,
                    "holder_client_id": client_id,
                    "refcnt": 0,
                    "handle_valid": true,
                    "base_addr": 4096,
                    "protocol": "rdma",
                }],
            })
            .to_string()
        };

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: payload("left", 0),
            }]),
            1
        );
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 2,
                producer_view_version: 1,
                payload: payload("overlap", 64),
            }]),
            0
        );

        assert_eq!(applier.get_expected_sequence_id(), 2);
        assert!(state.objects.contains_key("tenant-a\0left"));
        assert!(!state.objects.contains_key("tenant-a\0overlap"));
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(128));
    }

    #[test]
    fn test_replay_mount_object_and_unmount_converges_segment_state() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let mount = serde_json::json!({
            "op": "mount_segment",
            "schema_version": 1,
            "segment_name": "replayed-segment",
            "segment_id": segment_id.to_string(),
            "base": 4096,
            "size": 4096,
            "te_endpoint": "127.0.0.1:12345",
            "protocol": "rdma",
            "client_id": client_id.to_string(),
        })
        .to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: mount,
            }]),
            1
        );
        assert!(state.segments.contains_key(&segment_id));
        let replayed_segment = state.segments.get(&segment_id).unwrap();
        assert_eq!(replayed_segment.segment.base, 0);
        assert!(replayed_segment.segment.te_endpoint.is_empty());
        assert!(replayed_segment.segment.protocol.is_empty());
        drop(replayed_segment);
        assert!(
            state
                .allocator
                .write()
                .allocate_from_segment("replayed-segment", 64)
                .is_err(),
            "standby must not allocate from a leader process address"
        );

        let object = serde_json::json!({
            "op": "put_end",
            "key": "tenant-a\0mounted-key",
            "size": 128,
            "client_id": client_id.to_string(),
            "tenant_id": "tenant-a",
            "group_id": "",
            "user_key": "mounted-key",
            "replicas": [{
                "segment_id": segment_id,
                "segment_name": "replayed-segment",
                "offset": 0,
                "size": 128,
                "status": mooncake_store_core::ReplicaStatus::Complete,
                "replica_type": mooncake_store_core::ReplicaType::Memory,
                "holder_client_id": client_id,
                "refcnt": 0,
                "handle_valid": true,
                "base_addr": 4096,
                "protocol": "rdma",
            }],
        })
        .to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 2,
                producer_view_version: 1,
                payload: object,
            }]),
            1
        );
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(128));

        let unmount = serde_json::json!({
            "op": "unmount_segment",
            "segment_id": segment_id.to_string(),
            "segment_name": "replayed-segment",
        })
        .to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 3,
                producer_view_version: 1,
                payload: unmount,
            }]),
            1
        );
        assert!(!state.segments.contains_key(&segment_id));
        assert!(!state.objects.contains_key("tenant-a\0mounted-key"));
    }

    #[test]
    fn replay_unmount_prunes_only_matching_delayed_replica() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let client_id = Uuid::new_v4();
        let removed_segment_id = Uuid::new_v4();
        let retained_segment_id = Uuid::new_v4();
        insert_memory_segment(&state, removed_segment_id, client_id, 4096);
        insert_memory_segment(&state, retained_segment_id, client_id, 4096);
        let replica = |segment_id| ReplicaDescriptor {
            segment_id,
            segment_name: "seg-a".into(),
            offset: 0,
            size: 128,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: false,
            base_addr: 0,
            protocol: String::new(),
        };
        let release_id = Uuid::new_v4();
        state.delayed_replica_releases.insert(
            release_id,
            crate::service::state::DelayedReplicaReleaseEntry {
                id: release_id,
                scoped_key: TenantId::default().make_scoped_key("retired"),
                deadline_epoch_ms: 1_900_000_000_000,
                replicas: vec![replica(removed_segment_id), replica(retained_segment_id)],
            },
        );

        let unmount = serde_json::json!({
            "op": "unmount_segment",
            "segment_id": removed_segment_id.to_string(),
            "segment_name": "seg-a",
        })
        .to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: unmount,
            }]),
            1
        );

        let release = state.delayed_replica_releases.get(&release_id).unwrap();
        assert_eq!(release.replicas.len(), 1);
        assert_eq!(release.replicas[0].segment_id, retained_segment_id);
    }

    #[test]
    fn replay_unmount_aborts_affected_replication_without_removing_surviving_object() {
        let state = make_state_with_tenant_quota(true);
        let applier = OpLogApplier::new(state.clone());
        let client_id = Uuid::new_v4();
        let source_segment_id = Uuid::new_v4();
        let target_segment_id = Uuid::new_v4();
        insert_memory_segment(&state, source_segment_id, client_id, 4096);
        insert_memory_segment(&state, target_segment_id, client_id, 4096);
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("copy-during-unmount");
        let source = ReplicaDescriptor {
            segment_id: source_segment_id,
            segment_name: "seg-a".into(),
            offset: 0,
            size: 128,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 1,
            handle_valid: false,
            base_addr: 0,
            protocol: String::new(),
        };
        let target = ReplicaDescriptor {
            segment_id: target_segment_id,
            segment_name: "seg-a".into(),
            offset: 0,
            size: 128,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: false,
            base_addr: 0,
            protocol: String::new(),
        };
        state.objects.insert(
            scoped_key.clone(),
            crate::service::state::ObjectEntry {
                replicas: vec![source.clone(), target.clone()],
                size: 128,
                last_access: SystemTime::now(),
                hard_pinned: false,
                data_type: mooncake_store_core::ObjectDataType::General,
                client_id,
                put_start_time: Some(SystemTime::now()),
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: tenant_id.clone(),
                group_id: String::new(),
                quota_committed: true,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 128,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "copy-during-unmount".into(),
            },
        );
        state.replication_tasks.insert(
            scoped_key.clone(),
            ReplicationTaskEntry {
                client_id,
                start_time: Instant::now(),
                kind: ReplicationTaskKind::Copy,
                source,
                targets: vec![target],
                existing_move_target: None,
                reserved_quota_charge_bytes: 128,
            },
        );
        {
            let mut quotas = state.tenant_quotas.write();
            quotas.restore_object_checked(&tenant_id, 128).unwrap();
            quotas.restore_reservation_checked(&tenant_id, 128).unwrap();
        }

        let unmount = serde_json::json!({
            "op": "unmount_segment",
            "segment_id": target_segment_id.to_string(),
            "segment_name": "seg-a",
        })
        .to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: unmount,
            }]),
            1
        );

        assert!(!state.replication_tasks.contains_key(&scoped_key));
        let object = state.objects.get(&scoped_key).unwrap();
        assert_eq!(object.replicas.len(), 1);
        assert_eq!(object.replicas[0].segment_id, source_segment_id);
        assert_eq!(object.replicas[0].refcnt, 0);
        drop(object);
        let quota = state.tenant_quotas.read().get_snapshot(&tenant_id).unwrap();
        assert_eq!(quota.used_bytes, 128);
        assert_eq!(quota.reserved_bytes, 0);
        assert_eq!(quota.metadata_object_count, 1);
    }

    #[test]
    fn test_replay_graceful_unmount_preserves_deadline_until_completion() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let mount = serde_json::json!({
            "op": "mount_segment",
            "schema_version": 1,
            "segment_name": "graceful-replay",
            "segment_id": segment_id.to_string(),
            "base": 4096,
            "size": 4096,
            "te_endpoint": "127.0.0.1:12345",
            "protocol": "rdma",
            "client_id": client_id.to_string(),
        })
        .to_string();
        let graceful = serde_json::json!({
            "op": "graceful_unmount_segment",
            "schema_version": 1,
            "segment_name": "graceful-replay",
            "segment_id": segment_id.to_string(),
            "client_id": client_id.to_string(),
            "deadline_epoch_ms": 1_900_000_000_000_u64,
        })
        .to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[
                OpLogRecord {
                    seq: 1,
                    producer_view_version: 1,
                    payload: mount,
                },
                OpLogRecord {
                    seq: 2,
                    producer_view_version: 1,
                    payload: graceful,
                },
            ]),
            2
        );

        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            crate::proto::SegmentStatus::GracefullyUnmounting
        );
        assert_eq!(
            state
                .graceful_unmounts
                .get(&segment_id)
                .unwrap()
                .deadline_epoch_ms,
            1_900_000_000_000
        );

        let completion = serde_json::json!({
            "op": "unmount_segment",
            "segment_name": "graceful-replay",
            "segment_id": segment_id.to_string(),
        })
        .to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 3,
                producer_view_version: 1,
                payload: completion,
            }]),
            1
        );
        assert!(!state.segments.contains_key(&segment_id));
        assert!(!state.graceful_unmounts.contains_key(&segment_id));
    }

    #[test]
    fn test_replay_rejects_conflicting_graceful_unmount_identity() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, client_id, 4096);
        let payload = serde_json::json!({
            "op": "graceful_unmount_segment",
            "schema_version": 1,
            "segment_name": "different-name",
            "segment_id": segment_id.to_string(),
            "client_id": client_id.to_string(),
            "deadline_epoch_ms": 1_900_000_000_000_u64,
        })
        .to_string();

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload,
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            crate::proto::SegmentStatus::Active
        );
        assert!(!state.graceful_unmounts.contains_key(&segment_id));
    }

    #[test]
    fn test_replay_rejects_conflicting_graceful_intent_without_mutation() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let prior_client_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, client_id, 4096);
        state.graceful_unmounts.insert(
            segment_id,
            GracefulUnmountSnapshotEntry {
                segment_id,
                client_id: prior_client_id,
                deadline_epoch_ms: 1_800_000_000_000,
            },
        );
        let payload = serde_json::json!({
            "op": "graceful_unmount_segment",
            "schema_version": 1,
            "segment_name": "seg-a",
            "segment_id": segment_id.to_string(),
            "client_id": client_id.to_string(),
            "deadline_epoch_ms": 1_900_000_000_000_u64,
        })
        .to_string();

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload,
            }]),
            0
        );
        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            crate::proto::SegmentStatus::Active
        );
        let intent = state.graceful_unmounts.get(&segment_id).unwrap();
        assert_eq!(intent.client_id, prior_client_id);
        assert_eq!(intent.deadline_epoch_ms, 1_800_000_000_000);
    }

    #[test]
    fn test_replay_rejects_zero_deadline_graceful_unmount() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, client_id, 4096);
        let payload = serde_json::json!({
            "op": "graceful_unmount_segment",
            "schema_version": 1,
            "segment_name": "seg-a",
            "segment_id": segment_id.to_string(),
            "client_id": client_id.to_string(),
            "deadline_epoch_ms": 0,
        })
        .to_string();

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload,
            }]),
            0
        );
        assert_eq!(
            state.segments.get(&segment_id).unwrap().status,
            crate::proto::SegmentStatus::Active
        );
        assert!(!state.graceful_unmounts.contains_key(&segment_id));
    }

    #[test]
    fn test_replay_rejects_future_mount_schema() {
        let state = make_state();
        let applier = OpLogApplier::new(state);
        let payload = serde_json::json!({
            "op": "mount_segment",
            "schema_version": 2,
        })
        .to_string();

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload,
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
    }

    #[test]
    fn legacy_mount_advances_only_for_unique_snapshot_backed_topology() {
        let record = || OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: serde_json::json!({
                "op": "mount_segment",
                "segment_name": "seg-a",
            })
            .to_string(),
        };

        let missing = make_state();
        let missing_applier = OpLogApplier::new(missing.clone());
        assert_eq!(missing_applier.apply_op_log_entries(&[record()]), 0);
        assert_eq!(missing_applier.get_expected_sequence_id(), 1);

        let ambiguous = make_state();
        insert_memory_segment(&ambiguous, Uuid::new_v4(), Uuid::new_v4(), 4096);
        insert_memory_segment(&ambiguous, Uuid::new_v4(), Uuid::new_v4(), 4096);
        let ambiguous_applier = OpLogApplier::new(ambiguous);
        assert_eq!(ambiguous_applier.apply_op_log_entries(&[record()]), 0);
        assert_eq!(ambiguous_applier.get_expected_sequence_id(), 1);

        let unique = make_state();
        insert_memory_segment(&unique, Uuid::new_v4(), Uuid::new_v4(), 4096);
        let unique_applier = OpLogApplier::new(unique);
        assert_eq!(unique_applier.apply_op_log_entries(&[record()]), 1);
        assert_eq!(unique_applier.get_expected_sequence_id(), 2);
    }

    #[test]
    fn test_replay_rejects_mount_records_the_leader_cannot_publish() {
        let client_id = Uuid::new_v4();
        let segment_id = Uuid::new_v4();
        let valid_memory = serde_json::json!({
            "op": "mount_segment",
            "schema_version": 1,
            "segment_name": "memory-segment",
            "segment_id": segment_id.to_string(),
            "base": 4096,
            "size": 4096,
            "te_endpoint": "",
            "protocol": "",
            "host_id": "",
            "client_id": client_id.to_string(),
        });
        let valid_nof = serde_json::json!({
            "op": "mount_nof_segment",
            "schema_version": 1,
            "segment_name": "nof-segment",
            "segment_id": segment_id.to_string(),
            "base": 4096,
            "size": 4096,
            "te_endpoint": "nof://endpoint",
            "client_id": client_id.to_string(),
        });
        let mut malformed = Vec::new();
        for (field, value) in [
            ("segment_name", serde_json::json!("")),
            ("base", serde_json::json!(0)),
            ("client_id", serde_json::json!(Uuid::nil().to_string())),
            ("identity_version", serde_json::json!(1)),
            ("identity_version", serde_json::json!(2)),
        ] {
            let mut payload = valid_memory.clone();
            payload[field] = value;
            malformed.push(payload);
        }
        for (field, value) in [
            ("segment_name", serde_json::json!("")),
            ("segment_id", serde_json::json!(Uuid::nil().to_string())),
            ("size", serde_json::json!(0)),
            ("te_endpoint", serde_json::json!("")),
            ("client_id", serde_json::json!(Uuid::nil().to_string())),
        ] {
            let mut payload = valid_nof.clone();
            payload[field] = value;
            malformed.push(payload);
        }

        for payload in malformed {
            let state = make_state();
            let applier = OpLogApplier::new(state.clone());
            assert_eq!(
                applier.apply_op_log_entries(&[OpLogRecord {
                    seq: 1,
                    producer_view_version: 1,
                    payload: payload.to_string(),
                }]),
                0,
                "malformed mount record must fail closed: {payload}"
            );
            assert_eq!(applier.get_expected_sequence_id(), 1);
            assert!(state.segments.is_empty());
            assert!(state.nof_segments.is_empty());
        }
    }

    #[test]
    fn mount_replay_rejects_runtime_capability_and_allocator_config_mismatch() {
        let client_id = Uuid::new_v4();
        let memory_payload = |segment_id: Uuid, base: u64, size: u64, protocol: &str| {
            serde_json::json!({
                "op": "mount_segment",
                "schema_version": 1,
                "segment_name": "memory-segment",
                "segment_id": segment_id.to_string(),
                "base": base,
                "size": size,
                "te_endpoint": "tcp://endpoint",
                "protocol": protocol,
                "host_id": "host-a",
                "client_id": client_id.to_string(),
            })
        };
        let nof_payload = |segment_id: Uuid, base: u64, size: u64| {
            serde_json::json!({
                "op": "mount_nof_segment",
                "schema_version": 1,
                "segment_name": "nof-segment",
                "segment_id": segment_id.to_string(),
                "base": base,
                "size": size,
                "te_endpoint": "nof://endpoint",
                "client_id": client_id.to_string(),
            })
        };

        let mut cases = Vec::new();
        let cxl_disabled = make_state();
        cases.push((
            cxl_disabled,
            memory_payload(
                Uuid::new_v4(),
                0,
                crate::service::state::MasterRuntimeConfig::default().cxl_size,
                "cxl",
            ),
        ));

        let mut cachelib_memory = make_state();
        Arc::get_mut(&mut cachelib_memory)
            .expect("fresh test state must be uniquely owned")
            .runtime_config
            .memory_allocator_kind = crate::allocator::MemoryAllocatorKind::CachelibLike;
        cases.push((
            cachelib_memory,
            memory_payload(
                Uuid::new_v4(),
                crate::allocator::CACHELIB_SLAB_SIZE,
                crate::allocator::CACHELIB_MAX_SEGMENT_SIZE + crate::allocator::CACHELIB_SLAB_SIZE,
                "tcp",
            ),
        ));

        let mut cachelib_nof = make_state();
        Arc::get_mut(&mut cachelib_nof)
            .expect("fresh test state must be uniquely owned")
            .runtime_config
            .memory_allocator_kind = crate::allocator::MemoryAllocatorKind::CachelibLike;
        cases.push((
            cachelib_nof,
            nof_payload(
                Uuid::new_v4(),
                crate::allocator::CACHELIB_SLAB_SIZE,
                crate::allocator::CACHELIB_MAX_SEGMENT_SIZE + crate::allocator::CACHELIB_SLAB_SIZE,
            ),
        ));

        let mut nof_disabled = make_state();
        Arc::get_mut(&mut nof_disabled)
            .expect("fresh test state must be uniquely owned")
            .runtime_config
            .enable_nof = false;
        cases.push((
            nof_disabled,
            nof_payload(Uuid::new_v4(), 0, crate::allocator::CACHELIB_SLAB_SIZE),
        ));

        for (state, payload) in cases {
            let applier = OpLogApplier::new(state.clone());
            assert_eq!(
                applier.apply_op_log_entries(&[OpLogRecord {
                    seq: 1,
                    producer_view_version: 1,
                    payload: payload.to_string(),
                }]),
                0,
                "runtime-incompatible mount must fail closed: {payload}"
            );
            assert_eq!(applier.get_expected_sequence_id(), 1);
            assert!(state.segments.is_empty());
            assert!(state.nof_segments.is_empty());
        }
    }

    #[test]
    fn cachelib_mount_replay_accepts_page_aligned_external_base() {
        let mut state = make_state();
        Arc::get_mut(&mut state)
            .expect("fresh test state must be uniquely owned")
            .runtime_config
            .memory_allocator_kind = crate::allocator::MemoryAllocatorKind::CachelibLike;
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let payload = serde_json::json!({
            "op": "mount_segment",
            "schema_version": 1,
            "segment_name": "file-backed-memory",
            "segment_id": segment_id.to_string(),
            "base": 4096,
            "size": crate::allocator::CACHELIB_SLAB_SIZE,
            "te_endpoint": "tcp://endpoint",
            "protocol": "tcp",
            "host_id": "host-a",
            "client_id": client_id.to_string(),
        });
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: payload.to_string(),
            }]),
            1
        );
        assert!(state.segments.contains_key(&segment_id));
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));
    }

    #[test]
    fn mount_replay_rejects_allocator_only_segment_identity_without_overwrite() {
        let client_id = Uuid::new_v4();
        for nof in [false, true] {
            let state = make_state();
            let segment_id = Uuid::new_v4();
            let orphan = Segment {
                id: segment_id,
                name: "allocator-only".into(),
                base: 4096,
                size: 8192,
                te_endpoint: String::new(),
                protocol: "rdma".into(),
                host_id: String::new(),
            };
            if nof {
                state
                    .nof_allocator
                    .write()
                    .add_segment(orphan, 128, client_id);
            } else {
                state.allocator.write().add_segment(orphan, 128, client_id);
            }

            let payload = if nof {
                serde_json::json!({
                    "op": "mount_nof_segment",
                    "schema_version": 1,
                    "segment_name": "replacement",
                    "segment_id": segment_id.to_string(),
                    "base": 8192,
                    "size": 4096,
                    "te_endpoint": "nof://replacement",
                    "client_id": client_id.to_string(),
                })
            } else {
                serde_json::json!({
                    "op": "mount_segment",
                    "schema_version": 1,
                    "segment_name": "replacement",
                    "segment_id": segment_id.to_string(),
                    "base": 8192,
                    "size": 4096,
                    "te_endpoint": "tcp://replacement",
                    "protocol": "tcp",
                    "host_id": "host-a",
                    "client_id": client_id.to_string(),
                })
            };
            let applier = OpLogApplier::new(state.clone());
            assert_eq!(
                applier.apply_op_log_entries(&[OpLogRecord {
                    seq: 1,
                    producer_view_version: 1,
                    payload: payload.to_string(),
                }]),
                0
            );
            assert_eq!(applier.get_expected_sequence_id(), 1);
            assert_eq!(
                if nof {
                    state.nof_allocator.read().used_bytes(&segment_id)
                } else {
                    state.allocator.read().used_bytes(&segment_id)
                },
                Some(128)
            );
            assert!(!state.segments.contains_key(&segment_id));
            assert!(!state.nof_segments.contains_key(&segment_id));
        }
    }

    #[test]
    fn mount_replay_rejects_cross_type_segment_uuid_collision() {
        let client_id = Uuid::new_v4();
        for replay_nof in [false, true] {
            for opposite_topology in [false, true] {
                let state = make_state();
                let segment_id = Uuid::new_v4();
                let opposite_allocator_segment = Segment {
                    id: segment_id,
                    name: "opposite".into(),
                    base: 4096,
                    size: 8192,
                    te_endpoint: "opposite://endpoint".into(),
                    protocol: String::new(),
                    host_id: String::new(),
                };
                if replay_nof {
                    if opposite_topology {
                        state.segments.insert(
                            segment_id,
                            crate::service::SegmentEntry {
                                segment: opposite_allocator_segment,
                                used: 0,
                                client_id,
                                status: crate::proto::SegmentStatus::Active,
                            },
                        );
                    } else {
                        state.allocator.write().add_segment(
                            opposite_allocator_segment,
                            128,
                            client_id,
                        );
                    }
                } else if opposite_topology {
                    state.nof_segments.insert(
                        segment_id,
                        crate::service::NoFSegmentEntry {
                            segment: mooncake_store_core::NoFSegment {
                                id: segment_id,
                                name: "opposite".into(),
                                base: 4096,
                                size: 8192,
                                te_endpoint: "opposite://endpoint".into(),
                                client_id,
                            },
                            used: 0,
                            status: crate::proto::SegmentStatus::Active,
                        },
                    );
                } else {
                    state.nof_allocator.write().add_segment(
                        opposite_allocator_segment,
                        128,
                        client_id,
                    );
                }

                let payload = if replay_nof {
                    serde_json::json!({
                        "op": "mount_nof_segment",
                        "schema_version": 1,
                        "segment_name": "nof",
                        "segment_id": segment_id.to_string(),
                        "base": 8192,
                        "size": 4096,
                        "te_endpoint": "nof://endpoint",
                        "client_id": client_id.to_string(),
                    })
                } else {
                    serde_json::json!({
                        "op": "mount_segment",
                        "schema_version": 1,
                        "segment_name": "memory",
                        "segment_id": segment_id.to_string(),
                        "base": 8192,
                        "size": 4096,
                        "te_endpoint": "tcp://endpoint",
                        "protocol": "tcp",
                        "host_id": "host-a",
                        "client_id": client_id.to_string(),
                    })
                };
                let applier = OpLogApplier::new(state.clone());
                assert_eq!(
                    applier.apply_op_log_entries(&[OpLogRecord {
                        seq: 1,
                        producer_view_version: 1,
                        payload: payload.to_string(),
                    }]),
                    0
                );
                assert_eq!(applier.get_expected_sequence_id(), 1);
                if replay_nof {
                    assert!(!state.nof_segments.contains_key(&segment_id));
                    assert_eq!(state.nof_allocator.read().used_bytes(&segment_id), None);
                } else {
                    assert!(!state.segments.contains_key(&segment_id));
                    assert_eq!(state.allocator.read().used_bytes(&segment_id), None);
                }
            }
        }
    }

    #[test]
    fn test_replay_rejects_future_or_conflicting_unmount_identity() {
        for payload in [
            serde_json::json!({
                "op": "unmount_segment",
                "schema_version": 2,
                "segment_name": "seg-a",
            }),
            serde_json::json!({
                "op": "unmount_segment",
                "schema_version": 1,
                "segment_name": "different-name",
            }),
        ] {
            let state = make_state();
            let segment_id = Uuid::new_v4();
            insert_memory_segment(&state, segment_id, Uuid::new_v4(), 4096);
            let applier = OpLogApplier::new(state.clone());
            let mut payload = payload;
            payload["segment_id"] = serde_json::Value::String(segment_id.to_string());

            assert_eq!(
                applier.apply_op_log_entries(&[OpLogRecord {
                    seq: 1,
                    producer_view_version: 1,
                    payload: serde_json::to_string(&payload).unwrap(),
                }]),
                0
            );
            assert_eq!(applier.get_expected_sequence_id(), 1);
            assert!(state.segments.contains_key(&segment_id));
        }
    }

    #[test]
    fn test_replay_rejects_future_remove_schema_without_mutation() {
        let state = make_state();
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("future-remove");
        state.objects.insert(
            scoped_key.clone(),
            durable_disk_object(tenant_id, "future-remove"),
        );
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::json!({
                    "op": "remove",
                    "schema_version": 2,
                    "key": scoped_key,
                })
                .to_string(),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.objects.contains_key("default\0future-remove"));
    }

    #[test]
    fn test_replay_rejects_conflicting_duplicate_mount_identity() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let mount = |name: &str| {
            serde_json::json!({
                "op": "mount_segment",
                "schema_version": 1,
                "segment_name": name,
                "segment_id": segment_id.to_string(),
                "base": 4096,
                "size": 4096,
                "te_endpoint": "127.0.0.1:12345",
                "protocol": "rdma",
                "client_id": client_id.to_string(),
            })
            .to_string()
        };

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: mount("stable-name"),
            }]),
            1
        );
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 2,
                producer_view_version: 1,
                payload: mount("conflicting-name"),
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 2);
        assert_eq!(
            state.segments.get(&segment_id).unwrap().segment.name,
            "stable-name"
        );
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));
    }

    #[test]
    fn test_replay_put_end_and_remove_rebuild_tenant_quota() {
        let state = make_state_with_tenant_quota(true);
        let applier = OpLogApplier::new(state.clone());
        let tenant_id = TenantId::new("tenant-a".to_string()).unwrap();
        let scoped_key = tenant_id.make_scoped_key("quota-key");
        let segment_id = Uuid::new_v4();
        let holder_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, holder_id, 4096);
        let replica = mooncake_store_core::ReplicaDescriptor {
            segment_id,
            segment_name: "seg-a".to_string(),
            offset: 0,
            size: 256,
            status: mooncake_store_core::ReplicaStatus::Complete,
            replica_type: mooncake_store_core::ReplicaType::Memory,
            holder_client_id: Some(holder_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: "rdma".to_string(),
        };
        let put_end = serde_json::json!({
            "op": "put_end",
            "key": scoped_key.clone(),
            "size": 256,
            "client_id": Uuid::new_v4().to_string(),
            "tenant_id": "tenant-a",
            "group_id": "",
            "user_key": "quota-key",
            "replicas": [replica],
        })
        .to_string();

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: put_end,
            }]),
            1
        );
        let quota = state.tenant_quotas.read().get_snapshot(&tenant_id).unwrap();
        assert_eq!(quota.used_bytes, 256);
        assert_eq!(quota.committed_count, 1);
        assert_eq!(quota.metadata_object_count, 1);
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(256));

        let remove = serde_json::json!({"op": "remove", "key": scoped_key}).to_string();
        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 2,
                producer_view_version: 1,
                payload: remove,
            }]),
            1
        );
        let quota = state.tenant_quotas.read().get_snapshot(&tenant_id).unwrap();
        assert_eq!(quota.used_bytes, 0);
        assert_eq!(quota.committed_count, 0);
        assert_eq!(quota.metadata_object_count, 0);
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));
    }

    #[test]
    fn remove_replay_commits_rebuilt_allocator_without_releasing_stale_runtime_state() {
        let state = make_state();
        let tenant_id = TenantId::default();
        let scoped_key = tenant_id.make_scoped_key("stale-allocator-remove");
        let segment_id = Uuid::new_v4();
        let holder_id = Uuid::new_v4();
        insert_memory_segment(&state, segment_id, holder_id, 4096);
        let mut object = durable_disk_object(tenant_id, "stale-allocator-remove");
        object.size = 256;
        object.committed_quota_charge_bytes = 256;
        object.replicas = vec![ReplicaDescriptor {
            segment_id,
            segment_name: "seg-a".to_string(),
            offset: 0,
            size: 256,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(holder_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: false,
            base_addr: 0,
            protocol: String::new(),
        }];
        state
            .client_objects
            .entry(object.client_id)
            .or_default()
            .insert(scoped_key.clone());
        state.objects.insert(scoped_key.clone(), object);
        // Deliberately leave the old runtime allocator empty. Replay must use
        // the already validated post-record allocator candidate instead of
        // trying to release this stale runtime state after removing metadata.
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));
        let applier = OpLogApplier::new(state.clone());

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload: serde_json::json!({"op": "remove", "key": scoped_key}).to_string(),
            }]),
            1
        );

        assert_eq!(applier.get_expected_sequence_id(), 2);
        assert!(
            !state
                .objects
                .contains_key("default\0stale-allocator-remove")
        );
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(0));
        assert!(!state.service_fenced.load(Ordering::Acquire));
    }

    #[test]
    fn test_apply_put_end_normalizes_empty_tenant_and_rebuilds_scoped_key() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let payload = serde_json::json!({
            "op": "put_end",
            "key": "legacy-key",
            "size": 1,
            "tenant_id": "",
            "user_key": "legacy-key",
            "replicas": [{
                "segment_id": Uuid::nil(),
                "segment_name": "global-disk",
                "offset": 0,
                "size": 1,
                "status": mooncake_store_core::ReplicaStatus::Complete,
                "replica_type": ReplicaType::Disk,
                "holder_client_id": null,
                "local_disk_storage_id": null,
                "local_disk_generation_id": null,
                "refcnt": 0,
                "handle_valid": true,
                "base_addr": 0,
                "protocol": ""
            }],
        })
        .to_string();

        let applied = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload,
        }]);

        assert_eq!(applied, 1);
        let object = state.objects.get("default\0legacy-key").unwrap();
        assert_eq!(object.tenant_id, TenantId::default());
    }

    #[test]
    fn legacy_typed_put_end_rejects_malformed_object_image_without_advancing() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let payload = serde_json::json!({
            "op": "put_end",
            "key": "default\0bad-legacy-image",
            "size": 0,
            "client_id": "not-a-uuid",
            "tenant_id": "default",
            "group_id": "",
            "user_key": "bad-legacy-image",
            "replicas": [],
        })
        .to_string();

        assert_eq!(
            applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload,
            }]),
            0
        );
        assert_eq!(applier.get_expected_sequence_id(), 1);
        assert!(state.objects.is_empty());
    }

    #[test]
    fn test_apply_put_end_rejects_invalid_tenant() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let payload = serde_json::json!({
            "op": "put_end",
            "key": "bad\nname\0k1",
            "size": 1,
            "tenant_id": "bad\nname",
            "user_key": "k1",
            "replicas": [],
        })
        .to_string();

        let applied = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload,
        }]);

        assert_eq!(applied, 0);
        assert!(state.objects.is_empty());
    }

    #[test]
    fn test_apply_put_end_rejects_conflicting_scoped_and_metadata_tenants() {
        let state = make_state();
        let applier = OpLogApplier::new(state.clone());
        let payload = serde_json::json!({
            "op": "put_end",
            "key": "tenant-b\0k1",
            "size": 1,
            "tenant_id": "tenant-a",
            "user_key": "k1",
            "replicas": [],
        })
        .to_string();

        let applied = applier.apply_op_log_entries(&[OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload,
        }]);

        assert_eq!(applied, 0);
        assert!(state.objects.is_empty());
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
        let scoped_key = TenantId::default().make_scoped_key("k1");
        state.objects.insert(
            scoped_key.clone(),
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
                tenant_id: TenantId::default(),
                group_id: String::new(),
                quota_committed: false,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "k1".to_string(),
            },
        );
        state
            .client_objects
            .insert(client_id, std::iter::once(scoped_key.clone()).collect());
        state.processing_keys.insert(scoped_key.clone(), ());
        let replica = mooncake_store_core::ReplicaDescriptor {
            segment_id: uuid::Uuid::new_v4(),
            segment_name: "seg".to_string(),
            offset: 0,
            size: 0,
            status: mooncake_store_core::ReplicaStatus::Complete,
            replica_type: mooncake_store_core::ReplicaType::Memory,
            holder_client_id: Some(client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };
        state.replication_tasks.insert(
            scoped_key.clone(),
            crate::service::state::ReplicationTaskEntry {
                client_id,
                start_time: std::time::Instant::now(),
                kind: crate::service::state::ReplicationTaskKind::Copy,
                source: replica,
                targets: vec![],
                existing_move_target: None,
                reserved_quota_charge_bytes: 0,
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
        assert!(!state.objects.contains_key(&scoped_key));
        assert!(!state.processing_keys.contains_key(&scoped_key));
        assert!(!state.replication_tasks.contains_key(&scoped_key));
        assert!(
            !state
                .client_objects
                .get(&client_id)
                .unwrap()
                .contains(&scoped_key)
        );
    }

    #[test]
    fn test_apply_put_revoke_removes_object_and_processing_key() {
        let state = make_state();
        let scoped_key = TenantId::default().make_scoped_key("k1");
        state.objects.insert(
            scoped_key.clone(),
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
                tenant_id: TenantId::default(),
                group_id: String::new(),
                quota_committed: false,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "k1".to_string(),
            },
        );
        state.processing_keys.insert(scoped_key.clone(), ());
        let applier = OpLogApplier::new(state.clone());

        let payload = r#"{"op":"put_revoke","key":"k1"}"#;
        let entries = vec![OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: payload.to_string(),
        }];
        let n = applier.apply_op_log_entries(&entries);
        assert_eq!(n, 1);
        assert!(!state.objects.contains_key(&scoped_key));
        assert!(!state.processing_keys.contains_key(&scoped_key));
    }

    #[test]
    fn test_remove_like_rejects_invalid_scoped_tenant_without_advancing_sequence() {
        for op in ["remove", "put_revoke"] {
            let state = make_state();
            insert_default_tenant_object(&state, "k1");
            let applier = OpLogApplier::new(state.clone());
            let payload = serde_json::json!({
                "op": op,
                "key": "_reserved\0k1",
            })
            .to_string();

            let applied = applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload,
            }]);

            assert_eq!(applied, 0, "{op} must reject corrupt tenant identity");
            assert_eq!(applier.get_expected_sequence_id(), 1);
            assert!(state.objects.contains_key("default\0k1"));
        }
    }

    #[test]
    fn test_remove_like_canonicalizes_legacy_unscoped_key_to_default_tenant() {
        for op in ["remove", "put_revoke"] {
            let state = make_state();
            insert_default_tenant_object(&state, "legacy-key");
            state
                .processing_keys
                .insert("default\0legacy-key".to_string(), ());
            let applier = OpLogApplier::new(state.clone());
            let payload = serde_json::json!({
                "op": op,
                "key": "legacy-key",
            })
            .to_string();

            let applied = applier.apply_op_log_entries(&[OpLogRecord {
                seq: 1,
                producer_view_version: 1,
                payload,
            }]);

            assert_eq!(applied, 1);
            assert_eq!(applier.get_expected_sequence_id(), 2);
            assert!(!state.objects.contains_key("default\0legacy-key"));
            assert!(!state.processing_keys.contains_key("default\0legacy-key"));
        }
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
    fn concurrent_batches_apply_expected_sequence_once() {
        let applier = Arc::new(OpLogApplier::new(make_state()));
        let start = Arc::new(std::sync::Barrier::new(3));
        let record = OpLogRecord {
            seq: 1,
            producer_view_version: 1,
            payload: r#"{"op":"put_start","key":"k1"}"#.to_string(),
        };

        let workers = (0..2)
            .map(|_| {
                let applier = applier.clone();
                let start = start.clone();
                let record = record.clone();
                std::thread::spawn(move || {
                    start.wait();
                    applier.apply_op_log_entries(&[record])
                })
            })
            .collect::<Vec<_>>();
        start.wait();

        let applied = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum::<usize>();
        assert_eq!(applied, 1);
        assert_eq!(applier.get_expected_sequence_id(), 2);
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

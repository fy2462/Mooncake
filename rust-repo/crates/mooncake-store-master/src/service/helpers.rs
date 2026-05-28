use crate::http_metadata::MetadataState;
use crate::metrics;
use mooncake_store_core::{ReplicaDescriptor, ReplicaType, ReplicateConfig};
use chrono::Utc;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use tonic::Status;
use uuid::Uuid;

use super::background_ops::{clear_offloading_task, clear_promotion_task};
use super::state::{ClientEntry, MasterState, ObjectEntry};

pub(crate) fn bump_view_version(state: &MasterState) -> i64 {
    state.view_version.fetch_add(1, Ordering::Relaxed) + 1
}

pub(crate) fn host_from_segment_name(name: &str) -> String {
    name.split(':').next().unwrap_or(name).to_string()
}

pub(crate) fn port_from_segment_name(name: &str) -> u16 {
    name.split(':')
        .nth(1)
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0)
}

fn merge_addresses(
    existing: &[String],
    new_addresses: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut merged = existing.to_vec();
    for address in new_addresses {
        if !address.is_empty() && !merged.iter().any(|v| v == &address) {
            merged.push(address);
        }
    }
    merged
}

pub(crate) fn upsert_client_addresses(
    state: &MasterState,
    client_id: Uuid,
    addresses: Vec<String>,
) {
    let now = Utc::now();
    if let Some(mut entry) = state.clients.get_mut(&client_id) {
        entry.info.addresses = merge_addresses(&entry.info.addresses, addresses);
        entry.info.last_seen = now;
        entry.last_ping = SystemTime::now();
        return;
    }

    state.clients.insert(
        client_id,
        ClientEntry {
            info: mooncake_store_core::ClientInfo {
                id: client_id,
                addresses,
                segments: vec![],
                last_seen: now,
            },
            last_ping: SystemTime::now(),
        },
    );
}

pub(crate) fn sync_client_segments(state: &MasterState, client_id: Uuid) {
    let segments = state
        .segments
        .iter()
        .filter(|entry| entry.client_id == client_id)
        .map(|entry| entry.segment.clone())
        .collect::<Vec<_>>();
    if let Some(mut client) = state.clients.get_mut(&client_id) {
        client.info.segments = segments;
        client.info.last_seen = Utc::now();
    }
}

pub(crate) async fn register_metadata_segments(
    metadata_state: &MetadataState,
    segment_names: &[String],
) {
    for segment_name in segment_names {
        let host = host_from_segment_name(segment_name);
        if host.is_empty() {
            continue;
        }
        metadata_state
            .register_node(host, port_from_segment_name(segment_name), vec![])
            .await;
    }
}

pub(crate) fn client_id_by_segment_name(state: &MasterState, segment_name: &str) -> Option<Uuid> {
    state
        .segments
        .iter()
        .find(|entry| entry.segment.name == segment_name)
        .map(|entry| entry.client_id)
}

pub(crate) fn object_owner_client_id(state: &MasterState, object: &ObjectEntry) -> Option<Uuid> {
    object.replicas.iter().find_map(|replica| {
        replica
            .holder_client_id
            .or_else(|| client_id_by_replica_segment_name(state, &replica.segment_name))
    })
}

pub(crate) fn preferred_nof_segment_names(
    state: &MasterState,
    replicas: &[ReplicaDescriptor],
) -> Vec<String> {
    let mut names = Vec::new();
    for replica in replicas {
        if replica.replica_type != ReplicaType::Memory {
            continue;
        }
        let host = host_from_segment_name(&replica.segment_name);
        for segment in state.nof_segments.iter() {
            if host_from_segment_name(&segment.segment.name) == host
                && !names.iter().any(|name| name == &segment.segment.name)
            {
                names.push(segment.segment.name.clone());
            }
        }
    }
    names
}

pub(crate) fn client_id_by_nof_segment_name(
    state: &MasterState,
    segment_name: &str,
) -> Option<Uuid> {
    state
        .nof_segments
        .iter()
        .find(|entry| entry.segment.name == segment_name)
        .map(|entry| entry.segment.client_id)
}

pub(crate) fn client_id_by_replica_segment_name(
    state: &MasterState,
    segment_name: &str,
) -> Option<Uuid> {
    client_id_by_segment_name(state, segment_name)
        .or_else(|| client_id_by_nof_segment_name(state, segment_name))
}

pub(crate) fn unmount_segment_owned(
    state: &MasterState,
    segment_id: Uuid,
    client_id: Uuid,
) -> bool {
    let owned = state
        .segments
        .get(&segment_id)
        .map(|entry| entry.client_id == client_id)
        .unwrap_or(false);
    if !owned {
        return false;
    }

    state.segments.remove(&segment_id);
    state.allocator.write().remove_segment(&segment_id);
    sync_client_segments(state, client_id);
    metrics::SEGMENT_COUNT.set(state.segments.len() as i64);
    true
}

pub(crate) fn unmount_nof_segment_owned(state: &MasterState, segment_id: Uuid, client_id: Uuid) -> bool {
    let owned = state
        .nof_segments
        .get(&segment_id)
        .map(|entry| entry.segment.client_id == client_id)
        .unwrap_or(false);
    if !owned {
        return false;
    }

    state.nof_segments.remove(&segment_id);
    state.nof_allocator.write().remove_segment(&segment_id);
    true
}

pub(crate) fn addresses_for_client(state: &MasterState, client_id: Uuid) -> Vec<String> {
    if let Some(entry) = state.clients.get(&client_id) {
        if !entry.info.addresses.is_empty() {
            return entry.info.addresses.clone();
        }
    }

    let mut addresses = Vec::new();
    for segment in state.segments.iter() {
        if segment.client_id == client_id {
            let host = host_from_segment_name(&segment.segment.name);
            if !host.is_empty() && !addresses.iter().any(|v| v == &host) {
                addresses.push(host);
            }
        }
    }
    addresses
}

pub(crate) fn sync_segment_usage(state: &MasterState, segment_ids: impl IntoIterator<Item = Uuid>) {
    let allocator = state.allocator.read();
    for segment_id in segment_ids {
        let Some(used) = allocator.used_bytes(&segment_id) else {
            continue;
        };
        if let Some(mut entry) = state.segments.get_mut(&segment_id) {
            entry.used = used;
        }
    }
}

pub(crate) fn sync_nof_segment_usage(
    state: &MasterState,
    segment_ids: impl IntoIterator<Item = Uuid>,
) {
    let allocator = state.nof_allocator.read();
    for segment_id in segment_ids {
        let Some(used) = allocator.used_bytes(&segment_id) else {
            continue;
        };
        if let Some(mut entry) = state.nof_segments.get_mut(&segment_id) {
            entry.used = used;
        }
    }
}

pub(crate) fn release_replicas(state: &MasterState, replicas: &[ReplicaDescriptor]) {
    let memory = replicas
        .iter()
        .filter(|r| r.replica_type == ReplicaType::Memory)
        .cloned()
        .collect::<Vec<_>>();
    if !memory.is_empty() {
        let segment_ids = memory.iter().map(|r| r.segment_id).collect::<Vec<_>>();
        state.allocator.write().release(&memory);
        sync_segment_usage(state, segment_ids);
    }

    let nof = replicas
        .iter()
        .filter(|r| r.replica_type == ReplicaType::NoFSsd)
        .cloned()
        .collect::<Vec<_>>();
    if !nof.is_empty() {
        let segment_ids = nof.iter().map(|r| r.segment_id).collect::<Vec<_>>();
        state.nof_allocator.write().release(&nof);
        sync_nof_segment_usage(state, segment_ids);
    }
}

pub(crate) fn allocate_nof_replicas(
    state: &MasterState,
    key: &str,
    size: u64,
    count: usize,
    preferred_segment_names: &[String],
) -> Result<Vec<ReplicaDescriptor>, Status> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if state.nof_segments.is_empty() {
        return Err(Status::failed_precondition("no NoF segments mounted"));
    }

    let mut replicas = Vec::with_capacity(count);
    let mut used_names = Vec::new();
    for idx in 0..count {
        let preferred_segment = preferred_segment_names
            .iter()
            .find(|name| !used_names.iter().any(|used| used == *name))
            .cloned()
            .unwrap_or_default();
        let config = ReplicateConfig {
            preferred_segment,
            replica_num: 1,
            ..Default::default()
        };
        let allocated = state
            .nof_allocator
            .write()
            .allocate(key, size, 1, &config)
            .into_iter()
            .next()
            .ok_or(Status::resource_exhausted("no available NoF segment"))?;
        used_names.push(allocated.segment_name.clone());
        let mut replica = allocated;
        replica.replica_type = ReplicaType::NoFSsd;
        replicas.push(replica);
        if idx + 1 >= state.nof_segments.len() {
            break;
        }
    }
    sync_nof_segment_usage(state, replicas.iter().map(|r| r.segment_id));
    Ok(replicas)
}

pub(crate) fn memory_usage_ratio(state: &MasterState) -> f64 {
    let (total_bytes, used_bytes) = state.allocator.read().usage_totals();
    if total_bytes == 0 {
        return 0.0;
    }
    used_bytes as f64 / total_bytes as f64
}

pub(crate) fn release_replicas_scheduled(state: &MasterState, replicas: Vec<ReplicaDescriptor>) {
    release_replicas(state, &replicas);
}

/// Helper: release replicas and clear associated offloading/promotion tasks.
pub(crate) fn release_object_replicas(
    state: &MasterState,
    key: &str,
    replicas: &[ReplicaDescriptor],
) {
    if replicas.is_empty() {
        return;
    }
    clear_offloading_task(state, key);
    clear_promotion_task(state, key);
    release_replicas(state, replicas);
}

/// Returns true if the lease on an object has expired (or never set).
pub(crate) fn is_lease_expired(entry: &ObjectEntry) -> bool {
    entry
        .lease_timeout
        .map_or(true, |timeout| timeout <= SystemTime::now())
}

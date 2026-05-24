use crate::http_metadata::MetadataState;
use crate::metrics;
use chrono::Utc;
use std::time::SystemTime;
use uuid::Uuid;

use super::state::{ClientEntry, MasterState};

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
        .filter(|entry| entry.segment.client_id == client_id)
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
        .map(|entry| entry.segment.client_id)
}

pub(crate) fn unmount_segment_owned(
    state: &MasterState,
    segment_id: Uuid,
    client_id: Uuid,
) -> bool {
    let owned = state
        .segments
        .get(&segment_id)
        .map(|entry| entry.segment.client_id == client_id)
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

pub(crate) fn addresses_for_client(state: &MasterState, client_id: Uuid) -> Vec<String> {
    if let Some(entry) = state.clients.get(&client_id) {
        if !entry.info.addresses.is_empty() {
            return entry.info.addresses.clone();
        }
    }

    let mut addresses = Vec::new();
    for segment in state.segments.iter() {
        if segment.segment.client_id == client_id {
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
            entry.segment.used = used;
        }
    }
}

pub(crate) fn memory_usage_ratio(state: &MasterState) -> f64 {
    let (total_bytes, used_bytes) = state.allocator.read().usage_totals();
    if total_bytes == 0 {
        return 0.0;
    }
    used_bytes as f64 / total_bytes as f64
}

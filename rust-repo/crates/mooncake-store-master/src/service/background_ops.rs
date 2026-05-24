use crate::eviction::EvictionManager;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Instant;
use uuid::Uuid;

use super::helpers::{client_id_by_segment_name, memory_usage_ratio, sync_segment_usage};
use super::state::{MasterState, ObjectEntry, OffloadingTaskEntry, PromotionTaskEntry};

pub(crate) fn release_staged_promotion_replica(
    state: &MasterState,
    key: &str,
    segment_id: Uuid,
    offset: u64,
) {
    if let Some(mut object) = state.objects.get_mut(key) {
        let mut removed = Vec::new();
        object.replicas.retain(|replica| {
            let matched = replica.replica_type == ReplicaType::Memory
                && replica.segment_id == segment_id
                && replica.offset == offset
                && replica.status == ReplicaStatus::Allocating;
            if matched {
                removed.push(replica.clone());
            }
            !matched
        });
        drop(object);
        if !removed.is_empty() {
            state.allocator.write().release(&removed);
            sync_segment_usage(state, [segment_id]);
        }
    }
}

pub(crate) fn reap_expired_background_tasks(state: &MasterState, now: Instant) {
    let ttl = state.runtime_config.put_start_release_timeout;

    let expired_offloads = state
        .offloading_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_offloads {
        clear_offloading_task(state, &key);
    }

    let expired_promotions = state
        .promotion_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_promotions {
        if let Some(task) = clear_promotion_task(state, &key) {
            if let (Some(segment_id), Some(offset)) = (task.staged_segment_id, task.staged_offset) {
                release_staged_promotion_replica(state, &key, segment_id, offset);
            }
            if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.holder_id) {
                local_disk.promotion_objects.remove(&key);
            }
        }
    }
}

pub(crate) fn push_offloading_queue(state: &MasterState, client_id: Uuid, key: &str, size: u64) {
    let mut local_disk = match state.local_disk_segments.get_mut(&client_id) {
        Some(entry) => entry,
        None => return,
    };
    if !local_disk.enable_offloading {
        return;
    }
    local_disk
        .offloading_objects
        .insert(key.to_string(), size as i64);
    state.offloading_tasks.insert(
        key.to_string(),
        OffloadingTaskEntry {
            client_id,
            start_time: Instant::now(),
        },
    );
}

pub(crate) fn clear_offloading_task(state: &MasterState, key: &str) {
    if let Some((_, task)) = state.offloading_tasks.remove(key) {
        if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.client_id) {
            local_disk.offloading_objects.remove(key);
        }
    }
}

pub(crate) fn clear_promotion_task(state: &MasterState, key: &str) -> Option<PromotionTaskEntry> {
    let removed = state.promotion_tasks.remove(key).map(|(_, task)| task);
    if removed.is_some() {
        state
            .promotion_in_flight
            .fetch_sub(1, AtomicOrdering::Relaxed);
    }
    removed
}

fn evict_memory_replicas(object: &mut ObjectEntry) -> Vec<ReplicaDescriptor> {
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let should_remove = replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete;
        if should_remove {
            removed.push(replica.clone());
        }
        !should_remove
    });
    removed
}

fn evict_redundant_memory_replicas(object: &mut ObjectEntry) -> Vec<ReplicaDescriptor> {
    let total_memory = object
        .replicas
        .iter()
        .filter(|replica| {
            replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete
        })
        .count();
    if total_memory <= 1 {
        return Vec::new();
    }

    let mut kept_one = false;
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let is_memory_complete = replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete;
        if !is_memory_complete {
            return true;
        }
        if !kept_one {
            kept_one = true;
            return true;
        }
        removed.push(replica.clone());
        false
    });
    removed
}

pub(crate) fn run_eviction_cycle(state: &MasterState, target_count: usize) -> Vec<String> {
    let manager = EvictionManager::new(
        state.runtime_config.soft_pin_ttl,
        state.runtime_config.lease_ttl,
    );
    let candidate_data = state
        .objects
        .iter()
        .map(|entry| {
            (
                entry.key().clone(),
                entry.replicas.clone(),
                entry.soft_pinned,
                entry.last_access,
            )
        })
        .collect::<Vec<_>>();
    let candidate_refs = candidate_data
        .iter()
        .map(|(key, replicas, soft_pinned, last_access)| {
            (
                key.as_str(),
                replicas.as_slice(),
                *soft_pinned,
                *last_access,
            )
        })
        .collect::<Vec<_>>();
    let selected = manager.select_for_eviction(&candidate_refs, target_count);

    let mut evicted = Vec::new();
    for key in selected {
        if let Some(mut object) = state.objects.get_mut(&key) {
            let has_local_disk = object
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::LocalDisk);
            let owner_client = object
                .replicas
                .iter()
                .find(|replica| replica.replica_type == ReplicaType::Memory)
                .and_then(|replica| client_id_by_segment_name(state, &replica.segment_name));

            let should_offload =
                state.runtime_config.offload_on_evict && !has_local_disk && owner_client.is_some();
            if should_offload {
                push_offloading_queue(state, owner_client.unwrap(), &key, object.size);
                if !state.offloading_tasks.contains_key(&key)
                    && !state.runtime_config.offload_force_evict
                {
                    continue;
                }
            }

            let removed =
                if should_offload && !has_local_disk && state.offloading_tasks.contains_key(&key) {
                    evict_redundant_memory_replicas(&mut object)
                } else {
                    evict_memory_replicas(&mut object)
                };
            let became_empty = object.replicas.is_empty();
            drop(object);
            if !removed.is_empty() {
                let segment_ids = removed
                    .iter()
                    .map(|replica| replica.segment_id)
                    .collect::<Vec<_>>();
                state.allocator.write().release(&removed);
                sync_segment_usage(state, segment_ids);
                evicted.push(key.clone());
            }
            if became_empty {
                state.objects.remove(&key);
                clear_offloading_task(state, &key);
                clear_promotion_task(state, &key);
            }
        }
    }

    evicted
}

fn automatic_eviction_target_count(state: &MasterState) -> usize {
    let used_ratio = memory_usage_ratio(state);
    if used_ratio <= state.runtime_config.eviction_high_watermark_ratio {
        return 0;
    }

    let object_count = state.objects.len();
    if object_count == 0 {
        return 0;
    }

    let evict_ratio_target = state.runtime_config.eviction_ratio.max(
        used_ratio - state.runtime_config.eviction_high_watermark_ratio
            + state.runtime_config.eviction_ratio,
    );
    let target = (object_count as f64 * evict_ratio_target).ceil() as usize;
    target.max(1)
}

pub(crate) fn run_automatic_eviction_once(state: &MasterState) -> Vec<String> {
    let target_count = automatic_eviction_target_count(state);
    if target_count == 0 {
        return Vec::new();
    }
    run_eviction_cycle(state, target_count)
}

pub(crate) fn try_push_promotion_queue(state: &MasterState, key: &str) {
    if !state.runtime_config.promotion_on_hit {
        return;
    }

    let current_freq = {
        let mut entry = state
            .promotion_access_counts
            .entry(key.to_string())
            .or_insert(0);
        if *entry < u8::MAX {
            *entry += 1;
        }
        *entry
    };
    let threshold = state.runtime_config.promotion_admission_threshold.max(1);
    if current_freq < threshold {
        return;
    }

    let (holder_id, object_size) = match state.objects.get(key) {
        Some(object) => {
            let any_memory = object.replicas.iter().any(|replica| {
                replica.replica_type == ReplicaType::Memory
                    && replica.status == ReplicaStatus::Complete
            });
            if any_memory {
                return;
            }
            let Some(local_disk) = object.replicas.iter().find(|replica| {
                replica.replica_type == ReplicaType::LocalDisk
                    && replica.status == ReplicaStatus::Complete
                    && replica.holder_client_id.is_some()
            }) else {
                return;
            };
            (local_disk.holder_client_id.unwrap(), local_disk.size)
        }
        None => return,
    };

    if state.promotion_tasks.contains_key(key) {
        return;
    }
    if !state.local_disk_segments.contains_key(&holder_id) {
        return;
    }
    if state
        .promotion_in_flight
        .fetch_add(1, AtomicOrdering::Relaxed)
        >= state.runtime_config.promotion_queue_limit
    {
        state
            .promotion_in_flight
            .fetch_sub(1, AtomicOrdering::Relaxed);
        return;
    }
    if let Some(mut local_disk) = state.local_disk_segments.get_mut(&holder_id) {
        local_disk
            .promotion_objects
            .entry(key.to_string())
            .or_insert(object_size as i64);
    }
    state.promotion_tasks.insert(
        key.to_string(),
        PromotionTaskEntry {
            holder_id,
            object_size,
            staged_segment_id: None,
            staged_offset: None,
            start_time: Instant::now(),
        },
    );
}

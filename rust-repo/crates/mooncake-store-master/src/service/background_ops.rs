//! Master background operations shared by workers and gRPC handlers.
//!
//! This module owns offload/promotion queue bookkeeping and memory eviction.
//! Reaper and drain processing live in sibling submodules.

use crate::TenantId;
use crate::eviction::EvictionManager;
use crate::ha::HaError;
use crate::metrics;
use crate::proto;
use dashmap::mapref::entry::Entry;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, TaskStatus};
use std::collections::HashSet;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Instant, SystemTime};
use uuid::Uuid;

use super::helpers::{
    account_removed_object_quota, choose_drain_target_segment, client_id_by_exact_replica_segment,
    clone_object_for_mutation, completed_memory_quota_charge, default_drain_target_segments,
    has_pending_task_capacity, is_lease_expired, memory_usage_ratio, nof_usage_ratio,
    release_committed_memory_quota_charge, release_replicas, replica_is_routable,
    requested_memory_quota_charge, sync_cache_total_accounting, unique_task_id,
};
use super::state::{
    MasterState, ObjectEntry, OffloadingTaskEntry, PromotionCandidate, PromotionCandidateReason,
    PromotionTaskEntry,
};

const PROMOTION_CANDIDATE_LIMIT: usize = 50_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TenantQuotaEvictionResult {
    pub freed_bytes: u64,
    pub evicted_objects: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromotionQueueResult {
    Queued,
    Disabled,
    FrequencyRejected,
    WatermarkRejected,
    QueueCapRejected,
    AlreadyInFlight,
    MemoryReplicaPresent,
    NoLocalDiskSource,
    NotFound,
    PushFailed,
}

impl PromotionQueueResult {
    pub(crate) fn is_transient(self) -> bool {
        matches!(
            self,
            Self::WatermarkRejected | Self::QueueCapRejected | Self::PushFailed
        )
    }
}

fn decrement_candidate_count(state: &MasterState) {
    let _ = state.promotion_candidate_count.fetch_update(
        AtomicOrdering::Relaxed,
        AtomicOrdering::Relaxed,
        |count| count.checked_sub(1),
    );
}

pub(crate) fn erase_promotion_candidate(state: &MasterState, key: &str) {
    if state.promotion_candidates.remove(key).is_some() {
        decrement_candidate_count(state);
    }
}

fn record_or_refresh_candidate(
    state: &MasterState,
    key: &str,
    sketch_score: u8,
    reason: PromotionCandidateReason,
    last_error_code: Option<i32>,
) {
    let now = Instant::now();
    if let Some(mut candidate) = state.promotion_candidates.get_mut(key) {
        candidate.sketch_score = candidate.sketch_score.max(sketch_score);
        candidate.last_seen = now;
        candidate.retry_after = now;
        candidate.last_reason = reason;
        candidate.last_error_code = last_error_code;
        candidate.retry_count = 0;
        return;
    }

    let reserved = state
        .promotion_candidate_count
        .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |count| {
            (count < PROMOTION_CANDIDATE_LIMIT).then_some(count + 1)
        })
        .is_ok();
    if !reserved {
        metrics::PROMOTION_CANDIDATE_DROPPED_LIMIT.inc();
        return;
    }

    match state.promotion_candidates.entry(key.to_string()) {
        Entry::Vacant(entry) => {
            entry.insert(PromotionCandidate {
                sketch_score,
                first_seen: now,
                last_seen: now,
                retry_after: now,
                last_reason: reason,
                last_error_code,
                retry_count: 0,
            });
            metrics::PROMOTION_CANDIDATE_RECORDED.inc();
        }
        Entry::Occupied(mut entry) => {
            decrement_candidate_count(state);
            let candidate = entry.get_mut();
            candidate.sketch_score = candidate.sketch_score.max(sketch_score);
            candidate.last_seen = now;
            candidate.retry_after = now;
            candidate.last_reason = reason;
            candidate.last_error_code = last_error_code;
            candidate.retry_count = 0;
        }
    }
}

mod background_drain;
mod background_reaper;
mod promotion_retry;
pub(crate) use background_drain::process_drain_jobs;
pub(crate) use background_reaper::{
    detach_staged_promotion_replica, reap_expired_background_tasks,
};
pub(crate) use promotion_retry::{
    run_default_promotion_candidate_retry, run_promotion_candidate_retry,
};

fn same_replica_location(a: &ReplicaDescriptor, b: &ReplicaDescriptor) -> bool {
    a.segment_id == b.segment_id && a.offset == b.offset && a.replica_type == b.replica_type
}

fn dec_refcnt_for_replica(state: &MasterState, key: &str, source: &ReplicaDescriptor) {
    if let Some(mut object) = state.objects.get_mut(key) {
        if let Some(replica) = object
            .replicas
            .iter_mut()
            .find(|replica| same_replica_location(replica, source))
        {
            replica.dec_refcnt();
        }
    }
}

fn inc_refcnt_for_replica(state: &MasterState, key: &str, source: &ReplicaDescriptor) -> bool {
    let Some(mut object) = state.objects.get_mut(key) else {
        return false;
    };
    let Some(replica) = object
        .replicas
        .iter_mut()
        .find(|replica| same_replica_location(replica, source))
    else {
        return false;
    };
    replica.inc_refcnt();
    true
}

/// 将对象推入下沉队列，触发从内存到本地磁盘的数据下沉。
/// 仅在 client 启用了 offload 且对象不在处理中时才入队。
///
/// Push an object into the offload queue, triggering memory→local-disk data offload.
/// Only enqueues when the client has offload enabled and the object is not already being processed.
pub(crate) fn push_offloading_queue(state: &MasterState, client_id: Uuid, key: &str, size: u64) {
    if !state.runtime_config.enable_offload {
        return;
    }
    if state.offloading_tasks.contains_key(key) {
        return;
    }
    let source = {
        let Some(object) = state.objects.get(key) else {
            return;
        };
        let Some(source) = object
            .replicas
            .iter()
            .find(|replica| {
                replica.replica_type == ReplicaType::Memory
                    && replica.status == ReplicaStatus::Complete
                    && replica.handle_valid
                    && client_id_by_exact_replica_segment(state, replica) == Some(client_id)
            })
            .cloned()
        else {
            return;
        };
        source
    };
    let Some(storage_id) = state
        .local_disk_client_sessions
        .get(&client_id)
        .map(|entry| *entry)
    else {
        return;
    };
    let can_queue_offload = state
        .local_disk_segments
        .get(&storage_id)
        .is_some_and(|entry| {
            entry.active_client_id == Some(client_id)
                && entry.recovery_complete
                && entry.enable_offloading
                && entry.offloading_objects.len() < state.runtime_config.offloading_queue_limit
        });
    if !can_queue_offload {
        return;
    }
    if !inc_refcnt_for_replica(state, key, &source) {
        return;
    }
    let generation_id = Uuid::new_v4();
    match state.offloading_tasks.entry(key.to_string()) {
        Entry::Vacant(entry) => {
            entry.insert(OffloadingTaskEntry {
                client_id,
                storage_id,
                generation_id,
                source: source.clone(),
                start_time: Instant::now(),
            });
        }
        Entry::Occupied(_) => {
            dec_refcnt_for_replica(state, key, &source);
            return;
        }
    }
    let queued = match state.local_disk_segments.get_mut(&storage_id) {
        Some(mut local_disk) => {
            if local_disk.active_client_id == Some(client_id)
                && local_disk.recovery_complete
                && local_disk.enable_offloading
                && local_disk.offloading_objects.len() < state.runtime_config.offloading_queue_limit
            {
                local_disk
                    .offloading_objects
                    .insert(key.to_string(), size as i64);
                true
            } else {
                false
            }
        }
        _ => false,
    };
    if !queued {
        clear_offloading_task(state, key);
        return;
    }
}

/// 清理指定 key 的下沉任务，同时从 client 的 offloading_objects 中移除。
/// Clean up the offload task for a given key, also removing it from the client's offloading_objects.
pub(crate) fn clear_offloading_task(state: &MasterState, key: &str) {
    if let Some((_, task)) = state.offloading_tasks.remove(key) {
        dec_refcnt_for_replica(state, key, &task.source);
        if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.storage_id) {
            local_disk.offloading_objects.remove(key);
        }
    }
}

/// 清理指定 key 的晋升任务，并将全局在途晋升计数器减 1。
/// 返回被移除的任务条目，供调用方进一步清理暂存副本。
///
/// Clean up the promotion task for a given key, decrementing the global in-flight counter.
/// Returns the removed task entry so callers can further clean up staged replicas.
pub(crate) fn clear_promotion_task(state: &MasterState, key: &str) -> Option<PromotionTaskEntry> {
    let removed = state.promotion_tasks.remove(key).map(|(_, task)| task);
    if let Some(task) = &removed {
        dec_refcnt_for_replica(state, key, &task.source);
        state
            .promotion_in_flight
            .fetch_sub(1, AtomicOrdering::Relaxed);
    }
    removed
}

fn has_quota_evictable_memory_replica(object: &ObjectEntry) -> bool {
    object.replicas.iter().any(|replica| {
        replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete
            && !replica.is_busy()
    })
}

fn evict_quota_memory_replicas(object: &mut ObjectEntry) -> Vec<ReplicaDescriptor> {
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let should_remove = replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete
            && !replica.is_busy();
        if should_remove {
            removed.push(replica.clone());
        }
        !should_remove
    });
    removed
}

fn has_active_soft_pin(object: &ObjectEntry, now: SystemTime) -> bool {
    object.soft_pin_timeout.is_some_and(|timeout| now < timeout)
}

fn tenant_quota_candidate_keys(
    state: &MasterState,
    tenant_id: &TenantId,
    protected_key: Option<&str>,
    allow_soft_pinned: bool,
    now: SystemTime,
) -> Vec<String> {
    let mut candidates = state
        .objects
        .iter()
        .filter(|entry| {
            let key = entry.key();
            let object = entry.value();
            protected_key != Some(key.as_str())
                && object.tenant_id == *tenant_id
                && !state.processing_keys.contains_key(key)
                && !state.replication_tasks.contains_key(key)
                && !object.hard_pinned
                && is_lease_expired(object)
                && (allow_soft_pinned || !has_active_soft_pin(object, now))
                && has_quota_evictable_memory_replica(object)
        })
        .map(|entry| (entry.key().clone(), entry.last_access))
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(_, last_access)| *last_access);
    candidates.into_iter().map(|(key, _)| key).collect()
}

fn tenant_quota_group_keys(
    state: &MasterState,
    tenant_id: &TenantId,
    key: &str,
) -> (String, Vec<String>) {
    let Some(object) = state.objects.get(key) else {
        return (String::new(), Vec::new());
    };
    let group_id = object.group_id.clone();
    drop(object);
    if group_id.is_empty() {
        return (String::new(), vec![key.to_string()]);
    }
    let mut keys = state
        .objects
        .iter()
        .filter(|entry| {
            entry.tenant_id == *tenant_id && entry.group_id.as_str() == group_id.as_str()
        })
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    keys.sort();
    (group_id, keys)
}

fn evict_memory_pressure_object(
    state: &MasterState,
    tenant_id: &TenantId,
    key: &str,
    allow_soft_pinned: bool,
    now: SystemTime,
    offload_cap: usize,
    offload_enqueued: &mut usize,
    operation: &str,
) -> Result<u64, HaError> {
    let (has_local_disk, owner_client, size) = {
        let Some(object) = state.objects.get(key) else {
            return Ok(0);
        };
        if object.tenant_id != *tenant_id
            || state.processing_keys.contains_key(key)
            || state.replication_tasks.contains_key(key)
            || object.hard_pinned
            || !is_lease_expired(&object)
            || (!allow_soft_pinned && has_active_soft_pin(&object, now))
            || !has_quota_evictable_memory_replica(&object)
        {
            return Ok(0);
        }
        (
            object
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::LocalDisk),
            object
                .replicas
                .iter()
                .find(|replica| {
                    replica.replica_type == ReplicaType::Memory
                        && replica.status == ReplicaStatus::Complete
                        && replica.handle_valid
                        && !replica.is_busy()
                })
                .and_then(|replica| client_id_by_exact_replica_segment(state, replica)),
            object.size,
        )
    };

    let should_offload = state.runtime_config.offload_on_evict && !has_local_disk;
    let mut queued_here = false;
    if should_offload {
        let cap_reached = *offload_enqueued >= offload_cap;
        if cap_reached && !state.runtime_config.offload_force_evict {
            return Ok(0);
        }
        let force_without_offload = state.runtime_config.offload_force_evict && cap_reached;
        if !force_without_offload {
            if let Some(owner_client) = owner_client {
                let had_task = state.offloading_tasks.contains_key(key);
                push_offloading_queue(state, owner_client, key, size);
                queued_here = !had_task && state.offloading_tasks.contains_key(key);
                if queued_here {
                    *offload_enqueued = (*offload_enqueued).saturating_add(1);
                }
            }
            if !state.offloading_tasks.contains_key(key)
                && !state.runtime_config.offload_force_evict
            {
                return Ok(0);
            }
        }
    }

    let Some(mut object) = state.objects.get_mut(key) else {
        if queued_here {
            clear_offloading_task(state, key);
        }
        return Ok(0);
    };
    let mut projected = clone_object_for_mutation(&object);
    // A successfully queued offload increments the source refcount first, so
    // this removes only other unreferenced replicas and retains the source
    // until offload completion. Forced/no-offload paths remove every eligible
    // Memory replica.
    let removed = evict_quota_memory_replicas(&mut projected);
    let removed_memory_charge = requested_memory_quota_charge(
        projected.size,
        removed
            .iter()
            .filter(|replica| replica.replica_type == ReplicaType::Memory)
            .count(),
    );
    if removed_memory_charge == 0 {
        return Ok(0);
    }
    if let Err(error) =
        release_committed_memory_quota_charge(state, &mut projected, removed_memory_charge)
    {
        drop(object);
        if queued_here {
            clear_offloading_task(state, key);
        }
        state.fence_after_invariant_failure(
            operation,
            &format!("tenant={tenant_id} key={key} error={error:?}"),
        );
        return Err(HaError::Snapshot(format!(
            "tenant quota accounting invariant failed during {operation}: tenant={tenant_id} key={key}"
        )));
    }
    sync_cache_total_accounting(&mut projected);
    let event_user_key = projected.user_key_for_event(key).to_string();
    let event_tenant_id = projected.tenant_id.clone();
    let event_group_id = projected.group_id.clone();
    let became_empty = projected.replicas.is_empty();
    *object = projected;
    drop(object);

    let removed_object = if became_empty {
        state.objects.remove(key).map(|(_, object)| object)
    } else {
        None
    };
    let mut quota_removal_failed = false;
    if let Some(object) = &removed_object {
        quota_removal_failed = account_removed_object_quota(state, object).is_err();
        for mut entry in state.client_objects.iter_mut() {
            entry.value_mut().remove(key);
        }
        clear_offloading_task(state, key);
        clear_promotion_task(state, key);
    }
    if quota_removal_failed {
        return Err(HaError::Snapshot(format!(
            "tenant quota removal invariant failed during {operation}: tenant={tenant_id} key={key}"
        )));
    }
    state.persist_object_image_or_remove_or_fence(key, operation)?;
    release_replicas(state, &removed).map_err(|status| HaError::Snapshot(status.to_string()))?;
    state.kv_event_publisher.publish_removed(
        &event_user_key,
        "cpu",
        &event_tenant_id,
        &event_group_id,
    );
    Ok(removed_memory_charge)
}

/// Release up to `target_bytes` of committed Memory charge from one tenant.
///
/// This is deliberately invoked only after the caller has released its
/// tenant-scoped mutation guard. The exclusive coordinator barrier keeps group
/// membership, object replicas, allocator usage and quota accounting in one
/// snapshot-consistent mutation epoch without recursively acquiring a
/// colliding key stripe.
pub(crate) fn run_tenant_quota_eviction(
    state: &MasterState,
    tenant_id: &TenantId,
    protected_key: Option<&str>,
    target_bytes: u64,
) -> Result<TenantQuotaEvictionResult, HaError> {
    if !state.runtime_config.enable_tenant_quota || target_bytes == 0 {
        return Ok(TenantQuotaEvictionResult::default());
    }

    let _global_mutation_guard = state.key_mutations.lock_snapshot();
    let now = SystemTime::now();
    let offload_cap = if state.runtime_config.offload_on_evict {
        ((state.runtime_config.offloading_queue_limit as f64)
            * state.runtime_config.offload_cap_ratio)
            .floor() as usize
    } else {
        0
    };
    let mut offload_enqueued = 0usize;
    let mut result = TenantQuotaEvictionResult::default();
    let passes = if state.runtime_config.allow_evict_soft_pinned_objects {
        [Some(false), Some(true)]
    } else {
        [Some(false), None]
    };

    for allow_soft_pinned in passes.into_iter().flatten() {
        if result.freed_bytes >= target_bytes {
            break;
        }
        let candidates =
            tenant_quota_candidate_keys(state, tenant_id, protected_key, allow_soft_pinned, now);
        let mut processed_groups = HashSet::new();
        for key in candidates {
            if result.freed_bytes >= target_bytes {
                break;
            }
            let (group_id, group_keys) = tenant_quota_group_keys(state, tenant_id, &key);
            if group_keys.is_empty() {
                continue;
            }
            if !group_id.is_empty() && !processed_groups.insert(group_id) {
                continue;
            }
            // C++ grouped eviction protects the whole group while any member
            // has a live lease, even if the candidate member itself expired.
            if group_keys.iter().any(|member_key| {
                state
                    .objects
                    .get(member_key)
                    .is_some_and(|object| !is_lease_expired(&object))
            }) {
                continue;
            }
            for member_key in &group_keys {
                if protected_key == Some(member_key.as_str()) {
                    continue;
                }
                let freed = evict_memory_pressure_object(
                    state,
                    tenant_id,
                    member_key,
                    allow_soft_pinned,
                    now,
                    offload_cap,
                    &mut offload_enqueued,
                    "tenant_quota_eviction",
                )?;
                if freed != 0 {
                    result.freed_bytes = result.freed_bytes.saturating_add(freed);
                    result.evicted_objects = result.evicted_objects.saturating_add(1);
                }
            }
        }
    }
    Ok(result)
}

/// 执行一轮驱逐循环，使用 LRU 策略选出 target_count 个候选对象。
/// 已有 LocalDisk 副本时直接释放 Memory；没有磁盘副本且启用
/// offload-on-evict 时先固定一个 Memory source 并排队下沉。
///
/// Execute one eviction cycle using LRU strategy to select `target_count` candidate objects.
/// Existing LocalDisk replicas allow immediate Memory release. Otherwise,
/// offload-on-evict pins one Memory source before reclaiming redundant copies.
pub(crate) fn run_eviction_cycle(state: &MasterState, target_count: usize) -> Vec<String> {
    if target_count == 0 {
        return Vec::new();
    }
    let _global_mutation_guard = state.key_mutations.lock_snapshot();
    let manager = EvictionManager::new(
        state.runtime_config.soft_pin_ttl,
        state.runtime_config.lease_ttl,
    );
    let mut evicted = Vec::new();
    let offload_cap = if state.runtime_config.offload_on_evict {
        ((state.runtime_config.offloading_queue_limit as f64)
            * state.runtime_config.offload_cap_ratio)
            .floor() as usize
    } else {
        0
    };
    let mut offload_enqueued = 0usize;
    let passes = if state.runtime_config.allow_evict_soft_pinned_objects {
        [Some(false), Some(true)]
    } else {
        [Some(false), None]
    };
    for allow_soft_pinned in passes.into_iter().flatten() {
        if evicted.len() >= target_count {
            break;
        }
        let mut candidates = state
            .objects
            .iter()
            .filter(|entry| {
                let key = entry.key();
                !state.replication_tasks.contains_key(key)
                    && !state.processing_keys.contains_key(key)
                    && has_quota_evictable_memory_replica(entry.value())
            })
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.soft_pin_timeout,
                    entry.hard_pinned,
                    entry.lease_timeout,
                    entry.last_access,
                )
            })
            .collect::<Vec<_>>();
        // Select the full ordered eligibility set. Revalidation, grouped live
        // leases or offload deferral may skip early candidates; keep scanning
        // until the requested number of objects is actually evicted.
        let candidate_count = candidates.len();
        let selected = manager.select_for_eviction_with_lease_timeout_policy(
            &mut candidates,
            candidate_count,
            allow_soft_pinned,
        );
        let mut processed_groups = HashSet::new();
        for key in selected {
            if evicted.len() >= target_count {
                break;
            }
            let Some(candidate) = state.objects.get(&key) else {
                continue;
            };
            let tenant_id = candidate.tenant_id.clone();
            drop(candidate);
            let (group_id, group_keys) = tenant_quota_group_keys(state, &tenant_id, &key);
            if group_keys.is_empty() {
                continue;
            }
            if !group_id.is_empty()
                && !processed_groups.insert((tenant_id.clone(), group_id.clone()))
            {
                continue;
            }
            if group_keys.iter().any(|member_key| {
                state
                    .objects
                    .get(member_key)
                    .is_some_and(|object| !is_lease_expired(&object))
            }) {
                continue;
            }
            for member_key in group_keys {
                let user_key = state
                    .objects
                    .get(&member_key)
                    .map(|object| object.user_key_for_event(&member_key).to_string());
                let freed = match evict_memory_pressure_object(
                    state,
                    &tenant_id,
                    &member_key,
                    allow_soft_pinned,
                    SystemTime::now(),
                    offload_cap,
                    &mut offload_enqueued,
                    "automatic_eviction",
                ) {
                    Ok(freed) => freed,
                    Err(_) => return evicted,
                };
                if freed != 0 {
                    evicted.push(user_key.unwrap_or(member_key));
                }
            }
        }
    }
    evicted
}

/// 根据当前内存使用比例和配置的水位线，自动计算本轮需要驱逐的对象数量。
/// 内存使用低于高水位线时不触发驱逐；超过水位后按超出比例 + eviction_ratio 计算目标。
///
/// Compute the number of objects to evict this cycle based on current memory usage ratio
/// and the configured watermark. No eviction when memory is below the high watermark;
/// above the watermark, compute the target using excess ratio + eviction_ratio.
fn automatic_eviction_target_count(state: &MasterState) -> usize {
    let used_ratio = memory_usage_ratio(state);
    if used_ratio <= state.runtime_config.eviction_high_watermark_ratio {
        return 0; // 未超过水位线，无需驱逐 / Below watermark, no eviction needed
    }

    let evictable_object_count = state
        .objects
        .iter()
        .filter(|entry| has_quota_evictable_memory_replica(entry.value()))
        .count();
    if evictable_object_count == 0 {
        return 0;
    }

    // 计算驱逐目标比例：eviction_ratio 与 (used_ratio - watermark + eviction_ratio) 取较大值
    let evict_ratio_target = state.runtime_config.eviction_ratio.max(
        used_ratio - state.runtime_config.eviction_high_watermark_ratio
            + state.runtime_config.eviction_ratio,
    );
    let target = (evictable_object_count as f64 * evict_ratio_target).ceil() as usize;
    target.max(1)
}

/// 自动驱逐入口：计算目标数量后执行一轮驱逐。由后台 EvictionWorker 周期调用。
///
/// Automatic eviction entry point: compute target count, then execute one eviction cycle.
/// Called periodically by the background EvictionWorker.
pub(crate) fn run_automatic_eviction_once(state: &MasterState) -> Vec<String> {
    let target_count = automatic_eviction_target_count(state);
    if target_count == 0 {
        return Vec::new();
    }
    run_eviction_cycle(state, target_count)
}

fn has_evictable_nof_replica(object: &ObjectEntry) -> bool {
    object.replicas.iter().any(|replica| {
        replica.replica_type == ReplicaType::NoFSsd
            && replica.status == ReplicaStatus::Complete
            && replica.handle_valid
            && !replica.is_busy()
    })
}

/// Remove complete, unreferenced NoF replicas from up to `target_count`
/// tenant-scoped objects. Hard pins, live leases, soft pins and in-flight
/// object mutations use the same gates as C++ NoFBatchEvict.
pub(crate) fn run_nof_eviction_cycle(state: &MasterState, target_count: usize) -> Vec<String> {
    if target_count == 0 || !state.runtime_config.enable_nof {
        return Vec::new();
    }
    let manager = EvictionManager::new(
        state.runtime_config.soft_pin_ttl,
        state.runtime_config.lease_ttl,
    );
    let mut candidates = state
        .objects
        .iter()
        .filter(|entry| {
            let key = entry.key();
            !state.replication_tasks.contains_key(key)
                && !state.processing_keys.contains_key(key)
                && has_evictable_nof_replica(entry.value())
        })
        .map(|entry| {
            (
                entry.key().clone(),
                entry.soft_pin_timeout,
                entry.hard_pinned,
                entry.lease_timeout,
                entry.last_access,
            )
        })
        .collect::<Vec<_>>();
    let selected =
        manager.select_for_eviction_with_lease_timeout_policy(&mut candidates, target_count, false);

    let mut evicted = Vec::new();
    for key in selected {
        let _mutation_guard = state.key_mutations.lock(&key);
        let Some(mut object) = state.objects.get_mut(&key) else {
            continue;
        };
        let user_key = object.user_key_for_event(&key).to_string();
        let tenant_id = object.tenant_id.clone();
        let group_id = object.group_id.clone();
        let mut removed = Vec::new();
        object.replicas.retain(|replica| {
            let should_remove = replica.replica_type == ReplicaType::NoFSsd
                && replica.status == ReplicaStatus::Complete
                && replica.handle_valid
                && !replica.is_busy();
            if should_remove {
                removed.push(replica.clone());
            }
            !should_remove
        });
        if removed.is_empty() {
            continue;
        }
        sync_cache_total_accounting(&mut object);
        let became_empty = object.replicas.is_empty();
        drop(object);

        let removed_object = if became_empty {
            state.objects.remove(&key).map(|(_, object)| object)
        } else {
            None
        };
        if let Some(object) = &removed_object {
            if account_removed_object_quota(state, object).is_err() {
                return evicted;
            }
            for mut entry in state.client_objects.iter_mut() {
                entry.value_mut().remove(&key);
            }
            clear_offloading_task(state, &key);
            clear_promotion_task(state, &key);
        }
        if state
            .persist_object_image_or_remove_or_fence(&key, "automatic_nof_eviction")
            .is_err()
        {
            return evicted;
        }
        if release_replicas(state, &removed).is_err() {
            return evicted;
        }
        state
            .kv_event_publisher
            .publish_removed(&user_key, "disk", &tenant_id, &group_id);
        evicted.push(user_key);
    }

    if !evicted.is_empty() || state.objects.is_empty() {
        state
            .nof_eviction_requested
            .store(false, AtomicOrdering::Release);
    }
    evicted
}

fn automatic_nof_eviction_target_count(state: &MasterState) -> usize {
    if !state.runtime_config.enable_nof {
        return 0;
    }
    let used_ratio = nof_usage_ratio(state);
    let allocation_pressure = state.nof_eviction_requested.load(AtomicOrdering::Acquire);
    if used_ratio <= state.runtime_config.nof_eviction_high_watermark_ratio
        && !(allocation_pressure && state.runtime_config.nof_eviction_ratio > 0.0)
    {
        return 0;
    }
    let object_count = state.objects.len();
    if object_count == 0 {
        state
            .nof_eviction_requested
            .store(false, AtomicOrdering::Release);
        return 0;
    }
    let target_ratio = state.runtime_config.nof_eviction_ratio.max(
        used_ratio - state.runtime_config.nof_eviction_high_watermark_ratio
            + state.runtime_config.nof_eviction_ratio,
    );
    ((object_count as f64 * target_ratio).ceil() as usize).max(1)
}

/// Run one NoF-specific automatic eviction cycle. Its usage and pressure
/// signal are independent from Memory eviction.
pub(crate) fn run_automatic_nof_eviction_once(state: &MasterState) -> Vec<String> {
    let target_count = automatic_nof_eviction_target_count(state);
    run_nof_eviction_cycle(state, target_count)
}

/// 尝试将热 key 推入晋升队列（从本地磁盘提升到内存）。
/// 使用 CountMinSketch 统计访问频率，达到 admission_threshold 后才触发晋升申请，
/// 避免短时热点造成不必要的晋升开销。同时限制全局在途晋升数量防止打爆内存。
///
/// Try to push a hot key into the promotion queue (local disk → memory).
/// Uses CountMinSketch to track access frequency; only triggers promotion after reaching
/// the admission_threshold, preventing transient hotspots from causing unnecessary overhead.
/// Also limits global in-flight promotions to prevent memory exhaustion.
pub(crate) fn try_push_promotion_queue(
    state: &MasterState,
    key: &str,
    record_candidate: bool,
) -> PromotionQueueResult {
    if !state.runtime_config.enable_offload || !state.runtime_config.promotion_on_hit {
        return PromotionQueueResult::Disabled;
    }

    // CountMinSketch 近似统计访问频次，达到阈值后才考虑晋升
    // CountMinSketch approximates access frequency; only consider promotion above threshold
    let current_freq = state.promotion_sketch.write().increment(key);
    let threshold = state.runtime_config.promotion_admission_threshold.max(1);
    if current_freq < threshold {
        return PromotionQueueResult::FrequencyRejected;
    }
    if memory_usage_ratio(state) >= state.runtime_config.eviction_high_watermark_ratio {
        if record_candidate && state.objects.contains_key(key) {
            record_or_refresh_candidate(
                state,
                key,
                current_freq,
                PromotionCandidateReason::Watermark,
                None,
            );
        }
        return PromotionQueueResult::WatermarkRejected;
    }

    // Admission is a compound object/task mutation. Revalidate the source and
    // publish its refcount plus task while holding the same tenant-scoped gate
    // used by remove/upsert and background replica mutation paths.
    let _mutation_guard = state.key_mutations.lock(key);

    // 检查对象状态：必须有完整 LocalDisk 副本且无 Memory 副本才有晋升价值
    // Check object state: must have a complete LocalDisk replica and no Memory replica to be worth promoting
    let (storage_id, holder_id, object_size, source) = match state.objects.get(key) {
        Some(object) => {
            let any_memory = object.replicas.iter().any(|replica| {
                replica.replica_type == ReplicaType::Memory && replica_is_routable(state, replica)
            });
            if any_memory {
                erase_promotion_candidate(state, key);
                return PromotionQueueResult::MemoryReplicaPresent;
            }
            let Some(local_disk) = object.replicas.iter().find(|replica| {
                replica.replica_type == ReplicaType::LocalDisk
                    && replica_is_routable(state, replica)
            }) else {
                erase_promotion_candidate(state, key);
                return PromotionQueueResult::NoLocalDiskSource;
            };
            let storage_id = local_disk.local_disk_storage_id.unwrap();
            let Some(holder_id) = state
                .local_disk_segments
                .get(&storage_id)
                .and_then(|entry| {
                    if entry.recovery_complete {
                        entry.active_client_id
                    } else {
                        None
                    }
                })
            else {
                erase_promotion_candidate(state, key);
                return PromotionQueueResult::PushFailed;
            };
            if local_disk.holder_client_id != Some(holder_id) {
                erase_promotion_candidate(state, key);
                return PromotionQueueResult::PushFailed;
            }
            (storage_id, holder_id, local_disk.size, local_disk.clone())
        }
        None => {
            erase_promotion_candidate(state, key);
            return PromotionQueueResult::NotFound;
        }
    };

    if state.promotion_tasks.contains_key(key) {
        erase_promotion_candidate(state, key);
        return PromotionQueueResult::AlreadyInFlight;
    }
    if !state.local_disk_segments.contains_key(&storage_id) {
        if record_candidate {
            record_or_refresh_candidate(
                state,
                key,
                current_freq,
                PromotionCandidateReason::PushFailed,
                None,
            );
        }
        return PromotionQueueResult::PushFailed;
    }
    // CAS 式限流：原子递增在途计数，超过上限则回退并放弃本次晋升
    // CAS-style rate limiting: atomically increment in-flight count; if over limit, rollback and abandon
    if state
        .promotion_in_flight
        .fetch_add(1, AtomicOrdering::Relaxed)
        >= state.runtime_config.promotion_queue_limit
    {
        state
            .promotion_in_flight
            .fetch_sub(1, AtomicOrdering::Relaxed);
        if record_candidate {
            record_or_refresh_candidate(
                state,
                key,
                current_freq,
                PromotionCandidateReason::QueueCap,
                None,
            );
        }
        return PromotionQueueResult::QueueCapRejected;
    }
    if let Some(mut local_disk) = state.local_disk_segments.get_mut(&storage_id) {
        local_disk
            .promotion_objects
            .entry(key.to_string())
            .or_insert(object_size as i64);
    }
    if !inc_refcnt_for_replica(state, key, &source) {
        if let Some(mut local_disk) = state.local_disk_segments.get_mut(&storage_id) {
            local_disk.promotion_objects.remove(key);
        }
        state
            .promotion_in_flight
            .fetch_sub(1, AtomicOrdering::Relaxed);
        if record_candidate {
            record_or_refresh_candidate(
                state,
                key,
                current_freq,
                PromotionCandidateReason::PushFailed,
                None,
            );
        }
        return PromotionQueueResult::PushFailed;
    }
    state.promotion_tasks.insert(
        key.to_string(),
        PromotionTaskEntry {
            holder_id,
            storage_id,
            object_size,
            source,
            staged_segment_id: None,
            staged_offset: None,
            reserved_quota_charge_bytes: 0,
            start_time: Instant::now(),
        },
    );
    erase_promotion_candidate(state, key);
    PromotionQueueResult::Queued
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_replica(segment_name: &str, refcnt: u32) -> ReplicaDescriptor {
        ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: segment_name.into(),
            offset: 0,
            size: 128,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(Uuid::new_v4()),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt,
            handle_valid: true,
            base_addr: 0x1000,
            protocol: "tcp".into(),
        }
    }

    #[test]
    fn quota_eviction_projection_keeps_busy_replica_and_removes_idle_replica() {
        let busy_segment_id = Uuid::new_v4();
        let mut busy = memory_replica("busy:1", 2);
        busy.segment_id = busy_segment_id;
        let idle = memory_replica("idle:1", 0);
        let object = ObjectEntry {
            replicas: vec![busy, idle],
            size: 128,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: TenantId::default(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 256,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };

        let mut projected = clone_object_for_mutation(&object);
        let removed = evict_quota_memory_replicas(&mut projected);

        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].segment_name, "idle:1");
        assert_eq!(projected.replicas.len(), 1);
        assert_eq!(projected.replicas[0].segment_id, busy_segment_id);
        assert_eq!(projected.replicas[0].refcnt, 2);
    }
}

//! Master background operations shared by workers and gRPC handlers.
//!
//! This module owns offload/promotion queue bookkeeping and memory eviction.
//! Reaper and drain processing live in sibling submodules.

use crate::eviction::EvictionManager;
use crate::proto;
use dashmap::mapref::entry::Entry;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, TaskStatus};
use std::collections::HashSet;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Instant, SystemTime};
use uuid::Uuid;

use super::helpers::{
    account_removed_object_quota, choose_drain_target_segment, client_id_by_segment_name,
    default_drain_target_segments, has_pending_task_capacity, is_lease_expired, memory_usage_ratio,
    release_replicas, sync_cache_total_accounting,
};
use super::state::{
    MasterState, ObjectEntry, OffloadingTaskEntry, PromotionCandidate, PromotionCandidateReason,
    PromotionTaskEntry,
};

const PROMOTION_CANDIDATE_LIMIT: usize = 50_000;

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
    reap_expired_background_tasks, release_staged_promotion_replica,
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
                    && client_id_by_segment_name(state, &replica.segment_name) == Some(client_id)
            })
            .cloned()
        else {
            return;
        };
        source
    };
    let can_queue_offload = state
        .local_disk_segments
        .get(&client_id)
        .is_some_and(|entry| {
            entry.enable_offloading
                && entry.offloading_objects.len() < state.runtime_config.offloading_queue_limit
        });
    if !can_queue_offload {
        return;
    }
    if !inc_refcnt_for_replica(state, key, &source) {
        return;
    }
    let queued = match state.local_disk_segments.get_mut(&client_id) {
        Some(mut local_disk) => {
            if local_disk.enable_offloading
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
        dec_refcnt_for_replica(state, key, &source);
        return;
    }
    state.offloading_tasks.insert(
        key.to_string(),
        OffloadingTaskEntry {
            client_id,
            source,
            start_time: Instant::now(),
        },
    );
}

/// 清理指定 key 的下沉任务，同时从 client 的 offloading_objects 中移除。
/// Clean up the offload task for a given key, also removing it from the client's offloading_objects.
pub(crate) fn clear_offloading_task(state: &MasterState, key: &str) {
    if let Some((_, task)) = state.offloading_tasks.remove(key) {
        dec_refcnt_for_replica(state, key, &task.source);
        if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.client_id) {
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

/// 驱逐对象中的所有 Memory 类型副本（已完成的、未被占用的）。
/// 用于彻底删除对象时的强制清理。
///
/// Evict all Memory-type replicas from an object (completed and not busy).
/// Used for forced cleanup when fully deleting an object.
fn evict_memory_replicas(object: &mut ObjectEntry) -> Vec<ReplicaDescriptor> {
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let should_remove = replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete
            && !replica.is_busy(); // skip in-use replicas / 跳过正在使用中的副本
        if should_remove {
            removed.push(replica.clone());
        }
        !should_remove
    });
    sync_cache_total_accounting(object);
    removed
}

/// 驱逐多余的 Memory 副本，但始终保留至少一份完整副本。
/// 用于 offload 场景：数据已下沉到磁盘，内存中只需保留一份热副本即可。
///
/// Evict redundant Memory replicas but always keep at least one complete copy.
/// Used in offload scenarios: data is already on disk, only one hot copy needed in memory.
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

    let mut remaining_memory = total_memory;
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let is_memory_complete = replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete
            && !replica.is_busy();
        if !is_memory_complete {
            return true;
        }
        if remaining_memory <= 1 {
            return true; // 保留最后一份完整内存副本 / Keep the last complete memory copy
        }
        remaining_memory -= 1;
        removed.push(replica.clone());
        false
    });
    sync_cache_total_accounting(object);
    removed
}

fn has_evictable_memory_replica(object: &ObjectEntry) -> bool {
    object.replicas.iter().any(|replica| {
        replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete
            && !replica.is_busy()
    })
}

/// 执行一轮驱逐循环，使用 LRU 策略选出 target_count 个候选对象。
/// 对于有本地磁盘副本的对象，优先触发 offload（下沉到磁盘），
/// 避免直接删除数据；仅在没有磁盘兜底时才彻底驱逐内存副本。
///
/// Execute one eviction cycle using LRU strategy to select `target_count` candidate objects.
/// For objects with local disk replicas, prefer triggering offload (flush to disk)
/// instead of direct deletion; only fully evict memory replicas when no disk fallback exists.
pub(crate) fn run_eviction_cycle(state: &MasterState, target_count: usize) -> Vec<String> {
    let manager = EvictionManager::new(
        state.runtime_config.soft_pin_ttl,
        state.runtime_config.lease_ttl,
    );
    // 收集驱逐候选：排除正在复制或正在 PutStart 的对象
    // Collect eviction candidates: exclude objects being replicated or in PutStart
    let mut candidates: Vec<(
        String,
        Option<SystemTime>,
        bool,
        Option<SystemTime>,
        SystemTime,
    )> = state
        .objects
        .iter()
        .filter(|entry| {
            let key = entry.key();
            !state.replication_tasks.contains_key(key)
                && !state.processing_keys.contains_key(key)
                && has_evictable_memory_replica(entry.value())
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
        .collect();
    // EvictionManager::select_for_eviction_with_hard_pin 永远不会选择 hard_pinned 对象
    // 按 last_access LRU 排序，soft_pinned 对象在 TTL 过期前不会被选中
    let selected = manager.select_for_eviction_with_lease_timeout_policy(
        &mut candidates,
        target_count,
        state.runtime_config.offload_force_evict,
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
    for key in selected {
        let (user_key, has_local_disk, owner_client, size) = match state.objects.get(&key) {
            Some(object) => {
                let has_local_disk = object
                    .replicas
                    .iter()
                    .any(|replica| replica.replica_type == ReplicaType::LocalDisk);
                let owner_client = object
                    .replicas
                    .iter()
                    .find(|replica| replica.replica_type == ReplicaType::Memory)
                    .and_then(|replica| client_id_by_segment_name(state, &replica.segment_name));
                (
                    object.user_key.clone(),
                    has_local_disk,
                    owner_client,
                    object.size,
                )
            }
            None => continue,
        };

        // 如果开启了 offload_on_evict 且对象没有本地磁盘副本，
        // 则先触发下沉（offload），等数据安全写入磁盘后再驱逐内存。
        //
        // If offload_on_evict is enabled and the object has no local disk replica,
        // trigger offload first; evict memory only after data is safely written to disk.
        let should_offload =
            state.runtime_config.offload_on_evict && !has_local_disk && owner_client.is_some();
        if should_offload {
            if offload_enqueued < offload_cap {
                let had_task = state.offloading_tasks.contains_key(&key);
                push_offloading_queue(state, owner_client.unwrap(), &key, size);
                if !had_task && state.offloading_tasks.contains_key(&key) {
                    offload_enqueued += 1;
                }
            }
            // 下沉任务未创建成功且非强制驱逐模式，则跳过本次驱逐。
            // If no offload task exists (queue/cap/client disabled) and force eviction is disabled, skip.
            if !state.offloading_tasks.contains_key(&key)
                && !state.runtime_config.offload_force_evict
            {
                continue;
            }
        }

        if let Some(mut object) = state.objects.get_mut(&key) {
            // 下沉成功后仅移除冗余副本（保留一份），其他情况则清空所有 Memory 副本
            // After offload: remove redundant replicas only (keep one). Otherwise: remove all Memory replicas.
            let removed =
                if should_offload && !has_local_disk && state.offloading_tasks.contains_key(&key) {
                    evict_redundant_memory_replicas(&mut object)
                } else {
                    evict_memory_replicas(&mut object)
                };
            let became_empty = object.replicas.is_empty();
            drop(object);
            if !removed.is_empty() {
                release_replicas(state, &removed);
                evicted.push(user_key.clone());
            }
            if became_empty {
                if let Some((_, removed_object)) = state.objects.remove(&key) {
                    state.kv_event_publisher.publish_removed(
                        removed_object.user_key_for_event(&key),
                        "cpu",
                        &removed_object.tenant_id,
                        &removed_object.group_id,
                    );
                    account_removed_object_quota(state, &removed_object);
                }
                // Clean up per-client object index / 清理每个客户端的对象索引
                for mut entry in state.client_objects.iter_mut() {
                    entry.value_mut().remove(&key);
                }
                clear_offloading_task(state, &key);
                clear_promotion_task(state, &key);
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
        .filter(|entry| has_evictable_memory_replica(entry.value()))
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

    // 检查对象状态：必须有完整 LocalDisk 副本且无 Memory 副本才有晋升价值
    // Check object state: must have a complete LocalDisk replica and no Memory replica to be worth promoting
    let (holder_id, object_size, source) = match state.objects.get(key) {
        Some(object) => {
            let any_memory = object.replicas.iter().any(|replica| {
                replica.replica_type == ReplicaType::Memory
                    && replica.status == ReplicaStatus::Complete
            });
            if any_memory {
                erase_promotion_candidate(state, key);
                return PromotionQueueResult::MemoryReplicaPresent;
            }
            let Some(local_disk) = object.replicas.iter().find(|replica| {
                replica.replica_type == ReplicaType::LocalDisk
                    && replica.status == ReplicaStatus::Complete
                    && replica.holder_client_id.is_some()
            }) else {
                erase_promotion_candidate(state, key);
                return PromotionQueueResult::NoLocalDiskSource;
            };
            (
                local_disk.holder_client_id.unwrap(),
                local_disk.size,
                local_disk.clone(),
            )
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
    if !state.local_disk_segments.contains_key(&holder_id) {
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
    if let Some(mut local_disk) = state.local_disk_segments.get_mut(&holder_id) {
        local_disk
            .promotion_objects
            .entry(key.to_string())
            .or_insert(object_size as i64);
    }
    if !inc_refcnt_for_replica(state, key, &source) {
        if let Some(mut local_disk) = state.local_disk_segments.get_mut(&holder_id) {
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
            object_size,
            source,
            staged_segment_id: None,
            staged_offset: None,
            start_time: Instant::now(),
        },
    );
    erase_promotion_candidate(state, key);
    PromotionQueueResult::Queued
}

//! # Background Operations — 后台操作 / Periodic Background Operations
//!
//! 本模块实现 Master 服务的后台周期性操作，被 `workers.rs` 中的工作线程和
//! gRPC handler 调用。主要包括：
//!
//! This module implements the Master's background periodic operations,
//! called by worker threads in `workers.rs` and by gRPC handlers. Key operations:
//!
//! | Operation | 触发源 / Trigger | 功能 / Function |
//! |-----------|-----------------|----------------|
//! | `push_offloading_queue` | PutEnd handler | 将对象推入下沉队列（内存→本地磁盘） |
//! | `clear_offloading_task` | Remove/Cleanup | 清理下沉任务并从客户端队列中移除 |
//! | `clear_promotion_task` | Remove/Cleanup | 清理提升任务并释放全局在途计数 |
//! | `try_push_promotion_queue` | GetReplicaList handler | 热点检测后尝试入队提升（本地磁盘→内存） |
//! | `release_staged_promotion_replica` | Promotion failure/reaper | 释放提升过程中分配的暂存副本 |
//! | `reap_expired_background_tasks` | ProcessingReaper worker | 周期回收超时的 offload/promotion/remote_pull 任务 |
//! | `run_eviction_cycle` | EvictionWorker / test | 执行一轮 LRU 淘汰循环 |
//! | `run_automatic_eviction_once` | EvictionWorker | 基于内存水位计算目标后执行一轮淘汰 |
//! | `automatic_eviction_target_count` | internal | 根据水位计算本轮需淘汰的对象数量 |
//! | `process_drain_jobs` | DrainWorker | 周期性 drain 任务刷新、重试、完成检查 |

use crate::eviction::EvictionManager;
use crate::proto;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
use std::collections::HashSet;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Instant, SystemTime};
use uuid::Uuid;

use super::helpers::{client_id_by_segment_name, memory_usage_ratio, release_replicas};
use super::state::{MasterState, ObjectEntry, OffloadingTaskEntry, PromotionTaskEntry};

/// 释放晋升过程中暂存的副本占位（Allocating 状态，尚未写入数据）。
/// 晋升失败或放弃时调用，将占用的 segment 空间归还给分配器。
///
/// Release a staged replica placeholder from promotion (Allocating status, no data written).
/// Called on promotion failure or abandonment to return the occupied segment space to the allocator.
pub(crate) fn release_staged_promotion_replica(
    state: &MasterState,
    key: &str,
    segment_id: Uuid,
    offset: u64,
) {
    if let Some(mut object) = state.objects.get_mut(key) {
        let mut removed = Vec::new();
        // 只清理 Allocating 状态的 Memory 副本，避免误删已完成的副本
        // Only clean Allocating Memory replicas to avoid accidentally removing completed ones
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
            release_replicas(state, &removed);
        }
    }
}

/// 定期回收超时的后台任务（offload / promotion / remote_pull）。
/// 超过 TTL 的任务被视为失败，释放其持有的资源和锁，防止泄漏。
///
/// Periodically reap expired background tasks (offload / promotion / remote_pull).
/// Tasks exceeding their TTL are considered failed; their held resources and locks are released to prevent leaks.
pub(crate) fn reap_expired_background_tasks(state: &MasterState, now: Instant) {
    let ttl = state.runtime_config.put_start_release_timeout;

    // 回收过期 offload 任务 / Reap expired offload tasks
    let expired_offloads = state
        .offloading_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_offloads {
        clear_offloading_task(state, &key);
    }

    // 回收过期 promotion 任务 / Reap expired promotion tasks
    let expired_promotions = state
        .promotion_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_promotions {
        if let Some(task) = clear_promotion_task(state, &key) {
            // 释放已分配的暂存副本 / Release allocated staged replica
            if let (Some(segment_id), Some(offset)) = (task.staged_segment_id, task.staged_offset) {
                release_staged_promotion_replica(state, &key, segment_id, offset);
            }
            if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.holder_id) {
                local_disk.promotion_objects.remove(&key);
            }
        }
    }

    // Reap stale remote pull entries (pulling node crashed or timed out)
    // 回收过期的远端拉取条目（拉取节点崩溃或超时）
    let pull_ttl = state.runtime_config.remote_pull_ttl;
    let stale_pulls: Vec<String> = state
        .pending_remote_pulls
        .iter()
        .filter(|entry| entry.started_at.elapsed() > pull_ttl)
        .map(|entry| entry.key().clone())
        .collect();
    for key in stale_pulls {
        state.pending_remote_pulls.remove(&key);
        tracing::info!(key = %key, "reaped stale remote pull entry");
    }
}

/// 将对象推入下沉队列，触发从内存到本地磁盘的数据下沉。
/// 仅在 client 启用了 offload 且对象不在处理中时才入队。
///
/// Push an object into the offload queue, triggering memory→local-disk data offload.
/// Only enqueues when the client has offload enabled and the object is not already being processed.
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

/// 清理指定 key 的下沉任务，同时从 client 的 offloading_objects 中移除。
/// Clean up the offload task for a given key, also removing it from the client's offloading_objects.
pub(crate) fn clear_offloading_task(state: &MasterState, key: &str) {
    if let Some((_, task)) = state.offloading_tasks.remove(key) {
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
    if removed.is_some() {
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
            replica.replica_type == ReplicaType::Memory
                && replica.status == ReplicaStatus::Complete
                && !replica.is_busy()
        })
        .count();
    if total_memory <= 1 {
        return Vec::new();
    }

    let mut kept_one = false;
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let is_memory_complete = replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete
            && !replica.is_busy();
        if !is_memory_complete {
            return true;
        }
        if !kept_one {
            kept_one = true;
            return true; // 保留第一份 / Keep the first one
        }
        removed.push(replica.clone());
        false
    });
    removed
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
    let mut candidates: Vec<(String, Option<SystemTime>, bool, SystemTime)> = state
        .objects
        .iter()
        .filter(|entry| {
            let key = entry.key();
            !state.replication_tasks.contains_key(key) && !state.processing_keys.contains_key(key)
        })
        .map(|entry| {
            (
                entry.key().clone(),
                entry.soft_pin_timeout,
                entry.hard_pinned,
                entry.last_access,
            )
        })
        .collect();
    // EvictionManager::select_for_eviction_with_hard_pin 永远不会选择 hard_pinned 对象
    // 按 last_access LRU 排序，soft_pinned 对象在 TTL 过期前不会被选中
    let selected = manager.select_for_eviction_with_hard_pin(&mut candidates, target_count);

    let mut evicted = Vec::new();
    for key in selected {
        if let Some(mut object) = state.objects.get_mut(&key) {
            let user_key = object.user_key.clone();
            let has_local_disk = object
                .replicas
                .iter()
                .any(|replica| replica.replica_type == ReplicaType::LocalDisk);
            let owner_client = object
                .replicas
                .iter()
                .find(|replica| replica.replica_type == ReplicaType::Memory)
                .and_then(|replica| client_id_by_segment_name(state, &replica.segment_name));

            // 如果开启了 offload_on_evict 且对象没有本地磁盘副本，
            // 则先触发下沉（offload），等数据安全写入磁盘后再驱逐内存。
            //
            // If offload_on_evict is enabled and the object has no local disk replica,
            // trigger offload first; evict memory only after data is safely written to disk.
            let should_offload =
                state.runtime_config.offload_on_evict && !has_local_disk && owner_client.is_some();
            if should_offload {
                push_offloading_queue(state, owner_client.unwrap(), &key, object.size);
                // 下沉任务未创建成功且非强制驱逐模式，则跳过本次驱逐
                // If offload task was not created and force eviction is not enabled, skip
                if !state.offloading_tasks.contains_key(&key)
                    && !state.runtime_config.offload_force_evict
                {
                    continue;
                }
            }

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
                state.objects.remove(&key);
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

    let object_count = state.objects.len();
    if object_count == 0 {
        return 0;
    }

    // 计算驱逐目标比例：eviction_ratio 与 (used_ratio - watermark + eviction_ratio) 取较大值
    let evict_ratio_target = state.runtime_config.eviction_ratio.max(
        used_ratio - state.runtime_config.eviction_high_watermark_ratio
            + state.runtime_config.eviction_ratio,
    );
    let target = (object_count as f64 * evict_ratio_target).ceil() as usize;
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
pub(crate) fn try_push_promotion_queue(state: &MasterState, key: &str) {
    if !state.runtime_config.promotion_on_hit {
        return;
    }

    // CountMinSketch 近似统计访问频次，达到阈值后才考虑晋升
    // CountMinSketch approximates access frequency; only consider promotion above threshold
    let current_freq = state.promotion_sketch.write().increment(key);
    let threshold = state.runtime_config.promotion_admission_threshold.max(1);
    if current_freq < threshold {
        return;
    }

    // 检查对象状态：必须有完整 LocalDisk 副本且无 Memory 副本才有晋升价值
    // Check object state: must have a complete LocalDisk replica and no Memory replica to be worth promoting
    let (holder_id, object_size) = match state.objects.get(key) {
        Some(object) => {
            let any_memory = object.replicas.iter().any(|replica| {
                replica.replica_type == ReplicaType::Memory
                    && replica.status == ReplicaStatus::Complete
            });
            if any_memory {
                return; // 已在内存中，无需晋升 / Already in memory, no promotion needed
            }
            let Some(local_disk) = object.replicas.iter().find(|replica| {
                replica.replica_type == ReplicaType::LocalDisk
                    && replica.status == ReplicaStatus::Complete
                    && replica.holder_client_id.is_some()
            }) else {
                return; // 无可用的磁盘副本来源 / No usable disk replica source
            };
            (local_disk.holder_client_id.unwrap(), local_disk.size)
        }
        None => return,
    };

    if state.promotion_tasks.contains_key(key) {
        return; // 已在晋升队列中 / Already in promotion queue
    }
    if !state.local_disk_segments.contains_key(&holder_id) {
        return;
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

// ============================================================================
// Drain Job Processing / Drain 任务处理
// ============================================================================

/// Maximum retry count per drain unit; matches C++ kMaxDrainUnitRetries.
const K_MAX_DRAIN_UNIT_RETRIES: u32 = 3;

/// Periodic drain job processing: refresh task statuses, update counters, retry
/// failed tasks, and mark completed jobs.
/// C++ equivalent: MasterService::ProcessDrainJobs() in master_service.cpp:6906
pub(crate) fn process_drain_jobs(state: &MasterState) {
    let job_ids: Vec<Uuid> = state
        .drain_jobs
        .iter()
        .filter(|entry| {
            let s = entry.status;
            s == proto::JobStatus::Created
                || s == proto::JobStatus::Planning
                || s == proto::JobStatus::Running
        })
        .map(|e| *e.key())
        .collect();

    for job_id in job_ids {
        refresh_drain_job_tasks(state, job_id);
        if maybe_complete_drain_job(state, job_id) {
            continue;
        }
        schedule_drain_job_tasks_free(state, job_id);
    }
}

fn refresh_drain_job_tasks(state: &MasterState, job_id: Uuid) {
    use mooncake_store_core::TaskStatus;
    let Some(mut job) = state.drain_jobs.get_mut(&job_id) else {
        return;
    };
    let task_ids: Vec<Uuid> = job.active_tasks.keys().copied().collect();
    for task_id in task_ids {
        let Some(active_task) = job.active_tasks.get(&task_id) else {
            continue;
        };
        let unit_key = active_task.unit_key.clone();
        let bytes = active_task.bytes;
        match state.tasks.get(&task_id) {
            None => {
                job.failed_units += 1;
                let count = job.retry_counts.entry(unit_key.clone()).or_insert(0);
                *count += 1;
                if *count >= K_MAX_DRAIN_UNIT_RETRIES {
                    job.terminal_failed_unit_keys.insert(unit_key);
                }
                job.active_tasks.remove(&task_id);
            }
            Some(task) => match task.info.status {
                TaskStatus::Pending | TaskStatus::Processing => {}
                TaskStatus::Success => {
                    job.succeeded_units += 1;
                    job.migrated_bytes += bytes;
                    job.completed_unit_keys.insert(unit_key);
                    job.active_tasks.remove(&task_id);
                }
                TaskStatus::Failed => {
                    job.failed_units += 1;
                    let count = job.retry_counts.entry(unit_key.clone()).or_insert(0);
                    *count += 1;
                    if *count >= K_MAX_DRAIN_UNIT_RETRIES {
                        job.terminal_failed_unit_keys.insert(unit_key);
                    }
                    job.active_tasks.remove(&task_id);
                }
            },
        }
    }
    job.last_updated_at = SystemTime::now();
}

fn maybe_complete_drain_job(state: &MasterState, job_id: Uuid) -> bool {
    use super::state::ActiveDrainTask;
    let Some(mut job) = state.drain_jobs.get_mut(&job_id) else {
        return true;
    };
    if !job.active_tasks.is_empty() {
        return false;
    }
    let draining_segments: HashSet<String> = job.segments.iter().cloned().collect();
    let mut remaining = false;
    for entry in state.objects.iter() {
        for replica in &entry.replicas {
            if draining_segments.contains(&replica.segment_name)
                && replica.status == ReplicaStatus::Complete
            {
                remaining = true;
                break;
            }
        }
        if remaining {
            break;
        }
    }
    if !remaining {
        for seg_name in &job.segments {
            for mut entry in state.segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Active;
                }
            }
            for mut entry in state.nof_segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Active;
                }
            }
        }
        job.status = proto::JobStatus::Succeeded;
        job.message = "drain completed".into();
        job.last_updated_at = SystemTime::now();
        tracing::info!("Drain job succeeded: id={}", job_id);
        return true;
    }
    let mut retryable = false;
    for entry in state.objects.iter() {
        for replica in &entry.replicas {
            if draining_segments.contains(&replica.segment_name)
                && replica.status == ReplicaStatus::Complete
            {
                let unit_key = ActiveDrainTask::unit_key_for(entry.key(), &replica.segment_name);
                if !job.completed_unit_keys.contains(&unit_key)
                    && !job.terminal_failed_unit_keys.contains(&unit_key)
                {
                    retryable = true;
                }
            }
        }
    }
    if !retryable {
        for seg_name in &job.segments {
            for mut entry in state.segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Active;
                }
            }
            for mut entry in state.nof_segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Active;
                }
            }
        }
        job.status = proto::JobStatus::Failed;
        job.message = "all remaining units terminal failed".into();
        job.last_updated_at = SystemTime::now();
        tracing::warn!("Drain job failed: id={}", job_id);
        return true;
    }
    false
}

fn schedule_drain_job_tasks_free(state: &MasterState, job_id: Uuid) {
    use super::state::ActiveDrainTask;
    use mooncake_store_core::{TaskInfo, TaskStatus, TaskType};
    use serde::Serialize;
    let Some(mut job) = state.drain_jobs.get_mut(&job_id) else {
        return;
    };
    let draining_segments: HashSet<String> = job.segments.iter().cloned().collect();
    let targets = job.target_segments.clone();
    let max_concurrency = job.max_concurrency as usize;
    let available = max_concurrency.saturating_sub(job.active_tasks.len());
    if available == 0 {
        return;
    }
    let mut units: Vec<(String, String, u64)> = Vec::new();
    for entry in state.objects.iter() {
        let key = entry.key().clone();
        for replica in &entry.replicas {
            if draining_segments.contains(&replica.segment_name)
                && replica.status == ReplicaStatus::Complete
            {
                let unit_key = ActiveDrainTask::unit_key_for(&key, &replica.segment_name);
                if !job.completed_unit_keys.contains(&unit_key)
                    && !job.terminal_failed_unit_keys.contains(&unit_key)
                    && !job.active_tasks.values().any(|t| t.unit_key == unit_key)
                {
                    units.push((key.clone(), replica.segment_name.clone(), replica.size));
                }
                break;
            }
        }
    }
    let num_targets = targets.len().max(1);
    let mut scheduled = 0;
    for (i, (key, source_seg, bytes)) in units.into_iter().enumerate() {
        if scheduled >= available {
            break;
        }
        let unit_key = ActiveDrainTask::unit_key_for(&key, &source_seg);
        let target_seg = targets[i % num_targets].clone();
        let task_id = Uuid::new_v4();
        #[derive(Serialize)]
        struct ReplicaCopyPayload {
            key: String,
            source: String,
            targets: Vec<String>,
        }
        let payload = serde_json::to_string(&ReplicaCopyPayload {
            key: key.clone(),
            source: source_seg.clone(),
            targets: vec![target_seg.clone()],
        })
        .unwrap_or_default();
        let assigned_client = client_id_by_segment_name(state, &source_seg);
        let now = chrono::Utc::now();
        state.tasks.insert(
            task_id,
            crate::service::state::TaskEntry {
                info: TaskInfo {
                    id: task_id,
                    task_type: TaskType::ReplicaCopy,
                    status: TaskStatus::Pending,
                    created_at: now,
                    last_updated_at: now,
                    assigned_client,
                    message: format!("drain {key} from {source_seg} to {target_seg}"),
                },
                key: key.clone(),
                payload,
                max_retry_attempts: 3,
            },
        );
        job.active_tasks.insert(
            task_id,
            ActiveDrainTask {
                source_segment: source_seg,
                target_segment: target_seg,
                bytes,
                unit_key,
            },
        );
        scheduled += 1;
    }
    job.status = if job.active_tasks.is_empty()
        && scheduled == 0
        && job.completed_unit_keys.len() + job.terminal_failed_unit_keys.len() > 0
    {
        proto::JobStatus::Succeeded
    } else {
        proto::JobStatus::Running
    };
    job.last_updated_at = SystemTime::now();
}

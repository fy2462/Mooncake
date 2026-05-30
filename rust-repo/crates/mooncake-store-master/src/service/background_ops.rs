use crate::eviction::EvictionManager;
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Instant, SystemTime};
use uuid::Uuid;

use super::helpers::{client_id_by_segment_name, memory_usage_ratio, release_replicas};
use super::state::{MasterState, ObjectEntry, OffloadingTaskEntry, PromotionTaskEntry};

/// 释放晋升过程中暂存的副本占位（Allocating 状态，尚未写入数据）。
/// 晋升失败或放弃时调用，将占用的 segment 空间归还给分配器。
pub(crate) fn release_staged_promotion_replica(
    state: &MasterState,
    key: &str,
    segment_id: Uuid,
    offset: u64,
) {
    if let Some(mut object) = state.objects.get_mut(key) {
        let mut removed = Vec::new();
        // 只清理 Allocating 状态的 Memory 副本，避免误删已完成的副本
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

/// 定期回收超时的后台任务（offload / promotion）。
/// 超过 TTL 的任务被视为失败，释放其持有的资源和锁，防止泄漏。
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

    // Reap stale remote pull entries (pulling node crashed or timed out)
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
pub(crate) fn clear_offloading_task(state: &MasterState, key: &str) {
    if let Some((_, task)) = state.offloading_tasks.remove(key) {
        if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.client_id) {
            local_disk.offloading_objects.remove(key);
        }
    }
}

/// 清理指定 key 的晋升任务，并将全局在途晋升计数器减 1。
/// 返回被移除的任务条目，供调用方进一步清理暂存副本。
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
fn evict_memory_replicas(object: &mut ObjectEntry) -> Vec<ReplicaDescriptor> {
    let mut removed = Vec::new();
    object.replicas.retain(|replica| {
        let should_remove = replica.replica_type == ReplicaType::Memory
            && replica.status == ReplicaStatus::Complete
            && !replica.is_busy(); // skip in-use replicas
        if should_remove {
            removed.push(replica.clone());
        }
        !should_remove
    });
    removed
}

/// 驱逐多余的 Memory 副本，但始终保留至少一份完整副本。
/// 用于 offload 场景：数据已下沉到磁盘，内存中只需保留一份热副本即可。
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
            return true;
        }
        removed.push(replica.clone());
        false
    });
    removed
}

/// 执行一轮驱逐循环，使用 LRU 策略选出 target_count 个候选对象。
/// 对于有本地磁盘副本的对象，优先触发 offload（下沉到磁盘），
/// 避免直接删除数据；仅在没有磁盘兜底时才彻底驱逐内存副本。
pub(crate) fn run_eviction_cycle(state: &MasterState, target_count: usize) -> Vec<String> {
    let manager = EvictionManager::new(
        state.runtime_config.soft_pin_ttl,
        state.runtime_config.lease_ttl,
    );
    let mut candidates: Vec<(String, bool, bool, SystemTime)> = state
        .objects
        .iter()
        .filter(|entry| {
            let key = entry.key();
            !state.replication_tasks.contains_key(key) && !state.processing_keys.contains_key(key)
        })
        .map(|entry| {
            (
                entry.key().clone(),
                entry.soft_pinned,
                entry.hard_pinned,
                entry.last_access,
            )
        })
        .collect();
    let selected = manager.select_for_eviction_with_hard_pin(&mut candidates, target_count);

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

            // 如果开启了 offload_on_evict 且对象没有本地磁盘副本，
            // 则先触发下沉（offload），等数据安全写入磁盘后再驱逐内存。
            let should_offload =
                state.runtime_config.offload_on_evict && !has_local_disk && owner_client.is_some();
            if should_offload {
                push_offloading_queue(state, owner_client.unwrap(), &key, object.size);
                // 下沉任务未创建成功且非强制驱逐模式，则跳过本次驱逐
                if !state.offloading_tasks.contains_key(&key)
                    && !state.runtime_config.offload_force_evict
                {
                    continue;
                }
            }

            // 下沉成功后仅移除冗余副本（保留一份），其他情况则清空所有 Memory 副本
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
                evicted.push(key.clone());
            }
            if became_empty {
                state.objects.remove(&key);
                // Clean up per-client object index.
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

/// 自动驱逐入口：计算目标数量后执行一轮驱逐。由后台 EvictionWorker 周期调用。
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
pub(crate) fn try_push_promotion_queue(state: &MasterState, key: &str) {
    if !state.runtime_config.promotion_on_hit {
        return;
    }

    // CountMinSketch 近似统计访问频次，达到阈值后才考虑晋升
    let current_freq = state.promotion_sketch.write().increment(key);
    let threshold = state.runtime_config.promotion_admission_threshold.max(1);
    if current_freq < threshold {
        return;
    }

    // 检查对象状态：必须有完整 LocalDisk 副本且无 Memory 副本才有晋升价值
    let (holder_id, object_size) = match state.objects.get(key) {
        Some(object) => {
            let any_memory = object.replicas.iter().any(|replica| {
                replica.replica_type == ReplicaType::Memory
                    && replica.status == ReplicaStatus::Complete
            });
            if any_memory {
                return; // 已在内存中，无需晋升
            }
            let Some(local_disk) = object.replicas.iter().find(|replica| {
                replica.replica_type == ReplicaType::LocalDisk
                    && replica.status == ReplicaStatus::Complete
                    && replica.holder_client_id.is_some()
            }) else {
                return; // 无可用的磁盘副本来源
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
    // CAS 式限流：原子递增在途计数，超过上限则回退并放弃本次晋升
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

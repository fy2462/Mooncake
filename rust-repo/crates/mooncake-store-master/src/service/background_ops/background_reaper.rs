use super::*;

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
    let system_now = SystemTime::now();
    reap_client_tasks(state);

    // Reap expired PutStart objects. This mirrors C++ DiscardExpiredProcessingReplicas:
    // processing markers for complete/invalid objects are dropped, and timed-out
    // Allocating replicas are released.
    let mut expired_processing = Vec::new();
    let mut stale_processing_markers = Vec::new();
    for entry in state.processing_keys.iter() {
        let key = entry.key().clone();
        let Some(object) = state.objects.get(&key) else {
            stale_processing_markers.push(key);
            continue;
        };
        if object.replicas.is_empty()
            || object
                .replicas
                .iter()
                .all(|r| r.status == ReplicaStatus::Complete)
        {
            stale_processing_markers.push(key);
            continue;
        }
        if object
            .put_start_time
            .is_some_and(|start| system_now.duration_since(start).unwrap_or_default() >= ttl)
        {
            expired_processing.push(key);
        }
    }
    for key in stale_processing_markers {
        state.processing_keys.remove(&key);
    }
    for key in expired_processing {
        let mut removed = Vec::new();
        let mut remove_object = false;
        if let Some(mut object) = state.objects.get_mut(&key) {
            object.replicas.retain(|replica| {
                let should_remove = replica.status == ReplicaStatus::Allocating;
                if should_remove {
                    removed.push(replica.clone());
                }
                !should_remove
            });
            remove_object = object.replicas.is_empty();
        }
        state.processing_keys.remove(&key);
        if !removed.is_empty() {
            release_replicas(state, &removed);
        }
        if remove_object {
            state.objects.remove(&key);
            clear_offloading_task(state, &key);
            clear_promotion_task(state, &key);
        }
    }

    // Reap expired copy/move tasks. C++ unpins the source and discards the
    // allocated targets when replication has exceeded put_start_release_timeout.
    let expired_replications = state
        .replication_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_replications {
        let Some((_, task)) = state.replication_tasks.remove(&key) else {
            continue;
        };
        let mut removed_targets = Vec::new();
        let mut remove_object = false;
        if let Some(mut object) = state.objects.get_mut(&key) {
            if let Some(source) = object
                .replicas
                .iter_mut()
                .find(|replica| same_replica_location(replica, &task.source))
            {
                source.dec_refcnt();
            }
            object.replicas.retain(|replica| {
                let should_remove = task
                    .targets
                    .iter()
                    .any(|target| same_replica_location(replica, target));
                if should_remove {
                    removed_targets.push(replica.clone());
                }
                !should_remove
            });
            remove_object = object.replicas.is_empty();
        }
        if !removed_targets.is_empty() {
            release_replicas(state, &removed_targets);
        }
        if remove_object {
            state.objects.remove(&key);
            state.processing_keys.remove(&key);
            clear_offloading_task(state, &key);
            clear_promotion_task(state, &key);
        }
    }

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

fn reap_client_tasks(state: &MasterState) {
    let now = chrono::Utc::now();
    let pending_timeout = state.runtime_config.pending_task_timeout;
    let processing_timeout = state.runtime_config.processing_task_timeout;
    for mut task in state.tasks.iter_mut() {
        let (timeout, message) = match task.info.status {
            TaskStatus::Pending if !pending_timeout.is_zero() => {
                (pending_timeout, "pending timeout")
            }
            TaskStatus::Processing if !processing_timeout.is_zero() => {
                (processing_timeout, "processing timeout")
            }
            _ => continue,
        };
        let elapsed = now
            .signed_duration_since(if task.info.status == TaskStatus::Pending {
                task.info.created_at
            } else {
                task.info.last_updated_at
            })
            .to_std()
            .unwrap_or_default();
        if elapsed > timeout {
            task.info.status = TaskStatus::Failed;
            task.info.message = message.to_string();
            task.info.last_updated_at = now;
        }
    }

    let mut finished = state
        .tasks
        .iter()
        .filter(|task| matches!(task.info.status, TaskStatus::Success | TaskStatus::Failed))
        .map(|task| (*task.key(), task.info.last_updated_at))
        .collect::<Vec<_>>();
    finished.sort_by_key(|(_, updated_at)| *updated_at);
    let excess = finished
        .len()
        .saturating_sub(state.runtime_config.max_total_finished_tasks);
    for (task_id, _) in finished.into_iter().take(excess) {
        state.tasks.remove(&task_id);
    }
}


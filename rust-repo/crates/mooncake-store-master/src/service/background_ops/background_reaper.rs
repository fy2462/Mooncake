use super::*;
use crate::TenantId;

/// Detach a staged promotion placeholder from object metadata without
/// returning its range to the allocator.
///
/// The caller must durably publish the resulting object image together with a
/// delayed reservation before releasing the returned replicas. Keeping detach
/// and release separate prevents allocator reuse from racing ahead of oplog
/// persistence.
pub(crate) fn detach_staged_promotion_replica(
    state: &MasterState,
    key: &str,
    segment_id: Uuid,
    offset: u64,
) -> Vec<ReplicaDescriptor> {
    let mut removed = Vec::new();
    if let Some(mut object) = state.objects.get_mut(key) {
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
    }
    removed
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
    if state.service_fenced.load(AtomicOrdering::Acquire) {
        return;
    }
    reap_expired_delayed_replica_releases(state, system_now);
    if state.service_fenced.load(AtomicOrdering::Acquire) {
        return;
    }

    // Promotion tasks are intentionally runtime-only, while PromotionAllocStart
    // durably publishes its staged range before returning the writable
    // descriptor. After snapshot restore or standby promotion that leaves a
    // committed, still-readable object with an orphan Allocating replica.
    // Detach such replicas immediately, but keep their allocator ranges in the
    // durable delayed-release table for a full grace period.
    let orphaned_staged_keys = state
        .objects
        .iter()
        .filter(|entry| {
            let key = entry.key();
            entry.quota_committed
                && entry
                    .replicas
                    .iter()
                    .any(|replica| replica.status == ReplicaStatus::Complete)
                && entry.replicas.iter().any(|replica| {
                    matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd
                    ) && replica.status != ReplicaStatus::Complete
                })
                && !state.replication_tasks.contains_key(key)
                && !state.promotion_tasks.contains_key(key)
        })
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in orphaned_staged_keys {
        let _mutation_guard = state.key_mutations.lock(&key);
        if state.replication_tasks.contains_key(&key) || state.promotion_tasks.contains_key(&key) {
            continue;
        }
        let mut removed = Vec::new();
        let Some(mut object) = state.objects.get_mut(&key) else {
            continue;
        };
        if !object.quota_committed
            || !object
                .replicas
                .iter()
                .any(|replica| replica.status == ReplicaStatus::Complete)
        {
            continue;
        }
        object.replicas.retain(|replica| {
            let orphaned = matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) && replica.status != ReplicaStatus::Complete;
            if orphaned {
                removed.push(replica.clone());
            }
            !orphaned
        });
        if removed.is_empty() {
            continue;
        }
        sync_cache_total_accounting(&mut object);
        let authoritative_object = object.clone();
        drop(object);
        let Some(deadline) = system_now.checked_add(ttl) else {
            state.fence_after_invariant_failure(
                "reap_orphaned_staged_deadline",
                &format!("key={key:?} has no representable delayed-release deadline"),
            );
            return;
        };
        if state
            .schedule_delayed_replica_release_or_fence(
                &key,
                Some(authoritative_object),
                removed,
                Some(deadline),
                "reap_orphaned_staged_replica",
            )
            .is_err()
        {
            return;
        }
    }

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
        let _mutation_guard = state.key_mutations.lock(&key);
        state.processing_keys.remove(&key);
    }
    for key in expired_processing {
        let _mutation_guard = state.key_mutations.lock(&key);
        let mut removed = Vec::new();
        let mut remove_object = false;
        let mut completed_survivor = None;
        let mut release_deadline = None;
        if let Some(mut object) = state.objects.get_mut(&key) {
            release_deadline = object
                .put_start_time
                .and_then(|start| start.checked_add(ttl));
            if release_deadline.is_none() {
                drop(object);
                state.fence_after_invariant_failure(
                    "reap_expired_put_deadline",
                    &format!("key={key:?} has no representable PutStart release deadline"),
                );
                return;
            }
            let mut projected = clone_object_for_mutation(&object);
            projected.replicas.retain(|replica| {
                let should_remove = replica.status != ReplicaStatus::Complete;
                if should_remove {
                    removed.push(replica.clone());
                }
                !should_remove
            });
            remove_object = projected.replicas.is_empty();
            if !remove_object {
                if !projected.quota_committed {
                    let committed_charge = completed_memory_quota_charge(&projected);
                    if state.runtime_config.enable_tenant_quota {
                        let mut quotas = state.tenant_quotas.write();
                        let mut projected_quotas = quotas.clone();
                        let settle_result = projected_quotas
                            .settle(
                                &projected.tenant_id,
                                projected.reserved_quota_charge_bytes,
                                committed_charge,
                                true,
                            )
                            .and_then(|()| {
                                if projected.pending_replaced_quota_charge_bytes == 0 {
                                    Ok(())
                                } else {
                                    projected_quotas.release(
                                        &projected.tenant_id,
                                        projected.pending_replaced_quota_charge_bytes,
                                    )
                                }
                            });
                        if let Err(error) = settle_result {
                            drop(quotas);
                            drop(object);
                            state.fence_after_invariant_failure(
                                "reap_expired_put_quota",
                                &format!("key={key:?} error={error:?}"),
                            );
                            return;
                        }
                        *quotas = projected_quotas;
                    }
                    projected.quota_committed = true;
                    projected.reserved_quota_charge_bytes = 0;
                    projected.committed_quota_charge_bytes = committed_charge;
                    projected.pending_replaced_quota_charge_bytes = 0;
                }
                if projected
                    .replicas
                    .iter()
                    .all(|replica| replica.status == ReplicaStatus::Complete)
                {
                    projected.put_start_time = None;
                }
                sync_cache_total_accounting(&mut projected);
                if projected
                    .replicas
                    .iter()
                    .all(|replica| replica.status == ReplicaStatus::Complete)
                {
                    completed_survivor = Some(projected.client_id);
                }
                *object = projected;
            }
        }
        state.processing_keys.remove(&key);
        if remove_object {
            if let Some((_, object)) = state.objects.remove(&key) {
                if account_removed_object_quota(state, &object).is_err() {
                    return;
                }
            }
            clear_offloading_task(state, &key);
            clear_promotion_task(state, &key);
        }
        if state.service_fenced.load(AtomicOrdering::Acquire) {
            return;
        }
        if let Some(client_id) = completed_survivor.filter(|client_id| !client_id.is_nil()) {
            state
                .client_objects
                .entry(client_id)
                .or_default()
                .insert(key.clone());
        }
        if !removed.is_empty() {
            let Some(release_deadline) = release_deadline else {
                state.fence_after_invariant_failure(
                    "reap_expired_put_deadline",
                    &format!("key={key:?} lost its PutStart release deadline"),
                );
                return;
            };
            let authoritative_object = state.objects.get(&key).map(|object| object.clone());
            let delayed = match state.schedule_delayed_replica_release_or_fence(
                &key,
                authoritative_object,
                removed,
                Some(release_deadline),
                "reap_expired_put",
            ) {
                Ok(release_id) => release_id.is_some(),
                Err(_) => return,
            };
            if !delayed
                && state
                    .persist_object_image_or_remove_or_fence(&key, "reap_expired_put")
                    .is_err()
            {
                return;
            }
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
        let _mutation_guard = state.key_mutations.lock(&key);
        let Some(task) = state.replication_tasks.get(&key).map(|task| task.clone()) else {
            continue;
        };
        let projected_quotas = if state.runtime_config.enable_tenant_quota {
            let tenant_id = match TenantId::parse_scoped_key(&key) {
                Ok((tenant_id, _)) => tenant_id,
                Err(error) => {
                    state.fence_after_invariant_failure(
                        "reap_expired_replication_tenant",
                        &format!("key={key:?} error={error}"),
                    );
                    return;
                }
            };
            let mut projected = state.tenant_quotas.read().clone();
            if let Err(error) = projected.abort(&tenant_id, task.reserved_quota_charge_bytes) {
                state.fence_after_invariant_failure(
                    "reap_expired_replication_quota",
                    &format!(
                        "key={key:?} reserved_charge={} error={error:?}",
                        task.reserved_quota_charge_bytes
                    ),
                );
                return;
            }
            Some(projected)
        } else {
            None
        };
        let Some((_, task)) = state.replication_tasks.remove(&key) else {
            state.fence_after_invariant_failure(
                "reap_expired_replication_task",
                &format!("key={key:?} disappeared while holding its mutation guard"),
            );
            return;
        };
        if let Some(projected) = projected_quotas {
            *state.tenant_quotas.write() = projected;
        }
        let mut removed_targets = Vec::new();
        let mut remove_object = false;
        let mut object_mutated = false;
        if let Some(mut object) = state.objects.get_mut(&key) {
            if let Some(source) = object
                .replicas
                .iter_mut()
                .find(|replica| same_replica_location(replica, &task.source))
            {
                source.dec_refcnt();
                object_mutated = true;
            }
            object.replicas.retain(|replica| {
                let should_remove = task
                    .targets
                    .iter()
                    .any(|target| same_replica_location(replica, target));
                if should_remove {
                    removed_targets.push(replica.clone());
                    object_mutated = true;
                }
                !should_remove
            });
            sync_cache_total_accounting(&mut object);
            remove_object = object.replicas.is_empty();
        }
        if remove_object {
            if let Some((_, object)) = state.objects.remove(&key) {
                if account_removed_object_quota(state, &object).is_err() {
                    return;
                }
            }
            state.processing_keys.remove(&key);
            clear_offloading_task(state, &key);
            clear_promotion_task(state, &key);
        }
        if state.service_fenced.load(AtomicOrdering::Acquire) {
            return;
        }
        if object_mutated {
            let authoritative_object = state.objects.get(&key).map(|object| object.clone());
            let delayed = match state.schedule_delayed_replica_release_or_fence(
                &key,
                authoritative_object,
                removed_targets,
                Some(system_now),
                "reap_expired_replication",
            ) {
                Ok(release_id) => release_id.is_some(),
                Err(_) => return,
            };
            if !delayed
                && state
                    .persist_object_image_or_remove_or_fence(&key, "reap_expired_replication")
                    .is_err()
            {
                return;
            }
        }
    }
    // Copy/Move targets above are already past their C++ deadline. Retire their
    // just-persisted reservations in the same pass, but only after the durable
    // tombstone is committed.
    reap_expired_delayed_replica_releases(state, system_now);
    if state.service_fenced.load(AtomicOrdering::Acquire) {
        return;
    }

    // 回收过期 offload 任务 / Reap expired offload tasks
    let expired_offloads = state
        .offloading_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_offloads {
        let _mutation_guard = state.key_mutations.lock(&key);
        clear_offloading_task(state, &key);
        if state
            .persist_object_image_or_remove_or_fence(&key, "reap_expired_offload")
            .is_err()
        {
            return;
        }
    }

    // 回收过期 promotion 任务 / Reap expired promotion tasks
    let expired_promotions = state
        .promotion_tasks
        .iter()
        .filter(|entry| now.saturating_duration_since(entry.start_time) >= ttl)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in expired_promotions {
        let _mutation_guard = state.key_mutations.lock(&key);
        let Some(task) = state.promotion_tasks.get(&key).map(|task| task.clone()) else {
            continue;
        };
        let projected_quotas = if state.runtime_config.enable_tenant_quota {
            let tenant_id = match TenantId::parse_scoped_key(&key) {
                Ok((tenant_id, _)) => tenant_id,
                Err(error) => {
                    state.fence_after_invariant_failure(
                        "reap_expired_promotion_tenant",
                        &format!("key={key:?} error={error}"),
                    );
                    return;
                }
            };
            let mut projected = state.tenant_quotas.read().clone();
            if let Err(error) = projected.abort(&tenant_id, task.reserved_quota_charge_bytes) {
                state.fence_after_invariant_failure(
                    "reap_expired_promotion_quota",
                    &format!(
                        "key={key:?} reserved_charge={} error={error:?}",
                        task.reserved_quota_charge_bytes
                    ),
                );
                return;
            }
            Some(projected)
        } else {
            None
        };
        if let Some(task) = clear_promotion_task(state, &key) {
            if let Some(projected) = projected_quotas {
                *state.tenant_quotas.write() = projected;
            }
            let removed = match (task.staged_segment_id, task.staged_offset) {
                (Some(segment_id), Some(offset)) => {
                    detach_staged_promotion_replica(state, &key, segment_id, offset)
                }
                _ => Vec::new(),
            };
            if let Some(mut local_disk) = state.local_disk_segments.get_mut(&task.storage_id) {
                local_disk.promotion_objects.remove(&key);
            }
            let authoritative_object = state.objects.get(&key).map(|object| object.clone());
            let delayed = match state.schedule_delayed_replica_release_or_fence(
                &key,
                authoritative_object,
                removed,
                Some(system_now),
                "reap_expired_promotion",
            ) {
                Ok(release_id) => release_id.is_some(),
                Err(_) => return,
            };
            if !delayed
                && state
                    .persist_object_image_or_remove_or_fence(&key, "reap_expired_promotion")
                    .is_err()
            {
                return;
            }
        }
    }
    reap_expired_delayed_replica_releases(state, system_now);
    if state.service_fenced.load(AtomicOrdering::Acquire) {
        return;
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

fn reap_expired_delayed_replica_releases(state: &MasterState, now: SystemTime) {
    let now_epoch_ms = match now.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => return,
    };
    let expired = state
        .delayed_replica_releases
        .iter()
        .filter(|entry| entry.deadline_epoch_ms <= now_epoch_ms)
        .map(|entry| (entry.id, entry.scoped_key.clone()))
        .collect::<Vec<_>>();
    for (release_id, scoped_key) in expired {
        let _mutation_guard = state.key_mutations.lock(&scoped_key);
        let Some(entry) = state
            .delayed_replica_releases
            .get(&release_id)
            .map(|entry| entry.clone())
        else {
            continue;
        };
        if entry.deadline_epoch_ms > now_epoch_ms {
            continue;
        }
        if state
            .persist_delayed_replica_release_removal_or_fence(
                &entry,
                "reap_delayed_replica_release",
            )
            .is_err()
        {
            return;
        }
        let Some((_, removed)) = state.delayed_replica_releases.remove(&release_id) else {
            continue;
        };
        if release_replicas(state, &removed.replicas).is_err() {
            return;
        }
    }
}

fn reap_client_tasks(state: &MasterState) {
    // Task status/removal is independent of a single object key, so use the
    // exclusive barrier to prevent snapshots from observing a half-applied
    // timeout/reaping pass.
    let _global_mutation_guard = state.key_mutations.lock_snapshot();
    let now = chrono::Utc::now();
    let pending_timeout = state.runtime_config.pending_task_timeout;
    let processing_timeout = state.runtime_config.processing_task_timeout;
    let mut timed_out_ids = Vec::new();
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
            timed_out_ids.push(task.info.id);
        }
    }
    if !timed_out_ids.is_empty()
        && state
            .persist_task_state_batch_or_fence(&timed_out_ids, &[], "reap_timed_out_tasks")
            .is_err()
    {
        return;
    }

    // A Drain job is the consumer of its unit task's terminal status. Keep
    // that status available even when the generic finished-task retention is
    // zero; otherwise the Drain refresher would observe a missing task and
    // misclassify a successful move as a failed unit. Once the Drain job
    // removes the task from `active_tasks`, the next reaper pass may delete it.
    let active_drain_task_ids = state
        .drain_jobs
        .iter()
        .flat_map(|job| job.active_tasks.keys().copied().collect::<Vec<_>>())
        .collect::<HashSet<_>>();
    let mut finished = state
        .tasks
        .iter()
        .filter(|task| matches!(task.info.status, TaskStatus::Success | TaskStatus::Failed))
        .filter(|task| !active_drain_task_ids.contains(task.key()))
        .map(|task| (*task.key(), task.info.last_updated_at))
        .collect::<Vec<_>>();
    finished.sort_by_key(|(_, updated_at)| *updated_at);
    let excess = finished
        .len()
        .saturating_sub(state.runtime_config.max_total_finished_tasks);
    let removed_ids = finished
        .into_iter()
        .take(excess)
        .map(|(task_id, _)| task_id)
        .collect::<Vec<_>>();
    if !removed_ids.is_empty()
        && state
            .persist_task_state_batch_or_fence(&[], &removed_ids, "reap_finished_tasks")
            .is_err()
    {
        return;
    }
    for task_id in &removed_ids {
        state.tasks.remove(task_id);
    }
}

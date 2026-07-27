use super::*;
use crate::TenantId;

// ============================================================================
// Drain Job Processing / Drain 任务处理
// ============================================================================

/// Maximum retry count per drain unit; matches C++ kMaxDrainUnitRetries.
const K_MAX_DRAIN_UNIT_RETRIES: u32 = 3;

/// Periodic drain job processing: refresh task statuses, update counters, retry
/// failed tasks, and mark completed jobs.
/// C++ equivalent: MasterService::ProcessDrainJobs() in master_service.cpp:6906
pub(crate) fn process_drain_jobs(state: &MasterState) {
    // Drain job bookkeeping and task insertion span several maps and segment
    // status fields; snapshot them as one mutation epoch.
    let _global_mutation_guard = state.key_mutations.lock_snapshot();
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
                let Some(failed_units) = job.failed_units.checked_add(1) else {
                    drop(job);
                    state.fence_after_invariant_failure(
                        "drain task refresh",
                        "failed unit counter overflow",
                    );
                    return;
                };
                job.failed_units = failed_units;
                // C++ treats a task that vanished from the authoritative task
                // table as terminal for this Drain unit. There is no result
                // left to retry safely.
                job.terminal_failed_unit_keys.insert(unit_key);
                job.active_tasks.remove(&task_id);
            }
            Some(task) => match task.info.status {
                TaskStatus::Pending | TaskStatus::Processing => {}
                TaskStatus::Success => {
                    let Some(succeeded_units) = job.succeeded_units.checked_add(1) else {
                        drop(job);
                        state.fence_after_invariant_failure(
                            "drain task refresh",
                            "succeeded unit counter overflow",
                        );
                        return;
                    };
                    let Some(migrated_bytes) = job.migrated_bytes.checked_add(bytes) else {
                        drop(job);
                        state.fence_after_invariant_failure(
                            "drain task refresh",
                            "migrated byte counter overflow",
                        );
                        return;
                    };
                    job.succeeded_units = succeeded_units;
                    job.migrated_bytes = migrated_bytes;
                    job.completed_unit_keys.insert(unit_key);
                    job.active_tasks.remove(&task_id);
                }
                TaskStatus::Failed => {
                    let Some(failed_units) = job.failed_units.checked_add(1) else {
                        drop(job);
                        state.fence_after_invariant_failure(
                            "drain task refresh",
                            "failed unit counter overflow",
                        );
                        return;
                    };
                    job.failed_units = failed_units;
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
}

fn maybe_complete_drain_job(state: &MasterState, job_id: Uuid) -> bool {
    use crate::service::state::ActiveDrainTask;
    let Some(mut job) = state.drain_jobs.get_mut(&job_id) else {
        return true;
    };
    if !job.active_tasks.is_empty() {
        return false;
    }
    let draining_segment_ids = job
        .source_segments
        .iter()
        .map(|source| (source.id, source.replica_type))
        .collect::<HashSet<_>>();
    let mut remaining = false;
    for entry in state.objects.iter() {
        for replica in &entry.replicas {
            if draining_segment_ids.contains(&(replica.segment_id, replica.replica_type)) {
                remaining = true;
                break;
            }
        }
        if remaining {
            break;
        }
    }
    if !remaining {
        let mut resolved_sources = Vec::with_capacity(job.source_segments.len());
        for source in &job.source_segments {
            let current = match source.replica_type {
                ReplicaType::Memory => state
                    .segments
                    .get(&source.id)
                    .map(|entry| (entry.segment.name.clone(), entry.status)),
                ReplicaType::NoFSsd => state
                    .nof_segments
                    .get(&source.id)
                    .map(|entry| (entry.segment.name.clone(), entry.status)),
                _ => None,
            };
            let Some((name, status)) = current else {
                // An explicit durable unmount may have removed an already
                // empty source. No replacement with the same name inherits
                // this job because the identity is fixed by UUID.
                continue;
            };
            if name != source.name || status != proto::SegmentStatus::Draining {
                let detail = format!(
                    "source identity or status changed for segment {}",
                    source.id
                );
                drop(job);
                state.fence_after_invariant_failure("drain completion", &detail);
                return true;
            }
            resolved_sources.push((source.id, source.replica_type));
        }
        if !resolved_sources.is_empty() {
            let durable_statuses = resolved_sources
                .iter()
                .map(|(segment_id, replica_type)| {
                    (
                        *segment_id,
                        *replica_type == ReplicaType::NoFSsd,
                        proto::SegmentStatus::Unavailable as i32,
                    )
                })
                .collect::<Vec<_>>();
            if let Err(error) = state
                .oplog_manager
                .lock()
                .record_segment_status_batch_durable(&durable_statuses)
            {
                state.fence_after_durability_failure("drain completion", &error);
                return true;
            }
        }
        for (segment_id, replica_type) in resolved_sources {
            if replica_type == ReplicaType::NoFSsd {
                state
                    .nof_segments
                    .get_mut(&segment_id)
                    .expect("validated NoF source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Unavailable;
            } else {
                state
                    .segments
                    .get_mut(&segment_id)
                    .expect("validated Memory source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Unavailable;
            }
        }
        job.status = proto::JobStatus::Succeeded;
        job.message = "Drain job finished successfully".into();
        job.last_updated_at = SystemTime::now();
        tracing::info!("Drain job succeeded: id={}", job_id);
        return true;
    }
    let mut retryable = false;
    let mut remaining_source_ids = HashSet::new();
    for entry in state.objects.iter() {
        for replica in &entry.replicas {
            if draining_segment_ids.contains(&(replica.segment_id, replica.replica_type)) {
                remaining_source_ids.insert((replica.segment_id, replica.replica_type));
                let unit_key = ActiveDrainTask::unit_key_for(
                    entry.key(),
                    replica.segment_id,
                    replica.replica_type,
                );
                if !job.completed_unit_keys.contains(&unit_key)
                    && !job.terminal_failed_unit_keys.contains(&unit_key)
                {
                    retryable = true;
                }
            }
        }
    }
    if !retryable {
        // C++ MaybeCompleteDrainJob restores every still-mounted source to OK
        // when all remaining units have exhausted their retries. Keeping the
        // segments Draining after the terminal job would permanently remove
        // capacity from allocation and prevent a later Drain retry.
        let mut resolved_sources = Vec::with_capacity(job.source_segments.len());
        for source in &job.source_segments {
            let current = match source.replica_type {
                ReplicaType::Memory => state
                    .segments
                    .get(&source.id)
                    .map(|entry| (entry.segment.name.clone(), entry.status)),
                ReplicaType::NoFSsd => state
                    .nof_segments
                    .get(&source.id)
                    .map(|entry| (entry.segment.name.clone(), entry.status)),
                _ => None,
            };
            let Some((name, status)) = current else {
                if remaining_source_ids.contains(&(source.id, source.replica_type)) {
                    let detail = format!(
                        "remaining Drain replica references missing source segment {}",
                        source.id
                    );
                    drop(job);
                    state.fence_after_invariant_failure("drain terminal failure", &detail);
                    return true;
                }
                // An empty source may have been explicitly unmounted while
                // the job was running. Do not recreate or name-resolve it.
                continue;
            };
            if name != source.name || status != proto::SegmentStatus::Draining {
                let detail = format!(
                    "source identity or status changed for segment {}",
                    source.id
                );
                drop(job);
                state.fence_after_invariant_failure("drain terminal failure", &detail);
                return true;
            }
            resolved_sources.push((source.id, source.replica_type));
        }
        if !resolved_sources.is_empty() {
            let durable_statuses = resolved_sources
                .iter()
                .map(|(segment_id, replica_type)| {
                    (
                        *segment_id,
                        *replica_type == ReplicaType::NoFSsd,
                        proto::SegmentStatus::Active as i32,
                    )
                })
                .collect::<Vec<_>>();
            if let Err(error) = state
                .oplog_manager
                .lock()
                .record_segment_status_batch_durable(&durable_statuses)
            {
                state.fence_after_durability_failure("drain terminal failure", &error);
                return true;
            }
        }
        for (segment_id, replica_type) in resolved_sources {
            if replica_type == ReplicaType::NoFSsd {
                state
                    .nof_segments
                    .get_mut(&segment_id)
                    .expect("validated NoF source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Active;
            } else {
                state
                    .segments
                    .get_mut(&segment_id)
                    .expect("validated Memory source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Active;
            }
        }
        job.status = proto::JobStatus::Failed;
        job.message = "Drain job failed: unrecoverable units remain".into();
        job.last_updated_at = SystemTime::now();
        tracing::warn!("Drain job failed: id={}", job_id);
        return true;
    }
    false
}

fn schedule_drain_job_tasks_free(state: &MasterState, job_id: Uuid) {
    use crate::service::state::ActiveDrainTask;
    use mooncake_store_core::{TaskInfo, TaskStatus, TaskType};
    use serde::Serialize;
    let Some(mut job) = state.drain_jobs.get_mut(&job_id) else {
        return;
    };
    let draining_segments: HashSet<String> = job.segments.iter().cloned().collect();
    let draining_segment_ids = job
        .source_segments
        .iter()
        .map(|source| (source.id, source.replica_type))
        .collect::<HashSet<_>>();
    let targets = if job.target_segments.is_empty() {
        default_drain_target_segments(state, &draining_segments)
    } else {
        job.target_segments.clone()
    };
    let max_concurrency = job.max_concurrency as usize;
    let available = max_concurrency.saturating_sub(job.active_tasks.len());
    if job.status == proto::JobStatus::Created {
        job.status = proto::JobStatus::Planning;
    }
    if available == 0 {
        job.status = proto::JobStatus::Running;
        return;
    }
    let mut units: Vec<(TenantId, String, String, Uuid, ReplicaType, String, u64)> = Vec::new();
    let mut blocked_unit_keys = HashSet::new();
    for entry in state.objects.iter() {
        let scoped_key = entry.key().clone();
        if entry.hard_pinned
            || !is_lease_expired(entry.value())
            || !entry
                .replicas
                .iter()
                .all(|replica| replica.status == ReplicaStatus::Complete)
            || state.replication_tasks.contains_key(entry.key())
        {
            for replica in &entry.replicas {
                if draining_segment_ids.contains(&(replica.segment_id, replica.replica_type)) {
                    blocked_unit_keys.insert(ActiveDrainTask::unit_key_for(
                        entry.key(),
                        replica.segment_id,
                        replica.replica_type,
                    ));
                }
            }
            continue;
        }
        let mut seen_source_segments = HashSet::new();
        for replica in &entry.replicas {
            if draining_segment_ids.contains(&(replica.segment_id, replica.replica_type))
                && replica.status == ReplicaStatus::Complete
            {
                let unit_key = ActiveDrainTask::unit_key_for(
                    &scoped_key,
                    replica.segment_id,
                    replica.replica_type,
                );
                if !job.completed_unit_keys.contains(&unit_key)
                    && !job.terminal_failed_unit_keys.contains(&unit_key)
                    && !job.active_tasks.values().any(|t| t.unit_key == unit_key)
                    && seen_source_segments.insert((replica.segment_id, replica.replica_type))
                {
                    units.push((
                        entry.tenant_id.clone(),
                        entry.user_key.clone(),
                        scoped_key.clone(),
                        replica.segment_id,
                        replica.replica_type,
                        replica.segment_name.clone(),
                        replica.size,
                    ));
                }
            }
        }
    }
    let mut scheduled = 0;
    let mut scheduled_task_ids = Vec::new();
    for (
        tenant_id,
        user_key,
        scoped_key,
        source_segment_id,
        source_replica_type,
        source_seg,
        bytes,
    ) in units
    {
        if scheduled >= available {
            break;
        }
        let unit_key =
            ActiveDrainTask::unit_key_for(&scoped_key, source_segment_id, source_replica_type);
        let Some(object) = state.objects.get(&scoped_key) else {
            continue;
        };
        let Some(target_seg) = choose_drain_target_segment(state, &object, &source_seg, &targets)
        else {
            blocked_unit_keys.insert(unit_key.clone());
            continue;
        };
        let Some(target_segment_id) = unique_active_memory_segment_id(state, &target_seg) else {
            blocked_unit_keys.insert(unit_key.clone());
            continue;
        };
        drop(object);
        if !has_pending_task_capacity(state) {
            break;
        }
        #[derive(Serialize)]
        struct ReplicaMovePayload<'a> {
            tenant_id: &'a TenantId,
            key: &'a str,
            source: &'a str,
            target: &'a str,
        }
        let Some(assigned_client) =
            client_id_by_replica_segment_id(state, source_segment_id, source_replica_type)
        else {
            blocked_unit_keys.insert(unit_key);
            continue;
        };
        let payload = match serde_json::to_string(&ReplicaMovePayload {
            tenant_id: &tenant_id,
            key: &user_key,
            source: &source_seg,
            target: &target_seg,
        }) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!(
                    %error,
                    key = %scoped_key,
                    source = %source_seg,
                    target = %target_seg,
                    "failed to serialize background drain move task"
                );
                blocked_unit_keys.insert(unit_key);
                continue;
            }
        };
        let task_id = unique_task_id(state);
        let now = chrono::Utc::now();
        state.tasks.insert(
            task_id,
            crate::service::state::TaskEntry {
                info: TaskInfo {
                    id: task_id,
                    task_type: TaskType::ReplicaMove,
                    status: TaskStatus::Pending,
                    created_at: now,
                    last_updated_at: now,
                    assigned_client: Some(assigned_client),
                    message: String::new(),
                },
                key: scoped_key,
                payload,
                max_retry_attempts: state.runtime_config.max_task_retry_attempts,
            },
        );
        job.active_tasks.insert(
            task_id,
            ActiveDrainTask {
                source_segment_id,
                source_replica_type,
                source_segment: source_seg,
                target_segment: target_seg,
                target_segment_id,
                target_replica_type: ReplicaType::Memory,
                bytes,
                unit_key,
            },
        );
        scheduled_task_ids.push(task_id);
        scheduled += 1;
    }
    if !scheduled_task_ids.is_empty()
        && state
            .persist_task_state_batch_or_fence(
                &scheduled_task_ids,
                &[],
                "background_drain_schedule_tasks",
            )
            .is_err()
    {
        return;
    }
    job.blocked_units = blocked_unit_keys.len() as u64;
    job.status = proto::JobStatus::Running;
    job.last_updated_at = SystemTime::now();
    job.message = "Drain job running".into();
}

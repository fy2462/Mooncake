use super::*;

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
    use crate::service::state::ActiveDrainTask;
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
    use crate::service::state::ActiveDrainTask;
    use mooncake_store_core::{TaskInfo, TaskStatus, TaskType};
    use serde::Serialize;
    let Some(mut job) = state.drain_jobs.get_mut(&job_id) else {
        return;
    };
    let draining_segments: HashSet<String> = job.segments.iter().cloned().collect();
    let targets = if job.target_segments.is_empty() {
        default_drain_target_segments(state, &draining_segments)
    } else {
        job.target_segments.clone()
    };
    let max_concurrency = job.max_concurrency as usize;
    let available = max_concurrency.saturating_sub(job.active_tasks.len());
    if available == 0 {
        return;
    }
    let mut units: Vec<(String, String, u64)> = Vec::new();
    let mut blocked_unit_keys = HashSet::new();
    for entry in state.objects.iter() {
        let key = entry.key().clone();
        if !entry.tenant_id.is_default()
            || entry.hard_pinned
            || !is_lease_expired(entry.value())
            || !entry
                .replicas
                .iter()
                .all(|replica| replica.status == ReplicaStatus::Complete)
            || state.replication_tasks.contains_key(entry.key())
        {
            for replica in &entry.replicas {
                if draining_segments.contains(&replica.segment_name) {
                    blocked_unit_keys.insert(ActiveDrainTask::unit_key_for(
                        entry.key(),
                        &replica.segment_name,
                    ));
                }
            }
            continue;
        }
        let mut seen_source_segments = HashSet::new();
        for replica in &entry.replicas {
            if draining_segments.contains(&replica.segment_name)
                && replica.status == ReplicaStatus::Complete
            {
                let unit_key = ActiveDrainTask::unit_key_for(&key, &replica.segment_name);
                if !job.completed_unit_keys.contains(&unit_key)
                    && !job.terminal_failed_unit_keys.contains(&unit_key)
                    && !job.active_tasks.values().any(|t| t.unit_key == unit_key)
                    && seen_source_segments.insert(replica.segment_name.clone())
                {
                    units.push((key.clone(), replica.segment_name.clone(), replica.size));
                }
            }
        }
    }
    let mut scheduled = 0;
    for (key, source_seg, bytes) in units {
        if scheduled >= available {
            break;
        }
        let unit_key = ActiveDrainTask::unit_key_for(&key, &source_seg);
        let Some(object) = state.objects.get(&key) else {
            continue;
        };
        let Some(target_seg) = choose_drain_target_segment(state, &object, &source_seg, &targets)
        else {
            blocked_unit_keys.insert(unit_key.clone());
            continue;
        };
        drop(object);
        if !has_pending_task_capacity(state) {
            break;
        }
        let task_id = Uuid::new_v4();
        #[derive(Serialize)]
        struct ReplicaMovePayload {
            key: String,
            source: String,
            target: String,
        }
        let payload = serde_json::to_string(&ReplicaMovePayload {
            key: key.clone(),
            source: source_seg.clone(),
            target: target_seg.clone(),
        })
        .unwrap_or_default();
        let assigned_client = client_id_by_segment_name(state, &source_seg);
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
                    assigned_client,
                    message: format!("drain {key} from {source_seg} to {target_seg}"),
                },
                key: key.clone(),
                payload,
                max_retry_attempts: state.runtime_config.max_task_retry_attempts,
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
    job.blocked_units = blocked_unit_keys.len() as u64;
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

use super::super::*;
use crate::service::state::DrainSourceSegment;

impl MasterServiceImpl {
    // ---- CreateDrainJob ----
    // 创建 Drain 任务：将指定 source segments 上的数据迁移到 target segments。
    // 校验源/目标 segment 均处于 Active 状态后，将源 segment 标记为 Draining 并创建 job。
    // 调度后台任务逐 key 执行副本拷贝（ReplicaCopy），支持并发控制和重试。
    //
    // Create a Drain job: migrate data from source segments to target segments.
    // Validates that source/target segments are Active, marks source segments as Draining,
    // creates the job, and schedules per-key ReplicaCopy tasks with concurrency control and retry.
    pub(crate) async fn create_drain_job_impl(
        &self,
        request: Request<proto::CreateDrainJobRequest>,
    ) -> Result<Response<proto::CreateDrainJobResponse>, Status> {
        let req = request.into_inner();
        if req.segments.is_empty() {
            return Err(Status::invalid_argument("segments cannot be empty"));
        }
        if req.max_concurrency == 0 {
            return Err(Status::invalid_argument("max_concurrency must be non-zero"));
        }
        let unique_sources: HashSet<String> = req.segments.iter().cloned().collect();
        if unique_sources.len() != req.segments.len() {
            return Err(Status::invalid_argument("segments must be unique"));
        }
        if req
            .target_segments
            .iter()
            .any(|target| unique_sources.contains(target))
        {
            return Err(Status::invalid_argument(
                "target_segments cannot include draining segments",
            ));
        }
        // Source/target validation and the transition to DRAINING must share
        // one mutation epoch. Otherwise two concurrent CreateDrainJob calls
        // can both observe ACTIVE and install overlapping jobs.
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        // Validate that every name resolves to exactly one source. Rust permits
        // same-name segments with different UUIDs, while this legacy Drain API
        // carries names only; ambiguity must fail closed.
        let mut resolved_sources = Vec::with_capacity(req.segments.len());
        for seg_name in &req.segments {
            let memory = self
                .state
                .segments
                .iter()
                .filter(|entry| entry.segment.name == *seg_name)
                .map(|entry| (entry.segment.id, false, entry.status))
                .collect::<Vec<_>>();
            let nof = self
                .state
                .nof_segments
                .iter()
                .filter(|entry| entry.segment.name == *seg_name)
                .map(|entry| (entry.segment.id, true, entry.status))
                .collect::<Vec<_>>();
            if memory.len() + nof.len() != 1 {
                return Err(Status::failed_precondition(format!(
                    "source segment name must resolve to exactly one segment: {seg_name}"
                )));
            }
            let source = memory
                .into_iter()
                .chain(nof.into_iter())
                .next()
                .expect("exactly one source was validated");
            if source.2 != proto::SegmentStatus::Active {
                return Err(Status::failed_precondition(format!(
                    "segment not active: {seg_name}"
                )));
            }
            resolved_sources.push(DrainSourceSegment {
                id: source.0,
                replica_type: if source.1 {
                    ReplicaType::NoFSsd
                } else {
                    ReplicaType::Memory
                },
                name: seg_name.clone(),
            });
        }
        // Validate that all target segments exist
        // 校验所有目标 segment 存在且处于 Active 状态
        for tgt_name in &req.target_segments {
            let memory = self
                .state
                .segments
                .iter()
                .filter(|entry| entry.segment.name == *tgt_name)
                .map(|entry| entry.status)
                .collect::<Vec<_>>();
            let nof_count = self
                .state
                .nof_segments
                .iter()
                .filter(|entry| entry.segment.name == *tgt_name)
                .count();
            if memory.len() != 1 || nof_count != 0 || memory[0] != proto::SegmentStatus::Active {
                return Err(Status::failed_precondition(format!(
                    "target segment name must resolve to one active Memory segment: {tgt_name}"
                )));
            }
        }
        let durable_statuses = resolved_sources
            .iter()
            .map(|source| {
                (
                    source.id,
                    source.replica_type == ReplicaType::NoFSsd,
                    proto::SegmentStatus::Draining as i32,
                )
            })
            .collect::<Vec<_>>();
        self.state
            .oplog_manager
            .record_segment_status_batch_durable(&durable_statuses)
            .map_err(|error| {
                Status::unavailable(format!(
                    "failed to persist drain source transition: {error}"
                ))
            })?;
        // Transition source segments to DRAINING
        // 将源 segment 转为 Draining 状态
        for source in &resolved_sources {
            if source.replica_type == ReplicaType::NoFSsd {
                self.state
                    .nof_segments
                    .get_mut(&source.id)
                    .expect("validated NoF source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Draining;
            } else {
                self.state
                    .segments
                    .get_mut(&source.id)
                    .expect("validated Memory source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Draining;
            }
        }
        let job_id = Uuid::new_v4();
        let now = SystemTime::now();
        self.state.drain_jobs.insert(
            job_id,
            DrainJobEntry {
                id: job_id,
                status: proto::JobStatus::Created,
                segments: req.segments.clone(),
                source_segments: resolved_sources,
                target_segments: req.target_segments.clone(),
                max_concurrency: req.max_concurrency.max(1),
                created_at: now,
                last_updated_at: now,
                message: "Drain job created".into(),
                succeeded_units: 0,
                failed_units: 0,
                blocked_units: 0,
                migrated_bytes: 0,
                active_tasks: HashMap::new(),
                completed_unit_keys: HashSet::new(),
                terminal_failed_unit_keys: HashSet::new(),
                retry_counts: HashMap::new(),
            },
        );
        // Start planning immediately (find objects to drain)
        // 立即开始 planning 阶段（查找需要 drain 的对象）
        if let Some(mut job) = self.state.drain_jobs.get_mut(&job_id) {
            job.status = proto::JobStatus::Planning;
        }
        self.schedule_drain_job_tasks(job_id);
        if self.is_service_fenced() {
            return Err(Status::unavailable(
                "master fenced while persisting drain task scheduling",
            ));
        }
        tracing::info!(
            "Drain job created: id={}, segments={:?}, targets={:?}",
            job_id,
            req.segments,
            req.target_segments
        );
        Ok(Response::new(proto::CreateDrainJobResponse {
            job_id: Some(uuid_to_proto(job_id)),
        }))
    }

    // ---- QueryDrainJob ----
    // 查询 Drain 任务进度：返回状态、成功/失败/阻塞任务数、活跃任务数和已迁移字节数。
    // Query Drain job progress: returns status, succeeded/failed/blocked task counts, active tasks, and migrated bytes.
    pub(crate) async fn query_drain_job_impl(
        &self,
        request: Request<proto::QueryDrainJobRequest>,
    ) -> Result<Response<proto::QueryDrainJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = uuid_from_proto(
            req.job_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing job_id"))?,
        );
        let job = self
            .state
            .drain_jobs
            .get(&job_id)
            .ok_or(Status::not_found("drain job not found"))?;
        Ok(Response::new(proto::QueryDrainJobResponse {
            id: Some(uuid_to_proto(job.id)),
            r#type: proto::JobType::Drain as i32,
            status: job.status as i32,
            created_at_ms_epoch: job
                .created_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            last_updated_at_ms_epoch: job
                .last_updated_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            segments: job.segments.clone(),
            succeeded_units: job.succeeded_units,
            failed_units: job.failed_units,
            blocked_units: job.blocked_units,
            active_units: job.active_tasks.len() as u64,
            migrated_bytes: job.migrated_bytes,
            message: job.message.clone(),
        }))
    }

    // ---- CancelDrainJob ----
    // 取消 Drain 任务：将 draining segment 恢复为 Active 状态，job 标记为 Canceled。
    // 已处于终态（Success/Failed/Canceled）的 job 不允许重复取消。
    //
    // Cancel Drain job: restore draining segments to Active; mark job as Canceled.
    // Jobs already in terminal state (Success/Failed/Canceled) cannot be re-canceled.
    pub(crate) async fn cancel_drain_job_impl(
        &self,
        request: Request<proto::CancelDrainJobRequest>,
    ) -> Result<Response<proto::CancelDrainJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = uuid_from_proto(
            req.job_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing job_id"))?,
        );
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let mut job = self
            .state
            .drain_jobs
            .get_mut(&job_id)
            .ok_or(Status::not_found("drain job not found"))?;
        if job.status == proto::JobStatus::Succeeded
            || job.status == proto::JobStatus::Failed
            || job.status == proto::JobStatus::Canceled
            || !job.active_tasks.is_empty()
        {
            return Err(Status::failed_precondition(
                "drain job is terminal or still has active move tasks",
            ));
        }
        for source in &job.source_segments {
            let current = match source.replica_type {
                ReplicaType::Memory => self
                    .state
                    .segments
                    .get(&source.id)
                    .map(|entry| (entry.segment.name.clone(), entry.status)),
                ReplicaType::NoFSsd => self
                    .state
                    .nof_segments
                    .get(&source.id)
                    .map(|entry| (entry.segment.name.clone(), entry.status)),
                _ => None,
            };
            let Some((name, status)) = current else {
                return Err(Status::failed_precondition(format!(
                    "drain source segment disappeared: {}",
                    source.id
                )));
            };
            if name != source.name || status != proto::SegmentStatus::Draining {
                return Err(Status::failed_precondition(format!(
                    "drain source identity or status changed: {}",
                    source.id
                )));
            }
        }
        let durable_statuses = job
            .source_segments
            .iter()
            .map(|source| {
                (
                    source.id,
                    source.replica_type == ReplicaType::NoFSsd,
                    proto::SegmentStatus::Active as i32,
                )
            })
            .collect::<Vec<_>>();
        self.state
            .oplog_manager
            .record_segment_status_batch_durable(&durable_statuses)
            .map_err(|error| {
                Status::unavailable(format!("failed to persist drain cancellation: {error}"))
            })?;
        // Restore draining segments back to ACTIVE
        // 将 draining segment 恢复为 Active
        for source in &job.source_segments {
            if source.replica_type == ReplicaType::NoFSsd {
                self.state
                    .nof_segments
                    .get_mut(&source.id)
                    .expect("validated NoF source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Active;
            } else {
                self.state
                    .segments
                    .get_mut(&source.id)
                    .expect("validated Memory source disappeared inside mutation epoch")
                    .status = proto::SegmentStatus::Active;
            }
        }
        job.status = proto::JobStatus::Canceled;
        job.last_updated_at = SystemTime::now();
        job.message = "Drain job canceled".into();
        tracing::info!("Drain job canceled: id={}", job_id);
        Ok(Response::new(proto::CancelDrainJobResponse {}))
    }

    /// Helper: 查找 draining segment 上的所有对象并为每个 key 创建 ReplicaCopy 任务。
    /// 目标 segment 按 round-robin 分配，避免单目标热点。每个 key 只创建一个 drain unit。
    ///
    /// Helper: find all objects on draining segments and create ReplicaCopy tasks per key.
    /// Target segments are assigned round-robin to avoid single-target hotspots. One drain unit per key.
    pub(crate) fn schedule_drain_job_tasks(&self, job_id: Uuid) {
        let mut job = match self.state.drain_jobs.get_mut(&job_id) {
            Some(j) => j,
            None => return,
        };
        let draining_segments: HashSet<String> = job.segments.iter().cloned().collect();
        let targets = if job.target_segments.is_empty() {
            default_drain_target_segments(&self.state, &draining_segments)
        } else {
            job.target_segments.clone()
        };
        let max_concurrency = job.max_concurrency as usize;
        let draining_segment_ids = job
            .source_segments
            .iter()
            .map(|source| (source.id, source.replica_type))
            .collect::<HashSet<_>>();

        // Find objects with replicas on draining segments
        // 查找在 draining segment 上有副本的对象
        let mut units: Vec<(TenantId, String, String, Uuid, ReplicaType, String, u64)> = Vec::new();
        let mut blocked_unit_keys = HashSet::new();
        for entry in self.state.objects.iter() {
            let scoped_key = entry.key().clone();
            if entry.hard_pinned
                || !is_lease_expired(entry.value())
                || !entry
                    .replicas
                    .iter()
                    .all(|replica| replica.status == ReplicaStatus::Complete)
                || self.state.replication_tasks.contains_key(entry.key())
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
            if job.active_tasks.len() >= max_concurrency {
                break;
            }
            let unit_key =
                ActiveDrainTask::unit_key_for(&scoped_key, source_segment_id, source_replica_type);
            if job.completed_unit_keys.contains(&unit_key)
                || job.terminal_failed_unit_keys.contains(&unit_key)
            {
                continue;
            }
            let Some(object) = self.state.objects.get(&scoped_key) else {
                continue;
            };
            let Some(target_seg) =
                choose_drain_target_segment(&self.state, &object, &source_seg, &targets)
            else {
                blocked_unit_keys.insert(unit_key.clone());
                continue;
            };
            let Some(target_segment_id) = unique_active_memory_segment_id(&self.state, &target_seg)
            else {
                blocked_unit_keys.insert(unit_key.clone());
                continue;
            };
            drop(object);
            if !has_pending_task_capacity(&self.state) {
                break;
            }
            let Some(assigned_client) = client_id_by_replica_segment_id(
                &self.state,
                source_segment_id,
                source_replica_type,
            ) else {
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
                        "failed to serialize drain move task"
                    );
                    blocked_unit_keys.insert(unit_key);
                    continue;
                }
            };
            let task_id = unique_task_id(&self.state);
            job.active_tasks.insert(
                task_id,
                ActiveDrainTask {
                    source_segment_id,
                    source_replica_type,
                    source_segment: source_seg.clone(),
                    target_segment: target_seg.clone(),
                    target_segment_id,
                    target_replica_type: ReplicaType::Memory,
                    bytes,
                    unit_key: unit_key.clone(),
                },
            );
            let now = Utc::now();
            self.state.tasks.insert(
                task_id,
                TaskEntry {
                    info: TaskInfo {
                        id: task_id,
                        task_type: TaskType::ReplicaMove,
                        status: TaskStatus::Pending,
                        created_at: now,
                        last_updated_at: now,
                        assigned_client: Some(assigned_client),
                        message: String::new(),
                    },
                    key: scoped_key.clone(),
                    payload,
                    max_retry_attempts: self.state.runtime_config.max_task_retry_attempts,
                },
            );
            scheduled_task_ids.push(task_id);
        }
        if !scheduled_task_ids.is_empty()
            && self
                .state
                .persist_task_state_batch_or_fence(&scheduled_task_ids, &[], "drain_schedule_tasks")
                .is_err()
        {
            return;
        }
        job.blocked_units = blocked_unit_keys.len() as u64;

        // Scheduling alone cannot prove completion: the source may still
        // contain blocked replicas or replicas without an eligible target.
        // The background completion pass is the single authority that checks
        // the live object catalog before assigning a terminal job status.
        job.status = proto::JobStatus::Running;
        job.last_updated_at = SystemTime::now();
        job.message = "Drain job running".into();
    }

    // ---- GetFsdir ----
    // 返回 master 配置的存储文件系统目录路径，供客户端本地文件存储使用。
    // Returns the master's configured storage filesystem directory path for client local file storage.
    pub(crate) async fn get_fsdir_impl(
        &self,
        _request: Request<proto::GetFsdirRequest>,
    ) -> Result<Response<proto::GetFsdirResponse>, Status> {
        let fs_dir = storage_fs_dir_for_client(&self.state.runtime_config);
        Ok(Response::new(proto::GetFsdirResponse { fs_dir }))
    }
}

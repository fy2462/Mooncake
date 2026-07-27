//! # Task Queue — 任务队列管理 / Task Queue Management
//!
//! ============================================================================
//! gRPC task management handlers — master-side implementation.
//! gRPC 任务管理处理器 —— master 侧实现。
//!
//! These handlers manage the server-side lifecycle of copy/move tasks:
//!   create (client submits) → queue (master assigns to worker) →
//!   fetch (worker claims) → mark_complete (worker reports result).
//!
//! 这些处理器管理复制/移动任务的服务端生命周期：
//!   创建（客户端提交） → 排队（master 分配给 worker） →
//   获取（worker 认领） → 标记完成（worker 报告结果）。
//
// C++ equivalent: master_service.cpp task-related RPC handlers.
// ============================================================================

use super::*;

impl MasterServiceImpl {
    // -----------------------------------------------------------------------
    // CreateCopyTask — replicate a key's data to additional segments
    // 创建复制任务 —— 将 key 的数据复制到额外的 segment
    //
    // Flow (流程):
    //   1. Validate input (key exists, targets are mounted segments)
    //      验证输入（key 存在，target 是已挂载的 segment）
    //   2. Serialise the task payload (source segment + target list) as JSON
    //      将任务负载（源 segment + 目标列表）序列化为 JSON
    //   3. Insert a Pending TaskEntry into the task map, assigned to the
    //      client that owns the source segment
    //      将 Pending 状态的 TaskEntry 插入任务映射表，分配给拥有源 segment 的客户端
    //
    // The assigned client will later pick up the task via FetchTasks and
    // execute the RDMA copy. / 分配的客户端稍后通过 FetchTasks 认领任务并执行 RDMA 复制。
    // C++ equivalent: MasterServiceImpl::CreateCopyTask()
    // -----------------------------------------------------------------------
    pub(super) async fn create_copy_task_impl(
        &self,
        request: Request<proto::CreateCopyTaskRequest>,
    ) -> Result<Response<proto::CreateCopyTaskResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);

        // Validate key — must exist in the object store.
        // 验证 key —— 必须存在于对象存储中。
        if req.key.is_empty() {
            return Err(Status::invalid_argument("missing key"));
        }
        if req.targets.is_empty() {
            return Err(Status::invalid_argument("missing targets"));
        }
        // C++ TaskManager write access serializes capacity admission, UUID
        // selection, and insertion globally.
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();

        // Look up the object to find its source replica.
        // 查找对象以找到其源副本。
        let object = self
            .state
            .objects
            .get(&scoped_key)
            .ok_or(Status::not_found("key not found"))?;
        if object.replicas.is_empty() {
            return Err(Status::failed_precondition("object has no source replicas"));
        }

        // Resolve every legacy name-only target to one exact active allocation
        // domain. Rust permits same-name segment mounts, so ambiguity must not
        // be delegated to the worker.
        for target in &req.targets {
            let Some((target_segment_id, target_replica_type)) =
                unique_active_replica_segment_identity(&self.state, target)
            else {
                return Err(Status::failed_precondition(format!(
                    "target segment is missing, inactive, or ambiguous: {target}"
                )));
            };
            if object.replicas.iter().any(|replica| {
                replica.segment_name == *target
                    && (replica.segment_id != target_segment_id
                        || replica.replica_type != target_replica_type)
            }) {
                return Err(Status::failed_precondition(format!(
                    "same-name target replica does not match the unique active target: {target}"
                )));
            }
        }

        // Choose a routable Memory/NoF source whose name is unique within the
        // object. The worker wire still carries a name, so serializing an
        // ambiguous source would make the later CopyStart nondeterministic.
        let source_candidates = object
            .replicas
            .iter()
            .filter(|replica| {
                replica.handle_valid
                    && matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd
                    )
                    && client_id_by_exact_replica_segment(&self.state, replica).is_some()
            })
            .collect::<Vec<_>>();
        let source = source_candidates
            .iter()
            .copied()
            .find(|candidate| {
                source_candidates
                    .iter()
                    .filter(|replica| replica.segment_name == candidate.segment_name)
                    .count()
                    == 1
            })
            .ok_or(Status::failed_precondition(
                "object has no unambiguous routable source replica",
            ))?;
        let source_segment = source.segment_name.clone();
        let assigned_client = client_id_by_exact_replica_segment(&self.state, source)
            .ok_or(Status::failed_precondition("source segment missing"))?;

        // Serialise the task payload — JSON string sent to the worker.
        // 序列化任务负载 —— 发送给 worker 的 JSON 字符串。
        let task_key = scoped_key.clone();
        let task_payload = serde_json::to_string(&ReplicaCopyPayload {
            tenant_id: &tenant_id,
            key: &req.key,
            source: &source_segment,
            targets: &req.targets,
        })
        .map_err(|e| Status::internal(format!("serialize task payload: {e}")))?;

        // Drop the read lock before re-checking existence (defensive).
        // 在重新检查存在性之前释放读锁（防御性）。
        drop(object);
        if !self.state.objects.contains_key(&scoped_key) {
            return Err(Status::not_found("key not found"));
        }
        if !has_pending_task_capacity(&self.state) {
            return Err(Status::resource_exhausted("pending task limit reached"));
        }

        // Create and store the task entry.
        // 创建并存储任务条目。
        let task_id = unique_task_id(&self.state);
        let now = Utc::now();
        self.state.tasks.insert(
            task_id,
            TaskEntry {
                info: TaskInfo {
                    id: task_id,
                    task_type: TaskType::ReplicaCopy,
                    status: TaskStatus::Pending,
                    created_at: now,
                    last_updated_at: now,
                    assigned_client: Some(assigned_client),
                    message: String::new(),
                },
                key: task_key,
                payload: task_payload,
                max_retry_attempts: self.state.runtime_config.max_task_retry_attempts,
            },
        );
        self.state
            .persist_task_state_batch_or_fence(&[task_id], &[], "create_copy_task")
            .map_err(|error| {
                Status::unavailable(format!("failed to persist create_copy_task: {error}"))
            })?;
        Ok(Response::new(proto::CreateCopyTaskResponse {
            task_id: Some(uuid_to_proto(task_id)),
        }))
    }

    // -----------------------------------------------------------------------
    // CreateMoveTask — migrate a key's replica from one segment to another
    // 创建移动任务 —— 将 key 的副本从一个 segment 迁移到另一个
    //
    // Unlike copy (which adds replicas), move transfers the replica and then
    // releases the source.  The master coordinates the hand-off: after the
    // worker confirms completion, the source replica is removed.
    //
    // 与 copy（添加副本）不同，move 先传输副本再释放源。
    // Master 协调交接：worker 确认完成后，源副本被移除。
    //
    // C++ equivalent: MasterServiceImpl::CreateMoveTask()
    // -----------------------------------------------------------------------
    pub(super) async fn create_move_task_impl(
        &self,
        request: Request<proto::CreateMoveTaskRequest>,
    ) -> Result<Response<proto::CreateMoveTaskResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);

        // All three fields are required. / 三个字段都是必需的。
        if req.key.is_empty() || req.source.is_empty() || req.target.is_empty() {
            return Err(Status::invalid_argument("missing key/source/target"));
        }
        if req.source == req.target {
            return Err(Status::invalid_argument("source and target must differ"));
        }
        // C++ TaskManager write access serializes capacity admission, UUID
        // selection, and insertion globally.
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();

        // Verify the object exists and has a replica on the source segment.
        // 验证对象存在且在源 segment 上有副本。
        let object = self
            .state
            .objects
            .get(&scoped_key)
            .ok_or(Status::not_found("key not found"))?;
        let source_candidates = object
            .replicas
            .iter()
            .filter(|replica| {
                replica.segment_name == req.source
                    && replica.handle_valid
                    && matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd
                    )
                    && client_id_by_exact_replica_segment(&self.state, replica).is_some()
            })
            .collect::<Vec<_>>();
        let source = match source_candidates.as_slice() {
            [source] => *source,
            [] => return Err(Status::invalid_argument("source segment not found")),
            _ => {
                return Err(Status::failed_precondition(
                    "source segment name is ambiguous",
                ));
            }
        };

        // The task is assigned to the client that owns the source segment.
        // 任务分配给拥有源 segment 的客户端。
        let assigned_client = client_id_by_exact_replica_segment(&self.state, source)
            .ok_or(Status::failed_precondition("source segment missing"))?;
        let Some((target_segment_id, target_replica_type)) =
            unique_active_replica_segment_identity(&self.state, &req.target)
        else {
            return Err(Status::failed_precondition(
                "target segment is missing, inactive, or ambiguous",
            ));
        };
        if object.replicas.iter().any(|replica| {
            replica.segment_name == req.target
                && (replica.segment_id != target_segment_id
                    || replica.replica_type != target_replica_type)
        }) {
            return Err(Status::failed_precondition(
                "same-name target replica does not match the unique active target",
            ));
        }

        // Serialise the move payload. / 序列化移动负载。
        let task_key = scoped_key.clone();
        let task_payload = serde_json::to_string(&ReplicaMovePayload {
            tenant_id: &tenant_id,
            key: &req.key,
            source: &req.source,
            target: &req.target,
        })
        .map_err(|e| Status::internal(format!("serialize task payload: {e}")))?;

        drop(object);
        if !has_pending_task_capacity(&self.state) {
            return Err(Status::resource_exhausted("pending task limit reached"));
        }

        // Create and store the move task. / 创建并存储移动任务。
        let task_id = unique_task_id(&self.state);
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
                key: task_key,
                payload: task_payload,
                max_retry_attempts: self.state.runtime_config.max_task_retry_attempts,
            },
        );
        self.state
            .persist_task_state_batch_or_fence(&[task_id], &[], "create_move_task")
            .map_err(|error| {
                Status::unavailable(format!("failed to persist create_move_task: {error}"))
            })?;
        Ok(Response::new(proto::CreateMoveTaskResponse {
            task_id: Some(uuid_to_proto(task_id)),
        }))
    }

    // -----------------------------------------------------------------------
    // QueryTask — read back the status of a previously-created task
    // 查询任务 —— 读取先前创建的任务的状态
    //
    // This is a read-only operation. The caller provides a task UUID and
    // receives the current TaskInfo (type, status, assigned client, timestamps).
    //
    // 这是一个只读操作。调用者提供任务 UUID，收到当前的 TaskInfo
    // （类型、状态、分配的客户端、时间戳）。
    //
    // C++ equivalent: MasterServiceImpl::QueryTask()
    // -----------------------------------------------------------------------
    pub(super) async fn query_task_impl(
        &self,
        request: Request<proto::QueryTaskRequest>,
    ) -> Result<Response<proto::QueryTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = uuid_from_proto(
            req.task_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing task_id"))?,
        );
        let task = self
            .state
            .tasks
            .get(&task_id)
            .ok_or(Status::not_found("task not found"))?;
        Ok(Response::new(proto::QueryTaskResponse {
            id: Some(uuid_to_proto(task.info.id)),
            task_type: task_type_to_proto(task.info.task_type),
            status: task_status_to_proto(task.info.status),
            created_at_ms_epoch: task.info.created_at.timestamp_millis(),
            last_updated_at_ms_epoch: task.info.last_updated_at.timestamp_millis(),
            assigned_client: task.info.assigned_client.map(uuid_to_proto),
            message: task.info.message.clone(),
        }))
    }

    // -----------------------------------------------------------------------
    // FetchTasks — workers pull tasks assigned to them (heartbeat-driven)
    // 获取任务 —— worker 拉取分配给自己的任务（心跳驱动）
    //
    // Workers call this periodically to claim Pending tasks. The master:
    //   1. Filters tasks where assigned_client == caller AND status == Pending
    //      筛选 assigned_client == 调用者 且 status == Pending 的任务
    //   2. Sorts by creation time (FIFO order) / 按创建时间排序（FIFO 顺序）
    //   3. Takes up to batch_size tasks / 取最多 batch_size 个任务
    //   4. Transitions each to Processing status / 将每个任务状态转换为 Processing
    //
    // As in C++ ScopedTaskWriteAccess::pop_tasks, batch_size == 0 returns no
    // assignments because the queue loop is bounded by result.size() <
    // batch_size.
    //
    // C++ equivalent: MasterServiceImpl::FetchTasks()
    // -----------------------------------------------------------------------
    pub(super) async fn fetch_tasks_impl(
        &self,
        request: Request<proto::FetchTasksRequest>,
    ) -> Result<Response<proto::FetchTasksResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();

        let batch_size = (req.batch_size as usize).min(processing_task_capacity(&self.state));

        // Collect pending tasks for this client, sorted by creation time (FIFO).
        // 收集此客户端的待处理任务，按创建时间排序（FIFO）。
        let mut pending = self
            .state
            .tasks
            .iter()
            .filter(|entry| {
                entry.info.assigned_client == Some(client_id)
                    && entry.info.status == TaskStatus::Pending
            })
            .map(|entry| (entry.key().to_owned(), entry.info.created_at))
            .collect::<Vec<_>>();
        pending.sort_by_key(|(_, created_at)| *created_at);

        // Claim tasks: transition Pending → Processing, build response.
        // 认领任务：Pending → Processing，构建响应。
        let mut tasks = Vec::new();
        let mut claimed_ids = Vec::new();
        for (task_id, _) in pending.into_iter().take(batch_size) {
            if let Some(mut task) = self.state.tasks.get_mut(&task_id) {
                task.info.status = TaskStatus::Processing;
                task.info.last_updated_at = Utc::now();
                claimed_ids.push(task_id);
                tasks.push(proto::TaskAssignment {
                    id: Some(uuid_to_proto(task.info.id)),
                    r#type: task_type_to_proto(task.info.task_type),
                    payload: task.payload.clone(), // the JSON-serialised payload / JSON 序列化的负载
                    created_at_ms_epoch: task.info.created_at.timestamp_millis(),
                    max_retry_attempts: task.max_retry_attempts,
                });
            }
        }
        if !claimed_ids.is_empty() {
            self.state
                .persist_task_state_batch_or_fence(&claimed_ids, &[], "fetch_tasks_claim")
                .map_err(|error| {
                    Status::unavailable(format!("failed to persist task claims: {error}"))
                })?;
        }

        Ok(Response::new(proto::FetchTasksResponse { tasks }))
    }

    // -----------------------------------------------------------------------
    // MarkTaskToComplete — worker reports task outcome back to the master
    // 标记任务完成 —— worker 向 master 报告任务结果
    //
    // The worker calls this after executing the task (success or failure).
    // The master validates that:
    //   1. The task exists / 任务存在
    //   2. The caller is the assigned client / 调用者是指定的客户端
    //
    // Then it updates the task status and message. If the task succeeded,
    // the master also applies the side effects (e.g. updating replica lists
    // for copy/move).
    //
    // worker 在执行任务后（成功或失败）调用此方法。
    // Master 验证：(1) 任务存在，(2) 调用者是指定的客户端。
    // 然后更新任务状态和消息。如果任务成功，master 还会应用副作用
    // （例如更新复制/移动的副本列表）。
    //
    // C++ equivalent: MasterServiceImpl::MarkTaskToComplete()
    // -----------------------------------------------------------------------
    pub(super) async fn mark_task_to_complete_impl(
        &self,
        request: Request<proto::MarkTaskToCompleteRequest>,
    ) -> Result<Response<proto::MarkTaskToCompleteResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task_req = req
            .request
            .as_ref()
            .ok_or(Status::invalid_argument("missing request"))?;
        let task_id = uuid_from_proto(
            task_req
                .id
                .as_ref()
                .ok_or(Status::invalid_argument("missing task id"))?,
        );
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();

        // Look up the task and verify ownership. / 查找任务并验证所有权。
        let mut task = self
            .state
            .tasks
            .get_mut(&task_id)
            .ok_or(Status::not_found("task not found"))?;
        if task.info.assigned_client != Some(client_id) {
            return Err(Status::permission_denied(
                "task assigned to different client",
            ));
        }

        // Update status and message. The actual side effects (e.g. replica
        // list updates for a successful move) are handled by the background
        // task completion worker in workers.rs.
        //
        // 更新状态和消息。实际的副作用（例如成功移动后的副本列表更新）
        // 由 workers.rs 中的后台任务完成 worker 处理。
        let status = request_task_status_from_i32(task_req.status)?;
        if !matches!(status, TaskStatus::Success | TaskStatus::Failed) {
            return Err(Status::invalid_argument(
                "task completion status must be success or failed",
            ));
        }
        if matches!(task.info.status, TaskStatus::Success | TaskStatus::Failed) {
            // C++ ScopedTaskWriteAccess::complete_task treats every retry
            // after the first terminal transition as successful, regardless
            // of the repeated status/message, while preserving the original
            // terminal result.
            return Ok(Response::new(proto::MarkTaskToCompleteResponse {}));
        }
        task.info.status = status;
        task.info.message = task_req.message.clone();
        task.info.last_updated_at = Utc::now();
        drop(task);
        self.state
            .persist_task_state_batch_or_fence(&[task_id], &[], "complete_task")
            .map_err(|error| {
                Status::unavailable(format!("failed to persist task completion: {error}"))
            })?;
        Ok(Response::new(proto::MarkTaskToCompleteResponse {}))
    }
}

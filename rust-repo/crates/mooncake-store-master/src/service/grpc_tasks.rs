use super::*;

impl MasterServiceImpl {
    // ---- CreateCopyTask ----
    pub(super) async fn create_copy_task_impl(
        &self,
        request: Request<proto::CreateCopyTaskRequest>,
    ) -> Result<Response<proto::CreateCopyTaskResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("missing key"));
        }
        if req.targets.is_empty() {
            return Err(Status::invalid_argument("missing targets"));
        }
        let object = self
            .state
            .objects
            .get(&req.key)
            .ok_or(Status::not_found("key not found"))?;
        if object.replicas.is_empty() {
            return Err(Status::failed_precondition("object has no source replicas"));
        }
        for target in &req.targets {
            if client_id_by_segment_name(&self.state, target).is_none() {
                return Err(Status::invalid_argument(format!(
                    "target segment not mounted: {target}"
                )));
            }
        }
        let source_segment = object.replicas[0].segment_name.clone();
        let assigned_client = client_id_by_segment_name(&self.state, &source_segment)
            .ok_or(Status::failed_precondition("source segment missing"))?;
        let task_key = req.key.clone();
        let task_payload = serde_json::to_string(&ReplicaCopyPayload {
            key: &req.key,
            source: &source_segment,
            targets: &req.targets,
        })
        .map_err(|e| Status::internal(format!("serialize task payload: {e}")))?;
        drop(object);
        if !self.state.objects.contains_key(&req.key) {
            return Err(Status::not_found("key not found"));
        }
        let task_id = Uuid::new_v4();
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
                    message: format!("copy {} to {} target(s)", req.key, req.targets.len()),
                },
                key: task_key,
                payload: task_payload,
                max_retry_attempts: 0,
            },
        );
        Ok(Response::new(proto::CreateCopyTaskResponse {
            task_id: Some(uuid_to_proto(task_id)),
        }))
    }

    // ---- CreateMoveTask ----
    pub(super) async fn create_move_task_impl(
        &self,
        request: Request<proto::CreateMoveTaskRequest>,
    ) -> Result<Response<proto::CreateMoveTaskResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() || req.source.is_empty() || req.target.is_empty() {
            return Err(Status::invalid_argument("missing key/source/target"));
        }
        if req.source == req.target {
            return Err(Status::invalid_argument("source and target must differ"));
        }
        let object = self
            .state
            .objects
            .get(&req.key)
            .ok_or(Status::not_found("key not found"))?;
        if !object
            .replicas
            .iter()
            .any(|replica| replica.segment_name == req.source)
        {
            return Err(Status::invalid_argument("source segment not found"));
        }
        let assigned_client = client_id_by_segment_name(&self.state, &req.source)
            .ok_or(Status::failed_precondition("source segment missing"))?;
        if client_id_by_segment_name(&self.state, &req.target).is_none() {
            return Err(Status::invalid_argument("target segment not mounted"));
        }
        let task_key = req.key.clone();
        let task_payload = serde_json::to_string(&ReplicaMovePayload {
            key: &req.key,
            source: &req.source,
            target: &req.target,
        })
        .map_err(|e| Status::internal(format!("serialize task payload: {e}")))?;
        drop(object);
        let task_id = Uuid::new_v4();
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
                    message: format!("move {} from {} to {}", req.key, req.source, req.target),
                },
                key: task_key,
                payload: task_payload,
                max_retry_attempts: 0,
            },
        );
        Ok(Response::new(proto::CreateMoveTaskResponse {
            task_id: Some(uuid_to_proto(task_id)),
        }))
    }

    // ---- QueryTask ----
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

    // ---- FetchTasks ----
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
        let batch_size = if req.batch_size == 0 {
            usize::MAX
        } else {
            req.batch_size as usize
        };

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

        let mut tasks = Vec::new();
        for (task_id, _) in pending.into_iter().take(batch_size) {
            if let Some(mut task) = self.state.tasks.get_mut(&task_id) {
                task.info.status = TaskStatus::Processing;
                task.info.last_updated_at = Utc::now();
                tasks.push(proto::TaskAssignment {
                    id: Some(uuid_to_proto(task.info.id)),
                    r#type: task_type_to_proto(task.info.task_type),
                    payload: task.payload.clone(),
                    created_at_ms_epoch: task.info.created_at.timestamp_millis(),
                    max_retry_attempts: task.max_retry_attempts,
                });
            }
        }

        Ok(Response::new(proto::FetchTasksResponse { tasks }))
    }

    // ---- MarkTaskToComplete ----
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
        task.info.status = task_status_from_proto(task_req.status);
        task.info.message = task_req.message.clone();
        task.info.last_updated_at = Utc::now();
        Ok(Response::new(proto::MarkTaskToCompleteResponse {}))
    }
}

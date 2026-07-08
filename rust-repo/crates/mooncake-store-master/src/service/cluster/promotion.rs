use super::super::*;

impl MasterServiceImpl {
    // ---- PromotionObjectHeartbeat ----
    // 客户端心跳拉取待 promotion 的对象（从本地磁盘提升到内存）。每次返回一个对象并移出队列。
    // Client heartbeat fetches objects pending promotion (local disk → memory). Returns one object at a time and removes from queue.
    pub(crate) async fn promotion_object_heartbeat_impl(
        &self,
        request: Request<proto::PromotionObjectHeartbeatRequest>,
    ) -> Result<Response<proto::PromotionObjectHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&client_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        let mut objects = HashMap::new();
        let mut tasks = Vec::new();
        while tasks.len() < self.state.runtime_config.promotion_max_per_heartbeat {
            let Some((scoped_key, size)) = entry
                .promotion_objects
                .iter()
                .next()
                .map(|(key, size)| (key.clone(), *size))
            else {
                break;
            };
            let (_t, uk) = split_scoped_key(&scoped_key);
            entry.promotion_objects.remove(&scoped_key);
            objects.insert(uk, size);
            let (tenant_id, key) = split_scoped_key(&scoped_key);
            tasks.push(proto::PromotionTaskItem {
                tenant_id,
                key,
                size,
            });
        }
        Ok(Response::new(proto::PromotionObjectHeartbeatResponse {
            objects,
            tasks,
        }))
    }

    // ---- PromotionAllocStart ----
    // Promotion 第一阶段：为 promotion 任务分配一个 Memory 副本（staged），返回描述符供客户端
    // RDMA 写入数据。校验 holder_id 和 key 存在性，分配后写入 staged_* 字段供后续追踪。
    //
    // Promotion phase 1: allocate a staged Memory replica for the promotion task,
    // return descriptor for client RDMA write. Validates holder_id and key existence;
    // writes staged_* fields after allocation for tracking.
    pub(crate) async fn promotion_alloc_start_impl(
        &self,
        request: Request<proto::PromotionAllocStartRequest>,
    ) -> Result<Response<proto::PromotionAllocStartResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut task = self
            .state
            .promotion_tasks
            .get_mut(&scoped_key)
            .ok_or(Status::failed_precondition("promotion task not found"))?;
        if task.holder_id != client_id {
            return Err(Status::permission_denied(
                "promotion task assigned to different client",
            ));
        }
        if task.object_size != req.size {
            return Err(Status::invalid_argument("size mismatch"));
        }
        let object_exists = self.state.objects.contains_key(&scoped_key);
        if !object_exists {
            return Err(Status::not_found("key not found"));
        }

        let mut config = ReplicateConfig::default();
        if let Some(preferred) = req.preferred_segments.first() {
            config.preferred_segment = preferred.clone();
        }
        config.preferred_segments = req.preferred_segments.clone();
        let replicas = allocate_memory_replicas(
            &self.state,
            &scoped_key,
            Some(client_id),
            req.size,
            1,
            &config,
        );
        let Some(mut staged) = replicas.into_iter().next() else {
            return Err(Status::resource_exhausted("no available memory segment"));
        };
        sync_segment_usage(&self.state, [staged.segment_id]);
        let staged_segment_id = staged.segment_id;
        let staged_offset = staged.offset;
        staged.status = ReplicaStatus::Allocating;
        if let Some(mut object) = self.state.objects.get_mut(&scoped_key) {
            object.replicas.push(staged.clone());
        }
        task.staged_segment_id = Some(staged_segment_id);
        task.staged_offset = Some(staged_offset);
        task.start_time = std::time::Instant::now();
        Ok(Response::new(proto::PromotionAllocStartResponse {
            memory_descriptor: Some(replica_to_proto(&staged)),
        }))
    }

    // ---- NotifyPromotionSuccess ----
    // Promotion 完成通知：将 staged Memory 副本标记为 Complete，清理 promotion 任务和队列。
    // 通过 segment_id+offset+status 精确匹配 promoted replica 以防止误操作。
    //
    // Promotion success notification: mark staged Memory replica as Complete,
    // clean up promotion task and queue. Precisely matches promoted replica by segment_id+offset+status to prevent errors.
    pub(crate) async fn notify_promotion_success_impl(
        &self,
        request: Request<proto::NotifyPromotionSuccessRequest>,
    ) -> Result<Response<proto::NotifyPromotionSuccessResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .promotion_tasks
            .get(&scoped_key)
            .ok_or(Status::failed_precondition("promotion task not found"))?
            .clone();
        if task.holder_id != client_id {
            return Err(Status::permission_denied(
                "promotion task assigned to different client",
            ));
        }
        let Some(segment_id) = task.staged_segment_id else {
            return Err(Status::failed_precondition(
                "promotion buffer not allocated",
            ));
        };
        let Some(offset) = task.staged_offset else {
            return Err(Status::failed_precondition(
                "promotion buffer not allocated",
            ));
        };
        let mut committed = false;
        let mut object = self
            .state
            .objects
            .get_mut(&scoped_key)
            .ok_or(Status::not_found("key not found"))?;
        if let Some(replica) = object.replicas.iter_mut().find(|replica| {
            replica.replica_type == ReplicaType::Memory
                && replica.segment_id == segment_id
                && replica.offset == offset
                && replica.status == ReplicaStatus::Allocating
        }) {
            replica.status = ReplicaStatus::Complete;
            committed = true;
        }
        drop(object);
        clear_promotion_task(&self.state, &scoped_key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&scoped_key);
        }
        if !committed {
            return Err(Status::failed_precondition("promotion replica not ready"));
        }
        Ok(Response::new(proto::NotifyPromotionSuccessResponse {}))
    }

    // ---- NotifyPromotionFailure ----
    // Promotion 失败回滚：释放已分配的 staged Memory 副本，清理任务和队列。
    // 即使 task 不存在也返回成功（幂等），避免客户端重试时出错。
    //
    // Promotion failure rollback: release allocated staged Memory replica, clean up task and queue.
    // Returns success even if the task does not exist (idempotent) to avoid errors on client retry.
    pub(crate) async fn notify_promotion_failure_impl(
        &self,
        request: Request<proto::NotifyPromotionFailureRequest>,
    ) -> Result<Response<proto::NotifyPromotionFailureResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let Some(task) = self
            .state
            .promotion_tasks
            .get(&scoped_key)
            .map(|task| task.clone())
        else {
            return Ok(Response::new(proto::NotifyPromotionFailureResponse {}));
        };
        if task.holder_id != client_id {
            return Err(Status::permission_denied(
                "promotion task assigned to different client",
            ));
        }
        if let (Some(segment_id), Some(offset)) = (task.staged_segment_id, task.staged_offset) {
            release_staged_promotion_replica(&self.state, &scoped_key, segment_id, offset);
        }
        clear_promotion_task(&self.state, &scoped_key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&scoped_key);
        }
        Ok(Response::new(proto::NotifyPromotionFailureResponse {}))
    }
}

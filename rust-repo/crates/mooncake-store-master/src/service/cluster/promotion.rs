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
        let storage_id = self
            .state
            .local_disk_client_sessions
            .get(&client_id)
            .map(|entry| *entry)
            .ok_or(Status::not_found("local disk session not found"))?;
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&storage_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        if entry.active_client_id != Some(client_id) || !entry.recovery_complete {
            return Err(Status::failed_precondition(
                "local disk inventory recovery is not complete",
            ));
        }
        let mut objects = HashMap::new();
        let mut tasks = Vec::new();
        // C++ clamps a configured zero to one (see master_service.cpp config
        // normalization); honor the same contract at the delivery site.
        let max_tasks = self.state.runtime_config.promotion_max_per_heartbeat.max(1);
        while tasks.len() < max_tasks {
            let Some((scoped_key, size)) = entry
                .promotion_objects
                .iter()
                .next()
                .map(|(key, size)| (key.clone(), *size))
            else {
                break;
            };
            let (tenant_id, key) = TenantId::parse_scoped_key(&scoped_key).map_err(|error| {
                Status::internal(format!("invalid internal scoped key: {error}"))
            })?;
            entry.promotion_objects.remove(&scoped_key);
            objects.insert(key.clone(), size);
            tasks.push(proto::PromotionTaskItem {
                tenant_id: tenant_id.as_str().to_owned(),
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
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        // C++ checks object existence before task existence so a missing key
        // reports OBJECT_NOT_FOUND regardless of the transient task state.
        if !self.state.objects.contains_key(&scoped_key) {
            return Err(Status::not_found("key not found"));
        }
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
        if self
            .state
            .local_disk_client_sessions
            .get(&client_id)
            .is_none_or(|storage_id| *storage_id != task.storage_id)
        {
            return Err(Status::permission_denied(
                "promotion task belongs to a stale LocalDisk session",
            ));
        }
        if task.object_size != req.size {
            return Err(Status::invalid_argument("size mismatch"));
        }
        if task.staged_segment_id.is_some() || task.reserved_quota_charge_bytes != 0 {
            return Err(Status::failed_precondition(
                "promotion buffer already allocated",
            ));
        }
        let reserved_quota_charge = req.size;
        self.reserve_tenant_quota(&tenant_id, reserved_quota_charge)?;

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
            self.abort_tenant_quota(&tenant_id, reserved_quota_charge)?;
            return Err(Status::resource_exhausted("no available memory segment"));
        };
        sync_segment_usage(&self.state, [staged.segment_id]);
        let staged_segment_id = staged.segment_id;
        let staged_offset = staged.offset;
        staged.status = ReplicaStatus::Allocating;
        let Some(mut object) = self.state.objects.get_mut(&scoped_key) else {
            release_replicas(&self.state, std::slice::from_ref(&staged))?;
            self.abort_tenant_quota(&tenant_id, reserved_quota_charge)?;
            self.state.fence_after_invariant_failure(
                "promotion_alloc_start_object",
                &format!("key={scoped_key:?} disappeared under its mutation guard"),
            );
            return Err(Status::internal(
                "promotion object disappeared during allocation",
            ));
        };
        object.replicas.push(staged.clone());
        sync_cache_total_accounting(&mut object);
        drop(object);
        task.staged_segment_id = Some(staged_segment_id);
        task.staged_offset = Some(staged_offset);
        task.reserved_quota_charge_bytes = reserved_quota_charge;
        task.start_time = std::time::Instant::now();
        drop(task);
        // The promotion task itself is transient, but the writable descriptor
        // is not: once returned, a failed-over leader must keep its range
        // reserved until orphan cleanup durably retires it.
        self.persist_object_image_or_remove(&scoped_key, "promotion_alloc_start")?;
        Ok(Response::new(proto::PromotionAllocStartResponse {
            memory_descriptor: Some(replica_to_proto_for_state(&self.state, &staged)),
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
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        // C++ reports OBJECT_NOT_FOUND for a missing key before consulting the
        // transient promotion task table.
        if !self.state.objects.contains_key(&scoped_key) {
            return Err(Status::not_found("key not found"));
        }
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
        let known_committed_charge =
            if object.committed_quota_charge_bytes == 0 && object.quota_committed {
                completed_memory_quota_charge(&object)
            } else {
                object.committed_quota_charge_bytes
            };
        if let Some(replica) = object.replicas.iter_mut().find(|replica| {
            replica.replica_type == ReplicaType::Memory
                && replica.segment_id == segment_id
                && replica.offset == offset
                && replica.status == ReplicaStatus::Allocating
        }) {
            replica.status = ReplicaStatus::Complete;
            committed = true;
        }
        if committed {
            if let Err(error) = settle_additional_memory_quota_charge(
                &self.state,
                &mut object,
                known_committed_charge,
                task.reserved_quota_charge_bytes,
                task.object_size,
                known_committed_charge == 0,
            ) {
                if let Some(replica) = object.replicas.iter_mut().find(|replica| {
                    replica.replica_type == ReplicaType::Memory
                        && replica.segment_id == segment_id
                        && replica.offset == offset
                }) {
                    replica.status = ReplicaStatus::Allocating;
                }
                return Err(self.tenant_quota_mutation_status("promotion_success_quota", error));
            }
        } else {
            self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
        }
        sync_cache_total_accounting(&mut object);
        drop(object);
        if clear_promotion_task(&self.state, &scoped_key).is_some() {
            metrics::PROMOTION_IN_FLIGHT.dec();
            if committed {
                metrics::PROMOTION_COMPLETED.inc();
                metrics::PROMOTION_COMPLETED_BYTES.inc_by(task.object_size);
            } else {
                metrics::PROMOTION_CANCELLED.inc();
            }
        }
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&task.storage_id) {
            local_disk.promotion_objects.remove(&scoped_key);
        }
        if !committed {
            return Err(Status::failed_precondition("promotion replica not ready"));
        }
        self.persist_object_image_or_remove(&scoped_key, "promotion_success")?;
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
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        // C++ NotifyPromotionFailure reports OBJECT_NOT_FOUND for a missing
        // key and only then tolerates an absent task as idempotent success.
        if !self.state.objects.contains_key(&scoped_key) {
            return Err(Status::not_found("key not found"));
        }
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
        if self
            .state
            .local_disk_client_sessions
            .get(&client_id)
            .is_none_or(|storage_id| *storage_id != task.storage_id)
        {
            return Err(Status::permission_denied(
                "promotion task belongs to a stale LocalDisk session",
            ));
        }
        self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
        if self.state.service_fenced.load(Ordering::Acquire) {
            return Err(Status::unavailable(
                "tenant quota invariant failed while revoking promotion",
            ));
        }
        if clear_promotion_task(&self.state, &scoped_key).is_some() {
            metrics::PROMOTION_FAILED.inc();
            metrics::PROMOTION_IN_FLIGHT.dec();
        }
        let removed = match (task.staged_segment_id, task.staged_offset) {
            (Some(segment_id), Some(offset)) => {
                detach_staged_promotion_replica(&self.state, &scoped_key, segment_id, offset)
            }
            _ => Vec::new(),
        };
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&task.storage_id) {
            local_disk.promotion_objects.remove(&scoped_key);
        }
        self.persist_detached_allocator_replicas(&scoped_key, removed, "promotion_failure")?;
        Ok(Response::new(proto::NotifyPromotionFailureResponse {}))
    }
}

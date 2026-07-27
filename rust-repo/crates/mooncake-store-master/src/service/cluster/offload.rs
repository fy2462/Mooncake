use super::super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OffloadReportKind {
    Failed,
    AdmittedCompletion,
    RecoveryReattach,
    IgnoredRecovery,
}

impl MasterServiceImpl {
    // ---- OffloadObjectHeartbeat ----
    // 客户端周期性拉取需要 offload 的对象列表并上报心跳。
    // 若客户端禁用 offload，清空其 offload 队列并取消所有待 offload 任务。
    //
    // Client periodically fetches objects needing offload and reports heartbeat.
    // If offload is disabled, clears the client's offload queue and cancels all pending offload tasks.
    pub(crate) async fn offload_object_heartbeat_impl(
        &self,
        request: Request<proto::OffloadObjectHeartbeatRequest>,
    ) -> Result<Response<proto::OffloadObjectHeartbeatResponse>, Status> {
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
        entry.enable_offloading = req.enable_offloading;
        if !req.enable_offloading {
            let keys: Vec<String> = std::mem::take(&mut entry.offloading_objects)
                .into_keys()
                .collect();
            drop(entry);
            for key in keys {
                let _mutation_guard = self.state.key_mutations.lock(&key);
                clear_offloading_task(&self.state, &key);
                self.persist_object_image_or_remove(&key, "disable_offloading")?;
            }
            return Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
                objects: HashMap::new(),
                tasks: Vec::new(),
            }));
        }
        let objects = std::mem::take(&mut entry.offloading_objects);
        // Convert scoped keys back to user_keys for external API.
        // 将作用域 key 转换回 user_key，供外部 API 使用。
        let mut tasks = Vec::with_capacity(objects.len());
        let mut unscoped = HashMap::with_capacity(objects.len());
        for (scoped_key, size) in objects {
            let Some(offloading_task) = self.state.offloading_tasks.get(&scoped_key) else {
                // A queue entry without its authoritative task cannot carry a
                // generation and must never be handed to a client.
                tracing::warn!(
                    scoped_key,
                    "dropping orphaned LocalDisk offload queue entry"
                );
                continue;
            };
            if offloading_task.client_id != client_id
                || offloading_task.storage_id != storage_id
                || i64::try_from(offloading_task.source.size).ok() != Some(size)
            {
                tracing::warn!(
                    scoped_key,
                    "dropping inconsistent LocalDisk offload queue entry"
                );
                continue;
            }
            let (tenant_id, key) = TenantId::parse_scoped_key(&scoped_key).map_err(|error| {
                Status::internal(format!("invalid internal scoped key: {error}"))
            })?;
            tasks.push(proto::OffloadTaskItem {
                tenant_id: tenant_id.as_str().to_owned(),
                key: key.clone(),
                size,
                generation_id: Some(uuid_to_proto(offloading_task.generation_id)),
            });
            unscoped.insert(key, size);
        }
        Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
            objects: unscoped,
            tasks,
        }))
    }

    // ---- ReportSsdCapacity ----
    // 客户端上报本地 SSD 总容量，供 master 做 offload 容量规划。
    // Client reports local SSD total capacity for master offload capacity planning.
    pub(crate) async fn report_ssd_capacity_impl(
        &self,
        request: Request<proto::ReportSsdCapacityRequest>,
    ) -> Result<Response<proto::ReportSsdCapacityResponse>, Status> {
        let req = request.into_inner();
        if req.ssd_total_capacity_bytes < 0 {
            return Err(Status::invalid_argument(
                "ssd_total_capacity_bytes must be non-negative",
            ));
        }
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        // Capacity is a session-scoped runtime hint. Serialize its update with
        // snapshots, but never treat an old process report as durable authority.
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
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
        if entry.active_client_id != Some(client_id) {
            return Err(Status::permission_denied(
                "local disk session belongs to another client",
            ));
        }
        entry.ssd_total_capacity_bytes = req.ssd_total_capacity_bytes;
        Ok(Response::new(proto::ReportSsdCapacityResponse {}))
    }

    // ---- NotifyOffloadSuccess ----
    // 客户端通知 offload 完成：为每个 key 创建/更新 LocalDisk 类型副本（状态 Complete），
    // 同时清理 offload 任务。重启恢复只能重新关联 Master 已知的完整磁盘副本；
    // 磁盘扫描结果不得创建对象或覆盖对象的权威 size。
    //
    // Client notifies offload completion: create/update LocalDisk replicas (status Complete) for each key,
    // and clean up offload tasks. Restart recovery may only reattach a complete replica already known
    // to the Master; disk scan results must never create objects or overwrite authoritative object size.
    pub(crate) async fn notify_offload_success_impl(
        &self,
        request: Request<proto::NotifyOffloadSuccessRequest>,
    ) -> Result<Response<proto::NotifyOffloadSuccessResponse>, Status> {
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
            .ok_or(Status::failed_precondition(
                "local disk segment must be mounted before reporting offload results",
            ))?;
        let (recovery_complete, active_recovery_session_id) = self
            .state
            .local_disk_segments
            .get(&storage_id)
            .filter(|entry| entry.active_client_id == Some(client_id))
            .map(|entry| (entry.recovery_complete, entry.recovery_session_id))
            .ok_or(Status::failed_precondition(
                "local disk session is not active",
            ))?;
        if !recovery_complete {
            let reported_recovery_session_id =
                uuid_from_proto(req.recovery_session_id.as_ref().ok_or(
                    Status::failed_precondition("missing LocalDisk recovery_session_id"),
                )?);
            if reported_recovery_session_id.is_nil()
                || active_recovery_session_id != Some(reported_recovery_session_id)
            {
                return Err(Status::failed_precondition(
                    "LocalDisk recovery session is stale",
                ));
            }
        }
        let task_count = if req.tasks.is_empty() {
            req.keys.len()
        } else {
            req.tasks.len()
        };
        if task_count != req.metadatas.len() {
            return Err(Status::invalid_argument(
                "keys/tasks and metadatas must have same length",
            ));
        }
        let tasks: Vec<proto::OffloadTaskItem> = if req.tasks.is_empty() {
            req.keys
                .iter()
                .map(|key| proto::OffloadTaskItem {
                    tenant_id: String::new(),
                    key: key.clone(),
                    size: 0,
                    generation_id: None,
                })
                .collect()
        } else {
            req.tasks.clone()
        };
        let mut seen_keys = HashSet::with_capacity(tasks.len());
        let preflight = tasks
            .iter()
            .map(|task| {
                let tenant_id = resolve_request_tenant(
                    &task.tenant_id,
                    self.state.runtime_config.enable_tenant_quota,
                )?;
                let scoped_key = tenant_id.make_scoped_key(&task.key);
                if !seen_keys.insert(scoped_key.clone()) {
                    return Err(Status::invalid_argument(format!(
                        "duplicate offload result for key {}",
                        task.key
                    )));
                }
                Ok::<_, Status>((tenant_id, scoped_key))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Lock the complete batch before authorizing any item. This prevents a
        // Remove/Upsert from changing an object's generation between validation
        // and mutation. Recovery-only stale items are dropped individually;
        // admitted task completion remains all-or-error.
        let _mutation_guards = self
            .state
            .key_mutations
            .lock_many(preflight.iter().map(|(_, key)| key.as_str()));

        let mut report_kinds = Vec::with_capacity(tasks.len());
        for ((task, (_, key)), metadata) in tasks.iter().zip(&preflight).zip(&req.metadatas) {
            let reported_generation_id = task
                .generation_id
                .as_ref()
                .map(uuid_from_proto)
                .filter(|generation_id| !generation_id.is_nil());
            if recovery_complete {
                if let Some(offloading_task) = self.state.offloading_tasks.get(key)
                    && reported_generation_id != Some(offloading_task.generation_id)
                {
                    return Err(Status::failed_precondition(format!(
                        "offload generation is stale for key {}",
                        task.key
                    )));
                }
            } else if !recovery_complete && reported_generation_id.is_none() {
                report_kinds.push(OffloadReportKind::IgnoredRecovery);
                continue;
            }

            if metadata.data_size < 0 {
                if !recovery_complete {
                    report_kinds.push(OffloadReportKind::IgnoredRecovery);
                    continue;
                }
                if let Some(offloading_task) = self.state.offloading_tasks.get(key) {
                    if offloading_task.client_id != client_id {
                        return Err(Status::permission_denied(format!(
                            "offload task for key {} belongs to another client",
                            task.key
                        )));
                    }
                    if offloading_task.storage_id != storage_id {
                        return Err(Status::permission_denied(format!(
                            "offload task for key {} belongs to another LocalDisk namespace",
                            task.key
                        )));
                    }
                }
                report_kinds.push(OffloadReportKind::Failed);
                continue;
            }

            if metadata.transport_endpoint.is_empty() {
                if !recovery_complete {
                    report_kinds.push(OffloadReportKind::IgnoredRecovery);
                    continue;
                }
                return Err(Status::invalid_argument(format!(
                    "missing transport endpoint for key {}",
                    task.key
                )));
            }
            let reported_size = u64::try_from(metadata.data_size).map_err(|_| {
                Status::invalid_argument(format!("invalid data size for key {}", task.key))
            })?;
            if task.size > 0
                && u64::try_from(task.size).map_err(|_| {
                    Status::invalid_argument(format!("invalid task size for key {}", task.key))
                })? != reported_size
            {
                if !recovery_complete {
                    report_kinds.push(OffloadReportKind::IgnoredRecovery);
                    continue;
                }
                return Err(Status::failed_precondition(format!(
                    "offload task size does not match reported size for key {}",
                    task.key
                )));
            }

            if recovery_complete && let Some(offloading_task) = self.state.offloading_tasks.get(key)
            {
                if offloading_task.client_id != client_id {
                    return Err(Status::permission_denied(format!(
                        "offload task for key {} belongs to another client",
                        task.key
                    )));
                }
                if offloading_task.storage_id != storage_id {
                    return Err(Status::permission_denied(format!(
                        "offload task for key {} belongs to another LocalDisk namespace",
                        task.key
                    )));
                }
                let object = self.state.objects.get(key).ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "admitted offload task references missing object {}",
                        task.key
                    ))
                })?;
                if object.size != reported_size {
                    return Err(Status::failed_precondition(format!(
                        "reported LocalDisk size does not match authoritative object size for key {}",
                        task.key
                    )));
                }
                let source_is_current = offloading_task.source.size == reported_size
                    && object.replicas.iter().any(|replica| {
                        replica.segment_id == offloading_task.source.segment_id
                            && replica.offset == offloading_task.source.offset
                            && replica.replica_type == offloading_task.source.replica_type
                            && replica.size == reported_size
                            && replica.status == ReplicaStatus::Complete
                            && replica.handle_valid
                    });
                if !source_is_current {
                    return Err(Status::failed_precondition(format!(
                        "offload source is stale for key {}",
                        task.key
                    )));
                }
                report_kinds.push(OffloadReportKind::AdmittedCompletion);
                continue;
            }

            // A recovery report is a new control-plane mutation rather than
            // completion of a task already admitted by the Master.
            if recovery_complete {
                return Err(Status::failed_precondition(format!(
                    "no admitted offload task for key {}",
                    task.key
                )));
            }
            if self.resolve_write_tenant(&task.tenant_id).is_err() {
                report_kinds.push(OffloadReportKind::IgnoredRecovery);
                continue;
            }
            let Some(object) = self.state.objects.get(key) else {
                report_kinds.push(OffloadReportKind::IgnoredRecovery);
                continue;
            };
            if object.size != reported_size {
                report_kinds.push(OffloadReportKind::IgnoredRecovery);
                continue;
            }
            let can_reattach = !self.state.processing_keys.contains_key(key)
                && object.replicas.iter().any(|replica| {
                    replica.replica_type == ReplicaType::LocalDisk
                        && replica.local_disk_storage_id == Some(storage_id)
                        && replica.local_disk_generation_id == reported_generation_id
                        && replica.status == ReplicaStatus::Complete
                        && replica.size == reported_size
                });
            if !can_reattach {
                report_kinds.push(OffloadReportKind::IgnoredRecovery);
                continue;
            }
            report_kinds.push(OffloadReportKind::RecoveryReattach);
        }

        let stale_recovery_tasks = tasks
            .iter()
            .zip(&report_kinds)
            .filter(|(_, report_kind)| **report_kind == OffloadReportKind::IgnoredRecovery)
            .map(|(task, _)| task.clone())
            .collect();

        for (((task, (_, key)), metadata), report_kind) in tasks
            .iter()
            .zip(&preflight)
            .zip(&req.metadatas)
            .zip(report_kinds.iter().copied())
        {
            if report_kind == OffloadReportKind::IgnoredRecovery {
                continue;
            }
            if report_kind == OffloadReportKind::Failed {
                clear_offloading_task(&self.state, key);
                self.persist_object_image_or_remove(key, "offload_failure")?;
                continue;
            }
            let generation_id = task
                .generation_id
                .as_ref()
                .map(uuid_from_proto)
                .filter(|generation_id| !generation_id.is_nil())
                .expect("accepted LocalDisk report has a validated generation");

            let replica = ReplicaDescriptor {
                refcnt: 0,
                handle_valid: true,
                segment_id: Uuid::nil(),
                segment_name: metadata.transport_endpoint.clone(),
                offset: 0,
                size: metadata.data_size.max(0) as u64,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::LocalDisk,
                holder_client_id: Some(client_id),
                local_disk_storage_id: Some(storage_id),
                local_disk_generation_id: Some(generation_id),
                base_addr: 0,
                protocol: String::new(),
            };
            let mut object = self
                .state
                .objects
                .get_mut(key)
                .expect("offload batch remains protected by key mutation guards");
            if let Some(existing) = object.replicas.iter_mut().find(|existing| {
                existing.replica_type == ReplicaType::LocalDisk
                    && existing.local_disk_storage_id == Some(storage_id)
            }) {
                if report_kind == OffloadReportKind::RecoveryReattach {
                    // Preserve Master-owned state and only refresh the routing
                    // endpoint supplied by the remounted holder.
                    existing.segment_name = metadata.transport_endpoint.clone();
                    existing.holder_client_id = Some(client_id);
                    existing.handle_valid = true;
                } else {
                    *existing = replica;
                }
            } else {
                debug_assert_eq!(report_kind, OffloadReportKind::AdmittedCompletion);
                object.replicas.push(replica);
            }
            sync_cache_total_accounting(&mut object);
            drop(object);
            if report_kind == OffloadReportKind::AdmittedCompletion {
                match self.state.record_current_object_image_durable(key) {
                    Ok(true) => {}
                    Ok(false) => {
                        self.state.fence_after_invariant_failure(
                            "local_disk_generation",
                            &format!(
                                "admitted LocalDisk object disappeared before persistence: \
                                 scoped_key={key}"
                            ),
                        );
                        return Err(Status::unavailable(
                            "admitted LocalDisk object disappeared before persistence",
                        ));
                    }
                    Err(error) => {
                        self.state
                            .fence_after_durability_failure("local_disk_generation", &error);
                        return Err(Status::unavailable(
                            "failed to durably persist LocalDisk generation",
                        ));
                    }
                }
                clear_offloading_task(&self.state, key);
            } else if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&storage_id)
            {
                local_disk.recovered_objects.insert(key.clone());
            }
        }
        Ok(Response::new(proto::NotifyOffloadSuccessResponse {
            stale_recovery_tasks,
        }))
    }
}

use super::*;
use crate::service::helpers::ready_local_disk_storage_for_client;
use crate::service::proto_conv::replica_to_proto_for_state;

impl MasterServiceImpl {
    // ---- PutStart ----
    // 对象写入的第一阶段：分配副本、注册对象元数据。
    // 流程：(1) 校验 key/size → (2) 若对象已存在则清理过期 handle 或超时丢弃
    // (3) 分配 Memory 副本并可选分配 NoF 副本 → (4) 写入对象表并标记 processing_keys。
    // 返回分配的副本列表供客户端 RDMA 写入。
    //
    // Object write phase 1: allocate replicas, register object metadata.
    // Flow: (1) validate key/size → (2) if object exists, clean stale handles or timeout-discard
    // (3) allocate Memory replicas and optionally NoF replicas → (4) write object table and mark processing_keys.
    // Returns allocated replica list for client RDMA write.
    pub(super) async fn put_start_impl(
        &self,
        request: Request<proto::PutStartRequest>,
    ) -> Result<Response<proto::PutStartResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;

        // C++ master_service.cpp:1287-1294 对空 key 和零长度 slice 进行校验
        // Validate non-empty key and non-zero slice length
        if req.key.is_empty() {
            return Err(Status::invalid_argument("empty key"));
        }
        if req.slice_length == 0 {
            return Err(Status::invalid_argument("zero slice_length"));
        }
        // Validate key does not contain the tenant scope delimiter
        validate_user_key(&req.key)?;

        let user_key = req.key.clone();
        let scoped_key = tenant_id.make_scoped_key(&user_key);
        // Match C++ AcquireObjectOperationLock: keep the same tenant-scoped
        // PutStart serialized across quota eviction retries even though the
        // mutation guard below must be released for the global eviction epoch.
        let _operation_guard = self.state.key_mutations.lock_operation(&scoped_key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let alive_clients = get_alive_clients_snapshot(&self.state);
        let mutation_guard = self.state.key_mutations.lock(&scoped_key);
        clear_invalid_handles_for_key_locked(&self.state, &alive_clients, &scoped_key).map_err(
            |error| Status::unavailable(format!("stale handle cleanup failed: {error}")),
        )?;

        // C++ master_service.cpp:1467-1507 — prepare_existing:
        // 1. CleanupStaleHandles 清理无效 handle 和死亡客户端的副本
        // 2. 如果所有有效副本都被清理，删除对象并允许新 PutStart
        // 3. 如果 PutStart 超时且无 Completed 副本，丢弃旧对象
        // 4. 否则返回 OBJECT_ALREADY_EXISTS
        //
        // 1. CleanupStaleHandles: clean invalid handles and dead-client replicas
        // 2. If all valid replicas cleaned, delete object and allow new PutStart
        // 3. If PutStart timed out with no Completed replicas, discard old object
        // 4. Otherwise return OBJECT_ALREADY_EXISTS
        if let Some(existing) = self.state.objects.get_mut(&scoped_key) {
            // Object still has valid replicas, but check if it is a timed-out
            // in-flight PutStart with no completed data.
            let has_completed = existing
                .replicas
                .iter()
                .any(|r| r.status == ReplicaStatus::Complete);
            if !has_completed {
                if let Some(start) = existing.put_start_time {
                    let elapsed = SystemTime::now().duration_since(start).unwrap_or_default();
                    if elapsed >= self.state.runtime_config.put_start_discard_timeout {
                        let release_deadline = start
                            .checked_add(self.state.runtime_config.put_start_release_timeout)
                            .ok_or_else(|| {
                                Status::internal("expired PutStart release deadline overflow")
                            })?;
                        // PutStart timed out; remove the in-flight metadata and
                        // defer allocating-buffer release for writer safety.
                        let old_replicas = existing.replicas.clone();
                        drop(existing);
                        if let Some((_, removed)) = self.state.objects.remove(&scoped_key) {
                            self.account_removed_object_quota(&removed)?;
                        }
                        self.state.processing_keys.remove(&scoped_key);
                        self.state.replication_tasks.remove(&scoped_key);
                        let delayed = self
                            .state
                            .schedule_delayed_replica_release_or_fence(
                                &scoped_key,
                                None,
                                old_replicas,
                                Some(release_deadline),
                                "put_start_discard_expired",
                            )
                            .map_err(|error| {
                                Status::unavailable(format!(
                                    "failed to persist expired PutStart delayed release: {error}"
                                ))
                            })?
                            .is_some();
                        if !delayed {
                            self.persist_object_image_or_remove(
                                &scoped_key,
                                "put_start_discard_expired",
                            )?;
                        }
                    } else {
                        return Err(Status::already_exists(format!(
                            "object already exists: {user_key}"
                        )));
                    }
                } else {
                    return Err(Status::already_exists(format!(
                        "object already exists: {user_key}"
                    )));
                }
            } else {
                return Err(Status::already_exists(format!(
                    "object already exists: {user_key}"
                )));
            }
        }

        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        let disk_enabled =
            global_disk_replica(&self.state, &scoped_key, req.slice_length).is_some();
        if config.replica_num == 0 && config.nof_replica_num == 0 && !disk_enabled {
            return Err(Status::invalid_argument(
                "replica_num and nof_replica_num cannot both be zero when global DISK is disabled",
            ));
        }
        if config.prefer_alloc_in_same_node && config.nof_replica_num > 0 {
            return Err(Status::invalid_argument(
                "prefer_alloc_in_same_node is not supported with NoF replicas",
            ));
        }
        if !self.state.runtime_config.enable_nof && config.nof_replica_num > 0 {
            return Err(Status::invalid_argument("NoF is not enabled"));
        }
        let flexible_dual = config.replica_num == 1 && config.nof_replica_num == 1;
        let group_id = Self::group_id_for_key(&config, 1, 0)?;
        let replica_count = config.replica_num as usize;
        let requested_quota_charge =
            checked_requested_memory_quota_charge(req.slice_length, replica_count).map_err(
                |_| Status::invalid_argument("Memory replica quota charge overflows uint64"),
            )?;
        // Tenant quota eviction locks arbitrary keys from this tenant. Release
        // the requested key first to avoid recursively acquiring the same
        // mutation stripe, then revalidate existence after quota admission.
        drop(mutation_guard);
        self.reserve_tenant_quota_with_eviction(
            &tenant_id,
            requested_quota_charge,
            Some(&scoped_key),
        )?;
        let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
        if self.state.objects.contains_key(&scoped_key) {
            self.abort_tenant_quota(&tenant_id, requested_quota_charge)?;
            return Err(Status::already_exists(format!(
                "object already exists: {user_key}"
            )));
        }
        let mut reserved_quota_charge = requested_quota_charge;

        // 分配 Memory 副本 / Allocate Memory replicas
        let mut replicas = if replica_count > 0 {
            allocate_memory_replicas(
                &self.state,
                &scoped_key,
                Some(client_id),
                req.slice_length,
                replica_count,
                &config,
            )
        } else {
            Vec::new()
        };
        if replicas.len() != replica_count {
            release_replicas(&self.state, &replicas)?;
            replicas.clear();
            self.abort_tenant_quota(&tenant_id, reserved_quota_charge)?;
            reserved_quota_charge = 0;
            if !flexible_dual {
                return Err(Status::resource_exhausted(format!(
                    "failed to allocate {replica_count} replica(s) for key {user_key}{}",
                    PUT_NO_SPACE_HELPER_STR,
                )));
            }
        }
        // NoF 副本分配：使用显式 preferred_nof_segments；same-node NoF 组合按 C++ 拒绝。
        // NoF replica allocation: use explicit preferred_nof_segments; same-node NoF is rejected like C++.
        if config.nof_replica_num > 0 {
            let nof_replicas = match allocate_nof_replicas(
                &self.state,
                &scoped_key,
                req.slice_length,
                config.nof_replica_num as usize,
                &config.preferred_nof_segments,
            ) {
                Ok(replicas) => replicas,
                Err(status) => {
                    let unavailable_side_of_flexible_dual = flexible_dual
                        && !replicas.is_empty()
                        && matches!(
                            status.code(),
                            tonic::Code::FailedPrecondition | tonic::Code::ResourceExhausted
                        );
                    if unavailable_side_of_flexible_dual {
                        Vec::new()
                    } else {
                        release_replicas(&self.state, &replicas)?;
                        self.abort_tenant_quota(&tenant_id, reserved_quota_charge)?;
                        return Err(status);
                    }
                }
            };
            replicas.extend(nof_replicas);
        }
        if let Some(disk_replica) = global_disk_replica(&self.state, &scoped_key, req.slice_length)
        {
            replicas.push(disk_replica);
        }
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> = replicas
            .iter()
            .map(|replica| replica_to_proto_for_state(&self.state, replica))
            .collect();

        let now = SystemTime::now();
        self.state.objects.insert(
            scoped_key.clone(),
            ObjectEntry {
                replicas,
                size: req.slice_length,
                last_access: now,
                hard_pinned: config.with_hard_pin,
                data_type: config.data_type,
                client_id,
                put_start_time: Some(now),
                lease_timeout: None,
                soft_pin_timeout: if config.with_soft_pin {
                    crate::metrics::SOFT_PIN_KEY_COUNT.inc();
                    Some(SystemTime::UNIX_EPOCH)
                } else {
                    None
                },
                tenant_id: tenant_id.clone(),
                group_id,
                quota_committed: false,
                reserved_quota_charge_bytes: reserved_quota_charge,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key,
            },
        );
        self.register_tenant_metadata_object(&tenant_id);
        // 将 key 加入 processing_keys，防止并发 PutStart 冲突
        // Add key to processing_keys to prevent concurrent PutStart conflicts
        self.state.processing_keys.insert(scoped_key.clone(), ());
        // PutStart publishes writable physical descriptors. Persist the exact
        // Allocating image, quota reservation and allocator geometry before
        // returning so a promoted standby cannot reuse those ranges.
        self.persist_object_image_or_remove(&scoped_key, "put_start")?;

        metrics::PUT_START_REQUESTS.inc();
        Ok(Response::new(proto::PutStartResponse {
            replicas: proto_replicas,
        }))
    }

    // ---- PutEnd ----
    // 对象写入的第二阶段：将 Allocating 状态的副本标记为 Complete，更新 lease 和软 pin 超时。
    // 校验 client_id 防止越权写入。仅当所有副本都 Complete 时才从 processing_keys 移除，
    // 以避免并发 PutStart 冲突。同时触发 offload 队列攒批和 oplog 记录。
    //
    // Object write phase 2: mark Allocating replicas as Complete, update lease and soft-pin timeout.
    // Validates client_id to prevent unauthorized writes. Only removes from processing_keys
    // when ALL replicas are Complete to avoid concurrent PutStart conflicts.
    // Also triggers offload queue batching and oplog recording.
    pub(super) async fn put_end_impl(
        &self,
        request: Request<proto::PutEndRequest>,
    ) -> Result<Response<proto::PutEndResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        self.apply_put_end_for_key(
            &scoped_key,
            client_id,
            request_replica_type_from_i32(req.replica_type)?,
        )?;
        metrics::PUT_END_REQUESTS.inc();
        Ok(Response::new(proto::PutEndResponse {}))
    }

    // ---- AddReplica ----
    // Deprecated compatibility endpoint. It may only refresh routing metadata
    // for a LocalDisk replica already authorized by the Master. New replicas
    // are created exclusively by completion of a Master-admitted offload task.
    pub(super) async fn add_replica_impl(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let replica = req
            .replica
            .as_ref()
            .map(replica_from_proto)
            .ok_or(Status::invalid_argument("missing replica"))?;
        if replica.replica_type != ReplicaType::LocalDisk {
            return Err(Status::invalid_argument(
                "AddReplica only accepts LocalDisk replicas",
            ));
        }
        if replica.status != ReplicaStatus::Complete
            || replica.segment_name.is_empty()
            || replica.holder_client_id != Some(client_id)
            || replica.local_disk_generation_id.is_none()
        {
            return Err(Status::invalid_argument(
                "AddReplica requires a complete LocalDisk replica held by the caller",
            ));
        }
        let storage_id = ready_local_disk_storage_for_client(&self.state, client_id)?;
        if replica.local_disk_storage_id != Some(storage_id) {
            return Err(Status::permission_denied(
                "replica does not belong to the caller's LocalDisk storage namespace",
            ));
        }
        if self.state.processing_keys.contains_key(&scoped_key) {
            return Err(Status::failed_precondition("object is being mutated"));
        }
        let mut object =
            self.state
                .objects
                .get_mut(&scoped_key)
                .ok_or(Status::failed_precondition(
                    "AddReplica cannot create a missing object",
                ))?;
        if object.size != replica.size {
            return Err(Status::failed_precondition(
                "LocalDisk replica size does not match authoritative object size",
            ));
        }
        let object_size = object.size;
        let existing = object
            .replicas
            .iter_mut()
            .find(|existing| {
                existing.replica_type == ReplicaType::LocalDisk
                    && existing.local_disk_storage_id == Some(storage_id)
                    && existing.local_disk_generation_id == replica.local_disk_generation_id
                    && existing.status == ReplicaStatus::Complete
                    && existing.size == object_size
            })
            .ok_or(Status::failed_precondition(
                "AddReplica cannot append an unadmitted LocalDisk replica",
            ))?;
        existing.segment_name = replica.segment_name;
        existing.holder_client_id = Some(client_id);
        existing.handle_valid = true;
        drop(object);
        self.persist_object_image_or_remove(&scoped_key, "add_replica")?;
        Ok(Response::new(proto::AddReplicaResponse {}))
    }
}

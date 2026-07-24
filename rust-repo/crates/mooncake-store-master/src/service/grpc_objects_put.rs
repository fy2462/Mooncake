use super::*;

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
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );

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
        if let Some(mut existing) = self.state.objects.get_mut(&scoped_key) {
            let alive_clients = get_alive_clients_snapshot(&self.state);
            let should_remove = cleanup_stale_handles(&mut existing, &alive_clients);

            if should_remove {
                // 所有有效副本已被清理（handle 失效或客户端死亡），删除对象并允许新 PutStart
                // All valid replicas cleaned (stale handle or dead client); delete object and allow new PutStart
                let old_replicas = existing.replicas.clone();
                if let Some((_, removed)) = self.state.objects.remove(&scoped_key) {
                    self.account_removed_object_quota(&removed);
                }
                self.state.processing_keys.remove(&scoped_key);
                self.state.replication_tasks.remove(&scoped_key);
                drop(existing);
                release_replicas(&self.state, &old_replicas);
            } else {
                // 对象仍有有效副本，但需检查是否可超时丢弃：
                // 仅当无 Completed 副本且 PutStart 已超时时才允许覆盖，否则返回已存在错误
                //
                // Object still has valid replicas, but check if timeout-discardable:
                // Only allow overwrite when no Completed replicas and PutStart has timed out.
                let has_completed = existing
                    .replicas
                    .iter()
                    .any(|r| r.status == ReplicaStatus::Complete);
                if !has_completed {
                    if let Some(start) = existing.put_start_time {
                        let elapsed = SystemTime::now().duration_since(start).unwrap_or_default();
                        if elapsed >= self.state.runtime_config.put_start_discard_timeout {
                            // PutStart 超时，删除对象 / PutStart timed out; delete object
                            let old_replicas = existing.replicas.clone();
                            let expired = existing
                                .replicas
                                .iter()
                                .filter(|r| r.status == ReplicaStatus::Allocating)
                                .cloned()
                                .collect::<Vec<_>>();
                            if let Some((_, removed)) = self.state.objects.remove(&scoped_key) {
                                self.account_removed_object_quota(&removed);
                            }
                            self.state.processing_keys.remove(&scoped_key);
                            self.state.replication_tasks.remove(&scoped_key);
                            drop(existing);
                            if !expired.is_empty() {
                                release_replicas_scheduled(&self.state, expired);
                            } else if !old_replicas.is_empty() {
                                release_replicas(&self.state, &old_replicas);
                            }
                        } else {
                            return Err(Status::already_exists(format!(
                                "object already exists: {}",
                                user_key
                            )));
                        }
                    } else {
                        return Err(Status::already_exists(format!(
                            "object already exists: {}",
                            user_key
                        )));
                    }
                } else {
                    return Err(Status::already_exists(format!(
                        "object already exists: {}",
                        user_key
                    )));
                }
            }
        }

        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        if config.replica_num == 0 && config.nof_replica_num == 0 {
            return Err(Status::invalid_argument(
                "replica_num and nof_replica_num cannot both be zero",
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
        let group_id = Self::group_id_for_key(&config, 1, 0)?;
        let replica_count = config.replica_num as usize;
        self.reserve_tenant_quota(&tenant_id, req.slice_length)?;

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
            release_replicas(&self.state, &replicas);
            self.abort_tenant_quota(&tenant_id, req.slice_length);
            return Err(Status::resource_exhausted(format!(
                "failed to allocate {replica_count} replica(s) for key {user_key}{}",
                PUT_NO_SPACE_HELPER_STR,
            )));
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
                    release_replicas(&self.state, &replicas);
                    self.abort_tenant_quota(&tenant_id, req.slice_length);
                    return Err(status);
                }
            };
            replicas.extend(nof_replicas);
        }
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> =
            replicas.iter().map(replica_to_proto).collect();

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
                tenant_id,
                group_id,
                quota_committed: false,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key,
            },
        );
        // 将 key 加入 processing_keys，防止并发 PutStart 冲突
        // Add key to processing_keys to prevent concurrent PutStart conflicts
        self.state.processing_keys.insert(scoped_key, ());

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
            replica_type_from_i32(req.replica_type),
        )?;
        metrics::PUT_END_REQUESTS.inc();
        Ok(Response::new(proto::PutEndResponse {}))
    }

    // ---- AddReplica ----
    // 向已有对象追加副本。LocalDisk 类型副本按 holder_client_id 去重（同一客户端只保留最新），
    // 其他类型直接追加。若对象不存在，仅对 LocalDisk 类型自动创建对象条目。
    //
    // Append a replica to an existing object. LocalDisk replicas are deduplicated by holder_client_id
    // (only the latest per client is kept); other types are appended directly.
    // If the object does not exist, creates an entry only for LocalDisk type.
    pub(super) async fn add_replica_impl(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
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
        let replica = req
            .replica
            .as_ref()
            .map(replica_from_proto)
            .ok_or(Status::invalid_argument("missing replica"))?;
        match self.state.objects.get_mut(&scoped_key) {
            Some(mut entry) => {
                if replica.replica_type == ReplicaType::LocalDisk {
                    if let Some(existing) = entry.replicas.iter_mut().find(|existing| {
                        existing.replica_type == ReplicaType::LocalDisk
                            && existing.holder_client_id == replica.holder_client_id
                    }) {
                        *existing = replica;
                    } else {
                        entry.replicas.push(replica);
                    }
                } else {
                    entry.replicas.push(replica);
                }
                sync_cache_total_accounting(&mut entry);
            }
            _ => {
                if replica.replica_type == ReplicaType::LocalDisk {
                    let mut entry = ObjectEntry {
                        size: replica.size,
                        replicas: vec![replica],
                        last_access: SystemTime::now(),
                        hard_pinned: false,
                        data_type: ObjectDataType::Unknown,
                        client_id,
                        put_start_time: None,
                        lease_timeout: None,
                        soft_pin_timeout: None,
                        tenant_id,
                        group_id: String::new(),
                        quota_committed: false,
                        memory_cache_total_accounted: false,
                        disk_cache_total_accounted: false,
                        user_key: req.key.clone(),
                    };
                    sync_cache_total_accounting(&mut entry);
                    self.state.objects.insert(scoped_key.clone(), entry);
                }
            }
        }
        Ok(Response::new(proto::AddReplicaResponse {}))
    }
}

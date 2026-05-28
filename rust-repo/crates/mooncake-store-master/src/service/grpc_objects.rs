use super::*;

impl MasterServiceImpl {
    // ---- ExistKey ----
    pub(super) async fn exist_key_impl(
        &self,
        request: Request<proto::ExistKeyRequest>,
    ) -> Result<Response<proto::ExistKeyResponse>, Status> {
        let req = request.into_inner();
        let exists = self.state.objects.contains_key(&req.key);
        metrics::GET_REQUESTS.inc();
        Ok(Response::new(proto::ExistKeyResponse { exists }))
    }

    // ---- GetAllKeys ----
    pub(super) async fn get_all_keys_impl(
        &self,
        _request: Request<proto::GetAllKeysRequest>,
    ) -> Result<Response<proto::GetAllKeysResponse>, Status> {
        let keys: Vec<String> = self
            .state
            .objects
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        Ok(Response::new(proto::GetAllKeysResponse { keys }))
    }

    // ---- GetAllSegments ----
    pub(super) async fn get_all_segments_impl(
        &self,
        _request: Request<proto::GetAllSegmentsRequest>,
    ) -> Result<Response<proto::GetAllSegmentsResponse>, Status> {
        let segments: Vec<String> = self
            .state
            .segments
            .iter()
            .map(|entry| entry.segment.name.clone())
            .collect();
        Ok(Response::new(proto::GetAllSegmentsResponse { segments }))
    }

    // ---- GetAllNoFSegments ----
    pub(super) async fn get_all_nof_segments_impl(
        &self,
        _request: Request<proto::GetAllNoFSegmentsRequest>,
    ) -> Result<Response<proto::GetAllNoFSegmentsResponse>, Status> {
        let segments = self
            .state
            .nof_segments
            .iter()
            .map(|entry| nof_segment_to_proto(&entry.segment))
            .collect();
        Ok(Response::new(proto::GetAllNoFSegmentsResponse { segments }))
    }

    // ---- GetNoFSegmentsByName ----
    pub(super) async fn get_nof_segments_by_name_impl(
        &self,
        request: Request<proto::GetNoFSegmentsByNameRequest>,
    ) -> Result<Response<proto::GetNoFSegmentsByNameResponse>, Status> {
        let req = request.into_inner();
        let owners = self
            .state
            .nof_segments
            .iter()
            .filter(|entry| entry.segment.name == req.segment_name)
            .map(|entry| {
                nof_segment_owner_to_proto(&NoFSegmentOwnerInfo {
                    segment_id: entry.segment.id,
                    client_id: entry.segment.client_id,
                })
            })
            .collect();
        Ok(Response::new(proto::GetNoFSegmentsByNameResponse { owners }))
    }

    // ---- PutStart ----
    pub(super) async fn put_start_impl(
        &self,
        request: Request<proto::PutStartRequest>,
    ) -> Result<Response<proto::PutStartResponse>, Status> {
        let req = request.into_inner();

        // C++ master_service.cpp:1287-1294 对空 key 和零长度 slice 进行校验
        if req.key.is_empty() {
            return Err(Status::invalid_argument("empty key"));
        }
        if req.slice_length == 0 {
            return Err(Status::invalid_argument("zero slice_length"));
        }

        let key = req.key.clone();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );

        if let Some(existing) = self.state.objects.get_mut(&key) {
            let has_completed = existing
                .replicas
                .iter()
                .any(|r| r.status == ReplicaStatus::Complete);
            if !has_completed {
                if let Some(start) = existing.put_start_time {
                    let elapsed = SystemTime::now()
                        .duration_since(start)
                        .unwrap_or_default();
                    if elapsed >= self.state.runtime_config.put_start_discard_timeout {
                        let old_replicas = existing.replicas.clone();
                        let expired = existing
                            .replicas
                            .iter()
                            .filter(|r| r.status == ReplicaStatus::Allocating)
                            .cloned()
                            .collect::<Vec<_>>();
                        self.state.objects.remove(&key);
                        self.state.processing_keys.remove(&key);
                        drop(existing);
                        if !expired.is_empty() {
                            release_replicas_scheduled(&self.state, expired);
                        } else if !old_replicas.is_empty() {
                            release_replicas(&self.state, &old_replicas);
                        }
                    } else {
                        return Err(Status::already_exists(format!(
                            "object already exists: {}",
                            key
                        )));
                    }
                } else {
                    return Err(Status::already_exists(format!(
                        "object already exists: {}",
                        key
                    )));
                }
            } else {
                return Err(Status::already_exists(format!(
                    "object already exists: {}",
                    key
                )));
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
        let replica_count = config.replica_num.max(1) as usize;

        let mut replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate_for_client(
                &key,
                Some(client_id),
                req.slice_length,
                replica_count,
                &config,
            )
        };
        if replicas.is_empty() && replica_count > 0 {
            return Err(Status::resource_exhausted(format!(
                "failed to allocate {replica_count} replica(s) for key {key}{}",
                PUT_NO_SPACE_HELPER_STR,
            )));
        }
        if config.nof_replica_num > 0 {
            let preferred_nof = if config.prefer_alloc_in_same_node {
                preferred_nof_segment_names(&self.state, &replicas)
            } else {
                Vec::new()
            };
            if config.prefer_alloc_in_same_node && preferred_nof.is_empty() {
                release_replicas(&self.state, &replicas);
                return Err(Status::invalid_argument(
                    "prefer_alloc_in_same_node requires matching NoF segment",
                ));
            }
            let nof_replicas = allocate_nof_replicas(
                &self.state,
                &key,
                req.slice_length,
                config.nof_replica_num as usize,
                &preferred_nof,
            )?;
            replicas.extend(nof_replicas);
        }
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> =
            replicas.iter().map(replica_to_proto).collect();

        let now = SystemTime::now();
        self.state.objects.insert(
            key.clone(),
            ObjectEntry {
                replicas,
                size: req.slice_length,
                last_access: now,
                soft_pinned: config.with_soft_pin,
                hard_pinned: config.with_hard_pin,
                data_type: config.data_type,
                client_id,
                put_start_time: Some(now),
                lease_timeout: None,
                soft_pin_timeout: None,
            },
        );
        self.state.processing_keys.insert(key, ());

        metrics::PUT_START_REQUESTS.inc();
        Ok(Response::new(proto::PutStartResponse {
            replicas: proto_replicas,
        }))
    }

    // ---- PutEnd ----
    pub(super) async fn put_end_impl(
        &self,
        request: Request<proto::PutEndRequest>,
    ) -> Result<Response<proto::PutEndResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            // C++ master_service.cpp:1368-1372 校验调用者身份，防止其他 client 越权 PutEnd
            if entry.client_id != client_id {
                return Err(Status::permission_denied("illegal client"));
            }
            // C++ master_service.cpp:1368-1372 校验 handle 有效性，若 handle 已失效则保留 Allocating 状态
            // C++ checks !replica.has_invalid_mem_handle() and !replica.has_invalid_nof_handle()
            // before marking replicas Complete. If handle became invalid, replica stays in Allocating.
            for r in &mut entry.replicas {
                let matches_type = match req.replica_type {
                    x if x == proto::replica_descriptor::ReplicaType::All as i32 => true,
                    x if x == proto::replica_descriptor::ReplicaType::Memory as i32 => {
                        r.replica_type == ReplicaType::Memory
                    }
                    x if x == proto::replica_descriptor::ReplicaType::NofSsd as i32 => {
                        r.replica_type == ReplicaType::NoFSsd
                    }
                    _ => r.replica_type == ReplicaType::Memory,
                };
                if matches_type && r.status == ReplicaStatus::Allocating {
                    // C++ master_service.cpp:1368-1372 检查 !replica.has_invalid_mem_handle()
                    // 和 !replica.has_invalid_nof_handle()。handle 失效时 replica 保持在 Allocating 状态。
                    if r.handle_valid {
                        r.status = ReplicaStatus::Complete;
                    }
                }
            }
            let now = SystemTime::now();
            let new_lease = now
                .checked_add(self.state.runtime_config.lease_ttl)
                .unwrap_or(now);
            entry.lease_timeout = Some(match entry.lease_timeout {
                Some(current) if current > new_lease => current,
                _ => new_lease,
            });
            if entry.soft_pinned {
                let new_soft_pin = now
                    .checked_add(self.state.runtime_config.soft_pin_ttl)
                    .unwrap_or(now);
                entry.soft_pin_timeout = Some(match entry.soft_pin_timeout {
                    Some(current) if current > new_soft_pin => current,
                    _ => new_soft_pin,
                });
            }
            let all_complete = entry.replicas.iter().all(|r| r.status == ReplicaStatus::Complete);
            let size = entry.size;
            let offload_enabled = !self.state.runtime_config.offload_on_evict;
            drop(entry);
            if offload_enabled {
                push_offloading_queue(&self.state, client_id, &req.key, size);
            }
            // C++ 只在所有 replica 都 Complete 时才从 processing_keys 中移除，防止并发冲突
            // C++ only removes from processing_keys when ALL replicas are complete
            // AND the object is in the processing set.
            if all_complete && self.state.processing_keys.contains_key(&req.key) {
                self.state.processing_keys.remove(&req.key);
            }
            self.oplog_manager.lock().record_put_end(&req.key, size);
        }
        metrics::PUT_END_REQUESTS.inc();
        Ok(Response::new(proto::PutEndResponse {}))
    }

    // ---- AddReplica ----
    pub(super) async fn add_replica_impl(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
        let req = request.into_inner();
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
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
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
        } else if replica.replica_type == ReplicaType::LocalDisk {
            self.state.objects.insert(
                req.key.clone(),
                ObjectEntry {
                    size: replica.size,
                    replicas: vec![replica],
                    last_access: SystemTime::now(),
                    soft_pinned: false,
                    hard_pinned: false,
                    data_type: ObjectDataType::Unknown,
                    client_id,
                    put_start_time: None,
                    lease_timeout: None,
                    soft_pin_timeout: None,
                },
            );
        }
        Ok(Response::new(proto::AddReplicaResponse {}))
    }

    // ---- GetReplicaList ----
    pub(super) async fn get_replica_list_impl(
        &self,
        request: Request<proto::GetReplicaListRequest>,
    ) -> Result<Response<proto::GetReplicaListResponse>, Status> {
        let req = request.into_inner();
        match self.state.objects.get_mut(&req.key) {
            Some(mut entry) => {
                entry.last_access = SystemTime::now();
                let now = SystemTime::now();
                let new_lease = now
                    .checked_add(self.state.runtime_config.lease_ttl)
                    .unwrap_or(now);
                entry.lease_timeout = Some(match entry.lease_timeout {
                    Some(current) if current > new_lease => current,
                    _ => new_lease,
                });
                if entry.soft_pinned {
                    let new_soft_pin = now
                        .checked_add(self.state.runtime_config.soft_pin_ttl)
                        .unwrap_or(now);
                    entry.soft_pin_timeout = Some(match entry.soft_pin_timeout {
                        Some(current) if current > new_soft_pin => current,
                        _ => new_soft_pin,
                    });
                }
                let promotion_eligible = !entry.replicas.iter().any(|replica| {
                    replica.replica_type == ReplicaType::Memory
                        && replica.status == ReplicaStatus::Complete
                }) && entry.replicas.iter().any(|replica| {
                    replica.replica_type == ReplicaType::LocalDisk
                        && replica.status == ReplicaStatus::Complete
                });
                // C++ master_service.cpp:1097-1100 只返回 COMPLETED 状态的 replica
                // C++ filters by fn_is_completed; return REPLICA_IS_NOT_READY if empty.
                let completed_replicas: Vec<_> = entry
                    .replicas
                    .iter()
                    .filter(|r| r.status == ReplicaStatus::Complete)
                    .map(replica_to_proto)
                    .collect();
                if completed_replicas.is_empty() {
                    return Err(Status::failed_precondition("replica is not ready"));
                }
                drop(entry);
                if promotion_eligible {
                    try_push_promotion_queue(&self.state, &req.key);
                }
                metrics::GET_REQUESTS.inc();
                let lease_ttl_ms = self.state.runtime_config.lease_ttl.as_millis() as u64;
                Ok(Response::new(proto::GetReplicaListResponse { replicas: completed_replicas, lease_ttl_ms }))
            }
            None => Err(Status::not_found(format!("key not found: {}", req.key))),
        }
    }

    // ---- GetReplicaListByRegex ----
    pub(super) async fn get_replica_list_by_regex_impl(
        &self,
        request: Request<proto::GetReplicaListByRegexRequest>,
    ) -> Result<Response<proto::GetReplicaListByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.key_regex)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        // 遍历所有 key，按正则匹配后返回 COMPLETED 状态的 replica 列表
        for entry in self.state.objects.iter() {
            if pattern.is_match(entry.key()) {
                // Only include COMPLETE replicas, matching C++ GetReplicaListByRegex semantics
                let completed_replicas: Vec<_> = entry
                    .replicas
                    .iter()
                    .filter(|r| r.status == ReplicaStatus::Complete)
                    .map(replica_to_proto)
                    .collect();

                // Skip keys that match but have no complete replicas
                if completed_replicas.is_empty() {
                    tracing::warn!(
                        "key={} matched by regex, but has no complete replicas.",
                        entry.key()
                    );
                    continue;
                }

                entries.push(proto::get_replica_list_by_regex_response::ObjectEntry {
                    key: entry.key().clone(),
                    replicas: completed_replicas,
                });
            }
        }

        metrics::GET_REQUESTS.inc();
        Ok(Response::new(proto::GetReplicaListByRegexResponse { entries }))
    }

    // ---- Remove ----
    pub(super) async fn remove_impl(
        &self,
        request: Request<proto::RemoveRequest>,
    ) -> Result<Response<proto::RemoveResponse>, Status> {
        let req = request.into_inner();
        if !req.force && self.state.replication_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition(
                "object has an ongoing replication task",
            ));
        }
        if !req.force {
            if let Some(entry) = self.state.objects.get(&req.key) {
                // C++ master_service.cpp:2314 只有 lease 过期或 force=true 才允许删除
                if !is_lease_expired(&entry) {
                    return Err(Status::failed_precondition("object has lease"));
                }
                // C++ 只有所有 replica 都 Complete 才允许删除
                if !entry
                    .replicas
                    .iter()
                    .all(|r| r.status == ReplicaStatus::Complete)
                {
                    return Err(Status::failed_precondition("replica is not ready"));
                }
            }
        }
        if let Some((_, object)) = self.state.objects.remove(&req.key) {
            clear_offloading_task(&self.state, &req.key);
            clear_promotion_task(&self.state, &req.key);
            release_replicas(&self.state, &object.replicas);
            self.oplog_manager.lock().record_remove(&req.key);
        }
        metrics::REMOVE_REQUESTS.inc();
        Ok(Response::new(proto::RemoveResponse {}))
    }

    // ---- RemoveByRegex ----
    pub(super) async fn remove_by_regex_impl(
        &self,
        request: Request<proto::RemoveByRegexRequest>,
    ) -> Result<Response<proto::RemoveByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut removed = 0i64;
        let keys_to_remove: Vec<String> = self
            .state
            .objects
            .iter()
            .filter(|entry| pattern.is_match(entry.key()))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            if !req.force && self.state.replication_tasks.contains_key(&key) {
                continue;
            }
            if !req.force {
                if let Some(entry) = self.state.objects.get(&key) {
                    if !is_lease_expired(&entry) {
                        continue;
                    }
                    if !entry
                        .replicas
                        .iter()
                        .all(|r| r.status == ReplicaStatus::Complete)
                    {
                        continue;
                    }
                }
            }
            if let Some((_, object)) = self.state.objects.remove(&key) {
                clear_offloading_task(&self.state, &key);
                clear_promotion_task(&self.state, &key);
                release_replicas(&self.state, &object.replicas);
                removed += 1;
            }
        }

        metrics::REMOVE_BY_REGEX_REQUESTS.inc();
        metrics::REMOVE_REQUESTS.inc_by(removed as u64);
        Ok(Response::new(proto::RemoveByRegexResponse {
            removed_count: removed,
        }))
    }

    // ---- QueryByRegex ----
    pub(super) async fn query_by_regex_impl(
        &self,
        request: Request<proto::QueryByRegexRequest>,
    ) -> Result<Response<proto::QueryByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        for entry in self.state.objects.iter() {
            if pattern.is_match(entry.key()) {
                let r = entry.replicas.iter().map(replica_to_proto).collect();
                entries.push(proto::query_by_regex_response::Entry {
                    key: entry.key().clone(),
                    replicas: r,
                });
            }
        }
        Ok(Response::new(proto::QueryByRegexResponse { entries }))
    }

    // ---- QuerySegments ----
    pub(super) async fn query_segments_impl(
        &self,
        request: Request<proto::QuerySegmentsRequest>,
    ) -> Result<Response<proto::QuerySegmentsResponse>, Status> {
        let req = request.into_inner();
        for entry in self.state.segments.iter() {
            if entry.segment.name == req.segment_name {
                return Ok(Response::new(proto::QuerySegmentsResponse {
                    total_size: entry.segment.size,
                    used_size: entry.used,
                }));
            }
        }
        Err(Status::not_found("segment not found"))
    }

    // ---- QueryIp ----
    pub(super) async fn query_ip_impl(
        &self,
        request: Request<proto::QueryIpRequest>,
    ) -> Result<Response<proto::QueryIpResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let addresses = addresses_for_client(&self.state, client_id);
        if addresses.is_empty() {
            Err(Status::not_found("client not found"))
        } else {
            Ok(Response::new(proto::QueryIpResponse { addresses }))
        }
    }

    // ---- Upsert ----
    pub(super) async fn upsert_impl(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
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
        let replica_count = config.replica_num.max(1) as usize;

        // C++ 检查是否有进行中的 replication/offloading 任务，以及 replica 是否 busy
        // C++ checks if replication_tasks or offloading_tasks exist for the key.
        if self.state.replication_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition("object has replication task"));
        }
        if self.state.offloading_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition("object has offloading task"));
        }

        let (replicas, previous_soft_pinned, previous_hard_pinned) = if let Some(existing) =
            self.state.objects.get(&req.key)
        {
            // C++ checks HasReplica(&Replica::fn_is_busy)
            if existing.replicas.iter().any(|r| r.refcnt > 0) {
                return Err(Status::failed_precondition("object replica busy"));
            }
            if existing.size == req.slice_length {
                (
                    existing.replicas.clone(),
                    existing.soft_pinned,
                    existing.hard_pinned,
                )
            } else {
                let previous_soft_pinned = existing.soft_pinned;
                let previous_hard_pinned = existing.hard_pinned;
                let old_replicas = existing.replicas.clone();
                drop(existing);
                release_replicas(&self.state, &old_replicas);
                let mut allocator = self.state.allocator.write();
                (
                    allocator.allocate_for_client(
                        &req.key,
                        Some(client_id),
                        req.slice_length,
                        replica_count,
                        &config,
                    ),
                    previous_soft_pinned,
                    previous_hard_pinned,
                )
            }
        } else {
            let mut allocator = self.state.allocator.write();
            (
                allocator.allocate_for_client(
                    &req.key,
                    Some(client_id),
                    req.slice_length,
                    replica_count,
                    &config,
                ),
                false,
                false,
            )
        };
        let mut replicas = replicas;
        if config.nof_replica_num > 0 {
            let preferred_nof = if config.prefer_alloc_in_same_node {
                preferred_nof_segment_names(&self.state, &replicas)
            } else {
                Vec::new()
            };
            if config.prefer_alloc_in_same_node && preferred_nof.is_empty() {
                release_replicas(&self.state, &replicas);
                return Err(Status::invalid_argument(
                    "prefer_alloc_in_same_node requires matching NoF segment",
                ));
            }
            let nof_replicas = allocate_nof_replicas(
                &self.state,
                &req.key,
                req.slice_length,
                config.nof_replica_num as usize,
                &preferred_nof,
            )?;
            replicas.extend(nof_replicas);
        }
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> =
            replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(
            req.key.clone(),
            ObjectEntry {
                replicas,
                size: req.slice_length,
                last_access: SystemTime::now(),
                soft_pinned: config.with_soft_pin || previous_soft_pinned,
                hard_pinned: config.with_hard_pin || previous_hard_pinned,
                data_type: config.data_type,
                client_id,
                put_start_time: Some(SystemTime::now()),
                lease_timeout: None,
                soft_pin_timeout: None,
            },
        );

        Ok(Response::new(proto::UpsertResponse {
            replicas: proto_replicas,
        }))
    }
}

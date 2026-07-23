use super::*;

impl MasterServiceImpl {
    // ---- GetReplicaList ----
    // 返回对象的所有 Complete 副本列表，用于客户端选择传输端点。
    // 三阶段设计：(1) 读锁获取副本列表 → (2) 写锁更新 lease/软 pin 超时（微秒级）
    // → (3) 锁外检查是否符合 promotion 条件并入队。三阶段设计避免了读操作长时间持写锁。
    //
    // Return all Complete replicas for an object, used by clients to select transfer endpoints.
    // Three-phase design: (1) read lock to get replica list → (2) write lock for lease/soft-pin timeout update (microseconds)
    // → (3) promotion eligibility check outside lock and enqueue. Avoids holding write lock for long during reads.
    pub(super) async fn get_replica_list_impl(
        &self,
        request: Request<proto::GetReplicaListRequest>,
    ) -> Result<Response<proto::GetReplicaListResponse>, Status> {
        let req = request.into_inner();
        let response = self.replica_list_for_key(&req.tenant_id, &req.key)?;
        Ok(Response::new(response))
    }

    pub(super) fn replica_list_for_key(
        &self,
        tenant_id: &str,
        key: &str,
    ) -> Result<proto::GetReplicaListResponse, Status> {
        let scoped_key = make_tenant_scoped_key(tenant_id, key);

        // Phase 1: read-only (uses get() — shared lock, allows concurrent reads).
        // 阶段 1：只读（使用 get() — 共享锁，允许并发读）
        let (completed_replicas, promotion_eligible, first_replica_type, object_size) =
            match self.state.objects.get(&scoped_key) {
                Some(entry) => {
                    // 符合 promotion 条件：没有任何 Memory Complete 副本 + 有 LocalDisk Complete 副本
                    // Promotion eligible: no Memory Complete replicas + at least one LocalDisk Complete replica
                    let eligible = !entry.replicas.iter().any(|replica| {
                        replica.replica_type == ReplicaType::Memory
                            && replica.status == ReplicaStatus::Complete
                    }) && entry.replicas.iter().any(|replica| {
                        replica.replica_type == ReplicaType::LocalDisk
                            && replica.status == ReplicaStatus::Complete
                    });
                    let complete = entry
                        .replicas
                        .iter()
                        .filter(|r| r.status == ReplicaStatus::Complete)
                        .collect::<Vec<_>>();
                    let Some(first) = complete.first() else {
                        return Err(Status::failed_precondition("replica is not ready"));
                    };
                    let first_replica_type = first.replica_type;
                    let replicas = complete
                        .into_iter()
                        .map(replica_to_proto)
                        .collect::<Vec<_>>();
                    (replicas, eligible, first_replica_type, entry.size)
                }
                None => return Err(Status::not_found(format!("key not found: {key}"))),
            };

        // Phase 2: brief write lock for timestamp updates only (microseconds).
        // 阶段 2：短暂写锁仅更新时间戳（微秒级）
        let mut group_to_refresh = None;
        if let Some(mut entry) = self.state.objects.get_mut(&scoped_key) {
            entry.last_access = SystemTime::now();
            entry.grant_lease(
                self.state.runtime_config.lease_ttl,
                self.state.runtime_config.soft_pin_ttl,
            );
            if !entry.group_id.is_empty() {
                group_to_refresh = Some((entry.tenant_id.clone(), entry.group_id.clone()));
            }
        }
        if let Some((tenant_id, group_id)) = group_to_refresh {
            self.grant_group_lease(&tenant_id, &group_id);
        }

        // Phase 3: promotion after all locks released.
        // 阶段 3：锁释放后进行 promotion 条件检查和入队
        if promotion_eligible {
            let _ = try_push_promotion_queue(&self.state, &scoped_key, true);
        }
        metrics::GET_REQUESTS.inc();
        record_cache_hit_metrics(first_replica_type, object_size);
        let lease_ttl_ms = self.state.runtime_config.lease_ttl.as_millis() as u64;
        Ok(proto::GetReplicaListResponse {
            replicas: completed_replicas,
            lease_ttl_ms,
        })
    }

    pub(crate) fn replica_list_for_key_for_admin(
        &self,
        tenant_id: &str,
        key: &str,
    ) -> Result<proto::GetReplicaListResponse, Status> {
        let scoped_key = make_tenant_scoped_key(tenant_id, key);
        let completed_replicas = match self.state.objects.get(&scoped_key) {
            Some(entry) => {
                let replicas: Vec<_> = entry
                    .replicas
                    .iter()
                    .filter(|r| r.status == ReplicaStatus::Complete)
                    .map(replica_to_proto)
                    .collect();
                if replicas.is_empty() {
                    return Err(Status::failed_precondition("replica is not ready"));
                }
                replicas
            }
            None => return Err(Status::not_found(format!("key not found: {key}"))),
        };

        Ok(proto::GetReplicaListResponse {
            replicas: completed_replicas,
            lease_ttl_ms: self.state.runtime_config.lease_ttl.as_millis() as u64,
        })
    }

    pub fn batch_get_replica_list_for_admin(
        &self,
        keys: &[String],
        tenant_id: &str,
    ) -> Vec<proto::BatchGetReplicaListResult> {
        keys.iter()
            .map(
                |key| match self.replica_list_for_key_for_admin(tenant_id, key) {
                    Ok(response) => proto::BatchGetReplicaListResult {
                        status: 0,
                        response: Some(response),
                        error_message: String::new(),
                    },
                    Err(status) => proto::BatchGetReplicaListResult {
                        status: admin_query_status_code(&status),
                        response: None,
                        error_message: status.message().to_string(),
                    },
                },
            )
            .collect()
    }

    // ---- GetReplicaListByRegex ----
    // 按正则表达式批量获取对象的 Complete 副本列表。过滤掉无 Complete 副本的匹配 key。
    // Batch fetch Complete replica lists by regex; filters out matching keys with no Complete replicas.
    pub(super) async fn get_replica_list_by_regex_impl(
        &self,
        request: Request<proto::GetReplicaListByRegexRequest>,
    ) -> Result<Response<proto::GetReplicaListByRegexResponse>, Status> {
        let req = request.into_inner();
        let tenant_filter = normalize_tenant_id(&req.tenant_id);
        let pattern = regex::Regex::new(&req.key_regex)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        let mut lease_keys = vec![];
        // 遍历所有 key，按租户过滤后，对 user_key 进行正则匹配
        // Iterate all keys, filter by tenant, match regex against user_key
        for entry in self.state.objects.iter() {
            if entry.tenant_id != tenant_filter {
                continue;
            }
            if !pattern.is_match(&entry.user_key) {
                continue;
            }
            // Only include COMPLETE replicas, matching C++ GetReplicaListByRegex semantics
            let completed_replicas: Vec<_> = entry
                .replicas
                .iter()
                .filter(|r| r.status == ReplicaStatus::Complete)
                .map(replica_to_proto)
                .collect();

            // Skip keys that match but have no complete replicas
            // 跳过匹配但无 Complete 副本的 key
            if completed_replicas.is_empty() {
                tracing::warn!(
                    "user_key={} matched by regex, but has no complete replicas.",
                    entry.user_key
                );
                continue;
            }

            entries.push(proto::get_replica_list_by_regex_response::ObjectEntry {
                key: entry.key().clone(),
                replicas: completed_replicas,
                tenant_id: entry.tenant_id.clone(),
                user_key: entry.user_key.clone(),
            });
            lease_keys.push(entry.key().clone());
        }

        let mut groups_to_refresh = Vec::new();
        for key in lease_keys {
            if let Some(mut entry) = self.state.objects.get_mut(&key) {
                entry.last_access = SystemTime::now();
                entry.grant_lease(
                    self.state.runtime_config.lease_ttl,
                    self.state.runtime_config.soft_pin_ttl,
                );
                if !entry.group_id.is_empty() {
                    groups_to_refresh.push((entry.tenant_id.clone(), entry.group_id.clone()));
                }
            }
        }
        for (tenant_id, group_id) in groups_to_refresh {
            self.grant_group_lease(&tenant_id, &group_id);
        }

        metrics::GET_REQUESTS.inc();
        Ok(Response::new(proto::GetReplicaListByRegexResponse {
            entries,
        }))
    }

    // ---- Remove ----
    // 删除指定对象及其所有副本。非 force 模式会校验：(1) 无进行中的复制任务
    // (2) lease 已过期 (3) 所有副本均 Complete。同时清理 offload/promotion 任务和客户端索引。
    //
    // Delete an object and all its replicas. Non-force mode validates: (1) no ongoing replication task,
    // (2) lease is expired, (3) all replicas are Complete. Also cleans up offload/promotion tasks and client index.
    pub(super) async fn remove_impl(
        &self,
        request: Request<proto::RemoveRequest>,
    ) -> Result<Response<proto::RemoveResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        if self.state.replication_tasks.contains_key(&scoped_key) {
            return Err(Status::failed_precondition(
                "object has an ongoing replication task",
            ));
        }
        if let Some(entry) = self.state.objects.get(&scoped_key) {
            // C++ force only bypasses lease; complete replica and replication-task checks still apply.
            if !req.force && !is_lease_expired(&entry) {
                return Err(Status::failed_precondition("object has lease"));
            }
            if !entry
                .replicas
                .iter()
                .all(|r| r.status == ReplicaStatus::Complete)
            {
                return Err(Status::failed_precondition("replica is not ready"));
            }
        }
        let Some((_, object)) = self.state.objects.remove(&scoped_key) else {
            return Err(Status::not_found("key not found"));
        };
        // 先从目录摘除可以阻止新的读者获得该对象；若 allocator/后台索引清理失败，
        // 必须把原 ObjectEntry 放回，避免“RPC 失败但对象永久消失”的半提交状态。
        if let Err(status) = self.cleanup_removed_object(&scoped_key, &object) {
            self.state.objects.insert(scoped_key, object);
            return Err(status);
        }
        self.publish_kv_removed(&scoped_key, &object);
        metrics::REMOVE_REQUESTS.inc();
        Ok(Response::new(proto::RemoveResponse {}))
    }

    // ---- RemoveByRegex ----
    // 按正则批量删除对象，每个 key 执行与 Remove 相同的安全检查（force/lease/Complete）。
    // Batch delete objects by regex; each key goes through the same safety checks as Remove (force/lease/Complete).
    pub(super) async fn remove_by_regex_impl(
        &self,
        request: Request<proto::RemoveByRegexRequest>,
    ) -> Result<Response<proto::RemoveByRegexResponse>, Status> {
        let req = request.into_inner();
        let tenant_filter = normalize_tenant_id(&req.tenant_id);
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut removed = 0i64;
        let keys_to_remove: Vec<String> = self
            .state
            .objects
            .iter()
            .filter(|entry| entry.tenant_id == tenant_filter && pattern.is_match(&entry.user_key))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            if self.state.replication_tasks.contains_key(&key) {
                continue;
            }
            if let Some(entry) = self.state.objects.get(&key) {
                if !req.force && !is_lease_expired(&entry) {
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
            if let Some((_, object)) = self.state.objects.remove(&key) {
                if self.cleanup_removed_object(&key, &object).is_ok() {
                    self.publish_kv_removed(&key, &object);
                    removed += 1;
                } else {
                    self.state.objects.insert(key, object);
                }
            }
        }

        metrics::REMOVE_BY_REGEX_REQUESTS.inc();
        metrics::REMOVE_REQUESTS.inc_by(removed as u64);
        Ok(Response::new(proto::RemoveByRegexResponse {
            removed_count: removed,
        }))
    }

    // ---- QueryByRegex ----
    // 按正则查询对象及其所有副本（含非 Complete 状态），用于诊断和管理。
    // Query objects by regex including all replicas (including non-Complete), for diagnostics and admin.
    pub(super) async fn query_by_regex_impl(
        &self,
        request: Request<proto::QueryByRegexRequest>,
    ) -> Result<Response<proto::QueryByRegexResponse>, Status> {
        let req = request.into_inner();
        let tenant_filter = normalize_tenant_id(&req.tenant_id);
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        for entry in self.state.objects.iter() {
            if entry.tenant_id != tenant_filter {
                continue;
            }
            if !pattern.is_match(&entry.user_key) {
                continue;
            }
            let r = entry.replicas.iter().map(replica_to_proto).collect();
            entries.push(proto::query_by_regex_response::Entry {
                key: entry.key().clone(),
                replicas: r,
                tenant_id: entry.tenant_id.clone(),
                user_key: entry.user_key.clone(),
            });
        }
        Ok(Response::new(proto::QueryByRegexResponse { entries }))
    }

    // ---- QuerySegments ----
    // 按名称查询 segment 的总容量和已使用量，用于容量监控。
    // Query segment total capacity and used bytes by name, for capacity monitoring.
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
    // 查询指定客户端的 IP 地址列表，先查 clients 表，fallback 到 segment 名解析。
    // Query IP addresses of a client; checks clients table first, falls back to segment name resolution.
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
}

fn record_cache_hit_metrics(replica_type: ReplicaType, object_size: u64) {
    match replica_type {
        ReplicaType::Memory => {
            metrics::MEM_CACHE_HITS.inc();
            metrics::MEM_CACHE_HIT_BYTES.inc_by(object_size);
        }
        ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::NoFSsd => {
            metrics::FILE_CACHE_HITS.inc();
            metrics::FILE_CACHE_HIT_BYTES.inc_by(object_size);
        }
        ReplicaType::All => {}
    }
    metrics::VALID_GETS.inc();
}

fn admin_query_status_code(status: &Status) -> i32 {
    match status.code() {
        tonic::Code::NotFound => -1,
        tonic::Code::FailedPrecondition => -5,
        tonic::Code::PermissionDenied => -3,
        _ => -6,
    }
}

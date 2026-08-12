use super::*;
use crate::service::helpers::replica_is_routable;
use crate::service::proto_conv::replica_to_proto_for_state;

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
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let response = self.replica_list_for_key(&tenant_id, &req.key)?;
        Ok(Response::new(response))
    }

    pub(super) fn replica_list_for_key(
        &self,
        tenant_id: &TenantId,
        key: &str,
    ) -> Result<proto::GetReplicaListResponse, Status> {
        metrics::TOTAL_GETS.inc();
        let scoped_key = tenant_id.make_scoped_key(key);
        let Some(entry) = self.object_snapshot_and_grant_lease(&scoped_key, true)? else {
            return if self.state.objects.contains_key(&scoped_key) {
                Err(Status::failed_precondition("replica is not ready"))
            } else {
                Err(Status::not_found(format!("key not found: {key}")))
            };
        };
        let promotion_eligible = !entry.replicas.iter().any(|replica| {
            replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete
        }) && entry.replicas.iter().any(|replica| {
            replica.replica_type == ReplicaType::LocalDisk
                && replica_is_routable(&self.state, replica)
        });
        let complete = entry
            .replicas
            .iter()
            .filter(|replica| replica_is_routable(&self.state, replica))
            .collect::<Vec<_>>();
        let first_replica_type = complete[0].replica_type;
        let completed_replicas = complete
            .into_iter()
            .map(|replica| replica_to_proto_for_state(&self.state, replica))
            .collect();
        let object_size = entry.size;

        // Promotion admission reacquires the tenant-scoped mutation gate.
        if promotion_eligible {
            let _ = try_push_promotion_queue(&self.state, &scoped_key, true);
        }
        record_cache_hit_metrics(first_replica_type, object_size);
        let lease_ttl_ms = self.state.runtime_config.lease_ttl.as_millis() as u64;
        Ok(proto::GetReplicaListResponse {
            replicas: completed_replicas,
            lease_ttl_ms,
        })
    }

    pub(crate) fn replica_list_for_key_for_admin(
        &self,
        tenant_id: &TenantId,
        key: &str,
    ) -> Result<proto::GetReplicaListResponse, Status> {
        let scoped_key = tenant_id.make_scoped_key(key);
        let completed_replicas = match self.state.objects.get(&scoped_key) {
            Some(entry) => {
                let replicas: Vec<_> = entry
                    .replicas
                    .iter()
                    .filter(|r| replica_is_routable(&self.state, r))
                    .map(|replica| replica_to_proto_for_state(&self.state, replica))
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
        let Ok(tenant_id) = resolve_request_tenant(tenant_id, true) else {
            return keys
                .iter()
                .map(|_| proto::BatchGetReplicaListResult {
                    status: -6,
                    response: None,
                    error_message: "invalid tenant id".to_string(),
                })
                .collect();
        };
        self.batch_get_replica_list_for_admin_tenant(keys, &tenant_id)
    }

    pub(crate) fn batch_get_replica_list_for_admin_tenant(
        &self,
        keys: &[String],
        tenant_id: &TenantId,
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

    /// Test-only entry point mirroring the C++ `BatchGetReplicaListForAdmin`:
    /// the read-only admin batch query must neither admit promotion tasks nor
    /// update store-observed cache-hit counters.
    #[doc(hidden)]
    pub fn batch_replica_lists_for_admin_for_test(
        &self,
        keys: &[String],
        tenant_id: &str,
    ) -> Vec<proto::BatchGetReplicaListResult> {
        self.batch_get_replica_list_for_admin(keys, tenant_id)
    }

    // ---- GetReplicaListByRegex ----
    // 按正则表达式批量获取对象的 Complete 副本列表。过滤掉无 Complete 副本的匹配 key。
    // Batch fetch Complete replica lists by regex; filters out matching keys with no Complete replicas.
    pub(super) async fn get_replica_list_by_regex_impl(
        &self,
        request: Request<proto::GetReplicaListByRegexRequest>,
    ) -> Result<Response<proto::GetReplicaListByRegexResponse>, Status> {
        let req = request.into_inner();
        let tenant_filter = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let pattern = regex::Regex::new(&req.key_regex)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let candidate_keys = self
            .state
            .objects
            .iter()
            .filter(|entry| entry.tenant_id == tenant_filter && pattern.is_match(&entry.user_key))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let mut entries = vec![];
        for key in candidate_keys {
            let Some(entry) = self.object_snapshot_and_grant_lease(&key, true)? else {
                continue;
            };
            if entry.tenant_id != tenant_filter || !pattern.is_match(&entry.user_key) {
                continue;
            }
            let completed_replicas = entry
                .replicas
                .iter()
                .filter(|replica| replica_is_routable(&self.state, replica))
                .map(|replica| replica_to_proto_for_state(&self.state, replica))
                .collect::<Vec<_>>();
            if completed_replicas.is_empty() {
                tracing::warn!(
                    "user_key={} matched by regex, but has no complete replicas.",
                    entry.user_key
                );
                continue;
            }
            entries.push(proto::get_replica_list_by_regex_response::ObjectEntry {
                key: key.clone(),
                replicas: completed_replicas,
                tenant_id: entry.tenant_id.as_str().to_owned(),
                user_key: entry.user_key.clone(),
            });
        }

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
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
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
        if !self.state.objects.contains_key(&scoped_key) {
            return Err(Status::not_found("key not found"));
        }
        self.persist_remove_before_cleanup(&scoped_key)?;
        let (_, object) = self
            .state
            .objects
            .remove(&scoped_key)
            .expect("key mutation guard preserves object after durable remove");
        // Once the durable tombstone succeeds, cleanup failure must remain a
        // fenced authoritative removal. Re-inserting descriptors after quota,
        // index, task, or allocator cleanup may have partially committed would
        // resurrect an object that recovery will correctly keep deleted.
        if let Err(status) = self.cleanup_removed_object(&scoped_key, &object) {
            return Err(status);
        }
        self.publish_kv_removed(&scoped_key, &object);
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
        let tenant_filter = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
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
            let _mutation_guard = self.state.key_mutations.lock(&key);
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
            if self.state.objects.contains_key(&key) {
                self.persist_remove_before_cleanup(&key)?;
                let (_, object) = self
                    .state
                    .objects
                    .remove(&key)
                    .expect("key mutation guard preserves object after durable remove");
                if let Err(status) = self.cleanup_removed_object(&key, &object) {
                    return Err(status);
                }
                self.publish_kv_removed(&key, &object);
                removed += 1;
            }
        }

        metrics::REMOVE_BY_REGEX_REQUESTS.inc();
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
        let tenant_filter = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
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
            let r = entry
                .replicas
                .iter()
                .map(|replica| replica_to_proto_for_state(&self.state, replica))
                .collect();
            entries.push(proto::query_by_regex_response::Entry {
                key: entry.key().clone(),
                replicas: r,
                tenant_id: entry.tenant_id.as_str().to_owned(),
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
        let mut total_size = 0u64;
        let mut used_size = 0u64;
        let mut found = false;
        for entry in self.state.segments.iter() {
            if entry.segment.name == req.segment_name {
                found = true;
                total_size = total_size.checked_add(entry.segment.size).ok_or_else(|| {
                    Status::internal("segment capacity overflow while aggregating shards")
                })?;
                used_size = used_size.checked_add(entry.used).ok_or_else(|| {
                    Status::internal("segment usage overflow while aggregating shards")
                })?;
            }
        }
        if !found {
            return Err(Status::not_found("segment not found"));
        }
        Ok(Response::new(proto::QuerySegmentsResponse {
            total_size,
            used_size,
        }))
    }

    // ---- QueryIp ----
    // 查询指定客户端的传输端点 IP 地址列表。
    // Query transfer-endpoint IP addresses for a client.
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
        let Some(addresses) = query_ip_addresses_for_client(&self.state, client_id) else {
            return Err(Status::not_found("client not found"));
        };
        Ok(Response::new(proto::QueryIpResponse { addresses }))
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

use super::super::*;

impl MasterServiceImpl {
    // ---- Remote pull coordination ----
    // 分布式协调：确保同一 key 只有一个节点从远端（S3）拉取数据，其余等待。
    //
    // 流程：
    //   Node A miss → AcquireRemotePull(key) → PULL → S3.fetch → PutEnd → CompleteRemotePull
    //   Node B miss → AcquireRemotePull(key) → WAIT → 重试 GetReplicaList（数据已由 A 写入 store）
    //
    // Distributed coordination: ensures only one node pulls a key from remote source (S3);
    // others wait and retry GetReplicaList after data is written to store.

    /// 获取远端拉取权：若已有节点在拉取则返回 WAIT，否则返回 PULL。
    /// Acquire remote pull right: returns WAIT if another node is already pulling, otherwise PULL.
    pub(crate) async fn acquire_remote_pull_impl(
        &self,
        request: Request<proto::AcquireRemotePullRequest>,
    ) -> Result<Response<proto::AcquireRemotePullResponse>, Status> {
        let req = request.into_inner();
        let proto_id = req
            .client_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("client_id is required"))?;
        let client_id = uuid_from_proto(proto_id);
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        validate_user_key(&req.key)?;
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let key = tenant_id.make_scoped_key(&req.key);

        if !self.state.runtime_config.remote_source_enabled {
            return Ok(Response::new(proto::AcquireRemotePullResponse {
                action: proto::RemotePullAction::Abandon as i32,
                retry_after_ms: 0,
            }));
        }

        // Acquisition, stale-entry replacement, completion, and release share
        // the same tenant-scoped key gate. Otherwise two callers can both
        // observe an expired entry and each publish itself as the pull owner.
        let _mutation_guard = self.state.key_mutations.lock(&key);

        // Check if another node is already pulling this key
        // 检查是否有其他节点正在拉取此 key
        if let Some(entry) = self.state.pending_remote_pulls.get(&key) {
            let elapsed = entry.started_at.elapsed();
            let ttl = self.state.runtime_config.remote_pull_ttl;
            if elapsed < ttl {
                // 已有节点在拉取，返回 WAIT / Another node is pulling; return WAIT
                return Ok(Response::new(proto::AcquireRemotePullResponse {
                    action: proto::RemotePullAction::Wait as i32,
                    retry_after_ms: (ttl.saturating_sub(elapsed)).as_millis().min(5000) as u64,
                }));
            }
            // Stale entry — remove and let this node pull
            // 条目过期 — 移除并让此节点拉取
            drop(entry);
            self.state.pending_remote_pulls.remove(&key);
        }

        tracing::debug!(key = %key, client = %client_id, "remote pull acquired");
        self.state.pending_remote_pulls.insert(
            key,
            super::super::state::RemotePullEntry {
                started_at: std::time::Instant::now(),
                owner_client_id: client_id,
            },
        );
        Ok(Response::new(proto::AcquireRemotePullResponse {
            action: proto::RemotePullAction::Pull as i32,
            retry_after_ms: 0,
        }))
    }

    /// 完成远端拉取：从 pending_remote_pulls 中移除条目，释放拉取权。
    /// Complete remote pull: remove entry from pending_remote_pulls, release pull right.
    pub(crate) async fn complete_remote_pull_impl(
        &self,
        request: Request<proto::CompleteRemotePullRequest>,
    ) -> Result<Response<proto::CompleteRemotePullResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("client_id is required"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        validate_user_key(&req.key)?;
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let key = tenant_id.make_scoped_key(&req.key);

        let _mutation_guard = self.state.key_mutations.lock(&key);
        if let Some(entry) = self.state.pending_remote_pulls.get(&key) {
            if entry.owner_client_id != client_id {
                return Err(Status::permission_denied(
                    "remote pull is owned by another client",
                ));
            }
        } else {
            return Ok(Response::new(proto::CompleteRemotePullResponse {}));
        }
        let removed = self.state.pending_remote_pulls.remove(&key);
        tracing::debug!(
            key = %key,
            success = req.success,
            data_size = req.data_size,
            was_present = removed.is_some(),
            "remote pull completed"
        );

        Ok(Response::new(proto::CompleteRemotePullResponse {}))
    }

    /// 释放远端拉取权（放弃拉取）/ Release remote pull right (abandon pull).
    pub(crate) async fn release_remote_pull_impl(
        &self,
        request: Request<proto::ReleaseRemotePullRequest>,
    ) -> Result<Response<proto::ReleaseRemotePullResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("client_id is required"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        validate_user_key(&req.key)?;
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let key = tenant_id.make_scoped_key(&req.key);

        let _mutation_guard = self.state.key_mutations.lock(&key);
        if let Some(entry) = self.state.pending_remote_pulls.get(&key) {
            if entry.owner_client_id != client_id {
                return Err(Status::permission_denied(
                    "remote pull is owned by another client",
                ));
            }
        } else {
            return Ok(Response::new(proto::ReleaseRemotePullResponse {}));
        }
        self.state.pending_remote_pulls.remove(&key);
        tracing::debug!(key = %key, "remote pull released");
        Ok(Response::new(proto::ReleaseRemotePullResponse {}))
    }
}

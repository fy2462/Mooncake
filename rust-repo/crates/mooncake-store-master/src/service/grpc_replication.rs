use super::*;

fn allocate_replica_on_segment(
    state: &MasterState,
    key: &str,
    size: u64,
    segment_name: &str,
) -> Result<ReplicaDescriptor, Status> {
    let is_nof = client_id_by_nof_segment_name(state, segment_name).is_some();
    if is_nof {
        let config = ReplicateConfig {
            preferred_segment: segment_name.to_string(),
            replica_num: 1,
            ..Default::default()
        };
        let replicas = state.nof_allocator.write().allocate(key, size, 1, &config);
        if replicas.len() == 1 && replicas[0].segment_name == segment_name {
            sync_nof_segment_usage(state, replicas.iter().map(|r| r.segment_id));
            let mut replica = replicas[0].clone();
            replica.replica_type = ReplicaType::NoFSsd;
            return Ok(replica);
        }
        if !replicas.is_empty() {
            release_replicas(state, &replicas);
        }
        return Err(Status::resource_exhausted(format!(
            "failed to allocate on NoF target segment: {segment_name}"
        )));
    }

    let config = ReplicateConfig {
        preferred_segment: segment_name.to_string(),
        replica_num: 1,
        ..Default::default()
    };
    let replicas = state.allocator.write().allocate(key, size, 1, &config);
    if replicas.len() == 1 && replicas[0].segment_name == segment_name {
        sync_segment_usage(state, replicas.iter().map(|r| r.segment_id));
        return Ok(replicas[0].clone());
    }
    if !replicas.is_empty() {
        release_replicas(state, &replicas);
    }
    Err(Status::resource_exhausted(format!(
        "failed to allocate on target segment: {segment_name}"
    )))
}

fn same_replica(a: &ReplicaDescriptor, b: &ReplicaDescriptor) -> bool {
    a.segment_id == b.segment_id && a.offset == b.offset && a.replica_type == b.replica_type
}

impl MasterServiceImpl {
    pub(super) async fn put_revoke_impl(
        &self,
        request: Request<proto::PutRevokeRequest>,
    ) -> Result<Response<proto::PutRevokeResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if self.state.replication_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition(
                "object has an ongoing replication task",
            ));
        }
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            if object_owner_client_id(&self.state, &object) != Some(client_id) {
                return Err(Status::permission_denied("object owned by different client"));
            }
            let mut removed = Vec::new();
            // C++ master_service.cpp:1492-1504 只允许撤销 PROCESSING 状态的 replica，已完成的不能撤销
            let mut all_completed = true;
            let mut has_matching = false;

            // Check if all matching replicas are already complete
            for replica in &object.replicas {
                let matches = match req.replica_type {
                    x if x == proto::replica_descriptor::ReplicaType::All as i32 => true,
                    x if x == proto::replica_descriptor::ReplicaType::NofSsd as i32 => {
                        replica.replica_type == ReplicaType::NoFSsd
                    }
                    _ => replica.replica_type == ReplicaType::Memory,
                };
                if matches {
                    has_matching = true;
                    if replica.status != ReplicaStatus::Complete {
                        all_completed = false;
                    }
                }
            }

            if has_matching && all_completed {
                return Err(Status::failed_precondition(
                    "invalid write: replica already completed",
                ));
            }

            // 仅移除 Allocating 等非 Complete 状态的 replica
            // Remove only non-complete matching replicas
            object.replicas.retain(|replica| {
                let matches = match req.replica_type {
                    x if x == proto::replica_descriptor::ReplicaType::All as i32 => true,
                    x if x == proto::replica_descriptor::ReplicaType::NofSsd as i32 => {
                        replica.replica_type == ReplicaType::NoFSsd
                    }
                    _ => replica.replica_type == ReplicaType::Memory,
                };
                if matches && replica.status != ReplicaStatus::Complete {
                    removed.push(replica.clone());
                    false
                } else {
                    true
                }
            });
            let remove_object = object.replicas.is_empty();
            drop(object);
            release_object_replicas(&self.state, &req.key, &removed);
            if remove_object {
                self.state.objects.remove(&req.key);
            }
        }
        metrics::PUT_REVOKE_REQUESTS.inc();
        Ok(Response::new(proto::PutRevokeResponse {}))
    }

    pub(super) async fn remove_all_impl(
        &self,
        request: Request<proto::RemoveAllRequest>,
    ) -> Result<Response<proto::RemoveAllResponse>, Status> {
        let req = request.into_inner();
        let keys = self
            .state
            .objects
            .iter()
            .filter(|entry| {
                if req.force {
                    return true;
                }
                // C++ 只有 lease 过期或 force=true 才删除
                !self.state.replication_tasks.contains_key(entry.key())
                    && is_lease_expired(entry.value())
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let mut removed_count = 0i64;
        for key in keys {
            if let Some((_, object)) = self.state.objects.remove(&key) {
                release_object_replicas(&self.state, &key, &object.replicas);
                self.state.replication_tasks.remove(&key);
                removed_count += 1;
            }
        }
        Ok(Response::new(proto::RemoveAllResponse { removed_count }))
    }

    pub(super) async fn get_storage_config_impl(
        &self,
        _request: Request<proto::GetStorageConfigRequest>,
    ) -> Result<Response<proto::GetStorageConfigResponse>, Status> {
        let cfg = &self.state.runtime_config;
        Ok(Response::new(proto::GetStorageConfigResponse {
            fs_dir: cfg.storage_fs_dir.clone(),
            enable_disk_eviction: cfg.enable_disk_eviction,
            quota_bytes: cfg.quota_bytes,
        }))
    }

    pub(super) async fn copy_start_impl(
        &self,
        request: Request<proto::CopyStartRequest>,
    ) -> Result<Response<proto::CopyStartResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let object = self
            .state
            .objects
            .get(&req.key)
            .ok_or(Status::not_found("key not found"))?;
        if self.state.replication_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition(
                "object already has an ongoing replication task",
            ));
        }
        let source = object
            .replicas
            .iter()
            .find(|replica| {
                replica.segment_name == req.source && replica.status == ReplicaStatus::Complete
            })
            .cloned()
            .ok_or(Status::invalid_argument("source segment not found"))?;
        let size = object.size;
        let existing = object.replicas.clone();
        drop(object);

        let mut allocated = Vec::new();
        for target in &req.targets {
            if existing
                .iter()
                .any(|replica| replica.segment_name == *target)
            {
                continue;
            }
            if client_id_by_replica_segment_name(&self.state, target).is_none() {
                release_object_replicas(&self.state, &req.key, &allocated);
                return Err(Status::invalid_argument(format!(
                    "target segment not mounted: {target}"
                )));
            }
            // C++ master_service.cpp:1867-1872 检查目标 segment 是否处于可分配状态
            // Gap 20: verify target segment is allocatable (Active)
            let is_active = self
                .state
                .segments
                .iter()
                .any(|e| e.segment.name == *target && e.status == proto::SegmentStatus::Active)
                || self
                    .state
                    .nof_segments
                    .iter()
                    .any(|e| e.segment.name == *target && e.status == proto::SegmentStatus::Active);
            if !is_active {
                release_object_replicas(&self.state, &req.key, &allocated);
                return Err(Status::failed_precondition(format!(
                    "target segment not active or not allocatable: {target}"
                )));
            }
            allocated.push(allocate_replica_on_segment(
                &self.state,
                &req.key,
                size,
                target,
            )?);
        }

        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            object.replicas.extend(allocated.clone());
        }
        // Pin source replica via refcnt
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            if let Some(src) = object.replicas.iter_mut().find(|r| same_replica(r, &source)) {
                src.inc_refcnt();
            }
        }
        self.state.replication_tasks.insert(
            req.key.clone(),
            ReplicationTaskEntry {
                client_id,
                kind: ReplicationTaskKind::Copy,
                source: source.clone(),
                targets: allocated.clone(),
            },
        );

        Ok(Response::new(proto::CopyStartResponse {
            source: Some(replica_to_proto(&source)),
            targets: allocated.iter().map(replica_to_proto).collect(),
        }))
    }

    pub(super) async fn copy_end_impl(
        &self,
        request: Request<proto::CopyEndRequest>,
    ) -> Result<Response<proto::CopyEndResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .replication_tasks
            .get(&req.key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Copy {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }
        let mut all_present = true;
        let mut source_invalid = false;
        // C++ master_service.cpp:1985-1994 CopyEnd 时检查 source replica 的 handle 有效性
        // 如果 source handle 已失效，中止操作并撤销 targets
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            // C++ master_service.cpp:1985-1988 检查 source replica 是否 still present 且 handle_valid
            match object.replicas.iter().find(|r| same_replica(r, &task.source)) {
                Some(source_replica) => {
                    if !source_replica.handle_valid {
                        source_invalid = true;
                    }
                }
                None => all_present = false,
            }
            if !source_invalid {
                for target in &task.targets {
                    match object
                        .replicas
                        .iter_mut()
                        .find(|replica| same_replica(replica, target))
                    {
                        // C++ master_service.cpp:1990-1994 检查每个 target 的 handle_valid
                        // handle 无效的 target 不标记为 Complete，保持在当前状态
                        Some(replica) => {
                            if replica.handle_valid {
                                replica.status = ReplicaStatus::Complete;
                            }
                        }
                        None => all_present = false,
                    }
                }
            }
        } else {
            all_present = false;
        }
        // Release source replica refcnt
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            if let Some(src) = object.replicas.iter_mut().find(|r| same_replica(r, &task.source)) {
                src.dec_refcnt();
            }
        }
        self.state.replication_tasks.remove(&req.key);
        // C++ master_service.cpp:1988-1992 如果 source handle 在 Copy 过程中失效，撤销 targets
        if source_invalid {
            let mut removed_targets = Vec::new();
            if let Some(mut object) = self.state.objects.get_mut(&req.key) {
                object.replicas.retain(|replica| {
                    let matched = task
                        .targets
                        .iter()
                        .any(|target| same_replica(replica, target));
                    if matched {
                        removed_targets.push(replica.clone());
                    }
                    !matched
                });
            }
            release_object_replicas(&self.state, &req.key, &removed_targets);
            return Err(Status::failed_precondition(
                "source replica handle became invalid during transfer",
            ));
        }
        if !all_present {
            return Err(Status::failed_precondition(
                "copy target missing during completion",
            ));
        }
        Ok(Response::new(proto::CopyEndResponse {}))
    }

    pub(super) async fn copy_revoke_impl(
        &self,
        request: Request<proto::CopyRevokeRequest>,
    ) -> Result<Response<proto::CopyRevokeResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .replication_tasks
            .get(&req.key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Copy {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }

        let mut removed = Vec::new();
        let mut remove_object = false;
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            object.replicas.retain(|replica| {
                let matched = task
                    .targets
                    .iter()
                    .any(|target| same_replica(replica, target));
                if matched {
                    removed.push(replica.clone());
                }
                !matched
            });
            remove_object = object.replicas.is_empty();
        }
        release_object_replicas(&self.state, &req.key, &removed);
        if remove_object {
            self.state.objects.remove(&req.key);
        }
        // Release source replica refcnt
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            if let Some(src) = object.replicas.iter_mut().find(|r| same_replica(r, &task.source)) {
                src.dec_refcnt();
            }
        }
        self.state.replication_tasks.remove(&req.key);
        Ok(Response::new(proto::CopyRevokeResponse {}))
    }

    pub(super) async fn move_start_impl(
        &self,
        request: Request<proto::MoveStartRequest>,
    ) -> Result<Response<proto::MoveStartResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if req.source == req.target {
            return Err(Status::invalid_argument("source and target must differ"));
        }
        let object = self
            .state
            .objects
            .get(&req.key)
            .ok_or(Status::not_found("key not found"))?;
        if self.state.replication_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition(
                "object already has an ongoing replication task",
            ));
        }
        let source = object
            .replicas
            .iter()
            .find(|replica| {
                replica.segment_name == req.source && replica.status == ReplicaStatus::Complete
            })
            .cloned()
            .ok_or(Status::invalid_argument("source segment not found"))?;
        let existing_target = object
            .replicas
            .iter()
            .find(|replica| replica.segment_name == req.target)
            .cloned();
        let size = object.size;
        drop(object);

        let target = match existing_target.clone() {
            Some(replica) => replica,
            None => {
                if client_id_by_replica_segment_name(&self.state, &req.target).is_none() {
                    return Err(Status::invalid_argument("target segment not mounted"));
                }
                let replica =
                    allocate_replica_on_segment(&self.state, &req.key, size, &req.target)?;
                if let Some(mut object) = self.state.objects.get_mut(&req.key) {
                    object.replicas.push(replica.clone());
                }
                replica
            }
        };
        let targets = if same_replica(&target, &source) {
            Vec::new()
        } else if existing_target.is_some() {
            Vec::new()
        } else {
            vec![target.clone()]
        };
        // Pin source replica via refcnt
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            if let Some(src) = object.replicas.iter_mut().find(|r| same_replica(r, &source)) {
                src.inc_refcnt();
            }
        }
        self.state.replication_tasks.insert(
            req.key.clone(),
            ReplicationTaskEntry {
                client_id,
                kind: ReplicationTaskKind::Move,
                source: source.clone(),
                targets,
            },
        );
        Ok(Response::new(proto::MoveStartResponse {
            source: Some(replica_to_proto(&source)),
            target: Some(replica_to_proto(&target)),
        }))
    }

    pub(super) async fn move_end_impl(
        &self,
        request: Request<proto::MoveEndRequest>,
    ) -> Result<Response<proto::MoveEndResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .replication_tasks
            .get(&req.key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Move {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }
        let mut removed_source = Vec::new();
        // C++ master_service.cpp:2238-2243 MoveEnd 时检查 source/target 的 handle 有效性
        // 若 source handle 已失效，撤销 target 并返回错误
        let mut source_invalid = false;
        let remove_object;
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            // C++ master_service.cpp:2238-2240 检查 source replica handle 是否仍然有效
            if let Some(source_replica) = object.replicas.iter().find(|r| same_replica(r, &task.source)) {
                if !source_replica.handle_valid {
                    source_invalid = true;
                }
            }
            if !source_invalid {
                for target in &task.targets {
                    if let Some(replica) = object.replicas.iter_mut().find(|r| same_replica(r, target))
                    {
                        // C++ master_service.cpp:2240-2243 检查 target handle_valid
                        // handle 无效的 target 不标记为 Complete
                        if replica.handle_valid {
                            replica.status = ReplicaStatus::Complete;
                        }
                    }
                }
            }
            object.replicas.retain(|replica| {
                let matched = same_replica(replica, &task.source);
                if matched {
                    removed_source.push(replica.clone());
                }
                !matched
            });
            remove_object = object.replicas.is_empty();
        } else {
            return Err(Status::not_found("key not found"));
        }

        // C++ master_service.cpp:2238-2243 使用 discarded_replicas_ 延迟释放源 replica
        // 防止 RDMA in-flight 冲突，避免源 replica 的缓冲区在 transfer 仍在进行时被重用
        // C++ puts the source replica into discarded_replicas_ with a timeout
        // (put_start_release_timeout), then a background thread releases it after expiry.
        let release_timeout = self.state.runtime_config.put_start_release_timeout;
        let state = self.state.clone();
        let source_replicas = removed_source.clone();
        let key_clone = req.key.clone();
        tokio::spawn(async move {
            tracing::info!(
                "MoveEnd: starting delayed release for {} source replicas of key={}, timeout={:?}",
                source_replicas.len(),
                key_clone,
                release_timeout,
            );
            tokio::time::sleep(release_timeout).await;
            // C++ master_service.cpp:2238-2243 discarded_replicas_ 后台线程到期后释放
            state.allocator.write().release(&source_replicas);
            tracing::debug!(
                "MoveEnd: delayed release completed for key={}, released {} replicas",
                key_clone,
                source_replicas.len(),
            );
        });

        if source_invalid {
            // C++ master_service.cpp:2240-2243 source handle 失效时撤销 target replicas
            let mut removed_targets = Vec::new();
            if let Some(mut object) = self.state.objects.get_mut(&req.key) {
                object.replicas.retain(|replica| {
                    let matched = task
                        .targets
                        .iter()
                        .any(|target| same_replica(replica, target));
                    if matched {
                        removed_targets.push(replica.clone());
                    }
                    !matched
                });
            }
            release_object_replicas(&self.state, &req.key, &removed_targets);
            // Release source replica refcnt
            if let Some(mut object) = self.state.objects.get_mut(&req.key) {
                if let Some(src) = object.replicas.iter_mut().find(|r| same_replica(r, &task.source)) {
                    src.dec_refcnt();
                }
            }
            self.state.replication_tasks.remove(&req.key);
            return Err(Status::failed_precondition(
                "source replica handle became invalid during move",
            ));
        }

        if remove_object {
            self.state.objects.remove(&req.key);
        }
        // Release source replica refcnt
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            if let Some(src) = object.replicas.iter_mut().find(|r| same_replica(r, &task.source)) {
                src.dec_refcnt();
            }
        }
        self.state.replication_tasks.remove(&req.key);
        Ok(Response::new(proto::MoveEndResponse {}))
    }

    pub(super) async fn move_revoke_impl(
        &self,
        request: Request<proto::MoveRevokeRequest>,
    ) -> Result<Response<proto::MoveRevokeResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .replication_tasks
            .get(&req.key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Move {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }
        let mut removed = Vec::new();
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            object.replicas.retain(|replica| {
                let matched = task
                    .targets
                    .iter()
                    .any(|target| same_replica(replica, target));
                if matched {
                    removed.push(replica.clone());
                }
                !matched
            });
        }
        release_object_replicas(&self.state, &req.key, &removed);
        // Release source replica refcnt
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            if let Some(src) = object.replicas.iter_mut().find(|r| same_replica(r, &task.source)) {
                src.dec_refcnt();
            }
        }
        self.state.replication_tasks.remove(&req.key);
        Ok(Response::new(proto::MoveRevokeResponse {}))
    }
}

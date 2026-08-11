//! # Replica Replication — 副本复制与迁移 / Replica Copy & Move
//!
//! 本模块实现副本复制（Copy）和迁移（Move）的完整生命周期，对应 C++ master_service.cpp
//! 中的 CopyStart/CopyEnd/CopyRevoke、MoveStart/MoveEnd/MoveRevoke。
//!
//! This module implements the complete lifecycle of replica Copy and Move operations,
//! corresponding to CopyStart/CopyEnd/CopyRevoke, MoveStart/MoveEnd/MoveRevoke in C++ master_service.cpp.
//!
//! ## Copy 流程 / Copy Flow
//!
//! ```text
//! CopyStart → allocate target replicas on specified segments
//!          → pin source replica (inc refcnt, prevents concurrent eviction)
//!          → create ReplicationTaskEntry (kind=Copy)
//!          → client RDMA copies data to targets
//! CopyEnd   → mark targets Complete (only if handle is still valid)
//!          → unpin source (dec refcnt)
//!          → if source handle became invalid during copy: revoke all targets to prevent inconsistency
//! CopyRevoke → remove allocated targets, release source refcnt
//! ```
//!
//! ## Move 流程 / Move Flow
//!
//! ```text
//! MoveStart → allocate target replica (or reuse existing)
//!          → pin source (inc refcnt)
//!          → create ReplicationTaskEntry (kind=Move)
//!          → client RDMA copies data to target
//! MoveEnd   → mark target Complete
//!          → remove source replica (delayed release after put_start_release_timeout
//!            to prevent RDMA in-flight from accessing reclaimed memory)
//!          → unpin source (dec refcnt)
//! MoveRevoke → remove allocated targets, release source refcnt
//! ```
//!
//! ## 关键设计 / Key Design Decisions
//!
//! 1. **refcnt 引用计数**: Copy/Move 期间对 source replica 递增 refcnt 防止被并发驱逐。
//! 2. **handle_valid 校验**: 标记 Complete 前检查 handle 是否仍然有效（Copy/Move 期间可能因
//!    segment 卸载等原因失效）。
//! 3. **延迟释放 (delayed release)**: Move 完成后源副本不立即释放，而是延迟
//!    put_start_release_timeout (默认 600s) 后释放，防止仍在 RDMA 传输中的读写访问已回收的内存。
//! 4. **不完整故障回滚**: source handle 失效时撤销所有 target 副本，防止数据不一致。

use super::*;

fn allocate_replica_on_segment_id(
    state: &MasterState,
    size: u64,
    segment_id: Uuid,
    replica_type: ReplicaType,
    segment_name: &str,
) -> Result<ReplicaDescriptor, Status> {
    match replica_type {
        ReplicaType::Memory => {
            let replica = state
                .allocator
                .write()
                .allocate_from_segment_id(segment_id, size)
                .map_err(|err| allocation_error_status(err, segment_name, false))?;
            sync_segment_usage(state, [segment_id]);
            Ok(replica)
        }
        ReplicaType::NoFSsd => {
            let mut replica = state
                .nof_allocator
                .write()
                .allocate_from_segment_id(segment_id, size)
                .map_err(|err| allocation_error_status(err, segment_name, true))?;
            replica.replica_type = ReplicaType::NoFSsd;
            sync_nof_segment_usage(state, [segment_id]);
            Ok(replica)
        }
        ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => Err(
            Status::invalid_argument("move target must be a Memory or NoF segment"),
        ),
    }
}

#[derive(Clone, Copy)]
struct ExactMoveIdentity {
    source_segment_id: Uuid,
    source_replica_type: ReplicaType,
    target_segment_id: Uuid,
    target_replica_type: ReplicaType,
}

/// Recover the exact identities captured by a pending Drain task.
///
/// MoveStart remains name-based on the public wire, but a Drain task is
/// already registered in Master state. Binding the request to that task
/// prevents a same-name segment from inheriting the move.
fn exact_drain_move_identity(
    state: &MasterState,
    key: &str,
    client_id: Uuid,
    source: &str,
    target: &str,
) -> Result<Option<ExactMoveIdentity>, Status> {
    let mut identity = None;
    for job in state.drain_jobs.iter() {
        for (task_id, active) in &job.active_tasks {
            if active.source_segment != source || active.target_segment != target {
                continue;
            }
            let Some(task) = state.tasks.get(task_id) else {
                continue;
            };
            if task.key != key || task.info.assigned_client != Some(client_id) {
                continue;
            }
            let candidate = ExactMoveIdentity {
                source_segment_id: active.source_segment_id,
                source_replica_type: active.source_replica_type,
                target_segment_id: active.target_segment_id,
                target_replica_type: active.target_replica_type,
            };
            if identity.is_some() {
                return Err(Status::failed_precondition(
                    "multiple active Drain tasks match this move request",
                ));
            }
            identity = Some(candidate);
        }
    }
    Ok(identity)
}

fn validate_exact_segment(
    state: &MasterState,
    segment_id: Uuid,
    replica_type: ReplicaType,
    segment_name: &str,
    expected_status: Option<proto::SegmentStatus>,
) -> Result<(), Status> {
    let current = match replica_type {
        ReplicaType::Memory => state
            .segments
            .get(&segment_id)
            .map(|entry| (entry.segment.name.clone(), entry.status)),
        ReplicaType::NoFSsd => state
            .nof_segments
            .get(&segment_id)
            .map(|entry| (entry.segment.name.clone(), entry.status)),
        ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => None,
    };
    let Some((current_name, status)) = current else {
        return Err(Status::failed_precondition(format!(
            "exact segment no longer exists: {segment_id}"
        )));
    };
    if current_name != segment_name {
        return Err(Status::failed_precondition(format!(
            "exact segment {segment_id} no longer has expected name {segment_name}"
        )));
    }
    if expected_status.is_some_and(|expected| status != expected) {
        return Err(Status::failed_precondition(format!(
            "exact segment {segment_name} has unexpected status {status:?}"
        )));
    }
    Ok(())
}

fn allocation_error_status(
    err: SegmentAllocationError,
    segment_name: &str,
    is_nof: bool,
) -> Status {
    let target = if is_nof { "NoF target" } else { "target" };
    match err {
        SegmentAllocationError::InvalidParams => {
            Status::invalid_argument(format!("invalid allocation size for {target} segment"))
        }
        SegmentAllocationError::SegmentNotFound => {
            Status::not_found(format!("{target} segment not found: {segment_name}"))
        }
        SegmentAllocationError::NoAvailableHandle => Status::resource_exhausted(format!(
            "failed to allocate on {target} segment: {segment_name}"
        )),
    }
}

fn same_replica(a: &ReplicaDescriptor, b: &ReplicaDescriptor) -> bool {
    a.segment_id == b.segment_id && a.offset == b.offset && a.replica_type == b.replica_type
}

impl MasterServiceImpl {
    pub(crate) fn put_revoke_matches_target(
        replica: &ReplicaDescriptor,
        target: ReplicaType,
    ) -> bool {
        if target == ReplicaType::All {
            return matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            );
        }
        replica.replica_type == target
    }

    // PutRevoke: 撤销 PutStart 分配的副本。仅允许撤销非 Complete 状态的副本
    // （已完成写入的副本不能撤销防止数据丢失）。若所有副本被移除则删除对象。
    pub(super) async fn put_revoke_impl(
        &self,
        request: Request<proto::PutRevokeRequest>,
    ) -> Result<Response<proto::PutRevokeResponse>, Status> {
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
        if self.state.replication_tasks.contains_key(&scoped_key) {
            return Err(Status::failed_precondition(
                "object has an ongoing replication task",
            ));
        }
        let Some(mut object) = self.state.objects.get_mut(&scoped_key) else {
            return Err(Status::not_found("key not found"));
        };
        if object_owner_client_id(&self.state, &object) != Some(client_id) {
            return Err(Status::permission_denied(
                "object owned by different client",
            ));
        }
        let mut revoked = clone_object_for_mutation(&object);
        let mut removed = Vec::new();
        let target = request_replica_type_from_i32(req.replica_type)?;
        // C++ master_service.cpp:1492-1504 只允许撤销 PROCESSING 状态的 replica，已完成的不能撤销
        let mut all_completed = true;
        let mut has_matching = false;

        // Check if all matching replicas are already complete
        for replica in &revoked.replicas {
            let matches = Self::put_revoke_matches_target(replica, target);
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
        revoked.replicas.retain(|replica| {
            let matches = Self::put_revoke_matches_target(replica, target);
            if matches && replica.status != ReplicaStatus::Complete {
                removed.push(replica.clone());
                false
            } else {
                true
            }
        });
        if !revoked.quota_committed {
            let remaining_reservation = checked_allocating_memory_quota_charge(&revoked)
                .map_err(|_| Status::internal("PutRevoke quota charge overflow"))?;
            let released_reservation = revoked
                .reserved_quota_charge_bytes
                .checked_sub(remaining_reservation)
                .ok_or_else(|| {
                    Status::internal("PutRevoke increased the durable quota reservation")
                })?;
            self.abort_tenant_quota(&revoked.tenant_id, released_reservation)?;
            revoked.reserved_quota_charge_bytes = remaining_reservation;
        }
        let remove_object = revoked.replicas.is_empty();
        let quota_settled = if remove_object {
            false
        } else {
            self.settle_object_quota_if_ready(&mut revoked)?
        };
        *object = revoked;
        drop(object);
        if remove_object {
            if let Some((_, object)) = self.state.objects.remove(&scoped_key) {
                self.account_removed_object_quota(&object)?;
            }
            self.state.processing_keys.remove(&scoped_key);
        } else if quota_settled {
            self.state.processing_keys.remove(&scoped_key);
        }
        self.persist_detached_allocator_replicas(&scoped_key, removed, "put_revoke")?;
        metrics::PUT_REVOKE_REQUESTS.inc();
        Ok(Response::new(proto::PutRevokeResponse {}))
    }

    // RemoveAll: 批量删除所有 lease 已过期的对象（force 模式跳过此检查）。
    pub(super) async fn remove_all_impl(
        &self,
        request: Request<proto::RemoveAllRequest>,
    ) -> Result<Response<proto::RemoveAllResponse>, Status> {
        let req = request.into_inner();
        // Legacy C++ RemoveAll carries no tenant scope and removes every
        // tenant's objects.  Preserve that behavior for the empty-tenant
        // request even in strict multi-tenant mode; a named tenant still
        // scopes removal to that tenant.
        let all_tenants = req.tenant_id.is_empty();
        let tenant_filter = if all_tenants {
            None
        } else {
            // Legacy C++ RemoveAll is a scalar operation with no error
            // channel; an unparsable tenant simply removes nothing.
            match resolve_request_tenant(
                &req.tenant_id,
                self.state.runtime_config.enable_tenant_quota,
            ) {
                Ok(tenant_id) => Some(tenant_id),
                Err(_) => {
                    return Ok(Response::new(proto::RemoveAllResponse { removed_count: 0 }));
                }
            }
        };
        let keys = self
            .state
            .objects
            .iter()
            .filter(|entry| {
                if let Some(ref tenant_filter) = tenant_filter {
                    if entry.tenant_id != *tenant_filter {
                        return false;
                    }
                }
                !self.state.replication_tasks.contains_key(entry.key())
                    && entry
                        .replicas
                        .iter()
                        .all(|r| r.status == ReplicaStatus::Complete)
                    && (req.force || is_lease_expired(entry.value()))
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let mut removed_count = 0i64;
        for key in keys {
            let _mutation_guard = self.state.key_mutations.lock(&key);
            if let Some((_, object)) = self.state.objects.remove(&key) {
                if let Err(status) = self.cleanup_removed_object(&key, &object) {
                    self.state.objects.insert(key, object);
                    return Err(status);
                }
                removed_count += 1;
            }
        }
        Ok(Response::new(proto::RemoveAllResponse { removed_count }))
    }

    // GetStorageConfig: 返回当前的存储配置（fs_dir、eviction 开关、配额）。
    pub(super) async fn get_storage_config_impl(
        &self,
        _request: Request<proto::GetStorageConfigRequest>,
    ) -> Result<Response<proto::GetStorageConfigResponse>, Status> {
        let cfg = &self.state.runtime_config;
        Ok(Response::new(proto::GetStorageConfigResponse {
            fs_dir: storage_fs_dir_for_client(cfg),
            enable_disk_eviction: cfg.enable_disk_eviction,
            quota_bytes: cfg.quota_bytes,
            enable_tenant_scope: cfg.enable_tenant_quota,
            memory_allocator: match cfg.memory_allocator_kind {
                crate::allocator::MemoryAllocatorKind::Offset => "offset",
                crate::allocator::MemoryAllocatorKind::CachelibLike => "cachelib",
            }
            .to_string(),
            memory_segment_alignment: match cfg.memory_allocator_kind {
                crate::allocator::MemoryAllocatorKind::Offset => 1,
                crate::allocator::MemoryAllocatorKind::CachelibLike => {
                    crate::allocator::CACHELIB_SLAB_SIZE
                }
            },
        }))
    }

    // CopyStart: 对已有对象发起副本拷贝到新 segment。
    // 校验 source replica 存在且 Complete，target segment 处于 Active 可分配状态。
    // 在目标 segment 上分配新副本，通过 refcnt 固定源副本防止 concurrent evict。
    pub(super) async fn copy_start_impl(
        &self,
        request: Request<proto::CopyStartRequest>,
    ) -> Result<Response<proto::CopyStartResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        let key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if req.key.is_empty() || req.source.is_empty() {
            return Err(Status::invalid_argument("copy requires key and source"));
        }
        let object = self
            .state
            .objects
            .get(&key)
            .ok_or(Status::not_found("key not found"))?;
        if self.state.replication_tasks.contains_key(&key) {
            return Err(Status::failed_precondition(
                "object already has an ongoing replication task",
            ));
        }
        let source_candidates = object
            .replicas
            .iter()
            .filter(|replica| {
                replica.segment_name == req.source
                    && replica.status == ReplicaStatus::Complete
                    && replica.handle_valid
                    && matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let source = match source_candidates.as_slice() {
            [source] => source.clone(),
            [] => return Err(Status::invalid_argument("source segment not found")),
            _ => {
                return Err(Status::failed_precondition(
                    "source segment name is ambiguous",
                ));
            }
        };
        validate_exact_segment(
            &self.state,
            source.segment_id,
            source.replica_type,
            &req.source,
            None,
        )?;
        if client_id_by_replica_segment_id(&self.state, source.segment_id, source.replica_type)
            != Some(client_id)
        {
            return Err(Status::permission_denied(
                "copy source is owned by a different client",
            ));
        }
        let size = object.size;
        let existing = object.replicas.clone();
        drop(object);

        let mut target_identities = Vec::with_capacity(req.targets.len());
        for target in &req.targets {
            let Some((segment_id, replica_type)) =
                unique_active_replica_segment_identity(&self.state, target)
            else {
                return Err(Status::failed_precondition(format!(
                    "target segment is missing, inactive, or ambiguous: {target}"
                )));
            };
            let exact_exists = existing.iter().any(|replica| {
                replica.segment_id == segment_id && replica.replica_type == replica_type
            });
            if !exact_exists
                && existing
                    .iter()
                    .any(|replica| replica.segment_name == *target)
            {
                return Err(Status::failed_precondition(format!(
                    "same-name target replica does not match the unique active target: {target}"
                )));
            }
            target_identities.push((target.as_str(), segment_id, replica_type, exact_exists));
        }

        let new_memory_replica_count = target_identities
            .iter()
            .filter(|(_, _, replica_type, exists)| *replica_type == ReplicaType::Memory && !*exists)
            .count();
        let reserved_quota_charge =
            checked_requested_memory_quota_charge(size, new_memory_replica_count).map_err(
                |_| Status::invalid_argument("Memory replica quota charge overflows uint64"),
            )?;
        self.reserve_tenant_quota(&tenant_id, reserved_quota_charge)?;

        let mut allocated = Vec::new();
        for (target, segment_id, replica_type, exists) in target_identities {
            if exists {
                continue;
            }
            if let Err(status) = validate_exact_segment(
                &self.state,
                segment_id,
                replica_type,
                target,
                Some(proto::SegmentStatus::Active),
            ) {
                release_object_replicas(&self.state, &key, &allocated)?;
                self.abort_tenant_quota(&tenant_id, reserved_quota_charge)?;
                return Err(status);
            }
            match allocate_replica_on_segment_id(
                &self.state,
                size,
                segment_id,
                replica_type,
                target,
            ) {
                Ok(replica) => allocated.push(replica),
                Err(status) => {
                    release_object_replicas(&self.state, &key, &allocated)?;
                    self.abort_tenant_quota(&tenant_id, reserved_quota_charge)?;
                    return Err(status);
                }
            }
        }

        if let Some(mut object) = self.state.objects.get_mut(&key) {
            object.replicas.extend(allocated.clone());
        }
        // Pin source replica via refcnt
        if let Some(mut object) = self.state.objects.get_mut(&key) {
            if let Some(src) = object
                .replicas
                .iter_mut()
                .find(|r| same_replica(r, &source))
            {
                src.inc_refcnt();
            }
        }
        self.state.replication_tasks.insert(
            key.clone(),
            ReplicationTaskEntry {
                client_id,
                start_time: std::time::Instant::now(),
                kind: ReplicationTaskKind::Copy,
                source: source.clone(),
                targets: allocated.clone(),
                existing_move_target: None,
                reserved_quota_charge_bytes: reserved_quota_charge,
            },
        );
        self.state
            .persist_replication_start_or_fence(&key, "copy_start")
            .map_err(|error| {
                Status::unavailable(format!("failed to persist copy_start oplog: {error}"))
            })?;

        Ok(Response::new(proto::CopyStartResponse {
            source: Some(replica_to_proto_for_state(&self.state, &source)),
            targets: allocated
                .iter()
                .map(|replica| replica_to_proto_for_state(&self.state, replica))
                .collect(),
        }))
    }

    // CopyEnd: 拷贝完成确认。校验 handle_valid 后将目标副本标记为 Complete。
    // 若 source handle 在拷贝期间失效则撤销所有 target（防止数据不一致），释放 refcnt。
    pub(super) async fn copy_end_impl(
        &self,
        request: Request<proto::CopyEndRequest>,
    ) -> Result<Response<proto::CopyEndResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if !self.state.objects.contains_key(&key) {
            return Err(Status::not_found("key not found"));
        }
        let task = self
            .state
            .replication_tasks
            .get(&key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Copy {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }
        let mut all_present = true;
        let mut source_invalid = false;
        let mut invalid_targets = Vec::new();
        // C++ master_service.cpp:1985-1994 CopyEnd 时检查 source replica 的 handle 有效性
        // 如果 source handle 已失效，中止操作并撤销 targets
        match self.state.objects.get_mut(&key) {
            Some(mut object) => {
                let known_committed_charge =
                    if object.committed_quota_charge_bytes == 0 && object.quota_committed {
                        completed_memory_quota_charge(&object)
                    } else {
                        object.committed_quota_charge_bytes
                    };
                let mut completed = clone_object_for_mutation(&object);
                // C++ master_service.cpp:1985-1988 检查 source replica 是否 still present 且 handle_valid
                match completed
                    .replicas
                    .iter()
                    .find(|r| same_replica(r, &task.source))
                {
                    Some(source_replica) => {
                        if !source_replica.handle_valid
                            || source_replica.status != ReplicaStatus::Complete
                        {
                            source_invalid = true;
                        }
                    }
                    None => {
                        all_present = false;
                        source_invalid = true;
                    }
                }
                if !source_invalid {
                    for target in &task.targets {
                        match completed
                            .replicas
                            .iter_mut()
                            .find(|replica| same_replica(replica, target))
                        {
                            // C++ master_service.cpp:1990-1994 检查每个 target 的 handle_valid
                            // handle 无效的 target 不标记为 Complete，保持在当前状态
                            Some(replica) => {
                                if replica.handle_valid
                                    && matches!(
                                        replica.status,
                                        ReplicaStatus::Allocating | ReplicaStatus::Complete
                                    )
                                {
                                    replica.status = ReplicaStatus::Complete;
                                } else {
                                    all_present = false;
                                }
                            }
                            None => all_present = false,
                        }
                    }
                    completed.replicas.retain(|replica| {
                        let invalid = task.targets.iter().any(|target| {
                            same_replica(replica, target)
                                && replica.status != ReplicaStatus::Complete
                        });
                        if invalid {
                            invalid_targets.push(replica.clone());
                        }
                        !invalid
                    });
                    let committed_charge = task
                        .targets
                        .iter()
                        .filter(|target| {
                            target.replica_type == ReplicaType::Memory
                                && completed.replicas.iter().any(|replica| {
                                    same_replica(replica, target)
                                        && replica.status == ReplicaStatus::Complete
                                })
                        })
                        .map(|target| target.size)
                        .sum();
                    settle_additional_memory_quota_charge(
                        &self.state,
                        &mut completed,
                        known_committed_charge,
                        task.reserved_quota_charge_bytes,
                        committed_charge,
                        known_committed_charge == 0 && committed_charge != 0,
                    )
                    .map_err(|error| self.tenant_quota_mutation_status("copy_end_quota", error))?;
                }
                sync_cache_total_accounting(&mut completed);
                *object = completed;
            }
            _ => {
                all_present = false;
                source_invalid = true;
            }
        }
        // Release source replica refcnt
        if let Some(mut object) = self.state.objects.get_mut(&key) {
            if let Some(src) = object
                .replicas
                .iter_mut()
                .find(|r| same_replica(r, &task.source))
            {
                src.dec_refcnt();
            }
        }
        self.state.replication_tasks.remove(&key);
        // C++ master_service.cpp:1988-1992 如果 source handle 在 Copy 过程中失效，撤销 targets
        if source_invalid {
            self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
            if self.state.service_fenced.load(Ordering::Acquire) {
                return Err(Status::unavailable(
                    "tenant quota invariant failed while revoking Copy targets",
                ));
            }
            let mut removed_targets = Vec::new();
            if let Some(mut object) = self.state.objects.get_mut(&key) {
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
                sync_cache_total_accounting(&mut object);
            }
            self.persist_detached_allocator_replicas(
                &key,
                removed_targets,
                "copy_end_source_invalid",
            )?;
            return Err(Status::failed_precondition(
                "source replica handle became invalid during transfer",
            ));
        }
        if !all_present {
            if invalid_targets.is_empty() {
                self.persist_object_image_or_remove(&key, "copy_end_target_missing")?;
            } else {
                // A target whose segment was unmounted is already detached from
                // the allocator; releasing it would fence the master.  Mirror
                // the C++ REPLICA_IS_GONE outcome by only releasing replicas
                // whose segments are still registered.
                let invalid_targets = invalid_targets
                    .into_iter()
                    .filter(|replica| replica_segment_registered(self, replica))
                    .collect::<Vec<_>>();
                self.persist_detached_allocator_replicas(
                    &key,
                    invalid_targets,
                    "copy_end_target_invalid",
                )?;
            }
            return Err(Status::failed_precondition(
                "copy target missing or invalid during completion",
            ));
        }
        self.persist_object_image_or_remove(&key, "copy_end")?;
        Ok(Response::new(proto::CopyEndResponse {}))
    }

    // CopyRevoke: 撤销 Copy 任务，移除已分配但未完成的 target 副本，释放 source refcnt。
    pub(super) async fn copy_revoke_impl(
        &self,
        request: Request<proto::CopyRevokeRequest>,
    ) -> Result<Response<proto::CopyRevokeResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if !self.state.objects.contains_key(&key) {
            return Err(Status::not_found("key not found"));
        }
        let task = self
            .state
            .replication_tasks
            .get(&key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Copy {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }
        self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
        if self.state.service_fenced.load(Ordering::Acquire) {
            return Err(Status::unavailable(
                "tenant quota invariant failed while revoking Copy",
            ));
        }

        let mut removed = Vec::new();
        let mut remove_object = false;
        if let Some(mut object) = self.state.objects.get_mut(&key) {
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
        if remove_object {
            if let Some((_, object)) = self.state.objects.remove(&key) {
                self.account_removed_object_quota(&object)?;
            }
        }
        // Release source replica refcnt
        if let Some(mut object) = self.state.objects.get_mut(&key) {
            if let Some(src) = object
                .replicas
                .iter_mut()
                .find(|r| same_replica(r, &task.source))
            {
                src.dec_refcnt();
            }
        }
        self.state.replication_tasks.remove(&key);
        let removed = removed
            .into_iter()
            .filter(|replica| replica_segment_registered(self, replica))
            .collect::<Vec<_>>();
        self.persist_detached_allocator_replicas(&key, removed, "copy_revoke")?;
        Ok(Response::new(proto::CopyRevokeResponse {}))
    }

    // MoveStart: 发起对象副本迁移（不同于 Copy，Move 完成后会删除源副本）。
    // 若目标 segment 上已有副本则复用（避免重复分配），否则分配新副本。
    // 源和目标必须不同（同 segment 内 move 无意义）。
    pub(super) async fn move_start_impl(
        &self,
        request: Request<proto::MoveStartRequest>,
    ) -> Result<Response<proto::MoveStartResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        let key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&key);
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
            .get(&key)
            .ok_or(Status::not_found("key not found"))?;
        if self.state.replication_tasks.contains_key(&key) {
            return Err(Status::failed_precondition(
                "object already has an ongoing replication task",
            ));
        }
        let drain_identity =
            exact_drain_move_identity(&self.state, &key, client_id, &req.source, &req.target)?;
        let source_candidates = object
            .replicas
            .iter()
            .filter(|replica| {
                replica.segment_name == req.source
                    && replica.status == ReplicaStatus::Complete
                    && replica.handle_valid
                    && matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let source = if let Some(identity) = drain_identity {
            validate_exact_segment(
                &self.state,
                identity.source_segment_id,
                identity.source_replica_type,
                &req.source,
                Some(proto::SegmentStatus::Draining),
            )?;
            source_candidates
                .iter()
                .find(|replica| {
                    replica.segment_id == identity.source_segment_id
                        && replica.replica_type == identity.source_replica_type
                })
                .cloned()
                .ok_or(Status::failed_precondition(
                    "Drain source replica no longer matches the scheduled identity",
                ))?
        } else {
            match source_candidates.as_slice() {
                [source] => source.clone(),
                [] => return Err(Status::invalid_argument("source segment not found")),
                _ => {
                    return Err(Status::failed_precondition(
                        "source segment name is ambiguous",
                    ));
                }
            }
        };
        if client_id_by_replica_segment_id(&self.state, source.segment_id, source.replica_type)
            != Some(client_id)
        {
            return Err(Status::permission_denied(
                "move source is owned by a different client",
            ));
        }
        let existing_targets = object
            .replicas
            .iter()
            .filter(|replica| replica.segment_name == req.target)
            .cloned()
            .collect::<Vec<_>>();
        let existing_target = if let Some(identity) = drain_identity {
            let exact = existing_targets
                .iter()
                .find(|replica| {
                    replica.segment_id == identity.target_segment_id
                        && replica.replica_type == identity.target_replica_type
                })
                .cloned();
            if exact.is_none() && !existing_targets.is_empty() {
                return Err(Status::failed_precondition(
                    "same-name target replica does not match the scheduled Drain target",
                ));
            }
            exact
        } else {
            match existing_targets.as_slice() {
                [] => None,
                [target] => Some(target.clone()),
                _ => {
                    return Err(Status::failed_precondition(
                        "target segment name is ambiguous",
                    ));
                }
            }
        };
        let size = object.size;
        drop(object);

        let target_identity = if let Some(identity) = drain_identity {
            validate_exact_segment(
                &self.state,
                identity.target_segment_id,
                identity.target_replica_type,
                &req.target,
                Some(proto::SegmentStatus::Active),
            )?;
            (identity.target_segment_id, identity.target_replica_type)
        } else if let Some(target) = &existing_target {
            (target.segment_id, target.replica_type)
        } else {
            let memory = self
                .state
                .segments
                .iter()
                .filter(|entry| {
                    entry.segment.name == req.target && entry.status == proto::SegmentStatus::Active
                })
                .map(|entry| (entry.segment.id, ReplicaType::Memory))
                .collect::<Vec<_>>();
            let nof = self
                .state
                .nof_segments
                .iter()
                .filter(|entry| {
                    entry.segment.name == req.target && entry.status == proto::SegmentStatus::Active
                })
                .map(|entry| (entry.segment.id, ReplicaType::NoFSsd))
                .collect::<Vec<_>>();
            let candidates = memory.into_iter().chain(nof).collect::<Vec<_>>();
            match candidates.as_slice() {
                [identity] => *identity,
                [] => {
                    return Err(Status::failed_precondition(
                        "target segment not active or not mounted",
                    ));
                }
                _ => {
                    return Err(Status::failed_precondition(
                        "target segment name is ambiguous",
                    ));
                }
            }
        };
        if let Some(target) = &existing_target {
            validate_exact_segment(
                &self.state,
                target.segment_id,
                target.replica_type,
                &req.target,
                Some(proto::SegmentStatus::Active),
            )?;
            if target.status != ReplicaStatus::Complete || !target.handle_valid {
                return Err(Status::failed_precondition(
                    "existing move target is not a complete routable replica",
                ));
            }
        }
        let reserved_quota_charge =
            if existing_target.is_none() && target_identity.1 == ReplicaType::Memory {
                size
            } else {
                0
            };
        self.reserve_tenant_quota(&tenant_id, reserved_quota_charge)?;
        let target_was_existing = existing_target.is_some();
        let target = match existing_target.clone() {
            Some(replica) => replica,
            None => {
                let replica = match allocate_replica_on_segment_id(
                    &self.state,
                    size,
                    target_identity.0,
                    target_identity.1,
                    &req.target,
                ) {
                    Ok(replica) => replica,
                    Err(status) => {
                        self.abort_tenant_quota(&tenant_id, reserved_quota_charge)?;
                        return Err(status);
                    }
                };
                if let Some(mut object) = self.state.objects.get_mut(&key) {
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
        if let Some(mut object) = self.state.objects.get_mut(&key) {
            if let Some(src) = object
                .replicas
                .iter_mut()
                .find(|r| same_replica(r, &source))
            {
                src.inc_refcnt();
            }
        }
        self.state.replication_tasks.insert(
            key.clone(),
            ReplicationTaskEntry {
                client_id,
                start_time: std::time::Instant::now(),
                kind: ReplicationTaskKind::Move,
                source: source.clone(),
                targets,
                existing_move_target: existing_target,
                reserved_quota_charge_bytes: reserved_quota_charge,
            },
        );
        self.state
            .persist_replication_start_or_fence(&key, "move_start")
            .map_err(|error| {
                Status::unavailable(format!("failed to persist move_start oplog: {error}"))
            })?;
        Ok(Response::new(proto::MoveStartResponse {
            source: Some(replica_to_proto_for_state(&self.state, &source)),
            target: (!target_was_existing)
                .then(|| replica_to_proto_for_state(&self.state, &target)),
        }))
    }

    // MoveEnd: Move 完成确认。标记目标副本 Complete 并移除源副本。
    // 源副本通过延迟释放（discarded_replicas_ 机制）在 release_timeout 后异步回收，
    // 防止 RDMA in-flight 操作仍在使用源缓冲区时被重用。
    pub(super) async fn move_end_impl(
        &self,
        request: Request<proto::MoveEndRequest>,
    ) -> Result<Response<proto::MoveEndResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if !self.state.objects.contains_key(&key) {
            return Err(Status::not_found("key not found"));
        }
        let task = self
            .state
            .replication_tasks
            .get(&key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Move {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }
        let mut removed_source = Vec::new();
        // C++ master_service.cpp:2238-2243 MoveEnd 时检查 source/target 的 handle 有效性
        // 若 source handle 已失效，撤销 target 并返回错误
        let mut source_invalid = false;
        let mut target_invalid = false;
        let mut source_present = false;
        let remove_object;
        match self.state.objects.get_mut(&key) {
            Some(mut object) => {
                let known_committed_charge =
                    if object.committed_quota_charge_bytes == 0 && object.quota_committed {
                        completed_memory_quota_charge(&object)
                    } else {
                        object.committed_quota_charge_bytes
                    };
                let mut completed = clone_object_for_mutation(&object);
                // C++ master_service.cpp:2238-2240 检查 source replica handle 是否仍然有效
                if let Some(source_replica) = completed
                    .replicas
                    .iter()
                    .find(|r| same_replica(r, &task.source))
                {
                    source_present = true;
                    if !source_replica.handle_valid
                        || source_replica.status != ReplicaStatus::Complete
                    {
                        source_invalid = true;
                    }
                } else {
                    source_invalid = true;
                }
                if !source_invalid {
                    for target in &task.targets {
                        match completed
                            .replicas
                            .iter_mut()
                            .find(|r| same_replica(r, target))
                        {
                            Some(replica)
                                if replica.handle_valid
                                    && matches!(
                                        replica.status,
                                        ReplicaStatus::Allocating | ReplicaStatus::Complete
                                    ) =>
                            {
                                replica.status = ReplicaStatus::Complete;
                            }
                            Some(_) | None => target_invalid = true,
                        }
                    }
                    if let Some(existing_target) = &task.existing_move_target {
                        match completed
                            .replicas
                            .iter()
                            .find(|replica| same_replica(replica, existing_target))
                        {
                            Some(replica)
                                if replica.handle_valid
                                    && replica.status == ReplicaStatus::Complete => {}
                            Some(_) | None => target_invalid = true,
                        }
                    }
                    if !target_invalid {
                        let mut idx = 0;
                        while idx < completed.replicas.len() {
                            if same_replica(&completed.replicas[idx], &task.source) {
                                completed.replicas[idx].dec_refcnt();
                                removed_source.push(completed.replicas.remove(idx));
                            } else {
                                idx += 1;
                            }
                        }
                        let committed_target_charge = task
                            .targets
                            .iter()
                            .filter(|target| {
                                target.replica_type == ReplicaType::Memory
                                    && completed.replicas.iter().any(|replica| {
                                        same_replica(replica, target)
                                            && replica.status == ReplicaStatus::Complete
                                    })
                            })
                            .map(|target| target.size)
                            .sum();
                        let removed_source_charge = removed_source
                            .iter()
                            .filter(|replica| replica.replica_type == ReplicaType::Memory)
                            .map(|replica| replica.size)
                            .sum();
                        settle_and_release_memory_quota_charge(
                            &self.state,
                            &mut completed,
                            known_committed_charge,
                            task.reserved_quota_charge_bytes,
                            committed_target_charge,
                            removed_source_charge,
                        )
                        .map_err(|error| {
                            self.tenant_quota_mutation_status("move_end_quota", error)
                        })?;
                        sync_cache_total_accounting(&mut completed);
                        *object = completed;
                    }
                }
                remove_object = object.replicas.is_empty();
            }
            _ => {
                self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
                if self.state.service_fenced.load(Ordering::Acquire) {
                    return Err(Status::unavailable(
                        "tenant quota invariant failed while completing Move",
                    ));
                }
                self.state.replication_tasks.remove(&key);
                self.persist_object_image_or_remove(&key, "move_end_object_missing")?;
                return Err(Status::not_found("key not found"));
            }
        }

        if source_invalid || target_invalid {
            self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
            if self.state.service_fenced.load(Ordering::Acquire) {
                return Err(Status::unavailable(
                    "tenant quota invariant failed while revoking Move targets",
                ));
            }
            // C++ master_service.cpp:2240-2243 source handle 失效时撤销 target replicas
            let mut removed_targets = Vec::new();
            if let Some(mut object) = self.state.objects.get_mut(&key) {
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
                sync_cache_total_accounting(&mut object);
            }
            // Release source replica refcnt
            if source_present {
                if let Some(mut object) = self.state.objects.get_mut(&key) {
                    if let Some(src) = object
                        .replicas
                        .iter_mut()
                        .find(|r| same_replica(r, &task.source))
                    {
                        src.dec_refcnt();
                    }
                }
            }
            self.state.replication_tasks.remove(&key);
            self.persist_detached_allocator_replicas(
                &key,
                removed_targets,
                if source_invalid {
                    "move_end_source_invalid"
                } else {
                    "move_end_target_invalid"
                },
            )?;
            return Err(Status::failed_precondition(if source_invalid {
                "source replica handle became invalid during move"
            } else {
                "target replica handle became invalid during move"
            }));
        }

        if remove_object {
            if let Some((_, object)) = self.state.objects.remove(&key) {
                self.account_removed_object_quota(&object)?;
            }
        }
        // C++ master_service.cpp:2238-2243 使用 discarded_replicas_ 延迟释放源 replica
        // 防止 RDMA in-flight 冲突，避免源 replica 的缓冲区在 transfer 仍在进行时被重用
        // C++ puts the source replica into discarded_replicas_ with a timeout.
        // Rust persists the equivalent reservation in the object-mutation
        // oplog/snapshot and lets the durability-aware reaper retire it.
        let authoritative_object = self.state.objects.get(&key).map(|object| object.clone());
        let delayed = self
            .state
            .schedule_delayed_replica_release_or_fence(
                &key,
                authoritative_object,
                removed_source,
                None,
                "move_end",
            )
            .map_err(|error| {
                Status::unavailable(format!(
                    "failed to persist MoveEnd delayed source release: {error}"
                ))
            })?
            .is_some();
        self.state.replication_tasks.remove(&key);
        if !delayed {
            self.persist_object_image_or_remove(&key, "move_end")?;
        }
        Ok(Response::new(proto::MoveEndResponse {}))
    }

    // MoveRevoke: 撤销 Move 任务，移除已分配的 target 副本，释放 source refcnt。
    pub(super) async fn move_revoke_impl(
        &self,
        request: Request<proto::MoveRevokeRequest>,
    ) -> Result<Response<proto::MoveRevokeResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let key = tenant_id.make_scoped_key(&req.key);
        let _mutation_guard = self.state.key_mutations.lock(&key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if !self.state.objects.contains_key(&key) {
            return Err(Status::not_found("key not found"));
        }
        let task = self
            .state
            .replication_tasks
            .get(&key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Move {
            return Err(Status::permission_denied("replication task owner mismatch"));
        }
        self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
        if self.state.service_fenced.load(Ordering::Acquire) {
            return Err(Status::unavailable(
                "tenant quota invariant failed while revoking Move",
            ));
        }
        let mut removed = Vec::new();
        if let Some(mut object) = self.state.objects.get_mut(&key) {
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
        // Release source replica refcnt
        if let Some(mut object) = self.state.objects.get_mut(&key) {
            if let Some(src) = object
                .replicas
                .iter_mut()
                .find(|r| same_replica(r, &task.source))
            {
                src.dec_refcnt();
            }
        }
        self.state.replication_tasks.remove(&key);
        self.persist_detached_allocator_replicas(&key, removed, "move_revoke")?;
        Ok(Response::new(proto::MoveRevokeResponse {}))
    }
}

fn replica_segment_registered(service: &MasterServiceImpl, replica: &ReplicaDescriptor) -> bool {
    match replica.replica_type {
        ReplicaType::Memory => service.state.segments.contains_key(&replica.segment_id),
        ReplicaType::NoFSsd => service.state.nof_segments.contains_key(&replica.segment_id),
        _ => true,
    }
}

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

/// 在指定的 Memory 或 NoF segment 上分配一个副本，若分配成功则同步 usage。
/// Allocate one replica on a specified Memory or NoF segment; sync usage on success.
fn allocate_replica_on_segment(
    state: &MasterState,
    _key: &str,
    size: u64,
    segment_name: &str,
) -> Result<ReplicaDescriptor, Status> {
    let is_nof = client_id_by_nof_segment_name(state, segment_name).is_some();
    if is_nof {
        let mut replica = state
            .nof_allocator
            .write()
            .allocate_from_segment(segment_name, size)
            .map_err(|err| allocation_error_status(err, segment_name, true))?;
        replica.replica_type = ReplicaType::NoFSsd;
        sync_nof_segment_usage(state, [replica.segment_id]);
        return Ok(replica);
    }

    let replica = state
        .allocator
        .write()
        .allocate_from_segment(segment_name, size)
        .map_err(|err| allocation_error_status(err, segment_name, false))?;
    sync_segment_usage(state, [replica.segment_id]);
    Ok(replica)
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
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
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
        let mut removed = Vec::new();
        // C++ master_service.cpp:1492-1504 只允许撤销 PROCESSING 状态的 replica，已完成的不能撤销
        let mut all_completed = true;
        let mut has_matching = false;

        // Check if all matching replicas are already complete
        for replica in &object.replicas {
            let target = replica_type_from_i32(req.replica_type);
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
        object.replicas.retain(|replica| {
            let target = replica_type_from_i32(req.replica_type);
            let matches = Self::put_revoke_matches_target(replica, target);
            if matches && replica.status != ReplicaStatus::Complete {
                removed.push(replica.clone());
                false
            } else {
                true
            }
        });
        let remove_object = object.replicas.is_empty();
        drop(object);
        if remove_object {
            if let Err(e) = self
                .oplog_manager
                .lock()
                .record_put_revoke_durable(&scoped_key)
            {
                if let Some(mut object) = self.state.objects.get_mut(&scoped_key) {
                    object.replicas.extend(removed.clone());
                }
                return Err(Status::internal(format!(
                    "failed to persist put_revoke oplog: {e}"
                )));
            }
            if let Some((_, object)) = self.state.objects.remove(&scoped_key) {
                account_removed_object_quota(&self.state, &object);
            }
            self.state.processing_keys.remove(&scoped_key);
        }
        release_object_replicas(&self.state, &scoped_key, &removed);
        metrics::PUT_REVOKE_REQUESTS.inc();
        Ok(Response::new(proto::PutRevokeResponse {}))
    }

    // RemoveAll: 批量删除所有 lease 已过期的对象（force 模式跳过此检查）。
    pub(super) async fn remove_all_impl(
        &self,
        request: Request<proto::RemoveAllRequest>,
    ) -> Result<Response<proto::RemoveAllResponse>, Status> {
        let req = request.into_inner();
        let tenant_filter = if req.tenant_id.is_empty() {
            None
        } else {
            Some(normalize_tenant_id(&req.tenant_id))
        };
        let keys = self
            .state
            .objects
            .iter()
            .filter(|entry| {
                if let Some(tenant_filter) = tenant_filter.as_ref() {
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
            if let Some((_, object)) = self.state.objects.remove(&key) {
                if self.cleanup_removed_object(&key, &object).is_ok() {
                    removed_count += 1;
                } else {
                    self.state.objects.insert(key, object);
                }
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
        let key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
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
                release_object_replicas(&self.state, &key, &allocated);
                return Err(Status::invalid_argument(format!(
                    "target segment not mounted: {target}"
                )));
            }
            // C++ master_service.cpp:1867-1872 检查目标 segment 是否处于可分配状态
            // Gap 20: verify target segment is allocatable (Active)
            let is_active =
                self.state
                    .segments
                    .iter()
                    .any(|e| e.segment.name == *target && e.status == proto::SegmentStatus::Active)
                    || self.state.nof_segments.iter().any(|e| {
                        e.segment.name == *target && e.status == proto::SegmentStatus::Active
                    });
            if !is_active {
                release_object_replicas(&self.state, &key, &allocated);
                return Err(Status::failed_precondition(format!(
                    "target segment not active or not allocatable: {target}"
                )));
            }
            allocated.push(allocate_replica_on_segment(
                &self.state,
                &key,
                size,
                target,
            )?);
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
            },
        );

        Ok(Response::new(proto::CopyStartResponse {
            source: Some(replica_to_proto(&source)),
            targets: allocated.iter().map(replica_to_proto).collect(),
        }))
    }

    // CopyEnd: 拷贝完成确认。校验 handle_valid 后将目标副本标记为 Complete。
    // 若 source handle 在拷贝期间失效则撤销所有 target（防止数据不一致），释放 refcnt。
    pub(super) async fn copy_end_impl(
        &self,
        request: Request<proto::CopyEndRequest>,
    ) -> Result<Response<proto::CopyEndResponse>, Status> {
        let req = request.into_inner();
        let key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
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
        // C++ master_service.cpp:1985-1994 CopyEnd 时检查 source replica 的 handle 有效性
        // 如果 source handle 已失效，中止操作并撤销 targets
        if let Some(mut object) = self.state.objects.get_mut(&key) {
            // C++ master_service.cpp:1985-1988 检查 source replica 是否 still present 且 handle_valid
            match object
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
            sync_cache_total_accounting(&mut object);
        } else {
            all_present = false;
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
            release_object_replicas(&self.state, &key, &removed_targets);
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

    // CopyRevoke: 撤销 Copy 任务，移除已分配但未完成的 target 副本，释放 source refcnt。
    pub(super) async fn copy_revoke_impl(
        &self,
        request: Request<proto::CopyRevokeRequest>,
    ) -> Result<Response<proto::CopyRevokeResponse>, Status> {
        let req = request.into_inner();
        let key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .replication_tasks
            .get(&key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Copy {
            return Err(Status::permission_denied("replication task owner mismatch"));
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
        release_object_replicas(&self.state, &key, &removed);
        if remove_object {
            if let Some((_, object)) = self.state.objects.remove(&key) {
                account_removed_object_quota(&self.state, &object);
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
        let key = make_tenant_scoped_key(&req.tenant_id, &req.key);
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
                let replica = allocate_replica_on_segment(&self.state, &key, size, &req.target)?;
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
            },
        );
        Ok(Response::new(proto::MoveStartResponse {
            source: Some(replica_to_proto(&source)),
            target: Some(replica_to_proto(&target)),
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
        let key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
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
        let mut source_present = false;
        let remove_object;
        if let Some(mut object) = self.state.objects.get_mut(&key) {
            // C++ master_service.cpp:2238-2240 检查 source replica handle 是否仍然有效
            if let Some(source_replica) = object
                .replicas
                .iter()
                .find(|r| same_replica(r, &task.source))
            {
                source_present = true;
                if !source_replica.handle_valid || source_replica.status != ReplicaStatus::Complete
                {
                    source_invalid = true;
                }
            } else {
                source_invalid = true;
            }
            if !source_invalid {
                for target in &task.targets {
                    if let Some(replica) =
                        object.replicas.iter_mut().find(|r| same_replica(r, target))
                    {
                        // C++ master_service.cpp:2240-2243 检查 target handle_valid
                        // handle 无效的 target 不标记为 Complete
                        if replica.handle_valid {
                            replica.status = ReplicaStatus::Complete;
                        }
                    }
                }
                let mut idx = 0;
                while idx < object.replicas.len() {
                    if same_replica(&object.replicas[idx], &task.source) {
                        object.replicas[idx].dec_refcnt();
                        removed_source.push(object.replicas.remove(idx));
                    } else {
                        idx += 1;
                    }
                }
                sync_cache_total_accounting(&mut object);
            }
            remove_object = object.replicas.is_empty();
        } else {
            return Err(Status::not_found("key not found"));
        }

        if source_invalid {
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
            release_object_replicas(&self.state, &key, &removed_targets);
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
            return Err(Status::failed_precondition(
                "source replica handle became invalid during move",
            ));
        }

        if remove_object {
            if let Some((_, object)) = self.state.objects.remove(&key) {
                account_removed_object_quota(&self.state, &object);
            }
        }
        // C++ master_service.cpp:2238-2243 使用 discarded_replicas_ 延迟释放源 replica
        // 防止 RDMA in-flight 冲突，避免源 replica 的缓冲区在 transfer 仍在进行时被重用
        // C++ puts the source replica into discarded_replicas_ with a timeout
        // (put_start_release_timeout), then a background thread releases it after expiry.
        let release_timeout = self.state.runtime_config.put_start_release_timeout;
        let state = self.state.clone();
        let source_replicas = removed_source.clone();
        let key_clone = key.clone();
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
        self.state.replication_tasks.remove(&key);
        Ok(Response::new(proto::MoveEndResponse {}))
    }

    // MoveRevoke: 撤销 Move 任务，移除已分配的 target 副本，释放 source refcnt。
    pub(super) async fn move_revoke_impl(
        &self,
        request: Request<proto::MoveRevokeRequest>,
    ) -> Result<Response<proto::MoveRevokeResponse>, Status> {
        let req = request.into_inner();
        let key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .replication_tasks
            .get(&key)
            .ok_or(Status::failed_precondition("no replication task"))?
            .clone();
        if task.client_id != client_id || task.kind != ReplicationTaskKind::Move {
            return Err(Status::permission_denied("replication task owner mismatch"));
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
        release_object_replicas(&self.state, &key, &removed);
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
        Ok(Response::new(proto::MoveRevokeResponse {}))
    }
}

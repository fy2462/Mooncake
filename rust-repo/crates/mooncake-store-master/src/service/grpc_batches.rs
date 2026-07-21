//! # Batch Operations — 批量操作 / Batch Operations
//! 单次 RPC 处理多个 key；每个 key 独立返回状态，避免局部失败放大为整批失败。
//! Process multiple keys per RPC with independent per-key status.
//! | RPC | 功能 / Function | 返回码 / Return Codes |
//! |-----|----------------|----------------------|
//! | `BatchExistKey` | 批量检查 key 是否存在 / Batch key existence check | `Vec<bool>` |
//! | `BatchQueryIp` | 批量查询客户端 IP / Batch client IP query | `HashMap<id, addresses>` |
//! | `BatchReplicaClear` | 批量清理副本 / Batch replica cleanup | 0=成功, 跳过不满足条件的 key |
//! | `BatchPutEnd` | 批量 PutEnd / Batch PutEnd | 0=成功, -1=key不存在 |
//! | `BatchPutRevoke` | 批量撤销 / Batch revoke | 0=成功, -1=key不存在, -2=有复制任务, -3=权限拒绝 |
//! | `BatchRemove` | 批量删除 / Batch remove | 0=成功, -2=force=false且有复制任务 |
//! | `BatchUpsertEnd` | 批量 Upsert / Batch upsert | 返回全部分配的副本列表 |
//! | `BatchPutStart` | 批量 PutStart / Batch PutStart | 返回全部分配的副本列表 |
//! | `EvictDiskReplica` | 驱逐单个对象的磁盘副本 / Evict disk replicas for one key |
//! | `BatchEvictDiskReplica` | 批量驱逐磁盘副本 / Batch evict disk replicas |

use super::*;

mod batch_evict;
mod batch_put_start;

/// 批量操作返回状态码，对应 proto 层 BatchXxxResponse.statuses 的 int32 值。
/// 对标 C++ 的 ErrorCode 子集，提供类型安全的 batch 操作返回值。
///
/// Batch operation status codes, corresponding to int32 values in
/// BatchXxxResponse.statuses on the proto layer.
/// Mirrors a subset of C++ ErrorCode for type-safe batch return values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
enum BatchStatus {
    /// 操作成功 / Operation succeeded.
    Success = 0,
    /// key 不存在 / Key not found.
    KeyNotFound = -1,
    /// 有进行中的复制任务，跳过操作 / In-flight replication task, skipped.
    HasReplicationTask = -2,
    /// 权限拒绝（非法客户端）/ Permission denied (illegal client).
    IllegalClient = -3,
    /// 对象仍持有有效 lease / Object still has an active lease.
    ObjectHasLease = -4,
    /// 副本尚未全部完成 / Replicas are not all complete.
    ReplicaNotReady = -5,
    /// 参数或状态不满足操作要求 / Invalid state for the requested operation.
    InvalidState = -6,
    /// 对象已存在 / Object already exists.
    ObjectAlreadyExists = -7,
}

impl From<BatchStatus> for i32 {
    fn from(status: BatchStatus) -> Self {
        status as i32
    }
}

fn record_batch_cache_hit_metrics(replica_type: ReplicaType, object_size: u64) {
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

impl MasterServiceImpl {
    // ---- BatchGetReplicaList ----
    // C++ equivalent: WrappedMasterService::BatchGetReplicaList. Keep per-key
    // expected/error results while doing the read, lease refresh, promotion, and
    // metrics phases once for the batch instead of routing through repeated
    // single-key GetReplicaList calls.
    pub(super) async fn batch_get_replica_list_impl(
        &self,
        request: Request<proto::BatchGetReplicaListRequest>,
    ) -> Result<Response<proto::BatchGetReplicaListResponse>, Status> {
        let req = request.into_inner();
        let results = self.batch_replica_lists_for_keys(&req.tenant_id, &req.keys);
        Ok(Response::new(proto::BatchGetReplicaListResponse {
            results,
        }))
    }

    fn batch_replica_lists_for_keys(
        &self,
        tenant_id: &str,
        keys: &[String],
    ) -> Vec<proto::BatchGetReplicaListResult> {
        struct BatchGetHit {
            scoped_key: String,
            first_replica_type: ReplicaType,
            object_size: u64,
            promotion_eligible: bool,
        }

        let lease_ttl_ms = self.state.runtime_config.lease_ttl.as_millis() as u64;
        let mut results = Vec::with_capacity(keys.len());
        let mut hits = Vec::new();

        for key in keys {
            let scoped_key = make_tenant_scoped_key(tenant_id, key);
            let result = match self.state.objects.get(&scoped_key) {
                Some(entry) => {
                    let complete = entry
                        .replicas
                        .iter()
                        .filter(|replica| replica.status == ReplicaStatus::Complete)
                        .collect::<Vec<_>>();
                    if complete.is_empty() {
                        proto::BatchGetReplicaListResult {
                            status: BatchStatus::ReplicaNotReady.into(),
                            response: None,
                            error_message: "replica is not ready".to_string(),
                        }
                    } else {
                        let first_replica_type = complete[0].replica_type;
                        let promotion_eligible = !entry.replicas.iter().any(|replica| {
                            replica.replica_type == ReplicaType::Memory
                                && replica.status == ReplicaStatus::Complete
                        }) && entry.replicas.iter().any(|replica| {
                            replica.replica_type == ReplicaType::LocalDisk
                                && replica.status == ReplicaStatus::Complete
                        });
                        hits.push(BatchGetHit {
                            scoped_key,
                            first_replica_type,
                            object_size: entry.size,
                            promotion_eligible,
                        });
                        proto::BatchGetReplicaListResult {
                            status: BatchStatus::Success.into(),
                            response: Some(proto::GetReplicaListResponse {
                                replicas: complete.into_iter().map(replica_to_proto).collect(),
                                lease_ttl_ms,
                            }),
                            error_message: String::new(),
                        }
                    }
                }
                None => proto::BatchGetReplicaListResult {
                    status: BatchStatus::KeyNotFound.into(),
                    response: None,
                    error_message: format!("key not found: {key}"),
                },
            };
            results.push(result);
        }

        let mut group_leases = Vec::new();
        for hit in &hits {
            if let Some(mut entry) = self.state.objects.get_mut(&hit.scoped_key) {
                entry.last_access = SystemTime::now();
                entry.grant_lease(
                    self.state.runtime_config.lease_ttl,
                    self.state.runtime_config.soft_pin_ttl,
                );
                if !entry.group_id.is_empty() {
                    group_leases.push((entry.tenant_id.clone(), entry.group_id.clone()));
                }
            }
        }
        group_leases.sort();
        group_leases.dedup();
        for (tenant_id, group_id) in group_leases {
            self.grant_group_lease(&tenant_id, &group_id);
        }

        for hit in hits {
            if hit.promotion_eligible {
                try_push_promotion_queue(&self.state, &hit.scoped_key);
            }
            metrics::GET_REQUESTS.inc();
            record_batch_cache_hit_metrics(hit.first_replica_type, hit.object_size);
        }

        results
    }

    // ---- BatchExistKey ----
    // 批量检查 key 是否存在，返回布尔数组与输入 keys 一一对应。
    pub(super) async fn batch_exist_key_impl(
        &self,
        request: Request<proto::BatchExistKeyRequest>,
    ) -> Result<Response<proto::BatchExistKeyResponse>, Status> {
        let req = request.into_inner();
        let results = self.batch_completed_objects_exist_and_grant_lease(&req.tenant_id, &req.keys);
        Ok(Response::new(proto::BatchExistKeyResponse { results }))
    }

    fn batch_completed_objects_exist_and_grant_lease(
        &self,
        tenant_id: &str,
        keys: &[String],
    ) -> Vec<bool> {
        let mut results = Vec::with_capacity(keys.len());
        let mut lease_keys = Vec::new();

        for key in keys {
            let scoped_key = make_tenant_scoped_key(tenant_id, key);
            let exists = self
                .state
                .objects
                .get(&scoped_key)
                .map(|entry| {
                    entry
                        .replicas
                        .iter()
                        .any(|replica| replica.status == ReplicaStatus::Complete)
                })
                .unwrap_or(false);
            if exists {
                lease_keys.push(scoped_key);
            }
            results.push(exists);
        }

        let mut group_leases = Vec::new();
        for scoped_key in lease_keys {
            if let Some(mut entry) = self.state.objects.get_mut(&scoped_key) {
                entry.last_access = SystemTime::now();
                entry.grant_lease(
                    self.state.runtime_config.lease_ttl,
                    self.state.runtime_config.soft_pin_ttl,
                );
                if !entry.group_id.is_empty() {
                    group_leases.push((entry.tenant_id.clone(), entry.group_id.clone()));
                }
            }
        }
        group_leases.sort();
        group_leases.dedup();
        for (tenant_id, group_id) in group_leases {
            self.grant_group_lease(&tenant_id, &group_id);
        }

        results
    }

    // ---- BatchQueryIp ----
    // 批量查询客户端 IP 地址，返回 client_id -> addresses 的映射。
    pub(super) async fn batch_query_ip_impl(
        &self,
        request: Request<proto::BatchQueryIpRequest>,
    ) -> Result<Response<proto::BatchQueryIpResponse>, Status> {
        let req = request.into_inner();
        let mut ips = std::collections::HashMap::new();
        for cid in &req.client_ids {
            let id = uuid_from_proto(cid);
            let addresses = addresses_for_client(&self.state, id);
            if !addresses.is_empty() {
                ips.insert(id.to_string(), proto::IpList { addresses });
            }
        }
        Ok(Response::new(proto::BatchQueryIpResponse { ips }))
    }

    // ---- BatchReplicaClear ----
    // 批量清除指定客户端对象在指定 segment（或所有 segment）上的副本。
    // 校验 owner、lease 过期和所有副本 Complete 后才执行清理。
    pub(super) async fn batch_replica_clear_impl(
        &self,
        request: Request<proto::BatchReplicaClearRequest>,
    ) -> Result<Response<proto::BatchReplicaClearResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let clear_all_segments = req.segment_name.is_empty();
        let mut cleared = vec![];
        for raw_key in &req.object_keys {
            let key = make_tenant_scoped_key(&req.tenant_id, raw_key);
            let mut remove_entire_object = false;
            let mut removed_replicas = Vec::new();
            let mut had_match = false;
            if let Some(mut object) = self.state.objects.get_mut(&key) {
                if object_owner_client_id(&self.state, &object) != Some(client_id) {
                    continue;
                }
                // C++ checks for active lease; skip objects that still hold a valid lease.
                if !is_lease_expired(&object) {
                    continue;
                }
                if object
                    .replicas
                    .iter()
                    .any(|replica| replica.status != ReplicaStatus::Complete)
                {
                    continue;
                }
                if clear_all_segments {
                    had_match = !object.replicas.is_empty();
                    removed_replicas = object.replicas.clone();
                    remove_entire_object = had_match;
                } else {
                    let segment_name = req.segment_name.as_str();
                    object.replicas.retain(|replica| {
                        let matches = replica.segment_name == segment_name;
                        if matches {
                            had_match = true;
                            removed_replicas.push(replica.clone());
                        }
                        !matches
                    });
                    remove_entire_object = object.replicas.is_empty() && had_match;
                }
            }
            if had_match {
                clear_offloading_task(&self.state, &key);
                clear_promotion_task(&self.state, &key);
                release_replicas(&self.state, &removed_replicas);
                if remove_entire_object {
                    if let Some((_, object)) = self.state.objects.remove(&key) {
                        account_removed_object_quota(&self.state, &object);
                    }
                }
                cleared.push(raw_key.clone());
            }
        }
        Ok(Response::new(proto::BatchReplicaClearResponse {
            cleared_keys: cleared,
        }))
    }

    fn batch_status_from_status(status: &Status) -> BatchStatus {
        match status.code() {
            tonic::Code::NotFound => BatchStatus::KeyNotFound,
            tonic::Code::PermissionDenied => BatchStatus::IllegalClient,
            tonic::Code::FailedPrecondition => BatchStatus::InvalidState,
            _ => BatchStatus::InvalidState,
        }
    }

    // ---- BatchPutEnd ----
    // 批量 PutEnd：逐项复用单 key PutEnd 语义，保持 lease/soft-pin/processing_keys 行为一致。
    // 每个 entry 返回 0（成功）或负数错误码。
    pub(super) async fn batch_put_end_impl(
        &self,
        request: Request<proto::BatchPutEndRequest>,
    ) -> Result<Response<proto::BatchPutEndResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .entries
            .iter()
            .map(|entry| {
                let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) else {
                    return BatchStatus::IllegalClient.into();
                };
                let scoped_key = make_tenant_scoped_key(&entry.tenant_id, &entry.key);
                match self.apply_put_end_for_key(
                    &scoped_key,
                    client_id,
                    replica_type_from_i32(entry.replica_type),
                ) {
                    Ok(()) => BatchStatus::Success.into(),
                    Err(status) => Self::batch_status_from_status(&status).into(),
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutEndResponse { statuses }))
    }

    // ---- BatchPutRevoke ----
    // 批量撤销 PutStart：移除指定 keys 在指定 segment 上的所有副本（含 Complete 状态）。
    // 返回码：0=成功, -1=key 不存在, -2=有进行中的复制任务, -3=权限拒绝。
    pub(super) async fn batch_put_revoke_impl(
        &self,
        request: Request<proto::BatchPutRevokeRequest>,
    ) -> Result<Response<proto::BatchPutRevokeResponse>, Status> {
        let req = request.into_inner();
        let client_id = req.client_id.as_ref().map(uuid_from_proto);
        let target = replica_type_from_i32(req.replica_type);
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|raw_key| {
                let key = make_tenant_scoped_key(&req.tenant_id, raw_key);
                if self.state.replication_tasks.contains_key(&key) {
                    return BatchStatus::HasReplicationTask.into();
                }
                match self.state.objects.get_mut(&key) {
                    Some(mut object) => {
                        if let Some(cid) = client_id {
                            if object.client_id != cid {
                                return BatchStatus::IllegalClient.into();
                            }
                        }
                        let has_matching = object
                            .replicas
                            .iter()
                            .any(|replica| Self::put_revoke_matches_target(replica, target));
                        let has_non_complete_matching = object.replicas.iter().any(|replica| {
                            Self::put_revoke_matches_target(replica, target)
                                && replica.status != ReplicaStatus::Complete
                        });
                        if has_matching && !has_non_complete_matching {
                            return BatchStatus::InvalidState.into();
                        }
                        let mut removed = Vec::new();
                        object.replicas.retain(|replica| {
                            let matched = Self::put_revoke_matches_target(replica, target)
                                && replica.status != ReplicaStatus::Complete;
                            if matched {
                                removed.push(replica.clone());
                            }
                            !matched
                        });
                        let remove_object = object.replicas.is_empty();
                        drop(object);
                        release_object_replicas(&self.state, &key, &removed);
                        if remove_object {
                            if let Some((_, removed_object)) = self.state.objects.remove(&key) {
                                account_removed_object_quota(&self.state, &removed_object);
                            }
                            self.state.processing_keys.remove(&key);
                            self.oplog_manager.lock().record_put_revoke(&key);
                        }
                        BatchStatus::Success.into()
                    }
                    _ => BatchStatus::KeyNotFound.into(),
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutRevokeResponse { statuses }))
    }

    // ---- BatchRemove ----
    // 批量删除对象，每个 key 执行相同的 force/lease 检查（与 Remove 一致），记录 oplog。
    pub(super) async fn batch_remove_impl(
        &self,
        request: Request<proto::BatchRemoveRequest>,
    ) -> Result<Response<proto::BatchRemoveResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|raw_key| {
                let key = make_tenant_scoped_key(&req.tenant_id, raw_key);
                if self.state.replication_tasks.contains_key(&key) {
                    return BatchStatus::HasReplicationTask.into();
                }
                let Some(object) = self.state.objects.get(&key) else {
                    return BatchStatus::KeyNotFound.into();
                };
                if !req.force && !is_lease_expired(&object) {
                    return BatchStatus::ObjectHasLease.into();
                }
                if !object
                    .replicas
                    .iter()
                    .all(|r| r.status == ReplicaStatus::Complete)
                {
                    return BatchStatus::ReplicaNotReady.into();
                }
                drop(object);
                if let Some((_, object)) = self.state.objects.remove(&key) {
                    clear_offloading_task(&self.state, &key);
                    clear_promotion_task(&self.state, &key);
                    account_removed_object_quota(&self.state, &object);
                    release_replicas(&self.state, &object.replicas);
                    self.oplog_manager.lock().record_remove(&key);
                    BatchStatus::Success.into()
                } else {
                    BatchStatus::KeyNotFound.into()
                }
            })
            .collect();
        metrics::BATCH_REMOVE_REQUESTS.inc_by(req.keys.len() as u64);
        Ok(Response::new(proto::BatchRemoveResponse { statuses }))
    }

    // ---- BatchUpsertStart ----
    // C++ BatchUpsertStart 语义：逐 key 执行 UpsertStart，返回 descriptor 给客户端写入。
    pub(super) async fn batch_upsert_start_impl(
        &self,
        request: Request<proto::BatchUpsertStartRequest>,
    ) -> Result<Response<proto::BatchUpsertStartResponse>, Status> {
        let req = request.into_inner();
        let mut all_replicas = Vec::new();
        let mut statuses = Vec::with_capacity(req.entries.len());
        let mut results = Vec::with_capacity(req.entries.len());

        for entry in &req.entries {
            let config = entry
                .config
                .as_ref()
                .map(config_from_proto)
                .unwrap_or_default();
            let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) else {
                let status = BatchStatus::IllegalClient.into();
                statuses.push(status);
                results.push(proto::BatchStartEntryResult {
                    key: entry.key.clone(),
                    replicas: vec![],
                    status,
                    tenant_id: normalize_tenant_id(&entry.tenant_id),
                });
                continue;
            };
            match self.upsert_start_for_entry(
                client_id,
                &entry.key,
                &entry.tenant_id,
                entry.slice_length,
                config,
            ) {
                Ok(replicas) => {
                    let proto_replicas = replicas.iter().map(replica_to_proto).collect::<Vec<_>>();
                    let status = BatchStatus::Success.into();
                    statuses.push(status);
                    all_replicas.extend(proto_replicas.iter().cloned());
                    results.push(proto::BatchStartEntryResult {
                        key: entry.key.clone(),
                        replicas: proto_replicas,
                        status,
                        tenant_id: normalize_tenant_id(&entry.tenant_id),
                    });
                }
                Err(status) => {
                    let status = Self::batch_status_from_status(&status).into();
                    statuses.push(status);
                    results.push(proto::BatchStartEntryResult {
                        key: entry.key.clone(),
                        replicas: vec![],
                        status,
                        tenant_id: normalize_tenant_id(&entry.tenant_id),
                    });
                }
            }
        }

        Ok(Response::new(proto::BatchUpsertStartResponse {
            replicas: all_replicas,
            statuses,
            results,
        }))
    }

    // ---- BatchUpsertEnd ----
    // C++ BatchUpsertEnd 语义：等价于 BatchPutEnd，用于确认 UpsertStart 写入完成。
    pub(super) async fn batch_upsert_end_impl(
        &self,
        request: Request<proto::BatchUpsertEndRequest>,
    ) -> Result<Response<proto::BatchUpsertEndResponse>, Status> {
        let req = request.into_inner();
        let statuses = req
            .entries
            .iter()
            .map(|entry| {
                let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) else {
                    return BatchStatus::IllegalClient.into();
                };
                let scoped_key = make_tenant_scoped_key(&entry.tenant_id, &entry.key);
                match self.apply_put_end_for_key(
                    &scoped_key,
                    client_id,
                    replica_type_from_i32(entry.replica_type),
                ) {
                    Ok(()) => BatchStatus::Success.into(),
                    Err(status) => Self::batch_status_from_status(&status).into(),
                }
            })
            .collect();
        Ok(Response::new(proto::BatchUpsertEndResponse { statuses }))
    }

    // ---- BatchUpsertRevoke ----
    // C++ BatchUpsertRevoke 语义：等价于 BatchPutRevoke。
    pub(super) async fn batch_upsert_revoke_impl(
        &self,
        request: Request<proto::BatchUpsertRevokeRequest>,
    ) -> Result<Response<proto::BatchUpsertRevokeResponse>, Status> {
        let req = request.into_inner();
        let statuses = req
            .entries
            .iter()
            .map(|entry| {
                let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) else {
                    return BatchStatus::IllegalClient.into();
                };
                let scoped_key = make_tenant_scoped_key(&entry.tenant_id, &entry.key);
                if self.state.replication_tasks.contains_key(&scoped_key) {
                    return BatchStatus::HasReplicationTask.into();
                }
                let Some(mut object) = self.state.objects.get_mut(&scoped_key) else {
                    return BatchStatus::KeyNotFound.into();
                };
                if object.client_id != client_id {
                    return BatchStatus::IllegalClient.into();
                }
                let target = replica_type_from_i32(entry.replica_type);
                let has_matching = object
                    .replicas
                    .iter()
                    .any(|replica| Self::put_revoke_matches_target(replica, target));
                let has_non_complete_matching = object.replicas.iter().any(|replica| {
                    Self::put_revoke_matches_target(replica, target)
                        && replica.status != ReplicaStatus::Complete
                });
                if has_matching && !has_non_complete_matching {
                    return BatchStatus::InvalidState.into();
                }
                let mut removed = Vec::new();
                object.replicas.retain(|replica| {
                    let matched = Self::put_revoke_matches_target(replica, target)
                        && replica.status != ReplicaStatus::Complete;
                    if matched {
                        removed.push(replica.clone());
                    }
                    !matched
                });
                let remove_object = object.replicas.is_empty();
                drop(object);
                release_object_replicas(&self.state, &scoped_key, &removed);
                if remove_object {
                    if let Some((_, object)) = self.state.objects.remove(&scoped_key) {
                        account_removed_object_quota(&self.state, &object);
                    }
                    self.state.processing_keys.remove(&scoped_key);
                    self.oplog_manager.lock().record_put_revoke(&scoped_key);
                }
                BatchStatus::Success.into()
            })
            .collect();
        Ok(Response::new(proto::BatchUpsertRevokeResponse { statuses }))
    }
}

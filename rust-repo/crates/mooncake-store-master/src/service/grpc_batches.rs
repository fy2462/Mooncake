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
use crate::service::helpers::replica_is_routable;

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

#[cfg(test)]
mod tests {
    use super::{BatchStatus, MasterServiceImpl};
    use tonic::{Code, Status};

    #[test]
    fn batch_status_keeps_business_errors_per_item() {
        assert_eq!(
            MasterServiceImpl::batch_status_from_status(Status::not_found("missing")).unwrap(),
            BatchStatus::KeyNotFound
        );
        assert_eq!(
            MasterServiceImpl::batch_status_from_status(Status::already_exists("duplicate"))
                .unwrap(),
            BatchStatus::ObjectAlreadyExists
        );
        assert_eq!(
            MasterServiceImpl::batch_status_from_status(Status::resource_exhausted("quota"))
                .unwrap(),
            BatchStatus::InvalidState
        );
        assert_eq!(
            MasterServiceImpl::batch_status_from_status(Status::invalid_argument(
                "invalid replica selector",
            ))
            .unwrap(),
            BatchStatus::InvalidState
        );
    }

    #[test]
    fn batch_status_propagates_durability_and_infrastructure_errors() {
        for status in [
            Status::unavailable("oplog flush failed"),
            Status::internal("invariant failed"),
            Status::data_loss("corrupt state"),
        ] {
            let error = MasterServiceImpl::batch_status_from_status(status).unwrap_err();
            assert!(matches!(
                error.code(),
                Code::Unavailable | Code::Internal | Code::DataLoss
            ));
        }
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
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let results = self.batch_replica_lists_for_keys(&tenant_id, &req.keys)?;
        Ok(Response::new(proto::BatchGetReplicaListResponse {
            results,
        }))
    }

    fn batch_replica_lists_for_keys(
        &self,
        tenant_id: &TenantId,
        keys: &[String],
    ) -> Result<Vec<proto::BatchGetReplicaListResult>, Status> {
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
            let scoped_key = tenant_id.make_scoped_key(key);
            let result = match self.object_snapshot_and_grant_lease(&scoped_key, true)? {
                Some(entry) => {
                    let complete = entry
                        .replicas
                        .iter()
                        .filter(|replica| replica_is_routable(&self.state, replica))
                        .collect::<Vec<_>>();
                    if complete.is_empty() {
                        proto::BatchGetReplicaListResult {
                            status: BatchStatus::ReplicaNotReady.into(),
                            response: None,
                            error_message: "replica is not ready".to_string(),
                        }
                    } else {
                        let first_replica_type = complete[0].replica_type;
                        let promotion_eligible = !complete
                            .iter()
                            .any(|replica| replica.replica_type == ReplicaType::Memory)
                            && complete
                                .iter()
                                .any(|replica| replica.replica_type == ReplicaType::LocalDisk);
                        let replicas = complete
                            .into_iter()
                            .map(|replica| replica_to_proto_for_state(&self.state, replica))
                            .collect();
                        hits.push(BatchGetHit {
                            scoped_key: scoped_key.clone(),
                            first_replica_type,
                            object_size: entry.size,
                            promotion_eligible,
                        });
                        proto::BatchGetReplicaListResult {
                            status: BatchStatus::Success.into(),
                            response: Some(proto::GetReplicaListResponse {
                                replicas,
                                lease_ttl_ms,
                            }),
                            error_message: String::new(),
                        }
                    }
                }
                None if self.state.objects.contains_key(&scoped_key) => {
                    proto::BatchGetReplicaListResult {
                        status: BatchStatus::ReplicaNotReady.into(),
                        response: None,
                        error_message: "replica is not ready".to_string(),
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

        for hit in hits {
            if hit.promotion_eligible {
                let _ = try_push_promotion_queue(&self.state, &hit.scoped_key, true);
            }
            metrics::GET_REQUESTS.inc();
            record_batch_cache_hit_metrics(hit.first_replica_type, hit.object_size);
        }

        Ok(results)
    }

    // ---- BatchExistKey ----
    // 批量检查 key 是否存在，返回布尔数组与输入 keys 一一对应。
    pub(super) async fn batch_exist_key_impl(
        &self,
        request: Request<proto::BatchExistKeyRequest>,
    ) -> Result<Response<proto::BatchExistKeyResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let results = self.batch_completed_objects_exist_and_grant_lease(&tenant_id, &req.keys)?;
        Ok(Response::new(proto::BatchExistKeyResponse { results }))
    }

    fn batch_completed_objects_exist_and_grant_lease(
        &self,
        tenant_id: &TenantId,
        keys: &[String],
    ) -> Result<Vec<bool>, Status> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let scoped_key = tenant_id.make_scoped_key(key);
            results.push(
                self.object_snapshot_and_grant_lease(&scoped_key, false)?
                    .is_some(),
            );
        }

        Ok(results)
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
            if let Some(addresses) = query_ip_addresses_for_client(&self.state, id) {
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
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let clear_all_segments = req.segment_name.is_empty();
        let mut cleared = vec![];
        for raw_key in &req.object_keys {
            let key = tenant_id.make_scoped_key(raw_key);
            let _mutation_guard = self.state.key_mutations.lock(&key);
            let mut remove_entire_object = false;
            let mut removed_replicas = Vec::new();
            let mut had_match = false;
            if let Some(mut object) = self.state.objects.get_mut(&key) {
                let mut projected = clone_object_for_mutation(&object);
                if object_owner_client_id(&self.state, &object) != Some(client_id) {
                    continue;
                }
                // C++ checks for active lease; skip objects that still hold a valid lease.
                if !is_lease_expired(&object) {
                    continue;
                }
                if clear_all_segments {
                    // Clearing the entire object is unsafe while any write is
                    // incomplete. C++ applies this all-replica check only to
                    // the clear-all form.
                    if object
                        .replicas
                        .iter()
                        .any(|replica| replica.status != ReplicaStatus::Complete)
                    {
                        continue;
                    }
                    had_match = !object.replicas.is_empty();
                    removed_replicas = object.replicas.clone();
                    remove_entire_object = had_match;
                } else {
                    let segment_name = req.segment_name.as_str();
                    projected.replicas.retain(|replica| {
                        // Segment-scoped clear removes only completed matches
                        // and may coexist with an unrelated in-flight replica.
                        let matches = replica.segment_name == segment_name
                            && replica.status == ReplicaStatus::Complete;
                        if matches {
                            had_match = true;
                            removed_replicas.push(replica.clone());
                        }
                        !matches
                    });
                    remove_entire_object = projected.replicas.is_empty() && had_match;
                }
                if had_match && !remove_entire_object {
                    let removed_memory_charge = requested_memory_quota_charge(
                        projected.size,
                        removed_replicas
                            .iter()
                            .filter(|replica| replica.replica_type == ReplicaType::Memory)
                            .count(),
                    );
                    if let Err(error) = release_committed_memory_quota_charge(
                        &self.state,
                        &mut projected,
                        removed_memory_charge,
                    ) {
                        tracing::warn!(
                            key,
                            ?error,
                            removed_memory_charge,
                            "failed to release tenant quota for cleared Memory replicas"
                        );
                        return Err(
                            self.tenant_quota_mutation_status("batch_replica_clear_quota", error)
                        );
                    }
                    sync_cache_total_accounting(&mut projected);
                    *object = projected;
                }
            }
            if had_match && remove_entire_object {
                if let Some((_, object)) = self.state.objects.remove(&key) {
                    self.account_removed_object_quota(&object)?;
                }
            }
            if had_match {
                // Replica removal is authoritative metadata. Persist the exact
                // remaining image (or removal marker) before releasing buffers
                // or publishing success, so promotion cannot resurrect the
                // cleared generation.
                self.persist_object_image_or_remove(&key, "batch_replica_clear")?;
                clear_offloading_task(&self.state, &key);
                clear_promotion_task(&self.state, &key);
                release_replicas(&self.state, &removed_replicas)?;
                cleared.push(raw_key.clone());
            }
        }
        Ok(Response::new(proto::BatchReplicaClearResponse {
            cleared_keys: cleared,
        }))
    }

    fn batch_status_from_status(status: Status) -> Result<BatchStatus, Status> {
        match status.code() {
            tonic::Code::NotFound => Ok(BatchStatus::KeyNotFound),
            tonic::Code::PermissionDenied => Ok(BatchStatus::IllegalClient),
            tonic::Code::AlreadyExists => Ok(BatchStatus::ObjectAlreadyExists),
            tonic::Code::InvalidArgument
            | tonic::Code::FailedPrecondition
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted => Ok(BatchStatus::InvalidState),
            // Infrastructure, durability and invariant failures are RPC-level
            // failures. Compressing them into a per-key business status would
            // let a fenced Master appear to have completed the batch.
            _ => Err(status),
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
        let tenant_ids = req
            .entries
            .iter()
            .map(|entry| {
                resolve_request_tenant(
                    &entry.tenant_id,
                    self.state.runtime_config.enable_tenant_quota,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut statuses = Vec::with_capacity(req.entries.len());
        for (entry, tenant_id) in req.entries.iter().zip(&tenant_ids) {
            let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) else {
                statuses.push(BatchStatus::IllegalClient.into());
                continue;
            };
            let target = match request_replica_type_from_i32(entry.replica_type) {
                Ok(target) => target,
                Err(status) => {
                    statuses.push(Self::batch_status_from_status(status)?.into());
                    continue;
                }
            };
            let scoped_key = tenant_id.make_scoped_key(&entry.key);
            let status = match self.apply_put_end_for_key(&scoped_key, client_id, target) {
                Ok(()) => BatchStatus::Success,
                Err(status) => Self::batch_status_from_status(status)?,
            };
            statuses.push(status.into());
        }
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
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let client_id = req.client_id.as_ref().map(uuid_from_proto);
        let target = request_replica_type_from_i32(req.replica_type)?;
        let mut statuses = Vec::with_capacity(req.keys.len());
        for raw_key in &req.keys {
            let key = tenant_id.make_scoped_key(raw_key);
            let _mutation_guard = self.state.key_mutations.lock(&key);
            if self.state.replication_tasks.contains_key(&key) {
                statuses.push(BatchStatus::HasReplicationTask.into());
                continue;
            }
            let Some(mut object) = self.state.objects.get_mut(&key) else {
                statuses.push(BatchStatus::KeyNotFound.into());
                continue;
            };
            if client_id.is_some_and(|cid| object.client_id != cid) {
                statuses.push(BatchStatus::IllegalClient.into());
                continue;
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
                statuses.push(BatchStatus::InvalidState.into());
                continue;
            }
            let original_replicas = object.replicas.clone();
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
            let quota_settled = if remove_object {
                false
            } else {
                match self.settle_object_quota_if_ready(&mut object) {
                    Ok(settled) => settled,
                    Err(_) => {
                        object.replicas = original_replicas;
                        statuses.push(BatchStatus::InvalidState.into());
                        continue;
                    }
                }
            };
            drop(object);
            if remove_object {
                if let Some((_, removed_object)) = self.state.objects.remove(&key) {
                    self.account_removed_object_quota(&removed_object)?;
                }
                self.state.processing_keys.remove(&key);
            } else if quota_settled {
                self.state.processing_keys.remove(&key);
            }
            self.persist_detached_allocator_replicas(&key, removed, "batch_put_revoke")?;
            statuses.push(BatchStatus::Success.into());
        }
        Ok(Response::new(proto::BatchPutRevokeResponse { statuses }))
    }

    // ---- BatchRemove ----
    // 批量删除对象，每个 key 执行相同的 force/lease 检查（与 Remove 一致），记录 oplog。
    pub(super) async fn batch_remove_impl(
        &self,
        request: Request<proto::BatchRemoveRequest>,
    ) -> Result<Response<proto::BatchRemoveResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let mut statuses = Vec::with_capacity(req.keys.len());
        for raw_key in &req.keys {
            let key = tenant_id.make_scoped_key(raw_key);
            let _mutation_guard = self.state.key_mutations.lock(&key);
            if self.state.replication_tasks.contains_key(&key) {
                statuses.push(BatchStatus::HasReplicationTask.into());
                continue;
            }
            let Some(object) = self.state.objects.get(&key) else {
                statuses.push(BatchStatus::KeyNotFound.into());
                continue;
            };
            if !req.force && !is_lease_expired(&object) {
                statuses.push(BatchStatus::ObjectHasLease.into());
                continue;
            }
            if !object
                .replicas
                .iter()
                .all(|r| r.status == ReplicaStatus::Complete)
            {
                statuses.push(BatchStatus::ReplicaNotReady.into());
                continue;
            }
            drop(object);
            let Some((_, object)) = self.state.objects.remove(&key) else {
                statuses.push(BatchStatus::KeyNotFound.into());
                continue;
            };
            if let Err(status) = self.cleanup_removed_object(&key, &object) {
                self.state.objects.insert(key, object);
                return Err(status);
            }
            self.publish_kv_removed(&key, &object);
            statuses.push(BatchStatus::Success.into());
        }
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
        let tenant_ids = req
            .entries
            .iter()
            .map(|entry| self.resolve_write_tenant(&entry.tenant_id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut all_replicas = Vec::new();
        let mut statuses = Vec::with_capacity(req.entries.len());
        let mut results = Vec::with_capacity(req.entries.len());

        for (entry, tenant_id) in req.entries.iter().zip(&tenant_ids) {
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
                    tenant_id: tenant_id.as_str().to_owned(),
                });
                continue;
            };
            match self.upsert_start_for_entry(
                client_id,
                &entry.key,
                tenant_id,
                entry.slice_length,
                config,
            ) {
                Ok(replicas) => {
                    let proto_replicas = replicas
                        .iter()
                        .map(|replica| replica_to_proto_for_state(&self.state, replica))
                        .collect::<Vec<_>>();
                    let status = BatchStatus::Success.into();
                    statuses.push(status);
                    all_replicas.extend(proto_replicas.iter().cloned());
                    results.push(proto::BatchStartEntryResult {
                        key: entry.key.clone(),
                        replicas: proto_replicas,
                        status,
                        tenant_id: tenant_id.as_str().to_owned(),
                    });
                }
                Err(status) => {
                    let status = Self::batch_status_from_status(status)?.into();
                    statuses.push(status);
                    results.push(proto::BatchStartEntryResult {
                        key: entry.key.clone(),
                        replicas: vec![],
                        status,
                        tenant_id: tenant_id.as_str().to_owned(),
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
        let tenant_ids = req
            .entries
            .iter()
            .map(|entry| {
                resolve_request_tenant(
                    &entry.tenant_id,
                    self.state.runtime_config.enable_tenant_quota,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut statuses = Vec::with_capacity(req.entries.len());
        for (entry, tenant_id) in req.entries.iter().zip(&tenant_ids) {
            let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) else {
                statuses.push(BatchStatus::IllegalClient.into());
                continue;
            };
            let target = match request_replica_type_from_i32(entry.replica_type) {
                Ok(target) => target,
                Err(status) => {
                    statuses.push(Self::batch_status_from_status(status)?.into());
                    continue;
                }
            };
            let scoped_key = tenant_id.make_scoped_key(&entry.key);
            let status = match self.apply_put_end_for_key(&scoped_key, client_id, target) {
                Ok(()) => BatchStatus::Success,
                Err(status) => Self::batch_status_from_status(status)?,
            };
            statuses.push(status.into());
        }
        Ok(Response::new(proto::BatchUpsertEndResponse { statuses }))
    }

    // ---- BatchUpsertRevoke ----
    // C++ BatchUpsertRevoke 语义：等价于 BatchPutRevoke。
    pub(super) async fn batch_upsert_revoke_impl(
        &self,
        request: Request<proto::BatchUpsertRevokeRequest>,
    ) -> Result<Response<proto::BatchUpsertRevokeResponse>, Status> {
        let req = request.into_inner();
        let tenant_ids = req
            .entries
            .iter()
            .map(|entry| {
                resolve_request_tenant(
                    &entry.tenant_id,
                    self.state.runtime_config.enable_tenant_quota,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut statuses = Vec::with_capacity(req.entries.len());
        for (entry, tenant_id) in req.entries.iter().zip(&tenant_ids) {
            let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) else {
                statuses.push(BatchStatus::IllegalClient.into());
                continue;
            };
            let target = match request_replica_type_from_i32(entry.replica_type) {
                Ok(target) => target,
                Err(status) => {
                    statuses.push(Self::batch_status_from_status(status)?.into());
                    continue;
                }
            };
            let scoped_key = tenant_id.make_scoped_key(&entry.key);
            let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
            if self.state.replication_tasks.contains_key(&scoped_key) {
                statuses.push(BatchStatus::HasReplicationTask.into());
                continue;
            }
            let Some(mut object) = self.state.objects.get_mut(&scoped_key) else {
                statuses.push(BatchStatus::KeyNotFound.into());
                continue;
            };
            if object.client_id != client_id {
                statuses.push(BatchStatus::IllegalClient.into());
                continue;
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
                statuses.push(BatchStatus::InvalidState.into());
                continue;
            }
            let original_replicas = object.replicas.clone();
            let mut removed = Vec::new();
            object.replicas.retain(|replica| {
                let matched = Self::put_revoke_matches_target(replica, target)
                    && replica.status != ReplicaStatus::Complete;
                if matched {
                    removed.push(replica.clone());
                }
                !matched
            });
            let has_completed_write_target = object.replicas.iter().any(|replica| {
                matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                ) && replica.status == ReplicaStatus::Complete
            });
            if !has_completed_write_target {
                // A failed in-place upsert must make the previous LocalDisk
                // value readable again. On partial success it stays
                // Allocating until the new value is offloaded.
                for replica in &mut object.replicas {
                    if replica.replica_type == ReplicaType::LocalDisk
                        && replica.status == ReplicaStatus::Allocating
                    {
                        replica.status = ReplicaStatus::Complete;
                    }
                }
            }
            let remove_object = object.replicas.is_empty();
            let quota_settled = if remove_object {
                false
            } else {
                match self.settle_object_quota_if_ready(&mut object) {
                    Ok(settled) => settled,
                    Err(_) => {
                        object.replicas = original_replicas;
                        statuses.push(BatchStatus::InvalidState.into());
                        continue;
                    }
                }
            };
            // A global-DISK descriptor is also a Put/Upsert write target.
            // Reuse the durable generation predicate so removing the final
            // Memory/NoF target cannot expose an object while DISK is still
            // Allocating.
            let mutation_finished = !object_has_inflight_write(&object);
            drop(object);
            if remove_object {
                if let Some((_, object)) = self.state.objects.remove(&scoped_key) {
                    self.account_removed_object_quota(&object)?;
                }
                self.state.processing_keys.remove(&scoped_key);
            } else if mutation_finished || quota_settled {
                self.state.processing_keys.remove(&scoped_key);
            }
            self.persist_detached_allocator_replicas(&scoped_key, removed, "batch_upsert_revoke")?;
            statuses.push(BatchStatus::Success.into());
        }
        Ok(Response::new(proto::BatchUpsertRevokeResponse { statuses }))
    }
}

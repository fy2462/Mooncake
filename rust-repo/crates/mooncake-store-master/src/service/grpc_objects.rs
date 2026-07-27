//! # Object CRUD Operations — 对象增删改查
//!
//! 本模块实现对象的完整生命周期管理，包括：
//!
//! This module implements the complete object lifecycle management, including:
//!
//! ## Put 写入生命周期 / Put Write Lifecycle
//!
//! ```text
//! PutStart → 分配副本 / allocate replicas → 客户端 RDMA 写入 / client RDMA writes
//!         → PutEnd → 标记 Complete / mark Complete → 从 processing_keys 移除
//!
//! PutRevoke → 撤销未完成的副本 / revoke unfinished replicas
//! ```
//!
//! ## 读路径 / Read Path
//!
//! ```text
//! GetReplicaList → 返回 Complete 副本列表 / return Complete replica list
//!                → 更新 lease + soft_pin 超时 / update lease + soft_pin timeout
//!                → 检查 promotion 条件并入队 / check promotion eligibility and enqueue
//! ```
//!
//! ## RPC 列表 / RPC List
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `ExistKey` | 检查 key 是否存在 / Check if key exists |
//! | `GetAllKeys` | 获取所有 key 列表 / Get all key list |
//! | `GetAllSegments` | 获取所有 Memory segment 名称 / Get all Memory segment names |
//! | `GetAllNoFSegments` / `GetNoFSegmentsByName` | 查询 NoF segment 信息 / Query NoF segment info |
//! | `PutStart` | 对象写入第一阶段：分配副本 / Write phase 1: allocate replicas |
//! | `PutEnd` | 对象写入第二阶段：标记完成 / Write phase 2: mark complete |
//! | `PutRevoke` | 撤销未完成的 PutStart / Revoke incomplete PutStart |
//! | `AddReplica` | 向对象追加副本 / Append replica to object |
//! | `GetReplicaList` / `GetReplicaListByRegex` | 查询已完成的副本列表 / Query completed replica list |
//! | `Remove` / `RemoveByRegex` / `RemoveAll` | 删除对象 / Delete objects |
//! | `QueryByRegex` / `QuerySegments` / `QueryIp` | 查询操作 / Query operations |
//! | `Upsert` | 原子 Upsert / Atomic upsert |

use super::*;
use crate::service::helpers::replica_is_routable;

impl MasterServiceImpl {
    pub(crate) fn persist_object_image_or_remove(
        &self,
        scoped_key: &str,
        operation: &str,
    ) -> Result<(), Status> {
        if let Err(error) = self
            .state
            .persist_object_image_or_remove_or_fence(scoped_key, operation)
        {
            return Err(Status::unavailable(format!(
                "failed to persist {operation} oplog: {error}"
            )));
        }
        Ok(())
    }

    /// Publish a metadata detach and retain every removed allocator range
    /// until a durable tombstone authorizes reuse.
    pub(crate) fn persist_detached_allocator_replicas(
        &self,
        scoped_key: &str,
        replicas: Vec<ReplicaDescriptor>,
        operation: &str,
    ) -> Result<(), Status> {
        let authoritative_object = self
            .state
            .objects
            .get(scoped_key)
            .map(|object| object.clone());
        let release_id = self
            .state
            .schedule_delayed_replica_release_or_fence(
                scoped_key,
                authoritative_object,
                replicas,
                Some(SystemTime::now()),
                operation,
            )
            .map_err(|error| {
                Status::unavailable(format!(
                    "failed to persist {operation} allocator reservation: {error}"
                ))
            })?;
        let Some(release_id) = release_id else {
            return self.persist_object_image_or_remove(scoped_key, operation);
        };
        let entry = self
            .state
            .delayed_replica_releases
            .get(&release_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| {
                self.state.fence_after_invariant_failure(
                    operation,
                    &format!("delayed release {release_id} disappeared before its tombstone"),
                );
                Status::internal("delayed allocator reservation disappeared")
            })?;
        self.state
            .persist_delayed_replica_release_removal_or_fence(&entry, operation)
            .map_err(|error| {
                Status::unavailable(format!(
                    "failed to persist {operation} allocator tombstone: {error}"
                ))
            })?;
        let Some((_, removed)) = self.state.delayed_replica_releases.remove(&release_id) else {
            self.state.fence_after_invariant_failure(
                operation,
                &format!("delayed release {release_id} disappeared after its tombstone"),
            );
            return Err(Status::internal(
                "delayed allocator reservation disappeared after persistence",
            ));
        };
        release_replicas(&self.state, &removed.replicas)?;
        if self.state.service_fenced.load(Ordering::Acquire) {
            return Err(Status::unavailable(format!(
                "{operation} fenced the service"
            )));
        }
        Ok(())
    }

    pub(crate) fn group_id_for_key(
        config: &ReplicateConfig,
        key_count: usize,
        key_index: usize,
    ) -> Result<String, Status> {
        if config.group_ids.is_empty() {
            return Ok(String::new());
        }
        if config.group_ids.len() != key_count || key_index >= key_count {
            return Err(Status::invalid_argument("invalid group_ids"));
        }
        Ok(config.group_ids[key_index].clone())
    }

    fn object_is_lease_eligible(&self, object: &ObjectEntry, require_routable: bool) -> bool {
        object.replicas.iter().any(|replica| {
            if require_routable {
                replica_is_routable(&self.state, replica)
            } else {
                replica.status == ReplicaStatus::Complete
            }
        })
    }

    fn lease_refresh_entry(
        scoped_key: &str,
        object: &ObjectEntry,
    ) -> Result<crate::oplog::LeaseRefreshEntry, Status> {
        let Some(lease_timeout) = object.lease_timeout else {
            return Err(Status::internal(
                "lease refresh did not produce a lease deadline",
            ));
        };
        Ok(crate::oplog::LeaseRefreshEntry {
            key: scoped_key.to_owned(),
            tenant_id: object.tenant_id.clone(),
            group_id: object.group_id.clone(),
            last_access: object.last_access,
            lease_timeout,
            soft_pin_timeout: object.soft_pin_timeout,
        })
    }

    fn persist_lease_refresh(
        &self,
        entries: &[crate::oplog::LeaseRefreshEntry],
    ) -> Result<(), Status> {
        if let Err(error) = self
            .oplog_manager
            .lock()
            .record_lease_refresh_batch_durable(entries)
        {
            self.state
                .fence_after_durability_failure("lease_refresh", &error);
            return Err(Status::unavailable(format!(
                "failed to persist lease refresh oplog: {error}"
            )));
        }
        Ok(())
    }

    fn grant_group_lease_locked(
        &self,
        scoped_key: &str,
        tenant_id: &TenantId,
        group_id: &str,
    ) -> Result<(ObjectEntry, Vec<crate::oplog::LeaseRefreshEntry>), Status> {
        debug_assert!(!group_id.is_empty());
        let mut target_snapshot = None;
        let mut refreshes = Vec::new();
        for mut entry in self.state.objects.iter_mut() {
            if &entry.tenant_id == tenant_id
                && entry.group_id == group_id
                && entry
                    .replicas
                    .iter()
                    .any(|replica| replica.status == ReplicaStatus::Complete)
            {
                entry.last_access = SystemTime::now();
                entry.grant_lease(
                    self.state.runtime_config.lease_ttl,
                    self.state.runtime_config.soft_pin_ttl,
                );
                let entry_key = entry.key().clone();
                refreshes.push(Self::lease_refresh_entry(&entry_key, &entry)?);
                if entry_key == scoped_key {
                    target_snapshot = Some(entry.clone());
                }
            }
        }
        let Some(target_snapshot) = target_snapshot else {
            return Err(Status::internal(
                "eligible group target disappeared during lease refresh",
            ));
        };
        Ok((target_snapshot, refreshes))
    }

    /// Return one object snapshot and publish its read lease at the same
    /// linearization point. Grouped objects retry under the exclusive snapshot
    /// barrier so the response and all-member lease update are atomic with
    /// respect to remove/upsert/eviction.
    pub(crate) fn object_snapshot_and_grant_lease(
        &self,
        scoped_key: &str,
        require_routable: bool,
    ) -> Result<Option<ObjectEntry>, Status> {
        let mutation_guard = self.state.key_mutations.lock(scoped_key);
        let Some(mut entry) = self.state.objects.get_mut(scoped_key) else {
            return Ok(None);
        };
        if !self.object_is_lease_eligible(&entry, require_routable) {
            return Ok(None);
        }
        if entry.group_id.is_empty() {
            entry.last_access = SystemTime::now();
            entry.grant_lease(
                self.state.runtime_config.lease_ttl,
                self.state.runtime_config.soft_pin_ttl,
            );
            let snapshot = entry.clone();
            let refresh = Self::lease_refresh_entry(scoped_key, &snapshot)?;
            self.persist_lease_refresh(&[refresh])?;
            return Ok(Some(snapshot));
        }
        let tenant_id = entry.tenant_id.clone();
        let group_id = entry.group_id.clone();
        drop(entry);
        drop(mutation_guard);

        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let Some(entry) = self.state.objects.get(scoped_key) else {
            return Ok(None);
        };
        if entry.tenant_id != tenant_id
            || entry.group_id != group_id
            || !self.object_is_lease_eligible(&entry, require_routable)
        {
            return Ok(None);
        }
        drop(entry);
        let (snapshot, refreshes) =
            self.grant_group_lease_locked(scoped_key, &tenant_id, &group_id)?;
        self.persist_lease_refresh(&refreshes)?;
        Ok(Some(snapshot))
    }

    pub(crate) fn cleanup_removed_object(
        &self,
        scoped_key: &str,
        object: &ObjectEntry,
    ) -> Result<(), Status> {
        if let Err(error) = self.oplog_manager.lock().record_remove_durable(scoped_key) {
            self.state.fence_after_durability_failure("remove", &error);
            return Err(Status::unavailable(format!(
                "failed to persist remove oplog: {error}"
            )));
        }
        for mut entry in self.state.client_objects.iter_mut() {
            entry.value_mut().remove(scoped_key);
        }
        self.account_removed_object_quota(object)?;
        self.state.processing_keys.remove(scoped_key);
        self.state.replication_tasks.remove(scoped_key);
        clear_offloading_task(&self.state, scoped_key);
        clear_promotion_task(&self.state, scoped_key);
        release_object_replicas(&self.state, scoped_key, &object.replicas)?;
        Ok(())
    }

    pub(crate) fn completed_object_exists_and_grant_lease(
        &self,
        scoped_key: &str,
    ) -> Result<bool, Status> {
        Ok(self
            .object_snapshot_and_grant_lease(scoped_key, false)?
            .is_some())
    }

    pub(crate) fn apply_put_end_for_key(
        &self,
        scoped_key: &str,
        client_id: Uuid,
        target: ReplicaType,
    ) -> Result<(), Status> {
        let _mutation_guard = self.state.key_mutations.lock(scoped_key);
        match self.state.objects.get_mut(scoped_key) {
            Some(mut entry) => {
                if entry.client_id != client_id {
                    return Err(Status::permission_denied("illegal client"));
                }
                // Complete a projected image first. Quota mismatch must not
                // expose Complete replicas, lease changes, or cache metrics.
                let mut completed = clone_object_for_mutation(&entry);
                for r in &mut completed.replicas {
                    let matches_type = if target == ReplicaType::All {
                        matches!(r.replica_type, ReplicaType::Memory | ReplicaType::NoFSsd)
                    } else {
                        r.replica_type == target
                    };
                    if matches_type && r.status == ReplicaStatus::Allocating && r.handle_valid {
                        r.status = ReplicaStatus::Complete;
                    }
                }
                // C++ PutEnd grants ttl=0: the object starts without a hard read lease,
                // while soft pin is extended when enabled.
                completed.grant_lease(Duration::ZERO, self.state.runtime_config.soft_pin_ttl);
                let has_write_target = completed.replicas.iter().any(|r| {
                    matches!(
                        r.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd | ReplicaType::Disk
                    )
                });
                let all_complete = has_write_target
                    && completed
                        .replicas
                        .iter()
                        .filter(|r| {
                            matches!(
                                r.replica_type,
                                ReplicaType::Memory | ReplicaType::NoFSsd | ReplicaType::Disk
                            )
                        })
                        .all(|r| r.status == ReplicaStatus::Complete);
                let size = completed.size;
                let tenant_id = completed.tenant_id.clone();
                let has_memory_replica = completed
                    .replicas
                    .iter()
                    .any(|replica| replica.replica_type == ReplicaType::Memory);
                let should_settle_quota = !completed.quota_committed
                    && self.state.processing_keys.contains_key(scoped_key)
                    && (target == ReplicaType::Memory
                        || (target == ReplicaType::All && has_memory_replica)
                        || !has_memory_replica);
                if should_settle_quota {
                    let reserved_charge = completed.reserved_quota_charge_bytes;
                    let committed_charge = completed_memory_quota_charge(&completed);
                    self.settle_tenant_quota(
                        &tenant_id,
                        reserved_charge,
                        committed_charge,
                        true,
                        completed.pending_replaced_quota_charge_bytes,
                    )?;
                    completed.quota_committed = true;
                    completed.reserved_quota_charge_bytes = 0;
                    completed.committed_quota_charge_bytes = committed_charge;
                    completed.pending_replaced_quota_charge_bytes = 0;
                }
                if all_complete {
                    completed.put_start_time = None;
                }
                sync_cache_total_accounting(&mut completed);
                let offload_enabled = !self.state.runtime_config.offload_on_evict;
                let durable_image = completed.clone();
                *entry = completed;
                drop(entry);

                if let Err(error) = self
                    .oplog_manager
                    .lock()
                    .record_object_image_durable(scoped_key, &durable_image)
                {
                    self.state.fence_after_durability_failure("put_end", &error);
                    return Err(Status::unavailable(format!(
                        "failed to persist put_end oplog: {error}"
                    )));
                }

                if offload_enabled {
                    push_offloading_queue(&self.state, client_id, scoped_key, size);
                }
                if all_complete && self.state.processing_keys.contains_key(scoped_key) {
                    self.state.processing_keys.remove(scoped_key);
                }
                if all_complete {
                    self.state
                        .client_objects
                        .entry(client_id)
                        .or_default()
                        .insert(scoped_key.to_string());
                }
                match self.state.objects.get(scoped_key) {
                    Some(entry) => {
                        if all_complete {
                            self.publish_kv_stored(scoped_key, target, &entry);
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
            _ => Err(Status::not_found("key not found")),
        }
    }

    pub(crate) fn settle_object_quota_if_ready(
        &self,
        object: &mut ObjectEntry,
    ) -> Result<bool, Status> {
        let write_targets = object.replicas.iter().filter(|replica| {
            matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd | ReplicaType::Disk
            )
        });
        let mut has_write_target = false;
        let mut all_complete = true;
        for replica in write_targets {
            has_write_target = true;
            all_complete &= replica.status == ReplicaStatus::Complete;
        }
        if !has_write_target || !all_complete {
            return Ok(false);
        }
        object.put_start_time = None;
        if object.quota_committed {
            return Ok(true);
        }

        let committed_charge = completed_memory_quota_charge(object);
        self.settle_tenant_quota(
            &object.tenant_id,
            object.reserved_quota_charge_bytes,
            committed_charge,
            true,
            object.pending_replaced_quota_charge_bytes,
        )?;
        object.quota_committed = true;
        object.reserved_quota_charge_bytes = 0;
        object.committed_quota_charge_bytes = committed_charge;
        object.pending_replaced_quota_charge_bytes = 0;
        Ok(true)
    }

    // ---- ExistKey ----
    // 检查指定 key 是否存在于 master 的对象表中，O(1) 哈希查找。
    // Check if a key exists in the master's object table, O(1) hash lookup.
    pub(super) async fn exist_key_impl(
        &self,
        request: Request<proto::ExistKeyRequest>,
    ) -> Result<Response<proto::ExistKeyResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let scoped_key = tenant_id.make_scoped_key(&req.key);
        let exists = self.completed_object_exists_and_grant_lease(&scoped_key)?;
        metrics::GET_REQUESTS.inc();
        Ok(Response::new(proto::ExistKeyResponse { exists }))
    }

    // ---- GetAllKeys ----
    // 返回当前所有已存储对象的 key 列表，用于客户端全量扫描。
    // Return all currently stored object keys; used by clients for full scanning.
    pub(super) async fn get_all_keys_impl(
        &self,
        request: Request<proto::GetAllKeysRequest>,
    ) -> Result<Response<proto::GetAllKeysResponse>, Status> {
        let req = request.into_inner();
        let tenant_filter = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let keys: Vec<String> = self
            .state
            .objects
            .iter()
            .filter(|entry| entry.tenant_id == tenant_filter)
            .map(|entry| {
                // Return user_key — C++ equivalent: item.second.user_key
                // C++: item.second.user_key.empty() ? item.first : item.second.user_key
                if entry.user_key.is_empty() {
                    entry.key().clone()
                } else {
                    entry.user_key.clone()
                }
            })
            .collect();
        Ok(Response::new(proto::GetAllKeysResponse { keys }))
    }

    // ---- GetAllSegments ----
    // 返回所有已挂载的 Memory segment 名称列表，供管理端查询拓扑。
    // Return all mounted Memory segment names; used for admin topology queries.
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
    // 返回所有已挂载的 NoF (NVMe-oF) segment 列表，含传输端点等完整信息。
    // Return all mounted NoF segments with full transport endpoint info.
    pub(super) async fn get_all_nof_segments_impl(
        &self,
        _request: Request<proto::GetAllNoFSegmentsRequest>,
    ) -> Result<Response<proto::GetAllNoFSegmentsResponse>, Status> {
        if !self.state.runtime_config.enable_nof {
            return Err(Status::unavailable("NoF is not enabled"));
        }
        let segments = self
            .state
            .nof_segments
            .iter()
            .map(|entry| nof_segment_to_proto(&entry.segment))
            .collect();
        Ok(Response::new(proto::GetAllNoFSegmentsResponse { segments }))
    }

    // ---- GetNoFSegmentsByName ----
    // 按 segment 名称查询所属的 NoF owner 列表，用于定位特定 NoF 设备的所有者。
    // Query NoF owner list by segment name; used to locate owners of a specific NoF device.
    pub(super) async fn get_nof_segments_by_name_impl(
        &self,
        request: Request<proto::GetNoFSegmentsByNameRequest>,
    ) -> Result<Response<proto::GetNoFSegmentsByNameResponse>, Status> {
        if !self.state.runtime_config.enable_nof {
            return Err(Status::unavailable("NoF is not enabled"));
        }
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
        Ok(Response::new(proto::GetNoFSegmentsByNameResponse {
            owners,
        }))
    }

    pub(super) async fn get_segments_detail_impl(
        &self,
        _request: Request<proto::GetSegmentsDetailRequest>,
    ) -> Result<Response<proto::GetSegmentsDetailResponse>, Status> {
        Ok(Response::new(proto::GetSegmentsDetailResponse {
            segments: self.segments_detail_snapshot(),
        }))
    }

    pub(super) async fn service_ready_impl(
        &self,
        _request: Request<proto::ServiceReadyRequest>,
    ) -> Result<Response<proto::ServiceReadyResponse>, Status> {
        Ok(Response::new(proto::ServiceReadyResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    pub(super) async fn get_all_keys_for_admin_impl(
        &self,
        _request: Request<proto::GetAllKeysForAdminRequest>,
    ) -> Result<Response<proto::GetAllKeysForAdminResponse>, Status> {
        let keys = self.all_keys_for_admin();
        Ok(Response::new(proto::GetAllKeysForAdminResponse { keys }))
    }

    pub(crate) fn all_keys_for_admin(&self) -> Vec<String> {
        self.state
            .objects
            .iter()
            .map(|entry| entry.user_key.clone())
            .collect()
    }

    pub(super) async fn get_all_segments_for_admin_impl(
        &self,
        _request: Request<proto::GetAllSegmentsForAdminRequest>,
    ) -> Result<Response<proto::GetAllSegmentsForAdminResponse>, Status> {
        let segments = self
            .state
            .segments
            .iter()
            .map(|entry| entry.segment.name.clone())
            .collect();
        Ok(Response::new(proto::GetAllSegmentsForAdminResponse {
            segments,
        }))
    }

    pub(super) async fn query_segment_for_admin_impl(
        &self,
        request: Request<proto::QuerySegmentsRequest>,
    ) -> Result<Response<proto::QuerySegmentsResponse>, Status> {
        self.query_segments_impl(request).await
    }

    pub(super) async fn calc_cache_stats_impl(
        &self,
        _request: Request<proto::CalcCacheStatsRequest>,
    ) -> Result<Response<proto::CalcCacheStatsResponse>, Status> {
        let mut memory_total = 0f64;
        let mut ssd_total = 0f64;
        let mut memory_hits = 0f64;
        let mut ssd_hits = 0f64;

        for entry in self.state.objects.iter() {
            let has_memory = entry.replicas.iter().any(|replica| {
                replica.status == ReplicaStatus::Complete
                    && replica.replica_type == ReplicaType::Memory
            });
            let has_ssd = entry.replicas.iter().any(|replica| {
                replica.status == ReplicaStatus::Complete
                    && matches!(
                        replica.replica_type,
                        ReplicaType::LocalDisk | ReplicaType::Disk | ReplicaType::NoFSsd
                    )
            });
            if has_memory {
                memory_total += 1.0;
                if !is_lease_expired(&entry) {
                    memory_hits += 1.0;
                }
            }
            if has_ssd {
                ssd_total += 1.0;
                if !is_lease_expired(&entry) {
                    ssd_hits += 1.0;
                }
            }
        }

        let memory_hit_rate = if memory_total > 0.0 {
            memory_hits / memory_total
        } else {
            0.0
        };
        let ssd_hit_rate = if ssd_total > 0.0 {
            ssd_hits / ssd_total
        } else {
            0.0
        };
        let total = memory_total + ssd_total;
        let hits = memory_hits + ssd_hits;
        let overall_hit_rate = if total > 0.0 { hits / total } else { 0.0 };

        let mut stats = HashMap::new();
        stats.insert("memory_hits".to_string(), memory_hits);
        stats.insert("ssd_hits".to_string(), ssd_hits);
        stats.insert("memory_total".to_string(), memory_total);
        stats.insert("ssd_total".to_string(), ssd_total);
        stats.insert("memory_hit_rate".to_string(), memory_hit_rate);
        stats.insert("ssd_hit_rate".to_string(), ssd_hit_rate);
        stats.insert("overall_hit_rate".to_string(), overall_hit_rate);
        stats.insert(
            "valid_get_rate".to_string(),
            if self.state.objects.is_empty() {
                0.0
            } else {
                hits / self.state.objects.len() as f64
            },
        );
        Ok(Response::new(proto::CalcCacheStatsResponse { stats }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn completed_test_object(tenant_id: TenantId, user_key: &str) -> ObjectEntry {
        ObjectEntry {
            replicas: vec![ReplicaDescriptor {
                segment_id: Uuid::nil(),
                segment_name: String::new(),
                offset: 0,
                size: 1,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Disk,
                holder_client_id: None,
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: 0,
                protocol: String::new(),
            }],
            size: 1,
            last_access: SystemTime::UNIX_EPOCH,
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::nil(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id,
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: user_key.into(),
        }
    }

    #[test]
    fn exist_lease_refresh_waits_for_tenant_scoped_mutation_guard() {
        let service = Arc::new(MasterServiceImpl::default());
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("lease-atomic");
        service.state.objects.insert(
            key.clone(),
            completed_test_object(tenant_id, "lease-atomic"),
        );
        let mutation_guard = service.state.key_mutations.lock(&key);
        let worker = Arc::clone(&service);
        let worker_key = key.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let exists = worker
                .completed_object_exists_and_grant_lease(&worker_key)
                .unwrap();
            tx.send(exists).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(25)).is_err(),
            "lease mutation bypassed the tenant-scoped key guard"
        );
        drop(mutation_guard);
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        thread.join().unwrap();
        assert!(
            service
                .state
                .objects
                .get(&key)
                .unwrap()
                .lease_timeout
                .is_some()
        );
    }

    #[test]
    fn group_lease_refresh_waits_for_all_inflight_key_mutations() {
        let service = Arc::new(MasterServiceImpl::default());
        let tenant_id = TenantId::default();
        let first_key = tenant_id.make_scoped_key("group-first");
        let second_key = tenant_id.make_scoped_key("group-second");
        for (key, user_key) in [(&first_key, "group-first"), (&second_key, "group-second")] {
            let mut object = completed_test_object(tenant_id.clone(), user_key);
            object.group_id = "atomic-group".into();
            service.state.objects.insert(key.clone(), object);
        }
        let second_mutation = service.state.key_mutations.lock(&second_key);
        let worker = Arc::clone(&service);
        let worker_key = first_key.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let found = worker
                .object_snapshot_and_grant_lease(&worker_key, false)
                .unwrap()
                .is_some();
            tx.send(found).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(25)).is_err(),
            "group lease did not wait for the full mutation epoch"
        );
        drop(second_mutation);
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        thread.join().unwrap();
        assert!(
            service
                .state
                .objects
                .get(&first_key)
                .unwrap()
                .lease_timeout
                .is_some()
        );
        assert!(
            service
                .state
                .objects
                .get(&second_key)
                .unwrap()
                .lease_timeout
                .is_some()
        );
    }

    #[test]
    fn put_end_quota_failure_does_not_publish_projected_object_state() {
        let policy_uri = tempfile::NamedTempFile::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .into_owned();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_tenant_quota: true,
            tenant_quota_connector_uri: policy_uri,
            ..Default::default()
        });
        let tenant_id = TenantId::new("tenant-a".into()).unwrap();
        let key = tenant_id.make_scoped_key("quota-atomic");
        let client_id = Uuid::new_v4();
        {
            let mut quotas = service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 100, 100).unwrap();
            quotas.register_object(&tenant_id);
            quotas.reserve(&tenant_id, 50).unwrap();
        }
        service.state.objects.insert(
            key.clone(),
            ObjectEntry {
                replicas: vec![ReplicaDescriptor {
                    segment_id: Uuid::new_v4(),
                    segment_name: "memory".into(),
                    offset: 0,
                    size: 100,
                    status: ReplicaStatus::Allocating,
                    replica_type: ReplicaType::Memory,
                    holder_client_id: Some(client_id),
                    local_disk_storage_id: None,
                    local_disk_generation_id: None,
                    refcnt: 0,
                    handle_valid: true,
                    base_addr: 0,
                    protocol: String::new(),
                }],
                size: 100,
                last_access: SystemTime::now(),
                hard_pinned: false,
                data_type: Default::default(),
                client_id,
                put_start_time: Some(SystemTime::now()),
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: tenant_id.clone(),
                group_id: String::new(),
                quota_committed: false,
                reserved_quota_charge_bytes: 100,
                committed_quota_charge_bytes: 0,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "quota-atomic".into(),
            },
        );
        service.state.processing_keys.insert(key.clone(), ());

        assert!(
            service
                .apply_put_end_for_key(&key, client_id, ReplicaType::Memory)
                .is_err()
        );

        let object = service.state.objects.get(&key).unwrap();
        assert_eq!(object.replicas[0].status, ReplicaStatus::Allocating);
        assert!(!object.quota_committed);
        assert_eq!(object.reserved_quota_charge_bytes, 100);
        assert!(object.lease_timeout.is_none());
        assert!(!object.memory_cache_total_accounted);
        drop(object);
        let quota = service
            .state
            .tenant_quotas
            .read()
            .get_snapshot(&tenant_id)
            .unwrap();
        assert_eq!(quota.reserved_bytes, 50);
        assert_eq!(quota.used_bytes, 0);
        assert!(service.state.processing_keys.contains_key(&key));
    }

    #[test]
    fn in_place_upsert_put_end_preserves_committed_charge_and_closes_generation() {
        let service = MasterServiceImpl::default();
        let tenant_id = TenantId::default();
        let key = tenant_id.make_scoped_key("in-place-put-end");
        let client_id = Uuid::new_v4();
        service.state.objects.insert(
            key.clone(),
            ObjectEntry {
                replicas: vec![ReplicaDescriptor {
                    segment_id: Uuid::new_v4(),
                    segment_name: "memory".into(),
                    offset: 0,
                    size: 100,
                    status: ReplicaStatus::Allocating,
                    replica_type: ReplicaType::Memory,
                    holder_client_id: Some(client_id),
                    local_disk_storage_id: None,
                    local_disk_generation_id: None,
                    refcnt: 0,
                    handle_valid: true,
                    base_addr: 0,
                    protocol: String::new(),
                }],
                size: 100,
                last_access: SystemTime::now(),
                hard_pinned: false,
                data_type: Default::default(),
                client_id,
                put_start_time: Some(SystemTime::now()),
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id,
                group_id: String::new(),
                quota_committed: true,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 100,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "in-place-put-end".into(),
            },
        );
        service.state.processing_keys.insert(key.clone(), ());

        service
            .apply_put_end_for_key(&key, client_id, ReplicaType::Memory)
            .expect("in-place Upsert completion must produce a valid durable image");

        let object = service.state.objects.get(&key).unwrap();
        assert_eq!(object.replicas[0].status, ReplicaStatus::Complete);
        assert!(object.quota_committed);
        assert_eq!(object.committed_quota_charge_bytes, 100);
        assert!(object.put_start_time.is_none());
        assert!(!service.state.processing_keys.contains_key(&key));
        assert!(!service.is_service_fenced());
    }
}

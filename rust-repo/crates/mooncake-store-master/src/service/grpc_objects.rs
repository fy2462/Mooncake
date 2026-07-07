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

impl MasterServiceImpl {
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

    pub(crate) fn grant_group_lease(&self, tenant_id: &str, group_id: &str) {
        if group_id.is_empty() {
            return;
        }
        let keys = self
            .state
            .objects
            .iter()
            .filter(|entry| entry.tenant_id == tenant_id && entry.group_id == group_id)
            .filter(|entry| {
                entry
                    .replicas
                    .iter()
                    .any(|replica| replica.status == ReplicaStatus::Complete)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(mut entry) = self.state.objects.get_mut(&key) {
                entry.last_access = SystemTime::now();
                entry.grant_lease(
                    self.state.runtime_config.lease_ttl,
                    self.state.runtime_config.soft_pin_ttl,
                );
            }
        }
    }

    pub(crate) fn cleanup_removed_object(
        &self,
        scoped_key: &str,
        object: &ObjectEntry,
    ) -> Result<(), Status> {
        self.oplog_manager
            .lock()
            .record_remove_durable(scoped_key)
            .map_err(|e| Status::internal(format!("failed to persist remove oplog: {e}")))?;
        for mut entry in self.state.client_objects.iter_mut() {
            entry.value_mut().remove(scoped_key);
        }
        self.account_removed_object_quota(object);
        self.state.processing_keys.remove(scoped_key);
        self.state.replication_tasks.remove(scoped_key);
        clear_offloading_task(&self.state, scoped_key);
        clear_promotion_task(&self.state, scoped_key);
        release_object_replicas(&self.state, scoped_key, &object.replicas);
        Ok(())
    }

    pub(crate) fn completed_object_exists_and_grant_lease(&self, scoped_key: &str) -> bool {
        let Some(mut entry) = self.state.objects.get_mut(scoped_key) else {
            return false;
        };
        let exists = entry
            .replicas
            .iter()
            .any(|replica| replica.status == ReplicaStatus::Complete);
        if exists {
            entry.last_access = SystemTime::now();
            entry.grant_lease(
                self.state.runtime_config.lease_ttl,
                self.state.runtime_config.soft_pin_ttl,
            );
            let tenant_id = entry.tenant_id.clone();
            let group_id = entry.group_id.clone();
            drop(entry);
            self.grant_group_lease(&tenant_id, &group_id);
        }
        exists
    }

    pub(crate) fn apply_put_end_for_key(
        &self,
        scoped_key: &str,
        client_id: Uuid,
        target: ReplicaType,
    ) -> Result<(), Status> {
        if let Some(mut entry) = self.state.objects.get_mut(scoped_key) {
            if entry.client_id != client_id {
                return Err(Status::permission_denied("illegal client"));
            }
            for r in &mut entry.replicas {
                let matches_type = target == ReplicaType::All || r.replica_type == target;
                if matches_type && r.status == ReplicaStatus::Allocating && r.handle_valid {
                    r.status = ReplicaStatus::Complete;
                }
            }
            // C++ PutEnd grants ttl=0: the object starts without a hard read lease,
            // while soft pin is extended when enabled.
            entry.grant_lease(Duration::ZERO, self.state.runtime_config.soft_pin_ttl);
            let all_complete = entry
                .replicas
                .iter()
                .all(|r| r.status == ReplicaStatus::Complete);
            let size = entry.size;
            let tenant_id = entry.tenant_id.clone();
            let should_commit_quota = all_complete
                && !entry.quota_committed
                && self.state.processing_keys.contains_key(scoped_key);
            if should_commit_quota {
                entry.quota_committed = true;
            }
            let offload_enabled = !self.state.runtime_config.offload_on_evict;
            drop(entry);

            if should_commit_quota {
                self.commit_tenant_quota(&tenant_id, size)?;
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
            if let Some(entry) = self.state.objects.get(scoped_key) {
                self.oplog_manager.lock().record_put_end_with_metadata(
                    scoped_key,
                    size,
                    Some(entry.client_id),
                    &entry.tenant_id,
                    &entry.group_id,
                    &entry.user_key,
                    &entry.replicas,
                );
            } else {
                self.oplog_manager.lock().record_put_end(scoped_key, size);
            }
            Ok(())
        } else {
            Err(Status::not_found("key not found"))
        }
    }

    // ---- ExistKey ----
    // 检查指定 key 是否存在于 master 的对象表中，O(1) 哈希查找。
    // Check if a key exists in the master's object table, O(1) hash lookup.
    pub(super) async fn exist_key_impl(
        &self,
        request: Request<proto::ExistKeyRequest>,
    ) -> Result<Response<proto::ExistKeyResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let exists = self.completed_object_exists_and_grant_lease(&scoped_key);
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
        let tenant_filter = normalize_tenant_id(&req.tenant_id);
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
        let memory_used = self.state.allocator.read();
        let nof_used = self.state.nof_allocator.read();
        let mut segments =
            Vec::with_capacity(self.state.segments.len() + self.state.nof_segments.len());

        for entry in self.state.segments.iter() {
            let segment = &entry.segment;
            segments.push(proto::SegmentDetailInfo {
                segment_name: segment.name.clone(),
                segment_id: Some(uuid_to_proto(segment.id)),
                client_id: Some(uuid_to_proto(entry.client_id)),
                base_address: segment.base,
                size_bytes: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: segment.protocol.clone(),
                status: entry.status.into(),
                allocator_used_bytes: memory_used.used_bytes(&segment.id).unwrap_or(entry.used),
                allocator_capacity_bytes: segment.size,
                nof: false,
            });
        }

        for entry in self.state.nof_segments.iter() {
            let segment = &entry.segment;
            segments.push(proto::SegmentDetailInfo {
                segment_name: segment.name.clone(),
                segment_id: Some(uuid_to_proto(segment.id)),
                client_id: Some(uuid_to_proto(segment.client_id)),
                base_address: segment.base,
                size_bytes: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: "nof".to_string(),
                status: entry.status.into(),
                allocator_used_bytes: nof_used.used_bytes(&segment.id).unwrap_or(entry.used),
                allocator_capacity_bytes: segment.size,
                nof: true,
            });
        }

        Ok(Response::new(proto::GetSegmentsDetailResponse { segments }))
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

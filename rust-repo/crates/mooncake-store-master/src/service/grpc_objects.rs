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
    // ---- ExistKey ----
    // 检查指定 key 是否存在于 master 的对象表中，O(1) 哈希查找。
    // Check if a key exists in the master's object table, O(1) hash lookup.
    pub(super) async fn exist_key_impl(
        &self,
        request: Request<proto::ExistKeyRequest>,
    ) -> Result<Response<proto::ExistKeyResponse>, Status> {
        let req = request.into_inner();
        let exists = self.state.objects.contains_key(&req.key);
        metrics::GET_REQUESTS.inc();
        Ok(Response::new(proto::ExistKeyResponse { exists }))
    }

    // ---- GetAllKeys ----
    // 返回当前所有已存储对象的 key 列表，用于客户端全量扫描。
    // Return all currently stored object keys; used by clients for full scanning.
    pub(super) async fn get_all_keys_impl(
        &self,
        _request: Request<proto::GetAllKeysRequest>,
    ) -> Result<Response<proto::GetAllKeysResponse>, Status> {
        let keys: Vec<String> = self
            .state
            .objects
            .iter()
            .map(|entry| entry.key().clone())
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

    // ---- PutStart ----
    // 对象写入的第一阶段：分配副本、注册对象元数据。
    // 流程：(1) 校验 key/size → (2) 若对象已存在则清理过期 handle 或超时丢弃
    // (3) 分配 Memory 副本并可选分配 NoF 副本 → (4) 写入对象表并标记 processing_keys。
    // 返回分配的副本列表供客户端 RDMA 写入。
    //
    // Object write phase 1: allocate replicas, register object metadata.
    // Flow: (1) validate key/size → (2) if object exists, clean stale handles or timeout-discard
    // (3) allocate Memory replicas and optionally NoF replicas → (4) write object table and mark processing_keys.
    // Returns allocated replica list for client RDMA write.
    pub(super) async fn put_start_impl(
        &self,
        request: Request<proto::PutStartRequest>,
    ) -> Result<Response<proto::PutStartResponse>, Status> {
        let req = request.into_inner();

        // C++ master_service.cpp:1287-1294 对空 key 和零长度 slice 进行校验
        // Validate non-empty key and non-zero slice length
        if req.key.is_empty() {
            return Err(Status::invalid_argument("empty key"));
        }
        if req.slice_length == 0 {
            return Err(Status::invalid_argument("zero slice_length"));
        }

        let key = req.key.clone();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );

        // C++ master_service.cpp:1467-1507 — prepare_existing:
        // 1. CleanupStaleHandles 清理无效 handle 和死亡客户端的副本
        // 2. 如果所有有效副本都被清理，删除对象并允许新 PutStart
        // 3. 如果 PutStart 超时且无 Completed 副本，丢弃旧对象
        // 4. 否则返回 OBJECT_ALREADY_EXISTS
        //
        // 1. CleanupStaleHandles: clean invalid handles and dead-client replicas
        // 2. If all valid replicas cleaned, delete object and allow new PutStart
        // 3. If PutStart timed out with no Completed replicas, discard old object
        // 4. Otherwise return OBJECT_ALREADY_EXISTS
        if let Some(mut existing) = self.state.objects.get_mut(&key) {
            let alive_clients = get_alive_clients_snapshot(&self.state);
            let should_remove = cleanup_stale_handles(&mut existing, &alive_clients);

            if should_remove {
                // 所有有效副本已被清理（handle 失效或客户端死亡），删除对象并允许新 PutStart
                // All valid replicas cleaned (stale handle or dead client); delete object and allow new PutStart
                let old_replicas = existing.replicas.clone();
                self.state.objects.remove(&key);
                self.state.processing_keys.remove(&key);
                self.state.replication_tasks.remove(&key);
                drop(existing);
                release_replicas(&self.state, &old_replicas);
            } else {
                // 对象仍有有效副本，但需检查是否可超时丢弃：
                // 仅当无 Completed 副本且 PutStart 已超时时才允许覆盖，否则返回已存在错误
                //
                // Object still has valid replicas, but check if timeout-discardable:
                // Only allow overwrite when no Completed replicas and PutStart has timed out.
                let has_completed = existing
                    .replicas
                    .iter()
                    .any(|r| r.status == ReplicaStatus::Complete);
                if !has_completed {
                    if let Some(start) = existing.put_start_time {
                        let elapsed = SystemTime::now().duration_since(start).unwrap_or_default();
                        if elapsed >= self.state.runtime_config.put_start_discard_timeout {
                            // PutStart 超时，删除对象 / PutStart timed out; delete object
                            let old_replicas = existing.replicas.clone();
                            let expired = existing
                                .replicas
                                .iter()
                                .filter(|r| r.status == ReplicaStatus::Allocating)
                                .cloned()
                                .collect::<Vec<_>>();
                            self.state.objects.remove(&key);
                            self.state.processing_keys.remove(&key);
                            self.state.replication_tasks.remove(&key);
                            drop(existing);
                            if !expired.is_empty() {
                                release_replicas_scheduled(&self.state, expired);
                            } else if !old_replicas.is_empty() {
                                release_replicas(&self.state, &old_replicas);
                            }
                        } else {
                            return Err(Status::already_exists(format!(
                                "object already exists: {}",
                                key
                            )));
                        }
                    } else {
                        return Err(Status::already_exists(format!(
                            "object already exists: {}",
                            key
                        )));
                    }
                } else {
                    return Err(Status::already_exists(format!(
                        "object already exists: {}",
                        key
                    )));
                }
            }
        }

        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        if config.replica_num == 0 && config.nof_replica_num == 0 {
            return Err(Status::invalid_argument(
                "replica_num and nof_replica_num cannot both be zero",
            ));
        }
        let replica_count = config.replica_num.max(1) as usize;

        // 分配 Memory 副本 / Allocate Memory replicas
        let mut replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate_for_client(
                &key,
                Some(client_id),
                req.slice_length,
                replica_count,
                &config,
            )
        };
        if replicas.is_empty() && replica_count > 0 {
            return Err(Status::resource_exhausted(format!(
                "failed to allocate {replica_count} replica(s) for key {key}{}",
                PUT_NO_SPACE_HELPER_STR,
            )));
        }
        // NoF 副本分配：优先分配在与 Memory 副本相同节点上的 NoF segment，降低跨节点访问延迟
        // NoF replica allocation: prefer NoF segments on same node as Memory replicas for lower latency
        if config.nof_replica_num > 0 {
            let preferred_nof = if config.prefer_alloc_in_same_node {
                preferred_nof_segment_names(&self.state, &replicas)
            } else {
                Vec::new()
            };
            if config.prefer_alloc_in_same_node && preferred_nof.is_empty() {
                release_replicas(&self.state, &replicas);
                return Err(Status::invalid_argument(
                    "prefer_alloc_in_same_node requires matching NoF segment",
                ));
            }
            let nof_replicas = allocate_nof_replicas(
                &self.state,
                &key,
                req.slice_length,
                config.nof_replica_num as usize,
                &preferred_nof,
            )?;
            replicas.extend(nof_replicas);
        }
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> =
            replicas.iter().map(replica_to_proto).collect();

        let now = SystemTime::now();
        self.state.objects.insert(
            key.clone(),
            ObjectEntry {
                replicas,
                size: req.slice_length,
                last_access: now,
                soft_pinned: config.with_soft_pin,
                hard_pinned: config.with_hard_pin,
                data_type: config.data_type,
                client_id,
                put_start_time: Some(now),
                lease_timeout: None,
                soft_pin_timeout: None,
            },
        );
        // 将 key 加入 processing_keys，防止并发 PutStart 冲突
        // Add key to processing_keys to prevent concurrent PutStart conflicts
        self.state.processing_keys.insert(key, ());

        metrics::PUT_START_REQUESTS.inc();
        Ok(Response::new(proto::PutStartResponse {
            replicas: proto_replicas,
        }))
    }

    // ---- PutEnd ----
    // 对象写入的第二阶段：将 Allocating 状态的副本标记为 Complete，更新 lease 和软 pin 超时。
    // 校验 client_id 防止越权写入。仅当所有副本都 Complete 时才从 processing_keys 移除，
    // 以避免并发 PutStart 冲突。同时触发 offload 队列攒批和 oplog 记录。
    //
    // Object write phase 2: mark Allocating replicas as Complete, update lease and soft-pin timeout.
    // Validates client_id to prevent unauthorized writes. Only removes from processing_keys
    // when ALL replicas are Complete to avoid concurrent PutStart conflicts.
    // Also triggers offload queue batching and oplog recording.
    pub(super) async fn put_end_impl(
        &self,
        request: Request<proto::PutEndRequest>,
    ) -> Result<Response<proto::PutEndResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            // C++ master_service.cpp:1368-1372 校验调用者身份，防止其他 client 越权 PutEnd
            // Validate caller identity to prevent unauthorized PutEnd from other clients
            if entry.client_id != client_id {
                return Err(Status::permission_denied("illegal client"));
            }
            // C++ master_service.cpp:1368-1372 校验 handle 有效性，若 handle 已失效则保留 Allocating 状态
            // C++ checks !replica.has_invalid_mem_handle() and !replica.has_invalid_nof_handle()
            // before marking replicas Complete. If handle became invalid, replica stays in Allocating.
            for r in &mut entry.replicas {
                let matches_type = match req.replica_type {
                    x if x == proto::replica_descriptor::ReplicaType::All as i32 => true,
                    x if x == proto::replica_descriptor::ReplicaType::Memory as i32 => {
                        r.replica_type == ReplicaType::Memory
                    }
                    x if x == proto::replica_descriptor::ReplicaType::NofSsd as i32 => {
                        r.replica_type == ReplicaType::NoFSsd
                    }
                    _ => r.replica_type == ReplicaType::Memory,
                };
                if matches_type && r.status == ReplicaStatus::Allocating {
                    // C++ master_service.cpp:1368-1372 检查 !replica.has_invalid_mem_handle()
                    // 和 !replica.has_invalid_nof_handle()。handle 失效时 replica 保持在 Allocating 状态。
                    // If handle is still valid, mark as Complete; otherwise stays Allocating.
                    if r.handle_valid {
                        r.status = ReplicaStatus::Complete;
                    }
                }
            }
            // 更新 lease 超时：多次 PutEnd 取最晚超时 / Update lease timeout: latest wins across multiple PutEnds
            let now = SystemTime::now();
            let new_lease = now
                .checked_add(self.state.runtime_config.lease_ttl)
                .unwrap_or(now);
            entry.lease_timeout = Some(match entry.lease_timeout {
                Some(current) if current > new_lease => current,
                _ => new_lease,
            });
            // 更新 soft_pin 超时 / Update soft_pin timeout
            if entry.soft_pinned {
                let new_soft_pin = now
                    .checked_add(self.state.runtime_config.soft_pin_ttl)
                    .unwrap_or(now);
                entry.soft_pin_timeout = Some(match entry.soft_pin_timeout {
                    Some(current) if current > new_soft_pin => current,
                    _ => new_soft_pin,
                });
            }
            let all_complete = entry
                .replicas
                .iter()
                .all(|r| r.status == ReplicaStatus::Complete);
            let size = entry.size;
            let offload_enabled = !self.state.runtime_config.offload_on_evict;
            drop(entry);
            if offload_enabled {
                push_offloading_queue(&self.state, client_id, &req.key, size);
            }
            // C++ 只在所有 replica 都 Complete 时才从 processing_keys 中移除，防止并发冲突
            // C++ only removes from processing_keys when ALL replicas are complete
            // AND the object is in the processing set.
            if all_complete && self.state.processing_keys.contains_key(&req.key) {
                self.state.processing_keys.remove(&req.key);
            }
            // Maintain per-client object index for fast client cleanup.
            // 维护每个客户端的对象索引，用于快速客户端清理
            if all_complete {
                self.state
                    .client_objects
                    .entry(client_id)
                    .or_default()
                    .insert(req.key.clone());
            }
            self.oplog_manager.lock().record_put_end(&req.key, size);
        }
        metrics::PUT_END_REQUESTS.inc();
        Ok(Response::new(proto::PutEndResponse {}))
    }

    // ---- AddReplica ----
    // 向已有对象追加副本。LocalDisk 类型副本按 holder_client_id 去重（同一客户端只保留最新），
    // 其他类型直接追加。若对象不存在，仅对 LocalDisk 类型自动创建对象条目。
    //
    // Append a replica to an existing object. LocalDisk replicas are deduplicated by holder_client_id
    // (only the latest per client is kept); other types are appended directly.
    // If the object does not exist, creates an entry only for LocalDisk type.
    pub(super) async fn add_replica_impl(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let replica = req
            .replica
            .as_ref()
            .map(replica_from_proto)
            .ok_or(Status::invalid_argument("missing replica"))?;
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            if replica.replica_type == ReplicaType::LocalDisk {
                if let Some(existing) = entry.replicas.iter_mut().find(|existing| {
                    existing.replica_type == ReplicaType::LocalDisk
                        && existing.holder_client_id == replica.holder_client_id
                }) {
                    *existing = replica;
                } else {
                    entry.replicas.push(replica);
                }
            } else {
                entry.replicas.push(replica);
            }
        } else if replica.replica_type == ReplicaType::LocalDisk {
            self.state.objects.insert(
                req.key.clone(),
                ObjectEntry {
                    size: replica.size,
                    replicas: vec![replica],
                    last_access: SystemTime::now(),
                    soft_pinned: false,
                    hard_pinned: false,
                    data_type: ObjectDataType::Unknown,
                    client_id,
                    put_start_time: None,
                    lease_timeout: None,
                    soft_pin_timeout: None,
                },
            );
        }
        Ok(Response::new(proto::AddReplicaResponse {}))
    }

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

        // Phase 1: read-only (uses get() — shared lock, allows concurrent reads).
        // 阶段 1：只读（使用 get() — 共享锁，允许并发读）
        let (completed_replicas, promotion_eligible) = match self.state.objects.get(&req.key) {
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
                let replicas: Vec<_> = entry
                    .replicas
                    .iter()
                    .filter(|r| r.status == ReplicaStatus::Complete)
                    .map(replica_to_proto)
                    .collect();
                if replicas.is_empty() {
                    return Err(Status::failed_precondition("replica is not ready"));
                }
                (replicas, eligible)
            }
            None => return Err(Status::not_found(format!("key not found: {}", req.key))),
        };

        // Phase 2: brief write lock for timestamp updates only (microseconds).
        // 阶段 2：短暂写锁仅更新时间戳（微秒级）
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            let now = SystemTime::now();
            entry.last_access = now;
            let new_lease = now
                .checked_add(self.state.runtime_config.lease_ttl)
                .unwrap_or(now);
            entry.lease_timeout = Some(match entry.lease_timeout {
                Some(current) if current > new_lease => current,
                _ => new_lease,
            });
            if entry.soft_pinned {
                let new_soft_pin = now
                    .checked_add(self.state.runtime_config.soft_pin_ttl)
                    .unwrap_or(now);
                entry.soft_pin_timeout = Some(match entry.soft_pin_timeout {
                    Some(current) if current > new_soft_pin => current,
                    _ => new_soft_pin,
                });
            }
        }

        // Phase 3: promotion after all locks released.
        // 阶段 3：锁释放后进行 promotion 条件检查和入队
        if promotion_eligible {
            try_push_promotion_queue(&self.state, &req.key);
        }
        metrics::GET_REQUESTS.inc();
        let lease_ttl_ms = self.state.runtime_config.lease_ttl.as_millis() as u64;
        Ok(Response::new(proto::GetReplicaListResponse {
            replicas: completed_replicas,
            lease_ttl_ms,
        }))
    }

    // ---- GetReplicaListByRegex ----
    // 按正则表达式批量获取对象的 Complete 副本列表。过滤掉无 Complete 副本的匹配 key。
    // Batch fetch Complete replica lists by regex; filters out matching keys with no Complete replicas.
    pub(super) async fn get_replica_list_by_regex_impl(
        &self,
        request: Request<proto::GetReplicaListByRegexRequest>,
    ) -> Result<Response<proto::GetReplicaListByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.key_regex)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        // 遍历所有 key，按正则匹配后返回 COMPLETED 状态的 replica 列表
        // Iterate all keys, match by regex, return only COMPLETED replicas
        for entry in self.state.objects.iter() {
            if pattern.is_match(entry.key()) {
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
                        "key={} matched by regex, but has no complete replicas.",
                        entry.key()
                    );
                    continue;
                }

                entries.push(proto::get_replica_list_by_regex_response::ObjectEntry {
                    key: entry.key().clone(),
                    replicas: completed_replicas,
                });
            }
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
        if !req.force && self.state.replication_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition(
                "object has an ongoing replication task",
            ));
        }
        if !req.force {
            if let Some(entry) = self.state.objects.get(&req.key) {
                // C++ master_service.cpp:2314 只有 lease 过期或 force=true 才允许删除
                // Only allow delete when lease is expired or force=true
                if !is_lease_expired(&entry) {
                    return Err(Status::failed_precondition("object has lease"));
                }
                // C++ 只有所有 replica 都 Complete 才允许删除
                // Only allow delete when all replicas are Complete
                if !entry
                    .replicas
                    .iter()
                    .all(|r| r.status == ReplicaStatus::Complete)
                {
                    return Err(Status::failed_precondition("replica is not ready"));
                }
            }
        }
        if let Some((_, object)) = self.state.objects.remove(&req.key) {
            for mut entry in self.state.client_objects.iter_mut() {
                entry.value_mut().remove(&req.key);
            }
            clear_offloading_task(&self.state, &req.key);
            clear_promotion_task(&self.state, &req.key);
            release_replicas(&self.state, &object.replicas);
            self.oplog_manager.lock().record_remove(&req.key);
        }
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
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut removed = 0i64;
        let keys_to_remove: Vec<String> = self
            .state
            .objects
            .iter()
            .filter(|entry| pattern.is_match(entry.key()))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            if !req.force && self.state.replication_tasks.contains_key(&key) {
                continue;
            }
            if !req.force {
                if let Some(entry) = self.state.objects.get(&key) {
                    if !is_lease_expired(&entry) {
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
            }
            if let Some((_, object)) = self.state.objects.remove(&key) {
                for mut entry in self.state.client_objects.iter_mut() {
                    entry.value_mut().remove(&key);
                }
                clear_offloading_task(&self.state, &key);
                clear_promotion_task(&self.state, &key);
                release_replicas(&self.state, &object.replicas);
                removed += 1;
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
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        for entry in self.state.objects.iter() {
            if pattern.is_match(entry.key()) {
                let r = entry.replicas.iter().map(replica_to_proto).collect();
                entries.push(proto::query_by_regex_response::Entry {
                    key: entry.key().clone(),
                    replicas: r,
                });
            }
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

    // ---- Upsert ----
    // 原子 Upsert：若 key 已存在则检查 refcnt（busy 检查）并复用/替换副本，否则新建。
    // 与 PutStart 相比不写入 processing_keys，适合管理端直接注入对象。
    // 已有的 soft_pin/hard_pin 状态会保留（OR 语义），防止意外丢失 pin 保护。
    //
    // Atomic Upsert: if key exists, check refcnt (busy check) and reuse/replace replicas; otherwise create new.
    // Unlike PutStart, does not write to processing_keys — suitable for admin-side object injection.
    // Existing soft_pin/hard_pin state is retained (OR semantics) to prevent accidental loss of pin protection.
    pub(super) async fn upsert_impl(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        if config.replica_num == 0 && config.nof_replica_num == 0 {
            return Err(Status::invalid_argument(
                "replica_num and nof_replica_num cannot both be zero",
            ));
        }
        let replica_count = config.replica_num.max(1) as usize;

        // C++ 检查是否有进行中的 replication/offloading 任务，以及 replica 是否 busy
        // C++ checks if replication_tasks or offloading_tasks exist for the key.
        if self.state.replication_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition("object has replication task"));
        }
        if self.state.offloading_tasks.contains_key(&req.key) {
            return Err(Status::failed_precondition("object has offloading task"));
        }

        let (replicas, previous_soft_pinned, previous_hard_pinned) =
            if let Some(mut existing) = self.state.objects.get_mut(&req.key) {
                // C++ 先调用 CleanupStaleHandles 清理无效副本
                // First call CleanupStaleHandles to clean invalid replicas
                let alive_clients = get_alive_clients_snapshot(&self.state);
                let _ = cleanup_stale_handles(&mut existing, &alive_clients);

                // C++ checks HasReplica(&Replica::fn_is_busy)
                if existing.replicas.iter().any(|r| r.refcnt > 0) {
                    return Err(Status::failed_precondition("object replica busy"));
                }
                if existing.size == req.slice_length {
                    // 大小匹配，复用现有副本 / Size matches; reuse existing replicas
                    (
                        existing.replicas.clone(),
                        existing.soft_pinned,
                        existing.hard_pinned,
                    )
                } else {
                    // 大小不匹配，释放旧副本后重新分配 / Size mismatch; release old and reallocate
                    let previous_soft_pinned = existing.soft_pinned;
                    let previous_hard_pinned = existing.hard_pinned;
                    let old_replicas = existing.replicas.clone();
                    drop(existing);
                    release_replicas(&self.state, &old_replicas);
                    let mut allocator = self.state.allocator.write();
                    (
                        allocator.allocate_for_client(
                            &req.key,
                            Some(client_id),
                            req.slice_length,
                            replica_count,
                            &config,
                        ),
                        previous_soft_pinned,
                        previous_hard_pinned,
                    )
                }
            } else {
                let mut allocator = self.state.allocator.write();
                (
                    allocator.allocate_for_client(
                        &req.key,
                        Some(client_id),
                        req.slice_length,
                        replica_count,
                        &config,
                    ),
                    false,
                    false,
                )
            };
        let mut replicas = replicas;
        if config.nof_replica_num > 0 {
            let preferred_nof = if config.prefer_alloc_in_same_node {
                preferred_nof_segment_names(&self.state, &replicas)
            } else {
                Vec::new()
            };
            if config.prefer_alloc_in_same_node && preferred_nof.is_empty() {
                release_replicas(&self.state, &replicas);
                return Err(Status::invalid_argument(
                    "prefer_alloc_in_same_node requires matching NoF segment",
                ));
            }
            let nof_replicas = allocate_nof_replicas(
                &self.state,
                &req.key,
                req.slice_length,
                config.nof_replica_num as usize,
                &preferred_nof,
            )?;
            replicas.extend(nof_replicas);
        }
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> =
            replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(
            req.key.clone(),
            ObjectEntry {
                replicas,
                size: req.slice_length,
                last_access: SystemTime::now(),
                soft_pinned: config.with_soft_pin || previous_soft_pinned,
                hard_pinned: config.with_hard_pin || previous_hard_pinned,
                data_type: config.data_type,
                client_id,
                put_start_time: Some(SystemTime::now()),
                lease_timeout: None,
                soft_pin_timeout: None,
            },
        );

        Ok(Response::new(proto::UpsertResponse {
            replicas: proto_replicas,
        }))
    }
}

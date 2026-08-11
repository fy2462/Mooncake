use super::super::*;

fn rebind_runtime_replicas(
    state: &MasterState,
    segment: &mooncake_store_core::Segment,
    replica_type: ReplicaType,
) {
    for mut object in state.objects.iter_mut() {
        for replica in &mut object.replicas {
            if replica.replica_type == replica_type && replica.segment_id == segment.id {
                replica.segment_name.clone_from(&segment.name);
                replica.base_addr = if segment.protocol == "cxl" {
                    0
                } else {
                    segment.base
                };
                replica.protocol.clone_from(&segment.protocol);
                replica.handle_valid = true;
            }
        }
    }
    for mut task in state.replication_tasks.iter_mut() {
        if task.source.replica_type == replica_type && task.source.segment_id == segment.id {
            task.source.segment_name.clone_from(&segment.name);
            task.source.base_addr = if segment.protocol == "cxl" {
                0
            } else {
                segment.base
            };
            task.source.protocol.clone_from(&segment.protocol);
            task.source.handle_valid = true;
        }
        for target in &mut task.targets {
            if target.replica_type == replica_type && target.segment_id == segment.id {
                target.segment_name.clone_from(&segment.name);
                target.base_addr = if segment.protocol == "cxl" {
                    0
                } else {
                    segment.base
                };
                target.protocol.clone_from(&segment.protocol);
                target.handle_valid = true;
            }
        }
        if let Some(target) = &mut task.existing_move_target
            && target.replica_type == replica_type
            && target.segment_id == segment.id
        {
            target.segment_name.clone_from(&segment.name);
            target.base_addr = if segment.protocol == "cxl" {
                0
            } else {
                segment.base
            };
            target.protocol.clone_from(&segment.protocol);
            target.handle_valid = true;
        }
    }
}

impl MasterServiceImpl {
    // ---- Ping ----
    // 客户端心跳：更新客户端地址、last_ping 时间戳，注册 metadata segments。
    // 新客户端返回 NeedRemount 状态码触发客户端重新挂载；已有客户端仅更新 view_version。
    //
    // Client heartbeat: update client addresses, last_ping timestamp, register metadata segments.
    // New clients get NeedRemount status to trigger remount; existing clients only get updated view_version.
    pub(crate) async fn ping_impl(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let remounted = self.state.ok_clients.contains_key(&client_id);
        // 从 segment 名称中提取 host 地址 / Derive addresses from segment names
        let derived_addresses = req
            .mounted_segments
            .iter()
            .map(|segment| host_from_segment_name(segment))
            .collect::<Vec<_>>();
        upsert_client_addresses(&self.state, client_id, derived_addresses);

        if let Some(mut entry) = self.state.clients.get_mut(&client_id) {
            entry.last_ping = SystemTime::now();
            entry.info.last_seen = Utc::now();
        }
        register_metadata_segments(&self.metadata_state, &req.mounted_segments).await;
        metrics::PING_REQUESTS.inc();
        let view_version_id = self
            .state
            .view_version
            .load(std::sync::atomic::Ordering::Relaxed);
        let client_status = if remounted {
            proto::ClientStatus::Ok as i32
        } else {
            proto::ClientStatus::NeedRemount as i32
        };
        Ok(Response::new(proto::PingResponse {
            view_version_id,
            client_status,
        }))
    }

    // ---- MountSegment ----
    // 客户端挂载 Memory segment：注册到 segments 表和 allocator，同步 client-segments 索引。
    // 触发 view_version 递增通知其他客户端拓扑变更，记录 oplog 用于热备同步。
    //
    // Mount Memory segment: register in segments table and allocator, sync client-segments index.
    // Triggers view_version bump to notify other clients of topology change; records oplog for hot standby sync.
    pub(crate) async fn mount_segment_impl(
        &self,
        request: Request<proto::MountSegmentRequest>,
    ) -> Result<Response<proto::MountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        if req.segment_name.is_empty() {
            return Err(Status::invalid_argument("segment_name must not be empty"));
        }
        let segment_id = stable_memory_segment_id(
            client_id,
            &req.segment_name,
            req.base_addr,
            req.size,
            &req.te_endpoint,
            &req.protocol,
            &req.host_id,
        );
        let host = if req.host_id.is_empty() {
            host_from_segment_name(&req.segment_name)
        } else {
            req.host_id.clone()
        };
        let is_cxl = req.protocol == "cxl";
        if is_cxl && !self.state.runtime_config.enable_cxl {
            return Err(Status::unavailable("CXL is not enabled"));
        }
        if is_cxl && req.size != self.state.runtime_config.cxl_size {
            return Err(Status::invalid_argument(format!(
                "CXL segment size {} must match configured CXL size {}",
                req.size, self.state.runtime_config.cxl_size
            )));
        }
        if (!is_cxl && req.base_addr == 0) || req.size == 0 {
            return Err(Status::invalid_argument(
                "non-CXL base_addr and all segment sizes must be non-zero",
            ));
        }
        if self.state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
            && req.size > CACHELIB_MAX_SEGMENT_SIZE
        {
            return Err(Status::invalid_argument(format!(
                "segment size exceeds Cachelib u32 slab index capacity {CACHELIB_MAX_SEGMENT_SIZE}"
            )));
        }
        if !is_cxl
            && self.state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
            && req.size % CACHELIB_SLAB_SIZE != 0
        {
            return Err(Status::invalid_argument(format!(
                "size must be aligned to {CACHELIB_SLAB_SIZE} for cachelib"
            )));
        }

        let segment = mooncake_store_core::Segment {
            id: segment_id,
            name: req.segment_name.clone(),
            base: req.base_addr,
            size: req.size,
            te_endpoint: req.te_endpoint.clone(),
            protocol: req.protocol.clone(),
            host_id: req.host_id.clone(),
        };

        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        if self.state.nof_segments.contains_key(&segment_id) {
            return Err(Status::failed_precondition(
                "derived Memory segment UUID collides with an existing NoF segment",
            ));
        }
        if self
            .state
            .nof_allocator
            .read()
            .used_bytes(&segment_id)
            .is_some()
        {
            self.state.fence_after_invariant_failure(
                "mount_segment",
                "Memory segment UUID exists only in the NoF allocator",
            );
            return Err(Status::unavailable(
                "cross-allocator identity collision for Memory segment UUID",
            ));
        }
        if let Some(existing) = self.state.segments.get(&segment_id) {
            let durable_identity_matches = existing.client_id == client_id
                && existing.segment.name == req.segment_name
                && existing.segment.size == req.size
                && existing.segment.host_id == req.host_id;
            let status = existing.status;
            drop(existing);
            if !durable_identity_matches || status != proto::SegmentStatus::Active {
                return Err(Status::failed_precondition(
                    "Memory segment UUID is already mounted with conflicting identity or status",
                ));
            }
            if self
                .state
                .allocator
                .read()
                .used_bytes(&segment_id)
                .is_none()
            {
                self.state.fence_after_invariant_failure(
                    "mount_segment",
                    "Memory topology entry has no matching allocator segment",
                );
                return Err(Status::unavailable(
                    "allocator/topology mismatch for Memory segment UUID",
                ));
            }
            self.state
                .allocator
                .read()
                .validate_segment_rebind(&segment)
                .map_err(Status::failed_precondition)?;
            self.state
                .allocator
                .write()
                .rebind_segment(segment.clone(), client_id)
                .map_err(Status::failed_precondition)?;
            self.state
                .segments
                .get_mut(&segment_id)
                .expect("validated Memory segment disappeared inside mutation epoch")
                .segment = segment.clone();
            rebind_runtime_replicas(&self.state, &segment, ReplicaType::Memory);
            sync_client_segments(&self.state, client_id);
            drop(_global_mutation_guard);
            upsert_client_addresses(&self.state, client_id, vec![host]);
            register_metadata_segments(
                &self.metadata_state,
                std::slice::from_ref(&req.segment_name),
            )
            .await;
            return Ok(Response::new(proto::MountSegmentResponse {
                segment_id: Some(uuid_to_proto(segment_id)),
            }));
        }
        let matching_runtime_identity = self
            .state
            .segments
            .iter()
            .filter(|entry| {
                entry.client_id == client_id
                    && entry.segment.base == req.base_addr
                    && entry.segment.size == req.size
            })
            .map(|entry| {
                (
                    entry.segment.id,
                    entry.segment.name == req.segment_name
                        && entry.segment.te_endpoint == req.te_endpoint
                        && entry.segment.protocol == req.protocol
                        && entry.segment.host_id == req.host_id,
                    entry.status,
                )
            })
            .collect::<Vec<_>>();
        match matching_runtime_identity.as_slice() {
            [(existing_id, true, proto::SegmentStatus::Active)] => {
                return Ok(Response::new(proto::MountSegmentResponse {
                    segment_id: Some(uuid_to_proto(*existing_id)),
                }));
            }
            [] => {}
            [(existing_id, _, status)] => {
                return Err(Status::failed_precondition(format!(
                    "Memory segment runtime range already belongs to segment {existing_id} \
                     with status {status:?} or conflicting identity"
                )));
            }
            _ => {
                self.state.fence_after_invariant_failure(
                    "mount_segment",
                    "multiple Memory segments use the same client/base/size runtime identity",
                );
                return Err(Status::unavailable(
                    "ambiguous Memory segment runtime identity",
                ));
            }
        }
        let allocator_only_identity = self
            .state
            .allocator
            .read()
            .used_bytes(&segment_id)
            .is_some();
        if allocator_only_identity {
            self.state.fence_after_invariant_failure(
                "mount_segment",
                "segment UUID exists in an allocator without matching topology",
            );
            return Err(Status::unavailable(
                "allocator/topology mismatch for Memory segment UUID",
            ));
        }
        if let Err(error) = self.oplog_manager.record_mount_segment_durable(
            &req.segment_name,
            segment_id,
            req.base_addr,
            req.size,
            &req.te_endpoint,
            &req.protocol,
            &req.host_id,
            client_id,
        ) {
            self.state
                .fence_after_durability_failure("mount_segment", &error);
            return Err(Status::unavailable(
                "failed to persist segment mount; segment was not published locally",
            ));
        }
        self.state.segments.insert(
            segment_id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: proto::SegmentStatus::Active,
            },
        );
        sync_client_segments(&self.state, client_id);

        {
            let mut allocator = self.state.allocator.write();
            allocator.add_segment(segment, 0, client_id);
        }

        bump_view_version(&self.state);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        drop(_global_mutation_guard);
        upsert_client_addresses(&self.state, client_id, vec![host]);
        register_metadata_segments(
            &self.metadata_state,
            std::slice::from_ref(&req.segment_name),
        )
        .await;
        Ok(Response::new(proto::MountSegmentResponse {
            segment_id: Some(uuid_to_proto(segment_id)),
        }))
    }

    // ---- MountNoFSegment ----
    // 客户端挂载 NoF (NVMe-oF) segment：注册到 nof_segments 表和 nof_allocator。
    // Mount NoF segment: register in nof_segments table and nof_allocator.
    pub(crate) async fn mount_nof_segment_impl(
        &self,
        request: Request<proto::MountNoFSegmentRequest>,
    ) -> Result<Response<proto::MountNoFSegmentResponse>, Status> {
        if !self.state.runtime_config.enable_nof {
            return Err(Status::unavailable("NoF is not enabled"));
        }
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        let mut segment = req
            .segment
            .as_ref()
            .map(nof_segment_from_proto)
            .ok_or(Status::invalid_argument("missing segment"))?;
        segment.client_id = client_id;
        if segment.id.is_nil()
            || segment.name.is_empty()
            || segment.size == 0
            || segment.te_endpoint.is_empty()
        {
            return Err(Status::invalid_argument(
                "NoF segment requires non-nil id and non-empty name/size/te_endpoint",
            ));
        }
        if self.state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
            && (segment.base % CACHELIB_SLAB_SIZE != 0 || segment.size % CACHELIB_SLAB_SIZE != 0)
        {
            return Err(Status::invalid_argument(format!(
                "NoF base and size must be aligned to {CACHELIB_SLAB_SIZE} for cachelib"
            )));
        }
        if self.state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
            && segment.size > CACHELIB_MAX_SEGMENT_SIZE
        {
            return Err(Status::invalid_argument(format!(
                "NoF segment size exceeds Cachelib u32 slab index capacity {CACHELIB_MAX_SEGMENT_SIZE}"
            )));
        }

        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        if self.state.segments.contains_key(&segment.id) {
            return Err(Status::failed_precondition(
                "NoF segment UUID collides with an existing Memory segment",
            ));
        }
        if self
            .state
            .allocator
            .read()
            .used_bytes(&segment.id)
            .is_some()
        {
            self.state.fence_after_invariant_failure(
                "mount_nof_segment",
                "NoF segment UUID exists only in the Memory allocator",
            );
            return Err(Status::unavailable(
                "cross-allocator identity collision for NoF segment UUID",
            ));
        }
        if let Some(existing) = self.state.nof_segments.get(&segment.id) {
            let identity_matches = existing.segment.name == segment.name
                && existing.segment.size == segment.size
                && existing.segment.client_id == client_id;
            let runtime_identity_matches = (existing.segment.base == 0
                && existing.segment.te_endpoint.is_empty())
                || (existing.segment.base == segment.base
                    && existing.segment.te_endpoint == segment.te_endpoint);
            let status = existing.status;
            drop(existing);
            if !identity_matches
                || !runtime_identity_matches
                || status != proto::SegmentStatus::Active
            {
                return Err(Status::failed_precondition(
                    "NoF segment UUID is already mounted with conflicting identity or status",
                ));
            }
            if self
                .state
                .nof_allocator
                .read()
                .used_bytes(&segment.id)
                .is_none()
            {
                self.state.fence_after_invariant_failure(
                    "mount_nof_segment",
                    "NoF topology entry has no matching allocator segment",
                );
                return Err(Status::unavailable(
                    "allocator/topology mismatch for NoF segment UUID",
                ));
            }
            let allocator_segment = mooncake_store_core::Segment {
                id: segment.id,
                name: segment.name.clone(),
                base: segment.base,
                size: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: String::new(),
                host_id: String::new(),
            };
            self.state
                .nof_allocator
                .read()
                .validate_segment_rebind(&allocator_segment)
                .map_err(Status::failed_precondition)?;
            self.state
                .nof_allocator
                .write()
                .rebind_segment(allocator_segment.clone(), client_id)
                .map_err(Status::failed_precondition)?;
            self.state
                .nof_segments
                .get_mut(&segment.id)
                .expect("validated NoF segment disappeared inside mutation epoch")
                .segment = segment.clone();
            rebind_runtime_replicas(&self.state, &allocator_segment, ReplicaType::NoFSsd);
            return Ok(Response::new(proto::MountNoFSegmentResponse {}));
        }
        if self.state.nof_segments.iter().any(|existing| {
            existing.status == proto::SegmentStatus::Active
                && existing.segment.te_endpoint == segment.te_endpoint
        }) {
            return Err(Status::already_exists(
                "NoF endpoint is already mounted under a different segment UUID",
            ));
        }
        let allocator_only_identity = self
            .state
            .nof_allocator
            .read()
            .used_bytes(&segment.id)
            .is_some();
        if allocator_only_identity {
            self.state.fence_after_invariant_failure(
                "mount_nof_segment",
                "segment UUID exists in an allocator without matching topology",
            );
            return Err(Status::unavailable(
                "allocator/topology mismatch for NoF segment UUID",
            ));
        }
        if let Err(error) = self.oplog_manager.record_mount_nof_segment_durable(
            &segment.name,
            segment.id,
            segment.base,
            segment.size,
            &segment.te_endpoint,
            client_id,
        ) {
            self.state
                .fence_after_durability_failure("mount_nof_segment", &error);
            return Err(Status::unavailable(
                "failed to persist NoF segment mount; segment was not published locally",
            ));
        }
        self.state.nof_segments.insert(
            segment.id,
            NoFSegmentEntry {
                segment: segment.clone(),
                used: 0,
                status: proto::SegmentStatus::Active,
            },
        );
        self.state.nof_allocator.write().add_segment(
            mooncake_store_core::Segment {
                id: segment.id,
                name: segment.name.clone(),
                base: segment.base,
                size: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: String::new(),
                host_id: String::new(),
            },
            0,
            client_id,
        );
        bump_view_version(&self.state);
        Ok(Response::new(proto::MountNoFSegmentResponse {}))
    }

    // ---- UnmountSegment ----
    // 客户端卸载 Memory segment，校验所有权后从 segments 表和 allocator 移除。
    // Unmount Memory segment: verify ownership, then remove from segments table and allocator.
    pub(crate) async fn unmount_segment_impl(
        &self,
        request: Request<proto::UnmountSegmentRequest>,
    ) -> Result<Response<proto::UnmountSegmentResponse>, Status> {
        let req = request.into_inner();
        let segment_id = uuid_from_proto(
            req.segment_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing segment_id"))?,
        );
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }

        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let Some(segment) = self.state.segments.get(&segment_id) else {
            return Ok(Response::new(proto::UnmountSegmentResponse {}));
        };
        if segment.client_id != client_id {
            return Err(Status::not_found("segment not found for client"));
        }
        drop(segment);
        if !unmount_segment_owned_durable_locked(
            &self.state,
            segment_id,
            client_id,
            "unmount_segment",
        )? {
            return Err(Status::not_found("segment not found for client"));
        }
        if self.is_service_fenced() {
            return Err(Status::unavailable(
                "unmounted segment while master durability was fenced",
            ));
        }
        Ok(Response::new(proto::UnmountSegmentResponse {}))
    }

    // ---- UnmountNoFSegment ----
    // 客户端卸载 NoF segment，同时从 nof_segments 表和 nof_allocator 移除。
    // Unmount NoF segment: remove from nof_segments table and nof_allocator.
    pub(crate) async fn unmount_nof_segment_impl(
        &self,
        request: Request<proto::UnmountNoFSegmentRequest>,
    ) -> Result<Response<proto::UnmountNoFSegmentResponse>, Status> {
        if !self.state.runtime_config.enable_nof {
            return Err(Status::unavailable("NoF is not enabled"));
        }
        let req = request.into_inner();
        let segment_id = uuid_from_proto(
            req.segment_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing segment_id"))?,
        );
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let Some(segment) = self.state.nof_segments.get(&segment_id) else {
            return Ok(Response::new(proto::UnmountNoFSegmentResponse {}));
        };
        if segment.segment.client_id != client_id {
            return Err(Status::not_found("NoF segment not found for client"));
        }
        drop(segment);
        if !unmount_nof_segment_owned_durable_locked(
            &self.state,
            segment_id,
            client_id,
            "unmount_nof_segment",
        )? {
            return Err(Status::not_found("NoF segment not found for client"));
        }
        if self.is_service_fenced() {
            return Err(Status::unavailable(
                "unmounted NoF segment while master durability was fenced",
            ));
        }
        Ok(Response::new(proto::UnmountNoFSegmentResponse {}))
    }

    // ---- GracefulUnmountSegment ----
    // 优雅卸载：将 segment 加入调度器，在 grace_period_ms 内等待数据迁移后再真正移除。
    // Graceful unmount: schedule segment in the scheduler, wait for grace_period_ms for data migration before actual removal.
    pub(crate) async fn graceful_unmount_segment_impl(
        &self,
        request: Request<proto::GracefulUnmountSegmentRequest>,
    ) -> Result<Response<proto::GracefulUnmountSegmentResponse>, Status> {
        let req = request.into_inner();
        let segment_id = uuid_from_proto(
            req.segment_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing segment_id"))?,
        );
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let now_epoch_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock is before Unix epoch"))
            .and_then(|duration| {
                u64::try_from(duration.as_millis())
                    .map_err(|_| Status::invalid_argument("graceful unmount deadline overflow"))
            })?;
        let requested_deadline_epoch_ms =
            now_epoch_ms
                .checked_add(req.grace_period_ms)
                .ok_or(Status::invalid_argument(
                    "graceful unmount deadline overflow",
                ))?;
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let mut segment = self
            .state
            .segments
            .get_mut(&segment_id)
            .ok_or(Status::not_found("segment not found for client"))?;
        if segment.client_id != client_id {
            return Err(Status::not_found("segment not found for client"));
        }
        let segment_name = segment.segment.name.clone();
        let previous_status = segment.status;
        match previous_status {
            proto::SegmentStatus::Active => {}
            proto::SegmentStatus::GracefullyUnmounting => {}
            _ => {
                return Err(Status::failed_precondition(
                    "segment is not active and cannot begin graceful unmount",
                ));
            }
        }
        let previous_intent = self
            .state
            .graceful_unmounts
            .get(&segment_id)
            .map(|entry| entry.value().clone());
        if previous_intent
            .as_ref()
            .is_some_and(|intent| intent.client_id != client_id)
        {
            return Err(Status::internal(
                "graceful unmount owner does not match segment owner",
            ));
        }
        if previous_status == proto::SegmentStatus::Active && previous_intent.is_some() {
            return Err(Status::internal(
                "active segment has an inconsistent graceful unmount intent",
            ));
        }

        let deadline_epoch_ms = previous_intent
            .as_ref()
            .map(|entry| entry.deadline_epoch_ms.min(requested_deadline_epoch_ms))
            .unwrap_or(requested_deadline_epoch_ms);
        let intent_changed = previous_intent
            .as_ref()
            .map_or(true, |entry| deadline_epoch_ms < entry.deadline_epoch_ms);

        if intent_changed {
            // Publish the intent to durable HA state before making it visible
            // in the leader's in-memory maps. A failed/ambiguous append cannot
            // be acknowledged as a successful graceful unmount.
            let persist_result = self.oplog_manager.record_graceful_unmount_segment(
                &segment_name,
                segment_id,
                client_id,
                deadline_epoch_ms,
            );
            if let Err(error) = persist_result {
                tracing::error!(
                    %error,
                    %segment_id,
                    "failed to durably record graceful unmount intent; fencing master"
                );
                drop(segment);
                self.state
                    .fence_after_durability_failure("graceful_unmount_intent", &error);
                drop(_global_mutation_guard);
                self.graceful_unmount_scheduler
                    .sync_from_state(&self.state, false);
                return Err(Status::unavailable(
                    "failed to durably record graceful unmount intent",
                ));
            }

            if previous_status == proto::SegmentStatus::Active {
                segment.status = proto::SegmentStatus::GracefullyUnmounting;
                bump_view_version(&self.state);
            }
            self.state.graceful_unmounts.insert(
                segment_id,
                GracefulUnmountSnapshotEntry {
                    segment_id,
                    client_id,
                    deadline_epoch_ms,
                },
            );
        }
        drop(segment);
        drop(_global_mutation_guard);
        self.graceful_unmount_scheduler
            .schedule_at(segment_id, client_id, deadline_epoch_ms);
        Ok(Response::new(proto::GracefulUnmountSegmentResponse {}))
    }

    // ---- ReMountSegment ----
    // 重新挂载：客户端重启后批量注册已有 segment，已存在的跳过不重复创建。
    // Remount: batch register existing segments after client restart; skip already-existing ones.
    pub(crate) async fn re_mount_segment_impl(
        &self,
        request: Request<proto::ReMountSegmentRequest>,
    ) -> Result<Response<proto::ReMountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        if req.segment_names.len() != req.segment_sizes.len() {
            return Err(Status::invalid_argument(
                "segment_names and segment_sizes must have same length",
            ));
        }
        if !req.base_addrs.is_empty() && req.base_addrs.len() != req.segment_names.len() {
            return Err(Status::invalid_argument(
                "base_addrs must be empty or have same length as segment_names",
            ));
        }
        if !req.te_endpoints.is_empty() && req.te_endpoints.len() != req.segment_names.len() {
            return Err(Status::invalid_argument(
                "te_endpoints must be empty or have same length as segment_names",
            ));
        }
        if !req.protocols.is_empty() && req.protocols.len() != req.segment_names.len() {
            return Err(Status::invalid_argument(
                "protocols must be empty or have same length as segment_names",
            ));
        }
        if !req.segment_ids.is_empty() && req.segment_ids.len() != req.segment_names.len() {
            return Err(Status::invalid_argument(
                "segment_ids must be empty or have same length as segment_names",
            ));
        }
        if !req.host_ids.is_empty() && req.host_ids.len() != req.segment_names.len() {
            return Err(Status::invalid_argument(
                "host_ids must be empty or have same length as segment_names",
            ));
        }

        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let identity_aware = !req.segment_ids.is_empty();
        let mut seen_ids = HashSet::new();
        let mut seen_legacy_names = HashSet::new();
        let mut seen_runtime_ranges = HashSet::new();
        let mut remounts = Vec::with_capacity(req.segment_names.len());
        for (idx, (segment_name, size)) in req
            .segment_names
            .iter()
            .zip(req.segment_sizes.iter())
            .enumerate()
        {
            if !identity_aware && !seen_legacy_names.insert(segment_name.clone()) {
                return Err(Status::invalid_argument(format!(
                    "legacy remount without segment_ids cannot contain duplicate name: \
                     {segment_name:?}"
                )));
            }
            let requested_id = req.segment_ids.get(idx).map(uuid_from_proto);
            if requested_id.is_some_and(|segment_id| segment_id.is_nil()) {
                return Err(Status::invalid_argument("segment_ids must be non-nil"));
            }
            let base = req.base_addrs.get(idx).copied().unwrap_or(0);
            let te_endpoint = req.te_endpoints.get(idx).cloned().unwrap_or_default();
            let protocol = req.protocols.get(idx).cloned().unwrap_or_default();
            let is_cxl = protocol == "cxl";
            if is_cxl && !self.state.runtime_config.enable_cxl {
                return Err(Status::unavailable("CXL is not enabled"));
            }
            if is_cxl && *size != self.state.runtime_config.cxl_size {
                return Err(Status::invalid_argument(format!(
                    "CXL segment size {} must match configured CXL size {}",
                    size, self.state.runtime_config.cxl_size
                )));
            }
            if (!is_cxl && base == 0) || *size == 0 || te_endpoint.is_empty() || protocol.is_empty()
            {
                return Err(Status::invalid_argument(
                    "remounted Memory segment requires non-zero non-CXL base/size and current endpoint/protocol",
                ));
            }
            if !is_cxl
                && self.state.runtime_config.memory_allocator_kind
                    == MemoryAllocatorKind::CachelibLike
                && *size % CACHELIB_SLAB_SIZE != 0
            {
                return Err(Status::invalid_argument(format!(
                    "size must be aligned to {CACHELIB_SLAB_SIZE} for cachelib"
                )));
            }
            if self.state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
                && *size > CACHELIB_MAX_SEGMENT_SIZE
            {
                return Err(Status::invalid_argument(format!(
                    "remounted Memory segment size exceeds Cachelib u32 slab index capacity \
                     {CACHELIB_MAX_SEGMENT_SIZE}"
                )));
            }
            if !is_cxl && !seen_runtime_ranges.insert((base, *size)) {
                return Err(Status::invalid_argument(
                    "remounted Memory segments must use unique runtime ranges",
                ));
            }

            let restored_entry = if let Some(segment_id) = requested_id {
                self.state.segments.get(&segment_id).map(|entry| {
                    (
                        entry.segment.id,
                        entry.segment.name.clone(),
                        entry.client_id,
                        entry.segment.size,
                        entry.segment.host_id.clone(),
                    )
                })
            } else {
                let matches = self
                    .state
                    .segments
                    .iter()
                    .filter(|entry| entry.segment.name == *segment_name)
                    .map(|entry| {
                        (
                            entry.segment.id,
                            entry.segment.name.clone(),
                            entry.client_id,
                            entry.segment.size,
                            entry.segment.host_id.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                if matches.len() > 1 {
                    return Err(Status::failed_precondition(format!(
                        "multiple restored Memory segments use name {segment_name:?}; \
                         segment_ids are required"
                    )));
                }
                matches.into_iter().next()
            };
            let restored = restored_entry.is_some();
            if restored {
                let restored_segment_id = restored_entry
                    .as_ref()
                    .map(|(segment_id, _, _, _, _)| *segment_id)
                    .expect("restored entry always carries a segment id");
                if self
                    .state
                    .segments
                    .get(&restored_segment_id)
                    .is_some_and(|entry| {
                        entry.status == crate::proto::SegmentStatus::GracefullyUnmounting
                    })
                {
                    // C++ ReMountSegment during UNMOUNTING returns
                    // UNAVAILABLE_IN_CURRENT_STATUS; fail closed instead of
                    // resurrecting a segment that is being retired.
                    return Err(Status::unavailable("segment is being gracefully unmounted"));
                }
            }
            let (restored_segment_id, restored_host_id) = if let Some((
                segment_id,
                restored_name,
                owner,
                restored_size,
                restored_host_id,
            )) = restored_entry
            {
                if owner != client_id {
                    return Err(Status::permission_denied(format!(
                        "Memory segment {segment_id} belongs to another client session"
                    )));
                }
                if restored_name != *segment_name {
                    return Err(Status::failed_precondition(format!(
                        "Memory segment {segment_id} name changed from {restored_name:?} to \
                             {segment_name:?}"
                    )));
                }
                if restored_size != *size {
                    return Err(Status::failed_precondition(format!(
                        "Memory segment {segment_id} size changed from {restored_size} to {size}"
                    )));
                }
                (Some(segment_id), restored_host_id)
            } else {
                (None, String::new())
            };
            let host_id = req
                .host_ids
                .get(idx)
                .filter(|host| !host.is_empty())
                .cloned()
                .unwrap_or(restored_host_id);
            let segment_id = requested_id.or(restored_segment_id).unwrap_or_else(|| {
                stable_memory_segment_id(
                    client_id,
                    segment_name,
                    base,
                    *size,
                    &te_endpoint,
                    &protocol,
                    &host_id,
                )
            });
            if !seen_ids.insert(segment_id) {
                return Err(Status::invalid_argument(
                    "remounted Memory segment identities must be unique",
                ));
            }
            let cross_topology_collision = self.state.nof_segments.contains_key(&segment_id);
            if cross_topology_collision {
                if restored {
                    self.state.fence_after_invariant_failure(
                        "re_mount_segment",
                        "Memory and NoF topology contain the same segment UUID",
                    );
                    return Err(Status::unavailable(
                        "cross-topology identity collision for Memory segment UUID",
                    ));
                }
                return Err(Status::failed_precondition(
                    "Memory segment UUID collides with an existing NoF segment",
                ));
            }
            if self
                .state
                .nof_allocator
                .read()
                .used_bytes(&segment_id)
                .is_some()
            {
                self.state.fence_after_invariant_failure(
                    "re_mount_segment",
                    "Memory segment UUID exists in the NoF allocator",
                );
                return Err(Status::unavailable(
                    "cross-allocator identity collision for Memory segment UUID",
                ));
            }
            let allocator_present = self
                .state
                .allocator
                .read()
                .used_bytes(&segment_id)
                .is_some();
            if restored && !allocator_present {
                self.state.fence_after_invariant_failure(
                    "re_mount_segment",
                    "restored Memory topology has no matching allocator segment",
                );
                return Err(Status::unavailable(
                    "allocator/topology mismatch for Memory segment UUID",
                ));
            }
            if !restored && allocator_present {
                self.state.fence_after_invariant_failure(
                    "re_mount_segment",
                    "new Memory remount UUID already exists only in allocator",
                );
                return Err(Status::unavailable(
                    "allocator/topology mismatch for Memory segment UUID",
                ));
            }
            if !is_cxl
                && self.state.segments.iter().any(|entry| {
                    entry.segment.id != segment_id
                        && entry.client_id == client_id
                        && entry.segment.base == base
                        && entry.segment.size == *size
                })
            {
                if restored {
                    self.state.fence_after_invariant_failure(
                        "re_mount_segment",
                        "multiple Memory segments use the same client/base/size runtime identity",
                    );
                    return Err(Status::unavailable(
                        "ambiguous Memory segment runtime identity",
                    ));
                }
                return Err(Status::failed_precondition(
                    "Memory segment runtime range already belongs to another segment",
                ));
            }
            let segment = mooncake_store_core::Segment {
                id: segment_id,
                name: segment_name.clone(),
                base,
                size: *size,
                te_endpoint,
                protocol,
                host_id,
            };
            if restored {
                self.state
                    .allocator
                    .read()
                    .validate_segment_rebind(&segment)
                    .map_err(Status::failed_precondition)?;
            }
            remounts.push((segment, restored));
        }

        for (segment, restored) in &remounts {
            if *restored {
                self.state
                    .allocator
                    .write()
                    .rebind_segment(segment.clone(), client_id)
                    .map_err(Status::failed_precondition)?;
                let mut entry = self
                    .state
                    .segments
                    .get_mut(&segment.id)
                    .expect("restored Memory segment was preflighted");
                entry.segment = segment.clone();
                entry.client_id = client_id;
                drop(entry);
                rebind_runtime_replicas(&self.state, segment, ReplicaType::Memory);
            } else {
                if let Err(error) = self.oplog_manager.record_mount_segment_durable(
                    &segment.name,
                    segment.id,
                    segment.base,
                    segment.size,
                    &segment.te_endpoint,
                    &segment.protocol,
                    &segment.host_id,
                    client_id,
                ) {
                    self.state
                        .fence_after_durability_failure("remount_new_segment", &error);
                    return Err(Status::unavailable(
                        "failed to persist remounted segment; segment was not published locally",
                    ));
                }
                self.state.segments.insert(
                    segment.id,
                    SegmentEntry {
                        segment: segment.clone(),
                        used: 0,
                        client_id,
                        status: proto::SegmentStatus::Active,
                    },
                );
                self.state
                    .allocator
                    .write()
                    .add_segment(segment.clone(), 0, client_id);
            }
        }
        self.state.ok_clients.insert(client_id, ());
        sync_client_segments(&self.state, client_id);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        let addresses = remounts
            .iter()
            .map(|(segment, _)| {
                if segment.host_id.is_empty() {
                    host_from_segment_name(&segment.name)
                } else {
                    segment.host_id.clone()
                }
            })
            .collect::<Vec<_>>();
        upsert_client_addresses(&self.state, client_id, addresses);
        drop(_global_mutation_guard);
        register_metadata_segments(&self.metadata_state, &req.segment_names).await;
        Ok(Response::new(proto::ReMountSegmentResponse {}))
    }

    // ---- ReMountNoFSegment ----
    // 重新挂载 NoF segment，按 id/name 去重避免重复注册。
    // Remount NoF segments; deduplicate by id/name to avoid duplicate registration.
    pub(crate) async fn re_mount_nof_segment_impl(
        &self,
        request: Request<proto::ReMountNoFSegmentRequest>,
    ) -> Result<Response<proto::ReMountNoFSegmentResponse>, Status> {
        if !self.state.runtime_config.enable_nof {
            return Err(Status::unavailable("NoF is not enabled"));
        }
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if client_id.is_nil() {
            return Err(Status::invalid_argument("client_id must not be nil"));
        }
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let mut seen_ids = HashSet::new();
        let mut seen_names = HashSet::new();
        let mut seen_endpoints = HashSet::new();
        let mut remounts = Vec::with_capacity(req.segments.len());
        for proto_segment in &req.segments {
            let mut segment = nof_segment_from_proto(proto_segment);
            segment.client_id = client_id;
            if segment.id.is_nil()
                || segment.name.is_empty()
                || segment.size == 0
                || segment.te_endpoint.is_empty()
            {
                return Err(Status::invalid_argument(
                    "remounted NoF segment requires id/name/size/te_endpoint",
                ));
            }
            if self.state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
                && (segment.base % CACHELIB_SLAB_SIZE != 0
                    || segment.size % CACHELIB_SLAB_SIZE != 0)
            {
                return Err(Status::invalid_argument(format!(
                    "NoF base and size must be aligned to {CACHELIB_SLAB_SIZE} for cachelib"
                )));
            }
            if self.state.runtime_config.memory_allocator_kind == MemoryAllocatorKind::CachelibLike
                && segment.size > CACHELIB_MAX_SEGMENT_SIZE
            {
                return Err(Status::invalid_argument(format!(
                    "remounted NoF segment size exceeds Cachelib u32 slab index capacity \
                     {CACHELIB_MAX_SEGMENT_SIZE}"
                )));
            }
            if !seen_ids.insert(segment.id)
                || !seen_names.insert(segment.name.clone())
                || !seen_endpoints.insert(segment.te_endpoint.clone())
            {
                return Err(Status::invalid_argument(
                    "duplicate NoF segment id, name, or endpoint in remount request",
                ));
            }
            if self.state.segments.contains_key(&segment.id) {
                return Err(Status::failed_precondition(
                    "NoF segment UUID collides with an existing Memory segment",
                ));
            }
            if self
                .state
                .allocator
                .read()
                .used_bytes(&segment.id)
                .is_some()
            {
                self.state.fence_after_invariant_failure(
                    "re_mount_nof_segment",
                    "NoF segment UUID exists in the Memory allocator",
                );
                return Err(Status::unavailable(
                    "cross-allocator identity collision for NoF segment UUID",
                ));
            }
            let matching_ids = self
                .state
                .nof_segments
                .iter()
                .filter(|entry| {
                    entry.segment.id == segment.id || entry.segment.name == segment.name
                })
                .map(|entry| entry.segment.id)
                .collect::<HashSet<_>>();
            if matching_ids.len() > 1 {
                return Err(Status::failed_precondition(format!(
                    "NoF id/name identify different restored segments for {:?}",
                    segment.name
                )));
            }
            let restored = if let Some(restored_id) = matching_ids.iter().next() {
                let entry = self
                    .state
                    .nof_segments
                    .get(restored_id)
                    .expect("matching NoF segment exists");
                if entry.segment.client_id != client_id {
                    return Err(Status::permission_denied(format!(
                        "NoF segment {:?} belongs to another client session",
                        segment.name
                    )));
                }
                if entry.segment.id != segment.id
                    || entry.segment.name != segment.name
                    || entry.segment.size != segment.size
                {
                    return Err(Status::failed_precondition(format!(
                        "NoF segment identity changed during remount for {:?}",
                        segment.name
                    )));
                }
                true
            } else {
                false
            };
            let allocator_present = self
                .state
                .nof_allocator
                .read()
                .used_bytes(&segment.id)
                .is_some();
            if restored && !allocator_present {
                self.state.fence_after_invariant_failure(
                    "re_mount_nof_segment",
                    "restored NoF topology has no matching allocator segment",
                );
                return Err(Status::unavailable(
                    "allocator/topology mismatch for NoF segment UUID",
                ));
            }
            if !restored && allocator_present {
                self.state.fence_after_invariant_failure(
                    "re_mount_nof_segment",
                    "new NoF remount UUID already exists only in allocator",
                );
                return Err(Status::unavailable(
                    "allocator/topology mismatch for NoF segment UUID",
                ));
            }
            if self.state.nof_segments.iter().any(|entry| {
                entry.segment.id != segment.id && entry.segment.te_endpoint == segment.te_endpoint
            }) {
                return Err(Status::already_exists(
                    "NoF endpoint is already mounted under a different segment UUID",
                ));
            }
            let allocator_segment = mooncake_store_core::Segment {
                id: segment.id,
                name: segment.name.clone(),
                base: segment.base,
                size: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: String::new(),
                host_id: String::new(),
            };
            if restored {
                self.state
                    .nof_allocator
                    .read()
                    .validate_segment_rebind(&allocator_segment)
                    .map_err(Status::failed_precondition)?;
            }
            remounts.push((segment, allocator_segment, restored));
        }

        for (segment, allocator_segment, restored) in remounts {
            if restored {
                self.state
                    .nof_allocator
                    .write()
                    .rebind_segment(allocator_segment.clone(), client_id)
                    .map_err(Status::failed_precondition)?;
                let mut entry = self
                    .state
                    .nof_segments
                    .get_mut(&segment.id)
                    .expect("restored NoF segment was preflighted");
                entry.segment = segment;
                drop(entry);
                rebind_runtime_replicas(&self.state, &allocator_segment, ReplicaType::NoFSsd);
            } else {
                if let Err(error) = self.oplog_manager.record_mount_nof_segment_durable(
                    &segment.name,
                    segment.id,
                    segment.base,
                    segment.size,
                    &segment.te_endpoint,
                    client_id,
                ) {
                    self.state
                        .fence_after_durability_failure("remount_new_nof_segment", &error);
                    return Err(Status::unavailable(
                        "failed to persist remounted NoF segment; segment was not published locally",
                    ));
                }
                self.state.nof_segments.insert(
                    segment.id,
                    NoFSegmentEntry {
                        segment: segment.clone(),
                        used: 0,
                        status: proto::SegmentStatus::Active,
                    },
                );
                self.state
                    .nof_allocator
                    .write()
                    .add_segment(allocator_segment, 0, client_id);
            }
        }
        self.state.ok_clients.insert(client_id, ());
        upsert_client_addresses(
            &self.state,
            client_id,
            req.segments
                .iter()
                .map(|segment| host_from_segment_name(&segment.name))
                .collect(),
        );
        Ok(Response::new(proto::ReMountNoFSegmentResponse {}))
    }

    // ---- MountLocalDiskSegment ----
    // 注册客户端本地磁盘 segment，并执行 fail-closed 的持久盘 inventory 恢复。
    // storage_id 是持久命名空间身份，client_id 只是当前进程 session。
    pub(crate) async fn mount_local_disk_segment_impl(
        &self,
        request: Request<proto::MountLocalDiskSegmentRequest>,
    ) -> Result<Response<proto::MountLocalDiskSegmentResponse>, Status> {
        let req = request.into_inner();
        if !self.state.runtime_config.enable_offload {
            return Err(Status::failed_precondition("offload is not enabled"));
        }
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let storage_id = uuid_from_proto(
            req.storage_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing LocalDisk storage_id"))?,
        );
        if storage_id.is_nil() {
            return Err(Status::invalid_argument(
                "LocalDisk storage_id must not be nil",
            ));
        }
        let recovery_session_id = uuid_from_proto(req.recovery_session_id.as_ref().ok_or(
            Status::invalid_argument("missing LocalDisk recovery_session_id"),
        )?);
        if recovery_session_id.is_nil() {
            return Err(Status::invalid_argument(
                "LocalDisk recovery_session_id must not be nil",
            ));
        }
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();

        if !req.recovery_complete {
            self.state.ok_clients.remove(&client_id);
            if let Some(bound_storage_id) = self.state.local_disk_client_sessions.get(&client_id)
                && *bound_storage_id != storage_id
            {
                return Err(Status::failed_precondition(
                    "client is already bound to a different LocalDisk storage namespace",
                ));
            }

            if let Some(existing) = self.state.local_disk_segments.get(&storage_id)
                && existing.active_client_id == Some(client_id)
                && existing.recovery_session_id == Some(recovery_session_id)
                && !existing.recovery_complete
            {
                // A lost Begin response may be retried after some inventory
                // batches have already arrived. Preserve that accepted set.
                drop(existing);
                self.state
                    .local_disk_client_sessions
                    .insert(client_id, storage_id);
                upsert_client_addresses(&self.state, client_id, Vec::new());
                return Ok(Response::new(proto::MountLocalDiskSegmentResponse {}));
            }

            if let Some(existing) = self.state.local_disk_segments.get(&storage_id)
                && let Some(previous_client_id) = existing.active_client_id
                && previous_client_id != client_id
            {
                let previous_is_alive =
                    get_alive_clients_snapshot(&self.state).contains(&previous_client_id);
                if previous_is_alive {
                    return Err(Status::already_exists(
                        "LocalDisk storage namespace is still mounted by a live client",
                    ));
                }
                self.state
                    .local_disk_client_sessions
                    .remove(&previous_client_id);
            }

            // Starting a new recovery token fences every task admitted under
            // the previous binding, even when the process UUID is reused. A
            // delayed completion can therefore never become valid again after
            // this recovery commits.
            let stale_offloads = self
                .state
                .offloading_tasks
                .iter()
                .filter(|task| task.storage_id == storage_id)
                .map(|task| task.key().clone())
                .collect::<Vec<_>>();
            for key in stale_offloads {
                clear_offloading_task(&self.state, &key);
                self.persist_object_image_or_remove(&key, "local_disk_recovery_offload_fence")?;
            }
            let stale_promotions = self
                .state
                .promotion_tasks
                .iter()
                .filter(|task| task.storage_id == storage_id)
                .map(|task| task.key().clone())
                .collect::<Vec<_>>();
            for key in stale_promotions {
                let Some(task) = self
                    .state
                    .promotion_tasks
                    .get(&key)
                    .map(|task| task.clone())
                else {
                    continue;
                };
                let tenant_id = TenantId::parse_scoped_key(&key)
                    .map(|(tenant_id, _)| tenant_id)
                    .map_err(|error| {
                        self.state.fence_after_invariant_failure(
                            "local_disk_recovery_promotion_tenant",
                            &format!("key={key:?} error={error}"),
                        );
                        Status::internal("stale promotion has an invalid tenant-scoped key")
                    })?;
                self.abort_tenant_quota(&tenant_id, task.reserved_quota_charge_bytes)?;
                if self.state.service_fenced.load(Ordering::Acquire) {
                    return Err(Status::unavailable(
                        "tenant quota invariant failed while fencing stale promotion",
                    ));
                }
                let Some(task) = cancel_promotion_task(&self.state, &key) else {
                    self.state.fence_after_invariant_failure(
                        "local_disk_recovery_promotion_task",
                        &format!("key={key:?} disappeared under the snapshot mutation guard"),
                    );
                    return Err(Status::internal(
                        "stale promotion disappeared during recovery fencing",
                    ));
                };
                let removed = match (task.staged_segment_id, task.staged_offset) {
                    (Some(segment_id), Some(offset)) => {
                        detach_staged_promotion_replica(&self.state, &key, segment_id, offset)
                    }
                    _ => Vec::new(),
                };
                self.persist_detached_allocator_replicas(
                    &key,
                    removed,
                    "local_disk_recovery_promotion_fence",
                )?;
            }

            self.state.local_disk_segments.insert(
                storage_id,
                LocalDiskSegmentEntry {
                    active_client_id: Some(client_id),
                    persisted_client_id: Some(client_id),
                    recovery_complete: false,
                    recovery_session_id: Some(recovery_session_id),
                    recovered_objects: HashSet::new(),
                    enable_offloading: false,
                    offloading_objects: HashMap::new(),
                    promotion_objects: HashMap::new(),
                    // Capacity belongs to the previous process session and
                    // must be refreshed after every successful mount.
                    ssd_total_capacity_bytes: 0,
                },
            );
            self.state
                .local_disk_client_sessions
                .insert(client_id, storage_id);

            // A new process endpoint is not routable until the complete disk
            // inventory has been reported and committed.
            let keys = self
                .state
                .objects
                .iter()
                .filter(|object| {
                    object.replicas.iter().any(|replica| {
                        replica.replica_type == ReplicaType::LocalDisk
                            && replica.local_disk_storage_id == Some(storage_id)
                    })
                })
                .map(|object| object.key().clone())
                .collect::<Vec<_>>();
            for key in keys {
                if let Some(mut object) = self.state.objects.get_mut(&key) {
                    for replica in &mut object.replicas {
                        if replica.replica_type == ReplicaType::LocalDisk
                            && replica.local_disk_storage_id == Some(storage_id)
                        {
                            replica.holder_client_id = None;
                            replica.handle_valid = false;
                        }
                    }
                }
            }
        } else {
            let bound_storage_id = self
                .state
                .local_disk_client_sessions
                .get(&client_id)
                .map(|entry| *entry)
                .ok_or(Status::failed_precondition(
                    "LocalDisk recovery was not started for this client",
                ))?;
            if bound_storage_id != storage_id {
                return Err(Status::permission_denied(
                    "client is bound to a different LocalDisk storage namespace",
                ));
            }
            let recovered_objects = {
                let entry = self.state.local_disk_segments.get(&storage_id).ok_or(
                    Status::failed_precondition("LocalDisk recovery state not found"),
                )?;
                if entry.active_client_id != Some(client_id) {
                    return Err(Status::permission_denied(
                        "LocalDisk recovery belongs to another client",
                    ));
                }
                if entry.recovery_session_id != Some(recovery_session_id) {
                    return Err(Status::failed_precondition(
                        "LocalDisk recovery session is stale",
                    ));
                }
                if entry.recovery_complete {
                    drop(entry);
                    if let Some(mut entry) = self.state.local_disk_segments.get_mut(&storage_id) {
                        entry.enable_offloading = req.enable_offloading;
                    }
                    self.state.ok_clients.insert(client_id, ());
                    upsert_client_addresses(&self.state, client_id, Vec::new());
                    return Ok(Response::new(proto::MountLocalDiskSegmentResponse {}));
                }
                entry.recovered_objects.clone()
            };

            let keys = self
                .state
                .objects
                .iter()
                .filter(|object| {
                    object.replicas.iter().any(|replica| {
                        replica.replica_type == ReplicaType::LocalDisk
                            && replica.local_disk_storage_id == Some(storage_id)
                    })
                })
                .map(|object| object.key().clone())
                .collect::<Vec<_>>();
            for key in keys {
                let mut remove_object = false;
                let mut changed = false;
                if let Some(mut object) = self.state.objects.get_mut(&key) {
                    let before = object.replicas.len();
                    object.replicas.retain(|replica| {
                        replica.replica_type != ReplicaType::LocalDisk
                            || replica.local_disk_storage_id != Some(storage_id)
                            || recovered_objects.contains(&key)
                    });
                    changed = object.replicas.len() != before;
                    if changed {
                        sync_cache_total_accounting(&mut object);
                        remove_object = object.replicas.is_empty();
                    }
                }
                if remove_object {
                    if let Some((_, object)) = self.state.objects.remove(&key) {
                        self.account_removed_object_quota(&object)?;
                    }
                    self.state.processing_keys.remove(&key);
                    self.state.replication_tasks.remove(&key);
                    clear_offloading_task(&self.state, &key);
                    cancel_promotion_task(&self.state, &key);
                }
                if changed {
                    self.persist_object_image_or_remove(&key, "local_disk_recovery_cleanup")?;
                }
            }

            let mut entry = self
                .state
                .local_disk_segments
                .get_mut(&storage_id)
                .expect("LocalDisk recovery entry remains under the global mutation barrier");
            entry.recovery_complete = true;
            entry.enable_offloading = req.enable_offloading;
            entry.recovered_objects.clear();
            self.state.ok_clients.insert(client_id, ());
        }
        upsert_client_addresses(&self.state, client_id, Vec::new());
        Ok(Response::new(proto::MountLocalDiskSegmentResponse {}))
    }

    // ---- QuerySegmentStatus ----
    // 按名称查询 segment 状态（Active/Draining 等），同时查询 Memory 和 NoF segment。
    // Query segment status by name (Active/Draining, etc.); checks both Memory and NoF segments.
    pub(crate) async fn query_segment_status_impl(
        &self,
        request: Request<proto::QuerySegmentStatusRequest>,
    ) -> Result<Response<proto::QuerySegmentStatusResponse>, Status> {
        let req = request.into_inner();
        if let Some(entry) = self
            .state
            .segments
            .iter()
            .find(|e| e.segment.name == req.segment_name)
        {
            return Ok(Response::new(proto::QuerySegmentStatusResponse {
                status: entry.status as i32,
            }));
        }
        if let Some(entry) = self
            .state
            .nof_segments
            .iter()
            .find(|e| e.segment.name == req.segment_name)
        {
            return Ok(Response::new(proto::QuerySegmentStatusResponse {
                status: entry.status as i32,
            }));
        }
        Err(Status::not_found("segment not found"))
    }

    // ---- QuerySegmentStatusById ----
    // 按 UUID 查询 segment 状态，先查 Memory 后查 NoF segment。
    // Query segment status by UUID; checks Memory first, then NoF.
    pub(crate) async fn query_segment_status_by_id_impl(
        &self,
        request: Request<proto::QuerySegmentStatusByIdRequest>,
    ) -> Result<Response<proto::QuerySegmentStatusByIdResponse>, Status> {
        let req = request.into_inner();
        let seg_id = uuid_from_proto(
            req.segment_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing segment_id"))?,
        );
        if let Some(entry) = self.state.segments.get(&seg_id) {
            return Ok(Response::new(proto::QuerySegmentStatusByIdResponse {
                status: entry.status as i32,
            }));
        }
        if let Some(entry) = self.state.nof_segments.get(&seg_id) {
            return Ok(Response::new(proto::QuerySegmentStatusByIdResponse {
                status: entry.status as i32,
            }));
        }
        Err(Status::not_found("segment not found"))
    }
}

#[cfg(test)]
mod mount_invariant_tests {
    use super::*;

    fn memory_mount_request(
        client_id: Uuid,
        name: &str,
        base: u64,
        size: u64,
    ) -> proto::MountSegmentRequest {
        proto::MountSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segment_name: name.to_owned(),
            size,
            base_addr: base,
            te_endpoint: "127.0.0.1:12345".to_owned(),
            protocol: "tcp".to_owned(),
            host_id: "host-a".to_owned(),
        }
    }

    fn memory_segment(
        client_id: Uuid,
        request: &proto::MountSegmentRequest,
    ) -> mooncake_store_core::Segment {
        mooncake_store_core::Segment {
            id: stable_memory_segment_id(
                client_id,
                &request.segment_name,
                request.base_addr,
                request.size,
                &request.te_endpoint,
                &request.protocol,
                &request.host_id,
            ),
            name: request.segment_name.clone(),
            base: request.base_addr,
            size: request.size,
            te_endpoint: request.te_endpoint.clone(),
            protocol: request.protocol.clone(),
            host_id: request.host_id.clone(),
        }
    }

    #[tokio::test]
    async fn memory_mount_fences_allocator_only_identity_without_overwrite() {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let request = memory_mount_request(client_id, "memory-orphan", 0x1000, 1024);
        let segment = memory_segment(client_id, &request);
        let segment_id = segment.id;
        service
            .state
            .allocator
            .write()
            .add_segment(segment, 128, client_id);

        let error = service
            .mount_segment_impl(Request::new(request))
            .await
            .expect_err("allocator-only identity must fail closed");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.is_service_fenced());
        assert!(!service.state.segments.contains_key(&segment_id));
        assert_eq!(
            service.state.allocator.read().used_bytes(&segment_id),
            Some(128)
        );
    }

    #[tokio::test]
    async fn cachelib_memory_mount_accepts_page_aligned_external_base() {
        let runtime_config = MasterRuntimeConfig {
            memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
            ..MasterRuntimeConfig::default()
        };
        let service =
            MasterServiceImpl::new_with_runtime_config_and_oplog(None, None, runtime_config, None);
        let client_id = Uuid::new_v4();
        let request =
            memory_mount_request(client_id, "file-backed-memory", 0x1000, CACHELIB_SLAB_SIZE);
        let expected_id = memory_segment(client_id, &request).id;

        service
            .mount_segment_impl(Request::new(request))
            .await
            .expect("Cachelib allocation offsets do not require a slab-aligned external base");

        assert!(service.state.segments.contains_key(&expected_id));
        assert_eq!(
            service.state.allocator.read().used_bytes(&expected_id),
            Some(0)
        );
    }

    #[tokio::test]
    async fn memory_mount_fences_topology_without_allocator() {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let request = memory_mount_request(client_id, "memory-topology-only", 0x2000, 1024);
        let segment = memory_segment(client_id, &request);
        let segment_id = segment.id;
        service.state.segments.insert(
            segment_id,
            SegmentEntry {
                segment,
                used: 0,
                client_id,
                status: proto::SegmentStatus::Active,
            },
        );

        let error = service
            .mount_segment_impl(Request::new(request))
            .await
            .expect_err("topology-only identity must fail closed");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.is_service_fenced());
        assert_eq!(service.state.allocator.read().used_bytes(&segment_id), None);
    }

    #[tokio::test]
    async fn nof_mount_fences_topology_without_allocator() {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_id = Uuid::new_v4();
        let segment = mooncake_store_core::NoFSegment {
            id: segment_id,
            name: "nof-topology-only".to_owned(),
            base: 0x2800,
            size: 1024,
            te_endpoint: "127.0.0.1:22345".to_owned(),
            client_id,
        };
        service.state.nof_segments.insert(
            segment_id,
            NoFSegmentEntry {
                segment: segment.clone(),
                used: 0,
                status: proto::SegmentStatus::Active,
            },
        );
        let request = proto::MountNoFSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(uuid_to_proto(segment_id)),
                name: segment.name,
                base: segment.base,
                size: segment.size,
                te_endpoint: segment.te_endpoint,
                client_id: Some(uuid_to_proto(client_id)),
            }),
        };

        let error = service
            .mount_nof_segment_impl(Request::new(request))
            .await
            .expect_err("NoF topology-only identity must fail closed");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.is_service_fenced());
        assert_eq!(
            service.state.nof_allocator.read().used_bytes(&segment_id),
            None
        );
    }

    #[tokio::test]
    async fn nof_mount_rejects_memory_uuid_and_fences_allocator_only_identity() {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let memory_request = memory_mount_request(client_id, "memory-cross-type", 0x3000, 1024);
        let memory_segment = memory_segment(client_id, &memory_request);
        let segment_id = memory_segment.id;
        service.state.segments.insert(
            segment_id,
            SegmentEntry {
                segment: memory_segment,
                used: 0,
                client_id,
                status: proto::SegmentStatus::Active,
            },
        );

        let nof_request = proto::MountNoFSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(uuid_to_proto(segment_id)),
                name: "nof-cross-type".to_owned(),
                base: 0x4000,
                size: 1024,
                te_endpoint: "127.0.0.1:23456".to_owned(),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        };
        let error = service
            .mount_nof_segment_impl(Request::new(nof_request))
            .await
            .expect_err("Memory and NoF UUID namespaces must not overlap");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(!service.state.nof_segments.contains_key(&segment_id));

        service.state.segments.remove(&segment_id);
        service.state.nof_allocator.write().add_segment(
            mooncake_store_core::Segment {
                id: segment_id,
                name: "nof-orphan".to_owned(),
                base: 0x4000,
                size: 1024,
                te_endpoint: "127.0.0.1:23456".to_owned(),
                protocol: String::new(),
                host_id: String::new(),
            },
            256,
            client_id,
        );
        let allocator_only_request = proto::MountNoFSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segment: Some(proto::NoFSegment {
                id: Some(uuid_to_proto(segment_id)),
                name: "nof-orphan".to_owned(),
                base: 0x4000,
                size: 1024,
                te_endpoint: "127.0.0.1:23456".to_owned(),
                client_id: Some(uuid_to_proto(client_id)),
            }),
        };
        let error = service
            .mount_nof_segment_impl(Request::new(allocator_only_request))
            .await
            .expect_err("NoF allocator-only identity must fail closed");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.is_service_fenced());
        assert!(!service.state.nof_segments.contains_key(&segment_id));
        assert_eq!(
            service.state.nof_allocator.read().used_bytes(&segment_id),
            Some(256)
        );
    }

    #[tokio::test]
    async fn legacy_memory_remount_without_id_uses_canonical_stable_identity() {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let name = "legacy-memory-remount";
        let base = 0x4800;
        let size = 1024;
        let endpoint = "127.0.0.1:30345";
        let protocol = "tcp";
        let host_id = "host-a";
        let expected_id =
            stable_memory_segment_id(client_id, name, base, size, endpoint, protocol, host_id);
        let request = proto::ReMountSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segment_names: vec![name.to_owned()],
            segment_sizes: vec![size],
            base_addrs: vec![base],
            te_endpoints: vec![endpoint.to_owned()],
            protocols: vec![protocol.to_owned()],
            segment_ids: Vec::new(),
            host_ids: vec![host_id.to_owned()],
        };

        service
            .re_mount_segment_impl(Request::new(request.clone()))
            .await
            .expect("first legacy remount should install the canonical identity");
        service
            .re_mount_segment_impl(Request::new(request))
            .await
            .expect("retry must resolve to the same canonical identity");

        assert_eq!(service.state.segments.len(), 1);
        assert!(service.state.segments.contains_key(&expected_id));
        assert_eq!(
            service.state.allocator.read().used_bytes(&expected_id),
            Some(0)
        );
    }

    #[tokio::test]
    async fn cachelib_remount_rejects_unrepresentable_memory_and_nof_capacity() {
        let runtime_config = MasterRuntimeConfig {
            memory_allocator_kind: MemoryAllocatorKind::CachelibLike,
            ..MasterRuntimeConfig::default()
        };
        let service =
            MasterServiceImpl::new_with_runtime_config_and_oplog(None, None, runtime_config, None);
        let client_id = Uuid::new_v4();
        let oversized = CACHELIB_MAX_SEGMENT_SIZE + CACHELIB_SLAB_SIZE;
        let memory_id = Uuid::new_v4();
        let memory_request = proto::ReMountSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segment_names: vec!["oversized-memory".to_owned()],
            segment_sizes: vec![oversized],
            base_addrs: vec![CACHELIB_SLAB_SIZE],
            te_endpoints: vec!["127.0.0.1:31345".to_owned()],
            protocols: vec!["tcp".to_owned()],
            segment_ids: vec![uuid_to_proto(memory_id)],
            host_ids: vec!["host-a".to_owned()],
        };
        let memory_error = service
            .re_mount_segment_impl(Request::new(memory_request))
            .await
            .expect_err("Cachelib Memory remount must reject an unrepresentable slab count");
        assert_eq!(memory_error.code(), tonic::Code::InvalidArgument);
        assert!(!service.state.segments.contains_key(&memory_id));
        assert_eq!(service.state.allocator.read().used_bytes(&memory_id), None);

        let nof_id = Uuid::new_v4();
        let nof_request = proto::ReMountNoFSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segments: vec![proto::NoFSegment {
                id: Some(uuid_to_proto(nof_id)),
                name: "oversized-nof".to_owned(),
                base: CACHELIB_SLAB_SIZE,
                size: oversized,
                te_endpoint: "127.0.0.1:31346".to_owned(),
                client_id: Some(uuid_to_proto(client_id)),
            }],
        };
        let nof_error = service
            .re_mount_nof_segment_impl(Request::new(nof_request))
            .await
            .expect_err("Cachelib NoF remount must reject an unrepresentable slab count");
        assert_eq!(nof_error.code(), tonic::Code::InvalidArgument);
        assert!(!service.state.nof_segments.contains_key(&nof_id));
        assert_eq!(service.state.nof_allocator.read().used_bytes(&nof_id), None);
    }

    #[tokio::test]
    async fn memory_remount_preflights_full_batch_before_allocator_collision() {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let first_id = Uuid::new_v4();
        let orphan_id = Uuid::new_v4();
        service.state.allocator.write().add_segment(
            mooncake_store_core::Segment {
                id: orphan_id,
                name: "memory-orphan".to_owned(),
                base: 0x6000,
                size: 1024,
                te_endpoint: "127.0.0.1:32346".to_owned(),
                protocol: "tcp".to_owned(),
                host_id: "host-a".to_owned(),
            },
            128,
            client_id,
        );
        let request = proto::ReMountSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segment_names: vec!["memory-new".to_owned(), "memory-orphan".to_owned()],
            segment_sizes: vec![1024, 1024],
            base_addrs: vec![0x5000, 0x6000],
            te_endpoints: vec!["127.0.0.1:32345".to_owned(), "127.0.0.1:32346".to_owned()],
            protocols: vec!["tcp".to_owned(), "tcp".to_owned()],
            segment_ids: vec![uuid_to_proto(first_id), uuid_to_proto(orphan_id)],
            host_ids: vec!["host-a".to_owned(), "host-a".to_owned()],
        };

        let error = service
            .re_mount_segment_impl(Request::new(request))
            .await
            .expect_err("allocator-only identity must reject the whole remount batch");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.is_service_fenced());
        assert!(!service.state.segments.contains_key(&first_id));
        assert!(!service.state.segments.contains_key(&orphan_id));
        assert_eq!(
            service.state.allocator.read().used_bytes(&orphan_id),
            Some(128)
        );
    }

    #[tokio::test]
    async fn nof_remount_fences_topology_without_allocator() {
        let service = MasterServiceImpl::default();
        let client_id = Uuid::new_v4();
        let segment_id = Uuid::new_v4();
        let segment = mooncake_store_core::NoFSegment {
            id: segment_id,
            name: "nof-remount-topology-only".to_owned(),
            base: 0x7000,
            size: 1024,
            te_endpoint: "127.0.0.1:42345".to_owned(),
            client_id,
        };
        service.state.nof_segments.insert(
            segment_id,
            NoFSegmentEntry {
                segment: segment.clone(),
                used: 0,
                status: proto::SegmentStatus::Active,
            },
        );
        let request = proto::ReMountNoFSegmentRequest {
            client_id: Some(uuid_to_proto(client_id)),
            segments: vec![proto::NoFSegment {
                id: Some(uuid_to_proto(segment_id)),
                name: segment.name,
                base: segment.base,
                size: segment.size,
                te_endpoint: segment.te_endpoint,
                client_id: Some(uuid_to_proto(client_id)),
            }],
        };

        let error = service
            .re_mount_nof_segment_impl(Request::new(request))
            .await
            .expect_err("NoF topology-only identity must fail closed during remount");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.is_service_fenced());
        assert_eq!(
            service.state.nof_allocator.read().used_bytes(&segment_id),
            None
        );
    }
}

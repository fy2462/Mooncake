use super::*;

impl MasterServiceImpl {
    // ---- Ping ----
    pub(super) async fn ping_impl(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let existed = self.state.clients.contains_key(&client_id);
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
        let view_version_id = if existed {
            self.state.view_version.load(std::sync::atomic::Ordering::Relaxed)
        } else {
            bump_view_version(&self.state)
        };
        let client_status = if existed {
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
    pub(super) async fn mount_segment_impl(
        &self,
        request: Request<proto::MountSegmentRequest>,
    ) -> Result<Response<proto::MountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let segment_id = Uuid::new_v4();
        let host = host_from_segment_name(&req.segment_name);

        let segment = mooncake_store_core::Segment {
            id: segment_id,
            name: req.segment_name.clone(),
            size: req.size,
            used: 0,
            client_id,
        };

        self.state.segments.insert(
            segment_id,
            SegmentEntry {
                segment: segment.clone(),
                status: proto::SegmentStatus::Active,
            },
        );
        upsert_client_addresses(&self.state, client_id, vec![host]);
        sync_client_segments(&self.state, client_id);
        register_metadata_segments(&self.metadata_state, &[req.segment_name.clone()]).await;

        let mut allocator = self.state.allocator.write();
        allocator.add_segment(segment);

        bump_view_version(&self.state);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::MountSegmentResponse {}))
    }

    // ---- MountNoFSegment ----
    pub(super) async fn mount_nof_segment_impl(
        &self,
        request: Request<proto::MountNoFSegmentRequest>,
    ) -> Result<Response<proto::MountNoFSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut segment = req
            .segment
            .as_ref()
            .map(nof_segment_from_proto)
            .ok_or(Status::invalid_argument("missing segment"))?;
        segment.client_id = client_id;

        self.state.nof_segments.insert(
            segment.id,
            NoFSegmentEntry {
                segment: segment.clone(),
                used: 0,
                status: proto::SegmentStatus::Active,
            },
        );
        self.state.nof_allocator.write().add_segment(mooncake_store_core::Segment {
            id: segment.id,
            name: segment.name.clone(),
            size: segment.size,
            used: 0,
            client_id,
        });
        bump_view_version(&self.state);
        Ok(Response::new(proto::MountNoFSegmentResponse {}))
    }

    // ---- UnmountSegment ----
    pub(super) async fn unmount_segment_impl(
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

        if !unmount_segment_owned(&self.state, segment_id, client_id) {
            return Err(Status::not_found("segment not found for client"));
        }
        bump_view_version(&self.state);
        Ok(Response::new(proto::UnmountSegmentResponse {}))
    }

    // ---- UnmountNoFSegment ----
    pub(super) async fn unmount_nof_segment_impl(
        &self,
        request: Request<proto::UnmountNoFSegmentRequest>,
    ) -> Result<Response<proto::UnmountNoFSegmentResponse>, Status> {
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
        if !unmount_nof_segment_owned(&self.state, segment_id, client_id) {
            return Err(Status::not_found("NoF segment not found for client"));
        }
        bump_view_version(&self.state);
        Ok(Response::new(proto::UnmountNoFSegmentResponse {}))
    }

    // ---- GracefulUnmountSegment ----
    pub(super) async fn graceful_unmount_segment_impl(
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
        let owned = self
            .state
            .segments
            .get(&segment_id)
            .map(|entry| entry.segment.client_id == client_id)
            .unwrap_or(false);
        if !owned {
            return Err(Status::not_found("segment not found for client"));
        }
        self.graceful_unmount_scheduler
            .schedule(segment_id, client_id, req.grace_period_ms);
        Ok(Response::new(proto::GracefulUnmountSegmentResponse {}))
    }

    // ---- ReMountSegment ----
    pub(super) async fn re_mount_segment_impl(
        &self,
        request: Request<proto::ReMountSegmentRequest>,
    ) -> Result<Response<proto::ReMountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if req.segment_names.len() != req.segment_sizes.len() {
            return Err(Status::invalid_argument(
                "segment_names and segment_sizes must have same length",
            ));
        }

        let addresses = req
            .segment_names
            .iter()
            .map(|name| host_from_segment_name(name))
            .collect::<Vec<_>>();
        upsert_client_addresses(&self.state, client_id, addresses);
        register_metadata_segments(&self.metadata_state, &req.segment_names).await;

        for (segment_name, size) in req.segment_names.iter().zip(req.segment_sizes.iter()) {
            let exists = self.state.segments.iter().any(|entry| {
                entry.segment.client_id == client_id && entry.segment.name == *segment_name
            });
            if exists {
                continue;
            }

            let segment = mooncake_store_core::Segment {
                id: Uuid::new_v4(),
                name: segment_name.clone(),
                size: *size,
                used: 0,
                client_id,
            };
            self.state.segments.insert(
                segment.id,
                SegmentEntry {
                    segment: segment.clone(),
                    status: proto::SegmentStatus::Active,
                },
            );
            self.state.allocator.write().add_segment(segment);
        }
        sync_client_segments(&self.state, client_id);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::ReMountSegmentResponse {}))
    }

    // ---- ReMountNoFSegment ----
    pub(super) async fn re_mount_nof_segment_impl(
        &self,
        request: Request<proto::ReMountNoFSegmentRequest>,
    ) -> Result<Response<proto::ReMountNoFSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        for proto_segment in &req.segments {
            let mut segment = nof_segment_from_proto(proto_segment);
            segment.client_id = client_id;
            let exists = self
                .state
                .nof_segments
                .iter()
                .any(|entry| entry.segment.id == segment.id || entry.segment.name == segment.name);
            if exists {
                continue;
            }
            self.state.nof_segments.insert(
                segment.id,
                NoFSegmentEntry {
                    segment: segment.clone(),
                    used: 0,
                    status: proto::SegmentStatus::Active,
                },
            );
            self.state.nof_allocator.write().add_segment(mooncake_store_core::Segment {
                id: segment.id,
                name: segment.name.clone(),
                size: segment.size,
                used: 0,
                client_id,
            });
        }
        Ok(Response::new(proto::ReMountNoFSegmentResponse {}))
    }

    // ---- MountLocalDiskSegment ----
    pub(super) async fn mount_local_disk_segment_impl(
        &self,
        request: Request<proto::MountLocalDiskSegmentRequest>,
    ) -> Result<Response<proto::MountLocalDiskSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if let Some(mut entry) = self.state.local_disk_segments.get_mut(&client_id) {
            entry.enable_offloading = req.enable_offloading;
        } else {
            self.state.local_disk_segments.insert(
                client_id,
                LocalDiskSegmentEntry {
                    enable_offloading: req.enable_offloading,
                    offloading_objects: HashMap::new(),
                    promotion_objects: HashMap::new(),
                    ssd_total_capacity_bytes: 0,
                },
            );
        }
        Ok(Response::new(proto::MountLocalDiskSegmentResponse {}))
    }

    // ---- OffloadObjectHeartbeat ----
    pub(super) async fn offload_object_heartbeat_impl(
        &self,
        request: Request<proto::OffloadObjectHeartbeatRequest>,
    ) -> Result<Response<proto::OffloadObjectHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&client_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        entry.enable_offloading = req.enable_offloading;
        if !req.enable_offloading {
            let keys = entry.offloading_objects.keys().cloned().collect::<Vec<_>>();
            entry.offloading_objects.clear();
            drop(entry);
            for key in keys {
                clear_offloading_task(&self.state, &key);
            }
            return Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
                objects: HashMap::new(),
            }));
        }
        let objects = std::mem::take(&mut entry.offloading_objects);
        Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
            objects,
        }))
    }

    // ---- ReportSsdCapacity ----
    pub(super) async fn report_ssd_capacity_impl(
        &self,
        request: Request<proto::ReportSsdCapacityRequest>,
    ) -> Result<Response<proto::ReportSsdCapacityResponse>, Status> {
        let req = request.into_inner();
        if req.ssd_total_capacity_bytes < 0 {
            return Err(Status::invalid_argument(
                "ssd_total_capacity_bytes must be non-negative",
            ));
        }
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&client_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        entry.ssd_total_capacity_bytes = req.ssd_total_capacity_bytes;
        Ok(Response::new(proto::ReportSsdCapacityResponse {}))
    }

    // ---- NotifyOffloadSuccess ----
    pub(super) async fn notify_offload_success_impl(
        &self,
        request: Request<proto::NotifyOffloadSuccessRequest>,
    ) -> Result<Response<proto::NotifyOffloadSuccessResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if req.keys.len() != req.metadatas.len() {
            return Err(Status::invalid_argument(
                "keys and metadatas must have same length",
            ));
        }
        for (key, metadata) in req.keys.iter().zip(req.metadatas.iter()) {
            clear_offloading_task(&self.state, key);
            let replica = ReplicaDescriptor {
                segment_id: Uuid::nil(),
                segment_name: metadata.transport_endpoint.clone(),
                offset: 0,
                size: metadata.data_size.max(0) as u64,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::LocalDisk,
                holder_client_id: Some(client_id),
            };
            if let Some(mut object) = self.state.objects.get_mut(key) {
                if let Some(existing) = object.replicas.iter_mut().find(|existing| {
                    existing.replica_type == ReplicaType::LocalDisk
                        && existing.holder_client_id == Some(client_id)
                }) {
                    *existing = replica.clone();
                } else {
                    object.replicas.push(replica);
                }
                object.size = metadata.data_size.max(0) as u64;
            } else {
                self.state.objects.insert(
                    key.clone(),
                    ObjectEntry {
                        replicas: vec![replica],
                        size: metadata.data_size.max(0) as u64,
                        last_access: SystemTime::now(),
                        soft_pinned: false,
                        hard_pinned: false,
                        data_type: ObjectDataType::Unknown,
                        put_start_time: None,
                        lease_timeout: None,
                        soft_pin_timeout: None,
                    },
                );
            }
        }
        Ok(Response::new(proto::NotifyOffloadSuccessResponse {}))
    }

    // ---- PromotionObjectHeartbeat ----
    pub(super) async fn promotion_object_heartbeat_impl(
        &self,
        request: Request<proto::PromotionObjectHeartbeatRequest>,
    ) -> Result<Response<proto::PromotionObjectHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut entry = self
            .state
            .local_disk_segments
            .get_mut(&client_id)
            .ok_or(Status::not_found("local disk segment not found"))?;
        let mut objects = HashMap::new();
        if let Some((key, size)) = entry
            .promotion_objects
            .iter()
            .next()
            .map(|(key, size)| (key.clone(), *size))
        {
            entry.promotion_objects.remove(&key);
            objects.insert(key, size);
        }
        Ok(Response::new(proto::PromotionObjectHeartbeatResponse {
            objects,
        }))
    }

    // ---- PromotionAllocStart ----
    pub(super) async fn promotion_alloc_start_impl(
        &self,
        request: Request<proto::PromotionAllocStartRequest>,
    ) -> Result<Response<proto::PromotionAllocStartResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut task = self
            .state
            .promotion_tasks
            .get_mut(&req.key)
            .ok_or(Status::failed_precondition("promotion task not found"))?;
        if task.holder_id != client_id {
            return Err(Status::permission_denied(
                "promotion task assigned to different client",
            ));
        }
        if task.object_size != req.size {
            return Err(Status::invalid_argument("size mismatch"));
        }
        let object_exists = self.state.objects.contains_key(&req.key);
        if !object_exists {
            return Err(Status::not_found("key not found"));
        }

        let mut config = ReplicateConfig::default();
        if let Some(preferred) = req.preferred_segments.first() {
            config.preferred_segment = preferred.clone();
        }
        let replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate_for_client(&req.key, Some(client_id), req.size, 1, &config)
        };
        let Some(mut staged) = replicas.into_iter().next() else {
            return Err(Status::resource_exhausted("no available memory segment"));
        };
        sync_segment_usage(&self.state, [staged.segment_id]);
        let staged_segment_id = staged.segment_id;
        let staged_offset = staged.offset;
        staged.status = ReplicaStatus::Allocating;
        if let Some(mut object) = self.state.objects.get_mut(&req.key) {
            object.replicas.push(staged.clone());
        }
        task.staged_segment_id = Some(staged_segment_id);
        task.staged_offset = Some(staged_offset);
        task.start_time = std::time::Instant::now();
        Ok(Response::new(proto::PromotionAllocStartResponse {
            memory_descriptor: Some(replica_to_proto(&staged)),
        }))
    }

    // ---- NotifyPromotionSuccess ----
    pub(super) async fn notify_promotion_success_impl(
        &self,
        request: Request<proto::NotifyPromotionSuccessRequest>,
    ) -> Result<Response<proto::NotifyPromotionSuccessResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .promotion_tasks
            .get(&req.key)
            .ok_or(Status::failed_precondition("promotion task not found"))?
            .clone();
        if task.holder_id != client_id {
            return Err(Status::permission_denied(
                "promotion task assigned to different client",
            ));
        }
        let Some(segment_id) = task.staged_segment_id else {
            return Err(Status::failed_precondition(
                "promotion buffer not allocated",
            ));
        };
        let Some(offset) = task.staged_offset else {
            return Err(Status::failed_precondition(
                "promotion buffer not allocated",
            ));
        };
        let mut committed = false;
        let mut object = self
            .state
            .objects
            .get_mut(&req.key)
            .ok_or(Status::not_found("key not found"))?;
        if let Some(replica) = object.replicas.iter_mut().find(|replica| {
            replica.replica_type == ReplicaType::Memory
                && replica.segment_id == segment_id
                && replica.offset == offset
                && replica.status == ReplicaStatus::Allocating
        }) {
            replica.status = ReplicaStatus::Complete;
            committed = true;
        }
        drop(object);
        clear_promotion_task(&self.state, &req.key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&req.key);
        }
        if !committed {
            return Err(Status::failed_precondition("promotion replica not ready"));
        }
        Ok(Response::new(proto::NotifyPromotionSuccessResponse {}))
    }

    // ---- NotifyPromotionFailure ----
    pub(super) async fn notify_promotion_failure_impl(
        &self,
        request: Request<proto::NotifyPromotionFailureRequest>,
    ) -> Result<Response<proto::NotifyPromotionFailureResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let Some(task) = self
            .state
            .promotion_tasks
            .get(&req.key)
            .map(|task| task.clone())
        else {
            return Ok(Response::new(proto::NotifyPromotionFailureResponse {}));
        };
        if task.holder_id != client_id {
            return Err(Status::permission_denied(
                "promotion task assigned to different client",
            ));
        }
        if let (Some(segment_id), Some(offset)) = (task.staged_segment_id, task.staged_offset) {
            release_staged_promotion_replica(&self.state, &req.key, segment_id, offset);
        }
        clear_promotion_task(&self.state, &req.key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&req.key);
        }
        Ok(Response::new(proto::NotifyPromotionFailureResponse {}))
    }

    // ---- QuerySegmentStatus ----
    pub(super) async fn query_segment_status_impl(
        &self,
        request: Request<proto::QuerySegmentStatusRequest>,
    ) -> Result<Response<proto::QuerySegmentStatusResponse>, Status> {
        let req = request.into_inner();
        if let Some(entry) = self.state.segments.iter().find(|e| e.segment.name == req.segment_name) {
            return Ok(Response::new(proto::QuerySegmentStatusResponse {
                status: entry.status as i32,
            }));
        }
        if let Some(entry) = self.state.nof_segments.iter().find(|e| e.segment.name == req.segment_name) {
            return Ok(Response::new(proto::QuerySegmentStatusResponse {
                status: entry.status as i32,
            }));
        }
        Err(Status::not_found("segment not found"))
    }

    // ---- QuerySegmentStatusById ----
    pub(super) async fn query_segment_status_by_id_impl(
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

    // ---- CreateDrainJob ----
    pub(super) async fn create_drain_job_impl(
        &self,
        request: Request<proto::CreateDrainJobRequest>,
    ) -> Result<Response<proto::CreateDrainJobResponse>, Status> {
        let req = request.into_inner();
        if req.segments.is_empty() {
            return Err(Status::invalid_argument("segments cannot be empty"));
        }
        if req.target_segments.is_empty() {
            return Err(Status::invalid_argument("target_segments cannot be empty"));
        }
        // Validate that all source segments exist and are in ACTIVE state
        for seg_name in &req.segments {
            let found = self.state.segments.iter().any(|e| {
                e.segment.name == *seg_name && e.status == proto::SegmentStatus::Active
            }) || self.state.nof_segments.iter().any(|e| {
                e.segment.name == *seg_name && e.status == proto::SegmentStatus::Active
            });
            if !found {
                return Err(Status::failed_precondition(format!(
                    "segment not found or not active: {seg_name}"
                )));
            }
        }
        // Validate that all target segments exist
        for tgt_name in &req.target_segments {
            let found = self.state.segments.iter().any(|e| {
                e.segment.name == *tgt_name && e.status == proto::SegmentStatus::Active
            });
            if !found {
                return Err(Status::failed_precondition(format!(
                    "target segment not found or not active: {tgt_name}"
                )));
            }
        }
        // Transition source segments to DRAINING
        for seg_name in &req.segments {
            for mut entry in self.state.segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Draining;
                }
            }
            for mut entry in self.state.nof_segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Draining;
                }
            }
        }
        let job_id = Uuid::new_v4();
        let now = SystemTime::now();
        self.state.drain_jobs.insert(
            job_id,
            DrainJobEntry {
                id: job_id,
                status: proto::JobStatus::Created,
                segments: req.segments.clone(),
                target_segments: req.target_segments.clone(),
                max_concurrency: req.max_concurrency.max(1),
                created_at: now,
                last_updated_at: now,
                message: String::new(),
                succeeded_units: 0,
                failed_units: 0,
                blocked_units: 0,
                migrated_bytes: 0,
                active_tasks: HashMap::new(),
                completed_unit_keys: HashSet::new(),
                retry_counts: HashMap::new(),
                terminal_failed_unit_keys: HashSet::new(),
            },
        );
        // Start planning immediately (find objects to drain)
        if let Some(mut job) = self.state.drain_jobs.get_mut(&job_id) {
            job.status = proto::JobStatus::Planning;
        }
        self.schedule_drain_job_tasks(job_id);
        tracing::info!(
            "Drain job created: id={}, segments={:?}, targets={:?}",
            job_id,
            req.segments,
            req.target_segments
        );
        Ok(Response::new(proto::CreateDrainJobResponse {
            job_id: Some(uuid_to_proto(job_id)),
        }))
    }

    // ---- QueryDrainJob ----
    pub(super) async fn query_drain_job_impl(
        &self,
        request: Request<proto::QueryDrainJobRequest>,
    ) -> Result<Response<proto::QueryDrainJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = uuid_from_proto(
            req.job_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing job_id"))?,
        );
        let job = self
            .state
            .drain_jobs
            .get(&job_id)
            .ok_or(Status::not_found("drain job not found"))?;
        Ok(Response::new(proto::QueryDrainJobResponse {
            id: Some(uuid_to_proto(job.id)),
            r#type: proto::JobType::Drain as i32,
            status: job.status as i32,
            created_at_ms_epoch: job
                .created_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            last_updated_at_ms_epoch: job
                .last_updated_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            segments: job.segments.clone(),
            succeeded_units: job.succeeded_units,
            failed_units: job.failed_units,
            blocked_units: job.blocked_units,
            active_units: job.active_tasks.len() as u64,
            migrated_bytes: job.migrated_bytes,
            message: job.message.clone(),
        }))
    }

    // ---- CancelDrainJob ----
    pub(super) async fn cancel_drain_job_impl(
        &self,
        request: Request<proto::CancelDrainJobRequest>,
    ) -> Result<Response<proto::CancelDrainJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = uuid_from_proto(
            req.job_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing job_id"))?,
        );
        let mut job = self
            .state
            .drain_jobs
            .get_mut(&job_id)
            .ok_or(Status::not_found("drain job not found"))?;
        if job.status == proto::JobStatus::Succeeded
            || job.status == proto::JobStatus::Failed
            || job.status == proto::JobStatus::Canceled
        {
            return Err(Status::failed_precondition(
                "drain job already in terminal state",
            ));
        }
        // Restore draining segments back to ACTIVE
        for seg_name in &job.segments {
            for mut entry in self.state.segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Active;
                }
            }
            for mut entry in self.state.nof_segments.iter_mut() {
                if entry.segment.name == *seg_name {
                    entry.status = proto::SegmentStatus::Active;
                }
            }
        }
        job.status = proto::JobStatus::Canceled;
        job.last_updated_at = SystemTime::now();
        job.message = "job canceled".into();
        tracing::info!("Drain job canceled: id={}", job_id);
        Ok(Response::new(proto::CancelDrainJobResponse {}))
    }

    /// Helper: find objects on draining segments and create drain tasks.
    fn schedule_drain_job_tasks(&self, job_id: Uuid) {
        let mut job = match self.state.drain_jobs.get_mut(&job_id) {
            Some(j) => j,
            None => return,
        };
        let draining_segments: HashSet<String> = job.segments.iter().cloned().collect();
        let targets = job.target_segments.clone();
        let max_concurrency = job.max_concurrency as usize;

        // Find objects with replicas on draining segments
        let mut units: Vec<(String, String, u64)> = Vec::new(); // (key, source_seg, bytes)
        for entry in self.state.objects.iter() {
            let key = entry.key().clone();
            for replica in &entry.replicas {
                if draining_segments.contains(&replica.segment_name)
                    && replica.status == ReplicaStatus::Complete
                {
                    units.push((key.clone(), replica.segment_name.clone(), replica.size));
                    break; // one unit per key
                }
            }
        }

        // Pick a target for each unit (round-robin)
        let num_targets = targets.len().max(1);
        for (i, (key, source_seg, bytes)) in units.into_iter().enumerate() {
            if job.active_tasks.len() >= max_concurrency {
                break;
            }
            let unit_key = format!("{key}@{source_seg}");
            if job.completed_unit_keys.contains(&unit_key)
                || job.terminal_failed_unit_keys.contains(&unit_key)
            {
                continue;
            }
            let target_seg = targets[i % num_targets].clone();
            let task_id = Uuid::new_v4();
            job.active_tasks.insert(
                task_id,
                ActiveDrainTask {
                    task_id,
                    key: key.clone(),
                    source_segment: source_seg,
                    target_segment: target_seg,
                    bytes,
                    unit_key: unit_key.clone(),
                },
            );

            // Create a copy task for this drain unit
            let payload = serde_json::to_string(&ReplicaCopyPayload {
                key: &key,
                source: &job.active_tasks.get(&task_id).unwrap().source_segment,
                targets: &[job.active_tasks.get(&task_id).unwrap().target_segment.clone()],
            })
            .unwrap_or_default();
            let now = Utc::now();
            self.state.tasks.insert(
                task_id,
                TaskEntry {
                    info: TaskInfo {
                        id: task_id,
                        task_type: TaskType::ReplicaCopy,
                        status: TaskStatus::Pending,
                        created_at: now,
                        last_updated_at: now,
                        assigned_client: client_id_by_segment_name(
                            &self.state,
                            &job.active_tasks.get(&task_id).unwrap().source_segment,
                        ),
                        message: format!(
                            "drain {} from {} to {}",
                            key,
                            job.active_tasks.get(&task_id).unwrap().source_segment,
                            job.active_tasks.get(&task_id).unwrap().target_segment
                        ),
                    },
                    key: key.clone(),
                    payload,
                    max_retry_attempts: 3,
                },
            );
        }

        job.status = if job.active_tasks.is_empty() {
            proto::JobStatus::Succeeded
        } else {
            proto::JobStatus::Running
        };
        job.last_updated_at = SystemTime::now();
    }

    // ---- GetFsdir ----
    pub(super) async fn get_fsdir_impl(
        &self,
        _request: Request<proto::GetFsdirRequest>,
    ) -> Result<Response<proto::GetFsdirResponse>, Status> {
        let fs_dir = self.state.runtime_config.storage_fs_dir.clone();
        Ok(Response::new(proto::GetFsdirResponse { fs_dir }))
    }
}

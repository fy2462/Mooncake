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
}

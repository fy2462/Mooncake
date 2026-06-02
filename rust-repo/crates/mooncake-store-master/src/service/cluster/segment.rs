use super::super::*;

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
        let existed = self.state.clients.contains_key(&client_id);
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
        let view_version_id = if existed {
            self.state
                .view_version
                .load(std::sync::atomic::Ordering::Relaxed)
        } else {
            // 新客户端：递增 view_version 通知所有客户端 / New client: bump view_version to notify all
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
        let segment_id = Uuid::new_v4();
        let host = host_from_segment_name(&req.segment_name);

        let segment = mooncake_store_core::Segment {
            id: segment_id,
            name: req.segment_name.clone(),
            base: req.base_addr,
            size: req.size,
            te_endpoint: String::new(),
            protocol: String::new(),
        };

        self.state.segments.insert(
            segment_id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: proto::SegmentStatus::Active,
            },
        );
        upsert_client_addresses(&self.state, client_id, vec![host]);
        sync_client_segments(&self.state, client_id);
        register_metadata_segments(
            &self.metadata_state,
            std::slice::from_ref(&req.segment_name),
        )
        .await;

        let mut allocator = self.state.allocator.write();
        allocator.add_segment(segment, 0, client_id);

        bump_view_version(&self.state);
        self.oplog_manager
            .lock()
            .record_mount_segment(&req.segment_name, segment_id, req.size);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::MountSegmentResponse {}))
    }

    // ---- MountNoFSegment ----
    // 客户端挂载 NoF (NVMe-oF) segment：注册到 nof_segments 表和 nof_allocator。
    // Mount NoF segment: register in nof_segments table and nof_allocator.
    pub(crate) async fn mount_nof_segment_impl(
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
        self.state.nof_allocator.write().add_segment(
            mooncake_store_core::Segment {
                id: segment.id,
                name: segment.name.clone(),
                base: segment.base,
                size: segment.size,
                te_endpoint: segment.te_endpoint.clone(),
                protocol: String::new(),
            },
            0,
            client_id,
        );
        bump_view_version(&self.state);
        self.oplog_manager
            .lock()
            .record_mount_nof_segment(&segment.name, segment.id, segment.size);
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

        let segment_name = self
            .state
            .segments
            .get(&segment_id)
            .map(|e| e.segment.name.clone())
            .unwrap_or_default();

        if !unmount_segment_owned(&self.state, segment_id, client_id) {
            return Err(Status::not_found("segment not found for client"));
        }
        bump_view_version(&self.state);
        self.oplog_manager
            .lock()
            .record_unmount_segment(&segment_name, segment_id);
        Ok(Response::new(proto::UnmountSegmentResponse {}))
    }

    // ---- UnmountNoFSegment ----
    // 客户端卸载 NoF segment，同时从 nof_segments 表和 nof_allocator 移除。
    // Unmount NoF segment: remove from nof_segments table and nof_allocator.
    pub(crate) async fn unmount_nof_segment_impl(
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
        let nof_segment_name = self
            .state
            .nof_segments
            .get(&segment_id)
            .map(|e| e.segment.name.clone())
            .unwrap_or_default();

        if !unmount_nof_segment_owned(&self.state, segment_id, client_id) {
            return Err(Status::not_found("NoF segment not found for client"));
        }
        bump_view_version(&self.state);
        self.oplog_manager
            .lock()
            .record_unmount_nof_segment(&nof_segment_name, segment_id);
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
        let owned = self
            .state
            .segments
            .get(&segment_id)
            .map(|entry| entry.client_id == client_id)
            .unwrap_or(false);
        if !owned {
            return Err(Status::not_found("segment not found for client"));
        }
        self.graceful_unmount_scheduler
            .schedule(segment_id, client_id, req.grace_period_ms);
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
            let exists =
                self.state.segments.iter().any(|entry| {
                    entry.client_id == client_id && entry.segment.name == *segment_name
                });
            if exists {
                continue; // 已存在，跳过 / Already exists, skip
            }

            let segment = mooncake_store_core::Segment {
                id: Uuid::new_v4(),
                name: segment_name.clone(),
                base: 0,
                size: *size,
                te_endpoint: String::new(),
                protocol: String::new(),
            };
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
                .add_segment(segment, 0, client_id);
        }
        sync_client_segments(&self.state, client_id);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::ReMountSegmentResponse {}))
    }

    // ---- ReMountNoFSegment ----
    // 重新挂载 NoF segment，按 id/name 去重避免重复注册。
    // Remount NoF segments; deduplicate by id/name to avoid duplicate registration.
    pub(crate) async fn re_mount_nof_segment_impl(
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
            let exists =
                self.state.nof_segments.iter().any(|entry| {
                    entry.segment.id == segment.id || entry.segment.name == segment.name
                });
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
            self.state.nof_allocator.write().add_segment(
                mooncake_store_core::Segment {
                    id: segment.id,
                    name: segment.name.clone(),
                    base: segment.base,
                    size: segment.size,
                    te_endpoint: segment.te_endpoint.clone(),
                    protocol: String::new(),
                },
                0,
                client_id,
            );
        }
        Ok(Response::new(proto::ReMountNoFSegmentResponse {}))
    }

    // ---- MountLocalDiskSegment ----
    // 注册客户端本地磁盘 segment，用于 offload/promotion 功能。按 client_id 去重。
    // Register a local disk segment for offload/promotion; deduplicate by client_id.
    pub(crate) async fn mount_local_disk_segment_impl(
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

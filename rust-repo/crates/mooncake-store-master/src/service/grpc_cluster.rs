//! # Cluster Management — 集群管理 / Cluster Operations
//!
//! 本模块实现 Master 服务的集群级管理操作，包括：
//!
//! This module implements cluster-level management operations for the Master service:
//!
//! ## Segment 挂载/卸载 / Segment Mount/Unmount
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `Ping` | 客户端心跳：更新地址、last_ping，返回 view_version |
//! | `MountSegment` | 挂载 Memory segment 到注册表和分配器 |
//! | `MountNoFSegment` | 挂载 NoF (NVMe-oF) segment |
//! | `UnmountSegment` | 卸载 Memory segment |
//! | `UnmountNoFSegment` | 卸载 NoF segment |
//! | `GracefulUnmountSegment` | 优雅卸载：等待 grace_period 后卸载 |
//! | `ReMountSegment` | 重启后重新挂载：批量注册已有 segment |
//! | `ReMountNoFSegment` | 重启后重新挂载 NoF segment |
//! | `MountLocalDiskSegment` | 注册本地磁盘 segment（offload/promotion 用） |
//!
//! ## Offload/Promotion / 下沉/提升
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `OffloadObjectHeartbeat` | 客户端心跳拉取待 offload 对象列表 |
//! | `ReportSsdCapacity` | 上报本地 SSD 总容量 |
//! | `NotifyOffloadSuccess` | 通知 offload 完成 |
//! | `PromotionObjectHeartbeat` | 客户端心跳拉取待 promotion 对象 |
//! | `PromotionAllocStart` | 为 promotion 分配暂存 Memory 副本 |
//! | `NotifyPromotionSuccess` | 通知 promotion 完成 |
//! | `NotifyPromotionFailure` | 通知 promotion 失败（回滚） |
//!
//! ## Segment 状态查询 / Segment Status Queries
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `QuerySegmentStatus` | 按名称查询 segment 状态 |
//! | `QuerySegmentStatusById` | 按 UUID 查询 segment 状态 |
//! | `CreateDrainJob` | 创建 Drain 迁移任务 |
//! | `QueryDrainJob` | 查询 Drain 任务进度 |
//! | `CancelDrainJob` | 取消 Drain 任务 |
//!
//! ## 远端回源协调 / Remote Pull Coordination
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `AcquireRemotePull` | 获取远端拉取权 |
//! | `CompleteRemotePull` | 通知远端拉取完成 |
//! | `ReleaseRemotePull` | 释放远端拉取权 |

use super::*;

impl MasterServiceImpl {
    // ---- Ping ----
    // 客户端心跳：更新客户端地址、last_ping 时间戳，注册 metadata segments。
    // 新客户端返回 NeedRemount 状态码触发客户端重新挂载；已有客户端仅更新 view_version。
    //
    // Client heartbeat: update client addresses, last_ping timestamp, register metadata segments.
    // New clients get NeedRemount status to trigger remount; existing clients only get updated view_version.
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
        register_metadata_segments(&self.metadata_state, std::slice::from_ref(&req.segment_name)).await;

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
    // 客户端周期性拉取需要 offload 的对象列表并上报心跳。
    // 若客户端禁用 offload，清空其 offload 队列并取消所有待 offload 任务。
    //
    // Client periodically fetches objects needing offload and reports heartbeat.
    // If offload is disabled, clears the client's offload queue and cancels all pending offload tasks.
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
        // Convert scoped keys back to user_keys for external API.
        // 将作用域 key 转换回 user_key，供外部 API 使用。
        let unscoped: HashMap<String, i64> = objects
            .into_iter()
            .map(|(k, v)| {
                let (_t, uk) = split_scoped_key(&k);
                (uk, v)
            })
            .collect();
        Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
            objects: unscoped,
        }))
    }

    // ---- ReportSsdCapacity ----
    // 客户端上报本地 SSD 总容量，供 master 做 offload 容量规划。
    // Client reports local SSD total capacity for master offload capacity planning.
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
    // 客户端通知 offload 完成：为每个 key 创建/更新 LocalDisk 类型副本（状态 Complete），
    // 同时清理 offload 任务。若对象不存在则自动创建并关联到该客户端。
    //
    // Client notifies offload completion: create/update LocalDisk replicas (status Complete) for each key,
    // and clean up offload tasks. If the object does not exist, create it and associate with the client.
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
        for (raw_key, metadata) in req.keys.iter().zip(req.metadatas.iter()) {
            let key = make_tenant_scoped_key("", raw_key);
            clear_offloading_task(&self.state, &key);
            let replica = ReplicaDescriptor {
                refcnt: 0,
                handle_valid: true,
                segment_id: Uuid::nil(),
                segment_name: metadata.transport_endpoint.clone(),
                offset: 0,
                size: metadata.data_size.max(0) as u64,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::LocalDisk,
                holder_client_id: Some(client_id),
                base_addr: 0,
            };
            if let Some(mut object) = self.state.objects.get_mut(&key) {
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
                let (t_id, u_key) = split_scoped_key(&key);
                self.state.objects.insert(
                    key.clone(),
                    ObjectEntry {
                        replicas: vec![replica],
                        size: metadata.data_size.max(0) as u64,
                        last_access: SystemTime::now(),
                        soft_pinned: false,
                        hard_pinned: false,
                        data_type: ObjectDataType::Unknown,
                        client_id: Uuid::nil(),
                        put_start_time: None,
                        lease_timeout: None,
                        soft_pin_timeout: None,
                        tenant_id: t_id,
                        user_key: u_key,
                    },
                );
            }
        }
        Ok(Response::new(proto::NotifyOffloadSuccessResponse {}))
    }

    // ---- PromotionObjectHeartbeat ----
    // 客户端心跳拉取待 promotion 的对象（从本地磁盘提升到内存）。每次返回一个对象并移出队列。
    // Client heartbeat fetches objects pending promotion (local disk → memory). Returns one object at a time and removes from queue.
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
        if let Some((scoped_key, size)) = entry
            .promotion_objects
            .iter()
            .next()
            .map(|(key, size)| (key.clone(), *size))
        {
            let (_t, uk) = split_scoped_key(&scoped_key);
            entry.promotion_objects.remove(&scoped_key);
            objects.insert(uk, size);
        }
        Ok(Response::new(proto::PromotionObjectHeartbeatResponse {
            objects,
        }))
    }

    // ---- PromotionAllocStart ----
    // Promotion 第一阶段：为 promotion 任务分配一个 Memory 副本（staged），返回描述符供客户端
    // RDMA 写入数据。校验 holder_id 和 key 存在性，分配后写入 staged_* 字段供后续追踪。
    //
    // Promotion phase 1: allocate a staged Memory replica for the promotion task,
    // return descriptor for client RDMA write. Validates holder_id and key existence;
    // writes staged_* fields after allocation for tracking.
    pub(super) async fn promotion_alloc_start_impl(
        &self,
        request: Request<proto::PromotionAllocStartRequest>,
    ) -> Result<Response<proto::PromotionAllocStartResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let mut task = self
            .state
            .promotion_tasks
            .get_mut(&scoped_key)
            .ok_or(Status::failed_precondition("promotion task not found"))?;
        if task.holder_id != client_id {
            return Err(Status::permission_denied(
                "promotion task assigned to different client",
            ));
        }
        if task.object_size != req.size {
            return Err(Status::invalid_argument("size mismatch"));
        }
        let object_exists = self.state.objects.contains_key(&scoped_key);
        if !object_exists {
            return Err(Status::not_found("key not found"));
        }

        let mut config = ReplicateConfig::default();
        if let Some(preferred) = req.preferred_segments.first() {
            config.preferred_segment = preferred.clone();
        }
        let replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate_for_client(&scoped_key, Some(client_id), req.size, 1, &config)
        };
        let Some(mut staged) = replicas.into_iter().next() else {
            return Err(Status::resource_exhausted("no available memory segment"));
        };
        sync_segment_usage(&self.state, [staged.segment_id]);
        let staged_segment_id = staged.segment_id;
        let staged_offset = staged.offset;
        staged.status = ReplicaStatus::Allocating;
        if let Some(mut object) = self.state.objects.get_mut(&scoped_key) {
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
    // Promotion 完成通知：将 staged Memory 副本标记为 Complete，清理 promotion 任务和队列。
    // 通过 segment_id+offset+status 精确匹配 promoted replica 以防止误操作。
    //
    // Promotion success notification: mark staged Memory replica as Complete,
    // clean up promotion task and queue. Precisely matches promoted replica by segment_id+offset+status to prevent errors.
    pub(super) async fn notify_promotion_success_impl(
        &self,
        request: Request<proto::NotifyPromotionSuccessRequest>,
    ) -> Result<Response<proto::NotifyPromotionSuccessResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task = self
            .state
            .promotion_tasks
            .get(&scoped_key)
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
            .get_mut(&scoped_key)
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
        clear_promotion_task(&self.state, &scoped_key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&scoped_key);
        }
        if !committed {
            return Err(Status::failed_precondition("promotion replica not ready"));
        }
        Ok(Response::new(proto::NotifyPromotionSuccessResponse {}))
    }

    // ---- NotifyPromotionFailure ----
    // Promotion 失败回滚：释放已分配的 staged Memory 副本，清理任务和队列。
    // 即使 task 不存在也返回成功（幂等），避免客户端重试时出错。
    //
    // Promotion failure rollback: release allocated staged Memory replica, clean up task and queue.
    // Returns success even if the task does not exist (idempotent) to avoid errors on client retry.
    pub(super) async fn notify_promotion_failure_impl(
        &self,
        request: Request<proto::NotifyPromotionFailureRequest>,
    ) -> Result<Response<proto::NotifyPromotionFailureResponse>, Status> {
        let req = request.into_inner();
        let scoped_key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let Some(task) = self
            .state
            .promotion_tasks
            .get(&scoped_key)
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
            release_staged_promotion_replica(&self.state, &scoped_key, segment_id, offset);
        }
        clear_promotion_task(&self.state, &scoped_key);
        if let Some(mut local_disk) = self.state.local_disk_segments.get_mut(&client_id) {
            local_disk.promotion_objects.remove(&scoped_key);
        }
        Ok(Response::new(proto::NotifyPromotionFailureResponse {}))
    }

    // ---- QuerySegmentStatus ----
    // 按名称查询 segment 状态（Active/Draining 等），同时查询 Memory 和 NoF segment。
    // Query segment status by name (Active/Draining, etc.); checks both Memory and NoF segments.
    pub(super) async fn query_segment_status_impl(
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
    // 创建 Drain 任务：将指定 source segments 上的数据迁移到 target segments。
    // 校验源/目标 segment 均处于 Active 状态后，将源 segment 标记为 Draining 并创建 job。
    // 调度后台任务逐 key 执行副本拷贝（ReplicaCopy），支持并发控制和重试。
    //
    // Create a Drain job: migrate data from source segments to target segments.
    // Validates that source/target segments are Active, marks source segments as Draining,
    // creates the job, and schedules per-key ReplicaCopy tasks with concurrency control and retry.
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
        // 校验所有源 segment 存在且处于 Active 状态
        for seg_name in &req.segments {
            let found =
                self.state.segments.iter().any(|e| {
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
        // 校验所有目标 segment 存在且处于 Active 状态
        for tgt_name in &req.target_segments {
            let found =
                self.state.segments.iter().any(|e| {
                    e.segment.name == *tgt_name && e.status == proto::SegmentStatus::Active
                });
            if !found {
                return Err(Status::failed_precondition(format!(
                    "target segment not found or not active: {tgt_name}"
                )));
            }
        }
        // Transition source segments to DRAINING
        // 将源 segment 转为 Draining 状态
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
                terminal_failed_unit_keys: HashSet::new(),
            },
        );
        // Start planning immediately (find objects to drain)
        // 立即开始 planning 阶段（查找需要 drain 的对象）
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
    // 查询 Drain 任务进度：返回状态、成功/失败/阻塞任务数、活跃任务数和已迁移字节数。
    // Query Drain job progress: returns status, succeeded/failed/blocked task counts, active tasks, and migrated bytes.
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
    // 取消 Drain 任务：将 draining segment 恢复为 Active 状态，job 标记为 Canceled。
    // 已处于终态（Success/Failed/Canceled）的 job 不允许重复取消。
    //
    // Cancel Drain job: restore draining segments to Active; mark job as Canceled.
    // Jobs already in terminal state (Success/Failed/Canceled) cannot be re-canceled.
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
        // 将 draining segment 恢复为 Active
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

    /// Helper: 查找 draining segment 上的所有对象并为每个 key 创建 ReplicaCopy 任务。
    /// 目标 segment 按 round-robin 分配，避免单目标热点。每个 key 只创建一个 drain unit。
    ///
    /// Helper: find all objects on draining segments and create ReplicaCopy tasks per key.
    /// Target segments are assigned round-robin to avoid single-target hotspots. One drain unit per key.
    fn schedule_drain_job_tasks(&self, job_id: Uuid) {
        let mut job = match self.state.drain_jobs.get_mut(&job_id) {
            Some(j) => j,
            None => return,
        };
        let draining_segments: HashSet<String> = job.segments.iter().cloned().collect();
        let targets = job.target_segments.clone();
        let max_concurrency = job.max_concurrency as usize;

        // Find objects with replicas on draining segments
        // 查找在 draining segment 上有副本的对象
        let mut units: Vec<(String, String, u64)> = Vec::new(); // (key, source_seg, bytes)
        for entry in self.state.objects.iter() {
            let key = entry.key().clone();
            for replica in &entry.replicas {
                if draining_segments.contains(&replica.segment_name)
                    && replica.status == ReplicaStatus::Complete
                {
                    units.push((key.clone(), replica.segment_name.clone(), replica.size));
                    break; // one unit per key / 每个 key 只创建一个 drain unit
                }
            }
        }

        // Pick a target for each unit (round-robin)
        // 为每个单元选择目标（round-robin 轮询）
        let num_targets = targets.len().max(1);
        for (i, (key, source_seg, _bytes)) in units.into_iter().enumerate() {
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
                    source_segment: source_seg,
                    target_segment: target_seg,
                },
            );

            // Create a copy task for this drain unit
            // 为此 drain unit 创建 Copy 任务
            let payload = serde_json::to_string(&ReplicaCopyPayload {
                key: &key,
                source: &job.active_tasks.get(&task_id).unwrap().source_segment,
                targets: &[job
                    .active_tasks
                    .get(&task_id)
                    .unwrap()
                    .target_segment
                    .clone()],
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
    // 返回 master 配置的存储文件系统目录路径，供客户端本地文件存储使用。
    // Returns the master's configured storage filesystem directory path for client local file storage.
    pub(super) async fn get_fsdir_impl(
        &self,
        _request: Request<proto::GetFsdirRequest>,
    ) -> Result<Response<proto::GetFsdirResponse>, Status> {
        let fs_dir = self.state.runtime_config.storage_fs_dir.clone();
        Ok(Response::new(proto::GetFsdirResponse { fs_dir }))
    }

    // ---- Remote pull coordination ----
    // 分布式协调：确保同一 key 只有一个节点从远端（S3）拉取数据，其余等待。
    //
    // 流程：
    //   Node A miss → AcquireRemotePull(key) → PULL → S3.fetch → PutEnd → CompleteRemotePull
    //   Node B miss → AcquireRemotePull(key) → WAIT → 重试 GetReplicaList（数据已由 A 写入 store）
    //
    // Distributed coordination: ensures only one node pulls a key from remote source (S3);
    // others wait and retry GetReplicaList after data is written to store.

    /// 获取远端拉取权：若已有节点在拉取则返回 WAIT，否则返回 PULL。
    /// Acquire remote pull right: returns WAIT if another node is already pulling, otherwise PULL.
    pub(super) async fn acquire_remote_pull_impl(
        &self,
        request: Request<proto::AcquireRemotePullRequest>,
    ) -> Result<Response<proto::AcquireRemotePullResponse>, Status> {
        let req = request.into_inner();
        let proto_id = req
            .client_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("client_id is required"))?;
        let client_id = uuid_from_proto(proto_id);
        let key = req.key;

        if !self.state.runtime_config.remote_source_enabled {
            return Ok(Response::new(proto::AcquireRemotePullResponse {
                action: proto::RemotePullAction::Abandon as i32,
                retry_after_ms: 0,
            }));
        }

        // Check if another node is already pulling this key
        // 检查是否有其他节点正在拉取此 key
        if let Some(entry) = self.state.pending_remote_pulls.get(&key) {
            let elapsed = entry.started_at.elapsed();
            let ttl = self.state.runtime_config.remote_pull_ttl;
            if elapsed < ttl {
                // 已有节点在拉取，返回 WAIT / Another node is pulling; return WAIT
                return Ok(Response::new(proto::AcquireRemotePullResponse {
                    action: proto::RemotePullAction::Wait as i32,
                    retry_after_ms: (ttl.saturating_sub(elapsed)).as_millis().min(5000) as u64,
                }));
            }
            // Stale entry — remove and let this node pull
            // 条目过期 — 移除并让此节点拉取
            drop(entry);
            self.state.pending_remote_pulls.remove(&key);
        }

        tracing::debug!(key = %key, client = %client_id, "remote pull acquired");
        self.state.pending_remote_pulls.insert(
            key,
            super::state::RemotePullEntry {
                started_at: std::time::Instant::now(),
            },
        );
        Ok(Response::new(proto::AcquireRemotePullResponse {
            action: proto::RemotePullAction::Pull as i32,
            retry_after_ms: 0,
        }))
    }

    /// 完成远端拉取：从 pending_remote_pulls 中移除条目，释放拉取权。
    /// Complete remote pull: remove entry from pending_remote_pulls, release pull right.
    pub(super) async fn complete_remote_pull_impl(
        &self,
        request: Request<proto::CompleteRemotePullRequest>,
    ) -> Result<Response<proto::CompleteRemotePullResponse>, Status> {
        let req = request.into_inner();
        let key = req.key;

        let removed = self.state.pending_remote_pulls.remove(&key);
        tracing::debug!(
            key = %key,
            success = req.success,
            data_size = req.data_size,
            was_present = removed.is_some(),
            "remote pull completed"
        );

        Ok(Response::new(proto::CompleteRemotePullResponse {}))
    }

    /// 释放远端拉取权（放弃拉取）/ Release remote pull right (abandon pull).
    pub(super) async fn release_remote_pull_impl(
        &self,
        request: Request<proto::ReleaseRemotePullRequest>,
    ) -> Result<Response<proto::ReleaseRemotePullResponse>, Status> {
        let req = request.into_inner();
        let key = req.key;

        self.state.pending_remote_pulls.remove(&key);
        tracing::debug!(key = %key, "remote pull released");
        Ok(Response::new(proto::ReleaseRemotePullResponse {}))
    }
}

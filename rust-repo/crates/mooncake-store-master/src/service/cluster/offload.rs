use super::super::*;

impl MasterServiceImpl {
    // ---- OffloadObjectHeartbeat ----
    // 客户端周期性拉取需要 offload 的对象列表并上报心跳。
    // 若客户端禁用 offload，清空其 offload 队列并取消所有待 offload 任务。
    //
    // Client periodically fetches objects needing offload and reports heartbeat.
    // If offload is disabled, clears the client's offload queue and cancels all pending offload tasks.
    pub(crate) async fn offload_object_heartbeat_impl(
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
            let keys: Vec<String> = std::mem::take(&mut entry.offloading_objects)
                .into_keys()
                .collect();
            drop(entry);
            for key in keys {
                clear_offloading_task(&self.state, &key);
            }
            return Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
                objects: HashMap::new(),
                tasks: Vec::new(),
            }));
        }
        let objects = std::mem::take(&mut entry.offloading_objects);
        // Convert scoped keys back to user_keys for external API.
        // 将作用域 key 转换回 user_key，供外部 API 使用。
        let mut tasks = Vec::with_capacity(objects.len());
        let unscoped: HashMap<String, i64> = objects
            .into_iter()
            .map(|(k, v)| {
                let (tenant_id, key) = split_scoped_key(&k);
                tasks.push(proto::OffloadTaskItem {
                    tenant_id,
                    key: key.clone(),
                    size: v,
                });
                (key, v)
            })
            .collect();
        Ok(Response::new(proto::OffloadObjectHeartbeatResponse {
            objects: unscoped,
            tasks,
        }))
    }

    // ---- ReportSsdCapacity ----
    // 客户端上报本地 SSD 总容量，供 master 做 offload 容量规划。
    // Client reports local SSD total capacity for master offload capacity planning.
    pub(crate) async fn report_ssd_capacity_impl(
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
    pub(crate) async fn notify_offload_success_impl(
        &self,
        request: Request<proto::NotifyOffloadSuccessRequest>,
    ) -> Result<Response<proto::NotifyOffloadSuccessResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let task_count = if req.tasks.is_empty() {
            req.keys.len()
        } else {
            req.tasks.len()
        };
        if task_count != req.metadatas.len() {
            return Err(Status::invalid_argument(
                "keys/tasks and metadatas must have same length",
            ));
        }
        let tasks: Vec<proto::OffloadTaskItem> = if req.tasks.is_empty() {
            req.keys
                .iter()
                .map(|key| proto::OffloadTaskItem {
                    tenant_id: String::new(),
                    key: key.clone(),
                    size: 0,
                })
                .collect()
        } else {
            req.tasks.clone()
        };
        for (task, metadata) in tasks.iter().zip(req.metadatas.iter()) {
            let key = make_tenant_scoped_key(&task.tenant_id, &task.key);
            clear_offloading_task(&self.state, &key);
            if metadata.data_size < 0 {
                continue;
            }
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
                protocol: String::new(),
            };
            match self.state.objects.get_mut(&key) {
                Some(mut object) => {
                    if let Some(existing) = object.replicas.iter_mut().find(|existing| {
                        existing.replica_type == ReplicaType::LocalDisk
                            && existing.holder_client_id == Some(client_id)
                    }) {
                        *existing = replica.clone();
                    } else {
                        object.replicas.push(replica);
                    }
                    object.size = metadata.data_size.max(0) as u64;
                    sync_cache_total_accounting(&mut object);
                }
                _ => {
                    let (t_id, u_key) = split_scoped_key(&key);
                    let tenant_id = resolve_request_tenant(&t_id, true)?;
                    let mut object = ObjectEntry {
                        replicas: vec![replica],
                        size: metadata.data_size.max(0) as u64,
                        last_access: SystemTime::now(),
                        hard_pinned: false,
                        data_type: ObjectDataType::Unknown,
                        client_id: Uuid::nil(),
                        put_start_time: None,
                        lease_timeout: None,
                        soft_pin_timeout: None,
                        tenant_id,
                        group_id: String::new(),
                        quota_committed: false,
                        memory_cache_total_accounted: false,
                        disk_cache_total_accounted: false,
                        user_key: u_key,
                    };
                    sync_cache_total_accounting(&mut object);
                    self.state.objects.insert(key.clone(), object);
                }
            }
        }
        Ok(Response::new(proto::NotifyOffloadSuccessResponse {}))
    }
}

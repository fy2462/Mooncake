use super::*;

impl MasterServiceImpl {
    // ---- ExistKey ----
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

    // ---- PutStart ----
    pub(super) async fn put_start_impl(
        &self,
        request: Request<proto::PutStartRequest>,
    ) -> Result<Response<proto::PutStartResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();

        if self.state.objects.contains_key(&key) {
            return Err(Status::already_exists(format!(
                "object already exists: {}",
                key
            )));
        }

        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        let replica_count = if config.replica_num == 0 {
            1
        } else {
            config.replica_num as usize
        };

        let replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate(&key, req.slice_length, replica_count, &config)
        };
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> =
            replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(
            key,
            ObjectEntry {
                replicas,
                size: req.slice_length,
                last_access: SystemTime::now(),
                soft_pinned: config.with_soft_pin,
            },
        );

        metrics::PUT_START_REQUESTS.inc();
        Ok(Response::new(proto::PutStartResponse {
            replicas: proto_replicas,
        }))
    }

    // ---- PutEnd ----
    pub(super) async fn put_end_impl(
        &self,
        request: Request<proto::PutEndRequest>,
    ) -> Result<Response<proto::PutEndResponse>, Status> {
        let req = request.into_inner();
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            for r in &mut entry.replicas {
                if r.status == ReplicaStatus::Allocating {
                    r.status = ReplicaStatus::Complete;
                }
            }
            let size = entry.size;
            drop(entry);
            let client_id = uuid_from_proto(
                req.client_id
                    .as_ref()
                    .ok_or(Status::invalid_argument("missing client_id"))?,
            );
            push_offloading_queue(&self.state, client_id, &req.key, size);
        }
        Ok(Response::new(proto::PutEndResponse {}))
    }

    // ---- AddReplica ----
    pub(super) async fn add_replica_impl(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
        let req = request.into_inner();
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
                },
            );
        }
        Ok(Response::new(proto::AddReplicaResponse {}))
    }

    // ---- GetReplicaList ----
    pub(super) async fn get_replica_list_impl(
        &self,
        request: Request<proto::GetReplicaListRequest>,
    ) -> Result<Response<proto::GetReplicaListResponse>, Status> {
        let req = request.into_inner();
        match self.state.objects.get_mut(&req.key) {
            Some(mut entry) => {
                entry.last_access = SystemTime::now();
                let replicas = entry.replicas.iter().map(replica_to_proto).collect();
                let promotion_eligible = !entry.replicas.iter().any(|replica| {
                    replica.replica_type == ReplicaType::Memory
                        && replica.status == ReplicaStatus::Complete
                }) && entry.replicas.iter().any(|replica| {
                    replica.replica_type == ReplicaType::LocalDisk
                        && replica.status == ReplicaStatus::Complete
                });
                drop(entry);
                if promotion_eligible {
                    try_push_promotion_queue(&self.state, &req.key);
                }
                metrics::GET_REQUESTS.inc();
                Ok(Response::new(proto::GetReplicaListResponse { replicas }))
            }
            None => Err(Status::not_found(format!("key not found: {}", req.key))),
        }
    }

    // ---- Remove ----
    pub(super) async fn remove_impl(
        &self,
        request: Request<proto::RemoveRequest>,
    ) -> Result<Response<proto::RemoveResponse>, Status> {
        let req = request.into_inner();
        if let Some((_, object)) = self.state.objects.remove(&req.key) {
            clear_offloading_task(&self.state, &req.key);
            clear_promotion_task(&self.state, &req.key);
            let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
            self.state.allocator.write().release(&object.replicas);
            sync_segment_usage(&self.state, segment_ids);
        }
        metrics::REMOVE_REQUESTS.inc();
        Ok(Response::new(proto::RemoveResponse {}))
    }

    // ---- RemoveByRegex ----
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
            if let Some((_, object)) = self.state.objects.remove(&key) {
                clear_offloading_task(&self.state, &key);
                clear_promotion_task(&self.state, &key);
                let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&object.replicas);
                sync_segment_usage(&self.state, segment_ids);
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
    pub(super) async fn query_segments_impl(
        &self,
        request: Request<proto::QuerySegmentsRequest>,
    ) -> Result<Response<proto::QuerySegmentsResponse>, Status> {
        let req = request.into_inner();
        for entry in self.state.segments.iter() {
            if entry.segment.name == req.segment_name {
                return Ok(Response::new(proto::QuerySegmentsResponse {
                    total_size: entry.segment.size,
                    used_size: entry.segment.used,
                }));
            }
        }
        Err(Status::not_found("segment not found"))
    }

    // ---- QueryIp ----
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
    pub(super) async fn upsert_impl(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        let req = request.into_inner();
        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        let replica_count = if config.replica_num == 0 {
            1
        } else {
            config.replica_num as usize
        };

        let replicas = if let Some(existing) = self.state.objects.get(&req.key) {
            if existing.size == req.slice_length {
                existing.replicas.clone()
            } else {
                let old_replicas = existing.replicas.clone();
                drop(existing);
                let segment_ids: Vec<Uuid> = old_replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&old_replicas);
                sync_segment_usage(&self.state, segment_ids);
                let mut allocator = self.state.allocator.write();
                allocator.allocate(&req.key, req.slice_length, replica_count, &config)
            }
        } else {
            let mut allocator = self.state.allocator.write();
            allocator.allocate(&req.key, req.slice_length, replica_count, &config)
        };
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> =
            replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(
            req.key.clone(),
            ObjectEntry {
                replicas,
                size: req.slice_length,
                last_access: SystemTime::now(),
                soft_pinned: config.with_soft_pin,
            },
        );

        Ok(Response::new(proto::UpsertResponse {
            replicas: proto_replicas,
        }))
    }
}

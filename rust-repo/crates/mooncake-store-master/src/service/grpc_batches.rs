use super::*;

impl MasterServiceImpl {
    // ---- BatchExistKey ----
    pub(super) async fn batch_exist_key_impl(
        &self,
        request: Request<proto::BatchExistKeyRequest>,
    ) -> Result<Response<proto::BatchExistKeyResponse>, Status> {
        let req = request.into_inner();
        let results: Vec<bool> = req
            .keys
            .iter()
            .map(|k| self.state.objects.contains_key(k))
            .collect();
        Ok(Response::new(proto::BatchExistKeyResponse { results }))
    }

    // ---- BatchQueryIp ----
    pub(super) async fn batch_query_ip_impl(
        &self,
        request: Request<proto::BatchQueryIpRequest>,
    ) -> Result<Response<proto::BatchQueryIpResponse>, Status> {
        let req = request.into_inner();
        let mut ips = std::collections::HashMap::new();
        for cid in &req.client_ids {
            let id = uuid_from_proto(cid);
            let addresses = addresses_for_client(&self.state, id);
            if !addresses.is_empty() {
                ips.insert(id.to_string(), proto::IpList { addresses });
            }
        }
        Ok(Response::new(proto::BatchQueryIpResponse { ips }))
    }

    // ---- BatchReplicaClear ----
    pub(super) async fn batch_replica_clear_impl(
        &self,
        request: Request<proto::BatchReplicaClearRequest>,
    ) -> Result<Response<proto::BatchReplicaClearResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let clear_all_segments = req.segment_name.is_empty();
        let mut cleared = vec![];
        for key in &req.object_keys {
            let mut remove_entire_object = false;
            let mut removed_replicas = Vec::new();
            let mut had_match = false;
            if let Some(mut object) = self.state.objects.get_mut(key) {
                if object_owner_client_id(&self.state, &object) != Some(client_id) {
                    continue;
                }
                if object
                    .replicas
                    .iter()
                    .any(|replica| replica.status != ReplicaStatus::Complete)
                {
                    continue;
                }
                if clear_all_segments {
                    had_match = !object.replicas.is_empty();
                    removed_replicas = object.replicas.clone();
                    remove_entire_object = had_match;
                } else {
                    let segment_name = req.segment_name.as_str();
                    object.replicas.retain(|replica| {
                        let matches = replica.segment_name == segment_name;
                        if matches {
                            had_match = true;
                            removed_replicas.push(replica.clone());
                        }
                        !matches
                    });
                    remove_entire_object = object.replicas.is_empty() && had_match;
                }
            }
            if had_match {
                clear_offloading_task(&self.state, key);
                clear_promotion_task(&self.state, key);
                release_replicas(&self.state, &removed_replicas);
                if remove_entire_object {
                    self.state.objects.remove(key);
                }
                cleared.push(key.clone());
            }
        }
        Ok(Response::new(proto::BatchReplicaClearResponse {
            cleared_keys: cleared,
        }))
    }

    // ---- BatchPutEnd ----
    pub(super) async fn batch_put_end_impl(
        &self,
        request: Request<proto::BatchPutEndRequest>,
    ) -> Result<Response<proto::BatchPutEndResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .entries
            .iter()
            .map(|entry| {
                if let Some(mut obj) = self.state.objects.get_mut(&entry.key) {
                    let size = obj.size;
                    for r in &mut obj.replicas {
                        let matches_type = match entry.replica_type {
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
                            r.status = ReplicaStatus::Complete;
                        }
                    }
                    drop(obj);
                    if let Some(client_id) = entry.client_id.as_ref().map(uuid_from_proto) {
                        push_offloading_queue(&self.state, client_id, &entry.key, size);
                    }
                    0
                } else {
                    -1
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutEndResponse { statuses }))
    }

    // ---- BatchPutRevoke ----
    pub(super) async fn batch_put_revoke_impl(
        &self,
        request: Request<proto::BatchPutRevokeRequest>,
    ) -> Result<Response<proto::BatchPutRevokeResponse>, Status> {
        let req = request.into_inner();
        let client_id = req
            .client_id
            .as_ref()
            .map(uuid_from_proto);
        let segment_name = if req.segment_name.is_empty() {
            None
        } else {
            Some(req.segment_name.clone())
        };
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|key| {
                if self.state.replication_tasks.contains_key(key) {
                    return -2;
                }
                if let Some(mut object) = self.state.objects.get_mut(key) {
                    if let Some(cid) = client_id {
                        if object_owner_client_id(&self.state, &object) != Some(cid) {
                            return -3;
                        }
                    }
                    let mut removed = Vec::new();
                    object.replicas.retain(|replica| {
                        let matched = segment_name
                            .as_ref()
                            .map_or(true, |seg| replica.segment_name == *seg);
                        if matched {
                            removed.push(replica.clone());
                        }
                        !matched
                    });
                    let remove_object = object.replicas.is_empty();
                    drop(object);
                    clear_offloading_task(&self.state, key);
                    clear_promotion_task(&self.state, key);
                    release_replicas(&self.state, &removed);
                    if remove_object {
                        self.state.objects.remove(key);
                    }
                    0
                } else {
                    -1
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutRevokeResponse { statuses }))
    }

    // ---- BatchRemove ----
    pub(super) async fn batch_remove_impl(
        &self,
        request: Request<proto::BatchRemoveRequest>,
    ) -> Result<Response<proto::BatchRemoveResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|key| {
                if self.state.replication_tasks.contains_key(key) {
                    return -2;
                }
                if let Some((_, object)) = self.state.objects.remove(key) {
                    clear_offloading_task(&self.state, key);
                    clear_promotion_task(&self.state, key);
                    release_replicas(&self.state, &object.replicas);
                }
                0
            })
            .collect();
        metrics::BATCH_REMOVE_REQUESTS.inc_by(req.keys.len() as u64);
        Ok(Response::new(proto::BatchRemoveResponse { statuses }))
    }

    // ---- BatchUpsertEnd ----
    pub(super) async fn batch_upsert_end_impl(
        &self,
        request: Request<proto::BatchUpsertEndRequest>,
    ) -> Result<Response<proto::BatchUpsertEndResponse>, Status> {
        let req = request.into_inner();
        let mut all_replicas = Vec::new();

        for entry in &req.entries {
            let config = entry
                .config
                .as_ref()
                .map(config_from_proto)
                .unwrap_or_default();
            if config.replica_num == 0 && config.nof_replica_num == 0 {
                continue;
            }
            let replica_count = config.replica_num.max(1) as usize;

            let replicas = if let Some(existing) = self.state.objects.get(&entry.key) {
                if existing.size == entry.slice_length {
                    existing.replicas.clone()
                } else {
                    let old_replicas = existing.replicas.clone();
                    drop(existing);
                    release_replicas(&self.state, &old_replicas);
                    let mut allocator = self.state.allocator.write();
                    allocator.allocate_for_client(
                        &entry.key,
                        entry.client_id.as_ref().map(uuid_from_proto),
                        entry.slice_length,
                        replica_count,
                        &config,
                    )
                }
            } else {
                let mut allocator = self.state.allocator.write();
                allocator.allocate_for_client(
                    &entry.key,
                    entry.client_id.as_ref().map(uuid_from_proto),
                    entry.slice_length,
                    replica_count,
                    &config,
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
                    continue;
                }
                let nof_replicas = allocate_nof_replicas(
                    &self.state,
                    &entry.key,
                    entry.slice_length,
                    config.nof_replica_num as usize,
                    &preferred_nof,
                )?;
                replicas.extend(nof_replicas);
            }
            let proto_r: Vec<proto::ReplicaDescriptor> =
                replicas.iter().map(replica_to_proto).collect();
            sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));
            self.state.objects.insert(
                entry.key.clone(),
                ObjectEntry {
                    replicas,
                    size: entry.slice_length,
                    last_access: SystemTime::now(),
                    soft_pinned: config.with_soft_pin,
                    hard_pinned: config.with_hard_pin,
                },
            );
            all_replicas.extend(proto_r);
        }

        Ok(Response::new(proto::BatchUpsertEndResponse {
            replicas: all_replicas,
        }))
    }
}

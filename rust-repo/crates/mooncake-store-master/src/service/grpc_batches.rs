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
        let mut cleared = vec![];
        for key in &req.object_keys {
            if let Some((_, object)) = self.state.objects.remove(key) {
                clear_offloading_task(&self.state, key);
                clear_promotion_task(&self.state, key);
                let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&object.replicas);
                sync_segment_usage(&self.state, segment_ids);
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
                        if r.status == ReplicaStatus::Allocating {
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
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|key| {
                if let Some((_, object)) = self.state.objects.remove(key) {
                    clear_offloading_task(&self.state, key);
                    clear_promotion_task(&self.state, key);
                    let segment_ids: Vec<Uuid> =
                        object.replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&object.replicas);
                    sync_segment_usage(&self.state, segment_ids);
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
                if let Some((_, object)) = self.state.objects.remove(key) {
                    clear_offloading_task(&self.state, key);
                    clear_promotion_task(&self.state, key);
                    let segment_ids: Vec<Uuid> =
                        object.replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&object.replicas);
                    sync_segment_usage(&self.state, segment_ids);
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
            let replica_count = if config.replica_num == 0 {
                1
            } else {
                config.replica_num as usize
            };

            let replicas = if let Some(existing) = self.state.objects.get(&entry.key) {
                if existing.size == entry.slice_length {
                    existing.replicas.clone()
                } else {
                    let old_replicas = existing.replicas.clone();
                    drop(existing);
                    let segment_ids: Vec<Uuid> =
                        old_replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&old_replicas);
                    sync_segment_usage(&self.state, segment_ids);
                    let mut allocator = self.state.allocator.write();
                    allocator.allocate(&entry.key, entry.slice_length, replica_count, &config)
                }
            } else {
                let mut allocator = self.state.allocator.write();
                allocator.allocate(&entry.key, entry.slice_length, replica_count, &config)
            };

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
                },
            );
            all_replicas.extend(proto_r);
        }

        Ok(Response::new(proto::BatchUpsertEndResponse {
            replicas: all_replicas,
        }))
    }
}

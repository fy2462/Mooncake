use super::*;

impl MasterServiceImpl {
    // ---- EvictDiskReplica ----
    // 驱逐一个对象的指定类型磁盘副本（Disk/LocalDisk/All），若所有副本被移除则删除对象。
    pub(in crate::service) async fn evict_disk_replica_impl(
        &self,
        request: Request<proto::EvictDiskReplicaRequest>,
    ) -> Result<Response<proto::EvictDiskReplicaResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let key = tenant_id.make_scoped_key(&req.key);
        let target = request_replica_type_from_i32(req.replica_type)?;
        if target != ReplicaType::Disk && target != ReplicaType::LocalDisk {
            return Err(Status::invalid_argument(
                "evict_disk_replica only supports Disk or LocalDisk",
            ));
        }
        let storage_id = if target == ReplicaType::LocalDisk {
            Some(ready_local_disk_storage_for_client(&self.state, client_id)?)
        } else {
            None
        };
        let _mutation_guard = self.state.key_mutations.lock(&key);
        match self.state.objects.get_mut(&key) {
            Some(mut entry) => {
                entry.replicas.retain(|r| match target {
                    ReplicaType::Disk => r.replica_type != ReplicaType::Disk,
                    ReplicaType::LocalDisk => {
                        !(r.replica_type == ReplicaType::LocalDisk
                            && r.local_disk_storage_id == storage_id)
                    }
                    _ => true,
                });
                sync_cache_total_accounting(&mut entry);
                let remove_object = entry.replicas.is_empty();
                drop(entry);
                let removed_object = remove_object
                    .then(|| self.state.objects.remove(&key).map(|(_, object)| object))
                    .flatten();
                if let Some(object) = &removed_object {
                    self.account_removed_object_quota(object)?;
                }
                self.persist_object_image_or_remove(&key, "evict_disk_replica")?;
                if let Some(object) = &removed_object {
                    self.publish_kv_removed_with_medium(&key, object, "disk");
                }
            }
            _ => {
                return Err(Status::not_found("key not found"));
            }
        }
        Ok(Response::new(proto::EvictDiskReplicaResponse {}))
    }

    // ---- BatchEvictDiskReplica ----
    // 批量驱逐多个对象的磁盘副本，按 replica_type 过滤要驱逐的副本类型。
    pub(in crate::service) async fn batch_evict_disk_replica_impl(
        &self,
        request: Request<proto::BatchEvictDiskReplicaRequest>,
    ) -> Result<Response<proto::BatchEvictDiskReplicaResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = resolve_request_tenant(
            &req.tenant_id,
            self.state.runtime_config.enable_tenant_quota,
        )?;
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let target = request_replica_type_from_i32(req.replica_type)?;
        if target != ReplicaType::Disk && target != ReplicaType::LocalDisk {
            return Err(Status::invalid_argument(
                "batch_evict_disk_replica only supports Disk or LocalDisk",
            ));
        }
        let storage_id = if target == ReplicaType::LocalDisk {
            Some(ready_local_disk_storage_for_client(&self.state, client_id)?)
        } else {
            None
        };
        let mut statuses = Vec::with_capacity(req.keys.len());
        for raw_key in &req.keys {
            let key = tenant_id.make_scoped_key(raw_key);
            let _mutation_guard = self.state.key_mutations.lock(&key);
            if let Some(mut entry) = self.state.objects.get_mut(&key) {
                entry.replicas.retain(|r| match target {
                    ReplicaType::Disk => r.replica_type != ReplicaType::Disk,
                    ReplicaType::LocalDisk => {
                        !(r.replica_type == ReplicaType::LocalDisk
                            && r.local_disk_storage_id == storage_id)
                    }
                    _ => true,
                });
                sync_cache_total_accounting(&mut entry);
                let remove_object = entry.replicas.is_empty();
                drop(entry);
                let removed_object = remove_object
                    .then(|| self.state.objects.remove(&key).map(|(_, object)| object))
                    .flatten();
                if let Some(object) = &removed_object {
                    self.account_removed_object_quota(object)?;
                }
                self.persist_object_image_or_remove(&key, "batch_evict_disk_replica")?;
                if let Some(object) = &removed_object {
                    self.publish_kv_removed_with_medium(&key, object, "disk");
                }
                statuses.push(BatchStatus::Success.into());
            } else {
                // No mutation occurred, so do not manufacture a durable remove
                // marker for a missing key.
                statuses.push(BatchStatus::KeyNotFound.into());
            }
        }
        Ok(Response::new(proto::BatchEvictDiskReplicaResponse {
            statuses,
        }))
    }
}

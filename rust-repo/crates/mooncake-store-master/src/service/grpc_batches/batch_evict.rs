use super::*;

impl MasterServiceImpl {
    // ---- EvictDiskReplica ----
    // 驱逐一个对象的指定类型磁盘副本（Disk/LocalDisk/All），若所有副本被移除则删除对象。
    pub(in crate::service) async fn evict_disk_replica_impl(
        &self,
        request: Request<proto::EvictDiskReplicaRequest>,
    ) -> Result<Response<proto::EvictDiskReplicaResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let key = make_tenant_scoped_key(&req.tenant_id, &req.key);
        let target = replica_type_from_i32(req.replica_type);
        if target != ReplicaType::Disk && target != ReplicaType::LocalDisk {
            return Err(Status::invalid_argument(
                "evict_disk_replica only supports Disk or LocalDisk",
            ));
        }
        if let Some(mut entry) = self.state.objects.get_mut(&key) {
            entry.replicas.retain(|r| match target {
                ReplicaType::Disk => r.replica_type != ReplicaType::Disk,
                ReplicaType::LocalDisk => {
                    !(r.replica_type == ReplicaType::LocalDisk
                        && r.holder_client_id == Some(client_id))
                }
                _ => true,
            });
            let remove_object = entry.replicas.is_empty();
            drop(entry);
            if remove_object {
                self.state.objects.remove(&key);
            }
        } else {
            return Err(Status::not_found("key not found"));
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
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let target = replica_type_from_i32(req.replica_type);
        if target != ReplicaType::Disk && target != ReplicaType::LocalDisk {
            return Err(Status::invalid_argument(
                "batch_evict_disk_replica only supports Disk or LocalDisk",
            ));
        }
        for raw_key in &req.keys {
            let key = make_tenant_scoped_key(&req.tenant_id, raw_key);
            if let Some(mut entry) = self.state.objects.get_mut(&key) {
                entry.replicas.retain(|r| match target {
                    ReplicaType::Disk => r.replica_type != ReplicaType::Disk,
                    ReplicaType::LocalDisk => {
                        !(r.replica_type == ReplicaType::LocalDisk
                            && r.holder_client_id == Some(client_id))
                    }
                    _ => true,
                });
                let remove_object = entry.replicas.is_empty();
                drop(entry);
                if remove_object {
                    self.state.objects.remove(&key);
                }
            }
        }
        Ok(Response::new(proto::BatchEvictDiskReplicaResponse {}))
    }
}

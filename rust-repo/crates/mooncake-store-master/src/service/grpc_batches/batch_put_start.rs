use super::*;

impl MasterServiceImpl {
    // ---- BatchPutStart ----
    // 批量 PutStart：为多个 key 同时分配副本并注册对象。跳过已存在的 key。
    // 所有 key 共享同一份 ReplicateConfig，适用于批量初始化场景。
    pub(in crate::service) async fn batch_put_start_impl(
        &self,
        request: Request<proto::BatchPutStartRequest>,
    ) -> Result<Response<proto::BatchPutStartResponse>, Status> {
        let req = request.into_inner();
        if req.keys.len() != req.slice_lengths.len() || req.keys.is_empty() {
            return Err(Status::invalid_argument(
                "keys and slice_lengths mismatch or empty",
            ));
        }
        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        if config.replica_num == 0 && config.nof_replica_num == 0 {
            return Err(Status::invalid_argument(
                "replica_num and nof_replica_num cannot both be zero",
            ));
        }
        if config.prefer_alloc_in_same_node && config.nof_replica_num > 0 {
            return Err(Status::invalid_argument(
                "prefer_alloc_in_same_node is not supported with NoF replicas",
            ));
        }
        if config.nof_replica_num > 0 && !self.state.runtime_config.enable_nof {
            return Err(Status::invalid_argument("NoF is not enabled"));
        }
        let memory_replica_count = config.replica_num as usize;
        let nof_replica_count = config.nof_replica_num as usize;
        let mut all_replicas = Vec::new();
        let mut results = Vec::with_capacity(req.keys.len());
        let invalid_group_ids =
            !config.group_ids.is_empty() && config.group_ids.len() != req.keys.len();
        for (idx, (raw_key, slice_len)) in req.keys.iter().zip(req.slice_lengths.iter()).enumerate()
        {
            if invalid_group_ids {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: normalize_tenant_id(&req.tenant_id),
                });
                continue;
            }
            if *slice_len == 0 {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: normalize_tenant_id(&req.tenant_id),
                });
                continue;
            }
            let key = make_tenant_scoped_key(&req.tenant_id, raw_key);
            let group_id = Self::group_id_for_key(&config, req.keys.len(), idx)
                .map_err(|_| Status::invalid_argument("invalid group_ids"))?;
            if self.state.objects.contains_key(&key) {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::ObjectAlreadyExists.into(),
                    tenant_id: normalize_tenant_id(&req.tenant_id),
                });
                continue;
            }
            let mut replicas = {
                let mut allocator = self.state.allocator.write();
                allocator.allocate_for_client(
                    &key,
                    Some(client_id),
                    *slice_len,
                    memory_replica_count,
                    &config,
                )
            };
            if replicas.len() != memory_replica_count {
                release_replicas(&self.state, &replicas);
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: normalize_tenant_id(&req.tenant_id),
                });
                continue;
            }
            if nof_replica_count > 0 {
                match allocate_nof_replicas(
                    &self.state,
                    &key,
                    *slice_len,
                    nof_replica_count,
                    &config.preferred_nof_segments,
                ) {
                    Ok(nof_replicas) => replicas.extend(nof_replicas),
                    Err(_) => {
                        release_replicas(&self.state, &replicas);
                        results.push(proto::BatchStartEntryResult {
                            key: raw_key.clone(),
                            replicas: vec![],
                            status: BatchStatus::InvalidState.into(),
                            tenant_id: normalize_tenant_id(&req.tenant_id),
                        });
                        continue;
                    }
                }
            }
            if replicas.len() == memory_replica_count + nof_replica_count {
                let proto_r: Vec<_> = replicas.iter().map(replica_to_proto).collect();
                sync_segment_usage(
                    &self.state,
                    replicas
                        .iter()
                        .filter(|r| r.replica_type == ReplicaType::Memory)
                        .map(|r| r.segment_id),
                );
                sync_nof_segment_usage(
                    &self.state,
                    replicas
                        .iter()
                        .filter(|r| r.replica_type == ReplicaType::NoFSsd)
                        .map(|r| r.segment_id),
                );
                let now = SystemTime::now();
                let (t_id, u_key) = split_scoped_key(&key);
                self.state.objects.insert(
                    key.clone(),
                    ObjectEntry {
                        replicas,
                        size: *slice_len,
                        last_access: now,
                        hard_pinned: config.with_hard_pin,
                        data_type: config.data_type,
                        client_id,
                        put_start_time: Some(now),
                        lease_timeout: None,
                        soft_pin_timeout: if config.with_soft_pin {
                            crate::metrics::SOFT_PIN_KEY_COUNT.inc();
                            Some(SystemTime::UNIX_EPOCH)
                        } else {
                            None
                        },
                        tenant_id: t_id,
                        group_id,
                        user_key: u_key,
                    },
                );
                self.state.processing_keys.insert(key.clone(), ());
                all_replicas.extend(proto_r.iter().cloned());
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: proto_r,
                    status: BatchStatus::Success.into(),
                    tenant_id: normalize_tenant_id(&req.tenant_id),
                });
            } else {
                release_replicas(&self.state, &replicas);
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: normalize_tenant_id(&req.tenant_id),
                });
            }
        }
        metrics::PUT_START_REQUESTS.inc_by(req.keys.len() as u64);
        Ok(Response::new(proto::BatchPutStartResponse {
            replicas: all_replicas,
            results,
        }))
    }
}

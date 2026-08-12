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
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        if req.keys.is_empty() {
            return Err(Status::invalid_argument("batch keys must not be empty"));
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
        let disk_enabled = !storage_fs_dir_for_client(&self.state.runtime_config).is_empty();
        if config.replica_num == 0 && config.nof_replica_num == 0 && !disk_enabled {
            return Err(Status::invalid_argument(
                "replica_num and nof_replica_num cannot both be zero when global DISK is disabled",
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
        let tenant_id_wire = tenant_id.as_str().to_owned();
        let scoped_keys = req
            .keys
            .iter()
            .map(|key| tenant_id.make_scoped_key(key))
            .collect::<Vec<_>>();
        let mut all_replicas = Vec::new();
        let mut results = Vec::with_capacity(req.keys.len());
        let invalid_group_ids =
            !config.group_ids.is_empty() && config.group_ids.len() != req.keys.len();
        let invalid_size_count = req.slice_lengths.len() != req.keys.len();
        for (idx, raw_key) in req.keys.iter().enumerate() {
            if invalid_group_ids || invalid_size_count {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            }
            let slice_len = req.slice_lengths[idx];
            if slice_len == 0 {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            }
            if raw_key.is_empty() || validate_user_key(raw_key).is_err() {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            }
            let key = scoped_keys[idx].clone();
            // C++ BatchPutStart delegates to PutStart per key. Mirror that
            // boundary so each key keeps its operation stripe across quota
            // eviction retries without holding unrelated batch stripes.
            let _operation_guard = self.state.key_mutations.lock_operation(&key);
            let mutation_guard = self.state.key_mutations.lock(&key);
            let alive_clients = get_alive_clients_snapshot(&self.state);
            clear_invalid_handles_for_key_locked(&self.state, &alive_clients, &key).map_err(
                |error| Status::unavailable(format!("stale handle cleanup failed: {error}")),
            )?;
            let group_id = Self::group_id_for_key(&config, req.keys.len(), idx)
                .map_err(|_| Status::invalid_argument("invalid group_ids"))?;
            if self.state.objects.contains_key(&key) {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::ObjectAlreadyExists.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            }
            let Ok(requested_quota_charge) =
                checked_requested_memory_quota_charge(slice_len, memory_replica_count)
            else {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            };
            // Quota admission may evict arbitrary keys and therefore cannot
            // run while this key owns its mutation stripe. The operation
            // stripe remains held, then the key is revalidated after quota
            // admission.
            drop(mutation_guard);
            let quota_result = self.reserve_tenant_quota_with_eviction(
                &tenant_id,
                requested_quota_charge,
                Some(&key),
            );
            let _mutation_guard = self.state.key_mutations.lock(&key);
            if quota_result.is_err() {
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            }
            if self.state.objects.contains_key(&key) {
                self.abort_tenant_quota(&tenant_id, requested_quota_charge)?;
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::ObjectAlreadyExists.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            }
            let mut replicas = allocate_memory_replicas(
                &self.state,
                &key,
                Some(client_id),
                slice_len,
                memory_replica_count,
                &config,
            );
            if replicas.len() != memory_replica_count {
                release_replicas(&self.state, &replicas)?;
                self.abort_tenant_quota(&tenant_id, requested_quota_charge)?;
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
                continue;
            }
            if nof_replica_count > 0 {
                match allocate_nof_replicas(
                    &self.state,
                    &key,
                    slice_len,
                    nof_replica_count,
                    &config.preferred_nof_segments,
                ) {
                    Ok(nof_replicas) => replicas.extend(nof_replicas),
                    Err(_) => {
                        release_replicas(&self.state, &replicas)?;
                        self.abort_tenant_quota(&tenant_id, requested_quota_charge)?;
                        results.push(proto::BatchStartEntryResult {
                            key: raw_key.clone(),
                            replicas: vec![],
                            status: BatchStatus::InvalidState.into(),
                            tenant_id: tenant_id_wire.clone(),
                        });
                        continue;
                    }
                }
            }
            if let Some(disk_replica) = global_disk_replica(&self.state, &key, slice_len) {
                replicas.push(disk_replica);
            }
            let expected_replica_count =
                memory_replica_count + nof_replica_count + usize::from(disk_enabled);
            if replicas.len() == expected_replica_count {
                let proto_r: Vec<_> = replicas
                    .iter()
                    .map(|replica| replica_to_proto_for_state(&self.state, replica))
                    .collect();
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
                self.state.objects.insert(
                    key.clone(),
                    ObjectEntry {
                        replicas,
                        size: slice_len,
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
                        tenant_id: tenant_id.clone(),
                        group_id,
                        quota_committed: false,
                        reserved_quota_charge_bytes: requested_quota_charge,
                        committed_quota_charge_bytes: 0,
                        pending_replaced_quota_charge_bytes: 0,
                        memory_cache_total_accounted: false,
                        disk_cache_total_accounted: false,
                        user_key: raw_key.clone(),
                    },
                );
                self.register_tenant_metadata_object(&tenant_id);
                self.state.processing_keys.insert(key.clone(), ());
                // Each successful entry exposes writable addresses and must be
                // durable independently before its success result is returned.
                self.persist_object_image_or_remove(&key, "batch_put_start")?;
                all_replicas.extend(proto_r.iter().cloned());
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: proto_r,
                    status: BatchStatus::Success.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
            } else {
                release_replicas(&self.state, &replicas)?;
                self.abort_tenant_quota(&tenant_id, requested_quota_charge)?;
                results.push(proto::BatchStartEntryResult {
                    key: raw_key.clone(),
                    replicas: vec![],
                    status: BatchStatus::InvalidState.into(),
                    tenant_id: tenant_id_wire.clone(),
                });
            }
        }
        metrics::PUT_START_REQUESTS.inc_by(req.keys.len() as u64);
        let failed = results.iter().filter(|result| result.status != 0).count();
        metrics::record_batch_outcome(
            results.len(),
            failed,
            &metrics::BATCH_PUT_START_REQUESTS,
            &metrics::BATCH_PUT_START_FAILURES,
            &metrics::BATCH_PUT_START_PARTIAL_SUCCESSES,
            &metrics::BATCH_PUT_START_ITEMS,
            &metrics::BATCH_PUT_START_FAILED_ITEMS,
        );
        Ok(Response::new(proto::BatchPutStartResponse {
            replicas: all_replicas,
            results,
        }))
    }
}

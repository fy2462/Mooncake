use super::*;

impl MasterServiceImpl {
    fn schedule_delayed_release(&self, replicas: Vec<ReplicaDescriptor>) {
        if replicas.is_empty() {
            return;
        }
        let state = self.state.clone();
        let delay = self.state.runtime_config.put_start_release_timeout;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            release_replicas(&state, &replicas);
        });
    }

    fn reconcile_soft_pin(
        enable_soft_pin: bool,
        previous: Option<SystemTime>,
    ) -> Option<SystemTime> {
        match (enable_soft_pin, previous) {
            (true, None) => {
                crate::metrics::SOFT_PIN_KEY_COUNT.inc();
                Some(SystemTime::UNIX_EPOCH)
            }
            (true, Some(existing)) => Some(existing),
            (false, Some(_)) => {
                crate::metrics::SOFT_PIN_KEY_COUNT.dec();
                None
            }
            (false, None) => None,
        }
    }

    pub(crate) fn upsert_start_for_entry(
        &self,
        client_id: Uuid,
        user_key: &str,
        tenant_id: &TenantId,
        slice_length: u64,
        config: ReplicateConfig,
    ) -> Result<Vec<ReplicaDescriptor>, Status> {
        if user_key.is_empty() {
            return Err(Status::invalid_argument("empty key"));
        }
        if slice_length == 0 {
            return Err(Status::invalid_argument("zero slice_length"));
        }
        validate_user_key(user_key)?;
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
        if !self.state.runtime_config.enable_nof && config.nof_replica_num > 0 {
            return Err(Status::invalid_argument("NoF is not enabled"));
        }
        let requested_group_id = Self::group_id_for_key(&config, 1, 0)?;

        let scoped_key = tenant_id.make_scoped_key(user_key);
        if self.state.replication_tasks.contains_key(&scoped_key) {
            return Err(Status::failed_precondition("object has replication task"));
        }
        if self.state.offloading_tasks.contains_key(&scoped_key) {
            return Err(Status::failed_precondition("object has offloading task"));
        }

        let replica_count = config.replica_num as usize;
        let now = SystemTime::now();
        if let Some(mut existing) = self.state.objects.get_mut(&scoped_key) {
            let alive_clients = get_alive_clients_snapshot(&self.state);
            let should_remove = cleanup_stale_handles(&mut existing, &alive_clients);
            if should_remove {
                drop(existing);
                if let Some((_, removed)) = self.state.objects.remove(&scoped_key) {
                    self.account_removed_object_quota(&removed);
                }
            } else {
                if existing.replicas.iter().any(|r| r.refcnt > 0) {
                    return Err(Status::failed_precondition("object replica busy"));
                }
                let existing_group_id = existing.group_id.clone();
                if !config.group_ids.is_empty() && requested_group_id != existing_group_id {
                    return Err(Status::invalid_argument(
                        "group membership is immutable while object exists",
                    ));
                }
                let effective_group_id = if config.group_ids.is_empty() {
                    existing_group_id.clone()
                } else {
                    requested_group_id.clone()
                };
                if existing.size == slice_length {
                    existing.client_id = client_id;
                    existing.put_start_time = Some(now);
                    existing.last_access = now;
                    existing.soft_pin_timeout =
                        Self::reconcile_soft_pin(config.with_soft_pin, existing.soft_pin_timeout);
                    existing.data_type = config.data_type;
                    existing.group_id = effective_group_id;
                    for replica in &mut existing.replicas {
                        if replica.status == ReplicaStatus::Complete {
                            replica.status = ReplicaStatus::Allocating;
                        }
                    }
                    let replicas = existing.replicas.clone();
                    drop(existing);
                    self.state.processing_keys.insert(scoped_key, ());
                    return Ok(replicas);
                }

                let previous_soft_pin = existing.soft_pin_timeout;
                let previous_hard_pin = existing.hard_pinned;
                let old_replicas = existing.replicas.clone();
                drop(existing);
                if let Some((_, removed)) = self.state.objects.remove(&scoped_key) {
                    self.account_removed_object_quota(&removed);
                }
                self.schedule_delayed_release(old_replicas);

                let mut merged_config = config.clone();
                merged_config.with_hard_pin = merged_config.with_hard_pin || previous_hard_pin;
                merged_config.with_soft_pin =
                    merged_config.with_soft_pin || previous_soft_pin.is_some();
                let hard_pinned = merged_config.with_hard_pin;
                return self.allocate_and_insert_upsert(
                    client_id,
                    user_key,
                    tenant_id,
                    &scoped_key,
                    slice_length,
                    replica_count,
                    merged_config,
                    None,
                    hard_pinned,
                    effective_group_id,
                );
            }
        }

        self.allocate_and_insert_upsert(
            client_id,
            user_key,
            tenant_id,
            &scoped_key,
            slice_length,
            replica_count,
            config.clone(),
            None,
            config.with_hard_pin,
            requested_group_id,
        )
    }

    fn allocate_and_insert_upsert(
        &self,
        client_id: Uuid,
        user_key: &str,
        tenant_id: &TenantId,
        scoped_key: &str,
        slice_length: u64,
        replica_count: usize,
        config: ReplicateConfig,
        previous_soft_pin: Option<SystemTime>,
        hard_pinned: bool,
        group_id: String,
    ) -> Result<Vec<ReplicaDescriptor>, Status> {
        self.reserve_tenant_quota(tenant_id, slice_length)?;
        let mut replicas = if replica_count > 0 {
            allocate_memory_replicas(
                &self.state,
                scoped_key,
                Some(client_id),
                slice_length,
                replica_count,
                &config,
            )
        } else {
            Vec::new()
        };
        if replicas.len() != replica_count {
            release_replicas(&self.state, &replicas);
            self.abort_tenant_quota(tenant_id, slice_length);
            return Err(Status::resource_exhausted(format!(
                "failed to allocate {replica_count} replica(s) for key {user_key}{}",
                PUT_NO_SPACE_HELPER_STR,
            )));
        }
        if config.nof_replica_num > 0 {
            let nof_replicas = match allocate_nof_replicas(
                &self.state,
                scoped_key,
                slice_length,
                config.nof_replica_num as usize,
                &config.preferred_nof_segments,
            ) {
                Ok(replicas) => replicas,
                Err(status) => {
                    release_replicas(&self.state, &replicas);
                    self.abort_tenant_quota(tenant_id, slice_length);
                    return Err(status);
                }
            };
            replicas.extend(nof_replicas);
        }
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));
        sync_nof_segment_usage(
            &self.state,
            replicas
                .iter()
                .filter(|r| r.replica_type == ReplicaType::NoFSsd)
                .map(|r| r.segment_id),
        );

        let soft_pin_timeout = Self::reconcile_soft_pin(config.with_soft_pin, previous_soft_pin);
        let now = SystemTime::now();
        self.state.objects.insert(
            scoped_key.to_string(),
            ObjectEntry {
                replicas: replicas.clone(),
                size: slice_length,
                last_access: now,
                hard_pinned,
                data_type: config.data_type,
                client_id,
                put_start_time: Some(now),
                lease_timeout: None,
                soft_pin_timeout,
                tenant_id: tenant_id.clone(),
                group_id,
                quota_committed: false,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: user_key.to_string(),
            },
        );
        self.state
            .processing_keys
            .insert(scoped_key.to_string(), ());
        Ok(replicas)
    }

    // ---- Upsert ----
    // C++ UpsertStart 语义：返回可写 descriptor，直到 PutEnd/BatchUpsertEnd 前对象不可读。
    pub(super) async fn upsert_impl(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = self.resolve_write_tenant(&req.tenant_id)?;
        let client_id = uuid_from_proto(
            req.client_id
                .as_ref()
                .ok_or(Status::invalid_argument("missing client_id"))?,
        );
        let config = req
            .config
            .as_ref()
            .map(config_from_proto)
            .unwrap_or_default();
        let replicas =
            self.upsert_start_for_entry(client_id, &req.key, &tenant_id, req.slice_length, config)?;
        Ok(Response::new(proto::UpsertResponse {
            replicas: replicas.iter().map(replica_to_proto).collect(),
        }))
    }
}

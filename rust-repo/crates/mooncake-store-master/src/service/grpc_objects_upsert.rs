use super::*;

impl MasterServiceImpl {
    fn schedule_delayed_release(
        &self,
        scoped_key: &str,
        authoritative_object: Option<ObjectEntry>,
        replicas: Vec<ReplicaDescriptor>,
        operation: &str,
    ) -> Result<bool, Status> {
        self.state
            .schedule_delayed_replica_release_or_fence(
                scoped_key,
                authoritative_object,
                replicas,
                None,
                operation,
            )
            .map(|release_id| release_id.is_some())
            .map_err(|error| {
                Status::unavailable(format!(
                    "failed to persist {operation} delayed release: {error}"
                ))
            })
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
        const MAX_TENANT_QUOTA_EVICTION_RETRIES: usize = 2;
        let requested_quota_charge =
            checked_requested_memory_quota_charge(slice_length, config.replica_num as usize)
                .map_err(|_| {
                    Status::invalid_argument("Memory replica quota charge overflows uint64")
                })?;
        let scoped_key = tenant_id.make_scoped_key(user_key);
        // C++ holds AcquireObjectOperationLock across all quota-eviction
        // retries. Keep request identity stable while each attempt acquires
        // and releases the actual mutation/snapshot guard independently.
        let _operation_guard = self.state.key_mutations.lock_operation(&scoped_key);
        for attempt in 0..=MAX_TENANT_QUOTA_EVICTION_RETRIES {
            match self.upsert_start_for_entry_once(
                client_id,
                user_key,
                tenant_id,
                slice_length,
                config.clone(),
            ) {
                Err(status)
                    if Self::is_tenant_quota_exceeded_status(&status)
                        && attempt < MAX_TENANT_QUOTA_EVICTION_RETRIES =>
                {
                    // The failed attempt has returned and released its
                    // tenant-scoped mutation guard before eviction locks other
                    // keys. The next attempt fully revalidates object state.
                    self.evict_tenant_quota_deficit(
                        tenant_id,
                        requested_quota_charge,
                        Some(&scoped_key),
                    )?;
                }
                result => return result,
            }
        }
        unreachable!("bounded Upsert quota-admission loop always returns")
    }

    fn upsert_start_for_entry_once(
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
        let scoped_key = tenant_id.make_scoped_key(user_key);
        let disk_enabled = global_disk_replica(&self.state, &scoped_key, slice_length).is_some();
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
        if !self.state.runtime_config.enable_nof && config.nof_replica_num > 0 {
            return Err(Status::invalid_argument("NoF is not enabled"));
        }
        let requested_group_id = Self::group_id_for_key(&config, 1, 0)?;

        let _mutation_guard = self.state.key_mutations.lock(&scoped_key);
        let alive_clients = get_alive_clients_snapshot(&self.state);
        clear_invalid_handles_for_key_locked(&self.state, &alive_clients, &scoped_key).map_err(
            |error| Status::unavailable(format!("stale handle cleanup failed: {error}")),
        )?;
        if self.state.replication_tasks.contains_key(&scoped_key) {
            return Err(Status::failed_precondition("object has replication task"));
        }
        if self.state.offloading_tasks.contains_key(&scoped_key) {
            return Err(Status::failed_precondition("object has offloading task"));
        }

        let replica_count = config.replica_num as usize;
        let now = SystemTime::now();
        if let Some(mut existing) = self.state.objects.get_mut(&scoped_key) {
            if self.state.processing_keys.contains_key(&scoped_key) {
                let existing_group_id = existing.group_id.clone();
                if !config.group_ids.is_empty() && requested_group_id != existing_group_id {
                    return Err(Status::invalid_argument(
                        "group membership is immutable while object exists",
                    ));
                }
                let effective_group_id = if config.group_ids.is_empty() {
                    existing_group_id
                } else {
                    requested_group_id.clone()
                };
                let previous_soft_pin = existing.soft_pin_timeout;
                let previous_hard_pin = existing.hard_pinned;
                let mut preempted = Vec::new();
                existing.replicas.retain(|replica| {
                    if replica.status == ReplicaStatus::Allocating {
                        preempted.push(replica.clone());
                        false
                    } else {
                        true
                    }
                });
                sync_cache_total_accounting(&mut existing);
                self.state.processing_keys.remove(&scoped_key);

                let has_completed_write_target = existing.replicas.iter().any(|replica| {
                    matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd | ReplicaType::Disk
                    ) && replica.status == ReplicaStatus::Complete
                });
                if !has_completed_write_target {
                    // C++ performs PopReplicas(PROCESSING), clears the
                    // processing marker, and erases metadata with no COMPLETE
                    // survivor inside one object lock. Persist that resulting
                    // absence together with every retained old allocation;
                    // never publish an intermediate empty object image.
                    preempted.extend(existing.replicas.clone());
                    drop(existing);
                    if let Some((_, removed)) = self.state.objects.remove(&scoped_key) {
                        self.account_removed_object_quota(&removed)?;
                    }
                    let delayed = self.schedule_delayed_release(
                        &scoped_key,
                        None,
                        preempted,
                        "upsert_preempt_inflight_remove",
                    )?;
                    if !delayed {
                        self.persist_object_image_or_remove(
                            &scoped_key,
                            "upsert_preempt_inflight_remove",
                        )?;
                    }

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
                        false,
                        0,
                    );
                }
                self.schedule_delayed_release(
                    &scoped_key,
                    Some(existing.clone()),
                    preempted,
                    "upsert_preempt_allocating",
                )?;
            }

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
                // A LocalDisk replica contains the previous generation and
                // is not a write target of Upsert. Drop the descriptor
                // before making the new Memory/NoF/global-DISK generation
                // unreadable and writable.
                existing
                    .replicas
                    .retain(|replica| replica.replica_type != ReplicaType::LocalDisk);
                for replica in &mut existing.replicas {
                    if matches!(
                        replica.replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd | ReplicaType::Disk
                    ) && replica.status == ReplicaStatus::Complete
                    {
                        replica.status = ReplicaStatus::Allocating;
                    }
                }
                sync_cache_total_accounting(&mut existing);
                let replicas = existing.replicas.clone();
                drop(existing);
                self.state.processing_keys.insert(scoped_key.clone(), ());
                self.persist_object_image_or_remove(&scoped_key, "upsert_start_reuse")?;
                return Ok(replicas);
            }

            let previous_soft_pin = existing.soft_pin_timeout;
            let previous_hard_pin = existing.hard_pinned;
            let preserve_replaced_charge = existing.quota_committed;
            let pending_replaced_quota_charge_bytes = if preserve_replaced_charge {
                if existing.committed_quota_charge_bytes == 0 {
                    completed_memory_quota_charge(&existing)
                } else {
                    existing.committed_quota_charge_bytes
                }
            } else {
                0
            };
            let old_replicas = (!preserve_replaced_charge).then(|| existing.replicas.clone());
            drop(existing);
            if !preserve_replaced_charge {
                if let Some((_, removed)) = self.state.objects.remove(&scoped_key) {
                    self.account_removed_object_quota(&removed)?;
                }
                if let Some(old_replicas) = old_replicas {
                    let delayed = self.schedule_delayed_release(
                        &scoped_key,
                        None,
                        old_replicas,
                        "upsert_replace_uncommitted_remove",
                    )?;
                    if !delayed {
                        self.persist_object_image_or_remove(
                            &scoped_key,
                            "upsert_replace_uncommitted_remove",
                        )?;
                    }
                }
            }

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
                previous_soft_pin,
                hard_pinned,
                effective_group_id,
                preserve_replaced_charge,
                pending_replaced_quota_charge_bytes,
            );
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
            false,
            0,
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
        replacing_existing: bool,
        pending_replaced_quota_charge_bytes: u64,
    ) -> Result<Vec<ReplicaDescriptor>, Status> {
        let requested_quota_charge =
            checked_requested_memory_quota_charge(slice_length, replica_count).map_err(|_| {
                Status::invalid_argument("Memory replica quota charge overflows uint64")
            })?;
        self.reserve_tenant_quota(tenant_id, requested_quota_charge)?;
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
            release_replicas(&self.state, &replicas)?;
            self.abort_tenant_quota(tenant_id, requested_quota_charge)?;
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
                    release_replicas(&self.state, &replicas)?;
                    self.abort_tenant_quota(tenant_id, requested_quota_charge)?;
                    return Err(status);
                }
            };
            replicas.extend(nof_replicas);
        }
        if let Some(disk_replica) = global_disk_replica(&self.state, scoped_key, slice_length) {
            replicas.push(disk_replica);
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
        let mut object = ObjectEntry {
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
            reserved_quota_charge_bytes: requested_quota_charge,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            disk_allocated_bytes_accounted: 0,
            user_key: user_key.to_string(),
        };
        sync_cache_total_accounting(&mut object);
        let replaced = self.state.objects.insert(scoped_key.to_string(), object);
        if replacing_existing {
            let mut replaced = replaced.expect(
                "size-changing Upsert holds the key mutation lock and replaces an existing object",
            );
            account_cache_total_removal(&mut replaced);
            self.schedule_delayed_release(
                scoped_key,
                self.state
                    .objects
                    .get(scoped_key)
                    .map(|object| object.clone()),
                replaced.replicas,
                "upsert_start_replacement",
            )?;
        } else {
            debug_assert!(replaced.is_none());
            self.register_tenant_metadata_object(tenant_id);
        }
        self.state
            .processing_keys
            .insert(scoped_key.to_string(), ());
        self.persist_object_image_or_remove(scoped_key, "upsert_start")?;
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
            replicas: replicas
                .iter()
                .map(|replica| replica_to_proto_for_state(&self.state, replica))
                .collect(),
        }))
    }
}

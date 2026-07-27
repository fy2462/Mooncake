use super::oplog_wire::*;
use super::*;

#[derive(Debug, Clone)]
pub(crate) struct LeaseRefreshEntry {
    pub(crate) key: String,
    pub(crate) tenant_id: TenantId,
    pub(crate) group_id: String,
    pub(crate) last_access: SystemTime,
    pub(crate) lease_timeout: SystemTime,
    pub(crate) soft_pin_timeout: Option<SystemTime>,
}

pub struct OpLogManager {
    store: Option<Box<dyn OpLogStore + Send>>,
    view_version: u64,
}

impl OpLogManager {
    pub fn new(store: Option<Box<dyn OpLogStore + Send>>, view_version: u64) -> Self {
        Self {
            store,
            view_version,
        }
    }

    pub fn latest_sequence(&self) -> u64 {
        self.store
            .as_ref()
            .map(|s| s.latest_sequence())
            .unwrap_or(0)
    }

    pub fn max_sequence_id(&self) -> Result<u64, HaError> {
        self.store
            .as_ref()
            .map(|s| s.max_sequence_id())
            .unwrap_or(Ok(0))
    }

    pub fn set_view_version(&mut self, version: u64) {
        self.view_version = version;
    }

    pub fn store(&self) -> Option<&(dyn OpLogStore + Send)> {
        self.store.as_deref()
    }

    pub fn into_store(self) -> Option<Box<dyn OpLogStore + Send>> {
        self.store
    }

    fn append_payload(&mut self, payload: String) -> Result<u64, HaError> {
        let Some(store) = &mut self.store else {
            return Ok(0);
        };
        let record = OpLogRecord {
            seq: 0,
            producer_view_version: self.view_version,
            payload,
        };
        validate_record_size(&record)?;
        store.append(&record)
    }

    pub fn append_and_persist(&mut self, payload: String) -> Result<u64, HaError> {
        let Some(store) = &mut self.store else {
            return Ok(0);
        };
        let record = OpLogRecord {
            seq: 0,
            producer_view_version: self.view_version,
            payload,
        };
        validate_record_size(&record)?;
        // append 只取得全序 sequence；需要 durable 语义的控制面变更必须在向调用者
        // 报告成功前 flush，否则进程崩溃可能留下“已应答但 standby 无法重放”的空洞。
        let seq = store.append(&record)?;
        store.flush_durable()?;
        Ok(seq)
    }

    pub fn set_initial_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
        if let Some(store) = &mut self.store {
            store.update_latest_sequence_id(sequence_id)?;
        }
        Ok(())
    }

    pub fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
        if let Some(store) = &mut self.store {
            store.cleanup_before(before_sequence_id)?;
        }
        Ok(())
    }

    pub fn record_snapshot_sequence_id(
        &mut self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        if let Some(store) = &mut self.store {
            store.record_snapshot_sequence_id(snapshot_id, sequence_id)?;
        }
        Ok(())
    }

    pub fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        self.store
            .as_ref()
            .map(|s| s.get_snapshot_sequence_id(snapshot_id))
            .unwrap_or(Ok(0))
    }

    pub fn record_put_end(&mut self, key: &str, size: u64) {
        if TenantId::parse_scoped_key(key).is_err() {
            warn!("OpLogManager: refusing to record put_end with invalid scoped tenant key");
            return;
        }
        let payload = serde_json::json!({"op": "put_end", "key": key, "size": size}).to_string();
        if let Err(error) = self.append_payload(payload) {
            warn!("OpLogManager: failed to record legacy put_end marker for key={key}: {error}");
        }
    }

    /// Persist the exact authoritative object image used by Rust HA replay.
    /// v1/v2 remain readable for upgrade, while all new Store mutations use
    /// v3 so pin/type/deadline/quota semantics survive promotion.
    pub(crate) fn record_object_image_durable(
        &mut self,
        key: &str,
        object: &crate::service::ObjectEntry,
    ) -> Result<u64, HaError> {
        let payload = PutEndMetadataPayloadV3::try_new(key, object)?;
        self.append_and_persist(encode_put_end_object_image_msgpack(&payload)?)
    }

    /// Persist the complete state introduced by CopyStart/MoveStart.
    ///
    /// The object image and native replication task must share one sequence:
    /// replaying only the object would reserve allocator space without an owner
    /// task, while replaying only the task would point at absent target ranges.
    pub(crate) fn record_replication_start_durable(
        &mut self,
        key: &str,
        object: &crate::service::ObjectEntry,
        task: &crate::service::ReplicationTaskEntry,
    ) -> Result<u64, HaError> {
        let object_image = PutEndMetadataPayloadV3::try_new(key, object)?;
        if task.client_id.is_nil() {
            return Err(HaError::InvalidBackend(
                "replication_start task requires a client".into(),
            ));
        }
        match task.kind {
            crate::service::ReplicationTaskKind::Copy if task.existing_move_target.is_some() => {
                return Err(HaError::InvalidBackend(
                    "replication_start Copy cannot carry an existing Move target".into(),
                ));
            }
            crate::service::ReplicationTaskKind::Move
                if !matches!(
                    (task.targets.as_slice(), task.existing_move_target.as_ref()),
                    ([_], None) | ([], Some(_))
                ) =>
            {
                return Err(HaError::InvalidBackend(
                    "replication_start Move requires one allocated or one existing target".into(),
                ));
            }
            _ => {}
        }
        let same_location = |left: &ReplicaDescriptor, right: &ReplicaDescriptor| {
            left.segment_id == right.segment_id
                && left.offset == right.offset
                && left.size == right.size
                && left.replica_type == right.replica_type
                && left.local_disk_storage_id == right.local_disk_storage_id
                && left.local_disk_generation_id == right.local_disk_generation_id
        };
        let source_index = object
            .replicas
            .iter()
            .position(|replica| same_location(replica, &task.source))
            .filter(|index| {
                object.replicas[*index].status == mooncake_store_core::ReplicaStatus::Complete
                    && object.replicas[*index].handle_valid
                    && matches!(
                        object.replicas[*index].replica_type,
                        ReplicaType::Memory | ReplicaType::NoFSsd
                    )
            })
            .ok_or_else(|| {
                HaError::InvalidBackend("replication_start source is absent or not complete".into())
            })?;
        let mut target_indices = HashSet::with_capacity(task.targets.len());
        let mut expected_reservation = 0u64;
        for target in &task.targets {
            let target_index = object
                .replicas
                .iter()
                .position(|replica| same_location(replica, target))
                .filter(|index| {
                    object.replicas[*index].status == mooncake_store_core::ReplicaStatus::Allocating
                })
                .ok_or_else(|| {
                    HaError::InvalidBackend(
                        "replication_start target is absent or not allocating".into(),
                    )
                })?;
            if target_index == source_index || !target_indices.insert(target_index) {
                return Err(HaError::InvalidBackend(
                    "replication_start contains a duplicate target".into(),
                ));
            }
            if target.replica_type == ReplicaType::Memory {
                expected_reservation =
                    expected_reservation
                        .checked_add(target.size)
                        .ok_or_else(|| {
                            HaError::InvalidBackend(
                                "replication_start target reservation overflows uint64".into(),
                            )
                        })?;
            }
        }
        if let Some(existing_target) = &task.existing_move_target {
            let target_index = object
                .replicas
                .iter()
                .position(|replica| same_location(replica, existing_target))
                .filter(|index| {
                    object.replicas[*index].status == mooncake_store_core::ReplicaStatus::Complete
                        && object.replicas[*index].handle_valid
                        && matches!(
                            object.replicas[*index].replica_type,
                            ReplicaType::Memory | ReplicaType::NoFSsd
                        )
                })
                .ok_or_else(|| {
                    HaError::InvalidBackend(
                        "replication_start existing Move target is absent or not complete".into(),
                    )
                })?;
            if target_index == source_index {
                return Err(HaError::InvalidBackend(
                    "replication_start Move target aliases its source".into(),
                ));
            }
        }
        if object.replicas.iter().enumerate().any(|(index, replica)| {
            replica.status != mooncake_store_core::ReplicaStatus::Complete
                && !target_indices.contains(&index)
        }) || task.reserved_quota_charge_bytes != expected_reservation
        {
            return Err(HaError::InvalidBackend(
                "replication_start object and task reservations are inconsistent".into(),
            ));
        }
        let task = crate::service::ReplicationTaskSnapshotEntry::capture(key, task, Instant::now());
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "replication_start",
            "schema_version": 2,
            "key": object_image.key,
            "tenant_id": object_image.tenant_id,
            "user_key": object_image.user_key,
            "object": object_image.object,
            "task": task,
        }))?)
    }

    pub(crate) fn record_task_state_batch_durable(
        &mut self,
        upserts: &[crate::service::TaskEntry],
        removes: &[Uuid],
    ) -> Result<u64, HaError> {
        if upserts.is_empty() && removes.is_empty() {
            return Err(HaError::InvalidBackend(
                "task state batch must not be empty".into(),
            ));
        }
        let mut seen = HashSet::with_capacity(upserts.len() + removes.len());
        for task in upserts {
            if task.info.id.is_nil() || !seen.insert(task.info.id) {
                return Err(HaError::InvalidBackend(
                    "task state batch contains an invalid or duplicate upsert".into(),
                ));
            }
        }
        for task_id in removes {
            if task_id.is_nil() || !seen.insert(*task_id) {
                return Err(HaError::InvalidBackend(
                    "task state batch contains an invalid, duplicate, or overlapping removal"
                        .into(),
                ));
            }
        }
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "task_state_batch",
            "schema_version": 1,
            "upserts": upserts,
            "removes": removes,
        }))?)
    }

    /// Persist an object mutation and the delayed allocator reservations it
    /// creates as one replay unit. Splitting these records would allow a
    /// standby to observe the object removal without retaining the old range,
    /// or to retain a range while the old object still owns it.
    pub(crate) fn record_object_delayed_release_batch_durable(
        &mut self,
        key: &str,
        object: Option<&crate::service::ObjectEntry>,
        upserts: &[crate::service::state::DelayedReplicaReleaseEntry],
        removes: &[Uuid],
    ) -> Result<u64, HaError> {
        let (tenant_id, user_key) = TenantId::parse_scoped_key(key)
            .map_err(|error| HaError::InvalidBackend(error.to_string()))?;
        let object_image = object
            .map(|object| PutEndMetadataPayloadV3::try_new(key, object))
            .transpose()?;
        let mut seen = HashSet::with_capacity(upserts.len() + removes.len());
        for entry in upserts {
            if entry.id.is_nil()
                || entry.scoped_key != key
                || entry.deadline_epoch_ms == 0
                || entry.replicas.is_empty()
                || !seen.insert(entry.id)
            {
                return Err(HaError::InvalidBackend(
                    "delayed release batch contains an invalid or duplicate upsert".into(),
                ));
            }
            if entry.replicas.iter().any(|replica| {
                !matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd
                )
            }) {
                return Err(HaError::InvalidBackend(
                    "delayed release batch contains a non-allocator replica".into(),
                ));
            }
        }
        for release_id in removes {
            if release_id.is_nil() || !seen.insert(*release_id) {
                return Err(HaError::InvalidBackend(
                    "delayed release batch contains an invalid, duplicate, or overlapping removal"
                        .into(),
                ));
            }
        }
        if object_image.is_none() && upserts.is_empty() && removes.is_empty() {
            return Err(HaError::InvalidBackend(
                "object delayed release batch must not be empty".into(),
            ));
        }
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "object_delayed_release_batch",
            "schema_version": 1,
            "key": key,
            "tenant_id": tenant_id.as_str(),
            "user_key": user_key,
            "object_image": object_image,
            "upserts": upserts,
            "removes": removes,
        }))?)
    }

    /// Persist one read response's complete lease mutation. Group leases are
    /// encoded as one record so standby replay cannot expose a partially
    /// refreshed group.
    pub(crate) fn record_lease_refresh_batch_durable(
        &mut self,
        entries: &[LeaseRefreshEntry],
    ) -> Result<u64, HaError> {
        if entries.is_empty() {
            return Err(HaError::InvalidBackend(
                "lease refresh batch must not be empty".into(),
            ));
        }
        let mut seen = HashSet::with_capacity(entries.len());
        let mut encoded_entries = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.key.len() > MAX_OBJECT_KEY_SIZE || !seen.insert(entry.key.as_str()) {
                return Err(HaError::InvalidBackend(
                    "lease refresh batch contains an invalid or duplicate key".into(),
                ));
            }
            let (tenant_id, user_key) =
                TenantId::parse_scoped_key(&entry.key).map_err(|error| {
                    HaError::InvalidBackend(format!(
                        "lease refresh batch contains an invalid tenant key: {error}"
                    ))
                })?;
            if tenant_id != entry.tenant_id || tenant_id.make_scoped_key(&user_key) != entry.key {
                return Err(HaError::InvalidBackend(
                    "lease refresh batch contains a non-canonical object identity".into(),
                ));
            }
            encoded_entries.push(json!({
                "key": entry.key.as_str(),
                "tenant_id": entry.tenant_id.as_str(),
                "group_id": entry.group_id.as_str(),
                "last_access_ms": system_time_to_epoch_millis(entry.last_access),
                "lease_timeout_ms": system_time_to_epoch_millis(entry.lease_timeout),
                "soft_pin_timeout_ms": entry
                    .soft_pin_timeout
                    .map(system_time_to_epoch_millis),
            }));
        }
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "lease_refresh_batch",
            "schema_version": 1,
            "entries": encoded_entries,
        }))?)
    }

    /// Record a remove mutation: { "op": "remove", "key": "..." }
    pub fn record_remove(&mut self, key: &str) {
        match encode_msgpack_record_payload_value(
            &json!({"op": "remove", "schema_version": 1, "key": key}),
        ) {
            Ok(payload) => {
                if let Err(e) = self.append_payload(payload) {
                    warn!("OpLogManager: failed to record remove for key={key}: {e}");
                }
            }
            Err(e) => warn!("OpLogManager: failed to encode remove for key={key}: {e}"),
        }
    }

    pub fn record_remove_durable(&mut self, key: &str) -> Result<u64, HaError> {
        self.append_and_persist(encode_msgpack_record_payload_value(
            &json!({"op": "remove", "schema_version": 1, "key": key}),
        )?)
    }

    /// Record a put_revoke mutation that fully removes an unfinished object.
    pub fn record_put_revoke(&mut self, key: &str) {
        match encode_msgpack_record_payload_value(
            &json!({"op": "put_revoke", "schema_version": 1, "key": key}),
        ) {
            Ok(payload) => {
                if let Err(e) = self.append_payload(payload) {
                    warn!("OpLogManager: failed to record put_revoke for key={key}: {e}");
                }
            }
            Err(e) => warn!("OpLogManager: failed to encode put_revoke for key={key}: {e}"),
        }
    }

    pub fn record_put_revoke_durable(&mut self, key: &str) -> Result<u64, HaError> {
        self.append_and_persist(encode_msgpack_record_payload_value(
            &json!({"op": "put_revoke", "schema_version": 1, "key": key}),
        )?)
    }

    /// Record a mount-segment mutation.
    pub fn record_mount_segment(
        &mut self,
        segment_name: &str,
        segment_id: Uuid,
        base: u64,
        size: u64,
        te_endpoint: &str,
        protocol: &str,
        host_id: &str,
        client_id: Uuid,
    ) {
        if let Some(store) = &mut self.store {
            let mut record = json!({
                "op": "mount_segment",
                "schema_version": 1,
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "base": base,
                "size": size,
                "te_endpoint": te_endpoint,
                "protocol": protocol,
                "host_id": host_id,
                "client_id": client_id.to_string()
            });
            if mooncake_store_core::stable_memory_segment_id(
                client_id,
                segment_name,
                base,
                size,
                te_endpoint,
                protocol,
                host_id,
            ) == segment_id
            {
                record["identity_version"] = json!(1);
            }
            let payload = match encode_msgpack_record_payload_value(&record) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!("OpLogManager: failed to encode mount_segment for {segment_name}: {e}");
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record mount_segment for {segment_name}: {e}");
            }
        }
    }

    pub fn record_mount_segment_durable(
        &mut self,
        segment_name: &str,
        segment_id: Uuid,
        base: u64,
        size: u64,
        te_endpoint: &str,
        protocol: &str,
        host_id: &str,
        client_id: Uuid,
    ) -> Result<u64, HaError> {
        let mut record = json!({
            "op": "mount_segment",
            "schema_version": 1,
            "segment_name": segment_name,
            "segment_id": segment_id.to_string(),
            "base": base,
            "size": size,
            "te_endpoint": te_endpoint,
            "protocol": protocol,
            "host_id": host_id,
            "client_id": client_id.to_string()
        });
        if mooncake_store_core::stable_memory_segment_id(
            client_id,
            segment_name,
            base,
            size,
            te_endpoint,
            protocol,
            host_id,
        ) == segment_id
        {
            record["identity_version"] = json!(1);
        }
        self.append_and_persist(encode_msgpack_record_payload_value(&record)?)
    }

    /// Record the durable scheduling intent for a delayed memory-segment
    /// unmount. The absolute epoch deadline lets a promoted standby preserve
    /// elapsed grace time rather than restarting the full delay.
    pub fn record_graceful_unmount_segment(
        &mut self,
        segment_name: &str,
        segment_id: Uuid,
        client_id: Uuid,
        deadline_epoch_ms: u64,
    ) -> Result<u64, HaError> {
        if segment_name.is_empty()
            || segment_id.is_nil()
            || client_id.is_nil()
            || deadline_epoch_ms == 0
        {
            return Err(HaError::InvalidBackend(
                "graceful unmount oplog identity and deadline must be non-empty".into(),
            ));
        }
        let payload = encode_msgpack_record_payload_value(&json!({
            "op": "graceful_unmount_segment",
            "schema_version": 1,
            "segment_name": segment_name,
            "segment_id": segment_id.to_string(),
            "client_id": client_id.to_string(),
            "deadline_epoch_ms": deadline_epoch_ms,
        }))?;
        self.append_and_persist(payload)
    }

    /// Record an unmount-segment mutation.
    pub fn record_unmount_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "unmount_segment",
                "schema_version": 1,
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!("OpLogManager: failed to encode unmount_segment for {segment_name}: {e}");
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record unmount_segment for {segment_name}: {e}");
            }
        }
    }

    pub fn record_unmount_segment_durable(
        &mut self,
        segment_name: &str,
        segment_id: Uuid,
    ) -> Result<u64, HaError> {
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "unmount_segment",
            "schema_version": 1,
            "segment_name": segment_name,
            "segment_id": segment_id.to_string()
        }))?)
    }

    /// Record a mount-nof-segment mutation.
    pub fn record_mount_nof_segment(
        &mut self,
        segment_name: &str,
        segment_id: Uuid,
        base: u64,
        size: u64,
        te_endpoint: &str,
        client_id: Uuid,
    ) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "mount_nof_segment",
                "schema_version": 1,
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "base": base,
                "size": size,
                "te_endpoint": te_endpoint,
                "client_id": client_id.to_string()
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!(
                        "OpLogManager: failed to encode mount_nof_segment for {segment_name}: {e}"
                    );
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record mount_nof_segment for {segment_name}: {e}");
            }
        }
    }

    pub fn record_mount_nof_segment_durable(
        &mut self,
        segment_name: &str,
        segment_id: Uuid,
        base: u64,
        size: u64,
        te_endpoint: &str,
        client_id: Uuid,
    ) -> Result<u64, HaError> {
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "mount_nof_segment",
            "schema_version": 1,
            "segment_name": segment_name,
            "segment_id": segment_id.to_string(),
            "base": base,
            "size": size,
            "te_endpoint": te_endpoint,
            "client_id": client_id.to_string()
        }))?)
    }

    /// Record an unmount-nof-segment mutation.
    pub fn record_unmount_nof_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "unmount_nof_segment",
                "schema_version": 1,
                "segment_name": segment_name,
                "segment_id": segment_id.to_string()
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!(
                        "OpLogManager: failed to encode unmount_nof_segment for {segment_name}: {e}"
                    );
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record unmount_nof_segment for {segment_name}: {e}");
            }
        }
    }

    pub fn record_unmount_nof_segment_durable(
        &mut self,
        segment_name: &str,
        segment_id: Uuid,
    ) -> Result<u64, HaError> {
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "unmount_nof_segment",
            "schema_version": 1,
            "segment_name": segment_name,
            "segment_id": segment_id.to_string()
        }))?)
    }

    /// Persist one atomic batch of Memory/NoF segment status transitions.
    /// Drain may cover several source segments, so a single record prevents a
    /// promoted standby from observing only a prefix of the transition.
    pub(crate) fn record_segment_status_batch_durable(
        &mut self,
        entries: &[(Uuid, bool, i32)],
    ) -> Result<u64, HaError> {
        if entries.is_empty() {
            return Err(HaError::InvalidBackend(
                "segment status oplog batch must not be empty".into(),
            ));
        }
        let mut seen = HashSet::with_capacity(entries.len());
        for (segment_id, nof, status) in entries {
            let status = crate::proto::SegmentStatus::try_from(*status).map_err(|_| {
                HaError::InvalidBackend(format!(
                    "segment status oplog batch contains invalid status {status}"
                ))
            })?;
            if segment_id.is_nil()
                || !seen.insert((*nof, *segment_id))
                || !matches!(
                    status,
                    crate::proto::SegmentStatus::Active
                        | crate::proto::SegmentStatus::Draining
                        | crate::proto::SegmentStatus::Unavailable
                )
            {
                return Err(HaError::InvalidBackend(
                    "segment status oplog batch contains invalid or duplicate entry".into(),
                ));
            }
        }
        let entries = entries
            .iter()
            .map(|(segment_id, nof, status)| {
                json!({
                    "segment_id": segment_id.to_string(),
                    "nof": nof,
                    "status": status,
                })
            })
            .collect::<Vec<_>>();
        self.append_and_persist(encode_msgpack_record_payload_value(&json!({
            "op": "segment_status_batch",
            "schema_version": 1,
            "entries": entries,
        }))?)
    }

    /// Record a put-start mutation: { "op": "put_start", "key": "...", "client_id": "..." }
    pub fn record_put_start(&mut self, key: &str, client_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "put_start",
                "schema_version": 1,
                "key": key,
                "client_id": client_id.to_string()
            })) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!("OpLogManager: failed to encode put_start for key={key}: {e}");
                    return;
                }
            };
            if let Err(e) = store.append(&OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload,
            }) {
                warn!("OpLogManager: failed to record put_start for key={key}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded_mount_payload(segment_id: Uuid, client_id: Uuid) -> serde_json::Value {
        let mut manager = OpLogManager::new(Some(Box::new(InMemoryOpLog::new(8))), 1);
        manager
            .record_mount_segment_durable(
                "memory-segment",
                segment_id,
                0x1000,
                0x2000,
                "tcp://memory-segment",
                "tcp",
                "host-a",
                client_id,
            )
            .unwrap();
        let record = manager
            .store()
            .unwrap()
            .read_since(1, 1)
            .unwrap()
            .pop()
            .unwrap();
        decode_record_payload_value(&record.payload).unwrap()
    }

    #[test]
    fn mount_identity_version_is_only_emitted_for_canonical_uuid() {
        let client_id = Uuid::new_v4();
        let canonical = mooncake_store_core::stable_memory_segment_id(
            client_id,
            "memory-segment",
            0x1000,
            0x2000,
            "tcp://memory-segment",
            "tcp",
            "host-a",
        );
        assert_eq!(
            recorded_mount_payload(canonical, client_id)["identity_version"].as_u64(),
            Some(1)
        );

        let legacy = recorded_mount_payload(Uuid::new_v4(), client_id);
        assert!(
            legacy.get("identity_version").is_none(),
            "legacy/remount UUIDs must not claim canonical fingerprint semantics"
        );
    }

    #[test]
    fn malformed_ha_control_records_are_rejected_before_append() {
        let mut manager = OpLogManager::new(Some(Box::new(InMemoryOpLog::new(8))), 1);
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();

        assert!(
            manager
                .record_graceful_unmount_segment("", segment_id, client_id, 1)
                .is_err()
        );
        assert!(
            manager
                .record_graceful_unmount_segment("segment", Uuid::nil(), client_id, 1,)
                .is_err()
        );
        assert!(
            manager
                .record_graceful_unmount_segment("segment", segment_id, client_id, 0,)
                .is_err()
        );
        assert!(manager.record_segment_status_batch_durable(&[]).is_err());
        assert!(
            manager
                .record_segment_status_batch_durable(&[
                    (
                        segment_id,
                        false,
                        crate::proto::SegmentStatus::Draining as i32,
                    ),
                    (
                        segment_id,
                        false,
                        crate::proto::SegmentStatus::Unavailable as i32,
                    ),
                ])
                .is_err()
        );
        assert!(
            manager
                .record_segment_status_batch_durable(&[(
                    segment_id,
                    false,
                    crate::proto::SegmentStatus::GracefullyUnmounting as i32,
                )])
                .is_err()
        );

        assert_eq!(manager.latest_sequence(), 0);
    }
}

use super::oplog_wire::*;
use super::*;

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
        self.record_put_end_with_metadata(key, size, None, "", "", "", &[]);
    }

    /// Record a put_end mutation with enough metadata for standby replay to
    /// recreate an object that was created after the latest snapshot.
    pub fn record_put_end_with_metadata(
        &mut self,
        key: &str,
        size: u64,
        client_id: Option<Uuid>,
        tenant_id: &str,
        group_id: &str,
        user_key: &str,
        replicas: &[ReplicaDescriptor],
    ) {
        if let Some(store) = &mut self.store {
            let payload = PutEndMetadataPayloadV1 {
                op: "put_end".to_string(),
                key: key.to_string(),
                size,
                client_id: client_id.map(|id| id.to_string()),
                tenant_id: tenant_id.to_string(),
                group_id: group_id.to_string(),
                user_key: user_key.to_string(),
                replicas: replicas.to_vec(),
            };
            let record = OpLogRecord {
                seq: 0,
                producer_view_version: self.view_version,
                payload: match encode_put_end_record_payload(&payload) {
                    Ok(payload) => payload,
                    Err(e) => {
                        warn!("OpLogManager: failed to encode put_end for key={key}: {e}");
                        return;
                    }
                },
            };
            if let Err(e) = validate_record_size(&record).and_then(|_| store.append(&record)) {
                warn!("OpLogManager: failed to record put_end for key={key}: {e}");
            }
        }
    }

    /// Record a remove mutation: { "op": "remove", "key": "..." }
    pub fn record_remove(&mut self, key: &str) {
        match encode_msgpack_record_payload_value(&json!({"op": "remove", "key": key})) {
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
            &json!({"op": "remove", "key": key}),
        )?)
    }

    /// Record a put_revoke mutation that fully removes an unfinished object.
    pub fn record_put_revoke(&mut self, key: &str) {
        match encode_msgpack_record_payload_value(&json!({"op": "put_revoke", "key": key})) {
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
            &json!({"op": "put_revoke", "key": key}),
        )?)
    }

    /// Record a mount-segment mutation.
    pub fn record_mount_segment(&mut self, segment_name: &str, segment_id: Uuid, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "mount_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
            })) {
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

    /// Record an unmount-segment mutation.
    pub fn record_unmount_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "unmount_segment",
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

    /// Record a mount-nof-segment mutation.
    pub fn record_mount_nof_segment(&mut self, segment_name: &str, segment_id: Uuid, size: u64) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "mount_nof_segment",
                "segment_name": segment_name,
                "segment_id": segment_id.to_string(),
                "size": size
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

    /// Record an unmount-nof-segment mutation.
    pub fn record_unmount_nof_segment(&mut self, segment_name: &str, segment_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "unmount_nof_segment",
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

    /// Record a put-start mutation: { "op": "put_start", "key": "...", "client_id": "..." }
    pub fn record_put_start(&mut self, key: &str, client_id: Uuid) {
        if let Some(store) = &mut self.store {
            let payload = match encode_msgpack_record_payload_value(&json!({
                "op": "put_start",
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

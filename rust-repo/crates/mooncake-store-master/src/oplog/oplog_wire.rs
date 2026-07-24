use super::*;

pub(super) fn serialize_etcd_oplog_value(entry: &OpLogRecord) -> Result<String, HaError> {
    validate_record_size(entry)?;
    if let Some(wire) = cpp_wire_entry_from_record(entry) {
        serde_json::to_string(&wire)
            .map_err(|e| HaError::InvalidBackend(format!("oplog wire serialize: {e}")))
    } else {
        serde_json::to_string(entry)
            .map_err(|e| HaError::InvalidBackend(format!("oplog serialize: {e}")))
    }
}

pub(super) fn deserialize_etcd_oplog_value(value: &str) -> Result<OpLogRecord, HaError> {
    if let Ok(entry) = serde_json::from_str::<OpLogRecord>(value) {
        validate_recovered_record_identity(&entry)?;
        return Ok(entry);
    }

    let wire: CppOpLogWireEntry = serde_json::from_str(value)
        .map_err(|e| HaError::InvalidBackend(format!("oplog wire deserialize: {e}")))?;
    validate_wire_entry_size(&wire)?;
    let payload = rust_payload_from_cpp_wire_entry(&wire)?;
    let entry = OpLogRecord {
        seq: wire.sequence_id,
        producer_view_version: 0,
        payload,
    };
    validate_recovered_record_identity(&entry)?;
    Ok(entry)
}

fn validate_recovered_record_identity(entry: &OpLogRecord) -> Result<(), HaError> {
    let payload = match decode_record_payload_value(&entry.payload) {
        Ok(payload) => payload,
        Err(error) if entry.payload.starts_with(OPLOG_MSGPACK_RECORD_PREFIX) => {
            return Err(error);
        }
        Err(_) => return Ok(()),
    };
    match payload.get("op").and_then(serde_json::Value::as_str) {
        Some("put_end") => {
            recover_object_identity_from_payload(&payload)?;
            Ok(())
        }
        Some(op @ ("remove" | "put_revoke")) => {
            let Some(key) = payload.get("key").and_then(serde_json::Value::as_str) else {
                return Ok(());
            };
            TenantId::parse_scoped_key(key).map_err(|error| {
                HaError::InvalidBackend(format!("oplog {op} has invalid scoped tenant id: {error}"))
            })?;
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn validate_record_size(entry: &OpLogRecord) -> Result<(), HaError> {
    if entry.payload.len() > MAX_PAYLOAD_SIZE {
        return Err(HaError::InvalidBackend(format!(
            "oplog payload too large: {}",
            entry.payload.len()
        )));
    }
    if let Ok(payload_json) = decode_record_payload_value(&entry.payload) {
        if let Some(key) = payload_json.get("key").and_then(|key| key.as_str()) {
            if key.len() > MAX_OBJECT_KEY_SIZE {
                return Err(HaError::InvalidBackend(format!(
                    "oplog object key too large: {}",
                    key.len()
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_wire_entry_size(wire: &CppOpLogWireEntry) -> Result<(), HaError> {
    if wire.object_key.len() > MAX_OBJECT_KEY_SIZE {
        return Err(HaError::InvalidBackend(format!(
            "oplog object key too large: {}",
            wire.object_key.len()
        )));
    }
    let max_base64_payload = MAX_PAYLOAD_SIZE.div_ceil(3) * 4;
    if wire.payload.len() > max_base64_payload {
        return Err(HaError::InvalidBackend(format!(
            "oplog payload too large: {}",
            wire.payload.len()
        )));
    }
    Ok(())
}

pub(super) fn cpp_wire_entry_from_record(entry: &OpLogRecord) -> Option<CppOpLogWireEntry> {
    if let Some(payload_bytes) = decode_msgpack_record_payload_bytes(&entry.payload).ok()? {
        if payload_bytes.starts_with(PUT_END_MSGPACK_MAGIC) {
            let payload = decode_put_end_msgpack_typed(&payload_bytes).ok()?;
            let object_key = payload.key;
            let checksum = compute_cpp_checksum(&payload_bytes);
            let prefix_hash = compute_cpp_prefix_hash(&object_key);
            return Some(CppOpLogWireEntry {
                sequence_id: entry.seq,
                timestamp_ms: unix_timestamp_ms(),
                op_type: CPP_OP_PUT_END,
                object_key,
                payload: BASE64_STANDARD.encode(payload_bytes),
                checksum,
                prefix_hash,
            });
        }
        let payload_json: serde_json::Value = rmp_serde::from_slice(&payload_bytes).ok()?;
        return cpp_wire_entry_from_payload_json(entry, &payload_json);
    }

    let payload_json = serde_json::from_str::<serde_json::Value>(&entry.payload).ok()?;
    cpp_wire_entry_from_payload_json(entry, &payload_json)
}

pub(super) fn cpp_wire_entry_from_payload_json(
    entry: &OpLogRecord,
    payload_json: &serde_json::Value,
) -> Option<CppOpLogWireEntry> {
    let op = payload_json.get("op")?.as_str()?;
    let (op_type, object_key, payload_bytes) = match op {
        "put_end" => (
            CPP_OP_PUT_END,
            payload_json.get("key")?.as_str()?.to_string(),
            encode_put_end_msgpack_from_json(&payload_json).ok()?,
        ),
        "put_revoke" => (
            CPP_OP_PUT_REVOKE,
            payload_json.get("key")?.as_str()?.to_string(),
            Vec::new(),
        ),
        "remove" => (
            CPP_OP_REMOVE,
            payload_json.get("key")?.as_str()?.to_string(),
            Vec::new(),
        ),
        _ => return None,
    };

    let checksum = compute_cpp_checksum(&payload_bytes);
    let prefix_hash = compute_cpp_prefix_hash(&object_key);
    Some(CppOpLogWireEntry {
        sequence_id: entry.seq,
        timestamp_ms: unix_timestamp_ms(),
        op_type,
        object_key,
        payload: BASE64_STANDARD.encode(payload_bytes),
        checksum,
        prefix_hash,
    })
}

pub(super) fn encode_msgpack_record_payload_value(
    payload: &serde_json::Value,
) -> Result<String, HaError> {
    let bytes = rmp_serde::to_vec_named(payload)
        .map_err(|e| HaError::InvalidBackend(format!("oplog msgpack encode: {e}")))?;
    Ok(format!(
        "{}{}",
        OPLOG_MSGPACK_RECORD_PREFIX,
        BASE64_STANDARD.encode(bytes)
    ))
}

pub(super) fn encode_put_end_record_payload(
    payload: &PutEndMetadataPayloadV1,
) -> Result<String, HaError> {
    let bytes = encode_put_end_msgpack(payload)?;
    Ok(format!(
        "{}{}",
        OPLOG_MSGPACK_RECORD_PREFIX,
        BASE64_STANDARD.encode(bytes)
    ))
}

pub(super) fn decode_msgpack_record_payload_bytes(
    payload: &str,
) -> Result<Option<Vec<u8>>, HaError> {
    let Some(encoded) = payload.strip_prefix(OPLOG_MSGPACK_RECORD_PREFIX) else {
        return Ok(None);
    };
    BASE64_STANDARD
        .decode(encoded)
        .map(Some)
        .map_err(|e| HaError::InvalidBackend(format!("oplog msgpack base64 decode: {e}")))
}

pub(super) fn encode_put_end_msgpack(
    payload: &PutEndMetadataPayloadV1,
) -> Result<Vec<u8>, HaError> {
    let mut bytes = Vec::from(PUT_END_MSGPACK_MAGIC);
    let body = rmp_serde::to_vec_named(payload)
        .map_err(|e| HaError::InvalidBackend(format!("put_end msgpack encode: {e}")))?;
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub(super) fn encode_put_end_msgpack_from_json(
    payload_json: &serde_json::Value,
) -> Result<Vec<u8>, HaError> {
    let payload = PutEndMetadataPayloadV1 {
        op: "put_end".to_string(),
        key: payload_json
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        size: payload_json
            .get("size")
            .and_then(|v| v.as_u64())
            .unwrap_or_default(),
        client_id: payload_json
            .get("client_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        tenant_id: payload_json
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        group_id: payload_json
            .get("group_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        user_key: payload_json
            .get("user_key")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        replicas: payload_json
            .get("replicas")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| HaError::InvalidBackend(format!("put_end replicas decode: {e}")))?
            .unwrap_or_default(),
    };
    encode_put_end_msgpack(&payload)
}

pub(super) fn decode_put_end_msgpack_typed(
    bytes: &[u8],
) -> Result<PutEndMetadataPayloadV1, HaError> {
    let body = bytes
        .strip_prefix(PUT_END_MSGPACK_MAGIC)
        .ok_or_else(|| HaError::InvalidBackend("put_end msgpack magic mismatch".into()))?;
    let payload: PutEndMetadataPayloadV1 = rmp_serde::from_slice(body)
        .map_err(|e| HaError::InvalidBackend(format!("put_end msgpack decode: {e}")))?;
    recover_object_identity(&payload.key, &payload.tenant_id, &payload.user_key)?;
    Ok(payload)
}

pub(crate) fn decode_put_end_msgpack(bytes: &[u8]) -> Result<serde_json::Value, HaError> {
    serde_json::to_value(decode_put_end_msgpack_typed(bytes)?)
        .map_err(|e| HaError::InvalidBackend(format!("put_end msgpack to json: {e}")))
}

pub(crate) fn decode_record_payload_value(payload: &str) -> Result<serde_json::Value, HaError> {
    if let Some(bytes) = decode_msgpack_record_payload_bytes(payload)? {
        if bytes.starts_with(PUT_END_MSGPACK_MAGIC) {
            return decode_put_end_msgpack(&bytes);
        }
        return rmp_serde::from_slice(&bytes)
            .map_err(|e| HaError::InvalidBackend(format!("oplog msgpack decode: {e}")));
    }
    serde_json::from_str(payload)
        .map_err(|e| HaError::InvalidBackend(format!("oplog json decode: {e}")))
}

pub(super) fn rust_payload_from_cpp_wire_entry(
    wire: &CppOpLogWireEntry,
) -> Result<String, HaError> {
    let decoded_payload = if wire.payload.is_empty() {
        Vec::new()
    } else {
        BASE64_STANDARD
            .decode(&wire.payload)
            .map_err(|e| HaError::InvalidBackend(format!("oplog payload base64 decode: {e}")))?
    };
    if wire.checksum != 0 && compute_cpp_checksum(&decoded_payload) != wire.checksum {
        return Err(HaError::InvalidBackend(format!(
            "oplog checksum mismatch for seq={}",
            wire.sequence_id
        )));
    }

    match wire.op_type {
        CPP_OP_PUT_END => {
            let metadata_payload_base64 = BASE64_STANDARD.encode(&decoded_payload);
            if decoded_payload.starts_with(PUT_END_MSGPACK_MAGIC) {
                return Ok(format!(
                    "{}{}",
                    OPLOG_MSGPACK_RECORD_PREFIX,
                    BASE64_STANDARD.encode(decoded_payload)
                ));
            }
            if let Ok(payload) = String::from_utf8(decoded_payload) {
                if serde_json::from_str::<serde_json::Value>(&payload)
                    .ok()
                    .and_then(|v| v.get("op").and_then(|op| op.as_str()).map(str::to_string))
                    .as_deref()
                    == Some("put_end")
                {
                    return Ok(payload);
                }
            }
            Ok(json!({
                "op": "put_end",
                "key": wire.object_key,
                "size": 0,
                "metadata_payload_base64": metadata_payload_base64
            })
            .to_string())
        }
        CPP_OP_PUT_REVOKE => Ok(json!({"op": "put_revoke", "key": wire.object_key}).to_string()),
        CPP_OP_REMOVE => Ok(json!({"op": "remove", "key": wire.object_key}).to_string()),
        other => Err(HaError::InvalidBackend(format!(
            "unsupported C++ oplog op_type: {other}"
        ))),
    }
}

pub(super) fn compute_cpp_checksum(payload: &[u8]) -> u32 {
    xxh32(payload, 0)
}

pub(super) fn compute_cpp_prefix_hash(key: &str) -> u32 {
    if key.is_empty() {
        0
    } else {
        xxh32(key.as_bytes(), 0)
    }
}

pub(super) fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

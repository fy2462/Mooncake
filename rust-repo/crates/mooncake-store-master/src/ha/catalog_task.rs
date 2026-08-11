use super::catalog_snapshot::parse_uuid;
use super::snapshot::SnapshotObjectStore;
use super::types::HaError;
use crate::TenantId;
use crate::service::TaskEntry;
use chrono::{TimeZone, Utc};
use mooncake_store_core::{TaskInfo, TaskStatus, TaskType};
use rmpv::Value;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::io::{Cursor, Read};
use uuid::Uuid;

const TASK_SERIALIZED_FIELDS: usize = 8;
const MAX_TASK_PAYLOAD_SIZE: u64 = 1024 * 1024 * 1024;

pub(super) fn load_task_manager(
    object_store: &dyn SnapshotObjectStore,
    prefix: &str,
) -> Result<Vec<TaskEntry>, HaError> {
    match object_store.download_buffer(&format!("{prefix}task_manager")) {
        Ok(payload) => decode_task_manager(&payload),
        Err(error) if object_store.is_not_found_error(&error.to_string()) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

pub(super) fn decode_task_manager(data: &[u8]) -> Result<Vec<TaskEntry>, HaError> {
    let decoded = decode_zstd_bounded(data, MAX_TASK_PAYLOAD_SIZE)?;
    let mut cursor = Cursor::new(decoded.as_slice());
    let root =
        rmpv::decode::read_value(&mut cursor).map_err(|error| snapshot_error(error.to_string()))?;
    if cursor.position() != decoded.len() as u64 {
        return Err(snapshot_error(
            "task manager payload contains trailing bytes",
        ));
    }
    let tasks = root
        .as_array()
        .ok_or_else(|| snapshot_error("task manager payload is not an array"))?;
    let mut result = Vec::with_capacity(tasks.len());
    let mut task_ids = HashSet::new();
    for (index, task) in tasks.iter().enumerate() {
        let fields = task
            .as_array()
            .ok_or_else(|| snapshot_error(format!("task manager entry {index} is not an array")))?;
        if fields.len() != TASK_SERIALIZED_FIELDS {
            return Err(snapshot_error(format!(
                "task manager entry {index} has invalid shape"
            )));
        }
        let entry = decode_task(fields)?;
        if !task_ids.insert(entry.info.id) {
            return Err(snapshot_error("task manager contains duplicate task UUID"));
        }
        result.push(entry);
    }
    Ok(result)
}

fn decode_zstd_bounded(data: &[u8], max_size: u64) -> Result<Vec<u8>, HaError> {
    let decoder = zstd::stream::read::Decoder::new(Cursor::new(data))
        .map_err(|error| snapshot_error(error.to_string()))?;
    let mut decoded = Vec::new();
    decoder
        .take(max_size.saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| snapshot_error(error.to_string()))?;
    if decoded.len() as u64 > max_size {
        return Err(snapshot_error(format!(
            "decompressed payload exceeds {max_size} bytes"
        )));
    }
    Ok(decoded)
}

pub(super) fn encode_task_manager(tasks: &[TaskEntry]) -> Result<Vec<u8>, HaError> {
    let values = tasks
        .iter()
        .map(|task| {
            Value::Array(vec![
                cpp_uuid_string(task.info.id).into(),
                task_type_to_cxx(task.info.task_type).into(),
                task_status_to_cxx(task.info.status).into(),
                task.payload.clone().into(),
                task.info.created_at.timestamp_millis().into(),
                task.info.last_updated_at.timestamp_millis().into(),
                task.info.message.clone().into(),
                task.info
                    .assigned_client
                    .map(cpp_uuid_string)
                    .unwrap_or_default()
                    .into(),
            ])
        })
        .collect();
    let mut encoded = Vec::new();
    rmpv::encode::write_value(&mut encoded, &Value::Array(values))
        .map_err(|error| snapshot_error(error.to_string()))?;
    zstd::stream::encode_all(Cursor::new(encoded), 3)
        .map_err(|error| snapshot_error(error.to_string()))
}

/// Serializes a UUID in the C++ mooncake on-wire `{high}-{low}` decimal-pair
/// format used by `UuidToString` in `src/types.cpp`. The C++ task manager
/// persists task and assigned-client UUIDs in this shape.
fn cpp_uuid_string(uuid: Uuid) -> String {
    let (high, low) = uuid.as_u64_pair();
    format!("{high}-{low}")
}

fn decode_task(fields: &[Value]) -> Result<TaskEntry, HaError> {
    let task_id = fields[0]
        .as_str()
        .and_then(|value| parse_uuid(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(|| snapshot_error("task has invalid UUID"))?;
    let task_type = match fields[1].as_i64() {
        Some(0) => TaskType::ReplicaCopy,
        Some(1) => TaskType::ReplicaMove,
        _ => return Err(snapshot_error("task has invalid type")),
    };
    // C++ TaskStatus order is PENDING, PROCESSING, FAILED, SUCCESS.
    let status = match fields[2].as_i64() {
        Some(0) => TaskStatus::Pending,
        Some(1) => TaskStatus::Processing,
        Some(2) => TaskStatus::Failed,
        Some(3) => TaskStatus::Success,
        _ => return Err(snapshot_error("task has invalid status")),
    };
    let payload = fields[3]
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| snapshot_error("task payload is not a string"))?;
    let created_at = fields[4]
        .as_i64()
        .and_then(|value| Utc.timestamp_millis_opt(value).single())
        .ok_or_else(|| snapshot_error("task created timestamp is invalid"))?;
    let last_updated_at = fields[5]
        .as_i64()
        .and_then(|value| Utc.timestamp_millis_opt(value).single())
        .ok_or_else(|| snapshot_error("task update timestamp is invalid"))?;
    if last_updated_at < created_at {
        return Err(snapshot_error(
            "task update timestamp precedes creation timestamp",
        ));
    }
    let message = fields[6]
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| snapshot_error("task message is not a string"))?;
    let assigned_client = fields[7]
        .as_str()
        .and_then(|value| parse_uuid(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(|| snapshot_error("task assigned client UUID is invalid"))?;
    let key = extract_task_key(&payload, task_type)?;
    Ok(TaskEntry {
        info: TaskInfo {
            id: task_id,
            task_type,
            status,
            created_at,
            last_updated_at,
            assigned_client: Some(assigned_client),
            message,
        },
        key,
        payload,
        // C++ task snapshots do not serialize this field.
        max_retry_attempts: 0,
    })
}

fn extract_task_key(payload: &str, task_type: TaskType) -> Result<String, HaError> {
    let value = serde_json::from_str::<JsonValue>(payload)
        .map_err(|error| snapshot_error(format!("task payload is invalid JSON: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| snapshot_error("task payload is not an object"))?;
    let key = object
        .get("key")
        .and_then(JsonValue::as_str)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| snapshot_error("task payload has no key"))?;
    object
        .get("source")
        .and_then(JsonValue::as_str)
        .filter(|source| !source.is_empty())
        .ok_or_else(|| snapshot_error("task payload has no source"))?;
    match task_type {
        TaskType::ReplicaCopy => {
            let targets = object
                .get("targets")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| snapshot_error("copy task payload has no targets"))?;
            if targets.is_empty()
                || targets
                    .iter()
                    .any(|target| target.as_str().map(str::is_empty) != Some(false))
            {
                return Err(snapshot_error("copy task payload has invalid targets"));
            }
        }
        TaskType::ReplicaMove => {
            object
                .get("target")
                .and_then(JsonValue::as_str)
                .filter(|target| !target.is_empty())
                .ok_or_else(|| snapshot_error("move task payload has no target"))?;
        }
    }

    match object.get("tenant_id") {
        Some(value) => {
            let tenant = value
                .as_str()
                .ok_or_else(|| snapshot_error("task tenant id is not a string"))?;
            let tenant = TenantId::new(tenant.to_string())
                .map_err(|error| snapshot_error(format!("invalid task tenant id: {error}")))?;
            Ok(tenant.make_scoped_key(key))
        }
        None => {
            let (tenant, user_key) = TenantId::parse_scoped_key(key)
                .map_err(|error| snapshot_error(format!("invalid legacy task key: {error}")))?;
            if user_key.is_empty() {
                return Err(snapshot_error("legacy task payload has an empty key"));
            }
            Ok(tenant.make_scoped_key(&user_key))
        }
    }
}

fn task_type_to_cxx(task_type: TaskType) -> i64 {
    match task_type {
        TaskType::ReplicaCopy => 0,
        TaskType::ReplicaMove => 1,
    }
}

fn task_status_to_cxx(status: TaskStatus) -> i64 {
    match status {
        TaskStatus::Pending => 0,
        TaskStatus::Processing => 1,
        TaskStatus::Failed => 2,
        TaskStatus::Success => 3,
    }
}

fn snapshot_error(message: impl Into<String>) -> HaError {
    HaError::Snapshot(message.into())
}

#[cfg(test)]
mod tests {
    use super::{decode_task_manager, decode_zstd_bounded};
    use rmpv::Value;
    use std::io::Cursor;
    use uuid::Uuid;

    fn encoded_tasks(tasks: Vec<Value>) -> Vec<u8> {
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &Value::Array(tasks)).unwrap();
        zstd::stream::encode_all(Cursor::new(payload), 3).unwrap()
    }

    fn task(task_id: Uuid, payload: &str) -> Value {
        Value::Array(vec![
            task_id.to_string().into(),
            1.into(),
            0.into(),
            payload.into(),
            1_000_i64.into(),
            2_000_i64.into(),
            "".into(),
            Uuid::new_v4().to_string().into(),
        ])
    }

    #[test]
    fn current_task_payload_restores_scoped_identity() {
        let payload = r#"{"tenant_id":"tenant-a","key":"key-a","source":"a","target":"b"}"#;
        let tasks =
            decode_task_manager(&encoded_tasks(vec![task(Uuid::new_v4(), payload)])).unwrap();

        assert_eq!(tasks[0].key, "tenant-a\0key-a");
    }

    #[test]
    fn malformed_or_duplicate_tasks_fail_closed() {
        let payload = r#"{"tenant_id":"tenant-a","key":"key-a","source":"a","target":"b"}"#;
        let id = Uuid::new_v4();
        assert!(decode_task_manager(&encoded_tasks(vec![Value::Array(vec![1.into()])])).is_err());
        assert!(
            decode_task_manager(&encoded_tasks(vec![task(id, payload), task(id, payload)]))
                .is_err()
        );
    }

    #[test]
    fn invalid_zstd_payload_fails_closed() {
        let error = decode_task_manager(&[0xde, 0xad, 0xbe, 0xef]).unwrap_err();
        assert!(matches!(error, crate::ha::HaError::Snapshot(_)));
    }

    #[test]
    fn bounded_zstd_roundtrip_preserves_text_and_binary_bytes() {
        for payload in [
            b"Mooncake snapshot roundtrip test data!".as_slice(),
            &[0, 1, 2, 128, 254, 255, 0, 127],
        ] {
            let compressed = zstd::stream::encode_all(Cursor::new(payload), 3).unwrap();
            assert_eq!(decode_zstd_bounded(&compressed, 1024).unwrap(), payload);
        }
    }

    #[test]
    fn bounded_zstd_rejects_decoded_size_above_limit() {
        let compressed = zstd::stream::encode_all(Cursor::new(b"too large"), 3).unwrap();
        let error = decode_zstd_bounded(&compressed, 5).unwrap_err();
        assert!(error.to_string().contains("exceeds 5 bytes"));
    }
}

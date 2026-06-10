use super::snapshot::SnapshotObjectStore;
use super::types::HaError;
use crate::service::TaskEntry;
use chrono::{TimeZone, Utc};
use mooncake_store_core::{TaskInfo, TaskStatus, TaskType};
use rmpv::Value;
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
    let decoder = zstd::stream::read::Decoder::new(Cursor::new(data))
        .map_err(|error| snapshot_error(error.to_string()))?;
    let mut decoded = Vec::new();
    decoder
        .take(MAX_TASK_PAYLOAD_SIZE + 1)
        .read_to_end(&mut decoded)
        .map_err(|error| snapshot_error(error.to_string()))?;
    if decoded.len() as u64 > MAX_TASK_PAYLOAD_SIZE {
        return Err(snapshot_error("task manager payload exceeds 1 GiB"));
    }
    let root = rmpv::decode::read_value(&mut Cursor::new(decoded))
        .map_err(|error| snapshot_error(error.to_string()))?;
    let tasks = root
        .as_array()
        .ok_or_else(|| snapshot_error("task manager payload is not an array"))?;
    let mut result = Vec::with_capacity(tasks.len());
    for task in tasks {
        let fields = match task.as_array() {
            Some(fields) if fields.len() == TASK_SERIALIZED_FIELDS => fields,
            _ => continue,
        };
        if let Some(entry) = decode_task(fields)? {
            result.push(entry);
        }
    }
    Ok(result)
}

pub(super) fn encode_task_manager(tasks: &[TaskEntry]) -> Result<Vec<u8>, HaError> {
    let values = tasks
        .iter()
        .map(|task| {
            Value::Array(vec![
                task.info.id.to_string().into(),
                task_type_to_cxx(task.info.task_type).into(),
                task_status_to_cxx(task.info.status).into(),
                task.payload.clone().into(),
                task.info.created_at.timestamp_millis().into(),
                task.info.last_updated_at.timestamp_millis().into(),
                task.info.message.clone().into(),
                task.info
                    .assigned_client
                    .map(|id| id.to_string())
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

fn decode_task(fields: &[Value]) -> Result<Option<TaskEntry>, HaError> {
    let task_id = match fields[0]
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
    {
        Some(value) => value,
        None => return Ok(None),
    };
    let task_type = match fields[1].as_i64() {
        Some(0) => TaskType::ReplicaCopy,
        Some(1) => TaskType::ReplicaMove,
        _ => return Ok(None),
    };
    // C++ TaskStatus order is PENDING, PROCESSING, FAILED, SUCCESS.
    let status = match fields[2].as_i64() {
        Some(0) => TaskStatus::Pending,
        Some(1) => TaskStatus::Processing,
        Some(2) => TaskStatus::Failed,
        Some(3) => TaskStatus::Success,
        _ => return Ok(None),
    };
    let payload = match fields[3].as_str() {
        Some(value) => value.to_string(),
        None => return Ok(None),
    };
    let created_at = match fields[4]
        .as_i64()
        .and_then(|value| Utc.timestamp_millis_opt(value).single())
    {
        Some(value) => value,
        None => return Ok(None),
    };
    let last_updated_at = match fields[5]
        .as_i64()
        .and_then(|value| Utc.timestamp_millis_opt(value).single())
    {
        Some(value) => value,
        None => return Ok(None),
    };
    let message = match fields[6].as_str() {
        Some(value) => value.to_string(),
        None => return Ok(None),
    };
    let assigned_client = match fields[7]
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
    {
        Some(value) => value,
        None => return Ok(None),
    };
    let key = extract_task_key(&payload);
    Ok(Some(TaskEntry {
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
    }))
}

fn extract_task_key(payload: &str) -> String {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| value.get("key")?.as_str().map(ToString::to_string))
        .unwrap_or_default()
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

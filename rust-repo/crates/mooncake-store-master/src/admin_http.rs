use crate::ha::{MasterRuntimeState, MasterView};
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::{MasterServiceImpl, TenantId};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
};
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Code, Request as TonicRequest};
use uuid::Uuid;

#[derive(Clone)]
pub struct AdminRuntimeState {
    runtime: Arc<RwLock<AdminRuntimeSnapshot>>,
    service: Option<Arc<MasterServiceImpl>>,
}

#[derive(Clone)]
struct AdminRuntimeSnapshot {
    state: MasterRuntimeState,
    leader_view: Option<MasterView>,
    service_ready: bool,
}

impl AdminRuntimeState {
    pub fn new(
        state: MasterRuntimeState,
        leader_view: Option<MasterView>,
        service_ready: bool,
    ) -> Self {
        Self {
            runtime: Arc::new(RwLock::new(AdminRuntimeSnapshot {
                state,
                leader_view,
                service_ready,
            })),
            service: None,
        }
    }

    pub fn serving(leader_view: Option<MasterView>) -> Self {
        Self::new(MasterRuntimeState::Serving, leader_view, true)
    }

    pub fn serving_with_service(
        leader_view: Option<MasterView>,
        service: Arc<MasterServiceImpl>,
    ) -> Self {
        Self {
            service: Some(service),
            ..Self::serving(leader_view)
        }
    }

    pub fn set_runtime_state(&self, state: MasterRuntimeState) {
        self.runtime.write().state = state;
    }

    pub fn set_leader_view(&self, leader_view: Option<MasterView>) {
        self.runtime.write().leader_view = leader_view;
    }

    fn snapshot(&self) -> AdminRuntimeSnapshot {
        self.runtime.read().clone()
    }
}

pub fn admin_router(state: AdminRuntimeState) -> Router {
    Router::new()
        .route("/metrics/summary", get(metrics_summary_handler))
        .route("/health", get(health_handler))
        .route("/role", get(role_handler))
        .route("/ha_status", get(ha_status_handler))
        .route("/leader", get(leader_handler))
        .route("/kv_events/status", get(kv_events_status_handler))
        .route("/query_key", get(query_key_handler))
        .route("/get_all_keys", get(get_all_keys_handler))
        .route("/get_all_segments", get(get_all_segments_handler))
        .route("/get_segments_detail", get(get_segments_detail_handler))
        .route("/query_segment", get(query_segment_handler))
        .route("/batch_query_keys", get(batch_query_keys_handler))
        .route("/api/v1/drain_jobs", post(create_drain_job_handler))
        .route("/api/v1/drain_jobs/query", get(query_drain_job_handler))
        .route("/api/v1/drain_jobs/cancel", post(cancel_drain_job_handler))
        .route("/api/v1/segments/status", get(segment_status_handler))
        .route(
            "/api/v1/tenant_quotas",
            get(get_tenant_quotas_handler)
                .put(upsert_tenant_quota_handler)
                .delete(delete_tenant_quota_handler),
        )
        .with_state(state)
}

async fn metrics_summary_handler(
    axum::extract::State(state): axum::extract::State<AdminRuntimeState>,
) -> String {
    build_metrics_summary_text(&state)
}

async fn health_handler(
    axum::extract::State(state): axum::extract::State<AdminRuntimeState>,
) -> Json<Value> {
    Json(build_health_json(&state))
}

async fn role_handler(
    axum::extract::State(state): axum::extract::State<AdminRuntimeState>,
) -> String {
    state.snapshot().state.role().to_string()
}

async fn ha_status_handler(
    axum::extract::State(state): axum::extract::State<AdminRuntimeState>,
) -> String {
    state.snapshot().state.as_str().to_string()
}

async fn leader_handler(
    axum::extract::State(state): axum::extract::State<AdminRuntimeState>,
) -> Json<Value> {
    Json(build_leader_json(&state))
}

async fn get_all_keys_handler(
    State(state): State<AdminRuntimeState>,
) -> Result<String, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let mut body = service.all_keys_for_admin().join("\n");
    if !body.is_empty() {
        body.push('\n');
    }
    Ok(body)
}

async fn get_all_segments_handler(
    State(state): State<AdminRuntimeState>,
) -> Result<String, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let response = service
        .get_all_segments_for_admin(TonicRequest::new(proto::GetAllSegmentsForAdminRequest {}))
        .await
        .map_err(status_error)?
        .into_inner();
    let mut body = response.segments.join("\n");
    if !body.is_empty() {
        body.push('\n');
    }
    Ok(body)
}

async fn query_key_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let key = query.get("key").map(String::as_str).unwrap_or_default();
    let response = service
        .replica_list_for_key_for_admin(&TenantId::default(), key)
        .map_err(status_error)?;
    let data = response
        .replicas
        .iter()
        .filter(|replica| {
            proto::replica_descriptor::ReplicaType::try_from(replica.replica_type)
                == Ok(proto::replica_descriptor::ReplicaType::Memory)
        })
        .map(buffer_descriptor_json)
        .collect::<Vec<_>>();
    Ok(Json(json!({ "success": true, "data": data })))
}

async fn query_segment_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<String, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let segment_name = query.get("segment").map(String::as_str).unwrap_or_default();
    let segment = service
        .query_segment_for_admin(TonicRequest::new(proto::QuerySegmentsRequest {
            segment_name: segment_name.to_owned(),
        }))
        .await
        .map_err(|_| json_error(StatusCode::INTERNAL_SERVER_ERROR, "Failed to query segment"))?
        .into_inner();
    Ok(format!(
        "{}\nUsed(bytes): {}\nCapacity(bytes) : {}\n",
        segment_name, segment.used_size, segment.total_size
    ))
}

#[derive(Debug, Deserialize)]
struct CreateDrainJobBody {
    #[serde(default)]
    segments: Vec<String>,
    #[serde(default)]
    target_segments: Vec<String>,
    #[serde(default = "default_drain_max_concurrency")]
    max_concurrency: u32,
}

fn default_drain_max_concurrency() -> u32 {
    4
}

async fn create_drain_job_handler(
    State(state): State<AdminRuntimeState>,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let body: CreateDrainJobBody = serde_json::from_slice(&body).map_err(|error| {
        json_error(
            StatusCode::BAD_REQUEST,
            &format!("Invalid JSON body: {error}"),
        )
    })?;
    let service = service_or_unavailable(&state)?;
    let response = service
        .create_drain_job(TonicRequest::new(proto::CreateDrainJobRequest {
            segments: body.segments,
            target_segments: body.target_segments,
            max_concurrency: body.max_concurrency,
        }))
        .await
        .map_err(status_error)?
        .into_inner();
    let job_id = proto_uuid_string(response.job_id.as_ref());
    Ok(Json(json!({
        "success": true,
        "job_id": job_id,
        "status": "CREATED",
    })))
}

async fn query_drain_job_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let (job_id, proto_job_id) = parse_job_id(&query)?;
    let service = service_or_unavailable(&state)?;
    let response = service
        .query_drain_job(TonicRequest::new(proto::QueryDrainJobRequest {
            job_id: Some(proto_job_id),
        }))
        .await
        .map_err(status_error)?
        .into_inner();
    Ok(Json(json!({
        "success": true,
        "job_id": response
            .id
            .as_ref()
            .map_or_else(|| job_id.to_string(), |id| proto_uuid_string(Some(id))),
        "type": response.r#type,
        "type_name": "DRAIN",
        "status": response.status,
        "status_name": job_status_name(response.status),
        "created_at_ms_epoch": response.created_at_ms_epoch,
        "last_updated_at_ms_epoch": response.last_updated_at_ms_epoch,
        "segments": response.segments,
        "succeeded_units": response.succeeded_units,
        "failed_units": response.failed_units,
        "blocked_units": response.blocked_units,
        "active_units": response.active_units,
        "migrated_bytes": response.migrated_bytes,
        "message": response.message,
    })))
}

async fn cancel_drain_job_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let (job_id, proto_job_id) = parse_job_id(&query)?;
    let service = service_or_unavailable(&state)?;
    service
        .cancel_drain_job(TonicRequest::new(proto::CancelDrainJobRequest {
            job_id: Some(proto_job_id),
        }))
        .await
        .map_err(status_error)?;
    Ok(Json(json!({
        "success": true,
        "job_id": job_id.to_string(),
        "status": "CANCELED",
    })))
}

async fn segment_status_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let segment_name = query
        .get("segment")
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| json_error(StatusCode::BAD_REQUEST, "Missing segment query parameter"))?;
    let service = service_or_unavailable(&state)?;
    let segment = service
        .segments_detail_snapshot()
        .into_iter()
        .find(|segment| segment.segment_name == *segment_name)
        .ok_or_else(|| json_error(StatusCode::NOT_FOUND, "segment not found"))?;
    Ok(Json(json!({
        "success": true,
        "segment": segment_name,
        "status": segment.status,
        "status_name": segment_status_json_string(segment.status),
    })))
}

fn parse_job_id(
    query: &HashMap<String, String>,
) -> Result<(Uuid, proto::Uuid), (StatusCode, Json<Value>)> {
    let job_id = query
        .get("job_id")
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| json_error(StatusCode::BAD_REQUEST, "Missing or invalid job_id"))?;
    let (high, low) = job_id.as_u64_pair();
    Ok((job_id, proto::Uuid { high, low }))
}

fn proto_uuid_string(uuid: Option<&proto::Uuid>) -> String {
    uuid.map(|uuid| Uuid::from_u64_pair(uuid.high, uuid.low).to_string())
        .unwrap_or_default()
}

fn job_status_name(status: i32) -> &'static str {
    match proto::JobStatus::try_from(status) {
        Ok(proto::JobStatus::Created) => "CREATED",
        Ok(proto::JobStatus::Planning) => "PLANNING",
        Ok(proto::JobStatus::Running) => "RUNNING",
        Ok(proto::JobStatus::Succeeded) => "SUCCEEDED",
        Ok(proto::JobStatus::Failed) => "FAILED",
        Ok(proto::JobStatus::Canceled) => "CANCELED",
        _ => "UNKNOWN_JOB_STATUS",
    }
}

async fn batch_query_keys_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let keys_param = query.get("keys").ok_or_else(|| {
        json_error(
            StatusCode::BAD_REQUEST,
            "No keys provided. Use ?keys=key1,key2,...",
        )
    })?;
    let keys: Vec<String> = if keys_param.is_empty() {
        Vec::new()
    } else {
        keys_param.split(',').map(ToOwned::to_owned).collect()
    };
    if keys.is_empty() {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "No keys provided. Use ?keys=key1,key2,...",
        ));
    }

    let tenant_id = TenantId::default();
    let results = service.batch_get_replica_list_for_admin_tenant(&keys, &tenant_id);
    Ok(Json(build_batch_query_keys_json(&keys, &results)))
}

async fn get_segments_detail_handler(
    State(state): State<AdminRuntimeState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    Ok(Json(build_segments_detail_json(
        &service.segments_detail_snapshot(),
    )))
}

async fn kv_events_status_handler(
    State(state): State<AdminRuntimeState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    Ok(Json(build_kv_events_status_json(
        &service.kv_event_status(),
    )))
}

#[derive(Debug, Deserialize)]
struct TenantQuotaPolicyRequest {
    requested_quota_bytes: u64,
}

fn service_or_unavailable(
    state: &AdminRuntimeState,
) -> Result<Arc<MasterServiceImpl>, (StatusCode, Json<Value>)> {
    let service = state.service.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "success": false,
                "error_code": Code::Unavailable as i32,
                "error_message": "service plane is not active"
            })),
        )
    })?;
    if !service.is_service_available() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "success": false,
                "error_code": Code::Unavailable as i32,
                "error_message": "service plane is not active"
            })),
        ));
    }
    Ok(service)
}

fn tenant_id_from_query(
    query: &HashMap<String, String>,
) -> Result<TenantId, (StatusCode, Json<Value>)> {
    let Some(tenant_id) = query.get("tenant_id") else {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "Missing or invalid tenant_id",
        ));
    };
    parse_admin_tenant_id(tenant_id)
}

fn parse_admin_tenant_id(tenant_id: &str) -> Result<TenantId, (StatusCode, Json<Value>)> {
    if tenant_id.is_empty() {
        return Err(json_error(StatusCode::BAD_REQUEST, "Invalid tenant_id"));
    }
    TenantId::new(tenant_id.to_owned())
        .map_err(|_| json_error(StatusCode::BAD_REQUEST, "Invalid tenant_id"))
}

fn json_error(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "success": false,
            "error": message
        })),
    )
}

fn status_error(status: tonic::Status) -> (StatusCode, Json<Value>) {
    let http_status = match status.code() {
        Code::InvalidArgument => StatusCode::BAD_REQUEST,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::FailedPrecondition => StatusCode::CONFLICT,
        Code::ResourceExhausted => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        http_status,
        Json(json!({
            "success": false,
            "error_code": status.code() as i32,
            "error_message": status.message()
        })),
    )
}

async fn get_tenant_quotas_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    if let Some(tenant_id) = query.get("tenant_id") {
        let tenant_id = parse_admin_tenant_id(tenant_id)?;
        let snapshot = service
            .get_tenant_quota_snapshot_for_tenant(&tenant_id)
            .map_err(status_error)?
            .ok_or_else(|| json_error(StatusCode::NOT_FOUND, "tenant quota not found"))?;
        Ok(Json(json!({ "success": true, "data": snapshot })))
    } else {
        let snapshots = service
            .list_tenant_quota_snapshots()
            .map_err(status_error)?;
        Ok(Json(json!({ "success": true, "data": snapshots })))
    }
}

async fn upsert_tenant_quota_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
    Json(body): Json<TenantQuotaPolicyRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let tenant_id = tenant_id_from_query(&query)?;
    if body.requested_quota_bytes == 0 {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "Tenant quota must be positive",
        ));
    }
    let snapshot = service
        .upsert_tenant_quota_policy_for_tenant(&tenant_id, body.requested_quota_bytes)
        .map_err(status_error)?;
    Ok(Json(json!({ "success": true, "data": snapshot })))
}

async fn delete_tenant_quota_handler(
    State(state): State<AdminRuntimeState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let tenant_id = tenant_id_from_query(&query)?;
    let snapshot = service
        .delete_tenant_quota_policy_for_tenant(&tenant_id)
        .map_err(status_error)?;
    Ok(Json(json!({ "success": true, "data": snapshot })))
}

fn build_metrics_summary_text(state: &AdminRuntimeState) -> String {
    let snapshot = state.snapshot();
    let service_ready = effective_service_ready(state, snapshot.service_ready);
    let mut summary = format!(
        "role={}, state={}, service_ready={}",
        snapshot.state.role(),
        snapshot.state.as_str(),
        service_ready
    );
    if let Some(view) = &snapshot.leader_view {
        summary.push_str(&format!(
            ", leader={}, view_version={}",
            view.leader_address, view.view_version
        ));
    }
    summary
}

fn build_health_json(state: &AdminRuntimeState) -> Value {
    let snapshot = state.snapshot();
    let service_ready = effective_service_ready(state, snapshot.service_ready);
    let mut value = json!({
        "status": "ok",
        "role": snapshot.state.role(),
        "ha_state": snapshot.state.as_str(),
        "service_ready": service_ready,
    });
    if let (Value::Object(map), Some(view)) = (&mut value, &snapshot.leader_view) {
        map.insert(
            "leader_address".to_string(),
            Value::String(view.leader_address.clone()),
        );
        map.insert("view_version".to_string(), json!(view.view_version));
    }
    value
}

fn effective_service_ready(state: &AdminRuntimeState, configured_ready: bool) -> bool {
    configured_ready
        && state
            .service
            .as_ref()
            .map_or(true, |service| service.is_service_available())
}

fn build_leader_json(state: &AdminRuntimeState) -> Value {
    match state.snapshot().leader_view {
        Some(view) => json!({
            "present": true,
            "leader_address": view.leader_address,
            "view_version": view.view_version,
        }),
        None => json!({ "present": false }),
    }
}

fn build_batch_query_keys_json(
    keys: &[String],
    results: &[proto::BatchGetReplicaListResult],
) -> Value {
    let mut data = serde_json::Map::new();
    for (key, result) in keys.iter().zip(results.iter()) {
        if result.status != 0 {
            data.insert(
                key.clone(),
                json!({
                    "ok": false,
                    "error": result.error_message,
                }),
            );
            continue;
        }

        let mut item = serde_json::Map::new();
        item.insert("ok".to_string(), json!(true));
        item.insert("values".to_string(), json!([]));

        if let Some(response) = &result.response {
            let mut memory_values = Vec::new();
            let mut disk_values = Vec::new();
            let mut local_disk_values = Vec::new();
            let mut nof_values = Vec::new();
            for replica in &response.replicas {
                match proto::replica_descriptor::ReplicaType::try_from(replica.replica_type) {
                    Ok(proto::replica_descriptor::ReplicaType::Memory) => {
                        memory_values.push(buffer_descriptor_json(replica));
                    }
                    Ok(proto::replica_descriptor::ReplicaType::Disk) => {
                        disk_values.push(json!({
                            "file_path": replica.file_path,
                            "object_size": replica.object_size,
                        }));
                    }
                    Ok(proto::replica_descriptor::ReplicaType::LocalDisk) => {
                        local_disk_values.push(json!({
                            "client_id": uuid_json_string(replica.local_disk_client_id.as_ref()),
                            "object_size": replica.object_size,
                            "transport_endpoint": replica.transport_endpoint,
                        }));
                    }
                    Ok(proto::replica_descriptor::ReplicaType::NofSsd) => {
                        nof_values.push(buffer_descriptor_json(replica));
                    }
                    _ => {}
                }
            }

            item.insert("values".to_string(), json!(memory_values));
            if !disk_values.is_empty() {
                item.insert("disk_values".to_string(), json!(disk_values));
            }
            if !local_disk_values.is_empty() {
                item.insert("local_disk_values".to_string(), json!(local_disk_values));
            }
            if !nof_values.is_empty() {
                item.insert("nof_values".to_string(), json!(nof_values));
            }
        }

        data.insert(key.clone(), Value::Object(item));
    }

    json!({
        "success": true,
        "data": data,
    })
}

fn build_segments_detail_json(segments: &[proto::SegmentDetailInfo]) -> Value {
    let items = segments
        .iter()
        .map(|segment| {
            let used = segment.allocator_used_bytes;
            let capacity = segment.allocator_capacity_bytes;
            let usage_percent = if capacity > 0 {
                used as f64 / capacity as f64 * 100.0
            } else {
                0.0
            };
            json!({
                "segment_name": &segment.segment_name,
                "segment_id": uuid_json_string(segment.segment_id.as_ref()),
                "client_id": uuid_json_string(segment.client_id.as_ref()),
                "base_address": format!("0x{:x}", segment.base_address),
                "size_bytes": segment.size_bytes,
                "size_human": format!("{:.6} GiB", segment.size_bytes as f64 / 1024.0 / 1024.0 / 1024.0),
                "te_endpoint": &segment.te_endpoint,
                "protocol": &segment.protocol,
                "status": segment_status_json_string(segment.status),
                "allocator_used_bytes": used,
                "allocator_capacity_bytes": capacity,
                "allocator_usage_percent": usage_percent,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "total_segments": segments.len(),
        "segments": items,
    })
}

fn build_kv_events_status_json(status: &crate::kv_event::KvEventStatus) -> Value {
    json!({
        "enabled": status.enabled,
        "published_batches": status.stats.published_batches,
        "published_events": status.stats.published_events,
        "dropped_events": status.stats.dropped_events,
        "skipped_unparsed_keys": status.stats.skipped_unparsed_keys,
    })
}

fn segment_status_json_string(status: i32) -> &'static str {
    match proto::SegmentStatus::try_from(status) {
        Ok(proto::SegmentStatus::Active) => "ACTIVE",
        Ok(proto::SegmentStatus::Draining) => "DRAINING",
        Ok(proto::SegmentStatus::Unavailable) => "UNAVAILABLE",
        Ok(proto::SegmentStatus::GracefullyUnmounting) => "GRACEFULLY_UNMOUNTING",
        _ => "UNDEFINED",
    }
}

fn buffer_descriptor_json(replica: &proto::ReplicaDescriptor) -> Value {
    json!({
        "size_": replica.size,
        "buffer_address_": replica.base_addr.wrapping_add(replica.offset),
        "protocol_": replica.protocol,
        "transport_endpoint_": replica.transport_endpoint,
    })
}

fn uuid_json_string(uuid: Option<&proto::Uuid>) -> String {
    uuid.map(|uuid| format!("{:016x}-{:016x}", uuid.high, uuid.low))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    async fn request_router(
        router: &Router,
        method: Method,
        path: &str,
        body: &str,
    ) -> (StatusCode, String) {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    async fn get_router(router: &Router, path: &str) -> (StatusCode, String) {
        request_router(router, Method::GET, path, "").await
    }

    async fn post_router(router: &Router, path: &str, body: &str) -> (StatusCode, String) {
        request_router(router, Method::POST, path, body).await
    }

    async fn create_drain_job(router: &Router, segment_name: &str) -> (StatusCode, Value) {
        let body = json!({ "segments": [segment_name] }).to_string();
        let (status, body) = post_router(router, "/api/v1/drain_jobs", &body).await;
        (status, serde_json::from_str(&body).unwrap())
    }

    async fn get(state: AdminRuntimeState, path: &str) -> (StatusCode, String) {
        get_router(&admin_router(state), path).await
    }

    fn leader_state() -> AdminRuntimeState {
        AdminRuntimeState::serving(Some(MasterView {
            leader_address: "127.0.0.1:50051".to_string(),
            view_version: 7,
        }))
    }

    fn runtime_state(state: MasterRuntimeState) -> AdminRuntimeState {
        AdminRuntimeState::new(state, None, state == MasterRuntimeState::Serving)
    }

    async fn router_with_memory_segment(segment_name: &str) -> Router {
        let service = Arc::new(MasterServiceImpl::new(None, None));
        let client_id = Uuid::from_u128(0x100);
        let (high, low) = client_id.as_u64_pair();
        service
            .mount_segment(TonicRequest::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid { high, low }),
                segment_name: segment_name.to_owned(),
                size: 8 * 1024 * 1024,
                base_addr: 0x3000_0000_0,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }))
            .await
            .unwrap();
        admin_router(AdminRuntimeState::serving_with_service(None, service))
    }

    async fn service_with_completed_memory_key(
        key: &str,
    ) -> (Arc<MasterServiceImpl>, Router, Uuid) {
        let service = Arc::new(MasterServiceImpl::new(None, None));
        let client_id = Uuid::from_u128(0x200);
        let (high, low) = client_id.as_u64_pair();
        service
            .mount_segment(TonicRequest::new(proto::MountSegmentRequest {
                client_id: Some(proto::Uuid { high, low }),
                segment_name: "admin_test_segment".to_string(),
                size: 8 * 1024 * 1024,
                base_addr: 0x3000_0000_0,
                te_endpoint: String::new(),
                protocol: String::new(),
                host_id: String::new(),
            }))
            .await
            .unwrap();
        put_complete_memory_key(&service, client_id, key).await;
        let router = admin_router(AdminRuntimeState::serving_with_service(
            None,
            service.clone(),
        ));
        (service, router, client_id)
    }

    async fn put_complete_memory_key(service: &MasterServiceImpl, client_id: Uuid, key: &str) {
        let (high, low) = client_id.as_u64_pair();
        service
            .put_start(TonicRequest::new(proto::PutStartRequest {
                client_id: Some(proto::Uuid { high, low }),
                key: key.to_owned(),
                slice_length: 1024,
                config: Some(proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "admin_test_segment".to_string(),
                    ..Default::default()
                }),
                tenant_id: String::new(),
            }))
            .await
            .unwrap();
        service
            .put_end(TonicRequest::new(proto::PutEndRequest {
                client_id: Some(proto::Uuid { high, low }),
                key: key.to_owned(),
                replica_type: proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: String::new(),
            }))
            .await
            .unwrap();
    }

    #[test]
    fn test_admin_health_matches_cpp_shape() {
        let health = build_health_json(&leader_state());

        assert_eq!(health["status"], "ok");
        assert_eq!(health["role"], "leader");
        assert_eq!(health["ha_state"], "serving");
        assert_eq!(health["service_ready"], true);
        assert_eq!(health["leader_address"], "127.0.0.1:50051");
        assert_eq!(health["view_version"], 7);
    }

    #[test]
    fn test_admin_leader_absent_shape() {
        let leader = build_leader_json(&AdminRuntimeState::serving(None));

        assert_eq!(leader, json!({ "present": false }));
    }

    #[test]
    fn test_admin_summary_includes_leader_when_present() {
        let summary = build_metrics_summary_text(&leader_state());

        assert!(summary.contains("role=leader"));
        assert!(summary.contains("state=serving"));
        assert!(summary.contains("service_ready=true"));
        assert!(summary.contains("leader=127.0.0.1:50051"));
        assert!(summary.contains("view_version=7"));
    }

    #[test]
    fn test_admin_health_reflects_service_gate() {
        let service = Arc::new(MasterServiceImpl::new(None, None));
        service.set_service_available(false);
        let state = AdminRuntimeState::serving_with_service(None, service);

        let health = build_health_json(&state);
        let summary = build_metrics_summary_text(&state);

        assert_eq!(health["service_ready"], false);
        assert!(summary.contains("service_ready=false"));
    }

    #[tokio::test]
    async fn test_admin_http_metrics_summary_serving_response() {
        let (status, body) = get(AdminRuntimeState::serving(None), "/metrics/summary").await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("role=leader"));
        assert!(body.contains("state=serving"));
    }

    #[tokio::test]
    async fn test_admin_http_health_standby_returns_ok_role() {
        let state = runtime_state(MasterRuntimeState::Standby);
        let (status, body) = get(state, "/health").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["role"], "standby");
    }

    #[tokio::test]
    async fn test_admin_http_health_serving_returns_leader() {
        let (status, body) = get(AdminRuntimeState::serving(None), "/health").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["role"], "leader");
    }

    #[tokio::test]
    async fn test_admin_http_health_includes_observed_leader() {
        let state = AdminRuntimeState::new(
            MasterRuntimeState::Standby,
            Some(MasterView {
                leader_address: "10.0.0.1:19000".to_string(),
                view_version: 42,
            }),
            false,
        );
        let (status, body) = get(state, "/health").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["leader_address"], "10.0.0.1:19000");
        assert_eq!(body["view_version"], 42);
    }

    #[tokio::test]
    async fn test_admin_http_role_serving_is_leader() {
        let (status, body) = get(runtime_state(MasterRuntimeState::Serving), "/role").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "leader");
    }

    #[tokio::test]
    async fn test_admin_http_role_standby_is_standby() {
        let (status, body) = get(runtime_state(MasterRuntimeState::Standby), "/role").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "standby");
    }

    #[tokio::test]
    async fn test_admin_http_role_candidate_is_standby() {
        let (status, body) = get(runtime_state(MasterRuntimeState::Candidate), "/role").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "standby");
    }

    #[tokio::test]
    async fn test_admin_http_role_recovering_is_standby() {
        let (status, body) = get(runtime_state(MasterRuntimeState::Recovering), "/role").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "standby");
    }

    #[tokio::test]
    async fn test_admin_http_ha_status_state_matrix() {
        let state = runtime_state(MasterRuntimeState::Starting);
        let router = admin_router(state.clone());

        for runtime_state in [
            MasterRuntimeState::Standby,
            MasterRuntimeState::Serving,
            MasterRuntimeState::Recovering,
            MasterRuntimeState::CatchingUp,
        ] {
            state.set_runtime_state(runtime_state);
            let (status, body) = get_router(&router, "/ha_status").await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, runtime_state.as_str());
        }
    }

    #[tokio::test]
    async fn test_admin_http_leader_absent_response() {
        let (status, body) = get(runtime_state(MasterRuntimeState::Standby), "/leader").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["present"], false);
    }

    #[tokio::test]
    async fn test_admin_http_leader_present_response() {
        let state = runtime_state(MasterRuntimeState::Standby);
        state.set_leader_view(Some(MasterView {
            leader_address: "192.168.1.1:19000".to_string(),
            view_version: 5,
        }));
        let (status, body) = get(state, "/leader").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["present"], true);
        assert_eq!(body["leader_address"], "192.168.1.1:19000");
        assert_eq!(body["view_version"], 5);
    }

    #[tokio::test]
    async fn test_admin_http_leader_clear_transition() {
        let state = runtime_state(MasterRuntimeState::Standby);
        let router = admin_router(state.clone());
        state.set_leader_view(Some(MasterView {
            leader_address: "192.168.1.1:19000".to_string(),
            view_version: 5,
        }));
        let (present_status, present_body) = get_router(&router, "/leader").await;
        state.set_leader_view(None);
        let (absent_status, absent_body) = get_router(&router, "/leader").await;
        let present_body: Value = serde_json::from_str(&present_body).unwrap();
        let absent_body: Value = serde_json::from_str(&absent_body).unwrap();

        assert_eq!(present_status, StatusCode::OK);
        assert_eq!(present_body["present"], true);
        assert_eq!(absent_status, StatusCode::OK);
        assert_eq!(absent_body["present"], false);
    }

    #[tokio::test]
    async fn test_admin_http_leader_warmup_role_and_health() {
        let state = runtime_state(MasterRuntimeState::LeaderWarmup);
        let router = admin_router(state);

        let (role_status, role_body) = get_router(&router, "/role").await;
        let (health_status, health_body) = get_router(&router, "/health").await;
        let health_body: Value = serde_json::from_str(&health_body).unwrap();

        assert_eq!(role_status, StatusCode::OK);
        assert_eq!(role_body, "leader");
        assert_eq!(health_status, StatusCode::OK);
        assert_eq!(health_body["role"], "leader");
        assert_eq!(health_body["ha_state"], "leader_warmup");
    }

    #[tokio::test]
    async fn test_admin_http_role_all_state_matrix() {
        let state = runtime_state(MasterRuntimeState::Starting);
        let router = admin_router(state.clone());

        for (runtime_state, expected_role) in [
            (MasterRuntimeState::Starting, "standby"),
            (MasterRuntimeState::Standby, "standby"),
            (MasterRuntimeState::Candidate, "standby"),
            (MasterRuntimeState::Recovering, "standby"),
            (MasterRuntimeState::CatchingUp, "standby"),
            (MasterRuntimeState::LeaderWarmup, "leader"),
            (MasterRuntimeState::Serving, "leader"),
        ] {
            state.set_runtime_state(runtime_state);
            let (status, body) = get_router(&router, "/role").await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, expected_role, "state={}", runtime_state.as_str());
        }
    }

    #[tokio::test]
    async fn test_admin_http_service_unavailable_route_matrix() {
        let router = admin_router(runtime_state(MasterRuntimeState::Standby));
        let unavailable = "service plane is not active";

        for path in [
            "/get_all_keys",
            "/get_all_segments",
            "/get_segments_detail",
            "/query_segment?segment=foo",
            "/query_key?key=foo",
            "/batch_query_keys?keys=foo",
            "/api/v1/segments/status?segment=foo",
            "/api/v1/tenant_quotas",
        ] {
            let (status, body) = get_router(&router, path).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "path={path}");
            assert!(body.contains(unavailable), "path={path}, body={body}");
        }

        let valid_job_id = "00000000-0000-0000-0000-000000000001";
        for (method, path, body) in [
            (Method::POST, "/api/v1/drain_jobs".to_string(), "{}"),
            (
                Method::GET,
                format!("/api/v1/drain_jobs/query?job_id={valid_job_id}"),
                "",
            ),
            (
                Method::POST,
                format!("/api/v1/drain_jobs/cancel?job_id={valid_job_id}"),
                "",
            ),
        ] {
            let (status, _) = request_router(&router, method, &path, body).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "path={path}");
        }
    }

    #[tokio::test]
    async fn test_admin_http_query_segment_aggregates_same_name_memory_shards() {
        let service = Arc::new(MasterServiceImpl::new(None, None));
        for (client_id, base_addr, size) in [
            (Uuid::from_u128(1), 0x1000_0000, 4096),
            (Uuid::from_u128(2), 0x2000_0000, 8192),
        ] {
            let (high, low) = client_id.as_u64_pair();
            service
                .mount_segment(TonicRequest::new(proto::MountSegmentRequest {
                    client_id: Some(proto::Uuid { high, low }),
                    segment_name: "sharded-admin-segment".to_string(),
                    size,
                    base_addr,
                    te_endpoint: String::new(),
                    protocol: String::new(),
                    host_id: String::new(),
                }))
                .await
                .unwrap();
        }
        let router = admin_router(AdminRuntimeState::serving_with_service(None, service));

        let (status, body) =
            get_router(&router, "/query_segment?segment=sharded-admin-segment").await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Used(bytes): 0"));
        assert!(body.contains("Capacity(bytes) : 12288"));
    }

    #[tokio::test]
    async fn test_admin_http_query_segment_existing_response() {
        let router = router_with_memory_segment("admin_test_segment").await;
        let (status, body) = get_router(&router, "/query_segment?segment=admin_test_segment").await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("admin_test_segment"));
        assert!(body.contains("Used(bytes)"));
        assert!(body.contains("Capacity(bytes)"));
    }

    #[tokio::test]
    async fn test_admin_http_query_segment_missing_is_500() {
        let service = Arc::new(MasterServiceImpl::new(None, None));
        let router = admin_router(AdminRuntimeState::serving_with_service(None, service));
        let (status, _) = get_router(&router, "/query_segment?segment=nonexistent_seg").await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_admin_http_get_all_segments_returns_mounted_segment() {
        let router = router_with_memory_segment("admin_test_segment").await;
        let (status, body) = get_router(&router, "/get_all_segments").await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("admin_test_segment"));
    }

    #[tokio::test]
    async fn test_admin_http_segments_detail_response() {
        let router = router_with_memory_segment("admin_test_segment").await;
        let (status, body) = get_router(&router, "/get_segments_detail").await;
        let body: Value = serde_json::from_str(&body).unwrap();
        let segments = body["segments"].as_array().unwrap();
        let segment = segments
            .iter()
            .find(|segment| segment["segment_name"] == "admin_test_segment")
            .unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(body["total_segments"].as_u64().unwrap() > 0);
        assert!(segment.get("allocator_used_bytes").is_some());
        assert!(segment.get("allocator_capacity_bytes").is_some());
    }

    #[tokio::test]
    async fn test_admin_http_segment_status_existing() {
        let router = router_with_memory_segment("admin_test_segment").await;
        let (status, body) = get_router(
            &router,
            "/api/v1/segments/status?segment=admin_test_segment",
        )
        .await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], true);
        assert_eq!(body["segment"], "admin_test_segment");
        assert!(!body["status_name"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_admin_http_segment_status_missing_parameter_is_400() {
        let service = Arc::new(MasterServiceImpl::new(None, None));
        let router = admin_router(AdminRuntimeState::serving_with_service(None, service));
        let (status, _) = get_router(&router, "/api/v1/segments/status").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_segment_status_unknown_is_404() {
        let service = Arc::new(MasterServiceImpl::new(None, None));
        let router = admin_router(AdminRuntimeState::serving_with_service(None, service));
        let (status, _) =
            get_router(&router, "/api/v1/segments/status?segment=no_such_segment").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_admin_http_get_all_keys_returns_stored_key() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, body) = get_router(&router, "/get_all_keys").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "admin_test_key\n");
    }

    #[tokio::test]
    async fn test_admin_http_get_all_keys_excludes_removed_key() {
        let (service, router, client_id) =
            service_with_completed_memory_key("admin_test_key").await;
        put_complete_memory_key(&service, client_id, "ephemeral_empty_test_key").await;
        service
            .remove(TonicRequest::new(proto::RemoveRequest {
                key: "ephemeral_empty_test_key".to_string(),
                force: false,
                tenant_id: String::new(),
            }))
            .await
            .unwrap();

        let (status, body) = get_router(&router, "/get_all_keys").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "admin_test_key\n");
    }

    #[tokio::test]
    async fn test_admin_http_query_key_existing_response() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, body) = get_router(&router, "/query_key?key=admin_test_key").await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"buffer_address_\""));
    }

    #[tokio::test]
    async fn test_admin_http_query_key_missing_is_404() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, _) = get_router(&router, "/query_key?key=nonexistent_key_xyz").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_admin_http_query_key_missing_parameter_is_404() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, _) = get_router(&router, "/query_key").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_admin_http_batch_query_keys_missing_parameter_is_400() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, _) = get_router(&router, "/batch_query_keys").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_batch_query_keys_empty_parameter_is_400() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, _) = get_router(&router, "/batch_query_keys?keys=").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_batch_query_keys_existing_key() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, body) = get_router(&router, "/batch_query_keys?keys=admin_test_key").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], true);
        assert!(body["data"].is_object());
        assert_eq!(body["data"]["admin_test_key"]["ok"], true);
    }

    #[tokio::test]
    async fn test_admin_http_batch_query_keys_missing_key_is_embedded_error() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, body) = get_router(&router, "/batch_query_keys?keys=nonexistent_key").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], true);
        assert_eq!(body["data"]["nonexistent_key"]["ok"], false);
    }

    #[tokio::test]
    async fn test_admin_http_batch_query_keys_multiple_keys() {
        let (service, router, client_id) =
            service_with_completed_memory_key("admin_test_key").await;
        put_complete_memory_key(&service, client_id, "second_key").await;

        let (status, body) =
            get_router(&router, "/batch_query_keys?keys=admin_test_key,second_key").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["admin_test_key"]["ok"], true);
        assert_eq!(body["data"]["second_key"]["ok"], true);

        service
            .remove(TonicRequest::new(proto::RemoveRequest {
                key: "second_key".to_string(),
                force: false,
                tenant_id: String::new(),
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_admin_http_batch_query_keys_mixed_results() {
        let (_, router, _) = service_with_completed_memory_key("admin_test_key").await;
        let (status, body) =
            get_router(&router, "/batch_query_keys?keys=admin_test_key,nonexistent").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], true);
        assert_eq!(body["data"]["admin_test_key"]["ok"], true);
        assert_eq!(body["data"]["nonexistent"]["ok"], false);
    }

    #[tokio::test]
    async fn test_admin_http_create_drain_job() {
        let router = router_with_memory_segment("drain_create_segment").await;
        let (status, body) = create_drain_job(&router, "drain_create_segment").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], true);
        assert!(
            body["job_id"]
                .as_str()
                .is_some_and(|job_id| !job_id.is_empty())
        );
        assert_eq!(body["status"], "CREATED");
    }

    #[tokio::test]
    async fn test_admin_http_create_drain_job_invalid_json() {
        let router = router_with_memory_segment("drain_invalid_json_segment").await;
        let (status, body) = post_router(&router, "/api/v1/drain_jobs", "not json").await;
        let body: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["success"], false);
    }

    #[tokio::test]
    async fn test_admin_http_create_drain_job_empty_segments_is_400() {
        let router = router_with_memory_segment("drain_empty_segment").await;
        let (status, _) = post_router(&router, "/api/v1/drain_jobs", "{}").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_query_created_drain_job() {
        let router = router_with_memory_segment("drain_query_segment").await;
        let (create_status, create_body) = create_drain_job(&router, "drain_query_segment").await;
        let job_id = create_body["job_id"].as_str().unwrap();
        let (query_status, query_body) = get_router(
            &router,
            &format!("/api/v1/drain_jobs/query?job_id={job_id}"),
        )
        .await;
        let query_body: Value = serde_json::from_str(&query_body).unwrap();

        assert_eq!(create_status, StatusCode::OK);
        assert_eq!(query_status, StatusCode::OK);
        assert_eq!(query_body["success"], true);
        assert_eq!(query_body["job_id"], job_id);
    }

    #[tokio::test]
    async fn test_admin_http_query_drain_job_invalid_id_is_400() {
        let router = router_with_memory_segment("drain_query_invalid_segment").await;
        let (status, _) = get_router(&router, "/api/v1/drain_jobs/query?job_id=not-a-uuid").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_query_drain_job_missing_id_is_400() {
        let router = router_with_memory_segment("drain_query_missing_segment").await;
        let (status, _) = get_router(&router, "/api/v1/drain_jobs/query").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_query_drain_job_unknown_id_is_404() {
        let router = router_with_memory_segment("drain_query_unknown_segment").await;
        let (status, _) = get_router(
            &router,
            "/api/v1/drain_jobs/query?job_id=00000000-0000-0000-0000-000000000001",
        )
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_admin_http_cancel_drain_job() {
        let router = router_with_memory_segment("drain_cancel_segment").await;
        let (create_status, create_body) = create_drain_job(&router, "drain_cancel_segment").await;
        let job_id = create_body["job_id"].as_str().unwrap();
        let (cancel_status, cancel_body) = post_router(
            &router,
            &format!("/api/v1/drain_jobs/cancel?job_id={job_id}"),
            "",
        )
        .await;
        let cancel_body: Value = serde_json::from_str(&cancel_body).unwrap();

        assert_eq!(create_status, StatusCode::OK);
        assert_eq!(cancel_status, StatusCode::OK);
        assert_eq!(cancel_body["success"], true);
        assert_eq!(cancel_body["job_id"], job_id);
        assert_eq!(cancel_body["status"], "CANCELED");
    }

    #[tokio::test]
    async fn test_admin_http_cancel_drain_job_invalid_id_is_400() {
        let router = router_with_memory_segment("drain_cancel_invalid_segment").await;
        let (status, _) =
            post_router(&router, "/api/v1/drain_jobs/cancel?job_id=not-a-uuid", "").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_cancel_drain_job_missing_id_is_400() {
        let router = router_with_memory_segment("drain_cancel_missing_segment").await;
        let (status, _) = post_router(&router, "/api/v1/drain_jobs/cancel", "").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_admin_http_drain_job_full_lifecycle() {
        let router = router_with_memory_segment("drain_lifecycle_segment").await;
        let (create_status, create_body) =
            create_drain_job(&router, "drain_lifecycle_segment").await;
        let job_id = create_body["job_id"].as_str().unwrap();

        let query_path = format!("/api/v1/drain_jobs/query?job_id={job_id}");
        let (first_query_status, first_query_body) = get_router(&router, &query_path).await;
        let first_query_body: Value = serde_json::from_str(&first_query_body).unwrap();

        let (cancel_status, cancel_body) = post_router(
            &router,
            &format!("/api/v1/drain_jobs/cancel?job_id={job_id}"),
            "",
        )
        .await;
        let cancel_body: Value = serde_json::from_str(&cancel_body).unwrap();

        let (final_query_status, final_query_body) = get_router(&router, &query_path).await;
        let final_query_body: Value = serde_json::from_str(&final_query_body).unwrap();

        assert_eq!(create_status, StatusCode::OK);
        assert_eq!(create_body["success"], true);
        assert_eq!(first_query_status, StatusCode::OK);
        assert_eq!(first_query_body["success"], true);
        assert_eq!(first_query_body["job_id"], job_id);
        assert_eq!(cancel_status, StatusCode::OK);
        assert_eq!(cancel_body["success"], true);
        assert_eq!(cancel_body["job_id"], job_id);
        assert_eq!(cancel_body["status"], "CANCELED");
        assert_eq!(final_query_status, StatusCode::OK);
        assert_eq!(final_query_body["success"], true);
    }

    #[test]
    fn test_batch_query_keys_formats_local_disk_values() {
        let key = "disk-key".to_string();
        let client_id = proto::Uuid { high: 1, low: 2 };
        let payload = build_batch_query_keys_json(
            std::slice::from_ref(&key),
            &[proto::BatchGetReplicaListResult {
                status: 0,
                response: Some(proto::GetReplicaListResponse {
                    replicas: vec![proto::ReplicaDescriptor {
                        segment_id: Some(proto::Uuid { high: 0, low: 0 }),
                        segment_name: "127.0.0.1:9999".to_string(),
                        slice_key_hash: vec![],
                        offset: 0,
                        status: proto::replica_descriptor::ReplicaStatus::Complete as i32,
                        replica_type: proto::replica_descriptor::ReplicaType::LocalDisk as i32,
                        size: 2048,
                        holder_client_id: Some(client_id.clone()),
                        transport_endpoint: "127.0.0.1:9999".to_string(),
                        file_path: String::new(),
                        object_size: 2048,
                        local_disk_client_id: Some(client_id),
                        base_addr: 0,
                        protocol: String::new(),
                        local_disk_storage_id: None,
                        local_disk_generation_id: None,
                    }],
                    lease_ttl_ms: 5000,
                }),
                error_message: String::new(),
            }],
        );

        assert_eq!(payload["success"], true);
        assert_eq!(payload["data"][&key]["ok"], true);
        assert_eq!(payload["data"][&key]["values"], json!([]));
        assert_eq!(
            payload["data"][&key]["local_disk_values"][0]["transport_endpoint"],
            "127.0.0.1:9999"
        );
    }

    #[test]
    fn test_segments_detail_json_matches_cpp_admin_shape() {
        let segment_id = proto::Uuid { high: 3, low: 4 };
        let client_id = proto::Uuid { high: 5, low: 6 };
        let payload = build_segments_detail_json(&[proto::SegmentDetailInfo {
            segment_name: "detail-host:1234".to_string(),
            segment_id: Some(segment_id),
            client_id: Some(client_id),
            base_address: 0x300000000,
            size_bytes: 1024 * 1024 * 1024,
            te_endpoint: "tcp://detail-host:1234".to_string(),
            protocol: "tcp".to_string(),
            status: proto::SegmentStatus::Active as i32,
            allocator_used_bytes: 256 * 1024 * 1024,
            allocator_capacity_bytes: 1024 * 1024 * 1024,
            nof: false,
            host_id: String::new(),
        }]);

        assert_eq!(payload["total_segments"], 1);
        let item = &payload["segments"][0];
        assert_eq!(item["segment_name"], "detail-host:1234");
        assert_eq!(item["segment_id"], "0000000000000003-0000000000000004");
        assert_eq!(item["client_id"], "0000000000000005-0000000000000006");
        assert_eq!(item["base_address"], "0x300000000");
        assert_eq!(item["size_bytes"], 1024 * 1024 * 1024);
        assert_eq!(item["size_human"], "1.000000 GiB");
        assert_eq!(item["te_endpoint"], "tcp://detail-host:1234");
        assert_eq!(item["protocol"], "tcp");
        assert_eq!(item["status"], "ACTIVE");
        assert_eq!(item["allocator_used_bytes"], 256 * 1024 * 1024);
        assert_eq!(item["allocator_capacity_bytes"], 1024 * 1024 * 1024);
        assert_eq!(item["allocator_usage_percent"], 25.0);
    }

    #[test]
    fn test_kv_events_status_json_matches_cpp_admin_shape() {
        let payload = build_kv_events_status_json(&crate::kv_event::KvEventStatus {
            enabled: true,
            bind_endpoint: "tcp://127.0.0.1:5557".to_string(),
            backend_id: "backend-a".to_string(),
            stats: crate::kv_event::KvEventStats {
                published_batches: 2,
                published_events: 3,
                dropped_events: 4,
                skipped_unparsed_keys: 5,
            },
        });

        assert_eq!(payload["enabled"], true);
        assert_eq!(payload["published_batches"], 2);
        assert_eq!(payload["published_events"], 3);
        assert_eq!(payload["dropped_events"], 4);
        assert_eq!(payload["skipped_unparsed_keys"], 5);
    }
}

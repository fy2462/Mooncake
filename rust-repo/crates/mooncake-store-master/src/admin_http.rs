use crate::ha::{MasterRuntimeState, MasterView};
use crate::proto;
use crate::{MasterServiceImpl, TenantId};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::get,
};
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::Code;

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
        .route("/get_all_keys", get(get_all_keys_handler))
        .route("/get_segments_detail", get(get_segments_detail_handler))
        .route("/batch_query_keys", get(batch_query_keys_handler))
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
    Ok(service.all_keys_for_admin().join("\n"))
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
                "error_message": "master service unavailable"
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
        "segment_id": uuid_json_string(replica.segment_id.as_ref()),
        "segment_name": replica.segment_name,
        "offset": replica.offset,
        "size": replica.size,
        "base_addr": replica.base_addr,
        "transport_endpoint": replica.transport_endpoint,
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
    use axum::http::Request;
    use tower::ServiceExt;

    async fn get_router(router: &Router, path: &str) -> (StatusCode, String) {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
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

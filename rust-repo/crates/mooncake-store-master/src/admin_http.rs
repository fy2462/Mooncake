use crate::ha::{MasterRuntimeState, MasterView};
use crate::MasterServiceImpl;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::Code;

#[derive(Clone)]
pub struct AdminRuntimeState {
    pub state: MasterRuntimeState,
    pub leader_view: Option<MasterView>,
    pub service_ready: bool,
    pub service: Option<Arc<MasterServiceImpl>>,
}

impl AdminRuntimeState {
    pub fn serving(leader_view: Option<MasterView>) -> Self {
        Self {
            state: MasterRuntimeState::Serving,
            leader_view,
            service_ready: true,
            service: None,
        }
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
}

pub fn admin_router(state: AdminRuntimeState) -> Router {
    Router::new()
        .route("/metrics/summary", get(metrics_summary_handler))
        .route("/health", get(health_handler))
        .route("/role", get(role_handler))
        .route("/ha_status", get(ha_status_handler))
        .route("/leader", get(leader_handler))
        .route(
            "/api/v1/tenant_quotas",
            get(get_tenant_quotas_handler)
                .put(upsert_tenant_quota_handler)
                .delete(delete_tenant_quota_handler),
        )
        .route(
            "/api/v1/tenant_quotas/default",
            get(get_default_tenant_quota_handler).put(set_default_tenant_quota_handler),
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
    state.state.role().to_string()
}

async fn ha_status_handler(
    axum::extract::State(state): axum::extract::State<AdminRuntimeState>,
) -> String {
    state.state.as_str().to_string()
}

async fn leader_handler(
    axum::extract::State(state): axum::extract::State<AdminRuntimeState>,
) -> Json<Value> {
    Json(build_leader_json(&state))
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
) -> Result<String, (StatusCode, Json<Value>)> {
    let Some(tenant_id) = query.get("tenant_id") else {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "Missing or invalid tenant_id",
        ));
    };
    if tenant_id.trim().is_empty() || tenant_id.starts_with('_') {
        return Err(json_error(StatusCode::BAD_REQUEST, "Invalid tenant_id"));
    }
    Ok(tenant_id.clone())
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
        if tenant_id.trim().is_empty() || tenant_id.starts_with('_') {
            return Err(json_error(StatusCode::BAD_REQUEST, "Invalid tenant_id"));
        }
        let snapshot = service
            .get_tenant_quota_snapshot(tenant_id)
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
        .upsert_tenant_quota_policy(&tenant_id, body.requested_quota_bytes)
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
        .delete_tenant_quota_policy(&tenant_id)
        .map_err(status_error)?;
    Ok(Json(json!({ "success": true, "data": snapshot })))
}

async fn get_default_tenant_quota_handler(
    State(state): State<AdminRuntimeState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let requested = service
        .get_default_tenant_quota_policy()
        .map_err(status_error)?;
    Ok(Json(
        json!({ "success": true, "requested_quota_bytes": requested }),
    ))
}

async fn set_default_tenant_quota_handler(
    State(state): State<AdminRuntimeState>,
    Json(body): Json<TenantQuotaPolicyRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let service = service_or_unavailable(&state)?;
    let requested = service
        .set_default_tenant_quota_policy(body.requested_quota_bytes)
        .map_err(status_error)?;
    Ok(Json(
        json!({ "success": true, "requested_quota_bytes": requested }),
    ))
}

fn build_metrics_summary_text(state: &AdminRuntimeState) -> String {
    let service_ready = effective_service_ready(state);
    let mut summary = format!(
        "role={}, state={}, service_ready={}",
        state.state.role(),
        state.state.as_str(),
        service_ready
    );
    if let Some(view) = &state.leader_view {
        summary.push_str(&format!(
            ", leader={}, view_version={}",
            view.leader_address, view.view_version
        ));
    }
    summary
}

fn build_health_json(state: &AdminRuntimeState) -> Value {
    let service_ready = effective_service_ready(state);
    let mut value = json!({
        "status": "ok",
        "role": state.state.role(),
        "ha_state": state.state.as_str(),
        "service_ready": service_ready,
    });
    if let (Value::Object(map), Some(view)) = (&mut value, &state.leader_view) {
        map.insert(
            "leader_address".to_string(),
            Value::String(view.leader_address.clone()),
        );
        map.insert("view_version".to_string(), json!(view.view_version));
    }
    value
}

fn effective_service_ready(state: &AdminRuntimeState) -> bool {
    state.service_ready
        && state
            .service
            .as_ref()
            .map_or(true, |service| service.is_service_available())
}

fn build_leader_json(state: &AdminRuntimeState) -> Value {
    match &state.leader_view {
        Some(view) => json!({
            "present": true,
            "leader_address": view.leader_address,
            "view_version": view.view_version,
        }),
        None => json!({ "present": false }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leader_state() -> AdminRuntimeState {
        AdminRuntimeState::serving(Some(MasterView {
            leader_address: "127.0.0.1:50051".to_string(),
            view_version: 7,
        }))
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
}

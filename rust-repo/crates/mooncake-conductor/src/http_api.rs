//! Axum HTTP API server for mooncake-conductor.
//!
//! Routes: POST /query, POST /register, POST /unregister, GET /global_view.
//! Mirrors the Go conductor's HTTP endpoints in kvevent/event_manager.go.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::prefix_index::{ModelContext, PrefixCacheTable};
use crate::types::*;

// --- Shared application state ---

/// Shared state passed to all axum handlers.
pub struct AppState {
    pub indexer: Arc<PrefixCacheTable>,
    pub subscribers: dashmap::DashMap<String, ()>,
    pub active_configs: dashmap::DashMap<String, ServiceConfig>,
    pub tenant_instance_map: parking_lot::RwLock<HashMap<String, HashMap<String, ()>>>,
}

impl AppState {
    pub fn new(indexer: Arc<PrefixCacheTable>) -> Self {
        Self {
            indexer,
            subscribers: dashmap::DashMap::new(),
            active_configs: dashmap::DashMap::new(),
            tenant_instance_map: parking_lot::RwLock::new(HashMap::new()),
        }
    }
}

// --- Router ---

/// Create the axum Router with all HTTP endpoints.
pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/query", post(query_handler))
        .route("/register", post(register_handler))
        .route("/unregister", post(unregister_handler))
        .route("/global_view", get(global_view_handler))
        .with_state(state)
}

// --- Start server ---

/// Start the HTTP server, returning when the server exits.
pub async fn serve(
    state: Arc<AppState>,
    port: u16,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), String> {
    let app = create_router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("HTTP server listening on port {}", port);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("bind: {}", e))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown.await;
            info!("Shutting down HTTP server");
        })
        .await
        .map_err(|e| format!("server error: {}", e))?;

    Ok(())
}

// --- Handlers ---

/// POST /query — compute cache hit for a token sequence.
async fn query_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    debug!("query: model={}", req.model);

    let tenant_id = req.tenant_id.as_deref().unwrap_or("default").to_string();
    let lora_name = req.lora_name.unwrap_or_default();
    let cache_salt = req.cache_salt.unwrap_or_default();

    let model_ctx = ModelContext {
        tenant_id: tenant_id.clone(),
        model_name: req.model.clone(),
        lora_name,
        block_size: req.block_size,
        additional_salt: cache_salt,
    };

    let mut response = serde_json::Map::new();

    if let Some(ref instance_id) = req.instance_id {
        info!("search specific instance: {}", instance_id);
        let result = state
            .indexer
            .cache_hit_compute(&model_ctx, &req.token_ids, instance_id);
        let tenant_map = response
            .entry(tenant_id)
            .or_insert_with(|| json!({}));
        if let serde_json::Value::Object(ref mut map) = tenant_map {
            map.insert(
                instance_id.clone(),
                serde_json::to_value(&result).unwrap_or_default(),
            );
        }
    } else {
        let instance_map = state.tenant_instance_map.read();
        if let Some(instances) = instance_map.get(&tenant_id) {
            for instance_id in instances.keys() {
                let result = state
                    .indexer
                    .cache_hit_compute(&model_ctx, &req.token_ids, instance_id);
                let tenant_map = response
                    .entry(tenant_id.clone())
                    .or_insert_with(|| json!({}));
                if let serde_json::Value::Object(ref mut map) = tenant_map {
                    map.insert(
                        instance_id.clone(),
                        serde_json::to_value(&result).unwrap_or_default(),
                    );
                }
            }
        } else {
            warn!("tenant has no engine instances: tenant_id={}", tenant_id);
        }
    }

    debug!("cache hit status: {:?}", response);
    Ok(Json(serde_json::Value::Object(response)))
}

/// POST /register — dynamically register a service.
async fn register_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant_id = req.tenant_id.as_deref().unwrap_or("default").to_string();
    let lora_name = req.lora_name.unwrap_or_default();
    let additional_salt = req.additionalsalt.unwrap_or_default();

    let svc = ServiceConfig {
        endpoint: req.endpoint,
        replay_endpoint: req.replay_endpoint,
        service_type: req.service_type,
        model_name: req.modelname,
        lora_name: lora_name.clone(),
        tenant_id: tenant_id.clone(),
        instance_id: req.instance_id.clone(),
        block_size: req.block_size,
        dp_rank: req.dp_rank,
        additional_salt: additional_salt.clone(),
    };

    let svc_key = make_service_key(&svc.instance_id, &svc.tenant_id, svc.dp_rank);

    if state.active_configs.contains_key(&svc_key) {
        return Ok(Json(json!({
            "status": "already registered",
            "instance_id": svc.instance_id,
        })));
    }

    state.active_configs.insert(svc_key, svc.clone());

    // Register in tenant instance map
    {
        let mut map = state.tenant_instance_map.write();
        map.entry(tenant_id.clone())
            .or_default()
            .insert(svc.instance_id.clone(), ());
    }

    // Register DP size in the indexer
    let model_ctx = ModelContext {
        tenant_id,
        model_name: svc.model_name.clone(),
        lora_name,
        block_size: svc.block_size,
        additional_salt,
    };
    state
        .indexer
        .add_dp_size(&model_ctx, &svc.instance_id, svc.dp_rank);

    info!("Dynamic register: instance_id={}", svc.instance_id);
    Ok(Json(json!({
        "status": "registered successfully",
        "instance_id": svc.instance_id,
    })))
}

/// POST /unregister — dynamically unregister a service.
async fn unregister_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UnregisterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let target_tenant = req.tenant_id.as_deref().unwrap_or("default").to_string();
    let target_key = make_service_key(&req.instance_id, &target_tenant, req.dp_rank);

    if !state.active_configs.contains_key(&target_key) {
        return Err((
            StatusCode::NOT_FOUND,
            format!("service not found: {}", target_key),
        ));
    }

    state.subscribers.remove(&target_key);
    state.active_configs.remove(&target_key);

    // Remove from tenant instance map
    {
        let mut map = state.tenant_instance_map.write();
        if let Some(instances) = map.get_mut(&target_tenant) {
            instances.remove(&req.instance_id);
        }
    }

    info!("Dynamic unregister: key={}", target_key);
    Ok(Json(json!({
        "status": "unregistered successfully",
        "removed_instances": [target_key],
    })))
}

/// GET /global_view — debug view of all contexts.
async fn global_view_handler(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let view = state.indexer.get_global_view();
    Json(serde_json::to_value(&view).unwrap_or_default())
}

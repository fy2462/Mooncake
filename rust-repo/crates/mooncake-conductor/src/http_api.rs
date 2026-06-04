// ============================================================================
// HTTP API Server — Axum HTTP API 服务器
//
// Provides REST endpoints for the Conductor service:
//   POST /query         —  compute cache hit for a token sequence
//   POST /register      —  dynamically register a new service instance
//   POST /unregister    —  dynamically remove a service instance
//   GET  /global_view   —  diagnostic snapshot of all model contexts
//
// Mirrors the Go conductor's HTTP endpoints in kvevent/event_manager.go.
// 对应 Go conductor kvevent/event_manager.go 中的 HTTP 端点。
// ============================================================================

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

// ----------------------------------------------------------------------------
// Shared application state / 共享应用状态
// ----------------------------------------------------------------------------

/// State shared across all axum handlers. Wraps the prefix index plus
/// runtime registration data for dynamic service management.
///
/// 所有 axum 处理器共享的状态。包装前缀索引和动态服务管理的运行时注册数据。
pub struct AppState {
    /// Shared prefix cache index. / 共享前缀缓存索引。
    pub indexer: Arc<PrefixCacheTable>,
    /// Active subscribers (for unregister). / 活跃的订阅者（用于注销）。
    pub subscribers: dashmap::DashMap<String, ()>,
    /// Active service configs (for unregister). / 活跃的服务配置（用于注销）。
    pub active_configs: dashmap::DashMap<String, ServiceConfig>,
    /// Tenant → instance_id set mapping for broadcast queries.
    /// 租户 → 实例 ID 集合映射，用于广播查询。
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

// ----------------------------------------------------------------------------
// Router / 路由
// ----------------------------------------------------------------------------

/// Create the axum Router with all HTTP endpoints.
/// 创建包含所有 HTTP 端点的 axum Router。
pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/query", post(query_handler))
        .route("/register", post(register_handler))
        .route("/unregister", post(unregister_handler))
        .route("/global_view", get(global_view_handler))
        .with_state(state)
}

// ----------------------------------------------------------------------------
// Server start / 启动服务器
// ----------------------------------------------------------------------------

/// Start the HTTP server with graceful shutdown support.
/// The server binds to 0.0.0.0:{port} and runs until the shutdown signal fires.
///
/// 启动支持优雅关闭的 HTTP 服务器。绑定到 0.0.0.0:{port}，
/// 运行直到 shutdown 信号触发。
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

// ----------------------------------------------------------------------------
// Handlers / 处理器
// ----------------------------------------------------------------------------

/// POST /query — compute cache hit for a token sequence.
/// Walks the prefix hash chain to find the longest consecutive prefix match.
/// Supports both targeted (single instance) and broadcast (all instances) queries.
///
/// POST /query —— 计算 token 序列的缓存命中。
/// 遍历前缀哈希链找到最长的连续前缀匹配。
/// 支持定向查询（单实例）和广播查询（所有实例）。
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
        // Targeted query: single instance / 定向查询：单个实例
        info!("search specific instance: {}", instance_id);
        let result = state
            .indexer
            .cache_hit_compute(&model_ctx, &req.token_ids, instance_id);
        let tenant_map = response.entry(tenant_id).or_insert_with(|| json!({}));
        if let serde_json::Value::Object(ref mut map) = tenant_map {
            map.insert(
                instance_id.clone(),
                serde_json::to_value(&result).unwrap_or_default(),
            );
        }
    } else {
        // Broadcast query: all instances for this tenant / 广播查询：此租户的所有实例
        let instance_map = state.tenant_instance_map.read();
        if let Some(instances) = instance_map.get(&tenant_id) {
            for instance_id in instances.keys() {
                let result =
                    state
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

/// POST /register — dynamically register a new service instance.
/// Adds the service to the active config set and registers DP size in the indexer.
///
/// POST /register —— 动态注册新的服务实例。
/// 将服务添加到活跃配置集并在索引器中注册 DP size。
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

    // Idempotent: if already registered, return success without side effects.
    // 幂等：如果已注册，无副作用返回成功。
    if state.active_configs.contains_key(&svc_key) {
        return Ok(Json(json!({
            "status": "already registered",
            "instance_id": svc.instance_id,
        })));
    }

    state.active_configs.insert(svc_key, svc.clone());

    // Register in tenant → instance map for broadcast queries.
    // 在 tenant → instance 映射中注册，用于广播查询。
    {
        let mut map = state.tenant_instance_map.write();
        map.entry(tenant_id.clone())
            .or_default()
            .insert(svc.instance_id.clone(), ());
    }

    // Register DP rank in the indexer for load-aware routing.
    // 在索引器中注册 DP rank，用于负载感知路由。
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

/// POST /unregister — dynamically remove a service instance.
/// Removes from active configs, subscriber set, and tenant instance map.
///
/// POST /unregister —— 动态移除服务实例。
/// 从活跃配置、订阅者集合和租户实例映射中移除。
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

    // Clean up tenant instance map. / 清理租户实例映射。
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

/// GET /global_view — diagnostic snapshot of all contexts.
/// GET /global_view —— 所有上下文的诊断快照。
async fn global_view_handler(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let view = state.indexer.get_global_view();
    Json(serde_json::to_value(&view).unwrap_or_default())
}

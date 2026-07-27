// =============================================================================
// HTTP Metadata Server — HTTP 元数据服务器
// =============================================================================
// Provides a lightweight HTTP-based metadata service as an alternative to etcd
// for node registration and master address discovery.
// 提供一个轻量级的基于 HTTP 的元数据服务，作为 etcd 的替代方案，
// 用于节点注册和 master 地址发现。
//
// Architecture / 架构:
// ┌──────────────────────────────────────────────────┐
// │  HTTP Server (axum)                               │
// │  GET /metadata → JSON response                    │
// │  {                                                │
// │    nodes: { hostname → MetadataNodeInfo },         │
// │    master_addr: "host:port"                       │
// │  }                                                │
// └──────────────────────────────────────────────────┘
//
// Use case / 使用场景:
// - Small deployments where etcd is not available.
//   小型部署中 etcd 不可用的场景。
// - Testing and development environments.
//   测试和开发环境。
// - Simpler operational overhead compared to running etcd cluster.
//   相比运行 etcd 集群，运维开销更简单。

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Information about a single node registered in the cluster.
/// 集群中单个已注册节点的信息。
#[derive(Debug, Clone, Serialize)]
pub struct MetadataNodeInfo {
    /// Hostname of the node.
    /// 节点的主机名。
    pub local_hostname: String,
    /// RPC port exposed by this node.
    /// 该节点暴露的 RPC 端口。
    pub rpc_port: u16,
    /// RDMA device names available on this node.
    /// 该节点上可用的 RDMA 设备名称。
    pub rdma_devices: Vec<String>,
}

/// HTTP response structure for the /metadata endpoint.
/// /metadata 端点的 HTTP 响应结构。
///
/// Contains all registered nodes and the current master address.
/// 包含所有已注册节点和当前 master 地址。
#[derive(Debug, Clone, Serialize)]
struct MetadataResponse {
    /// Map of hostname → node info.
    /// 主机名 → 节点信息的映射。
    nodes: HashMap<String, MetadataNodeInfo>,
    /// Current master address (host:port).
    /// 当前 master 地址（host:port）。
    master_addr: String,
}

/// Shared state for the HTTP metadata server.
/// HTTP 元数据服务器的共享状态。
///
/// Wrapped in Arc<RwLock<...>> for concurrent access from both the HTTP handler
/// and the master service logic.
/// 使用 Arc<RwLock<...>> 包装，支持 HTTP 处理器和 master 服务逻辑的并发访问。
#[derive(Clone)]
pub struct MetadataState {
    /// Registered nodes: hostname → MetadataNodeInfo.
    /// 已注册节点：主机名 → MetadataNodeInfo。
    pub nodes: std::sync::Arc<tokio::sync::RwLock<HashMap<String, MetadataNodeInfo>>>,
    /// Current master address string.
    /// 当前 master 地址字符串。
    pub master_addr: std::sync::Arc<tokio::sync::RwLock<String>>,
    /// Transfer Engine bootstrap metadata keyed by the `key` query parameter.
    metadata: std::sync::Arc<tokio::sync::RwLock<HashMap<String, Vec<u8>>>>,
    metadata_prefix: std::sync::Arc<str>,
}

impl MetadataState {
    /// Create a new MetadataState with the given initial master address.
    /// 使用给定的初始 master 地址创建新的 MetadataState。
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self::with_metadata_cluster_id(master_addr, "")
    }

    pub fn from_environment(master_addr: impl Into<String>) -> Self {
        let cluster_id = std::env::var("MC_METADATA_CLUSTER_ID").unwrap_or_default();
        Self::with_metadata_cluster_id(master_addr, &cluster_id)
    }

    pub fn with_metadata_cluster_id(master_addr: impl Into<String>, cluster_id: &str) -> Self {
        Self {
            nodes: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            master_addr: std::sync::Arc::new(tokio::sync::RwLock::new(master_addr.into())),
            metadata: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            metadata_prefix: metadata_prefix(cluster_id).into(),
        }
    }

    /// Register or update a node's metadata.
    /// 注册或更新节点的元数据。
    ///
    /// If the hostname already exists, its info is overwritten.
    /// 若主机名已存在，则覆盖其信息。
    pub async fn register_node(&self, hostname: String, rpc_port: u16, rdma_devices: Vec<String>) {
        self.nodes.write().await.insert(
            hostname.clone(),
            MetadataNodeInfo {
                local_hostname: hostname,
                rpc_port,
                rdma_devices,
            },
        );
    }

    /// Remove a node's metadata entry.
    /// 删除节点的元数据条目。
    pub async fn remove_node(&self, hostname: &str) -> Option<MetadataNodeInfo> {
        self.nodes.write().await.remove(hostname)
    }

    /// Blocking variant for background worker threads.
    /// 后台 worker 线程使用的阻塞删除接口。
    pub fn remove_node_blocking(&self, hostname: &str) -> Option<MetadataNodeInfo> {
        self.nodes.blocking_write().remove(hostname)
    }

    pub async fn remove_segment_metadata(&self, segment_name: &str) -> (bool, bool) {
        let (ram_key, rpc_key) = self.segment_metadata_keys(segment_name);
        let mut metadata = self.metadata.write().await;
        (
            metadata.remove(&ram_key).is_some(),
            metadata.remove(&rpc_key).is_some(),
        )
    }

    pub fn remove_segment_metadata_blocking(&self, segment_name: &str) -> (bool, bool) {
        let (ram_key, rpc_key) = self.segment_metadata_keys(segment_name);
        let mut metadata = self.metadata.blocking_write();
        (
            metadata.remove(&ram_key).is_some(),
            metadata.remove(&rpc_key).is_some(),
        )
    }

    fn segment_metadata_keys(&self, segment_name: &str) -> (String, String) {
        (
            format!("{}ram/{segment_name}", self.metadata_prefix),
            format!("{}rpc_meta/{segment_name}", self.metadata_prefix),
        )
    }

    /// Update the master address.
    /// 更新 master 地址。
    pub async fn set_master_addr(&self, master_addr: impl Into<String>) {
        *self.master_addr.write().await = master_addr.into();
    }

    /// Get the current master address.
    /// 获取当前 master 地址。
    pub async fn get_master_addr(&self) -> String {
        self.master_addr.read().await.clone()
    }
}

fn metadata_prefix(cluster_id: &str) -> String {
    if cluster_id.is_empty() {
        "mooncake/".to_string()
    } else {
        let mut prefix = format!("mooncake/{cluster_id}");
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        prefix
    }
}

#[derive(Debug, Deserialize)]
struct MetadataQuery {
    key: Option<String>,
}

#[derive(Clone)]
struct MetadataHttpState {
    metadata: MetadataState,
    service: Option<Arc<crate::service::MasterServiceImpl>>,
}

impl MetadataHttpState {
    fn serving(metadata: MetadataState) -> Self {
        Self {
            metadata,
            service: None,
        }
    }

    fn with_service_gate(
        metadata: MetadataState,
        service: Arc<crate::service::MasterServiceImpl>,
    ) -> Self {
        Self {
            metadata,
            service: Some(service),
        }
    }

    fn begin_request(
        &self,
    ) -> Result<Option<crate::service::state::ForegroundRequestGuard>, Response> {
        let Some(service) = self.service.as_ref() else {
            return Ok(None);
        };
        service.begin_foreground_request().map(Some).map_err(|_| {
            text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "master service is not serving",
            )
        })
    }

    fn begin_mutation(&self) -> Result<Option<parking_lot::RwLockReadGuard<'_, ()>>, Response> {
        let Some(service) = self.service.as_ref() else {
            return Ok(None);
        };
        service.begin_external_mutation().map(Some).ok_or_else(|| {
            text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "master service is not serving",
            )
        })
    }
}

fn text_response(status: StatusCode, body: &'static str) -> Response {
    (status, body).into_response()
}

/// GET with `?key=` implements the Transfer Engine metadata protocol. A GET
/// without a key preserves the older Rust aggregate endpoint.
async fn get_metadata_handler(
    State(http_state): State<MetadataHttpState>,
    Query(query): Query<MetadataQuery>,
) -> Response {
    let _request_guard = match http_state.begin_request() {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    let state = &http_state.metadata;
    if let Some(key) = query.key.filter(|key| !key.is_empty()) {
        let metadata = state.metadata.read().await;
        let Some(value) = metadata.get(&key) else {
            return text_response(StatusCode::NOT_FOUND, "metadata not found");
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(value.clone()))
            .unwrap_or_else(|_| {
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "response build failed")
            });
    }

    let nodes = state.nodes.read().await.clone();
    let master_addr = state.get_master_addr().await;
    Json(MetadataResponse { nodes, master_addr }).into_response()
}

async fn put_metadata_handler(
    State(http_state): State<MetadataHttpState>,
    Query(query): Query<MetadataQuery>,
    body: Bytes,
) -> Response {
    let _request_guard = match http_state.begin_request() {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    let Some(key) = query.key.filter(|key| !key.is_empty()) else {
        return text_response(StatusCode::BAD_REQUEST, "Missing key parameter");
    };

    let mut metadata = http_state.metadata.metadata.write().await;
    let _mutation_guard = match http_state.begin_mutation() {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    if key.contains("rpc_meta") {
        if let Some(existing) = metadata.get(&key) {
            if existing.as_slice() == body.as_ref() {
                return text_response(StatusCode::OK, "metadata unchanged");
            }
            return text_response(
                StatusCode::BAD_REQUEST,
                "Duplicate rpc_meta key not allowed",
            );
        }
    }
    metadata.insert(key, body.to_vec());
    text_response(StatusCode::OK, "metadata updated")
}

async fn delete_metadata_handler(
    State(http_state): State<MetadataHttpState>,
    Query(query): Query<MetadataQuery>,
) -> Response {
    let _request_guard = match http_state.begin_request() {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    let Some(key) = query.key.filter(|key| !key.is_empty()) else {
        return text_response(StatusCode::BAD_REQUEST, "Missing key parameter");
    };
    let mut metadata = http_state.metadata.metadata.write().await;
    let _mutation_guard = match http_state.begin_mutation() {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    if metadata.remove(&key).is_none() {
        return text_response(StatusCode::NOT_FOUND, "metadata not found");
    }
    text_response(StatusCode::OK, "metadata deleted")
}

/// Start the HTTP metadata server on the given address.
/// 在给定地址上启动 HTTP 元数据服务器。
///
/// This function blocks indefinitely (axum::serve) — call it in a spawned task.
/// 此函数无限期阻塞（axum::serve）—— 在 spawned task 中调用。
///
/// Example / 示例:
/// ```ignore
/// let state = MetadataState::new("localhost:50051");
/// tokio::spawn(serve_metadata_http("0.0.0.0:8080".parse().unwrap(), state));
/// ```
pub async fn bind_metadata_listener(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr).await
}

pub async fn serve_metadata_listener(
    listener: tokio::net::TcpListener,
    state: MetadataState,
) -> std::io::Result<()> {
    serve_metadata_listener_with_state(listener, MetadataHttpState::serving(state)).await
}

pub async fn serve_metadata_listener_with_service_gate(
    listener: tokio::net::TcpListener,
    state: MetadataState,
    service: Arc<crate::service::MasterServiceImpl>,
) -> std::io::Result<()> {
    serve_metadata_listener_with_state(
        listener,
        MetadataHttpState::with_service_gate(state, service),
    )
    .await
}

async fn serve_metadata_listener_with_state(
    listener: tokio::net::TcpListener,
    state: MetadataHttpState,
) -> std::io::Result<()> {
    let app = Router::new()
        .route(
            "/metadata",
            get(get_metadata_handler)
                .put(put_metadata_handler)
                .delete(delete_metadata_handler),
        )
        .with_state(state);

    axum::serve(listener, app).await
}

pub async fn serve_metadata_http(addr: SocketAddr, state: MetadataState) -> std::io::Result<()> {
    let listener = bind_metadata_listener(addr).await?;
    serve_metadata_listener(listener, state).await
}

#[cfg(test)]
mod tests {
    use super::{MetadataState, metadata_prefix};

    #[test]
    fn metadata_cluster_prefix_matches_cpp_shape() {
        assert_eq!(metadata_prefix(""), "mooncake/");
        assert_eq!(metadata_prefix("cluster-a"), "mooncake/cluster-a/");
        assert_eq!(metadata_prefix("cluster-a/"), "mooncake/cluster-a/");
    }

    #[tokio::test]
    async fn segment_cleanup_removes_only_cluster_scoped_te_keys() {
        let state = MetadataState::with_metadata_cluster_id("", "cluster-a");
        {
            let mut metadata = state.metadata.write().await;
            metadata.insert("mooncake/cluster-a/ram/node-a:1234".into(), vec![1]);
            metadata.insert("mooncake/cluster-a/rpc_meta/node-a:1234".into(), vec![2]);
            metadata.insert("mooncake/cluster-a/ram/node-b:1234".into(), vec![3]);
            metadata.insert("mooncake/ram/node-a:1234".into(), vec![4]);
        }

        assert_eq!(
            state.remove_segment_metadata("node-a:1234").await,
            (true, true)
        );
        let metadata = state.metadata.read().await;
        assert!(!metadata.contains_key("mooncake/cluster-a/ram/node-a:1234"));
        assert!(!metadata.contains_key("mooncake/cluster-a/rpc_meta/node-a:1234"));
        assert!(metadata.contains_key("mooncake/cluster-a/ram/node-b:1234"));
        assert!(metadata.contains_key("mooncake/ram/node-a:1234"));
    }
}

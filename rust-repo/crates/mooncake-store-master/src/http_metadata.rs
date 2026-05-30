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

use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;

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
}

impl MetadataState {
    /// Create a new MetadataState with the given initial master address.
    /// 使用给定的初始 master 地址创建新的 MetadataState。
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self {
            nodes: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            master_addr: std::sync::Arc::new(tokio::sync::RwLock::new(master_addr.into())),
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

/// HTTP handler for GET /metadata.
/// GET /metadata 的 HTTP 处理器。
///
/// Returns a JSON snapshot of all registered nodes and the current master address.
/// 返回所有已注册节点和当前 master 地址的 JSON 快照。
async fn metadata_handler(State(state): State<MetadataState>) -> Json<MetadataResponse> {
    let nodes = state.nodes.read().await.clone();
    let master_addr = state.get_master_addr().await;
    Json(MetadataResponse { nodes, master_addr })
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
pub async fn serve_metadata_http(addr: SocketAddr, state: MetadataState) {
    let app = Router::new()
        .route("/metadata", get(metadata_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

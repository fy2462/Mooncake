use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize)]
pub struct MetadataNodeInfo {
    local_hostname: String,
    rpc_port: u16,
    rdma_devices: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MetadataResponse {
    nodes: HashMap<String, MetadataNodeInfo>,
    master_addr: String,
}

#[derive(Clone)]
pub struct MetadataState {
    pub nodes: std::sync::Arc<tokio::sync::RwLock<HashMap<String, MetadataNodeInfo>>>,
    pub master_addr: String,
}

impl MetadataState {
    pub fn new(master_addr: impl Into<String>) -> Self {
        Self {
            nodes: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            master_addr: master_addr.into(),
        }
    }

    pub async fn register_node(
        &self,
        hostname: String,
        rpc_port: u16,
        rdma_devices: Vec<String>,
    ) {
        self.nodes.write().await.insert(
            hostname.clone(),
            MetadataNodeInfo {
                local_hostname: hostname,
                rpc_port,
                rdma_devices,
            },
        );
    }
}

async fn metadata_handler(State(state): State<MetadataState>) -> Json<MetadataResponse> {
    let nodes = state.nodes.read().await.clone();
    Json(MetadataResponse {
        nodes,
        master_addr: state.master_addr.clone(),
    })
}

pub async fn serve_metadata_http(addr: SocketAddr, state: MetadataState) {
    let app = Router::new()
        .route("/metadata", get(metadata_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

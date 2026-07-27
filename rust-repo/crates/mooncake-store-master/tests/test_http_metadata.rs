use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::http_metadata::{
    MetadataNodeInfo, MetadataState, bind_metadata_listener, serve_metadata_listener,
    serve_metadata_listener_with_service_gate,
};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn send_http(address: std::net::SocketAddr, request: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

async fn start_metadata_server() -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = bind_metadata_listener("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_metadata_listener(
        listener,
        MetadataState::new("master:50051"),
    ));
    (address, server)
}

#[tokio::test]
async fn test_production_metadata_listener_rejects_requests_after_service_gate_closes() {
    let listener = bind_metadata_listener("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let service = Arc::new(MasterServiceImpl::new(None, None));
    service.set_service_available(false);
    let server = tokio::spawn(serve_metadata_listener_with_service_gate(
        listener,
        MetadataState::new("master:50051"),
        service,
    ));

    let get = send_http(
        address,
        "GET /metadata HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(get.starts_with("HTTP/1.1 503"), "{get}");
    let put = send_http(
        address,
        "PUT /metadata?key=rpc_meta/node-a HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
    )
    .await;
    assert!(put.starts_with("HTTP/1.1 503"), "{put}");

    server.abort();
}

#[test]
fn test_metadata_node_info_creation() {
    let node = MetadataNodeInfo {
        local_hostname: "node1".into(),
        rpc_port: 50051,
        rdma_devices: vec!["mlx5_0".into(), "mlx5_1".into()],
    };
    assert_eq!(node.local_hostname, "node1");
    assert_eq!(node.rpc_port, 50051);
    assert_eq!(node.rdma_devices.len(), 2);
}

#[tokio::test]
async fn test_metadata_listener_reports_occupied_port() {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();

    let error = bind_metadata_listener(address).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
}

#[test]
fn test_metadata_node_info_empty_devices() {
    let node = MetadataNodeInfo {
        local_hostname: "cpu-only".into(),
        rpc_port: 8080,
        rdma_devices: vec![],
    };
    assert!(node.rdma_devices.is_empty());
}

#[test]
fn test_metadata_node_info_clone() {
    let node = MetadataNodeInfo {
        local_hostname: "n1".into(),
        rpc_port: 9999,
        rdma_devices: vec!["dev0".into()],
    };
    let cloned = node.clone();
    assert_eq!(cloned.local_hostname, node.local_hostname);
    assert_eq!(cloned.rpc_port, node.rpc_port);
}

#[test]
fn test_metadata_state_new() {
    let state = MetadataState::new("192.168.1.1:50051");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let master = runtime.block_on(state.get_master_addr());
    assert_eq!(master, "192.168.1.1:50051");
}

#[tokio::test]
async fn test_metadata_state_set_master_addr() {
    let state = MetadataState::new("old:1");
    state.set_master_addr("new:2").await;
    assert_eq!(state.get_master_addr().await, "new:2");
}

#[tokio::test]
async fn test_metadata_state_register_node() {
    let state = MetadataState::new("127.0.0.1:50051");
    state
        .register_node("node-a".into(), 12345, vec!["mlx5_0".into()])
        .await;
    let nodes = state.nodes.read().await;
    assert!(nodes.contains_key("node-a"));
    let registered = nodes.get("node-a").unwrap();
    assert_eq!(registered.local_hostname, "node-a");
    assert_eq!(registered.rpc_port, 12345);
    assert_eq!(registered.rdma_devices, vec!["mlx5_0"]);
}

#[tokio::test]
async fn test_metadata_state_multiple_nodes() {
    let state = MetadataState::new("master:50051");
    state
        .register_node("n1".into(), 10001, vec!["d0".into()])
        .await;
    state
        .register_node("n2".into(), 10002, vec!["d1".into()])
        .await;
    state.register_node("n3".into(), 10003, vec![]).await;

    let nodes = state.nodes.read().await;
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains_key("n1"));
    assert!(nodes.contains_key("n2"));
    assert!(nodes.contains_key("n3"));
}

#[tokio::test]
async fn test_metadata_state_overwrite_node() {
    let state = MetadataState::new("m:1");
    state
        .register_node("n".into(), 100, vec!["old".into()])
        .await;
    state
        .register_node("n".into(), 200, vec!["new".into()])
        .await;

    let nodes = state.nodes.read().await;
    assert_eq!(nodes.len(), 1);
    let registered = nodes.get("n").unwrap();
    assert_eq!(registered.rpc_port, 200);
    assert_eq!(registered.rdma_devices, vec!["new"]);
}

#[test]
fn test_metadata_node_info_serialize() {
    let node = MetadataNodeInfo {
        local_hostname: "s1".into(),
        rpc_port: 443,
        rdma_devices: vec!["dev".into()],
    };
    let json = serde_json::to_string(&node).unwrap();
    assert!(json.contains("s1"));
    assert!(json.contains("443"));
    assert!(json.contains("dev"));
}

#[tokio::test]
async fn test_transfer_engine_http_metadata_get_put_delete_protocol() {
    let (address, server) = start_metadata_server().await;
    let key = "mooncake/rpc_meta/node-a";
    let body = r#"{"name":"node-a","port":12345}"#;

    let put = send_http(
        address,
        &format!(
            "PUT /metadata?key={key} HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    assert!(put.starts_with("HTTP/1.1 200"));
    assert!(put.ends_with("metadata updated"));

    let unchanged = send_http(
        address,
        &format!(
            "PUT /metadata?key={key} HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    assert!(unchanged.starts_with("HTTP/1.1 200"));
    assert!(unchanged.ends_with("metadata unchanged"));

    let conflicting_body = r#"{"name":"different"}"#;
    let conflict = send_http(
        address,
        &format!(
            "PUT /metadata?key={key} HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{conflicting_body}",
            conflicting_body.len()
        ),
    )
    .await;
    assert!(conflict.starts_with("HTTP/1.1 400"));

    let get = send_http(
        address,
        &format!(
            "GET /metadata?key={key} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(get.starts_with("HTTP/1.1 200"));
    assert!(get.contains("content-type: application/json"));
    assert!(get.ends_with(body));

    let delete = send_http(
        address,
        &format!(
            "DELETE /metadata?key={key} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(delete.starts_with("HTTP/1.1 200"));

    let missing = send_http(
        address,
        &format!(
            "GET /metadata?key={key} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(missing.starts_with("HTTP/1.1 404"));

    server.abort();
}

#[tokio::test]
async fn test_aggregate_metadata_advertises_rust_tonic_wire_identity() {
    let (address, server) = start_metadata_server().await;
    let response = send_http(
        address,
        &format!("GET /metadata HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"),
    )
    .await;
    let body = response.split_once("\r\n\r\n").unwrap().1;
    let document: serde_json::Value = serde_json::from_str(body).unwrap();

    assert_eq!(document["master_addr"], "master:50051");
    server.abort();
}

#[tokio::test]
async fn test_http_metadata_mutations_require_key() {
    let (address, server) = start_metadata_server().await;
    for request in [
        format!(
            "PUT /metadata HTTP/1.1\r\nHost: {address}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
        ),
        format!("DELETE /metadata HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"),
    ] {
        let response = send_http(address, &request).await;
        assert!(response.starts_with("HTTP/1.1 400"));
    }
    server.abort();
}

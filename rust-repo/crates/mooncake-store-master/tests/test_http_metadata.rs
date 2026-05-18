use mooncake_store_master::http_metadata::{MetadataNodeInfo, MetadataState};

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
    let master = state.master_addr.clone();
    assert_eq!(master, "192.168.1.1:50051");
}

#[tokio::test]
async fn test_metadata_state_register_node() {
    let state = MetadataState::new("127.0.0.1:50051");
    state.register_node("node-a".into(), 12345, vec!["mlx5_0".into()]).await;
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
    state.register_node("n1".into(), 10001, vec!["d0".into()]).await;
    state.register_node("n2".into(), 10002, vec!["d1".into()]).await;
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
    state.register_node("n".into(), 100, vec!["old".into()]).await;
    state.register_node("n".into(), 200, vec!["new".into()]).await;

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

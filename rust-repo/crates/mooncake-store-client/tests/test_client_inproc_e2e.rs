#![cfg(feature = "link-native")]

use mooncake_store_client::MooncakeClient;
use mooncake_store_core::ReplicateConfig;
use mooncake_store_master::MasterServiceImpl;
use mooncake_store_master::proto::master_service_server::MasterServiceServer;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

async fn start_master() -> (String, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = Arc::new(MasterServiceImpl::default());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MasterServiceServer::from_arc(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    (address.to_string(), shutdown_tx)
}

async fn create_tcp_client(master: &str) -> MooncakeClient {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_host = probe.local_addr().unwrap().to_string();
    drop(probe);
    MooncakeClient::create(
        master,
        "P2PHANDSHAKE",
        &local_host,
        "tcp",
        "",
        16 * 1024 * 1024,
        8 * 1024 * 1024,
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_tcp_clients_roundtrip_exact_bytes_through_one_master() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;
    let payload = b"rust-client-inproc-e2e";

    writer
        .put(
            "cross-client-roundtrip",
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: writer.get_hostname(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.get("cross-client-roundtrip").await.unwrap(), payload);

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_integration_basic_remove_batch_upsert_and_large_payload_parity() {
    let (master, shutdown) = start_master().await;
    let mut writer = create_tcp_client(&master).await;
    let mut reader = create_tcp_client(&master).await;
    let config = ReplicateConfig {
        replica_num: 1,
        preferred_segment: writer.get_hostname(),
        ..Default::default()
    };

    let large = vec![0x5a; 5 * 1024 * 1024];
    writer
        .put("large-object", &large, Some(config.clone()))
        .await
        .unwrap();
    assert_eq!(reader.get("large-object").await.unwrap(), large);

    let keys = vec!["batch-a".to_string(), "batch-b".to_string()];
    let first = b"first".as_slice();
    let second = b"second".as_slice();
    assert_eq!(
        writer
            .batch_put(&keys, &[first, second], Some(config.clone()))
            .await
            .unwrap(),
        vec![0, 0]
    );
    assert_eq!(
        reader.batch_is_exist(&keys).await.unwrap(),
        vec![true, true]
    );
    assert_eq!(
        reader.batch_get(&keys).await.unwrap(),
        vec![Some(first.to_vec()), Some(second.to_vec())]
    );

    writer
        .upsert("batch-a", b"same-size", Some(config.clone()))
        .await
        .unwrap();
    assert_eq!(reader.get("batch-a").await.unwrap(), b"same-size");
    let replacement = b"different-sized-replacement".as_slice();
    assert_eq!(
        writer
            .batch_upsert(&keys, &[replacement, b"B"], Some(config))
            .await
            .unwrap(),
        vec![0, 0]
    );
    assert_eq!(reader.get("batch-a").await.unwrap(), replacement);
    assert_eq!(reader.get("batch-b").await.unwrap(), b"B");

    let addresses = reader
        .batch_query_ip(&[writer.client_id(), reader.client_id()])
        .await
        .unwrap();
    assert_eq!(addresses.len(), 2);
    writer.remove("batch-a", true).await.unwrap();
    assert_eq!(
        reader.batch_is_exist(&keys).await.unwrap(),
        vec![false, true]
    );

    drop(writer);
    drop(reader);
    let _ = shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_tcp_clients_copy_and_move_preserve_bytes_across_distinct_segments() {
    let (master, shutdown) = start_master().await;
    let mut source = create_tcp_client(&master).await;
    let mut copy_target = create_tcp_client(&master).await;
    let mut move_target = create_tcp_client(&master).await;
    let source_name = source.get_hostname();
    let copy_target_name = copy_target.get_hostname();
    let move_target_name = move_target.get_hostname();
    let payload = b"three-node-copy-move-payload";

    source
        .put(
            "copy-move-object",
            payload,
            Some(ReplicateConfig {
                replica_num: 1,
                preferred_segment: source_name.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    source
        .copy(
            "copy-move-object",
            &source_name,
            std::slice::from_ref(&copy_target_name),
        )
        .await
        .unwrap();
    assert_eq!(copy_target.get("copy-move-object").await.unwrap(), payload);

    source
        .move_object("copy-move-object", &source_name, &move_target_name)
        .await
        .unwrap();
    assert_eq!(copy_target.get("copy-move-object").await.unwrap(), payload);
    assert_eq!(move_target.get("copy-move-object").await.unwrap(), payload);

    drop(source);
    drop(copy_target);
    drop(move_target);
    let _ = shutdown.send(());
}

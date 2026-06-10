#![cfg(feature = "link-native")]

use mooncake_store_client::MooncakeClient;

fn live_e2e_enabled() -> bool {
    matches!(
        std::env::var("MOONCAKE_CLIENT_E2E").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

fn master_candidates() -> Vec<String> {
    std::env::var("MOONCAKE_CLIENT_E2E_MASTERS")
        .or_else(|_| std::env::var("MOONCAKE_CLIENT_E2E_MASTER"))
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[tokio::test]
async fn test_live_client_ha_health_and_admin_e2e() {
    if !live_e2e_enabled() {
        eprintln!("skipping live client e2e; set MOONCAKE_CLIENT_E2E=1 to enable");
        return;
    }

    let masters = master_candidates();
    assert!(
        !masters.is_empty(),
        "at least one master address is required"
    );

    let metadata = std::env::var("MOONCAKE_CLIENT_E2E_METADATA")
        .unwrap_or_else(|_| "http://127.0.0.1:8080/metadata".to_string());
    let local_host = std::env::var("MOONCAKE_CLIENT_E2E_LOCAL_HOST")
        .unwrap_or_else(|_| "127.0.0.1:0".to_string());

    let mut client = MooncakeClient::create_with_master_candidates(
        &masters,
        &metadata,
        &local_host,
        "tcp",
        "",
        0,
        1024 * 1024,
    )
    .await
    .expect("client should connect to a live master");

    assert!(masters.contains(&client.current_master_addr()));
    assert_eq!(client.master_candidates(), masters);

    client
        .health_check()
        .await
        .expect("health_check should pass");
    assert!(client.is_ping_healthy());

    let _ = client
        .service_ready()
        .await
        .expect("service_ready should pass");
    let _ = client
        .get_storage_config()
        .await
        .expect("get_storage_config should pass");
}

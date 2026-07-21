use mooncake_store_client::ClientHttpConfig;

#[test]
fn client_http_is_disabled_by_default_on_port_9300() {
    let config = ClientHttpConfig::default();

    assert!(!config.enabled);
    assert_eq!(config.port, 9300);
}

#[test]
fn client_http_accepts_an_explicit_port() {
    let config = ClientHttpConfig {
        enabled: true,
        port: 19_300,
    };

    assert!(config.enabled);
    assert_eq!(config.port, 19_300);
}

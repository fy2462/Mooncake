use super::background::ClientBackgroundConfig;
use super::config::ClientConfig;
use super::finalize::{
    determine_finalize_decision, ReplicaFinalizeDecision, ReplicaTransferSummary,
};
use super::read::scoped_cache_key;
use super::{CachedQueryResultResponse, ClientHttpConfig, MooncakeClient};
use mooncake_store_core::{ReplicaDescriptor, ReplicaType, ReplicateConfig, StoreError};
use std::ffi::c_void;
use std::time::Duration;

#[test]
fn client_bootstrap_config_carries_http_configuration() {
    let masters = ["127.0.0.1:50051".to_string()];
    let config = ClientConfig {
        master_addrs: &masters,
        metadata_conn_string: "P2PHANDSHAKE",
        local_host: "127.0.0.1:50052",
        protocol: "tcp",
        device: "",
        global_segment_size: 0,
        local_buffer_size: 0,
        tenant_id: "default",
        http: ClientHttpConfig {
            enabled: true,
            port: 19_300,
        },
    };

    assert!(config.http.enabled);
    assert_eq!(config.http.port, 19_300);
}

#[test]
fn enabled_client_http_rejects_port_zero() {
    let error = MooncakeClient::validate_client_http_config(ClientHttpConfig {
        enabled: true,
        port: 0,
    })
    .unwrap_err();

    assert!(matches!(error, StoreError::InvalidParams(_)));
    assert!(
        MooncakeClient::validate_client_http_config(ClientHttpConfig {
            enabled: false,
            port: 0,
        })
        .is_ok()
    );
}

#[test]
fn normalize_master_url_accepts_plain_and_url_forms() {
    assert_eq!(
        MooncakeClient::normalize_master_url("127.0.0.1:50051").unwrap(),
        "http://127.0.0.1:50051"
    );
    assert_eq!(
        MooncakeClient::normalize_master_url("http://leader:50051").unwrap(),
        "http://leader:50051"
    );
    assert_eq!(
        MooncakeClient::normalize_master_url("https://leader:50051").unwrap(),
        "https://leader:50051"
    );
}

#[test]
fn normalize_master_url_rejects_empty_values() {
    let err = MooncakeClient::normalize_master_url("  ").unwrap_err();
    assert!(matches!(err, StoreError::InvalidParams(_)));
}

#[test]
fn rpc_timeout_parsing_matches_cpp_env_rules() {
    assert_eq!(
        MooncakeClient::rpc_timeout_from_value(None, 30_000),
        Some(Duration::from_millis(30_000))
    );
    assert_eq!(
        MooncakeClient::rpc_timeout_from_value(Some("1000"), 30_000),
        Some(Duration::from_millis(1_000))
    );
    assert_eq!(
        MooncakeClient::rpc_timeout_from_value(Some("0"), 30_000),
        Some(Duration::from_millis(0))
    );
    assert_eq!(
        MooncakeClient::rpc_timeout_from_value(Some("-1"), 30_000),
        None
    );
    assert_eq!(
        MooncakeClient::rpc_timeout_from_value(Some("not-a-number"), 30_000),
        Some(Duration::from_millis(30_000))
    );
}

#[test]
fn deadline_exceeded_maps_to_rpc_timeout() {
    let err = MooncakeClient::rpc_status_to_error(tonic::Status::deadline_exceeded("expired"));
    assert!(matches!(err, StoreError::RpcTimeout(_)));
}

#[test]
fn validate_local_buffer_size_matches_cpp_config_rules() {
    assert!(MooncakeClient::validate_local_buffer_size(0).is_ok());
    assert!(MooncakeClient::validate_local_buffer_size(1024).is_ok());
    assert!(MooncakeClient::validate_local_buffer_size(1024 * 1024 * 1024 * 1024).is_ok());

    let too_small = MooncakeClient::validate_local_buffer_size(1023).unwrap_err();
    assert!(matches!(too_small, StoreError::InvalidParams(_)));

    let too_large =
        MooncakeClient::validate_local_buffer_size(1024 * 1024 * 1024 * 1024 + 1).unwrap_err();
    assert!(matches!(too_large, StoreError::InvalidParams(_)));
}

#[test]
fn validate_global_segment_size_matches_cpp_config_rules() {
    assert!(MooncakeClient::validate_global_segment_size(0).is_ok());
    assert!(MooncakeClient::validate_global_segment_size(1024).is_ok());
    assert!(MooncakeClient::validate_global_segment_size(1024 * 1024 * 1024 * 1024).is_ok());
    assert!(MooncakeClient::validate_global_segment_size(1024 * 1024 * 1024 * 1024 + 1).is_ok());

    let too_small = MooncakeClient::validate_global_segment_size(1023).unwrap_err();
    assert!(matches!(too_small, StoreError::InvalidParams(_)));
}

#[test]
fn resolve_auto_discover_matches_cpp_env_rules() {
    assert!(MooncakeClient::resolve_auto_discover("rdma", "", None));
    assert!(MooncakeClient::resolve_auto_discover("efa", "   ", None));
    assert!(!MooncakeClient::resolve_auto_discover(
        "rdma", "mlx5_0", None
    ));
    assert!(!MooncakeClient::resolve_auto_discover("tcp", "", None));

    assert!(MooncakeClient::resolve_auto_discover("tcp", "", Some("1")));
    assert!(MooncakeClient::resolve_auto_discover("tcp", "", Some(" 1")));
    assert!(MooncakeClient::resolve_auto_discover(
        "tcp",
        "",
        Some("1abc")
    ));
    assert!(!MooncakeClient::resolve_auto_discover(
        "rdma",
        "",
        Some("0")
    ));
    assert!(!MooncakeClient::resolve_auto_discover(
        "rdma",
        "mlx5_0",
        Some("2")
    ));
    assert!(MooncakeClient::resolve_auto_discover(
        "rdma",
        "",
        Some("not-a-number")
    ));
    assert!(!MooncakeClient::resolve_auto_discover(
        "rdma",
        "mlx5_0",
        Some("not-a-number")
    ));
    assert!(!MooncakeClient::resolve_auto_discover(
        "tcp",
        "",
        Some("not-a-number")
    ));
}

#[test]
fn effective_transport_protocol_honors_force_tcp_env() {
    assert_eq!(
        MooncakeClient::effective_transport_protocol("rdma", None),
        "rdma"
    );
    assert_eq!(
        MooncakeClient::effective_transport_protocol("rdma", Some("1".to_string())),
        "tcp"
    );
    assert_eq!(
        MooncakeClient::effective_transport_protocol("rdma", Some(String::new())),
        "tcp"
    );
}

#[test]
fn metadata_value_buffers_preserve_metadata_only_zero_data() {
    let data = 0x1000usize as *mut c_void;
    let metadata = 0x2000usize as *mut c_void;

    let (buffers, sizes) = super::write_batch::metadata_value_buffers(data, metadata, 8, 4)
        .expect("metadata and data buffers");
    assert_eq!(buffers, vec![metadata, data]);
    assert_eq!(sizes, vec![4, 8]);

    let (buffers, sizes) = super::write_batch::metadata_value_buffers(data, metadata, 0, 4)
        .expect("metadata-only buffer");
    assert_eq!(buffers, vec![metadata]);
    assert_eq!(sizes, vec![4]);

    let (buffers, sizes) =
        super::write_batch::metadata_value_buffers(data, metadata, 8, 0).expect("data buffer");
    assert_eq!(buffers, vec![data]);
    assert_eq!(sizes, vec![8]);

    assert!(super::write_batch::metadata_value_buffers(data, metadata, 0, 0).is_none());
}

#[test]
fn transport_topology_matrix_matches_cpp_protocol_rules() {
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("rdma", "mlx5_0", None),
        Some("mlx5_0".to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("efa", "efa0", None),
        Some("efa0".to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("ub", "bonding_dev_0", None),
        Some("bonding_dev_0".to_string())
    );

    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("cxi", "ignored", None),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("cxl", "ignored", None),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("ascend", "ignored", None),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("ubshmem", "ignored", None),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("tcp", "ignored", None),
        None
    );
}

#[test]
fn transport_topology_matrix_env_fallback_matches_cpp_filters() {
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("rdma", "mlx5_0", Some("mlx5_1")),
        Some("mlx5_0".to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("rdma", "", Some(" mlx5_0 , mlx5_1 ,")),
        Some("mlx5_0,mlx5_1".to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("efa", "  ", Some("efa0")),
        Some("efa0".to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("rdma", "", Some(" ,  ")),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("ub", "", None),
        Some("bonding_dev_0".to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("ub", "ub_dev_0", None),
        Some("ub_dev_0".to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("cxi", "ignored", Some("mlx5_0")),
        None
    );
}

#[test]
fn background_config_default_keeps_short_task_poll_interval() {
    let cfg = ClientBackgroundConfig::default();
    assert!(cfg.enable_task_poll);
    assert!(cfg.task_poll_interval <= Duration::from_millis(250));
}

#[test]
fn finalize_decision_reliable_memory_nof_requires_all_transfers() {
    let config = ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 2,
        ..Default::default()
    };
    let summary = ReplicaTransferSummary {
        allocated_memory_replicas: 1,
        allocated_nof_replicas: 2,
        successful_memory_transfers: 1,
        successful_nof_transfers: 1,
        failed_nof_transfers: 1,
        ..Default::default()
    };

    assert_eq!(
        determine_finalize_decision(&config, &summary),
        ReplicaFinalizeDecision {
            end_type: None,
            revoke_type: Some(ReplicaType::All),
            success: false,
        }
    );
}

#[test]
fn finalize_decision_flexible_dual_can_keep_one_successful_side() {
    let config = ReplicateConfig {
        replica_num: 1,
        nof_replica_num: 1,
        ..Default::default()
    };
    let memory_only = ReplicaTransferSummary {
        allocated_memory_replicas: 1,
        allocated_nof_replicas: 1,
        successful_memory_transfers: 1,
        failed_nof_transfers: 1,
        ..Default::default()
    };
    assert_eq!(
        determine_finalize_decision(&config, &memory_only),
        ReplicaFinalizeDecision {
            end_type: Some(ReplicaType::Memory),
            revoke_type: Some(ReplicaType::NoFSsd),
            success: true,
        }
    );

    let nof_only = ReplicaTransferSummary {
        allocated_memory_replicas: 1,
        allocated_nof_replicas: 1,
        failed_memory_transfers: 1,
        successful_nof_transfers: 1,
        ..Default::default()
    };
    assert_eq!(
        determine_finalize_decision(&config, &nof_only),
        ReplicaFinalizeDecision {
            end_type: Some(ReplicaType::NoFSsd),
            revoke_type: Some(ReplicaType::Memory),
            success: true,
        }
    );
}

#[test]
fn nof_endpoint_builder_defaults_invalid_transport_to_rdma() {
    let endpoint = MooncakeClient::build_nof_te_endpoint_with_trtype(
        "nqn.test",
        1,
        "10.0.0.1",
        4420,
        Some("bad"),
    );
    assert_eq!(
        endpoint,
        "traddr:10.0.0.1 trsvcid:4420 subnqn:nqn.test trtype:RDMA adrfam:IPv4 ns:1"
    );
}

#[test]
fn nof_endpoint_builder_honors_tcp_transport() {
    let endpoint = MooncakeClient::build_nof_te_endpoint_with_trtype(
        "nqn.test",
        7,
        "127.0.0.1",
        8009,
        Some("tcp"),
    );
    assert!(endpoint.contains("trtype:TCP"));
    assert!(endpoint.contains("ns:7"));
}

#[test]
fn cached_query_result_tracks_lease_expiry() {
    let fresh = CachedQueryResultResponse::success(Vec::<ReplicaDescriptor>::new(), 1000);
    assert!(!fresh.is_lease_expired());

    let expired = CachedQueryResultResponse::success(Vec::<ReplicaDescriptor>::new(), 0);
    assert!(expired.is_lease_expired());

    let failure = CachedQueryResultResponse::failure(-1, "missing");
    assert!(!failure.success);
    assert_eq!(failure.error_status, -1);
    assert_eq!(failure.error_message, "missing");
}

#[test]
fn tenant_scoped_cache_key_preserves_legacy_empty_tenant() {
    assert_eq!(scoped_cache_key("", "shared").as_ref(), "shared");
    assert_eq!(
        scoped_cache_key("tenant-a", "shared").as_ref(),
        "tenant-a\0shared"
    );
}

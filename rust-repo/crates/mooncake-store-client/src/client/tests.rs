use super::background::ClientBackgroundConfig;
use super::finalize::{
    determine_finalize_decision, ReplicaFinalizeDecision, ReplicaTransferSummary, REPLICA_TYPE_ALL,
    REPLICA_TYPE_MEMORY, REPLICA_TYPE_NOF_SSD,
};
use super::read::scoped_cache_key;
use super::{CachedQueryResultResponse, MooncakeClient};
use mooncake_store_core::{ReplicaDescriptor, ReplicateConfig, StoreError};
use std::time::Duration;

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
    assert!(!MooncakeClient::resolve_auto_discover(
        "rdma",
        "",
        Some("0")
    ));
    assert!(MooncakeClient::resolve_auto_discover(
        "rdma",
        "",
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
fn transport_topology_matrix_matches_cpp_protocol_rules() {
    assert_eq!(
        MooncakeClient::transport_topology_matrix("rdma", "mlx5_0"),
        Some("mlx5_0")
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix("efa", "efa0"),
        Some("efa0")
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix("ub", "bonding_dev_0"),
        Some("bonding_dev_0")
    );

    assert_eq!(
        MooncakeClient::transport_topology_matrix("cxi", "ignored"),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix("cxl", "ignored"),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix("ascend", "ignored"),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix("ubshmem", "ignored"),
        None
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix("tcp", "ignored"),
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
            revoke_type: Some(REPLICA_TYPE_ALL),
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
            end_type: Some(REPLICA_TYPE_MEMORY),
            revoke_type: Some(REPLICA_TYPE_NOF_SSD),
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
            end_type: Some(REPLICA_TYPE_NOF_SSD),
            revoke_type: Some(REPLICA_TYPE_MEMORY),
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

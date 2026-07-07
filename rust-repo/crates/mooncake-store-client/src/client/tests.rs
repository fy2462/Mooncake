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

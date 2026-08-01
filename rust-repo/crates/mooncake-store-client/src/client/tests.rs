use super::background::ClientBackgroundConfig;
use super::config::ClientConfig;
use super::finalize::{
    ReplicaFinalizeDecision, ReplicaTransferSummary, determine_finalize_decision,
};
use super::read::scoped_cache_key;
use super::{CachedQueryResultResponse, ClientHttpConfig, MooncakeClient, proto};
use mooncake_store_core::{ReplicaDescriptor, ReplicaType, ReplicateConfig, StoreError};
use std::ffi::c_void;
use std::process::Command;
use std::time::{Duration, Instant};

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
fn cxl_device_size_requires_a_positive_strict_integer() {
    assert_eq!(
        MooncakeClient::cxl_device_size_from_value(Some("8589934592")).unwrap(),
        8 * 1024 * 1024 * 1024
    );
    for value in [None, Some(""), Some("0"), Some("-1"), Some("12GiB")] {
        assert!(matches!(
            MooncakeClient::cxl_device_size_from_value(value),
            Err(StoreError::InvalidParams(_))
        ));
    }
}

#[test]
fn cxl_writes_force_the_local_segment_alias() {
    assert_eq!(
        MooncakeClient::placement_preferred_segment_for(
            "cxl",
            "writer-host:1234",
            "caller-choice:1234",
        ),
        "writer-host:1234"
    );
    assert_eq!(
        MooncakeClient::placement_preferred_segment_for(
            "rdma",
            "writer-host:1234",
            "caller-choice:1234",
        ),
        "caller-choice:1234"
    );
}

#[test]
fn deadline_exceeded_maps_to_rpc_timeout() {
    let err = MooncakeClient::rpc_status_to_error(tonic::Status::deadline_exceeded("expired"));
    assert!(matches!(err, StoreError::RpcTimeout(_)));
}

#[tokio::test]
async fn rpc_request_timeout_bounds_unresponsive_master() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let black_hole = tokio::spawn(async move {
        let (_connection, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });

    let mut master = MooncakeClient::connect_master_addr(&address.to_string(), None)
        .await
        .unwrap();
    let timeout = Duration::from_millis(50);
    let started = Instant::now();
    let status = master
        .service_ready(MooncakeClient::rpc_request_with_timeout(
            proto::ServiceReadyRequest {},
            Some(timeout),
        ))
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    black_hole.abort();
    assert_eq!(status.code(), tonic::Code::Cancelled);
    assert_eq!(status.message(), "Timeout expired");
    assert!(matches!(
        MooncakeClient::rpc_status_to_error(status),
        StoreError::RpcTimeout(_)
    ));
    assert!(
        elapsed >= timeout,
        "request returned before its deadline: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "request exceeded its bounded deadline: {elapsed:?}"
    );
}

#[test]
fn rpc_timeout_production_client_subprocess_helper() {
    let Ok(master_addr) = std::env::var("MOONCAKE_RPC_TIMEOUT_HELPER_MASTER") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let timeout = Duration::from_millis(200);
        let started = Instant::now();
        let result = MooncakeClient::create(
            &master_addr,
            "P2PHANDSHAKE",
            "127.0.0.1",
            "rpc_only",
            "",
            0,
            0,
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("black-hole master unexpectedly created a client"),
        };
        let elapsed = started.elapsed();

        assert!(matches!(error, StoreError::RpcTimeout(_)), "{error:?}");
        assert!(
            elapsed >= timeout - Duration::from_millis(50),
            "production client returned before configured timeout: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "production client exceeded bounded timeout: {elapsed:?}"
        );
    });
}

#[test]
fn cpp_parity_rpc_timeout_test_cpp_rpctimeouttest_rpctimesoutagainstunresponsivemaster_4ef20575() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let master_addr = listener.local_addr().unwrap().to_string();
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("client::tests::rpc_timeout_production_client_subprocess_helper")
        .arg("--exact")
        .arg("--nocapture")
        .env("MOONCAKE_RPC_TIMEOUT_HELPER_MASTER", master_addr)
        .env("MC_RPC_TIMEOUT_MS", "200")
        .env("MC_RPC_CONNECT_TIMEOUT_MS", "1000")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "timeout helper failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn rpc_status_mapping_preserves_remote_fallback_signal() {
    let missing = MooncakeClient::rpc_status_to_error(tonic::Status::not_found("missing-key"));
    assert!(matches!(missing, StoreError::KeyNotFound(key) if key == "missing-key"));

    let unavailable =
        MooncakeClient::rpc_status_to_error(tonic::Status::unavailable("master down"));
    assert!(matches!(unavailable, StoreError::ServiceUnavailable));

    let exhausted =
        MooncakeClient::rpc_status_to_error(tonic::Status::resource_exhausted("segment full"));
    assert!(matches!(exhausted, StoreError::NoAvailableHandle));

    let cancelled = MooncakeClient::rpc_status_to_error(tonic::Status::cancelled("caller left"));
    assert!(matches!(cancelled, StoreError::Internal(_)));
}

#[test]
fn mount_rollback_treats_not_found_as_confirmed_absence() {
    assert!(super::lifecycle::unmount_confirms_segment_absent(Ok::<
        (),
        tonic::Status,
    >(())));
    assert!(super::lifecycle::unmount_confirms_segment_absent::<()>(
        Err(tonic::Status::not_found("already absent"))
    ));
    assert!(!super::lifecycle::unmount_confirms_segment_absent::<()>(
        Err(tonic::Status::unavailable("outcome unknown"))
    ));
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
fn max_mr_size_splits_total_capacity_without_losing_tail() {
    assert_eq!(
        MooncakeClient::split_segment_capacity(10, 4).unwrap(),
        vec![4, 4, 2]
    );
    assert_eq!(
        MooncakeClient::split_segment_capacity(8, 4).unwrap(),
        vec![4, 4]
    );
    assert!(
        MooncakeClient::split_segment_capacity(1, 0).is_err(),
        "a zero MR cap would make the split loop non-progressing"
    );
    assert_eq!(
        MooncakeClient::split_segment_capacity_aligned(64, 40, 16).unwrap(),
        vec![32, 32]
    );
    assert!(MooncakeClient::split_segment_capacity_aligned(63, 40, 16).is_err());
    assert!(MooncakeClient::split_segment_capacity_aligned(64, 8, 16).is_err());
}

#[test]
fn rdma_requires_explicit_max_mr_size_without_device_clamp_ffi() {
    assert!(MooncakeClient::resolve_max_mr_size("rdma", 4096, None).is_err());
    assert_eq!(
        MooncakeClient::resolve_max_mr_size("rdma", 4096, Some("2048")).unwrap(),
        2048
    );
    assert_eq!(
        MooncakeClient::resolve_max_mr_size("tcp", 4096, None).unwrap(),
        1024 * 1024 * 1024 * 1024
    );
    assert!(MooncakeClient::resolve_max_mr_size("tcp", 4096, Some("0")).is_err());
    assert_eq!(
        MooncakeClient::validate_memory_segment_alignment("offset", 1).unwrap(),
        1
    );
    assert_eq!(
        MooncakeClient::validate_memory_segment_alignment("cachelib", 1 << 24).unwrap(),
        1 << 24
    );
    assert!(MooncakeClient::validate_memory_segment_alignment("cachelib", 3).is_err());
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
fn rpc_only_skips_transfer_engine_initialization() {
    assert!(!MooncakeClient::uses_transfer_engine("rpc_only"));
    for protocol in ["", "tcp", "rdma", "efa", "cxi"] {
        assert!(MooncakeClient::uses_transfer_engine(protocol));
    }
}

#[test]
fn tent_environment_presence_is_rejected_before_classic_setup() {
    assert!(!MooncakeClient::tent_mode_requested(false, false));
    assert!(MooncakeClient::tent_mode_requested(true, false));
    assert!(MooncakeClient::tent_mode_requested(false, true));
    assert!(MooncakeClient::tent_mode_requested(true, true));
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
        Some(r#"{"cpu:0":[["mlx5_0"],[]]}"#.to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("efa", "efa0", None),
        Some(r#"{"cpu:0":[["efa0"],[]]}"#.to_string())
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
        Some(r#"{"cpu:0":[["mlx5_0"],[]]}"#.to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("rdma", "", Some(" mlx5_0 , mlx5_1 ,")),
        Some(r#"{"cpu:0":[["mlx5_0","mlx5_1"],[]]}"#.to_string())
    );
    assert_eq!(
        MooncakeClient::transport_topology_matrix_from_env("efa", "  ", Some("efa0")),
        Some(r#"{"cpu:0":[["efa0"],[]]}"#.to_string())
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
fn finalize_decision_supports_global_disk_only_objects() {
    let config = ReplicateConfig {
        replica_num: 0,
        nof_replica_num: 0,
        ..Default::default()
    };
    let success = ReplicaTransferSummary {
        allocated_disk_replicas: 1,
        successful_disk_writes: 1,
        ..Default::default()
    };
    assert_eq!(
        determine_finalize_decision(&config, &success),
        ReplicaFinalizeDecision {
            end_type: None,
            revoke_type: None,
            success: true,
        }
    );

    let failure = ReplicaTransferSummary {
        allocated_disk_replicas: 1,
        failed_disk_writes: 1,
        ..Default::default()
    };
    assert!(!determine_finalize_decision(&config, &failure).success);
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

    let issued_before_rpc = std::time::Instant::now() - std::time::Duration::from_millis(2);
    let expired_during_rpc = CachedQueryResultResponse::success_from(
        issued_before_rpc,
        Vec::<ReplicaDescriptor>::new(),
        1,
    );
    assert!(expired_during_rpc.is_lease_expired());
}

#[test]
fn tenant_scoped_cache_key_preserves_legacy_empty_tenant() {
    assert_eq!(scoped_cache_key("", "shared").as_ref(), "shared");
    assert_eq!(
        scoped_cache_key("tenant-a", "shared").as_ref(),
        "tenant-a\0shared"
    );
}

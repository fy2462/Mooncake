use clap::Parser;
use mooncake_store_master::ha::{HABackendSpec, HABackendType, HaError};
use mooncake_store_master::main_args::Args;
use mooncake_store_master::main_config::{
    build_ha_spec, build_master_service, build_runtime_config, create_coordinator,
    snapshot_dir_for_cluster, validate_rpc_protocol,
};
use std::sync::Arc;
use std::time::Duration;

fn base_args() -> Args {
    Args {
        enable_offload: true,
        rpc_address: "127.0.0.1".to_string(),
        rpc_port: 50051,
        http_metadata_server_host: "127.0.0.1".to_string(),
        http_metadata_server_port: 8080,
        metrics_port: 9003,
        rpc_thread_num: 4,
        enable_nof: true,
        allocation_strategy: "random".to_string(),
        memory_allocator: "offset".to_string(),
        default_kv_lease_ttl_ms: 5000,
        client_ttl_secs: 10,
        root_fs_dir: String::new(),
        eviction_high_watermark_ratio: 0.95,
        eviction_ratio: 0.05,
        offload_on_evict: false,
        offload_force_evict: false,
        offloading_queue_limit: 50_000,
        offload_cap_ratio: 0.5,
        enable_disk_eviction: true,
        quota_bytes: 0,
        enable_multi_tenants: false,
        enable_tenant_quota: false,
        default_tenant_quota_bytes: 0,
        tenant_quota_connector_type: "file".to_string(),
        tenant_quota_connector_uri: String::new(),
        tenant_quota_pool_capacity_bytes: 0,
        nof_heartbeat_interval_sec: 10,
        nof_heartbeat_probe_timeout_ms: 1000,
        nof_heartbeat_failures_threshold: 3,
        put_start_discard_timeout_sec: 30,
        put_start_release_timeout_sec: 600,
        promotion_max_per_heartbeat: 1,
        max_total_finished_tasks: 10_000,
        max_total_pending_tasks: 10_000,
        max_total_processing_tasks: 10_000,
        pending_task_timeout_secs: 300,
        processing_task_timeout_secs: 300,
        max_task_retry_attempts: 10,
        enable_ha: true,
        etcd_endpoints: None,
        ha_backend_type: "etcd".to_string(),
        ha_backend_connstring: None,
        cluster_id: Some("cluster-a".to_string()),
        snapshot_backend_type: None,
        snapshot_backup_dir: None,
        snapshot_object_store_type: None,
        snapshot_catalog_store_type: "embedded".to_string(),
        snapshot_catalog_store_connstring: None,
        enable_snapshot: false,
        snapshot_interval_seconds: 600,
        snapshot_child_timeout_seconds: 300,
        snapshot_retention_count: 2,
        ha_lease_ttl_secs: 30,
        pod_name: None,
        pod_namespace: None,
    }
}

#[test]
fn test_build_ha_spec_etcd_falls_back_to_etcd_endpoints() {
    let mut args = base_args();
    args.etcd_endpoints = Some("http://127.0.0.1:2379".to_string());

    let spec = build_ha_spec(&args).unwrap();

    assert_eq!(spec.backend_type, HABackendType::Etcd);
    assert_eq!(spec.connstring, "http://127.0.0.1:2379");
    assert_eq!(spec.cluster_namespace, "cluster-a");
}

#[test]
fn test_build_ha_spec_redis_uses_explicit_connstring() {
    let mut args = base_args();
    args.ha_backend_type = "redis".to_string();
    args.ha_backend_connstring = Some("redis://127.0.0.1:6379".to_string());

    let spec = build_ha_spec(&args).unwrap();

    assert_eq!(spec.backend_type, HABackendType::Redis);
    assert_eq!(spec.connstring, "redis://127.0.0.1:6379");
}

#[test]
fn test_build_ha_spec_k8s_uses_explicit_connstring() {
    let mut args = base_args();
    args.ha_backend_type = "k8s".to_string();
    args.ha_backend_connstring = Some("ns-a/lease-a".to_string());

    let spec = build_ha_spec(&args).unwrap();

    assert_eq!(spec.backend_type, HABackendType::K8s);
    assert_eq!(spec.connstring, "ns-a/lease-a");
}

#[test]
fn test_build_ha_spec_k8s_uses_pod_identity() {
    let mut args = base_args();
    args.ha_backend_type = "k8s".to_string();
    args.ha_backend_connstring = Some("ns-a/lease-a".to_string());
    args.pod_name = Some("pod-a".to_string());
    args.pod_namespace = Some("pod-ns".to_string());

    let spec = build_ha_spec(&args).unwrap();
    let identity = spec.pod_identity.expect("k8s pod identity");

    assert_eq!(identity.pod_name, "pod-a");
    assert_eq!(identity.namespace, "pod-ns");
}

#[test]
fn test_build_ha_spec_k8s_requires_connstring() {
    let mut args = base_args();
    args.ha_backend_type = "k8s".to_string();

    let err = build_ha_spec(&args).unwrap_err();

    assert!(matches!(err, HaError::InvalidParams(_)));
}

#[tokio::test]
async fn test_create_coordinator_k8s_builds_lazy_coordinator() {
    let spec = HABackendSpec {
        backend_type: HABackendType::K8s,
        connstring: "ns-a/lease-a".to_string(),
        cluster_namespace: "cluster-a".to_string(),
        pod_identity: None,
    };

    let coordinator = create_coordinator(&spec).await.unwrap();

    assert_eq!(
        coordinator.wait_for_role().await.unwrap(),
        mooncake_store_master::ha::LeaderRole::Standby
    );
}

#[test]
fn test_build_master_service_returns_fresh_instance_per_call() {
    let runtime_config = build_runtime_config(&base_args()).unwrap();

    let first = build_master_service(None, None, runtime_config.clone());
    let second = build_master_service(None, None, runtime_config);

    assert!(!Arc::ptr_eq(&first, &second));
}

#[test]
fn test_build_runtime_config_rejects_cxl_strategy_explicitly() {
    let mut args = base_args();
    args.allocation_strategy = "cxl".to_string();

    let err = build_runtime_config(&args).unwrap_err();

    assert!(err.to_string().contains("not supported"));
}

#[test]
fn test_build_runtime_config_accepts_local_first_strategy() {
    let mut args = base_args();
    args.allocation_strategy = "local_first".to_string();

    let config = build_runtime_config(&args).unwrap();

    assert_eq!(
        config.allocation_strategy,
        mooncake_store_master::allocator::AllocationStrategy::LocalFirst
    );
}

#[test]
fn test_build_runtime_config_accepts_ssd_free_ratio_first_strategy() {
    let mut args = base_args();
    args.allocation_strategy = "ssd_free_ratio_first".to_string();

    let config = build_runtime_config(&args).unwrap();

    assert_eq!(
        config.allocation_strategy,
        mooncake_store_master::allocator::AllocationStrategy::SsdFreeRatioFirst
    );
}

#[test]
fn test_build_runtime_config_applies_task_manager_limits() {
    let mut args = base_args();
    args.client_ttl_secs = 9;
    args.root_fs_dir = "/storage/root".to_string();
    args.enable_disk_eviction = false;
    args.quota_bytes = 4096;
    args.enable_multi_tenants = true;
    args.default_tenant_quota_bytes = 2048;
    args.tenant_quota_connector_type = "file".to_string();
    args.tenant_quota_connector_uri = "/tmp/tenant-quota.yaml".to_string();
    args.tenant_quota_pool_capacity_bytes = 8192;
    args.nof_heartbeat_interval_sec = 17;
    args.nof_heartbeat_probe_timeout_ms = 250;
    args.nof_heartbeat_failures_threshold = 4;
    args.put_start_discard_timeout_sec = 19;
    args.put_start_release_timeout_sec = 23;
    args.snapshot_child_timeout_seconds = 29;
    args.snapshot_retention_count = 5;
    args.max_total_finished_tasks = 11;
    args.max_total_pending_tasks = 12;
    args.max_total_processing_tasks = 13;
    args.pending_task_timeout_secs = 14;
    args.processing_task_timeout_secs = 15;
    args.max_task_retry_attempts = 16;

    let config = build_runtime_config(&args).unwrap();

    assert_eq!(config.client_live_ttl, Duration::from_secs(9));
    assert_eq!(config.storage_fs_dir, "/storage/root");
    assert!(!config.enable_disk_eviction);
    assert_eq!(config.quota_bytes, 4096);
    assert!(config.enable_tenant_quota);
    assert_eq!(config.default_tenant_quota_bytes, 2048);
    assert_eq!(config.tenant_quota_connector_type, "file");
    assert_eq!(config.tenant_quota_connector_uri, "/tmp/tenant-quota.yaml");
    assert_eq!(config.tenant_quota_pool_capacity_bytes, 8192);
    assert_eq!(config.nof_heartbeat_interval, Duration::from_secs(17));
    assert_eq!(
        config.nof_heartbeat_probe_timeout,
        Duration::from_millis(250)
    );
    assert_eq!(config.nof_heartbeat_failures_threshold, 4);
    assert_eq!(config.put_start_discard_timeout, Duration::from_secs(19));
    assert_eq!(config.put_start_release_timeout, Duration::from_secs(23));
    assert_eq!(config.snapshot_child_timeout, Duration::from_secs(29));
    assert_eq!(config.snapshot_retention_count, 5);
    assert_eq!(config.max_total_finished_tasks, 11);
    assert_eq!(config.max_total_pending_tasks, 12);
    assert_eq!(config.max_total_processing_tasks, 13);
    assert_eq!(config.pending_task_timeout, Duration::from_secs(14));
    assert_eq!(config.processing_task_timeout, Duration::from_secs(15));
    assert_eq!(config.max_task_retry_attempts, 16);
}

#[test]
fn test_build_runtime_config_applies_offload_tuning() {
    let mut args = base_args();
    args.offloading_queue_limit = 123;
    args.offload_cap_ratio = 0.75;

    let config = build_runtime_config(&args).unwrap();

    assert_eq!(config.offloading_queue_limit, 123);
    assert_eq!(config.offload_cap_ratio, 0.75);
}

#[test]
fn test_build_runtime_config_rejects_invalid_offload_tuning() {
    let mut args = base_args();
    args.offloading_queue_limit = 0;
    assert!(build_runtime_config(&args)
        .unwrap_err()
        .to_string()
        .contains("offloading_queue_limit"));

    let mut args = base_args();
    args.offload_cap_ratio = 1.5;
    assert!(build_runtime_config(&args)
        .unwrap_err()
        .to_string()
        .contains("offload_cap_ratio"));
}

#[test]
fn test_cli_disk_eviction_defaults_true_and_accepts_false() {
    let default_args = Args::try_parse_from(["mooncake-master"]).unwrap();
    assert!(default_args.enable_disk_eviction);

    let disabled =
        Args::try_parse_from(["mooncake-master", "--enable-disk-eviction", "false"]).unwrap();
    assert!(!disabled.enable_disk_eviction);
}

#[test]
fn test_snapshot_dir_for_cluster_scopes_ha_snapshots() {
    let root = std::path::PathBuf::from("/tmp/mooncake-snapshots");

    assert_eq!(
        snapshot_dir_for_cluster(Some(root.clone()), "cluster-a").unwrap(),
        root.join("cluster-a")
    );
    assert_eq!(
        snapshot_dir_for_cluster(Some(root.clone()), "").unwrap(),
        root
    );
    assert!(snapshot_dir_for_cluster(None, "cluster-a").is_none());
}

#[test]
fn test_validate_rpc_protocol_rejects_unsupported_rdma() {
    assert!(validate_rpc_protocol(None).is_ok());
    assert!(validate_rpc_protocol(Some("tcp")).is_ok());

    let err = validate_rpc_protocol(Some("rdma")).unwrap_err();
    let ha_error = err.downcast_ref::<HaError>().unwrap();
    assert!(matches!(ha_error, HaError::UnavailableInCurrentMode(_)));
}

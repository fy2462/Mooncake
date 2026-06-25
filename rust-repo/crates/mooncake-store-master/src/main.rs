//! # Mooncake Master — 入口点 / Entry Point
//!
//! 主函数负责解析 CLI 参数、初始化 tracing、并根据是否启用 HA 模式
//! 选择不同的启动路径：`run_standalone`（单节点）或 `run_ha_loop`（高可用主备循环）。
//!
//! The main function parses CLI arguments, initializes tracing, and chooses
//! between `run_standalone` (single-node) or `run_ha_loop` (HA leader/standby loop).

use clap::Parser;
use mooncake_store_master::admin_http::AdminRuntimeState;
use mooncake_store_master::ha::{
    parse_snapshot_catalog_store_type, parse_snapshot_object_store_type, HABackendSpec,
    HABackendType, HaError, LeaderCoordinator, LeadershipMonitorHandle, LeadershipSession,
    MasterServiceSupervisor, MasterServiceSupervisorConfig, MasterView,
};
use mooncake_store_master::http_metadata::serve_metadata_http;
use mooncake_store_master::metrics;
use mooncake_store_master::MasterServiceImpl;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

use mooncake_store_master::main_args::Args;
use mooncake_store_master::main_config::{
    build_catalog_snapshot_publisher, build_ha_spec, build_master_service, build_runtime_config,
    create_coordinator, ensure_supported_rpc_protocol, new_supervisor, parse_snapshot_config,
    publish_catalog_snapshot, resolve_cluster_id, snapshot_dir_for_cluster,
};

mod main_ha;
mod main_server;

/// 主入口：初始化日志系统，解析参数，按模式分派。
/// Main entry point: init logging, parse args, dispatch by mode.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    if args.enable_ha {
        main_ha::run_ha_loop(args, shutdown_signal()).await
    } else {
        main_server::run_standalone(args).await
    }
}

fn shutdown_signal() -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_process_shutdown().await;
        info!("Process shutdown signal received");
        let _ = shutdown_tx.send(true);
    });
    shutdown_rx
}

// One writer broadcasts process shutdown to many async wait points. Each
// consumer clones the receiver and waits inside its own tokio::select!.
#[cfg(unix)]
async fn wait_for_process_shutdown() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_process_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn wait_and_continue(
    coordinator: &LeaderCoordinator,
    current_view: &Option<MasterView>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> bool {
    let version = current_view.as_ref().map(|v| v.view_version).unwrap_or(0);
    tokio::select! {
        result = coordinator.wait_for_view_change(version, Duration::from_secs(1)) => {
            match result {
                Ok(Some(v)) => info!(
                    "View changed: leader={}, version={}",
                    v.leader_address, v.view_version
                ),
                Ok(None) => {}
                Err(e) => warn!("View wait error: {}", e),
            }
            false
        }
        changed = shutdown_rx.changed() => {
            changed.is_err() || *shutdown_rx.borrow()
        }
    }
}

async fn warmup_with_renewal(
    coordinator: &LeaderCoordinator,
    session: &LeadershipSession,
    ttl_secs: i64,
    mut shutdown_rx: watch::Receiver<bool>,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(ttl_secs as u64);
    info!("Warmup {}s, renewing each second", ttl_secs);
    while tokio::time::Instant::now() < deadline {
        if coordinator.try_renew_leadership(session).await.is_err() {
            warn!("Warmup renewal failed");
            return false;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    info!("Warmup interrupted by shutdown");
                    return false;
                }
            }
        }
    }
    info!("Warmup complete");
    true
}

async fn start_leadership_monitor(
    coordinator: &LeaderCoordinator,
    session: &LeadershipSession,
    tx: tokio::sync::watch::Sender<bool>,
) -> Result<LeadershipMonitorHandle, HaError> {
    let mut role_rx = coordinator.subscribe_role_for_session(session)?;
    let h = tokio::spawn(async move {
        loop {
            if role_rx.changed().await.is_err() {
                warn!("LeadershipMonitor: role channel closed, shutting down");
                let _ = tx.send(true);
                break;
            }
            if *role_rx.borrow() == mooncake_store_master::ha::LeaderRole::Standby {
                warn!("LeadershipMonitor: leadership lost, shutting down");
                let _ = tx.send(true);
                break;
            }
        }
    });
    Ok(LeadershipMonitorHandle::new(h))
}

// ---------------------------------------------------------------------------
// Helpers — 工具函数
// ---------------------------------------------------------------------------

async fn build_leader_oplog_manager(
    spec: &HABackendSpec,
    view_version: u64,
) -> Option<mooncake_store_master::oplog::OpLogManager> {
    if spec.backend_type != HABackendType::Etcd || spec.connstring.trim().is_empty() {
        return None;
    }
    let endpoints: Vec<String> = spec
        .connstring
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
        .collect();

    let client = match etcd_client::Client::connect(endpoints, None).await {
        Ok(client) => client,
        Err(e) => {
            warn!("Failed to connect etcd for leader oplog: {}", e);
            return None;
        }
    };
    let oplog_prefix = format!("/oplog/{}", spec.cluster_namespace);
    let store = match mooncake_store_master::oplog::EtcdOpLogStore::new(client, &oplog_prefix).await
    {
        Ok(store) => store,
        Err(e) => {
            warn!("Failed to initialize leader oplog store: {}", e);
            return None;
        }
    };
    Some(mooncake_store_master::oplog::OpLogManager::new(
        Some(Box::new(store)),
        view_version,
    ))
}

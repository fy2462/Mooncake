use super::*;

// ---------------------------------------------------------------------------
// HA perpetual retry loop
// HA 永久重试循环
//
// 流程 / Flow:
//   1. 读取当前 view → 进入 standby 模式
//   2. 若不存在 leader → 尝试获取 leadership → warmup → 启动 gRPC 服务
//   3. 若已存在 leader → 等待 view 变更后重新检查
//
//   1. Read current view → enter standby mode
//   2. If no leader → try acquire leadership → warmup → start gRPC server
//   3. If leader exists → wait for view change and re-check
// ---------------------------------------------------------------------------

// Helper: enter standby and sleep before continuing the outer loop.
async fn back_to_standby(supervisor: &mut MasterServiceSupervisor, sleep_secs: u64) {
    supervisor.enable_standby_updates();
    let _ = supervisor.enter_standby_mode(None);
    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
}

// Helper: release leadership, enter standby, and sleep.
async fn release_and_retry(
    coordinator: &LeaderCoordinator,
    supervisor: &mut MasterServiceSupervisor,
    session: &LeadershipSession,
) {
    let _ = coordinator.release_leadership(session).await;
    back_to_standby(supervisor, 1).await;
}

pub(super) async fn run_ha_loop(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let (snapshot_backend_type, snapshot_dir) = parse_snapshot_config(&args);
    let ha_spec = build_ha_spec(&args)?;
    let snapshot_object_store_type = args
        .snapshot_object_store_type
        .as_deref()
        .map(parse_snapshot_object_store_type)
        .transpose()?;
    let snapshot_catalog_store_type =
        parse_snapshot_catalog_store_type(&args.snapshot_catalog_store_type)?;
    let cluster_id = ha_spec.cluster_namespace.clone();
    let runtime_config = build_runtime_config(&args)?;
    let supervisor_config = MasterServiceSupervisorConfig {
        local_hostname: format!("{}:{}", args.rpc_address, args.rpc_port),
        cluster_id,
        enable_snapshot_restore: snapshot_dir.is_some() || snapshot_object_store_type.is_some(),
        snapshot_backup_dir: snapshot_dir.clone(),
        snapshot_backend_type,
        snapshot_object_store_type,
        snapshot_catalog_store_type,
        snapshot_catalog_store_connstring: args.snapshot_catalog_store_connstring.clone(),
    };
    let leader_addr = format!("{}:{}", args.rpc_address, args.rpc_port);
    info!("HA loop started");

    // --- Outer loop: re-create coordinator each iteration (matches C++) ---
    loop {
        let coordinator = match create_coordinator(&ha_spec).await {
            Ok(c) => c,
            Err(e) => {
                if e.downcast_ref::<HaError>().map_or(false, |h| h.is_fatal()) {
                    return Err(e);
                }
                warn!("Coordinator creation failed: {}, retrying in 1s", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let service_arc = build_master_service(
            snapshot_backend_type,
            snapshot_dir_for_cluster(snapshot_dir.clone(), &ha_spec.cluster_namespace),
            runtime_config.clone(),
        );
        service_arc.set_service_available(false);
        let mut supervisor = new_supervisor(&ha_spec, &supervisor_config, service_arc.clone());
        if let Err(e) = supervisor.enter_standby_mode(None) {
            warn!("enter_standby_mode failed: {}, retrying in 1s", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        // --- Inner loop: read view, try to acquire leadership ---
        loop {
            supervisor.begin_candidacy();

            let current_view = match coordinator.read_current_view().await {
                Ok(v) => v,
                Err(e) => {
                    warn!("read_current_view failed: {}", e);
                    if e.is_fatal() {
                        return Err(Box::new(e));
                    }
                    back_to_standby(&mut supervisor, 1).await;
                    break; // re-create coordinator
                }
            };

            info!(
                "Current view: {}",
                current_view
                    .as_ref()
                    .map(|v| v.leader_address.as_str())
                    .unwrap_or("none")
            );
            supervisor.update_observed_leader(current_view.clone());

            // --- Path A: no leader → bid for it ---
            if current_view.is_some() {
                wait_and_continue(&coordinator, &current_view).await;
                continue; // back to top of inner loop
            }

            info!("No leader — acquiring leadership");
            let acquire = match coordinator
                .try_acquire_leadership(&leader_addr, args.ha_lease_ttl_secs)
                .await
            {
                Ok(r) if r.acquired => r,
                Ok(r) => {
                    let observed_view = r.view.clone();
                    info!(
                        "Not acquired, waiting for view change from version {:?}",
                        observed_view.as_ref().map(|view| view.view_version)
                    );
                    supervisor.update_observed_leader(observed_view.clone());
                    if let Err(e) = supervisor.enter_standby_mode(observed_view.clone()) {
                        warn!("enter_standby_mode after acquire contention failed: {}", e);
                    }
                    wait_and_continue(&coordinator, &observed_view).await;
                    continue;
                }
                Err(e) => {
                    warn!("Acquire error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break; // re-create coordinator
                }
            };

            let session = match acquire.session {
                Some(session) => session,
                None => {
                    warn!("Leadership acquired without a session, retrying");
                    back_to_standby(&mut supervisor, 1).await;
                    break;
                }
            };
            if service_arc.oplog_manager().lock().store().is_none() {
                match build_leader_oplog_manager(&ha_spec, 0).await {
                    Some(manager) => {
                        *service_arc.oplog_manager().lock() = manager;
                    }
                    None => {
                        warn!("Leader oplog store is unavailable, releasing leadership");
                        release_and_retry(&coordinator, &mut supervisor, &session).await;
                        break;
                    }
                }
            }
            if let Some(view) = &acquire.view {
                service_arc.set_view_version(view.view_version as i64);
                service_arc
                    .oplog_manager()
                    .lock()
                    .set_view_version(view.view_version);
            }
            info!("Leadership acquired, lease TTL={}s", args.ha_lease_ttl_secs);

            // Promote standby. Match C++: stop accepting standby runtime callbacks
            // before promotion, so LeaderWarmup cannot be overwritten by standby.
            supervisor.disable_standby_updates();
            if let Err(e) = supervisor.promote_to_leader_warmup() {
                error!("Promotion failed: {}", e);
                supervisor.enable_standby_updates();
                release_and_retry(&coordinator, &mut supervisor, &session).await;
                break;
            }

            // Start keepalive (background tokio task renews lease every 3s).
            let keepalive_handle = match coordinator.start_leadership_keepalive(&session).await {
                Ok(handle) => handle,
                Err(e) => {
                    error!("Keepalive start failed: {}", e);
                    supervisor.enable_standby_updates();
                    release_and_retry(&coordinator, &mut supervisor, &session).await;
                    break;
                }
            };

            // Active warmup: renew lease every second.
            if !warmup_with_renewal(&coordinator, &session, args.ha_lease_ttl_secs).await {
                supervisor.enable_standby_updates();
                release_and_retry(&coordinator, &mut supervisor, &session).await;
                break;
            }

            // Preflight: final renewal before serving.
            if let Err(e) = coordinator.try_renew_leadership(&session).await {
                warn!("Preflight renewal failed: {}", e);
                supervisor.enable_standby_updates();
                release_and_retry(&coordinator, &mut supervisor, &session).await;
                break;
            }

            // LeadershipMonitor + server. Monitor MUST exist (fallback: dummy tx).
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let monitor = match start_leadership_monitor(&coordinator, &session, shutdown_tx).await
            {
                Ok(handle) => handle,
                Err(e) => {
                    warn!("Leadership monitor start failed: {}", e);
                    supervisor.enable_standby_updates();
                    release_and_retry(&coordinator, &mut supervisor, &session).await;
                    break;
                }
            };

            supervisor.activate_serving_state();
            service_arc.set_service_available(true);

            let server_result = super::main_server::run_leader_server(
                service_arc.clone(),
                &args,
                shutdown_rx,
                Some(session.view.clone()),
            )
            .await;

            match &server_result {
                Ok(()) => info!("gRPC server exited cleanly"),
                Err(e) => error!("gRPC server error: {}", e),
            }

            // Cleanup.
            service_arc.set_service_available(false);
            drop(monitor);
            drop(keepalive_handle);
            supervisor.deactivate_serving_state();
            supervisor.enable_standby_updates();
            let _ = coordinator.release_leadership(&session).await;

            // Re-read view before re-entering standby.
            if let Ok(post_view) = coordinator.read_current_view().await {
                supervisor.update_observed_leader(post_view.clone());
                let _ = supervisor.enter_standby_mode(post_view);
            } else {
                let _ = supervisor.enter_standby_mode(None);
            }

            info!("Returning to standby loop");
            break; // re-create coordinator
        }
    }
}

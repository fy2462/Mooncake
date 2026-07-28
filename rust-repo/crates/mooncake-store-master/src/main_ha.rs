use super::*;

type HaLoopResult<T> = Result<T, Box<dyn std::error::Error>>;
type ShutdownReceiver = tokio::sync::watch::Receiver<bool>;

const RETRY_DELAY: Duration = Duration::from_secs(1);

struct HaLoopContext {
    args: Args,
    ha_spec: HABackendSpec,
    supervisor_config: MasterServiceSupervisorConfig,
    snapshot_backend_type: Option<mooncake_store_master::storage_backend::StorageBackendType>,
    snapshot_dir: Option<std::path::PathBuf>,
    runtime_config: mooncake_store_master::MasterRuntimeConfig,
    leader_addr: String,
}

enum CandidacyOutcome {
    KeepWatching,
    RestartCoordinator,
    Shutdown,
}

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
async fn back_to_standby(
    supervisor: &mut MasterServiceSupervisor,
    sleep_secs: u64,
    shutdown_rx: ShutdownReceiver,
) {
    supervisor.enable_standby_updates();
    if let Err(e) = supervisor.enter_standby_mode(None) {
        warn!("enter_standby_mode during retry failed: {}", e);
    }
    let _ = sleep_or_shutdown(Duration::from_secs(sleep_secs), shutdown_rx).await;
}

// Helper: release leadership, enter standby, and sleep.
async fn release_and_retry(
    coordinator: &LeaderCoordinator,
    supervisor: &mut MasterServiceSupervisor,
    session: &LeadershipSession,
    shutdown_rx: ShutdownReceiver,
) {
    if let Err(error) = coordinator.release_leadership(session).await {
        warn!(
            "leadership release failed; local role is fenced and remote lease will expire: {error}"
        );
    }
    back_to_standby(supervisor, 1, shutdown_rx).await;
}

pub(super) async fn run_ha_loop(args: Args, shutdown_rx: ShutdownReceiver) -> HaLoopResult<()> {
    let context = build_ha_loop_context(args)?;
    info!("HA loop started");

    run_until_shutdown(&context, shutdown_rx).await
}

async fn run_until_shutdown(
    context: &HaLoopContext,
    shutdown_rx: ShutdownReceiver,
) -> HaLoopResult<()> {
    loop {
        tokio::select! {
            result = run_coordinator_lifecycle(context, shutdown_rx.clone()) => result?,
            _ = wait_for_shutdown(shutdown_rx.clone()) => {
                info!("HA loop stopped by shutdown");
                return Ok(());
            }
        }
    }
}

fn build_ha_loop_context(args: Args) -> HaLoopResult<HaLoopContext> {
    let (snapshot_backend_type, snapshot_dir) = parse_snapshot_config(&args)?;
    let ha_spec = build_ha_spec(&args)?;
    validate_ha_backend_for_serving(&ha_spec)?;
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
    Ok(HaLoopContext {
        leader_addr: format!("{}:{}", args.rpc_address, args.rpc_port),
        args,
        ha_spec,
        supervisor_config,
        snapshot_backend_type,
        snapshot_dir,
        runtime_config,
    })
}

async fn run_coordinator_lifecycle(
    context: &HaLoopContext,
    shutdown_rx: ShutdownReceiver,
) -> HaLoopResult<()> {
    let Some(coordinator_result) =
        create_coordinator_until_shutdown(&context.ha_spec, shutdown_rx.clone()).await
    else {
        return Ok(());
    };
    let coordinator = match coordinator_result {
        Ok(coordinator) => coordinator,
        Err(e) => {
            if e.downcast_ref::<HaError>().map_or(false, |h| h.is_fatal()) {
                return Err(e);
            }
            warn!("Coordinator creation failed: {}, retrying in 1s", e);
            let _ = sleep_or_shutdown(RETRY_DELAY, shutdown_rx.clone()).await;
            return Ok(());
        }
    };

    let service_arc = build_master_service(
        context.snapshot_backend_type,
        snapshot_dir_for_cluster(
            context.snapshot_dir.clone(),
            &context.ha_spec.cluster_namespace,
        ),
        context.runtime_config.clone(),
    )?;
    service_arc.set_service_available(false);

    let mut supervisor = new_supervisor(
        &context.ha_spec,
        &context.supervisor_config,
        service_arc.clone(),
    );
    if let Err(e) = supervisor.enter_standby_mode(None) {
        warn!("enter_standby_mode failed: {}, retrying in 1s", e);
        let _ = sleep_or_shutdown(RETRY_DELAY, shutdown_rx.clone()).await;
        return Ok(());
    }

    while matches!(
        run_candidacy_round(
            context,
            &coordinator,
            &service_arc,
            &mut supervisor,
            shutdown_rx.clone()
        )
        .await?,
        CandidacyOutcome::KeepWatching
    ) {}

    Ok(())
}

async fn create_coordinator_until_shutdown(
    spec: &HABackendSpec,
    shutdown_rx: ShutdownReceiver,
) -> Option<Result<LeaderCoordinator, Box<dyn std::error::Error>>> {
    tokio::select! {
        result = create_coordinator(spec) => Some(result),
        _ = wait_for_shutdown(shutdown_rx) => None,
    }
}

async fn run_candidacy_round(
    context: &HaLoopContext,
    coordinator: &LeaderCoordinator,
    service_arc: &Arc<MasterServiceImpl>,
    supervisor: &mut MasterServiceSupervisor,
    shutdown_rx: ShutdownReceiver,
) -> HaLoopResult<CandidacyOutcome> {
    supervisor.begin_candidacy();

    let current_view = match tokio::select! {
        result = coordinator.read_current_view() => result,
        _ = wait_for_shutdown(shutdown_rx.clone()) => return Ok(CandidacyOutcome::Shutdown),
    } {
        Ok(view) => view,
        Err(e) => {
            warn!("read_current_view failed: {}", e);
            if e.is_fatal() {
                return Err(Box::new(e));
            }
            back_to_standby(supervisor, 1, shutdown_rx.clone()).await;
            return Ok(CandidacyOutcome::RestartCoordinator);
        }
    };

    log_current_view(&current_view);
    supervisor.update_observed_leader(current_view.clone());

    if current_view.is_some() {
        if wait_and_continue(coordinator, &current_view, shutdown_rx.clone()).await {
            return Ok(CandidacyOutcome::Shutdown);
        }
        return Ok(CandidacyOutcome::KeepWatching);
    }

    acquire_and_serve(context, coordinator, service_arc, supervisor, shutdown_rx).await
}

async fn acquire_and_serve(
    context: &HaLoopContext,
    coordinator: &LeaderCoordinator,
    service_arc: &Arc<MasterServiceImpl>,
    supervisor: &mut MasterServiceSupervisor,
    shutdown_rx: ShutdownReceiver,
) -> HaLoopResult<CandidacyOutcome> {
    info!("No leader — acquiring leadership");
    let acquire = match tokio::select! {
        result = coordinator
            .try_acquire_leadership(&context.leader_addr, context.args.ha_lease_ttl_secs) => result,
        _ = wait_for_shutdown(shutdown_rx.clone()) => return Ok(CandidacyOutcome::Shutdown),
    } {
        Ok(result) if result.acquired => result,
        Ok(result) => {
            if wait_after_acquire_contention(
                coordinator,
                supervisor,
                result.view,
                shutdown_rx.clone(),
            )
            .await
            {
                return Ok(CandidacyOutcome::Shutdown);
            }
            return Ok(CandidacyOutcome::KeepWatching);
        }
        Err(e) => {
            warn!("Acquire error: {}", e);
            if sleep_or_shutdown(RETRY_DELAY, shutdown_rx.clone()).await {
                return Ok(CandidacyOutcome::Shutdown);
            }
            return Ok(CandidacyOutcome::RestartCoordinator);
        }
    };

    let acquire_view = acquire.view.clone();
    let Some(session) = acquire.session else {
        warn!("Leadership acquired without a session, retrying");
        back_to_standby(supervisor, 1, shutdown_rx.clone()).await;
        return Ok(CandidacyOutcome::RestartCoordinator);
    };

    if !ensure_leader_oplog(
        context,
        coordinator,
        service_arc,
        supervisor,
        &session,
        shutdown_rx.clone(),
    )
    .await
    {
        return Ok(CandidacyOutcome::RestartCoordinator);
    }
    apply_acquired_view(service_arc, &acquire_view);
    info!(
        "Leadership acquired, lease TTL={}s",
        context.args.ha_lease_ttl_secs
    );

    run_leader_term(
        context,
        coordinator,
        service_arc,
        supervisor,
        session,
        shutdown_rx.clone(),
    )
    .await;
    Ok(CandidacyOutcome::RestartCoordinator)
}

async fn wait_after_acquire_contention(
    coordinator: &LeaderCoordinator,
    supervisor: &mut MasterServiceSupervisor,
    observed_view: Option<MasterView>,
    shutdown_rx: ShutdownReceiver,
) -> bool {
    info!(
        "Not acquired, waiting for view change from version {:?}",
        observed_view.as_ref().map(|view| view.view_version)
    );
    supervisor.update_observed_leader(observed_view.clone());
    if let Err(e) = supervisor.enter_standby_mode(observed_view.clone()) {
        warn!("enter_standby_mode after acquire contention failed: {}", e);
    }
    wait_and_continue(coordinator, &observed_view, shutdown_rx).await
}

async fn ensure_leader_oplog(
    context: &HaLoopContext,
    coordinator: &LeaderCoordinator,
    service_arc: &Arc<MasterServiceImpl>,
    supervisor: &mut MasterServiceSupervisor,
    session: &LeadershipSession,
    shutdown_rx: ShutdownReceiver,
) -> bool {
    // Always replace the standby reader with an election-fenced writer.
    // Reusing the reader store would permit writes without comparing the
    // current election-key revision.
    match build_leader_oplog_manager(&context.ha_spec, session.view.view_version).await {
        Some(manager) => {
            if let Err(error) = service_arc.replace_oplog_manager(manager) {
                warn!(
                    "Leader oplog manager replacement failed, releasing leadership: {}",
                    error
                );
                release_and_retry(coordinator, supervisor, session, shutdown_rx).await;
                return false;
            }
            true
        }
        None => {
            warn!("Leader oplog store is unavailable, releasing leadership");
            release_and_retry(coordinator, supervisor, session, shutdown_rx).await;
            false
        }
    }
}

fn apply_acquired_view(service_arc: &Arc<MasterServiceImpl>, view: &Option<MasterView>) {
    if let Some(view) = view {
        service_arc.set_view_version(view.view_version as i64);
        service_arc.set_leadership_view_version(view.view_version);
        service_arc
            .oplog_manager()
            .set_view_version(view.view_version);
    }
}

async fn run_leader_term(
    context: &HaLoopContext,
    coordinator: &LeaderCoordinator,
    service_arc: &Arc<MasterServiceImpl>,
    supervisor: &mut MasterServiceSupervisor,
    session: LeadershipSession,
    shutdown_rx: ShutdownReceiver,
) {
    let Some(keepalive_handle) = prepare_leader_for_serving(
        context,
        coordinator,
        supervisor,
        &session,
        shutdown_rx.clone(),
    )
    .await
    else {
        return;
    };

    if let Err(error) = service_arc.prepare_tenant_quota_leadership_term(session.view.view_version)
    {
        error!("Tenant quota policy term preflight failed: {error}");
        supervisor.enable_standby_updates();
        drop(keepalive_handle);
        release_and_retry(coordinator, supervisor, &session, shutdown_rx.clone()).await;
        return;
    }

    let catalog_publisher = match preflight_snapshot_pipeline(
        &context.args,
        &context.ha_spec.cluster_namespace,
        service_arc,
    ) {
        Ok(publisher) => publisher,
        Err(error) => {
            error!("Snapshot pipeline preflight failed: {error}");
            supervisor.enable_standby_updates();
            drop(keepalive_handle);
            release_and_retry(coordinator, supervisor, &session, shutdown_rx.clone()).await;
            return;
        }
    };

    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_forwarder =
        forward_process_shutdown(shutdown_rx.clone(), server_shutdown_tx.clone());
    let monitor = match start_leadership_monitor(
        coordinator,
        &session,
        server_shutdown_tx,
        service_arc.clone(),
    )
    .await
    {
        Ok(handle) => handle,
        Err(e) => {
            warn!("Leadership monitor start failed: {}", e);
            supervisor.enable_standby_updates();
            shutdown_forwarder.abort();
            drop(keepalive_handle);
            release_and_retry(coordinator, supervisor, &session, shutdown_rx.clone()).await;
            return;
        }
    };

    supervisor.activate_serving_state();
    service_arc.set_service_available(true);
    if !service_arc.is_service_available() {
        error!("Refusing to publish or serve leadership because the master is durability-fenced");
        coordinator.set_k8s_leader_label(false);
        drop(monitor);
        shutdown_forwarder.abort();
        drop(keepalive_handle);
        cleanup_leader_term(coordinator, supervisor, &session).await;
        return;
    }
    // Match C++ routing semantics: advertise the K8s leader label only after
    // the service plane is actually accepting requests.
    coordinator.set_k8s_leader_label(true);

    let server_result = super::main_server::run_leader_server(
        service_arc.clone(),
        &context.args,
        server_shutdown_rx,
        Some(session.view.clone()),
        catalog_publisher,
    )
    .await;

    match &server_result {
        Ok(()) => info!("gRPC server exited cleanly"),
        Err(e) => error!("gRPC server error: {}", e),
    }

    service_arc.set_service_available(false);
    coordinator.set_k8s_leader_label(false);
    drop(monitor);
    shutdown_forwarder.abort();
    drop(keepalive_handle);
    cleanup_leader_term(coordinator, supervisor, &session).await;
}

async fn prepare_leader_for_serving(
    context: &HaLoopContext,
    coordinator: &LeaderCoordinator,
    supervisor: &mut MasterServiceSupervisor,
    session: &LeadershipSession,
    shutdown_rx: ShutdownReceiver,
) -> Option<mooncake_store_master::ha::LeadershipHandle> {
    // Keep the lease alive before promotion/final catch-up. Promotion can
    // legitimately take longer than one lease TTL when it resolves gaps.
    let keepalive_handle = match tokio::select! {
        result = coordinator.start_leadership_keepalive(session) => result,
        _ = wait_for_shutdown(shutdown_rx.clone()) => {
            release_and_retry(coordinator, supervisor, session, shutdown_rx.clone()).await;
            return None;
        }
    } {
        Ok(handle) => handle,
        Err(e) => {
            error!("Keepalive start failed: {}", e);
            release_and_retry(coordinator, supervisor, session, shutdown_rx.clone()).await;
            return None;
        }
    };

    // Match C++: stop accepting standby runtime callbacks before promotion, so
    // LeaderWarmup cannot be overwritten by standby.
    supervisor.disable_standby_updates();
    if let Err(e) = supervisor.promote_to_leader_warmup() {
        error!("Promotion failed: {}", e);
        supervisor.enable_standby_updates();
        drop(keepalive_handle);
        release_and_retry(coordinator, supervisor, session, shutdown_rx.clone()).await;
        return None;
    }

    if !warmup_with_renewal(
        coordinator,
        session,
        context.args.ha_lease_ttl_secs,
        shutdown_rx.clone(),
    )
    .await
    {
        supervisor.enable_standby_updates();
        drop(keepalive_handle);
        release_and_retry(coordinator, supervisor, session, shutdown_rx.clone()).await;
        return None;
    }

    if let Err(e) = tokio::select! {
        result = coordinator.try_renew_leadership(session) => result,
        _ = wait_for_shutdown(shutdown_rx.clone()) => {
            supervisor.enable_standby_updates();
            drop(keepalive_handle);
            release_and_retry(coordinator, supervisor, session, shutdown_rx.clone()).await;
            return None;
        }
    } {
        warn!("Preflight renewal failed: {}", e);
        supervisor.enable_standby_updates();
        drop(keepalive_handle);
        release_and_retry(coordinator, supervisor, session, shutdown_rx.clone()).await;
        return None;
    }

    Some(keepalive_handle)
}

async fn cleanup_leader_term(
    coordinator: &LeaderCoordinator,
    supervisor: &mut MasterServiceSupervisor,
    session: &LeadershipSession,
) {
    supervisor.deactivate_serving_state();
    supervisor.enable_standby_updates();
    if let Err(error) = coordinator.release_leadership(session).await {
        warn!(
            "leader cleanup could not revoke remote lease; local role is fenced and remote lease will expire: {error}"
        );
    }

    if let Ok(post_view) = coordinator.read_current_view().await {
        supervisor.update_observed_leader(post_view.clone());
        if let Err(e) = supervisor.enter_standby_mode(post_view) {
            warn!("enter_standby_mode after leader cleanup failed: {}", e);
        }
    } else {
        if let Err(e) = supervisor.enter_standby_mode(None) {
            warn!(
                "enter_standby_mode after leader cleanup without a current view failed: {}",
                e
            );
        }
    }

    info!("Returning to standby loop");
}

fn log_current_view(current_view: &Option<MasterView>) {
    info!(
        "Current view: {}",
        current_view
            .as_ref()
            .map(|v| v.leader_address.as_str())
            .unwrap_or("none")
    );
}

fn shutdown_requested(shutdown_rx: &ShutdownReceiver) -> bool {
    *shutdown_rx.borrow()
}

// A watch receiver cloned after shutdown may already hold `true`; check the
// current value before awaiting a future change so late readers do not block.
async fn wait_for_shutdown(mut shutdown_rx: ShutdownReceiver) {
    if shutdown_requested(&shutdown_rx) {
        return;
    }
    while shutdown_rx.changed().await.is_ok() {
        if shutdown_requested(&shutdown_rx) {
            return;
        }
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown_rx: ShutdownReceiver) -> bool {
    if shutdown_requested(&shutdown_rx) {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = wait_for_shutdown(shutdown_rx) => true,
    }
}

fn forward_process_shutdown(
    shutdown_rx: ShutdownReceiver,
    server_shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        wait_for_shutdown(shutdown_rx).await;
        let _ = server_shutdown_tx.send(true);
    })
}

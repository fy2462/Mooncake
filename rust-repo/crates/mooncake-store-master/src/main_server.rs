use super::*;

// ---------------------------------------------------------------------------
// Standalone (non-HA) path.
// 单机模式（非 HA）路径。
// ---------------------------------------------------------------------------
pub(super) async fn run_standalone(
    args: Args,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    // --- Master gRPC service ---
    // 构建 Master gRPC 服务
    let (snapshot_backend_type, snapshot_dir) = parse_snapshot_config(&args)?;

    let rpc_addr = SocketAddr::new(args.rpc_address.parse()?, args.rpc_port);
    let metadata_addr = SocketAddr::new(
        args.http_metadata_server_host.parse()?,
        args.http_metadata_server_port,
    );
    let metadata_listener = bind_metadata_listener(metadata_addr).await?;

    let runtime_config = build_runtime_config(&args)?;
    let service = MasterServiceImpl::try_new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir,
        runtime_config,
    )?;
    let service_arc = std::sync::Arc::new(service);
    let catalog_publisher =
        preflight_snapshot_pipeline(&args, &resolve_cluster_id(&args), &service_arc)?;
    let metrics_addr = SocketAddr::new(args.rpc_address.parse()?, args.metrics_port);
    tokio::spawn(metrics::serve_metrics_http_with_admin(
        metrics_addr,
        AdminRuntimeState::serving_with_service(None, service_arc.clone()),
    ));
    service_arc
        .metadata_state()
        .set_master_addr(format!("http://{}", rpc_addr))
        .await;

    // 启动 HTTP metadata 服务
    let metadata_state = service_arc.metadata_state();
    let metadata_service = service_arc.clone();
    tokio::spawn(async move {
        if let Err(error) = serve_metadata_listener_with_service_gate(
            metadata_listener,
            metadata_state,
            metadata_service,
        )
        .await
        {
            error!("HTTP metadata server stopped: {error}");
        }
    });

    if args.enable_snapshot {
        let svc = service_arc.clone();
        let interval = args.snapshot_interval_seconds;
        let mut snapshot_shutdown_rx = shutdown_rx.clone();
        let retention_count = args.snapshot_retention_count as usize;
        tokio::spawn(async move {
            loop {
                if !mooncake_store_master::snapshot_scheduler::wait_for_interval_or_shutdown(
                    interval,
                    &mut snapshot_shutdown_rx,
                )
                .await
                {
                    break;
                }
                svc.save_snapshot();
                if let Some(ref publisher) = catalog_publisher {
                    publish_catalog_snapshot(&svc, publisher, 0, retention_count);
                }
            }
        });
    }

    info!("Mooncake Master starting on {}", rpc_addr);
    ensure_supported_rpc_protocol()?;
    let shutdown_service = service_arc.clone();
    // 启动 gRPC server，阻塞直到服务停止
    tonic::transport::Server::builder()
        .add_service(
            mooncake_store_master::proto::master_service_server::MasterServiceServer::from_arc(
                service_arc,
            ),
        )
        .serve_with_shutdown(
            rpc_addr,
            wait_for_server_shutdown(shutdown_rx, shutdown_service),
        )
        .await?;

    Ok(())
}

/// Start the full gRPC server along with metrics, HTTP metadata, and snapshots.
/// Returns when the gRPC server stops (leadership lost / shutdown).
///
/// 启动完整的 gRPC 服务以及 metrics、HTTP metadata 和定时快照。
/// 当 gRPC server 停止时返回（leadership 丢失或主动关闭）。
pub(super) async fn run_leader_server(
    service_arc: Arc<MasterServiceImpl>,
    args: &Args,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    leader_view: Option<MasterView>,
    catalog_publisher: Option<CatalogBackedSnapshotProvider>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut background_tasks = Vec::new();
    let producer_view_version = leader_view
        .as_ref()
        .map(|view| view.view_version)
        .unwrap_or(0);

    // Metrics HTTP server.
    let metrics_addr = SocketAddr::new(args.rpc_address.parse()?, args.metrics_port);
    background_tasks.push(tokio::spawn(metrics::serve_metrics_http_with_admin(
        metrics_addr,
        AdminRuntimeState::serving_with_service(leader_view, service_arc.clone()),
    )));

    let rpc_addr = SocketAddr::new(args.rpc_address.parse()?, args.rpc_port);
    let metadata_addr = SocketAddr::new(
        args.http_metadata_server_host.parse()?,
        args.http_metadata_server_port,
    );
    let metadata_listener = bind_metadata_listener(metadata_addr).await?;

    service_arc
        .metadata_state()
        .set_master_addr(format!("http://{}", rpc_addr))
        .await;

    let metadata_state = service_arc.metadata_state();
    let metadata_service = service_arc.clone();
    background_tasks.push(tokio::spawn(async move {
        if let Err(error) = serve_metadata_listener_with_service_gate(
            metadata_listener,
            metadata_state,
            metadata_service,
        )
        .await
        {
            error!("HTTP metadata server stopped: {error}");
        }
    }));

    if args.enable_snapshot {
        let svc = service_arc.clone();
        let interval = args.snapshot_interval_seconds;
        let mut snapshot_shutdown_rx = shutdown_rx.clone();
        let retention_count = args.snapshot_retention_count as usize;
        background_tasks.push(tokio::spawn(async move {
            loop {
                if !mooncake_store_master::snapshot_scheduler::wait_for_interval_or_shutdown(
                    interval,
                    &mut snapshot_shutdown_rx,
                )
                .await
                {
                    break;
                }
                if !svc.is_service_available() {
                    tracing::debug!(
                        "Skipping scheduled HA snapshot because this master is not serving"
                    );
                    continue;
                }
                svc.save_snapshot();
                if let Some(ref publisher) = catalog_publisher {
                    publish_catalog_snapshot(
                        &svc,
                        publisher,
                        producer_view_version,
                        retention_count,
                    );
                }
            }
        }));
    }

    info!("Mooncake Master (HA leader) starting on {}", rpc_addr);
    ensure_supported_rpc_protocol()?;

    let gate = service_arc.clone();
    let shutdown_service = service_arc.clone();
    let master_service =
        mooncake_store_master::proto::master_service_server::MasterServiceServer::from_arc(
            service_arc,
        );
    let master_service =
        tonic::service::interceptor::InterceptedService::new(master_service, move |request| {
            if gate.is_service_available() {
                Ok(request)
            } else {
                Err(tonic::Status::unavailable("master service is not serving"))
            }
        });

    // Serve with shutdown signal — LeadershipMonitor triggers graceful stop on lease loss.
    // C++ equivalent: LeadershipMonitor callback calls server.stop().
    let serve_future = tonic::transport::Server::builder()
        .add_service(master_service)
        .serve_with_shutdown(
            rpc_addr,
            wait_for_server_shutdown(shutdown_rx, shutdown_service),
        );

    let result = serve_future.await;
    for task in background_tasks {
        task.abort();
    }
    result?;
    Ok(())
}

async fn wait_for_server_shutdown(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    service: Arc<MasterServiceImpl>,
) {
    loop {
        if should_stop_server(*shutdown_rx.borrow(), service.is_service_fenced()) {
            if service.is_service_fenced() {
                service.set_service_available(false);
                error!("Master durability fence raised, stopping gRPC server");
            } else {
                info!("Shutdown signal received, stopping gRPC server");
            }
            return;
        }
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || should_stop_server(
                    *shutdown_rx.borrow(),
                    service.is_service_fenced(),
                ) {
                    if service.is_service_fenced() {
                        service.set_service_available(false);
                        error!("Master durability fence raised, stopping gRPC server");
                    } else {
                        info!("Shutdown signal received, stopping gRPC server");
                    }
                    return;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}

fn should_stop_server(process_shutdown: bool, service_fenced: bool) -> bool {
    process_shutdown || service_fenced
}

#[cfg(test)]
mod tests {
    use super::should_stop_server;

    #[test]
    fn server_shutdown_predicate_includes_irreversible_service_fence() {
        assert!(!should_stop_server(false, false));
        assert!(should_stop_server(true, false));
        assert!(should_stop_server(false, true));
        assert!(should_stop_server(true, true));
    }
}

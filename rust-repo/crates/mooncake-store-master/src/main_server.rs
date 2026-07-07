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
    let snapshot_backend_type = args.snapshot_backend_type.as_deref().and_then(|s| {
        if s == "local-disk" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::LocalDisk)
        } else if s == "hf3fs" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::Hf3fs)
        } else if s == "distributed" {
            Some(mooncake_store_master::storage_backend::StorageBackendType::Distributed)
        } else {
            None
        }
    });
    let snapshot_dir = args
        .snapshot_backup_dir
        .clone()
        .map(std::path::PathBuf::from);

    let rpc_addr = SocketAddr::new(args.rpc_address.parse()?, args.rpc_port);
    let metadata_addr = SocketAddr::new(
        args.http_metadata_server_host.parse()?,
        args.http_metadata_server_port,
    );

    let runtime_config = build_runtime_config(&args)?;
    let service = MasterServiceImpl::new_with_runtime_config(
        snapshot_backend_type,
        snapshot_dir,
        runtime_config,
    );
    let service_arc = std::sync::Arc::new(service);
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
    tokio::spawn(serve_metadata_http(
        metadata_addr,
        service_arc.metadata_state(),
    ));

    if args.enable_snapshot {
        let svc = service_arc.clone();
        let interval = args.snapshot_interval_seconds;
        let mut snapshot_shutdown_rx = shutdown_rx.clone();
        let catalog_publisher =
            build_catalog_snapshot_publisher(&args, &resolve_cluster_id(&args))?;
        let retention_count = args.snapshot_retention_count as usize;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(interval)) => {
                        svc.save_snapshot();
                        if let Some(ref publisher) = catalog_publisher {
                            publish_catalog_snapshot(&svc, publisher, 0, retention_count);
                        }
                    }
                    changed = snapshot_shutdown_rx.changed() => {
                        if changed.is_err() || *snapshot_shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
    }

    info!("Mooncake Master starting on {}", rpc_addr);
    ensure_supported_rpc_protocol()?;
    // 启动 gRPC server，阻塞直到服务停止
    tonic::transport::Server::builder()
        .add_service(
            mooncake_store_master::proto::master_service_server::MasterServiceServer::from_arc(
                service_arc,
            ),
        )
        .serve_with_shutdown(rpc_addr, async move {
            if *shutdown_rx.borrow() {
                return;
            }
            while shutdown_rx.changed().await.is_ok() {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        })
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

    service_arc
        .metadata_state()
        .set_master_addr(format!("http://{}", rpc_addr))
        .await;

    background_tasks.push(tokio::spawn(serve_metadata_http(
        metadata_addr,
        service_arc.metadata_state(),
    )));

    if args.enable_snapshot {
        let svc = service_arc.clone();
        let interval = args.snapshot_interval_seconds;
        let mut snapshot_shutdown_rx = shutdown_rx.clone();
        let catalog_publisher = build_catalog_snapshot_publisher(args, &resolve_cluster_id(args))?;
        let retention_count = args.snapshot_retention_count as usize;
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(interval)) => {
                        svc.save_snapshot();
                        if let Some(ref publisher) = catalog_publisher {
                            publish_catalog_snapshot(&svc, publisher, producer_view_version, retention_count);
                        }
                    }
                    changed = snapshot_shutdown_rx.changed() => {
                        if changed.is_err() || *snapshot_shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }));
    }

    info!("Mooncake Master (HA leader) starting on {}", rpc_addr);
    ensure_supported_rpc_protocol()?;

    let gate = service_arc.clone();
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
        .serve_with_shutdown(rpc_addr, async move {
            loop {
                if shutdown_rx.changed().await.is_err() || *shutdown_rx.borrow() {
                    info!("Shutdown signal received, stopping gRPC server");
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

    let result = serve_future.await;
    for task in background_tasks {
        task.abort();
    }
    result?;
    Ok(())
}

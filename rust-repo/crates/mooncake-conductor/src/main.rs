//! Mooncake Conductor — KV Cache Prefix Index Service
//!
//! Ported from Go: mooncake-conductor/conductor-ctrl/main.go

use std::{process, sync::Arc};

use tokio::sync::oneshot;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use mooncake_conductor::config;
use mooncake_conductor::event_manager::EventManager;
use mooncake_conductor::http_api;

#[tokio::main]
async fn main() {
    // Setup logging
    let log_level = config::parse_log_level();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&log_level))
        .init();

    info!("Starting Conductor KV Event Manager...");

    let cfg = config::load_config();
    info!(
        "Parsed {} service configurations, HTTP port={}",
        cfg.services.len(),
        cfg.http_port
    );

    let manager = Arc::new(EventManager::new(cfg.services, cfg.http_port));

    // Start ZMQ subscriptions
    if let Err(e) = manager.start() {
        error!("Failed to start manager: {}", e);
        process::exit(1);
    }

    // Set up HTTP server shutdown channel
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Start HTTP server in a background task
    let http_state = manager.http_state();
    let http_port = manager.http_port();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = http_api::serve(http_state, http_port, shutdown_rx).await {
            error!("HTTP server error: {}", e);
        }
    });

    // Wait for SIGINT/SIGTERM
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("failed to install SIGINT handler");
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    info!("Manager is running. Press Ctrl+C to stop.");

    tokio::select! {
        _ = sigint.recv() => info!("Received SIGINT"),
        _ = sigterm.recv() => info!("Received SIGTERM"),
    }

    info!("Shutting down...");
    let _ = shutdown_tx.send(());
    manager.stop();
    let _ = http_handle.await;
    info!("Shutdown complete.");
}

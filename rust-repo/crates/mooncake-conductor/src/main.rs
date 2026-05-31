// ============================================================================
// Mooncake Conductor — Entry Point / 入口点
//
// Starts the Conductor service:
//   1. Load configuration from JSON file.
//      从 JSON 文件加载配置。
//   2. Create EventManager and start ZMQ subscriptions.
//      创建 EventManager 并启动 ZMQ 订阅。
//   3. Launch HTTP API server (Axum) in a background tokio task.
//      在后台 tokio 任务中启动 HTTP API 服务器（Axum）。
//   4. Wait for SIGINT/SIGTERM, then graceful shutdown.
//      等待 SIGINT/SIGTERM 信号，然后优雅关闭。
//
// Ported from Go: mooncake-conductor/conductor-ctrl/main.go
// ============================================================================

use std::{process, sync::Arc};

use tokio::sync::oneshot;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use mooncake_conductor::config;
use mooncake_conductor::event_manager::EventManager;
use mooncake_conductor::http_api;

#[tokio::main]
async fn main() {
    // Setup structured logging with env-controlled filter level.
    // 设置结构化日志，通过环境变量控制过滤级别。
    let log_level = config::parse_log_level();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&log_level))
        .init();

    info!("Starting Conductor KV Event Manager...");

    // Load service configurations from JSON config file.
    // 从 JSON 配置文件加载服务配置。
    let cfg = config::load_config();
    info!(
        "Parsed {} service configurations, HTTP port={}",
        cfg.services.len(),
        cfg.http_port
    );

    // Create the event manager, which coordinates ZMQ clients and the prefix index.
    // 创建事件管理器，协调 ZMQ 客户端和前缀索引。
    let manager = Arc::new(EventManager::new(cfg.services, cfg.http_port));

    // Start all ZMQ subscriptions in background threads.
    // 在后台线程中启动所有 ZMQ 订阅。
    if let Err(e) = manager.start() {
        error!("Failed to start manager: {}", e);
        process::exit(1);
    }

    // Set up HTTP server shutdown channel for graceful termination.
    // 设置 HTTP 服务器关闭通道，用于优雅终止。
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Spawn HTTP server in a background tokio task.
    // 在后台 tokio 任务中启动 HTTP 服务器。
    let http_state = manager.http_state();
    let http_port = manager.http_port();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = http_api::serve(http_state, http_port, shutdown_rx).await {
            error!("HTTP server error: {}", e);
        }
    });

    // Install signal handlers for graceful shutdown.
    // 安装信号处理器，用于优雅关闭。
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("failed to install SIGINT handler");
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    info!("Manager is running. Press Ctrl+C to stop.");

    // Wait for termination signal.
    // 等待终止信号。
    tokio::select! {
        _ = sigint.recv() => info!("Received SIGINT"),
        _ = sigterm.recv() => info!("Received SIGTERM"),
    }

    // Graceful shutdown: stop HTTP server, then ZMQ subscriptions.
    // 优雅关闭：先停 HTTP 服务器，再停 ZMQ 订阅。
    info!("Shutting down...");
    let _ = shutdown_tx.send(());
    manager.stop();
    let _ = http_handle.await;
    info!("Shutdown complete.");
}

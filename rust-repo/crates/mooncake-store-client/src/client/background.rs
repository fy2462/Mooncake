use super::MooncakeClient;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientBackgroundConfig {
    pub health_interval: Duration,
    pub storage_interval: Duration,
    pub task_poll_interval: Duration,
    pub task_batch_size: u32,
    pub enable_offloading: bool,
    pub enable_promotion: bool,
    pub enable_task_poll: bool,
    pub report_ssd_capacity: bool,
    pub enable_disk_watermark_eviction: bool,
    pub disk_eviction_high_watermark_ratio: f64,
    pub disk_eviction_low_watermark_ratio: f64,
}

impl Default for ClientBackgroundConfig {
    fn default() -> Self {
        let configured_high = env_ratio(
            "MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO",
            "MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO",
            0.90,
        );
        let configured_low = env_ratio(
            "MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO",
            "MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO",
            0.80,
        );
        let (high, low) = if configured_low < configured_high {
            (configured_high, configured_low)
        } else {
            tracing::warn!(
                high = configured_high,
                low = configured_low,
                "invalid disk eviction watermarks; using defaults"
            );
            (0.90, 0.80)
        };
        Self {
            health_interval: Duration::from_secs(1),
            storage_interval: Duration::from_secs(5),
            task_poll_interval: Duration::from_millis(200),
            task_batch_size: 16,
            enable_offloading: true,
            enable_promotion: true,
            enable_task_poll: true,
            report_ssd_capacity: true,
            enable_disk_watermark_eviction: env_bool(
                "MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION",
                true,
            ),
            disk_eviction_high_watermark_ratio: high,
            disk_eviction_low_watermark_ratio: low,
        }
    }
}

pub struct ClientBackgroundHandle {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    join_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl ClientBackgroundHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        for handle in self.join_handles {
            let _ = handle.await;
        }
    }
}

impl MooncakeClient {
    /// Start C++-style client background workers.
    ///
    /// The workers periodically run health/remount checks, storage
    /// offload/promotion heartbeats, and replica copy/move task polling.
    /// The client is passed behind a Tokio mutex so all existing `&mut self`
    /// APIs can be reused without making the whole client cloneable.
    pub fn start_background_workers(
        client: Arc<tokio::sync::Mutex<Self>>,
        config: ClientBackgroundConfig,
    ) -> ClientBackgroundHandle {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut join_handles = Vec::new();

        join_handles.push(tokio::spawn(Self::health_worker_loop(
            Arc::clone(&client),
            config.health_interval,
            shutdown_rx.clone(),
        )));

        if config.enable_offloading
            || config.enable_promotion
            || config.report_ssd_capacity
            || config.enable_disk_watermark_eviction
        {
            join_handles.push(tokio::spawn(Self::storage_worker_loop(
                Arc::clone(&client),
                config.clone(),
                shutdown_rx.clone(),
            )));
        }

        if config.enable_task_poll {
            join_handles.push(tokio::spawn(Self::task_worker_loop(
                client,
                config.task_poll_interval,
                config.task_batch_size,
                shutdown_rx,
            )));
        }

        ClientBackgroundHandle {
            shutdown_tx,
            join_handles,
        }
    }

    async fn health_worker_loop(
        client: Arc<tokio::sync::Mutex<Self>>,
        interval: Duration,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let result = client.lock().await.health_check().await;
                    if let Err(e) = result {
                        tracing::warn!(target: "client_background", %e, "health_check worker iteration failed");
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn storage_worker_loop(
        client: Arc<tokio::sync::Mutex<Self>>,
        config: ClientBackgroundConfig,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(config.storage_interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let mut guard = client.lock().await;
                    if config.report_ssd_capacity {
                        if let Some(storage) = guard.local_storage.as_ref() {
                            let (_, total) = storage.space_usage();
                            if let Err(e) = guard.report_ssd_capacity(total as i64).await {
                                tracing::warn!(target: "client_background", %e, "report_ssd_capacity worker iteration failed");
                            }
                        }
                    }
                    if config.enable_offloading && guard.local_storage.is_some() {
                        if let Err(e) = guard.offload_objects(config.enable_offloading).await {
                            tracing::warn!(target: "client_background", %e, "offload worker iteration failed");
                        }
                    }
                    if config.enable_promotion && guard.local_storage.is_some() {
                        if let Err(e) = guard.promote_objects().await {
                            tracing::warn!(target: "client_background", %e, "promotion worker iteration failed");
                        }
                    }
                    if config.enable_disk_watermark_eviction && guard.local_storage.is_some() {
                        match guard.run_disk_watermark_eviction(
                            config.disk_eviction_high_watermark_ratio,
                            config.disk_eviction_low_watermark_ratio,
                        ).await {
                            Ok(0) => {}
                            Ok(count) => tracing::info!(target: "client_background", count, "disk watermark eviction completed"),
                            Err(e) => tracing::warn!(target: "client_background", %e, "disk watermark eviction failed"),
                        }
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn task_worker_loop(
        client: Arc<tokio::sync::Mutex<Self>>,
        interval: Duration,
        batch_size: u32,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let mut guard = client.lock().await;
                    let tasks = match guard.fetch_tasks(batch_size).await {
                        Ok(tasks) => tasks,
                        Err(e) => {
                            tracing::warn!(target: "client_background", %e, "fetch_tasks worker iteration failed");
                            continue;
                        }
                    };
                    for task in tasks {
                        if let Err(e) = guard.execute_task_assignment(task).await {
                            tracing::warn!(target: "client_background", %e, "execute_task_assignment failed");
                        }
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => true,
            "0" | "false" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn env_ratio(preferred: &str, fallback: &str, default: f64) -> f64 {
    [preferred, fallback]
        .into_iter()
        .filter_map(|name| std::env::var(name).ok())
        .find_map(|value| {
            value
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite() && *value > 0.0 && *value <= 1.0)
        })
        .unwrap_or(default)
}

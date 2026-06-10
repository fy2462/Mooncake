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
}

impl Default for ClientBackgroundConfig {
    fn default() -> Self {
        Self {
            health_interval: Duration::from_secs(1),
            storage_interval: Duration::from_secs(5),
            task_poll_interval: Duration::from_millis(200),
            task_batch_size: 16,
            enable_offloading: true,
            enable_promotion: true,
            enable_task_poll: true,
            report_ssd_capacity: true,
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

        if config.enable_offloading || config.enable_promotion || config.report_ssd_capacity {
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

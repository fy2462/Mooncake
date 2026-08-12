/// Wait for the next periodic snapshot, returning `false` when shutdown must
/// interrupt the interval instead.
#[doc(hidden)]
pub async fn wait_for_interval_or_shutdown(
    interval_seconds: u64,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return false;
                }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(interval_seconds)) => {
                return true;
            }
        }
    }
}

/// Run the production periodic snapshot cycle until shutdown.
#[doc(hidden)]
pub async fn run_periodic_snapshots(
    service: Arc<MasterServiceImpl>,
    interval_seconds: u64,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    catalog_publisher: Option<CatalogBackedSnapshotProvider>,
    producer_view_version: u64,
    retention_count: usize,
    require_serving: bool,
) {
    loop {
        if !wait_for_interval_or_shutdown(interval_seconds, &mut shutdown_rx).await {
            return;
        }
        if require_serving && !service.is_service_available() {
            tracing::debug!("Skipping scheduled HA snapshot because this master is not serving");
            continue;
        }
        service.save_snapshot();
        if let Some(ref publisher) = catalog_publisher {
            publish_catalog_snapshot(&service, publisher, producer_view_version, retention_count);
        }
    }
}
use crate::MasterServiceImpl;
use crate::ha::CatalogBackedSnapshotProvider;
use crate::main_config::publish_catalog_snapshot;
use std::sync::Arc;

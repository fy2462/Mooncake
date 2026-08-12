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

use super::*;

// ============================================================================
// NofHeartbeatWorker — NoF segment liveness probe and auto-unmount
// NofHeartbeatWorker —— NoF segment 存活探测与自动卸载
// ============================================================================

/// Periodically probes NoF segments for liveness. Segments that fail
/// `nof_heartbeat_failures_threshold` consecutive probes are unmounted.
/// Probes at most one segment per ~100ms cycle to avoid overwhelming the probe
/// mechanism. Uses mpsc channel for stoppable periodic loop.
///
/// C++ equivalent: `NofHeartbeatThreadFunc` + `TryUnmountNoFSegmentByHeartbeat`
/// in master_service.cpp.
///
/// 周期性探测 NoF segment 存活状态。连续失败达到阈值后自动卸载。
/// 每个周期最多探测一个 segment（~100ms），避免探测风暴。
/// 使用 mpsc channel 实现可停止的周期性循环。
pub(crate) struct NofHeartbeatWorker {
    sender: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl NofHeartbeatWorker {
    /// Start the NoF heartbeat probe thread.
    /// `probe_fn` is a closure that probes a given transport endpoint (te_endpoint)
    /// and returns Ok(()) on success or Err(reason) on failure. Injected for testability.
    ///
    /// 启动 NoF 心跳探测线程。
    /// probe_fn 是探测闭包：传入 te_endpoint，返回 Ok 表示成功，Err 表示失败原因。
    pub(crate) fn new(
        state: Arc<MasterState>,
        probe_fn: Box<dyn Fn(&str, Duration) -> Result<(), String> + Send + Sync>,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let worker = thread::spawn(move || {
            let cycle_sleep = Duration::from_millis(100);
            let probe_timeout = state.runtime_config.nof_heartbeat_probe_timeout;
            let interval = state.runtime_config.nof_heartbeat_interval;
            let threshold = state.runtime_config.nof_heartbeat_failures_threshold;
            let mut probe_index = 0usize;

            loop {
                match rx.recv_timeout(cycle_sleep) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                }

                let now = Instant::now();

                // Snapshot active NoF segments.
                let active_segments: Vec<(Uuid, Uuid, String, String)> = state
                    .nof_segments
                    .iter()
                    .filter(|entry| entry.status == crate::proto::SegmentStatus::Active)
                    .map(|entry| {
                        (
                            entry.segment.id,
                            entry.segment.client_id,
                            entry.segment.name.clone(),
                            entry.segment.te_endpoint.clone(),
                        )
                    })
                    .collect();

                // Sync heartbeat states: add new, remove stale.
                let active_ids: std::collections::HashSet<Uuid> =
                    active_segments.iter().map(|(id, _, _, _)| *id).collect();
                for (id, _client_id, name, te) in &active_segments {
                    state.nof_heartbeat_states.entry(*id).or_insert_with(|| {
                        // Stagger initial probe time across the interval.
                        let spread = std::time::Duration::from_secs_f64(
                            interval.as_secs_f64()
                                * (state.nof_heartbeat_states.len() as f64
                                    / active_segments.len().max(1) as f64),
                        );
                        NoFHeartbeatState {
                            segment_id: *id,
                            segment_name: name.clone(),
                            te_endpoint: te.clone(),
                            next_probe_at: now + interval + spread,
                            last_success_at: now,
                            consecutive_failures: 0,
                        }
                    });
                }
                // Remove heartbeat state for unmounted segments.
                state
                    .nof_heartbeat_states
                    .retain(|id, _| active_ids.contains(id));

                // Find next segment due for probing (round-robin, at most 1/cycle).
                let probe_targets: Vec<Uuid> = state
                    .nof_heartbeat_states
                    .iter()
                    .filter(|entry| entry.next_probe_at <= now)
                    .map(|entry| *entry.key())
                    .collect();

                if probe_targets.is_empty() {
                    continue;
                }

                probe_index = probe_index.min(probe_targets.len() - 1);
                let target_id = probe_targets[probe_index];
                probe_index = (probe_index + 1) % probe_targets.len();

                let mut entry_result = None;
                if let Some(mut entry) = state.nof_heartbeat_states.get_mut(&target_id) {
                    let te_endpoint = entry.te_endpoint.clone();
                    let success = probe_fn(&te_endpoint, probe_timeout);
                    match success {
                        Ok(()) => {
                            entry.consecutive_failures = 0;
                            entry.last_success_at = now;
                            entry.next_probe_at = now + interval;
                            entry_result = None;
                        }
                        Err(_reason) => {
                            entry.consecutive_failures += 1;
                            entry.next_probe_at = now + interval;
                            let alive_timeout = Duration::from_secs_f64(
                                interval.as_secs_f64() * threshold.max(1) as f64,
                            );
                            if now.saturating_duration_since(entry.last_success_at) >= alive_timeout
                            {
                                entry_result = Some((entry.segment_id, entry.segment_name.clone()));
                            }
                        }
                    }
                }

                // Unmount failed segment outside the DashMap lock.
                if let Some((seg_id, seg_name)) = entry_result {
                    tracing::warn!(
                        "NoF heartbeat: unmounting segment {} after {} consecutive failures",
                        seg_name,
                        threshold
                    );
                    let owner = state
                        .nof_segments
                        .get(&seg_id)
                        .map(|entry| entry.segment.client_id);
                    if let Some(owner) = owner {
                        unmount_nof_segment_owned(&state, seg_id, owner);
                    } else {
                        state.nof_heartbeat_states.remove(&seg_id);
                    }
                }
            }
        });
        Self {
            sender: Some(tx),
            worker: Some(worker),
        }
    }

    pub(crate) fn stop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}


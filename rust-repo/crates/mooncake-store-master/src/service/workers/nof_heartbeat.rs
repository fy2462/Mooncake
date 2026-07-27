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

                let Some(_background_mutation_guard) = state.begin_background_mutation() else {
                    continue;
                };
                let now = Instant::now();

                // Snapshot active NoF segments.
                let active_segments: Vec<(Uuid, Uuid, String, String)> = state
                    .nof_segments
                    .iter()
                    .filter(|entry| {
                        entry.status == crate::proto::SegmentStatus::Active
                            && entry.segment.base != 0
                            && !entry.segment.te_endpoint.is_empty()
                    })
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
                for (index, (id, _client_id, name, te)) in active_segments.iter().enumerate() {
                    // Compute the stagger before acquiring the DashMap entry
                    // lock. Re-entering the same map from `or_insert_with`
                    // can deadlock when `len()` needs the locked shard.
                    let spread = std::time::Duration::from_secs_f64(
                        interval.as_secs_f64()
                            * (index as f64 / active_segments.len().max(1) as f64),
                    );
                    state
                        .nof_heartbeat_states
                        .entry(*id)
                        .or_insert_with(|| NoFHeartbeatState {
                            segment_id: *id,
                            segment_name: name.clone(),
                            te_endpoint: te.clone(),
                            next_probe_at: now + interval + spread,
                            last_success_at: now,
                            consecutive_failures: 0,
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
                        let _ = unmount_nof_segment_owned_durable(
                            &state,
                            seg_id,
                            owner,
                            "nof_heartbeat_unmount",
                        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::NoFSegmentEntry;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn state_with_heartbeat(interval: Duration, threshold: u32) -> Arc<MasterState> {
        let mut state = MasterState::empty();
        state.runtime_config.nof_heartbeat_interval = interval;
        state.runtime_config.nof_heartbeat_probe_timeout = Duration::from_millis(10);
        state.runtime_config.nof_heartbeat_failures_threshold = threshold;
        Arc::new(state)
    }

    fn mount_nof(state: &MasterState, endpoint: &str, base: u64) -> Uuid {
        let segment_id = Uuid::new_v4();
        state.nof_segments.insert(
            segment_id,
            NoFSegmentEntry {
                segment: mooncake_store_core::NoFSegment {
                    id: segment_id,
                    name: format!("nof-{endpoint}"),
                    base,
                    size: 16 * 1024 * 1024,
                    te_endpoint: endpoint.into(),
                    client_id: Uuid::new_v4(),
                },
                used: 0,
                status: crate::proto::SegmentStatus::Active,
            },
        );
        segment_id
    }

    fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        predicate()
    }

    #[test]
    fn healthy_segment_resets_failures_after_initial_probe_grace() {
        let state = state_with_heartbeat(Duration::from_millis(300), 3);
        let segment_id = mount_nof(&state, "healthy", 0x5000_0000_0);
        let calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = calls.clone();
        let mut worker = NofHeartbeatWorker::new(
            state.clone(),
            Box::new(move |_, _| {
                probe_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
        );

        thread::sleep(Duration::from_millis(180));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(state.nof_segments.contains_key(&segment_id));
        assert!(wait_until(Duration::from_millis(400), || calls
            .load(Ordering::Relaxed)
            >= 1));
        assert_eq!(
            state
                .nof_heartbeat_states
                .get(&segment_id)
                .unwrap()
                .consecutive_failures,
            0
        );
        worker.stop();
    }

    #[test]
    fn failed_segment_unmounts_only_after_heartbeat_threshold() {
        let state = state_with_heartbeat(Duration::from_millis(100), 3);
        let segment_id = mount_nof(&state, "failed", 0x5000_0000_0);
        let calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = calls.clone();
        let mut worker = NofHeartbeatWorker::new(
            state.clone(),
            Box::new(move |_, _| {
                probe_calls.fetch_add(1, Ordering::Relaxed);
                Err("submit_fail".into())
            }),
        );

        assert!(wait_until(Duration::from_secs(1), || !state
            .nof_segments
            .contains_key(&segment_id)));
        assert!(calls.load(Ordering::Relaxed) >= 3);
        assert!(!state.nof_heartbeat_states.contains_key(&segment_id));
        worker.stop();
    }

    #[test]
    fn successful_probe_recovers_failure_count_without_unmounting() {
        let state = state_with_heartbeat(Duration::from_millis(100), 3);
        let segment_id = mount_nof(&state, "recovers", 0x5000_0000_0);
        let calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = calls.clone();
        let mut worker = NofHeartbeatWorker::new(
            state.clone(),
            Box::new(move |_, _| {
                let call = probe_calls.fetch_add(1, Ordering::Relaxed);
                if call < 2 {
                    Err("submit_fail".into())
                } else {
                    Ok(())
                }
            }),
        );

        assert!(wait_until(Duration::from_secs(1), || {
            calls.load(Ordering::Relaxed) >= 4
                && state
                    .nof_heartbeat_states
                    .get(&segment_id)
                    .map(|entry| entry.consecutive_failures == 0)
                    .unwrap_or(false)
        }));
        assert!(state.nof_segments.contains_key(&segment_id));
        worker.stop();
    }

    #[test]
    fn heartbeat_unmount_isolated_to_failed_segment() {
        let state = state_with_heartbeat(Duration::from_millis(300), 2);
        let good_segment = mount_nof(&state, "good", 0x5000_0000_0);
        let bad_segment = mount_nof(&state, "bad", 0x5010_0000_0);
        let good_calls = Arc::new(AtomicUsize::new(0));
        let bad_calls = Arc::new(AtomicUsize::new(0));
        let good_probe_calls = good_calls.clone();
        let bad_probe_calls = bad_calls.clone();
        let mut worker = NofHeartbeatWorker::new(
            state.clone(),
            Box::new(move |endpoint, _| {
                if endpoint == "good" {
                    good_probe_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                } else {
                    bad_probe_calls.fetch_add(1, Ordering::Relaxed);
                    Err("submit_fail".into())
                }
            }),
        );

        assert!(wait_until(Duration::from_secs(2), || !state
            .nof_segments
            .contains_key(&bad_segment)));
        assert!(state.nof_segments.contains_key(&good_segment));
        assert!(good_calls.load(Ordering::Relaxed) >= 1);
        assert!(bad_calls.load(Ordering::Relaxed) >= 2);
        worker.stop();
    }
}

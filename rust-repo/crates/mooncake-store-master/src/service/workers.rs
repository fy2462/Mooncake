//! # Background Workers — 后台工作线程 / Background Worker Threads
//!
//! 本模块包含 Master 服务的四个后台工作线程，各自独立运行在 std::thread 上：
//!
//! This module contains the four background worker threads of the Master service,
//! each running on its own std::thread:
//!
//! | Worker | 职责 / Responsibility | 停止方式 |
//! |--------|----------------------|---------|
//! | `GracefulUnmountScheduler` | 优雅卸载调度：Condvar + BinaryHeap，按 deadline 触发 | notify_all → join |
//! | `ProcessingReaper` | 任务回收：周期清理超时的 offload/promotion/PutStart 任务 | drop(tx) → join |
//! | `EvictionWorker` | 驱逐工作：周期检查内存水位并触发自动驱逐 | drop(tx) → join |
//! | `ClientMonitorWorker` | 客户端监控：检测心跳超时客户端并清理资源 | drop(tx) → join |
//!
//! GracefulUnmountScheduler 使用 Condvar 模式以确保紧急唤醒语义；
//! 其余三个周期性 worker 使用 mpsc channel —— stop() 时 drop sender 即可中断 recv_timeout。

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use super::background_ops::{
    reap_expired_background_tasks, run_automatic_eviction_once, run_automatic_nof_eviction_once,
    run_default_promotion_candidate_retry,
};
use super::helpers::{
    bump_view_version, clear_invalid_handles_locked, get_alive_clients_snapshot,
    host_from_segment_name, sync_client_segments, unmount_nof_segment_owned_durable,
    unmount_segment_owned_durable_locked,
};
use super::state::{MasterState, NoFHeartbeatState};
use crate::http_metadata::MetadataState;

mod nof_heartbeat;
pub(crate) use nof_heartbeat::NofHeartbeatWorker;

/// 优雅卸载记录：segment 被标记为待卸载后不会立即移除，
/// 而是等待一个宽限期（grace_period）让进行中的请求有机会完成。
///
/// Graceful unmount record: a segment marked for unmount will not be removed immediately;
/// instead, a grace period is waited to allow in-flight requests to complete.
#[derive(Debug, Clone)]
struct GracefulUnmountRecord {
    segment_id: Uuid,
    client_id: Uuid,
    /// Portable wall-clock deadline used by snapshots and oplog replay.
    deadline_epoch_ms: u64,
}

impl PartialEq for GracefulUnmountRecord {
    fn eq(&self, other: &Self) -> bool {
        self.segment_id == other.segment_id
            && self.client_id == other.client_id
            && self.deadline_epoch_ms == other.deadline_epoch_ms
    }
}

impl Eq for GracefulUnmountRecord {}

impl PartialOrd for GracefulUnmountRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// 使用 BinaryHeap（最大堆），按过期时间升序（即最早过期的在最顶端），
/// 方便 worker 快速获取下一批到期的卸载任务。
///
/// Uses BinaryHeap (max-heap) ordered by expiry time ascending (earliest expiry at top),
/// allowing the worker to quickly retrieve the next batch of due unmounts.
impl Ord for GracefulUnmountRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        other.deadline_epoch_ms.cmp(&self.deadline_epoch_ms)
    }
}

/// 优雅卸载调度器的内部状态。
/// Inner state of the graceful unmount scheduler.
struct GracefulUnmountSchedulerState {
    /// 按过期时间排序的待卸载队列 / Queue of pending unmounts, ordered by expiry.
    queue: BinaryHeap<GracefulUnmountRecord>,
    /// False while the service is a standby. Pending intents remain durable in
    /// MasterState and are repopulated on promotion.
    active: bool,
    /// 停止标志 / Stop flag.
    stopping: bool,
}

/// 优雅卸载调度器的内部结构（Arc 共享）。
/// Inner structure of graceful unmount scheduler (Arc-shared).
struct GracefulUnmountSchedulerInner {
    state: Mutex<GracefulUnmountSchedulerState>,
    condvar: Condvar,
}

/// 优雅卸载调度器：管理 segment 的延迟卸载。
/// GracefulUnmountScheduler: manages delayed segment unmounts.
pub(crate) struct GracefulUnmountScheduler {
    inner: Arc<GracefulUnmountSchedulerInner>,
    worker: Option<JoinHandle<()>>,
}

/// 任务回收器：周期清理超时的后台任务。
/// 使用 mpsc channel 实现可停止的周期性循环：stop() 时 drop sender，
/// worker 线程的 recv_timeout 收到 Disconnected 后退出。
///
/// ProcessingReaper: periodically cleans up expired background tasks.
/// Uses mpsc channel for stoppable periodic loop: stop() drops the sender,
/// worker thread exits on recv_timeout Disconnected.
pub(crate) struct ProcessingReaper {
    sender: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

/// 驱逐 worker：周期检查内存水位并触发自动驱逐。
/// 使用 mpsc channel 实现可停止的周期性循环。
///
/// EvictionWorker: periodically checks memory watermark and triggers auto eviction.
/// Uses mpsc channel for stoppable periodic loop.
pub(crate) struct EvictionWorker {
    sender: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

/// 客户端监控 worker：检测心跳超时的客户端并清理资源。
/// 使用 mpsc channel 实现可停止的周期性循环。
///
/// ClientMonitorWorker: detects heartbeat-timeout clients and cleans up resources.
/// Uses mpsc channel for stoppable periodic loop.
pub(crate) struct ClientMonitorWorker {
    sender: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl GracefulUnmountScheduler {
    /// 启动优雅卸载调度线程。使用 Condvar + BinaryHeap 实现定时触发：
    /// 等待下一个最早到期的卸载记录，到期后批量执行实际卸载。
    ///
    /// Start the graceful unmount scheduler thread. Uses Condvar + BinaryHeap for timed triggers:
    /// wait until the next earliest-expiry record is due, then batch-execute actual unmounts.
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let inner = Arc::new(GracefulUnmountSchedulerInner {
            state: Mutex::new(GracefulUnmountSchedulerState {
                queue: BinaryHeap::new(),
                active: state
                    .service_available
                    .load(std::sync::atomic::Ordering::Acquire),
                stopping: false,
            }),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let worker = thread::spawn(move || {
            loop {
                let mut guard = worker_inner.state.lock().expect("scheduler mutex poisoned");
                // 队列空时无限等待，有新记录加入时被 notify 唤醒
                // Wait indefinitely when queue is empty; woken by notify on new records
                while !guard.stopping && (!guard.active || guard.queue.is_empty()) {
                    guard = worker_inner
                        .condvar
                        .wait(guard)
                        .expect("scheduler condvar wait failed");
                }
                if guard.stopping {
                    break;
                }

                // 计算到下一个到期时间的间隔，带超时等待避免空转
                // Compute interval to next expiry, wait with timeout to avoid busy-wait
                let Some(next) = guard.queue.peek().cloned() else {
                    continue;
                };
                let now_epoch_ms = current_epoch_millis();
                if next.deadline_epoch_ms > now_epoch_ms {
                    // Periodically re-read wall time so large deadlines and
                    // clock adjustments cannot turn into an unbounded OS wait.
                    let timeout =
                        Duration::from_millis((next.deadline_epoch_ms - now_epoch_ms).min(60_000));
                    let (g, timeout_res) = worker_inner
                        .condvar
                        .wait_timeout(guard, timeout)
                        .expect("scheduler condvar timeout failed");
                    guard = g;
                    if guard.stopping {
                        break;
                    }
                    if !timeout_res.timed_out() {
                        continue; // 被新记录提前唤醒，重新检查队列 / Woken early by new record; re-check queue
                    }
                }

                // 批量收集所有已到期的记录，一次性释放锁后再执行卸载
                // Batch-collect all expired records, release lock, then execute unmounts
                let mut expired = Vec::new();
                let now_epoch_ms = current_epoch_millis();
                while let Some(record) = guard.queue.peek().cloned() {
                    if record.deadline_epoch_ms > now_epoch_ms {
                        break;
                    }
                    expired.push(record);
                    guard.queue.pop();
                }
                drop(guard); // 尽早释放锁，卸载操作可能耗时 / Release lock early; unmount may take time

                for record in expired {
                    if !state
                        .service_available
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        continue;
                    }
                    let _global_mutation_guard = state.key_mutations.lock_snapshot();
                    if !state
                        .service_available
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        continue;
                    }
                    let Some(pending) = state
                        .graceful_unmounts
                        .get(&record.segment_id)
                        .map(|entry| entry.value().clone())
                    else {
                        continue;
                    };
                    if pending.client_id != record.client_id
                        || pending.deadline_epoch_ms != record.deadline_epoch_ms
                    {
                        continue;
                    }
                    if pending.deadline_epoch_ms > current_epoch_millis() {
                        // The wall clock moved backwards after this record was
                        // collected. Requeue the authoritative deadline.
                        let mut guard =
                            worker_inner.state.lock().expect("scheduler mutex poisoned");
                        if guard.active && !guard.stopping {
                            guard.queue.push(record);
                        }
                        drop(guard);
                        worker_inner.condvar.notify_all();
                        continue;
                    }

                    let Some(segment) = state.segments.get(&record.segment_id) else {
                        state.graceful_unmounts.remove(&record.segment_id);
                        continue;
                    };
                    if segment.client_id != record.client_id
                        || segment.status != crate::proto::SegmentStatus::GracefullyUnmounting
                    {
                        drop(segment);
                        state.graceful_unmounts.remove(&record.segment_id);
                        continue;
                    }
                    drop(segment);
                    let _ = unmount_segment_owned_durable_locked(
                        &state,
                        record.segment_id,
                        record.client_id,
                        "graceful_unmount_completion",
                    );
                }
            }
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    /// Schedule an authoritative persisted epoch deadline.
    pub(crate) fn schedule_at(&self, segment_id: Uuid, client_id: Uuid, deadline_epoch_ms: u64) {
        let mut guard = self.inner.state.lock().expect("scheduler mutex poisoned");
        if guard.stopping {
            return;
        }
        guard.queue.push(GracefulUnmountRecord {
            segment_id,
            client_id,
            deadline_epoch_ms,
        });
        drop(guard);
        self.inner.condvar.notify_all(); // 唤醒 worker 线程重新计算等待时间 / Wake worker to recalculate wait
    }

    /// Replace volatile scheduling hints from the durable state map. This is
    /// used after snapshot restore and whenever a standby is promoted.
    pub(crate) fn sync_from_state(&self, state: &MasterState, active: bool) {
        let records = state
            .graceful_unmounts
            .iter()
            .map(|entry| GracefulUnmountRecord {
                segment_id: entry.segment_id,
                client_id: entry.client_id,
                deadline_epoch_ms: entry.deadline_epoch_ms,
            })
            .collect::<Vec<_>>();
        let mut guard = self.inner.state.lock().expect("scheduler mutex poisoned");
        if guard.stopping {
            return;
        }
        guard.queue.clear();
        guard.active = active;
        if active {
            guard.queue.extend(records);
        }
        drop(guard);
        self.inner.condvar.notify_all();
    }

    /// 停止调度线程：设置停止标志、唤醒、join 线程。
    /// Stop the scheduler thread: set stop flag, wake, join.
    pub(crate) fn stop(&mut self) {
        {
            let mut guard = self.inner.state.lock().expect("scheduler mutex poisoned");
            if guard.stopping {
                return;
            }
            guard.stopping = true;
        }
        self.inner.condvar.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn current_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

impl ProcessingReaper {
    /// 启动后台任务回收线程，周期性地清理超时的 offload/promotion/PutStart 任务。
    /// 使用 mpsc channel 实现可停止的周期性循环：stop() 时 drop sender 即中断。
    ///
    /// Start background task reaper thread; periodically cleans up expired offload/promotion tasks.
    /// Uses mpsc channel for stoppable periodic loop: drop sender to interrupt.
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let interval = state.runtime_config.reaper_interval;
        let worker = thread::spawn(move || {
            loop {
                match rx.recv_timeout(interval) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let Some(_background_mutation_guard) = state.begin_background_mutation()
                        else {
                            continue;
                        };
                        reap_expired_background_tasks(&state, Instant::now());
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

impl EvictionWorker {
    /// 启动后台驱逐线程，按 eviction_interval 间隔检查内存水位并触发自动驱逐。
    /// 使用 mpsc channel 实现可停止的周期性循环。
    ///
    /// Start background eviction thread; checks memory watermark at eviction_interval
    /// and triggers auto eviction. Uses mpsc channel for stoppable periodic loop.
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let interval = state.runtime_config.eviction_interval;
        let worker = thread::spawn(move || {
            loop {
                match rx.recv_timeout(interval) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let Some(_background_mutation_guard) = state.begin_background_mutation()
                        else {
                            continue;
                        };
                        let _ = run_automatic_eviction_once(&state);
                        let _ = run_automatic_nof_eviction_once(&state);
                        run_default_promotion_candidate_retry(&state);
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

/// 彻底清理一个过期的客户端：释放其所有副本、移除 segment 挂载、
/// 删除任务、清理 offload/promotion 队列，最后从 client 列表中移除。
/// 优先使用 O(client_keys) 的索引查找（client_objects），
/// 仅在索引缺失时回退到全表扫描（兼容旧客户端）。
///
/// Fully purge an expired client: release all its replicas, unmount its Memory
/// segments, delete its tasks, clean offload/promotion queues, and finally
/// remove it from the client list. NoF segments have an independent heartbeat
/// lifecycle and survive client expiry.
/// Prefers O(N_keys) index lookup via client_objects;
/// falls back to full scan only when the index is missing (legacy client compatibility).
fn purge_expired_client(state: &MasterState, metadata_state: &MetadataState, client_id: Uuid) {
    let _global_mutation_guard = state.key_mutations.lock_snapshot();
    state.client_objects.remove(&client_id);

    // 移除该 client 的所有待处理任务 / Remove all pending tasks assigned to this client
    let task_ids = state
        .tasks
        .iter()
        .filter(|entry| entry.info.assigned_client == Some(client_id))
        .map(|entry| *entry.key())
        .collect::<Vec<_>>();
    if !task_ids.is_empty()
        && state
            .persist_task_state_batch_or_fence(&[], &task_ids, "expired_client_remove_tasks")
            .is_err()
    {
        return;
    }
    for task_id in &task_ids {
        state.tasks.remove(task_id);
    }

    if let Some((_, storage_id)) = state.local_disk_client_sessions.remove(&client_id)
        && let Some(mut local_disk) = state.local_disk_segments.get_mut(&storage_id)
        && local_disk.active_client_id == Some(client_id)
    {
        local_disk.active_client_id = None;
        local_disk.recovery_complete = false;
        local_disk.recovery_session_id = None;
        local_disk.enable_offloading = false;
        local_disk.replace_reported_ssd_capacity(0);
        local_disk.recovered_objects.clear();
        local_disk.offloading_objects.clear();
        local_disk.promotion_objects.clear();
    }

    // 卸载该 client 拥有的所有常规 Memory segment / Unmount all Memory segments owned by this client
    let segment_ids = state
        .segments
        .iter()
        .filter(|entry| entry.client_id == client_id)
        .map(|entry| (entry.segment.id, entry.segment.name.clone()))
        .collect::<Vec<_>>();
    for (segment_id, segment_name) in segment_ids {
        if unmount_segment_owned_durable_locked(
            state,
            segment_id,
            client_id,
            "expired_client_unmount_memory",
        )
        .is_err()
        {
            return;
        }
        metadata_state.remove_node_blocking(&host_from_segment_name(&segment_name));
        let (ram_removed, rpc_removed) =
            metadata_state.remove_segment_metadata_blocking(&segment_name);
        tracing::info!(
            %client_id,
            %segment_name,
            ram_removed,
            rpc_removed,
            "cleaned expired client HTTP metadata"
        );
    }
    state.clients.remove(&client_id);
    // A client that has been fully purged must complete ReMount again before
    // Ping can report Ok. Keeping this tombstone would let a restarted client
    // skip topology reconstruction after all of its segments were removed.
    state.ok_clients.remove(&client_id);
    let alive_clients = get_alive_clients_snapshot(state);
    clear_invalid_handles_locked(state, &alive_clients);
    sync_client_segments(state, client_id);
}

impl ClientMonitorWorker {
    /// 启动客户端存活监控线程，按 client_monitor_interval 间隔扫描所有 client，
    /// 将超过 client_live_ttl 未心跳的客户端标记为过期并执行 purge_expired_client 清理。
    /// 使用 mpsc channel 实现可停止的周期性循环。
    ///
    /// Start client liveness monitor thread; scans all clients at client_monitor_interval,
    /// marks clients exceeding client_live_ttl without heartbeat as expired and runs
    /// purge_expired_client cleanup. Uses mpsc channel for stoppable periodic loop.
    pub(crate) fn new(state: Arc<MasterState>, metadata_state: MetadataState) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let interval = state.runtime_config.client_monitor_interval;
        let ttl = state.runtime_config.client_live_ttl;
        let worker = thread::spawn(move || {
            loop {
                match rx.recv_timeout(interval) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let Some(_background_mutation_guard) = state.begin_background_mutation()
                        else {
                            continue;
                        };
                        let now = SystemTime::now();
                        let expired = state
                            .clients
                            .iter()
                            .filter_map(|entry| {
                                now.duration_since(entry.last_ping)
                                    .ok()
                                    .filter(|elapsed| *elapsed >= ttl)
                                    .map(|_| *entry.key())
                            })
                            .collect::<Vec<_>>();
                        for client_id in expired {
                            purge_expired_client(&state, &metadata_state, client_id);
                        }
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

// ============================================================================
// DrainWorker — periodic drain job processor
// DrainWorker —— 周期性 drain 任务处理器
// ============================================================================

/// Periodically calls process_drain_jobs to refresh task statuses, retry
/// failed tasks, and mark completed drain jobs. Uses mpsc channel for
/// stoppable periodic loop.
///
/// C++ equivalent: JobDispatchThreadFunc in master_service.cpp:6934
///
/// 周期调用 process_drain_jobs 刷新任务状态、重试失败任务、标记完成。
/// 使用 mpsc channel 实现可停止的周期性循环。
pub(crate) struct DrainWorker {
    sender: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl DrainWorker {
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let worker = thread::spawn(move || {
            loop {
                match rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let Some(_background_mutation_guard) = state.begin_background_mutation()
                        else {
                            continue;
                        };
                        crate::service::background_ops::process_drain_jobs(&state);
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
    use crate::service::{GracefulUnmountSnapshotEntry, NoFSegmentEntry};

    fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(2));
        }
        predicate()
    }

    fn add_pending(state: &MasterState, deadline_epoch_ms: u64) -> (Uuid, Uuid) {
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        state.graceful_unmounts.insert(
            segment_id,
            GracefulUnmountSnapshotEntry {
                segment_id,
                client_id,
                deadline_epoch_ms,
            },
        );
        (segment_id, client_id)
    }

    #[test]
    fn graceful_unmount_scheduler_orders_deadlines_and_preempts_wait() {
        let mut queue = BinaryHeap::new();
        let ordered_ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        for (segment_id, deadline_epoch_ms) in ordered_ids.into_iter().zip([90, 30, 60]) {
            queue.push(GracefulUnmountRecord {
                segment_id,
                client_id: Uuid::new_v4(),
                deadline_epoch_ms,
            });
        }
        assert_eq!(queue.pop().unwrap().segment_id, ordered_ids[1]);
        assert_eq!(queue.pop().unwrap().segment_id, ordered_ids[2]);
        assert_eq!(queue.pop().unwrap().segment_id, ordered_ids[0]);

        let state = Arc::new(MasterState::empty());
        let mut scheduler = GracefulUnmountScheduler::new(state.clone());
        let now = current_epoch_millis();
        let (late_segment, late_client) = add_pending(&state, now + 500);
        scheduler.schedule_at(late_segment, late_client, now + 500);
        thread::sleep(Duration::from_millis(20));

        let (early_segment, early_client) = add_pending(&state, now + 60);
        scheduler.schedule_at(early_segment, early_client, now + 60);

        assert!(wait_until(Duration::from_millis(250), || !state
            .graceful_unmounts
            .contains_key(&early_segment)));
        assert!(state.graceful_unmounts.contains_key(&late_segment));
        scheduler.stop();
    }

    #[test]
    fn graceful_unmount_scheduler_sync_replaces_pending_queue() {
        let state = Arc::new(MasterState::empty());
        let mut scheduler = GracefulUnmountScheduler::new(state.clone());
        let removed_deadline = current_epoch_millis() + 200;
        let (removed_segment, removed_client) = add_pending(&state, removed_deadline);
        scheduler.schedule_at(removed_segment, removed_client, removed_deadline);
        let kept_deadline = current_epoch_millis() + 250;
        let (kept_segment, kept_client) = add_pending(&state, kept_deadline);
        scheduler.schedule_at(kept_segment, kept_client, kept_deadline);

        state.graceful_unmounts.remove(&removed_segment);
        scheduler.sync_from_state(&state, true);
        {
            let guard = scheduler
                .inner
                .state
                .lock()
                .expect("scheduler mutex poisoned");
            assert_eq!(guard.queue.len(), 1);
            assert_eq!(guard.queue.peek().unwrap().segment_id, kept_segment);
        }

        state.graceful_unmounts.clear();
        scheduler.sync_from_state(&state, true);

        let guard = scheduler
            .inner
            .state
            .lock()
            .expect("scheduler mutex poisoned");
        assert!(guard.queue.is_empty());
        drop(guard);
        scheduler.stop();
    }

    #[test]
    fn graceful_unmount_scheduler_stop_cancels_pending_and_is_idempotent() {
        let state = Arc::new(MasterState::empty());
        let mut scheduler = GracefulUnmountScheduler::new(state.clone());
        let deadline = current_epoch_millis() + 80;
        let (segment_id, client_id) = add_pending(&state, deadline);
        scheduler.schedule_at(segment_id, client_id, deadline);

        scheduler.stop();
        scheduler.stop();
        thread::sleep(Duration::from_millis(120));

        assert!(state.graceful_unmounts.contains_key(&segment_id));
    }

    #[test]
    fn graceful_unmount_scheduler_empty_sync_and_stop_are_noops() {
        let state = Arc::new(MasterState::empty());
        let mut scheduler = GracefulUnmountScheduler::new(state.clone());

        scheduler.sync_from_state(&state, true);
        scheduler.stop();
        scheduler.stop();

        assert!(state.graceful_unmounts.is_empty());
    }

    #[test]
    fn graceful_unmount_scheduler_runs_expired_then_accepts_more_work() {
        let state = Arc::new(MasterState::empty());
        let mut scheduler = GracefulUnmountScheduler::new(state.clone());
        let now = current_epoch_millis();
        let (future_segment, future_client) = add_pending(&state, now + 100);
        scheduler.schedule_at(future_segment, future_client, now + 100);
        let (expired_segment, expired_client) = add_pending(&state, now.saturating_sub(1));
        scheduler.schedule_at(expired_segment, expired_client, now.saturating_sub(1));

        assert!(wait_until(Duration::from_millis(50), || !state
            .graceful_unmounts
            .contains_key(&expired_segment)));
        assert!(state.graceful_unmounts.contains_key(&future_segment));
        assert!(wait_until(Duration::from_millis(250), || !state
            .graceful_unmounts
            .contains_key(&future_segment)));

        let next_deadline = current_epoch_millis();
        let (next_segment, next_client) = add_pending(&state, next_deadline);
        scheduler.schedule_at(next_segment, next_client, next_deadline);
        assert!(wait_until(Duration::from_millis(100), || !state
            .graceful_unmounts
            .contains_key(&next_segment)));
        scheduler.stop();
    }

    #[test]
    fn expired_client_cleanup_preserves_nof_segment_for_heartbeat_ownership() {
        let state = MasterState::empty();
        let client_id = Uuid::new_v4();
        let segment_id = Uuid::new_v4();
        state.nof_segments.insert(
            segment_id,
            NoFSegmentEntry {
                segment: mooncake_store_core::NoFSegment {
                    id: segment_id,
                    name: "nof-survives-client-expiry".into(),
                    base: 0x5000_0000_0,
                    size: 16 * 1024 * 1024,
                    te_endpoint: "nof-endpoint".into(),
                    client_id,
                },
                used: 0,
                status: crate::proto::SegmentStatus::Active,
            },
        );

        purge_expired_client(&state, &MetadataState::new("master"), client_id);

        assert!(state.nof_segments.contains_key(&segment_id));
    }
}

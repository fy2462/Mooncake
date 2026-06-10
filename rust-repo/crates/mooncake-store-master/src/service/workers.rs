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
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

use super::background_ops::{reap_expired_background_tasks, run_automatic_eviction_once};
use super::helpers::{
    clear_invalid_handles, get_alive_clients_snapshot, sync_client_segments,
    unmount_nof_segment_owned, unmount_segment_owned,
};
use super::state::{MasterState, NoFHeartbeatState};

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
    /// 过期时间，到达后执行实际卸载 / Expiry time; actual unmount happens after this.
    expire_at: Instant,
}

impl PartialEq for GracefulUnmountRecord {
    fn eq(&self, other: &Self) -> bool {
        self.segment_id == other.segment_id
            && self.client_id == other.client_id
            && self.expire_at == other.expire_at
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
        other.expire_at.cmp(&self.expire_at)
    }
}

/// 优雅卸载调度器的内部状态。
/// Inner state of the graceful unmount scheduler.
struct GracefulUnmountSchedulerState {
    /// 按过期时间排序的待卸载队列 / Queue of pending unmounts, ordered by expiry.
    queue: BinaryHeap<GracefulUnmountRecord>,
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
                stopping: false,
            }),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let worker = thread::spawn(move || loop {
            let mut guard = worker_inner.state.lock().expect("scheduler mutex poisoned");
            // 队列空时无限等待，有新记录加入时被 notify 唤醒
            // Wait indefinitely when queue is empty; woken by notify on new records
            while !guard.stopping && guard.queue.is_empty() {
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
            let now = Instant::now();
            if next.expire_at > now {
                let timeout = next.expire_at.saturating_duration_since(now);
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
            let now = Instant::now();
            while let Some(record) = guard.queue.peek().cloned() {
                if record.expire_at > now {
                    break;
                }
                expired.push(record);
                guard.queue.pop();
            }
            drop(guard); // 尽早释放锁，卸载操作可能耗时 / Release lock early; unmount may take time

            for record in expired {
                unmount_segment_owned(&state, record.segment_id, record.client_id);
            }
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    /// 安排 segment 在 grace_period_ms 毫秒后执行卸载。
    /// Schedule a segment to be unmounted after grace_period_ms milliseconds.
    pub(crate) fn schedule(&self, segment_id: Uuid, client_id: Uuid, grace_period_ms: u64) {
        let mut guard = self.inner.state.lock().expect("scheduler mutex poisoned");
        if guard.stopping {
            return;
        }
        guard.queue.push(GracefulUnmountRecord {
            segment_id,
            client_id,
            expire_at: Instant::now() + Duration::from_millis(grace_period_ms),
        });
        drop(guard);
        self.inner.condvar.notify_all(); // 唤醒 worker 线程重新计算等待时间 / Wake worker to recalculate wait
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

impl ProcessingReaper {
    /// 启动后台任务回收线程，周期性地清理超时的 offload/promotion/PutStart 任务。
    /// 使用 mpsc channel 实现可停止的周期性循环：stop() 时 drop sender 即中断。
    ///
    /// Start background task reaper thread; periodically cleans up expired offload/promotion tasks.
    /// Uses mpsc channel for stoppable periodic loop: drop sender to interrupt.
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let interval = state.runtime_config.reaper_interval;
        let worker = thread::spawn(move || loop {
            match rx.recv_timeout(interval) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    reap_expired_background_tasks(&state, Instant::now());
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
        let worker = thread::spawn(move || loop {
            match rx.recv_timeout(interval) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let _ = run_automatic_eviction_once(&state);
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
/// Fully purge an expired client: release all its replicas, unmount its segments,
/// delete its tasks, clean offload/promotion queues, and finally remove from client list.
/// Prefers O(N_keys) index lookup via client_objects;
/// falls back to full scan only when the index is missing (legacy client compatibility).
fn purge_expired_client(state: &MasterState, client_id: Uuid) {
    state.client_objects.remove(&client_id);

    // 移除该 client 的所有待处理任务 / Remove all pending tasks assigned to this client
    let task_ids = state
        .tasks
        .iter()
        .filter(|entry| entry.info.assigned_client == Some(client_id))
        .map(|entry| *entry.key())
        .collect::<Vec<_>>();
    for task_id in task_ids {
        state.tasks.remove(&task_id);
    }

    state.local_disk_segments.remove(&client_id);

    // 卸载该 client 拥有的所有 NOF segment / Unmount all NoF segments owned by this client
    let nof_segment_ids = state
        .nof_segments
        .iter()
        .filter(|entry| entry.segment.client_id == client_id)
        .map(|entry| entry.segment.id)
        .collect::<Vec<_>>();
    for segment_id in nof_segment_ids {
        unmount_nof_segment_owned(state, segment_id, client_id);
    }

    // 卸载该 client 拥有的所有常规 Memory segment / Unmount all Memory segments owned by this client
    let segment_ids = state
        .segments
        .iter()
        .filter(|entry| entry.client_id == client_id)
        .map(|entry| entry.segment.id)
        .collect::<Vec<_>>();
    for segment_id in segment_ids {
        unmount_segment_owned(state, segment_id, client_id);
    }
    state.clients.remove(&client_id);
    let alive_clients = get_alive_clients_snapshot(state);
    clear_invalid_handles(state, &alive_clients);
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
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let interval = state.runtime_config.client_monitor_interval;
        let ttl = state.runtime_config.client_live_ttl;
        let worker = thread::spawn(move || loop {
            match rx.recv_timeout(interval) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
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
                        purge_expired_client(&state, client_id);
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
        let worker = thread::spawn(move || loop {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    crate::service::background_ops::process_drain_jobs(&state);
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

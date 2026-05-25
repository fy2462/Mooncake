use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

use super::background_ops::{
    clear_offloading_task, clear_promotion_task, reap_expired_background_tasks,
    run_automatic_eviction_once,
};
use super::helpers::{
    client_id_by_replica_segment_name, release_replicas, sync_client_segments,
    unmount_nof_segment_owned, unmount_segment_owned,
};
use super::state::MasterState;

#[derive(Debug, Clone)]
struct GracefulUnmountRecord {
    segment_id: Uuid,
    client_id: Uuid,
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

impl Ord for GracefulUnmountRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        other.expire_at.cmp(&self.expire_at)
    }
}

struct GracefulUnmountSchedulerState {
    queue: BinaryHeap<GracefulUnmountRecord>,
    stopping: bool,
}

struct GracefulUnmountSchedulerInner {
    state: Mutex<GracefulUnmountSchedulerState>,
    condvar: Condvar,
}

pub(crate) struct GracefulUnmountScheduler {
    inner: Arc<GracefulUnmountSchedulerInner>,
    worker: Option<JoinHandle<()>>,
}

struct ProcessingReaperInner {
    state: Mutex<bool>,
    condvar: Condvar,
}

pub(crate) struct ProcessingReaper {
    inner: Arc<ProcessingReaperInner>,
    worker: Option<JoinHandle<()>>,
}

struct EvictionWorkerInner {
    state: Mutex<bool>,
    condvar: Condvar,
}

pub(crate) struct EvictionWorker {
    inner: Arc<EvictionWorkerInner>,
    worker: Option<JoinHandle<()>>,
}

struct ClientMonitorInner {
    state: Mutex<bool>,
    condvar: Condvar,
}

pub(crate) struct ClientMonitorWorker {
    inner: Arc<ClientMonitorInner>,
    worker: Option<JoinHandle<()>>,
}

impl GracefulUnmountScheduler {
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
            while !guard.stopping && guard.queue.is_empty() {
                guard = worker_inner
                    .condvar
                    .wait(guard)
                    .expect("scheduler condvar wait failed");
            }
            if guard.stopping {
                break;
            }

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
                    continue;
                }
            }

            let mut expired = Vec::new();
            let now = Instant::now();
            while let Some(record) = guard.queue.peek().cloned() {
                if record.expire_at > now {
                    break;
                }
                expired.push(record);
                guard.queue.pop();
            }
            drop(guard);

            for record in expired {
                unmount_segment_owned(&state, record.segment_id, record.client_id);
            }
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

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
        self.inner.condvar.notify_all();
    }

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
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let inner = Arc::new(ProcessingReaperInner {
            state: Mutex::new(false),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let interval = state.runtime_config.reaper_interval;
        let worker = thread::spawn(move || loop {
            let guard = worker_inner.state.lock().expect("reaper mutex poisoned");
            let (guard, _) = worker_inner
                .condvar
                .wait_timeout(guard, interval)
                .expect("reaper condvar timeout failed");
            if *guard {
                break;
            }
            drop(guard);
            reap_expired_background_tasks(&state, Instant::now());
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    pub(crate) fn stop(&mut self) {
        {
            let mut stopping = self.inner.state.lock().expect("reaper mutex poisoned");
            if *stopping {
                return;
            }
            *stopping = true;
        }
        self.inner.condvar.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl EvictionWorker {
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let inner = Arc::new(EvictionWorkerInner {
            state: Mutex::new(false),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let interval = state.runtime_config.eviction_interval;
        let worker = thread::spawn(move || loop {
            let guard = worker_inner.state.lock().expect("eviction mutex poisoned");
            let (guard, _) = worker_inner
                .condvar
                .wait_timeout(guard, interval)
                .expect("eviction condvar timeout failed");
            if *guard {
                break;
            }
            drop(guard);
            let _ = run_automatic_eviction_once(&state);
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    pub(crate) fn stop(&mut self) {
        {
            let mut stopping = self.inner.state.lock().expect("eviction mutex poisoned");
            if *stopping {
                return;
            }
            *stopping = true;
        }
        self.inner.condvar.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn purge_expired_client(state: &MasterState, client_id: Uuid) {
    let mut released_replicas = Vec::new();
    let mut emptied_keys = Vec::new();

    for mut object in state.objects.iter_mut() {
        let key = object.key().clone();
        let mut removed_any = false;
        object.replicas.retain(|replica| {
            let owner = replica
                .holder_client_id
                .or_else(|| client_id_by_replica_segment_name(state, &replica.segment_name));
            let keep = owner != Some(client_id);
            if !keep {
                removed_any = true;
                released_replicas.push(replica.clone());
            }
            keep
        });
        if removed_any && object.replicas.is_empty() {
            emptied_keys.push(key);
        }
    }

    if !released_replicas.is_empty() {
        release_replicas(state, &released_replicas);
    }

    for key in emptied_keys {
        state.objects.remove(&key);
        clear_offloading_task(state, &key);
        clear_promotion_task(state, &key);
        state.replication_tasks.remove(&key);
    }

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

    let nof_segment_ids = state
        .nof_segments
        .iter()
        .filter(|entry| entry.segment.client_id == client_id)
        .map(|entry| entry.segment.id)
        .collect::<Vec<_>>();
    for segment_id in nof_segment_ids {
        unmount_nof_segment_owned(state, segment_id, client_id);
    }

    let segment_ids = state
        .segments
        .iter()
        .filter(|entry| entry.segment.client_id == client_id)
        .map(|entry| entry.segment.id)
        .collect::<Vec<_>>();
    for segment_id in segment_ids {
        unmount_segment_owned(state, segment_id, client_id);
    }
    sync_client_segments(state, client_id);
    state.clients.remove(&client_id);
}

impl ClientMonitorWorker {
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        let inner = Arc::new(ClientMonitorInner {
            state: Mutex::new(false),
            condvar: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let interval = state.runtime_config.client_monitor_interval;
        let ttl = state.runtime_config.client_live_ttl;
        let worker = thread::spawn(move || loop {
            let guard = worker_inner
                .state
                .lock()
                .expect("client monitor mutex poisoned");
            let (guard, _) = worker_inner
                .condvar
                .wait_timeout(guard, interval)
                .expect("client monitor condvar timeout failed");
            if *guard {
                break;
            }
            drop(guard);

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
        });
        Self {
            inner,
            worker: Some(worker),
        }
    }

    pub(crate) fn stop(&mut self) {
        {
            let mut stopping = self
                .inner
                .state
                .lock()
                .expect("client monitor mutex poisoned");
            if *stopping {
                return;
            }
            *stopping = true;
        }
        self.inner.condvar.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

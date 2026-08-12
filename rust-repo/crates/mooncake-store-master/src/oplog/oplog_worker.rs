use super::oplog_wire::validate_record_size;
use super::{HaError, OpLogRecord, OpLogStore};
use crate::metrics;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

#[cfg(test)]
struct TestPauseState {
    reached: bool,
    released: bool,
}

#[cfg(test)]
pub(super) struct TestPause {
    state: std::sync::Mutex<TestPauseState>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl TestPause {
    pub(super) fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(TestPauseState {
                reached: false,
                released: false,
            }),
            changed: std::sync::Condvar::new(),
        }
    }

    pub(super) fn pause(&self) {
        let mut state = self.state.lock().unwrap();
        state.reached = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }

    pub(super) fn wait_until_reached(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap();
        while !state.reached {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, result) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if result.timed_out() && !state.reached {
                return false;
            }
        }
        true
    }

    pub(super) fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }
}

#[cfg(test)]
#[derive(Default)]
struct WorkerTestHooks {
    after_acceptance: Mutex<Option<Arc<TestPause>>>,
    before_flush_call: Mutex<Option<Arc<TestPause>>>,
    after_first_completion: Mutex<Option<Arc<TestPause>>>,
    before_worker_exit: Mutex<Option<Arc<TestPause>>>,
    after_shutdown_start: Mutex<Option<Arc<TestPause>>>,
    after_batch_completion: Mutex<Option<Arc<TestPause>>>,
}

#[cfg(test)]
fn run_test_pause(slot: &Mutex<Option<Arc<TestPause>>>) {
    if let Some(pause) = slot.lock().clone() {
        pause.pause();
    }
}

#[derive(Debug, Clone)]
pub struct OpLogWorkerConfig {
    pub batch_window: Duration,
    pub max_batch_records: usize,
    pub max_batch_payload_bytes: usize,
    pub queue_capacity: usize,
    pub completion_timeout: Duration,
}

impl Default for OpLogWorkerConfig {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_millis(10),
            max_batch_records: 100,
            max_batch_payload_bytes: 1_000_000,
            queue_capacity: 1_024,
            completion_timeout: Duration::from_secs(20),
        }
    }
}

struct PersistCommand {
    payload: String,
    producer_view_version: u64,
    operation: &'static str,
    submitted_at: Instant,
    completion: mpsc::SyncSender<Result<u64, HaError>>,
}

enum Command {
    Persist(PersistCommand),
    ReadSince {
        since_seq: u64,
        max_count: usize,
        completion: mpsc::SyncSender<Result<Vec<OpLogRecord>, HaError>>,
    },
    MaxSequenceId {
        completion: mpsc::SyncSender<Result<u64, HaError>>,
    },
    UpdateLatestSequenceId {
        sequence_id: u64,
        completion: mpsc::SyncSender<Result<(), HaError>>,
    },
    RecordSnapshotSequenceId {
        snapshot_id: String,
        sequence_id: u64,
        completion: mpsc::SyncSender<Result<(), HaError>>,
    },
    GetSnapshotSequenceId {
        snapshot_id: String,
        completion: mpsc::SyncSender<Result<u64, HaError>>,
    },
    CleanupBefore {
        before_sequence_id: u64,
        completion: mpsc::SyncSender<Result<(), HaError>>,
    },
    Shutdown,
}

#[derive(Clone)]
enum ShutdownState {
    NotStarted,
    Requested,
    Complete(Result<(), HaError>),
}

struct WorkerState {
    terminal: Option<HaError>,
    accepting: bool,
    backend_call_in_progress: bool,
    shutdown: ShutdownState,
}

struct WorkerControl {
    state: Mutex<WorkerState>,
    changed: Condvar,
}

pub(crate) struct SequencedOpLogWorker {
    sender: mpsc::SyncSender<Command>,
    latest_assigned: Arc<AtomicU64>,
    latest_committed: Arc<AtomicU64>,
    has_sequence_history: Arc<AtomicBool>,
    control: Arc<WorkerControl>,
    queue_depth: Arc<AtomicUsize>,
    completion_timeout: Duration,
    #[cfg(test)]
    test_hooks: Arc<WorkerTestHooks>,
}

#[derive(Clone, Copy)]
enum CompletionKind {
    Durable,
    Command(&'static str),
}

impl SequencedOpLogWorker {
    pub(crate) fn start(store: Box<dyn OpLogStore + Send>, config: OpLogWorkerConfig) -> Self {
        let initial_sequence = store.latest_sequence();
        let latest_assigned = Arc::new(AtomicU64::new(initial_sequence));
        let latest_committed = Arc::new(AtomicU64::new(initial_sequence));
        let has_sequence_history = Arc::new(AtomicBool::new(initial_sequence != 0));
        let control = Arc::new(WorkerControl {
            state: Mutex::new(WorkerState {
                terminal: None,
                accepting: true,
                backend_call_in_progress: false,
                shutdown: ShutdownState::NotStarted,
            }),
            changed: Condvar::new(),
        });
        let queue_depth = Arc::new(AtomicUsize::new(0));
        #[cfg(test)]
        let test_hooks = Arc::new(WorkerTestHooks::default());
        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);

        let thread_assigned = Arc::clone(&latest_assigned);
        let thread_committed = Arc::clone(&latest_committed);
        let thread_has_sequence_history = Arc::clone(&has_sequence_history);
        let thread_control = Arc::clone(&control);
        let thread_queue_depth = Arc::clone(&queue_depth);
        #[cfg(test)]
        let thread_test_hooks = Arc::clone(&test_hooks);
        let thread_config = config.clone();
        let spawn_result = std::thread::Builder::new()
            .name("mooncake-oplog-writer".to_string())
            .spawn(move || {
                run_worker(
                    store,
                    receiver,
                    thread_config,
                    thread_assigned,
                    thread_committed,
                    thread_has_sequence_history,
                    thread_control,
                    thread_queue_depth,
                    #[cfg(test)]
                    thread_test_hooks,
                );
            });
        if let Err(error) = spawn_result {
            install_poison(
                &control,
                HaError::InvalidBackend(format!("failed to start oplog writer thread: {error}")),
            );
        }

        Self {
            sender,
            latest_assigned,
            latest_committed,
            has_sequence_history,
            control,
            queue_depth,
            completion_timeout: config.completion_timeout,
            #[cfg(test)]
            test_hooks,
        }
    }

    pub(crate) fn submit_durable(
        &self,
        payload: String,
        producer_view_version: u64,
        operation: &'static str,
    ) -> Result<u64, HaError> {
        let record = OpLogRecord {
            seq: 0,
            producer_view_version,
            payload: payload.clone(),
        };
        validate_record_size(&record)?;
        self.enqueue_and_wait("persist", true, CompletionKind::Durable, |completion| {
            Command::Persist(PersistCommand {
                payload,
                producer_view_version,
                operation,
                submitted_at: Instant::now(),
                completion,
            })
        })
    }

    pub(crate) fn latest_assigned(&self) -> u64 {
        self.latest_assigned.load(Ordering::Acquire)
    }

    pub(crate) fn latest_committed(&self) -> u64 {
        self.latest_committed.load(Ordering::Acquire)
    }

    pub(crate) fn read_since(
        &self,
        since_seq: u64,
        max_count: usize,
    ) -> Result<Vec<OpLogRecord>, HaError> {
        self.enqueue_and_wait(
            "read_since",
            false,
            CompletionKind::Command("read_since"),
            |completion| Command::ReadSince {
                since_seq,
                max_count,
                completion,
            },
        )
    }

    pub(crate) fn max_sequence_id(&self) -> Result<u64, HaError> {
        self.enqueue_and_wait(
            "max_sequence_id",
            false,
            CompletionKind::Command("max_sequence_id"),
            |completion| Command::MaxSequenceId { completion },
        )
    }

    pub(crate) fn update_latest_sequence_id(&self, sequence_id: u64) -> Result<(), HaError> {
        self.enqueue_and_wait(
            "update_latest_sequence_id",
            false,
            CompletionKind::Command("update_latest_sequence_id"),
            |completion| Command::UpdateLatestSequenceId {
                sequence_id,
                completion,
            },
        )
    }

    pub(crate) fn record_snapshot_sequence_id(
        &self,
        snapshot_id: &str,
        sequence_id: u64,
    ) -> Result<(), HaError> {
        self.enqueue_and_wait(
            "record_snapshot_sequence_id",
            false,
            CompletionKind::Command("record_snapshot_sequence_id"),
            |completion| Command::RecordSnapshotSequenceId {
                snapshot_id: snapshot_id.to_string(),
                sequence_id,
                completion,
            },
        )
    }

    pub(crate) fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
        self.enqueue_and_wait(
            "get_snapshot_sequence_id",
            false,
            CompletionKind::Command("get_snapshot_sequence_id"),
            |completion| Command::GetSnapshotSequenceId {
                snapshot_id: snapshot_id.to_string(),
                completion,
            },
        )
    }

    pub(crate) fn cleanup_before(&self, before_sequence_id: u64) -> Result<(), HaError> {
        self.enqueue_and_wait(
            "cleanup_before",
            false,
            CompletionKind::Command("cleanup_before"),
            |completion| Command::CleanupBefore {
                before_sequence_id,
                completion,
            },
        )
    }

    #[cfg(test)]
    pub(super) fn queued_command_count_for_test(&self) -> usize {
        let _state = self.control.state.lock();
        self.queue_depth.load(Ordering::Acquire)
    }

    pub(crate) fn shutdown(&self) -> Result<(), HaError> {
        let started = Instant::now();
        let is_leader = {
            let mut state = self.control.state.lock();
            if let Some(error) = state.terminal.clone() {
                metrics::OPLOG_WRITER_POST_POISON_FAILURES.inc();
                return Err(error);
            }
            match &state.shutdown {
                ShutdownState::NotStarted => {
                    state.accepting = false;
                    state.shutdown = ShutdownState::Requested;
                    true
                }
                ShutdownState::Requested => false,
                ShutdownState::Complete(result) => return result.clone(),
            }
        };
        #[cfg(test)]
        if is_leader {
            run_test_pause(&self.test_hooks.after_shutdown_start);
        }

        if is_leader {
            loop {
                let send_result = {
                    let state = self.control.state.lock();
                    if let ShutdownState::Complete(result) = &state.shutdown {
                        return result.clone();
                    }
                    if let Some(error) = state.terminal.clone() {
                        return Err(error);
                    }
                    self.note_enqueue();
                    self.sender.try_send(Command::Shutdown)
                };
                match send_result {
                    Ok(()) => break,
                    Err(mpsc::TrySendError::Full(_)) => {
                        self.rollback_enqueue();
                        if started.elapsed() >= self.completion_timeout {
                            return Err(install_poison(
                                &self.control,
                                HaError::InvalidBackend(format!(
                                    "oplog writer shutdown timed out after {:?}",
                                    self.completion_timeout
                                )),
                            ));
                        }
                        std::thread::yield_now();
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        self.rollback_enqueue();
                        return Err(install_poison(
                            &self.control,
                            HaError::InvalidBackend(
                                "oplog writer disconnected during shutdown".into(),
                            ),
                        ));
                    }
                }
            }
        }

        let mut state = self.control.state.lock();
        loop {
            if let ShutdownState::Complete(result) = &state.shutdown {
                return result.clone();
            }
            let remaining = self.completion_timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                let error = install_poison_locked(
                    &self.control,
                    &mut state,
                    HaError::InvalidBackend(format!(
                        "oplog writer shutdown timed out after {:?}",
                        self.completion_timeout
                    )),
                );
                return Err(error);
            }
            self.control.changed.wait_for(&mut state, remaining);
        }
    }

    fn enqueue_and_wait<T>(
        &self,
        operation: &'static str,
        persist: bool,
        completion_kind: CompletionKind,
        build: impl FnOnce(mpsc::SyncSender<Result<T, HaError>>) -> Command,
    ) -> Result<T, HaError> {
        let (completion, result) = mpsc::sync_channel(1);
        {
            let mut state = self.control.state.lock();
            if let Some(error) = state.terminal.clone() {
                metrics::OPLOG_WRITER_POST_POISON_FAILURES.inc();
                return Err(error);
            }
            if !state.accepting {
                return Err(HaError::InvalidBackend("oplog writer is shut down".into()));
            }
            #[cfg(test)]
            run_test_pause(&self.test_hooks.after_acceptance);
            self.note_enqueue();
            match self.sender.try_send(build(completion)) {
                Ok(()) => {
                    if persist {
                        metrics::OPLOG_WRITER_SUBMITTED.inc();
                    }
                }
                Err(mpsc::TrySendError::Full(_)) => {
                    self.rollback_enqueue();
                    metrics::OPLOG_WRITER_QUEUE_REJECTIONS.inc();
                    return Err(HaError::InvalidBackend(format!(
                        "oplog writer queue is full for {operation}"
                    )));
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    self.rollback_enqueue();
                    if let Some(error) = state.terminal.clone() {
                        metrics::OPLOG_WRITER_POST_POISON_FAILURES.inc();
                        return Err(error);
                    }
                    let error = install_poison_locked(
                        &self.control,
                        &mut state,
                        HaError::InvalidBackend(format!(
                            "oplog writer channel disconnected while submitting {operation}"
                        )),
                    );
                    return Err(error);
                }
            }
        }

        match blocking_recv_timeout(&result, self.completion_timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let mut state = self.control.state.lock();
                if let Ok(result) = result.try_recv() {
                    return result;
                }
                if let Some(error) = state.terminal.clone() {
                    return Err(error);
                }
                let message = match completion_kind {
                    CompletionKind::Durable => format!(
                        "oplog durable completion timed out after {:?}",
                        self.completion_timeout
                    ),
                    CompletionKind::Command(label) => format!(
                        "oplog {label} completion timed out after {:?}",
                        self.completion_timeout
                    ),
                };
                Err(install_poison_locked(
                    &self.control,
                    &mut state,
                    HaError::InvalidBackend(message),
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let mut state = self.control.state.lock();
                if let Some(error) = state.terminal.clone() {
                    return Err(error);
                }
                Err(install_poison_locked(
                    &self.control,
                    &mut state,
                    HaError::InvalidBackend(format!(
                        "oplog {operation} completion channel disconnected"
                    )),
                ))
            }
        }
    }

    fn note_enqueue(&self) {
        self.queue_depth.fetch_add(1, Ordering::AcqRel);
        metrics::OPLOG_WRITER_QUEUE_DEPTH.inc();
    }

    fn rollback_enqueue(&self) {
        self.queue_depth.fetch_sub(1, Ordering::AcqRel);
        metrics::OPLOG_WRITER_QUEUE_DEPTH.dec();
    }
}

fn blocking_recv_timeout<T>(
    receiver: &mpsc::Receiver<T>,
    timeout: Duration,
) -> Result<T, mpsc::RecvTimeoutError> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) =>
        {
            tokio::task::block_in_place(|| receiver.recv_timeout(timeout))
        }
        _ => receiver.recv_timeout(timeout),
    }
}

fn install_poison(control: &WorkerControl, error: HaError) -> HaError {
    let mut state = control.state.lock();
    install_poison_locked(control, &mut state, error)
}

fn install_poison_locked(
    control: &WorkerControl,
    state: &mut WorkerState,
    error: HaError,
) -> HaError {
    if let Some(existing) = state.terminal.as_ref() {
        return existing.clone();
    }
    metrics::OPLOG_WRITER_POISON_EVENTS.inc();
    state.terminal = Some(error.clone());
    state.accepting = false;
    if matches!(state.shutdown, ShutdownState::Requested) {
        state.shutdown = ShutdownState::Complete(Err(error.clone()));
    }
    control.changed.notify_all();
    error
}

fn run_worker(
    mut store: Box<dyn OpLogStore + Send>,
    receiver: mpsc::Receiver<Command>,
    config: OpLogWorkerConfig,
    latest_assigned: Arc<AtomicU64>,
    latest_committed: Arc<AtomicU64>,
    has_sequence_history: Arc<AtomicBool>,
    control: Arc<WorkerControl>,
    queue_depth: Arc<AtomicUsize>,
    #[cfg(test)] test_hooks: Arc<WorkerTestHooks>,
) {
    let mut deferred = None;
    loop {
        {
            let state = control.state.lock();
            if let Some(error) = state.terminal.clone() {
                fail_deferred_and_queued(deferred.take(), &receiver, &queue_depth, &error);
                return;
            }
        }
        let first = match deferred.take() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => {
                    note_dequeued(&queue_depth);
                    command
                }
                Err(_) => return,
            },
        };
        let first = match first {
            Command::Persist(first) => first,
            Command::ReadSince {
                since_seq,
                max_count,
                completion,
            } => {
                if finish_backend_command(
                    execute_backend_call(&control, || store.read_since(since_seq, max_count)),
                    completion,
                    &control,
                    &receiver,
                    &queue_depth,
                ) {
                    return;
                }
                continue;
            }
            Command::MaxSequenceId { completion } => {
                if finish_backend_command(
                    execute_backend_call(&control, || store.max_sequence_id()),
                    completion,
                    &control,
                    &receiver,
                    &queue_depth,
                ) {
                    return;
                }
                continue;
            }
            Command::UpdateLatestSequenceId {
                sequence_id,
                completion,
            } => {
                let result = if has_sequence_history.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    execute_backend_call(&control, || store.update_latest_sequence_id(sequence_id))
                        .map(|()| {
                            latest_assigned.store(sequence_id, Ordering::Release);
                            latest_committed.store(sequence_id, Ordering::Release);
                            if sequence_id != 0 {
                                has_sequence_history.store(true, Ordering::Release);
                            }
                        })
                };
                if finish_backend_command(result, completion, &control, &receiver, &queue_depth) {
                    return;
                }
                continue;
            }
            Command::RecordSnapshotSequenceId {
                snapshot_id,
                sequence_id,
                completion,
            } => {
                if finish_backend_command(
                    execute_backend_call(&control, || {
                        store.record_snapshot_sequence_id(&snapshot_id, sequence_id)
                    }),
                    completion,
                    &control,
                    &receiver,
                    &queue_depth,
                ) {
                    return;
                }
                continue;
            }
            Command::GetSnapshotSequenceId {
                snapshot_id,
                completion,
            } => {
                if finish_backend_command(
                    execute_backend_call(&control, || store.get_snapshot_sequence_id(&snapshot_id)),
                    completion,
                    &control,
                    &receiver,
                    &queue_depth,
                ) {
                    return;
                }
                continue;
            }
            Command::CleanupBefore {
                before_sequence_id,
                completion,
            } => {
                if finish_backend_command(
                    execute_backend_call(&control, || store.cleanup_before(before_sequence_id)),
                    completion,
                    &control,
                    &receiver,
                    &queue_depth,
                ) {
                    return;
                }
                continue;
            }
            Command::Shutdown => {
                let mut state = control.state.lock();
                #[cfg(test)]
                run_test_pause(&test_hooks.before_worker_exit);
                let result = state.terminal.clone().map_or(Ok(()), Err);
                state.shutdown = ShutdownState::Complete(result);
                control.changed.notify_all();
                return;
            }
        };
        let deadline = Instant::now() + config.batch_window;
        let mut payload_bytes = first.payload.len();
        metrics::OPLOG_WRITER_QUEUE_WAIT_US
            .observe(first.submitted_at.elapsed().as_micros() as f64);
        let mut batch = vec![first];

        while batch.len() < config.max_batch_records.max(1) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let command = match receiver.recv_timeout(remaining) {
                Ok(command) => {
                    note_dequeued(&queue_depth);
                    command
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            match command {
                Command::Persist(candidate) => {
                    if payload_bytes.saturating_add(candidate.payload.len())
                        > config.max_batch_payload_bytes
                    {
                        deferred = Some(Command::Persist(candidate));
                        break;
                    }
                    payload_bytes += candidate.payload.len();
                    metrics::OPLOG_WRITER_QUEUE_WAIT_US
                        .observe(candidate.submitted_at.elapsed().as_micros() as f64);
                    batch.push(candidate);
                }
                command => {
                    deferred = Some(command);
                    break;
                }
            }
        }

        metrics::OPLOG_WRITER_BATCH_RECORDS.observe(batch.len() as f64);
        let mut sequences = Vec::with_capacity(batch.len());
        let mut batch_error = None;
        for command in &batch {
            match execute_backend_call(&control, || {
                store.append(&OpLogRecord {
                    seq: 0,
                    producer_view_version: command.producer_view_version,
                    payload: command.payload.clone(),
                })
            }) {
                Ok(sequence_id) => {
                    has_sequence_history.store(true, Ordering::Release);
                    let expected = latest_assigned
                        .load(Ordering::Acquire)
                        .checked_add(1)
                        .ok_or_else(|| {
                            HaError::InvalidBackend(
                                "oplog writer sequence exhausted at u64::MAX".into(),
                            )
                        });
                    let expected = match expected {
                        Ok(expected) => expected,
                        Err(error) => {
                            batch_error = Some(install_poison(&control, error));
                            break;
                        }
                    };
                    if sequence_id != expected {
                        batch_error = Some(install_poison(
                            &control,
                            HaError::InvalidBackend(format!(
                                "oplog writer expected sequence {expected} but backend assigned {sequence_id}"
                            )),
                        ));
                        break;
                    }
                    latest_assigned.store(sequence_id, Ordering::Release);
                    sequences.push(sequence_id);
                }
                Err(error) => {
                    batch_error = Some(error);
                    break;
                }
            }
        }
        if batch_error.is_none() {
            #[cfg(test)]
            run_test_pause(&test_hooks.before_flush_call);
            if let Err(error) = execute_backend_call(&control, || store.flush_durable()) {
                batch_error = Some(error);
            } else if let Some(sequence_id) = sequences.last().copied() {
                latest_committed.store(sequence_id, Ordering::Release);
            }
        }

        #[cfg(test)]
        let batch_len = batch.len();
        let state = control.state.lock();
        if let Some(error) = state.terminal.clone() {
            batch_error = Some(error);
        }
        for (index, command) in batch.into_iter().enumerate() {
            let _ = command.operation;
            metrics::OPLOG_WRITER_DURABLE_WAIT_US
                .observe(command.submitted_at.elapsed().as_micros() as f64);
            let result = match &batch_error {
                Some(error) => Err(error.clone()),
                None => Ok(sequences[index]),
            };
            let _ = command.completion.send(result);
            #[cfg(test)]
            if index == 0 && batch_len > 1 {
                run_test_pause(&test_hooks.after_first_completion);
            }
        }
        #[cfg(test)]
        run_test_pause(&test_hooks.after_batch_completion);
        if let Some(error) = batch_error {
            fail_deferred_and_queued(deferred.take(), &receiver, &queue_depth, &error);
            return;
        }
        drop(state);
    }
}

fn execute_backend_call<T>(
    control: &WorkerControl,
    operation: impl FnOnce() -> Result<T, HaError>,
) -> Result<T, HaError> {
    {
        let mut state = control.state.lock();
        if let Some(error) = state.terminal.clone() {
            return Err(error);
        }
        state.backend_call_in_progress = true;
    }

    let result = operation();
    let mut state = control.state.lock();
    state.backend_call_in_progress = false;
    match result {
        Ok(value) => match state.terminal.clone() {
            Some(error) => Err(error),
            None => Ok(value),
        },
        Err(error) => Err(install_poison_locked(control, &mut state, error)),
    }
}

fn note_dequeued(queue_depth: &AtomicUsize) {
    queue_depth.fetch_sub(1, Ordering::AcqRel);
    metrics::OPLOG_WRITER_QUEUE_DEPTH.dec();
}

fn finish_backend_command<T>(
    result: Result<T, HaError>,
    completion: mpsc::SyncSender<Result<T, HaError>>,
    control: &WorkerControl,
    receiver: &mpsc::Receiver<Command>,
    queue_depth: &AtomicUsize,
) -> bool {
    let mut state = control.state.lock();
    let result = match (state.terminal.clone(), result) {
        (Some(error), _) => Err(error),
        (None, result) => result,
    };
    match result {
        Ok(value) => {
            let _ = completion.send(Ok(value));
            false
        }
        Err(error) => {
            let error = install_poison_locked(control, &mut state, error);
            let _ = completion.send(Err(error.clone()));
            fail_deferred_and_queued(None, receiver, queue_depth, &error);
            true
        }
    }
}

fn fail_deferred_and_queued(
    deferred: Option<Command>,
    receiver: &mpsc::Receiver<Command>,
    queue_depth: &AtomicUsize,
    error: &HaError,
) {
    if let Some(command) = deferred {
        fail_command(command, error);
    }
    while let Ok(command) = receiver.try_recv() {
        note_dequeued(queue_depth);
        fail_command(command, error);
    }
}

fn fail_command(command: Command, error: &HaError) {
    match command {
        Command::Persist(command) => {
            metrics::OPLOG_WRITER_DURABLE_WAIT_US
                .observe(command.submitted_at.elapsed().as_micros() as f64);
            let _ = command.completion.send(Err(error.clone()));
        }
        Command::ReadSince { completion, .. } => {
            let _ = completion.send(Err(error.clone()));
        }
        Command::MaxSequenceId { completion }
        | Command::GetSnapshotSequenceId { completion, .. } => {
            let _ = completion.send(Err(error.clone()));
        }
        Command::UpdateLatestSequenceId { completion, .. }
        | Command::RecordSnapshotSequenceId { completion, .. }
        | Command::CleanupBefore { completion, .. } => {
            let _ = completion.send(Err(error.clone()));
        }
        Command::Shutdown => {}
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::{OpLogWorkerConfig, SequencedOpLogWorker, TestPause};
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
    use std::time::{Duration, Instant};

    struct CountingGateState {
        append_calls: usize,
        flush_calls: usize,
        flush_open: bool,
        committed_records: Vec<OpLogRecord>,
    }

    struct CountingGateStore {
        inner: InMemoryOpLog,
        state: Arc<(Mutex<CountingGateState>, Condvar)>,
        pending_records: Vec<OpLogRecord>,
        flush_error: Option<HaError>,
        returned_sequence_offset: u64,
    }

    impl CountingGateStore {
        fn new(
            flush_open: bool,
            flush_error: Option<HaError>,
            returned_sequence_offset: u64,
        ) -> (Self, Arc<(Mutex<CountingGateState>, Condvar)>) {
            let state = Arc::new((
                Mutex::new(CountingGateState {
                    append_calls: 0,
                    flush_calls: 0,
                    flush_open,
                    committed_records: Vec::new(),
                }),
                Condvar::new(),
            ));
            (
                Self {
                    inner: InMemoryOpLog::new(100),
                    state: Arc::clone(&state),
                    pending_records: Vec::new(),
                    flush_error,
                    returned_sequence_offset,
                },
                state,
            )
        }
    }

    impl OpLogStore for CountingGateStore {
        fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
            let sequence_id = self.inner.append(entry)?;
            self.pending_records.push(OpLogRecord {
                seq: sequence_id,
                ..entry.clone()
            });
            self.state.0.lock().unwrap().append_calls += 1;
            Ok(sequence_id.saturating_add(self.returned_sequence_offset))
        }

        fn read_since(
            &self,
            since_seq: u64,
            max_count: usize,
        ) -> Result<Vec<OpLogRecord>, HaError> {
            self.inner.read_since(since_seq, max_count)
        }

        fn latest_sequence(&self) -> u64 {
            self.inner.latest_sequence()
        }

        fn max_sequence_id(&self) -> Result<u64, HaError> {
            self.inner.max_sequence_id()
        }

        fn update_latest_sequence_id(&mut self, sequence_id: u64) -> Result<(), HaError> {
            self.inner.update_latest_sequence_id(sequence_id)
        }

        fn record_snapshot_sequence_id(
            &mut self,
            snapshot_id: &str,
            sequence_id: u64,
        ) -> Result<(), HaError> {
            self.inner
                .record_snapshot_sequence_id(snapshot_id, sequence_id)
        }

        fn get_snapshot_sequence_id(&self, snapshot_id: &str) -> Result<u64, HaError> {
            self.inner.get_snapshot_sequence_id(snapshot_id)
        }

        fn cleanup_before(&mut self, before_sequence_id: u64) -> Result<(), HaError> {
            self.inner.cleanup_before(before_sequence_id)
        }

        fn flush_durable(&mut self) -> Result<(), HaError> {
            let (lock, changed) = &*self.state;
            let mut state = lock.lock().unwrap();
            state.flush_calls += 1;
            changed.notify_all();
            while !state.flush_open {
                state = changed.wait(state).unwrap();
            }
            if let Some(error) = self.flush_error.take() {
                return Err(error);
            }
            state.committed_records.append(&mut self.pending_records);
            Ok(())
        }

        fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
            self.inner.poll_from(since_seq, max_count)
        }
    }

    fn test_config() -> OpLogWorkerConfig {
        OpLogWorkerConfig {
            batch_window: Duration::from_millis(50),
            max_batch_records: 100,
            max_batch_payload_bytes: 1_000_000,
            queue_capacity: 16,
            completion_timeout: Duration::from_secs(2),
        }
    }

    fn wait_for_flush_calls(state: &Arc<(Mutex<CountingGateState>, Condvar)>, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        let (lock, changed) = &**state;
        let mut guard = lock.lock().unwrap();
        while guard.flush_calls < expected {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "worker never entered flush_durable");
            let (next_guard, timeout) = changed.wait_timeout(guard, remaining).unwrap();
            guard = next_guard;
            assert!(!timeout.timed_out(), "worker never entered flush_durable");
        }
    }

    fn open_flush_gate(state: &Arc<(Mutex<CountingGateState>, Condvar)>) {
        let (lock, changed) = &**state;
        lock.lock().unwrap().flush_open = true;
        changed.notify_all();
    }

    fn wait_for_queue_depth(worker: &SequencedOpLogWorker, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while worker.queue_depth.load(Ordering::Acquire) != expected {
            assert!(
                Instant::now() < deadline,
                "writer queue depth never reached {expected}"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn concurrent_durable_commands_share_one_flush_and_get_consecutive_sequences() {
        let (store, state) = CountingGateStore::new(false, None, 0);
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), test_config()));
        let barrier = Arc::new(Barrier::new(9));
        let (result_tx, result_rx) = mpsc::channel();
        let mut threads = Vec::new();

        for index in 0..8 {
            let worker = Arc::clone(&worker);
            let barrier = Arc::clone(&barrier);
            let result_tx = result_tx.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                let result = worker.submit_durable(format!("payload-{index}"), 7, "test");
                result_tx.send(result).unwrap();
            }));
        }
        drop(result_tx);
        barrier.wait();

        let deadline = Instant::now() + Duration::from_secs(1);
        let (lock, changed) = &*state;
        let mut guard = lock.lock().unwrap();
        while guard.flush_calls == 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "worker never entered flush_durable");
            let (next_guard, timeout) = changed.wait_timeout(guard, remaining).unwrap();
            guard = next_guard;
            assert!(!timeout.timed_out(), "worker never entered flush_durable");
        }
        assert_eq!(guard.flush_calls, 1);
        assert!(matches!(
            result_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        guard.flush_open = true;
        changed.notify_all();
        drop(guard);

        let mut sequences = Vec::new();
        for result in result_rx {
            sequences.push(result.unwrap());
        }
        for thread in threads {
            thread.join().unwrap();
        }
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=8).collect::<Vec<_>>());
        assert_eq!(worker.latest_assigned(), 8);
        assert_eq!(worker.latest_committed(), 8);

        let guard = lock.lock().unwrap();
        assert_eq!(guard.append_calls, 8);
        assert_eq!(guard.flush_calls, 1);
        assert_eq!(guard.committed_records.len(), 8);
    }

    #[test]
    fn failed_batch_poisons_all_waiters_and_prevents_later_backend_calls() {
        let injected = HaError::InvalidBackend("injected ambiguous flush".into());
        let (store, state) = CountingGateStore::new(true, Some(injected.clone()), 0);
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), test_config()));
        let barrier = Arc::new(Barrier::new(5));
        let mut threads = Vec::new();

        for index in 0..4 {
            let worker = Arc::clone(&worker);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                worker.submit_durable(format!("poison-{index}"), 9, "test")
            }));
        }
        barrier.wait();

        for thread in threads {
            assert_eq!(thread.join().unwrap(), Err(injected.clone()));
        }
        let before = {
            let guard = state.0.lock().unwrap();
            (guard.append_calls, guard.flush_calls)
        };
        assert_eq!(
            worker.submit_durable("after-poison".into(), 9, "test"),
            Err(injected)
        );
        let guard = state.0.lock().unwrap();
        assert_eq!((guard.append_calls, guard.flush_calls), before);
    }

    #[test]
    fn full_queue_rejects_immediately_and_never_blocks_sender() {
        let (store, state) = CountingGateStore::new(false, None, 0);
        let mut config = test_config();
        config.queue_capacity = 1;
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), config));

        let first_worker = Arc::clone(&worker);
        let first =
            std::thread::spawn(move || first_worker.submit_durable("first".into(), 1, "test"));
        wait_for_flush_calls(&state, 1);

        let second_worker = Arc::clone(&worker);
        let second =
            std::thread::spawn(move || second_worker.submit_durable("second".into(), 1, "test"));
        wait_for_queue_depth(&worker, 1);

        let third_worker = Arc::clone(&worker);
        let (third_tx, third_rx) = mpsc::sync_channel(1);
        let started = Instant::now();
        let third = std::thread::spawn(move || {
            let result = third_worker.submit_durable("third".into(), 1, "test");
            third_tx.send(result).unwrap();
        });
        let third_result = third_rx.recv_timeout(Duration::from_millis(100));
        open_flush_gate(&state);

        let third_result = third_result.expect("full queue blocked the third sender");
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(
            third_result
                .unwrap_err()
                .to_string()
                .contains("oplog writer queue is full")
        );
        assert!(first.join().unwrap().is_ok());
        assert!(second.join().unwrap().is_ok());
        third.join().unwrap();
    }

    #[test]
    fn shutdown_is_bounded_and_rejects_new_submissions() {
        let (store, state) = CountingGateStore::new(true, None, 0);
        let mut config = test_config();
        config.completion_timeout = Duration::from_millis(200);
        let worker = SequencedOpLogWorker::start(Box::new(store), config);

        let started = Instant::now();
        assert_eq!(worker.shutdown(), Ok(()));
        assert!(started.elapsed() < Duration::from_millis(200));
        let error = worker
            .submit_durable("after-shutdown".into(), 1, "test")
            .unwrap_err();
        assert!(error.to_string().contains("oplog writer is shut down"));
        let guard = state.0.lock().unwrap();
        assert_eq!((guard.append_calls, guard.flush_calls), (0, 0));
    }

    #[test]
    fn caller_timeout_poison_prevents_an_already_queued_backend_mutation() {
        let (store, state) = CountingGateStore::new(false, None, 0);
        let mut config = test_config();
        config.completion_timeout = Duration::from_millis(100);
        config.queue_capacity = 2;
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), config));

        let first_worker = Arc::clone(&worker);
        let first = std::thread::spawn(move || {
            first_worker.submit_durable("timeout-first".into(), 3, "test")
        });
        wait_for_flush_calls(&state, 1);

        let second_worker = Arc::clone(&worker);
        let second = std::thread::spawn(move || {
            second_worker.submit_durable("queued-second".into(), 3, "test")
        });
        wait_for_queue_depth(&worker, 1);
        let timeout_error = first.join().unwrap().unwrap_err();
        assert!(
            timeout_error
                .to_string()
                .contains("oplog durable completion timed out after")
        );
        open_flush_gate(&state);

        assert_eq!(second.join().unwrap(), Err(timeout_error));
        let guard = state.0.lock().unwrap();
        assert_eq!((guard.append_calls, guard.flush_calls), (1, 1));
    }

    #[test]
    fn fix_round_one_timeout_before_flush_decision_prevents_flush_call() {
        let (store, state) = CountingGateStore::new(true, None, 0);
        let mut config = test_config();
        config.batch_window = Duration::ZERO;
        config.completion_timeout = Duration::from_millis(80);
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), config));
        let pause = Arc::new(TestPause::new());
        let completed = Arc::new(TestPause::new());
        *worker.test_hooks.before_flush_call.lock() = Some(Arc::clone(&pause));
        *worker.test_hooks.after_batch_completion.lock() = Some(Arc::clone(&completed));

        let submit_worker = Arc::clone(&worker);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let started = Instant::now();
        let submitter = std::thread::spawn(move || {
            result_tx
                .send(submit_worker.submit_durable("pre-flush-timeout".into(), 1, "test"))
                .unwrap();
        });
        assert!(pause.wait_until_reached(Duration::from_secs(1)));
        let result = result_rx.recv_timeout(Duration::from_millis(200));
        let elapsed = started.elapsed();
        pause.release();
        assert!(completed.wait_until_reached(Duration::from_secs(1)));
        submitter.join().unwrap();

        assert!(elapsed < Duration::from_millis(200));
        assert!(
            result
                .expect("pre-flush timeout exceeded its completion deadline")
                .unwrap_err()
                .to_string()
                .contains("oplog durable completion timed out after")
        );
        let guard = state.0.lock().unwrap();
        assert_eq!(guard.flush_calls, 0);
        drop(guard);
        completed.release();
    }

    #[test]
    fn fix_round_one_timeout_during_flush_is_bounded_and_fans_out_one_error() {
        let (store, state) = CountingGateStore::new(false, None, 0);
        let mut config = test_config();
        config.batch_window = Duration::from_millis(20);
        config.completion_timeout = Duration::from_millis(80);
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), config));
        let barrier = Arc::new(Barrier::new(3));
        let (result_tx, result_rx) = mpsc::channel();
        let mut submitters = Vec::new();

        for index in 0..2 {
            let worker = Arc::clone(&worker);
            let barrier = Arc::clone(&barrier);
            let result_tx = result_tx.clone();
            submitters.push(std::thread::spawn(move || {
                barrier.wait();
                result_tx
                    .send(worker.submit_durable(format!("flush-timeout-{index}"), 2, "test"))
                    .unwrap();
            }));
        }
        drop(result_tx);
        barrier.wait();
        wait_for_flush_calls(&state, 1);
        let started = Instant::now();
        let first = result_rx.recv_timeout(Duration::from_millis(200));
        let second = result_rx.recv_timeout(Duration::from_millis(200));
        let elapsed = started.elapsed();
        open_flush_gate(&state);
        for submitter in submitters {
            submitter.join().unwrap();
        }

        assert!(elapsed < Duration::from_millis(200));
        let first = first
            .expect("first flush waiter exceeded its completion deadline")
            .unwrap_err();
        let second = second
            .expect("second flush waiter exceeded its completion deadline")
            .unwrap_err();
        assert_eq!(first, second);
        assert!(
            first
                .to_string()
                .contains("oplog durable completion timed out after")
        );
    }

    #[test]
    fn fix_round_one_timeout_during_fanout_cannot_split_batch_outcome() {
        let (store, _state) = CountingGateStore::new(true, None, 0);
        let mut config = test_config();
        config.batch_window = Duration::from_millis(20);
        config.completion_timeout = Duration::from_millis(150);
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), config));
        let pause = Arc::new(TestPause::new());
        *worker.test_hooks.after_first_completion.lock() = Some(Arc::clone(&pause));
        let barrier = Arc::new(Barrier::new(3));
        let (result_tx, result_rx) = mpsc::channel();
        let mut submitters = Vec::new();

        for index in 0..2 {
            let worker = Arc::clone(&worker);
            let barrier = Arc::clone(&barrier);
            let result_tx = result_tx.clone();
            submitters.push(std::thread::spawn(move || {
                barrier.wait();
                result_tx
                    .send(worker.submit_durable(format!("fanout-{index}"), 3, "test"))
                    .unwrap();
            }));
        }
        drop(result_tx);
        barrier.wait();
        assert!(pause.wait_until_reached(Duration::from_secs(1)));

        let first = result_rx.recv_timeout(Duration::from_millis(50)).unwrap();
        let early_second = result_rx.recv_timeout(Duration::from_millis(200));
        let second_was_late = matches!(&early_second, Err(mpsc::RecvTimeoutError::Timeout));
        pause.release();
        let second = match early_second {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                result_rx.recv_timeout(Duration::from_secs(1)).unwrap()
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("fanout result channel disconnected")
            }
        };
        for submitter in submitters {
            submitter.join().unwrap();
        }

        assert!(second_was_late);
        assert!(first.is_ok());
        assert!(second.is_ok());
    }

    #[test]
    fn fix_round_one_admission_race_with_worker_exit_balances_queue_depth() {
        let (store, _state) = CountingGateStore::new(true, None, 0);
        let mut config = test_config();
        config.completion_timeout = Duration::from_millis(500);
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), config));
        let acceptance_pause = Arc::new(TestPause::new());
        let exit_pause = Arc::new(TestPause::new());
        *worker.test_hooks.after_acceptance.lock() = Some(Arc::clone(&acceptance_pause));
        *worker.test_hooks.before_worker_exit.lock() = Some(Arc::clone(&exit_pause));

        let producer_worker = Arc::clone(&worker);
        let producer = std::thread::spawn(move || {
            producer_worker.submit_durable("admission-race".into(), 4, "test")
        });
        assert!(acceptance_pause.wait_until_reached(Duration::from_secs(1)));

        let shutdown_worker = Arc::clone(&worker);
        let shutdown = std::thread::spawn(move || shutdown_worker.shutdown());
        let exited_early = exit_pause.wait_until_reached(Duration::from_millis(200));
        acceptance_pause.release();
        assert!(exited_early || exit_pause.wait_until_reached(Duration::from_secs(1)));
        exit_pause.release();

        assert!(producer.join().unwrap().is_ok());
        assert_eq!(shutdown.join().unwrap(), Ok(()));
        assert_eq!(worker.queue_depth.load(Ordering::Acquire), 0);
    }

    #[test]
    fn fix_round_one_concurrent_shutdown_callers_wait_for_same_result() {
        let (store, state) = CountingGateStore::new(false, None, 0);
        let mut config = test_config();
        config.completion_timeout = Duration::from_millis(500);
        let worker = Arc::new(SequencedOpLogWorker::start(Box::new(store), config));
        let shutdown_pause = Arc::new(TestPause::new());
        *worker.test_hooks.after_shutdown_start.lock() = Some(Arc::clone(&shutdown_pause));

        let submit_worker = Arc::clone(&worker);
        let submitter = std::thread::spawn(move || {
            submit_worker.submit_durable("active-before-shutdown".into(), 5, "test")
        });
        wait_for_flush_calls(&state, 1);

        let (result_tx, result_rx) = mpsc::channel();
        let first_worker = Arc::clone(&worker);
        let first_tx = result_tx.clone();
        let first = std::thread::spawn(move || first_tx.send(first_worker.shutdown()).unwrap());
        assert!(shutdown_pause.wait_until_reached(Duration::from_secs(1)));

        let second_worker = Arc::clone(&worker);
        let second = std::thread::spawn(move || result_tx.send(second_worker.shutdown()).unwrap());
        let early = result_rx.recv_timeout(Duration::from_millis(100));
        shutdown_pause.release();
        open_flush_gate(&state);

        let mut results = Vec::new();
        if let Ok(result) = early.as_ref() {
            results.push(result.clone());
        }
        while results.len() < 2 {
            results.push(result_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        }
        first.join().unwrap();
        second.join().unwrap();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        assert_eq!(results, vec![Ok(()), Ok(())]);
        assert!(submitter.join().unwrap().is_ok());
    }

    #[test]
    fn query_and_admin_commands_execute_on_the_worker_backend() {
        let worker = SequencedOpLogWorker::start(Box::new(InMemoryOpLog::new(100)), test_config());
        assert_eq!(worker.submit_durable("one".into(), 5, "test"), Ok(1));
        assert_eq!(worker.submit_durable("two".into(), 5, "test"), Ok(2));
        assert_eq!(worker.max_sequence_id(), Ok(2));
        assert_eq!(
            worker
                .read_since(1, 10)
                .unwrap()
                .into_iter()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            vec!["one", "two"]
        );
        assert_eq!(worker.record_snapshot_sequence_id("snapshot-1", 2), Ok(()));
        assert_eq!(worker.get_snapshot_sequence_id("snapshot-1"), Ok(2));
        assert_eq!(worker.cleanup_before(2), Ok(()));
        assert_eq!(
            worker
                .read_since(1, 10)
                .unwrap()
                .into_iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn updating_initial_sequence_changes_the_next_assigned_sequence() {
        let worker = SequencedOpLogWorker::start(Box::new(InMemoryOpLog::new(100)), test_config());
        assert_eq!(worker.update_latest_sequence_id(40), Ok(()));
        assert_eq!(worker.update_latest_sequence_id(50), Ok(()));
        assert_eq!(worker.max_sequence_id(), Ok(40));
        assert_eq!(worker.latest_assigned(), 40);
        assert_eq!(worker.latest_committed(), 40);
        assert_eq!(worker.submit_durable("after-40".into(), 6, "test"), Ok(41));
    }

    #[test]
    fn non_contiguous_backend_sequence_poisons_without_flushing() {
        let (store, state) = CountingGateStore::new(true, None, 1);
        let worker = SequencedOpLogWorker::start(Box::new(store), test_config());

        let error = worker
            .submit_durable("wrong-sequence".into(), 4, "test")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("oplog writer expected sequence 1 but backend assigned 2")
        );
        assert_eq!(worker.latest_assigned(), 0);
        assert_eq!(worker.latest_committed(), 0);
        assert_eq!(
            worker.submit_durable("after-wrong-sequence".into(), 4, "test"),
            Err(error)
        );
        let guard = state.0.lock().unwrap();
        assert_eq!((guard.append_calls, guard.flush_calls), (1, 0));
    }
}

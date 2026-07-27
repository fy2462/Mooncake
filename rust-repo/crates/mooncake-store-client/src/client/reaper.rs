use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use transfer_engine_ffi::{
    SegmentId, SubmittedRegisteredBatch, TransferEngine, TransferEngineError, TransferEngineResult,
    TransferStatus, TransferStatusEnum,
};

pub(crate) struct TransferCompletion<P> {
    pub(crate) statuses: Vec<TransferStatus>,
    pub(crate) payload: P,
}

#[derive(Clone, Copy)]
enum ReaperMode {
    Submitted,
    FailedSubmission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchReleaseDecision {
    Released,
    RetryBusy,
    RetainUnproven,
}

fn classify_batch_release(result: &TransferEngineResult<()>) -> BatchReleaseDecision {
    match result {
        Ok(()) | Err(TransferEngineError::BatchAlreadyReleased) => BatchReleaseDecision::Released,
        Err(TransferEngineError::BatchBusy) => BatchReleaseDecision::RetryBusy,
        Err(_) => BatchReleaseDecision::RetainUnproven,
    }
}

trait ReaperJob: Send {
    fn run(self: Box<Self>);
}

struct RegisteredBatchJob<P> {
    engine: Arc<TransferEngine>,
    batch: SubmittedRegisteredBatch,
    segment_id: SegmentId,
    mode: ReaperMode,
    warn_after: Duration,
    payload: Option<P>,
    response: Option<oneshot::Sender<TransferEngineResult<TransferCompletion<P>>>>,
}

impl<P> RegisteredBatchJob<P>
where
    P: Send + 'static,
{
    fn batch_id_for_log(&self) -> u64 {
        self.batch
            .batch_id()
            .map(|batch_id| batch_id.as_raw())
            .unwrap_or(u64::MAX)
    }

    fn release_when_quiescent(&mut self, task_count: usize) -> TransferEngineResult<()> {
        let start = Instant::now();
        let mut warned = false;
        loop {
            let release_result = self.batch.try_release();
            match classify_batch_release(&release_result) {
                BatchReleaseDecision::Released => return Ok(()),
                BatchReleaseDecision::RetryBusy => {
                    for task_id in 0..task_count {
                        let _ = self.batch.transfer_status(task_id);
                    }
                    if !warned && start.elapsed() > self.warn_after {
                        warned = true;
                        tracing::warn!(
                            batch_id = self.batch_id_for_log(),
                            task_count,
                            "registered batch remains busy; retaining payload and retrying"
                        );
                    }
                }
                BatchReleaseDecision::RetainUnproven => {
                    return release_result;
                }
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    fn release_failed_submission(&mut self) -> TransferEngineResult<()> {
        let start = Instant::now();
        let mut warned = false;
        loop {
            let release_result = self.batch.try_release();
            match classify_batch_release(&release_result) {
                BatchReleaseDecision::Released => return Ok(()),
                BatchReleaseDecision::RetryBusy => {
                    if !warned && start.elapsed() > self.warn_after {
                        warned = true;
                        tracing::warn!(
                            batch_id = self.batch_id_for_log(),
                            "failed submission is not proven quiescent; retaining payload and retrying"
                        );
                    }
                }
                BatchReleaseDecision::RetainUnproven => {
                    return release_result;
                }
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    fn poll_submitted(&mut self, task_count: usize) -> TransferEngineResult<Vec<TransferStatus>> {
        let start = Instant::now();
        let mut warned = false;
        let mut statuses = vec![
            TransferStatus {
                status: TransferStatusEnum::Waiting,
                transferred_bytes: 0,
            };
            task_count
        ];
        let mut terminal = vec![false; task_count];

        loop {
            let mut all_terminal = true;
            for task_id in 0..task_count {
                if terminal[task_id] {
                    continue;
                }
                let status = match self.batch.transfer_status(task_id) {
                    Ok(status) => status,
                    Err(error) => {
                        self.release_when_quiescent(task_count)?;
                        return Err(error);
                    }
                };
                statuses[task_id] = status.clone();
                if status.status.is_terminal() {
                    terminal[task_id] = true;
                } else {
                    all_terminal = false;
                }
            }
            if all_terminal {
                self.release_when_quiescent(task_count)?;
                return Ok(statuses);
            }
            if !warned && start.elapsed() > self.warn_after {
                warned = true;
                tracing::warn!(
                    batch_id = self.batch_id_for_log(),
                    task_count,
                    elapsed_ms = start.elapsed().as_millis(),
                    "transfer exceeded warning threshold in reaper; retaining payload"
                );
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }
}

impl<P> ReaperJob for RegisteredBatchJob<P>
where
    P: Send + 'static,
{
    fn run(mut self: Box<Self>) {
        let task_count = self.batch.request_count();
        let status_result = match self.mode {
            ReaperMode::Submitted => self.poll_submitted(task_count),
            ReaperMode::FailedSubmission => self.release_failed_submission().map(|()| Vec::new()),
        };
        if let Err(error) = &status_result
            && !self.batch.is_released()
        {
            tracing::error!(
                %error,
                batch_id = self.batch_id_for_log(),
                segment_id = self.segment_id.0,
                "registered native quiescence cannot be proven; leaking the complete transfer job"
            );
            if let Some(response) = self.response.take() {
                let _ = response.send(Err(TransferEngineError::QuiescenceUnproven));
            }
            std::mem::forget(self);
            return;
        }
        let close_result = self.engine.close_segment(self.segment_id);
        let statuses = match (status_result, close_result) {
            (Ok(statuses), Ok(())) => Ok(statuses),
            (Err(status_error), Ok(())) => Err(status_error),
            (Ok(_), Err(close_error)) => Err(close_error),
            (Err(status_error), Err(close_error)) => {
                tracing::error!(
                    %close_error,
                    segment_id = self.segment_id.0,
                    "failed to close segment after transfer status error"
                );
                Err(status_error)
            }
        };
        let result = statuses.map(|statuses| TransferCompletion {
            statuses,
            payload: self
                .payload
                .take()
                .expect("reaper payload is moved exactly once"),
        });
        if let Some(response) = self.response.take() {
            let _ = response.send(result);
        }
    }
}

pub(crate) struct TransferReaper {
    sender: Option<mpsc::Sender<Box<dyn ReaperJob>>>,
    workers: Vec<JoinHandle<()>>,
}

impl TransferReaper {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Box<dyn ReaperJob>>();
        let receiver = Arc::new(Mutex::new(receiver));
        let worker_count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .clamp(1, 4);
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("mooncake-transfer-reaper-{worker_index}"))
                    .spawn(move || {
                        loop {
                            let job = receiver
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .recv();
                            match job {
                                Ok(job) => job.run(),
                                Err(_) => break,
                            }
                        }
                    })
                    .expect("failed to spawn transfer reaper worker"),
            );
        }
        Self {
            sender: Some(sender),
            workers,
        }
    }

    fn submit<P>(
        &self,
        engine: Arc<TransferEngine>,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        mode: ReaperMode,
        warn_after: Duration,
        payload: P,
    ) -> oneshot::Receiver<TransferEngineResult<TransferCompletion<P>>>
    where
        P: Send + 'static,
    {
        let (response, receiver) = oneshot::channel();
        let job: Box<dyn ReaperJob> = Box::new(RegisteredBatchJob {
            engine,
            batch,
            segment_id,
            mode,
            warn_after,
            payload: Some(payload),
            response: Some(response),
        });
        if let Err(error) = self
            .sender
            .as_ref()
            .expect("reaper sender exists before Drop")
            .send(job)
        {
            // The coordinator is unavailable, but running the returned job
            // synchronously still preserves payload lifetime.
            error.0.run();
        }
        receiver
    }

    pub(crate) fn submit_batch<P>(
        &self,
        engine: Arc<TransferEngine>,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        warn_after: Duration,
        payload: P,
    ) -> oneshot::Receiver<TransferEngineResult<TransferCompletion<P>>>
    where
        P: Send + 'static,
    {
        self.submit(
            engine,
            batch,
            segment_id,
            ReaperMode::Submitted,
            warn_after,
            payload,
        )
    }

    pub(crate) fn submit_failed_batch<P>(
        &self,
        engine: Arc<TransferEngine>,
        batch: SubmittedRegisteredBatch,
        segment_id: SegmentId,
        warn_after: Duration,
        payload: P,
    ) -> oneshot::Receiver<TransferEngineResult<TransferCompletion<P>>>
    where
        P: Send + 'static,
    {
        self.submit(
            engine,
            batch,
            segment_id,
            ReaperMode::FailedSubmission,
            warn_after,
            payload,
        )
    }
}

impl Drop for TransferReaper {
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            if worker.is_finished() {
                let _ = worker.join();
            } else {
                // A Busy native batch may have no bounded completion time.
                // Detaching leaves its job-owned engine, segment and payload
                // alive instead of hanging Client destruction or freeing
                // memory that native code may still access.
                drop(worker);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchReleaseDecision, classify_batch_release};
    use transfer_engine_ffi::{TransferEngineError, TransferStatusEnum};

    #[test]
    fn timeout_is_a_terminal_status_for_reaping() {
        assert!(TransferStatusEnum::Timeout.is_terminal());
    }

    #[test]
    fn batch_release_decisions_preserve_unproven_payloads() {
        assert_eq!(
            classify_batch_release(&Ok(())),
            BatchReleaseDecision::Released
        );
        assert_eq!(
            classify_batch_release(&Err(TransferEngineError::BatchAlreadyReleased)),
            BatchReleaseDecision::Released
        );
        assert_eq!(
            classify_batch_release(&Err(TransferEngineError::BatchBusy)),
            BatchReleaseDecision::RetryBusy
        );
        assert_eq!(
            classify_batch_release(&Err(TransferEngineError::OperationFailed(202))),
            BatchReleaseDecision::RetainUnproven
        );
    }
}

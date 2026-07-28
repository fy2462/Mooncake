use mooncake_store_master::ha::{HaError, OpLogPollResult, OpLogRecord};
use mooncake_store_master::oplog::{InMemoryOpLog, OpLogManager, OpLogStore};
use serde_json::json;
use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct FlushCounter {
    flushes: Arc<AtomicU64>,
}

impl FlushCounter {
    fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Acquire)
    }
}

struct DelayCountingStore {
    inner: InMemoryOpLog,
    flush_latency: Duration,
    flushes: Arc<AtomicU64>,
}

impl DelayCountingStore {
    fn new(max_entries: usize, flush_latency: Duration) -> (Self, FlushCounter) {
        let flushes = Arc::new(AtomicU64::new(0));
        (
            Self {
                inner: InMemoryOpLog::new(max_entries),
                flush_latency,
                flushes: Arc::clone(&flushes),
            },
            FlushCounter { flushes },
        )
    }
}

impl OpLogStore for DelayCountingStore {
    fn append(&mut self, entry: &OpLogRecord) -> Result<u64, HaError> {
        self.inner.append(entry)
    }

    fn read_since(&self, since_seq: u64, max_count: usize) -> Result<Vec<OpLogRecord>, HaError> {
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
        thread::sleep(self.flush_latency);
        self.flushes.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn poll_from(&self, since_seq: u64, max_count: usize) -> OpLogPollResult {
        self.inner.poll_from(since_seq, max_count)
    }
}

struct ThreadSamples {
    sequences: Vec<u64>,
    latency_us: Vec<f64>,
}

struct BenchmarkResult {
    mode: &'static str,
    threads: usize,
    operations: usize,
    elapsed_ms: f64,
    throughput_ops_s: f64,
    p50_us: f64,
    p99_us: f64,
    flushes: u64,
    mean_records_per_flush: f64,
    #[cfg(test)]
    sequences: Vec<u64>,
    #[cfg(test)]
    committed_boundary: u64,
}

fn checked_operation_count(threads: usize, ops_per_thread: usize) -> Result<usize, String> {
    if threads == 0 || ops_per_thread == 0 {
        return Err("--threads and --ops-per-thread must both be greater than zero".into());
    }
    threads
        .checked_mul(ops_per_thread)
        .ok_or_else(|| "operation count overflows usize".into())
}

fn percentile_us(latency_us: &mut [f64], percentile: usize) -> f64 {
    latency_us.sort_by(f64::total_cmp);
    let index = (latency_us.len() * percentile)
        .div_ceil(100)
        .saturating_sub(1);
    latency_us[index]
}

fn finish_result(
    mode: &'static str,
    threads: usize,
    ops_per_thread: usize,
    elapsed: Duration,
    samples: Vec<ThreadSamples>,
    flushes: u64,
    committed_boundary: u64,
) -> BenchmarkResult {
    let operations = checked_operation_count(threads, ops_per_thread)
        .expect("benchmark configuration was validated before worker startup");
    let mut sequences = Vec::with_capacity(operations);
    let mut latency_us = Vec::with_capacity(operations);
    for sample in samples {
        sequences.extend(sample.sequences);
        latency_us.extend(sample.latency_us);
    }
    assert_eq!(
        sequences.len(),
        operations,
        "{mode} lost a successful operation"
    );
    assert_eq!(
        latency_us.len(),
        operations,
        "{mode} missed a latency sample"
    );
    assert!(flushes > 0, "{mode} completed without a durable flush");

    sequences.sort_unstable();
    for (index, sequence) in sequences.iter().enumerate() {
        assert_eq!(
            *sequence,
            (index + 1) as u64,
            "{mode} produced a non-contiguous sequence"
        );
    }
    assert_eq!(
        committed_boundary, operations as u64,
        "{mode} committed boundary does not cover every operation"
    );

    let elapsed_ms = elapsed.as_secs_f64() * 1_000.0;
    let throughput_ops_s = operations as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let p50_us = percentile_us(&mut latency_us, 50);
    let p99_us = percentile_us(&mut latency_us, 99);
    let mean_records_per_flush = operations as f64 / flushes as f64;
    for value in [
        elapsed_ms,
        throughput_ops_s,
        p50_us,
        p99_us,
        mean_records_per_flush,
    ] {
        assert!(
            value.is_finite(),
            "{mode} produced a non-finite JSON number"
        );
    }

    BenchmarkResult {
        mode,
        threads,
        operations,
        elapsed_ms,
        throughput_ops_s,
        p50_us,
        p99_us,
        flushes,
        mean_records_per_flush,
        #[cfg(test)]
        sequences,
        #[cfg(test)]
        committed_boundary,
    }
}

fn wait_for_start(ready: &Barrier, start: &AtomicBool) {
    ready.wait();
    while !start.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
}

fn join_samples(
    handles: Vec<thread::JoinHandle<Result<ThreadSamples, String>>>,
) -> Result<Vec<ThreadSamples>, String> {
    handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .map_err(|_| "benchmark worker thread panicked".to_string())?
        })
        .collect()
}

fn run_sync_baseline(
    threads: usize,
    ops_per_thread: usize,
    flush_latency: Duration,
) -> Result<BenchmarkResult, String> {
    let operations = checked_operation_count(threads, ops_per_thread)?;
    let (store, counter) = DelayCountingStore::new(operations, flush_latency);
    let store = Arc::new(Mutex::new(store));
    let ready = Arc::new(Barrier::new(threads + 1));
    let start = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(threads);

    for thread_index in 0..threads {
        let store = Arc::clone(&store);
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || -> Result<ThreadSamples, String> {
            wait_for_start(&ready, &start);
            let mut sequences = Vec::with_capacity(ops_per_thread);
            let mut latency_us = Vec::with_capacity(ops_per_thread);
            for operation_index in 0..ops_per_thread {
                let operation_start = Instant::now();
                let sequence = {
                    let mut store = store
                        .lock()
                        .map_err(|_| "sync benchmark store mutex was poisoned".to_string())?;
                    let sequence = store
                        .append(&OpLogRecord {
                            seq: 0,
                            producer_view_version: 1,
                            payload: format!("remove:{thread_index}:{operation_index}"),
                        })
                        .map_err(|error| format!("sync append failed: {error}"))?;
                    store
                        .flush_durable()
                        .map_err(|error| format!("sync flush failed: {error}"))?;
                    sequence
                };
                sequences.push(sequence);
                latency_us.push(operation_start.elapsed().as_secs_f64() * 1_000_000.0);
            }
            Ok(ThreadSamples {
                sequences,
                latency_us,
            })
        }));
    }

    ready.wait();
    let elapsed_start = Instant::now();
    start.store(true, Ordering::Release);
    let samples = join_samples(handles)?;
    let elapsed = elapsed_start.elapsed();
    let committed_boundary = store
        .lock()
        .map_err(|_| "sync benchmark store mutex was poisoned".to_string())?
        .latest_sequence();
    Ok(finish_result(
        "sync_baseline",
        threads,
        ops_per_thread,
        elapsed,
        samples,
        counter.flushes(),
        committed_boundary,
    ))
}

fn run_group_commit(
    threads: usize,
    ops_per_thread: usize,
    flush_latency: Duration,
) -> Result<BenchmarkResult, String> {
    let operations = checked_operation_count(threads, ops_per_thread)?;
    let (store, counter) = DelayCountingStore::new(operations, flush_latency);
    let manager = Arc::new(OpLogManager::new(Some(Box::new(store)), 1));
    let ready = Arc::new(Barrier::new(threads + 1));
    let start = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(threads);

    for thread_index in 0..threads {
        let manager = Arc::clone(&manager);
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || -> Result<ThreadSamples, String> {
            wait_for_start(&ready, &start);
            let mut sequences = Vec::with_capacity(ops_per_thread);
            let mut latency_us = Vec::with_capacity(ops_per_thread);
            for operation_index in 0..ops_per_thread {
                let operation_start = Instant::now();
                let sequence = manager
                    .record_remove_durable(&format!(
                        "default\0benchmark-{thread_index}-{operation_index}"
                    ))
                    .map_err(|error| format!("group-commit append failed: {error}"))?;
                sequences.push(sequence);
                latency_us.push(operation_start.elapsed().as_secs_f64() * 1_000_000.0);
            }
            Ok(ThreadSamples {
                sequences,
                latency_us,
            })
        }));
    }

    ready.wait();
    let elapsed_start = Instant::now();
    start.store(true, Ordering::Release);
    let samples = join_samples(handles)?;
    let elapsed = elapsed_start.elapsed();
    let committed_boundary = manager.latest_sequence();
    Ok(finish_result(
        "group_commit",
        threads,
        ops_per_thread,
        elapsed,
        samples,
        counter.flushes(),
        committed_boundary,
    ))
}

struct Arguments {
    threads: usize,
    ops_per_thread: usize,
    flush_latency_ms: u64,
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut threads = None;
    let mut ops_per_thread = None;
    let mut flush_latency_ms = None;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| format!("{argument} requires a value"))?;
        match argument.as_str() {
            "--threads" => threads = Some(value.parse().map_err(|_| "invalid --threads value")?),
            "--ops-per-thread" => {
                ops_per_thread = Some(
                    value
                        .parse()
                        .map_err(|_| "invalid --ops-per-thread value")?,
                )
            }
            "--flush-latency-ms" => {
                flush_latency_ms = Some(
                    value
                        .parse()
                        .map_err(|_| "invalid --flush-latency-ms value")?,
                )
            }
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    let arguments = Arguments {
        threads: threads.ok_or("missing --threads")?,
        ops_per_thread: ops_per_thread.ok_or("missing --ops-per-thread")?,
        flush_latency_ms: flush_latency_ms.ok_or("missing --flush-latency-ms")?,
    };
    checked_operation_count(arguments.threads, arguments.ops_per_thread)?;
    Ok(arguments)
}

fn print_result(result: &BenchmarkResult) {
    println!(
        "{}",
        json!({
            "mode": result.mode,
            "threads": result.threads,
            "operations": result.operations,
            "elapsed_ms": result.elapsed_ms,
            "throughput_ops_s": result.throughput_ops_s,
            "p50_us": result.p50_us,
            "p99_us": result.p99_us,
            "flushes": result.flushes,
            "mean_records_per_flush": result.mean_records_per_flush,
        })
    );
}

fn main() -> ExitCode {
    let arguments = match parse_arguments() {
        Ok(arguments) => arguments,
        Err(error) => {
            eprintln!(
                "usage: oplog_group_commit_bench --threads N --ops-per-thread N --flush-latency-ms N"
            );
            eprintln!("argument error: {error}");
            return ExitCode::from(2);
        }
    };
    let flush_latency = Duration::from_millis(arguments.flush_latency_ms);
    let sync = match run_sync_baseline(arguments.threads, arguments.ops_per_thread, flush_latency) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("sync_baseline failed: {error}");
            return ExitCode::from(1);
        }
    };
    let group = match run_group_commit(arguments.threads, arguments.ops_per_thread, flush_latency) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("group_commit failed: {error}");
            return ExitCode::from(1);
        }
    };
    print_result(&sync);
    print_result(&group);
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_prove_flush_accounting_and_sequence_oracle() {
        let sync = run_sync_baseline(4, 5, Duration::ZERO).expect("sync benchmark succeeds");
        let group = run_group_commit(4, 5, Duration::ZERO).expect("group benchmark succeeds");

        assert_eq!(sync.operations, 20);
        assert_eq!(sync.flushes, 20);
        assert_eq!(sync.committed_boundary, 20);
        assert_eq!(sync.sequences, (1..=20).collect::<Vec<_>>());
        assert_eq!(group.operations, 20);
        assert!((1..=group.operations as u64).contains(&group.flushes));
        assert_eq!(
            group.mean_records_per_flush,
            group.operations as f64 / group.flushes as f64
        );
        assert_eq!(group.committed_boundary, 20);
        assert_eq!(group.sequences, (1..=20).collect::<Vec<_>>());
    }
}

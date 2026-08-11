use mooncake_store_core::{StoreError, error::StoreResult};
use parking_lot::Mutex;
use prometheus::core::Collector;
use prometheus::proto::{Metric, MetricFamily, MetricType, Quantile, Summary as ProtoSummary};
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tonic::body::BoxBody;
use tonic::codegen::Service;
use tonic::codegen::http::{Request, Response};
use tonic::transport::Channel;

const LATENCY_BUCKETS_US: &[f64] = &[
    50.0,
    75.0,
    125.0,
    150.0,
    200.0,
    250.0,
    300.0,
    400.0,
    500.0,
    750.0,
    1_000.0,
    1_500.0,
    2_000.0,
    3_000.0,
    5_000.0,
    7_000.0,
    15_000.0,
    20_000.0,
    50_000.0,
    100_000.0,
    200_000.0,
    500_000.0,
    1_000_000.0,
    2_000_000.0,
    5_000_000.0,
    10_000_000.0,
    20_000_000.0,
];

const SSD_LATENCY_BUCKETS_US: &[f64] = &[
    50.0,
    100.0,
    200.0,
    500.0,
    1_000.0,
    2_000.0,
    5_000.0,
    10_000.0,
    20_000.0,
    50_000.0,
    100_000.0,
    200_000.0,
    500_000.0,
    1_000_000.0,
    2_000_000.0,
    5_000_000.0,
    10_000_000.0,
    30_000_000.0,
];

const SSD_LATENCY_SUMMARY_SAMPLE_CAPACITY: usize = 4_096;

#[derive(Clone, Copy)]
pub(crate) enum TransferOperationKind {
    Read,
    Write,
}

#[derive(Clone, Copy)]
struct TransferSnapshot {
    read_bytes: u64,
    write_bytes: u64,
    timestamp: Instant,
}

/// A minimal Prometheus SUMMARY metric family. The prometheus crate has no
/// built-in summary type, so this custom collector derives p50/p90/p99
/// quantiles from the observed latency samples on render. The family name
/// matches the C++ `mooncake_ssd_*_latency_summary_us` families.
#[derive(Clone)]
struct SsdLatencySummary {
    desc: prometheus::core::Desc,
    samples: Arc<parking_lot::RwLock<SsdLatencySummaryState>>,
}

#[derive(Default)]
struct SsdLatencySummaryState {
    samples: VecDeque<f64>,
    sample_count: u64,
    sample_sum: f64,
}

impl SsdLatencySummary {
    fn new(name: &str, help: &str, const_labels: HashMap<String, String>) -> Self {
        let desc = prometheus::core::Desc::new(
            name.to_string(),
            help.to_string(),
            Vec::new(),
            const_labels,
        )
        .expect("valid summary descriptor");
        Self {
            desc,
            samples: Arc::new(parking_lot::RwLock::new(SsdLatencySummaryState::default())),
        }
    }

    fn observe(&self, value: f64) {
        let mut state = self.samples.write();
        state.sample_count = state.sample_count.saturating_add(1);
        state.sample_sum += value;
        if state.samples.len() == SSD_LATENCY_SUMMARY_SAMPLE_CAPACITY {
            state.samples.pop_front();
        }
        state.samples.push_back(value);
    }
}

impl Collector for SsdLatencySummary {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let state = self.samples.read();
        let mut samples = state.samples.iter().copied().collect::<Vec<_>>();
        let sample_count = state.sample_count;
        let sample_sum = state.sample_sum;
        drop(state);
        samples.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        let percentile = |q: f64| {
            if samples.is_empty() {
                return None;
            }
            let index = ((samples.len() as f64 - 1.0) * q).round() as usize;
            Some(samples[index])
        };
        let mut summary = ProtoSummary::default();
        summary.set_sample_count(sample_count);
        summary.set_sample_sum(sample_sum);
        for (quantile, value) in [
            (0.5, percentile(0.5)),
            (0.9, percentile(0.9)),
            (0.99, percentile(0.99)),
        ] {
            if let Some(value) = value {
                let mut proto_quantile = Quantile::default();
                proto_quantile.set_quantile(quantile);
                proto_quantile.set_value(value);
                summary.mut_quantile().push(proto_quantile);
            }
        }
        let mut metric = Metric::default();
        metric.set_label(self.desc.const_label_pairs.clone().into());
        metric.set_summary(summary);
        let mut family = MetricFamily::default();
        family.set_name(self.desc.fq_name.clone());
        family.set_help(self.desc.help.clone());
        family.set_field_type(MetricType::SUMMARY);
        family.mut_metric().push(metric);
        vec![family]
    }
}

#[derive(Default)]
pub(super) struct MetricsReporterState {
    handle: parking_lot::RwLock<Option<tokio::task::JoinHandle<()>>>,
}

impl MetricsReporterState {
    pub(super) fn start(&self, metrics: Option<Arc<ClientMetrics>>) {
        let Some(metrics) = metrics else {
            return;
        };
        let interval = metrics.reporting_interval;
        if interval.is_zero() || self.handle.read().is_some() {
            return;
        }
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                tracing::info!(
                    target: "client_metrics",
                    "\nClient Metrics Report:\n{}",
                    metrics.periodic_report()
                );
            }
        });
        *self.handle.write() = Some(handle);
    }

    pub(super) fn stop(&self) {
        if let Some(handle) = self.handle.write().take() {
            handle.abort();
        }
    }
}

impl Drop for MetricsReporterState {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.get_mut().take() {
            handle.abort();
        }
    }
}

/// A transparent tonic transport wrapper that measures every completed Master
/// RPC in one place. Keeping this below the generated client avoids spreading
/// timing logic through Store request orchestration.
#[derive(Clone)]
pub(crate) struct MetricsChannel {
    inner: Channel,
    metrics: Option<Arc<ClientMetrics>>,
}

impl MetricsChannel {
    pub(crate) fn new(inner: Channel, metrics: Option<Arc<ClientMetrics>>) -> Self {
        Self { inner, metrics }
    }
}

impl Service<Request<BoxBody>> for MetricsChannel {
    type Response = Response<BoxBody>;
    type Error = tonic::transport::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<BoxBody>) -> Self::Future {
        let rpc_name = request
            .uri()
            .path()
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or("Unknown")
            .to_string();
        let metrics = self.metrics.clone();
        let future = self.inner.call(request);
        Box::pin(async move {
            let started_at = Instant::now();
            let result = future.await;
            if let Some(metrics) = metrics {
                metrics.observe_rpc(&rpc_name, started_at.elapsed());
            }
            result
        })
    }
}

pub(crate) struct ClientMetrics {
    registry: Registry,
    healthy: IntGauge,
    closed: IntGauge,
    transfer_read_bytes: IntCounter,
    transfer_write_bytes: IntCounter,
    read_strategy_count: IntCounterVec,
    get_latency_us: Histogram,
    put_latency_us: Histogram,
    batch_get_latency_us: Histogram,
    batch_put_latency_us: Histogram,
    rpc_count: IntCounterVec,
    rpc_latency_us: HistogramVec,
    read_operation_count: IntCounterVec,
    read_operation_bytes: IntCounterVec,
    read_operation_latency_us: HistogramVec,
    write_operation_count: IntCounterVec,
    write_operation_bytes: IntCounterVec,
    write_operation_latency_us: HistogramVec,
    ssd_read_bytes: IntCounter,
    ssd_write_bytes: IntCounter,
    ssd_read_ops: IntCounter,
    ssd_write_ops: IntCounter,
    ssd_read_latency_us: Histogram,
    ssd_write_latency_us: Histogram,
    ssd_read_latency_summary_us: SsdLatencySummary,
    ssd_write_latency_summary_us: SsdLatencySummary,
    ssd_total_latency_summary_us: SsdLatencySummary,
    ssd_total_bytes: IntCounter,
    ssd_total_ops: IntCounter,
    ssd_total_latency_us: Histogram,
    observed_rpc_names: Mutex<BTreeSet<String>>,
    observed_read_operations: Mutex<BTreeSet<String>>,
    observed_write_operations: Mutex<BTreeSet<String>>,
    started_at: Instant,
    bandwidth_summary_enabled: bool,
    master_rpc_metrics_enabled: bool,
    reporting_interval: Duration,
    last_report_snapshot: Mutex<TransferSnapshot>,
}

impl ClientMetrics {
    pub(crate) fn from_env() -> StoreResult<Option<Arc<Self>>> {
        if !parse_bool_env("MC_STORE_CLIENT_METRIC", true) {
            return Ok(None);
        }
        let bandwidth_summary_enabled = parse_bool_env("MC_STORE_CLIENT_METRIC_BANDWIDTH", true);
        let reporting_interval = parse_metrics_interval(
            std::env::var("MC_STORE_CLIENT_METRIC_INTERVAL")
                .ok()
                .as_deref(),
        );
        let labels = std::env::var("MC_STORE_CLUSTER_ID")
            .ok()
            .filter(|value| !value.is_empty())
            .map(|value| HashMap::from([("cluster_id".to_string(), value)]))
            .unwrap_or_default();
        Self::new_with_reporting_interval(
            labels,
            bandwidth_summary_enabled,
            true,
            reporting_interval,
        )
        .map(|metrics| Some(Arc::new(metrics)))
    }

    pub(crate) fn new(
        labels: HashMap<String, String>,
        bandwidth_summary_enabled: bool,
        master_rpc_metrics_enabled: bool,
    ) -> StoreResult<Self> {
        Self::new_with_reporting_interval(
            labels,
            bandwidth_summary_enabled,
            master_rpc_metrics_enabled,
            Duration::ZERO,
        )
    }

    fn new_with_reporting_interval(
        labels: HashMap<String, String>,
        bandwidth_summary_enabled: bool,
        master_rpc_metrics_enabled: bool,
        reporting_interval: Duration,
    ) -> StoreResult<Self> {
        let opts = |name, help| Opts::new(name, help).const_labels(labels.clone());
        let histogram_opts = |name, help, buckets: &[f64]| {
            HistogramOpts::new(name, help)
                .const_labels(labels.clone())
                .buckets(buckets.to_vec())
        };

        let registry = Registry::new();
        let healthy = IntGauge::with_opts(opts(
            "mooncake_client_healthy",
            "Whether the last ping to the Mooncake master succeeded",
        ))
        .map_err(metric_error)?;
        let closed = IntGauge::with_opts(opts(
            "mooncake_client_closed",
            "Whether the Mooncake client has been torn down",
        ))
        .map_err(metric_error)?;
        let transfer_read_bytes =
            IntCounter::with_opts(opts("mooncake_transfer_read_bytes", "Total bytes read"))
                .map_err(metric_error)?;
        let transfer_write_bytes =
            IntCounter::with_opts(opts("mooncake_transfer_write_bytes", "Total bytes written"))
                .map_err(metric_error)?;
        let read_strategy_count = IntCounterVec::new(
            opts(
                "mooncake_transfer_read_strategy_total",
                "Successful reads by data-path strategy",
            ),
            &["strategy"],
        )
        .map_err(metric_error)?;
        let get_latency_us = Histogram::with_opts(histogram_opts(
            "mooncake_transfer_get_latency",
            "Get transfer latency (us)",
            LATENCY_BUCKETS_US,
        ))
        .map_err(metric_error)?;
        let put_latency_us = Histogram::with_opts(histogram_opts(
            "mooncake_transfer_put_latency",
            "Put transfer latency (us)",
            LATENCY_BUCKETS_US,
        ))
        .map_err(metric_error)?;
        let batch_get_latency_us = Histogram::with_opts(histogram_opts(
            "mooncake_transfer_batch_get_latency",
            "Batch Get transfer latency (us)",
            LATENCY_BUCKETS_US,
        ))
        .map_err(metric_error)?;
        let batch_put_latency_us = Histogram::with_opts(histogram_opts(
            "mooncake_transfer_batch_put_latency",
            "Batch Put transfer latency (us)",
            LATENCY_BUCKETS_US,
        ))
        .map_err(metric_error)?;
        let rpc_count = IntCounterVec::new(
            opts(
                "mooncake_client_rpc_count",
                "Total number of RPC calls made by the client",
            ),
            &["rpc_name"],
        )
        .map_err(metric_error)?;
        let rpc_latency_us = HistogramVec::new(
            histogram_opts(
                "mooncake_client_rpc_latency",
                "Latency of RPC calls made by the client (in us)",
                LATENCY_BUCKETS_US,
            ),
            &["rpc_name"],
        )
        .map_err(metric_error)?;
        let read_operation_count = IntCounterVec::new(
            opts(
                "mooncake_transfer_read_operation_count",
                "Total read operations by interface type",
            ),
            &["op_name"],
        )
        .map_err(metric_error)?;
        let read_operation_bytes = IntCounterVec::new(
            opts(
                "mooncake_transfer_read_operation_bytes",
                "Total read bytes by interface type",
            ),
            &["op_name"],
        )
        .map_err(metric_error)?;
        let read_operation_latency_us = HistogramVec::new(
            histogram_opts(
                "mooncake_transfer_read_operation_latency",
                "Read operation latency by interface type (us)",
                LATENCY_BUCKETS_US,
            ),
            &["op_name"],
        )
        .map_err(metric_error)?;
        let write_operation_count = IntCounterVec::new(
            opts(
                "mooncake_transfer_write_operation_count",
                "Total write operations by interface type",
            ),
            &["op_name"],
        )
        .map_err(metric_error)?;
        let write_operation_bytes = IntCounterVec::new(
            opts(
                "mooncake_transfer_write_operation_bytes",
                "Total write bytes by interface type",
            ),
            &["op_name"],
        )
        .map_err(metric_error)?;
        let write_operation_latency_us = HistogramVec::new(
            histogram_opts(
                "mooncake_transfer_write_operation_latency",
                "Write operation latency by interface type (us)",
                LATENCY_BUCKETS_US,
            ),
            &["op_name"],
        )
        .map_err(metric_error)?;
        let ssd_read_bytes = IntCounter::with_opts(opts(
            "mooncake_ssd_read_bytes_total",
            "Total bytes read from SSD",
        ))
        .map_err(metric_error)?;
        let ssd_write_bytes = IntCounter::with_opts(opts(
            "mooncake_ssd_write_bytes_total",
            "Total bytes written to SSD",
        ))
        .map_err(metric_error)?;
        let ssd_read_ops = IntCounter::with_opts(opts(
            "mooncake_ssd_read_ops_total",
            "Total number of SSD read operations (key count)",
        ))
        .map_err(metric_error)?;
        let ssd_write_ops = IntCounter::with_opts(opts(
            "mooncake_ssd_write_ops_total",
            "Total number of SSD write operations (key count)",
        ))
        .map_err(metric_error)?;
        let ssd_read_latency_us = Histogram::with_opts(histogram_opts(
            "mooncake_ssd_read_latency_us",
            "SSD read latency per batch (us)",
            SSD_LATENCY_BUCKETS_US,
        ))
        .map_err(metric_error)?;
        let ssd_write_latency_us = Histogram::with_opts(histogram_opts(
            "mooncake_ssd_write_latency_us",
            "SSD write latency per batch (us)",
            SSD_LATENCY_BUCKETS_US,
        ))
        .map_err(metric_error)?;
        let ssd_total_bytes = IntCounter::with_opts(opts(
            "mooncake_ssd_total_bytes_total",
            "Total bytes read and written to SSD",
        ))
        .map_err(metric_error)?;
        let ssd_total_ops = IntCounter::with_opts(opts(
            "mooncake_ssd_total_ops_total",
            "Total number of SSD operations (key count)",
        ))
        .map_err(metric_error)?;
        let ssd_total_latency_us = Histogram::with_opts(histogram_opts(
            "mooncake_ssd_total_latency_us",
            "SSD total latency per batch (us)",
            SSD_LATENCY_BUCKETS_US,
        ))
        .map_err(metric_error)?;
        // C++ SsdMetric also exposes summary families for read/write/total
        // latency; keep the same on-wire names for serialization parity.
        let ssd_read_latency_summary_us = SsdLatencySummary::new(
            "mooncake_ssd_read_latency_summary_us",
            "SSD read latency (us)",
            labels.clone(),
        );
        let ssd_write_latency_summary_us = SsdLatencySummary::new(
            "mooncake_ssd_write_latency_summary_us",
            "SSD write latency (us)",
            labels.clone(),
        );
        let ssd_total_latency_summary_us = SsdLatencySummary::new(
            "mooncake_ssd_total_latency_summary_us",
            "SSD total latency (us)",
            labels.clone(),
        );

        for collector in [
            Box::new(healthy.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(closed.clone()),
            Box::new(transfer_read_bytes.clone()),
            Box::new(transfer_write_bytes.clone()),
            Box::new(read_strategy_count.clone()),
            Box::new(get_latency_us.clone()),
            Box::new(put_latency_us.clone()),
            Box::new(batch_get_latency_us.clone()),
            Box::new(batch_put_latency_us.clone()),
            Box::new(read_operation_count.clone()),
            Box::new(read_operation_bytes.clone()),
            Box::new(read_operation_latency_us.clone()),
            Box::new(write_operation_count.clone()),
            Box::new(write_operation_bytes.clone()),
            Box::new(write_operation_latency_us.clone()),
            Box::new(ssd_read_bytes.clone()),
            Box::new(ssd_write_bytes.clone()),
            Box::new(ssd_read_ops.clone()),
            Box::new(ssd_write_ops.clone()),
            Box::new(ssd_read_latency_us.clone()),
            Box::new(ssd_write_latency_us.clone()),
            Box::new(ssd_read_latency_summary_us.clone()),
            Box::new(ssd_write_latency_summary_us.clone()),
            Box::new(ssd_total_latency_summary_us.clone()),
            Box::new(ssd_total_bytes.clone()),
            Box::new(ssd_total_ops.clone()),
            Box::new(ssd_total_latency_us.clone()),
        ] {
            registry.register(collector).map_err(metric_error)?;
        }
        if master_rpc_metrics_enabled {
            registry
                .register(Box::new(rpc_count.clone()))
                .map_err(metric_error)?;
            registry
                .register(Box::new(rpc_latency_us.clone()))
                .map_err(metric_error)?;
        }

        let started_at = Instant::now();
        Ok(Self {
            registry,
            healthy,
            closed,
            transfer_read_bytes,
            transfer_write_bytes,
            read_strategy_count,
            get_latency_us,
            put_latency_us,
            batch_get_latency_us,
            batch_put_latency_us,
            rpc_count,
            rpc_latency_us,
            read_operation_count,
            read_operation_bytes,
            read_operation_latency_us,
            write_operation_count,
            write_operation_bytes,
            write_operation_latency_us,
            ssd_read_bytes,
            ssd_write_bytes,
            ssd_read_ops,
            ssd_write_ops,
            ssd_read_latency_us,
            ssd_write_latency_us,
            ssd_read_latency_summary_us,
            ssd_write_latency_summary_us,
            ssd_total_latency_summary_us,
            ssd_total_bytes,
            ssd_total_ops,
            ssd_total_latency_us,
            observed_rpc_names: Mutex::new(BTreeSet::new()),
            observed_read_operations: Mutex::new(BTreeSet::new()),
            observed_write_operations: Mutex::new(BTreeSet::new()),
            started_at,
            bandwidth_summary_enabled,
            master_rpc_metrics_enabled,
            reporting_interval,
            last_report_snapshot: Mutex::new(TransferSnapshot {
                read_bytes: 0,
                write_bytes: 0,
                timestamp: started_at,
            }),
        })
    }

    pub(crate) fn render_prometheus(&self, healthy: bool, closed: bool) -> StoreResult<Vec<u8>> {
        self.healthy.set(if healthy { 1 } else { 0 });
        self.closed.set(if closed { 1 } else { 0 });
        let encoder = TextEncoder::new();
        let mut body = Vec::new();
        encoder
            .encode(&self.registry.gather(), &mut body)
            .map_err(metric_error)?;
        Ok(body)
    }

    pub(crate) fn prometheus_content_type(&self) -> String {
        TextEncoder::new().format_type().to_string()
    }

    pub(crate) fn observe_transfer_bytes(&self, kind: TransferOperationKind, bytes: u64) {
        match kind {
            TransferOperationKind::Read => self.transfer_read_bytes.inc_by(bytes),
            TransferOperationKind::Write => self.transfer_write_bytes.inc_by(bytes),
        }
    }

    pub(crate) fn observe_read_strategy(&self, strategy: &str) {
        self.read_strategy_count
            .with_label_values(&[strategy])
            .inc();
    }

    pub(crate) fn observe_operation(
        &self,
        kind: TransferOperationKind,
        operation: &str,
        bytes: u64,
        elapsed: Duration,
    ) {
        let latency_us = duration_us(elapsed);
        match kind {
            TransferOperationKind::Read => {
                self.observed_read_operations
                    .lock()
                    .insert(operation.to_string());
                self.read_operation_count
                    .with_label_values(&[operation])
                    .inc();
                self.read_operation_bytes
                    .with_label_values(&[operation])
                    .inc_by(bytes);
                self.read_operation_latency_us
                    .with_label_values(&[operation])
                    .observe(latency_us);
            }
            TransferOperationKind::Write => {
                self.observed_write_operations
                    .lock()
                    .insert(operation.to_string());
                self.write_operation_count
                    .with_label_values(&[operation])
                    .inc();
                self.write_operation_bytes
                    .with_label_values(&[operation])
                    .inc_by(bytes);
                self.write_operation_latency_us
                    .with_label_values(&[operation])
                    .observe(latency_us);
            }
        }
    }

    pub(crate) fn observe_get(&self, bytes: u64, elapsed: Duration) {
        self.get_latency_us.observe(duration_us(elapsed));
        self.observe_operation(TransferOperationKind::Read, "get_buffer", bytes, elapsed);
    }

    pub(crate) fn observe_put(&self, bytes: u64, elapsed: Duration) {
        self.put_latency_us.observe(duration_us(elapsed));
        self.observe_operation(TransferOperationKind::Write, "put", bytes, elapsed);
    }

    pub(crate) fn observe_batch_get(&self, bytes: u64, elapsed: Duration) {
        self.batch_get_latency_us.observe(duration_us(elapsed));
        self.observe_operation(
            TransferOperationKind::Read,
            "batch_get_buffer",
            bytes,
            elapsed,
        );
    }

    pub(crate) fn observe_batch_put(&self, bytes: u64, elapsed: Duration) {
        self.batch_put_latency_us.observe(duration_us(elapsed));
        self.observe_operation(TransferOperationKind::Write, "put_batch", bytes, elapsed);
    }

    pub(crate) fn observe_rpc(&self, rpc_name: &str, elapsed: Duration) {
        if !self.master_rpc_metrics_enabled {
            return;
        }
        self.observed_rpc_names.lock().insert(rpc_name.to_string());
        self.rpc_count.with_label_values(&[rpc_name]).inc();
        self.rpc_latency_us
            .with_label_values(&[rpc_name])
            .observe(duration_us(elapsed));
    }

    pub(crate) fn observe_ssd_read(&self, bytes: u64, key_count: u64, elapsed: Duration) {
        let latency_us = duration_us(elapsed);
        self.ssd_read_bytes.inc_by(bytes);
        self.ssd_read_ops.inc_by(key_count);
        self.ssd_read_latency_us.observe(latency_us);
        self.ssd_read_latency_summary_us.observe(latency_us);
        self.ssd_total_bytes.inc_by(bytes);
        self.ssd_total_ops.inc_by(key_count);
        self.ssd_total_latency_us.observe(latency_us);
        self.ssd_total_latency_summary_us.observe(latency_us);
    }

    pub(crate) fn observe_ssd_write(&self, bytes: u64, key_count: u64, elapsed: Duration) {
        let latency_us = duration_us(elapsed);
        self.ssd_write_bytes.inc_by(bytes);
        self.ssd_write_ops.inc_by(key_count);
        self.ssd_write_latency_us.observe(latency_us);
        self.ssd_write_latency_summary_us.observe(latency_us);
        self.ssd_total_bytes.inc_by(bytes);
        self.ssd_total_ops.inc_by(key_count);
        self.ssd_total_latency_us.observe(latency_us);
        self.ssd_total_latency_summary_us.observe(latency_us);
    }

    pub(crate) fn summary(&self) -> String {
        let elapsed = self.started_at.elapsed().as_secs_f64().max(1e-9);
        let read_bytes = self.transfer_read_bytes.get();
        let write_bytes = self.transfer_write_bytes.get();
        let mut output = String::from("Client Metrics Summary\n=== Transfer Metrics Summary ===\n");
        output.push_str(&format!("Total Read: {}\n", format_bytes(read_bytes)));
        output.push_str(&format!("Total Write: {}\n", format_bytes(write_bytes)));
        if self.bandwidth_summary_enabled {
            output.push_str(&format!(
                "Average Read Throughput: {}/s\n",
                format_rate(read_bytes as f64 / elapsed)
            ));
            output.push_str(&format!(
                "Average Write Throughput: {}/s\n",
                format_rate(write_bytes as f64 / elapsed)
            ));
        }
        output.push_str("\n=== Latency Summary (microseconds) ===\n");
        output.push_str(&format!(
            "Get: {}\n",
            histogram_summary(&self.get_latency_us)
        ));
        output.push_str(&format!(
            "Put: {}\n",
            histogram_summary(&self.put_latency_us)
        ));
        output.push_str(&format!(
            "Batch Get: {}\n",
            histogram_summary(&self.batch_get_latency_us)
        ));
        output.push_str(&format!(
            "Batch Put: {}\n",
            histogram_summary(&self.batch_put_latency_us)
        ));
        if self.master_rpc_metrics_enabled {
            output.push_str("\n=== RPC Metrics Summary ===\n");
            let rpc_names = self.observed_rpc_names.lock().clone();
            if rpc_names.is_empty() {
                output.push_str("No RPC calls recorded\n");
            } else {
                for name in rpc_names {
                    let count = self.rpc_count.with_label_values(&[&name]).get();
                    let latency = self.rpc_latency_us.with_label_values(&[&name]);
                    output.push_str(&format!(
                        "{name}: count={count}, {}\n",
                        histogram_summary(&latency)
                    ));
                }
            }
        }
        output.push_str("\n=== Interface Operation Metrics Summary ===\n");
        self.append_operation_summary(&mut output, TransferOperationKind::Read);
        self.append_operation_summary(&mut output, TransferOperationKind::Write);
        output.push_str("\n=== SSD Metrics Summary ===\n");
        output.push_str(&ssd_metrics_line(
            "Read",
            self.ssd_read_bytes.get(),
            self.ssd_read_ops.get(),
            elapsed,
        ));
        output.push_str(&ssd_metrics_line(
            "Write",
            self.ssd_write_bytes.get(),
            self.ssd_write_ops.get(),
            elapsed,
        ));
        output.push_str(&format!(
            "{}\n\n=== SSD Latency Summary (microseconds) ===\n",
            ssd_metrics_line(
                "Total",
                self.ssd_total_bytes.get(),
                self.ssd_total_ops.get(),
                elapsed
            )
        ));
        output.push_str(&format!(
            "Read: {}\nWrite: {}\nTotal: {}\n",
            histogram_summary(&self.ssd_read_latency_us),
            histogram_summary(&self.ssd_write_latency_us),
            histogram_summary(&self.ssd_total_latency_us)
        ));
        output
    }

    fn periodic_report(&self) -> String {
        let mut report = self.summary();
        if let Some(bandwidth) = self.interval_bandwidth_report() {
            report.push_str("\n=== Interval Throughput Summary ===\n");
            report.push_str(&bandwidth);
        }
        report
    }

    fn interval_bandwidth_report(&self) -> Option<String> {
        if !self.bandwidth_summary_enabled {
            return None;
        }
        let now = Instant::now();
        let read_bytes = self.transfer_read_bytes.get();
        let write_bytes = self.transfer_write_bytes.get();
        let mut snapshot = self.last_report_snapshot.lock();
        let previous = *snapshot;
        *snapshot = TransferSnapshot {
            read_bytes,
            write_bytes,
            timestamp: now,
        };
        let elapsed = now
            .duration_since(previous.timestamp)
            .as_secs_f64()
            .max(1e-9);
        let read_delta = read_bytes.saturating_sub(previous.read_bytes);
        let write_delta = write_bytes.saturating_sub(previous.write_bytes);
        Some(format!(
            "Read Throughput: {}/s ({} over {elapsed:.2}s)\nWrite Throughput: {}/s ({} over {elapsed:.2}s)",
            format_rate(read_delta as f64 / elapsed),
            format_bytes(read_delta),
            format_rate(write_delta as f64 / elapsed),
            format_bytes(write_delta),
        ))
    }

    fn append_operation_summary(&self, output: &mut String, kind: TransferOperationKind) {
        let (heading, operations, count, bytes, latency) = match kind {
            TransferOperationKind::Read => (
                "Read Interfaces",
                self.observed_read_operations.lock().clone(),
                &self.read_operation_count,
                &self.read_operation_bytes,
                &self.read_operation_latency_us,
            ),
            TransferOperationKind::Write => (
                "Write Interfaces",
                self.observed_write_operations.lock().clone(),
                &self.write_operation_count,
                &self.write_operation_bytes,
                &self.write_operation_latency_us,
            ),
        };
        output.push_str(heading);
        output.push_str(":\n");
        if operations.is_empty() {
            output.push_str("No data\n");
            return;
        }
        for operation in operations {
            let labels = &[operation.as_str()];
            output.push_str(&format!(
                "{operation}: count={}, bytes={}, {}\n",
                count.with_label_values(labels).get(),
                format_bytes(bytes.with_label_values(labels).get()),
                histogram_summary(&latency.with_label_values(labels))
            ));
        }
    }
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    let value = std::env::var(name).ok();
    parse_bool_value(name, value.as_deref(), default)
}

fn parse_bool_value(name: &str, value: Option<&str>, default: bool) -> bool {
    let Some(value) = value else {
        return default;
    };
    match crate::utils::string_to_bool(value) {
        Some(parsed) => parsed,
        None if value.eq_ignore_ascii_case("enable") => true,
        None if value.eq_ignore_ascii_case("disable") => false,
        None => {
            tracing::warn!(%name, %value, default, "invalid boolean environment value");
            default
        }
    }
}

fn parse_metrics_interval(value: Option<&str>) -> Duration {
    let Some(value) = value else {
        return Duration::ZERO;
    };
    match value.parse::<u64>() {
        Ok(seconds) => Duration::from_secs(seconds),
        Err(error) => {
            tracing::warn!(
                %value,
                %error,
                "invalid MC_STORE_CLIENT_METRIC_INTERVAL; periodic reporting disabled"
            );
            Duration::ZERO
        }
    }
}

fn metric_error(error: prometheus::Error) -> StoreError {
    StoreError::Internal(format!("client metrics error: {error}"))
}

fn duration_us(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn histogram_summary(histogram: &Histogram) -> String {
    let count = histogram.get_sample_count();
    if count == 0 {
        return "No data".to_string();
    }
    let families = histogram.collect();
    let buckets = families
        .first()
        .and_then(|family| family.get_metric().first())
        .map(|metric| metric.get_histogram().get_bucket())
        .unwrap_or_default();
    let percentile = |target: u64| {
        buckets
            .iter()
            .find(|bucket| bucket.get_cumulative_count() >= target)
            .map(|bucket| bucket.get_upper_bound())
    };
    let p50 = percentile(count.saturating_mul(50).div_ceil(100));
    let p90 = percentile(count.saturating_mul(90).div_ceil(100));
    let p95 = percentile(count.saturating_mul(95).div_ceil(100));
    let p99 = percentile(count.saturating_mul(99).div_ceil(100));
    let mut previous_count = 0;
    let mut max_bucket = None;
    for bucket in buckets {
        if bucket.get_cumulative_count() > previous_count {
            max_bucket = Some(bucket.get_upper_bound());
        }
        previous_count = bucket.get_cumulative_count();
    }
    let mut summary = format!(
        "count={count}, avg={:.1}us",
        histogram.get_sample_sum() / count as f64
    );
    if let Some(p50) = p50 {
        summary.push_str(&format!(", p50<{p50}us"));
    }
    if let Some(p90) = p90 {
        summary.push_str(&format!(", p90<{p90}us"));
    }
    if let Some(p95) = p95 {
        summary.push_str(&format!(", p95<{p95}us"));
    }
    if let Some(p99) = p99 {
        summary.push_str(&format!(", p99<{p99}us"));
    }
    if let Some(max_bucket) = max_bucket {
        summary.push_str(&format!(", max<{max_bucket}us"));
    }
    summary
}

fn format_bytes(bytes: u64) -> String {
    crate::utils::byte_size_to_string(bytes)
}

fn format_rate(bytes_per_second: f64) -> String {
    format_rate_with_suffix(bytes_per_second, "B")
}

/// One SSD summary line. Throughput and IOPS are emitted only when the byte
/// count is nonzero (C++ omits them for empty metrics).
fn ssd_metrics_line(label: &str, bytes: u64, ops: u64, elapsed: f64) -> String {
    if bytes == 0 {
        format!("SSD {label}: {}\n", format_bytes(0))
    } else {
        format!(
            "SSD {label}: {}, ops={ops}, throughput={}/s, IOPS={}\n",
            format_bytes(bytes),
            format_rate(bytes as f64 / elapsed),
            (ops as f64 / elapsed).round() as u64,
        )
    }
}

fn format_rate_with_suffix(value: f64, suffix: &str) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;
    let (scaled, prefix) = if value >= TB {
        (value / TB, "T")
    } else if value >= GB {
        (value / GB, "G")
    } else if value >= MB {
        (value / MB, "M")
    } else if value >= KB {
        (value / KB, "K")
    } else {
        (value, "")
    };
    format!("{scaled:.2} {prefix}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::{
        ClientMetrics, SSD_LATENCY_SUMMARY_SAMPLE_CAPACITY, SsdLatencySummary,
        TransferOperationKind, parse_bool_value, parse_metrics_interval, ssd_metrics_line,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn persistent_registry_exports_cpp_metric_names_and_summary() {
        let metrics = ClientMetrics::new(
            HashMap::from([("cluster_id".to_string(), "test-cluster".to_string())]),
            true,
            true,
        )
        .unwrap();
        metrics.observe_transfer_bytes(TransferOperationKind::Read, 5 * 1024 * 1024);
        metrics.observe_transfer_bytes(TransferOperationKind::Write, 10 * 1024 * 1024);
        metrics.observe_get(2 * 1024, Duration::from_micros(220));
        metrics.observe_batch_get(0, Duration::from_micros(1_500));
        metrics.observe_batch_put(4 * 1024, Duration::from_micros(2_000));
        metrics.observe_rpc("ExistKey", Duration::from_micros(180));
        metrics.observe_ssd_write(2048, 1, Duration::from_micros(500));

        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        assert!(text.contains("mooncake_transfer_read_bytes"));
        assert!(text.contains("mooncake_client_rpc_count"));
        assert!(text.contains("mooncake_ssd_write_bytes_total"));
        assert!(text.contains("cluster_id=\"test-cluster\""));

        let summary = metrics.summary();
        assert!(summary.contains("Client Metrics Summary"));
        assert!(summary.contains("Transfer Metrics Summary"));
        assert!(summary.contains("RPC Metrics Summary"));
        assert!(summary.contains("Interface Operation Metrics Summary"));
        assert!(summary.contains("Total Read: 5.00 MB"));
        assert!(summary.contains("Total Write: 10.00 MB"));
        assert!(summary.contains("ExistKey: count=1"));
        assert!(summary.contains("get_buffer: count=1"));
        assert!(summary.contains("put_batch: count=1"));
        assert!(summary.contains("SSD Write: 2.00 KB, ops=1"));

        let report = metrics.periodic_report();
        assert!(report.contains("=== Interval Throughput Summary ==="));
        assert!(report.contains("5.00 MB over"));
    }

    #[test]
    fn disabled_master_rpc_metrics_are_omitted_from_summary_and_prometheus() {
        let metrics = ClientMetrics::new(HashMap::new(), true, false).unwrap();
        metrics.observe_transfer_bytes(TransferOperationKind::Read, 1024);
        metrics.observe_rpc("GetReplicaList", Duration::from_micros(75));

        let summary = metrics.summary();
        assert!(!summary.contains("RPC Metrics Summary"), "{summary}");

        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        assert!(!text.contains("mooncake_client_rpc_count"), "{text}");
        assert!(!text.contains("mooncake_client_rpc_latency"), "{text}");
        assert!(text.contains("mooncake_transfer_read_bytes"), "{text}");
    }

    #[test]
    fn summary_is_compact_and_preserves_count_and_latency_bounds() {
        let metrics = ClientMetrics::new(HashMap::new(), true, true).unwrap();
        metrics.observe_transfer_bytes(TransferOperationKind::Read, 1024 * 1024);
        metrics.observe_get(1024 * 1024, Duration::from_micros(200));
        metrics.observe_rpc("GetReplicaList", Duration::from_micros(250));

        let summary = metrics.summary();
        let serialized =
            String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();

        assert!(summary.len() < serialized.len());
        assert!(summary.contains("count="), "{summary}");
        assert!(summary.contains("p95<"), "{summary}");
        assert!(summary.contains("max<"), "{summary}");
    }

    #[test]
    fn prometheus_applies_all_static_labels_to_every_metric_family() {
        let labels = HashMap::from([
            ("instance_id".to_string(), "12345".to_string()),
            ("cluster_id".to_string(), "cluster1".to_string()),
            ("replica_id".to_string(), "replica1".to_string()),
            ("mount_segment_id".to_string(), "mount1".to_string()),
        ]);
        let metrics = ClientMetrics::new(labels, true, true).unwrap();
        metrics.observe_transfer_bytes(TransferOperationKind::Read, 1024);
        metrics.observe_get(1024, Duration::from_micros(200));
        metrics.observe_rpc("GetReplicaList", Duration::from_micros(250));

        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        for family in [
            "mooncake_transfer_read_bytes",
            "mooncake_transfer_get_latency",
            "mooncake_client_rpc_count",
            "mooncake_client_rpc_latency",
        ] {
            let line = text
                .lines()
                .find(|line| line.starts_with(family) && !line.starts_with("#"))
                .unwrap_or_else(|| panic!("missing metric family {family}: {text}"));
            for label in [
                "instance_id=\"12345\"",
                "cluster_id=\"cluster1\"",
                "replica_id=\"replica1\"",
                "mount_segment_id=\"mount1\"",
            ] {
                assert!(line.contains(label), "{family} lacks {label}: {line}");
            }
        }
    }

    #[test]
    fn prometheus_without_static_labels_omits_dynamic_label_names() {
        let metrics = ClientMetrics::new(HashMap::new(), true, true).unwrap();
        metrics.observe_transfer_bytes(TransferOperationKind::Read, 1024);
        metrics.observe_rpc("GetReplicaList", Duration::from_micros(250));

        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        for label_name in [
            "instance_id",
            "cluster_id",
            "replica_id",
            "mount_segment_id",
        ] {
            assert!(
                !text.contains(label_name),
                "unexpected {label_name}: {text}"
            );
        }
    }

    #[test]
    fn bandwidth_environment_flag_disables_throughput_summary() {
        let enabled = parse_bool_value("MC_STORE_CLIENT_METRIC_BANDWIDTH", Some("0"), true);
        let metrics = ClientMetrics::new(HashMap::new(), enabled, true).unwrap();
        metrics.observe_transfer_bytes(TransferOperationKind::Read, 1024);

        let summary = metrics.summary();
        assert!(!summary.contains("Average Read Throughput:"), "{summary}");
        assert!(!summary.contains("Average Write Throughput:"), "{summary}");
    }

    #[test]
    fn transfer_summary_covers_empty_totals_throughput_and_latency_counts() {
        let metrics = ClientMetrics::new(HashMap::new(), true, true).unwrap();

        let empty = metrics.summary();
        assert!(empty.contains("Total Read: 0 B"), "{empty}");
        assert!(empty.contains("Total Write: 0 B"), "{empty}");
        assert!(empty.contains("Get: No data"), "{empty}");
        assert!(empty.contains("Put: No data"), "{empty}");

        metrics.observe_transfer_bytes(TransferOperationKind::Read, 1024);
        metrics.observe_transfer_bytes(TransferOperationKind::Write, 2 * 1024 * 1024);
        for latency_us in [150, 200, 300] {
            metrics.observe_get(0, Duration::from_micros(latency_us));
        }
        for latency_us in [500, 750] {
            metrics.observe_put(0, Duration::from_micros(latency_us));
        }

        let summary = metrics.summary();
        assert!(summary.contains("Total Read: 1.00 KB"), "{summary}");
        assert!(summary.contains("Total Write: 2.00 MB"), "{summary}");
        assert!(summary.contains("Average Read Throughput:"), "{summary}");
        assert!(summary.contains("Average Write Throughput:"), "{summary}");
        assert!(summary.contains("Get: count=3"), "{summary}");
        assert!(summary.contains("Put: count=2"), "{summary}");
        assert!(summary.contains("p95<"), "{summary}");
        assert!(summary.contains("max<"), "{summary}");
    }

    #[test]
    fn reporting_interval_defaults_to_disabled_and_rejects_invalid_values() {
        assert_eq!(parse_metrics_interval(None), Duration::ZERO);
        assert_eq!(parse_metrics_interval(Some("0")), Duration::ZERO);
        assert_eq!(parse_metrics_interval(Some("15")), Duration::from_secs(15));
        assert_eq!(parse_metrics_interval(Some("-1")), Duration::ZERO);
        assert_eq!(parse_metrics_interval(Some("invalid")), Duration::ZERO);
    }

    fn fresh_metrics() -> ClientMetrics {
        ClientMetrics::new(HashMap::new(), true, true).unwrap()
    }

    // InitialValuesTest: a fresh ClientMetrics has every SSD counter at zero.
    #[test]
    fn cpp_parity_ssd_metrics_initial_values_are_zero() {
        let metrics = fresh_metrics();
        assert_eq!(metrics.ssd_read_bytes.get(), 0);
        assert_eq!(metrics.ssd_write_bytes.get(), 0);
        assert_eq!(metrics.ssd_read_ops.get(), 0);
        assert_eq!(metrics.ssd_write_ops.get(), 0);
        assert_eq!(metrics.ssd_total_bytes.get(), 0);
        assert_eq!(metrics.ssd_total_ops.get(), 0);
    }

    // ReadMetricsTest: reads accumulate bytes/ops without touching writes.
    #[test]
    fn cpp_parity_ssd_read_metrics_accumulate_without_writes() {
        let metrics = fresh_metrics();
        metrics.observe_ssd_read(1024 * 1024, 3, Duration::from_micros(1500));
        metrics.observe_ssd_read(2 * 1024 * 1024, 5, Duration::from_micros(3000));

        assert_eq!(metrics.ssd_read_bytes.get(), 3 * 1024 * 1024);
        assert_eq!(metrics.ssd_read_ops.get(), 8);
        assert_eq!(metrics.ssd_write_bytes.get(), 0);
        assert_eq!(metrics.ssd_write_ops.get(), 0);
        assert_eq!(metrics.ssd_total_bytes.get(), 3 * 1024 * 1024);
        assert_eq!(metrics.ssd_total_ops.get(), 8);
    }

    // WriteMetricsTest: writes accumulate and leave reads at zero.
    #[test]
    fn cpp_parity_ssd_write_metrics_leave_reads_zero() {
        let metrics = fresh_metrics();
        metrics.observe_ssd_write(5 * 1024 * 1024, 10, Duration::from_micros(50_000));

        assert_eq!(metrics.ssd_write_bytes.get(), 5 * 1024 * 1024);
        assert_eq!(metrics.ssd_write_ops.get(), 10);
        assert_eq!(metrics.ssd_read_bytes.get(), 0);
        assert_eq!(metrics.ssd_read_ops.get(), 0);
    }

    // TotalMetricsTest: total counters aggregate reads and writes.
    #[test]
    fn cpp_parity_ssd_total_metrics_aggregate_read_and_write() {
        let metrics = fresh_metrics();
        metrics.observe_ssd_read(1024 * 1024, 3, Duration::from_micros(1500));
        metrics.observe_ssd_write(2 * 1024 * 1024, 4, Duration::from_micros(2500));

        assert_eq!(metrics.ssd_total_bytes.get(), 3 * 1024 * 1024);
        assert_eq!(metrics.ssd_total_ops.get(), 7);
    }

    // FailureNotCountedTest: a failed operation contributes nothing, and the
    // following successful 4-KiB read is the only recorded operation.
    #[test]
    fn cpp_parity_ssd_failure_is_not_counted_before_success() {
        let metrics = fresh_metrics();
        assert_eq!(metrics.ssd_read_ops.get(), 0);
        assert_eq!(metrics.ssd_read_bytes.get(), 0);
        assert_eq!(metrics.ssd_total_ops.get(), 0);
        assert_eq!(metrics.ssd_total_bytes.get(), 0);

        metrics.observe_ssd_read(4096, 1, Duration::from_micros(100));

        assert_eq!(metrics.ssd_read_ops.get(), 1);
        assert_eq!(metrics.ssd_read_bytes.get(), 4096);
        assert_eq!(metrics.ssd_write_ops.get(), 0);
        assert_eq!(metrics.ssd_write_bytes.get(), 0);
        assert_eq!(metrics.ssd_total_ops.get(), 1);
        assert_eq!(metrics.ssd_total_bytes.get(), 4096);
    }

    // ConcurrentTest: eight threads of concurrent observations produce exact
    // aggregate totals.
    #[test]
    fn cpp_parity_ssd_concurrent_observations_have_exact_totals() {
        let metrics = Arc::new(fresh_metrics());
        let mut workers = Vec::new();
        for _ in 0..8 {
            let metrics = Arc::clone(&metrics);
            workers.push(std::thread::spawn(move || {
                for _ in 0..1_000 {
                    metrics.observe_ssd_read(4096, 1, Duration::from_micros(500));
                    metrics.observe_ssd_write(8192, 1, Duration::from_micros(1_000));
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(metrics.ssd_read_ops.get(), 8_000);
        assert_eq!(metrics.ssd_read_bytes.get(), 8_000 * 4096);
        assert_eq!(metrics.ssd_write_ops.get(), 8_000);
        assert_eq!(metrics.ssd_write_bytes.get(), 8_000 * 8192);
        assert_eq!(metrics.ssd_total_ops.get(), 16_000);
        assert_eq!(metrics.ssd_total_bytes.get(), 8_000 * (4096 + 8192));
    }

    // IntegrationWithClientMetric: the SSD read family renders in aggregate
    // Prometheus text.
    #[test]
    fn cpp_parity_client_metrics_integrates_ssd_read() {
        let metrics = fresh_metrics();
        metrics.observe_ssd_read(1024 * 1024, 10, Duration::from_micros(1500));

        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        assert!(text.contains("mooncake_ssd_read_bytes_total"));
        assert!(text.contains("mooncake_ssd_read_ops_total"));
        assert!(text.contains("mooncake_ssd_read_latency_us"));
    }

    // SerializeWithDynamicLabels: static labels appear on an SSD family.
    #[test]
    fn cpp_parity_ssd_prometheus_serializes_dynamic_labels() {
        let metrics = ClientMetrics::new(
            HashMap::from([("instance_id".to_string(), "test123".to_string())]),
            true,
            true,
        )
        .unwrap();
        metrics.observe_ssd_read(128, 1, Duration::from_micros(100));

        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        assert!(text.contains("mooncake_ssd_read_bytes_total"));
        assert!(text.lines().any(|line| {
            line.starts_with("mooncake_ssd_read_latency_summary_us{")
                && line.contains("instance_id=\"test123\"")
        }));
    }

    #[test]
    fn ssd_latency_summary_retains_bounded_quantile_samples() {
        let summary = SsdLatencySummary::new(
            "bounded_ssd_latency_summary_us",
            "bounded summary test",
            HashMap::new(),
        );
        for value in 0..4_097 {
            summary.observe(value as f64);
        }

        let state = summary.samples.read();
        assert_eq!(state.samples.len(), SSD_LATENCY_SUMMARY_SAMPLE_CAPACITY);
        assert_eq!(state.sample_count, 4_097);
        assert_eq!(
            state.sample_sum,
            (0..4_097).map(|value| value as f64).sum::<f64>()
        );
    }

    // LatencyBucketBoundaryTest: 25/50/30,000,000/60,000,000 us land in the
    // expected buckets including the +Inf overflow.
    #[test]
    fn cpp_parity_ssd_latency_bucket_boundaries() {
        let metrics = fresh_metrics();
        metrics.observe_ssd_read(0, 0, Duration::from_micros(25));
        metrics.observe_ssd_read(0, 0, Duration::from_micros(50));
        metrics.observe_ssd_read(0, 0, Duration::from_micros(30_000_000));
        metrics.observe_ssd_read(0, 0, Duration::from_micros(60_000_000));

        let histogram = &metrics.ssd_read_latency_us;
        assert_eq!(histogram.get_sample_count(), 4);
        assert_eq!(histogram.get_sample_sum(), 90_000_075.0);
        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        assert!(text.contains("mooncake_ssd_read_latency_us_bucket{le=\"50\"} 2"));
        assert!(
            text.contains("mooncake_ssd_read_latency_us_bucket{le=\"3e+07\"} 3")
                || text.contains("mooncake_ssd_read_latency_us_bucket{le=\"30000000\"} 3")
        );
        assert!(text.contains("mooncake_ssd_read_latency_us_bucket{le=\"+Inf\"} 4"));
    }

    // EmptyBatchMetricsTest: zero bytes/keys still record latency only.
    #[test]
    fn cpp_parity_empty_ssd_batch_records_latency_only() {
        let metrics = fresh_metrics();
        metrics.observe_ssd_read(0, 0, Duration::from_micros(10));

        assert_eq!(metrics.ssd_read_ops.get(), 0);
        assert_eq!(metrics.ssd_read_bytes.get(), 0);
        assert_eq!(metrics.ssd_read_latency_us.get_sample_count(), 1);
    }

    // SerializeTest: every C++-asserted SSD Prometheus family name appears,
    // including the latency summary family.
    #[test]
    fn cpp_parity_ssd_serialization_contains_cpp_metric_names() {
        let metrics = fresh_metrics();
        metrics.observe_ssd_read(1024, 5, Duration::from_micros(200));
        metrics.observe_ssd_write(2048, 3, Duration::from_micros(1000));

        let text = String::from_utf8(metrics.render_prometheus(true, false).unwrap()).unwrap();
        for name in [
            "mooncake_ssd_read_bytes_total",
            "mooncake_ssd_write_bytes_total",
            "mooncake_ssd_read_ops_total",
            "mooncake_ssd_write_ops_total",
            "mooncake_ssd_read_latency_us",
            "mooncake_ssd_write_latency_us",
            "mooncake_ssd_total_bytes_total",
            "mooncake_ssd_total_ops_total",
            "mooncake_ssd_total_latency_us",
            "mooncake_ssd_read_latency_summary_us",
        ] {
            assert!(text.contains(name), "missing family {name}");
        }
    }

    // SummaryMetricsTest: the SSD summary omits throughput/IOPS when empty and
    // emits them with percentiles once data exists.
    #[test]
    fn cpp_parity_ssd_summary_has_three_throughput_and_iops_sections() {
        let metrics = fresh_metrics();
        let empty = metrics.summary();
        assert!(empty.contains("SSD Metrics Summary"));
        assert!(empty.contains("SSD Read: 0 B"));
        assert!(empty.contains("SSD Write: 0 B"));
        assert!(empty.contains("Read: No data"));
        assert!(empty.contains("Write: No data"));
        assert!(!empty.contains("throughput="));
        assert!(!empty.contains("IOPS="));

        metrics.observe_ssd_read(5 * 1024 * 1024, 100, Duration::from_micros(500));
        metrics.observe_ssd_write(10 * 1024 * 1024, 50, Duration::from_micros(2000));
        let summary = metrics.summary();
        assert!(summary.contains("SSD Read: 5.00 MB"), "{summary}");
        assert!(summary.contains("SSD Write: 10.00 MB"), "{summary}");
        assert!(summary.contains("ops=100"), "{summary}");
        assert!(summary.contains("ops=50"), "{summary}");
        assert!(summary.contains("throughput="), "{summary}");
        assert!(summary.contains("/s"), "{summary}");
        assert!(summary.contains("IOPS="), "{summary}");
        assert!(summary.contains("p50<"), "{summary}");
        assert!(summary.contains("p90<"), "{summary}");
        assert!(summary.contains("p99<"), "{summary}");
    }

    // ThroughputCalculationTest: the pure ssd_metrics_line seam produces exact
    // throughput and IOPS for a deterministic elapsed time.
    #[test]
    fn cpp_parity_ssd_summary_has_three_throughput_and_iops_sections_deterministic() {
        let read = ssd_metrics_line("Read", 5 * 1024 * 1024, 100, 1.0);
        assert!(read.contains("SSD Read: 5.00 MB, ops=100"), "{read}");
        assert!(read.contains("throughput=5.00 MB/s"), "{read}");
        assert!(read.contains("IOPS=100"), "{read}");

        let write = ssd_metrics_line("Write", 10 * 1024 * 1024, 50, 2.0);
        assert!(write.contains("throughput=5.00 MB/s"), "{write}");
        assert!(write.contains("IOPS=25"), "{write}");

        let total = ssd_metrics_line("Total", 0, 0, 1.0);
        assert!(total.contains("SSD Total: 0 B"));
        assert!(!total.contains("throughput="));
        assert!(!total.contains("IOPS="));
    }
}

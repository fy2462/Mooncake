use axum::{routing::get, Router};
use lazy_static::lazy_static;
use prometheus::{Encoder, IntCounter, IntGauge, TextEncoder};
use std::net::SocketAddr;

lazy_static! {
    pub static ref PUT_COUNTER: IntCounter =
        IntCounter::new("mooncake_store_put_total", "total put operations").unwrap();
    pub static ref GET_COUNTER: IntCounter =
        IntCounter::new("mooncake_store_get_total", "total get operations").unwrap();
    pub static ref REMOVE_COUNTER: IntCounter =
        IntCounter::new("mooncake_store_remove_total", "total remove operations").unwrap();
    pub static ref PING_COUNTER: IntCounter =
        IntCounter::new("mooncake_store_ping_total", "total client pings").unwrap();
    pub static ref SEGMENT_COUNT: IntGauge =
        IntGauge::new("mooncake_store_segments", "number of mounted segments").unwrap();
    pub static ref OBJECT_COUNT: IntGauge =
        IntGauge::new("mooncake_store_objects", "number of stored objects").unwrap();
    pub static ref ERROR_COUNTER: IntCounter =
        IntCounter::new("mooncake_store_errors_total", "total error count").unwrap();
}

pub fn register_metrics() {
    prometheus::register(Box::new(PUT_COUNTER.clone())).ok();
    prometheus::register(Box::new(GET_COUNTER.clone())).ok();
    prometheus::register(Box::new(REMOVE_COUNTER.clone())).ok();
    prometheus::register(Box::new(PING_COUNTER.clone())).ok();
    prometheus::register(Box::new(SEGMENT_COUNT.clone())).ok();
    prometheus::register(Box::new(OBJECT_COUNT.clone())).ok();
    prometheus::register(Box::new(ERROR_COUNTER.clone())).ok();
}

/// Start the Prometheus metrics HTTP endpoint.
pub async fn serve_metrics_http(addr: SocketAddr) {
    register_metrics();

    let app = Router::new().route("/metrics", get(metrics_handler));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};
use parking_lot::RwLock;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::metrics::ClientMetrics;

pub const DEFAULT_CLIENT_HTTP_PORT: u16 = 9300;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientHttpConfig {
    pub enabled: bool,
    pub port: u16,
}

impl Default for ClientHttpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: DEFAULT_CLIENT_HTTP_PORT,
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct ClientHttpSnapshot {
    healthy: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    metrics: Option<Arc<ClientMetrics>>,
}

impl ClientHttpSnapshot {
    pub(super) fn new(
        healthy: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
        metrics: Option<Arc<ClientMetrics>>,
    ) -> Self {
        Self {
            healthy,
            closed,
            metrics,
        }
    }
}

struct RunningClientHttpServer {
    handle: tokio::task::JoinHandle<()>,
    port: u16,
}

#[derive(Default)]
pub(super) struct ClientHttpServerState {
    running: RwLock<Option<RunningClientHttpServer>>,
}

impl ClientHttpServerState {
    pub(super) async fn start(&self, config: ClientHttpConfig, snapshot: ClientHttpSnapshot) {
        if !config.enabled || self.running.read().is_some() {
            return;
        }

        let listener = match tokio::net::TcpListener::bind(("0.0.0.0", config.port)).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::warn!(port = config.port, %error, "failed to start client HTTP server; continuing without endpoints");
                return;
            }
        };
        let port = match listener.local_addr() {
            Ok(address) => address.port(),
            Err(error) => {
                tracing::warn!(%error, "failed to read client HTTP listener address");
                return;
            }
        };
        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/metrics", get(metrics_handler))
            .route("/metrics/summary", get(metrics_summary_handler))
            .with_state(snapshot);
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::warn!(%error, "client HTTP server stopped with an error");
            }
        });

        let mut running = self.running.write();
        if running.is_some() {
            handle.abort();
            return;
        }
        *running = Some(RunningClientHttpServer { handle, port });
    }

    pub(super) fn stop(&self) {
        if let Some(server) = self.running.write().take() {
            server.handle.abort();
        }
    }

    pub(super) fn port(&self) -> Option<u16> {
        self.running.read().as_ref().map(|server| server.port)
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    code: u8,
}

async fn health_handler(State(snapshot): State<ClientHttpSnapshot>) -> impl IntoResponse {
    let (http_status, status, code) = if snapshot.closed.load(Ordering::SeqCst) {
        (StatusCode::SERVICE_UNAVAILABLE, "not_initialized", 1)
    } else if snapshot.healthy.load(Ordering::SeqCst) {
        (StatusCode::OK, "healthy", 0)
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "master_unreachable", 2)
    };
    (http_status, Json(HealthResponse { status, code }))
}

async fn metrics_handler(State(snapshot): State<ClientHttpSnapshot>) -> impl IntoResponse {
    let Some(metrics) = snapshot.metrics else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "text/plain".to_string())],
            b"metrics not available".to_vec(),
        );
    };
    let content_type = metrics.prometheus_content_type();
    let (status, body) = match metrics.render_prometheus(
        snapshot.healthy.load(Ordering::SeqCst),
        snapshot.closed.load(Ordering::SeqCst),
    ) {
        Ok(body) => (StatusCode::OK, body),
        Err(error) => {
            tracing::warn!(%error, "failed to encode client metrics");
            (StatusCode::SERVICE_UNAVAILABLE, Vec::new())
        }
    };
    (status, [(header::CONTENT_TYPE, content_type)], body)
}

async fn metrics_summary_handler(State(snapshot): State<ClientHttpSnapshot>) -> impl IntoResponse {
    let Some(metrics) = snapshot.metrics else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "text/plain")],
            "metrics not available".to_string(),
        );
    };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain")],
        metrics.summary(),
    )
}

#[cfg(test)]
mod tests {
    use super::{ClientHttpConfig, ClientHttpServerState, ClientHttpSnapshot};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn get(port: u16, path: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn disabled_server_does_not_bind() {
        let state = ClientHttpServerState::default();

        state
            .start(ClientHttpConfig::default(), ClientHttpSnapshot::default())
            .await;

        assert_eq!(state.port(), None);
    }

    #[tokio::test]
    async fn health_and_prometheus_metrics_reflect_shared_state() {
        let healthy = Arc::new(AtomicBool::new(true));
        let closed = Arc::new(AtomicBool::new(false));
        let metrics =
            super::ClientMetrics::new(std::collections::HashMap::new(), true, true).unwrap();
        let snapshot =
            ClientHttpSnapshot::new(healthy.clone(), closed.clone(), Some(Arc::new(metrics)));
        let state = ClientHttpServerState::default();
        state
            .start(
                ClientHttpConfig {
                    enabled: true,
                    port: 0,
                },
                snapshot,
            )
            .await;
        let port = state.port().expect("server should bind an ephemeral port");

        let health = get(port, "/health").await;
        assert!(health.starts_with("HTTP/1.1 200"), "{health}");
        assert!(
            health.contains(r#"{"status":"healthy","code":0}"#),
            "{health}"
        );

        let metrics = get(port, "/metrics").await;
        assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
        assert!(metrics.contains("mooncake_client_healthy 1"), "{metrics}");
        let summary = get(port, "/metrics/summary").await;
        assert!(summary.starts_with("HTTP/1.1 200"), "{summary}");
        assert!(summary.contains("Client Metrics Summary"), "{summary}");

        healthy.store(false, Ordering::SeqCst);
        let unhealthy = get(port, "/health").await;
        assert!(unhealthy.starts_with("HTTP/1.1 503"), "{unhealthy}");
        assert!(unhealthy.contains(r#"{"status":"master_unreachable","code":2}"#));

        closed.store(true, Ordering::SeqCst);
        let closed_response = get(port, "/health").await;
        assert!(closed_response.contains(r#"{"status":"not_initialized","code":1}"#));
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_503_when_metrics_are_disabled() {
        let state = ClientHttpServerState::default();
        state
            .start(
                ClientHttpConfig {
                    enabled: true,
                    port: 0,
                },
                ClientHttpSnapshot::default(),
            )
            .await;
        let port = state.port().expect("server should bind an ephemeral port");

        let response = get(port, "/metrics").await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.contains("metrics not available"), "{response}");
    }

    #[tokio::test]
    async fn occupied_port_and_duplicate_start_do_not_replace_the_running_server() {
        // Bind the same wildcard interface used by ClientHttpServerState.
        // On macOS a loopback-only listener does not reliably conflict with a
        // later wildcard bind to the same port.
        let occupied = tokio::net::TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
        let occupied_port = occupied.local_addr().unwrap().port();
        let state = ClientHttpServerState::default();

        state
            .start(
                ClientHttpConfig {
                    enabled: true,
                    port: occupied_port,
                },
                ClientHttpSnapshot::default(),
            )
            .await;
        assert_eq!(state.port(), None);

        drop(occupied);
        state
            .start(
                ClientHttpConfig {
                    enabled: true,
                    port: occupied_port,
                },
                ClientHttpSnapshot::default(),
            )
            .await;
        assert_eq!(state.port(), Some(occupied_port));

        state
            .start(
                ClientHttpConfig {
                    enabled: true,
                    port: 0,
                },
                ClientHttpSnapshot::default(),
            )
            .await;
        assert_eq!(state.port(), Some(occupied_port));
    }
}

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use mooncake_store_client::proto::{self, master_service_client::MasterServiceClient};
use mooncake_store_client::{
    DistributedMissHandler, MissHandler, RemoteSource, RemoteSourceConfig, RemoteSourceError,
    RemoteSourceResult,
};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{Body, BoxFuture, Service, StdError, http};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Default)]
struct CoordinationState {
    pending: Mutex<HashSet<(String, String)>>,
    acquired: Mutex<Vec<(String, String)>>,
    completed: Mutex<Vec<(String, String)>>,
    events: Mutex<Vec<&'static str>>,
}

#[derive(Clone)]
struct MockMaster {
    state: Arc<CoordinationState>,
}

struct AcquireRemotePull(Arc<CoordinationState>);

impl tonic::server::UnaryService<proto::AcquireRemotePullRequest> for AcquireRemotePull {
    type Response = proto::AcquireRemotePullResponse;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<proto::AcquireRemotePullRequest>) -> Self::Future {
        let state = Arc::clone(&self.0);
        Box::pin(async move {
            let request = request.into_inner();
            let identity = (request.tenant_id, request.key);
            state.acquired.lock().unwrap().push(identity.clone());
            let action = if state.pending.lock().unwrap().insert(identity) {
                proto::RemotePullAction::Pull
            } else {
                proto::RemotePullAction::Wait
            };
            Ok(Response::new(proto::AcquireRemotePullResponse {
                action: action as i32,
                retry_after_ms: 0,
            }))
        })
    }
}

struct CompleteRemotePull(Arc<CoordinationState>);

impl tonic::server::UnaryService<proto::CompleteRemotePullRequest> for CompleteRemotePull {
    type Response = proto::CompleteRemotePullResponse;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<proto::CompleteRemotePullRequest>) -> Self::Future {
        let state = Arc::clone(&self.0);
        Box::pin(async move {
            let request = request.into_inner();
            let identity = (request.tenant_id, request.key);
            state.events.lock().unwrap().push("complete");
            state.completed.lock().unwrap().push(identity.clone());
            state.pending.lock().unwrap().remove(&identity);
            Ok(Response::new(proto::CompleteRemotePullResponse {}))
        })
    }
}

impl<B> Service<http::Request<B>> for MockMaster
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let state = Arc::clone(&self.state);
        match request.uri().path() {
            "/mooncake.store.MasterService/AcquireRemotePull" => Box::pin(async move {
                let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                Ok(grpc.unary(AcquireRemotePull(state), request).await)
            }),
            "/mooncake.store.MasterService/CompleteRemotePull" => Box::pin(async move {
                let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                Ok(grpc.unary(CompleteRemotePull(state), request).await)
            }),
            _ => Box::pin(async move {
                let mut response = http::Response::new(tonic::body::empty_body());
                response.headers_mut().insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                Ok(response)
            }),
        }
    }
}

impl tonic::server::NamedService for MockMaster {
    const NAME: &'static str = "mooncake.store.MasterService";
}

struct MemorySource {
    values: Mutex<HashMap<String, Vec<u8>>>,
    called: mpsc::UnboundedSender<()>,
    release: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl RemoteSource for MemorySource {
    async fn get(&self, key: &str) -> RemoteSourceResult<Vec<u8>> {
        self.called.send(()).unwrap();
        self.release.acquire().await.unwrap().forget();
        self.values
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| RemoteSourceError::NotFound(key.to_string()))
    }
}

async fn strict_remote_pull_master() -> (
    MasterServiceClient<Channel>,
    Arc<CoordinationState>,
    tokio::task::JoinHandle<()>,
) {
    let state = Arc::new(CoordinationState::default());
    let service = MockMaster {
        state: Arc::clone(&state),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let client = MasterServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    (client, state, server)
}

fn handler_for_tenant(
    master: MasterServiceClient<Channel>,
    tenant_id: &str,
) -> (
    Arc<DistributedMissHandler<MemorySource>>,
    mpsc::UnboundedReceiver<()>,
    Arc<Semaphore>,
) {
    let (called, calls) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let source = MemorySource {
        values: Mutex::new(HashMap::from([(
            "shared-key".to_string(),
            tenant_id.as_bytes().to_vec(),
        )])),
        called,
        release: Arc::clone(&release),
    };
    let handler = DistributedMissHandler::new_for_tenant(
        MissHandler::new(
            source,
            RemoteSourceConfig {
                enabled: true,
                ..Default::default()
            },
        ),
        master,
        Uuid::new_v4(),
        tenant_id,
    )
    .with_wait_policy(1, Duration::from_secs(5));
    (Arc::new(handler), calls, release)
}

#[tokio::test]
async fn fetched_data_is_published_before_remote_pull_completion() {
    let (master, state, server) = strict_remote_pull_master().await;
    let (handler, _calls, release) = handler_for_tenant(master, "tenant-a");
    release.add_permits(1);
    let callback_state = Arc::clone(&state);

    let data = handler
        .handle_miss("shared-key", move |_| {
            callback_state.events.lock().unwrap().push("callback");
        })
        .await
        .unwrap();

    assert_eq!(data, b"tenant-a");
    assert_eq!(
        state.events.lock().unwrap().as_slice(),
        ["callback", "complete"]
    );
    server.abort();
}

#[tokio::test]
async fn strict_tenants_coordinate_same_user_key_independently_and_complete_in_scope() {
    let (master, state, server) = strict_remote_pull_master().await;
    let (tenant_a, mut calls_a, release_a) = handler_for_tenant(master.clone(), "tenant-a");
    let (tenant_b, mut calls_b, release_b) = handler_for_tenant(master, "tenant-b");

    let first_a = {
        let tenant_a = Arc::clone(&tenant_a);
        tokio::spawn(async move { tenant_a.handle_miss("shared-key", |_| {}).await })
    };
    tokio::time::timeout(Duration::from_millis(250), calls_a.recv())
        .await
        .expect("tenant A should acquire its pull immediately")
        .unwrap();

    let first_b = {
        let tenant_b = Arc::clone(&tenant_b);
        tokio::spawn(async move { tenant_b.handle_miss("shared-key", |_| {}).await })
    };
    tokio::time::timeout(Duration::from_millis(250), calls_b.recv())
        .await
        .expect("tenant B should acquire the same user key independently")
        .unwrap();

    release_a.add_permits(1);
    release_b.add_permits(1);
    assert_eq!(first_a.await.unwrap().unwrap(), b"tenant-a");
    assert_eq!(first_b.await.unwrap().unwrap(), b"tenant-b");

    // Each completion must clear only its own tenant-scoped coordination key.
    let second_a = {
        let tenant_a = Arc::clone(&tenant_a);
        tokio::spawn(async move { tenant_a.handle_miss("shared-key", |_| {}).await })
    };
    let second_b = {
        let tenant_b = Arc::clone(&tenant_b);
        tokio::spawn(async move { tenant_b.handle_miss("shared-key", |_| {}).await })
    };
    tokio::time::timeout(Duration::from_millis(250), calls_a.recv())
        .await
        .expect("tenant A completion should release tenant A's scoped key")
        .unwrap();
    tokio::time::timeout(Duration::from_millis(250), calls_b.recv())
        .await
        .expect("tenant B completion should release tenant B's scoped key")
        .unwrap();
    release_a.add_permits(1);
    release_b.add_permits(1);
    assert_eq!(second_a.await.unwrap().unwrap(), b"tenant-a");
    assert_eq!(second_b.await.unwrap().unwrap(), b"tenant-b");

    let mut acquired = state.acquired.lock().unwrap().clone();
    acquired.sort();
    assert_eq!(
        acquired,
        vec![
            ("tenant-a".into(), "shared-key".into()),
            ("tenant-a".into(), "shared-key".into()),
            ("tenant-b".into(), "shared-key".into()),
            ("tenant-b".into(), "shared-key".into()),
        ]
    );
    let mut completed = state.completed.lock().unwrap().clone();
    completed.sort();
    assert_eq!(
        completed,
        vec![
            ("tenant-a".into(), "shared-key".into()),
            ("tenant-a".into(), "shared-key".into()),
            ("tenant-b".into(), "shared-key".into()),
            ("tenant-b".into(), "shared-key".into()),
        ]
    );

    server.abort();
}

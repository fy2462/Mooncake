//! P2P offload read server — handles offload read requests from peers.
//! C++ equivalent: `RealClient::batch_get_offload_object` handler + coro_rpc server.

use std::sync::Arc;
use std::time::Instant;

use super::buffer::OffloadBufferPool;
use crate::local_storage_backend::{AttachedLocalStorage, local_storage_key};
use crate::memory_ffi::RemoteReadableRegistration;
use crate::offload_proto::offload_read_service_server::{
    OffloadReadService, OffloadReadServiceServer,
};
use crate::offload_proto::{
    BatchGetOffloadObjectRequest, BatchGetOffloadObjectResponse, ReleaseOffloadBufferRequest,
    ReleaseOffloadBufferResponse,
};
use tonic::{Request, Response, Status};

/// gRPC handler implementing the OffloadReadService.
pub(crate) struct OffloadReadHandler {
    pub storage: AttachedLocalStorage,
    pub engine: Arc<transfer_engine_ffi::TransferEngine>,
    pub pool: Arc<OffloadBufferPool>,
    pub te_endpoint: String,
}

#[tonic::async_trait]
impl OffloadReadService for OffloadReadHandler {
    /// Handle a batch offload read request from a peer.
    /// Reads keys from local SSD, registers buffers with TE, returns pointers.
    ///
    /// C++ equivalent: `RealClient::batch_get_offload_object`
    async fn batch_get_offload_object(
        &self,
        request: Request<BatchGetOffloadObjectRequest>,
    ) -> Result<Response<BatchGetOffloadObjectResponse>, Status> {
        let req = request.into_inner();
        let n = req.keys.len();
        if n == 0 {
            return Err(Status::invalid_argument(
                "offload request must contain at least one key",
            ));
        }
        if req.sizes.len() != n {
            return Err(Status::invalid_argument(format!(
                "sizes length {} does not match keys length {}",
                req.sizes.len(),
                n
            )));
        }
        if !req.tenant_ids.is_empty() && req.tenant_ids.len() != n {
            return Err(Status::invalid_argument(format!(
                "tenant_ids length {} does not match keys length {}",
                req.tenant_ids.len(),
                n
            )));
        }

        let mut expected_sizes = Vec::with_capacity(n);
        let mut total_bytes = 0usize;
        for (key, size) in req.keys.iter().zip(&req.sizes) {
            if key.is_empty() {
                return Err(Status::invalid_argument(
                    "offload request keys must not be empty",
                ));
            }
            let size = usize::try_from(*size).map_err(|_| {
                Status::invalid_argument(format!(
                    "offload size for key {key} is negative or exceeds the local address space"
                ))
            })?;
            if size == 0 {
                return Err(Status::invalid_argument(format!(
                    "offload size for key {key} must be positive"
                )));
            }
            total_bytes = total_bytes.checked_add(size).ok_or_else(|| {
                Status::invalid_argument("offload request aggregate size overflows usize")
            })?;
            expected_sizes.push(size);
        }

        let reservation = self
            .pool
            .try_reserve(total_bytes)
            .map_err(Status::resource_exhausted)?;
        let storage = self.storage.clone();
        let engine = Arc::clone(&self.engine);
        let keys = req.keys;
        let tenant_ids = req.tenant_ids;
        let (batch_id, pointers) = tokio::task::spawn_blocking(move || {
            let mut registrations = Vec::with_capacity(keys.len());
            for (index, ((key, expected_size), tenant_id)) in keys
                .iter()
                .zip(&expected_sizes)
                .zip(
                    tenant_ids
                        .iter()
                        .map(String::as_str)
                        .chain(std::iter::repeat("")),
                )
                .take(keys.len())
                .enumerate()
            {
                let storage_key = local_storage_key(tenant_id, key);
                let data = storage
                    .read_object(&storage_key)
                    .map_err(|error| Status::internal(format!("read {key} failed: {error}")))?;
                if data.len() != *expected_size {
                    return Err(Status::failed_precondition(format!(
                        "offload object {key} at index {index} has {} bytes, expected {expected_size}",
                        data.len()
                    )));
                }
                let registration = RemoteReadableRegistration::register(&engine, data)
                    .map_err(|error| {
                        Status::internal(format!("register offload object {key}: {error}"))
                    })?;
                registrations.push(registration);
            }
            reservation.commit(registrations).map_err(Status::internal)
        })
        .await
        .map_err(|error| Status::internal(format!("offload worker failed: {error}")))??;

        let response = BatchGetOffloadObjectResponse {
            batch_id,
            pointers,
            transfer_engine_addr: self.te_endpoint.clone(),
            gc_ttl_ms: self.pool.ttl_ms(),
        };

        Ok(Response::new(response))
    }

    /// Release a previously-allocated offload batch.
    /// C++ equivalent: `RealClient::release_offload_buffer`
    async fn release_offload_buffer(
        &self,
        request: Request<ReleaseOffloadBufferRequest>,
    ) -> Result<Response<ReleaseOffloadBufferResponse>, Status> {
        let batch_id = request.into_inner().batch_id;
        self.pool.release(batch_id);
        Ok(Response::new(ReleaseOffloadBufferResponse {}))
    }
}

/// Start the offload RPC gRPC server on an auto-allocated port.
/// Returns the bound port and the server handle.
///
/// C++ equivalent: `RealClient` offload_rpc_server_ startup in `setup_internal`.
pub(crate) async fn start_offload_server(
    handler: OffloadReadHandler,
) -> Result<(u16, tokio::task::JoinHandle<()>), String> {
    let pool = Arc::clone(&handler.pool);
    let service = OffloadReadServiceServer::new(handler);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 0));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|error| format!("failed to bind offload server: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| format!("failed to query offload server address: {error}"))?
        .port();
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .map_err(|error| format!("failed to create offload TcpIncoming: {error}"))?;

    let handle = tokio::spawn(async move {
        let server = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming);
        tokio::pin!(server);
        let mut gc = tokio::time::interval(pool.gc_interval());
        gc.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = &mut server => {
                    if let Err(error) = result {
                        tracing::error!("offload RPC server error: {error}");
                    }
                    break;
                }
                _ = gc.tick() => {
                    let released = pool.release_expired(Instant::now());
                    if released != 0 {
                        tracing::debug!("released {released} expired offload batches");
                    }
                }
            }
        }
    });

    Ok((port, handle))
}

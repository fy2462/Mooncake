//! P2P offload read server — handles offload read requests from peers.
//! C++ equivalent: `RealClient::batch_get_offload_object` handler + coro_rpc server.

use std::ffi::c_void;
use std::sync::Arc;

use crate::local_storage_backend::LocalStorageBackend;
use super::buffer::{OffloadBatch, OffloadBufferPool};
use crate::offload_proto::offload_read_service_server::{OffloadReadService, OffloadReadServiceServer};
use crate::offload_proto::{
    BatchGetOffloadObjectRequest, BatchGetOffloadObjectResponse, ReleaseOffloadBufferRequest,
    ReleaseOffloadBufferResponse,
};
use tonic::{Request, Response, Status};

/// Default GC TTL for offload buffers (milliseconds).
/// Peers must complete their RDMA read within this window.
const DEFAULT_GC_TTL_MS: u64 = 30_000;

/// gRPC handler implementing the OffloadReadService.
pub(crate) struct OffloadReadHandler {
    pub storage: Arc<LocalStorageBackend>,
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

        let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(n);
        let mut pointers: Vec<u64> = Vec::with_capacity(n);

        // Read each key from local SSD.
        for i in 0..n {
            let data = self
                .storage
                .read_object(&req.keys[i])
                .map_err(|e| Status::internal(format!("read {} failed: {e}", req.keys[i])))?;
            buffers.push(data);
        }

        // Register buffers with TE so peers can RDMA-read them.
        for buf in &buffers {
            let ptr = buf.as_ptr() as u64;
            // Safety: buffer lives for the lifetime of the batch.
            unsafe {
                self.engine.register_local_memory(
                    buf.as_ptr() as *mut c_void,
                    buf.len(),
                    "cpu:0", // registered on CPU memory
                    true,    // remote accessible
                ).map_err(|e| Status::internal(format!("register memory: {e}")))?;
            }
            pointers.push(ptr);
        }

        let batch = OffloadBatch { buffers };
        let batch_id = self.pool.register(batch);

        let response = BatchGetOffloadObjectResponse {
            batch_id,
            pointers,
            transfer_engine_addr: self.te_endpoint.clone(),
            gc_ttl_ms: DEFAULT_GC_TTL_MS,
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
        if let Some(batch) = self.pool.release(batch_id) {
            // Unregister TE buffers. Drop will free the host memory.
            for buf in &batch.buffers {
                unsafe {
                    let _ = self
                        .engine
                        .unregister_local_memory(buf.as_ptr() as *mut c_void);
                }
            }
        }
        Ok(Response::new(ReleaseOffloadBufferResponse {}))
    }
}

/// Start the offload RPC gRPC server on an auto-allocated port.
/// Returns the bound port and the server handle.
///
/// C++ equivalent: `RealClient` offload_rpc_server_ startup in `setup_internal`.
pub(crate) async fn start_offload_server(
    handler: OffloadReadHandler,
) -> (u16, tokio::task::JoinHandle<()>) {
    let service = OffloadReadServiceServer::new(handler);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 0));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind offload server");
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(
                tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
                    .expect("failed to create TcpIncoming"),
            )
            .await
        {
            tracing::error!("offload RPC server error: {}", e);
        }
    });

    (port, handle)
}

//! P2P offload read client — calls peer's OffloadReadService to read offloaded data.
//! C++ equivalent: `ClientRequester::batch_get_offload_object` +
//! `RealClient::batch_get_into_offload_object_internal`

use crate::offload_proto::offload_read_service_client::OffloadReadServiceClient;
use crate::offload_proto::{
    BatchGetOffloadObjectRequest, BatchGetOffloadObjectResponse, ReleaseOffloadBufferRequest,
};
use tonic::transport::Channel;

/// Result of a batch offload read from a peer.
#[derive(Debug, Clone)]
pub struct BatchOffloadResult {
    pub pointers: Vec<u64>,
    pub transfer_engine_addr: String,
    pub batch_id: u64,
    pub gc_ttl_ms: u64,
}

/// Call a peer's OffloadReadService to read offloaded objects.
///
/// C++ equivalent: `RealClient::batch_get_into_offload_object_internal` (RPC step).
pub async fn batch_get_offload_objects(
    peer_addr: &str,
    keys: &[String],
    sizes: &[i64],
    tenant_ids: &[String],
) -> Result<BatchOffloadResult, String> {
    if peer_addr.is_empty() {
        return Err("peer address must not be empty".to_string());
    }
    if keys.is_empty() {
        return Err("offload request must contain at least one key".to_string());
    }
    if keys.len() != sizes.len() {
        return Err(format!(
            "sizes length {} does not match keys length {}",
            sizes.len(),
            keys.len()
        ));
    }
    if !tenant_ids.is_empty() && tenant_ids.len() != keys.len() {
        return Err(format!(
            "tenant_ids length {} does not match keys length {}",
            tenant_ids.len(),
            keys.len()
        ));
    }
    for (key, size) in keys.iter().zip(sizes) {
        if key.is_empty() {
            return Err("offload request keys must not be empty".to_string());
        }
        if *size <= 0 {
            return Err(format!("offload size for key {key} must be positive"));
        }
    }

    let url = format!("http://{peer_addr}");
    let channel = Channel::from_shared(url)
        .map_err(|e| format!("invalid peer address {peer_addr}: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("connect to {peer_addr}: {e}"))?;

    let mut client = OffloadReadServiceClient::new(channel);

    let request = BatchGetOffloadObjectRequest {
        keys: keys.to_vec(),
        sizes: sizes.to_vec(),
        tenant_ids: tenant_ids.to_vec(),
    };

    let response: BatchGetOffloadObjectResponse = client
        .batch_get_offload_object(request)
        .await
        .map_err(|e| format!("batch_get_offload_object RPC to {peer_addr}: {e}"))?
        .into_inner();
    if response.batch_id == 0 {
        return Err("peer returned invalid offload batch id 0".to_string());
    }
    if response.pointers.len() != keys.len() {
        return Err(format!(
            "peer returned {} pointers for {} offload keys",
            response.pointers.len(),
            keys.len()
        ));
    }
    if response.transfer_engine_addr.is_empty() {
        return Err("peer returned an empty Transfer Engine endpoint".to_string());
    }
    if response.gc_ttl_ms == 0 {
        return Err("peer returned an invalid zero offload GC TTL".to_string());
    }

    Ok(BatchOffloadResult {
        pointers: response.pointers,
        transfer_engine_addr: response.transfer_engine_addr,
        batch_id: response.batch_id,
        gc_ttl_ms: response.gc_ttl_ms,
    })
}

/// Release a batch on the peer (fire-and-forget).
///
/// C++ equivalent: `ClientRequester::release_offload_buffer`
pub async fn release_offload_buffer(peer_addr: &str, batch_id: u64) {
    let url = format!("http://{peer_addr}");
    let channel = match Channel::from_shared(url) {
        Ok(channel) => match channel.connect().await {
            Ok(channel) => channel,
            Err(e) => {
                tracing::warn!("release_offload_buffer: cannot connect to {peer_addr}: {e}");
                return;
            }
        },
        Err(e) => {
            tracing::warn!("release_offload_buffer: cannot connect to {peer_addr}: {e}");
            return;
        }
    };

    let mut client = OffloadReadServiceClient::new(channel);
    let request = ReleaseOffloadBufferRequest { batch_id };
    match client.release_offload_buffer(request).await {
        Ok(_) => {
            tracing::debug!("released offload batch {batch_id} on {peer_addr}");
        }
        Err(e) => {
            // Fire-and-forget: failure is expected (network, GC, etc.)
            tracing::debug!("release_offload_buffer batch {batch_id}: {e}");
        }
    }
}

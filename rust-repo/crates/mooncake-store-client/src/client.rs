use mooncake_store_core::{ReplicaDescriptor, ReplicateConfig, StoreError};
use mooncake_store_core::error::StoreResult;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;
use tonic::transport::Channel;
use transfer_engine_ffi::{
    Opcode, TransferEngine, TransferRequest, TransferStatusEnum,
};
use uuid::Uuid;

use crate::proto;

// ---------------------------------------------------------------------------
// MooncakeClient
// ---------------------------------------------------------------------------

/// A client that can perform Put/Get/Remove operations against a Mooncake
/// Store cluster.  It talks to the Master via gRPC for metadata, and uses
/// `transfer_engine_ffi` for the actual data-plane transfers.
pub struct MooncakeClient {
    master: proto::master_service_client::MasterServiceClient<Channel>,
    engine: Arc<TransferEngine>,

    /// Our UUID (assigned by Master).
    client_id: Uuid,

    /// Pre-allocated, TE-registered local transfer buffer (copy-based path).
    local_buffer: Vec<u8>,

    /// Locally registered buffers: ptr → (size, location).
    registered_buffers: RwLock<HashMap<usize, (usize, String)>>,
}

impl MooncakeClient {
    /// Create and initialise a new client.
    ///
    /// - `master_addr` — `"IP:port"` of the Master gRPC service.
    /// - `metadata_conn_string` — Transfer Engine metadata (etcd/HTTP/P2P).
    /// - `local_host` — IP or hostname of this node.
    /// - `protocol` — transport protocol (`"tcp"` or `"rdma"`).
    /// - `device` — RDMA device name (empty = auto).
    /// - `global_segment_size` — size of segment to contribute (0 = pure client).
    /// - `local_buffer_size` — size of local transfer buffer.
    pub async fn create(
        master_addr: &str,
        metadata_conn_string: &str,
        local_host: &str,
        protocol: &str,
        device: &str,
        global_segment_size: u64,
        local_buffer_size: u64,
    ) -> StoreResult<Self> {
        let master_url = format!("http://{master_addr}");
        let channel = Channel::from_shared(master_url)
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let mut master =
            proto::master_service_client::MasterServiceClient::new(channel);

        // --- Init Transfer Engine ---
        let parts: Vec<&str> = local_host.split(':').collect();
        let ip = parts.first().copied().unwrap_or(local_host);
        let port: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);

        let engine = TransferEngine::create(
            metadata_conn_string,
            local_host,
            ip,
            port,
            true,
        )?;

        if protocol != "tcp" {
            engine.install_transport(protocol, Some(device))?;
        } else {
            engine.install_transport("tcp", None)?;
        }

        engine.discover_topology()?;

        let engine = Arc::new(engine);

        // --- Pre-allocate and register local transfer buffer ---
        let local_buffer = vec![0u8; local_buffer_size as usize];
        engine.register_local_memory(
            local_buffer.as_ptr() as *mut c_void,
            local_buffer_size as usize,
            "cpu:0",
            true,
        )?;

        // --- Register with Master ---
        let client_id = Uuid::new_v4();

        // Mount segments if contributing memory.
        if global_segment_size > 0 {
            let request = proto::MountSegmentRequest {
                client_id: Some(proto::Uuid {
                    high: client_id.as_u64_pair().0,
                    low: client_id.as_u64_pair().1,
                }),
                segment_name: local_host.to_string(),
                size: global_segment_size,
            };
            master.mount_segment(request).await.map_err(|e| StoreError::Internal(e.to_string()))?;
        }

        Ok(Self {
            master,
            engine,
            client_id,
            local_buffer,
            registered_buffers: RwLock::new(HashMap::new()),
        })
    }

    // -----------------------------------------------------------------------
    // Put
    // -----------------------------------------------------------------------

    /// Store a value under a key (copy-based).
    pub async fn put(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();

        // 1. Ask Master where to put the data.
        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: value.len() as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
            }),
        };

        let response = self
            .master
            .put_start(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();

        let replicas: Vec<ReplicaDescriptor> = response
            .replicas
            .iter()
            .filter_map(|r| {
                let sid = r.segment_id.as_ref()?;
                Some(ReplicaDescriptor {
                    segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                    segment_name: r.segment_name.clone(),
                    offset: r.offset,
                    status: mooncake_store_core::ReplicaStatus::Allocating,
                    replica_type: mooncake_store_core::ReplicaType::Memory,
                })
            })
            .collect();

        if replicas.is_empty() {
            return Err(StoreError::NoAvailableHandle);
        }

        // 2. Write data to each replica via Transfer Engine.
        for replica in &replicas {
            self.write_to_replica(replica, value).await?;
        }

        // 3. Notify Master that Put is complete.
        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
        };
        self.master
            .put_end(end_request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Store data from a pre-registered buffer (zero-copy).
    ///
    /// # Safety
    /// `buffer` must point to a registered memory region of at least `size` bytes.
    pub async unsafe fn put_from(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();

        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: size as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
            }),
        };

        let response = self.master.put_start(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();

        let replicas: Vec<ReplicaDescriptor> = response.replicas.iter().filter_map(|r| {
            let sid = r.segment_id.as_ref()?;
            Some(ReplicaDescriptor {
                segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                segment_name: r.segment_name.clone(),
                offset: r.offset,
                status: mooncake_store_core::ReplicaStatus::Allocating,
                replica_type: mooncake_store_core::ReplicaType::Memory,
            })
        }).collect();

        for replica in &replicas {
            self.zero_copy_write(replica, buffer, size).await?;
        }

        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
        };
        self.master.put_end(end_request).await.map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Get
    // -----------------------------------------------------------------------

    /// Retrieve the value for `key` as bytes (copy-based).
    pub async fn get(&mut self, key: &str) -> StoreResult<Vec<u8>> {
        let request = proto::GetReplicaListRequest { key: key.to_string() };
        let response = self
            .master
            .get_replica_list(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();

        let replicas: Vec<ReplicaDescriptor> = response
            .replicas
            .iter()
            .filter_map(|r| {
                let sid = r.segment_id.as_ref()?;
                Some(ReplicaDescriptor {
                    segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                    segment_name: r.segment_name.clone(),
                    offset: r.offset,
                    status: mooncake_store_core::ReplicaStatus::Allocating,
                    replica_type: mooncake_store_core::ReplicaType::Memory,
                })
            })
            .collect();

        if replicas.is_empty() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }

        // Read from the first available replica.
        self.read_from_replica(&replicas[0]).await
    }

    /// Retrieve data directly into a registered buffer (zero-copy).
    ///
    /// # Safety
    /// `buffer` must point to a registered memory region of at least `size` bytes.
    pub async unsafe fn get_into(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<usize> {
        let request = proto::GetReplicaListRequest { key: key.to_string() };
        let response = self
            .master
            .get_replica_list(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();

        let replicas: Vec<ReplicaDescriptor> = response
            .replicas
            .iter()
            .filter_map(|r| {
                let sid = r.segment_id.as_ref()?;
                Some(ReplicaDescriptor {
                    segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                    segment_name: r.segment_name.clone(),
                    offset: r.offset,
                    status: mooncake_store_core::ReplicaStatus::Allocating,
                    replica_type: mooncake_store_core::ReplicaType::Memory,
                })
            })
            .collect();

        if replicas.is_empty() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }

        self.zero_copy_read(&replicas[0], buffer, size).await
    }

    // -----------------------------------------------------------------------
    // Remove / Exist
    // -----------------------------------------------------------------------

    /// Remove the object identified by `key`.
    pub async fn remove(&mut self, key: &str) -> StoreResult<()> {
        let request = proto::RemoveRequest { key: key.to_string() };
        self.master
            .remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Check whether a key exists.
    pub async fn exists(&mut self, key: &str) -> StoreResult<bool> {
        let request = proto::ExistKeyRequest { key: key.to_string() };
        let response = self
            .master
            .exist_key(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.exists)
    }

    // -----------------------------------------------------------------------
    // Buffer registration (zero-copy path)
    // -----------------------------------------------------------------------

    /// Register a buffer for RDMA / zero-copy access.
    ///
    /// # Safety
    /// `buffer` must point to valid memory of at least `size` bytes.
    pub unsafe fn register_buffer(
        &self,
        buffer: *mut c_void,
        size: usize,
        location: &str,
    ) -> StoreResult<()> {
        self.engine
            .register_local_memory(buffer, size, location, true)?;
        self.registered_buffers
            .write()
            .insert(buffer as usize, (size, location.to_string()));
        Ok(())
    }

    /// Unregister a previously registered buffer.
    ///
    /// # Safety
    /// `buffer` must be the same pointer passed to `register_buffer`.
    pub unsafe fn unregister_buffer(&self, buffer: *mut c_void) -> StoreResult<()> {
        self.engine.unregister_local_memory(buffer)?;
        self.registered_buffers.write().remove(&(buffer as usize));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn client_id_proto(&self) -> proto::Uuid {
        let (h, l) = self.client_id.as_u64_pair();
        proto::Uuid { high: h, low: l }
    }

    /// Copy-based write to a replica.
    async fn write_to_replica(
        &self,
        replica: &ReplicaDescriptor,
        data: &[u8],
    ) -> StoreResult<()> {
        if data.len() > self.local_buffer.len() {
            return Err(StoreError::InvalidParams(format!(
                "data size {} exceeds local buffer size {}",
                data.len(),
                self.local_buffer.len()
            )));
        }

        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        let batch_id = self.engine.allocate_batch_id(1)?;

        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.local_buffer.as_ptr() as *mut u8,
                data.len(),
            );
        }

        let request = TransferRequest {
            opcode: Opcode::Write,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset: replica.offset,
            length: data.len() as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        loop {
            let status = self.engine.get_transfer_status(batch_id, 0)?;
            if status.status == TransferStatusEnum::Completed {
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                return Err(StoreError::OperationFailed(-1));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        Ok(())
    }

    /// Zero-copy write from a registered buffer.
    async unsafe fn zero_copy_write(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<()> {
        let segment_id = self.engine.open_segment(&replica.segment_name)?;
        let batch_id = self.engine.allocate_batch_id(1)?;

        let request = TransferRequest {
            opcode: Opcode::Write,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.offset,
            length: size as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        loop {
            let status = self.engine.get_transfer_status(batch_id, 0)?;
            if status.status.is_terminal() {
                if status.status != TransferStatusEnum::Completed {
                    return Err(StoreError::OperationFailed(-1));
                }
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        Ok(())
    }

    /// Copy-based read from a replica.
    async fn read_from_replica(
        &self,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        let segment_id = self.engine.open_segment(&replica.segment_name)?;

        let read_len = self.local_buffer.len();
        let batch_id = self.engine.allocate_batch_id(1)?;

        let request = TransferRequest {
            opcode: Opcode::Read,
            source: self.local_buffer.as_ptr() as *mut c_void,
            target_id: segment_id,
            target_offset: replica.offset,
            length: read_len as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        let mut transferred: u64;
        loop {
            let status: transfer_engine_ffi::TransferStatus = self.engine.get_transfer_status(batch_id, 0)?;
            transferred = status.transferred_bytes;
            if status.status == TransferStatusEnum::Completed {
                break;
            }
            if status.status == TransferStatusEnum::Failed {
                return Err(StoreError::OperationFailed(-1));
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;

        let result: Vec<u8> = self.local_buffer[..(transferred as usize)].to_vec();
        Ok(result)
    }

    /// Zero-copy read into a registered buffer.
    async unsafe fn zero_copy_read(
        &self,
        replica: &ReplicaDescriptor,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<usize> {
        let segment_id: transfer_engine_ffi::SegmentId = self.engine.open_segment(&replica.segment_name)?;
        let batch_id: transfer_engine_ffi::BatchId = self.engine.allocate_batch_id(1)?;

        let request: TransferRequest = TransferRequest {
            opcode: Opcode::Read,
            source: buffer,
            target_id: segment_id,
            target_offset: replica.offset,
            length: size as u64,
        };

        self.engine.submit_transfer(batch_id, &[request])?;

        let mut transferred: u64;
        loop {
            let status: transfer_engine_ffi::TransferStatus = self.engine.get_transfer_status(batch_id, 0)?;
            transferred = status.transferred_bytes;
            if status.status.is_terminal() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
        }

        self.engine.free_batch_id(batch_id)?;
        self.engine.close_segment(segment_id)?;
        Ok(transferred as usize)
    }
}

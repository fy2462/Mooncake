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
// BufferHandle
// ---------------------------------------------------------------------------

pub struct BufferHandle {
    pub data: Vec<u8>,
    pub key: String,
    pub size: usize,
}

// ---------------------------------------------------------------------------
// MooncakeClient
// ---------------------------------------------------------------------------

pub struct MooncakeClient {
    master: proto::master_service_client::MasterServiceClient<Channel>,
    engine: Arc<TransferEngine>,
    client_id: Uuid,
    local_hostname: String,
    local_buffer: Vec<u8>,
    registered_buffers: RwLock<HashMap<usize, (usize, String)>>,
    tear_down: Arc<RwLock<bool>>,
}

impl MooncakeClient {
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

        let local_buffer = vec![0u8; local_buffer_size as usize];
        unsafe {
            engine.register_local_memory(
                local_buffer.as_ptr() as *mut c_void,
                local_buffer_size as usize,
                "cpu:0",
                true,
            )?;
        }

        let client_id = Uuid::new_v4();

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
            local_hostname: local_host.to_string(),
            local_buffer,
            registered_buffers: RwLock::new(HashMap::new()),
            tear_down: Arc::new(RwLock::new(false)),
        })
    }

    // -----------------------------------------------------------------------
    // Put
    // -----------------------------------------------------------------------

    pub async fn put(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();

        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: value.len() as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self
            .master
            .put_start(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();

        let replicas = self.replicas_from_proto(&response.replicas);
        if replicas.is_empty() {
            return Err(StoreError::NoAvailableHandle);
        }

        for replica in &replicas {
            self.write_to_replica(replica, value).await?;
        }

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
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self.master.put_start(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

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
    // Put parts (split data across multiple writes)
    // -----------------------------------------------------------------------

    pub async fn put_parts(
        &mut self,
        key: &str,
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<()> {
        let cfg = config.unwrap_or_default();
        let total_len: usize = values.iter().map(|v| v.len()).sum();

        let request = proto::PutStartRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: total_len as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self.master.put_start(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        if replicas.is_empty() {
            return Err(StoreError::NoAvailableHandle);
        }

        for replica in &replicas {
            let segment_id = self.engine.open_segment(&replica.segment_name)?;
            let batch_id = self.engine.allocate_batch_id(values.len())?;

            let requests: Vec<TransferRequest> = values
                .iter()
                .enumerate()
                .map(|(i, data)| {
                    let src_offset = values[..i].iter().map(|v| v.len()).sum::<usize>();
                    let tgt_offset = replica.offset + src_offset as u64;
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            data.as_ptr(),
                            self.local_buffer[src_offset..].as_ptr() as *mut u8,
                            data.len(),
                        );
                    }
                    TransferRequest {
                        opcode: Opcode::Write,
                        source: unsafe { self.local_buffer.as_ptr().add(src_offset) as *mut c_void },
                        target_id: segment_id,
                        target_offset: tgt_offset,
                        length: data.len() as u64,
                    }
                })
                .collect();

            self.engine.submit_transfer(batch_id, &requests)?;

            for i in 0..values.len() {
                loop {
                    let status = self.engine.get_transfer_status(batch_id, i)?;
                    if status.status == TransferStatusEnum::Completed {
                        break;
                    }
                    if status.status == TransferStatusEnum::Failed {
                        return Err(StoreError::OperationFailed(-1));
                    }
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                }
            }

            self.engine.free_batch_id(batch_id)?;
            self.engine.close_segment(segment_id)?;
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
    // Batch Put
    // -----------------------------------------------------------------------

    pub async fn batch_put(
        &mut self,
        keys: &[String],
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        let mut statuses = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self.put(key, values[i], config.clone()).await {
                Ok(()) => statuses.push(0),
                Err(_) => statuses.push(-1),
            }
        }

        let end_entries: Vec<proto::PutEndEntry> = keys
            .iter()
            .map(|key| proto::PutEndEntry {
                client_id: Some(self.client_id_proto()),
                key: key.clone(),
                replica_type: 0,
            })
            .collect();

        let _ = self.master.batch_put_end(proto::BatchPutEndRequest { entries: end_entries }).await;
        Ok(statuses)
    }

    pub async unsafe fn batch_put_from(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<i32>> {
        let mut statuses = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self.put_from(key, buffers[i], sizes[i], config.clone()).await {
                Ok(()) => statuses.push(0),
                Err(_) => statuses.push(-1),
            }
        }
        Ok(statuses)
    }

    // -----------------------------------------------------------------------
    // Get
    // -----------------------------------------------------------------------

    pub async fn get(&mut self, key: &str) -> StoreResult<Vec<u8>> {
        let replicas = self.fetch_replicas(key).await?;
        if replicas.is_empty() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }
        self.read_from_replica(&replicas[0]).await
    }

    pub async unsafe fn get_into(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
    ) -> StoreResult<usize> {
        let replicas = self.fetch_replicas(key).await?;
        if replicas.is_empty() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }
        self.zero_copy_read(&replicas[0], buffer, size).await
    }

    // -----------------------------------------------------------------------
    // Get into ranges (zero-copy multi-range read)
    // -----------------------------------------------------------------------

    pub async unsafe fn get_into_ranges(
        &mut self,
        buffers: &[*mut c_void],
        keys: &[Vec<String>],
        dst_offsets: &[Vec<Vec<usize>>],
        src_offsets: &[Vec<Vec<usize>>],
        sizes: &[Vec<Vec<usize>>],
    ) -> StoreResult<Vec<Vec<Vec<i64>>>> {
        let count = buffers.len().min(keys.len()).min(dst_offsets.len()).min(src_offsets.len()).min(sizes.len());
        let mut results = Vec::with_capacity(count);
        for buf_idx in 0..count {
            let mut buf_results = vec![];
            for (key_idx, key) in keys[buf_idx].iter().enumerate() {
                let replicas = self.fetch_replicas(key).await?;
                if replicas.is_empty() {
                    buf_results.push(vec![-1]);
                    continue;
                }
                let seg = self.engine.open_segment(&replicas[0].segment_name)?;
                let batch_id = self.engine.allocate_batch_id(sizes[buf_idx][key_idx].len())?;

                let reqs: Vec<TransferRequest> = sizes[buf_idx][key_idx]
                    .iter()
                    .enumerate()
                    .map(|(ri, &sz)| TransferRequest {
                        opcode: Opcode::Read,
                        source: buffers[buf_idx].byte_add(dst_offsets[buf_idx][key_idx][ri]),
                        target_id: seg,
                        target_offset: replicas[0].offset + src_offsets[buf_idx][key_idx][ri] as u64,
                        length: sz as u64,
                    })
                    .collect();

                self.engine.submit_transfer(batch_id, &reqs)?;

                let mut range_results: Vec<i64> = vec![0; sizes[buf_idx][key_idx].len()];
                for ri in 0..sizes[buf_idx][key_idx].len() {
                    loop {
                        let status = self.engine.get_transfer_status(batch_id, ri)?;
                        if status.status == TransferStatusEnum::Completed {
                            range_results[ri] = status.transferred_bytes as i64;
                            break;
                        }
                        if status.status == TransferStatusEnum::Failed {
                            range_results[ri] = -1;
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                    }
                }
                self.engine.free_batch_id(batch_id)?;
                self.engine.close_segment(seg)?;
                buf_results.push(range_results);
            }
            results.push(buf_results);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Batch Get
    // -----------------------------------------------------------------------

    pub async fn batch_get(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get(key).await {
                Ok(data) => results.push(Some(data)),
                Err(_) => results.push(None),
            }
        }
        Ok(results)
    }

    pub async unsafe fn batch_get_into(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
    ) -> StoreResult<Vec<i64>> {
        let mut results = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            match self.get_into(key, buffers[i], sizes[i]).await {
                Ok(n) => results.push(n as i64),
                Err(_) => results.push(-1),
            }
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Batch get into multi buffers
    // -----------------------------------------------------------------------

    pub async unsafe fn batch_get_into_multi_buffers(
        &mut self,
        keys: &[String],
        all_buffers: &[Vec<*mut c_void>],
        all_sizes: &[Vec<usize>],
        _prefer_same_node: bool,
    ) -> StoreResult<Vec<Vec<i64>>> {
        let mut results = vec![];
        for (key_idx, key) in keys.iter().enumerate() {
            let replicas = self.fetch_replicas(key).await?;
            if replicas.is_empty() {
                results.push(vec![-1; all_buffers[key_idx].len()]);
                continue;
            }
            let seg = self.engine.open_segment(&replicas[0].segment_name)?;
            let count = all_buffers[key_idx].len();
            let batch_id = self.engine.allocate_batch_id(count)?;

            let reqs: Vec<TransferRequest> = (0..count)
                .map(|i| TransferRequest {
                    opcode: Opcode::Read,
                    source: all_buffers[key_idx][i],
                    target_id: seg,
                    target_offset: replicas[0].offset,
                    length: all_sizes[key_idx][i] as u64,
                })
                .collect();

            self.engine.submit_transfer(batch_id, &reqs)?;

            let mut key_results: Vec<i64> = vec![0; count];
            for i in 0..count {
                loop {
                    let status = self.engine.get_transfer_status(batch_id, i)?;
                    if status.status == TransferStatusEnum::Completed {
                        key_results[i] = status.transferred_bytes as i64;
                        break;
                    }
                    if status.status == TransferStatusEnum::Failed {
                        key_results[i] = -1;
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                }
            }
            self.engine.free_batch_id(batch_id)?;
            self.engine.close_segment(seg)?;
            results.push(key_results);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Buffer-based get (returns owned BufferHandle)
    // -----------------------------------------------------------------------

    pub async fn get_buffer(
        &mut self,
        key: &str,
    ) -> StoreResult<BufferHandle> {
        let data = self.get(key).await?;
        let size = data.len();
        Ok(BufferHandle {
            key: key.to_string(),
            size,
            data,
        })
    }

    pub async fn batch_get_buffer(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<Option<BufferHandle>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_buffer(key).await {
                Ok(bh) => results.push(Some(bh)),
                Err(_) => results.push(None),
            }
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Remove / Exist
    // -----------------------------------------------------------------------

    pub async fn remove(&mut self, key: &str) -> StoreResult<()> {
        let request = proto::RemoveRequest { key: key.to_string(), force: false };
        self.master
            .remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

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
    // Batch Remove / Exist
    // -----------------------------------------------------------------------

    pub async fn batch_remove(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<i32>> {
        let request = proto::BatchRemoveRequest {
            keys: keys.to_vec(),
            force: false,
        };
        let response = self
            .master
            .batch_remove(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.statuses)
    }

    pub async fn batch_is_exist(
        &mut self,
        keys: &[String],
    ) -> StoreResult<Vec<bool>> {
        let request = proto::BatchExistKeyRequest {
            keys: keys.to_vec(),
        };
        let response = self
            .master
            .batch_exist_key(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.results)
    }

    // -----------------------------------------------------------------------
    // Remove by regex / Remove all
    // -----------------------------------------------------------------------

    pub async fn remove_by_regex(
        &mut self,
        pattern: &str,
    ) -> StoreResult<i64> {
        let request = proto::RemoveByRegexRequest {
            pattern: pattern.to_string(),
            force: false,
        };
        let response = self
            .master
            .remove_by_regex(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.removed_count)
    }

    pub async fn remove_all(&mut self) -> StoreResult<i64> {
        self.remove_by_regex(".*").await
    }

    // -----------------------------------------------------------------------
    // get_size / get_hostname / health_check / tearDownAll / is_closed
    // -----------------------------------------------------------------------

    pub async fn get_size(&mut self, key: &str) -> StoreResult<i64> {
        let replicas = self.fetch_replicas(key).await?;
        if replicas.is_empty() {
            return Err(StoreError::KeyNotFound(key.to_string()));
        }
        Ok(replicas[0].size as i64)
    }

    pub fn get_hostname(&self) -> String {
        self.local_hostname.clone()
    }

    pub async fn health_check(&mut self) -> StoreResult<()> {
        let request = proto::PingRequest {
            client_id: Some(self.client_id_proto()),
            mounted_segments: vec![],
        };
        self.master
            .ping(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        *self.tear_down.read()
    }

    pub async fn tear_down_all(&mut self) -> StoreResult<()> {
        *self.tear_down.write() = true;

        // unregister local buffer
        unsafe { let _ = self.engine.unregister_local_memory(self.local_buffer.as_ptr() as *mut c_void); }

        // unregister all user-registered buffers
        let ptrs: Vec<usize> = self.registered_buffers.read().keys().copied().collect();
        for ptr in &ptrs {
            unsafe { let _ = self.engine.unregister_local_memory(*ptr as *mut c_void); }
        }
        self.registered_buffers.write().clear();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Upsert
    // -----------------------------------------------------------------------

    pub async fn upsert(
        &mut self,
        key: &str,
        value: &[u8],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let cfg = config.unwrap_or_default();
        let request = proto::UpsertRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: value.len() as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self.master.upsert(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        for replica in &replicas {
            self.write_to_replica(replica, value).await?;
        }

        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
        };
        self.master.put_end(end_request).await.map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(replicas)
    }

    pub async unsafe fn upsert_from(
        &mut self,
        key: &str,
        buffer: *mut c_void,
        size: usize,
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let cfg = config.unwrap_or_default();
        let request = proto::UpsertRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            slice_length: size as u64,
            config: Some(proto::ReplicateConfig {
                replica_num: cfg.replica_num,
                nof_replica_num: cfg.nof_replica_num,
                with_soft_pin: cfg.with_soft_pin,
                with_hard_pin: cfg.with_hard_pin,
                preferred_segment: cfg.preferred_segment.clone(),
                prefer_alloc_in_same_node: cfg.prefer_alloc_in_same_node,
                preferred_segments: cfg.preferred_segments.clone(),
                preferred_nof_segments: cfg.preferred_nof_segments.clone(),
                data_type: cfg.data_type as i32,
            }),
        };

        let response = self.master.upsert(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        let replicas = self.replicas_from_proto(&response.replicas);

        for replica in &replicas {
            self.zero_copy_write(replica, buffer, size).await?;
        }

        let end_request = proto::PutEndRequest {
            client_id: Some(self.client_id_proto()),
            key: key.to_string(),
            replica_type: 0,
        };
        self.master.put_end(end_request).await.map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(replicas)
    }

    pub async unsafe fn batch_upsert_from(
        &mut self,
        keys: &[String],
        buffers: &[*mut c_void],
        sizes: &[usize],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<Vec<ReplicaDescriptor>>> {
        let mut results = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            results.push(
                self.upsert_from(key, buffers[i], sizes[i], config.clone())
                    .await?
            );
        }
        Ok(results)
    }

    pub async fn upsert_parts(
        &mut self,
        key: &str,
        values: &[&[u8]],
        config: Option<ReplicateConfig>,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let total_len: usize = values.iter().map(|v| v.len()).sum();
        let mut concatenated = Vec::with_capacity(total_len);
        for v in values {
            concatenated.extend_from_slice(v);
        }
        self.upsert(key, &concatenated, config).await
    }

    // -----------------------------------------------------------------------
    // Task management
    // -----------------------------------------------------------------------

    pub async fn create_copy_task(
        &mut self,
        key: &str,
        targets: &[String],
    ) -> StoreResult<Uuid> {
        let request = proto::CreateCopyTaskRequest {
            key: key.to_string(),
            targets: targets.to_vec(),
        };
        let response = self.master.create_copy_task(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        match response.task_id {
            Some(id) => Ok(Uuid::from_u64_pair(id.high, id.low)),
            None => Err(StoreError::OperationFailed(-1)),
        }
    }

    pub async fn create_move_task(
        &mut self,
        key: &str,
        source: &str,
        target: &str,
    ) -> StoreResult<Uuid> {
        let request = proto::CreateMoveTaskRequest {
            key: key.to_string(),
            source: source.to_string(),
            target: target.to_string(),
        };
        let response = self.master.create_move_task(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        match response.task_id {
            Some(id) => Ok(Uuid::from_u64_pair(id.high, id.low)),
            None => Err(StoreError::OperationFailed(-1)),
        }
    }

    pub async fn query_task(
        &mut self,
        task_id: Uuid,
    ) -> StoreResult<proto::QueryTaskResponse> {
        let request = proto::QueryTaskRequest {
            task_id: Some(proto::Uuid {
                high: task_id.as_u64_pair().0,
                low: task_id.as_u64_pair().1,
            }),
        };
        let response = self.master.query_task(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        Ok(response)
    }

    pub async fn fetch_tasks(
        &mut self,
        batch_size: u32,
    ) -> StoreResult<Vec<proto::TaskAssignment>> {
        let request = proto::FetchTasksRequest {
            client_id: Some(self.client_id_proto()),
            batch_size,
        };
        let response = self.master.fetch_tasks(request).await.map_err(|e| StoreError::Internal(e.to_string()))?.into_inner();
        Ok(response.tasks)
    }

    pub async fn mark_task_to_complete(
        &mut self,
        task_id: Uuid,
        status: proto::TaskStatus,
        message: &str,
    ) -> StoreResult<()> {
        let request = proto::MarkTaskToCompleteRequest {
            client_id: Some(self.client_id_proto()),
            request: Some(proto::TaskCompleteRequest {
                id: Some(proto::Uuid {
                    high: task_id.as_u64_pair().0,
                    low: task_id.as_u64_pair().1,
                }),
                status: status as i32,
                message: message.to_string(),
            }),
        };
        self.master
            .mark_task_to_complete(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn mount_local_disk_segment(&mut self, enable_offloading: bool) -> StoreResult<()> {
        self.master
            .mount_local_disk_segment(proto::MountLocalDiskSegmentRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn offload_object_heartbeat(
        &mut self,
        enable_offloading: bool,
    ) -> StoreResult<std::collections::HashMap<String, i64>> {
        let response = self
            .master
            .offload_object_heartbeat(proto::OffloadObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
                enable_offloading,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.objects)
    }

    pub async fn report_ssd_capacity(&mut self, bytes: i64) -> StoreResult<()> {
        self.master
            .report_ssd_capacity(proto::ReportSsdCapacityRequest {
                client_id: Some(self.client_id_proto()),
                ssd_total_capacity_bytes: bytes,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn notify_offload_success(
        &mut self,
        keys: Vec<String>,
        metadatas: Vec<proto::StorageObjectMetadata>,
    ) -> StoreResult<()> {
        self.master
            .notify_offload_success(proto::NotifyOffloadSuccessRequest {
                client_id: Some(self.client_id_proto()),
                keys,
                metadatas,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn promotion_object_heartbeat(
        &mut self,
    ) -> StoreResult<std::collections::HashMap<String, i64>> {
        let response = self
            .master
            .promotion_object_heartbeat(proto::PromotionObjectHeartbeatRequest {
                client_id: Some(self.client_id_proto()),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(response.objects)
    }

    pub async fn promotion_alloc_start(
        &mut self,
        key: &str,
        size: u64,
        preferred_segments: Vec<String>,
    ) -> StoreResult<ReplicaDescriptor> {
        let response = self
            .master
            .promotion_alloc_start(proto::PromotionAllocStartRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
                size,
                preferred_segments,
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        let descriptor = response
            .memory_descriptor
            .as_ref()
            .ok_or(StoreError::OperationFailed(-1))?;
        Ok(self.replicas_from_proto(std::slice::from_ref(descriptor)).remove(0))
    }

    pub async fn notify_promotion_success(&mut self, key: &str) -> StoreResult<()> {
        self.master
            .notify_promotion_success(proto::NotifyPromotionSuccessRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    pub async fn notify_promotion_failure(&mut self, key: &str) -> StoreResult<()> {
        self.master
            .notify_promotion_failure(proto::NotifyPromotionFailureRequest {
                client_id: Some(self.client_id_proto()),
                key: key.to_string(),
            })
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Buffer registration (zero-copy path)
    // -----------------------------------------------------------------------

    pub unsafe fn register_buffer(
        &self,
        buffer: *mut c_void,
        size: usize,
        location: &str,
    ) -> StoreResult<()> {
        unsafe {
            self.engine
                .register_local_memory(buffer, size, location, true)?;
        }
        self.registered_buffers
            .write()
            .insert(buffer as usize, (size, location.to_string()));
        Ok(())
    }

    pub unsafe fn unregister_buffer(&self, buffer: *mut c_void) -> StoreResult<()> {
        unsafe { self.engine.unregister_local_memory(buffer)?; }
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

    async fn fetch_replicas(&mut self, key: &str) -> StoreResult<Vec<ReplicaDescriptor>> {
        let request = proto::GetReplicaListRequest { key: key.to_string() };
        let response = self
            .master
            .get_replica_list(request)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?
            .into_inner();
        Ok(self.replicas_from_proto(&response.replicas))
    }

    fn replicas_from_proto(&self, replicas: &[proto::ReplicaDescriptor]) -> Vec<ReplicaDescriptor> {
        replicas.iter().filter_map(|r| {
            let sid = r.segment_id.as_ref()?;
            Some(ReplicaDescriptor {
                refcnt: 0,
                segment_id: Uuid::from_u64_pair(sid.high, sid.low),
                segment_name: r.segment_name.clone(),
                offset: r.offset,
                size: r.size,
                status: match r.status {
                    1 => mooncake_store_core::ReplicaStatus::Allocating,
                    2 => mooncake_store_core::ReplicaStatus::Written,
                    3 => mooncake_store_core::ReplicaStatus::Complete,
                    4 => mooncake_store_core::ReplicaStatus::Failed,
                    _ => mooncake_store_core::ReplicaStatus::Undefined,
                },
                replica_type: match r.replica_type {
                    1 => mooncake_store_core::ReplicaType::Disk,
                    2 => mooncake_store_core::ReplicaType::LocalDisk,
                    _ => mooncake_store_core::ReplicaType::Memory,
                },
                holder_client_id: r
                    .holder_client_id
                    .as_ref()
                    .map(|id| Uuid::from_u64_pair(id.high, id.low)),
            })
        }).collect()
    }

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

    async fn read_from_replica(
        &self,
        replica: &ReplicaDescriptor,
    ) -> StoreResult<Vec<u8>> {
        if replica.size > self.local_buffer.len() as u64 {
            return Err(StoreError::InvalidParams(format!(
                "object size {} exceeds local buffer size {}",
                replica.size,
                self.local_buffer.len()
            )));
        }
        let segment_id = self.engine.open_segment(&replica.segment_name)?;

        let read_len = replica.size as usize;
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

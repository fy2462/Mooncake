use crate::error::P2pStoreError;
use crate::metadata::{Location, MetadataStore, Payload, PayloadInfo, Shard, METADATA_KEY_PREFIX};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;
use transfer_engine_ffi::{Opcode, TransferEngine, TransferRequest};

pub const MAX_CHUNK_SIZE: u64 = 4096 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Buffer {
    pub addr: usize,
    pub size: u64,
}

#[derive(Debug, Clone)]
struct CatalogEntry {
    addr_list: Vec<usize>,
}

pub struct P2pStore {
    local_server_name: String,
    catalog: Mutex<HashMap<String, CatalogEntry>>,
    metadata: Mutex<MetadataStore>,
    engine: Arc<TransferEngine>,
}

impl P2pStore {
    pub async fn new(
        metadata_conn_string: &str,
        local_server_name: &str,
        nic_priority_matrix: &str,
    ) -> Result<Self, P2pStoreError> {
        let metadata = MetadataStore::new(metadata_conn_string, METADATA_KEY_PREFIX).await?;

        let parts: Vec<&str> = local_server_name.split(':').collect();
        let ip = parts.first().copied().unwrap_or(local_server_name);
        let port: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);

        let engine = TransferEngine::create(metadata_conn_string, local_server_name, ip, port, true)
            .map_err(|_| P2pStoreError::TransferEngine)?;

        if nic_priority_matrix.is_empty() {
            engine
                .install_transport("tcp", None)
                .map_err(|_| P2pStoreError::TransferEngine)?;
        } else {
            engine
                .install_transport("rdma", Some(nic_priority_matrix))
                .map_err(|_| P2pStoreError::TransferEngine)?;
        }

        let engine = Arc::new(engine);

        Ok(Self {
            local_server_name: local_server_name.to_string(),
            catalog: Mutex::new(HashMap::new()),
            metadata: Mutex::new(metadata),
            engine,
        })
    }

    pub fn get_local_server_name(&self) -> Result<String, P2pStoreError> {
        self.engine
            .get_local_ip_and_port()
            .map_err(|_| P2pStoreError::TransferEngine)
    }

    pub async fn register(
        &self,
        name: &str,
        addr_list: &[usize],
        size_list: &[u64],
        max_shard_size: u64,
        location: &str,
        force_create: bool,
    ) -> Result<(), P2pStoreError> {
        if addr_list.is_empty() || addr_list.len() != size_list.len() {
            return Err(P2pStoreError::InvalidArgument);
        }

        {
            let catalog = self.catalog.lock();
            if catalog.contains_key(name) {
                return Err(P2pStoreError::PayloadOpened);
            }
        }

        let mut payload = Payload {
            name: name.to_string(),
            size: 0,
            size_list: size_list.to_vec(),
            max_shard_size,
            shards: Vec::new(),
        };

        for i in 0..addr_list.len() {
            let addr = addr_list[i] as *mut c_void;
            let size = size_list[i];

            unsafe {
                self.engine
                    .register_local_memory(addr, size as usize, location, true)
                    .map_err(|_| P2pStoreError::TransferEngine)?;
            }

            payload.size += size;
            let mut offset: u64 = 0;
            while offset < size {
                let shard_len = max_shard_size.min(size - offset);
                payload.shards.push(Shard {
                    length: shard_len,
                    gold: vec![Location {
                        segment_name: self.local_server_name.clone(),
                        offset: addr as u64 + offset,
                    }],
                    replica_list: Vec::new(),
                });
                offset += max_shard_size;
            }
        }

        {
            let mut meta = self.metadata.lock();
            if force_create {
                meta.put(name, &payload).await?;
            } else {
                meta.create(name, &payload).await?;
            }
        }

        self.catalog.lock().insert(
            name.to_string(),
            CatalogEntry {
                addr_list: addr_list.to_vec(),
            },
        );

        Ok(())
    }

    pub async fn unregister(&self, name: &str) -> Result<(), P2pStoreError> {
        let catalog_entry = {
            let catalog = self.catalog.lock();
            catalog.get(name).cloned().ok_or(P2pStoreError::PayloadNotOpened)?
        };

        loop {
            let (payload, revision) = {
                let mut meta = self.metadata.lock();
                meta.get(name).await?
            };
            let payload = payload.ok_or(P2pStoreError::PayloadNotFound)?;

            let mut cleared = payload.clone();
            for shard in &mut cleared.shards {
                shard.gold.clear();
            }

            let success = {
                let mut meta = self.metadata.lock();
                meta.update(name, &cleared, revision).await?
            };
            if success {
                self.catalog.lock().remove(name);
                for i in 0..catalog_entry.addr_list.len() {
                    unsafe {
                        self.engine
                            .unregister_local_memory(catalog_entry.addr_list[i] as *mut c_void)
                            .map_err(|_| P2pStoreError::TransferEngine)?;
                    }
                }
                return Ok(());
            }
        }
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<PayloadInfo>, P2pStoreError> {
        let mut meta = self.metadata.lock();
        let payloads: Vec<Payload> = meta.list(prefix).await?;
        Ok(payloads
            .into_iter()
            .map(|p| PayloadInfo {
                name: p.name,
                max_shard_size: p.max_shard_size,
                total_size: p.size,
                size_list: p.size_list,
            })
            .collect())
    }

    pub async fn get_replica(
        &self,
        name: &str,
        addr_list: &[usize],
        size_list: &[u64],
    ) -> Result<(), P2pStoreError> {
        let (payload_opt, _) = {
            let mut meta = self.metadata.lock();
            meta.get(name).await?
        };
        let payload = payload_opt.ok_or(P2pStoreError::PayloadNotFound)?;

        let max_shard_size = payload.max_shard_size;

        for i in 0..addr_list.len() {
            let addr = addr_list[i] as *mut c_void;
            let size = size_list[i] as usize;
            unsafe {
                self.engine
                    .register_local_memory(addr, size, "cpu:0", true)
                    .map_err(|_| P2pStoreError::TransferEngine)?;
            }
        }

        let mut task_id = 0usize;
        for i in 0..addr_list.len() {
            let size = size_list[i];
            let mut local_off: u64 = 0;
            while local_off < size && task_id < payload.shards.len() {
                let source = addr_list[i] as *mut c_void;
                let shard = &payload.shards[task_id];

                if let Some(loc) = shard.get_location(3) {
                    let segment_id = self
                        .engine
                        .open_segment(&loc.segment_name)
                        .map_err(|_| P2pStoreError::TransferEngine)?;

                    let batch_id = self
                        .engine
                        .allocate_batch_id(1)
                        .map_err(|_| P2pStoreError::TransferEngine)?;

                    let request = TransferRequest {
                        opcode: Opcode::Read,
                        source: unsafe { source.offset(local_off as isize) },
                        target_id: segment_id,
                        target_offset: loc.offset,
                        length: shard.length,
                    };

                    self.engine
                        .submit_transfer(batch_id, &[request])
                        .map_err(|_| P2pStoreError::TransferEngine)?;

                    loop {
                        let status = self
                            .engine
                            .get_transfer_status(batch_id, 0)
                            .map_err(|_| P2pStoreError::TransferEngine)?;
                        if status.status.is_terminal() {
                            break;
                        }
                    }

                    self.engine
                        .free_batch_id(batch_id)
                        .map_err(|_| P2pStoreError::TransferEngine)?;
                    self.engine
                        .close_segment(segment_id)
                        .map_err(|_| P2pStoreError::TransferEngine)?;
                }

                task_id += 1;
                local_off += max_shard_size;
            }
        }

        Ok(())
    }
}

use crate::catalog::{Catalog, CatalogEntry};
use crate::error::P2pStoreError;
use crate::memory::RegisteredMemory;
use crate::metadata::{Location, MetadataStore, Payload, PayloadInfo, METADATA_KEY_PREFIX};
use parking_lot::Mutex;
use std::sync::Arc;
use transfer_engine_ffi::TransferEngine;

pub const MAX_CHUNK_SIZE: u64 = 4096 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Buffer {
    pub addr: usize,
    pub size: u64,
}

pub struct P2pStore {
    pub(crate) local_server_name: String,
    pub(crate) catalog: Catalog,
    pub(crate) memory: RegisteredMemory,
    pub(crate) metadata: Mutex<MetadataStore>,
    pub(crate) engine: Arc<TransferEngine>,
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
        let engine =
            TransferEngine::create(metadata_conn_string, local_server_name, ip, port, true)
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
            catalog: Catalog::default(),
            memory: RegisteredMemory::new(engine.clone()),
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
        if self.catalog.contains(name) {
            return Err(P2pStoreError::PayloadOpened);
        }

        let mut payload = Payload {
            name: name.to_string(),
            size: 0,
            size_list: size_list.to_vec(),
            max_shard_size,
            shards: Vec::new(),
        };

        for i in 0..addr_list.len() {
            let addr = addr_list[i];
            let size = size_list[i];
            self.memory.add(addr, size, max_shard_size, location)?;
            payload.size += size;

            let mut offset = 0;
            while offset < size {
                let shard_len = max_shard_size.min(size - offset);
                payload.shards.push(crate::metadata::Shard {
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

        if force_create {
            self.metadata.lock().put(name, &payload).await?;
        } else {
            self.metadata.lock().create(name, &payload).await?;
        }

        self.catalog.add(
            name.to_string(),
            CatalogEntry {
                is_gold: true,
                addr_list: addr_list.to_vec(),
                size_list: size_list.to_vec(),
                max_shard_size,
            },
        );
        Ok(())
    }

    pub async fn unregister(&self, name: &str) -> Result<(), P2pStoreError> {
        let catalog_entry = self
            .catalog
            .get(name)
            .ok_or(P2pStoreError::PayloadNotOpened)?;
        let _is_gold = catalog_entry.is_gold;

        loop {
            let (payload, revision) = self.metadata.lock().get(name).await?;
            let mut payload = payload.ok_or(P2pStoreError::PayloadNotFound)?;
            for shard in &mut payload.shards {
                shard.gold.clear();
            }

            if self
                .metadata
                .lock()
                .update(name, &payload, revision)
                .await?
            {
                self.catalog.remove(name);
                for i in 0..catalog_entry.addr_list.len() {
                    self.memory.remove(
                        catalog_entry.addr_list[i],
                        catalog_entry.size_list[i],
                        catalog_entry.max_shard_size,
                    )?;
                }
                return Ok(());
            }
        }
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<PayloadInfo>, P2pStoreError> {
        let payloads: Vec<Payload> = self.metadata.lock().list(prefix).await?;
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
}

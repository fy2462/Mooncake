use crate::catalog::CatalogEntry;
use crate::error::P2pStoreError;
use crate::metadata::{Location, Payload, Shard};
use crate::store::P2pStore;
use std::ffi::c_void;
use transfer_engine_ffi::{Opcode, TransferRequest, TransferStatusEnum};

impl P2pStore {
    pub async fn get_replica(
        &self,
        name: &str,
        addr_list: &[usize],
        size_list: &[u64],
    ) -> Result<(), P2pStoreError> {
        if addr_list.is_empty() || addr_list.len() != size_list.len() {
            return Err(P2pStoreError::InvalidArgument);
        }
        if self.catalog.contains(name) {
            return Err(P2pStoreError::PayloadOpened);
        }

        let (payload, mut revision) = self.metadata.lock().get(name).await?;
        let mut payload = payload.ok_or(P2pStoreError::PayloadNotFound)?;
        loop {
            self.do_get_replica(&payload, addr_list, size_list)?;
            let (new_payload, recheck_revision) = self.metadata.lock().get(name).await?;
            let Some(new_payload) = new_payload else {
                return Err(P2pStoreError::PayloadNotFound);
            };
            if revision == recheck_revision || is_subset_of(&payload, &new_payload) {
                break;
            }
            payload = new_payload;
            revision = recheck_revision;
        }

        self.update_payload_metadata(name, addr_list, size_list, &mut payload, revision)
            .await
    }

    pub async fn delete_replica(&self, name: &str) -> Result<(), P2pStoreError> {
        let catalog_entry = self
            .catalog
            .get(name)
            .ok_or(P2pStoreError::PayloadNotOpened)?;
        let _is_gold = catalog_entry.is_gold;

        loop {
            let (payload, revision) = self.metadata.lock().get(name).await?;
            let mut payload = payload.ok_or(P2pStoreError::PayloadNotFound)?;
            for shard in &mut payload.shards {
                shard
                    .replica_list
                    .retain(|replica| replica.segment_name != self.local_server_name);
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

    fn do_get_replica(
        &self,
        payload: &Payload,
        addr_list: &[usize],
        size_list: &[u64],
    ) -> Result<(), P2pStoreError> {
        if addr_list.len() != payload.size_list.len() || size_list != payload.size_list.as_slice() {
            return Err(P2pStoreError::InvalidArgument);
        }
        for i in 0..addr_list.len() {
            self.memory
                .add(addr_list[i], size_list[i], payload.max_shard_size, "cpu:0")?;
        }

        let mut task_id = 0usize;
        for i in 0..addr_list.len() {
            let mut local_off = 0;
            while local_off < size_list[i] && task_id < payload.shards.len() {
                self.perform_transfer(addr_list[i] + local_off as usize, &payload.shards[task_id])?;
                task_id += 1;
                local_off += payload.max_shard_size;
            }
        }
        Ok(())
    }

    fn perform_transfer(&self, source: usize, shard: &Shard) -> Result<(), P2pStoreError> {
        let max_retry_count = std::cmp::max(3, shard.gold.len() + shard.replica_list.len());
        for retry in 0..max_retry_count {
            let Some(location) = shard.get_location(retry) else {
                break;
            };
            let batch_id = self
                .engine
                .allocate_batch_id(1)
                .map_err(|_| P2pStoreError::TransferEngine)?;
            let segment_id = if retry == 0 {
                self.engine.open_segment(&location.segment_name)
            } else {
                self.engine.open_segment_no_cache(&location.segment_name)
            }
            .map_err(|_| P2pStoreError::TransferEngine)?;
            let request = TransferRequest {
                opcode: Opcode::Read,
                source: source as *mut c_void,
                target_id: segment_id,
                target_offset: location.offset,
                length: shard.length,
            };
            if self.engine.submit_transfer(batch_id, &[request]).is_err() {
                let _ = self.engine.close_segment(segment_id);
                let _ = self.engine.free_batch_id(batch_id);
                return Err(P2pStoreError::TransferEngine);
            }

            let terminal_status = loop {
                let status = self
                    .engine
                    .get_transfer_status(batch_id, 0)
                    .map_err(|_| P2pStoreError::TransferEngine)?;
                if status.status.is_terminal() {
                    break status.status;
                }
            };
            self.engine
                .free_batch_id(batch_id)
                .map_err(|_| P2pStoreError::TransferEngine)?;
            self.engine
                .close_segment(segment_id)
                .map_err(|_| P2pStoreError::TransferEngine)?;
            if terminal_status == TransferStatusEnum::Completed {
                return Ok(());
            }
        }
        Err(P2pStoreError::TooManyRetries)
    }

    async fn update_payload_metadata(
        &self,
        name: &str,
        addr_list: &[usize],
        size_list: &[u64],
        payload: &mut Payload,
        mut revision: i64,
    ) -> Result<(), P2pStoreError> {
        loop {
            append_replica_locations(payload, &self.local_server_name, addr_list, size_list)?;
            if self.metadata.lock().update(name, payload, revision).await? {
                self.catalog.add(
                    name.to_string(),
                    CatalogEntry {
                        is_gold: false,
                        addr_list: addr_list.to_vec(),
                        size_list: size_list.to_vec(),
                        max_shard_size: payload.max_shard_size,
                    },
                );
                return Ok(());
            }
            let (new_payload, new_revision) = self.metadata.lock().get(name).await?;
            *payload = new_payload.ok_or(P2pStoreError::PayloadNotFound)?;
            revision = new_revision;
        }
    }
}

fn append_replica_locations(
    payload: &mut Payload,
    local_server_name: &str,
    addr_list: &[usize],
    size_list: &[u64],
) -> Result<(), P2pStoreError> {
    let mut task_id = 0usize;
    for i in 0..addr_list.len() {
        let mut offset = 0;
        while offset < size_list[i] {
            let Some(shard) = payload.shards.get_mut(task_id) else {
                return Err(P2pStoreError::InvalidArgument);
            };
            let location = Location {
                segment_name: local_server_name.to_string(),
                offset: addr_list[i] as u64 + offset,
            };
            if !shard.replica_list.contains(&location) {
                shard.replica_list.push(location);
            }
            task_id += 1;
            offset += payload.max_shard_size;
        }
    }
    Ok(())
}

fn is_subset_of(old: &Payload, new: &Payload) -> bool {
    if old.shards.len() != new.shards.len() {
        return false;
    }
    old.shards.iter().zip(&new.shards).all(|(old, new)| {
        old.gold.iter().all(|loc| new.gold.contains(loc))
            && old
                .replica_list
                .iter()
                .all(|loc| new.replica_list.contains(loc))
    })
}

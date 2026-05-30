//! KV event handler: converts ZMQ events to prefix-index operations.
//!
//! Ported from Go: kvevent/event_handler.go

use std::sync::Arc;

use tracing::{debug, error, warn};

use crate::prefix_index::PrefixCacheTable;
use crate::types::*;

/// Handles KV events by routing them to the prefix cache table.
pub struct KVEventHandler {
    pub instance_id: String,
    pub model_name: String,
    pub lora_name: String,
    pub block_size: i64,
    pub additional_salt: String,
    pub tenant_id: String,
    pub indexer: Arc<PrefixCacheTable>,
}

impl KVEventHandler {
    pub fn new(indexer: Arc<PrefixCacheTable>, instance_id: String, model_name: String) -> Self {
        Self {
            instance_id,
            model_name,
            lora_name: String::new(),
            block_size: 0,
            additional_salt: String::new(),
            tenant_id: "default".into(),
            indexer,
        }
    }

    /// Process a single KV event.
    pub fn handle_event(&self, event: &KVEventData, dp_rank: i64) {
        match event {
            KVEventData::BlockStored(e) => self.handle_block_stored(e, dp_rank),
            KVEventData::BlockRemoved(e) => self.handle_block_removed(e, dp_rank),
            _ => warn!("Unknown event type: {:?}", event.event_type()),
        }
    }

    fn handle_block_stored(&self, event: &BlockStoredEvent, dp_rank: i64) {
        debug!(
            "BlockStored: instance_id={}, dp_rank={}, blocks={}",
            self.instance_id,
            dp_rank,
            event.block_hashes.len()
        );

        let stored = StoredEvent {
            block_hashes: event.block_hashes.clone(),
            block_size: event.block_size,
            model_name: self.model_name.clone(),
            lora_name: self.lora_name.clone(),
            instance_id: self.instance_id.clone(),
            parent_block_hash: event.parent_block_hash,
            token_ids: event.token_ids.clone(),
            medium: event.medium.clone(),
        };

        if let Err(e) = self
            .indexer
            .process_store_event(&stored, dp_rank, &self.instance_id)
        {
            error!("process_store_event failed: {}", e);
        }

        debug!("handle_block_stored: stored_event={:?}", stored);
    }

    fn handle_block_removed(&self, event: &BlockRemovedEvent, dp_rank: i64) {
        debug!(
            "BlockRemoved: instance_id={}, dp_rank={}, blocks={}",
            self.instance_id,
            dp_rank,
            event.block_hashes.len()
        );

        let removed = RemovedEvent {
            block_hashes: event.block_hashes.clone(),
            model_name: self.model_name.clone(),
            lora_name: self.lora_name.clone(),
            instance_id: self.instance_id.clone(),
            block_size: self.block_size,
            medium: String::new(),
        };

        if let Err(e) = self
            .indexer
            .process_remove_event(&removed, dp_rank, &self.instance_id)
        {
            error!("process_remove_event failed: {}", e);
        }

        debug!("handle_block_removed: removed_event={:?}", removed);
    }
}

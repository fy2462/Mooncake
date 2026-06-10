// ============================================================================
// KV Event Handler — ZMQ 事件 → 前缀索引操作
//
// Converts decoded ZMQ KV events (BlockStored, BlockRemoved) into prefix-index
// operations (process_store_event, process_remove_event). Each KVEventHandler
// is bound to a single service instance and holds a reference to the shared
// PrefixCacheTable.
//
// 将解码后的 ZMQ KV 事件（BlockStored、BlockRemoved）转换为前缀索引操作
// （process_store_event、process_remove_event）。每个 KVEventHandler
// 绑定到单个服务实例并持有共享 PrefixCacheTable 的引用。
//
// Ported from Go: mooncake-conductor/conductor-ctrl/kvevent/event_handler.go
// ============================================================================

use std::sync::Arc;

use tracing::{debug, error};

use crate::prefix_index::PrefixCacheTable;
use crate::types::*;

/// Routes KV events from ZMQ to the prefix cache table for indexing.
/// 将 ZMQ 的 KV 事件路由到前缀缓存表进行索引。
pub struct KVEventHandler {
    /// Unique instance identifier. / 唯一实例标识符。
    pub instance_id: String,
    /// Model name for this handler. / 此处理器对应的模型名称。
    pub model_name: String,
    /// LoRA adapter name. / LoRA 适配器名称。
    pub lora_name: String,
    /// Block size for hash computation. / 哈希计算的块大小。
    pub block_size: i64,
    /// Additional salt for hash separation. / 哈希隔离的额外盐值。
    pub additional_salt: String,
    /// Tenant ID for multi-tenant isolation. / 租户隔离 ID。
    pub tenant_id: String,
    /// Reference to the shared prefix cache table. / 共享前缀缓存表的引用。
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

    /// Dispatch a single KV event to the appropriate handler.
    /// 将单个 KV 事件分派到相应的处理器。
    pub fn handle_event(&self, event: &KVEventData, dp_rank: i64) {
        match event {
            KVEventData::BlockStored(e) => self.handle_block_stored(e, dp_rank),
            KVEventData::BlockRemoved(e) => self.handle_block_removed(e, dp_rank),
            KVEventData::BlockUpdate(e) => self.handle_block_update(e, dp_rank),
            KVEventData::AllBlocksCleared(e) => self.handle_all_blocks_cleared(e),
        }
    }

    /// Handle a BlockStored event: convert to StoredEvent and index.
    /// 处理 BlockStored 事件：转换为 StoredEvent 并建立索引。
    fn handle_block_stored(&self, event: &BlockStoredEvent, dp_rank: i64) {
        debug!(
            "BlockStored: instance_id={}, dp_rank={}, blocks={}",
            self.instance_id,
            dp_rank,
            event.block_hashes.len()
        );

        // Map ZMQ event fields to the conductor-internal StoredEvent.
        // 将 ZMQ 事件字段映射到 conductor 内部 StoredEvent。
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

    /// Handle a BlockRemoved event: convert to RemovedEvent and remove from index.
    /// 处理 BlockRemoved 事件：转换为 RemovedEvent 并从索引中移除。
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

    fn handle_block_update(&self, event: &BlockUpdateEvent, dp_rank: i64) {
        let stored = StoredEvent {
            block_hashes: event.block_hashes.clone(),
            block_size: event.block_size,
            model_name: choose_event_or_handler_model(&event.model_name, &self.model_name),
            lora_name: self.lora_name.clone(),
            instance_id: self.instance_id.clone(),
            parent_block_hash: event.parent_block_hash,
            token_ids: event.token_ids.clone(),
            medium: "GPU".to_string(),
        };
        if let Err(e) = self
            .indexer
            .process_store_event(&stored, dp_rank, &self.instance_id)
        {
            error!("process block update failed: {}", e);
        }
    }

    fn handle_all_blocks_cleared(&self, event: &AllBlocksClearedEvent) {
        let model_name = choose_event_or_handler_model(&event.model_name, &self.model_name);
        self.indexer.clear_model_context(
            &model_name,
            &self.lora_name,
            self.block_size,
            &self.additional_salt,
            &self.tenant_id,
        );
    }
}

fn choose_event_or_handler_model(event_model: &str, handler_model: &str) -> String {
    if event_model.is_empty() {
        handler_model.to_string()
    } else {
        event_model.to_string()
    }
}

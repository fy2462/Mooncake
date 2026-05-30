//! # Service Helpers — 服务工具函数 / Utility Functions
//!
//! 本模块提供 gRPC handler 之间共用的工具函数，包括：
//! - 客户端和 segment 的地址/归属关系查询
//! - 副本分配与释放
//! - Segment 使用量同步
//! - Lease 过期检查
//! - 失效 handle 清理
//! - View version 管理
//!
//! This module provides utility functions shared across gRPC handlers, including:
//! - Client and segment address/ownership lookups
//! - Replica allocation and release
//! - Segment usage synchronization
//! - Lease expiry checks
//! - Stale handle cleanup
//! - View version management

use crate::http_metadata::MetadataState;
use crate::metrics;
use chrono::Utc;
use mooncake_store_core::{ReplicaDescriptor, ReplicaType, ReplicateConfig};
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use tonic::Status;
use uuid::Uuid;

use super::background_ops::{clear_offloading_task, clear_promotion_task};
use super::state::{ClientEntry, MasterState, ObjectEntry};

/// 递增全局 view_version，触发所有客户端感知拓扑变更并重新拉取最新视图。
/// Increment global view_version, triggering all clients to detect topology changes and re-fetch.
pub(crate) fn bump_view_version(state: &MasterState) -> i64 {
    state.view_version.fetch_add(1, Ordering::Relaxed) + 1
}

/// 从 segment 名称（格式 host:port）中提取 host 部分。
/// Extract host part from segment name (format: host:port).
pub(crate) fn host_from_segment_name(name: &str) -> String {
    name.split(':').next().unwrap_or(name).to_string()
}

/// 从 segment 名称中提取端口号，解析失败返回 0。
/// Extract port from segment name; returns 0 on parse failure.
pub(crate) fn port_from_segment_name(name: &str) -> u16 {
    name.split(':')
        .nth(1)
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0)
}

/// 合并地址列表，去重保留已有地址，追加新地址。
/// Merge address lists: deduplicate, retain existing, append new.
fn merge_addresses(
    existing: &[String],
    new_addresses: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut merged = existing.to_vec();
    for address in new_addresses {
        if !address.is_empty() && !merged.iter().any(|v| v == &address) {
            merged.push(address);
        }
    }
    merged
}

/// 更新或插入客户端信息：已有客户端合并地址并更新 last_ping，新客户端创建 entry。
/// Upsert client info: merge addresses and update last_ping for existing clients,
/// create a new entry for new clients.
pub(crate) fn upsert_client_addresses(
    state: &MasterState,
    client_id: Uuid,
    addresses: Vec<String>,
) {
    let now = Utc::now();
    if let Some(mut entry) = state.clients.get_mut(&client_id) {
        // 已有客户端：合并地址，更新心跳时间
        // Existing client: merge addresses, update ping time
        entry.info.addresses = merge_addresses(&entry.info.addresses, addresses);
        entry.info.last_seen = now;
        entry.last_ping = SystemTime::now();
        return;
    }

    // 新客户端：创建完整的ClientInfo并插入
    // New client: create full ClientInfo and insert
    state.clients.insert(
        client_id,
        ClientEntry {
            info: mooncake_store_core::ClientInfo {
                id: client_id,
                addresses,
                segments: vec![],
                last_seen: now,
            },
            last_ping: SystemTime::now(),
        },
    );
}

/// 同步客户端的 segment 列表：将属于该 client 的所有 segment 写回 client.info.segments。
/// Sync client segment list: write all segments belonging to this client back to client.info.segments.
pub(crate) fn sync_client_segments(state: &MasterState, client_id: Uuid) {
    let segments = state
        .segments
        .iter()
        .filter(|entry| entry.client_id == client_id)
        .map(|entry| entry.segment.clone())
        .collect::<Vec<_>>();
    if let Some(mut client) = state.clients.get_mut(&client_id) {
        client.info.segments = segments;
        client.info.last_seen = Utc::now();
    }
}

/// 向 HTTP metadata 服务注册 segment 名称对应的节点。
/// Register nodes in HTTP metadata server from segment names (host:port).
pub(crate) async fn register_metadata_segments(
    metadata_state: &MetadataState,
    segment_names: &[String],
) {
    for segment_name in segment_names {
        let host = host_from_segment_name(segment_name);
        if host.is_empty() {
            continue;
        }
        metadata_state
            .register_node(host, port_from_segment_name(segment_name), vec![])
            .await;
    }
}

/// 按 segment 名称查找所属客户端 UUID（仅 Memory segment）。
/// Look up owning client UUID by Memory segment name.
pub(crate) fn client_id_by_segment_name(state: &MasterState, segment_name: &str) -> Option<Uuid> {
    state
        .segments
        .iter()
        .find(|entry| entry.segment.name == segment_name)
        .map(|entry| entry.client_id)
}

/// 获取对象的 owner client_id：优先使用 replica 的 holder_client_id，
/// fallback 到 segment 所属客户端。
/// Get object's owner client_id: prefer replica's holder_client_id,
/// fallback to the segment's owning client.
pub(crate) fn object_owner_client_id(state: &MasterState, object: &ObjectEntry) -> Option<Uuid> {
    object.replicas.iter().find_map(|replica| {
        replica
            .holder_client_id
            .or_else(|| client_id_by_replica_segment_name(state, &replica.segment_name))
    })
}

/// 根据已分配的 Memory 副本所在 host，查找同 host 上的 NoF segment 名作为优选目标。
/// 用于 prefer_alloc_in_same_node 策略，降低 Memory↔NoF 跨节点数据传输延迟。
///
/// Find NoF segment names on the same host as already-allocated Memory replicas.
/// Used by prefer_alloc_in_same_node strategy to reduce cross-node data transfer latency.
pub(crate) fn preferred_nof_segment_names(
    state: &MasterState,
    replicas: &[ReplicaDescriptor],
) -> Vec<String> {
    let mut names = Vec::new();
    for replica in replicas {
        if replica.replica_type != ReplicaType::Memory {
            continue;
        }
        let host = host_from_segment_name(&replica.segment_name);
        for segment in state.nof_segments.iter() {
            if host_from_segment_name(&segment.segment.name) == host
                && !names.iter().any(|name| name == &segment.segment.name)
            {
                names.push(segment.segment.name.clone());
            }
        }
    }
    names
}

/// 按 NoF segment 名称查找所属客户端 UUID。
/// Look up owning client UUID by NoF segment name.
pub(crate) fn client_id_by_nof_segment_name(
    state: &MasterState,
    segment_name: &str,
) -> Option<Uuid> {
    state
        .nof_segments
        .iter()
        .find(|entry| entry.segment.name == segment_name)
        .map(|entry| entry.segment.client_id)
}

/// 按 segment 名称查找客户端（先查 Memory 再查 NoF）。
/// Look up client by segment name (Memory first, then NoF).
pub(crate) fn client_id_by_replica_segment_name(
    state: &MasterState,
    segment_name: &str,
) -> Option<Uuid> {
    client_id_by_segment_name(state, segment_name)
        .or_else(|| client_id_by_nof_segment_name(state, segment_name))
}

/// 卸载客户端拥有的 Memory segment，校验所有权后从 segments 表和 allocator 移除。
/// 返回 true 表示成功移除。
///
/// Unmount a client-owned Memory segment: verify ownership, then remove from
/// segments table and allocator. Returns true on successful removal.
pub(crate) fn unmount_segment_owned(
    state: &MasterState,
    segment_id: Uuid,
    client_id: Uuid,
) -> bool {
    let owned = state
        .segments
        .get(&segment_id)
        .map(|entry| entry.client_id == client_id)
        .unwrap_or(false);
    if !owned {
        return false;
    }

    state.segments.remove(&segment_id);
    state.allocator.write().remove_segment(&segment_id);
    sync_client_segments(state, client_id);
    metrics::SEGMENT_COUNT.set(state.segments.len() as i64);
    true
}

/// 卸载客户端拥有的 NoF segment，同时从 nof_segments 表和 nof_allocator 移除。
/// Unmount a client-owned NoF segment: remove from nof_segments table and nof_allocator.
pub(crate) fn unmount_nof_segment_owned(
    state: &MasterState,
    segment_id: Uuid,
    client_id: Uuid,
) -> bool {
    let owned = state
        .nof_segments
        .get(&segment_id)
        .map(|entry| entry.segment.client_id == client_id)
        .unwrap_or(false);
    if !owned {
        return false;
    }

    state.nof_segments.remove(&segment_id);
    state.nof_allocator.write().remove_segment(&segment_id);
    true
}

/// 获取客户端的地址列表：优先使用 clients 表的 addresses，fallback 到 segment host 名。
/// Get client addresses: prefer clients table addresses, fallback to segment host names.
pub(crate) fn addresses_for_client(state: &MasterState, client_id: Uuid) -> Vec<String> {
    if let Some(entry) = state.clients.get(&client_id) {
        if !entry.info.addresses.is_empty() {
            return entry.info.addresses.clone();
        }
    }

    let mut addresses = Vec::new();
    for segment in state.segments.iter() {
        if segment.client_id == client_id {
            let host = host_from_segment_name(&segment.segment.name);
            if !host.is_empty() && !addresses.iter().any(|v| v == &host) {
                addresses.push(host);
            }
        }
    }
    addresses
}

/// 从 allocator 同步指定 Memory segment 的 used 字节数到 segments 表。
/// Sync used bytes for specified Memory segments from allocator to segments table.
pub(crate) fn sync_segment_usage(state: &MasterState, segment_ids: impl IntoIterator<Item = Uuid>) {
    let allocator = state.allocator.read();
    for segment_id in segment_ids {
        let Some(used) = allocator.used_bytes(&segment_id) else {
            continue;
        };
        if let Some(mut entry) = state.segments.get_mut(&segment_id) {
            entry.used = used;
        }
    }
}

/// 从 nof_allocator 同步指定 NoF segment 的 used 字节数到 nof_segments 表。
/// Sync used bytes for specified NoF segments from nof_allocator to nof_segments table.
pub(crate) fn sync_nof_segment_usage(
    state: &MasterState,
    segment_ids: impl IntoIterator<Item = Uuid>,
) {
    let allocator = state.nof_allocator.read();
    for segment_id in segment_ids {
        let Some(used) = allocator.used_bytes(&segment_id) else {
            continue;
        };
        if let Some(mut entry) = state.nof_segments.get_mut(&segment_id) {
            entry.used = used;
        }
    }
}

/// 释放副本 back 到 allocator：按类型分拣 Memory 和 NoF 副本各自释放，并同步 usage。
/// Release replicas back to allocator: separate Memory and NoF replicas by type,
/// release each to the appropriate allocator, and sync usage.
pub(crate) fn release_replicas(state: &MasterState, replicas: &[ReplicaDescriptor]) {
    let memory = replicas
        .iter()
        .filter(|r| r.replica_type == ReplicaType::Memory)
        .cloned()
        .collect::<Vec<_>>();
    if !memory.is_empty() {
        let segment_ids = memory.iter().map(|r| r.segment_id).collect::<Vec<_>>();
        state.allocator.write().release(&memory);
        sync_segment_usage(state, segment_ids);
    }

    let nof = replicas
        .iter()
        .filter(|r| r.replica_type == ReplicaType::NoFSsd)
        .cloned()
        .collect::<Vec<_>>();
    if !nof.is_empty() {
        let segment_ids = nof.iter().map(|r| r.segment_id).collect::<Vec<_>>();
        state.nof_allocator.write().release(&nof);
        sync_nof_segment_usage(state, segment_ids);
    }
}

/// 分配 NoF 副本：逐个分配（每次 1 个），避免一次分配多个错过同 host 优化。
/// 分配数量不超过已挂载 NoF segment 总数，防止无意义的重复分配。
///
/// Allocate NoF replicas: allocate one at a time to avoid missing same-host optimization.
/// Allocation count is capped at total mounted NoF segments to prevent redundant allocation.
pub(crate) fn allocate_nof_replicas(
    state: &MasterState,
    key: &str,
    size: u64,
    count: usize,
    preferred_segment_names: &[String],
) -> Result<Vec<ReplicaDescriptor>, Status> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if state.nof_segments.is_empty() {
        return Err(Status::failed_precondition("no NoF segments mounted"));
    }

    let mut replicas = Vec::with_capacity(count);
    let mut used_names = Vec::new();
    for idx in 0..count {
        let preferred_segment = preferred_segment_names
            .iter()
            .find(|name| !used_names.iter().any(|used| used == *name))
            .cloned()
            .unwrap_or_default();
        let config = ReplicateConfig {
            preferred_segment,
            replica_num: 1,
            ..Default::default()
        };
        let allocated = state
            .nof_allocator
            .write()
            .allocate(key, size, 1, &config)
            .into_iter()
            .next()
            .ok_or(Status::resource_exhausted("no available NoF segment"))?;
        used_names.push(allocated.segment_name.clone());
        let mut replica = allocated;
        replica.replica_type = ReplicaType::NoFSsd;
        replicas.push(replica);
        // 不超过已挂载的 NoF segment 总数
        // Cap at total mounted NoF segments
        if idx + 1 >= state.nof_segments.len() {
            break;
        }
    }
    sync_nof_segment_usage(state, replicas.iter().map(|r| r.segment_id));
    Ok(replicas)
}

/// 计算当前所有 Memory segment 的内存使用率（已用/总量）。
/// Compute current memory usage ratio across all Memory segments (used / total).
pub(crate) fn memory_usage_ratio(state: &MasterState) -> f64 {
    let (total_bytes, used_bytes) = state.allocator.read().usage_totals();
    if total_bytes == 0 {
        return 0.0;
    }
    used_bytes as f64 / total_bytes as f64
}

/// 立即释放副本（非延迟）/ Immediately release replicas (non-delayed).
pub(crate) fn release_replicas_scheduled(state: &MasterState, replicas: Vec<ReplicaDescriptor>) {
    release_replicas(state, &replicas);
}

/// Helper: 释放对象副本并同时清理关联的 offload/promotion 任务。
/// Release object replicas and simultaneously clean up associated offload/promotion tasks.
pub(crate) fn release_object_replicas(
    state: &MasterState,
    key: &str,
    replicas: &[ReplicaDescriptor],
) {
    if replicas.is_empty() {
        return;
    }
    clear_offloading_task(state, key);
    clear_promotion_task(state, key);
    release_replicas(state, replicas);
}

/// 检查对象的 lease 是否已过期（或从未设置），用于决定是否允许删除/驱逐等操作。
/// Check whether an object's lease has expired (or was never set).
/// Used to decide whether delete/evict operations are allowed.
pub(crate) fn is_lease_expired(entry: &ObjectEntry) -> bool {
    entry
        .lease_timeout
        .is_none_or(|timeout| timeout <= SystemTime::now())
}

/// 获取当前存活客户端的 UUID 快照。
/// C++ 等价：`MasterService::getAliveClientsSnapshot()`（master_service.cpp:587）。
/// 如果客户端的上次 ping 时间在 `client_live_ttl` 内，则认为存活。
///
/// Get a snapshot of currently alive client UUIDs.
/// C++ equivalent: `MasterService::getAliveClientsSnapshot()` (master_service.cpp:587).
/// A client is considered alive if its last ping is within `client_live_ttl`.
pub(crate) fn get_alive_clients_snapshot(state: &MasterState) -> HashSet<Uuid> {
    let now = SystemTime::now();
    let ttl = state.runtime_config.client_live_ttl;
    state
        .clients
        .iter()
        .filter_map(|entry| {
            let elapsed = now.duration_since(entry.last_ping).unwrap_or_default();
            if elapsed <= ttl {
                Some(entry.info.id)
            } else {
                None
            }
        })
        .collect()
}

/// 清理指定对象中失效的副本。
///
/// C++ 等价：`MasterService::CleanupStaleHandles()`（master_service.cpp:2832-2846）。
/// 移除以下类型的副本：
/// 1. `handle_valid == false` 的 MEMORY / NoF 副本（handle 已失效）
/// 2. `holder_client_id` 不在 `alive_clients` 中的 LOCAL_DISK 副本（客户端已死亡）
///
/// 仅清理状态为 Complete 的副本。返回 `true` 表示对象应该被完全移除
/// （所有有效副本已被清理，调用者应删除该对象）。
///
/// Clean up stale replicas within a given object.
/// Removes:
/// 1. MEMORY / NoF replicas with `handle_valid == false` (handle invalidated)
/// 2. LOCAL_DISK replicas whose `holder_client_id` is not in `alive_clients` (client dead)
///
/// Only cleans replicas in Complete status. Returns `true` if the object should be
/// fully removed (all live replicas cleaned; caller should delete the object).
pub(crate) fn cleanup_stale_handles(
    entry: &mut ObjectEntry,
    alive_clients: &HashSet<Uuid>,
) -> bool {
    let original_len = entry.replicas.len();

    entry.replicas.retain(|r| {
        if r.status != mooncake_store_core::ReplicaStatus::Complete {
            return true; // 保留非 Complete 状态的副本 / Retain non-Complete replicas
        }
        let is_stale = match r.replica_type {
            ReplicaType::Memory | ReplicaType::NoFSsd => !r.handle_valid,
            ReplicaType::LocalDisk => r
                .holder_client_id
                .is_some_and(|cid| !alive_clients.contains(&cid)),
            _ => false,
        };
        !is_stale
    });

    // 检查是否还有有效副本 / Check if any valid replicas remain
    let has_completed = entry
        .replicas
        .iter()
        .any(|r| r.status == mooncake_store_core::ReplicaStatus::Complete);

    // 如果清理掉了一些副本，且没有有效的 Complete 副本残留，对象应该被移除
    // If some replicas were cleaned and no valid Complete replicas remain, the object should be removed
    entry.replicas.len() != original_len && !has_completed
}

// =============================================================================
// Tenant helpers — 租户工具函数
// C++ equivalent: NormalizeTenantId in types.h, MakeTenantScopedKey / MakeObjectIdentity in master_service.h
// =============================================================================

/// Sentinel delimiter between tenant_id and user_key.
/// NUL byte ('\0') is used because it cannot appear in user-provided keys.
/// NUL 字节分隔符，因为用户提供的 key 不能包含 '\0'。
pub const TENANT_SCOPE_DELIMITER: char = '\0';

/// Default tenant identifier when none is provided.
/// 未提供租户标识符时的默认值。
/// C++ equivalent: NormalizeTenantId("") -> "default"
pub const DEFAULT_TENANT: &str = "default";

/// Normalize an incoming tenant_id: empty string → "default".
/// 规范化传入的 tenant_id：空字符串 → "default"。
/// C++ equivalent: types.h:225-227 NormalizeTenantId()
pub fn normalize_tenant_id(tenant_id: &str) -> String {
    if tenant_id.is_empty() {
        DEFAULT_TENANT.to_string()
    } else {
        tenant_id.to_string()
    }
}

/// Build a tenant-scoped internal key: `"{tenant_id}\0{user_key}"`.
/// 构造租户作用域的内部 key："{tenant_id}\0{user_key}"。
/// C++ equivalent: master_service.h MakeTenantScopedKey()
pub fn make_tenant_scoped_key(tenant_id: &str, user_key: &str) -> String {
    let tenant = normalize_tenant_id(tenant_id);
    let mut buf = String::with_capacity(tenant.len() + 1 + user_key.len());
    buf.push_str(&tenant);
    buf.push(TENANT_SCOPE_DELIMITER);
    buf.push_str(user_key);
    buf
}

/// Reverse: extract (tenant_id, user_key) from a scoped key.
/// If no delimiter is found, the whole key is the user_key with tenant="default"
/// (handles legacy/upgrade keys).
///
/// 反向提取：从作用域 key 中提取 (tenant_id, user_key)。
/// 如果找不到分隔符，整个 key 即为 user_key，tenant 为 "default"（处理旧格式/升级数据）。
pub fn split_scoped_key(scoped: &str) -> (String, String) {
    scoped
        .find(TENANT_SCOPE_DELIMITER)
        .map(|pos| {
            (
                scoped[..pos].to_string(),
                scoped[pos + 1..].to_string(),
            )
        })
        .unwrap_or_else(|| (DEFAULT_TENANT.to_string(), scoped.to_string()))
}

/// Validate that a user key does not contain the tenant scope delimiter.
/// Returns Ok(()) if valid, Err(Status) with invalid_argument if it contains '\0'.
/// 验证用户 key 不包含租户作用域分隔符。
/// 有效时返回 Ok(())，包含 '\0' 时返回 invalid_argument 的 Err(Status)。
pub fn validate_user_key(key: &str) -> Result<(), Status> {
    if key.contains(TENANT_SCOPE_DELIMITER) {
        Err(Status::invalid_argument(format!(
            "key must not contain NUL byte (U+0000)"
        )))
    } else {
        Ok(())
    }
}

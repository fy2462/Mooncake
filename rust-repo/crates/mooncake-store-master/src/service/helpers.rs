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

use crate::allocator::{AllocationStrategy, SsdUsageMetrics};
use crate::http_metadata::MetadataState;
use crate::metrics;
use chrono::Utc;
use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, TaskStatus,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use tonic::Status;
use uuid::Uuid;

use super::background_ops::{clear_offloading_task, clear_promotion_task};
use super::state::{ClientEntry, MasterRuntimeConfig, MasterState, ObjectEntry};

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

pub(crate) fn storage_fs_dir_for_client(config: &MasterRuntimeConfig) -> String {
    if config.storage_fs_dir.trim().is_empty() || config.cluster_id.trim().is_empty() {
        return String::new();
    }
    std::path::Path::new(&config.storage_fs_dir)
        .join(config.cluster_id.trim())
        .to_string_lossy()
        .into_owned()
}

/// Build per-client local SSD usage metrics from reported capacity and LocalDisk replicas.
/// 从客户端上报容量和 LocalDisk 副本构造本地 SSD 使用指标。
pub(crate) fn local_ssd_usage_metrics(state: &MasterState) -> HashMap<Uuid, SsdUsageMetrics> {
    let mut metrics = state
        .local_disk_segments
        .iter()
        .map(|entry| {
            (
                *entry.key(),
                SsdUsageMetrics {
                    total_capacity_bytes: entry.value().ssd_total_capacity_bytes.max(0) as u64,
                    used_bytes: 0,
                },
            )
        })
        .collect::<HashMap<_, _>>();

    for object in state.objects.iter() {
        for replica in &object.value().replicas {
            if replica.replica_type != ReplicaType::LocalDisk {
                continue;
            }
            let Some(client_id) = replica.holder_client_id else {
                continue;
            };
            let entry = metrics.entry(client_id).or_insert(SsdUsageMetrics {
                total_capacity_bytes: 0,
                used_bytes: 0,
            });
            entry.used_bytes = entry.used_bytes.saturating_add(replica.size);
        }
    }
    metrics
}

/// Allocate memory replicas, using SSD free-ratio metrics only when that strategy is configured.
/// 分配内存副本；仅在配置 ssd_free_ratio_first 时引入本地 SSD 空闲率指标。
pub(crate) fn allocate_memory_replicas(
    state: &MasterState,
    key: &str,
    client_id: Option<Uuid>,
    size: u64,
    count: usize,
    config: &ReplicateConfig,
) -> Vec<ReplicaDescriptor> {
    let use_ssd_metrics =
        state.allocator.read().allocation_strategy() == AllocationStrategy::SsdFreeRatioFirst;
    if use_ssd_metrics {
        let ssd_metrics = local_ssd_usage_metrics(state);
        state
            .allocator
            .write()
            .allocate_for_client_with_ssd_metrics(key, client_id, size, count, config, &ssd_metrics)
    } else {
        state
            .allocator
            .write()
            .allocate_for_client(key, client_id, size, count, config)
    }
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

pub(crate) fn default_drain_target_segments(
    state: &MasterState,
    draining_segments: &HashSet<String>,
) -> Vec<String> {
    let mut targets = state
        .segments
        .iter()
        .filter(|entry| {
            entry.status == crate::proto::SegmentStatus::Active
                && !draining_segments.contains(&entry.segment.name)
        })
        .map(|entry| entry.segment.name.clone())
        .collect::<Vec<_>>();
    targets.sort();
    targets
}

pub(crate) fn choose_drain_target_segment(
    state: &MasterState,
    object: &ObjectEntry,
    source_segment: &str,
    candidates: &[String],
) -> Option<String> {
    candidates
        .iter()
        .filter(|target| target.as_str() != source_segment)
        .filter(|target| {
            !object
                .replicas
                .iter()
                .any(|replica| replica.segment_name == **target)
        })
        .filter_map(|target| {
            state
                .segments
                .iter()
                .find(|entry| {
                    entry.segment.name == *target
                        && entry.status == crate::proto::SegmentStatus::Active
                })
                .map(|entry| (target.clone(), entry.used, entry.segment.size))
        })
        .min_by(|(_, used_a, size_a), (_, used_b, size_b)| {
            ((*used_a as u128) * (*size_b as u128)).cmp(&((*used_b as u128) * (*size_a as u128)))
        })
        .map(|(target, _, _)| target)
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

    let invalidated = HashSet::from([segment_id]);
    invalidate_replicas_on_segments(state, &invalidated);
    state.segments.remove(&segment_id);
    state.allocator.write().remove_segment(&segment_id);
    let alive_clients = get_alive_clients_snapshot(state);
    clear_invalid_handles(state, &alive_clients);
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

    let invalidated = HashSet::from([segment_id]);
    invalidate_replicas_on_segments(state, &invalidated);
    state.nof_segments.remove(&segment_id);
    state.nof_allocator.write().remove_segment(&segment_id);
    state.nof_heartbeat_states.remove(&segment_id);
    let alive_clients = get_alive_clients_snapshot(state);
    clear_invalid_handles(state, &alive_clients);
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

    let config = ReplicateConfig {
        preferred_segments: preferred_segment_names.to_vec(),
        replica_num: count as u32,
        ..Default::default()
    };
    let mut replicas = state
        .nof_allocator
        .write()
        .allocate(key, size, count, &config);
    if replicas.len() != count {
        let allocated = replicas.len();
        let segment_ids = replicas.iter().map(|r| r.segment_id).collect::<Vec<_>>();
        state.nof_allocator.write().release(&replicas);
        sync_nof_segment_usage(state, segment_ids);
        return Err(Status::resource_exhausted(format!(
            "failed to allocate {count} NoF replica(s), allocated {allocated}"
        )));
    }
    for replica in &mut replicas {
        replica.replica_type = ReplicaType::NoFSsd;
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

pub(crate) fn account_removed_object_quota(state: &MasterState, object: &ObjectEntry) {
    if !state.runtime_config.enable_tenant_quota {
        return;
    }
    let mut quotas = state.tenant_quotas.write();
    let result = if object.quota_committed {
        quotas.release(&object.tenant_id, object.size)
    } else {
        quotas.abort(&object.tenant_id, object.size)
    };
    if let Err(error) = result {
        tracing::warn!(
            tenant_id = %object.tenant_id,
            size = object.size,
            quota_committed = object.quota_committed,
            ?error,
            "tenant quota accounting failed while removing object"
        );
    }
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
        .any(|r| r.status == ReplicaStatus::Complete);

    // 如果清理掉了一些副本，且没有有效的 Complete 副本残留，对象应该被移除
    // If some replicas were cleaned and no valid Complete replicas remain, the object should be removed
    entry.replicas.len() != original_len && !has_completed
}

/// Mark complete Memory/NoF replicas on the provided segments invalid.
/// This mirrors the C++ prepare-unmount phase, after which ClearInvalidHandles
/// removes the invalid metadata.
pub(crate) fn invalidate_replicas_on_segments(state: &MasterState, segment_ids: &HashSet<Uuid>) {
    if segment_ids.is_empty() {
        return;
    }
    for mut object in state.objects.iter_mut() {
        for replica in &mut object.replicas {
            if matches!(
                replica.replica_type,
                ReplicaType::Memory | ReplicaType::NoFSsd
            ) && segment_ids.contains(&replica.segment_id)
            {
                replica.handle_valid = false;
            }
        }
    }
}

/// Sweep all metadata and drop stale handles, cleaning per-key task state when
/// no valid complete replica remains.
pub(crate) fn clear_invalid_handles(state: &MasterState, alive_clients: &HashSet<Uuid>) {
    let mut remove_keys = Vec::new();
    for mut object in state.objects.iter_mut() {
        if cleanup_stale_handles(&mut object, alive_clients) {
            remove_keys.push(object.key().clone());
        }
    }

    for key in remove_keys {
        if let Some((_, object)) = state.objects.remove(&key) {
            account_removed_object_quota(state, &object);
        }
        state.processing_keys.remove(&key);
        state.replication_tasks.remove(&key);
        clear_offloading_task(state, &key);
        clear_promotion_task(state, &key);
        for mut entry in state.client_objects.iter_mut() {
            entry.value_mut().remove(&key);
        }
    }
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
        .map(|pos| (scoped[..pos].to_string(), scoped[pos + 1..].to_string()))
        .unwrap_or_else(|| (DEFAULT_TENANT.to_string(), scoped.to_string()))
}

pub(crate) fn task_count_with_status(state: &MasterState, status: TaskStatus) -> usize {
    state
        .tasks
        .iter()
        .filter(|task| task.info.status == status)
        .count()
}

pub(crate) fn has_pending_task_capacity(state: &MasterState) -> bool {
    task_count_with_status(state, TaskStatus::Pending)
        < state.runtime_config.max_total_pending_tasks
}

pub(crate) fn processing_task_capacity(state: &MasterState) -> usize {
    state
        .runtime_config
        .max_total_processing_tasks
        .saturating_sub(task_count_with_status(state, TaskStatus::Processing))
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

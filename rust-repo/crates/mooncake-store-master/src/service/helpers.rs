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
use crate::ha::HaError;
use crate::http_metadata::MetadataState;
use crate::metrics;
use crate::tenant_id::TenantId;
use crate::tenant_quota::{TenantQuotaError, TenantQuotaTable};
use chrono::Utc;
use dashmap::DashMap;
use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, TaskStatus,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use tonic::Status;
use uuid::Uuid;

use super::background_ops::{cancel_promotion_task, clear_offloading_task, clear_promotion_task};
use super::state::{
    ClientEntry, DelayedReplicaReleaseEntry, MasterRuntimeConfig, MasterState, ObjectEntry,
};

/// 递增全局 view_version，触发所有客户端感知拓扑变更并重新拉取最新视图。
/// Increment global view_version, triggering all clients to detect topology changes and re-fetch.
pub(crate) fn bump_view_version(state: &MasterState) -> i64 {
    state.view_version.fetch_add(1, Ordering::Relaxed) + 1
}

/// 从 segment 名称中提取 C++ 兼容的 host identity。
/// Extract a C++-compatible host identity from a segment name.
///
/// Mirrors `ResolveMooncakeHostId`: trim whitespace, strip a host:port suffix
/// for ordinary hostnames, preserve raw IPv6 literals, and ignore loopback or
/// wildcard endpoints because they are not stable cross-node identities.
pub(crate) fn host_from_segment_name(name: &str) -> String {
    mooncake_store_core::resolve_host_id(name)
}

pub(crate) fn storage_fs_dir_for_client(config: &MasterRuntimeConfig) -> String {
    if config.storage_fs_dir.trim().is_empty() || config.cluster_id.trim().is_empty() {
        return String::new();
    }
    let root = std::path::Path::new(&config.storage_fs_dir).join(config.cluster_id.trim());
    let root = if root.is_absolute() {
        root
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("."))
            .join(root)
    };
    root.to_string_lossy().into_owned()
}

/// Build the durable shared-filesystem descriptor used by the global DISK
/// tier. The digest is computed from the tenant-scoped key so equal user keys
/// in different tenants can never alias the same path.
pub(crate) fn global_disk_replica(
    state: &MasterState,
    scoped_key: &str,
    size: u64,
) -> Option<ReplicaDescriptor> {
    let root = storage_fs_dir_for_client(&state.runtime_config);
    if root.is_empty() {
        return None;
    }
    let digest = Sha256::digest(scoped_key.as_bytes());
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let path = std::path::Path::new(&root)
        .join("global-disk")
        .join(&digest[..2])
        .join(&digest[2..4])
        .join(format!("{digest}.data"));
    Some(ReplicaDescriptor {
        segment_id: Uuid::nil(),
        segment_name: path.to_string_lossy().into_owned(),
        offset: 0,
        size,
        status: ReplicaStatus::Allocating,
        replica_type: ReplicaType::Disk,
        holder_client_id: None,
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        refcnt: 0,
        handle_valid: true,
        base_addr: 0,
        protocol: String::new(),
    })
}

/// Return whether a completed replica is safe to expose on the data path.
///
/// LocalDisk descriptors remain durable while their process is offline, so
/// `Complete` alone is insufficient: the storage namespace must have a Ready
/// active session and the descriptor must point at that exact session.
pub(crate) fn replica_is_routable(state: &MasterState, replica: &ReplicaDescriptor) -> bool {
    if replica.status != ReplicaStatus::Complete {
        return false;
    }
    match replica.replica_type {
        ReplicaType::Memory | ReplicaType::NoFSsd => return replica.handle_valid,
        ReplicaType::Disk => return true,
        ReplicaType::All => return false,
        ReplicaType::LocalDisk => {}
    }
    if !replica.handle_valid {
        return false;
    }
    let (Some(storage_id), Some(holder_client_id), Some(_generation_id)) = (
        replica.local_disk_storage_id,
        replica.holder_client_id,
        replica.local_disk_generation_id,
    ) else {
        return false;
    };
    let session_matches = state
        .local_disk_client_sessions
        .get(&holder_client_id)
        .is_some_and(|bound_storage_id| *bound_storage_id == storage_id);
    session_matches
        && state
            .local_disk_segments
            .get(&storage_id)
            .is_some_and(|entry| {
                entry.recovery_complete && entry.active_client_id == Some(holder_client_id)
            })
}

/// Resolve a process session to its durable LocalDisk namespace only after the
/// current inventory transaction has committed.
pub(crate) fn ready_local_disk_storage_for_client(
    state: &MasterState,
    client_id: Uuid,
) -> Result<Uuid, Status> {
    let storage_id = state
        .local_disk_client_sessions
        .get(&client_id)
        .map(|entry| *entry)
        .ok_or(Status::permission_denied(
            "client has no active LocalDisk storage session",
        ))?;
    let ready = state
        .local_disk_segments
        .get(&storage_id)
        .is_some_and(|entry| entry.recovery_complete && entry.active_client_id == Some(client_id));
    if !ready {
        return Err(Status::failed_precondition(
            "local disk inventory recovery is not complete",
        ));
    }
    Ok(storage_id)
}

/// Build per-client local SSD usage metrics from reported capacity and LocalDisk replicas.
/// 从客户端上报容量和 LocalDisk 副本构造本地 SSD 使用指标。
pub(crate) fn local_ssd_usage_metrics(state: &MasterState) -> HashMap<Uuid, SsdUsageMetrics> {
    let mut metrics = state
        .local_disk_segments
        .iter()
        .filter_map(|entry| {
            if !entry.recovery_complete {
                return None;
            }
            entry.active_client_id.map(|client_id| {
                (
                    client_id,
                    SsdUsageMetrics {
                        total_capacity_bytes: entry.value().ssd_total_capacity_bytes.max(0) as u64,
                        used_bytes: 0,
                    },
                )
            })
        })
        .collect::<HashMap<_, _>>();

    for object in state.objects.iter() {
        for replica in &object.value().replicas {
            if replica.replica_type != ReplicaType::LocalDisk {
                continue;
            }
            let Some(storage_id) = replica.local_disk_storage_id else {
                continue;
            };
            let Some(client_id) = state
                .local_disk_segments
                .get(&storage_id)
                .and_then(|entry| {
                    if entry.recovery_complete {
                        entry.active_client_id
                    } else {
                        None
                    }
                })
            else {
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
    let excluded_segments = state
        .segments
        .iter()
        .filter(|entry| entry.status != crate::proto::SegmentStatus::Active)
        .map(|entry| entry.segment.name.clone())
        .collect::<Vec<_>>();
    let use_ssd_metrics =
        state.allocator.read().allocation_strategy() == AllocationStrategy::SsdFreeRatioFirst;
    if use_ssd_metrics {
        let ssd_metrics = local_ssd_usage_metrics(state);
        state.allocator.write().allocate_for_client_with_exclusions(
            key,
            client_id,
            size,
            count,
            config,
            &excluded_segments,
            Some(&ssd_metrics),
        )
    } else {
        state.allocator.write().allocate_for_client_with_exclusions(
            key,
            client_id,
            size,
            count,
            config,
            &excluded_segments,
            None,
        )
    }
}

/// 从 segment 名称中提取端口号，解析失败返回 0。
/// Extract port from segment name; returns 0 on parse failure.
pub(crate) fn port_from_segment_name(name: &str) -> u16 {
    let name = name.trim();
    if let Some(bracketed) = name.strip_prefix('[') {
        return bracketed
            .find(']')
            .and_then(|closing| bracketed[closing + 1..].strip_prefix(':'))
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(0);
    }
    if name.bytes().filter(|byte| *byte == b':').count() != 1 {
        return 0;
    }
    name.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
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

/// 获取对象的 owner client_id：优先使用对象记录的写入客户端；旧快照中该字段为 nil 时，
/// fallback 到 replica holder 或 segment 所属客户端。
/// Get an object's owner client ID from its recorded writer. For legacy snapshots
/// where that field is nil, fall back to a replica holder or segment owner.
pub(crate) fn object_owner_client_id(state: &MasterState, object: &ObjectEntry) -> Option<Uuid> {
    if object.client_id != Uuid::nil() {
        return Some(object.client_id);
    }
    object.replicas.iter().find_map(|replica| {
        replica
            .holder_client_id
            .or_else(|| client_id_by_exact_replica_segment(state, replica))
    })
}

/// Resolve the exact owner of a Memory/NoF replica by durable segment UUID.
/// Drain uses this instead of a name lookup because Rust permits same-name
/// segments and NoF sources do not live in the Memory segment table.
pub(crate) fn client_id_by_replica_segment_id(
    state: &MasterState,
    segment_id: Uuid,
    replica_type: ReplicaType,
) -> Option<Uuid> {
    match replica_type {
        ReplicaType::Memory => state.segments.get(&segment_id).map(|entry| entry.client_id),
        ReplicaType::NoFSsd => state
            .nof_segments
            .get(&segment_id)
            .map(|entry| entry.segment.client_id),
        ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => None,
    }
}

/// Resolve the owner only when a replica's durable UUID, type, and name all
/// identify the same mounted Memory/NoF segment.
pub(crate) fn client_id_by_exact_replica_segment(
    state: &MasterState,
    replica: &ReplicaDescriptor,
) -> Option<Uuid> {
    match replica.replica_type {
        ReplicaType::Memory => state
            .segments
            .get(&replica.segment_id)
            .filter(|entry| entry.segment.name == replica.segment_name)
            .map(|entry| entry.client_id),
        ReplicaType::NoFSsd => state
            .nof_segments
            .get(&replica.segment_id)
            .filter(|entry| entry.segment.name == replica.segment_name)
            .map(|entry| entry.segment.client_id),
        ReplicaType::Disk | ReplicaType::LocalDisk | ReplicaType::All => None,
    }
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
            let mut matches = state.segments.iter().filter(|entry| {
                entry.segment.name == *target && entry.status == crate::proto::SegmentStatus::Active
            });
            let entry = matches.next()?;
            let candidate = (target.clone(), entry.used, entry.segment.size);
            if matches.next().is_some() {
                return None;
            }
            Some(candidate)
        })
        .min_by(|(_, used_a, size_a), (_, used_b, size_b)| {
            ((*used_a as u128) * (*size_b as u128)).cmp(&((*used_b as u128) * (*size_a as u128)))
        })
        .map(|(target, _, _)| target)
}

/// Resolve one active Memory segment name to its exact durable identity.
///
/// Returning `None` for zero or multiple matches keeps legacy name-based
/// scheduling fail closed.
pub(crate) fn unique_active_memory_segment_id(
    state: &MasterState,
    segment_name: &str,
) -> Option<Uuid> {
    let mut matches = state.segments.iter().filter(|entry| {
        entry.segment.name == segment_name && entry.status == crate::proto::SegmentStatus::Active
    });
    let segment_id = matches.next()?.segment.id;
    matches.next().is_none().then_some(segment_id)
}

/// Resolve a legacy name-only target to one exact active Memory/NoF segment.
///
/// Rust permits same-name segments, so callers must fail closed unless the
/// name identifies exactly one physical allocation domain across both tables.
pub(crate) fn unique_active_replica_segment_identity(
    state: &MasterState,
    segment_name: &str,
) -> Option<(Uuid, ReplicaType)> {
    let memory = state
        .segments
        .iter()
        .filter(|entry| {
            entry.segment.name == segment_name
                && entry.status == crate::proto::SegmentStatus::Active
        })
        .map(|entry| (entry.segment.id, ReplicaType::Memory));
    let nof = state
        .nof_segments
        .iter()
        .filter(|entry| {
            entry.segment.name == segment_name
                && entry.status == crate::proto::SegmentStatus::Active
        })
        .map(|entry| (entry.segment.id, ReplicaType::NoFSsd));
    let mut matches = memory.chain(nof);
    let identity = matches.next()?;
    matches.next().is_none().then_some(identity)
}

/// 卸载客户端拥有的 Memory segment，校验所有权后从 segments 表和 allocator 移除。
/// 返回 true 表示成功移除。
///
/// Unmount a client-owned Memory segment: verify ownership, then remove from
/// segments table and allocator. Returns true on successful removal.
pub(crate) fn unmount_segment_owned_durable_locked(
    state: &MasterState,
    segment_id: Uuid,
    client_id: Uuid,
    operation: &str,
) -> Result<bool, Status> {
    let Some(segment) = state.segments.get(&segment_id) else {
        return Ok(false);
    };
    if segment.client_id != client_id {
        return Ok(false);
    }
    let segment_name = segment.segment.name.clone();
    drop(segment);

    if let Err(error) = state
        .oplog_manager
        .record_unmount_segment_durable(&segment_name, segment_id)
    {
        state.fence_after_durability_failure(operation, &error);
        return Err(Status::unavailable(
            "failed to persist segment unmount; segment remains mounted",
        ));
    }
    if !unmount_segment_owned_locked(state, segment_id, client_id) {
        state.fence_after_invariant_failure(
            operation,
            &format!(
                "durable Memory segment unmount could not be applied locally: \
                 segment_id={segment_id} client_id={client_id}"
            ),
        );
        return Err(Status::unavailable(
            "persisted segment unmount could not be applied locally",
        ));
    }
    bump_view_version(state);
    Ok(true)
}

pub(crate) fn unmount_segment_owned_locked(
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
    invalidate_replicas_on_segments_locked(state, &invalidated, ReplicaType::Memory);
    prune_delayed_replicas_on_segment(
        &state.delayed_replica_releases,
        segment_id,
        ReplicaType::Memory,
    );
    state.segments.remove(&segment_id);
    state.graceful_unmounts.remove(&segment_id);
    state.allocator.write().remove_segment(&segment_id);
    let alive_clients = get_alive_clients_snapshot(state);
    clear_invalid_handles_locked(state, &alive_clients);
    sync_client_segments(state, client_id);
    metrics::SEGMENT_COUNT.set(state.segments.len() as i64);
    true
}

/// 卸载客户端拥有的 NoF segment，同时从 nof_segments 表和 nof_allocator 移除。
/// Unmount a client-owned NoF segment: remove from nof_segments table and nof_allocator.
pub(crate) fn unmount_nof_segment_owned_durable(
    state: &MasterState,
    segment_id: Uuid,
    client_id: Uuid,
    operation: &str,
) -> Result<bool, Status> {
    let _global_mutation_guard = state.key_mutations.lock_snapshot();
    unmount_nof_segment_owned_durable_locked(state, segment_id, client_id, operation)
}

pub(crate) fn unmount_nof_segment_owned_durable_locked(
    state: &MasterState,
    segment_id: Uuid,
    client_id: Uuid,
    operation: &str,
) -> Result<bool, Status> {
    let Some(segment) = state.nof_segments.get(&segment_id) else {
        return Ok(false);
    };
    if segment.segment.client_id != client_id {
        return Ok(false);
    }
    let segment_name = segment.segment.name.clone();
    drop(segment);

    if let Err(error) = state
        .oplog_manager
        .record_unmount_nof_segment_durable(&segment_name, segment_id)
    {
        state.fence_after_durability_failure(operation, &error);
        return Err(Status::unavailable(
            "failed to persist NoF segment unmount; segment remains mounted",
        ));
    }
    if !unmount_nof_segment_owned_locked(state, segment_id, client_id) {
        state.fence_after_invariant_failure(
            operation,
            &format!(
                "durable NoF segment unmount could not be applied locally: \
                 segment_id={segment_id} client_id={client_id}"
            ),
        );
        return Err(Status::unavailable(
            "persisted NoF segment unmount could not be applied locally",
        ));
    }
    bump_view_version(state);
    Ok(true)
}

pub(crate) fn unmount_nof_segment_owned_locked(
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
    invalidate_replicas_on_segments_locked(state, &invalidated, ReplicaType::NoFSsd);
    prune_delayed_replicas_on_segment(
        &state.delayed_replica_releases,
        segment_id,
        ReplicaType::NoFSsd,
    );
    state.nof_segments.remove(&segment_id);
    state.nof_allocator.write().remove_segment(&segment_id);
    state.nof_heartbeat_states.remove(&segment_id);
    let alive_clients = get_alive_clients_snapshot(state);
    clear_invalid_handles_locked(state, &alive_clients);
    true
}

fn prune_delayed_replicas_on_segment(
    delayed_releases: &DashMap<Uuid, DelayedReplicaReleaseEntry>,
    segment_id: Uuid,
    replica_type: ReplicaType,
) {
    delayed_releases.retain(|_, entry| {
        entry.replicas.retain(|replica| {
            replica.replica_type != replica_type || replica.segment_id != segment_id
        });
        !entry.replicas.is_empty()
    });
}

fn transfer_endpoint_host(endpoint: &str) -> String {
    if let Some(rest) = endpoint.strip_prefix('[')
        && let Some(closing) = rest.find(']')
    {
        return rest[..closing].to_string();
    }
    if endpoint.parse::<std::net::Ipv6Addr>().is_ok() {
        return endpoint.to_string();
    }
    if let Some(scope) = endpoint.find('%')
        && let Some(relative_colon) = endpoint[scope..].find(':')
    {
        let colon = scope + relative_colon;
        let host = &endpoint[..colon];
        let address = host.split('%').next().unwrap_or_default();
        if address.parse::<std::net::Ipv6Addr>().is_ok() {
            return host.to_string();
        }
    }
    endpoint
        .rsplit_once(':')
        .map(|(host, _)| host.to_string())
        .unwrap_or_else(|| endpoint.to_string())
}

/// Return C++-compatible QueryIp addresses, distinguishing no segments from
/// mounted segments whose transfer endpoints are all empty.
pub(super) fn query_ip_addresses_for_client(
    state: &MasterState,
    client_id: Uuid,
) -> Option<Vec<String>> {
    let mut found_segment = false;
    let mut addresses = Vec::new();
    for entry in state.segments.iter() {
        if entry.client_id != client_id {
            continue;
        }
        found_segment = true;
        if entry.segment.te_endpoint.is_empty() {
            continue;
        }
        let host = transfer_endpoint_host(&entry.segment.te_endpoint);
        if !addresses.iter().any(|address| address == &host) {
            addresses.push(host);
        }
    }
    found_segment.then_some(addresses)
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
pub(crate) fn release_replicas(
    state: &MasterState,
    replicas: &[ReplicaDescriptor],
) -> Result<(), Status> {
    let memory = replicas
        .iter()
        .filter(|r| r.replica_type == ReplicaType::Memory)
        .cloned()
        .collect::<Vec<_>>();
    let nof = replicas
        .iter()
        .filter(|r| r.replica_type == ReplicaType::NoFSsd)
        .cloned()
        .collect::<Vec<_>>();
    let memory_segment_ids = memory.iter().map(|r| r.segment_id).collect::<Vec<_>>();
    let nof_segment_ids = nof.iter().map(|r| r.segment_id).collect::<Vec<_>>();
    let release_result = {
        // Use one fixed lock order and validate both allocators before either
        // is mutated, so a mixed Memory/NoF batch cannot be half-released.
        let mut memory_allocator = state.allocator.write();
        let mut nof_allocator = state.nof_allocator.write();
        memory_allocator
            .validate_release(&memory)
            .and_then(|_| nof_allocator.validate_release(&nof))
            .and_then(|_| memory_allocator.release(&memory))
            .and_then(|_| nof_allocator.release(&nof))
    };
    if let Err(error) = release_result {
        state.fence_after_invariant_failure(
            "release_replicas",
            &format!("allocator rejected authoritative replica release: {error}"),
        );
        return Err(Status::unavailable(
            "allocator release invariant failed; master is fenced",
        ));
    }
    sync_segment_usage(state, memory_segment_ids);
    sync_nof_segment_usage(state, nof_segment_ids);
    Ok(())
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
        .allocate_for_client_with_exclusions(
            key,
            None,
            size,
            count,
            &config,
            &state
                .nof_segments
                .iter()
                .filter(|entry| entry.status != crate::proto::SegmentStatus::Active)
                .map(|entry| entry.segment.name.clone())
                .collect::<Vec<_>>(),
            None,
        );
    if replicas.len() != count {
        let allocated = replicas.len();
        let segment_ids = replicas.iter().map(|r| r.segment_id).collect::<Vec<_>>();
        if let Err(error) = state.nof_allocator.write().release(&replicas) {
            state.fence_after_invariant_failure(
                "allocate_nof_replicas_rollback",
                &format!("fresh NoF allocation could not be released: {error}"),
            );
            return Err(Status::unavailable(
                "NoF allocator rollback invariant failed; master is fenced",
            ));
        }
        sync_nof_segment_usage(state, segment_ids);
        state.nof_eviction_requested.store(true, Ordering::Release);
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

/// Compute current NoF usage independently from Memory pressure.
pub(crate) fn nof_usage_ratio(state: &MasterState) -> f64 {
    let (total_bytes, used_bytes) = state.nof_allocator.read().usage_totals();
    if total_bytes == 0 {
        return 0.0;
    }
    used_bytes as f64 / total_bytes as f64
}

/// Helper: 释放对象副本并同时清理关联的 offload/promotion 任务。
/// Release object replicas and simultaneously clean up associated offload/promotion tasks.
pub(crate) fn release_object_replicas(
    state: &MasterState,
    key: &str,
    replicas: &[ReplicaDescriptor],
) -> Result<(), Status> {
    if replicas.is_empty() {
        return Ok(());
    }
    // Validate and release allocator ownership before deleting auxiliary task
    // state. If the authoritative descriptor is inconsistent, the helper
    // fences the Master and leaves those task records available for diagnosis
    // and restart reconstruction.
    release_replicas(state, replicas)?;
    clear_offloading_task(state, key);
    cancel_promotion_task(state, key);
    Ok(())
}

fn has_completed_memory_cache_replica(object: &ObjectEntry) -> bool {
    object
        .replicas
        .iter()
        .any(|r| r.replica_type == ReplicaType::Memory && r.status == ReplicaStatus::Complete)
}

fn has_completed_disk_cache_replica(object: &ObjectEntry) -> bool {
    object.replicas.iter().any(|r| {
        matches!(r.replica_type, ReplicaType::Disk | ReplicaType::LocalDisk)
            && r.status == ReplicaStatus::Complete
    })
}

pub(crate) fn sync_cache_total_accounting(object: &mut ObjectEntry) {
    let has_memory = has_completed_memory_cache_replica(object);
    if !object.memory_cache_total_accounted && has_memory {
        metrics::MEM_CACHE_TOTAL.inc();
        object.memory_cache_total_accounted = true;
    } else if object.memory_cache_total_accounted && !has_memory {
        metrics::MEM_CACHE_TOTAL.dec();
        object.memory_cache_total_accounted = false;
    }

    let has_disk = has_completed_disk_cache_replica(object);
    if !object.disk_cache_total_accounted && has_disk {
        metrics::FILE_CACHE_TOTAL.inc();
        object.disk_cache_total_accounted = true;
    } else if object.disk_cache_total_accounted && !has_disk {
        metrics::FILE_CACHE_TOTAL.dec();
        object.disk_cache_total_accounted = false;
    }
}

pub(crate) fn account_cache_total_removal(object: &mut ObjectEntry) {
    if object.memory_cache_total_accounted {
        metrics::MEM_CACHE_TOTAL.dec();
        object.memory_cache_total_accounted = false;
    }
    if object.disk_cache_total_accounted {
        metrics::FILE_CACHE_TOTAL.dec();
        object.disk_cache_total_accounted = false;
    }
}

pub(crate) fn requested_memory_quota_charge(size: u64, replica_count: usize) -> u64 {
    checked_requested_memory_quota_charge(size, replica_count).unwrap_or(u64::MAX)
}

/// Calculate a physical-Memory quota charge for untrusted request or durable
/// input. Callers must reject overflow before allocating replicas or replacing
/// live state; saturating would silently admit an under-specified ledger.
pub(crate) fn checked_requested_memory_quota_charge(
    size: u64,
    replica_count: usize,
) -> Result<u64, TenantQuotaError> {
    let replica_count =
        u64::try_from(replica_count).map_err(|_| TenantQuotaError::InvalidArgument)?;
    size.checked_mul(replica_count)
        .ok_or(TenantQuotaError::InvalidArgument)
}

pub(crate) fn completed_memory_quota_charge(object: &ObjectEntry) -> u64 {
    checked_completed_memory_quota_charge(object).unwrap_or(u64::MAX)
}

pub(crate) fn checked_completed_memory_quota_charge(
    object: &ObjectEntry,
) -> Result<u64, TenantQuotaError> {
    let completed_memory_replicas = object
        .replicas
        .iter()
        .filter(|replica| {
            replica.replica_type == ReplicaType::Memory && replica.status == ReplicaStatus::Complete
        })
        .count();
    checked_requested_memory_quota_charge(object.size, completed_memory_replicas)
}

/// Whether an object image represents a Put/Upsert generation that has not
/// reached a terminal set of write targets yet.
///
/// `quota_committed` cannot answer this by itself: an in-place same-size
/// Upsert keeps the already occupied Memory bytes committed while those same
/// descriptors temporarily move from Complete back to Allocating.
pub(crate) fn object_has_inflight_write(object: &ObjectEntry) -> bool {
    object.put_start_time.is_some()
        && (!object.quota_committed
            || object.replicas.iter().any(|replica| {
                matches!(
                    replica.replica_type,
                    ReplicaType::Memory | ReplicaType::NoFSsd | ReplicaType::Disk
                ) && replica.status != ReplicaStatus::Complete
            }))
}

/// Reconstruct the committed physical-Memory charge represented by a durable
/// object image.
///
/// During an in-place Upsert, Allocating Memory descriptors are not additional
/// reservations: they are the same already charged physical allocations whose
/// contents are being replaced. Copy/Move targets have no PutStart timestamp
/// and remain accounted by their replication-task reservation instead.
pub(crate) fn checked_durable_committed_memory_quota_charge(
    object: &ObjectEntry,
) -> Result<u64, TenantQuotaError> {
    let include_inflight_allocations = object_has_inflight_write(object);
    let committed_memory_replicas = object
        .replicas
        .iter()
        .filter(|replica| {
            replica.replica_type == ReplicaType::Memory
                && (replica.status == ReplicaStatus::Complete
                    || (include_inflight_allocations
                        && replica.status == ReplicaStatus::Allocating))
        })
        .count();
    checked_requested_memory_quota_charge(object.size, committed_memory_replicas)
}

pub(crate) fn allocating_memory_quota_charge(object: &ObjectEntry) -> u64 {
    checked_allocating_memory_quota_charge(object).unwrap_or(u64::MAX)
}

pub(crate) fn checked_allocating_memory_quota_charge(
    object: &ObjectEntry,
) -> Result<u64, TenantQuotaError> {
    let allocating_memory_replicas = object
        .replicas
        .iter()
        .filter(|replica| {
            replica.replica_type == ReplicaType::Memory
                && replica.status == ReplicaStatus::Allocating
        })
        .count();
    checked_requested_memory_quota_charge(object.size, allocating_memory_replicas)
}

pub(crate) fn settle_additional_memory_quota_charge(
    state: &MasterState,
    object: &mut ObjectEntry,
    known_committed_bytes: u64,
    reserved_bytes: u64,
    committed_bytes: u64,
    register_committed_charge: bool,
) -> Result<(), TenantQuotaError> {
    let total_committed_bytes = known_committed_bytes
        .checked_add(committed_bytes)
        .ok_or(TenantQuotaError::AccountingMismatch)?;
    if state.runtime_config.enable_tenant_quota {
        state.tenant_quotas.write().settle(
            &object.tenant_id,
            reserved_bytes,
            committed_bytes,
            register_committed_charge,
        )?;
    }
    object.committed_quota_charge_bytes = total_committed_bytes;
    Ok(())
}

pub(crate) fn release_committed_memory_quota_charge(
    state: &MasterState,
    object: &mut ObjectEntry,
    bytes: u64,
) -> Result<u64, TenantQuotaError> {
    let known_charge = if object.committed_quota_charge_bytes == 0 && object.quota_committed {
        completed_memory_quota_charge(object)
    } else {
        object.committed_quota_charge_bytes
    };
    if bytes > known_charge {
        return Err(TenantQuotaError::AccountingMismatch);
    }
    let released = bytes;
    if state.runtime_config.enable_tenant_quota {
        let mut quotas = state.tenant_quotas.write();
        if released != 0 && released == known_charge {
            quotas.release(&object.tenant_id, released)?;
        } else {
            quotas.release_bytes(&object.tenant_id, released)?;
        }
    }
    object.committed_quota_charge_bytes = known_charge - released;
    Ok(released)
}

pub(crate) fn settle_and_release_memory_quota_charge(
    state: &MasterState,
    object: &mut ObjectEntry,
    known_committed_bytes: u64,
    reserved_bytes: u64,
    newly_committed_bytes: u64,
    release_bytes: u64,
) -> Result<u64, TenantQuotaError> {
    let total_committed_bytes = known_committed_bytes
        .checked_add(newly_committed_bytes)
        .ok_or(TenantQuotaError::AccountingMismatch)?;
    if release_bytes > total_committed_bytes {
        return Err(TenantQuotaError::AccountingMismatch);
    }
    let released = release_bytes;
    if state.runtime_config.enable_tenant_quota {
        // Move changes the reservation and committed ledgers together. Apply
        // both operations to a projected table so a failure in the release
        // half cannot leave the settle half committed.
        let mut quotas = state.tenant_quotas.write();
        let mut projected = quotas.clone();
        projected.settle(
            &object.tenant_id,
            reserved_bytes,
            newly_committed_bytes,
            known_committed_bytes == 0 && newly_committed_bytes != 0,
        )?;
        if released != 0 && released == total_committed_bytes {
            projected.release(&object.tenant_id, released)?;
        } else {
            projected.release_bytes(&object.tenant_id, released)?;
        }
        *quotas = projected;
    }
    object.committed_quota_charge_bytes = total_committed_bytes - released;
    Ok(released)
}

pub(crate) fn account_removed_object_quota(
    state: &MasterState,
    object: &ObjectEntry,
) -> Result<(), TenantQuotaError> {
    let mut object = object.clone();
    account_cache_total_removal(&mut object);

    if !state.runtime_config.enable_tenant_quota {
        return Ok(());
    }
    let mut quotas = state.tenant_quotas.write();
    let mut projected = quotas.clone();
    let result = remove_object_from_quota_projection(&mut projected, &object);
    if let Err(error) = result {
        tracing::warn!(
            tenant_id = %object.tenant_id,
            size = object.size,
            quota_committed = object.quota_committed,
            reserved_charge = object.reserved_quota_charge_bytes,
            committed_charge = object.committed_quota_charge_bytes,
            ?error,
            "tenant quota accounting failed while removing object"
        );
        state.fence_after_invariant_failure(
            "remove_object_quota",
            &format!(
                "tenant={} key={} error={error:?}",
                object.tenant_id, object.user_key
            ),
        );
        Err(error)
    } else {
        *quotas = projected;
        Ok(())
    }
}

/// Clone an authoritative object for an in-memory transactional projection.
///
/// `ReplicaDescriptor::clone` deliberately clears `refcnt` because descriptors
/// copied into responses, tasks, and durable images must not inherit runtime
/// pins. A transactional object projection is different: it will be written
/// back into the live object table, so every unrelated in-flight pin must
/// survive the clone/validate/commit sequence.
pub(crate) fn clone_object_for_mutation(object: &ObjectEntry) -> ObjectEntry {
    let mut projected = object.clone();
    debug_assert_eq!(projected.replicas.len(), object.replicas.len());
    for (projected_replica, live_replica) in projected.replicas.iter_mut().zip(&object.replicas) {
        projected_replica.refcnt = live_replica.refcnt;
    }
    projected
}

pub(crate) fn remove_object_from_quota_projection(
    quotas: &mut TenantQuotaTable,
    object: &ObjectEntry,
) -> Result<(), TenantQuotaError> {
    if object.quota_committed {
        let charge = if object.committed_quota_charge_bytes == 0 {
            completed_memory_quota_charge(object)
        } else {
            object.committed_quota_charge_bytes
        };
        quotas
            .remove_object(&object.tenant_id, charge)
            .and_then(|()| {
                if object.pending_replaced_quota_charge_bytes == 0 {
                    Ok(())
                } else {
                    quotas.release(
                        &object.tenant_id,
                        object.pending_replaced_quota_charge_bytes,
                    )
                }
            })
    } else {
        let charge = if object.reserved_quota_charge_bytes == 0 {
            allocating_memory_quota_charge(&object)
        } else {
            object.reserved_quota_charge_bytes
        };
        quotas
            .abort(&object.tenant_id, charge)
            .and_then(|()| {
                if object.pending_replaced_quota_charge_bytes == 0 {
                    Ok(())
                } else {
                    quotas.release(
                        &object.tenant_id,
                        object.pending_replaced_quota_charge_bytes,
                    )
                }
            })
            .and_then(|()| quotas.unregister_object(&object.tenant_id))
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

    entry.replicas.retain_mut(|r| {
        if r.status != mooncake_store_core::ReplicaStatus::Complete {
            return true; // 保留非 Complete 状态的副本 / Retain non-Complete replicas
        }
        let is_stale = match r.replica_type {
            ReplicaType::Memory | ReplicaType::NoFSsd => !r.handle_valid,
            // C++ CleanupStaleHandles removes every completed LocalDisk
            // replica whose owner client is no longer alive (see
            // Replica::has_stale_local_disk_client); keep the identical
            // contract regardless of durable storage identity.
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

/// Mark Memory/NoF replicas invalid while the caller holds the global snapshot
/// mutation guard. ClearInvalidHandles then removes the invalid metadata.
fn invalidate_replicas_on_segments_locked(
    state: &MasterState,
    segment_ids: &HashSet<Uuid>,
    replica_type: ReplicaType,
) {
    if segment_ids.is_empty() {
        return;
    }
    let keys = state
        .objects
        .iter()
        .map(|object| object.key().clone())
        .collect::<Vec<_>>();
    invalidate_replicas_on_segments_for_keys(state, segment_ids, replica_type, &keys);
}

fn invalidate_replicas_on_segments_for_keys(
    state: &MasterState,
    segment_ids: &HashSet<Uuid>,
    replica_type: ReplicaType,
    keys: &[String],
) {
    for key in keys {
        if let Some(mut object) = state.objects.get_mut(key) {
            for replica in &mut object.replicas {
                if replica.replica_type == replica_type && segment_ids.contains(&replica.segment_id)
                {
                    replica.handle_valid = false;
                }
            }
        }
    }
}

/// Sweep metadata while the caller holds the global snapshot mutation guard.
pub(crate) fn clear_invalid_handles_locked(state: &MasterState, alive_clients: &HashSet<Uuid>) {
    let keys = state
        .objects
        .iter()
        .map(|object| object.key().clone())
        .collect::<Vec<_>>();
    clear_invalid_handles_for_keys(state, alive_clients, &keys);
}

/// Clean one key while the caller holds its mutation guard.
///
/// PutStart/UpsertStart use this after acquiring the tenant-scoped mutation
/// stripe. Partial stale-replica cleanup therefore updates quota and
/// durability in the same per-key mutation epoch as start-state revalidation.
pub(crate) fn clear_invalid_handles_for_key_locked(
    state: &MasterState,
    alive_clients: &HashSet<Uuid>,
    key: &String,
) -> Result<(), HaError> {
    clear_invalid_handles_for_keys(state, alive_clients, std::slice::from_ref(key));
    if state.service_fenced.load(Ordering::Acquire) {
        return Err(HaError::Snapshot(format!(
            "master fenced while clearing invalid handles for key {key:?}"
        )));
    }
    Ok(())
}

fn clear_invalid_handles_for_keys(
    state: &MasterState,
    alive_clients: &HashSet<Uuid>,
    keys: &[String],
) {
    for key in keys {
        let mut should_remove = false;
        let mut object_changed = false;
        if let Some(mut object) = state.objects.get_mut(key) {
            let mut projected = clone_object_for_mutation(&object);
            let before_replica_state = projected
                .replicas
                .iter()
                .map(|replica| {
                    (
                        replica.segment_id,
                        replica.offset,
                        replica.replica_type,
                        replica.holder_client_id,
                        replica.handle_valid,
                    )
                })
                .collect::<Vec<_>>();
            let before_charge = completed_memory_quota_charge(&projected);
            should_remove = cleanup_stale_handles(&mut projected, alive_clients);
            object_changed = before_replica_state
                != projected
                    .replicas
                    .iter()
                    .map(|replica| {
                        (
                            replica.segment_id,
                            replica.offset,
                            replica.replica_type,
                            replica.holder_client_id,
                            replica.handle_valid,
                        )
                    })
                    .collect::<Vec<_>>();
            let after_charge = completed_memory_quota_charge(&projected);
            if before_charge > after_charge {
                if let Err(error) = release_committed_memory_quota_charge(
                    state,
                    &mut projected,
                    before_charge - after_charge,
                ) {
                    drop(object);
                    state.fence_after_invariant_failure(
                        "clear_invalid_handles",
                        &format!("key={key:?} error={error:?}"),
                    );
                    return;
                }
            }
            sync_cache_total_accounting(&mut projected);
            *object = projected;
        }
        if should_remove {
            if let Some((_, object)) = state.objects.remove(key) {
                if account_removed_object_quota(state, &object).is_err() {
                    return;
                }
            }
            state.processing_keys.remove(key);
            state.replication_tasks.remove(key);
            clear_offloading_task(state, key);
            cancel_promotion_task(state, key);
            for mut entry in state.client_objects.iter_mut() {
                entry.value_mut().remove(key);
            }
        }
        if state.service_fenced.load(Ordering::Acquire) {
            return;
        }
        if object_changed {
            if state
                .persist_object_image_or_remove_or_fence(key, "clear_invalid_handles")
                .is_err()
            {
                return;
            }
        }
    }
    cancel_promotions_for_stale_holders(state, alive_clients);
}

/// Cancel promotion tasks whose LocalDisk holder is no longer alive, mirroring
/// the C++ ClearInvalidHandles end-state: a dead holder cannot drive the
/// promotion to completion, so the task must be erased and the cluster-wide
/// in-flight slot released immediately (C++ reaches the same effect through
/// CleanupStaleHandles removing the stale LocalDisk replica and EraseMetadata
/// calling ErasePromotionTaskIfPresent).
fn cancel_promotions_for_stale_holders(state: &MasterState, alive_clients: &HashSet<Uuid>) {
    let stale_keys = state
        .promotion_tasks
        .iter()
        .filter(|entry| !alive_clients.contains(&entry.holder_id))
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in stale_keys {
        cancel_promotion_task(state, &key);
    }
}

// =============================================================================
// Tenant helpers — 租户工具函数
// C++ equivalent: NormalizeTenantId in types.h, MakeTenantScopedKey / MakeObjectIdentity in master_service.h
// =============================================================================

/// Sentinel delimiter between tenant_id and user_key.
/// The first NUL byte separates tenant identity from the opaque local key;
/// later NUL bytes remain part of that local key for C++ compatibility.
/// 第一个 NUL 字节分隔租户与原始 local key；后续 NUL 属于 local key。
pub const TENANT_SCOPE_DELIMITER: char = '\0';

pub fn resolve_request_tenant(raw: &str, strict: bool) -> Result<TenantId, Status> {
    if !strict {
        return Ok(TenantId::default());
    }
    TenantId::new(raw.to_owned()).map_err(|error| Status::invalid_argument(error.to_string()))
}

pub fn resolve_write_tenant(raw: &str, strict: bool) -> Result<TenantId, Status> {
    if !strict {
        return Ok(TenantId::default());
    }
    if raw.is_empty() {
        return Err(Status::resource_exhausted("tenant not registered"));
    }
    TenantId::new(raw.to_owned()).map_err(|_| Status::resource_exhausted("tenant not registered"))
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

/// Generate a task identity that is not present in the authoritative task map.
///
/// The caller must hold the global mutation barrier across this check and the
/// following insertion. This mirrors C++ TaskManager::submit_task, which
/// retries UUID generation while holding its write access.
pub(crate) fn unique_task_id(state: &MasterState) -> Uuid {
    loop {
        let task_id = Uuid::new_v4();
        if !state.tasks.contains_key(&task_id) {
            return task_id;
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::MasterServiceImpl;
    use crate::service::state::LocalDiskSegmentEntry;

    #[test]
    fn mutation_projection_preserves_runtime_replica_refcounts() {
        let replica = ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: "memory:1".into(),
            offset: 64,
            size: 128,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(Uuid::new_v4()),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 3,
            handle_valid: true,
            base_addr: 0x1000,
            protocol: "tcp".into(),
        };
        let object = ObjectEntry {
            replicas: vec![replica],
            size: 128,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: TenantId::default(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 128,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };

        assert_eq!(object.clone().replicas[0].refcnt, 0);
        assert_eq!(
            clone_object_for_mutation(&object).replicas[0].refcnt,
            object.replicas[0].refcnt
        );
    }

    #[test]
    fn checked_memory_quota_charge_rejects_multiplication_overflow() {
        assert_eq!(
            checked_requested_memory_quota_charge(u64::MAX / 2 + 1, 2),
            Err(TenantQuotaError::InvalidArgument)
        );
        assert_eq!(
            checked_requested_memory_quota_charge(u64::MAX / 2, 2),
            Ok(u64::MAX - 1)
        );
    }

    #[test]
    fn durable_committed_charge_distinguishes_in_place_upsert_from_copy_target() {
        let replica = ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: "memory:1".into(),
            offset: 0,
            size: 128,
            status: ReplicaStatus::Allocating,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(Uuid::new_v4()),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0x1000,
            protocol: "tcp".into(),
        };
        let mut object = ObjectEntry {
            replicas: vec![replica],
            size: 128,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: Some(SystemTime::now()),
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: TenantId::default(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 128,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };

        assert!(object_has_inflight_write(&object));
        assert_eq!(checked_completed_memory_quota_charge(&object), Ok(0));
        assert_eq!(
            checked_durable_committed_memory_quota_charge(&object),
            Ok(128)
        );

        object.put_start_time = None;
        assert!(!object_has_inflight_write(&object));
        assert_eq!(
            checked_durable_committed_memory_quota_charge(&object),
            Ok(0)
        );
    }

    fn quota_enabled_service() -> MasterServiceImpl {
        let policy_uri = tempfile::NamedTempFile::new()
            .unwrap()
            .path()
            .to_string_lossy()
            .into_owned();
        MasterServiceImpl::with_runtime_config(crate::service::state::MasterRuntimeConfig {
            enable_tenant_quota: true,
            tenant_quota_connector_uri: policy_uri,
            ..Default::default()
        })
    }

    #[test]
    fn object_owner_prefers_explicit_writer_without_replicas() {
        let state = MasterState::empty();
        let writer = Uuid::new_v4();
        let object = ObjectEntry {
            replicas: Vec::new(),
            size: 1,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: writer,
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: TenantId::default(),
            group_id: String::new(),
            quota_committed: false,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".to_string(),
        };

        assert_eq!(object_owner_client_id(&state, &object), Some(writer));
    }

    #[test]
    fn move_quota_transaction_rolls_back_when_release_half_fails() {
        let service = quota_enabled_service();
        let tenant_id = TenantId::new("tenant-a".into()).unwrap();
        {
            let mut quotas = service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 500, 500).unwrap();
            quotas.recompute_effective_quotas(500);
            quotas.restore_object_checked(&tenant_id, 60).unwrap();
            quotas.reserve(&tenant_id, 50).unwrap();
        }
        let mut object = ObjectEntry {
            replicas: Vec::new(),
            size: 100,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 100,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };

        let error =
            settle_and_release_memory_quota_charge(&service.state, &mut object, 100, 50, 50, 150)
                .unwrap_err();

        assert_eq!(error, TenantQuotaError::AccountingMismatch);
        assert_eq!(object.committed_quota_charge_bytes, 100);
        let quota = service
            .state
            .tenant_quotas
            .read()
            .get_snapshot(&tenant_id)
            .unwrap();
        assert_eq!(quota.used_bytes, 60);
        assert_eq!(quota.reserved_bytes, 50);
        assert_eq!(quota.committed_count, 1);
    }

    #[test]
    fn quota_release_rejects_amounts_above_the_authoritative_charge() {
        let service = quota_enabled_service();
        let tenant_id = TenantId::new("tenant-a".into()).unwrap();
        {
            let mut quotas = service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 500, 500).unwrap();
            quotas.recompute_effective_quotas(500);
            quotas.restore_object_checked(&tenant_id, 100).unwrap();
            quotas.reserve(&tenant_id, 50).unwrap();
        }
        let mut object = ObjectEntry {
            replicas: Vec::new(),
            size: 100,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 100,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };

        assert_eq!(
            release_committed_memory_quota_charge(&service.state, &mut object, 101),
            Err(TenantQuotaError::AccountingMismatch)
        );
        assert_eq!(
            settle_and_release_memory_quota_charge(&service.state, &mut object, 100, 50, 50, 151,),
            Err(TenantQuotaError::AccountingMismatch)
        );

        assert_eq!(object.committed_quota_charge_bytes, 100);
        let quota = service
            .state
            .tenant_quotas
            .read()
            .get_snapshot(&tenant_id)
            .unwrap();
        assert_eq!(quota.used_bytes, 100);
        assert_eq!(quota.reserved_bytes, 50);
        assert_eq!(quota.committed_count, 1);
        assert_eq!(quota.metadata_object_count, 1);
    }

    #[test]
    fn replacement_quota_settle_and_revoke_release_the_old_physical_charge() {
        let tenant_id = TenantId::new("tenant-a".into()).unwrap();

        let settled_service = quota_enabled_service();
        {
            let mut quotas = settled_service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 500, 500).unwrap();
            quotas.recompute_effective_quotas(500);
            quotas.restore_object_checked(&tenant_id, 100).unwrap();
            quotas.reserve(&tenant_id, 150).unwrap();
        }
        settled_service
            .settle_tenant_quota(&tenant_id, 150, 150, true, 100)
            .unwrap();
        let settled = settled_service
            .state
            .tenant_quotas
            .read()
            .get_snapshot(&tenant_id)
            .unwrap();
        assert_eq!(settled.used_bytes, 150);
        assert_eq!(settled.reserved_bytes, 0);
        assert_eq!(settled.committed_count, 1);
        assert_eq!(settled.metadata_object_count, 1);

        let revoked_service = quota_enabled_service();
        {
            let mut quotas = revoked_service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 500, 500).unwrap();
            quotas.recompute_effective_quotas(500);
            quotas.restore_object_checked(&tenant_id, 100).unwrap();
            quotas.reserve(&tenant_id, 150).unwrap();
        }
        let replacement = ObjectEntry {
            replicas: Vec::new(),
            size: 150,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: false,
            reserved_quota_charge_bytes: 150,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 100,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };
        account_removed_object_quota(&revoked_service.state, &replacement).unwrap();
        let revoked = revoked_service
            .state
            .tenant_quotas
            .read()
            .get_snapshot(&tenant_id)
            .unwrap();
        assert_eq!(revoked.used_bytes, 0);
        assert_eq!(revoked.reserved_bytes, 0);
        assert_eq!(revoked.committed_count, 0);
        assert_eq!(revoked.metadata_object_count, 0);
    }

    #[test]
    fn move_quota_registers_first_memory_charge_for_nof_only_object() {
        let service = quota_enabled_service();
        let tenant_id = TenantId::new("tenant-a".into()).unwrap();
        {
            let mut quotas = service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 100, 100).unwrap();
            quotas.recompute_effective_quotas(100);
            quotas.register_object(&tenant_id);
            quotas.reserve(&tenant_id, 100).unwrap();
        }
        let mut object = ObjectEntry {
            replicas: Vec::new(),
            size: 100,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id: tenant_id.clone(),
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 0,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };

        settle_and_release_memory_quota_charge(&service.state, &mut object, 0, 100, 100, 0)
            .unwrap();

        assert_eq!(object.committed_quota_charge_bytes, 100);
        let quota = service
            .state
            .tenant_quotas
            .read()
            .get_snapshot(&tenant_id)
            .unwrap();
        assert_eq!(quota.used_bytes, 100);
        assert_eq!(quota.reserved_bytes, 0);
        assert_eq!(quota.committed_count, 1);
    }

    #[test]
    fn authoritative_remove_quota_mismatch_fences_service() {
        let service = quota_enabled_service();
        let tenant_id = TenantId::new("tenant-a".into()).unwrap();
        {
            let mut quotas = service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 100, 100).unwrap();
            quotas.recompute_effective_quotas(100);
        }
        let object = ObjectEntry {
            replicas: Vec::new(),
            size: 100,
            last_access: SystemTime::now(),
            hard_pinned: false,
            data_type: Default::default(),
            client_id: Uuid::new_v4(),
            put_start_time: None,
            lease_timeout: None,
            soft_pin_timeout: None,
            tenant_id,
            group_id: String::new(),
            quota_committed: true,
            reserved_quota_charge_bytes: 0,
            committed_quota_charge_bytes: 100,
            pending_replaced_quota_charge_bytes: 0,
            memory_cache_total_accounted: false,
            disk_cache_total_accounted: false,
            user_key: "key".into(),
        };

        let result = account_removed_object_quota(&service.state, &object);

        assert_eq!(result, Err(TenantQuotaError::AccountingMismatch));
        assert!(service.state.service_fenced.load(Ordering::Acquire));
        assert!(!service.state.service_available.load(Ordering::Acquire));
    }

    #[test]
    fn partial_stale_memory_cleanup_reconciles_physical_quota_before_revalidation() {
        let service = quota_enabled_service();
        let tenant_id = TenantId::new("tenant-a".into()).unwrap();
        let key = tenant_id.make_scoped_key("partial-stale");
        {
            let mut quotas = service.state.tenant_quotas.write();
            quotas.upsert_policy(&tenant_id, 400, 400).unwrap();
            quotas.recompute_effective_quotas(400);
            quotas.restore_object_checked(&tenant_id, 200).unwrap();
        }
        let replica = |handle_valid| ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: "memory".into(),
            offset: 0,
            size: 100,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: None,
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid,
            base_addr: 0,
            protocol: String::new(),
        };
        service.state.objects.insert(
            key.clone(),
            ObjectEntry {
                replicas: vec![replica(false), replica(true)],
                size: 100,
                last_access: SystemTime::now(),
                hard_pinned: false,
                data_type: Default::default(),
                client_id: Uuid::new_v4(),
                put_start_time: None,
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: tenant_id.clone(),
                group_id: String::new(),
                quota_committed: true,
                reserved_quota_charge_bytes: 0,
                committed_quota_charge_bytes: 200,
                pending_replaced_quota_charge_bytes: 0,
                memory_cache_total_accounted: false,
                disk_cache_total_accounted: false,
                user_key: "partial-stale".into(),
            },
        );

        let _mutation = service.state.key_mutations.lock(&key);
        clear_invalid_handles_for_key_locked(&service.state, &HashSet::new(), &key).unwrap();

        let object = service.state.objects.get(&key).unwrap();
        assert_eq!(object.replicas.len(), 1);
        assert!(object.replicas[0].handle_valid);
        assert_eq!(object.committed_quota_charge_bytes, 100);
        drop(object);
        let quota = service
            .state
            .tenant_quotas
            .read()
            .get_snapshot(&tenant_id)
            .unwrap();
        assert_eq!(quota.used_bytes, 100);
        assert_eq!(quota.committed_count, 1);
        assert_eq!(quota.metadata_object_count, 1);
    }

    #[test]
    fn host_from_segment_name_matches_cpp_host_id_rules() {
        assert_eq!(host_from_segment_name(" node-a:1234 "), "node-a");
        assert_eq!(host_from_segment_name("node-a"), "node-a");
        assert_eq!(host_from_segment_name("2001:db8::1"), "2001:db8::1");
        assert_eq!(host_from_segment_name("[2001:db8::1]:1234"), "2001:db8::1");

        for local in [
            "localhost:1234",
            "127.0.0.1:1234",
            "0.0.0.0:1234",
            "::1",
            "[::1]:1234",
            "::",
            "[::]:1234",
        ] {
            assert_eq!(host_from_segment_name(local), "");
        }
    }

    #[test]
    fn port_from_segment_name_handles_ipv4_hostname_and_ipv6() {
        assert_eq!(port_from_segment_name("node-a:1234"), 1234);
        assert_eq!(port_from_segment_name("10.0.0.1:2345"), 2345);
        assert_eq!(port_from_segment_name("[2001:db8::1]:3456"), 3456);
        assert_eq!(port_from_segment_name("2001:db8::1"), 0);
        assert_eq!(port_from_segment_name("[2001:db8::1]"), 0);
        assert_eq!(port_from_segment_name("node-a"), 0);
        assert_eq!(port_from_segment_name("node-a:invalid"), 0);
    }

    #[test]
    fn routable_replica_rejects_invalid_memory_handle() {
        let state = MasterState::empty();
        let mut replica = ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: "memory".to_string(),
            offset: 0,
            size: 1,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: None,
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };

        assert!(replica_is_routable(&state, &replica));
        replica.handle_valid = false;
        assert!(!replica_is_routable(&state, &replica));
    }

    #[test]
    fn routable_local_disk_requires_ready_exact_session() {
        let state = MasterState::empty();
        let storage_id = Uuid::new_v4();
        let holder_client_id = Uuid::new_v4();
        let mut replica = ReplicaDescriptor {
            segment_id: Uuid::nil(),
            segment_name: "127.0.0.1:4321".to_string(),
            offset: 0,
            size: 1,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::LocalDisk,
            holder_client_id: Some(holder_client_id),
            local_disk_storage_id: Some(storage_id),
            local_disk_generation_id: Some(Uuid::new_v4()),
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };

        state
            .local_disk_client_sessions
            .insert(holder_client_id, storage_id);
        state.local_disk_segments.insert(
            storage_id,
            LocalDiskSegmentEntry {
                active_client_id: Some(holder_client_id),
                persisted_client_id: Some(holder_client_id),
                recovery_complete: false,
                recovery_session_id: Some(Uuid::new_v4()),
                recovered_objects: HashSet::new(),
                enable_offloading: false,
                offloading_objects: HashMap::new(),
                promotion_objects: HashMap::new(),
                ssd_total_capacity_bytes: 0,
            },
        );

        assert!(!replica_is_routable(&state, &replica));
        state
            .local_disk_segments
            .get_mut(&storage_id)
            .unwrap()
            .recovery_complete = true;
        assert!(replica_is_routable(&state, &replica));

        replica.local_disk_generation_id = None;
        assert!(
            !replica_is_routable(&state, &replica),
            "a Ready LocalDisk session must not make legacy generation-less bytes routable"
        );
        replica.local_disk_generation_id = Some(Uuid::new_v4());

        state
            .local_disk_client_sessions
            .insert(holder_client_id, Uuid::new_v4());
        assert!(!replica_is_routable(&state, &replica));
    }

    #[test]
    fn authoritative_release_mismatch_fences_without_freeing_live_range() {
        let state = MasterState::empty();
        let segment_id = Uuid::new_v4();
        state.allocator.write().add_segment(
            mooncake_store_core::Segment {
                id: segment_id,
                name: "release-invariant:1".to_string(),
                base: 0,
                size: 200,
                te_endpoint: String::new(),
                protocol: "tcp".to_string(),
                host_id: String::new(),
            },
            0,
            Uuid::new_v4(),
        );
        let mut replicas = state.allocator.write().allocate(
            "tenant-a\0release-invariant",
            100,
            1,
            &ReplicateConfig::default(),
        );
        replicas[0].size = 99;

        let error = release_replicas(&state, &replicas).unwrap_err();

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(state.service_fenced.load(Ordering::Acquire));
        assert_eq!(state.allocator.read().used_bytes(&segment_id), Some(100));
    }
}

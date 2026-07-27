// ============================================================================
// Batch RPC operations — raw gRPC batch calls to the master service.
// 批量 RPC 操作 —— 向 master 服务发送的原始 gRPC 批量调用。
//
// These methods are the Rust equivalent of C++ MasterClient::BatchXxx() methods
// in master_client.h / master_client.cpp. They send true batch RPCs where
// multiple keys are processed in a single network round-trip.
//
// 这些方法是 C++ MasterClient::BatchXxx() 的 Rust 等价实现。
// 它们发送真正的批量 RPC，多个 key 在单次网络往返中处理。
//
// C++ equivalents:
//   master_client.h / master_client.cpp — MasterClient class
// ============================================================================

use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{ReplicaDescriptor, ReplicateConfig, StoreError};
use std::collections::HashMap;
use uuid::Uuid;

use super::MooncakeClient;
use super::batch_types::{BatchPutStartResult, BatchUpsertEntry};
use crate::proto;

fn batch_upsert_entry_config(
    config: &ReplicateConfig,
    key_count: usize,
    index: usize,
) -> StoreResult<ReplicateConfig> {
    if !config.group_ids.is_empty() && config.group_ids.len() != key_count {
        return Err(StoreError::InvalidParams(
            "group_ids length must match keys length".to_string(),
        ));
    }
    let mut entry_config = config.clone();
    if !entry_config.group_ids.is_empty() {
        entry_config.group_ids = vec![entry_config.group_ids[index].clone()];
    }
    Ok(entry_config)
}

fn ensure_batch_response_len(operation: &str, expected: usize, actual: usize) -> StoreResult<()> {
    if expected != actual {
        return Err(StoreError::Internal(format!(
            "{operation} response size mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

impl MooncakeClient {
    // -----------------------------------------------------------------------
    // BatchQueryIp — batch query client IP addresses
    // 批量查询客户端 IP 地址
    //
    // C++ equivalent: MasterClient::BatchQueryIp(client_ids)
    // -----------------------------------------------------------------------

    /// Query IP addresses for multiple client IDs in a single RPC.
    /// 在单次 RPC 中查询多个 client ID 的 IP 地址。
    ///
    /// Returns a map from client_id string to a list of IP addresses.
    /// 返回 client_id 字符串到 IP 地址列表的映射。
    pub async fn batch_query_ip(
        &mut self,
        client_ids: &[Uuid],
    ) -> StoreResult<HashMap<String, Vec<String>>> {
        let request = proto::BatchQueryIpRequest {
            client_ids: client_ids
                .iter()
                .map(|id| {
                    let (h, l) = id.as_u64_pair();
                    proto::Uuid { high: h, low: l }
                })
                .collect(),
        };
        let response = self
            .master
            .batch_query_ip(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response
            .ips
            .into_iter()
            .map(|(k, v)| (k, v.addresses))
            .collect())
    }

    // -----------------------------------------------------------------------
    // BatchReplicaClear — batch clear replicas on segments
    // 批量清除 segment 上的副本
    //
    // C++ equivalent: MasterClient::BatchReplicaClear(keys, client_id, segment_name)
    // -----------------------------------------------------------------------

    /// Clear replicas for multiple object keys on a specific segment (or all
    /// segments if segment_name is empty) for a given client. The master
    /// validates ownership, lease expiry, and replica completeness before
    /// performing the clear.
    ///
    /// 批量清除指定客户端对象在指定 segment（或所有 segment）上的副本。
    /// Master 在执行清除前会校验所有权、lease 过期和副本完整性。
    ///
    /// Returns the list of keys that were actually cleared.
    /// 返回实际被清除的 key 列表。
    pub async fn batch_replica_clear(
        &mut self,
        keys: &[String],
        client_id: Uuid,
        segment_name: &str,
        tenant_id: &str,
    ) -> StoreResult<Vec<String>> {
        let request = proto::BatchReplicaClearRequest {
            object_keys: keys.to_vec(),
            client_id: Some({
                let (h, l) = client_id.as_u64_pair();
                proto::Uuid { high: h, low: l }
            }),
            segment_name: segment_name.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .batch_replica_clear(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        Ok(response.cleared_keys)
    }

    // -----------------------------------------------------------------------
    // BatchPutStart — batch allocate replicas for multiple keys
    // 批量 PutStart — 为多个 key 分配副本
    //
    // C++ equivalent: MasterClient::BatchPutStart(keys, slice_lengths, config)
    // -----------------------------------------------------------------------

    /// Start a batch of put operations: allocate replicas for N objects in a
    /// single RPC. All keys share the same ReplicateConfig.
    ///
    /// 批量开始 put 操作：在单次 RPC 中为 N 个对象分配副本。
    /// 所有 key 共享同一份 ReplicateConfig。
    ///
    /// Returns the full list of allocated replica descriptors.
    /// 返回所有分配的副本描述符合集。
    pub async fn batch_put_start(
        &mut self,
        keys: &[String],
        slice_lengths: &[u64],
        config: &ReplicateConfig,
        tenant_id: &str,
    ) -> StoreResult<Vec<ReplicaDescriptor>> {
        let results = self
            .batch_put_start_results(keys, slice_lengths, config, tenant_id)
            .await?;
        if let Some(failed) = results.iter().find(|result| result.status != 0) {
            return Err(StoreError::OperationFailed(failed.status));
        }
        Ok(results
            .into_iter()
            .flat_map(|result| result.replicas)
            .collect())
    }

    pub async fn batch_put_start_results(
        &mut self,
        keys: &[String],
        slice_lengths: &[u64],
        config: &ReplicateConfig,
        tenant_id: &str,
    ) -> StoreResult<Vec<BatchPutStartResult>> {
        let request = proto::BatchPutStartRequest {
            client_id: Some(self.client_id_proto()),
            keys: keys.to_vec(),
            slice_lengths: slice_lengths.to_vec(),
            config: Some(self.replicate_config_to_proto(config)),
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .batch_put_start(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        if response.results.len() != keys.len() {
            return Err(StoreError::Internal(format!(
                "BatchPutStart response size mismatch: expected {}, got {}",
                keys.len(),
                response.results.len()
            )));
        }
        response
            .results
            .iter()
            .zip(keys)
            .map(|(result, requested_key)| {
                if result.key != *requested_key {
                    return Err(StoreError::Internal(format!(
                        "BatchPutStart response key mismatch: expected {requested_key:?}, got {:?}",
                        result.key
                    )));
                }
                Ok(BatchPutStartResult {
                    key: result.key.clone(),
                    replicas: self.replicas_from_proto(&result.replicas),
                    status: result.status,
                    tenant_id: result.tenant_id.clone(),
                })
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // BatchPutEnd — batch commit put operations
    // 批量 PutEnd — 批量提交 put 操作
    //
    // C++ equivalent: MasterClient::BatchPutEnd(keys, replica_type)
    // -----------------------------------------------------------------------

    /// End a batch of put operations: mark Allocating replicas as Complete for
    /// each key, and trigger offloading. Each entry returns a status code:
    /// `0` = success, `-1` = key not found.
    ///
    /// 批量结束 put 操作：将每个 key 的 Allocating 副本标记为 Complete，
    /// 并触发 offloading。每个条目返回状态码：0=成功, -1=key不存在。
    pub async fn batch_put_end(
        &mut self,
        keys: &[String],
        replica_type: i32,
        tenant_id: &str,
    ) -> StoreResult<Vec<i32>> {
        let request = proto::BatchPutEndRequest {
            entries: keys
                .iter()
                .map(|key| proto::PutEndEntry {
                    client_id: Some(self.client_id_proto()),
                    key: key.clone(),
                    replica_type,
                    tenant_id: tenant_id.to_string(),
                })
                .collect(),
        };
        let response = self
            .master
            .batch_put_end(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        ensure_batch_response_len("BatchPutEnd", keys.len(), response.statuses.len())?;
        Ok(response.statuses)
    }

    // -----------------------------------------------------------------------
    // BatchPutRevoke — batch revoke put allocations
    // 批量 PutRevoke — 批量撤销 put 分配
    //
    // C++ equivalent: MasterClient::BatchPutRevoke(keys, replica_type)
    // -----------------------------------------------------------------------

    /// Revoke a batch of put operations: remove non-complete replicas for the given keys
    /// matching the requested replica type.
    /// Returns per-key status codes: `0` = success, `-1` = key not found,
    /// `-2` = has replication task, `-3` = permission denied.
    ///
    /// 批量撤销 put 操作：移除指定 keys 中匹配副本类型的未完成副本。
    /// 返回每个 key 的状态码：0=成功, -1=key不存在, -2=有复制任务, -3=权限拒绝。
    pub async fn batch_put_revoke(
        &mut self,
        keys: &[String],
        replica_type: i32,
        tenant_id: &str,
    ) -> StoreResult<Vec<i32>> {
        let request = proto::BatchPutRevokeRequest {
            keys: keys.to_vec(),
            client_id: Some(self.client_id_proto()),
            replica_type,
            tenant_id: tenant_id.to_string(),
        };
        let response = self
            .master
            .batch_put_revoke(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        ensure_batch_response_len("BatchPutRevoke", keys.len(), response.statuses.len())?;
        Ok(response.statuses)
    }

    // -----------------------------------------------------------------------
    // BatchUpsertEnd — true multi-key upsert commit
    // 批量 UpsertEnd —— 真正的多 key upsert 提交
    //
    // C++ equivalent: MasterClient::BatchUpsertEnd(keys)
    // -----------------------------------------------------------------------

    /// Commit a batch of upsert operations: allocate or update replicas for
    /// multiple keys in a single RPC. Unlike the single-key convenience
    /// wrapper in [`upsert`](Self::upsert), this method sends all entries in
    /// one gRPC call, reducing network round-trips.
    ///
    /// 批量提交 upsert 操作：在单次 RPC 中为多个 key 分配或更新副本。
    /// 与 upsert() 中的单 key 便捷封装不同，此方法在一次 gRPC 调用中发送
    /// 所有条目，减少网络往返。
    ///
    /// Each entry carries its own slice_length and config.
    /// 每个条目携带自己的 slice_length 和 config。
    pub async fn batch_upsert_end(
        &mut self,
        entries: &[BatchUpsertEntry<'_>],
    ) -> StoreResult<Vec<i32>> {
        let request = proto::BatchUpsertEndRequest {
            entries: entries
                .iter()
                .map(|e| proto::PutEndEntry {
                    client_id: Some(self.client_id_proto()),
                    key: e.key.to_string(),
                    replica_type: e.replica_type,
                    tenant_id: e.tenant_id.to_string(),
                })
                .collect(),
        };
        let response = self
            .master
            .batch_upsert_end(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        ensure_batch_response_len("BatchUpsertEnd", entries.len(), response.statuses.len())?;
        Ok(response.statuses)
    }

    pub async fn batch_upsert_start_results(
        &mut self,
        keys: &[String],
        slice_lengths: &[u64],
        config: &ReplicateConfig,
        tenant_id: &str,
    ) -> StoreResult<Vec<BatchPutStartResult>> {
        if keys.len() != slice_lengths.len() {
            return Err(StoreError::InvalidParams(
                "keys and slice_lengths length mismatch".to_string(),
            ));
        }
        let request = proto::BatchUpsertStartRequest {
            entries: keys
                .iter()
                .zip(slice_lengths.iter())
                .enumerate()
                .map(|(index, (key, &slice_length))| {
                    batch_upsert_entry_config(config, keys.len(), index).map(|entry_config| {
                        proto::UpsertEntry {
                            client_id: Some(self.client_id_proto()),
                            key: key.clone(),
                            slice_length,
                            config: Some(self.replicate_config_to_proto(&entry_config)),
                            tenant_id: tenant_id.to_string(),
                        }
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?,
        };
        let response = self
            .master
            .batch_upsert_start(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();

        if response.results.len() != keys.len() {
            return Err(StoreError::Internal(format!(
                "BatchUpsertStart response size mismatch: expected {}, got {}",
                keys.len(),
                response.results.len()
            )));
        }
        response
            .results
            .iter()
            .zip(keys)
            .map(|(result, requested_key)| {
                if result.key != *requested_key {
                    return Err(StoreError::Internal(format!(
                        "BatchUpsertStart response key mismatch: expected {requested_key:?}, got {:?}",
                        result.key
                    )));
                }
                Ok(BatchPutStartResult {
                    key: result.key.clone(),
                    status: result.status,
                    replicas: self.replicas_from_proto(&result.replicas),
                    tenant_id: result.tenant_id.clone(),
                })
            })
            .collect()
    }

    pub async fn batch_upsert_revoke(
        &mut self,
        entries: &[BatchUpsertEntry<'_>],
    ) -> StoreResult<Vec<i32>> {
        let request = proto::BatchUpsertRevokeRequest {
            entries: entries
                .iter()
                .map(|e| proto::PutEndEntry {
                    client_id: Some(self.client_id_proto()),
                    key: e.key.to_string(),
                    replica_type: e.replica_type,
                    tenant_id: e.tenant_id.to_string(),
                })
                .collect(),
        };
        let response = self
            .master
            .batch_upsert_revoke(self.rpc_request(request))
            .await
            .map_err(Self::rpc_status_to_error)?
            .into_inner();
        ensure_batch_response_len("BatchUpsertRevoke", entries.len(), response.statuses.len())?;
        Ok(response.statuses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_upsert_indexes_group_ids_per_single_key_entry() {
        let config = ReplicateConfig {
            group_ids: vec!["group-a".to_string(), "group-b".to_string()],
            ..ReplicateConfig::default()
        };
        assert_eq!(
            batch_upsert_entry_config(&config, 2, 0).unwrap().group_ids,
            vec!["group-a"]
        );
        assert_eq!(
            batch_upsert_entry_config(&config, 2, 1).unwrap().group_ids,
            vec!["group-b"]
        );
        assert!(batch_upsert_entry_config(&config, 1, 0).is_err());
    }
}

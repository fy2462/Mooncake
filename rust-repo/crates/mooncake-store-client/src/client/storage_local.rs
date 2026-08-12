use super::MooncakeClient;
use super::storage::{OffloadTaskItem, PromotionTaskItem};
use crate::local_storage_backend::{
    AttachedLocalStorage, LocalStorageRecordMetadata, PendingStorageEviction, local_storage_key,
    parse_local_storage_key,
};
use crate::proto;
use async_trait::async_trait;
use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use transfer_engine_ffi::{RegisteredMemory, RegisteredMemoryAccess, StableMemoryOwner};
use uuid::Uuid;

const RECOVERED_LOCAL_DISK_NOTIFY_BATCH_SIZE: usize = 20_000;
const EVICTION_NOTIFY_MAX_ATTEMPTS: usize = 3;
const EVICTION_NOTIFY_RETRY_BASE_DELAY: Duration = Duration::from_millis(10);

struct OwnedExternalMountGuard {
    master: proto::master_service_client::MasterServiceClient<super::metrics::MetricsChannel>,
    runtime: tokio::runtime::Handle,
    client_id: Uuid,
    expected_segment_id: Uuid,
    rpc_request_timeout: Option<Duration>,
    registration: Option<RegisteredMemory>,
    armed: bool,
}

struct UnconfirmedResource<T>(Option<T>);

impl<T> UnconfirmedResource<T> {
    fn confirmed_absent(mut self) {
        drop(self.0.take());
    }
}

impl<T> Drop for UnconfirmedResource<T> {
    fn drop(&mut self) {
        if let Some(resource) = self.0.take() {
            std::mem::forget(resource);
        }
    }
}

impl OwnedExternalMountGuard {
    fn accept(mut self) -> RegisteredMemory {
        self.armed = false;
        self.registration
            .take()
            .expect("owned external mount registration must exist")
    }
}

impl Drop for OwnedExternalMountGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let Some(registration) = self.registration.take() else {
            return;
        };
        let mut master = self.master.clone();
        let client_id = self.client_id;
        let segment_id = self.expected_segment_id;
        let rpc_request_timeout = self.rpc_request_timeout;
        let registration = UnconfirmedResource(Some(registration));
        self.runtime.spawn(async move {
            let absent = super::lifecycle::unmount_confirms_segment_absent(
                master
                    .unmount_segment(MooncakeClient::rpc_request_with_timeout(
                        proto::UnmountSegmentRequest {
                            segment_id: Some(MooncakeClient::uuid_to_proto_uuid(segment_id)),
                            client_id: Some(MooncakeClient::uuid_to_proto_uuid(client_id)),
                        },
                        rpc_request_timeout,
                    ))
                    .await,
            );
            if absent {
                registration.confirmed_absent();
            } else {
                tracing::error!(
                    %segment_id,
                    "leaking owned external registration because cancelled mount absence was not proven"
                );
            }
        });
    }
}

#[derive(Debug, Default)]
struct TenantEvictionNotification {
    storage_keys: Vec<String>,
    user_keys: Vec<String>,
}

#[derive(Debug)]
pub(super) struct EvictionNotificationError {
    pub(super) accepted_storage_keys: HashSet<String>,
    pub(super) source: StoreError,
}

#[async_trait]
pub(super) trait DiskEvictionNotifier: Send {
    async fn notify_disk_eviction(
        &mut self,
        tenant_id: &str,
        keys: &[String],
        replica_type: i32,
    ) -> StoreResult<Vec<i32>>;
}

#[async_trait]
impl DiskEvictionNotifier for MooncakeClient {
    async fn notify_disk_eviction(
        &mut self,
        tenant_id: &str,
        keys: &[String],
        replica_type: i32,
    ) -> StoreResult<Vec<i32>> {
        self.batch_evict_disk_replica(keys, replica_type, tenant_id)
            .await
    }
}

pub(super) async fn notify_evicted_disk_replicas_with(
    notifier: &mut (impl DiskEvictionNotifier + ?Sized),
    storage_keys: &[String],
    replica_type: i32,
) -> Result<HashSet<String>, EvictionNotificationError> {
    let mut keys_by_tenant: BTreeMap<String, TenantEvictionNotification> = BTreeMap::new();
    for storage_key in storage_keys {
        let (tenant_id, key) = parse_local_storage_key(storage_key);
        let notification = keys_by_tenant.entry(tenant_id.to_string()).or_default();
        notification.storage_keys.push(storage_key.clone());
        notification.user_keys.push(key.to_string());
    }

    let mut accepted_storage_keys = HashSet::with_capacity(storage_keys.len());
    for (tenant_id, notification) in keys_by_tenant {
        let mut remaining = notification
            .storage_keys
            .into_iter()
            .zip(notification.user_keys)
            .collect::<Vec<_>>();
        let mut last_error = None;
        for attempt in 0..EVICTION_NOTIFY_MAX_ATTEMPTS {
            let user_keys = remaining
                .iter()
                .map(|(_, user_key)| user_key.clone())
                .collect::<Vec<_>>();
            match notifier
                .notify_disk_eviction(&tenant_id, &user_keys, replica_type)
                .await
            {
                Ok(statuses) if statuses.len() == remaining.len() => {
                    let mut retry = Vec::new();
                    for ((storage_key, user_key), status) in remaining.into_iter().zip(statuses) {
                        if status == 0 || status == -1 {
                            accepted_storage_keys.insert(storage_key);
                        } else {
                            retry.push((storage_key, user_key));
                        }
                    }
                    remaining = retry;
                    if remaining.is_empty() {
                        last_error = None;
                        break;
                    }
                    last_error = Some(StoreError::Internal(format!(
                        "Master rejected {} disk eviction item(s) for tenant {tenant_id}",
                        remaining.len()
                    )));
                }
                Ok(statuses) => {
                    last_error = Some(StoreError::Internal(format!(
                        "disk eviction returned {} statuses for {} keys",
                        statuses.len(),
                        remaining.len()
                    )));
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
            if attempt + 1 < EVICTION_NOTIFY_MAX_ATTEMPTS {
                tokio::time::sleep(EVICTION_NOTIFY_RETRY_BASE_DELAY * (attempt as u32 + 1)).await;
            }
        }

        if let Some(source) = last_error {
            return Err(EvictionNotificationError {
                accepted_storage_keys,
                source,
            });
        }
    }
    Ok(accepted_storage_keys)
}

async fn finalize_partially_accepted_eviction(
    storage: AttachedLocalStorage,
    pending: PendingStorageEviction,
    accepted_storage_keys: &HashSet<String>,
) -> StoreResult<usize> {
    let (accepted, unaccepted) = pending.partition_accepted(accepted_storage_keys);
    let accepted_count = accepted.keys().len();
    tokio::task::spawn_blocking(move || {
        let commit_result = if accepted_count == 0 {
            storage.rollback_eviction(accepted);
            Ok(())
        } else {
            storage.commit_eviction(accepted)
        };
        storage.rollback_eviction(unaccepted);
        commit_result
    })
    .await
    .map_err(|error| StoreError::Internal(error.to_string()))??;
    Ok(accepted_count)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveredLocalDiskRecord {
    task: OffloadTaskItem,
    key_size: i64,
    data_size: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct RecoveredLocalDiskNotificationBatch {
    tasks: Vec<OffloadTaskItem>,
    metadatas: Vec<proto::StorageObjectMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveredLocalDiskServerAction {
    Start,
    Reuse,
}

fn parse_recovered_local_storage_key(storage_key: &str) -> StoreResult<(&str, &str)> {
    let encoded = storage_key.strip_prefix("v1:").ok_or_else(|| {
        StoreError::InvalidParams(format!(
            "recovered local-storage key is not tenant-scoped: {storage_key:?}"
        ))
    })?;
    let (tenant_len, payload) = encoded.split_once(':').ok_or_else(|| {
        StoreError::InvalidParams(format!(
            "recovered local-storage key has an invalid tenant length: {storage_key:?}"
        ))
    })?;
    if tenant_len.is_empty() || !tenant_len.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(StoreError::InvalidParams(format!(
            "recovered local-storage key has an invalid tenant length: {storage_key:?}"
        )));
    }
    let tenant_len = tenant_len.parse::<usize>().map_err(|_| {
        StoreError::InvalidParams(format!(
            "recovered local-storage key tenant length overflows usize: {storage_key:?}"
        ))
    })?;
    if tenant_len > payload.len() || !payload.is_char_boundary(tenant_len) {
        return Err(StoreError::InvalidParams(format!(
            "recovered local-storage key has an invalid tenant boundary: {storage_key:?}"
        )));
    }
    let (tenant_id, key) = payload.split_at(tenant_len);
    if tenant_id.is_empty()
        || tenant_id.starts_with('_')
        || tenant_id
            .as_bytes()
            .iter()
            .any(|byte| *byte < 0x20 || *byte == 0x7f)
    {
        return Err(StoreError::InvalidParams(format!(
            "recovered local-storage key has an invalid tenant: {storage_key:?}"
        )));
    }
    if key.is_empty() {
        return Err(StoreError::InvalidParams(format!(
            "recovered local-storage key has an empty user key: {storage_key:?}"
        )));
    }
    if local_storage_key(tenant_id, key) != storage_key {
        return Err(StoreError::InvalidParams(format!(
            "recovered local-storage key is not canonically encoded: {storage_key:?}"
        )));
    }
    Ok((tenant_id, key))
}

fn prepare_recovered_local_disk_records(
    mut metadata: Vec<(String, u64)>,
) -> StoreResult<Vec<RecoveredLocalDiskRecord>> {
    metadata.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    prepare_recovered_local_disk_records_with_generation(
        metadata
            .into_iter()
            .map(|(storage_key, value_size)| LocalStorageRecordMetadata {
                storage_key,
                value_size,
                generation_id: Uuid::nil(),
            })
            .collect(),
    )
}

fn prepare_recovered_local_disk_records_with_generation(
    mut metadata: Vec<LocalStorageRecordMetadata>,
) -> StoreResult<Vec<RecoveredLocalDiskRecord>> {
    metadata.sort_unstable_by(|left, right| left.storage_key.cmp(&right.storage_key));
    metadata
        .into_iter()
        .map(|record| {
            let storage_key = record.storage_key;
            let (tenant_id, key) = parse_recovered_local_storage_key(&storage_key)?;
            let key_size = i64::try_from(key.len()).map_err(|_| {
                StoreError::InvalidParams(format!(
                    "recovered local-storage key is too large: {storage_key:?}"
                ))
            })?;
            let data_size = i64::try_from(record.value_size).map_err(|_| {
                StoreError::InvalidParams(format!(
                    "recovered local-storage value is too large: {storage_key:?}"
                ))
            })?;
            Ok(RecoveredLocalDiskRecord {
                task: OffloadTaskItem {
                    tenant_id: tenant_id.to_string(),
                    key: key.to_string(),
                    size: data_size,
                    generation_id: record.generation_id,
                },
                key_size,
                data_size,
            })
        })
        .collect()
}

fn recovered_local_disk_notification_batch(
    records: &[RecoveredLocalDiskRecord],
    transport_endpoint: &str,
) -> StoreResult<RecoveredLocalDiskNotificationBatch> {
    if records.is_empty() {
        return Ok(RecoveredLocalDiskNotificationBatch {
            tasks: Vec::new(),
            metadatas: Vec::new(),
        });
    }
    if transport_endpoint.is_empty() {
        return Err(StoreError::InvalidParams(
            "recovered local-disk transport endpoint must not be empty".to_string(),
        ));
    }

    Ok(RecoveredLocalDiskNotificationBatch {
        tasks: records.iter().map(|record| record.task.clone()).collect(),
        metadatas: records
            .iter()
            .map(|record| proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: record.key_size,
                data_size: record.data_size,
                transport_endpoint: transport_endpoint.to_string(),
            })
            .collect(),
    })
}

fn recovered_local_disk_record_batches(
    records: &[RecoveredLocalDiskRecord],
    batch_size: usize,
) -> StoreResult<std::slice::Chunks<'_, RecoveredLocalDiskRecord>> {
    if batch_size == 0 {
        return Err(StoreError::InvalidParams(
            "recovered local-disk notification batch size must be positive".to_string(),
        ));
    }
    Ok(records.chunks(batch_size))
}

fn recovered_local_disk_server_action(
    record_count: usize,
    transfer_engine_enabled: bool,
    server_running: bool,
) -> StoreResult<Option<RecoveredLocalDiskServerAction>> {
    if record_count == 0 {
        return Ok(None);
    }
    if !transfer_engine_enabled {
        return Err(StoreError::Internal(
            "cannot recover local-disk replicas without a Transfer Engine".to_string(),
        ));
    }
    Ok(Some(if server_running {
        RecoveredLocalDiskServerAction::Reuse
    } else {
        RecoveredLocalDiskServerAction::Start
    }))
}

#[cfg(test)]
#[derive(Default)]
struct PromotionTestCallCounts {
    heartbeat: std::sync::atomic::AtomicUsize,
    disk_read: std::sync::atomic::AtomicUsize,
    alloc: std::sync::atomic::AtomicUsize,
    transfer_write: std::sync::atomic::AtomicUsize,
    notify_success: std::sync::atomic::AtomicUsize,
    notify_failure: std::sync::atomic::AtomicUsize,
    notify_failure_keys: std::sync::Mutex<Vec<(String, String)>>,
}

#[cfg(test)]
static PROMOTION_TEST_CALL_COUNTS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<Uuid, std::sync::Arc<PromotionTestCallCounts>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
struct PromotionTestCallGuard(Uuid);

#[cfg(test)]
impl Drop for PromotionTestCallGuard {
    fn drop(&mut self) {
        PROMOTION_TEST_CALL_COUNTS.lock().unwrap().remove(&self.0);
    }
}

#[cfg(test)]
fn observe_promotion_test_calls(
    client_id: Uuid,
) -> (
    std::sync::Arc<PromotionTestCallCounts>,
    PromotionTestCallGuard,
) {
    let counts = std::sync::Arc::new(PromotionTestCallCounts::default());
    PROMOTION_TEST_CALL_COUNTS
        .lock()
        .unwrap()
        .insert(client_id, std::sync::Arc::clone(&counts));
    (counts, PromotionTestCallGuard(client_id))
}

#[cfg(test)]
fn record_promotion_test_call(
    client_id: Uuid,
    field: fn(&PromotionTestCallCounts) -> &std::sync::atomic::AtomicUsize,
) {
    if let Some(counts) = PROMOTION_TEST_CALL_COUNTS.lock().unwrap().get(&client_id) {
        field(counts).fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
fn record_promotion_test_failure(client_id: Uuid, tenant_id: &str, key: &str) {
    if let Some(counts) = PROMOTION_TEST_CALL_COUNTS.lock().unwrap().get(&client_id) {
        counts
            .notify_failure
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        counts
            .notify_failure_keys
            .lock()
            .unwrap()
            .push((tenant_id.to_string(), key.to_string()));
    }
}

#[cfg(test)]
static PROMOTION_TEST_ALLOC_SUCCESSES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<(Uuid, String), u64>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
struct PromotionTestAllocGuard(Uuid, String);

#[cfg(test)]
impl Drop for PromotionTestAllocGuard {
    fn drop(&mut self) {
        PROMOTION_TEST_ALLOC_SUCCESSES
            .lock()
            .unwrap()
            .remove(&(self.0, self.1.clone()));
    }
}

#[cfg(test)]
fn inject_promotion_test_alloc_success(
    client_id: Uuid,
    key: &str,
    size: u64,
) -> PromotionTestAllocGuard {
    PROMOTION_TEST_ALLOC_SUCCESSES
        .lock()
        .unwrap()
        .insert((client_id, key.to_string()), size);
    PromotionTestAllocGuard(client_id, key.to_string())
}

#[cfg(test)]
fn take_promotion_test_alloc_success(client_id: Uuid, key: &str) -> Option<u64> {
    PROMOTION_TEST_ALLOC_SUCCESSES
        .lock()
        .unwrap()
        .remove(&(client_id, key.to_string()))
}

#[cfg(test)]
fn promotion_test_replica(size: u64) -> mooncake_store_core::ReplicaDescriptor {
    mooncake_store_core::ReplicaDescriptor {
        segment_id: Uuid::new_v4(),
        segment_name: "promotion-test-target".to_string(),
        offset: 0,
        size,
        status: mooncake_store_core::ReplicaStatus::Allocating,
        replica_type: mooncake_store_core::ReplicaType::Memory,
        holder_client_id: None,
        local_disk_storage_id: None,
        local_disk_generation_id: None,
        refcnt: 0,
        handle_valid: true,
        base_addr: 0,
        protocol: "tcp".to_string(),
    }
}

#[cfg(test)]
static PROMOTION_TEST_TRANSFER_FAILURES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<(Uuid, String, String)>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
static PROMOTION_TEST_TRANSFER_SUCCESSES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<(Uuid, String, String)>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
struct PromotionTestTransferFailureGuard(Uuid, String, String);

#[cfg(test)]
impl Drop for PromotionTestTransferFailureGuard {
    fn drop(&mut self) {
        PROMOTION_TEST_TRANSFER_FAILURES.lock().unwrap().remove(&(
            self.0,
            self.1.clone(),
            self.2.clone(),
        ));
    }
}

#[cfg(test)]
fn inject_promotion_test_transfer_failure(
    client_id: Uuid,
    tenant_id: &str,
    key: &str,
) -> PromotionTestTransferFailureGuard {
    let entry = (client_id, tenant_id.to_string(), key.to_string());
    assert!(
        !PROMOTION_TEST_TRANSFER_SUCCESSES
            .lock()
            .unwrap()
            .contains(&entry),
        "cannot inject both promotion transfer success and failure"
    );
    PROMOTION_TEST_TRANSFER_FAILURES
        .lock()
        .unwrap()
        .insert(entry);
    PromotionTestTransferFailureGuard(client_id, tenant_id.to_string(), key.to_string())
}

#[cfg(test)]
fn take_promotion_test_transfer_failure(client_id: Uuid, tenant_id: &str, key: &str) -> bool {
    PROMOTION_TEST_TRANSFER_FAILURES.lock().unwrap().remove(&(
        client_id,
        tenant_id.to_string(),
        key.to_string(),
    ))
}

#[cfg(test)]
struct PromotionTestTransferSuccessGuard(Uuid, String, String);

#[cfg(test)]
impl Drop for PromotionTestTransferSuccessGuard {
    fn drop(&mut self) {
        PROMOTION_TEST_TRANSFER_SUCCESSES.lock().unwrap().remove(&(
            self.0,
            self.1.clone(),
            self.2.clone(),
        ));
    }
}

#[cfg(test)]
fn inject_promotion_test_transfer_success(
    client_id: Uuid,
    tenant_id: &str,
    key: &str,
) -> PromotionTestTransferSuccessGuard {
    let entry = (client_id, tenant_id.to_string(), key.to_string());
    assert!(
        !PROMOTION_TEST_TRANSFER_FAILURES
            .lock()
            .unwrap()
            .contains(&entry),
        "cannot inject both promotion transfer success and failure"
    );
    PROMOTION_TEST_TRANSFER_SUCCESSES
        .lock()
        .unwrap()
        .insert(entry);
    PromotionTestTransferSuccessGuard(client_id, tenant_id.to_string(), key.to_string())
}

#[cfg(test)]
fn take_promotion_test_transfer_success(client_id: Uuid, tenant_id: &str, key: &str) -> bool {
    PROMOTION_TEST_TRANSFER_SUCCESSES.lock().unwrap().remove(&(
        client_id,
        tenant_id.to_string(),
        key.to_string(),
    ))
}

impl MooncakeClient {
    /// Re-publish persistent local-disk replicas after the disk segment has
    /// been mounted. This is intentionally fail-closed: every record is
    /// parsed and size-checked before the first master notification.
    pub(super) async fn recover_local_disk_replicas(
        &mut self,
        recovery_session_id: Uuid,
    ) -> StoreResult<usize> {
        let Some(storage) = self.local_storage.as_ref().cloned() else {
            return Ok(0);
        };
        let scan_storage = storage.clone();
        let metadata = tokio::task::spawn_blocking(move || scan_storage.scan_records())
            .await
            .map_err(|error| {
                StoreError::Internal(format!(
                    "recovered local-storage metadata scan task failed: {error}"
                ))
            })??;
        let records = prepare_recovered_local_disk_records_with_generation(metadata)?;
        // A LOCAL_DISK replica is only useful when peers can read it. Merely
        // publishing an endpoint from an rpc_only client would create
        // unreachable master metadata, so require the data plane up front.
        match recovered_local_disk_server_action(
            records.len(),
            self.engine.is_enabled(),
            self.offload_server_state.is_running(),
        )? {
            None => return Ok(0),
            Some(RecoveredLocalDiskServerAction::Start) => {
                self.start_offload_server().await?;
            }
            Some(RecoveredLocalDiskServerAction::Reuse) => {}
        }
        let transport_endpoint = self.offload_rpc_address();
        if transport_endpoint.is_empty() {
            return Err(StoreError::Internal(
                "offload server did not publish a transport endpoint".to_string(),
            ));
        }

        for records in
            recovered_local_disk_record_batches(&records, RECOVERED_LOCAL_DISK_NOTIFY_BATCH_SIZE)?
        {
            let batch = recovered_local_disk_notification_batch(records, &transport_endpoint)?;
            let stale_tasks = self
                .notify_offload_success_tasks_for_recovery(
                    batch.tasks,
                    batch.metadatas,
                    recovery_session_id,
                )
                .await?;
            if !stale_tasks.is_empty() {
                let storage = storage.clone();
                tokio::task::spawn_blocking(move || {
                    for task in stale_tasks {
                        storage.delete_object_if_generation(
                            &local_storage_key(&task.tenant_id, &task.key),
                            task.generation_id,
                        )?;
                    }
                    Ok::<(), StoreError>(())
                })
                .await
                .map_err(|error| StoreError::Internal(error.to_string()))??;
            }
        }
        Ok(records.len())
    }

    /// Execute a complete offload cycle:
    /// 1. Heartbeat to get objects-to-offload from master.
    /// 2. Read object data from memory.
    /// 3. Write data to local disk.
    /// 4. Notify master of success.
    ///
    /// Requires a [`LocalStorageBackend`] to be attached via
    /// [`with_local_storage_backend`](Self::with_local_storage_backend).
    ///
    /// Returns the number of objects successfully offloaded.
    ///
    /// 执行完整的 offload 循环：
    /// 1. 心跳获取待 offload 对象。
    /// 2. 从内存读取对象数据。
    /// 3. 将数据写入本地磁盘。
    /// 4. 通知 master 成功。
    ///
    /// 需要先通过 with_local_storage_backend 挂载本地存储后端。
    ///
    /// 返回成功 offload 的对象数量。
    pub async fn offload_objects(&mut self, enable_offloading: bool) -> StoreResult<usize> {
        let tasks = self
            .offload_object_heartbeat_tasks(enable_offloading)
            .await?;
        if tasks.is_empty() {
            return Ok(0);
        }

        if !self.offload_server_state.is_running() {
            if let Err(error) = self.start_offload_server().await {
                self.notify_offload_failure_tasks(tasks).await?;
                return Err(error);
            }
        }
        let transport_endpoint = self.offload_rpc_address();

        let Some(storage) = self.local_storage.as_ref() else {
            self.notify_offload_failure_tasks(tasks).await?;
            return Err(StoreError::Internal(
                "no local storage backend configured".to_string(),
            ));
        };
        let storage = storage.clone();

        let mut offloaded = 0usize;
        let mut notify_tasks = Vec::with_capacity(tasks.len());
        let mut metadatas = Vec::with_capacity(tasks.len());
        let mut committed_storage_keys = Vec::with_capacity(tasks.len());

        for task in &tasks {
            let key = task.key.as_str();
            let tenant_id = task.tenant_id.as_str();
            if task.size < 0 {
                tracing::warn!(target: "storage_debug", %tenant_id, %key, size = task.size, "offload: invalid negative task size");
                notify_tasks.push(task.clone());
                metadatas.push(failed_offload_metadata());
                continue;
            }
            if task.generation_id.is_nil() {
                return Err(StoreError::Internal(format!(
                    "master returned an offload task without a generation for key {key:?}"
                )));
            }
            // Read object data from memory.
            let data = match self.get_for_tenant(key, tenant_id).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "offload: failed to get object from memory, skipping");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
            };

            // Reserve FIFO victims first. Master metadata is updated before
            // those files are deleted, so readers never get routed to an
            // already-removed LOCAL_DISK replica.
            let key_owned = local_storage_key(tenant_id, key);
            let key_for_prepare = key_owned.clone();
            let s = storage.clone();
            let pending = match tokio::task::spawn_blocking(move || {
                s.prepare_write(&key_for_prepare, data.len() as u64)
                    .map(|pending| (pending, data))
            })
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "offload: failed to reserve local storage");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "offload: local storage write task failed");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
            };
            let (pending_eviction, data) = pending;
            let evicted_keys = pending_eviction.keys();
            if let Err(EvictionNotificationError {
                accepted_storage_keys,
                source,
            }) = self.notify_evicted_disk_replicas(&evicted_keys).await
            {
                match finalize_partially_accepted_eviction(
                    storage.clone(),
                    pending_eviction,
                    &accepted_storage_keys,
                )
                .await
                {
                    Ok(accepted_count) if accepted_count > 0 => {
                        tracing::info!(
                            target: "storage_debug",
                            %tenant_id,
                            %key,
                            accepted_count,
                            "offload: committed the acknowledged subset of local evictions"
                        );
                    }
                    Ok(_) => {}
                    Err(cleanup_error) => {
                        tracing::warn!(
                            target: "storage_debug",
                            %tenant_id,
                            %key,
                            %cleanup_error,
                            "offload: failed to finalize acknowledged local evictions"
                        );
                    }
                }
                tracing::warn!(target: "storage_debug", %tenant_id, %key, error = %source, "offload: failed to publish local eviction");
                notify_tasks.push(task.clone());
                metadatas.push(failed_offload_metadata());
                continue;
            }

            let key_for_write = key_owned.clone();
            let generation_id = task.generation_id;
            let s = storage.clone();
            let write_started_at = std::time::Instant::now();
            let written_bytes = data.len() as u64;
            let write_result = tokio::task::spawn_blocking(move || {
                s.commit_write(&key_for_write, &data, pending_eviction, generation_id)
            })
            .await;
            match write_result {
                Ok(Ok(())) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.observe_ssd_write(written_bytes, 1, write_started_at.elapsed());
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %error, "offload: failed to write object to local storage");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
                Err(error) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %error, "offload: local storage write task failed");
                    notify_tasks.push(task.clone());
                    metadatas.push(failed_offload_metadata());
                    continue;
                }
            }

            for evicted_key in &evicted_keys {
                tracing::info!(target: "storage_debug", %evicted_key, "offload: evicted old file");
            }

            offloaded += 1;
            committed_storage_keys.push(key_owned);
            notify_tasks.push(task.clone());
            metadatas.push(proto::StorageObjectMetadata {
                bucket_id: 0,
                offset: 0,
                key_size: key.len() as i64,
                data_size: task.size,
                transport_endpoint: transport_endpoint.clone(),
            });
        }

        if !notify_tasks.is_empty() {
            let notification_result = self
                .notify_offload_success_tasks(notify_tasks, metadatas)
                .await;
            finalize_offload_publication(storage, committed_storage_keys, notification_result)
                .await?;
        }

        Ok(offloaded)
    }

    async fn notify_offload_failure_tasks(
        &mut self,
        tasks: Vec<OffloadTaskItem>,
    ) -> StoreResult<()> {
        if tasks.is_empty() {
            return Ok(());
        }
        let metadatas = tasks.iter().map(|_| failed_offload_metadata()).collect();
        self.notify_offload_success_tasks(tasks, metadatas).await
    }

    async fn notify_evicted_disk_replicas(
        &mut self,
        storage_keys: &[String],
    ) -> Result<HashSet<String>, EvictionNotificationError> {
        notify_evicted_disk_replicas_with(
            self,
            storage_keys,
            proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        )
        .await
    }

    async fn run_local_disk_watermark_eviction(
        &mut self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<usize> {
        let Some(storage) = self.local_storage.as_ref().cloned() else {
            return Ok(0);
        };
        let prepare_storage = storage.clone();
        let pending = tokio::task::spawn_blocking(move || {
            prepare_storage.prepare_watermark_eviction(high_watermark_ratio, low_watermark_ratio)
        })
        .await
        .map_err(|error| StoreError::Internal(error.to_string()))??;
        let evicted_keys = pending.keys();
        if let Err(EvictionNotificationError {
            accepted_storage_keys,
            source,
        }) = self.notify_evicted_disk_replicas(&evicted_keys).await
        {
            return match finalize_partially_accepted_eviction(
                storage,
                pending,
                &accepted_storage_keys,
            )
            .await
            {
                Ok(_) => Err(source),
                Err(cleanup_error) => Err(StoreError::Internal(format!(
                    "disk eviction notification failed: {source}; \
                     acknowledged local eviction finalization failed: {cleanup_error}"
                ))),
            };
        }
        let count = evicted_keys.len();
        tokio::task::spawn_blocking(move || storage.commit_eviction(pending))
            .await
            .map_err(|error| StoreError::Internal(error.to_string()))??;
        Ok(count)
    }

    async fn run_global_disk_watermark_eviction(
        &mut self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<usize> {
        let Some(storage) = self.global_disk.as_ref().cloned() else {
            return Ok(0);
        };
        let pending = storage
            .prepare_watermark_eviction(high_watermark_ratio, low_watermark_ratio)
            .await?;
        let evicted_keys = pending.eviction_storage_keys();
        if let Err(EvictionNotificationError {
            accepted_storage_keys,
            source,
        }) = notify_evicted_disk_replicas_with(
            self,
            &evicted_keys,
            proto::replica_descriptor::ReplicaType::Disk as i32,
        )
        .await
        {
            return match storage
                .finalize_partial_watermark(pending, accepted_storage_keys)
                .await
            {
                Ok(_) => Err(source),
                Err(cleanup_error) => Err(StoreError::Internal(format!(
                    "global DISK watermark notification failed: {source}; \
                     accepted victim finalization failed: {cleanup_error}"
                ))),
            };
        }
        storage.commit_watermark_eviction(pending).await
    }

    pub async fn run_disk_watermark_eviction(
        &mut self,
        high_watermark_ratio: f64,
        low_watermark_ratio: f64,
    ) -> StoreResult<usize> {
        let local = self
            .run_local_disk_watermark_eviction(high_watermark_ratio, low_watermark_ratio)
            .await?;
        let global = self
            .run_global_disk_watermark_eviction(high_watermark_ratio, low_watermark_ratio)
            .await?;
        local.checked_add(global).ok_or_else(|| {
            StoreError::Internal("disk watermark eviction count overflow".to_string())
        })
    }

    /// Execute a complete promotion cycle:
    /// 1. Heartbeat to get objects-to-promote from master.
    /// 2. Read data from local disk.
    /// 3. Allocate a memory replica via `promotion_alloc_start`.
    /// 4. Write data to the allocated replica via `write_to_replica`.
    /// 5. Notify master of success or failure.
    ///
    /// Requires a [`LocalStorageBackend`] to be attached via
    /// [`with_local_storage_backend`](Self::with_local_storage_backend).
    ///
    /// Returns the number of objects successfully promoted.
    ///
    /// 执行完整的 promotion 循环：
    /// 1. 心跳获取待 promotion 对象。
    /// 2. 从本地磁盘读取数据。
    /// 3. 通过 promotion_alloc_start 分配内存副本。
    /// 4. 通过 write_to_replica 将数据写入分配的副本。
    /// 5. 通知 master 成功或失败。
    ///
    /// 需要先通过 with_local_storage_backend 挂载本地存储后端。
    ///
    /// 返回成功 promotion 的对象数量。
    pub async fn promote_objects(&mut self) -> StoreResult<usize> {
        #[cfg(test)]
        record_promotion_test_call(self.client_id, |counts| &counts.heartbeat);
        let tasks = match self.promotion_object_heartbeat_tasks().await {
            Ok(tasks) => tasks,
            Err(StoreError::KeyNotFound(message)) => {
                tracing::debug!(target: "storage_debug", %message, "promotion: local disk session disappeared; waiting for remount");
                return Ok(0);
            }
            Err(error) => return Err(error),
        };
        self.process_promotion_tasks(tasks).await
    }

    async fn process_promotion_tasks(
        &mut self,
        tasks: Vec<PromotionTaskItem>,
    ) -> StoreResult<usize> {
        if tasks.is_empty() {
            return Ok(0);
        }

        let storage = self.local_storage.as_ref().ok_or_else(|| {
            StoreError::Internal("no local storage backend configured".to_string())
        })?;
        let storage = storage.clone();

        let mut promoted = 0usize;

        for task in &tasks {
            let key = task.key.as_str();
            let tenant_id = task.tenant_id.as_str();
            if task.size <= 0 {
                tracing::warn!(target: "storage_debug", %tenant_id, %key, size = task.size, "promotion: skipping non-positive task size");
                continue;
            }
            // Allocate a memory replica.
            #[cfg(test)]
            record_promotion_test_call(self.client_id, |counts| &counts.alloc);
            #[cfg(test)]
            let injected_allocation = take_promotion_test_alloc_success(self.client_id, key);
            #[cfg(not(test))]
            let injected_allocation: Option<u64> = None;
            let replica = if let Some(size) = injected_allocation {
                #[cfg(test)]
                {
                    promotion_test_replica(size)
                }
                #[cfg(not(test))]
                unreachable!()
            } else {
                match self
                    .promotion_alloc_start_for_tenant(key, tenant_id, task.size as u64, vec![])
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "promotion: alloc failed");
                        #[cfg(test)]
                        record_promotion_test_failure(self.client_id, tenant_id, key);
                        let _ = self
                            .notify_promotion_failure_for_tenant(key, tenant_id)
                            .await;
                        continue;
                    }
                }
            };

            // Read from local disk (blocking I/O) only after the Master has
            // reserved the promotion target, matching the C++ orchestration.
            #[cfg(test)]
            record_promotion_test_call(self.client_id, |counts| &counts.disk_read);
            let key_owned = local_storage_key(tenant_id, key);
            let read_started_at = std::time::Instant::now();
            let data_result = {
                let s = storage.clone();
                tokio::task::spawn_blocking(move || s.read_object(&key_owned))
                    .await
                    .map_err(|e| StoreError::Internal(e.to_string()))
                    .and_then(|result| result)
            };
            let data = match data_result {
                Ok(data) => data,
                Err(error) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %error, "promotion: local disk read failed");
                    #[cfg(test)]
                    record_promotion_test_failure(self.client_id, tenant_id, key);
                    let _ = self
                        .notify_promotion_failure_for_tenant(key, tenant_id)
                        .await;
                    continue;
                }
            };
            if let Some(metrics) = &self.metrics {
                metrics.observe_ssd_read(data.len() as u64, 1, read_started_at.elapsed());
            }

            // Write data to the allocated memory replica.
            #[cfg(test)]
            record_promotion_test_call(self.client_id, |counts| &counts.transfer_write);
            #[cfg(test)]
            let injected_transfer_failure =
                take_promotion_test_transfer_failure(self.client_id, tenant_id, key);
            #[cfg(test)]
            let injected_transfer_success =
                take_promotion_test_transfer_success(self.client_id, tenant_id, key);
            #[cfg(not(test))]
            let injected_transfer_failure = false;
            #[cfg(not(test))]
            let injected_transfer_success = false;
            debug_assert!(!(injected_transfer_failure && injected_transfer_success));
            let write_result = if injected_transfer_failure {
                Err(StoreError::OperationFailed(-1))
            } else if injected_transfer_success {
                Ok(())
            } else {
                self.write_to_replica(&replica, &data).await
            };
            match write_result {
                Ok(()) => {
                    #[cfg(test)]
                    record_promotion_test_call(self.client_id, |counts| &counts.notify_success);
                    match self
                        .notify_promotion_success_for_tenant(key, tenant_id)
                        .await
                    {
                        Ok(()) => promoted += 1,
                        Err(error) => {
                            tracing::warn!(target: "storage_debug", %tenant_id, %key, %error, "promotion: success notification failed");
                            #[cfg(test)]
                            record_promotion_test_failure(self.client_id, tenant_id, key);
                            let _ = self
                                .notify_promotion_failure_for_tenant(key, tenant_id)
                                .await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(target: "storage_debug", %tenant_id, %key, %e, "promotion: write_to_replica failed");
                    #[cfg(test)]
                    record_promotion_test_failure(self.client_id, tenant_id, key);
                    let _ = self
                        .notify_promotion_failure_for_tenant(key, tenant_id)
                        .await;
                }
            }
        }

        Ok(promoted)
    }

    // -----------------------------------------------------------------------
    // Dynamic segment mount/unmount
    // 动态 segment 挂载/卸载
    //
    // C++ equivalent: RealClient::mountSegment / unmountSegment /
    // allocateAndMountSegment / unmountAndFreeSegment
    // -----------------------------------------------------------------------

    /// Mount a memory segment with the given name, size, and base address.
    /// The memory must already be allocated and registered with the
    /// TransferEngine before calling this (for externally-mapped segments).
    /// After mounting, the segment is registered as a local endpoint for
    /// locality-aware replica selection.
    ///
    /// 挂载指定名称、大小和基地址的内存 segment。
    /// 调用前内存必须已分配并已向 TransferEngine 注册（用于外部映射的 segment）。
    /// 挂载后，该 segment 被注册为本地端点，用于本地性感知副本选择。
    ///
    /// For internally-allocated segments (where this node allocates memory and
    /// opens the segment on the TE), this is handled automatically in
    /// [`create`](Self::create) when `global_segment_size > 0`.
    ///
    /// 对于内部分配的 segment（本节点分配内存并在 TE 上打开 segment），
    /// 在 create() 中 global_segment_size > 0 时自动处理。
    ///
    /// C++ equivalent: `Client::MountSegment()`
    pub async fn mount_segment(
        &mut self,
        segment_name: &str,
        size: u64,
        base_addr: u64,
    ) -> StoreResult<()> {
        self.mount_segment_with_id(segment_name, size, base_addr)
            .await
            .map(|_| ())
    }

    /// Split a file-backed mount using the C++ page-aligned max-MR rule.
    pub fn file_mount_chunk_sizes(&self, size: u64) -> StoreResult<Vec<u64>> {
        if size == 0 {
            return Err(StoreError::InvalidParams(
                "file-backed Store segment size must be greater than zero".to_string(),
            ));
        }
        let max_mr_size = Self::resolve_max_mr_size(
            &self.protocol,
            size,
            std::env::var("MC_MAX_MR_SIZE").ok().as_deref(),
        )?;
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(StoreError::Internal(
                "failed to determine system page size".to_string(),
            ));
        }
        let aligned_max = max_mr_size / page_size as u64 * page_size as u64;
        if aligned_max == 0 {
            return Err(StoreError::InvalidParams(format!(
                "MC_MAX_MR_SIZE {max_mr_size} is smaller than page size {page_size}"
            )));
        }
        let mut chunks = Vec::new();
        let mut remaining = size;
        while remaining > 0 {
            let chunk = remaining.min(aligned_max);
            chunks.push(chunk);
            remaining -= chunk;
        }
        Ok(chunks)
    }

    /// Register an owner-bearing local allocation and mount it as an external
    /// Store segment. The registration owns the allocation until a successful
    /// UUID unmount or complete client teardown.
    pub async fn mount_owned_external_segment<O>(
        &mut self,
        segment_name: &str,
        owner: O,
        protocol: &str,
        location: &str,
    ) -> StoreResult<Uuid>
    where
        O: StableMemoryOwner,
    {
        if protocol != self.protocol {
            return Err(StoreError::InvalidParams(format!(
                "mounted segment protocol {protocol:?} does not match client protocol {:?}",
                self.protocol
            )));
        }
        let registration_location = if location.trim().is_empty() {
            "cpu:0"
        } else {
            location
        };
        let registration = self.engine.register_owned_memory(
            owner,
            registration_location,
            true,
            RegisteredMemoryAccess::ReadWrite,
        )?;
        let base_addr = registration.id()?.base_address() as u64;
        let size = registration.len()? as u64;
        let expected_segment_id = mooncake_store_core::stable_memory_segment_id(
            self.client_id,
            segment_name,
            base_addr,
            size,
            &self.local_transport_endpoint,
            &self.protocol,
            &self.host_id,
        );
        let guard = OwnedExternalMountGuard {
            master: self.master.clone(),
            runtime: tokio::runtime::Handle::current(),
            client_id: self.client_id,
            expected_segment_id,
            rpc_request_timeout: self.rpc_request_timeout,
            registration: Some(registration),
            armed: true,
        };
        match self
            .mount_segment_with_id(segment_name, size, base_addr)
            .await
        {
            Ok(segment_id) => {
                let registration = guard.accept();
                self.mounted_owned_external_registrations
                    .write()
                    .insert(segment_id, registration);
                Ok(segment_id)
            }
            Err(error) => Err(error),
        }
    }

    /// UUID-returning form of [`mount_segment`](Self::mount_segment).
    ///
    /// Segment names are not unique once a capacity is split into multiple
    /// MRs. Callers that may mount duplicate names must retain this UUID and
    /// use [`unmount_segment_by_id`](Self::unmount_segment_by_id).
    ///
    /// If rollback cannot resolve an ambiguous RPC outcome, this returns
    /// [`StoreError::SegmentMountOutcomeAmbiguous`]. The caller must retain the
    /// memory owner until the reported deterministic UUID is confirmed absent.
    pub async fn mount_segment_with_id(
        &mut self,
        segment_name: &str,
        size: u64,
        base_addr: u64,
    ) -> StoreResult<Uuid> {
        if segment_name.is_empty() || size == 0 || base_addr == 0 {
            return Err(StoreError::InvalidParams(
                "mounted Memory segment requires non-empty name and non-zero base/size".to_string(),
            ));
        }
        let alignment = self.memory_segment_alignment as u64;
        if size % alignment != 0 {
            return Err(StoreError::InvalidParams(format!(
                "mounted Memory segment size must be aligned to Master requirement {alignment}"
            )));
        }
        let end = base_addr.checked_add(size).ok_or_else(|| {
            StoreError::InvalidParams("mounted Memory segment range overflows u64".to_string())
        })?;
        let overlaps_owned = self.owned_store_segments.iter().any(|segment| {
            ranges_overlap_u64(
                base_addr,
                end,
                segment.base_addr(),
                segment.base_addr().saturating_add(segment.size),
            )
        });
        let overlaps_external = self
            .mounted_external_segments
            .read()
            .values()
            .any(|segment| {
                ranges_overlap_u64(
                    base_addr,
                    end,
                    segment.base_addr,
                    segment.base_addr.saturating_add(segment.size),
                )
            });
        if overlaps_owned || overlaps_external {
            return Err(StoreError::InvalidParams(
                "mounted Memory segment overlaps an existing local segment".to_string(),
            ));
        }
        let transport_endpoint = self.local_transport_endpoint.clone();
        let expected_segment_id = mooncake_store_core::stable_memory_segment_id(
            self.client_id,
            segment_name,
            base_addr,
            size,
            &transport_endpoint,
            &self.protocol,
            &self.host_id,
        );
        let response = match self
            .master
            .mount_segment(self.rpc_request(proto::MountSegmentRequest {
                client_id: Some(self.client_id_proto()),
                segment_name: segment_name.to_string(),
                size,
                base_addr,
                te_endpoint: transport_endpoint.clone(),
                protocol: self.protocol.clone(),
                host_id: self.host_id.clone(),
            }))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let original = Self::rpc_status_to_error(status);
                if !super::lifecycle::unmount_confirms_segment_absent(
                    self.master
                        .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                            segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                            client_id: Some(self.client_id_proto()),
                        }))
                        .await,
                ) {
                    return Err(StoreError::SegmentMountOutcomeAmbiguous {
                        segment_id: expected_segment_id,
                        reason: original.to_string(),
                    });
                }
                return Err(original);
            }
        };
        let Some(segment_id) = response.segment_id.as_ref() else {
            if !super::lifecycle::unmount_confirms_segment_absent(
                self.master
                    .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                        segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                        client_id: Some(self.client_id_proto()),
                    }))
                    .await,
            ) {
                return Err(StoreError::SegmentMountOutcomeAmbiguous {
                    segment_id: expected_segment_id,
                    reason: "Master returned no segment UUID".to_string(),
                });
            }
            return Err(StoreError::Internal(
                "MountSegment response missing segment_id".to_string(),
            ));
        };
        let segment_id = Uuid::from_u64_pair(segment_id.high, segment_id.low);
        if segment_id != expected_segment_id {
            let returned_unmounted = super::lifecycle::unmount_confirms_segment_absent(
                self.master
                    .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                        segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
                        client_id: Some(self.client_id_proto()),
                    }))
                    .await,
            );
            let expected_unmounted = super::lifecycle::unmount_confirms_segment_absent(
                self.master
                    .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                        segment_id: Some(Self::uuid_to_proto_uuid(expected_segment_id)),
                        client_id: Some(self.client_id_proto()),
                    }))
                    .await,
            );
            if !returned_unmounted || !expected_unmounted {
                return Err(StoreError::SegmentMountOutcomeAmbiguous {
                    segment_id: expected_segment_id,
                    reason: format!("Master returned non-canonical segment UUID {segment_id}"),
                });
            }
            return Err(StoreError::Internal(format!(
                "MountSegment returned non-canonical segment UUID {segment_id}"
            )));
        }
        self.mounted_segment_ids
            .write()
            .insert(segment_id, segment_name.to_string());
        self.mounted_external_segments.write().insert(
            segment_id,
            super::MountedExternalSegment {
                segment_id,
                segment_name: segment_name.to_string(),
                size,
                base_addr,
                te_endpoint: transport_endpoint.clone(),
                protocol: self.protocol.clone(),
                host_id: self.host_id.clone(),
            },
        );
        // Register as a local endpoint for subsequent locality checks
        self.register_local_endpoint(&transport_endpoint);
        Ok(segment_id)
    }

    /// Unmount a previously mounted segment from the master.
    ///
    /// 从 master 卸载之前挂载的 segment。
    ///
    /// # Arguments
    /// - `segment_name` — the name of the segment to unmount.
    ///   要卸载的 segment 名称。
    /// - `grace_period_ms` — if > 0, schedules a graceful unmount where the
    ///   master waits for the grace period before actually removing the
    ///   segment. If 0, unmounts immediately.
    ///   如果 > 0，安排优雅卸载——master 在优雅期等待后再实际删除 segment。
    ///   如果为 0，立即卸载。
    ///
    /// C++ equivalent: `Client::UnmountSegment()`
    pub async fn unmount_segment(
        &mut self,
        segment_name: &str,
        grace_period_ms: u64,
    ) -> StoreResult<()> {
        let segment_ids = self
            .mounted_segment_ids
            .read()
            .iter()
            .filter_map(|(id, name)| (name == segment_name).then_some(*id))
            .collect::<Vec<_>>();
        let segment_id = match segment_ids.as_slice() {
            [] => return Err(StoreError::SegmentNotFound(segment_name.to_string())),
            [segment_id] => *segment_id,
            _ => {
                return Err(StoreError::InvalidParams(format!(
                    "segment name {segment_name:?} is ambiguous; unmount by UUID"
                )));
            }
        };
        self.unmount_segment_by_id(segment_id, grace_period_ms)
            .await
    }

    /// Unmount one exact segment UUID.
    pub async fn unmount_segment_by_id(
        &mut self,
        segment_id: Uuid,
        grace_period_ms: u64,
    ) -> StoreResult<()> {
        let segment_name = self
            .mounted_segment_ids
            .read()
            .get(&segment_id)
            .cloned()
            .ok_or_else(|| StoreError::SegmentNotFound(segment_id.to_string()))?;
        let segment_id_proto = Self::uuid_to_proto_uuid(segment_id);

        if grace_period_ms > 0 {
            self.master
                .graceful_unmount_segment(self.rpc_request(proto::GracefulUnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                    grace_period_ms,
                }))
                .await
                .map_err(Self::rpc_status_to_error)?;
            let grace = Duration::from_millis(grace_period_ms);
            let completion_budget = grace
                .checked_add(
                    self.rpc_request_timeout
                        .unwrap_or_else(|| Duration::from_secs(30)),
                )
                .and_then(|duration| duration.checked_add(Duration::from_secs(5)))
                .ok_or_else(|| {
                    StoreError::InvalidParams(
                        "graceful unmount completion deadline overflow".to_string(),
                    )
                })?;
            let deadline = tokio::time::Instant::now()
                .checked_add(completion_budget)
                .ok_or_else(|| {
                    StoreError::InvalidParams(
                        "graceful unmount completion deadline overflow".to_string(),
                    )
                })?;
            loop {
                let still_present = self
                    .get_segments_detail()
                    .await?
                    .iter()
                    .any(|segment| segment.segment_id == segment_id);
                if !still_present {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(StoreError::RpcTimeout(format!(
                        "graceful unmount completion was not observed for segment {segment_id}"
                    )));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        } else {
            self.master
                .unmount_segment(self.rpc_request(proto::UnmountSegmentRequest {
                    segment_id: Some(segment_id_proto),
                    client_id: Some(self.client_id_proto()),
                }))
                .await
                .map_err(Self::rpc_status_to_error)?;
        };
        let mut mounted = self.mounted_segment_ids.write();
        mounted.remove(&segment_id);
        let name_still_mounted = mounted.values().any(|name| name == &segment_name);
        drop(mounted);
        let removed_external = self.mounted_external_segments.write().remove(&segment_id);
        if let Some(mut registration) = self
            .mounted_owned_external_registrations
            .write()
            .remove(&segment_id)
        {
            self.engine.unregister_owned_memory(&mut registration)?;
        }
        let transport_endpoint = removed_external
            .as_ref()
            .map(|segment| segment.te_endpoint.clone())
            .or_else(|| {
                (self.protocol != "cxl")
                    .then(|| self.engine.get_local_ip_and_port().ok())
                    .flatten()
            })
            .unwrap_or_else(|| self.local_hostname.clone());
        let endpoint_still_mounted = self
            .mounted_external_segments
            .read()
            .values()
            .any(|segment| segment.te_endpoint == transport_endpoint)
            || self
                .owned_store_segments
                .iter()
                .any(|segment| segment.segment_id != segment_id);
        if !name_still_mounted && !endpoint_still_mounted {
            self.unregister_local_endpoint(&transport_endpoint);
        }
        Ok(())
    }

    /// Unmount a segment backed by an owner-bearing external registration.
    /// Client-owned capacity and raw external mounts are rejected.
    pub async fn unmount_owned_external_segment_by_id(
        &mut self,
        segment_id: Uuid,
        grace_period_ms: u64,
    ) -> StoreResult<()> {
        if !self
            .mounted_owned_external_registrations
            .read()
            .contains_key(&segment_id)
        {
            return Err(StoreError::SegmentNotFound(segment_id.to_string()));
        }
        self.unmount_segment_by_id(segment_id, grace_period_ms)
            .await
    }

    /// Allocate, register and mount a client-owned Store capacity.
    ///
    /// The requested size is split at `MC_MAX_MR_SIZE`; every returned UUID
    /// owns one independently registered allocation. Partial preparation or
    /// mount failure rolls back every chunk before returning.
    pub async fn allocate_and_mount_segments(&mut self, size: u64) -> StoreResult<Vec<Uuid>> {
        self.allocate_and_mount_segments_with_size(size)
            .await
            .map(|(segment_ids, _)| segment_ids)
    }

    /// Same as [`allocate_and_mount_segments`](Self::allocate_and_mount_segments),
    /// also returning the actual allocator-aligned capacity.
    pub async fn allocate_and_mount_segments_with_size(
        &mut self,
        size: u64,
    ) -> StoreResult<(Vec<Uuid>, u64)> {
        if size == 0 {
            return Err(StoreError::InvalidParams(
                "allocated Store segment size must be greater than zero".to_string(),
            ));
        }
        if self.protocol == "cxl" {
            return Err(StoreError::InvalidParams(
                "dynamic owned allocation is not supported for CXL mappings".to_string(),
            ));
        }
        let engine = self.engine.required_arc().map_err(StoreError::from)?;
        let max_mr_size = Self::resolve_max_mr_size(
            &self.protocol,
            size,
            std::env::var("MC_MAX_MR_SIZE").ok().as_deref(),
        )?;
        let alignment = self.memory_segment_alignment as u64;
        let aligned_size = size
            .checked_add(alignment.saturating_sub(1))
            .map(|value| value / alignment * alignment)
            .ok_or_else(|| {
                StoreError::InvalidParams(
                    "allocated Store segment size overflows alignment".to_string(),
                )
            })?;
        let chunks = Self::split_segment_capacity_aligned(aligned_size, max_mr_size, alignment)?;
        let mut prepared = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let chunk = usize::try_from(chunk).map_err(|_| {
                StoreError::InvalidParams(
                    "Store segment chunk exceeds addressable memory".to_string(),
                )
            })?;
            let buffer = crate::memory_ffi::allocate_store_segment(
                chunk,
                &self.protocol,
                self.memory_segment_alignment,
            )
            .map_err(|error| {
                StoreError::Internal(format!(
                    "failed to populate allocated Store segment: {error}"
                ))
            })?;
            if let Err(error) =
                crate::memory_ffi::register_local_memory(&engine, &buffer, "cpu:0", true)
            {
                for buffer in prepared {
                    super::lifecycle::release_failed_store_segment(&engine, buffer);
                }
                return Err(error);
            }
            prepared.push(buffer);
        }

        let segment_name = self.local_hostname.clone();
        let mut mounted: Vec<super::OwnedStoreSegment> = Vec::new();
        let mut remaining = prepared.into_iter();
        while let Some(buffer) = remaining.next() {
            let mount = self
                .mount_segment_with_id(&segment_name, buffer.len() as u64, buffer.as_ptr() as u64)
                .await;
            let segment_id = match mount {
                Ok(segment_id) => segment_id,
                Err(error) => {
                    let retain_current_owner =
                        matches!(&error, StoreError::SegmentMountOutcomeAmbiguous { .. });
                    let mut endpoint_safe_to_remove = !retain_current_owner;
                    for segment in mounted.drain(..) {
                        if self
                            .unmount_segment_by_id(segment.segment_id, 0)
                            .await
                            .is_ok()
                        {
                            super::lifecycle::release_failed_store_segment(&engine, segment.buffer);
                        } else {
                            endpoint_safe_to_remove = false;
                            tracing::error!(
                                segment_id = %segment.segment_id,
                                "leaking allocated Store segment because rollback unmount failed"
                            );
                            std::mem::forget(segment);
                        }
                    }
                    let mut buffers = Vec::new();
                    if retain_current_owner {
                        tracing::error!(
                            %error,
                            "leaking allocated Store segment because mount rollback was ambiguous"
                        );
                        std::mem::forget(buffer);
                    } else {
                        buffers.push(buffer);
                    }
                    buffers.extend(remaining);
                    if endpoint_safe_to_remove {
                        super::lifecycle::release_failed_store_segments(
                            &engine,
                            &segment_name,
                            buffers,
                        );
                    } else {
                        for buffer in buffers {
                            super::lifecycle::release_failed_store_segment(&engine, buffer);
                        }
                    }
                    return Err(error);
                }
            };
            mounted.push(super::OwnedStoreSegment {
                segment_id,
                segment_name: self.local_hostname.clone(),
                size: buffer.len() as u64,
                buffer,
            });
            self.mounted_external_segments.write().remove(&segment_id);
        }
        let ids = mounted
            .iter()
            .map(|segment| segment.segment_id)
            .collect::<Vec<_>>();
        self.owned_store_segments.extend(mounted);
        Ok((ids, aligned_size))
    }

    /// Unmount and free exact client-owned segment UUIDs.
    ///
    /// For graceful requests the owner is retained until `GetSegmentsDetail`
    /// proves that the exact UUID has disappeared from Master.
    pub async fn unmount_and_free_segments(
        &mut self,
        segment_ids: &[Uuid],
        grace_period_ms: u64,
    ) -> StoreResult<()> {
        let requested = segment_ids.iter().copied().collect::<HashSet<_>>();
        if requested.len() != segment_ids.len() {
            return Err(StoreError::InvalidParams(
                "duplicate segment UUID in unmount-and-free request".to_string(),
            ));
        }
        for segment_id in &requested {
            if !self
                .owned_store_segments
                .iter()
                .any(|segment| segment.segment_id == *segment_id)
            {
                return Err(StoreError::SegmentNotFound(segment_id.to_string()));
            }
        }

        let engine = self.engine.required_arc().map_err(StoreError::from)?;
        for segment_id in segment_ids {
            self.unmount_segment_by_id(*segment_id, grace_period_ms)
                .await?;
            let position = self
                .owned_store_segments
                .iter()
                .position(|segment| segment.segment_id == *segment_id)
                .ok_or_else(|| StoreError::SegmentNotFound(segment_id.to_string()))?;
            let mut segment = self.owned_store_segments.swap_remove(position);
            if let Err(error) = crate::memory_ffi::unregister_local_memory(&engine, &segment.buffer)
            {
                tracing::error!(
                    %error,
                    %segment_id,
                    "leaking unmounted Store segment because TE unregister failed"
                );
                std::mem::forget(segment);
                return Err(error);
            }
            if !segment.buffer.release() {
                return Err(StoreError::Internal(format!(
                    "CUDA host unregister failed for Store segment {segment_id}; allocation leaked"
                )));
            }
        }
        Ok(())
    }
}

fn ranges_overlap_u64(left_start: u64, left_end: u64, right_start: u64, right_end: u64) -> bool {
    left_start < right_end && right_start < left_end
}

async fn finalize_offload_publication(
    storage: AttachedLocalStorage,
    committed_storage_keys: Vec<String>,
    notification_result: StoreResult<()>,
) -> StoreResult<()> {
    let Err(error) = notification_result else {
        return Ok(());
    };

    tokio::task::spawn_blocking(move || {
        for storage_key in committed_storage_keys {
            if let Err(cleanup_error) = storage.delete_object(&storage_key) {
                tracing::warn!(target: "storage_debug", %storage_key, %cleanup_error, "offload: failed to roll back unpublished local object");
            }
        }
    })
    .await
    .map_err(|join_error| StoreError::Internal(join_error.to_string()))?;
    Err(error)
}

fn failed_offload_metadata() -> proto::StorageObjectMetadata {
    proto::StorageObjectMetadata {
        bucket_id: -1,
        offset: 0,
        key_size: 0,
        data_size: -1,
        transport_endpoint: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_storage_backend::{
        AttachedLocalStorage, BucketEvictionPolicy, BucketStorageBackend, BucketStorageConfig,
        LocalStorageBackend, LocalStorageConfig, OffsetAllocatorConfig,
        OffsetAllocatorStorageBackend, OffsetEvictionPolicy,
    };
    use mooncake_store_master::proto as master_proto;
    use mooncake_store_master::proto::master_service_server::{MasterService, MasterServiceServer};
    use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tonic::transport::{Channel, Server};

    #[test]
    fn unpolled_cleanup_future_retains_unconfirmed_resource() {
        static DROPPED: AtomicBool = AtomicBool::new(false);
        struct DropProbe;
        impl Drop for DropProbe {
            fn drop(&mut self) {
                DROPPED.store(true, Ordering::SeqCst);
            }
        }

        DROPPED.store(false, Ordering::SeqCst);
        let resource = UnconfirmedResource(Some(DropProbe));
        let cleanup = async move {
            tokio::task::yield_now().await;
            resource.confirmed_absent();
        };
        drop(cleanup);

        assert!(!DROPPED.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cpp_parity_offset_completion_failure_propagates_and_rolls_back() {
        let root = tempfile::tempdir().unwrap();
        let offset = Arc::new(OffsetAllocatorStorageBackend::new(OffsetAllocatorConfig {
            root_dir: root.path().to_path_buf(),
            fsdir: "offset-completion-failure".to_string(),
            eviction_policy: OffsetEvictionPolicy::None,
            quota_bytes: 1024 * 1024,
            total_keys_limit: 100,
            high_ratio: 0.90,
            low_ratio: 0.80,
            keys_high_ratio: 0.90,
            keys_low_ratio: 0.80,
            max_evict_per_offload: 16,
            fallback_evict_batch: 2,
        }));
        offset.init().unwrap();
        let storage = AttachedLocalStorage::OffsetAllocator(Arc::clone(&offset));
        let storage_key = local_storage_key("tenant-a", "key");
        let pending = storage.prepare_write(&storage_key, 5).unwrap();
        storage
            .commit_write(&storage_key, b"value", pending, Uuid::new_v4())
            .unwrap();
        assert!(offset.exists(&storage_key));

        let result = finalize_offload_publication(
            storage,
            vec![storage_key.clone()],
            Err(StoreError::Internal(
                "injected completion failure".to_string(),
            )),
        )
        .await;

        assert!(
            matches!(result, Err(StoreError::Internal(message)) if message == "injected completion failure")
        );
        assert!(!offset.exists(&storage_key));
        assert_eq!(offset.space_usage().0, 0);
        assert!(offset.scan_meta().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cpp_parity_bucket_batch_completion_failure_is_atomic() {
        let root = tempfile::tempdir().unwrap();
        let config = BucketStorageConfig {
            root_dir: root.path().to_path_buf(),
            fsdir: "bucket-batch-completion-failure".to_string(),
            bucket_size_limit: 8 * 1024,
            bucket_keys_limit: 10,
            eviction_policy: BucketEvictionPolicy::Fifo,
            quota_bytes: 64 * 1024,
            total_keys_limit: 100,
        };
        let backend_dir = config.root_dir.join(&config.fsdir);
        let bucket = Arc::new(BucketStorageBackend::new(config));
        let storage = AttachedLocalStorage::Bucket(Arc::clone(&bucket));
        storage.storage_id().unwrap();
        let baseline_usage = storage.space_usage();
        let fixtures = [
            (local_storage_key("tenant-a", "rollback-key-0"), b"value-0"),
            (local_storage_key("tenant-a", "rollback-key-1"), b"value-1"),
            (local_storage_key("tenant-a", "rollback-key-2"), b"value-2"),
        ];

        let mut committed_storage_keys = Vec::new();
        for (storage_key, value) in &fixtures {
            let pending = storage
                .prepare_write(storage_key, value.len() as u64)
                .unwrap();
            storage
                .commit_write(storage_key, *value, pending, Uuid::new_v4())
                .unwrap();
            committed_storage_keys.push(storage_key.clone());
        }
        assert_eq!(storage.scan_records().unwrap().len(), 3);
        assert_ne!(storage.space_usage(), baseline_usage);

        let result = finalize_offload_publication(
            storage.clone(),
            committed_storage_keys,
            Err(StoreError::Internal(
                "injected completion failure".to_string(),
            )),
        )
        .await;

        assert!(
            matches!(result, Err(StoreError::Internal(message)) if message == "injected completion failure")
        );
        for (storage_key, _) in &fixtures {
            assert!(matches!(
                storage.read_object(storage_key),
                Err(StoreError::KeyNotFound(key)) if key == *storage_key
            ));
        }
        assert_eq!(storage.space_usage(), baseline_usage);
        assert!(storage.scan_records().unwrap().is_empty());
        assert_eq!(
            std::fs::read_dir(backend_dir.join("buckets"))
                .unwrap()
                .count(),
            0
        );
        assert_eq!(
            std::fs::read_dir(backend_dir.join(".mooncake-tmp"))
                .unwrap()
                .count(),
            0
        );
        assert!(
            !backend_dir
                .join(".mooncake-accepted-evictions.json")
                .exists()
        );
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_empty_queue_is_noop() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "empty-promotion-queue".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        assert!(disk.scan_meta().unwrap().is_empty());
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(Arc::clone(&disk));
        client.mount_local_disk_segment(false).await.unwrap();

        assert!(client.local_storage.is_some());
        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        assert_eq!(client.promote_objects().await.unwrap(), 0);
        assert!(disk.scan_meta().unwrap().is_empty());
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 1);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 0);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 0);

        client.tear_down_all().await.unwrap();
        server.abort();
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_missing_session_is_benign() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "missing-promotion-session".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(Arc::clone(&disk));

        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        assert_eq!(client.promote_objects().await.unwrap(), 0);
        assert!(disk.scan_meta().unwrap().is_empty());
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 1);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 0);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 0);

        client.tear_down_all().await.unwrap();
        server.abort();
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_hard_heartbeat_error_propagates() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "hard-promotion-heartbeat-error".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(Arc::clone(&disk));

        let storage_uuid = disk.storage_id().unwrap();
        let (storage_high, storage_low) = storage_uuid.as_u64_pair();
        let recovery_uuid = Uuid::new_v4();
        let (recovery_high, recovery_low) = recovery_uuid.as_u64_pair();
        let request = client.rpc_request(proto::MountLocalDiskSegmentRequest {
            client_id: Some(client.client_id_proto()),
            enable_offloading: false,
            storage_id: Some(proto::Uuid {
                high: storage_high,
                low: storage_low,
            }),
            recovery_complete: false,
            recovery_session_id: Some(proto::Uuid {
                high: recovery_high,
                low: recovery_low,
            }),
        });
        client
            .master
            .mount_local_disk_segment(request)
            .await
            .unwrap();

        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let result = client.promote_objects().await;
        assert!(
            matches!(&result, Err(StoreError::Internal(message)) if message.contains("inventory recovery is not complete")),
            "unexpected heartbeat result: {result:?}"
        );
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 1);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 0);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 0);
        assert!(disk.scan_meta().unwrap().is_empty());

        drop(client);
        server.abort();
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_non_positive_size_is_skipped() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "non-positive-promotion-size".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        disk.write_object(&local_storage_key("tenant-a", "good"), b"data")
            .unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(Arc::clone(&disk));

        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let promoted = client
            .process_promotion_tasks(vec![
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "negative".to_string(),
                    size: -1,
                },
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "zero".to_string(),
                    size: 0,
                },
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "good".to_string(),
                    size: 4,
                },
            ])
            .await
            .unwrap();

        assert_eq!(promoted, 0);
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 0);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 1);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 1);
        assert_eq!(
            disk.read_object(&local_storage_key("tenant-a", "good"))
                .unwrap(),
            b"data"
        );

        drop(client);
        server.abort();
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_alloc_failure_skips_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "promotion-alloc-failure".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(disk);

        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let promoted = client
            .process_promotion_tasks(vec![
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "first".to_string(),
                    size: 1024,
                },
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "second".to_string(),
                    size: 1024,
                },
            ])
            .await
            .unwrap();

        assert_eq!(promoted, 0);
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 2);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 0);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 2);

        drop(client);
        server.abort();
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_batch_load_failure_releases_and_continues() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "promotion-batch-load-failure".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(disk);

        let _alloc_success =
            inject_promotion_test_alloc_success(client.client_id, "missing-after-alloc", 1024);
        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let promoted = client
            .process_promotion_tasks(vec![
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "missing-after-alloc".to_string(),
                    size: 1024,
                },
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "later-task".to_string(),
                    size: 1024,
                },
            ])
            .await
            .unwrap();

        assert_eq!(promoted, 0);
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 2);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 1);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 2);

        drop(client);
        server.abort();
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_transfer_failure_releases_and_continues() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "promotion-transfer-failure".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        let payload = vec![0x5a; 1024];
        disk.write_object(&local_storage_key("tenant-a", "transfer-fails"), &payload)
            .unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(disk);

        let _alloc_success =
            inject_promotion_test_alloc_success(client.client_id, "transfer-fails", 1024);
        let _transfer_failure =
            inject_promotion_test_transfer_failure(client.client_id, "tenant-a", "transfer-fails");
        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let promoted = client
            .process_promotion_tasks(vec![
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "transfer-fails".to_string(),
                    size: 1024,
                },
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "later-task".to_string(),
                    size: 1024,
                },
            ])
            .await
            .unwrap();

        assert_eq!(promoted, 0);
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 2);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 1);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 1);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 2);

        drop(client);
        server.abort();
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_notify_failure_releases_and_continues() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "promotion-notify-failure".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        let payload = vec![0x6b; 1024];
        disk.write_object(&local_storage_key("tenant-a", "notify-fails"), &payload)
            .unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(disk);

        let _alloc_success =
            inject_promotion_test_alloc_success(client.client_id, "notify-fails", 1024);
        let _transfer_success =
            inject_promotion_test_transfer_success(client.client_id, "tenant-a", "notify-fails");
        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let promoted = client
            .process_promotion_tasks(vec![
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "notify-fails".to_string(),
                    size: 1024,
                },
                PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: "later-task".to_string(),
                    size: 1024,
                },
            ])
            .await
            .unwrap();

        assert_eq!(promoted, 0);
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 2);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 1);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 1);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 1);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 2);

        drop(client);
        server.abort();
    }

    #[cfg(feature = "link-native")]
    async fn run_promotion_failure_matrix() -> (usize, Arc<PromotionTestCallCounts>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "promotion-failure-matrix".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        disk.write_object(
            &local_storage_key("tenant-a", "transfer-fails"),
            &vec![0x7c; 1024],
        )
        .unwrap();
        disk.write_object(
            &local_storage_key("tenant-a", "notify-fails"),
            &vec![0x8d; 1024],
        )
        .unwrap();

        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(disk);

        let _load_alloc = inject_promotion_test_alloc_success(client.client_id, "load-fails", 1024);
        let _transfer_alloc =
            inject_promotion_test_alloc_success(client.client_id, "transfer-fails", 1024);
        let _notify_alloc =
            inject_promotion_test_alloc_success(client.client_id, "notify-fails", 1024);
        let _transfer_failure =
            inject_promotion_test_transfer_failure(client.client_id, "tenant-a", "transfer-fails");
        let _transfer_success =
            inject_promotion_test_transfer_success(client.client_id, "tenant-a", "notify-fails");
        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let promoted = client
            .process_promotion_tasks(
                [
                    "alloc-fails",
                    "load-fails",
                    "transfer-fails",
                    "notify-fails",
                    "after-failures",
                ]
                .into_iter()
                .map(|key| PromotionTaskItem {
                    tenant_id: "tenant-a".to_string(),
                    key: key.to_string(),
                    size: 1024,
                })
                .collect(),
            )
            .await
            .unwrap();

        drop(client);
        server.abort();
        (promoted, counts)
    }

    #[cfg(feature = "link-native")]
    async fn run_single_promotion_alloc_failure() -> (usize, Arc<PromotionTestCallCounts>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let disk_root = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalStorageBackend::new_persistent(LocalStorageConfig {
            root_dir: disk_root.path().to_path_buf(),
            fsdir: "promotion-single-alloc-failure".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        }));
        disk.init().unwrap();
        let mut client = MooncakeClient::create(
            &master_address.to_string(),
            "P2PHANDSHAKE",
            "127.0.0.1",
            "tcp",
            "",
            0,
            8 * 1024 * 1024,
        )
        .await
        .unwrap()
        .with_local_storage_backend(disk);

        let (counts, _observer) = observe_promotion_test_calls(client.client_id);
        let promoted = client
            .process_promotion_tasks(vec![PromotionTaskItem {
                tenant_id: "tenant-a".to_string(),
                key: "alloc-fails".to_string(),
                size: 1024,
            }])
            .await
            .unwrap();

        drop(client);
        server.abort();
        (promoted, counts)
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_per_key_failures_are_independent() {
        let (promoted, counts) = run_promotion_failure_matrix().await;

        assert_eq!(promoted, 0);
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 5);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 3);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 2);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 1);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 5);
        assert_eq!(
            counts.notify_failure_keys.lock().unwrap().last(),
            Some(&("tenant-a".to_string(), "after-failures".to_string()))
        );
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_alloc_failure_notifies_master() {
        let (promoted, counts) = run_single_promotion_alloc_failure().await;

        assert_eq!(promoted, 0);
        assert_eq!(counts.heartbeat.load(Ordering::Relaxed), 0);
        assert_eq!(counts.alloc.load(Ordering::Relaxed), 1);
        assert_eq!(counts.disk_read.load(Ordering::Relaxed), 0);
        assert_eq!(counts.transfer_write.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_success.load(Ordering::Relaxed), 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 1);
        assert_eq!(
            *counts.notify_failure_keys.lock().unwrap(),
            vec![("tenant-a".to_string(), "alloc-fails".to_string())]
        );
    }

    #[cfg(feature = "link-native")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpp_parity_file_storage_promotion_post_alloc_failures_all_notify_master() {
        let (promoted, counts) = run_promotion_failure_matrix().await;

        assert_eq!(promoted, 0);
        assert_eq!(counts.notify_failure.load(Ordering::Relaxed), 5);
        assert_eq!(
            *counts.notify_failure_keys.lock().unwrap(),
            vec![
                ("tenant-a".to_string(), "alloc-fails".to_string()),
                ("tenant-a".to_string(), "load-fails".to_string()),
                ("tenant-a".to_string(), "transfer-fails".to_string()),
                ("tenant-a".to_string(), "notify-fails".to_string()),
                ("tenant-a".to_string(), "after-failures".to_string()),
            ]
        );
    }

    #[derive(Default)]
    struct ScriptedEvictionNotifier {
        outcomes: BTreeMap<String, VecDeque<bool>>,
        calls: Vec<(String, Vec<String>)>,
    }

    #[derive(Default)]
    struct StatusEvictionNotifier {
        statuses: VecDeque<Vec<i32>>,
        calls: Vec<Vec<String>>,
    }

    #[derive(Default)]
    struct RecordingSuccessEvictionNotifier {
        calls: Vec<(String, Vec<String>, i32)>,
    }

    #[async_trait::async_trait]
    impl DiskEvictionNotifier for ScriptedEvictionNotifier {
        async fn notify_disk_eviction(
            &mut self,
            tenant_id: &str,
            keys: &[String],
            _replica_type: i32,
        ) -> StoreResult<Vec<i32>> {
            self.calls.push((tenant_id.to_string(), keys.to_vec()));
            if self
                .outcomes
                .get_mut(tenant_id)
                .and_then(VecDeque::pop_front)
                .unwrap_or(false)
            {
                Ok(vec![0; keys.len()])
            } else {
                Err(StoreError::ServiceUnavailable)
            }
        }
    }

    #[async_trait::async_trait]
    impl DiskEvictionNotifier for StatusEvictionNotifier {
        async fn notify_disk_eviction(
            &mut self,
            _tenant_id: &str,
            keys: &[String],
            _replica_type: i32,
        ) -> StoreResult<Vec<i32>> {
            self.calls.push(keys.to_vec());
            Ok(self.statuses.pop_front().unwrap_or_default())
        }
    }

    #[async_trait::async_trait]
    impl DiskEvictionNotifier for RecordingSuccessEvictionNotifier {
        async fn notify_disk_eviction(
            &mut self,
            tenant_id: &str,
            keys: &[String],
            replica_type: i32,
        ) -> StoreResult<Vec<i32>> {
            self.calls
                .push((tenant_id.to_string(), keys.to_vec(), replica_type));
            Ok(vec![0; keys.len()])
        }
    }

    #[test]
    fn failed_offload_metadata_uses_negative_size_sentinel() {
        let metadata = failed_offload_metadata();
        assert_eq!(metadata.data_size, -1);
        assert_eq!(metadata.bucket_id, -1);
    }

    #[tokio::test]
    async fn eviction_notification_keeps_tenant_a_accepted_when_tenant_b_fails() {
        let tenant_a_key = local_storage_key("tenant-a", "key-a");
        let tenant_b_key = local_storage_key("tenant-b", "key-b");
        let mut notifier = ScriptedEvictionNotifier {
            outcomes: BTreeMap::from([
                ("tenant-a".to_string(), VecDeque::from([true])),
                (
                    "tenant-b".to_string(),
                    VecDeque::from([false, false, false]),
                ),
            ]),
            calls: Vec::new(),
        };

        let error = notify_evicted_disk_replicas_with(
            &mut notifier,
            &[tenant_b_key, tenant_a_key.clone()],
            proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        )
        .await
        .unwrap_err();

        assert_eq!(error.accepted_storage_keys, HashSet::from([tenant_a_key]));
        assert!(matches!(error.source, StoreError::ServiceUnavailable));
        assert_eq!(
            notifier
                .calls
                .iter()
                .map(|(tenant_id, _)| tenant_id.as_str())
                .collect::<Vec<_>>(),
            ["tenant-a", "tenant-b", "tenant-b", "tenant-b"]
        );
    }

    #[tokio::test]
    async fn eviction_notification_returns_all_keys_after_successful_retry() {
        let tenant_a_key = local_storage_key("tenant-a", "key-a");
        let tenant_b_key = local_storage_key("tenant-b", "key-b");
        let mut notifier = ScriptedEvictionNotifier {
            outcomes: BTreeMap::from([
                ("tenant-a".to_string(), VecDeque::from([false, true])),
                ("tenant-b".to_string(), VecDeque::from([true])),
            ]),
            calls: Vec::new(),
        };

        let accepted = notify_evicted_disk_replicas_with(
            &mut notifier,
            &[tenant_b_key.clone(), tenant_a_key.clone()],
            proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        )
        .await
        .unwrap();

        assert_eq!(accepted, HashSet::from([tenant_a_key, tenant_b_key]));
        assert_eq!(
            notifier
                .calls
                .iter()
                .map(|(tenant_id, _)| tenant_id.as_str())
                .collect::<Vec<_>>(),
            ["tenant-a", "tenant-a", "tenant-b"]
        );
    }

    #[tokio::test]
    async fn cpp_parity_notify_evicted_disk_replicas_routes_same_key_by_tenant() {
        let tenant_a_key = local_storage_key("tenant-a", "shared-key");
        let tenant_b_key = local_storage_key("tenant-b", "shared-key");
        let mut notifier = ScriptedEvictionNotifier {
            outcomes: BTreeMap::from([
                ("tenant-a".to_string(), VecDeque::from([true])),
                ("tenant-b".to_string(), VecDeque::from([true])),
            ]),
            calls: Vec::new(),
        };

        let accepted = notify_evicted_disk_replicas_with(
            &mut notifier,
            &[tenant_b_key.clone(), tenant_a_key.clone()],
            proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        )
        .await
        .unwrap();

        assert_eq!(accepted, HashSet::from([tenant_a_key, tenant_b_key]));
        assert_eq!(
            notifier.calls,
            [
                ("tenant-a".to_string(), vec!["shared-key".to_string()]),
                ("tenant-b".to_string(), vec!["shared-key".to_string()]),
            ]
        );
    }

    #[tokio::test]
    async fn eviction_notification_retries_only_unaccepted_keys_within_tenant() {
        let accepted_key = local_storage_key("tenant-a", "accepted");
        let rejected_key = local_storage_key("tenant-a", "rejected");
        let mut notifier = StatusEvictionNotifier {
            statuses: VecDeque::from([vec![0, -6], vec![-6], vec![-6]]),
            calls: Vec::new(),
        };

        let error = notify_evicted_disk_replicas_with(
            &mut notifier,
            &[accepted_key.clone(), rejected_key],
            proto::replica_descriptor::ReplicaType::Disk as i32,
        )
        .await
        .unwrap_err();

        assert_eq!(error.accepted_storage_keys, HashSet::from([accepted_key]));
        assert_eq!(
            notifier
                .calls
                .iter()
                .map(|keys| keys.len())
                .collect::<Vec<_>>(),
            [2, 1, 1]
        );
    }

    #[tokio::test]
    async fn partial_finalize_commits_only_the_accepted_file_per_key_victim() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalStorageBackend::new_ephemeral(LocalStorageConfig {
            root_dir: temp.path().to_path_buf(),
            fsdir: "partial-finalize".to_string(),
            enable_eviction: true,
            quota_bytes: 200,
        }));
        backend.init().unwrap();

        let tenant_a_key = local_storage_key("tenant-a", "key-a");
        let tenant_b_key = local_storage_key("tenant-b", "key-b");
        let target_key = local_storage_key("tenant-c", "target");
        backend.write_object(&tenant_a_key, &[1_u8; 60]).unwrap();
        backend.write_object(&tenant_b_key, &[2_u8; 20]).unwrap();

        let storage = AttachedLocalStorage::FilePerKey(backend.clone());
        let pending = storage.prepare_write(&target_key, 100).unwrap();
        assert_eq!(pending.keys(), [tenant_a_key.clone(), tenant_b_key.clone()]);

        let accepted = HashSet::from([tenant_a_key.clone()]);
        assert_eq!(
            finalize_partially_accepted_eviction(storage, pending, &accepted)
                .await
                .unwrap(),
            1
        );

        assert!(!backend.exists(&tenant_a_key));
        assert!(backend.exists(&tenant_b_key));
        assert!(!backend.exists(&target_key));
        assert_eq!(backend.read_object(&tenant_b_key).unwrap(), vec![2_u8; 20]);

        backend.write_object(&tenant_b_key, b"replacement").unwrap();
        let target_retry = backend.prepare_write(&target_key, 1).unwrap();
        backend.rollback_eviction(target_retry);
    }

    #[test]
    fn recovered_storage_keys_require_canonical_tenant_scope() {
        let storage_key = local_storage_key("租户-a", "object");
        assert_eq!(
            parse_recovered_local_storage_key(&storage_key).unwrap(),
            ("租户-a", "object")
        );

        for invalid in [
            "legacy-key",
            "v1::defaultkey",
            "v1:07:defaultkey",
            "v1:99:x",
            "v1:0:key",
            "v1:7:_tenantkey",
            "v1:3:a\nbkey",
            "v1:7:default",
            "v1:1:ékey",
        ] {
            assert!(
                parse_recovered_local_storage_key(invalid).is_err(),
                "{invalid:?} should be rejected"
            );
        }
    }

    #[test]
    fn recovered_records_are_preflighted_and_sorted_before_publication() {
        let records = prepare_recovered_local_disk_records(vec![
            (local_storage_key("tenant-b", "key-b"), 4),
            (local_storage_key("tenant-a", "key-a"), 2),
        ])
        .unwrap();
        assert_eq!(records[0].task.tenant_id, "tenant-a");
        assert_eq!(records[0].task.key, "key-a");
        assert_eq!(records[0].task.size, 2);
        assert_eq!(records[0].key_size, 5);
        assert_eq!(records[1].task.tenant_id, "tenant-b");

        let error = prepare_recovered_local_disk_records(vec![(
            local_storage_key("tenant-a", "too-large"),
            u64::MAX,
        )])
        .unwrap_err();
        assert!(matches!(error, StoreError::InvalidParams(_)));
    }

    #[test]
    fn empty_recovery_builds_no_notifications_and_requires_no_endpoint() {
        assert!(
            prepare_recovered_local_disk_records(Vec::new())
                .unwrap()
                .is_empty()
        );
        let batch = recovered_local_disk_notification_batch(&[], "").unwrap();
        assert!(batch.tasks.is_empty());
        assert!(batch.metadatas.is_empty());
        assert_eq!(
            recovered_local_disk_record_batches(&[], 2).unwrap().count(),
            0
        );
    }

    #[test]
    fn recovery_server_decision_is_fail_closed_and_idempotent() {
        assert_eq!(
            recovered_local_disk_server_action(0, false, false).unwrap(),
            None,
            "empty storage must not force a Transfer Engine or server"
        );
        assert!(
            recovered_local_disk_server_action(1, false, false).is_err(),
            "rpc_only recovery must not publish unreachable records"
        );
        assert_eq!(
            recovered_local_disk_server_action(1, true, false).unwrap(),
            Some(RecoveredLocalDiskServerAction::Start)
        );
        assert_eq!(
            recovered_local_disk_server_action(1, true, true).unwrap(),
            Some(RecoveredLocalDiskServerAction::Reuse),
            "repeated recovery must reuse the running server"
        );
    }

    #[test]
    fn recovered_notifications_are_batched_and_repeatable() {
        let records = prepare_recovered_local_disk_records(vec![
            (local_storage_key("tenant-b", "key-b"), 4),
            (local_storage_key("tenant-a", "key-a"), 2),
            (local_storage_key("tenant-c", "key-c"), 6),
        ])
        .unwrap();

        let build_batches = || {
            recovered_local_disk_record_batches(&records, 2)
                .unwrap()
                .map(|records| recovered_local_disk_notification_batch(records, "127.0.0.1:4321"))
                .collect::<StoreResult<Vec<_>>>()
                .unwrap()
        };
        let first = build_batches();
        let repeated = build_batches();

        assert_eq!(first, repeated);
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].tasks.len(), 2);
        assert_eq!(first[1].tasks.len(), 1);
        assert!(
            first
                .iter()
                .flat_map(|batch| &batch.metadatas)
                .all(|metadata| metadata.transport_endpoint == "127.0.0.1:4321")
        );
        assert!(
            recovered_local_disk_notification_batch(&records, "").is_err(),
            "non-empty recovery must not publish an unreachable endpoint"
        );
        assert!(recovered_local_disk_record_batches(&records, 0).is_err());
    }

    #[tokio::test]
    async fn cpp_parity_storage_backend_test_cpp_storagebackendtest_adaptorwatermarkevictionnotifiesrecoveredkeysafterrestart_567ce404()
     {
        let root = tempfile::tempdir().unwrap();
        let config = LocalStorageConfig {
            root_dir: root.path().to_path_buf(),
            fsdir: "file-per-key-watermark-restart".to_string(),
            enable_eviction: true,
            quota_bytes: 4096,
        };
        let expected_values = [
            ("restart_key_1", vec![b'a'; 512]),
            ("restart_key_2", vec![b'b'; 512]),
            ("restart_key_3", vec![b'c'; 512]),
        ];
        {
            let backend = LocalStorageBackend::new_persistent(config.clone());
            backend.init().unwrap();
            for (key, value) in &expected_values {
                backend
                    .write_object(&local_storage_key("default", key), value)
                    .unwrap();
            }
        }

        let restarted = LocalStorageBackend::new_persistent(config);
        restarted.init().unwrap();
        for (key, expected) in &expected_values {
            assert_eq!(
                restarted
                    .read_object(&local_storage_key("default", key))
                    .unwrap(),
                *expected
            );
        }
        assert_eq!(restarted.scan_records().unwrap().len(), 3);

        let pending = restarted
            .prepare_watermark_eviction(1e-12, 0.5e-12)
            .unwrap();
        let storage_keys = pending.keys();
        let mut returned_keys = storage_keys
            .iter()
            .map(|storage_key| {
                parse_recovered_local_storage_key(storage_key)
                    .unwrap()
                    .1
                    .to_string()
            })
            .collect::<Vec<_>>();
        returned_keys.sort();
        assert_eq!(
            returned_keys,
            ["restart_key_1", "restart_key_2", "restart_key_3"]
        );
        let mut notifier = RecordingSuccessEvictionNotifier::default();
        let accepted_storage_keys = notify_evicted_disk_replicas_with(
            &mut notifier,
            &storage_keys,
            proto::replica_descriptor::ReplicaType::LocalDisk as i32,
        )
        .await
        .unwrap();
        assert_eq!(
            accepted_storage_keys,
            storage_keys.iter().cloned().collect()
        );
        assert_eq!(
            notifier.calls,
            [(
                "default".to_string(),
                vec![
                    "restart_key_1".to_string(),
                    "restart_key_2".to_string(),
                    "restart_key_3".to_string(),
                ],
                proto::replica_descriptor::ReplicaType::LocalDisk as i32,
            )]
        );
        restarted.commit_eviction(pending).unwrap();
        assert!(restarted.scan_records().unwrap().is_empty());
    }

    async fn mount_test_local_disk(
        master: &mut proto::master_service_client::MasterServiceClient<Channel>,
        client_id: &proto::Uuid,
        storage_id: &proto::Uuid,
        recovery_session_id: &proto::Uuid,
        enable_offloading: bool,
        recovery_complete: bool,
    ) {
        master
            .mount_local_disk_segment(proto::MountLocalDiskSegmentRequest {
                client_id: Some(client_id.clone()),
                enable_offloading,
                storage_id: Some(storage_id.clone()),
                recovery_complete,
                recovery_session_id: Some(recovery_session_id.clone()),
            })
            .await
            .unwrap();
    }

    async fn publish_test_recovered_records(
        master: &mut proto::master_service_client::MasterServiceClient<Channel>,
        client_id: &proto::Uuid,
        records: &[RecoveredLocalDiskRecord],
        transport_endpoint: &str,
        recovery_session_id: &proto::Uuid,
    ) {
        for records in
            recovered_local_disk_record_batches(records, RECOVERED_LOCAL_DISK_NOTIFY_BATCH_SIZE)
                .unwrap()
        {
            let batch =
                recovered_local_disk_notification_batch(records, transport_endpoint).unwrap();
            let keys = batch.tasks.iter().map(|task| task.key.clone()).collect();
            master
                .notify_offload_success(proto::NotifyOffloadSuccessRequest {
                    client_id: Some(client_id.clone()),
                    keys,
                    metadatas: batch.metadatas,
                    tasks: batch
                        .tasks
                        .into_iter()
                        .map(|task| proto::OffloadTaskItem {
                            tenant_id: task.tenant_id,
                            key: task.key,
                            size: task.size,
                            generation_id: (!task.generation_id.is_nil()).then(|| {
                                let (high, low) = task.generation_id.as_u64_pair();
                                proto::Uuid { high, low }
                            }),
                        })
                        .collect(),
                    recovery_session_id: Some(recovery_session_id.clone()),
                })
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn persistent_restart_republishes_one_routable_replica_idempotently_over_grpc() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = LocalStorageConfig {
            root_dir: temp.path().to_path_buf(),
            fsdir: "recovery-e2e".to_string(),
            enable_eviction: false,
            quota_bytes: 1024 * 1024,
        };
        let storage_key = local_storage_key("tenant-a", "persistent-key");
        let id = Uuid::new_v4();
        let (high, low) = id.as_u64_pair();
        let master_client_id = master_proto::Uuid { high, low };
        let writer = Arc::new(LocalStorageBackend::new_persistent(config.clone()));
        writer.init().unwrap();
        let storage_uuid = writer.storage_id().unwrap();
        let (high, low) = storage_uuid.as_u64_pair();
        let master_storage_id = master_proto::Uuid { high, low };

        // Create the object and LocalDisk descriptor exclusively through the
        // Master-authoritative Put -> task -> generation -> Notify flow.
        let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
            enable_offload: true,
            ..Default::default()
        });
        MasterService::mount_segment(
            &service,
            tonic::Request::new(master_proto::MountSegmentRequest {
                client_id: Some(master_client_id.clone()),
                segment_name: "recovery-e2e-memory".to_string(),
                size: 4096,
                base_addr: 0x1000_0000,
                te_endpoint: "memory-holder".to_string(),
                protocol: "tcp".to_string(),
                host_id: String::new(),
            }),
        )
        .await
        .unwrap();
        let initial_session_uuid = Uuid::new_v4();
        let (high, low) = initial_session_uuid.as_u64_pair();
        let initial_session = master_proto::Uuid { high, low };
        MasterService::mount_local_disk_segment(
            &service,
            tonic::Request::new(master_proto::MountLocalDiskSegmentRequest {
                client_id: Some(master_client_id.clone()),
                enable_offloading: false,
                storage_id: Some(master_storage_id.clone()),
                recovery_complete: false,
                recovery_session_id: Some(initial_session.clone()),
            }),
        )
        .await
        .unwrap();
        MasterService::mount_local_disk_segment(
            &service,
            tonic::Request::new(master_proto::MountLocalDiskSegmentRequest {
                client_id: Some(master_client_id.clone()),
                enable_offloading: true,
                storage_id: Some(master_storage_id.clone()),
                recovery_complete: true,
                recovery_session_id: Some(initial_session),
            }),
        )
        .await
        .unwrap();
        MasterService::put_start(
            &service,
            tonic::Request::new(master_proto::PutStartRequest {
                client_id: Some(master_client_id.clone()),
                key: "persistent-key".to_string(),
                slice_length: 5,
                tenant_id: "tenant-a".to_string(),
                config: Some(master_proto::ReplicateConfig {
                    replica_num: 1,
                    preferred_segment: "recovery-e2e-memory".to_string(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .unwrap();
        MasterService::put_end(
            &service,
            tonic::Request::new(master_proto::PutEndRequest {
                client_id: Some(master_client_id.clone()),
                key: "persistent-key".to_string(),
                replica_type: master_proto::replica_descriptor::ReplicaType::Memory as i32,
                tenant_id: "tenant-a".to_string(),
            }),
        )
        .await
        .unwrap();
        let offload_task = MasterService::offload_object_heartbeat(
            &service,
            tonic::Request::new(master_proto::OffloadObjectHeartbeatRequest {
                client_id: Some(master_client_id.clone()),
                enable_offloading: true,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .tasks
        .into_iter()
        .next()
        .unwrap();
        let generation_proto = offload_task.generation_id.as_ref().unwrap();
        let generation = Uuid::from_u64_pair(generation_proto.high, generation_proto.low);
        let pending = writer.prepare_write(&storage_key, 5).unwrap();
        writer
            .commit_write_with_generation(&storage_key, b"value", pending, generation)
            .unwrap();
        MasterService::notify_offload_success(
            &service,
            tonic::Request::new(master_proto::NotifyOffloadSuccessRequest {
                client_id: Some(master_client_id.clone()),
                keys: vec!["persistent-key".to_string()],
                metadatas: vec![master_proto::StorageObjectMetadata {
                    bucket_id: 0,
                    offset: 0,
                    key_size: "persistent-key".len() as i64,
                    data_size: 5,
                    transport_endpoint: "holder-before-restart".to_string(),
                }],
                tasks: vec![offload_task],
                recovery_session_id: None,
            }),
        )
        .await
        .unwrap();
        drop(writer);

        let restarted = Arc::new(LocalStorageBackend::new_persistent(config));
        restarted.init().unwrap();
        let attached = AttachedLocalStorage::FilePerKey(Arc::clone(&restarted));
        let records =
            prepare_recovered_local_disk_records_with_generation(attached.scan_records().unwrap())
                .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].task.tenant_id, "tenant-a");
        assert_eq!(records[0].task.key, "persistent-key");
        assert_eq!(records[0].task.size, 5);
        assert_eq!(records[0].task.generation_id, generation);

        // Keep a real socket bound at the advertised endpoint.
        let route_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let transport_endpoint = route_listener.local_addr().unwrap().to_string();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MasterServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let mut master = proto::master_service_client::MasterServiceClient::connect(format!(
            "http://{master_address}"
        ))
        .await
        .unwrap();
        let (high, low) = id.as_u64_pair();
        let client_id = proto::Uuid { high, low };
        let (high, low) = storage_uuid.as_u64_pair();
        let storage_id = proto::Uuid { high, low };

        let recovery_session_uuid = Uuid::new_v4();
        let (high, low) = recovery_session_uuid.as_u64_pair();
        let recovery_session = proto::Uuid { high, low };
        mount_test_local_disk(
            &mut master,
            &client_id,
            &storage_id,
            &recovery_session,
            false,
            false,
        )
        .await;
        publish_test_recovered_records(
            &mut master,
            &client_id,
            &records,
            &transport_endpoint,
            &recovery_session,
        )
        .await;
        mount_test_local_disk(
            &mut master,
            &client_id,
            &storage_id,
            &recovery_session,
            true,
            true,
        )
        .await;

        let first = master
            .get_replica_list(proto::GetReplicaListRequest {
                key: "persistent-key".to_string(),
                tenant_id: "tenant-a".to_string(),
            })
            .await
            .unwrap()
            .into_inner();
        let local_disk_replicas = first
            .replicas
            .iter()
            .filter(|replica| {
                replica.replica_type == proto::replica_descriptor::ReplicaType::LocalDisk as i32
            })
            .collect::<Vec<_>>();
        assert_eq!(local_disk_replicas.len(), 1);
        let replica = local_disk_replicas[0];
        assert_eq!(
            replica.replica_type,
            proto::replica_descriptor::ReplicaType::LocalDisk as i32
        );
        assert_eq!(
            replica.status,
            proto::replica_descriptor::ReplicaStatus::Complete as i32
        );
        assert_eq!(replica.size, 5);
        assert_eq!(replica.transport_endpoint, transport_endpoint);
        assert_eq!(replica.holder_client_id.as_ref(), Some(&client_id));

        // A repeated mount/recovery updates the same holder replica instead
        // of appending a duplicate.
        let repeated_session_uuid = Uuid::new_v4();
        let (high, low) = repeated_session_uuid.as_u64_pair();
        let repeated_session = proto::Uuid { high, low };
        mount_test_local_disk(
            &mut master,
            &client_id,
            &storage_id,
            &repeated_session,
            false,
            false,
        )
        .await;
        publish_test_recovered_records(
            &mut master,
            &client_id,
            &records,
            &transport_endpoint,
            &repeated_session,
        )
        .await;
        mount_test_local_disk(
            &mut master,
            &client_id,
            &storage_id,
            &repeated_session,
            true,
            true,
        )
        .await;
        let repeated = master
            .get_replica_list(proto::GetReplicaListRequest {
                key: "persistent-key".to_string(),
                tenant_id: "tenant-a".to_string(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(repeated.replicas, first.replicas);

        drop(route_listener);
        server.abort();
    }
}

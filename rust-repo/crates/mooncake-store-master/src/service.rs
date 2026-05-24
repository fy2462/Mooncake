use crate::allocator::SegmentAllocator;
use crate::http_metadata::MetadataState;
use crate::metrics;
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use dashmap::DashMap;
use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig, TaskInfo,
    TaskStatus, TaskType,
};
use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tonic::{Request, Response, Status};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Master state
// ---------------------------------------------------------------------------

pub(crate) struct MasterState {
    pub(crate) clients: DashMap<Uuid, ClientEntry>,
    pub(crate) objects: DashMap<String, ObjectEntry>,
    pub(crate) segments: DashMap<Uuid, SegmentEntry>,
    pub(crate) tasks: DashMap<Uuid, TaskEntry>,
    pub(crate) allocator: RwLock<SegmentAllocator>,
    storage_backend: RwLock<Option<StorageBackend>>,
}

pub(crate) struct ClientEntry {
    pub(crate) info: mooncake_store_core::ClientInfo,
    pub(crate) last_ping: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub replicas: Vec<ReplicaDescriptor>,
    pub size: u64,
}

pub struct SegmentEntry {
    pub segment: mooncake_store_core::Segment,
}

#[derive(Debug, Clone)]
pub struct TaskEntry {
    pub info: TaskInfo,
    pub key: String,
}

// ---------------------------------------------------------------------------
// MasterServiceImpl
// ---------------------------------------------------------------------------

pub struct MasterServiceImpl {
    state: Arc<MasterState>,
    metadata_state: MetadataState,
}

impl MasterServiceImpl {
    pub fn new(backend_type: Option<StorageBackendType>, backup_dir: Option<PathBuf>) -> Self {
        let storage_backend = match (backend_type, backup_dir) {
            (Some(btype), Some(dir)) => RwLock::new(Some(StorageBackend::new(btype, &dir))),
            _ => RwLock::new(None),
        };

        let state = Arc::new(MasterState {
            clients: DashMap::new(),
            objects: DashMap::new(),
            segments: DashMap::new(),
            tasks: DashMap::new(),
            allocator: RwLock::new(SegmentAllocator::new()),
            storage_backend,
        });
        let metadata_state = MetadataState::new("");

        // Load existing state from snapshot
        if let Some(ref backend) = *state.storage_backend.read() {
            if let Ok(Some((segments, objects))) = backend.load() {
                for seg in segments {
                    state.segments.insert(seg.id, SegmentEntry { segment: seg.clone() });
                    state.allocator.write().add_segment(seg);
                }
                for (key, object) in objects {
                    state.objects.insert(key, object);
                }
                tracing::info!("Restored state from snapshot");
            }
        }

        Self {
            state,
            metadata_state,
        }
    }

    pub fn save_snapshot(&self) {
        let start = std::time::Instant::now();
        if let Some(ref backend) = *self.state.storage_backend.read() {
            if let Err(e) = backend.save(&self.state.segments, &self.state.objects) {
                metrics::SNAPSHOT_FAIL_COUNT.inc();
                tracing::error!("Failed to save snapshot: {}", e);
            } else {
                metrics::SNAPSHOT_DURATION_MS.set(start.elapsed().as_millis() as i64);
                metrics::SNAPSHOT_SUCCESS_COUNT.inc();
            }
        }
    }

    pub fn metadata_state(&self) -> MetadataState {
        self.metadata_state.clone()
    }
}

impl Default for MasterServiceImpl {
    fn default() -> Self {
        Self::new(None, None)
    }
}

// ---------------------------------------------------------------------------
// Helpers: UUID conversions
// ---------------------------------------------------------------------------

fn uuid_to_proto(id: Uuid) -> proto::Uuid {
    let (high, low) = id.as_u64_pair();
    proto::Uuid { high, low }
}

fn uuid_from_proto(p: &proto::Uuid) -> Uuid {
    Uuid::from_u64_pair(p.high, p.low)
}

fn replica_to_proto(r: &ReplicaDescriptor) -> proto::ReplicaDescriptor {
    proto::ReplicaDescriptor {
        segment_id: Some(uuid_to_proto(r.segment_id)),
        segment_name: r.segment_name.clone(),
        offset: r.offset,
        status: r.status as i32,
        replica_type: r.replica_type as i32,
        slice_key_hash: vec![],
        size: r.size,
    }
}

fn replica_from_proto(p: &proto::ReplicaDescriptor) -> ReplicaDescriptor {
    ReplicaDescriptor {
        segment_id: p.segment_id.as_ref().map_or(Uuid::nil(), uuid_from_proto),
        segment_name: p.segment_name.clone(),
        offset: p.offset,
        size: p.size,
        status: match p.status {
            1 => ReplicaStatus::Allocating,
            2 => ReplicaStatus::Written,
            3 => ReplicaStatus::Complete,
            4 => ReplicaStatus::Failed,
            _ => ReplicaStatus::Undefined,
        },
        replica_type: if p.replica_type == 1 {
            ReplicaType::Disk
        } else {
            ReplicaType::Memory
        },
    }
}

fn config_from_proto(c: &proto::ReplicateConfig) -> ReplicateConfig {
    ReplicateConfig {
        replica_num: c.replica_num,
        with_soft_pin: c.with_soft_pin,
        with_hard_pin: c.with_hard_pin,
        preferred_segment: c.preferred_segment.clone(),
        prefer_alloc_in_same_node: c.prefer_alloc_in_same_node,
    }
}

fn task_type_to_proto(task_type: TaskType) -> i32 {
    match task_type {
        TaskType::ReplicaCopy => proto::TaskType::ReplicaCopy as i32,
        TaskType::ReplicaMove => proto::TaskType::ReplicaMove as i32,
    }
}

fn task_status_to_proto(status: TaskStatus) -> i32 {
    match status {
        TaskStatus::Pending => proto::TaskStatus::TaskPending as i32,
        TaskStatus::Processing => proto::TaskStatus::TaskProcessing as i32,
        TaskStatus::Success => proto::TaskStatus::TaskSuccess as i32,
        TaskStatus::Failed => proto::TaskStatus::TaskFailed as i32,
    }
}

fn host_from_segment_name(name: &str) -> String {
    name.split(':').next().unwrap_or(name).to_string()
}

fn port_from_segment_name(name: &str) -> u16 {
    name.split(':')
        .nth(1)
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0)
}

fn merge_addresses(existing: &[String], new_addresses: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut merged = existing.to_vec();
    for address in new_addresses {
        if !address.is_empty() && !merged.iter().any(|v| v == &address) {
            merged.push(address);
        }
    }
    merged
}

fn upsert_client_addresses(state: &MasterState, client_id: Uuid, addresses: Vec<String>) {
    let now = Utc::now();
    if let Some(mut entry) = state.clients.get_mut(&client_id) {
        entry.info.addresses = merge_addresses(&entry.info.addresses, addresses);
        entry.info.last_seen = now;
        entry.last_ping = SystemTime::now();
        return;
    }

    state.clients.insert(client_id, ClientEntry {
        info: mooncake_store_core::ClientInfo {
            id: client_id,
            addresses,
            segments: vec![],
            last_seen: now,
        },
        last_ping: SystemTime::now(),
    });
}

async fn register_metadata_segments(metadata_state: &MetadataState, segment_names: &[String]) {
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

fn client_id_by_segment_name(state: &MasterState, segment_name: &str) -> Option<Uuid> {
    state
        .segments
        .iter()
        .find(|entry| entry.segment.name == segment_name)
        .map(|entry| entry.segment.client_id)
}

fn addresses_for_client(state: &MasterState, client_id: Uuid) -> Vec<String> {
    if let Some(entry) = state.clients.get(&client_id) {
        if !entry.info.addresses.is_empty() {
            return entry.info.addresses.clone();
        }
    }

    let mut addresses = Vec::new();
    for segment in state.segments.iter() {
        if segment.segment.client_id == client_id {
            let host = host_from_segment_name(&segment.segment.name);
            if !host.is_empty() && !addresses.iter().any(|v| v == &host) {
                addresses.push(host);
            }
        }
    }
    addresses
}

fn sync_segment_usage(
    state: &MasterState,
    segment_ids: impl IntoIterator<Item = Uuid>,
) {
    let allocator = state.allocator.read();
    for segment_id in segment_ids {
        let Some(used) = allocator.used_bytes(&segment_id) else {
            continue;
        };
        if let Some(mut entry) = state.segments.get_mut(&segment_id) {
            entry.segment.used = used;
        }
    }
}

// ---------------------------------------------------------------------------
// gRPC service impl
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl MasterService for MasterServiceImpl {
    // ---- Ping ----
    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let derived_addresses = req
            .mounted_segments
            .iter()
            .map(|segment| host_from_segment_name(segment))
            .collect::<Vec<_>>();
        if !derived_addresses.is_empty() {
            upsert_client_addresses(&self.state, client_id, derived_addresses);
        }

        if let Some(mut entry) = self.state.clients.get_mut(&client_id) {
            entry.last_ping = SystemTime::now();
            entry.info.last_seen = Utc::now();
        }
        register_metadata_segments(&self.metadata_state, &req.mounted_segments).await;
        metrics::PING_REQUESTS.inc();
        Ok(Response::new(proto::PingResponse {}))
    }

    // ---- MountSegment ----
    async fn mount_segment(
        &self,
        request: Request<proto::MountSegmentRequest>,
    ) -> Result<Response<proto::MountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let segment_id = Uuid::new_v4();
        let host = host_from_segment_name(&req.segment_name);

        let segment = mooncake_store_core::Segment {
            id: segment_id,
            name: req.segment_name.clone(),
            size: req.size,
            used: 0,
            client_id,
        };

        self.state.segments.insert(segment_id, SegmentEntry { segment: segment.clone() });
        upsert_client_addresses(&self.state, client_id, vec![host]);
        register_metadata_segments(&self.metadata_state, &[req.segment_name.clone()]).await;

        let mut allocator = self.state.allocator.write();
        allocator.add_segment(segment);

        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::MountSegmentResponse {}))
    }

    // ---- UnmountSegment ----
    async fn unmount_segment(
        &self,
        request: Request<proto::UnmountSegmentRequest>,
    ) -> Result<Response<proto::UnmountSegmentResponse>, Status> {
        let req = request.into_inner();
        let segment_id = uuid_from_proto(req.segment_id.as_ref().ok_or(Status::invalid_argument("missing segment_id"))?);

        self.state.segments.remove(&segment_id);
        {
            let mut allocator = self.state.allocator.write();
            allocator.remove_segment(&segment_id);
        }
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::UnmountSegmentResponse {}))
    }

    // ---- ReMountSegment ----
    async fn re_mount_segment(
        &self,
        request: Request<proto::ReMountSegmentRequest>,
    ) -> Result<Response<proto::ReMountSegmentResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        if req.segment_names.len() != req.segment_sizes.len() {
            return Err(Status::invalid_argument("segment_names and segment_sizes must have same length"));
        }

        let addresses = req
            .segment_names
            .iter()
            .map(|name| host_from_segment_name(name))
            .collect::<Vec<_>>();
        upsert_client_addresses(&self.state, client_id, addresses);
        register_metadata_segments(&self.metadata_state, &req.segment_names).await;

        for (segment_name, size) in req.segment_names.iter().zip(req.segment_sizes.iter()) {
            let exists = self
                .state
                .segments
                .iter()
                .any(|entry| entry.segment.client_id == client_id && entry.segment.name == *segment_name);
            if exists {
                continue;
            }

            let segment = mooncake_store_core::Segment {
                id: Uuid::new_v4(),
                name: segment_name.clone(),
                size: *size,
                used: 0,
                client_id,
            };
            self.state.segments.insert(segment.id, SegmentEntry { segment: segment.clone() });
            self.state.allocator.write().add_segment(segment);
        }
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(Response::new(proto::ReMountSegmentResponse {}))
    }

    // ---- ExistKey ----
    async fn exist_key(
        &self,
        request: Request<proto::ExistKeyRequest>,
    ) -> Result<Response<proto::ExistKeyResponse>, Status> {
        let req = request.into_inner();
        let exists = self.state.objects.contains_key(&req.key);
        metrics::GET_REQUESTS.inc();
        Ok(Response::new(proto::ExistKeyResponse { exists }))
    }

    // ---- GetAllKeys ----
    async fn get_all_keys(
        &self,
        _request: Request<proto::GetAllKeysRequest>,
    ) -> Result<Response<proto::GetAllKeysResponse>, Status> {
        let keys: Vec<String> = self.state.objects.iter().map(|entry| entry.key().clone()).collect();
        Ok(Response::new(proto::GetAllKeysResponse { keys }))
    }

    // ---- GetAllSegments ----
    async fn get_all_segments(
        &self,
        _request: Request<proto::GetAllSegmentsRequest>,
    ) -> Result<Response<proto::GetAllSegmentsResponse>, Status> {
        let segments: Vec<String> = self.state.segments.iter().map(|entry| entry.segment.name.clone()).collect();
        Ok(Response::new(proto::GetAllSegmentsResponse { segments }))
    }

    // ---- PutStart ----
    async fn put_start(
        &self,
        request: Request<proto::PutStartRequest>,
    ) -> Result<Response<proto::PutStartResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();

        if self.state.objects.contains_key(&key) {
            return Err(Status::already_exists(format!("object already exists: {}", key)));
        }

        let config = req.config.as_ref().map(config_from_proto).unwrap_or_default();
        let replica_count = if config.replica_num == 0 { 1 } else { config.replica_num as usize };

        let replicas = {
            let mut allocator = self.state.allocator.write();
            allocator.allocate(&key, req.slice_length, replica_count, &config)
        };
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(key, ObjectEntry {
            replicas,
            size: req.slice_length,
        });

        metrics::PUT_START_REQUESTS.inc();
        Ok(Response::new(proto::PutStartResponse { replicas: proto_replicas }))
    }

    // ---- PutEnd ----
    async fn put_end(
        &self,
        request: Request<proto::PutEndRequest>,
    ) -> Result<Response<proto::PutEndResponse>, Status> {
        let req = request.into_inner();
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            for r in &mut entry.replicas {
                if r.status == ReplicaStatus::Allocating {
                    r.status = ReplicaStatus::Complete;
                }
            }
        }
        Ok(Response::new(proto::PutEndResponse {}))
    }

    // ---- AddReplica ----
    async fn add_replica(
        &self,
        request: Request<proto::AddReplicaRequest>,
    ) -> Result<Response<proto::AddReplicaResponse>, Status> {
        let req = request.into_inner();
        let replica = req.replica.as_ref().map(replica_from_proto).ok_or(Status::invalid_argument("missing replica"))?;
        if let Some(mut entry) = self.state.objects.get_mut(&req.key) {
            entry.replicas.push(replica);
        }
        Ok(Response::new(proto::AddReplicaResponse {}))
    }

    // ---- GetReplicaList ----
    async fn get_replica_list(
        &self,
        request: Request<proto::GetReplicaListRequest>,
    ) -> Result<Response<proto::GetReplicaListResponse>, Status> {
        let req = request.into_inner();
        match self.state.objects.get(&req.key) {
            Some(entry) => {
                let replicas = entry.replicas.iter().map(replica_to_proto).collect();
                metrics::GET_REQUESTS.inc();
                Ok(Response::new(proto::GetReplicaListResponse { replicas }))
            }
            None => Err(Status::not_found(format!("key not found: {}", req.key))),
        }
    }

    // ---- Remove ----
    async fn remove(
        &self,
        request: Request<proto::RemoveRequest>,
    ) -> Result<Response<proto::RemoveResponse>, Status> {
        let req = request.into_inner();
        if let Some((_, object)) = self.state.objects.remove(&req.key) {
            let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
            self.state.allocator.write().release(&object.replicas);
            sync_segment_usage(&self.state, segment_ids);
        }
        metrics::REMOVE_REQUESTS.inc();
        Ok(Response::new(proto::RemoveResponse {}))
    }

    // ---- RemoveByRegex ----
    async fn remove_by_regex(
        &self,
        request: Request<proto::RemoveByRegexRequest>,
    ) -> Result<Response<proto::RemoveByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut removed = 0i64;
        let keys_to_remove: Vec<String> = self
            .state
            .objects
            .iter()
            .filter(|entry| pattern.is_match(entry.key()))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            if let Some((_, object)) = self.state.objects.remove(&key) {
                let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&object.replicas);
                sync_segment_usage(&self.state, segment_ids);
                removed += 1;
            }
        }

        metrics::REMOVE_BY_REGEX_REQUESTS.inc();
        metrics::REMOVE_REQUESTS.inc_by(removed as u64);
        Ok(Response::new(proto::RemoveByRegexResponse { removed_count: removed }))
    }

    // ---- BatchExistKey ----
    async fn batch_exist_key(
        &self,
        request: Request<proto::BatchExistKeyRequest>,
    ) -> Result<Response<proto::BatchExistKeyResponse>, Status> {
        let req = request.into_inner();
        let results: Vec<bool> = req.keys.iter().map(|k| self.state.objects.contains_key(k)).collect();
        Ok(Response::new(proto::BatchExistKeyResponse { results }))
    }

    // ---- BatchQueryIp ----
    async fn batch_query_ip(
        &self,
        request: Request<proto::BatchQueryIpRequest>,
    ) -> Result<Response<proto::BatchQueryIpResponse>, Status> {
        let req = request.into_inner();
        let mut ips = std::collections::HashMap::new();
        for cid in &req.client_ids {
            let id = uuid_from_proto(cid);
            let addresses = addresses_for_client(&self.state, id);
            if !addresses.is_empty() {
                ips.insert(
                    id.to_string(),
                    proto::IpList { addresses },
                );
            }
        }
        Ok(Response::new(proto::BatchQueryIpResponse { ips }))
    }

    // ---- BatchReplicaClear ----
    async fn batch_replica_clear(
        &self,
        request: Request<proto::BatchReplicaClearRequest>,
    ) -> Result<Response<proto::BatchReplicaClearResponse>, Status> {
        let req = request.into_inner();
        let mut cleared = vec![];
        for key in &req.object_keys {
            if let Some((_, object)) = self.state.objects.remove(key) {
                let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&object.replicas);
                sync_segment_usage(&self.state, segment_ids);
                cleared.push(key.clone());
            }
        }
        Ok(Response::new(proto::BatchReplicaClearResponse { cleared_keys: cleared }))
    }

    // ---- QueryByRegex ----
    async fn query_by_regex(
        &self,
        request: Request<proto::QueryByRegexRequest>,
    ) -> Result<Response<proto::QueryByRegexResponse>, Status> {
        let req = request.into_inner();
        let pattern = regex::Regex::new(&req.pattern)
            .map_err(|e| Status::invalid_argument(format!("invalid regex: {e}")))?;

        let mut entries = vec![];
        for entry in self.state.objects.iter() {
            if pattern.is_match(entry.key()) {
                let r = entry.replicas.iter().map(replica_to_proto).collect();
                entries.push(proto::query_by_regex_response::Entry {
                    key: entry.key().clone(),
                    replicas: r,
                });
            }
        }
        Ok(Response::new(proto::QueryByRegexResponse { entries }))
    }

    // ---- QuerySegments ----
    async fn query_segments(
        &self,
        request: Request<proto::QuerySegmentsRequest>,
    ) -> Result<Response<proto::QuerySegmentsResponse>, Status> {
        let req = request.into_inner();
        for entry in self.state.segments.iter() {
            if entry.segment.name == req.segment_name {
                return Ok(Response::new(proto::QuerySegmentsResponse {
                    total_size: entry.segment.size,
                    used_size: entry.segment.used,
                }));
            }
        }
        Err(Status::not_found("segment not found"))
    }

    // ---- QueryIp ----
    async fn query_ip(
        &self,
        request: Request<proto::QueryIpRequest>,
    ) -> Result<Response<proto::QueryIpResponse>, Status> {
        let req = request.into_inner();
        let client_id = uuid_from_proto(req.client_id.as_ref().ok_or(Status::invalid_argument("missing client_id"))?);
        let addresses = addresses_for_client(&self.state, client_id);
        if addresses.is_empty() {
            Err(Status::not_found("client not found"))
        } else {
            Ok(Response::new(proto::QueryIpResponse { addresses }))
        }
    }

    // ---- Upsert ----
    async fn upsert(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        let req = request.into_inner();
        let config = req.config.as_ref().map(config_from_proto).unwrap_or_default();
        let replica_count = if config.replica_num == 0 { 1 } else { config.replica_num as usize };

        // Match the C++ store behavior: only reuse placement when the object
        // size stays the same, otherwise release old space and allocate again.
        let replicas = if let Some(existing) = self.state.objects.get(&req.key) {
            if existing.size == req.slice_length {
                existing.replicas.clone()
            } else {
                let old_replicas = existing.replicas.clone();
                drop(existing);
                let segment_ids: Vec<Uuid> = old_replicas.iter().map(|r| r.segment_id).collect();
                self.state.allocator.write().release(&old_replicas);
                sync_segment_usage(&self.state, segment_ids);
                let mut allocator = self.state.allocator.write();
                allocator.allocate(&req.key, req.slice_length, replica_count, &config)
            }
        } else {
            let mut allocator = self.state.allocator.write();
            allocator.allocate(&req.key, req.slice_length, replica_count, &config)
        };
        sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));

        let proto_replicas: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(req.key.clone(), ObjectEntry {
            replicas,
            size: req.slice_length,
        });

        Ok(Response::new(proto::UpsertResponse { replicas: proto_replicas }))
    }

    // ---- CreateCopyTask ----
    async fn create_copy_task(
        &self,
        request: Request<proto::CreateCopyTaskRequest>,
    ) -> Result<Response<proto::CreateCopyTaskResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("missing key"));
        }
        if req.targets.is_empty() {
            return Err(Status::invalid_argument("missing targets"));
        }
        let object = self.state.objects.get(&req.key).ok_or(Status::not_found("key not found"))?;
        if object.replicas.is_empty() {
            return Err(Status::failed_precondition("object has no source replicas"));
        }
        for target in &req.targets {
            if client_id_by_segment_name(&self.state, target).is_none() {
                return Err(Status::invalid_argument(format!("target segment not mounted: {target}")));
            }
        }
        let assigned_client = client_id_by_segment_name(&self.state, &object.replicas[0].segment_name)
            .ok_or(Status::failed_precondition("source segment missing"))?;
        drop(object);
        if !self.state.objects.contains_key(&req.key) {
            return Err(Status::not_found("key not found"));
        }
        let task_id = Uuid::new_v4();
        let now = Utc::now();
        self.state.tasks.insert(task_id, TaskEntry {
            info: TaskInfo {
                id: task_id,
                task_type: TaskType::ReplicaCopy,
                status: TaskStatus::Pending,
                created_at: now,
                last_updated_at: now,
                assigned_client: Some(assigned_client),
                message: format!("copy {} to {} target(s)", req.key, req.targets.len()),
            },
            key: req.key,
        });
        Ok(Response::new(proto::CreateCopyTaskResponse { task_id: Some(uuid_to_proto(task_id)) }))
    }

    // ---- CreateMoveTask ----
    async fn create_move_task(
        &self,
        request: Request<proto::CreateMoveTaskRequest>,
    ) -> Result<Response<proto::CreateMoveTaskResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() || req.source.is_empty() || req.target.is_empty() {
            return Err(Status::invalid_argument("missing key/source/target"));
        }
        if req.source == req.target {
            return Err(Status::invalid_argument("source and target must differ"));
        }
        let object = self.state.objects.get(&req.key).ok_or(Status::not_found("key not found"))?;
        if !object.replicas.iter().any(|replica| replica.segment_name == req.source) {
            return Err(Status::invalid_argument("source segment not found"));
        }
        let assigned_client = client_id_by_segment_name(&self.state, &req.source)
            .ok_or(Status::failed_precondition("source segment missing"))?;
        if client_id_by_segment_name(&self.state, &req.target).is_none() {
            return Err(Status::invalid_argument("target segment not mounted"));
        }
        drop(object);
        let task_id = Uuid::new_v4();
        let now = Utc::now();
        self.state.tasks.insert(task_id, TaskEntry {
            info: TaskInfo {
                id: task_id,
                task_type: TaskType::ReplicaMove,
                status: TaskStatus::Pending,
                created_at: now,
                last_updated_at: now,
                assigned_client: Some(assigned_client),
                message: format!("move {} from {} to {}", req.key, req.source, req.target),
            },
            key: req.key,
        });
        Ok(Response::new(proto::CreateMoveTaskResponse { task_id: Some(uuid_to_proto(task_id)) }))
    }

    // ---- QueryTask ----
    async fn query_task(
        &self,
        request: Request<proto::QueryTaskRequest>,
    ) -> Result<Response<proto::QueryTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = uuid_from_proto(req.task_id.as_ref().ok_or(Status::invalid_argument("missing task_id"))?);
        let task = self.state.tasks.get(&task_id).ok_or(Status::not_found("task not found"))?;
        Ok(Response::new(proto::QueryTaskResponse {
            id: Some(uuid_to_proto(task.info.id)),
            task_type: task_type_to_proto(task.info.task_type),
            status: task_status_to_proto(task.info.status),
            created_at_ms_epoch: task.info.created_at.timestamp_millis(),
            last_updated_at_ms_epoch: task.info.last_updated_at.timestamp_millis(),
            assigned_client: task.info.assigned_client.map(uuid_to_proto),
            message: task.info.message.clone(),
        }))
    }

    // ---- BatchPutEnd ----
    async fn batch_put_end(
        &self,
        request: Request<proto::BatchPutEndRequest>,
    ) -> Result<Response<proto::BatchPutEndResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .entries
            .iter()
            .map(|entry| {
                if let Some(mut obj) = self.state.objects.get_mut(&entry.key) {
                    for r in &mut obj.replicas {
                        if r.status == ReplicaStatus::Allocating {
                            r.status = ReplicaStatus::Complete;
                        }
                    }
                    0
                } else {
                    -1
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutEndResponse { statuses }))
    }

    // ---- BatchPutRevoke ----
    async fn batch_put_revoke(
        &self,
        request: Request<proto::BatchPutRevokeRequest>,
    ) -> Result<Response<proto::BatchPutRevokeResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|key| {
                if let Some((_, object)) = self.state.objects.remove(key) {
                    let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&object.replicas);
                    sync_segment_usage(&self.state, segment_ids);
                    0
                } else {
                    -1
                }
            })
            .collect();
        Ok(Response::new(proto::BatchPutRevokeResponse { statuses }))
    }

    // ---- BatchRemove ----
    async fn batch_remove(
        &self,
        request: Request<proto::BatchRemoveRequest>,
    ) -> Result<Response<proto::BatchRemoveResponse>, Status> {
        let req = request.into_inner();
        let statuses: Vec<i32> = req
            .keys
            .iter()
            .map(|key| {
                if let Some((_, object)) = self.state.objects.remove(key) {
                    let segment_ids: Vec<Uuid> = object.replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&object.replicas);
                    sync_segment_usage(&self.state, segment_ids);
                }
                0
            })
            .collect();
        metrics::BATCH_REMOVE_REQUESTS.inc_by(req.keys.len() as u64);
        Ok(Response::new(proto::BatchRemoveResponse { statuses }))
    }

    // ---- BatchUpsertEnd ----
    async fn batch_upsert_end(
        &self,
        request: Request<proto::BatchUpsertEndRequest>,
    ) -> Result<Response<proto::BatchUpsertEndResponse>, Status> {
        let req = request.into_inner();
        let mut all_replicas = Vec::new();

        for entry in &req.entries {
            let config = entry.config.as_ref().map(config_from_proto).unwrap_or_default();
            let replica_count = if config.replica_num == 0 { 1 } else { config.replica_num as usize };

            let replicas = if let Some(existing) = self.state.objects.get(&entry.key) {
                if existing.size == entry.slice_length {
                    existing.replicas.clone()
                } else {
                    let old_replicas = existing.replicas.clone();
                    drop(existing);
                    let segment_ids: Vec<Uuid> = old_replicas.iter().map(|r| r.segment_id).collect();
                    self.state.allocator.write().release(&old_replicas);
                    sync_segment_usage(&self.state, segment_ids);
                    let mut allocator = self.state.allocator.write();
                    allocator.allocate(&entry.key, entry.slice_length, replica_count, &config)
                }
            } else {
                let mut allocator = self.state.allocator.write();
                allocator.allocate(&entry.key, entry.slice_length, replica_count, &config)
            };

            let proto_r: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();
            sync_segment_usage(&self.state, replicas.iter().map(|r| r.segment_id));
            self.state.objects.insert(entry.key.clone(), ObjectEntry {
                replicas,
                size: entry.slice_length,
            });
            all_replicas.extend(proto_r);
        }

        Ok(Response::new(proto::BatchUpsertEndResponse { replicas: all_replicas }))
    }
}

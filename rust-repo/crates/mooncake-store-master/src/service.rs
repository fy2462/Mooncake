use crate::allocator::SegmentAllocator;
use crate::metrics;
use crate::proto;
use crate::proto::master_service_server::MasterService;
use crate::storage_backend::{StorageBackend, StorageBackendType};
use dashmap::DashMap;
use mooncake_store_core::{
    ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig,
};
use parking_lot::RwLock;
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
    pub(crate) allocator: RwLock<SegmentAllocator>,
    storage_backend: RwLock<Option<StorageBackend>>,
}

pub(crate) struct ClientEntry {
    pub(crate) info: mooncake_store_core::ClientInfo,
    pub(crate) last_ping: SystemTime,
}

pub struct ObjectEntry {
    pub replicas: Vec<ReplicaDescriptor>,
}

pub struct SegmentEntry {
    pub segment: mooncake_store_core::Segment,
}

// ---------------------------------------------------------------------------
// MasterServiceImpl
// ---------------------------------------------------------------------------

pub struct MasterServiceImpl {
    state: Arc<MasterState>,
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
            allocator: RwLock::new(SegmentAllocator::new()),
            storage_backend,
        });

        // Load existing state from snapshot
        if let Some(ref backend) = *state.storage_backend.read() {
            if let Ok(Some((segments, objects))) = backend.load() {
                for seg in segments {
                    state.segments.insert(seg.id, SegmentEntry { segment: seg.clone() });
                    state.allocator.write().add_segment(seg);
                }
                for (key, replicas) in objects {
                    state.objects.insert(key, ObjectEntry { replicas });
                }
                tracing::info!("Restored state from snapshot");
            }
        }

        Self { state }
    }

    pub fn save_snapshot(&self) {
        if let Some(ref backend) = *self.state.storage_backend.read() {
            if let Err(e) = backend.save(&self.state.segments, &self.state.objects) {
                tracing::error!("Failed to save snapshot: {}", e);
            }
        }
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
    }
}

fn replica_from_proto(p: &proto::ReplicaDescriptor) -> ReplicaDescriptor {
    ReplicaDescriptor {
        segment_id: p.segment_id.as_ref().map_or(Uuid::nil(), uuid_from_proto),
        segment_name: p.segment_name.clone(),
        offset: p.offset,
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

        if let Some(mut entry) = self.state.clients.get_mut(&client_id) {
            entry.last_ping = SystemTime::now();
        }
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

        let segment = mooncake_store_core::Segment {
            id: segment_id,
            name: req.segment_name.clone(),
            size: req.size,
            used: 0,
            client_id,
        };

        self.state.segments.insert(segment_id, SegmentEntry { segment: segment.clone() });

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
        _request: Request<proto::ReMountSegmentRequest>,
    ) -> Result<Response<proto::ReMountSegmentResponse>, Status> {
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
            let allocator = self.state.allocator.read();
            allocator.allocate(&key, req.slice_length, replica_count, &config)
        };

        let proto_replicas: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(key, ObjectEntry {
            replicas,
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
        self.state.objects.remove(&req.key);
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
            self.state.objects.remove(&key);
            removed += 1;
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
            if let Some(entry) = self.state.clients.get(&id) {
                ips.insert(
                    id.to_string(),
                    proto::IpList { addresses: entry.info.addresses.clone() },
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
            if self.state.objects.remove(key).is_some() {
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
        if let Some(entry) = self.state.clients.get(&client_id) {
            Ok(Response::new(proto::QueryIpResponse { addresses: entry.info.addresses.clone() }))
        } else {
            Err(Status::not_found("client not found"))
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

        // If object exists, reuse existing placement; otherwise allocate new.
        let replicas = if let Some(existing) = self.state.objects.get(&req.key) {
            existing.replicas.clone()
        } else {
            let allocator = self.state.allocator.read();
            allocator.allocate(&req.key, req.slice_length, replica_count, &config)
        };

        let proto_replicas: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();

        self.state.objects.insert(req.key.clone(), ObjectEntry {
            replicas,
        });

        Ok(Response::new(proto::UpsertResponse { replicas: proto_replicas }))
    }

    // ---- CreateCopyTask ----
    async fn create_copy_task(
        &self,
        _request: Request<proto::CreateCopyTaskRequest>,
    ) -> Result<Response<proto::CreateCopyTaskResponse>, Status> {
        let task_id = Uuid::new_v4();
        Ok(Response::new(proto::CreateCopyTaskResponse { task_id: Some(uuid_to_proto(task_id)) }))
    }

    // ---- CreateMoveTask ----
    async fn create_move_task(
        &self,
        _request: Request<proto::CreateMoveTaskRequest>,
    ) -> Result<Response<proto::CreateMoveTaskResponse>, Status> {
        let task_id = Uuid::new_v4();
        Ok(Response::new(proto::CreateMoveTaskResponse { task_id: Some(uuid_to_proto(task_id)) }))
    }

    // ---- QueryTask ----
    async fn query_task(
        &self,
        request: Request<proto::QueryTaskRequest>,
    ) -> Result<Response<proto::QueryTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = uuid_from_proto(req.task_id.as_ref().ok_or(Status::invalid_argument("missing task_id"))?);
        Ok(Response::new(proto::QueryTaskResponse {
            id: Some(uuid_to_proto(task_id)),
            task_type: proto::TaskType::ReplicaCopy as i32,
            status: proto::TaskStatus::TaskPending as i32,
            created_at_ms_epoch: 0,
            last_updated_at_ms_epoch: 0,
            assigned_client: None,
            message: String::new(),
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
                if self.state.objects.remove(key).is_some() {
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
                self.state.objects.remove(key);
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
                existing.replicas.clone()
            } else {
                let allocator = self.state.allocator.read();
                allocator.allocate(&entry.key, entry.slice_length, replica_count, &config)
            };

            let proto_r: Vec<proto::ReplicaDescriptor> = replicas.iter().map(replica_to_proto).collect();
            self.state.objects.insert(entry.key.clone(), ObjectEntry { replicas });
            all_replicas.extend(proto_r);
        }

        Ok(Response::new(proto::BatchUpsertEndResponse { replicas: all_replicas }))
    }
}

use crate::proto;
use mooncake_store_core::{
    NoFSegment, NoFSegmentOwnerInfo, ReplicaDescriptor, ReplicaStatus, ReplicaType, ReplicateConfig,
    TaskStatus, TaskType,
};
use uuid::Uuid;

pub(crate) fn uuid_to_proto(id: Uuid) -> proto::Uuid {
    let (high, low) = id.as_u64_pair();
    proto::Uuid { high, low }
}

pub(crate) fn uuid_from_proto(p: &proto::Uuid) -> Uuid {
    Uuid::from_u64_pair(p.high, p.low)
}

pub(crate) fn replica_to_proto(r: &ReplicaDescriptor) -> proto::ReplicaDescriptor {
    proto::ReplicaDescriptor {
        segment_id: Some(uuid_to_proto(r.segment_id)),
        segment_name: r.segment_name.clone(),
        offset: r.offset,
        status: r.status as i32,
        replica_type: r.replica_type as i32,
        slice_key_hash: vec![],
        size: r.size,
        holder_client_id: r.holder_client_id.map(uuid_to_proto),
    }
}

pub(crate) fn replica_from_proto(p: &proto::ReplicaDescriptor) -> ReplicaDescriptor {
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
        replica_type: match p.replica_type {
            1 => ReplicaType::Disk,
            2 => ReplicaType::LocalDisk,
            3 => ReplicaType::NoFSsd,
            _ => ReplicaType::Memory,
        },
        holder_client_id: p.holder_client_id.as_ref().map(uuid_from_proto),
    }
}

pub(crate) fn config_from_proto(c: &proto::ReplicateConfig) -> ReplicateConfig {
    ReplicateConfig {
        replica_num: c.replica_num,
        nof_replica_num: c.nof_replica_num,
        with_soft_pin: c.with_soft_pin,
        with_hard_pin: c.with_hard_pin,
        preferred_segment: c.preferred_segment.clone(),
        prefer_alloc_in_same_node: c.prefer_alloc_in_same_node,
    }
}

pub(crate) fn nof_segment_to_proto(segment: &NoFSegment) -> proto::NoFSegment {
    proto::NoFSegment {
        id: Some(uuid_to_proto(segment.id)),
        name: segment.name.clone(),
        base: segment.base,
        size: segment.size,
        te_endpoint: segment.te_endpoint.clone(),
        client_id: Some(uuid_to_proto(segment.client_id)),
    }
}

pub(crate) fn nof_segment_from_proto(segment: &proto::NoFSegment) -> NoFSegment {
    NoFSegment {
        id: segment.id.as_ref().map_or(Uuid::new_v4(), uuid_from_proto),
        name: segment.name.clone(),
        base: segment.base,
        size: segment.size,
        te_endpoint: segment.te_endpoint.clone(),
        client_id: segment.client_id.as_ref().map_or(Uuid::nil(), uuid_from_proto),
    }
}

pub(crate) fn nof_segment_owner_to_proto(owner: &NoFSegmentOwnerInfo) -> proto::NoFSegmentOwnerInfo {
    proto::NoFSegmentOwnerInfo {
        segment_id: Some(uuid_to_proto(owner.segment_id)),
        client_id: Some(uuid_to_proto(owner.client_id)),
    }
}

pub(crate) fn task_type_to_proto(task_type: TaskType) -> i32 {
    match task_type {
        TaskType::ReplicaCopy => proto::TaskType::ReplicaCopy as i32,
        TaskType::ReplicaMove => proto::TaskType::ReplicaMove as i32,
    }
}

pub(crate) fn task_status_to_proto(status: TaskStatus) -> i32 {
    match status {
        TaskStatus::Pending => proto::TaskStatus::TaskPending as i32,
        TaskStatus::Processing => proto::TaskStatus::TaskProcessing as i32,
        TaskStatus::Success => proto::TaskStatus::TaskSuccess as i32,
        TaskStatus::Failed => proto::TaskStatus::TaskFailed as i32,
    }
}

pub(crate) fn task_status_from_proto(status: i32) -> TaskStatus {
    match status {
        1 => TaskStatus::Processing,
        2 => TaskStatus::Success,
        3 => TaskStatus::Failed,
        _ => TaskStatus::Pending,
    }
}

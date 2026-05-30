//! # Protobuf ↔ Internal Type Conversions
//! ## Protobuf 与内部类型的双向转换 / Bidirectional conversion between protobuf and internal types
//!
//! 本模块负责 Mooncake 内部 Rust 类型与 gRPC proto 类型之间的转换。
//! 所有 `to_proto` 函数将内部类型序列化为 protobuf 消息（用于 gRPC 响应），
//! 所有 `from_proto` 函数将 protobuf 消息反序列化为内部类型（用于 gRPC 请求）。
//!
//! This module handles conversions between Mooncake internal Rust types and gRPC proto types.
//! All `to_proto` functions serialize internal types to protobuf messages (for gRPC responses),
//! and all `from_proto` functions deserialize protobuf messages to internal types (for gRPC requests).

use crate::proto;
use mooncake_store_core::{
    NoFSegment, NoFSegmentOwnerInfo, ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType,
    ReplicateConfig, TaskStatus, TaskType,
};
use uuid::Uuid;

// UUID 转换为 proto 格式（拆分为高64位和低64位）。
// Convert UUID to proto format (split into high 64 bits and low 64 bits).
pub(crate) fn uuid_to_proto(id: Uuid) -> proto::Uuid {
    let (high, low) = id.as_u64_pair();
    proto::Uuid { high, low }
}

// proto UUID 转回 Rust Uuid 类型。
// Convert proto UUID back to Rust Uuid type.
pub(crate) fn uuid_from_proto(p: &proto::Uuid) -> Uuid {
    Uuid::from_u64_pair(p.high, p.low)
}

/// 将内部 ReplicaDescriptor 序列化为 proto 格式，供 gRPC 响应使用。
/// Serialize internal ReplicaDescriptor to proto format for gRPC responses.
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
        transport_endpoint: r.segment_name.clone(),
        file_path: String::new(),
        object_size: r.size,
        local_disk_client_id: r.holder_client_id.map(uuid_to_proto),
        base_addr: r.base_addr,
    }
}

/// 从 proto 反序列化回 ReplicaDescriptor，status/replica_type 枚举值按 C++ 约定映射。
/// Deserialize from proto back to ReplicaDescriptor. Enum values mapped per C++ conventions:
///   status: 1=Allocating, 2=Written, 3=Complete, 4=Failed, _=Undefined
///   replica_type: 1=Disk, 2=LocalDisk, 3=NoFSsd, _=Memory (default)
pub(crate) fn replica_from_proto(p: &proto::ReplicaDescriptor) -> ReplicaDescriptor {
    ReplicaDescriptor {
        refcnt: 0,
        handle_valid: true,
        segment_id: p.segment_id.as_ref().map_or(Uuid::nil(), uuid_from_proto),
        segment_name: p.segment_name.clone(),
        offset: p.offset,
        size: p.size,
        base_addr: p.base_addr,
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

/// 将 proto ReplicateConfig 转换为内部类型，preferred_segment 向后兼容并合并到 preferred_segments。
/// Convert proto ReplicateConfig to internal type.
/// `preferred_segment` is merged into `preferred_segments` for backward compatibility.
pub(crate) fn config_from_proto(c: &proto::ReplicateConfig) -> ReplicateConfig {
    let preferred_segment = c.preferred_segment.clone();
    let preferred_segments = if !c.preferred_segments.is_empty() {
        c.preferred_segments.clone()
    } else if !preferred_segment.is_empty() {
        vec![preferred_segment.clone()]
    } else {
        vec![]
    };

    ReplicateConfig {
        replica_num: c.replica_num,
        nof_replica_num: c.nof_replica_num,
        with_soft_pin: c.with_soft_pin,
        with_hard_pin: c.with_hard_pin,
        preferred_segment,
        preferred_segments,
        preferred_nof_segments: c.preferred_nof_segments.clone(),
        prefer_alloc_in_same_node: c.prefer_alloc_in_same_node,
        data_type: match c.data_type {
            x if x == proto::ObjectDataType::Kvcache as i32 => ObjectDataType::Kvcache,
            x if x == proto::ObjectDataType::Tensor as i32 => ObjectDataType::Tensor,
            x if x == proto::ObjectDataType::Weight as i32 => ObjectDataType::Weight,
            x if x == proto::ObjectDataType::Sample as i32 => ObjectDataType::Sample,
            x if x == proto::ObjectDataType::Activation as i32 => ObjectDataType::Activation,
            x if x == proto::ObjectDataType::Gradient as i32 => ObjectDataType::Gradient,
            x if x == proto::ObjectDataType::OptimizerState as i32 => {
                ObjectDataType::OptimizerState
            }
            x if x == proto::ObjectDataType::Metadata as i32 => ObjectDataType::Metadata,
            x if x == proto::ObjectDataType::General as i32 => ObjectDataType::General,
            _ => ObjectDataType::Unknown,
        },
    }
}

/// 将内部 NoFSegment 转换为 proto 格式，包含传输端点和客户端信息。
/// Serialize internal NoFSegment to proto format, including transport endpoint and client info.
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

/// 从 proto 反序列化 NoFSegment，UUID 缺省时生成新 UUID（新挂载场景）。
/// Deserialize NoFSegment from proto. Generates a new UUID if missing (new mount scenario).
pub(crate) fn nof_segment_from_proto(segment: &proto::NoFSegment) -> NoFSegment {
    NoFSegment {
        id: segment.id.as_ref().map_or(Uuid::new_v4(), uuid_from_proto),
        name: segment.name.clone(),
        base: segment.base,
        size: segment.size,
        te_endpoint: segment.te_endpoint.clone(),
        client_id: segment
            .client_id
            .as_ref()
            .map_or(Uuid::nil(), uuid_from_proto),
    }
}

/// 将 NoF segment owner 信息转换为 proto 格式 / Serialize NoF segment owner info to proto.
pub(crate) fn nof_segment_owner_to_proto(
    owner: &NoFSegmentOwnerInfo,
) -> proto::NoFSegmentOwnerInfo {
    proto::NoFSegmentOwnerInfo {
        segment_id: Some(uuid_to_proto(owner.segment_id)),
        client_id: Some(uuid_to_proto(owner.client_id)),
    }
}

// TaskType/Status 枚举的 proto 转换，用于任务查询接口。
// Proto conversions for TaskType/TaskStatus enums, used in task query APIs.

/// TaskType 转换为 proto 枚举值 / Convert TaskType to proto enum value.
pub(crate) fn task_type_to_proto(task_type: TaskType) -> i32 {
    match task_type {
        TaskType::ReplicaCopy => proto::TaskType::ReplicaCopy as i32,
        TaskType::ReplicaMove => proto::TaskType::ReplicaMove as i32,
    }
}

/// TaskStatus 转换为 proto 枚举值 / Convert TaskStatus to proto enum value.
pub(crate) fn task_status_to_proto(status: TaskStatus) -> i32 {
    match status {
        TaskStatus::Pending => proto::TaskStatus::TaskPending as i32,
        TaskStatus::Processing => proto::TaskStatus::TaskProcessing as i32,
        TaskStatus::Success => proto::TaskStatus::TaskSuccess as i32,
        TaskStatus::Failed => proto::TaskStatus::TaskFailed as i32,
    }
}

/// 从 proto 枚举值还原 TaskStatus / Restore TaskStatus from proto enum value.
///   1=Processing, 2=Success, 3=Failed, _=Pending (default)
pub(crate) fn task_status_from_proto(status: i32) -> TaskStatus {
    match status {
        1 => TaskStatus::Processing,
        2 => TaskStatus::Success,
        3 => TaskStatus::Failed,
        _ => TaskStatus::Pending,
    }
}

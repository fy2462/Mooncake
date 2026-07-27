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

/// Decode a replica-type selector supplied by a client request.
///
/// Replica descriptors retain their historical compatibility fallback in
/// `ReplicaType::from_replica_wire`, but a mutation selector must fail closed:
/// treating an unknown value as Memory could complete, revoke, or evict the
/// wrong physical replica.
pub(crate) fn request_replica_type_from_i32(v: i32) -> Result<ReplicaType, tonic::Status> {
    ReplicaType::try_from(v)
        .map_err(|_| tonic::Status::invalid_argument(format!("invalid replica_type: {v}")))
}

/// 将 proto 的 data_type (i32) 转为内部 ObjectDataType，未知值回退为 Unknown。
/// Convert proto data_type (i32) to internal ObjectDataType; unknown values fall back to Unknown.
pub(crate) fn object_data_type_from_i32(v: i32) -> ObjectDataType {
    ObjectDataType::try_from(v).unwrap_or(ObjectDataType::Unknown)
}

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
        status: r.status.into(),
        replica_type: r.replica_type.into(),
        slice_key_hash: vec![],
        size: r.size,
        holder_client_id: r.holder_client_id.map(uuid_to_proto),
        transport_endpoint: r.segment_name.clone(),
        file_path: if r.replica_type == ReplicaType::Disk {
            r.segment_name.clone()
        } else {
            String::new()
        },
        object_size: r.size,
        local_disk_client_id: r.holder_client_id.map(uuid_to_proto),
        base_addr: r.base_addr,
        protocol: r.protocol.clone(),
        local_disk_storage_id: r.local_disk_storage_id.map(uuid_to_proto),
        local_disk_generation_id: r.local_disk_generation_id.map(uuid_to_proto),
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
        segment_name: if ReplicaType::from_replica_wire(p.replica_type) == ReplicaType::Disk
            && !p.file_path.is_empty()
        {
            p.file_path.clone()
        } else {
            p.segment_name.clone()
        },
        offset: p.offset,
        size: p.size,
        base_addr: p.base_addr,
        protocol: p.protocol.clone(),
        status: ReplicaStatus::from_replica_wire(p.status),
        replica_type: ReplicaType::from_replica_wire(p.replica_type),
        holder_client_id: p.holder_client_id.as_ref().map(uuid_from_proto),
        local_disk_storage_id: p.local_disk_storage_id.as_ref().map(uuid_from_proto),
        local_disk_generation_id: p.local_disk_generation_id.as_ref().map(uuid_from_proto),
    }
}

/// 将 proto ReplicateConfig 转换为内部类型，保留 preferred_segment 的 C++ 优先级语义。
/// Convert proto ReplicateConfig to internal type.
/// `preferred_segment` is preserved so allocation can prefer it over `preferred_segments`.
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
        data_type: object_data_type_from_i32(c.data_type),
        host_id: c.host_id.clone(),
        group_ids: c.group_ids.clone(),
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

/// 从 proto 反序列化 NoFSegment；缺失 UUID 保留为 nil，由 ReMount 边界拒绝。
/// Deserialize NoFSegment. A missing UUID remains nil so the ReMount boundary
/// can reject the request instead of inventing a non-durable identity.
pub(crate) fn nof_segment_from_proto(segment: &proto::NoFSegment) -> NoFSegment {
    NoFSegment {
        id: segment.id.as_ref().map_or(Uuid::nil(), uuid_from_proto),
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

/// TaskType 转换为 proto i32 / Convert TaskType to proto i32.
pub(crate) fn task_type_to_proto(task_type: TaskType) -> i32 {
    task_type.into()
}

/// TaskStatus 转换为 proto i32 / Convert TaskStatus to proto i32.
pub(crate) fn task_status_to_proto(status: TaskStatus) -> i32 {
    status.into()
}

/// Decode a task status supplied by a client mutation request.
pub(crate) fn request_task_status_from_i32(status: i32) -> Result<TaskStatus, tonic::Status> {
    TaskStatus::try_from(status)
        .map_err(|_| tonic::Status::invalid_argument(format!("invalid task status: {status}")))
}

#[cfg(test)]
mod tests {
    use super::{
        nof_segment_from_proto, replica_from_proto, replica_to_proto,
        request_replica_type_from_i32, request_task_status_from_i32,
    };
    use crate::proto;
    use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, TaskStatus};
    use uuid::Uuid;

    fn wire_replica(status: i32, replica_type: i32) -> proto::ReplicaDescriptor {
        proto::ReplicaDescriptor {
            segment_id: Some(proto::Uuid { high: 7, low: 11 }),
            segment_name: "segment-a".to_string(),
            offset: 13,
            status,
            replica_type,
            size: 17,
            base_addr: 19,
            protocol: "tcp".to_string(),
            holder_client_id: Some(proto::Uuid { high: 23, low: 29 }),
            ..Default::default()
        }
    }

    #[test]
    fn nof_segment_conversion_does_not_invent_missing_remount_identity() {
        let segment = nof_segment_from_proto(&proto::NoFSegment {
            name: "nof-a".to_string(),
            size: 4096,
            te_endpoint: "tcp://127.0.0.1:4420".to_string(),
            ..Default::default()
        });

        assert!(segment.id.is_nil());
    }

    #[test]
    fn request_replica_type_rejects_unknown_mutation_selector() {
        assert_eq!(
            request_replica_type_from_i32(ReplicaType::NoFSsd as i32).unwrap(),
            ReplicaType::NoFSsd
        );
        let error = request_replica_type_from_i32(99).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn request_task_status_rejects_unknown_mutation_value() {
        assert_eq!(
            request_task_status_from_i32(TaskStatus::Success as i32).unwrap(),
            TaskStatus::Success
        );
        let error = request_task_status_from_i32(99).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn replica_status_wire_mapping_preserves_cpp_fallback() {
        let cases = [
            (0, ReplicaStatus::Undefined),
            (1, ReplicaStatus::Allocating),
            (2, ReplicaStatus::Written),
            (3, ReplicaStatus::Complete),
            (4, ReplicaStatus::Failed),
            (-1, ReplicaStatus::Undefined),
            (99, ReplicaStatus::Undefined),
        ];

        for (wire, expected) in cases {
            assert_eq!(replica_from_proto(&wire_replica(wire, 0)).status, expected);
        }
    }

    #[test]
    fn replica_type_wire_mapping_preserves_cpp_fallback() {
        let cases = [
            (0, ReplicaType::Memory),
            (1, ReplicaType::Disk),
            (2, ReplicaType::LocalDisk),
            (3, ReplicaType::NoFSsd),
            (4, ReplicaType::Memory),
            (-1, ReplicaType::Memory),
            (99, ReplicaType::Memory),
        ];

        for (wire, expected) in cases {
            assert_eq!(
                replica_from_proto(&wire_replica(3, wire)).replica_type,
                expected
            );
        }
    }

    #[test]
    fn replica_proto_adapter_preserves_compatibility_alias_fields() {
        let holder = Uuid::from_u64_pair(31, 37);
        let replica = ReplicaDescriptor {
            segment_id: Uuid::from_u64_pair(41, 43),
            segment_name: "tcp://node-a:1234".to_string(),
            offset: 47,
            size: 53,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(holder),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 59,
            protocol: "rdma".to_string(),
        };

        let wire = replica_to_proto(&replica);
        assert_eq!(wire.status, 3);
        assert_eq!(wire.replica_type, 0);
        assert_eq!(wire.transport_endpoint, replica.segment_name);
        assert_eq!(wire.object_size, replica.size);
        assert_eq!(wire.local_disk_client_id, wire.holder_client_id);
        assert_eq!(wire.base_addr, replica.base_addr);
        assert_eq!(wire.protocol, replica.protocol);

        let round_trip = replica_from_proto(&wire);
        assert_eq!(round_trip.segment_id, replica.segment_id);
        assert_eq!(round_trip.segment_name, replica.segment_name);
        assert_eq!(round_trip.offset, replica.offset);
        assert_eq!(round_trip.size, replica.size);
        assert_eq!(round_trip.status, replica.status);
        assert_eq!(round_trip.replica_type, replica.replica_type);
        assert_eq!(round_trip.holder_client_id, replica.holder_client_id);
        assert_eq!(
            round_trip.local_disk_storage_id,
            replica.local_disk_storage_id
        );
        assert_eq!(round_trip.base_addr, replica.base_addr);
        assert_eq!(round_trip.protocol, replica.protocol);
    }

    #[test]
    fn disk_replica_round_trip_uses_file_path_as_authoritative_location() {
        let mut replica = ReplicaDescriptor {
            segment_id: Uuid::nil(),
            segment_name: "/shared/cluster/global-disk/aa/bb/object.data".to_string(),
            offset: 0,
            size: 64,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Disk,
            holder_client_id: None,
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: String::new(),
        };
        let wire = replica_to_proto(&replica);
        assert_eq!(wire.file_path, replica.segment_name);

        replica.segment_name = "/must/not/be/used".to_string();
        let mut wire_with_alias = replica_to_proto(&replica);
        wire_with_alias.segment_name = "legacy-segment-alias".to_string();
        assert_eq!(
            replica_from_proto(&wire_with_alias).segment_name,
            wire_with_alias.file_path
        );
    }
}

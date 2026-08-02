use super::{AllocatorSnapshotConfig, MemoryAllocatorKind, SegmentAllocator, SegmentLayout};
use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use uuid::Uuid;
use xxhash_rust::xxh64::xxh64;

const OFFSET_SNAPSHOT_MAGIC: &[u8; 8] = b"MCOSNAP1";
const OFFSET_SNAPSHOT_ENVELOPE_VERSION: u32 = 1;
const OFFSET_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const OFFSET_SNAPSHOT_HEADER_LEN: usize = 8 + 4 + 8 + 8;

#[derive(Debug, Serialize, Deserialize)]
struct OffsetSegmentSnapshotV1 {
    schema_version: u32,
    allocator_config: AllocatorSnapshotConfig,
    segment: Segment,
    client_id: Uuid,
    used: u64,
    allocations: Vec<OffsetSnapshotRange>,
    free_ranges: Vec<OffsetSnapshotRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct OffsetSnapshotRange {
    offset: u64,
    size: u64,
}

impl SegmentAllocator {
    /// Serialize one ordinary, runtime-bound Offset segment into the canonical
    /// Rust semantic snapshot format.
    pub fn serialize_offset_segment(&self, segment_id: &Uuid) -> Result<Vec<u8>, String> {
        let state = self
            .segments
            .get(segment_id)
            .ok_or_else(|| format!("segment {segment_id} does not exist"))?;
        if !state.runtime_bound {
            return Err(format!("segment {segment_id} is not runtime-bound"));
        }
        if state.segment.protocol == "cxl" {
            return Err("CXL aliases do not own an Offset segment layout".to_string());
        }
        let SegmentLayout::Offset(offset) = &state.layout else {
            return Err(format!("segment {segment_id} is not Offset-backed"));
        };

        let mut allocations = offset
            .allocations
            .iter()
            .map(|(&offset, &size)| OffsetSnapshotRange { offset, size })
            .collect::<Vec<_>>();
        allocations.sort_unstable_by_key(|range| range.offset);
        let free_ranges = offset
            .free_ranges
            .iter()
            .map(|&(offset, size)| OffsetSnapshotRange { offset, size })
            .collect::<Vec<_>>();
        let snapshot = OffsetSegmentSnapshotV1 {
            schema_version: OFFSET_SNAPSHOT_SCHEMA_VERSION,
            allocator_config: self.snapshot_config(),
            segment: state.segment.clone(),
            client_id: state.client_id,
            used: state.used,
            allocations,
            free_ranges,
        };
        validate_snapshot(&snapshot)?;
        encode_snapshot(&snapshot)
    }

    /// Restore one canonical Rust Offset snapshot and return descriptors that
    /// are rebound to every installed live allocation.
    pub fn restore_offset_segment_snapshot(
        &mut self,
        encoded: &[u8],
    ) -> Result<Vec<ReplicaDescriptor>, String> {
        let snapshot = decode_snapshot(encoded)?;
        validate_snapshot(&snapshot)?;
        if snapshot.allocator_config != self.snapshot_config() {
            return Err(format!(
                "snapshot allocator configuration {:?} does not match target {:?}",
                snapshot.allocator_config,
                self.snapshot_config()
            ));
        }
        if self.segments.contains_key(&snapshot.segment.id) {
            return Err(format!("segment {} already exists", snapshot.segment.id));
        }

        let descriptors = snapshot
            .allocations
            .iter()
            .map(|range| ReplicaDescriptor {
                segment_id: snapshot.segment.id,
                segment_name: snapshot.segment.name.clone(),
                offset: range.offset,
                size: range.size,
                status: ReplicaStatus::Complete,
                replica_type: ReplicaType::Memory,
                holder_client_id: Some(snapshot.client_id),
                local_disk_storage_id: None,
                local_disk_generation_id: None,
                refcnt: 0,
                handle_valid: true,
                base_addr: snapshot.segment.base,
                protocol: snapshot.segment.protocol.clone(),
            })
            .collect::<Vec<_>>();
        let segment_id = snapshot.segment.id;
        self.restore_segment(snapshot.segment, snapshot.client_id, &descriptors)?;

        let canonical = self.serialize_offset_segment(&segment_id);
        match canonical {
            Ok(canonical) if canonical == encoded => Ok(descriptors),
            Ok(_) => {
                self.remove_segment(&segment_id);
                Err("restored Offset snapshot is not canonical".to_string())
            }
            Err(error) => {
                self.remove_segment(&segment_id);
                Err(format!(
                    "restored Offset snapshot failed post-install validation: {error}"
                ))
            }
        }
    }
}

fn encode_snapshot(snapshot: &OffsetSegmentSnapshotV1) -> Result<Vec<u8>, String> {
    let payload = rmp_serde::to_vec_named(snapshot)
        .map_err(|error| format!("Offset snapshot serialization failed: {error}"))?;
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| "Offset snapshot payload length exceeds u64".to_string())?;
    let mut encoded = Vec::with_capacity(
        OFFSET_SNAPSHOT_HEADER_LEN
            .checked_add(payload.len())
            .ok_or_else(|| "Offset snapshot encoded length overflow".to_string())?,
    );
    encoded.extend_from_slice(OFFSET_SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&OFFSET_SNAPSHOT_ENVELOPE_VERSION.to_le_bytes());
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&xxh64(&payload, 0).to_le_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_snapshot(encoded: &[u8]) -> Result<OffsetSegmentSnapshotV1, String> {
    if encoded.len() < OFFSET_SNAPSHOT_HEADER_LEN {
        return Err("Offset snapshot is shorter than its envelope".to_string());
    }
    if &encoded[..OFFSET_SNAPSHOT_MAGIC.len()] != OFFSET_SNAPSHOT_MAGIC {
        return Err("Offset snapshot magic mismatch".to_string());
    }
    let envelope_version = u32::from_le_bytes(
        encoded[8..12]
            .try_into()
            .map_err(|_| "Offset snapshot envelope version is truncated".to_string())?,
    );
    if envelope_version != OFFSET_SNAPSHOT_ENVELOPE_VERSION {
        return Err(format!(
            "unsupported Offset snapshot envelope version {envelope_version}"
        ));
    }
    let payload_len_u64 = u64::from_le_bytes(
        encoded[12..20]
            .try_into()
            .map_err(|_| "Offset snapshot payload length is truncated".to_string())?,
    );
    let payload_len = usize::try_from(payload_len_u64)
        .map_err(|_| "Offset snapshot payload length exceeds usize".to_string())?;
    let expected_len = OFFSET_SNAPSHOT_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| "Offset snapshot total length overflow".to_string())?;
    if encoded.len() != expected_len {
        return Err(format!(
            "Offset snapshot length mismatch: encoded={}, expected={expected_len}",
            encoded.len()
        ));
    }
    let expected_checksum = u64::from_le_bytes(
        encoded[20..28]
            .try_into()
            .map_err(|_| "Offset snapshot checksum is truncated".to_string())?,
    );
    let payload = &encoded[OFFSET_SNAPSHOT_HEADER_LEN..];
    let actual_checksum = xxh64(payload, 0);
    if actual_checksum != expected_checksum {
        return Err("Offset snapshot checksum mismatch".to_string());
    }

    let mut cursor = Cursor::new(payload);
    let mut deserializer = rmp_serde::Deserializer::new(&mut cursor);
    let snapshot = OffsetSegmentSnapshotV1::deserialize(&mut deserializer)
        .map_err(|error| format!("Offset snapshot payload decoding failed: {error}"))?;
    drop(deserializer);
    if cursor.position() != payload_len_u64 {
        return Err("Offset snapshot payload contains trailing bytes".to_string());
    }
    Ok(snapshot)
}

fn validate_snapshot(snapshot: &OffsetSegmentSnapshotV1) -> Result<(), String> {
    if snapshot.schema_version != OFFSET_SNAPSHOT_SCHEMA_VERSION {
        return Err(format!(
            "unsupported Offset snapshot schema version {}",
            snapshot.schema_version
        ));
    }
    if snapshot.allocator_config.memory_allocator_kind != MemoryAllocatorKind::Offset {
        return Err("Offset snapshot uses a non-Offset allocator configuration".to_string());
    }
    if snapshot.allocator_config.offset_max_allocation_nodes == Some(0) {
        return Err("Offset snapshot maximum allocation node count must be positive".to_string());
    }
    if snapshot.segment.id.is_nil()
        || snapshot.segment.name.is_empty()
        || snapshot.segment.size == 0
        || snapshot.client_id.is_nil()
    {
        return Err("Offset snapshot has an incomplete segment identity".to_string());
    }
    if snapshot.segment.protocol == "cxl" {
        return Err("Offset snapshot cannot contain a CXL alias".to_string());
    }
    if snapshot.segment.base == 0
        || snapshot.segment.te_endpoint.is_empty()
        || snapshot.segment.protocol.is_empty()
    {
        return Err("Offset snapshot has an incomplete runtime transport identity".to_string());
    }
    snapshot
        .segment
        .base
        .checked_add(snapshot.segment.size)
        .ok_or_else(|| "Offset snapshot segment address range overflows".to_string())?;

    validate_sorted_ranges("allocation", &snapshot.allocations, snapshot.segment.size)?;
    validate_sorted_ranges("free", &snapshot.free_ranges, snapshot.segment.size)?;

    let used = snapshot
        .allocations
        .iter()
        .try_fold(0_u64, |total, range| total.checked_add(range.size))
        .ok_or_else(|| "Offset snapshot allocated-byte sum overflow".to_string())?;
    if used != snapshot.used {
        return Err(format!(
            "Offset snapshot used bytes {} do not match live allocation sum {used}",
            snapshot.used
        ));
    }

    let active_nodes = u64::try_from(snapshot.allocations.len())
        .map_err(|_| "Offset snapshot allocation count exceeds u64".to_string())?
        .checked_add(
            u64::try_from(snapshot.free_ranges.len())
                .map_err(|_| "Offset snapshot free-range count exceeds u64".to_string())?,
        )
        .ok_or_else(|| "Offset snapshot partition count overflow".to_string())?;
    if snapshot
        .allocator_config
        .offset_max_allocation_nodes
        .is_some_and(|maximum| active_nodes > maximum)
    {
        return Err("Offset snapshot partition count exceeds configured maximum".to_string());
    }

    let mut partitions = snapshot
        .allocations
        .iter()
        .map(|range| (range.offset, range.size, true))
        .chain(
            snapshot
                .free_ranges
                .iter()
                .map(|range| (range.offset, range.size, false)),
        )
        .collect::<Vec<_>>();
    partitions.sort_unstable_by_key(|&(offset, _, _)| offset);
    let mut cursor = 0_u64;
    let mut previous_was_free = false;
    for (offset, size, allocated) in partitions {
        if offset != cursor {
            return Err(format!(
                "Offset snapshot partition starts at {offset}, expected {cursor}"
            ));
        }
        if !allocated && previous_was_free {
            return Err("Offset snapshot contains adjacent free ranges".to_string());
        }
        cursor = cursor
            .checked_add(size)
            .ok_or_else(|| "Offset snapshot partition end overflow".to_string())?;
        previous_was_free = !allocated;
    }
    if cursor != snapshot.segment.size {
        return Err(format!(
            "Offset snapshot partitions cover {cursor} bytes, expected {}",
            snapshot.segment.size
        ));
    }
    Ok(())
}

fn validate_sorted_ranges(
    kind: &str,
    ranges: &[OffsetSnapshotRange],
    capacity: u64,
) -> Result<(), String> {
    let mut previous_offset = None;
    for range in ranges {
        if range.size == 0 {
            return Err(format!(
                "Offset snapshot contains a zero-sized {kind} range"
            ));
        }
        if previous_offset.is_some_and(|previous| range.offset <= previous) {
            return Err(format!(
                "Offset snapshot {kind} ranges are not strictly ordered"
            ));
        }
        let end = range
            .offset
            .checked_add(range.size)
            .ok_or_else(|| format!("Offset snapshot {kind} range end overflow"))?;
        if end > capacity {
            return Err(format!(
                "Offset snapshot {kind} range [{}, {end}) exceeds capacity {capacity}",
                range.offset
            ));
        }
        previous_offset = Some(range.offset);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(id: Uuid, name: &str, base: u64, size: u64) -> Segment {
        Segment {
            id,
            name: name.to_string(),
            base,
            size,
            te_endpoint: "127.0.0.1:12345".to_string(),
            protocol: "tcp".to_string(),
            host_id: "snapshot-test-host".to_string(),
        }
    }

    fn valid_snapshot() -> OffsetSegmentSnapshotV1 {
        let allocator = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(32))
            .expect("valid Offset node budget");
        OffsetSegmentSnapshotV1 {
            schema_version: OFFSET_SNAPSHOT_SCHEMA_VERSION,
            allocator_config: allocator.snapshot_config(),
            segment: segment(Uuid::new_v4(), "incoming", 16 * 1024, 1_024),
            client_id: Uuid::new_v4(),
            used: 200,
            allocations: vec![
                OffsetSnapshotRange {
                    offset: 0,
                    size: 100,
                },
                OffsetSnapshotRange {
                    offset: 200,
                    size: 100,
                },
            ],
            free_ranges: vec![
                OffsetSnapshotRange {
                    offset: 100,
                    size: 100,
                },
                OffsetSnapshotRange {
                    offset: 300,
                    size: 724,
                },
            ],
        }
    }

    fn envelope(payload: &[u8], version: u32) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(OFFSET_SNAPSHOT_HEADER_LEN + payload.len());
        encoded.extend_from_slice(OFFSET_SNAPSHOT_MAGIC);
        encoded.extend_from_slice(&version.to_le_bytes());
        encoded.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&xxh64(payload, 0).to_le_bytes());
        encoded.extend_from_slice(payload);
        encoded
    }

    fn assert_restore_rejected_atomically(
        encoded: &[u8],
        incoming_id: Uuid,
        target_maximum_nodes: Option<u64>,
    ) {
        let unrelated_id = Uuid::new_v4();
        let mut target = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(target_maximum_nodes)
            .expect("valid target Offset node budget");
        target
            .try_add_segment(
                segment(unrelated_id, "unrelated", 32 * 1024, 512),
                0,
                Uuid::new_v4(),
            )
            .expect("unrelated target segment");
        target
            .allocate_from_segment_id(unrelated_id, 64)
            .expect("unrelated live allocation");
        let unrelated_before = target
            .offset_allocator_report(&unrelated_id)
            .expect("unrelated report before rejection");

        assert!(target.restore_offset_segment_snapshot(encoded).is_err());
        assert!(target.offset_allocator_report(&incoming_id).is_none());
        assert_eq!(
            target
                .offset_allocator_report(&unrelated_id)
                .expect("unrelated segment survives rejection"),
            unrelated_before
        );
    }

    #[test]
    fn runtime_bound_snapshot_rejects_invalid_transport_identity_atomically() {
        const MAXIMUM_NODES: u64 = 32;
        let incoming_id = Uuid::new_v4();
        let mut source = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
            .expect("valid Offset node budget");
        source
            .try_add_segment(
                segment(incoming_id, "incoming", 16 * 1024, 1_024),
                0,
                Uuid::new_v4(),
            )
            .expect("source segment");
        let encoded = source
            .serialize_offset_segment(&incoming_id)
            .expect("valid source snapshot");

        for mutation in [
            "zero-base",
            "empty-endpoint",
            "empty-protocol",
            "address-overflow",
        ] {
            let mut snapshot = decode_snapshot(&encoded).expect("decode valid fixture");
            match mutation {
                "zero-base" => snapshot.segment.base = 0,
                "empty-endpoint" => snapshot.segment.te_endpoint.clear(),
                "empty-protocol" => snapshot.segment.protocol.clear(),
                "address-overflow" => snapshot.segment.base = u64::MAX,
                _ => unreachable!(),
            }
            let mutated = encode_snapshot(&snapshot).expect("encode checksum-valid mutation");

            let unrelated_id = Uuid::new_v4();
            let mut target = SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
                .expect("valid Offset node budget");
            target
                .try_add_segment(
                    segment(unrelated_id, "unrelated", 32 * 1024, 512),
                    0,
                    Uuid::new_v4(),
                )
                .expect("unrelated target segment");
            let unrelated_before = target
                .offset_allocator_report(&unrelated_id)
                .expect("unrelated report before rejection");

            assert!(
                target.restore_offset_segment_snapshot(&mutated).is_err(),
                "{mutation} snapshot must be rejected"
            );
            assert!(target.offset_allocator_report(&incoming_id).is_none());
            assert_eq!(
                target
                    .offset_allocator_report(&unrelated_id)
                    .expect("unrelated segment survives rejection"),
                unrelated_before
            );
        }
    }

    #[test]
    fn snapshot_envelope_rejects_corruption_versions_and_trailing_payload_atomically() {
        let snapshot = valid_snapshot();
        let encoded = encode_snapshot(&snapshot).expect("valid snapshot");
        let mut cases = Vec::new();

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        cases.push(bad_magic);

        let mut bad_envelope_version = encoded.clone();
        bad_envelope_version[8..12].copy_from_slice(&2_u32.to_le_bytes());
        cases.push(bad_envelope_version);

        let mut bad_checksum = encoded.clone();
        bad_checksum[20] ^= 0xff;
        cases.push(bad_checksum);

        let mut bad_schema = valid_snapshot();
        bad_schema.schema_version = 2;
        cases.push(encode_snapshot(&bad_schema).expect("unknown-schema envelope"));

        let mut payload = rmp_serde::to_vec_named(&snapshot).expect("named payload");
        payload.push(0);
        cases.push(envelope(&payload, OFFSET_SNAPSHOT_ENVELOPE_VERSION));

        for corrupted in cases {
            assert_restore_rejected_atomically(&corrupted, snapshot.segment.id, Some(32));
        }
    }

    #[test]
    fn snapshot_semantics_reject_invalid_ranges_accounting_and_configuration_atomically() {
        for mutation in [
            "cachelib-config",
            "zero-node-budget",
            "nil-segment-id",
            "empty-segment-name",
            "zero-capacity",
            "nil-client",
            "cxl-protocol",
            "zero-allocation",
            "unordered-allocations",
            "allocation-out-of-bounds",
            "zero-free-range",
            "unordered-free-ranges",
            "free-range-out-of-bounds",
            "wrong-used",
            "node-budget-exceeded",
            "partition-gap",
            "partition-overlap",
            "adjacent-free-ranges",
        ] {
            let mut snapshot = valid_snapshot();
            let target_maximum = match mutation {
                "node-budget-exceeded" => Some(3),
                _ => Some(32),
            };
            match mutation {
                "cachelib-config" => {
                    snapshot.allocator_config.memory_allocator_kind =
                        MemoryAllocatorKind::CachelibLike;
                }
                "zero-node-budget" => {
                    snapshot.allocator_config.offset_max_allocation_nodes = Some(0);
                }
                "nil-segment-id" => snapshot.segment.id = Uuid::nil(),
                "empty-segment-name" => snapshot.segment.name.clear(),
                "zero-capacity" => snapshot.segment.size = 0,
                "nil-client" => snapshot.client_id = Uuid::nil(),
                "cxl-protocol" => snapshot.segment.protocol = "cxl".to_string(),
                "zero-allocation" => snapshot.allocations[0].size = 0,
                "unordered-allocations" => snapshot.allocations.swap(0, 1),
                "allocation-out-of-bounds" => {
                    snapshot.allocations[1] = OffsetSnapshotRange {
                        offset: 1_000,
                        size: 100,
                    };
                }
                "zero-free-range" => snapshot.free_ranges[0].size = 0,
                "unordered-free-ranges" => snapshot.free_ranges.swap(0, 1),
                "free-range-out-of-bounds" => {
                    snapshot.free_ranges[1] = OffsetSnapshotRange {
                        offset: 1_000,
                        size: 100,
                    };
                }
                "wrong-used" => snapshot.used = 199,
                "node-budget-exceeded" => {
                    snapshot.allocator_config.offset_max_allocation_nodes = Some(3);
                }
                "partition-gap" => snapshot.free_ranges[1].size = 723,
                "partition-overlap" => {
                    snapshot.free_ranges[0] = OffsetSnapshotRange {
                        offset: 99,
                        size: 101,
                    };
                }
                "adjacent-free-ranges" => {
                    snapshot.used = 100;
                    snapshot.allocations.truncate(1);
                    snapshot.free_ranges = vec![
                        OffsetSnapshotRange {
                            offset: 100,
                            size: 100,
                        },
                        OffsetSnapshotRange {
                            offset: 200,
                            size: 824,
                        },
                    ];
                }
                _ => unreachable!(),
            }
            let incoming_id = snapshot.segment.id;
            let encoded = encode_snapshot(&snapshot).expect("checksum-valid semantic mutation");
            assert_restore_rejected_atomically(&encoded, incoming_id, target_maximum);
        }

        let mut overflow = valid_snapshot();
        overflow.segment.base = 1;
        overflow.segment.size = u64::MAX - 1;
        overflow.used = u64::MAX;
        overflow.allocations = vec![
            OffsetSnapshotRange {
                offset: 0,
                size: u64::MAX - 1,
            },
            OffsetSnapshotRange {
                offset: 1,
                size: u64::MAX - 2,
            },
        ];
        overflow.free_ranges.clear();
        let incoming_id = overflow.segment.id;
        assert_restore_rejected_atomically(
            &encode_snapshot(&overflow).expect("checksum-valid overflow mutation"),
            incoming_id,
            Some(32),
        );
    }

    #[test]
    fn snapshot_restore_rejects_source_and_target_conflicts_and_rebinds_exact_identity() {
        let missing = SegmentAllocator::new();
        assert!(missing.serialize_offset_segment(&Uuid::new_v4()).is_err());

        let cachelib_id = Uuid::new_v4();
        let mut cachelib =
            SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
        cachelib.add_segment(
            segment(cachelib_id, "cachelib", 16 * 1024, 1 << 24),
            0,
            Uuid::new_v4(),
        );
        assert!(cachelib.serialize_offset_segment(&cachelib_id).is_err());

        let cxl_id = Uuid::new_v4();
        let mut cxl = SegmentAllocator::new().with_cxl_capacity(1 << 24);
        let mut cxl_alias = segment(cxl_id, "cxl-alias", 0, 1 << 24);
        cxl_alias.protocol = "cxl".to_string();
        cxl.add_segment(cxl_alias, 0, Uuid::new_v4());
        assert!(cxl.serialize_offset_segment(&cxl_id).is_err());

        let unbound_id = Uuid::new_v4();
        let mut unbound = SegmentAllocator::new();
        unbound.add_segment(
            segment(unbound_id, "unbound", 16 * 1024, 1_024),
            0,
            Uuid::new_v4(),
        );
        unbound
            .invalidate_segment_runtime(&unbound_id)
            .expect("invalidate source runtime");
        assert!(unbound.serialize_offset_segment(&unbound_id).is_err());

        let snapshot = valid_snapshot();
        let encoded = encode_snapshot(&snapshot).expect("valid conflict fixture");
        assert_restore_rejected_atomically(&encoded, snapshot.segment.id, Some(33));

        let mut duplicate_target = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(32))
            .expect("valid target Offset node budget");
        duplicate_target
            .try_add_segment(snapshot.segment.clone(), 0, snapshot.client_id)
            .expect("preexisting duplicate segment");
        duplicate_target
            .allocate_from_segment_id(snapshot.segment.id, 64)
            .expect("preexisting duplicate allocation");
        let duplicate_before = duplicate_target
            .offset_allocator_report(&snapshot.segment.id)
            .expect("duplicate report before rejection");
        assert!(
            duplicate_target
                .restore_offset_segment_snapshot(&encoded)
                .is_err()
        );
        assert_eq!(
            duplicate_target
                .offset_allocator_report(&snapshot.segment.id)
                .expect("duplicate remains intact"),
            duplicate_before
        );

        let tuple_payload = rmp_serde::to_vec(&snapshot).expect("tuple-form payload");
        let noncanonical = envelope(&tuple_payload, OFFSET_SNAPSHOT_ENVELOPE_VERSION);
        assert_ne!(noncanonical, encoded);
        let decoded_noncanonical =
            decode_snapshot(&noncanonical).expect("tuple form decodes semantically");
        validate_snapshot(&decoded_noncanonical).expect("tuple form passes pre-install validation");
        assert_restore_rejected_atomically(&noncanonical, snapshot.segment.id, Some(32));

        let mut target = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(32))
            .expect("valid target Offset node budget");
        let rebound = target
            .restore_offset_segment_snapshot(&encoded)
            .expect("canonical restore");
        assert_eq!(rebound.len(), 2);
        for (replica, expected_offset) in rebound.iter().zip([0_u64, 200]) {
            assert_eq!(replica.segment_id, snapshot.segment.id);
            assert_eq!(replica.segment_name, snapshot.segment.name);
            assert_eq!(replica.offset, expected_offset);
            assert_eq!(replica.size, 100);
            assert_eq!(replica.status, ReplicaStatus::Complete);
            assert_eq!(replica.replica_type, ReplicaType::Memory);
            assert_eq!(replica.holder_client_id, Some(snapshot.client_id));
            assert_eq!(replica.local_disk_storage_id, None);
            assert_eq!(replica.local_disk_generation_id, None);
            assert_eq!(replica.refcnt, 0);
            assert!(replica.handle_valid);
            assert_eq!(replica.base_addr, snapshot.segment.base);
            assert_eq!(replica.protocol, snapshot.segment.protocol);
        }
    }
}

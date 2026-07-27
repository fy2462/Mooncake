use super::oplog_wire::{
    compute_cpp_checksum, compute_cpp_prefix_hash, decode_record_payload_value,
    deserialize_etcd_oplog_value, encode_put_end_msgpack_from_json, serialize_etcd_oplog_value,
    validate_record_size, validate_wire_entry_size,
};
use super::*;

pub const TEST_CPP_OP_PUT_END: u8 = CPP_OP_PUT_END;
pub const TEST_CPP_OP_PUT_REVOKE: u8 = CPP_OP_PUT_REVOKE;
pub const TEST_CPP_OP_REMOVE: u8 = CPP_OP_REMOVE;
pub const TEST_MAX_OBJECT_KEY_SIZE: usize = MAX_OBJECT_KEY_SIZE;
pub const TEST_MAX_PAYLOAD_SIZE: usize = MAX_PAYLOAD_SIZE;
pub const TEST_PUT_END_MSGPACK_MAGIC: &[u8] = PUT_END_MSGPACK_MAGIC_V2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CppWireTestEntry {
    pub sequence_id: u64,
    pub timestamp_ms: u64,
    pub op_type: u8,
    pub object_key: String,
    pub payload: String,
    pub checksum: u32,
    pub prefix_hash: u32,
}

impl From<CppWireTestEntry> for CppOpLogWireEntry {
    fn from(entry: CppWireTestEntry) -> Self {
        Self {
            sequence_id: entry.sequence_id,
            timestamp_ms: entry.timestamp_ms,
            op_type: entry.op_type,
            object_key: entry.object_key,
            payload: entry.payload,
            checksum: entry.checksum,
            prefix_hash: entry.prefix_hash,
        }
    }
}

impl From<CppOpLogWireEntry> for CppWireTestEntry {
    fn from(entry: CppOpLogWireEntry) -> Self {
        Self {
            sequence_id: entry.sequence_id,
            timestamp_ms: entry.timestamp_ms,
            op_type: entry.op_type,
            object_key: entry.object_key,
            payload: entry.payload,
            checksum: entry.checksum,
            prefix_hash: entry.prefix_hash,
        }
    }
}

pub fn serialize_etcd_value_for_test(entry: &OpLogRecord) -> Result<String, HaError> {
    serialize_etcd_oplog_value(entry)
}

pub fn deserialize_etcd_value_for_test(value: &str) -> Result<OpLogRecord, HaError> {
    deserialize_etcd_oplog_value(value)
}

pub fn decode_record_payload_value_for_test(payload: &str) -> Result<serde_json::Value, HaError> {
    decode_record_payload_value(payload)
}

pub fn validate_record_size_for_test(entry: &OpLogRecord) -> Result<(), HaError> {
    validate_record_size(entry)
}

pub fn validate_wire_entry_size_for_test(entry: CppWireTestEntry) -> Result<(), HaError> {
    validate_wire_entry_size(&entry.into())
}

pub fn compute_cpp_checksum_for_test(payload: &[u8]) -> u32 {
    compute_cpp_checksum(payload)
}

pub fn compute_cpp_prefix_hash_for_test(key: &str) -> u32 {
    compute_cpp_prefix_hash(key)
}

pub fn encode_put_end_msgpack_from_json_for_test(
    payload: &serde_json::Value,
) -> Result<Vec<u8>, HaError> {
    encode_put_end_msgpack_from_json(payload)
}

pub fn format_etcd_entry_key_for_test(key_prefix: &str, seq: u64) -> String {
    EtcdOpLogStore::format_entry_key(key_prefix, seq)
}

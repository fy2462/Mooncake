// Each integration test binary compiles this module independently but uses a
// different subset of helpers. `allow(dead_code)` is the Rust convention for
// shared test helpers — the compiler cannot see cross-test-file usage.
#![allow(dead_code)]
use mooncake_store_core::Segment;
use std::path::PathBuf;
use uuid::Uuid;

/// Convert a `uuid::Uuid` to a proto `Uuid`.
pub fn proto_uuid(id: Uuid) -> mooncake_store_master::proto::Uuid {
    let (high, low) = id.as_u64_pair();
    mooncake_store_master::proto::Uuid { high, low }
}

/// Create a temporary directory with a new random subdirectory inside.
pub fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mooncake-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Create a `Segment` for test use, with default `base`=0, `te_endpoint`="", `protocol`="tcp".
pub fn make_seg(name: &str, size: u64) -> Segment {
    Segment {
        id: Uuid::new_v4(),
        name: name.to_string(),
        size,
        base: 0,
        te_endpoint: String::new(),
        protocol: "tcp".to_string(),
        host_id: String::new(),
    }
}

/// Convenience: create a segment and return (segment, used, client_id) for `add_segment`.
pub fn make_seg_with_usage(name: &str, size: u64, used: u64) -> (Segment, u64, Uuid) {
    (make_seg(name, size), used, Uuid::new_v4())
}

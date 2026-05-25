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

/// Create a `Segment` for test use.
pub fn make_seg(name: &str, size: u64, used: u64) -> Segment {
    Segment {
        id: Uuid::new_v4(),
        name: name.to_string(),
        size,
        used,
        client_id: Uuid::new_v4(),
    }
}

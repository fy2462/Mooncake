pub mod allocator;
pub mod count_min_sketch;
pub mod eviction;
pub mod ha;
pub mod hf3fs;
pub mod http_metadata;
pub mod metrics;
pub mod oplog;
pub mod service;
pub mod storage_backend;

pub use service::{MasterRuntimeConfig, MasterServiceImpl};

// Generated protobuf code — compiled by build.rs from proto/mooncake_store.proto
pub mod proto {
    tonic::include_proto!("mooncake.store");
}

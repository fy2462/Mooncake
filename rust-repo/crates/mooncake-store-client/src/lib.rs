pub mod client;
pub mod hot_cache;

pub use client::{BufferHandle, MooncakeClient};
pub use hot_cache::LocalHotCache;

// Generated protobuf code
pub(crate) mod proto {
    tonic::include_proto!("mooncake.store");
}

pub mod buffer_allocator;
pub mod client;
pub mod engram;
pub mod hot_cache;

pub use client::{BufferHandle, MooncakeClient};
pub use engram::{EngramStore, EngramStoreConfig};
pub use hot_cache::LocalHotCache;

// Generated protobuf code
pub mod proto {
    tonic::include_proto!("mooncake.store");
}

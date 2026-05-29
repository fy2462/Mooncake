pub mod buffer_allocator;
pub mod client;
pub mod engram;
pub mod hot_cache;
pub mod remote_source;

pub use client::{BufferHandle, MooncakeClient};
pub use engram::{EngramStore, EngramStoreConfig};
pub use hot_cache::LocalHotCache;
pub use remote_source::{
    config::{RemoteSourceConfig, S3Config},
    distributed::DistributedMissHandler,
    error::{RemoteSourceError, RemoteSourceResult},
    local_fs::LocalFsSource,
    miss_handler::{MissHandler, MissHandlerSnapshot, MissHandlerStats},
    RemoteSource,
};

#[cfg(feature = "s3")]
pub use remote_source::s3_source::S3RemoteSource;

// Generated protobuf code
pub mod proto {
    tonic::include_proto!("mooncake.store");
}

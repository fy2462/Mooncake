//! # Mooncake Store Client
//!
//! 客户端核心库，提供高性能分布式对象存储的客户端接口。
//! (Client-side core library providing high-performance distributed object storage APIs.)
//!
//! ## 模块结构 (Module Structure)
//!
//! | 模块 (Module) | 职责 (Role) |
//! |---|---|
//! | [`client`] | MooncakeClient — 分布式存储客户端主体，负责读写、传输、删除等操作 |
//! | [`buffer_allocator`] | ClientBufferAllocator — 基于 offset 的子分配器，管理 RDMA 缓冲区 |
//! | [`engram`] | EngramStore — 面向 ML 嵌入表查找的高层抽象 (Embedding Table Lookup) |
//! | [`hot_cache`] | LocalHotCache — 本地 LRU 热缓存，减少远程获取延迟 |
//! | [`remote_source`] | RemoteSource trait + 多种实现 (S3 / 本地文件系统 / 分布式协调) |
//!
//! ## 公共导出 (Public Exports)
//!
//! - **客户端主体 (Client Core):** [`MooncakeClient`], [`BufferHandle`]
//! - **嵌入存储 (Embedding Store):** [`EngramStore`], [`EngramStoreConfig`]
//! - **热缓存 (Hot Cache):** [`LocalHotCache`]
//! - **远程源抽象 (Remote Source Abstraction):**
//!   - Trait: [`RemoteSource`]
//!   - 错误类型 (Error types): [`RemoteSourceError`], [`RemoteSourceResult`]
//!   - 本地文件系统 (Local FS): [`LocalFsSource`]
//!   - S3 (需 feature flag): [`S3RemoteSource`]
//!   - 分布式协调 (Distributed): [`DistributedMissHandler`]
//! - **未命中处理 (Miss Handling):** [`MissHandler`], [`MissHandlerSnapshot`], [`MissHandlerStats`]
//! - **配置 (Configuration):** [`RemoteSourceConfig`], [`S3Config`]
//!
//! ## 使用示例 (Quick Start)
//!
//! ```ignore
//! use mooncake_store_client::{MooncakeClient, RemoteSourceConfig, MissHandler, LocalHotCache};
//! use std::sync::Arc;
//!
//! // 创建客户端并连接 etcd 集群
//! // Create client and connect to etcd cluster
//! let client = MooncakeClient::new(/* ... */).await?;
//!
//! // 配置热缓存 + 远程源回退
//! // Configure hot cache with remote source fallback
//! let hot_cache = Arc::new(LocalHotCache::default());
//! let config = RemoteSourceConfig { enabled: true, ..Default::default() };
//! ```
//!
//! ## 特性开关 (Feature Flags)
//!
//! - `s3`: 启用 AWS S3 远程源支持 (enables S3RemoteSource via `aws-sdk-s3`)

pub mod buffer_allocator;
pub mod client;
pub mod dummy;
#[cfg(test)]
mod dummy_tests;
pub mod engram;
pub mod hot_cache;
pub mod local_storage_backend;
pub mod offload;
pub mod remote_source;

pub use client::{
    BufferHandle, CachedQueryResultResponse, ClientBackgroundConfig, ClientBackgroundHandle,
    MooncakeClient, OffloadTaskItem, PromotionTaskItem, SegmentDetail,
};
pub use dummy::{
    DummyIpcChannel, DummyMemoryPool, ShmFdRequest, ShmFdResponse, ShmRegisterRequest,
    INVALID_PHYSICAL_DEVICE_ID, IPC_SHM_FD_REQUEST, IPC_SHM_REGISTER, SHM_SEG_HOT_CACHE,
};
pub use engram::{EngramStore, EngramStoreConfig};
pub use hot_cache::LocalHotCache;
pub use local_storage_backend::{LocalStorageBackend, LocalStorageConfig};
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
// 自动生成的 Protobuf 代码（mooncake.store 包）
pub mod proto {
    tonic::include_proto!("mooncake.store");
}

// Generated protobuf code for P2P offload RPC
pub mod offload_proto {
    tonic::include_proto!("mooncake.offload");
}

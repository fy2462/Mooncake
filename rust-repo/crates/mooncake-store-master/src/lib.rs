//! # Mooncake Store Master — Crate Overview
//! ## 模块总览 / Module Overview
//!
//! mooncake-store-master 是 Mooncake 分布式 KV 缓存系统的**主控节点 (Master Service)**。
//! Master 负责全局元数据管理，包括 segment 注册、对象副本分配、客户端存活监控、
//! 副本复制/迁移协调、后台驱逐和快照持久化。
//!
//! mooncake-store-master is the **control-plane node** of the Mooncake distributed KV cache.
//! The Master owns global metadata: segment registry, object replica allocation,
//! client liveness monitoring, replica copy/move coordination, background eviction,
//! and snapshot persistence.
//!
//! ## 模块结构 / Module Structure
//!
//! | 模块 / Module | 职责 / Purpose |
//! |---------------|----------------|
//! | `allocator`   | Segment 内存分配器，支持 Random / FreeRatioFirst / SsdFreeRatioFirst / LocalFirst 策略 |
//! | `count_min_sketch` | Count-Min Sketch 频率统计，用于 promotion 准入控制 |
//! | `eviction`    | LRU 驱逐管理器，按 last_access + soft_pin + lease 选择驱逐候选 |
//! | `ha`          | 高可用 (HA) 支持：Leader 选举、热备、etcd/redis 协调 |
//! | `hf3fs`       | HF3FS 文件系统后端集成 |
//! | `hot_standby` | 热备支持模块 |
//! | `http_metadata` | HTTP metadata server，将元数据通过 REST API 暴露 |
//! | `metrics`     | Prometheus metrics 指标采集 |
//! | `oplog`       | 操作日志 (OpLog)，记录变更供热备同步 |
//! | `service`     | 核心 gRPC 服务实现，包含所有 RPC 处理逻辑和状态管理 |
//! | `storage_backend` | 快照持久化后端 (LocalDisk / HF3FS) |
//! | `proto`       | 自动生成的 protobuf 代码 (tonic include) |

pub mod admin_http;
pub mod allocator;
pub mod count_min_sketch;
pub mod eviction;
pub mod ha;
pub mod hf3fs;
pub mod hot_standby;
pub mod http_metadata;
pub mod main_args;
pub mod main_config;
pub mod metrics;
pub mod oplog;
pub mod service;
pub mod storage_backend;
pub mod storage_distributed;
pub mod tenant_quota;
pub mod tenant_quota_policy_store;

pub use service::{MasterRuntimeConfig, MasterServiceImpl};
// Re-export tenant helpers for integration test use
pub use service::helpers::{make_tenant_scoped_key, normalize_tenant_id};

// Generated protobuf code — compiled by build.rs from
// proto/mooncake_store_grpc.proto + proto/mooncake_store_types.proto
// 自动生成的 protobuf 代码，由 build.rs 编译自 proto 定义文件。
pub mod proto {
    tonic::include_proto!("mooncake.store");
}

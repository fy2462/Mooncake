//! # Mooncake Store Core — shared types, errors, and utilities.
//!
//! This crate is the **foundation layer** of the Rust Mooncake Store implementation.
//! It defines the data model that every other store crate depends on:
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`error`] | Unified error type (`StoreError`) and result alias (`StoreResult`) |
//! | [`types`] | Domain types: segments, replicas, tasks, client info, and config structs |
//!
//! The crate has **zero business logic** — it is purely data definitions.
//! Downstream crates (`mooncake-store`, `mooncake-master`, etc.) depend on this
//! crate and implement the actual algorithms (allocation, eviction, transfer
//! orchestration, metadata persistence).
//!
//! # Mooncake Store 核心共享层
//!
//! 本 crate 是 Rust Mooncake Store 实现的**基础层**。
//! 定义了所有其他 store crate 所依赖的数据模型：
//!
//! | 模块 | 用途 |
//! |------|------|
//! | [`error`] | 统一错误类型 (`StoreError`) 和结果别名 (`StoreResult`) |
//! | [`types`] | 领域类型：segment、replica、task、client info 和配置结构体 |
//!
//! 本 crate **不包含任何业务逻辑**——纯数据定义。
//! 下游 crate（`mooncake-store`、`mooncake-master` 等）依赖本 crate
//! 并实现实际算法（分配、驱逐、传输编排、元数据持久化）。
//!
//! ## C++ Equivalents 对应关系
//!
//! | Rust type | C++ header / class |
//! |-----------|-------------------|
//! | `Segment` | `Client::GetLocalEndpoints()` / segment metadata in allocator |
//! | `ReplicaDescriptor` | `Replica` (replica.h) |
//! | `ReplicateConfig` | `ReplicateConfig` (allocator.h) |
//! | `TaskInfo` / `TaskAssignment` | task manager structures in master_service |
//! | `StoreError` | exception hierarchy + error-code returns |

pub mod error;
pub mod types;

// Re-export the most commonly used types so downstream crates can write
// `use mooncake_store_core::{StoreError, StoreResult, ...}` instead of
// importing from sub-modules.
// 重新导出最常用的类型，下游 crate 可以直接从 crate root 导入。
pub use error::StoreError;
pub use types::*;

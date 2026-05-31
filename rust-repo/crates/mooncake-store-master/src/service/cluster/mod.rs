//! # Cluster Management — 集群管理 / Cluster Operations
//!
//! 本模块实现 Master 服务的集群级管理操作，包括：
//!
//! This module implements cluster-level management operations for the Master service:
//!
//! ## Segment 挂载/卸载 / Segment Mount/Unmount
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `Ping` | 客户端心跳：更新地址、last_ping，返回 view_version |
//! | `MountSegment` | 挂载 Memory segment 到注册表和分配器 |
//! | `MountNoFSegment` | 挂载 NoF (NVMe-oF) segment |
//! | `UnmountSegment` | 卸载 Memory segment |
//! | `UnmountNoFSegment` | 卸载 NoF segment |
//! | `GracefulUnmountSegment` | 优雅卸载：等待 grace_period 后卸载 |
//! | `ReMountSegment` | 重启后重新挂载：批量注册已有 segment |
//! | `ReMountNoFSegment` | 重启后重新挂载 NoF segment |
//! | `MountLocalDiskSegment` | 注册本地磁盘 segment（offload/promotion 用） |
//!
//! ## Offload/Promotion / 下沉/提升
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `OffloadObjectHeartbeat` | 客户端心跳拉取待 offload 对象列表 |
//! | `ReportSsdCapacity` | 上报本地 SSD 总容量 |
//! | `NotifyOffloadSuccess` | 通知 offload 完成 |
//! | `PromotionObjectHeartbeat` | 客户端心跳拉取待 promotion 对象 |
//! | `PromotionAllocStart` | 为 promotion 分配暂存 Memory 副本 |
//! | `NotifyPromotionSuccess` | 通知 promotion 完成 |
//! | `NotifyPromotionFailure` | 通知 promotion 失败（回滚） |
//!
//! ## Segment 状态查询 / Segment Status Queries
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `QuerySegmentStatus` | 按名称查询 segment 状态 |
//! | `QuerySegmentStatusById` | 按 UUID 查询 segment 状态 |
//! | `CreateDrainJob` | 创建 Drain 迁移任务 |
//! | `QueryDrainJob` | 查询 Drain 任务进度 |
//! | `CancelDrainJob` | 取消 Drain 任务 |
//!
//! ## 远端回源协调 / Remote Pull Coordination
//!
//! | RPC | 功能 / Function |
//! |-----|----------------|
//! | `AcquireRemotePull` | 获取远端拉取权 |
//! | `CompleteRemotePull` | 通知远端拉取完成 |
//! | `ReleaseRemotePull` | 释放远端拉取权 |

pub mod drain;
pub mod offload;
pub mod promotion;
pub mod remote_pull;
pub mod segment;

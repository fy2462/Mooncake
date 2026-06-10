use super::*;

/// Tracks an in-flight drain unit task for a single key during segment draining.
/// 记录 segment drain 过程中单个 key 的迁移单元任务。
#[derive(Debug, Clone)]
pub(crate) struct ActiveDrainTask {
    /// 源 segment 名称 / Source segment name.
    pub(crate) source_segment: String,
    /// 目标 segment 名称 / Target segment name.
    pub(crate) target_segment: String,
    /// 迁移的字节数 / Bytes to migrate.
    pub(crate) bytes: u64,
    /// unit_key = "{key}@{source_segment}", used for dedup and retry tracking.
    /// C++ equivalent: ActiveDrainTask::unit_key
    pub(crate) unit_key: String,
}

impl ActiveDrainTask {
    /// Build the unit_key used for deduplication and retry tracking.
    /// 构建用于去重和重试跟踪的 unit_key。
    /// C++ equivalent: ActiveDrainTask::unit_key = "{key}@{source_segment}"
    pub(crate) fn unit_key_for(key: &str, source_segment: &str) -> String {
        format!("{key}@{source_segment}")
    }
}

/// A drain job that moves objects from draining segments to target segments.
/// Drain 任务：将对象从 draining segment 迁移到 target segment。
#[derive(Debug, Clone)]
pub(crate) struct DrainJobEntry {
    /// 任务 ID / Job ID.
    pub(crate) id: Uuid,
    /// 任务状态 / Job status: Created, Planning, Running, Succeeded, Failed, Canceled.
    pub(crate) status: crate::proto::JobStatus,
    /// 待 drain 的源 segment 名称列表 / Source segment names to drain.
    pub(crate) segments: Vec<String>,
    /// 目标 segment 名称列表 / Target segment names.
    pub(crate) target_segments: Vec<String>,
    /// 最大并发 drain 单元数 / Max concurrent drain units.
    pub(crate) max_concurrency: u32,
    /// 任务创建时间 / Job creation time.
    pub(crate) created_at: SystemTime,
    /// 最后更新时间 / Last update time.
    pub(crate) last_updated_at: SystemTime,
    /// 状态消息 / Status message.
    pub(crate) message: String,
    /// 已成功迁移的单元数 / Succeeded unit count.
    pub(crate) succeeded_units: u64,
    /// 已失败的单元数 / Failed unit count.
    pub(crate) failed_units: u64,
    /// 被阻塞的单元数 / Blocked unit count.
    pub(crate) blocked_units: u64,
    /// 已迁移的总字节数 / Total migrated bytes.
    pub(crate) migrated_bytes: u64,
    /// 活跃的任务映射 / Active task map: unit_id → ActiveDrainTask.
    pub(crate) active_tasks: HashMap<Uuid, ActiveDrainTask>,
    /// 已完成的单元 key 集合 / Completed unit key set.
    pub(crate) completed_unit_keys: HashSet<String>,
    /// 终极失败的单元 key 集合（不可重试）/ Terminal failed unit key set (non-retryable).
    pub(crate) terminal_failed_unit_keys: HashSet<String>,
    /// 每个 unit_key 的重试计数，超过 kMaxDrainUnitRetries(3) 后标记为 terminal_failed。
    /// Retry count per unit_key; after exceeding kMaxDrainUnitRetries(3), marked terminal_failed.
    /// C++ equivalent: DrainJob::retry_counts
    pub(crate) retry_counts: HashMap<String, u32>,
}

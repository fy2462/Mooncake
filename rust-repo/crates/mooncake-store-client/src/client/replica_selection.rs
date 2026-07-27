use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
use std::collections::HashSet;
use std::sync::Arc;

/// Scores a remote MEMORY replica; lower values are preferred.
pub type ReplicaScorer = Arc<dyn Fn(&ReplicaDescriptor) -> f64 + Send + Sync>;

/// Per-client policy for opt-in remote MEMORY replica scoring.
#[derive(Clone)]
pub struct ReplicaSelectionPolicy {
    scoring_enabled: bool,
    scorer: Option<ReplicaScorer>,
}

impl ReplicaSelectionPolicy {
    /// Preserve historical behavior by selecting the first remote MEMORY replica.
    pub fn disabled() -> Self {
        Self {
            scoring_enabled: false,
            scorer: None,
        }
    }

    /// Enable the built-in `rdma < tcp < unknown` protocol score.
    pub fn builtin() -> Self {
        Self {
            scoring_enabled: true,
            scorer: None,
        }
    }

    /// Enable scoring with a client-owned custom scorer.
    pub fn with_scorer(scorer: ReplicaScorer) -> Self {
        Self {
            scoring_enabled: true,
            scorer: Some(scorer),
        }
    }

    pub(crate) fn from_env() -> Self {
        Self::from_env_value(std::env::var("MC_STORE_REPLICA_SCORING").ok().as_deref())
    }

    fn from_env_value(value: Option<&str>) -> Self {
        if value == Some("1") {
            Self::builtin()
        } else {
            Self::disabled()
        }
    }

    fn score(&self, replica: &ReplicaDescriptor) -> f64 {
        self.scorer.as_ref().map_or_else(
            || builtin_remote_replica_score(replica),
            |score| score(replica),
        )
    }
}

impl Default for ReplicaSelectionPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Score a remote replica using the built-in protocol preference.
pub fn builtin_remote_replica_score(replica: &ReplicaDescriptor) -> f64 {
    if replica.replica_type != ReplicaType::Memory {
        return 100.0;
    }
    match replica.protocol.as_str() {
        "rdma" => 0.0,
        "tcp" => 1.0,
        _ => 2.0,
    }
}

pub(super) fn select_best_replica<'a>(
    replicas: &'a [ReplicaDescriptor],
    local_endpoints: &HashSet<String>,
    policy: &ReplicaSelectionPolicy,
) -> Option<&'a ReplicaDescriptor> {
    let mut first_memory = None;
    let mut first_nof = None;

    for replica in replicas {
        if replica.status != ReplicaStatus::Complete {
            continue;
        }
        match replica.replica_type {
            ReplicaType::Memory => {
                if local_endpoints.contains(&replica.segment_name) {
                    return Some(replica);
                }
                first_memory.get_or_insert(replica);
            }
            ReplicaType::NoFSsd => {
                if local_endpoints.contains(&replica.segment_name) {
                    return Some(replica);
                }
                first_nof.get_or_insert(replica);
            }
            _ => {}
        }
    }

    if policy.scoring_enabled && first_memory.is_some() {
        let mut best = None;
        let mut best_score = f64::MAX;
        for replica in replicas {
            if replica.status != ReplicaStatus::Complete
                || replica.replica_type != ReplicaType::Memory
                || local_endpoints.contains(&replica.segment_name)
            {
                continue;
            }
            let score = policy.score(replica);
            // 只在严格更优时替换，分数相同会保留 Master 返回的第一个候选；这使
            // transport 优化保持稳定，不会因一次 Get 随机改变副本选择。
            if score < best_score {
                best_score = score;
                best = Some(replica);
            }
        }
        if best.is_some() {
            return best;
        }
    }

    if first_memory.is_some() {
        return first_memory;
    }
    if first_nof.is_some() {
        return first_nof;
    }

    // 磁盘只在没有可读 Memory/NoF 时兜底；LocalDisk 的后续读取还会触发 promotion，
    // 因而不能把它提前到远端 Memory 之前仅为了追求节点本地性。
    let mut best = None;
    for replica in replicas {
        if replica.status != ReplicaStatus::Complete {
            continue;
        }
        match replica.replica_type {
            ReplicaType::LocalDisk => best = Some(replica),
            ReplicaType::Disk if best.is_none() => best = Some(replica),
            _ => {}
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::{ReplicaSelectionPolicy, select_best_replica};
    use mooncake_store_core::{ReplicaDescriptor, ReplicaStatus, ReplicaType};
    use std::collections::HashSet;
    use std::sync::Arc;
    use uuid::Uuid;

    fn memory(name: &str, protocol: &str) -> ReplicaDescriptor {
        ReplicaDescriptor {
            segment_id: Uuid::new_v4(),
            segment_name: name.to_string(),
            offset: 0,
            size: 1,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: None,
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: 0,
            protocol: protocol.to_string(),
        }
    }

    #[test]
    fn disabled_scoring_keeps_first_remote_memory() {
        let replicas = vec![memory("tcp-node", "tcp"), memory("rdma-node", "rdma")];

        let selected = select_best_replica(
            &replicas,
            &HashSet::new(),
            &ReplicaSelectionPolicy::disabled(),
        )
        .unwrap();

        assert_eq!(selected.segment_name, "tcp-node");
    }

    #[test]
    fn builtin_scoring_prefers_rdma_and_keeps_ties_in_master_order() {
        let replicas = vec![
            memory("tcp-node", "tcp"),
            memory("rdma-first", "rdma"),
            memory("rdma-second", "rdma"),
        ];

        let selected = select_best_replica(
            &replicas,
            &HashSet::new(),
            &ReplicaSelectionPolicy::builtin(),
        )
        .unwrap();

        assert_eq!(selected.segment_name, "rdma-first");
    }

    #[test]
    fn injected_scorer_is_client_owned_and_local_memory_still_wins() {
        let replicas = vec![memory("remote", "tcp"), memory("local", "tcp")];
        let local = HashSet::from(["local".to_string()]);
        let scorer = Arc::new(|replica: &ReplicaDescriptor| {
            if replica.segment_name == "remote" {
                0.0
            } else {
                100.0
            }
        });
        let policy = ReplicaSelectionPolicy::with_scorer(scorer);

        let selected = select_best_replica(&replicas, &local, &policy).unwrap();

        assert_eq!(selected.segment_name, "local");
    }

    #[test]
    fn injected_scorer_picks_its_lowest_remote_score() {
        let replicas = vec![memory("expensive", "rdma"), memory("cheap", "tcp")];
        let policy = ReplicaSelectionPolicy::with_scorer(Arc::new(
            |replica: &ReplicaDescriptor| match replica.segment_name.as_str() {
                "cheap" => 1.0,
                _ => 10.0,
            },
        ));

        let selected = select_best_replica(&replicas, &HashSet::new(), &policy).unwrap();

        assert_eq!(selected.segment_name, "cheap");
    }

    #[test]
    fn environment_opt_in_requires_exactly_one() {
        assert!(ReplicaSelectionPolicy::from_env_value(Some("1")).scoring_enabled);
        assert!(!ReplicaSelectionPolicy::from_env_value(Some("true")).scoring_enabled);
        assert!(!ReplicaSelectionPolicy::from_env_value(None).scoring_enabled);
    }

    #[test]
    fn scoring_skips_incomplete_memory_replicas() {
        let mut incomplete = memory("incomplete-rdma", "rdma");
        incomplete.status = ReplicaStatus::Allocating;
        let replicas = vec![incomplete, memory("complete-tcp", "tcp")];

        let selected = select_best_replica(
            &replicas,
            &HashSet::new(),
            &ReplicaSelectionPolicy::builtin(),
        )
        .unwrap();

        assert_eq!(selected.segment_name, "complete-tcp");
    }

    #[test]
    fn nof_and_disk_fallbacks_match_historical_order() {
        let mut first_nof = memory("first-nof", "nof");
        first_nof.replica_type = ReplicaType::NoFSsd;
        let mut second_nof = memory("second-nof", "nof");
        second_nof.replica_type = ReplicaType::NoFSsd;
        let nof_replicas = vec![first_nof, second_nof];
        assert_eq!(
            select_best_replica(
                &nof_replicas,
                &HashSet::new(),
                &ReplicaSelectionPolicy::builtin(),
            )
            .unwrap()
            .segment_name,
            "first-nof"
        );

        let mut disk = memory("disk", "");
        disk.replica_type = ReplicaType::Disk;
        let mut local_disk = memory("local-disk", "");
        local_disk.replica_type = ReplicaType::LocalDisk;
        let disk_replicas = vec![local_disk, disk];
        assert_eq!(
            select_best_replica(
                &disk_replicas,
                &HashSet::new(),
                &ReplicaSelectionPolicy::builtin(),
            )
            .unwrap()
            .segment_name,
            "local-disk"
        );
    }

    #[test]
    fn local_nof_precedes_remote_nof() {
        let mut remote = memory("remote-nof", "nof");
        remote.replica_type = ReplicaType::NoFSsd;
        let mut local = memory("local-nof", "nof");
        local.replica_type = ReplicaType::NoFSsd;
        let replicas = vec![remote, local];

        let selected = select_best_replica(
            &replicas,
            &HashSet::from(["local-nof".to_string()]),
            &ReplicaSelectionPolicy::builtin(),
        )
        .unwrap();

        assert_eq!(selected.segment_name, "local-nof");
    }

    #[test]
    fn builtin_score_ranks_unknown_and_non_memory() {
        let unknown = memory("unknown", "ucx");
        let mut nof = memory("nof", "rdma");
        nof.replica_type = ReplicaType::NoFSsd;

        assert_eq!(super::builtin_remote_replica_score(&unknown), 2.0);
        assert_eq!(super::builtin_remote_replica_score(&nof), 100.0);
    }

    #[test]
    fn no_complete_replica_returns_none() {
        let mut incomplete = memory("incomplete", "rdma");
        incomplete.status = ReplicaStatus::Written;

        assert!(
            select_best_replica(
                &[incomplete],
                &HashSet::new(),
                &ReplicaSelectionPolicy::builtin(),
            )
            .is_none()
        );
    }
}

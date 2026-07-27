use mooncake_store_core::{ReplicaDescriptor, ReplicaType, ReplicateConfig};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReplicaTransferSummary {
    pub(crate) allocated_memory_replicas: usize,
    pub(crate) allocated_nof_replicas: usize,
    pub(crate) allocated_disk_replicas: usize,
    pub(crate) successful_memory_transfers: usize,
    pub(crate) successful_nof_transfers: usize,
    pub(crate) failed_memory_transfers: usize,
    pub(crate) failed_nof_transfers: usize,
    pub(crate) successful_disk_writes: usize,
    pub(crate) failed_disk_writes: usize,
}

impl ReplicaTransferSummary {
    pub(crate) fn from_replicas(replicas: &[ReplicaDescriptor]) -> Self {
        let mut summary = Self::default();
        for replica in replicas {
            match replica.replica_type {
                ReplicaType::Memory => summary.allocated_memory_replicas += 1,
                ReplicaType::NoFSsd => summary.allocated_nof_replicas += 1,
                ReplicaType::Disk => summary.allocated_disk_replicas += 1,
                _ => {}
            }
        }
        summary
    }

    pub(crate) fn record_success(&mut self, replica_type: ReplicaType) {
        match replica_type {
            ReplicaType::Memory => self.successful_memory_transfers += 1,
            ReplicaType::NoFSsd => self.successful_nof_transfers += 1,
            ReplicaType::Disk => self.successful_disk_writes += 1,
            _ => {}
        }
    }

    pub(crate) fn record_failure(&mut self, replica_type: ReplicaType) {
        match replica_type {
            ReplicaType::Memory => self.failed_memory_transfers += 1,
            ReplicaType::NoFSsd => self.failed_nof_transfers += 1,
            ReplicaType::Disk => self.failed_disk_writes += 1,
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaFinalizeDecision {
    pub(crate) end_type: Option<ReplicaType>,
    pub(crate) revoke_type: Option<ReplicaType>,
    pub(crate) success: bool,
}

fn has_expected_replica_allocation(
    config: &ReplicateConfig,
    summary: &ReplicaTransferSummary,
) -> bool {
    if config.replica_num == 0 && config.nof_replica_num == 0 {
        return summary.allocated_disk_replicas > 0;
    }
    if config.nof_replica_num == 0 {
        return summary.allocated_memory_replicas > 0;
    }
    if config.replica_num == 1 && config.nof_replica_num == 1 {
        return summary.allocated_memory_replicas + summary.allocated_nof_replicas > 0;
    }
    summary.allocated_memory_replicas == config.replica_num as usize
        && summary.allocated_nof_replicas == config.nof_replica_num as usize
}

pub(crate) fn determine_finalize_decision(
    config: &ReplicateConfig,
    summary: &ReplicaTransferSummary,
) -> ReplicaFinalizeDecision {
    let allocation_satisfied = has_expected_replica_allocation(config, summary);
    if config.replica_num == 0 && config.nof_replica_num == 0 {
        return ReplicaFinalizeDecision {
            end_type: None,
            revoke_type: None,
            success: allocation_satisfied
                && summary.successful_disk_writes == summary.allocated_disk_replicas
                && summary.failed_disk_writes == 0,
        };
    }
    let flexible_dual = config.replica_num == 1 && config.nof_replica_num == 1;

    if !flexible_dual {
        let all_transfers_succeeded = summary.successful_memory_transfers
            == summary.allocated_memory_replicas
            && summary.successful_nof_transfers == summary.allocated_nof_replicas
            && summary.failed_memory_transfers == 0
            && summary.failed_nof_transfers == 0;
        if allocation_satisfied && all_transfers_succeeded {
            return ReplicaFinalizeDecision {
                end_type: Some(ReplicaType::All),
                revoke_type: None,
                success: true,
            };
        }
        return ReplicaFinalizeDecision {
            end_type: None,
            revoke_type: Some(ReplicaType::All),
            success: false,
        };
    }

    let memory_succeeded = summary.successful_memory_transfers > 0;
    let nof_succeeded = summary.successful_nof_transfers > 0;
    if memory_succeeded && nof_succeeded {
        ReplicaFinalizeDecision {
            end_type: Some(ReplicaType::All),
            revoke_type: None,
            success: true,
        }
    } else if memory_succeeded {
        ReplicaFinalizeDecision {
            end_type: Some(ReplicaType::Memory),
            revoke_type: Some(ReplicaType::NoFSsd),
            success: true,
        }
    } else if nof_succeeded {
        ReplicaFinalizeDecision {
            end_type: Some(ReplicaType::NoFSsd),
            revoke_type: Some(ReplicaType::Memory),
            success: true,
        }
    } else {
        ReplicaFinalizeDecision {
            end_type: None,
            revoke_type: Some(ReplicaType::All),
            success: false,
        }
    }
}

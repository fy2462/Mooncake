use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::metrics;
use mooncake_store_core::ReplicaType;

use super::{MasterState, erase_promotion_candidate, try_push_promotion_queue};

const MAX_RETRIES: u32 = 8;
const RETRY_BATCH_SIZE: usize = 128;
const PARTITION_COUNT: usize = 256;
const PARTITIONS_PER_TICK: usize = 64;
const CANDIDATE_TTL: Duration = Duration::from_secs(60);
const INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const MAX_BACKOFF: Duration = Duration::from_secs(1);

fn partition_for(key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize % PARTITION_COUNT
}

fn partition_is_selected(partition: usize, start: usize, count: usize) -> bool {
    (0..count).any(|offset| (start + offset) % PARTITION_COUNT == partition)
}

fn retry_backoff(retry_count: u32) -> Duration {
    let multiplier = 1_u32.checked_shl(retry_count.min(30)).unwrap_or(u32::MAX);
    INITIAL_BACKOFF
        .checked_mul(multiplier)
        .unwrap_or(MAX_BACKOFF)
        .min(MAX_BACKOFF)
}

/// Retry a bounded, partitioned slice of transient promotion candidates.
pub(crate) fn run_promotion_candidate_retry(state: &MasterState, partitions: usize) {
    let partitions = partitions.min(PARTITION_COUNT);
    let start = state
        .promotion_retry_cursor
        .fetch_add(partitions, Ordering::Relaxed)
        % PARTITION_COUNT;
    let now = Instant::now();
    let keys = state
        .promotion_candidates
        .iter()
        .filter(|entry| partition_is_selected(partition_for(entry.key()), start, partitions))
        .filter(|entry| entry.retry_after <= now)
        .take(RETRY_BATCH_SIZE)
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();

    for key in keys {
        let Some(candidate) = state.promotion_candidates.get(&key) else {
            continue;
        };
        debug_assert!(candidate.first_seen <= candidate.last_seen);
        if now.saturating_duration_since(candidate.last_seen) >= CANDIDATE_TTL
            || candidate.retry_count >= MAX_RETRIES
        {
            if candidate.retry_count == 0 {
                metrics::PROMOTION_CANDIDATE_EXPIRED_UNEVALUATED.inc();
            } else {
                metrics::PROMOTION_CANDIDATE_EXPIRED_EVALUATED.inc();
            }
            drop(candidate);
            erase_promotion_candidate(state, &key);
            continue;
        }
        drop(candidate);

        // C++ pre-filters candidates under the shard lock: an object that is
        // missing, invalid, already in flight, or no longer LocalDisk-only is
        // erased without consulting the transient admission gates (which would
        // otherwise return WatermarkRejected before reaching object state).
        let ineligible = state.objects.get(&key).is_none_or(|object| {
            object.replicas.is_empty()
                || state.promotion_tasks.contains_key(&key)
                || object
                    .replicas
                    .iter()
                    .any(|replica| replica.replica_type == ReplicaType::Memory)
                || !object
                    .replicas
                    .iter()
                    .any(|replica| replica.replica_type == ReplicaType::LocalDisk)
        });
        if ineligible {
            erase_promotion_candidate(state, &key);
            continue;
        }

        let result = try_push_promotion_queue(state, &key, false);
        if result == super::PromotionQueueResult::Queued {
            metrics::PROMOTION_CANDIDATE_ADMITTED.inc();
        }
        if !result.is_transient() {
            erase_promotion_candidate(state, &key);
            continue;
        }

        if let Some(mut candidate) = state.promotion_candidates.get_mut(&key) {
            metrics::PROMOTION_CANDIDATE_ADMISSION_REJECTED.inc();
            candidate.retry_count += 1;
            if candidate.retry_count >= MAX_RETRIES {
                metrics::PROMOTION_CANDIDATE_EXPIRED_EVALUATED.inc();
                drop(candidate);
                erase_promotion_candidate(state, &key);
            } else {
                candidate.retry_after = now + retry_backoff(candidate.retry_count - 1);
            }
        }
    }
}

pub(crate) fn run_default_promotion_candidate_retry(state: &MasterState) {
    run_promotion_candidate_retry(state, PARTITIONS_PER_TICK);
}

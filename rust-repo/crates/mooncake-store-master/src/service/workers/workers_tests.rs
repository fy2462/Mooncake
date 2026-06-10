// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use crate::allocator::SegmentAllocator;
    use crate::count_min_sketch::CountMinSketch;
    use crate::proto;
    use crate::service::helpers::unmount_nof_segment_owned;
    use crate::service::state::{
        MasterRuntimeConfig, MasterState, NoFHeartbeatState, NoFSegmentEntry, ObjectEntry,
    };
    use dashmap::DashMap;
    use mooncake_store_core::{
        NoFSegment, ObjectDataType, ReplicaDescriptor, ReplicaStatus, ReplicaType, Segment,
    };
    use parking_lot::RwLock;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime};
    use uuid::Uuid;

    fn make_state(config: MasterRuntimeConfig) -> Arc<MasterState> {
        Arc::new(MasterState {
            clients: DashMap::new(),
            ok_clients: DashMap::new(),
            objects: DashMap::new(),
            processing_keys: DashMap::new(),
            client_objects: DashMap::new(),
            segments: DashMap::new(),
            nof_segments: DashMap::new(),
            local_disk_segments: DashMap::new(),
            tasks: DashMap::new(),
            replication_tasks: DashMap::new(),
            offloading_tasks: DashMap::new(),
            promotion_tasks: DashMap::new(),
            promotion_sketch: RwLock::new(CountMinSketch::new()),
            drain_jobs: DashMap::new(),
            allocator: RwLock::new(SegmentAllocator::new()),
            nof_allocator: RwLock::new(SegmentAllocator::new()),
            storage_backend: RwLock::new(None),
            promotion_in_flight: AtomicUsize::new(0),
            view_version: std::sync::atomic::AtomicI64::new(0),
            runtime_config: config,
            pending_remote_pulls: DashMap::new(),
            nof_heartbeat_states: DashMap::new(),
        })
    }

    fn add_nof_segment(state: &MasterState, name: &str, te_endpoint: &str, size: u64) -> Uuid {
        let id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let seg = NoFSegment {
            id,
            name: name.to_string(),
            base: 0,
            size,
            te_endpoint: te_endpoint.to_string(),
            client_id,
        };
        state.nof_segments.insert(
            id,
            NoFSegmentEntry {
                segment: seg.clone(),
                used: 0,
                status: proto::SegmentStatus::Active,
            },
        );
        state.nof_allocator.write().add_segment(
            Segment {
                id,
                name: name.to_string(),
                base: 0,
                size,
                te_endpoint: te_endpoint.to_string(),
                protocol: "nvmeof".to_string(),
            },
            0,
            client_id,
        );
        id
    }

    // ------------------------------------------------------------------
    // Unit tests: heartbeat state lifecycle (no timing dependency)
    // ------------------------------------------------------------------

    #[test]
    fn test_heartbeat_state_created_for_active_segment() {
        let state = make_state(MasterRuntimeConfig::default());
        let seg_id = add_nof_segment(&state, "nof1:8000", "10.0.0.1:8000", 1024 * 1024);

        // Simulate what the worker sync does: add heartbeat state for new active segment.
        let now = Instant::now();
        state
            .nof_heartbeat_states
            .entry(seg_id)
            .or_insert_with(|| NoFHeartbeatState {
                segment_id: seg_id,
                segment_name: "nof1:8000".to_string(),
                te_endpoint: "10.0.0.1:8000".to_string(),
                next_probe_at: now,
                last_success_at: now,
                consecutive_failures: 0,
            });

        assert!(state.nof_heartbeat_states.contains_key(&seg_id));
        let entry = state.nof_heartbeat_states.get(&seg_id).unwrap();
        assert_eq!(entry.consecutive_failures, 0);
        assert_eq!(entry.segment_name, "nof1:8000");
    }

    #[test]
    fn test_heartbeat_state_removed_when_segment_unmounted() {
        let state = make_state(MasterRuntimeConfig::default());
        let seg_id = add_nof_segment(&state, "nof2:8000", "10.0.0.2:8000", 1024 * 1024);

        // Simulate worker sync: insert state.
        state.nof_heartbeat_states.insert(
            seg_id,
            NoFHeartbeatState {
                segment_id: seg_id,
                segment_name: "nof2:8000".to_string(),
                te_endpoint: "10.0.0.2:8000".to_string(),
                next_probe_at: Instant::now(),
                last_success_at: Instant::now(),
                consecutive_failures: 0,
            },
        );

        // Simulate unmount: remove segment and clean up heartbeat state.
        state.nof_segments.remove(&seg_id);
        state.nof_heartbeat_states.remove(&seg_id);

        assert!(!state.nof_heartbeat_states.contains_key(&seg_id));
    }

    #[test]
    fn test_consecutive_failures_tracked() {
        let state = make_state(MasterRuntimeConfig::default());
        let seg_id = add_nof_segment(&state, "nof3:8000", "10.0.0.3:8000", 1024 * 1024);

        let now = Instant::now();
        state.nof_heartbeat_states.insert(
            seg_id,
            NoFHeartbeatState {
                segment_id: seg_id,
                segment_name: "nof3:8000".to_string(),
                te_endpoint: "10.0.0.3:8000".to_string(),
                next_probe_at: now,
                last_success_at: now,
                consecutive_failures: 1,
            },
        );

        // Simulate probe failure: increment consecutive failures.
        if let Some(mut entry) = state.nof_heartbeat_states.get_mut(&seg_id) {
            entry.consecutive_failures += 1;
        }

        let entry = state.nof_heartbeat_states.get(&seg_id).unwrap();
        assert_eq!(entry.consecutive_failures, 2);
    }

    #[test]
    fn test_failure_count_resets_on_success() {
        let state = make_state(MasterRuntimeConfig::default());
        let seg_id = add_nof_segment(&state, "nof4:8000", "10.0.0.4:8000", 1024 * 1024);

        let now = Instant::now();
        state.nof_heartbeat_states.insert(
            seg_id,
            NoFHeartbeatState {
                segment_id: seg_id,
                segment_name: "nof4:8000".to_string(),
                te_endpoint: "10.0.0.4:8000".to_string(),
                next_probe_at: now,
                last_success_at: now,
                consecutive_failures: 5,
            },
        );

        // Simulate probe success: reset failures.
        if let Some(mut entry) = state.nof_heartbeat_states.get_mut(&seg_id) {
            entry.consecutive_failures = 0;
            entry.last_success_at = Instant::now();
        }

        let entry = state.nof_heartbeat_states.get(&seg_id).unwrap();
        assert_eq!(entry.consecutive_failures, 0);
    }

    #[test]
    fn test_alive_timeout_triggers_unmount_only_after_last_success_window() {
        let mut config = MasterRuntimeConfig::default();
        config.nof_heartbeat_failures_threshold = 3;
        config.nof_heartbeat_interval = Duration::from_secs(10);

        let state = make_state(config);
        let seg_id = add_nof_segment(&state, "fail_seg:8000", "10.0.0.5:8000", 1024 * 1024);

        let now = Instant::now();
        state.nof_heartbeat_states.insert(
            seg_id,
            NoFHeartbeatState {
                segment_id: seg_id,
                segment_name: "fail_seg:8000".to_string(),
                te_endpoint: "10.0.0.5:8000".to_string(),
                next_probe_at: now,
                last_success_at: now,
                consecutive_failures: 3,
            },
        );

        let alive_timeout = state.runtime_config.nof_heartbeat_interval
            * state.runtime_config.nof_heartbeat_failures_threshold;
        let should_unmount = state
            .nof_heartbeat_states
            .get(&seg_id)
            .is_some_and(|e| now.saturating_duration_since(e.last_success_at) >= alive_timeout);
        assert!(!should_unmount);

        if let Some(mut entry) = state.nof_heartbeat_states.get_mut(&seg_id) {
            entry.last_success_at = now - Duration::from_secs(31);
        }
        let should_unmount = state
            .nof_heartbeat_states
            .get(&seg_id)
            .is_some_and(|e| now.saturating_duration_since(e.last_success_at) >= alive_timeout);
        assert!(should_unmount);
    }

    #[test]
    fn test_nof_unmount_clears_invalid_object_handles() {
        let state = make_state(MasterRuntimeConfig::default());
        let seg_id = add_nof_segment(&state, "nof-cleanup:8000", "10.0.0.9:8000", 1024 * 1024);
        let owner = state.nof_segments.get(&seg_id).unwrap().segment.client_id;
        state.nof_heartbeat_states.insert(
            seg_id,
            NoFHeartbeatState {
                segment_id: seg_id,
                segment_name: "nof-cleanup:8000".to_string(),
                te_endpoint: "10.0.0.9:8000".to_string(),
                next_probe_at: Instant::now(),
                last_success_at: Instant::now(),
                consecutive_failures: 0,
            },
        );
        state.objects.insert(
            "nof-only".to_string(),
            ObjectEntry {
                replicas: vec![ReplicaDescriptor {
                    handle_valid: true,
                    segment_id: seg_id,
                    segment_name: "nof-cleanup:8000".to_string(),
                    offset: 0,
                    size: 128,
                    status: ReplicaStatus::Complete,
                    replica_type: ReplicaType::NoFSsd,
                    holder_client_id: Some(owner),
                    base_addr: 0x100000000,
                    refcnt: 0,
                }],
                size: 128,
                last_access: SystemTime::now(),
                hard_pinned: false,
                data_type: ObjectDataType::Unknown,
                client_id: owner,
                put_start_time: None,
                lease_timeout: None,
                soft_pin_timeout: None,
                tenant_id: "default".to_string(),
                group_id: String::new(),
                user_key: "nof-only".to_string(),
            },
        );

        assert!(unmount_nof_segment_owned(&state, seg_id, owner));
        assert!(!state.nof_segments.contains_key(&seg_id));
        assert!(!state.nof_heartbeat_states.contains_key(&seg_id));
        assert!(!state.objects.contains_key("nof-only"));
    }
}

use super::super::helpers::unmount_segment_owned_locked;
use super::super::*;
use std::sync::Arc;

/// Error codes returned by the strict segment-catalog layer.
///
/// This mirrors the C++ `SegmentManager` contract: the catalog is strict about
/// duplicate identities and missing segments, while the higher-level
/// `MasterService` converts those specific codes into idempotent success.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SegmentCatalogError {
    #[error("segment already exists")]
    SegmentAlreadyExists,
    #[error("segment not found")]
    SegmentNotFound,
    #[error("conflicting segment identity")]
    ConflictingIdentity,
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),
}

/// Strict segment catalog over the authoritative Master state.
///
/// Unlike the idempotent `MasterService` RPC handlers, this layer reports
/// `SEGMENT_ALREADY_EXISTS` / `SEGMENT_NOT_FOUND` for duplicate mounts and
/// duplicate unmounts, matching the C++ `SegmentManager`.
pub(crate) struct SegmentCatalog {
    state: Arc<MasterState>,
}

impl SegmentCatalog {
    pub(crate) fn new(state: Arc<MasterState>) -> Self {
        Self { state }
    }

    /// Register a client-owned Memory segment using its explicit identity.
    pub(crate) fn mount_memory_segment(
        &self,
        segment: mooncake_store_core::Segment,
        client_id: Uuid,
    ) -> Result<(), SegmentCatalogError> {
        if segment.id.is_nil() {
            return Err(SegmentCatalogError::InvalidArguments(
                "segment id must not be nil".into(),
            ));
        }
        if segment.name.is_empty() {
            return Err(SegmentCatalogError::InvalidArguments(
                "segment name must not be empty".into(),
            ));
        }
        if segment.size == 0 {
            return Err(SegmentCatalogError::InvalidArguments(
                "segment size must be non-zero".into(),
            ));
        }
        if segment.base == 0 {
            return Err(SegmentCatalogError::InvalidArguments(
                "segment base must be non-zero".into(),
            ));
        }

        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        if self.state.segments.contains_key(&segment.id) {
            return Err(SegmentCatalogError::SegmentAlreadyExists);
        }
        if self.state.nof_segments.contains_key(&segment.id) {
            return Err(SegmentCatalogError::ConflictingIdentity);
        }
        self.state.segments.insert(
            segment.id,
            SegmentEntry {
                segment: segment.clone(),
                used: 0,
                client_id,
                status: proto::SegmentStatus::Active,
            },
        );
        sync_client_segments(&self.state, client_id);
        self.state
            .allocator
            .write()
            .add_segment(segment, 0, client_id);
        bump_view_version(&self.state);
        metrics::SEGMENT_COUNT.set(self.state.segments.len() as i64);
        Ok(())
    }

    /// Remove a client-owned Memory segment and report a missing segment
    /// instead of silently succeeding on a duplicate unmount.
    pub(crate) fn unmount_segment(
        &self,
        segment_id: Uuid,
        client_id: Uuid,
    ) -> Result<(), SegmentCatalogError> {
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        let Some(segment) = self.state.segments.get(&segment_id) else {
            return Err(SegmentCatalogError::SegmentNotFound);
        };
        if segment.client_id != client_id {
            return Err(SegmentCatalogError::SegmentNotFound);
        }
        drop(segment);
        if !unmount_segment_owned_locked(&self.state, segment_id, client_id) {
            return Err(SegmentCatalogError::SegmentNotFound);
        }
        bump_view_version(&self.state);
        Ok(())
    }

    /// Register a completed LocalDisk binding with strict duplicate detection.
    pub(crate) fn mount_local_disk_segment(
        &self,
        client_id: Uuid,
        storage_id: Uuid,
        enable_offloading: bool,
    ) -> Result<(), SegmentCatalogError> {
        if storage_id.is_nil() {
            return Err(SegmentCatalogError::InvalidArguments(
                "LocalDisk storage id must not be nil".into(),
            ));
        }
        let _global_mutation_guard = self.state.key_mutations.lock_snapshot();
        if let Some(existing) = self.state.local_disk_segments.get(&storage_id) {
            if existing.active_client_id == Some(client_id) && existing.recovery_complete {
                return Err(SegmentCatalogError::SegmentAlreadyExists);
            }
        }
        self.state.local_disk_segments.insert(
            storage_id,
            LocalDiskSegmentEntry {
                active_client_id: Some(client_id),
                persisted_client_id: Some(client_id),
                recovery_complete: true,
                recovery_session_id: None,
                recovered_objects: HashSet::new(),
                enable_offloading,
                offloading_objects: HashMap::new(),
                promotion_objects: HashMap::new(),
                ssd_total_capacity_bytes: 0,
                ssd_capacity_metric_accounted: false,
            },
        );
        self.state
            .local_disk_client_sessions
            .insert(client_id, storage_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MasterServiceImpl;

    fn catalog() -> SegmentCatalog {
        MasterServiceImpl::default().segment_catalog()
    }

    fn segment(name: &str, size: u64, base: u64) -> mooncake_store_core::Segment {
        mooncake_store_core::Segment {
            id: Uuid::new_v4(),
            name: name.to_string(),
            base,
            size,
            te_endpoint: name.to_string(),
            protocol: "tcp".to_string(),
            host_id: "host-0".to_string(),
        }
    }

    #[test]
    fn cpp_parity_mount_segment_duplicate_identity() {
        let catalog = catalog();
        let client_id = Uuid::new_v4();
        let first = segment("mount-dup", 16 * 1024 * 1024, 0x100000000);

        catalog
            .mount_memory_segment(first.clone(), client_id)
            .unwrap();
        assert_eq!(
            catalog.mount_memory_segment(first.clone(), client_id),
            Err(SegmentCatalogError::SegmentAlreadyExists)
        );
        assert_eq!(catalog.state.segments.len(), 1);

        // A distinct ID with the same name and adjacent base coexists.
        let second = segment(
            "mount-dup",
            32 * 1024 * 1024,
            0x100000000 + 16 * 1024 * 1024,
        );
        catalog
            .mount_memory_segment(second.clone(), client_id)
            .unwrap();
        assert_eq!(catalog.state.segments.len(), 2);
    }

    #[test]
    fn cpp_parity_unmount_segment_duplicate() {
        let catalog = catalog();
        let client_id = Uuid::new_v4();
        let segment = segment("unmount-dup", 16 * 1024 * 1024, 0x100000000);
        catalog
            .mount_memory_segment(segment.clone(), client_id)
            .unwrap();

        catalog.unmount_segment(segment.id, client_id).unwrap();
        assert_eq!(
            catalog.unmount_segment(segment.id, client_id),
            Err(SegmentCatalogError::SegmentNotFound)
        );
        assert_eq!(catalog.state.segments.len(), 0);
    }

    #[test]
    fn cpp_parity_mount_local_disk_segment_duplicate() {
        let catalog = catalog();
        let client_a = Uuid::new_v4();
        let storage_a = Uuid::new_v4();

        catalog
            .mount_local_disk_segment(client_a, storage_a, true)
            .unwrap();
        assert_eq!(
            catalog.mount_local_disk_segment(client_a, storage_a, true),
            Err(SegmentCatalogError::SegmentAlreadyExists)
        );
        assert_eq!(catalog.state.local_disk_segments.len(), 1);

        let client_b = Uuid::new_v4();
        let storage_b = Uuid::new_v4();
        catalog
            .mount_local_disk_segment(client_b, storage_b, true)
            .unwrap();
        assert_eq!(catalog.state.local_disk_segments.len(), 2);
    }
}

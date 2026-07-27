use crate::tenant_id::TenantId;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TenantQuotaSnapshot {
    pub tenant_id: TenantId,
    pub requested_quota_bytes: u64,
    pub effective_quota_bytes: u64,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub committed_count: u64,
    pub metadata_object_count: u64,
    pub over_quota: bool,
    pub has_explicit_policy: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantQuotaError {
    QuotaExceeded,
    TenantNotRegistered,
    InvalidArgument,
    AccountingMismatch,
    TenantNotEmpty,
}

#[derive(Debug, Clone, Default)]
struct TenantQuotaState {
    requested_quota_bytes: u64,
    effective_quota_bytes: u64,
    used_bytes: u64,
    reserved_bytes: u64,
    committed_count: u64,
    metadata_object_count: u64,
    has_explicit_policy: bool,
    over_quota: bool,
}

#[derive(Debug, Clone)]
pub struct TenantQuotaTable {
    tenants: BTreeMap<TenantId, TenantQuotaState>,
}

impl TenantQuotaTable {
    pub fn new(_default_requested_quota_bytes: u64) -> Self {
        Self {
            tenants: BTreeMap::new(),
        }
    }

    pub fn upsert_policy(
        &mut self,
        tenant_id: &TenantId,
        requested_quota_bytes: u64,
        capacity: u64,
    ) -> Result<TenantQuotaSnapshot, TenantQuotaError> {
        if requested_quota_bytes == 0 {
            return Err(TenantQuotaError::InvalidArgument);
        }
        let state = self.get_or_create_state(tenant_id);
        state.requested_quota_bytes = requested_quota_bytes;
        state.has_explicit_policy = true;
        self.recompute_effective_quotas(capacity);
        Ok(self.snapshot_for_existing(tenant_id))
    }

    pub fn erase_policy(
        &mut self,
        tenant_id: &TenantId,
        capacity: u64,
    ) -> Result<Option<TenantQuotaSnapshot>, TenantQuotaError> {
        let Some(state) = self.tenants.get_mut(tenant_id) else {
            return Ok(None);
        };
        if state.metadata_object_count > 0
            || state.used_bytes > 0
            || state.reserved_bytes > 0
            || state.committed_count > 0
        {
            return Err(TenantQuotaError::TenantNotEmpty);
        }
        state.requested_quota_bytes = 0;
        state.effective_quota_bytes = 0;
        state.has_explicit_policy = false;
        self.recompute_effective_quotas(capacity);
        Ok(self
            .tenants
            .get(tenant_id)
            .filter(|state| !is_lazy_empty(state))
            .map(|_| self.snapshot_for_existing(tenant_id)))
    }

    /// Replace only the explicit policy layer while retaining all runtime
    /// usage/reservation accounting restored by the standby.
    pub fn replace_policies(
        &mut self,
        policies: &BTreeMap<TenantId, u64>,
        capacity: u64,
    ) -> Result<(), TenantQuotaError> {
        if policies.values().any(|quota| *quota == 0) {
            return Err(TenantQuotaError::InvalidArgument);
        }
        for state in self.tenants.values_mut() {
            state.requested_quota_bytes = 0;
            state.has_explicit_policy = false;
        }
        for (tenant_id, quota) in policies {
            let state = self.get_or_create_state(tenant_id);
            state.requested_quota_bytes = *quota;
            state.has_explicit_policy = true;
        }
        self.recompute_effective_quotas(capacity);
        self.tenants.retain(|_, state| !is_lazy_empty(state));
        Ok(())
    }

    #[doc(hidden)]
    pub fn remove_registration_for_test(&mut self, tenant_id: &TenantId, capacity: u64) {
        if let Some(state) = self.tenants.get_mut(tenant_id) {
            state.requested_quota_bytes = 0;
            state.effective_quota_bytes = 0;
            state.has_explicit_policy = false;
            self.recompute_effective_quotas(capacity);
        }
    }

    pub fn get_snapshot(&self, tenant_id: &TenantId) -> Option<TenantQuotaSnapshot> {
        self.tenants
            .get(tenant_id)
            .map(|state| self.make_snapshot(tenant_id, state))
    }

    /// Returns whether the tenant has an explicit quota policy and is therefore
    /// registered for writes. Accounting-only states restored from metadata do
    /// not constitute registration.
    pub fn is_registered(&self, tenant_id: &TenantId) -> bool {
        self.tenants
            .get(tenant_id)
            .is_some_and(|state| state.has_explicit_policy)
    }

    pub fn list_snapshots(&self) -> Vec<TenantQuotaSnapshot> {
        self.tenants
            .iter()
            .filter(|(_, state)| !is_lazy_empty(state))
            .map(|(tenant_id, state)| self.make_snapshot(tenant_id, state))
            .collect()
    }

    /// Return the number of committed Memory bytes that must be released before
    /// an admission of `incoming_bytes` can fit. This mirrors the C++
    /// `TenantQuotaTable::ComputeDeficit` rule and intentionally includes
    /// already-reserved bytes in the demand.
    pub fn compute_deficit(&self, tenant_id: &TenantId, incoming_bytes: u64) -> u64 {
        let Some(state) = self.tenants.get(tenant_id) else {
            return incoming_bytes;
        };
        let demand =
            state.used_bytes as u128 + state.reserved_bytes as u128 + incoming_bytes as u128;
        let deficit = demand.saturating_sub(state.effective_quota_bytes as u128);
        deficit.min(u64::MAX as u128) as u64
    }

    /// Clear runtime usage while retaining explicit tenant policies.
    /// Used before rebuilding accounting from a replacement snapshot.
    pub fn reset_usage(&mut self) {
        for state in self.tenants.values_mut() {
            state.used_bytes = 0;
            state.reserved_bytes = 0;
            state.committed_count = 0;
            state.metadata_object_count = 0;
            refresh_over_quota(state);
        }
        self.tenants.retain(|_, state| !is_lazy_empty(state));
    }

    pub fn recompute_effective_quotas(&mut self, capacity: u64) {
        for state in self.tenants.values_mut() {
            state.effective_quota_bytes = 0;
        }

        for (tenant_id, quota) in build_effective_quota_assignments(&self.tenants, capacity) {
            if let Some(state) = self.tenants.get_mut(&tenant_id) {
                state.effective_quota_bytes = quota;
            }
        }
        for state in self.tenants.values_mut() {
            refresh_over_quota(state);
        }
    }

    pub fn reserve(&mut self, tenant_id: &TenantId, bytes: u64) -> Result<(), TenantQuotaError> {
        let Some(state) = self.tenants.get_mut(tenant_id) else {
            return Err(TenantQuotaError::TenantNotRegistered);
        };
        if !state.has_explicit_policy {
            return Err(TenantQuotaError::TenantNotRegistered);
        }
        if bytes == 0 {
            return Ok(());
        }
        let next = state.used_bytes as u128 + state.reserved_bytes as u128 + bytes as u128;
        if next > state.effective_quota_bytes as u128 {
            return Err(TenantQuotaError::QuotaExceeded);
        }
        state.reserved_bytes = state
            .reserved_bytes
            .checked_add(bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        refresh_over_quota(state);
        Ok(())
    }

    pub fn commit(&mut self, tenant_id: &TenantId, bytes: u64) -> Result<(), TenantQuotaError> {
        self.settle(tenant_id, bytes, bytes, true)
    }

    /// Settle one reservation, optionally registering the object's first
    /// positive physical Memory charge.
    ///
    /// `reserved_bytes` is always removed from the reservation ledger, while
    /// only `committed_bytes` becomes physical Memory usage. This permits NoF-
    /// only objects (`committed_bytes == 0`) and permits partial Memory/NoF
    /// completion. Metadata object accounting is intentionally independent and
    /// starts when the object entry is created, not when its write completes.
    pub fn settle(
        &mut self,
        tenant_id: &TenantId,
        reserved_bytes: u64,
        committed_bytes: u64,
        register_committed_charge: bool,
    ) -> Result<(), TenantQuotaError> {
        if committed_bytes > reserved_bytes {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        let state = self.get_or_create_state(tenant_id);
        if state.reserved_bytes < reserved_bytes {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        let remaining_reserved_bytes = state.reserved_bytes - reserved_bytes;
        let used_bytes = state
            .used_bytes
            .checked_add(committed_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        used_bytes
            .checked_add(remaining_reserved_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        let committed_count = if register_committed_charge && committed_bytes != 0 {
            state
                .committed_count
                .checked_add(1)
                .ok_or(TenantQuotaError::AccountingMismatch)?
        } else {
            state.committed_count
        };
        state.reserved_bytes = remaining_reserved_bytes;
        state.used_bytes = used_bytes;
        state.committed_count = committed_count;
        refresh_over_quota(state);
        Ok(())
    }

    pub fn register_object(&mut self, tenant_id: &TenantId) {
        let state = self.get_or_create_state(tenant_id);
        state.metadata_object_count = state.metadata_object_count.saturating_add(1);
        refresh_over_quota(state);
    }

    pub fn unregister_object(&mut self, tenant_id: &TenantId) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        if state.metadata_object_count == 0 {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.metadata_object_count -= 1;
        refresh_over_quota(state);
        Ok(())
    }

    pub fn abort(&mut self, tenant_id: &TenantId, bytes: u64) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        if state.reserved_bytes < bytes {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.reserved_bytes -= bytes;
        refresh_over_quota(state);
        Ok(())
    }

    pub fn release(&mut self, tenant_id: &TenantId, bytes: u64) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        if state.used_bytes < bytes || (bytes != 0 && state.committed_count == 0) {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.used_bytes -= bytes;
        if bytes != 0 {
            state.committed_count -= 1;
        }
        refresh_over_quota(state);
        Ok(())
    }

    pub fn release_bytes(
        &mut self,
        tenant_id: &TenantId,
        bytes: u64,
    ) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        if state.used_bytes < bytes {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.used_bytes -= bytes;
        refresh_over_quota(state);
        Ok(())
    }

    pub fn remove_object(
        &mut self,
        tenant_id: &TenantId,
        committed_bytes: u64,
    ) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        if state.used_bytes < committed_bytes
            || (committed_bytes != 0 && state.committed_count == 0)
            || state.metadata_object_count == 0
        {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.used_bytes -= committed_bytes;
        if committed_bytes != 0 {
            state.committed_count -= 1;
        }
        state.metadata_object_count -= 1;
        refresh_over_quota(state);
        Ok(())
    }

    /// Rebuild committed accounting from durable object metadata. All
    /// counters are projected before mutation so an overflowing candidate can
    /// be rejected without contaminating the current ledger.
    pub fn restore_object_checked(
        &mut self,
        tenant_id: &TenantId,
        committed_bytes: u64,
    ) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        let used_bytes = state
            .used_bytes
            .checked_add(committed_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        used_bytes
            .checked_add(state.reserved_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        let committed_count = if committed_bytes == 0 {
            state.committed_count
        } else {
            state
                .committed_count
                .checked_add(1)
                .ok_or(TenantQuotaError::AccountingMismatch)?
        };
        let metadata_object_count = state
            .metadata_object_count
            .checked_add(1)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        state.used_bytes = used_bytes;
        state.committed_count = committed_count;
        state.metadata_object_count = metadata_object_count;
        refresh_over_quota(state);
        Ok(())
    }

    /// Rebuild an in-flight reservation from durable object metadata.
    pub fn restore_reservation_checked(
        &mut self,
        tenant_id: &TenantId,
        reserved_bytes: u64,
    ) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        let reserved_bytes = state
            .reserved_bytes
            .checked_add(reserved_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        state
            .used_bytes
            .checked_add(reserved_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        state.reserved_bytes = reserved_bytes;
        refresh_over_quota(state);
        Ok(())
    }

    /// Rebuild an in-flight size-changing replacement. The new object owns one
    /// reservation and one metadata slot while the old physical Memory charge
    /// remains live until the replacement commits or is revoked.
    pub fn restore_replacement_checked(
        &mut self,
        tenant_id: &TenantId,
        reserved_bytes: u64,
        pending_replaced_bytes: u64,
    ) -> Result<(), TenantQuotaError> {
        let state = self.get_or_create_state(tenant_id);
        let restored_reserved_bytes = state
            .reserved_bytes
            .checked_add(reserved_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        let used_bytes = state
            .used_bytes
            .checked_add(pending_replaced_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        used_bytes
            .checked_add(restored_reserved_bytes)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        let committed_count = if pending_replaced_bytes == 0 {
            state.committed_count
        } else {
            state
                .committed_count
                .checked_add(1)
                .ok_or(TenantQuotaError::AccountingMismatch)?
        };
        let metadata_object_count = state
            .metadata_object_count
            .checked_add(1)
            .ok_or(TenantQuotaError::AccountingMismatch)?;
        state.reserved_bytes = restored_reserved_bytes;
        state.used_bytes = used_bytes;
        state.committed_count = committed_count;
        state.metadata_object_count = metadata_object_count;
        refresh_over_quota(state);
        Ok(())
    }

    fn get_or_create_state(&mut self, tenant_id: &TenantId) -> &mut TenantQuotaState {
        self.tenants
            .entry(tenant_id.clone())
            .or_insert_with(TenantQuotaState::default)
    }

    fn snapshot_for_existing(&self, tenant_id: &TenantId) -> TenantQuotaSnapshot {
        self.make_snapshot(
            tenant_id,
            self.tenants.get(tenant_id).expect("tenant exists"),
        )
    }

    fn make_snapshot(&self, tenant_id: &TenantId, state: &TenantQuotaState) -> TenantQuotaSnapshot {
        TenantQuotaSnapshot {
            tenant_id: tenant_id.clone(),
            requested_quota_bytes: state.requested_quota_bytes,
            effective_quota_bytes: state.effective_quota_bytes,
            used_bytes: state.used_bytes,
            reserved_bytes: state.reserved_bytes,
            committed_count: state.committed_count,
            metadata_object_count: state.metadata_object_count,
            over_quota: state.over_quota,
            has_explicit_policy: state.has_explicit_policy,
        }
    }
}

fn is_lazy_empty(state: &TenantQuotaState) -> bool {
    !state.has_explicit_policy
        && state.used_bytes == 0
        && state.reserved_bytes == 0
        && state.committed_count == 0
        && state.metadata_object_count == 0
}

fn refresh_over_quota(state: &mut TenantQuotaState) {
    state.over_quota = (!state.has_explicit_policy && state.metadata_object_count > 0)
        || state.used_bytes as u128 + state.reserved_bytes as u128
            > state.effective_quota_bytes as u128;
}

fn build_effective_quota_assignments(
    tenants: &BTreeMap<TenantId, TenantQuotaState>,
    capacity: u64,
) -> Vec<(TenantId, u64)> {
    let explicit: Vec<_> = tenants
        .iter()
        .filter(|(_, state)| state.has_explicit_policy)
        .map(|(tenant_id, _)| tenant_id.clone())
        .collect();
    let explicit_sum = explicit.iter().fold(0u128, |sum, tenant_id| {
        sum + tenants[tenant_id].requested_quota_bytes as u128
    });

    let mut assigned = tenants
        .keys()
        .map(|tenant_id| (tenant_id.clone(), 0))
        .collect::<BTreeMap<_, _>>();
    if explicit_sum <= capacity as u128 {
        for tenant_id in &explicit {
            assigned.insert(tenant_id.clone(), tenants[tenant_id].requested_quota_bytes);
        }
    } else {
        distribute(&mut assigned, tenants, &explicit, capacity, true);
    }
    assigned.into_iter().collect()
}

fn distribute(
    assigned: &mut BTreeMap<TenantId, u64>,
    tenants: &BTreeMap<TenantId, TenantQuotaState>,
    tenant_ids: &[TenantId],
    capacity: u64,
    proportional: bool,
) {
    if tenant_ids.is_empty() || capacity == 0 {
        return;
    }
    let denominator = if proportional {
        tenant_ids.iter().fold(0u128, |sum, tenant_id| {
            sum + tenants[tenant_id].requested_quota_bytes as u128
        })
    } else {
        tenant_ids.len() as u128
    };
    if denominator == 0 {
        return;
    }
    let mut shares = Vec::with_capacity(tenant_ids.len());
    let mut base_total = 0u64;
    for tenant_id in tenant_ids {
        let numerator = if proportional {
            tenants[tenant_id].requested_quota_bytes as u128
        } else {
            1
        };
        let product = capacity as u128 * numerator;
        let base = (product / denominator) as u64;
        let remainder = product % denominator;
        base_total = base_total.saturating_add(base);
        shares.push((tenant_id.clone(), base, remainder));
    }
    shares.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    let mut remaining = capacity.saturating_sub(base_total);
    for (tenant_id, mut base, _) in shares {
        if remaining > 0 {
            base += 1;
            remaining -= 1;
        }
        assigned.insert(tenant_id, base);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacing_policy_layer_preserves_runtime_accounting() {
        let tenant = TenantId::new("tenant-a".to_string()).unwrap();
        let mut table = TenantQuotaTable::new(0);
        table.upsert_policy(&tenant, 100, 100).unwrap();
        table.reserve(&tenant, 20).unwrap();
        table.register_object(&tenant);

        let mut replacement = BTreeMap::new();
        replacement.insert(tenant.clone(), 80);
        table.replace_policies(&replacement, 80).unwrap();

        let snapshot = table.get_snapshot(&tenant).unwrap();
        assert_eq!(snapshot.requested_quota_bytes, 80);
        assert_eq!(snapshot.effective_quota_bytes, 80);
        assert_eq!(snapshot.reserved_bytes, 20);
        assert_eq!(snapshot.metadata_object_count, 1);
        assert!(snapshot.has_explicit_policy);
    }

    #[test]
    fn replacing_policy_layer_can_leave_accounting_only_tenant() {
        let tenant = TenantId::new("tenant-a".to_string()).unwrap();
        let mut table = TenantQuotaTable::new(0);
        table.upsert_policy(&tenant, 100, 100).unwrap();
        table.reserve(&tenant, 20).unwrap();

        table.replace_policies(&BTreeMap::new(), 100).unwrap();

        let snapshot = table.get_snapshot(&tenant).unwrap();
        assert_eq!(snapshot.reserved_bytes, 20);
        assert!(!snapshot.has_explicit_policy);
        assert!(snapshot.over_quota);
    }
}

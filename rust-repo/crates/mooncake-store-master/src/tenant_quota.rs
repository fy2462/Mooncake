use crate::service::helpers::normalize_tenant_id;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TenantQuotaSnapshot {
    pub tenant_id: String,
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
    tenants: BTreeMap<String, TenantQuotaState>,
}

impl TenantQuotaTable {
    pub fn new(_default_requested_quota_bytes: u64) -> Self {
        Self {
            tenants: BTreeMap::new(),
        }
    }

    pub fn upsert_policy(
        &mut self,
        tenant_id: &str,
        requested_quota_bytes: u64,
        capacity: u64,
    ) -> Result<TenantQuotaSnapshot, TenantQuotaError> {
        if requested_quota_bytes == 0 {
            return Err(TenantQuotaError::InvalidArgument);
        }
        let tenant_id = normalize_admin_tenant_id(tenant_id)?;
        let state = self.get_or_create_state(&tenant_id);
        state.requested_quota_bytes = requested_quota_bytes;
        state.has_explicit_policy = true;
        self.recompute_effective_quotas(capacity);
        Ok(self.snapshot_for_existing(&tenant_id))
    }

    pub fn erase_policy(
        &mut self,
        tenant_id: &str,
        capacity: u64,
    ) -> Result<Option<TenantQuotaSnapshot>, TenantQuotaError> {
        let tenant_id = normalize_admin_tenant_id(tenant_id)?;
        let Some(state) = self.tenants.get_mut(&tenant_id) else {
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
            .get(&tenant_id)
            .filter(|state| !is_lazy_empty(state))
            .map(|_| self.snapshot_for_existing(&tenant_id)))
    }

    pub fn get_snapshot(&self, tenant_id: &str) -> Option<TenantQuotaSnapshot> {
        let tenant_id = normalize_tenant_id(tenant_id);
        self.tenants
            .get(&tenant_id)
            .map(|state| self.make_snapshot(&tenant_id, state))
    }

    pub fn list_snapshots(&self) -> Vec<TenantQuotaSnapshot> {
        self.tenants
            .iter()
            .filter(|(_, state)| !is_lazy_empty(state))
            .map(|(tenant_id, state)| self.make_snapshot(tenant_id, state))
            .collect()
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

    pub fn reserve(&mut self, tenant_id: &str, bytes: u64) -> Result<(), TenantQuotaError> {
        let tenant_id = normalize_tenant_id(tenant_id);
        if bytes == 0 {
            self.get_or_create_state(&tenant_id);
            return Ok(());
        }
        let Some(state) = self.tenants.get_mut(&tenant_id) else {
            return Err(TenantQuotaError::QuotaExceeded);
        };
        if !state.has_explicit_policy {
            return Err(TenantQuotaError::QuotaExceeded);
        }
        let next = state.used_bytes as u128 + state.reserved_bytes as u128 + bytes as u128;
        if next > state.effective_quota_bytes as u128 {
            return Err(TenantQuotaError::QuotaExceeded);
        }
        state.reserved_bytes = state.reserved_bytes.saturating_add(bytes);
        refresh_over_quota(state);
        Ok(())
    }

    pub fn commit(&mut self, tenant_id: &str, bytes: u64) -> Result<(), TenantQuotaError> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let state = self.get_or_create_state(&tenant_id);
        if bytes == 0 {
            return Ok(());
        }
        if state.reserved_bytes < bytes {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.reserved_bytes -= bytes;
        state.used_bytes = state.used_bytes.saturating_add(bytes);
        state.committed_count = state.committed_count.saturating_add(1);
        state.metadata_object_count = state.metadata_object_count.saturating_add(1);
        refresh_over_quota(state);
        Ok(())
    }

    pub fn abort(&mut self, tenant_id: &str, bytes: u64) -> Result<(), TenantQuotaError> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let state = self.get_or_create_state(&tenant_id);
        if state.reserved_bytes < bytes {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.reserved_bytes -= bytes;
        refresh_over_quota(state);
        Ok(())
    }

    pub fn release(&mut self, tenant_id: &str, bytes: u64) -> Result<(), TenantQuotaError> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let state = self.get_or_create_state(&tenant_id);
        if state.used_bytes < bytes {
            return Err(TenantQuotaError::AccountingMismatch);
        }
        state.used_bytes -= bytes;
        if state.committed_count > 0 {
            state.committed_count -= 1;
        }
        if state.metadata_object_count > 0 {
            state.metadata_object_count -= 1;
        }
        refresh_over_quota(state);
        Ok(())
    }

    fn get_or_create_state(&mut self, tenant_id: &str) -> &mut TenantQuotaState {
        self.tenants
            .entry(tenant_id.to_string())
            .or_insert_with(TenantQuotaState::default)
    }

    fn snapshot_for_existing(&self, tenant_id: &str) -> TenantQuotaSnapshot {
        self.make_snapshot(
            tenant_id,
            self.tenants.get(tenant_id).expect("tenant exists"),
        )
    }

    fn make_snapshot(&self, tenant_id: &str, state: &TenantQuotaState) -> TenantQuotaSnapshot {
        TenantQuotaSnapshot {
            tenant_id: tenant_id.to_string(),
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

fn normalize_admin_tenant_id(tenant_id: &str) -> Result<String, TenantQuotaError> {
    let tenant_id = normalize_tenant_id(tenant_id);
    if tenant_id.is_empty()
        || tenant_id.starts_with('_')
        || tenant_id.bytes().any(|c| c < 0x20 || c == 0x7f)
    {
        return Err(TenantQuotaError::InvalidArgument);
    }
    Ok(tenant_id)
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
        || state.used_bytes.saturating_add(state.reserved_bytes) > state.effective_quota_bytes;
}

fn build_effective_quota_assignments(
    tenants: &BTreeMap<String, TenantQuotaState>,
    capacity: u64,
) -> Vec<(String, u64)> {
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
    assigned: &mut BTreeMap<String, u64>,
    tenants: &BTreeMap<String, TenantQuotaState>,
    tenant_ids: &[String],
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

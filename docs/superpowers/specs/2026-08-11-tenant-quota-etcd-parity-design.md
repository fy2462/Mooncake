# Tenant Quota etcd Persistence Parity Design

## Goal

Close the two remaining `tenant_quota_test.cpp` live-etcd rows with Rust
integration witnesses:

- `TenantQuotaPolicyStoreTest.EtcdMissingKeyLoadsEmptySnapshot`
- `TenantQuotaPolicyStoreTest.EtcdRoundTripsSnapshot`

## C++ Oracle

Both C++ tests use `MOONCAKE_TENANT_QUOTA_ETCD_ENDPOINTS`, isolate their data
under a process-specific cluster ID, and clean the key before and after the
test. A missing key loads an empty tenant map. Saving quotas for `tenant-a`
and `tenant-b` must then load the same map.

## Rust Boundary

Add two tests to `test_tenant_quota.rs` that exercise the public
`load_tenant_quota_policy` and `save_tenant_quota_policy` functions with the
`etcd` connector. The fixture will:

1. Read the same optional endpoint variable as C++ and return early with an
   explicit skip message when it is absent.
2. Generate a unique valid cluster ID per test so parallel or interrupted
   runs cannot share state.
3. Delete only that cluster's exact tenant-quota key before and after the
   witness through an etcd client.
4. For the missing-key test, assert the complete default snapshot.
5. For the round-trip test, save and reload an exact two-tenant snapshot.

Cleanup is guarded by a fixture `Drop` implementation so assertion failures do
not deliberately leave the test key behind. The connector API remains the
subject under test; direct etcd access is used only for fixture isolation.

## Scope

No production change is expected because the Rust connector already has the
missing-key and save/load paths. These are opt-in live-service tests, matching
the C++ hardware/service-gated contract. The ordinary test suite must still
compile and pass with a truthful skip when etcd is not configured.

## Verification

Run both exact tests without the environment variable to verify the default
skip path, run them against live etcd when available, run the complete tenant
quota test binary, scoped formatting, all parity validators, validator pytest,
both shell contracts, JSON parsing, and `git diff --check`. Record any missing
live-service prerequisite in the remediation ledger.

# Tenant Quota Admin HTTP Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close three C++ tenant-quota admin HTTP parity rows with real Rust router tests and exact compatibility error tokens.

**Architecture:** Exercise the existing Axum router and `MasterServiceImpl` directly, using a temporary file-backed quota policy and a mounted memory segment. Add a quota-handler-only error adapter so the HTTP boundary emits C++ tokens without changing tonic service messages, remove lazy-empty quota entries as C++ does, then update the validation ledger separately.

**Tech Stack:** Rust 2024, Axum, tonic, Tokio, tempfile, Cargo tests, Python parity validators.

## Global Constraints

- Work only in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- Treat all four manifests under `rust-repo/tools/store-validation/` as authoritative.
- Do not modify or format C/C++ files.
- Preserve existing tonic service error wording; translate only at the tenant-quota admin HTTP boundary.
- Match C++ `EraseIfLazyEmpty` after deleting an empty tenant policy.
- Use real router requests and service state, not mocks.
- Commit implementation before replacing remediation `PENDING` values; commit the ledger separately.

---

### Task 1: Add request-level tests and witness RED

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/admin_http.rs`
- Temporarily modify and restore: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: `admin_router`, `MasterServiceImpl::with_runtime_config`, `MasterService::{mount_segment,put_start,put_end,remove}`.
- Produces: `test_admin_http_tenant_quota_lifecycle`, `test_admin_http_tenant_quota_validation_errors`, and `test_admin_http_tenant_quota_disabled_conflict`.

- [ ] Temporarily bind the validation row to its planned stable test, run `validate_parity.py`, record the missing-test RED, and restore the manifest.
- [ ] Add a helper that creates a file-backed quota-configured service and optionally mounts `admin_quota_segment` for a stable client UUID.
- [ ] Add a tenant-aware committed-memory-key helper whose `PutStartRequest` and `PutEndRequest` both carry `tenant-a`.
- [ ] Add the lifecycle test: assert PUT quota 800 fields, list/single GET fields, create one object, assert DELETE 409 with `TENANT_NOT_EMPTY`, remove it, assert DELETE 200, and assert final GET 404.
- [ ] Add the five-request validation test and assert `400, 400, 400, 400, 404` in the C++ order.
- [ ] Add the disabled-mode GET test and assert 409 with `UNAVAILABLE_IN_CURRENT_MODE`.
- [ ] Run the three exact filters and record expected token failures for lifecycle and disabled mode while the validation characterization passes.

### Task 2: Add the minimal compatibility fixes

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/admin_http.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/tenant_quota.rs`

**Interfaces:**
- Consumes: tonic statuses returned by tenant quota service methods.
- Produces: `tenant_quota_status_error(tonic::Status) -> (StatusCode, Json<Value>)`.

- [ ] Implement `tenant_quota_status_error` by preserving `status_error`'s HTTP/code mapping and replacing only `tenant not empty` and `tenant quota is disabled` with their exact C++ tokens.
- [ ] Route quota list/get/upsert/delete service errors through the new adapter.
- [ ] After `erase_policy` recomputes quotas, remove the tenant entry if `is_lazy_empty` and return `None`, matching C++ `EraseIfLazyEmpty`.
- [ ] Run all three exact tests and the complete `admin_http::tests` module GREEN.
- [ ] Temporarily restore each old token in turn and require its exact test to fail, then restore the adapter and rerun GREEN.
- [ ] Run exact-file Rust 2024 rustfmt, inspect the scoped diff, and commit the docs/tests/adapter with a `[Store]` prefix.

### Task 3: Ledger, evidence, and gates

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`
- Update after commits: `/home/fy2462/Mooncake/.git/worktrees/ha-chaos-live/codex-parity-handoff.md`

**Interfaces:**
- Consumes: the three committed stable Rust tests from Task 1.
- Produces: three covered manifest rows and three remediation records with the implementation SHA.

- [ ] Change only the three named rows from `missing` to `covered` and bind each to its exact Rust test.
- [ ] Append exactly three remediation records with focused commands, implementation SHA, and `/tmp/mooncake-tenant-quota-admin-parity.typescript` evidence.
- [ ] Run all three fully-qualified exact tests for ten consecutive rounds; audit ten round markers, thirty one-test passes, zero failures, and zero-test runs.
- [ ] Run the complete admin HTTP module, complete Store Master library, and all-target cargo check.
- [ ] Run all four manifest validators, validator unit tests, shell contract tests, exact-file rustfmt, touched-file pre-commit through `/home/fy2462/Mooncake/.venv`, JSON validation, `git diff --check`, and zero C/C++ diff audit.
- [ ] Request independent review and address every actionable finding.
- [ ] Commit the ledger separately, update and audit the external handoff, and report exact aggregate covered/missing totals.

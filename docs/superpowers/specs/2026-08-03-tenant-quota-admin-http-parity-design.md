# Tenant Quota Admin HTTP Parity Design

## Scope

Close the three applicable `master_admin_server_test.cpp` tenant-quota admin
HTTP rows without changing C/C++:

- `MasterAdminServerTest.TenantQuotaAdminLifecycleEndpoints`
- `MasterAdminServerTest.TenantQuotaAdminValidationErrors`
- `MasterAdminServerTest.TenantQuotaAdminDisabledModeReturns409`

The Rust service and Axum routes already implement quota CRUD and validation.
The missing behavior is request-level coverage, the two exact C++ error tokens
exposed at the HTTP compatibility boundary, and removal of the lazy-empty quota
table entry after its explicit policy is deleted.

## Design

Add `tenant_quota_status_error`, a narrow adapter used only by the three quota
handlers. It preserves `status_error`'s HTTP status and numeric tonic code while
rewriting the two known service messages: `tenant not empty` becomes
`TENANT_NOT_EMPTY`, and `tenant quota is disabled` becomes
`UNAVAILABLE_IN_CURRENT_MODE`. Other messages remain unchanged, and the tonic
service API keeps its existing wording.

Align `TenantQuotaTable::erase_policy` with C++ `EraseIfLazyEmpty`: after the
policy layer is cleared and effective quotas are recomputed, remove the tenant
entry when it has no policy or accounting state. This makes a subsequent
single-tenant GET return 404 instead of exposing an internal empty shell.

Add three real-router tests in `admin_http.rs`. A shared fixture creates a
quota-configured `MasterServiceImpl` backed by a temporary file; the enabled
fixture mounts a memory segment. A tenant-aware put helper creates a committed
object through the production `put_start` and `put_end` methods so lifecycle
deletion exercises real tenant state. The tests issue actual Axum PUT, GET, and
DELETE requests and assert the C++ status, JSON fields, and error tokens.

No new production API, mock service, network listener, native TE library, or
C/C++ change is needed. The in-process router reaches the same Rust handlers and
service methods deterministically.

## Verification

First add all three tests and run exact filters. The lifecycle test must fail on
the old nonempty token and, after that is fixed, on the final 200-versus-404
empty-shell mismatch; the disabled-mode test must fail on its old lowercase
message. The validation test is an existing-behavior characterization. Then add
the narrow error adapter and lazy-empty cleanup and require all tests to pass.

Mutation-check both token mappings by temporarily restoring each old message
and requiring its exact test to fail. Run all three exact tests for ten rounds,
the complete admin HTTP module, the complete Store Master library, all four
manifest validators, validator self-tests, formatting/pre-commit checks, and a
zero C/C++ diff audit.

# Canonical TenantId Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry a validated canonical `TenantId` through the Rust Store master while preserving protobuf, snapshot, oplog, quota-policy, and scoped-key wire compatibility with C++ commit `c45fb07c`.

**Architecture:** Add one Store-owned domain type at the master crate boundary, then migrate durable state and quota accounting before converting RPC handlers. Raw strings remain only at protobuf, HTTP, configuration, logging, and serialization boundaries; every inbound boundary performs an explicit fallible conversion before state mutation.

**Tech Stack:** Rust 2024, tonic, serde, thiserror, DashMap, rmpv/rmp-serde, cargo test, pre-commit.

## Global Constraints

- Do not compile, link, or call the C++ `mooncake-store`; it is a behavioral oracle only.
- Do not change protobuf field types, field numbers, snapshot shapes, oplog JSON keys, quota-policy formats, or the scoped-key byte representation.
- Preserve `tenant + '\0' + local_key`; split only the first NUL so embedded NUL bytes in the local key round-trip.
- Empty tenant input normalizes to `default` except strict write admission, where it retains tenant-not-registered behavior.
- Reject `_` prefix, bytes below `0x20`, and byte `0x7f`; accept printable punctuation and valid non-ASCII UTF-8.
- Validate a batch request tenant before processing any item or mutating state.
- Use red-green TDD and keep each commit independently testable.

---

### Task 1: Introduce the canonical TenantId domain type

**Files:**
- Create: `rust-repo/crates/mooncake-store-master/src/tenant_id.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/lib.rs`
- Test: `rust-repo/crates/mooncake-store-master/src/tenant_id.rs`

**Interfaces:**
- Consumes: untrusted UTF-8 `String` values and legacy scoped-key strings.
- Produces: `TenantId`, `TenantIdError`, `TenantId::new(String) -> Result<Self, TenantIdError>`, `TenantId::as_str() -> &str`, `TenantId::into_string() -> String`, `TenantId::make_scoped_key(&self, &str) -> String`, and `TenantId::parse_scoped_key(&str) -> Result<(Self, String), TenantIdError>`.

- [ ] **Step 1: Write the failing domain tests**

Add a `#[cfg(test)]` module that asserts normalization, validation, ordering,
hashing, serde, and exact scoped-key compatibility:

```rust
#[test]
fn normalizes_and_validates_canonical_values() {
    assert_eq!(TenantId::new(String::new()).unwrap(), TenantId::default());
    assert!(TenantId::new("tenant:模型".into()).is_ok());
    assert!(matches!(TenantId::new("_reserved".into()), Err(TenantIdError::ReservedPrefix)));
    for value in ["bad\nname", "bad\0name", "bad\u{7f}"] {
        assert!(matches!(TenantId::new(value.into()), Err(TenantIdError::InvalidByte(_))));
    }
}

#[test]
fn scoped_key_preserves_wire_format_and_local_nuls() {
    let tenant = TenantId::new("tenant:one".into()).unwrap();
    let local = "part1\0part2";
    let scoped = tenant.make_scoped_key(local);
    assert_eq!(scoped.as_bytes(), b"tenant:one\0part1\0part2");
    assert_eq!(TenantId::parse_scoped_key(&scoped).unwrap(), (tenant, local.into()));
    assert_eq!(TenantId::parse_scoped_key("legacy").unwrap(), (TenantId::default(), "legacy".into()));
}

#[test]
fn serde_rejects_invalid_tenant_values() {
    assert_eq!(serde_json::to_string(&TenantId::default()).unwrap(), "\"default\"");
    assert!(serde_json::from_str::<TenantId>("\"_reserved\"").is_err());
}
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```bash
cd rust-repo
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master tenant_id --lib
```

Expected: compilation fails because `tenant_id` and `TenantId` do not exist.

- [ ] **Step 3: Implement the domain type and crate export**

Implement an owned, validated newtype with manual deserialization:

```rust
use serde::{Deserialize, Deserializer, Serialize};
use std::borrow::Borrow;
use std::fmt;
use thiserror::Error;

pub const DEFAULT_TENANT: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct TenantId(String);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TenantIdError {
    #[error("tenant id uses the reserved '_' prefix")]
    ReservedPrefix,
    #[error("tenant id contains invalid byte 0x{0:02x}")]
    InvalidByte(u8),
}

impl TenantId {
    pub fn new(raw: String) -> Result<Self, TenantIdError> {
        let value = if raw.is_empty() { DEFAULT_TENANT.to_owned() } else { raw };
        if value.starts_with('_') { return Err(TenantIdError::ReservedPrefix); }
        if let Some(byte) = value.bytes().find(|byte| *byte < 0x20 || *byte == 0x7f) {
            return Err(TenantIdError::InvalidByte(byte));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str { &self.0 }
    pub fn into_string(self) -> String { self.0 }
    pub fn is_default(&self) -> bool { self.0 == DEFAULT_TENANT }

    pub fn make_scoped_key(&self, local_key: &str) -> String {
        let mut key = String::with_capacity(self.0.len() + 1 + local_key.len());
        key.push_str(&self.0);
        key.push('\0');
        key.push_str(local_key);
        key
    }

    pub fn parse_scoped_key(scoped: &str) -> Result<(Self, String), TenantIdError> {
        match scoped.split_once('\0') {
            Some((tenant, key)) => Ok((Self::new(tenant.to_owned())?, key.to_owned())),
            None => Ok((Self::default(), scoped.to_owned())),
        }
    }
}

impl Default for TenantId {
    fn default() -> Self { Self(DEFAULT_TENANT.to_owned()) }
}
impl AsRef<str> for TenantId { fn as_ref(&self) -> &str { self.as_str() } }
impl Borrow<str> for TenantId { fn borrow(&self) -> &str { self.as_str() } }
impl fmt::Display for TenantId { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) } }
impl<'de> Deserialize<'de> for TenantId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
```

Declare `pub mod tenant_id;` and re-export `TenantId`, `TenantIdError`, and
`DEFAULT_TENANT` from `lib.rs`.

- [ ] **Step 4: Run focused tests and formatting**

Run the Task 1 test command again, then:

```bash
cd rust-repo
cargo fmt --check
```

Expected: all `tenant_id` tests pass and formatting is clean.

- [ ] **Step 5: Commit the domain type**

```bash
git add rust-repo/crates/mooncake-store-master/src/tenant_id.rs \
        rust-repo/crates/mooncake-store-master/src/lib.rs
git commit -m "[Store] add canonical Rust TenantId"
```

### Task 2: Migrate Store state and quota accounting to TenantId

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/state.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/helpers.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/tenant_quota.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/tenant_quota_policy_store.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/kv_event.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_tenant_quota.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_tenant_isolation.rs`

**Interfaces:**
- Consumes: the Task 1 `TenantId` type.
- Produces: `ObjectEntry::tenant_id: TenantId`, `TenantQuotaTable` keyed by `TenantId`, typed quota APIs, and compatibility helpers returning validated tenant identities.

- [ ] **Step 1: Add failing quota and object-state type tests**

Add tests proving `TenantQuotaTable` accepts `&TenantId`, rejects no invalid
identity internally, and preserves deterministic proportional assignment:

```rust
let alpha = TenantId::new("alpha".into()).unwrap();
let beta = TenantId::new("beta".into()).unwrap();
let mut table = TenantQuotaTable::new(0);
table.upsert_policy(&alpha, 2, 3).unwrap();
table.upsert_policy(&beta, 2, 3).unwrap();
assert_eq!(table.get_snapshot(&alpha).unwrap().effective_quota_bytes, 2);
assert_eq!(table.get_snapshot(&beta).unwrap().effective_quota_bytes, 1);
```

Update one isolation fixture to construct `ObjectEntry` with
`TenantId::default()` and assert its tenant remains typed after retrieval.

- [ ] **Step 2: Run focused tests and verify compilation fails**

```bash
cd rust-repo
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_tenant_quota --test test_tenant_isolation
```

Expected: type mismatch failures while quota and object state still use
`String`.

- [ ] **Step 3: Migrate state and quota keys**

Change `ObjectEntry::tenant_id` and `TenantQuotaSnapshot::tenant_id` to
`TenantId`, replace `BTreeMap<String, TenantQuotaState>` with
`BTreeMap<TenantId, TenantQuotaState>`, and update quota methods to receive
`&TenantId`. Keep policy YAML/etcd maps string-shaped by parsing keys on load
and calling `as_str()` on save.

Replace helper normalization with typed compatibility wrappers:

```rust
pub fn resolve_request_tenant(raw: &str, strict: bool) -> Result<TenantId, Status> {
    if !strict { return Ok(TenantId::default()); }
    TenantId::new(raw.to_owned()).map_err(|error| Status::invalid_argument(error.to_string()))
}

pub fn resolve_write_tenant(raw: &str, strict: bool) -> Result<TenantId, Status> {
    if !strict { return Ok(TenantId::default()); }
    if raw.is_empty() { return Err(Status::resource_exhausted("tenant not registered")); }
    TenantId::new(raw.to_owned())
        .map_err(|_| Status::resource_exhausted("tenant not registered"))
}
```

Change internal quota and event methods to take `&TenantId`; convert to string
only while constructing protobuf/JSON/event wire values.

- [ ] **Step 4: Mechanically update typed object fixtures and run tests**

Use compiler errors to replace only internal `ObjectEntry` tenant fields with
`TenantId::default()` or `TenantId::new(...).unwrap()`. Do not change protobuf
request fields, which must remain strings.

Run the Task 2 focused command and `cargo test -p mooncake-store-master --lib`.
Expected: both focused integration suites and all library tests pass.

- [ ] **Step 5: Commit the state migration**

```bash
git add rust-repo/crates/mooncake-store-master/src/service \
        rust-repo/crates/mooncake-store-master/src/tenant_quota.rs \
        rust-repo/crates/mooncake-store-master/src/tenant_quota_policy_store.rs \
        rust-repo/crates/mooncake-store-master/src/kv_event.rs \
        rust-repo/crates/mooncake-store-master/tests
git commit -m "[Store] carry TenantId through Rust master state"
```

### Task 3: Enforce TenantId at snapshot and oplog boundaries

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog/oplog_manager.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/oplog/oplog_wire.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/oplog_applier.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/catalog_snapshot.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_oplog.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs`

**Interfaces:**
- Consumes: typed `ObjectEntry` and `TenantId::parse_scoped_key`.
- Produces: unchanged string wire fields plus fallible recovery that rejects invalid or conflicting tenant identities.

- [ ] **Step 1: Add failing invalid-recovery and compatibility tests**

Add vectors that prove:

```rust
// Legacy empty tenant becomes default.
assert_eq!(restored.tenant_id, TenantId::default());
// Explicit invalid tenants do not enter state.
assert!(decode_catalog_with_tenant("_reserved").unwrap_err().to_string().contains("tenant"));
assert!(!apply_oplog_put_end_with_tenant("bad\nname"));
// Encoding remains a string field with the same value.
assert_eq!(encoded_json["tenant_id"], "tenant-a");
```

Add a catalog vector whose scoped tenant and metadata tenant disagree and
assert contextual recovery failure.

- [ ] **Step 2: Run recovery tests and verify failure**

```bash
cd rust-repo
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_oplog --test test_catalog_snapshot
```

Expected: invalid tenant vectors are currently accepted or fail to compile.

- [ ] **Step 3: Add explicit wire conversions**

Keep `OpLogRecord` serialization string-compatible but construct it from a
`&TenantId`. In catalog encoding use `object.tenant_id.as_str()`. During
catalog and oplog recovery call `TenantId::new`; map failures to contextual
`HaError`/corrupt-record results. Build scoped keys only through
`tenant_id.make_scoped_key(user_key)`.

For legacy keys, parse the first NUL with `TenantId::parse_scoped_key`. If both
the durable key and metadata specify tenants, require equality before inserting
the object.

- [ ] **Step 4: Run recovery and HA regression suites**

Run the Task 3 focused command plus:

```bash
cd rust-repo
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_ha --test test_storage
```

Expected: all tests pass without snapshot/oplog shape changes.

- [ ] **Step 5: Commit persistence-boundary enforcement**

```bash
git add rust-repo/crates/mooncake-store-master/src/oplog.rs \
        rust-repo/crates/mooncake-store-master/src/oplog \
        rust-repo/crates/mooncake-store-master/src/ha \
        rust-repo/crates/mooncake-store-master/tests/test_oplog.rs \
        rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs
git commit -m "[Store] validate TenantId during Rust recovery"
```

### Task 4: Resolve tenants once at every request boundary

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_put.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_query.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_upsert.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches/batch_put_start.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches/batch_evict.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_replication.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_tasks.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/cluster/promotion.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/cluster/offload.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/cluster/drain.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/admin_http.rs`
- Test: `rust-repo/crates/mooncake-store-master/tests/test_tenant_id_parity.rs`

**Interfaces:**
- Consumes: `resolve_request_tenant`, `resolve_write_tenant`, and typed state APIs.
- Produces: complete request-level parity with C++ tenant validation and batch atomicity.

- [ ] **Step 1: Add failing request-boundary parity tests**

Create helpers that instantiate services with strict mode on/off and cover:

```rust
#[tokio::test]
async fn strict_put_rejects_empty_and_invalid_tenants_before_mutation() {
    for tenant in ["", "_reserved", "bad\nname", "bad\u{7f}"] {
        let service = strict_service();
        let error = put_start(&service, tenant).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(service.object_count_for_test(), 0);
    }
}

#[tokio::test]
async fn non_strict_requests_ignore_tenant_and_use_default() {
    let service = non_strict_service();
    put_complete(&service, "_ignored", "key").await;
    assert!(exists(&service, "another-ignored-value", "key").await);
}

#[tokio::test]
async fn invalid_batch_tenant_is_rejected_before_any_item_changes_state() {
    let service = strict_service();
    let error = batch_put_start(&service, "_reserved", &["a", "b"]).await.unwrap_err();
    assert!(matches!(error.code(), tonic::Code::ResourceExhausted | tonic::Code::InvalidArgument));
    assert_eq!(service.object_count_for_test(), 0);
}
```

Add read/remove/admin cases asserting invalid ordinary tenants map to
`InvalidArgument` and valid `tenant:with:colon` remains isolated.

- [ ] **Step 2: Run the new integration test and verify failure**

```bash
cd rust-repo
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master --test test_tenant_id_parity
```

Expected: tests fail because handlers still normalize raw strings locally.

- [ ] **Step 3: Migrate object, batch, replication, and task handlers**

At the start of each handler, resolve once using
`self.state.runtime_config.enable_tenant_quota`. Use write resolution before
PutStart, BatchPutStart, Upsert admission, and operations that create new
tenant-owned state. Use ordinary request resolution for reads, completion,
remove, replica, promotion, replication, task, and admin paths.

Pass `&TenantId` through internal helpers and derive scoped keys with
`tenant_id.make_scoped_key(&req.key)`. For protobuf responses and task/event
payloads use `tenant_id.as_str().to_owned()` explicitly.

- [ ] **Step 4: Prove batch atomicity and run affected suites**

Run the Task 4 test plus:

```bash
cd rust-repo
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master \
    --test test_master_object --test test_batch --test test_master_replication_invalid \
    --test test_master_tasks --test test_master_promotion --test test_master_offload \
    --test test_drain_job
```

Expected: all suites pass, including assertions that failed batch validation
leaves objects, quota reservations, and task queues unchanged.

- [ ] **Step 5: Commit request-boundary parity**

```bash
git add rust-repo/crates/mooncake-store-master/src/service \
        rust-repo/crates/mooncake-store-master/src/admin_http.rs \
        rust-repo/crates/mooncake-store-master/tests/test_tenant_id_parity.rs
git commit -m "[Store] enforce TenantId across Rust requests"
```

### Task 5: Audit full-chain typing and close regression gaps

**Files:**
- Modify: files reported by the audit only when the remaining `String` is Store-owned tenant identity.
- Modify: `rust-repo/docs/superpowers/specs/2026-07-24-canonical-tenant-id-parity-design.md`
- Create: `rust-repo/change_logs/2026-07-24-001.md`

**Interfaces:**
- Consumes: Tasks 1-4 implementation and tests.
- Produces: explicit evidence that raw tenant strings remain only at approved external boundaries.

- [ ] **Step 1: Audit remaining tenant string storage**

Run:

```bash
rg -n "tenant_id:\s*String|BTreeMap<String, TenantQuota|HashMap<String, Tenant" \
  rust-repo/crates/mooncake-store-master/src
rg -n "normalize_tenant_id|make_tenant_scoped_key|split_scoped_key" \
  rust-repo/crates/mooncake-store-master/src
```

Classify every match as protobuf/wire, configuration, logging, user data, or a
remaining Store-domain gap. Convert every domain gap to `TenantId`; document
the allowed boundary matches in the change log.

- [ ] **Step 2: Run focused and crate-wide verification**

```bash
cd rust-repo
cargo fmt --check
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo test -p mooncake-store-master
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target \
  cargo clippy -p mooncake-store-master --all-targets -- -D warnings
```

Expected: formatting, all master tests, and Clippy pass. If Clippy exposes a
pre-existing unrelated baseline failure, record the exact diagnostic and run a
focused no-warning check for every touched target instead of changing unrelated
code.

- [ ] **Step 3: Run pre-commit on exactly the touched files**

List the changed files with `git diff --name-only <design-commit>..HEAD`, then
run the repository's configured pre-commit command only on that list. Do not
include hook rewrites to unrelated files.

- [ ] **Step 4: Record disposition and verification evidence**

Write `rust-repo/change_logs/2026-07-24-001.md` with:

- upstream commit `c45fb07c` and the observable contract implemented;
- the full-chain type boundary and unchanged wire formats;
- focused, crate-wide, Clippy, and pre-commit command results;
- any environment-gated checks with their exact missing prerequisite.

Update the design's implementation-status section with the implementation
commit IDs and final commands.

- [ ] **Step 5: Commit the audit**

```bash
git add rust-repo/docs/superpowers/specs/2026-07-24-canonical-tenant-id-parity-design.md \
        rust-repo/change_logs/2026-07-24-001.md
git commit -m "[Store] record canonical TenantId parity"
```

### Task 6: Review the TenantId slice before the next parity subsystem

**Files:**
- Inspect: all files changed since design commit `2a58bac5`.

**Interfaces:**
- Consumes: the complete TenantId slice.
- Produces: a review verdict and a clean starting point for the independent OffsetAllocator persistence design.

- [ ] **Step 1: Review scope and compatibility**

Confirm the diff contains no protobuf schema change, no C++ Store dependency,
no scoped-key byte change, and no unrelated formatting.

- [ ] **Step 2: Review requirement-to-evidence mapping**

Map every requirement in the approved design to a source location and a
passing test. Treat missing or indirect evidence as incomplete work.

- [ ] **Step 3: Fix and reverify any findings**

For each finding, add a failing regression test, run it to observe failure,
apply the minimal correction, and rerun the focused and master-wide commands.

- [ ] **Step 4: Hand off to OffsetAllocator design**

Once review is clean, start a separate brainstorming/spec/plan cycle for
OffsetAllocator crash-consistent persistence; do not fold that independent
storage protocol into the TenantId commits.

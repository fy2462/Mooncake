# Batch Query IP Snapshot Parity Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. For this run, execute inline with
> `superpowers:executing-plans`; subagent delegation is not permitted.

**Goal:** Prove all eight C++ BatchQueryIp snapshot behaviors through Rust's
real native save, fresh-master restore, second save, and second restore path.

**Architecture:** Add eight integration tests to `test_storage.rs`, each with a
distinct C++-matching fixture and one shared double-roundtrip helper. Compare
normalized BatchQueryIp results and a canonical complete state for these
segment-only fixtures; upgrade manifest evidence only after all eight tests
execute successfully.

**Tech Stack:** Rust, Tokio, tonic, Mooncake Store master service, native
LocalDisk MessagePack snapshots, Cargo, schema-v2 parity manifests, Python
validation tooling, Git, and repository pre-commit hooks.

**Execution status:** Deferred after the planned RED phase exposed a real HA
state-model divergence. Seven populated fixtures consistently failed because
Rust intentionally scrubs restored runtime coordinates pending remount. The
test experiment was removed, the original 28-test storage target was restored,
and the eight parity rows remain `missing`. See the design's “Investigation
Outcome (Deferred)” section for the root-cause boundary and the required future
architecture decision. The remaining unchecked implementation and evidence
steps are intentionally not executed under this plan.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on
  `codex/store-cpp-parity-only`.
- Never edit, format, build, link, load, import, or execute C/C++ reference
  inputs or `mooncake-wheel/tests`; they are read-only source oracles.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling.
- Use `apply_patch` for semantic edits.
- Every pre-commit invocation sets `SKIP=mooncake-code-format`.
- Keep etcd-only production HA policy unchanged.
- Do not modify protobufs, public APIs, snapshot format versions, production
  HA configuration, or persistence policy unless a semantic test failure proves
  the approved design cannot pass without such a change.
- Do not start Store LocalDisk io_uring work.
- An immediately GREEN evidence test is acceptable; do not manufacture a RED.
- If a test fails semantically, keep all eight snapshot manifest rows missing,
  invoke systematic debugging, and revise the design before any production
  change outside the inspected snapshot/query path.
- Ordinary BatchQueryIp, snapshot BatchReplicaClear, SSD LocalDisk, TE, TENT,
  and wheel dispositions remain unchanged.

---

### Task 1: Add the shared native snapshot harness and eight exact tests

**Files:**

- Modify: `rust-repo/crates/mooncake-store-master/tests/test_storage.rs`

**Interfaces:**

- Consumes: `MasterServiceImpl::new_with_runtime_config`,
  `MasterService::mount_segment`, `MasterService::batch_query_ip`,
  `MasterServiceImpl::capture_loaded_snapshot`, and
  `MasterServiceImpl::save_snapshot`.
- Produces: eight exact discoverable tests named in the approved design and a
  shared test-only double-roundtrip harness.

- [ ] **Step 1: Verify the clean isolated baseline**

Run:

```bash
git status --short --branch
git rev-parse --show-superproject-working-tree
git rev-parse --git-dir
git rev-parse --git-common-dir
cd rust-repo
cargo test -p mooncake-store-master --test test_storage
```

Expected: the linked worktree is on `codex/store-cpp-parity-only`, no tracked
changes exist, this is not a submodule, and all current 28 `test_storage` tests
pass. Existing unrelated unused/dead-code warnings may remain.

- [ ] **Step 2: Add imports, canonical types, and snapshot service constructor**

Change the relevant imports to:

```rust
use mooncake_store_master::allocator::AllocatorSnapshotConfig;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
```

Keep the existing imports. Add these test-only definitions near the top-level
helpers:

```rust
type NormalizedIpMap = BTreeMap<String, Vec<String>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct QueryIpSnapshotSegment {
    id: Uuid,
    name: String,
    base: u64,
    size: u64,
    te_endpoint: String,
    protocol: String,
    host_id: String,
    used: u64,
    client_id: Uuid,
    status: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryIpSnapshotState {
    snapshot_sequence_id: u64,
    allocator_config: Option<AllocatorSnapshotConfig>,
    segments: Vec<QueryIpSnapshotSegment>,
}

fn query_ip_snapshot_service(path: &Path) -> MasterServiceImpl {
    let mut runtime_config = MasterRuntimeConfig::default();
    runtime_config.snapshot_retention_count = 0;
    MasterServiceImpl::new_with_runtime_config(
        Some(StorageBackendType::LocalDisk),
        Some(path.to_path_buf()),
        runtime_config,
    )
}
```

Retention is disabled only in these test services. The current snapshot writer
closes and fsyncs the temporary file before atomically renaming it to
`master_snapshot.msgpack`; with retention disabled it performs no later file
operation on that path.

- [ ] **Step 3: Add mount, normalized query, canonical state, and bounded wait helpers**

Add:

```rust
async fn mount_query_ip_snapshot_segment(
    service: &MasterServiceImpl,
    client_id: Uuid,
    segment_name: &str,
    base_addr: u64,
    te_endpoint: &str,
) {
    MasterService::mount_segment(
        service,
        Request::new(proto::MountSegmentRequest {
            client_id: Some(proto_uuid(client_id)),
            segment_name: segment_name.into(),
            size: 16 * 1024 * 1024,
            base_addr,
            te_endpoint: te_endpoint.into(),
            protocol: String::new(),
            host_id: String::new(),
        }),
    )
    .await
    .unwrap();
}

async fn normalized_batch_query_ips(
    service: &MasterServiceImpl,
    client_ids: &[Uuid],
) -> NormalizedIpMap {
    let response = MasterService::batch_query_ip(
        service,
        Request::new(proto::BatchQueryIpRequest {
            client_ids: client_ids.iter().copied().map(proto_uuid).collect(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    response
        .ips
        .into_iter()
        .map(|(client_id, mut list)| {
            list.addresses.sort();
            (client_id, list.addresses)
        })
        .collect()
}

fn capture_query_ip_snapshot_state(service: &MasterServiceImpl) -> QueryIpSnapshotState {
    let snapshot = service.capture_loaded_snapshot("batch-query-ip-snapshot-parity");
    assert!(snapshot.nof_segments.is_empty());
    assert!(snapshot.objects.is_empty());
    assert!(snapshot.tasks.is_empty());
    assert!(snapshot.replication_tasks.is_empty());
    assert!(snapshot.graceful_unmounts.is_empty());
    assert!(snapshot.delayed_replica_releases.is_empty());
    assert!(snapshot.local_disk_segments.is_empty());
    let mut segments = snapshot
        .segments
        .into_iter()
        .map(|entry| QueryIpSnapshotSegment {
            id: entry.segment.id,
            name: entry.segment.name,
            base: entry.segment.base,
            size: entry.segment.size,
            te_endpoint: entry.segment.te_endpoint,
            protocol: entry.segment.protocol,
            host_id: entry.segment.host_id,
            used: entry.used,
            client_id: entry.client_id,
            status: entry.status as i32,
        })
        .collect::<Vec<_>>();
    segments.sort();
    QueryIpSnapshotState {
        snapshot_sequence_id: snapshot.snapshot_sequence_id,
        allocator_config: snapshot.allocator_config,
        segments,
    }
}

async fn wait_for_native_snapshot(path: &Path) {
    for _ in 0..200 {
        if path
            .metadata()
            .is_ok_and(|metadata| metadata.len() > 0)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("native snapshot was not published at {}", path.display());
}
```

Do not call `dedup` while normalizing addresses: duplicate removal is behavior
under test, and sorting alone retains an erroneous duplicate.

- [ ] **Step 4: Add the shared double-roundtrip assertion**

Add:

```rust
async fn assert_batch_query_ip_snapshot_roundtrip(
    service: MasterServiceImpl,
    path: &Path,
    client_ids: &[Uuid],
    expected: &NormalizedIpMap,
) {
    assert_eq!(normalized_batch_query_ips(&service, client_ids).await, *expected);
    let expected_state = capture_query_ip_snapshot_state(&service);
    let latest_snapshot = path.join("master_snapshot.msgpack");

    service.save_snapshot();
    wait_for_native_snapshot(&latest_snapshot).await;
    drop(service);

    let restored = query_ip_snapshot_service(path);
    assert_eq!(capture_query_ip_snapshot_state(&restored), expected_state);
    assert_eq!(normalized_batch_query_ips(&restored, client_ids).await, *expected);

    let first_snapshot = path.join("first_master_snapshot.msgpack");
    std::fs::rename(&latest_snapshot, &first_snapshot).unwrap();
    restored.save_snapshot();
    wait_for_native_snapshot(&latest_snapshot).await;
    drop(restored);

    assert!(
        first_snapshot
            .metadata()
            .is_ok_and(|metadata| metadata.len() > 0),
        "first native snapshot must remain available"
    );
    let restored_twice = query_ip_snapshot_service(path);
    assert_eq!(
        capture_query_ip_snapshot_state(&restored_twice),
        expected_state
    );
    assert_eq!(
        normalized_batch_query_ips(&restored_twice, client_ids).await,
        *expected
    );
}
```

- [ ] **Step 5: Add the single/unknown, multi-segment, and empty-request tests**

Add:

```rust
#[tokio::test]
async fn batch_query_ip_snapshot_single_and_unknown_client_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    let client_id = Uuid::new_v4();
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-single",
        0x300000000,
        "127.0.0.1:12345",
    )
    .await;
    let unknown = Uuid::new_v4();
    let expected = BTreeMap::from([(
        client_id.to_string(),
        vec!["127.0.0.1".to_string()],
    )]);
    assert_batch_query_ip_snapshot_roundtrip(
        service,
        &path,
        &[client_id, unknown],
        &expected,
    )
    .await;
}

#[tokio::test]
async fn batch_query_ip_snapshot_deduplicates_multiple_segment_addresses_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    let client_id = Uuid::new_v4();
    for (index, (name, endpoint)) in [
        ("snapshot-query-multi-a", "127.0.0.1:12345"),
        ("snapshot-query-multi-b", "127.0.0.1:12346"),
        ("snapshot-query-multi-c", "192.168.1.1:12345"),
    ]
    .into_iter()
    .enumerate()
    {
        mount_query_ip_snapshot_segment(
            &service,
            client_id,
            name,
            0x300000000 + index as u64 * 0x100000000,
            endpoint,
        )
        .await;
    }
    let expected = BTreeMap::from([(
        client_id.to_string(),
        vec!["127.0.0.1".to_string(), "192.168.1.1".to_string()],
    )]);
    assert_batch_query_ip_snapshot_roundtrip(service, &path, &[client_id], &expected).await;
}

#[tokio::test]
async fn batch_query_ip_snapshot_empty_request_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    assert_batch_query_ip_snapshot_roundtrip(service, &path, &[], &BTreeMap::new()).await;
}
```

- [ ] **Step 6: Add the empty-endpoint and four IPv6/mixed tests**

Add:

```rust
#[tokio::test]
async fn batch_query_ip_snapshot_retains_client_with_only_empty_endpoints_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    let client_id = Uuid::new_v4();
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-empty-a",
        0x300000000,
        "",
    )
    .await;
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-empty-b",
        0x400000000,
        "",
    )
    .await;
    let expected = BTreeMap::from([(client_id.to_string(), Vec::new())]);
    assert_batch_query_ip_snapshot_roundtrip(service, &path, &[client_id], &expected).await;
}

#[tokio::test]
async fn batch_query_ip_snapshot_parses_bracketed_ipv6_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    let client_id = Uuid::new_v4();
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-bracketed-v6",
        0x300000000,
        "[::1]:17813",
    )
    .await;
    let expected = BTreeMap::from([(client_id.to_string(), vec!["::1".to_string()])]);
    assert_batch_query_ip_snapshot_roundtrip(service, &path, &[client_id], &expected).await;
}

#[tokio::test]
async fn batch_query_ip_snapshot_preserves_ipv6_scope_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    let client_id = Uuid::new_v4();
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-scoped-v6",
        0x300000000,
        "fe80::a236:bcff:fecb:a1be%eno2:15773",
    )
    .await;
    let expected = BTreeMap::from([(
        client_id.to_string(),
        vec!["fe80::a236:bcff:fecb:a1be%eno2".to_string()],
    )]);
    assert_batch_query_ip_snapshot_roundtrip(service, &path, &[client_id], &expected).await;
}

#[tokio::test]
async fn batch_query_ip_snapshot_accepts_ipv6_without_port_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    let client_id = Uuid::new_v4();
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-raw-v6",
        0x300000000,
        "::1",
    )
    .await;
    let expected = BTreeMap::from([(client_id.to_string(), vec!["::1".to_string()])]);
    assert_batch_query_ip_snapshot_roundtrip(service, &path, &[client_id], &expected).await;
}

#[tokio::test]
async fn batch_query_ip_snapshot_mixed_ipv4_ipv6_parity() {
    let path = temp_dir();
    let service = query_ip_snapshot_service(&path);
    let client_id = Uuid::new_v4();
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-mixed-v4",
        0x300000000,
        "192.168.1.1:12345",
    )
    .await;
    mount_query_ip_snapshot_segment(
        &service,
        client_id,
        "snapshot-query-mixed-v6",
        0x400000000,
        "[::1]:17813",
    )
    .await;
    let expected = BTreeMap::from([(
        client_id.to_string(),
        vec!["192.168.1.1".to_string(), "::1".to_string()],
    )]);
    assert_batch_query_ip_snapshot_roundtrip(service, &path, &[client_id], &expected).await;
}
```

- [ ] **Step 7: Format and run the exact eight-test subset**

Run:

```bash
cd rust-repo
rustfmt --edition 2024 crates/mooncake-store-master/tests/test_storage.rs
cargo test -p mooncake-store-master --test test_storage batch_query_ip_snapshot_
```

Expected: eight tests execute. GREEN is acceptable evidence. A compilation or
fixture failure is corrected in test code and rerun. A semantic state/query
failure triggers systematic debugging and stops manifest work; do not weaken
the assertions.

- [ ] **Step 8: Run the complete owning target and commit the tests**

Run:

```bash
cargo test -p mooncake-store-master --test test_storage
cargo fmt --check -p mooncake-store-master
cd ..
git diff --check
git diff --quiet -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
git diff --cached --quiet -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
git add rust-repo/crates/mooncake-store-master/tests/test_storage.rs
git diff --cached --check
git commit -m "[Store] cover batch query ip snapshot parity"
```

Expected: 36 `test_storage` tests pass and only the Rust test file is committed.

### Task 2: Upgrade exactly the eight freshly proved snapshot rows

**Files:**

- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**

- Consumes: the eight exact successful test identities from Task 1.
- Produces: eight `covered` snapshot entries with executed Rust evidence.

- [ ] **Step 1: Update the exact reference-to-Rust mappings**

Set `status` to `covered`, replace the backlog reason with concise executed
evidence, and provide exactly one Rust evidence object per row:

| C++ reference test | Rust test |
| --- | --- |
| `MasterServiceSnapshotTest.BatchQueryIpTest` | `batch_query_ip_snapshot_single_and_unknown_client_parity` |
| `MasterServiceSnapshotTest.BatchQueryIpMultipleSegmentsTest` | `batch_query_ip_snapshot_deduplicates_multiple_segment_addresses_parity` |
| `MasterServiceSnapshotTest.BatchQueryIpEmptyClientIdTest` | `batch_query_ip_snapshot_empty_request_parity` |
| `MasterServiceSnapshotTest.BatchQueryIpMultipleSegmentsEmptyTeEndpointTest` | `batch_query_ip_snapshot_retains_client_with_only_empty_endpoints_parity` |
| `MasterServiceSnapshotTest.BatchQueryIpBracketedIpv6Test` | `batch_query_ip_snapshot_parses_bracketed_ipv6_parity` |
| `MasterServiceSnapshotTest.BatchQueryIpLinkLocalIpv6WithScopeTest` | `batch_query_ip_snapshot_preserves_ipv6_scope_parity` |
| `MasterServiceSnapshotTest.BatchQueryIpIpv6NoPortTest` | `batch_query_ip_snapshot_accepts_ipv6_without_port_parity` |
| `MasterServiceSnapshotTest.BatchQueryIpMixedIpv4AndIpv6Test` | `batch_query_ip_snapshot_mixed_ipv4_ipv6_parity` |

Every evidence object uses:

```json
{
  "file": "mooncake-store-master/tests/test_storage.rs",
  "test": "the exact test name from the table"
}
```

Preserve `behavior`, `boundary`, `reference`, and `review`, and do not modify
any other entry.

- [ ] **Step 2: Validate manifests and strict disposition**

From `rust-repo/tools/store-validation`, run:

```bash
/home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -v
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json --repo-root ../../..
/home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
  --manifest parity-map.json \
  --manifest transfer-engine-parity-map.json \
  --manifest tent-parity-map.json \
  --manifest wheel-store-parity-map.json \
  --repo-root ../../..
```

Expected Store counts: 1,399 total, 204 covered, 1,097 missing, and 98 N/A.
Expected combined counts: 2,451 total, 211 covered, 1,395 missing, and 845 N/A.

Capture strict Store output in memory with Python `subprocess.run`; assert
return code one and zero lines beginning `ERROR`. Do not treat remaining
missing rows as a structural failure.

- [ ] **Step 3: Source-plan all eight identities without execution**

Import `discover_rust_tests` from `inventory` and `plan_parity_runs` from
`run_parity_gate`, load only `parity-map.json`, discover under `../../crates`,
and assert the scheduled test-name set contains all eight exact names from the
mapping table. Print the eight generated Cargo commands. Do not import or call
`execute_plan`.

- [ ] **Step 4: Run manifest pre-commit and commit evidence**

Run:

```bash
cd ../../..
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
  --files rust-repo/tools/store-validation/parity-map.json
git diff --check
git diff --quiet -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
git diff --cached --quiet -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
git add rust-repo/tools/store-validation/parity-map.json
git diff --cached --check
git commit -m "[Store] record batch query ip snapshot evidence"
```

### Task 3: Run final gates for this remediation wave

**Files:**

- Verify only; no planned tracked edits.

**Interfaces:**

- Consumes: the Task 1 test commit and Task 2 manifest commit.
- Produces: fresh correctness, manifest, formatting, hook, and immutability
  evidence for the completed wave while leaving the full goal active.

- [ ] **Step 1: Run fresh Rust gates**

Run:

```bash
cd rust-repo
cargo test -p mooncake-store-master --test test_storage batch_query_ip_snapshot_
cargo test -p mooncake-store-master --test test_storage
cargo fmt --check -p mooncake-store-master
```

Expected: 8/8 focused and 36/36 complete tests pass.

- [ ] **Step 2: Run fresh manifest gates**

Repeat all 41 validation-tool tests, ordinary Store validation, four-manifest
combined validation, strict Store return-code/error-line assertions, and the
source-only eight-test planning assertion from Task 2. Confirm the exact counts
printed by the validator rather than relying only on the expected values.

- [ ] **Step 3: Run scoped pre-commit and immutable-reference guards**

Run:

```bash
cd ..
SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
  --files \
  rust-repo/crates/mooncake-store-master/tests/test_storage.rs \
  rust-repo/tools/store-validation/parity-map.json \
  docs/superpowers/specs/2026-07-31-batch-query-ip-snapshot-parity-remediation-design.md \
  docs/superpowers/plans/2026-07-31-batch-query-ip-snapshot-parity-remediation.md
git diff --quiet -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
git diff --cached --quiet -- mooncake-store mooncake-transfer-engine mooncake-wheel/tests
git diff --check
git status --short --branch
git log -6 --oneline
```

The tracked worktree must be clean. Record the results in the ignored progress
log, retain the full parity goal as active, and select the next stable missing
behavior family. Do not invoke Store LocalDisk io_uring work.

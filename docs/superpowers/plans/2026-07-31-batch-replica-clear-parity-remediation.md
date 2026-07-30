# Batch Replica Clear Parity Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Execute inline in the existing
> worktree; subagent delegation is not permitted for this run.

**Goal:** Add executable Rust evidence for the eight ordinary C++ Store
`BatchReplicaClear` tests and seven wheel client-visible tests, make only
RED-proven Rust corrections, and upgrade only manifest rows proved by fresh
passing runs.

**Architecture:** Exercise the master implementation first through its tonic
service trait so ownership, lease, filtering, ordering, and metadata state are
isolated from native data movement. Exercise byte preservation, replication,
and multi-client observations separately through the existing in-process
`MooncakeClient` TCP fixture. A new semantic test is the sole authority for a
production change; infrastructure and linker failures do not authorize one.

**Tech Stack:** Rust, Tokio, tonic service tests, Mooncake Store in-process TCP
client tests, Cargo, schema-v2 JSON parity manifests, Python validation tools,
Git, and repository pre-commit hooks.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on
  `codex/store-cpp-parity-only`.
- Treat `mooncake-store/tests`, all other C/C++ inputs, and
  `mooncake-wheel/tests` as immutable source oracles. Do not edit, format,
  build, link, load, import, or execute them.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling only.
- Use `apply_patch` for semantic edits.
- Every pre-commit invocation must set `SKIP=mooncake-code-format`.
- Use only `mooncake-store-client`, `mooncake-store-core`,
  `mooncake-store-master`, and `transfer-engine-ffi` as Rust parity evidence.
- Keep the etcd-only HA policy unchanged.
- Do not begin Store LocalDisk io_uring work in this plan.
- Do not upgrade snapshot or LocalDisk SSD rows; they require additional
  oracles outside this wave.
- If a focused test passes on its first meaningful run, classify the gap as
  evidence-only and do not edit production code.
- If a focused test has a semantic failure, preserve the RED output, invoke
  `superpowers:systematic-debugging`, identify the exact violated invariant,
  and amend this plan with the concrete minimal production edit before making
  it. A compilation, fixture, environment, or linker failure is not semantic
  RED.
- A test that was not executed successfully cannot be cited as `covered`.

---

### Task 1: Add reusable master-service test helpers

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_object.rs`

**Interfaces:**
- `mount_batch_clear_segment(service, client_id, segment_name, index)` mounts a
  unique 1 MiB in-memory segment with a nonzero base address.
- `put_complete_batch_clear_object(service, client_id, key, replica_num,
  preferred_segment)` calls `PutStart`, asserts the expected replica count,
  calls memory `PutEnd`, and returns the allocated replica descriptors.
- `batch_clear(service, client_id, keys, segment_name)` calls
  `BatchReplicaClear` in the default tenant and returns `cleared_keys`.
- `object_exists(service, key)` calls `ExistKey` in the default tenant and
  returns its boolean field.

- [ ] Add the four private async helpers directly above the existing
      `test_batch_replica_clear_respects_client_and_segment_name` test. Keep
      every request explicit: `tenant_id: String::new()`, memory replica type,
      and a `ReplicateConfig` whose unspecified fields use `Default::default()`.
- [ ] Refactor only the setup/assertion boilerplate of the existing batch-clear
      test when a helper is an exact semantic replacement; do not weaken its
      oplog sequence or remaining-replica assertions.
- [ ] Run the existing test before adding new behavior:

      ```bash
      cargo test -p mooncake-store-master --test test_master_object \
        test_batch_replica_clear_respects_client_and_segment_name -- --exact
      ```

- [ ] Run `cargo fmt --check -p mooncake-store-master` and
      `git diff --check`. Do not commit yet; the helpers are committed with the
      first tests that consume them.

### Task 2: Add master boundary and guard tests

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_object.rs`

**Interfaces:**
- Adds these exact test identities:
  - `batch_replica_clear_empty_input_parity`
  - `batch_replica_clear_missing_keys_parity`
  - `batch_replica_clear_skips_active_lease_parity`
  - `batch_replica_clear_rejects_nonowner_parity`

- [ ] Add `batch_replica_clear_empty_input_parity`. Construct a default
      service, submit `[]` as the owner, and assert the returned vector is
      empty.
- [ ] Run only that test and confirm it produces a meaningful pass or semantic
      RED:

      ```bash
      cargo test -p mooncake-store-master --test test_master_object \
        batch_replica_clear_empty_input_parity -- --exact
      ```

- [ ] Add `batch_replica_clear_missing_keys_parity`. Submit exactly
      `missing_key1` and `missing_key2`; assert successful empty output.
- [ ] Run that exact test with the same Cargo target.
- [ ] Add `batch_replica_clear_skips_active_lease_parity` with
      `lease_ttl = Duration::from_millis(2000)`. Mount one owner segment,
      complete `active-lease-key`, call `GetReplicaList` to mirror the C++ read
      boundary, immediately clear all segments, then assert an empty response
      and `object_exists == true`.
- [ ] Run that exact test.
- [ ] Add `batch_replica_clear_rejects_nonowner_parity` with a 50 ms lease.
      Complete one owner key, wait 60 ms, clear it using a distinct UUID, then
      assert empty output and continued existence.
- [ ] Run that exact test.
- [ ] Run all four exact names together using a substring filter, then the
      complete test target:

      ```bash
      cargo test -p mooncake-store-master --test test_master_object \
        batch_replica_clear_
      cargo test -p mooncake-store-master --test test_master_object
      ```

- [ ] If all tests pass without production edits, record them as
      evidence-only. If any produces semantic RED, stop this task and follow
      the global RED procedure.
- [ ] Run `cargo fmt --check -p mooncake-store-master` and
      `git diff --check`.

### Task 3: Add master multi-key and segment tests

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_object.rs`

**Interfaces:**
- Adds these exact test identities:
  - `batch_replica_clear_all_segments_five_keys_parity`
  - `batch_replica_clear_specific_segment_polling_parity`
  - `batch_replica_clear_skips_empty_and_missing_strings_parity`
  - `batch_replica_clear_mixed_owner_missing_empty_parity`

- [ ] Add `batch_replica_clear_all_segments_five_keys_parity` with a 50 ms
      lease. Mount one owner segment, complete exactly five one-replica keys,
      assert all five exist, wait 60 ms, submit all five in one all-segment
      clear request, assert the returned vector equals the input order, and
      assert all five are absent.
- [ ] Run that exact test.
- [ ] Add `batch_replica_clear_specific_segment_polling_parity` with a 50 ms
      lease and two mounted owner segments. Complete one one-replica object
      using the first segment as `preferred_segment`, wait an initial 10 ms,
      and wrap a clear/sleep polling loop in
      `tokio::time::timeout(Duration::from_secs(5), poll_until_cleared)`.
      `poll_until_cleared` is the local async block that calls named-segment
      clear, returns the first nonempty result, and otherwise sleeps 10 ms.
      Assert that result is exactly the key and the object is absent.
- [ ] Run that exact test.
- [ ] Add `batch_replica_clear_skips_empty_and_missing_strings_parity` with a
      50 ms lease. Complete `valid_key`, wait 60 ms, submit exactly
      `["", "valid_key", "", "another_empty"]`, and assert the output is
      exactly `["valid_key"]`.
- [ ] Run that exact test.
- [ ] Add `batch_replica_clear_mixed_owner_missing_empty_parity` with a 50 ms
      lease. Mount one segment per client; client 1 completes `key1` and
      `key2`, client 2 completes `key3`; wait 60 ms; submit
      `["key1", "key2", "key3", "nonexistent", ""]` as client 1. Assert
      the result is exactly `["key1", "key2"]`, key1/key2 are absent, and
      key3 remains.
- [ ] Run that exact test, all `batch_replica_clear_` tests, and the complete
      `test_master_object` target.
- [ ] If the tests are GREEN without production edits, commit the master
      evidence:

      ```bash
      git add rust-repo/crates/mooncake-store-master/tests/test_master_object.rs
      git diff --cached --check
      git commit -m "[Store] cover batch replica clear master parity"
      ```

### Task 4: Add client lease and filtering E2E tests

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs`

**Interfaces:**
- Adds these exact test identities:
  - `batch_replica_clear_preserves_active_lease_bytes`
  - `batch_replica_clear_mixes_expired_and_active_keys`
  - `batch_replica_clear_invalid_segment_preserves_bytes`

- [ ] Add `batch_replica_clear_preserves_active_lease_bytes`. Start a master
      with a 2 s lease, create one client, batch-put three distinct byte values
      as one-replica objects on its host, clear all three immediately, assert an
      empty result, then assert `batch_get` returns all three exact values.
- [ ] Run the exact native E2E test:

      ```bash
      cargo test -p mooncake-store-client --features link-native \
        --test test_client_inproc_e2e \
        batch_replica_clear_preserves_active_lease_bytes -- --exact
      ```

- [ ] Add `batch_replica_clear_mixes_expired_and_active_keys`. Use a 50 ms
      lease. Put two keys, wait 60 ms, then put two active keys; submit all four
      in one clear request. Assert the result equals the two expired keys in
      input order, their existence flags are false, and `batch_get` returns the
      two active values byte-for-byte.
- [ ] Run that exact test.
- [ ] Add `batch_replica_clear_invalid_segment_preserves_bytes`. Put one key,
      wait for its 50 ms lease to expire, clear using
      `not-a-mounted-segment:1`, assert empty output, and assert a second client
      reads the exact original bytes.
- [ ] Run that exact test.
- [ ] If the target cannot link because the configured native Transfer Engine
      library is absent, capture the complete linker diagnostic in the local
      progress record, leave all three wheel rows `missing`, and do not commit
      unexecuted tests as evidence. Otherwise run all three tests again with
      the `batch_replica_clear_` filter.

### Task 5: Add client batch, replication, and isolation E2E tests

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs`

**Interfaces:**
- Adds these exact test identities:
  - `batch_replica_clear_clears_fifty_keys_in_one_request`
  - `batch_replica_clear_all_replicas_removes_replicated_key`
  - `batch_replica_clear_named_replica_preserves_other_replica_bytes`
  - `batch_replica_clear_keeps_unrelated_key_bytes`

- [ ] Add `batch_replica_clear_clears_fifty_keys_in_one_request`. Batch-put 50
      distinct keys/values with a 50 ms lease, wait 60 ms, invoke exactly one
      all-segment clear request, assert returned keys equal the 50 input keys in
      order, and assert all 50 existence flags are false.
- [ ] Run that exact test.
- [ ] Add `batch_replica_clear_all_replicas_removes_replicated_key`. Create two
      clients, put one value with `replica_num: 2`, assert both clients read the
      exact bytes, wait for the 50 ms lease to expire, clear all segments as the
      owner, assert the exact key is returned, and assert it no longer exists.
- [ ] Run that exact test.
- [ ] Add `batch_replica_clear_named_replica_preserves_other_replica_bytes`.
      Put a replica-two value, obtain the owner's segment name from
      `get_hostname`, wait for expiry, clear that named segment, assert the
      exact key is returned, and assert the other client still reads the exact
      bytes from the remaining replica.
- [ ] Run that exact test. If placement makes the selected segment ambiguous,
      inspect the Rust client/master replica descriptors and adjust the test
      fixture to select an actually allocated segment; do not weaken the byte
      assertion or change production placement merely for the test.
- [ ] Add `batch_replica_clear_keeps_unrelated_key_bytes`. Put one target and
      one unrelated key, wait for expiry, submit only the target key, assert
      only it is returned/absent, and assert a second client reads the unrelated
      value byte-for-byte.
- [ ] Run that exact test and the complete `batch_replica_clear_` E2E subset.
- [ ] Run the full E2E target:

      ```bash
      cargo test -p mooncake-store-client --features link-native \
        --test test_client_inproc_e2e
      ```

- [ ] If every cited client test executed and passed, commit:

      ```bash
      git add rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs
      git diff --cached --check
      git commit -m "[Store] cover batch replica clear client parity"
      ```

### Task 6: Update only freshly proved manifest rows

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/wheel-store-parity-map.json`

**Interfaces:**
- Ordinary Store mappings:
  - `MasterServiceTest.BatchReplicaClearAllSegments` ->
    `batch_replica_clear_all_segments_five_keys_parity`
  - `MasterServiceTest.BatchReplicaClearSpecificSegment` ->
    `batch_replica_clear_specific_segment_polling_parity`
  - `MasterServiceTest.BatchReplicaClearWithLeaseActive` ->
    `batch_replica_clear_skips_active_lease_parity`
  - `MasterServiceTest.BatchReplicaClearWithDifferentClientId` ->
    `batch_replica_clear_rejects_nonowner_parity`
  - `MasterServiceTest.BatchReplicaClearWithNonExistentKeys` ->
    `batch_replica_clear_missing_keys_parity`
  - `MasterServiceTest.BatchReplicaClearWithEmptyKeys` ->
    `batch_replica_clear_empty_input_parity`
  - `MasterServiceTest.BatchReplicaClearWithEmptyStringKeys` ->
    `batch_replica_clear_skips_empty_and_missing_strings_parity`
  - `MasterServiceTest.BatchReplicaClearMixedScenario` ->
    `batch_replica_clear_mixed_owner_missing_empty_parity`
- Wheel mappings use the seven exact client test identities from Tasks 4-5.

- [ ] For each fresh passing test, change its reference row to `covered`, add
      the exact package/file/test Rust identity, and replace the remediation
      reason with a concise statement of the aggregate evidence. Leave
      unexecuted or partially proved rows unchanged.
- [ ] Assert all eight snapshot `BatchReplicaClear` rows and the LocalDisk SSD
      usage row remain `missing`.
- [ ] Run Store and wheel ordinary validators plus the combined validator.
      Discover the exact CLI from `--help`; do not invent flags:

      ```bash
      cd rust-repo/tools/store-validation
      /home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -v
      /home/fy2462/Mooncake/.venv/bin/python validate_parity.py --help
      ```

- [ ] Run strict validation and confirm any nonzero result is caused only by
      remaining `missing` rows, not schema, evidence, duplicate, inventory, or
      N/A-category errors.
- [ ] Run source-only parity planning and confirm the upgraded rows no longer
      appear in the missing execution plan.
- [ ] Commit only the two manifests:

      ```bash
      git add rust-repo/tools/store-validation/parity-map.json \
        rust-repo/tools/store-validation/wheel-store-parity-map.json
      git diff --cached --check
      git commit -m "[Store] record batch replica clear parity evidence"
      ```

### Task 7: Run the correctness and immutability handoff gates

**Files:**
- Verify only; no planned edits.

- [ ] Run both complete owning Rust test targets again.
- [ ] Run Store-validation unit tests, Python compilation of changed Python
      tooling if any, ordinary/strict validation, source-only planning, and the
      module-gate contract used by the current audit baseline.
- [ ] Run scoped pre-commit on every changed tracked file:

      ```bash
      SKIP=mooncake-code-format pre-commit run --files \
        rust-repo/crates/mooncake-store-master/tests/test_master_object.rs \
        rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs \
        rust-repo/tools/store-validation/parity-map.json \
        rust-repo/tools/store-validation/wheel-store-parity-map.json \
        docs/superpowers/plans/2026-07-31-batch-replica-clear-parity-remediation.md
      ```

- [ ] Verify immutable reference trees have no staged or unstaged changes:

      ```bash
      git diff --quiet -- mooncake-store mooncake-transfer-engine \
        mooncake-wheel/tests
      git diff --cached --quiet -- mooncake-store mooncake-transfer-engine \
        mooncake-wheel/tests
      ```

- [ ] Run `git status --short`, inspect every remaining diff, and report exact
      passed/failed/skipped test counts. Do not claim the overall parity goal is
      complete; continue with the next stable missing-behavior batch after this
      wave.

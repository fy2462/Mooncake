# FileStorage Empty-Work Heartbeat Eviction Parity Implementation Plan

**Goal:** Prove the real Rust storage heartbeat runs disk watermark eviction
when its offload heartbeat returns no tasks.

---

## Task 1: Add the integration witness

**File:** `rust-repo/crates/mooncake-store-client/tests/test_client_inproc_e2e.rs`

- [ ] Add
  `cpp_parity_empty_offload_heartbeat_still_runs_disk_watermark_eviction`.
- [ ] Seed three canonical persistent FilePerKey records and attach them to a
  zero-memory-segment client.
- [ ] Mount LocalDisk through the real recovery path and assert all three
  Master queries expose LocalDisk replicas.
- [ ] Assert an explicit offload heartbeat returns zero tasks.
- [ ] Start real background workers with a short storage interval and tiny
  watermarks; use a bounded poll until backend and Master state are empty.
- [ ] Run the exact test, expected to pass if the existing production sequence
  already matches C++.

## Task 2: Verify and commit the witness

- [ ] Run the complete integration binary when practical and client lib.
- [ ] Run scoped rustfmt and `git diff --check`.
- [ ] Stage only the new integration hunk and commit:

```bash
git commit -m '[Store] test empty-heartbeat disk eviction'
```

## Task 3: Record parity and run gates

**Files:**

- `rust-repo/tools/store-validation/parity-map.json`
- `rust-repo/tools/store-validation/remediation-log.json`

- [ ] Move exactly the heartbeat row to covered and reference the exact
  integration witness.
- [ ] Append one remediation record using the Task 2 SHA.
- [ ] Run JSON parsing, all four validators, validator pytest, both shell
  contracts, and `git diff --check`.
- [ ] Assert committed Store counts `754/530/115` and accumulated counts
  `1096/188/115`; wheel remains unchanged.
- [ ] Commit exact JSON hunks, request read-only review, address all
  Critical/Important findings, then continue auditing the remaining Store
  missing rows without marking the global goal complete.

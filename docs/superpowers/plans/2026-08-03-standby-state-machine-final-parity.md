# Standby State Machine Final Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the final two applicable C++ StandbyStateMachine parity rows with exact Rust tests and existing production behavior.

**Architecture:** Add one direct timing test beside `StandbyStateMachine`; reuse the pre-existing direct recovery test and production HotStandby recovery-success integration evidence from `e840795b`; and make no production or C/C++ changes. Keep manifest and remediation changes in a separate ledger commit.

**Tech Stack:** Rust 2024, `std::time`, Cargo test filters, Python parity validators.

## Global Constraints

- Work only in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- Treat all four manifests under `rust-repo/tools/store-validation/` as authoritative.
- Do not modify or format C/C++ files.
- Preserve the literal 100-through-200-millisecond C++ timing oracle.
- Cite the pre-existing direct recovery test and production HotStandby recovery-success integration test for the complete recovery row.
- Commit tests before replacing remediation `PENDING` values; commit the ledger separately.

---

### Task 1: Witness timing parity RED and add the exact timing test

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/state_machine.rs`
- Temporarily modify and restore: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: `StandbyStateMachine::{process_event,get_time_in_current_state}`.
- Produces: `cpp_parity_time_in_connecting_is_between_100_and_200_milliseconds`.

- [ ] Temporarily mark `TestTimeInState` covered with `cpp_parity_time_in_connecting_is_between_100_and_200_milliseconds`, run `validate_parity.py`, record the missing-test RED, and restore the manifest.
- [ ] Add the timing test: process `Start`, assert the exact transition to `Connecting`, sleep 100 milliseconds, and assert `elapsed.as_millis()` is in the inclusive `100..=200` range.
- [ ] Verify that commit `e840795b` introduced `test_cpp_parity_recovering_success` and `test_etcd_recovery_success_transitions_actual_state_machine` and that both remain discoverable.
- [ ] Run the timing and pre-existing direct recovery exact filters and the complete `ha::state_machine::tests` module GREEN.
- [ ] Remove the timing sleep and require the millisecond lower-bound assertion to fail, then restore it.
- [ ] Run the exact tests and the module GREEN, inspect the diff, and commit the docs/state-machine fix with a `[Store]` prefix.

### Task 2: Ledger, evidence, and gates

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`
- Update after commits: `/home/fy2462/Mooncake/.git/worktrees/ha-chaos-live/codex-parity-handoff.md`

**Interfaces:**
- Consumes: the timing test from Task 1 and the pre-existing `state_machine.rs:test_cpp_parity_recovering_success` and `hot_standby.rs:test_etcd_recovery_success_transitions_actual_state_machine` witnesses from `e840795b`.
- Produces: two covered manifest rows and two remediation records with the implementation SHA.

- [ ] Change only the two named rows from `missing` to `covered`; bind the recovery row to both pre-existing recovery witnesses.
- [ ] Append exactly two remediation records with focused commands, implementation SHA, and `/tmp/mooncake-standby-final-parity.typescript` evidence.
- [ ] Run the fully-qualified exact timing test, direct recovery test, and HotStandby production integration test for ten consecutive rounds into the evidence transcript; audit ten markers, thirty one-test passes, zero failures, and zero-test runs.
- [ ] Run the complete state-machine module, existing HotStandby recovery-success test, complete Store Master library, and all-target cargo check.
- [ ] Run all four manifest validators, 44 validator unit tests, two shell contract tests, exact-file rustfmt, touched-file pre-commit, JSON validation, `git diff --check`, and zero C/C++ diff audit.
- [ ] Request independent review and address every Critical, Important, and Minor finding.
- [ ] Commit the ledger separately, update and audit the external handoff, and report exact aggregate covered/missing totals.

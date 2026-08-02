# Standby State Machine Final Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the final two applicable C++ StandbyStateMachine parity rows with exact Rust tests and existing production behavior.

**Architecture:** Add two direct tests beside `StandbyStateMachine`, reuse the existing production HotStandby recovery-success integration evidence, and make no production or C/C++ changes. Keep manifest and remediation changes in a separate ledger commit.

**Tech Stack:** Rust 2024, `std::time`, Cargo test filters, Python parity validators.

## Global Constraints

- Work only in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on `codex/store-cpp-parity-only`.
- Treat all four manifests under `rust-repo/tools/store-validation/` as authoritative.
- Do not modify or format C/C++ files.
- Preserve the literal 100-through-200-millisecond C++ timing oracle.
- Cite the existing production HotStandby recovery-success integration test for the complete recovery row.
- Commit tests before replacing remediation `PENDING` values; commit the ledger separately.

---

### Task 1: Witness parity RED and add exact tests

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/ha/state_machine.rs`
- Temporarily modify and restore: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Consumes: `StandbyStateMachine::{process_event,get_state,get_time_in_current_state}` and `reach_watching`.
- Produces: `cpp_parity_time_in_connecting_is_between_100_and_200_milliseconds` and `cpp_parity_complete_recovery_flow_watching_recovering_watching`.

- [ ] Temporarily mark `TestTimeInState` covered with `cpp_parity_time_in_connecting_is_between_100_and_200_milliseconds`, run `validate_parity.py`, record the missing-test RED, and restore the manifest.
- [ ] Add the timing test: process `Start`, assert the exact transition to `Connecting`, sleep 100 milliseconds, and assert inclusive elapsed bounds of 100 and 200 milliseconds.
- [ ] Temporarily mark `TestCompleteRecoveryFlow` covered with `cpp_parity_complete_recovery_flow_watching_recovering_watching`, run `validate_parity.py`, record the missing-test RED, and restore the manifest.
- [ ] Add the recovery test: reach `Watching`, assert allowed `MaxErrorsReached` to `Recovering`, then allowed `RecoverySuccess` to `Watching`.
- [ ] Run both exact filters and the complete `ha::state_machine::tests` module GREEN.
- [ ] Remove the timing sleep and require the timing test to fail, then restore it.
- [ ] Temporarily map `Recovering + RecoverySuccess` to the wrong state and require the recovery test to fail, then restore it.
- [ ] Run exact tests and the module GREEN, inspect the diff, and commit with a `[Store]` prefix.

### Task 2: Ledger, evidence, and gates

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/remediation-log.json`
- Update after commits: `/home/fy2462/Mooncake/.git/worktrees/ha-chaos-live/codex-parity-handoff.md`

**Interfaces:**
- Consumes: the two stable tests from Task 1 and existing `hot_standby.rs:test_etcd_recovery_success_transitions_actual_state_machine`.
- Produces: two covered manifest rows and two remediation records with the implementation SHA.

- [ ] Change only the two named rows from `missing` to `covered`; bind the recovery row to both the new direct test and the existing HotStandby integration test.
- [ ] Append exactly two remediation records with focused commands, implementation SHA, and `/tmp/mooncake-standby-final-parity.typescript` evidence.
- [ ] Run the two exact tests for ten consecutive rounds into the evidence transcript and audit exact markers, pass counts, zero failures, and zero-test runs.
- [ ] Run the complete state-machine module, existing HotStandby recovery-success test, complete Store Master library, and all-target cargo check.
- [ ] Run all four manifest validators, 44 validator unit tests, two shell contract tests, exact-file rustfmt, touched-file pre-commit, JSON validation, `git diff --check`, and zero C/C++ diff audit.
- [ ] Request independent review and address every Critical, Important, and Minor finding.
- [ ] Commit the ledger separately, update and audit the external handoff, and report exact aggregate covered/missing totals.


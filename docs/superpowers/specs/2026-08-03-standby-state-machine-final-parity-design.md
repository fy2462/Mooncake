# Standby State Machine Final Parity Design

## Scope

Close the two remaining applicable rows in
`ha/standby/standby_state_machine_test.cpp` without changing C/C++ or widening
the Rust production API:

- `StandbyStateMachineTest.TestTimeInState`
- `StandbyStateMachineTest.TestCompleteRecoveryFlow`

The existing Rust `StandbyStateMachine` already exposes the required elapsed
time and transition behavior. The existing
`test_etcd_recovery_success_transitions_actual_state_machine` also proves the
recovery-success chain through the production `HotStandbyService` path.

## Design

Add two stable, discoverable tests to `ha/state_machine.rs`.

The timing test processes `Start`, sleeps for the literal C++ interval of 100
milliseconds, reads `get_time_in_current_state`, and checks the same inclusive
100-through-200-millisecond window. It also checks that `Start` committed
`Connecting`, so the elapsed duration is tied to the intended state entry.

The recovery-flow test reaches `Watching` through `Start`, `Connected`, and
`SyncComplete`; then it checks the complete allowed transition results and
committed states for `MaxErrorsReached` to `Recovering` and `RecoverySuccess`
back to `Watching`. The manifest row will cite both this direct deterministic
test and the existing production HotStandby recovery-success integration test.

No clock abstraction, production hook, or state-machine behavior changes are
needed. A clock abstraction would add an API solely for testing and would no
longer execute the literal real-time C++ oracle.

## Verification

Before adding each test, temporarily mark its manifest row covered with the
planned stable test reference and run the validator to witness a missing-test
RED, then restore the manifest until the implementation commit exists.

After the tests are GREEN, use mutation probes to prove sensitivity:

- remove the 100-millisecond sleep and require the timing lower-bound assertion
  to fail;
- temporarily map `RecoverySuccess` from `Recovering` to the wrong state and
  require the recovery-flow test to fail.

Run both exact tests for ten consecutive rounds, the complete state-machine
module, the existing HotStandby production integration test, the complete
Store Master library, all four manifest validators, validator self-tests,
format/pre-commit checks, and a zero C/C++ diff audit.


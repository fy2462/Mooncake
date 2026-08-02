# Standby State Machine Final Parity Design

## Scope

Close the two remaining applicable rows in
`ha/standby/standby_state_machine_test.cpp` without changing C/C++ or widening
the Rust production API:

- `StandbyStateMachineTest.TestTimeInState`
- `StandbyStateMachineTest.TestCompleteRecoveryFlow`

The Rust `StandbyStateMachine` already exposes the required elapsed-time
behavior. Commit `e840795b` added the pre-existing direct
`test_cpp_parity_recovering_success` witness and the
`test_etcd_recovery_success_transitions_actual_state_machine` production
`HotStandbyService` integration witness for the complete recovery chain.

## Design

Add one stable, discoverable timing test to `ha/state_machine.rs`. Reuse the
pre-existing direct state-machine and production HotStandby recovery witnesses
instead of adding an alias for behavior they already cover.

The timing test processes `Start`, sleeps for the literal C++ interval of 100
milliseconds, reads `get_time_in_current_state`, truncates it to milliseconds
with `Duration::as_millis`, and checks the same inclusive
100-through-200-millisecond window as C++. It also checks that `Start` committed
`Connecting`, so the elapsed duration is tied to the intended state entry.

The recovery-flow row cites `test_cpp_parity_recovering_success`, which directly
checks `Watching` to `Recovering` on `MaxErrorsReached` and back to `Watching`
on `RecoverySuccess`, plus the production HotStandby recovery-success
integration test. Both witnesses originate in `e840795b`.

No clock abstraction, production hook, or state-machine behavior changes are
needed. A clock abstraction would add an API solely for testing and would no
longer execute the literal real-time C++ oracle.

## Verification

Before adding the timing test, temporarily mark its manifest row covered with
the planned stable test reference and run the validator to witness a
missing-test RED, then restore the manifest until the implementation commit
exists. Verify from Git history that the recovery witnesses predate this work.

After the timing test is GREEN, remove the 100-millisecond sleep and require its
millisecond lower-bound assertion to fail, then restore the sleep.

For ten consecutive rounds, run the exact timing test, the pre-existing direct
recovery test, and the production HotStandby integration test. Also run the
complete state-machine module, complete Store Master library, all four manifest
validators, validator self-tests, format/pre-commit checks, and a zero C/C++
diff audit.

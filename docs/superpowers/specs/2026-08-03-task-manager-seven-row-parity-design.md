# Task Manager Seven-Row Parity Design

## Context

The Store parity manifest has ten applicable missing rows from
`task_manager_test.cpp`. Seven are portable on the current host and can be
proved through existing production Rust boundaries. The other three
(`PruningLogic`, `PruneExpiredTasksPendingTimeout`, and
`PruneExpiredTasksProcessingTimeoutFreesSlot`) require a deterministic
production clock to preserve strict ordering and timeout semantics without
sleep-based evidence; they remain missing in this batch.

The seven selected rows cover four public task RPC lifecycles and three
production snapshot behaviors. Existing Rust tests exercise adjacent pieces,
but do not directly observe each complete C++ contract. This batch adds exact,
stable-named witnesses rather than treating source code or partial tests as
coverage.

## Design

Add four tests to
`rust-repo/crates/mooncake-store-master/tests/test_master_tasks.rs`:

1. `cpp_parity_submit_and_pop_task` creates exactly one real ReplicaCopy task,
   fetches it for its assigned source client with batch size ten, proves the
   returned ID is exact, and queries the production task state as PROCESSING.
2. `cpp_parity_mark_task_complete_lifecycle` observes the same task as PENDING,
   PROCESSING after fetch, and SUCCESS after the assigned client completes it
   with the literal message `Completed successfully`. The message is input,
   not an asserted persistence requirement.
3. `cpp_parity_multiple_clients_fetch_only_own_tasks` creates one task for each
   of two source clients and proves each fetch returns only its own ID, followed
   by an empty second fetch for the first client.
4. `cpp_parity_pending_limit_exceeded` configures the production pending limit
   to one, submits one task, leaves it pending, and proves the second submission
   fails with tonic `ResourceExhausted`.

All four tests mount real named source and target segments, establish source
objects through `PutStart`/`PutEnd`, and create tasks through public Master
RPCs. Shared test-only helpers may remove fixture duplication, but assertions
remain explicit per C++ row. No test inspects private task maps or performs
transfer I/O.

Add two tests to
`rust-repo/crates/mooncake-store-master/tests/test_catalog_snapshot.rs`:

1. `cpp_parity_task_snapshot_round_trip_preserves_four_states` publishes and
   loads exactly four production `TaskEntry` values in Copy/Copy/Move/Move
   order with SUCCESS/FAILED/PENDING/PROCESSING states. It proves every ID,
   type, status, assigned client, nonempty payload, and created/update timestamp
   within one second of its original.
2. `cpp_parity_empty_task_catalog_round_trip` publishes and loads a production
   snapshot with an explicitly empty task vector and proves it remains empty.

Add `cpp_parity_empty_snapshot_replaces_live_task_state` to the existing
`snapshot_restore_tests` unit module in
`rust-repo/crates/mooncake-store-master/src/service/mod.rs`. It seeds a valid
live task in a real `MasterServiceImpl`, invokes the same atomic
`restore_loaded_snapshot_state` production boundary used by snapshot startup
with an empty task vector, then uses public `QueryTask` and `FetchTasks` RPCs to
prove the old ID is NotFound and no task remains fetchable by its assigned
client. The unit location is necessary only to invoke the crate-private atomic
restore entry point; externally observable assertions remain at public RPC
boundaries.

No production behavior change is planned. If a witness fails because the
production boundary lacks the C++ behavior, retain the failing test and make
only the smallest production correction through a separate red-green cycle.

## TDD and Evidence

Each new witness receives an explicit mutation RED before being accepted:
temporarily invert or alter one primary expected ID/status/limit/cardinality,
run that exact test, and retain the expected assertion failure. Restore the
oracle and prove the exact test GREEN. This establishes that the newly added
test can detect the behavior it claims.

After all seven pass, run each exact stable name for ten rounds and retain one
transcript with 70 one-test passes, no failures, no zero-test selections, and a
zero command exit. Then run the complete `test_master_tasks` integration
target, the complete `test_catalog_snapshot` target, the Store Master library
suite, and `cargo check -p mooncake-store-master --all-targets`.

## Manifest and Ledger

Change exactly these seven manifest rows from `missing` to `covered`:

- `ClientTaskManagerTest.SubmitAndPopTask`
- `ClientTaskManagerTest.MarkTaskComplete`
- `ClientTaskManagerTest.MultipleClients`
- `ClientTaskManagerTest.PendingLimitExceeded`
- `ClientTaskManagerTest.SerializerRoundTrip`
- `ClientTaskManagerTest.SerializerEmptyManager`
- `ClientTaskManagerTest.SerializerReset`

Append exactly seven remediation records using the implementation commit SHA,
the exact Rust test filter, the relevant full target, and the retained evidence
transcript. Run all four validators and their contract/self-test suites. The
authoritative aggregate missing count must move from 869 to 862, with no other
row status changed.

## Non-Goals

- No sleep-based timeout or pruning evidence.
- No deterministic clock injection in this batch.
- No task payload byte-format or C++ serializer-layout claim.
- No transfer execution, retry-count, cleanup, or persisted-message claim.
- No change to unrelated missing, N/A, CXL, NUMA, Kubernetes, Sunrise, or SHM
  rows.
- No C/C++ source or formatting changes.

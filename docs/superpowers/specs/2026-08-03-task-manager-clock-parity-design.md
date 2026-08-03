# Task Manager Clock Parity Design

## Context

After the seven-row task-manager batch, the Store parity manifest has three
remaining applicable rows from `task_manager_test.cpp`:

- `ClientTaskManagerTest.PruningLogic`
- `ClientTaskManagerTest.PruneExpiredTasksPendingTimeout`
- `ClientTaskManagerTest.PruneExpiredTasksProcessingTimeoutFreesSlot`

The Rust service already implements finished-task retention and pending and
processing timeouts in its production background reaper. Exact, non-flaky
witnesses are blocked because task creation, fetch, completion, and reaping
each read the wall clock independently. Sleeping would reproduce the C++ test
mechanism but would be slow and timing-dependent evidence.

## Chosen Design

Add one crate-private `TaskLifecycleClock` owned by `MasterState`. Its normal
`now()` result is `chrono::Utc::now()`, so production behavior is unchanged.
Under `cfg(test)` only, the clock supports setting and advancing a fixed UTC
instant. The test controls stay crate-private and do not enter the public API.

Route every client-task lifecycle timestamp through this clock:

- Copy task creation
- Move task creation
- task fetch (`PENDING` to `PROCESSING`)
- task completion
- client-task timeout and finished-task pruning
- synchronous and background drain task creation

Using one source is essential: the reaper must compare against the same time
domain that stamped each state transition, and completed-task ordering must be
deterministic. A reaper-only time parameter would leave creation, fetch, and
completion ordering uncontrolled. A public clock trait and constructor would
add API and plumbing that production callers do not need.

The clock is a small value type in `service/state.rs`. Production builds store
no manual override. Test builds store an `RwLock<Option<DateTime<Utc>>>`; clone
or sharing semantics are unnecessary because all users reach the same clock
through the shared `Arc<MasterState>`.

## Exact Parity Witnesses

Place three crate unit tests beside the task RPC implementation so they can
control the crate-private clock while exercising public RPCs and the existing
production reaper test seam. Tests must use a real `MasterServiceImpl`, mount
real source and target segments, publish source objects through
`PutStart`/`PutEnd`, and create Copy tasks through the public RPC boundary.
They must not inspect the private task map.

### Finished-task pruning

Configure `max_total_finished_tasks = 5`. Create, fetch, and successfully
complete seven tasks, advancing the manual clock between completions to give
each terminal transition a strict order. Invoke the production reaper. Public
`QueryTask` calls must return `NotFound` for the first two IDs and `SUCCESS`
for the last five IDs.

### Pending timeout

Configure the pending limit to one and pending timeout to one second. Create
the first task and advance the clock by two seconds so the production strict
`elapsed > timeout` rule is satisfied. Invoke the production reaper. Public
`QueryTask` must show `FAILED` with the exact message `pending timeout`, and
`FetchTasks` must return empty. Creating a second task must then succeed,
proving that the failed task no longer consumes the pending slot.

### Processing timeout

Configure the processing limit to one and processing timeout to one second.
Create two tasks at distinct manual instants, then fetch and prove only the
first ID enters `PROCESSING`. Advance the clock by two seconds and invoke the
production reaper. Public `QueryTask` must show the first task as `FAILED`
with the exact message `processing timeout`. A second fetch must return only
the second ID in `PROCESSING`, proving that the expired task released the sole
processing slot.

## TDD and Evidence

First add the three exact witnesses against the existing code and retain their
RED result: they cannot compile or deterministically drive the lifecycle clock.
Then add the minimum clock boundary and timestamp routing needed to make them
GREEN. For each final test, retain an assertion mutation RED that changes a
primary expected status, message, retained ID, or cardinality and proves the
witness detects the wrong behavior.

Run every exact stable test name ten times and retain one transcript containing
30 one-test passes, no failures, no zero-test selections, and a zero command
exit. Then run the complete Store Master package, all-targets check, the four
parity validators and validator contracts/self-tests, Rust 2024 rustfmt checks,
pre-commit on touched files, and a C/C++ diff guard.

## Manifest and Ledger

Change exactly the three named manifest rows from `missing` to `covered` and
append exactly three remediation records. Each record cites the implementation
commit SHA, exact Rust test filter, complete package gate, and retained evidence
transcript. The authoritative aggregate missing count moves from 862 to 859;
no other row changes status.

## Non-Goals

- No sleep-based test evidence.
- No public test clock or configurable production clock.
- No change to timeout comparison (`>` remains exact).
- No change to retention semantics or task scheduling policy.
- No private-map assertions or source-only evidence.
- No changes or formatting to C/C++ files.
- No changes to unrelated missing or N/A rows.

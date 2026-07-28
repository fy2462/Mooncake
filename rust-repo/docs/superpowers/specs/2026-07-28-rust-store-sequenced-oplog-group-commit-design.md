# Rust Store Sequenced OpLog Group Commit Design

## Context

The Rust Store Master currently protects `OpLogManager` with one
`parking_lot::Mutex` and performs synchronous etcd persistence while holding
that mutex. Under eviction pressure, one slow transaction keeps the mutex and
other Tokio request handlers block on it. Because those waits are synchronous,
they can exhaust Tokio workers; the etcd request timer and client RPC deadline
then fail to provide a reliable bound.

The three-Master live gate reproduced this failure after removing an unrelated
global eviction snapshot barrier. A `PutStart` for an unrelated key waited
behind the global oplog mutex until the harness shut etcd down. Adding a tonic
request deadline alone passed a raw-server regression but did not fix the live
runtime-starvation path.

The classic C++ Store avoids this topology. It assigns sequence numbers under
a short manager lock, queues records to a dedicated writer thread, drains a
group-commit batch, performs etcd I/O without the queue lock, and wakes durable
waiters after the batch commits.

## Goals

- Remove synchronous etcd I/O and global oplog-manager mutex waits from Tokio
  request workers.
- Preserve the current Rust correctness contract: every mutation that is
  durable today still reports success only after its record is committed.
- Preserve a single total oplog order, contiguous sequence IDs, writer fencing,
  atomic `/latest` advancement, poisoning, and standby replay behavior.
- Coalesce concurrent durable mutations into one etcd transaction.
- Bound queueing, commit, shutdown, and failure propagation.
- Make batch size, latency, queue depth, and failure state observable.

## Non-goals

- Do not weaken `PutStart`, `PutEnd`, remove, revoke, task, segment, or lease
  durability in this change.
- Do not copy the C++ rule that treats `PUT_END` as asynchronous. That policy
  can be evaluated separately after the HA correctness gate passes.
- Do not change Transfer Engine semantics, object mutation ordering, snapshot
  formats, or standby replay schemas.
- Do not add retries after an ambiguous etcd transaction failure. The Rust
  writer remains fail-closed and fences the current service.

## Architecture

### Sequenced worker

Introduce a `SequencedOpLogWorker` owned by `MasterState`. The worker owns the
only mutable `OpLogManager` and `EtcdOpLogStore`; request paths no longer lock
or call either object directly.

The worker runs on a dedicated OS thread with its own Tokio current-thread
runtime for async etcd calls. Therefore etcd progress and deadline timers do
not depend on the Master's request-worker pool.

Request paths submit an `OpLogCommand` through a bounded channel. Each command
contains the encoded payload, producer view, operation label, and a completion
sender. The worker assigns sequence IDs in dequeue order. A queue-full or
closed error is a durability failure and follows the existing service-fencing
path; a mutation that changed in-memory state must never continue serving after
its oplog request is rejected.

### Group commit

After receiving the first command, the worker drains commands until the first
of these limits is reached:

- 10 milliseconds from the first command;
- 100 records;
- the existing etcd transaction payload/operation limit;
- shutdown or a command explicitly requiring an immediate flush.

All records receive consecutive sequence IDs. The worker sends one atomic etcd
transaction containing every oplog entry, the `/latest` pointer, and the
existing leadership-fence comparison. The batch is successful only when the
fence comparison succeeds and etcd confirms the transaction.

On success, the worker publishes the committed sequence atomically and
completes every waiter in the batch. On any timeout, transport error, failed
fence comparison, malformed response, or sequence inconsistency, the worker
poisons itself, completes every current waiter with the same terminal error,
drains queued commands as immediate failures without another backend call, and
causes the existing wrapper to fence the service.

### Waiting without starving Tokio

The first implementation keeps existing synchronous persistence helper
signatures to avoid a repository-wide async API migration. Waiting for the
worker completion uses a blocking-aware adapter:

- on a Tokio multi-thread runtime, wait inside `tokio::task::block_in_place`;
- outside Tokio, wait directly;
- on a current-thread Tokio runtime, use a direct bounded wait because the
  oplog worker and etcd runtime are independent OS threads.

No request worker holds an oplog-global mutex while waiting. Existing per-key
mutation guards may remain held across durable acknowledgement because they
protect same-key ordering; unrelated keys can submit and join the same batch.

The completion wait is bounded below the external Master RPC deadline. A wait
timeout fences the service. The worker may later learn that the transaction
committed, but no further mutations may be served by that process after the
ambiguous result.

### Read-side state and snapshots

The latest assigned and latest committed sequence IDs are exposed as atomics.
Snapshot capture reads the committed sequence boundary, not a mutable manager
behind a mutex. Operations that require a store query send an explicit worker
command rather than taking ownership of the store.

Standby readers and change notifiers remain separate read-only etcd clients;
they do not share the writer queue.

### Shutdown

Shutdown closes submissions, flushes an already formed batch within the normal
deadline, fails remaining queued commands if durability cannot be established,
and joins the worker. It never starts an unbounded final flush. Destroying an
already poisoned worker performs no additional mutation.

## Correctness invariants

1. Sequence IDs are assigned by one worker and are strictly increasing.
2. A committed `/latest = N` is in the same fenced transaction as every new
   entry through `N`; standby never observes a committed latest pointer with a
   missing prefix.
3. A request succeeds only after the batch containing its record commits.
4. Same-key guards remain held until the durable result, preventing an older
   remove from committing after a replacement put.
5. Any ambiguous persistence result irreversibly poisons the writer and fences
   the service.
6. Once poisoned, the writer performs no additional backend mutations.
7. Queue overload is fail-closed, not best-effort.
8. Writer transactions retain the existing election-view comparison.

## Configuration and observability

Initial defaults mirror the C++ batching envelope:

- batch window: 10 ms;
- maximum records: 100;
- bounded queue: 1,024 commands;
- etcd request deadline: 10 seconds;
- caller completion deadline: 20 seconds.

The initial change keeps these internal constants unless a test requires
runtime injection. Test constructors may inject shorter values. Production CLI
surface is deferred until benchmarks show a supported tuning need.

Add counters/histograms for submitted commands, queue rejection, batch count,
records per batch, queue wait, commit latency, durable waiter latency, poison
events, and immediate failures after poison.

## Testing

### Unit and concurrency tests

- A stalled backend does not prevent an unrelated request from submitting, and
  no Tokio worker starvation is required for the backend deadline to fire.
- Concurrent commands receive consecutive sequence IDs and share one flush.
- Every waiter completes only after its batch commit.
- Batch transaction failure fans the same terminal error to all members,
  poisons the writer, and later submissions make no backend call.
- Queue-full and shutdown paths are bounded and fail-closed.
- Same-key remove/replacement ordering remains durable.
- `/latest`, fence comparison, replay, snapshot boundary, and promotion tests
  remain green.

### Live correctness gate

Run the canonical three-Master large-object scenario once after focused tests.
It must complete four crash/restart rounds, record pressure evidence, perform at
least 168 exact stable reads, cover every victim, and leave no stale Master or
etcd process. Small-object evidence must remain PASS.

### Performance stage

After correctness is GREEN, benchmark batch size, queue wait, commit latency,
and request throughput against the current synchronous Rust path and the C++
configuration. Only then consider selective asynchronous `PutEnd` semantics or
additional batching tunables.

## Rollout

Implement behind the existing Rust HA oplog construction path; non-HA and
reader-only paths remain unchanged. Land the worker and its tests separately
from the large-object chaos scenario. Retain the defensive etcd request
deadline, but correctness must not rely on a Tokio request worker remaining
available to drive it.

# Storage Backend Writer-Death Parity Design

## Goal

Close `StorageBackendE2ETest.DiskReplicaSurvivesWriterDeath` with one real
Rust shared-Disk and client-liveness cleanup witness.

## C++ Oracle

The C++ fixture writes four distinct 2-KiB objects, waits until every object
has a shared Disk replica, destroys the writer, and waits past the client live
TTL. A newly created reader must observe Disk and no Memory replica for every
object. Reads are attempted for all four objects; successful reads must return
the exact written bytes, while a read error is logged but does not fail the
oracle.

## Rust Boundary

Use an isolated global FilePerKey backend and a short production client-live
TTL/monitor interval. A writer with an owned 16-MiB segment writes four indexed
2-KiB values and proves Disk metadata before it is dropped without explicit
Master teardown. After an observation-free wait beyond the live TTL, create a
fresh reader and query each key exactly once. Every query must contain Disk and
must not contain Memory. Attempt public `get_buffer` for every key and require
exact size and bytes whenever it succeeds, matching the C++ conditional read
assertion.

The witness does not use `tear_down_all`: that is graceful unmount, whereas
this oracle specifically exercises stale-client liveness cleanup after the
writer disappears.

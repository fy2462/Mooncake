# LocalDisk snapshot round-trip parity design

## Scope

Cover four C++ promotion snapshot rows through a live `MasterServiceImpl`, a
published native snapshot, a fresh service restore, and public query evidence:

- `LocalDiskReplicaRoundTrip`
- `MixedMemoryAndLocalDiskRoundTrip`
- `LocalDiskSegmentEnableOffloadingPreserved`
- `MultipleLocalDiskHoldersRoundTrip`

The following `InFlightPromotionTaskSnapshotSafe` row will reuse this fixture
in a subsequent wave.

## Required semantic bridge

C++ restores process-bound Memory and LocalDisk coordinates as immediately
live. Rust deliberately fences all snapshot process sessions until their owners
remount and, for LocalDisk, complete an inventory transaction. Removing that
fence would make stale endpoints routable and violate the Rust recovery model.

Rust will therefore preserve the same durable metadata while retaining a
two-phase activation boundary:

1. A fresh restore retains each LocalDisk descriptor's holder identity,
   transport endpoint, size, storage identity, and byte generation, but sets
   `handle_valid=false`.
2. A restored LocalDisk entry retains its last client identity,
   `enable_offloading`, and pending offloading map as dormant snapshot state,
   while `active_client_id=None` and `recovery_complete=false` prevent work or
   routing.
3. Snapshot capture uses the active client when present and otherwise the last
   persisted client. A second save therefore does not erase client association
   merely because recovery has not completed yet.
4. The same clients then use production Memory remount and LocalDisk inventory
   recovery. Only after successful recovery do public replica queries expose
   the preserved metadata, matching the C++ observable descriptor end state.

`active_client_id`, recovery tokens, recovered inventory sets, capacity, and
promotion task maps remain runtime-only. No dormant entry may schedule offload,
promotion, metrics, or reads before a current session is Ready.

## Tests

Four uniquely named async tests create source services through public RPCs,
publish snapshots through the real snapshot provider, construct fresh services,
and compare first/second durable snapshots. They then reactivate the exact
clients through production APIs and assert:

- one LocalDisk descriptor preserves client, size, endpoint, and generation;
- a mixed object returns Memory then LocalDisk in original order;
- the dormant and second-snapshot LocalDisk entry preserves
  `enable_offloading=true`;
- two storage holders remain distinct and their keys retain the correct holder,
  size, and endpoint.

Before recovery, public query must remain `FailedPrecondition`, proving the new
metadata preservation does not bypass routing fences.

## Verification

Capture compile-red evidence before adding the dormant-client field and restore
logic. Run the four exact tests, existing LocalDisk recovery/fencing tests, the
snapshot restore unit group, and the full master library suite. Update only the
four exact manifest rows after all public-query and second-save assertions pass.

# FileStorage Tenant-Scoped Eviction Parity Design

## Goal

Close `FileStorageTest.NotifyEvictedDiskReplicasUsesTenantScopedKeys` with
direct Rust evidence for both scoped-key routing and real Master metadata
removal.

## C++ Oracle

The C++ test publishes two LocalDisk replicas with the same user key under
`tenant_a` and `tenant_b`. Before eviction, each tenant query returns a
LocalDisk replica. `FileStorage::NotifyEvictedDiskReplicas` receives the two
tenant-scoped storage keys, routes the unscoped user key under the correct
tenant for each notification, and both tenant queries then return
`OBJECT_NOT_FOUND`.

## Rust Boundaries

Rust already separates the two responsibilities:

1. `notify_evicted_disk_replicas_with` parses each canonical local-storage key,
   groups user keys by tenant, calls the notifier once per tenant, and tracks
   accepted scoped storage keys.
2. Master `batch_evict_disk_replica` removes replicas under the request tenant
   and deletes the object entry when no replica remains.

Use two complementary witnesses rather than duplicating production code:

- A client unit witness passes `local_storage_key("tenant-a", "shared-key")`
  and `local_storage_key("tenant-b", "shared-key")` through the real helper and
  asserts two tenant-specific notifier calls both carry only `shared-key`.
- A Master integration witness creates two tenant objects with that same user
  key, obtains generation-bearing offload tasks, publishes real LocalDisk
  replicas, proves both tenant queries see them, evicts each through the real
  RPC, and proves both tenant queries become not found.

Together these witnesses cover the exact C++ chain: scoped storage-key parsing,
tenant RPC routing, and tenant-isolated metadata deletion.

## Scope

No production change is expected because both boundaries already implement the
required behavior. If either new witness fails, repair only the first observed
production divergence. Do not expose private helper state or weaken
generation-bearing offload validation.

## Manifest and Counts

Reference both exact Rust witnesses from the one Store row and append one
remediation record. Against the committed parent, Store moves from
`covered=752, missing=532, not-applicable=115` to
`covered=753, missing=531, not-applicable=115`. In the accumulated checkout,
Store moves from `covered=1094, missing=190, not-applicable=115` to
`covered=1095, missing=189, not-applicable=115`. Wheel is unchanged.

## Verification

Run each exact witness, the complete client library suite, the exact Master
integration target and its complete test binary, all parity validators,
validator contracts, scoped formatting/pre-commit, JSON parsing, and
`git diff --check`. Preserve all accumulated worktree changes via exact hunk
staging.

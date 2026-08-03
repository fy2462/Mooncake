# Snapshot child catalog descriptor parity design

## Scope

Close three portable `SnapshotChildProcessTest` rows through Rust's production
catalog boundary:

- `FormatTimestamp_MatchesExpectedFormat`
- `PersistState_PublishesSnapshotDescriptor`
- `PersistState_UsesFrozenSnapshotDescriptor`

The remaining child-exit, signal, timeout, destructor, and automatic-thread
rows are outside this batch. They must not be claimed from ordinary Rust task
behavior.

## Replacement boundary

Rust does not expose the C++ private `MasterSnapshotManager::PersistState`
method. Its production replacement is split between
`CatalogBackedSnapshotProvider::publish_loaded_snapshot`, which publishes the
payload set and descriptor, and `SnapshotCatalogStore::publish/get_latest`,
which durably publishes and reloads an already-frozen descriptor.

Three independent integration tests in `test_catalog_snapshot.rs` will use a
real `LocalFileSnapshotObjectStore` and `EmbeddedSnapshotCatalogStore`:

1. Publish an empty-ID snapshot and assert the returned production-generated ID
   has exactly 19 ASCII bytes in `YYYYMMDD_HHMMSS_mmm` digit/underscore shape.
2. Publish a requested fixed ID at sequence 0 and producer view 37, bound the
   descriptor creation timestamp by wall-clock reads immediately around the
   call, and assert the exact derived manifest key, object prefix, and catalog
   latest descriptor.
3. Construct a descriptor with fixed sequence 123, view 41, and creation time
   1717243200123, publish it directly through the catalog trait, and assert the
   complete reloaded descriptor equals the frozen input.

No production change is expected.

## Evidence and gates

Each test must have an independent primary-assertion mutation RED and pass ten
exact rounds. Then run `test_catalog_snapshot`, the Master library and complete
package, all-target check, all four manifest validators, validator self-tests,
both shell contracts, touched-file pre-commit, Rust 2024 formatting, JSON and
diff checks, and a zero C/C++ change audit. Upgrade exactly three manifest rows
and append exactly three remediation records only after GREEN evidence exists.

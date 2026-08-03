# Local File Snapshot Object Store Parity Design

## Context

The Store parity manifest has ten applicable missing rows from
`ha/snapshot/object/backends/local/local_file_snapshot_object_store_test.cpp`.
They cover direct binary and string round trips, recursive prefix listing,
prefix deletion, connection information, deep-directory creation, empty-root
construction, missing downloads, and empty-buffer rejection.

Rust already implements the production local object-store trait boundary with
atomic temporary-file publication and directory fsync. Eight rows need direct,
discoverable tests. Two rows expose real behavior gaps: the constructor accepts
an empty `PathBuf`, and `upload_buffer` accepts an empty byte slice.

## Chosen Design

Add ten stable unit tests to `ha::snapshot::tests`, beside the production
implementation. Each test directly calls `LocalFileSnapshotObjectStore`
through `SnapshotObjectStore`; catalog publication is not evidence for these
direct API contracts.

Add the minimum production validation:

- `LocalFileSnapshotObjectStore::new` rejects an empty OS path immediately.
  Because the existing public constructor returns `Self`, rejection is a panic,
  the Rust constructor analogue of the C++ `runtime_error`. Keep all existing
  call sites and API shape unchanged.
- `upload_buffer` returns `HaError::InvalidParams` for an empty slice before
  creating directories or temporary files.
- Override `upload_string` for this implementation and route both writes
  through one private atomic byte writer. String upload remains allowed to
  write an empty string, matching the distinct C++ string API; the new buffer
  guard must not accidentally change it.

Alternatives rejected: integration tests in `tests/test_ha.rs` would cover the
same public type but separate the witnesses from the implementation without
adding a stronger boundary, and one consolidated ten-behavior test would make
manifest discovery and mutation evidence less precise.

## Exact Witnesses

- Upload/download the exact bytes `{0,1,2,128,254,255}` at `test/buf`.
- Upload/download exact string `hello mooncake snapshot` at `test/str`.
- Upload the exact three `snap/...` keys and assert narrow listing is exactly
  the two expected keys while broad listing is exactly all three.
- Upload two strings, delete `snap/20240101/`, and prove direct download of the
  exact metadata key returns `HaError::Snapshot` and classifies as not found.
- Assert `connection_info()` contains the configured temporary root path.
- Upload/download byte `{42}` at `a/b/c/deep_file` and prove its parent
  directories exist.
- Construct with `PathBuf::new()` under `catch_unwind` and assert rejection.
- Directly download buffer and string at `no/such/key`; both must return
  `HaError::Snapshot` and classify as not found.
- Upload an empty buffer at `test/empty`; assert `HaError::InvalidParams` and
  prove the key was not created.

Listing assertions compare sorted exact key vectors, strengthening the C++
cardinality checks without adding unrelated semantics.

## TDD and Evidence

Add the ten exact tests first. Retain the initial REDs for empty-root and
empty-buffer behavior. For the eight already implemented behaviors, retain an
independent primary-assertion mutation RED. Restore the exact assertions and
run every stable test ten times, requiring 100 one-test passes, no failures,
and no zero-test selections.

Run the complete `ha::snapshot::tests` module, catalog and HA integration
targets, Master library and package, all-targets check, four manifest
validators, validator self-tests and shell contracts, Rust 2024 formatting,
pre-commit on touched files, JSON/diff checks, and the C/C++ zero-change guard.

## Manifest and Ledger

Change exactly the ten selected rows from `missing` to `covered` and append ten
remediation records citing the implementation commit and retained evidence.
The authoritative aggregate missing count moves from 855 to 845.

## Non-Goals

- No change to S3 or catalog-store behavior.
- No weakening of atomic temp-file publication or directory fsync.
- No broad constructor canonicalization or eager directory creation.
- No claim about unselected path-traversal or file-prefix deletion rows.
- No changes or formatting to C/C++ files.

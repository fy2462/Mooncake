# Task 2 local-file snapshot object-store parity audit

## Scope and result

Reviewed all 10 GoogleTest references in
`mooncake-store/tests/ha/snapshot/object/backends/local/local_file_snapshot_object_store_test.cpp`
against the directly exercised C++ local filesystem implementation and every
discoverable Rust test using `LocalFileSnapshotObjectStore`.

The manifest records 2 exact covered rows and 8 missing rows. There are no
not-applicable or blocked rows. All ten observations are local object-store API
behavior, so an environmental or implementation-only disposition would be
incorrect.

## Exact covered mappings

| C++ reference | Rust evidence | Exact observable oracle |
| --- | --- | --- |
| `UploadDownloadString_Roundtrip` | `test_snapshot_catalog_uses_object_store_abstraction` | A nested string is successfully published through the local store and a direct `download_string` returns exactly the serialized descriptor. |
| `UploadBuffer_CreatesSubdirectories` | `test_snapshot_catalog_uses_object_store_abstraction` | Catalog publication reaches nested `mooncake_master_snapshot/<id>/descriptor.txt` through the trait default `upload_string` → local `upload_buffer`; direct readback proves successful directory creation and byte retention. |

## Missing rows

- `UploadDownloadBuffer_Roundtrip`: no direct non-text byte-vector upload and
  download equality assertion.
- `ListObjectsWithPrefix`: no complete narrow-plus-broad recursive prefix
  cardinality assertion; source sorting does not constitute coverage.
- `DeleteObjectsWithPrefix`: no direct prefix delete followed by direct missing
  object download error.
- `GetConnectionInfo`: source formats the base path, but no test asserts it.
- `Constructor_EmptyPath_Throws`: Rust accepts an empty `PathBuf`; this is a
  behavior difference, not a C++-only branch.
- `DownloadBuffer_NonExistentKey` and `DownloadString_NonExistentKey`: source
  error paths have no direct local-store error tests; catalog `None` mapping is
  a different API observation.
- `UploadBuffer_EmptyBuffer`: Rust writes an empty slice; C++ returns an
  error. This is a behavior difference, not a C++-only branch.

## Source observations outside the tested inventory

The C++ implementation refuses deletion of the base directory, whereas Rust
trims an empty prefix and can remove its base path. C++ confines canonicalized
paths; Rust rejects lexical absolute and `..` components. Rust sorts listings
while C++ only has cardinality assertions. None of these observations creates
an extra row because this C++ test file does not assert them.

## Validation

The focused parity validator discovers exactly these 10 C++ references and
reports `covered=2`, `missing=8`, `not-applicable=0`, `blocked=0`; the full
19-test store-validation suite is run at handoff.

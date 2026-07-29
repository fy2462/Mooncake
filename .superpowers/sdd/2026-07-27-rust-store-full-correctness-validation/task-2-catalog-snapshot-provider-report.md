# Task 2 catalog-backed snapshot-provider parity audit

## Scope and method

Reviewed all and only the eight canonical `TEST_P` bodies in
`mooncake-store/tests/ha/snapshot/catalog_backed_snapshot_provider_test.cpp`.
The C++ assertions and directly exercised provider implementation were the
oracle; discoverable Rust tests count only when they assert the same observable
result, fields/replicas, and error semantics.

The manifest has one row per `TEST_P` body. `BuildCatalogBackendParams()` always
instantiates `Embedded` and additionally instantiates `Redis` under
`STORE_USE_REDIS`; the Redis setup skips without `--redis_endpoint`. Thus the
inventory remains eight canonical rows, with eight Embedded concrete tests in a
default build and sixteen concrete identities in a Redis-enabled build. Rust
provider fixtures inspected here use `EmbeddedSnapshotCatalogStore` and local
object storage only. Etcd is the separate HA coordinator/shared-oplog boundary,
not a snapshot-catalog backend or a blocker for these rows.

## Result

| Disposition | Count |
| --- | ---: |
| Covered | 0 |
| Missing | 8 |
| Not applicable | 0 |
| Blocked | 0 |

All candidates are partial rather than covered under the strict assertion
rule:

| C++ reference | Disposition | Evidence-based result |
| --- | --- | --- |
| `LoadLatestSnapshotReturnsEmptyWhenCatalogMissing` | Missing | Rust source may produce no candidates from an empty Embedded catalog, but no discoverable test starts with no latest marker and asserts `Ok(None)`. |
| `LoadLatestSnapshotRoundTrip` | Missing | Rust's synthetic-C++ and Rust-producer tests do not assert C++'s one complete disk replica, path/object size, client id, and default-object contract from C++-emitted bytes. |
| `LoadLatestSnapshotWithDataTypeField` | Missing | The Rust shape loop checks retained optional Rust fields from a synthetic Memory-replica fixture, not the C++ default disk-replica result. |
| `LoadLatestSnapshotWithHardPinnedField` | Missing | The same Embedded-only shape loop observes Rust `hard_pinned`; C++ accepts then discards that field and asserts a default disk replica instead. |
| `LoadLatestSnapshotWithDataTypeAndHardPinned` | Missing | Rust's V3 fixture differs in producer, replica type, and asserted fields from the C++ V3 default-object oracle. |
| `LoadLatestSnapshotWithGroupId` | Missing | Rust retains CurrentV4 optional fields; C++ only verifies default-object/disk-replica restoration and its loaded type does not expose those values. |
| `RejectsOverflowingReplicaCount` | Missing | Rust source has checked conversion/addition, but no discoverable provider test feeds the seven-field `UINT32_MAX` declaration and asserts the resulting error. |
| `RejectsClusterMismatch` | Missing | Rust's Embedded test asserts only `is_err()`; it does not assert `HaError::InvalidParams` or the before-catalog-access property corresponding to C++ `INVALID_PARAMS`. |

## Follow-up matrix

Add provider tests that use C++-emitted fixtures (or a shared cross-language
producer) and assert the one-object/default disk replica fields for legacy and
all four metadata layouts. Add an empty-latest-marker `Ok(None)` test, the
declared-replica-count overflow fixture with a Rust-native error assertion, and
a cluster-mismatch assertion on `HaError::InvalidParams` before any catalog
read. Run the same suite against Redis when an endpoint is available; the
endpoint is a runtime execution prerequisite, not a reason to mark the missing
behavior blocked.

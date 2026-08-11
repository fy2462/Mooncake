# FileStorage IsEnableOffloading Parity Design

## Goal

Close `FileStorageTest.IsEnableOffloading` with a direct Rust witness against a
production bucket-admission boundary.

## C++ Oracle

`BucketStorageBackend::IsEnableOffloading` separates two configuration owners:

- `BucketBackendConfig` supplies the maximum keys and data bytes in one bucket,
  plus the optional eviction policy/quota.
- `FileStorageConfig` supplies global key and byte limits across all buckets.

When eviction is active with a positive bucket quota, admission is always
allowed because eviction creates capacity. Otherwise, the current stored key
and byte totals plus one complete configured bucket must fit within the global
limits.

The C++ test checks an empty default backend is admitted, a global key limit of
9 rejects a 10-key bucket, and a global byte limit of 100 rejects a 969-byte
bucket.

## Rust Boundary

Add `BucketStorageBackend::is_enable_offloading(total_keys_limit,
total_size_limit)`. Passing the two global limits explicitly preserves their
`FileStorageConfig` ownership and avoids conflating them with Rust
`BucketStorageConfig::quota_bytes`, which represents the backend eviction
quota.

The method reads the backend metadata under its existing state mutex. It
returns true immediately when eviction is enabled and either an explicit quota
is configured or an initialized backend has resolved an automatic physical
capacity. Otherwise it uses checked additions for current keys plus
`bucket_keys_limit` and current logical bytes plus `bucket_size_limit`; overflow
is a closed admission result rather than a panic.

The method does not initialize the backend. This matches the C++ constructor-
time test boundary and avoids filesystem side effects in a pure admission
query. Production callers after initialization still observe recovered
metadata and resolved capacity from the state.

## Test

Add one exact unit witness:

`cpp_parity_file_storage_is_enable_offloading_preflights_full_bucket`

It creates three fresh backends matching the C++ cases and asserts `true`,
`false`, `false`. The tight-limit cases keep automatic quota unresolved so the
whole-bucket global preflight is exercised rather than the eviction fast path.

## Manifest, Ledger, and Counts

After focused and complete client-library passes, change exactly the one
selected Store row from `missing` to `covered` and append one remediation
record. Against the independently reviewable committed parent, Store moves from
`covered=751, missing=533, not-applicable=115` to
`covered=752, missing=532, not-applicable=115`. In the accumulated checkout,
Store moves from `covered=1093, missing=191, not-applicable=115` to
`covered=1094, missing=190, not-applicable=115`. Wheel counts do not change.

## Verification

Use TDD, then run the exact witness, all bucket tests, the complete client
library suite, scoped rustfmt, all four parity validators, validator contracts,
JSON parsing, scoped pre-commit, and `git diff --check`. Preserve all earlier
working-tree changes and commit only this wave's exact hunks.

# Storage Backend Disk-Only Read Parity Design

## Goal

Close `StorageBackendE2ETest.DiskOnlyReadAfterEviction` with one real Rust
global-Disk and background memory-eviction witness.

## C++ Oracle

The C++ fixture writes four 256-KiB seed objects, waits for their Disk replicas
and leases to expire, then writes twelve equally sized pressure objects. At
least one seed must retain Disk metadata after its Memory replica is evicted,
and an expected-size read must return the exact seed bytes.

## Rust Boundary

Use an isolated global FilePerKey backend and a 16-MiB writer segment. Configure
short lease and eviction intervals, a 10% high watermark, and a 5% eviction
target. Then:

1. write four indexed 256-KiB seed values and assert every Disk replica;
2. wait past the configured lease TTL;
3. write twelve indexed 256-KiB pressure values;
4. bounded-poll the seed queries until one has Disk and no Memory replica;
5. read that key through public `get_buffer`, assert its reported size is
   exactly 256 KiB, and assert all bytes equal the indexed seed value.

Rust `get_buffer` delegates to the production `get` path, which uses replica
size metadata and the global-Disk reader's exact-size validation. The combined
size and byte assertions are the portable result of C++ `GetWithExpectedSize`.

## Scope and Verification

No production change is expected. Run the exact witness repeatedly, the full
non-CXL integration binary, client lib, scoped formatting, all parity
validators and validator contracts. Stage only the new integration hunk.

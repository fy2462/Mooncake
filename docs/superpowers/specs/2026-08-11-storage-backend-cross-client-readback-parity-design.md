# Storage Backend Cross-Client Readback Parity Design

## Goal

Close `StorageBackendE2ETest.CrossClientReadback` with one production-path
Rust client integration witness.

## C++ Oracle

One client writes five distinct 4-KiB values and waits until each object has a
shared Disk replica. A separately mounted client must observe that Disk replica
for every key and return the exact corresponding bytes.

## Rust Boundary

Use the existing in-process TCP master/client harness and the Master's real
global FilePerKey storage backend:

1. start a master with an isolated `storage_fs_dir`;
2. create a writer and put five exact 4-KiB values to its Memory segment;
3. query every object and assert a `ReplicaType::Disk` replica exists;
4. create a distinct reader client;
5. from that reader, query every key for Disk visibility and call the public
   `get`, asserting exact bytes for each index.

The test intentionally keeps the writer alive. Writer death and disk-only
readback remain separate C++ lifecycle contracts.

## Scope and Verification

No production change is expected. Run the exact witness, the non-CXL portion
of the full in-process client binary, client lib, scoped formatting, parity
validators and validator contracts. Stage only the added test hunk because the
integration file contains unrelated accumulated work.

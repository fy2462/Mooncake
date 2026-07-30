# Batch Query IP Parity Remediation Design

## Purpose

Close the eight ordinary C++ `MasterServiceTest.BatchQueryIp*` gaps through
executable Rust master-service evidence and correct the real semantic mismatch
those tests expose. The C++ Store source remains the authority and stays
immutable and unexecuted.

This is a bounded correctness wave. It does not change etcd leadership,
client data movement, Store LocalDisk, or the io_uring performance gate.

## Observed Mismatch

C++ `MasterService::QueryIp` obtains the requested client's mounted segments,
skips empty `segment.te_endpoint` values, strips the port from every remaining
endpoint, and de-duplicates the resulting hosts. It distinguishes these two
states:

- a client with mounted segments but no nonempty endpoints succeeds with an
  empty address vector;
- an unknown client, or a client with no mounted segments, returns
  `CLIENT_NOT_FOUND` and is omitted by `BatchQueryIp`.

Rust currently calls `addresses_for_client`, which prefers the client's cached
addresses and otherwise derives hosts from `segment.name`. `BatchQueryIp` then
inserts a client only when that vector is nonempty. Consequently it can return
the segment name instead of the transfer endpoint and cannot represent the
C++ successful-known-client/empty-address-vector state.

The existing Rust single-query test reinforces the wrong source by mounting a
segment named `10.0.0.1:1234` with an empty transfer endpoint.

## Chosen Approach

Introduce one master-internal query helper used by both `QueryIp` and
`BatchQueryIp`. The helper directly examines mounted Memory/CXL segment records
for the requested client and returns `Option<Vec<String>>`:

- `None` means no mounted segment belongs to the client;
- `Some(vec![])` means the client has mounted segments but every transfer
  endpoint is empty;
- `Some(addresses)` contains unique hosts derived only from nonempty
  `segment.te_endpoint` values.

This keeps single and batch APIs aligned with the C++ layering, where
`BatchQueryIp` delegates each item to `QueryIp`. It avoids modifying mount,
remount, ping, metadata discovery, or `ClientInfo.addresses`, whose address
caches serve separate topology and liveness concerns.

Two rejected alternatives are:

1. Fix only `batch_query_ip_impl`. This is a smaller diff but leaves the Rust
   single and batch APIs with conflicting address sources, unlike C++.
2. Change mount and ping to cache transfer-endpoint hosts in
   `ClientInfo.addresses`. This has a much wider effect on metadata,
   heartbeat, host identity, and placement behavior than the eight query
   oracles require.

## Endpoint Host Semantics

The query helper uses a small C++-compatible endpoint-host parser rather than
`resolve_host_id`. `resolve_host_id` intentionally removes loopback and
wildcard values for physical-node identity; C++ `getHostNameWithoutPort` does
not, and the query contract explicitly returns `127.0.0.1` and `::1`.

The parser must preserve the forms asserted by the C++ tests:

- `127.0.0.1:12345` becomes `127.0.0.1`;
- `[::1]:17813` becomes `::1`;
- raw `::1` remains `::1`;
- `fe80::a236:bcff:fecb:a1be%eno2:15773` becomes
  `fe80::a236:bcff:fecb:a1be%eno2`;
- ordinary hostnames without a colon remain unchanged.

Empty endpoints are skipped before parsing. Duplicate hosts from different
ports are returned once. Multi-address result order is not a contract because
the C++ implementation collects addresses through an unordered set and its
tests compare membership rather than order.

## RPC Behavior

`query_ip_impl` calls the shared helper:

- `None` maps to tonic `NotFound`, preserving the Rust public error boundary
  corresponding to C++ `CLIENT_NOT_FOUND`;
- `Some(addresses)` returns success, including when `addresses` is empty.

`batch_query_ip_impl` calls the same helper for each requested UUID:

- every `Some(addresses)` result is inserted into the response map, including
  an empty vector;
- every `None` result is silently omitted;
- empty request input succeeds with an empty map.

No protobuf, public Rust type, configuration, or persisted state changes.

## TDD Evidence

Add these exact service tests to
`rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs`:

| C++ reference | Rust test |
|---|---|
| `BatchQueryIpTest` | `batch_query_ip_single_and_unknown_client_parity` |
| `BatchQueryIpMultipleSegmentsTest` | `batch_query_ip_deduplicates_multiple_segment_addresses_parity` |
| `BatchQueryIpEmptyClientIdTest` | `batch_query_ip_empty_request_parity` |
| `BatchQueryIpMultipleSegmentsEmptyTeEndpointTest` | `batch_query_ip_retains_client_with_only_empty_endpoints_parity` |
| `BatchQueryIpBracketedIpv6Test` | `batch_query_ip_parses_bracketed_ipv6_parity` |
| `BatchQueryIpLinkLocalIpv6WithScopeTest` | `batch_query_ip_preserves_ipv6_scope_parity` |
| `BatchQueryIpIpv6NoPortTest` | `batch_query_ip_accepts_ipv6_without_port_parity` |
| `BatchQueryIpMixedIpv4AndIpv6Test` | `batch_query_ip_mixed_ipv4_ipv6_parity` |

The tests mount segments with arbitrary names and the exact C++ transfer
endpoints so a regression to name-based derivation cannot pass accidentally.
The multi-address tests compare literal sets; single-address and empty-vector
tests compare exact vectors and exact map membership.

Before changing production code, run the newly added tests and preserve the
expected semantic RED results. The single, IPv6, de-duplication, mixed-family,
and empty-endpoint cases should fail for the inspected address-source and map
insertion differences. An empty-request test may pass immediately as
characterization evidence and does not authorize a production edit.

After the shared helper correction, update the existing
`test_query_ip_derives_address_from_mounted_segment` fixture to use an arbitrary
segment name and `10.0.0.1:1234` as its transfer endpoint. Keep its exact
single-address assertion so it protects the corrected source rather than
weakening coverage.

## Production Change Boundary

The planned production edits are limited to:

- `rust-repo/crates/mooncake-store-master/src/service/helpers.rs` for the
  endpoint parser and optional query result;
- `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_query.rs`
  for single-query result mapping;
- `rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs` for
  batch inclusion/omission;
- the helper import list in
  `rust-repo/crates/mooncake-store-master/src/service/mod.rs` if required.

Do not change segment publication, client address caches, host identity,
metadata registration, protobufs, or metrics. If the RED output identifies a
different cause, stop and revise this design before broadening the edit.

## Manifest and Verification

Only after all eight focused tests and the complete owning target pass, move
the corresponding rows in
`rust-repo/tools/store-validation/parity-map.json` from `missing` to `covered`
with their exact Rust evidence identities.

Run the focused tests, complete `test_master_mount` target, master library
tests implicated by the helper, all Store validation-tool tests, ordinary and
strict manifest validation, source-only combined planning, scoped pre-commit
with `SKIP=mooncake-code-format`, and staged/unstaged immutable-reference
guards. Strict validation may remain nonzero only because other manifest rows
are still missing; it must report no schema or evidence errors.

The eight snapshot/restore variants and unrelated LocalDisk rows remain
missing. The seven wheel `BatchReplicaClear` rows remain missing until a
prebuilt native Transfer Engine library permits their actual E2E execution.

# Canonical TenantId Parity Design

## Goal

Bring the canonical tenant identity introduced by upstream commit `c45fb07c`
into the Rust Store. The Rust implementation must reject invalid request
tenant identifiers at the same boundaries as the C++ Store and must carry a
validated `TenantId` through Store-owned state instead of using interchangeable
`String` values.

This is a domain-type migration inside `rust-repo`; protobuf fields and durable
wire formats remain string-compatible with existing Rust and C++ deployments.

## Compatibility contract

`TenantId` has these observable rules:

- an empty identifier normalizes to `default` when a request is allowed to
  address the default tenant;
- identifiers beginning with `_` are invalid;
- bytes below ASCII space (`0x20`) and DEL (`0x7f`) are invalid;
- printable non-ASCII UTF-8 remains valid, matching the C++ byte validation;
- colons and other printable punctuation remain valid;
- the tenant-scoped key encoding remains `tenant_id + NUL + local_key`;
- a legacy unscoped key parses as a key in the default tenant;
- an embedded NUL in the local key is preserved because only the first NUL is
  the tenant separator.

When strict multi-tenant quota mode is disabled, public Store requests ignore
their tenant field and operate on `default`. When strict mode is enabled,
ordinary read and mutation requests accept an empty identifier as `default`,
but write-admission requests require a non-empty, valid, registered tenant.
Invalid ordinary requests return `InvalidArgument`; empty, invalid, or
unregistered write tenants retain the existing tenant-not-registered status.

Batch requests validate their request-level tenant before processing any item.
A rejected tenant therefore cannot create partial object, quota, task, or
replica state.

## Domain type

Add a focused `TenantId` type in the master crate. It owns its canonical string
and provides:

- `TenantId::new(String)`, which normalizes empty input and validates the
  canonical value;
- `TenantId::default()` and `TenantId::is_default()`;
- `TenantId::as_str()` and an explicit consuming string conversion for wire
  and persistence boundaries;
- `TenantId::make_scoped_key()` and `TenantId::parse_scoped_key()`;
- ordering, hashing, display, borrowing, and serde support needed by existing
  maps and persisted structs.

Construction from untrusted strings is fallible. Deserialization is also
fallible, so invalid snapshot or oplog tenant values cannot silently enter the
state machine. Internal code must not expose an unchecked public constructor.

The type belongs to the Store control plane, not Transfer Engine or the public
protobuf crate. No native or FFI change is required.

## Internal migration boundary

Use `TenantId` for Store-owned tenant identity throughout the master:

- `ObjectEntry` and other object metadata;
- tenant quota keys and snapshots;
- promotion, replication, offload, remote-pull, drain, and client task state;
- helper and service method parameters that represent a tenant;
- snapshot/catalog reconstruction;
- oplog record construction and application;
- maps, sets, grouping keys, and shard selection keyed by tenant.

User object keys, group identifiers, serialized JSON fields, protobuf fields,
admin HTTP path/query values, and logging fields remain strings. Conversion is
allowed only at an explicit external boundary or where an existing stable
format requires it.

This migration must not change protobuf schemas, snapshot field names, oplog
JSON keys, tenant quota policy formats, or the byte representation of scoped
keys.

## Request resolution

Centralize request policy instead of repeating normalization in each handler:

- `resolve_request_tenant(raw, strict_enabled)` returns `default` immediately
  when strict mode is disabled; otherwise it parses `raw`, allowing empty to
  select `default`;
- `resolve_write_tenant(raw, strict_enabled)` returns `default` when strict
  mode is disabled; otherwise it rejects empty or invalid input with the
  tenant-not-registered semantic before quota reservation;
- batch handlers call the relevant resolver once before iterating;
- internal callbacks receive `&TenantId` or an owned `TenantId`, never the raw
  request string.

The gRPC layer maps a structurally invalid ordinary tenant to
`tonic::Code::InvalidArgument`. Write admission continues to use the current
quota error mapping for missing or unregistered tenants so existing clients do
not observe a new status contract.

## Persistence and recovery

Snapshots and oplogs continue to encode tenant identifiers as strings.
Serialization obtains the canonical string from `TenantId`. Recovery parses
each string through the same validated constructor.

Legacy compatibility rules are:

- a missing or empty tenant metadata field becomes `default`;
- an unscoped legacy object key belongs to `default`;
- a valid explicit tenant is preserved byte-for-byte;
- an invalid explicit tenant fails recovery with contextual snapshot/oplog
  corruption information instead of being normalized or skipped.

Catalog recovery follows the current C++ field semantics exactly. A
three-field metadata item is `[tenant_id, user_key, metadata]`; `user_key` is
always an opaque local key and may contain embedded NUL bytes. It must never be
reinterpreted as a tenant-scoped key. A two-field legacy item has no explicit
tenant field and may use the older scoped-key representation, so recovery
splits only its first NUL and otherwise assigns it to `default`.

Oplog records have a separate durable scoped key. When an oplog also carries
an explicit tenant, recovery must reject a conflict between the two identities.
Legacy empty tenant/user fields are treated as unspecified when the durable
key already supplies the identity. These rules preserve legacy entries without
allowing two logical identities for one oplog object.

## Error handling

`TenantIdError` distinguishes an invalid reserved prefix from an invalid ASCII
control byte for diagnostics, while public RPCs retain stable status codes.
Errors should include escaped/contextual input where safe, but must not use a
lossy conversion that changes the identifier under validation.

No handler may reserve quota, allocate replicas, mutate object maps, enqueue a
task, or append an oplog record before tenant resolution succeeds.

## Testing

Unit tests for the domain type cover:

- empty/default normalization;
- valid ASCII punctuation, colons, and UTF-8;
- `_reserved`, newline, NUL, and DEL rejection;
- ordering and hashing;
- scoped-key round trips, including embedded NUL in the local key;
- legacy unscoped-key parsing;
- serde rejection of invalid values.

Focused service tests cover strict mode enabled and disabled for:

- PutStart and Upsert write admission;
- GetReplicaList, ExistKey, Remove, and RemoveAll;
- request-level batch operations with proof that rejection is atomic;
- promotion/replication/task APIs that accept tenant identifiers;
- quota admin APIs.

Recovery tests cover valid and invalid snapshot/oplog tenant values, legacy
empty values, two-field legacy scoped keys, three-field user keys containing
embedded NUL bytes, oplog identity conflicts, and unchanged serialized field
shapes.
Existing multi-tenant isolation, quota, promotion, replication, snapshot, and
oplog suites remain required regression coverage.

## Delivery and verification

Implement this slice with red-green TDD and reviewable commits:

1. introduce and test the domain type without changing wire formats;
2. migrate state and persistence boundaries;
3. migrate request resolution and service paths;
4. add cross-path parity and recovery tests;
5. run formatting, focused tests, the master crate suite, pre-commit on touched
   files, and the relevant workspace checks.

The slice is complete only when searches show that tenant identity in the
master core is no longer represented by an unconstrained `String`, except at
documented wire, persistence, configuration, logging, or user-data boundaries.

## Implementation status

Implemented on 2026-07-24 in these commits:

- `5b43d744` introduces the canonical `TenantId` domain type;
- `0f58eba3` carries it through master state, quota, policy, and events;
- `cedff1c7` and `8b223076` validate recovery while preserving the C++
  three-field catalog key contract;
- `e4ec71c2` and `9e57d35b` resolve request tenants before mutation and enforce
  registered-tenant write admission;
- `650f8071` closes the remote-pull, task-payload, legacy-helper, and stale-test
  audit gaps;
- `c345a3eb` directly verifies disabled-mode remote-pull canonicalization;
- `86dd9f08` closes final-review gaps in AddReplica/offload admission,
  remove/revoke recovery, non-lossy durable/config decoding, and distributed
  miss-handler tenant propagation.

The final audit found no unchecked Store-domain tenant identity path. Raw
tenant strings remain only in protobuf/HTTP entry values, oplog and event wire
payloads, quota-policy configuration, logging, and test-facing compatibility
methods; each Store-owned path converts to `TenantId` before state access.
Legacy `normalize_tenant_id`, `make_tenant_scoped_key`, and `split_scoped_key`
exports were removed so they cannot bypass validation.

The C++ `Ping` control-plane API accepts no tenant identity, so the Rust proto's
compatibility `tenant_id` field is intentionally ignored by Ping. Remote-pull
coordination is a Rust-only API with no same-named C++ oracle. Its strict-mode
tenant isolation and disabled-mode collapse to `default` are an inference from
the canonical identity contract applied to its object-key coordination state.

Verification completed with `cargo fmt --all -- --check`, `git diff --check`,
the TenantId parity target (17 passed), oplog target (24 passed), distributed
miss-handler/client targets (73 passed), and the complete
`mooncake-store-master` suite (373 passed, 0 failed; 2 documentation tests
ignored). Strict all-target Clippy remains blocked by the existing crate-wide
baseline (102 library and 108 library-test warnings promoted to errors); a
non-denying all-target run completed and the warning-to-hunk audit found no
warning on a Task 5 added line. Pre-commit disposition is recorded in the
matching migration log.

## Out of scope

- changing protobuf field types or field numbers;
- changing snapshot or oplog schemas;
- linking or calling the C++ Store;
- changing Transfer Engine APIs;
- redesigning tenant quota allocation policy;
- moving client-side tenant configuration to a public Rust newtype in this
  slice, unless required to keep an existing Rust client API compiling.

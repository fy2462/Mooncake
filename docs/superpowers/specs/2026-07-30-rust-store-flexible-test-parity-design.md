# Rust Store Flexible Test Parity Design

## Purpose

Replace the overly strict one-to-one Rust primary-test rule with semantic Store
test parity. A C++ Store test is covered when one or more discoverable Rust
Store tests collectively assert its complete observable behavior. A Rust test
may provide evidence for more than one C++ Store test when it genuinely asserts
each oracle.

This design supersedes the uniqueness and single-primary requirements in
`2026-07-30-rust-store-one-to-one-test-parity-design.md`. It does not weaken the
requirement to match the executed C++ assertions and Store-visible results.

## Audit Scope

The C++ inventory is limited to tests discovered beneath
`mooncake-store/tests`. Sources beneath `mooncake-transfer-engine`, including
Transfer Engine, TENT, transport, and accelerator test suites, are outside this
audit and must not contribute C++ inventory rows.

Rust parity evidence may use discoverable tests beneath these Store-facing
crates:

- `mooncake-store-client`;
- `mooncake-store-core`; and
- `mooncake-store-master`; and
- `transfer-engine-ffi`.

`transfer-engine-ffi` tests may satisfy a C++ Store row when they assert the
same Store-visible FFI boundary result. This does not add independent C++
Transfer Engine tests to the inventory. Tests from `mooncake-p2p-store`,
`mooncake-conductor`, or another unrelated Rust package cannot satisfy a C++ Store
parity row.

C and C++ source and header files remain immutable read-only oracle inputs.
They must not be edited, formatted, built, linked, loaded, staged, or committed.

## Coverage Model

### Covered

A covered C++ Store row has one or more discoverable Rust Store test references.
The referenced tests, considered together, assert every executed C++ result
relevant at the Rust Store boundary.

Both of these mappings are valid:

1. one Rust test supplies complete evidence for several C++ Store tests; and
2. several Rust tests collectively supply complete evidence for one C++ Store
   test.

Test reuse and aggregation are not errors. Similar names, implementation source
alone, comments, or incomplete assertions remain insufficient.

### Missing

A missing row lacks complete aggregate Rust Store test evidence. It may have no
Rust test at all or only partial existing evidence. The manifest keeps
`rust: []` for missing rows so it does not claim coverage; the reason records
any partial evidence and the exact remaining semantic gap.

Missing therefore means "complete C++ Store oracle not yet proved", not "the
Rust repository contains no related test".

### Blocked

A blocked row is semantically covered by one or more discoverable Rust Store
tests, but a named external prerequisite prevents their required execution.
Blocked rows retain an explicit prerequisite and cannot roll up to correctness
PASS.

### Not applicable

The existing reviewed N/A categories remain unchanged. Architecture or test
organization differences alone do not justify N/A.

## Manifest and Validator Contract

The validator must enforce:

- every discovered C++ test under `mooncake-store/tests` appears exactly once;
- every covered or blocked row has at least one discoverable Rust Store test;
- covered and blocked rows may contain multiple Rust test references;
- a Rust Store test may be referenced by multiple C++ rows;
- missing and N/A rows do not claim Rust coverage references;
- referenced Rust tests belong to one of the four approved Store-facing
  crates;
- every non-covered disposition has its existing concrete reviewed reason; and
- C++ inventory, disposition, Rust-reference, unique-Rust-test, shared-Rust-test,
  and multi-test-row counts are reported independently.

The validator must no longer emit errors for multiple Rust references on one
covered row or for the same Rust reference appearing on several covered rows.
The `primary_reviewed` marker and primary-owner terminology are removed because
the evidence model no longer has a unique primary.

## Restoration

Restore exactly the 128 rows that changed from covered to missing solely in the
one-to-one structural migration between `daa8b64c` and `cb5d4a0d`. Restore each
row's reviewed Rust evidence references and covered disposition from the
pre-migration manifest.

Do not restore the later 47 rows downgraded by source-level semantic review;
their Rust evidence omits an executed assertion or scenario and they remain
missing.

The expected post-restoration baseline is:

- 1,399 C++ Store tests;
- 1,301 applicable rows;
- 180 covered;
- 1,121 missing;
- 0 blocked;
- 98 not applicable;
- 189 covered Rust references;
- 108 unique covered Rust tests;
- 47 Rust tests referenced by more than one C++ row; and
- 9 covered C++ rows using more than one Rust test.

These counts describe different dimensions and are not required to be equal.

## Test-Driven Change

Before modifying the validator, add focused tests proving that:

1. one covered row accepts multiple discoverable Rust Store references;
2. two covered rows accept the same discoverable Rust Store reference;
3. covered and blocked rows without Rust evidence still fail;
4. missing and N/A rows with Rust coverage references still fail;
5. a `transfer-engine-ffi` test can satisfy a Store row while an unrelated
   Rust package cannot; and
6. the summary reports reference reuse and aggregation without treating either
   as an error.

Run the focused tests red, implement the minimum validator change, then run the
full validation-tool suite, real manifest validation, Python compilation,
scoped pre-commit, the C/C++ immutability guard, and `git diff --check`.

## Correctness and Performance Gates

Restoring shared or aggregate coverage changes test-accounting semantics only.
It does not mark any known partial row covered and does not complete the Store
correctness gate. Module, multi-node, and fault-recovery gates remain required.
Store LocalDisk `io_uring` performance work remains gated until the revised
correctness criteria pass.

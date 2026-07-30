# Rust Store Flexible Test Parity Design

## Purpose

Replace the overly strict one-to-one Rust primary-test rule with semantic
parity across the C++ Store, C++ Transfer Engine, C++ TENT, and the Store/TE
portion of the Python wheel suite. A reference test is covered when one or more
discoverable Rust tests collectively assert its complete observable behavior.
A Rust test may provide evidence for more than one reference test when it
genuinely asserts each oracle.

This design supersedes the uniqueness and single-primary requirements in
`2026-07-30-rust-store-one-to-one-test-parity-design.md`. It does not weaken the
requirement to match the executed reference assertions and Rust-observable
Store or Transfer Engine FFI results.

## Audit Scope

The audit has four independent reference inventories:

- 1,399 GoogleTest declarations beneath `mooncake-store/tests`;
- 348 GoogleTest declarations beneath `mooncake-transfer-engine/tests`;
- 319 GoogleTest declarations beneath `mooncake-transfer-engine/tent/tests`;
  and
- 352 Python test declarations beneath `mooncake-wheel/tests` after explicitly
  excluding the two release-packaging tests in `test_release_wheel_tags.py`.

The wheel inventory includes Store, structured/tensor, buffer-pool,
configuration, CLI, CUDA/HIP, EP, Transfer Engine, and import-compatibility
runtime/API tests. Helper functions that merely happen to start with `test_`
but are not collected by the owning Python test framework must not be counted;
inventory discovery must match actual pytest/unittest collection semantics at
the declaration level. Parameterized declarations count once, consistently
with the existing treatment of C++ `TEST_P` declarations.

Rust parity evidence may use discoverable tests beneath these Store-facing
crates:

- `mooncake-store-client`;
- `mooncake-store-core`; and
- `mooncake-store-master`; and
- `transfer-engine-ffi`.

`transfer-engine-ffi` tests may satisfy Store, Transfer Engine, TENT, or wheel
rows when they assert the same observable FFI result. Tests from
`mooncake-p2p-store`, `mooncake-conductor`, or another unrelated Rust package
cannot satisfy a parity row.

All four reference suites are immutable read-only oracle inputs. C and C++
source and header files must not be edited, formatted, built, linked, loaded,
staged, or committed. Files beneath `mooncake-wheel/tests` must not be edited or
used as a place to add parity tests. Remediation changes are limited to Rust
implementation, Rust tests, validation tools, and audit documentation.

## Coverage Model

### Covered

A covered reference row has one or more discoverable Rust test references. The
referenced tests, considered together, assert every executed result relevant at
the corresponding Rust Store or Transfer Engine FFI boundary.

Both of these mappings are valid:

1. one Rust test supplies complete evidence for several reference tests; and
2. several Rust tests collectively supply complete evidence for one reference
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
organization differences alone do not justify N/A. A C++ Transfer Engine or
TENT test may be N/A only when source review proves it exercises an internal
C++ boundary with no Rust-observable Store or FFI result.

## Manifest Layout

Keep the existing Store manifest and add three scoped manifests:

- `parity-map.json` for C++ Store;
- `transfer-engine-parity-map.json` for C++ Transfer Engine;
- `tent-parity-map.json` for C++ TENT; and
- `wheel-store-parity-map.json` for the selected wheel tests.

Each row identifies its reference framework, root-relative file, and collected
test name. The three new manifests start as reviewed inventory backlogs rather
than claiming coverage from name similarity. A shared validator checks each
manifest independently and emits an aggregate result across all four without
merging their rows into one large file.

All four manifests use a generic schema-version-2 identity:

```json
{
  "suite": {
    "id": "store-cpp",
    "framework": "gtest",
    "reference_root": "mooncake-store/tests"
  },
  "entries": [
    {
      "reference": {
        "file": "example_test.cpp",
        "test": "ExampleTest.ObservableBehavior"
      }
    }
  ]
}
```

The wheel suite uses `framework: "python"`; its test identity is the collected
module-level function or `ClassName.method_name`. The wheel suite descriptor
also records `test_release_wheel_tags.py` as its only excluded test file.

Migrating the existing Store manifest from `cpp` to `reference` is mechanical:
the validator must prove that all 1,399 `(file, test)` identities, behaviors,
boundaries, dispositions, reasons, review data, and Rust evidence are preserved
apart from the separately approved restoration of 128 rows and removal of the
obsolete primary-review marker.

## Manifest and Validator Contract

The validator must enforce:

- every in-scope reference test appears exactly once in its owning manifest;
- `test_release_wheel_tags.py` remains explicitly excluded and no other
  collected wheel test is silently omitted;
- every covered or blocked row has at least one discoverable approved Rust
  test;
- covered and blocked rows may contain multiple Rust test references;
- an approved Rust test may be referenced by multiple reference rows;
- missing and N/A rows do not claim Rust coverage references;
- referenced Rust tests belong to one of the four approved Store-facing
  crates;
- every non-covered disposition has its existing concrete reviewed reason; and
- per-suite and aggregate inventory, disposition, Rust-reference,
  unique-Rust-test, shared-Rust-test, and multi-test-row counts are reported
  independently.

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
They apply only to the existing Store manifest. The new Transfer Engine, TENT,
and wheel baselines are established by source-level audit; they must not be
inferred from filenames or APIs.

The expected aggregate reference inventory is 2,418 declarations before any
source-level N/A disposition: 1,399 Store, 348 Transfer Engine, 319 TENT, and
352 selected wheel tests.

## Test-Driven Change

Before modifying the validator, add focused tests proving that:

1. one covered row accepts multiple discoverable Rust Store references;
2. two covered rows accept the same discoverable Rust Store reference;
3. covered and blocked rows without Rust evidence still fail;
4. missing and N/A rows with Rust coverage references still fail;
5. a `transfer-engine-ffi` test can satisfy a Store row while an unrelated
   Rust package cannot; and
6. the C++ Store, C++ Transfer Engine, C++ TENT, and selected wheel inventories
   are routed to distinct manifests with exact completeness checks;
7. the wheel release-packaging tests are excluded while every other collected
   wheel test is inventoried; and
8. per-suite and aggregate summaries report reference reuse and aggregation
   without treating either as an error.

Run the focused tests red, implement the minimum validator change, then run the
full validation-tool suite, real manifest validation, Python compilation,
scoped pre-commit, the C/C++ immutability guard, and `git diff --check`.

## Correctness and Performance Gates

Restoring shared or aggregate Store coverage changes test-accounting semantics
only. It does not mark any known partial row covered. The three new inventories
are audited before remediation begins, then genuine missing rows are fixed in
stable suite/file/test order with test-first Rust changes.

Store, Transfer Engine FFI, TENT-facing FFI, wheel API, module, multi-node, and
fault-recovery gates remain required as applicable. Hardware- or
service-dependent tests may be blocked only when a discoverable Rust test
already exists and the named external prerequisite is unavailable. Store
LocalDisk `io_uring` performance work remains gated until the revised
correctness criteria pass.

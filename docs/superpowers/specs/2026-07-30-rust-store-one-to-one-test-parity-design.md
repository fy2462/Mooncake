# Rust Store One-to-One Test Parity Design

## Purpose

Make the C++ Store test inventory and the independent Rust Store parity suite
agree at both the behavioral and test-count dimensions. Every applicable C++
`TEST`, `TEST_F`, or `TEST_P` case must ultimately own one unique,
independently discoverable Rust parity test. A C++ case may remain
`not-applicable` only after a second source-level review proves that the tested
state or boundary does not exist in the Rust product.

The C++ Store remains a read-only source oracle. No C++ Store target is built,
linked, loaded, or executed as acceptance evidence.

## C and C++ Source Immutability

All C and C++ sources and headers are strictly read-only throughout this work.
This includes `.c`, `.cc`, `.cpp`, `.cxx`, `.cu`, `.cuh`, `.h`, `.hh`, and
`.hpp` files in Store, Transfer Engine, tests, examples, and support modules.
They may be inspected to derive Rust behavior, but they must never be edited,
formatted, generated, staged, or committed.

Commands with repository-wide write side effects are prohibited. File-level
pre-commit runs must skip the `mooncake-code-format` hook so it cannot rewrite
C++ files outside the requested Rust/manifest/document scope. Every batch
checks the worktree before and after its changes and fails its own handoff if
any C or C++ source/header path differs from the batch baseline.

If parity investigation exposes a C++ defect, ambiguity, or desirable C++
change, record it as oracle context and continue only with authorized Rust-side
work. Do not repair or normalize the C++ source.

## Current Baseline

The reviewed manifest currently contains 1,399 C++ tests:

- 227 `covered` rows;
- 1,065 `missing` rows;
- 107 `not-applicable` rows; and
- 0 `blocked` rows.

The 227 covered rows currently contain 236 Rust references but only 155 unique
Rust test functions. Forty-seven Rust functions are reused by more than one
C++ row, and one function is reused by ten rows. This is valid under the old
semantic-map policy but does not satisfy one-to-one test-count parity.

## Chosen Policy

For every applicable C++ test, the manifest has exactly one primary Rust parity
test. That Rust test is unique to the C++ row and can be selected and executed
independently. Different parity tests may share fixtures and helpers, but each
test function must drive and assert the complete observable result of its own
C++ oracle. Empty wrappers, aliases, ignored tests, source-text checks, or
multiple names that merely call one broad test do not count.

The final count invariant is:

```text
C++ inventory count = unique primary Rust parity test count + reviewed N/A count
```

During remediation, `missing` rows explain the temporary difference. At final
correctness acceptance, `missing` and required `blocked` are both zero.

## Disposition Rules

### Covered

A `covered` row has exactly one discoverable primary Rust test. The test owns
the row's C++ observable assertions and passes in its required gate. The same
Rust test cannot be the primary test for another C++ row.

When a C++ behavior crosses Client and Master boundaries, the primary test is
an integration test that observes the complete result. Lower-level unit tests
may remain as supporting evidence, but they do not replace the primary test or
alter the one-to-one count.

### Missing

A `missing` row has no primary Rust test. Its reason names the exact difference,
the intended Rust boundary, and the focused test to add. A difficult, slow,
flaky, hardware-dependent, or currently unimplemented behavior is still
`missing`; those properties never justify N/A.

### Blocked

A `blocked` row is applicable and already has one unique, discoverable Rust
test, but a named external prerequisite prevents execution, such as a shared
library, device, kernel capability, permission, or service. If the test itself
does not yet exist, the row is `missing`, not `blocked`. A required blocked row
cannot roll up to PASS.

### Not applicable

N/A is an exception, not a success result. It has no Rust parity test and must
identify the exact C++ test body, directly exercised implementation, and the
reason no Rust product result can be observed.

The following categories may qualify:

1. **Language-unrepresentable state.** Safe Rust makes the C++ state impossible
   to construct, such as using a moved-from owner, passing a null pointer with
   an independent nonzero length, or operating a nullable RAII lock wrapper.
2. **Absent Rust product boundary.** The C++ case tests only a private wrapper
   or helper that Rust intentionally replaces with the language, standard
   library, Tokio, or a maintained dependency, and no Store-visible result is
   asserted. Delegated primitive conformance alone is not a Rust Store test.
3. **C++ build or ABI plumbing.** The case observes a CMake macro, private
   layout, overload, raw-pointer ABI, or compile-only mechanism with no Rust
   runtime/configuration analogue.
4. **Noncanonical duplicate source.** The discovered source is byte-identical
   to a canonical C++ test source and is not a built test target. The canonical
   case remains separately mapped.
5. **Explicitly excluded component internals.** The project scope deliberately
   excludes an optional C++ component, and the case tests only that component's
   internal API. For example, Redis/K8s leadership-monitor internals may be N/A
   under the approved etcd-only serving policy, while Rust's externally
   observable fail-fast rejection still requires its own Rust test.
6. **No executable C++ oracle.** The discovered C++ case is disabled or contains
   no assertion, returned status, state transition, or other pass/fail result
   that can define a Rust acceptance test. A descriptive test name or comment
   alone does not create an oracle.
7. **Explicitly excluded performance scope.** The C++ case measures only a
   performance requirement for a component that the user explicitly excluded
   from optimization scope. Correctness cases for the same component remain
   applicable; this category cannot hide a correctness assertion or the
   approved Store LocalDisk `io_uring` performance work.

Architecture differences alone are insufficient. If Rust exposes an
equivalent user-visible return value, state transition, persistence result,
resource lifecycle, or failure behavior, the row is applicable and must be
`missing`, `blocked`, or `covered`.

## Second N/A Audit

All 107 existing N/A rows are reviewed again from source. Review proceeds in
bounded families: ownership/raw-pointer APIs, synchronization/runtime helpers,
serialization and utilities, LocalFS components, storage/mmap, optional HA
backends, duplicate sources, and remaining isolated cases.

For each row, the reviewer:

1. reads the complete C++ test body and directly exercised implementation;
2. lists only assertions executed by the test, excluding comments and fixture
   cleanup that does not affect pass/fail;
3. traces the corresponding Rust product boundary, if one exists;
4. checks existing Rust tests for the exact result;
5. retains N/A only when one allowed category is proven;
6. otherwise changes the row to `missing` or `covered`; and
7. records the decision and receives independent scoped review.

The audit must not preserve an N/A merely because implementing its equivalent
would be expensive or because the current environment cannot execute it.

## Manifest and Validator Contract

The validator will enforce:

- every discovered C++ test appears exactly once;
- every `covered` row has exactly one discoverable Rust primary test;
- every `blocked` row has exactly one discoverable Rust primary test and an
  explicit external prerequisite;
- primary Rust references are globally unique across applicable rows;
- `missing` and N/A rows have no primary Rust reference;
- every non-covered disposition has a concrete reviewed reason;
- every N/A reason identifies one allowed exclusion category; and
- the summary reports C++ total, applicable total, unique primary Rust total,
  missing, blocked, N/A, and duplicate-primary counts.

Supporting Rust evidence, when useful, is recorded separately from the primary
test and never affects the count. The validator tests are written first and
must demonstrate rejection of shared primary tests, multiple primary tests on
one row, a blocked row without a test, and an N/A row carrying a Rust test.

## Existing Covered-Row Migration

After the N/A audit and validator change, the existing 227 covered rows are
normalized. A shared broad Rust test is split into independently named tests
only when each new test contains its own oracle assertions. Shared setup and
operation helpers are encouraged; shared assertions hidden behind a generic
"run everything" helper are not.

If an existing Rust test proves only part of a C++ row, the row returns to
`missing`. If one C++ row currently cites several Rust tests, add one focused
integration test that observes the complete result, or keep the row missing
until such a test exists.

## HA Chaos Gates

The C++ random HA cases receive one primary Rust test each rather than sharing
the current combined small/large test.

The regular gate uses a fixed recorded seed and bounded duration/round count,
while preserving the C++ dimensions: three Masters, five clients, independent
50% kill/start choices, immediate hard kill, possible all-Master-down periods,
cross-client exact-byte checks, and stable-service verification after a leader
returns. The small- and large-object cases are separate test functions and
result records.

A second night/manual soak gate uses the same implementation with a recorded
variable seed and the C++ one-hour duration. A bounded regular pass does not
claim that the one-hour soak ran; both results remain separately visible.

## Delivery Order

1. Commit this policy and obtain user review.
2. Re-audit all 107 N/A rows and independently review every retained exception.
3. Add validator tests and enforce the one-to-one primary-test contract.
4. Normalize the currently covered rows to unique primary Rust tests.
5. Close `missing` rows in stable manifest order with strict red-green TDD.
6. Run module, one-to-one parity, multi-node, and fault-recovery gates.
7. Only after every required correctness gate passes, establish and optimize
   the Store LocalDisk `io_uring` benchmark.

TENT, accelerator DLPack, and shared-memory hot-cache performance work remain
outside the approved optimization scope. Their correctness behaviors remain
in scope when a C++ Store test requires them.

## Acceptance Criteria

Correctness test parity is accepted only when:

1. all discovered C++ tests have a second-review disposition;
2. every retained N/A meets an allowed exclusion category and has no Rust test;
3. every applicable C++ row owns exactly one unique, discoverable Rust test;
4. the number of unique primary Rust tests equals C++ total minus final N/A;
5. no row is missing, required-blocked, skipped, stale, or undiscoverable;
6. every primary test passes its focused and owning module gate;
7. the module, multi-node, and fault-recovery gates pass; and
8. C++ Store remains source-only oracle evidence; and
9. no C or C++ source/header file was modified.

Performance work remains gated until these criteria are satisfied.

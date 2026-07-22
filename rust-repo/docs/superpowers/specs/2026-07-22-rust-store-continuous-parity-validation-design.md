# Rust Store Continuous Parity and Validation Design

## Objective

Use the completed Task 10 audit as the baseline for continuously porting
applicable C++ Mooncake Store behavior into `rust-repo`, while keeping the
Rust Store independent of the C++ Store implementation. Validate every batch
against the Rust workspace and the native Transfer Engine/TENT libraries that
the Rust Store actually consumes.

## Dependency boundary

The supported native dependency graph is:

```text
Rust Store -> transfer-engine-ffi -> libtransfer_engine.so
                                  -> libtent_shared.so (when enabled)
Rust master -> spdk-io -> SPDK v26.01 (when spdk-nof-probe is enabled)
```

The C++ `mooncake-store` remains a behavioral reference, not a build or
runtime dependency. Missing Store behavior is implemented in `rust-repo`.
Only capabilities owned by the data-transfer layer may require changes to
Transfer Engine and its C ABI.

## SPDK baseline

The repository default SPDK version changes from v23.01.1 to v26.01. The
dependency installer must install the version consistently and preserve RDMA
support. Its installation must include libraries, headers, and pkg-config
metadata required by `spdk-io-sys`; copying static libraries alone is not a
complete installation.

SPDK v26.01 is selected because the Rust `spdk-io 0.1.0` dependency documents
and generates bindings against that generation. The acceptance gate is
`mooncake-store-master --features spdk-nof-probe`, not the legacy C++ Store
`USE_NOF` build.

## Incremental parity loop

For every C++ Store commit after the recorded audit boundary:

1. Record the commit exactly once in an audit matrix.
2. Classify it as Rust Store behavior, inherited Transfer Engine behavior,
   explicit FFI exposure, non-applicable C++ implementation detail, or an
   environment-gated verification item.
3. Port observable Store behavior idiomatically into `rust-repo`, starting
   with a failing Rust test.
4. Extend the Transfer Engine C ABI only when the behavior belongs to the
   native data plane and is not already reachable.
5. Run focused tests, then the complete validation matrix.
6. Write a dated migration/verification record containing commands, results,
   skipped prerequisites, and remaining gaps.

## Validation matrix

The required software-only matrix is:

- Rust formatting and all workspace tests.
- Python extension tests with the system Python library explicitly linked
  when Linux extension-module semantics require it.
- Rust optional features, including S3 and `spdk-nof-probe`.
- Clippy for master and client across all targets; warnings are recorded and
  deny-by-default errors must be fixed.
- CPU/TCP/HTTP/RDMA Transfer Engine and TENT shared-library builds.
- Transfer Engine and TENT C++ unit/integration tests that do not require
  unavailable hardware.
- Open RDMA mock driver tests.
- Repository diff, formatting, and pre-commit checks when available.

Real HugeTLB, RDMA device, accelerator, Kubernetes, external etcd/Redis/S3,
and NVMe-oF target tests run when their prerequisites exist. An unavailable
prerequisite is recorded precisely and never counted as a pass. Compilation,
mock, parser, and local loopback coverage must still run where possible.

## Failure handling

Failures are attributed to one of four categories:

- Rust Store regression: fix in `rust-repo` with a failing test first.
- Native Transfer Engine regression: fix and test inside Transfer Engine, then
  verify the Rust FFI path.
- Dependency incompatibility: align the supported dependency version or add a
  narrow compatibility layer; do not patch generated output.
- Environment gate: record the exact missing device, permission, kernel
  facility, or external service and continue all unaffected tests.

The full goal is complete only when every required software gate passes and
every hardware/service gate has either passed or has an evidence-backed skip.


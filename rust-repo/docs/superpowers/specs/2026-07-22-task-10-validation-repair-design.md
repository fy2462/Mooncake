# Task 10 Validation Repair Design

> Its SPDK 26.01 section is superseded by
> `2026-07-22-spdk-rs-nof-probe-design.md`.

## Objective

Repair every software failure found by the 2026-07-22 Task 10 validation run,
including the external Open-RDMA mock suite and the repository's
clang-format 20 gate. Re-run the full Rust Store to native Transfer
Engine/TENT validation matrix without introducing a C++ Store dependency.

## Scope and boundaries

The supported runtime boundary remains:

```text
Rust Store -> transfer-engine-ffi -> libtransfer_engine.so
                                  -> libtent_shared.so (optional)
Rust master -> spdk-io-sys -> SPDK v26.01 (optional)
```

Changes may touch `rust-repo`, `mooncake-common`,
`mooncake-transfer-engine`, repository formatting configuration, and the
external `/home/fy2462/workspace/PFS/open-rdma-driver` checkout. They must not
compile, link, or call the C++ Store. Existing unrelated worktree changes are
preserved and excluded from commits.

## Repair strategy

Each failure follows a red-green cycle: reproduce the focused failure, trace
its root cause, add or identify the narrow regression test, make one minimal
fix, and rerun the focused test before moving to the next subsystem.

- Apply rustfmt's current output to the two stale Engram assertions.
- Make the S3 environment fallback test safe under Rust edition 2024 by
  encapsulating process-environment mutation behind a test-only serialized
  helper with a documented safety invariant and restoration on drop.
- Correct the SPDK v26.01 static dependency closure so
  `libspdk_env_dpdk.a` is emitted in the linker group. Test the parser/link
  output rather than changing generated bindings.
- Make `default_config_test` locate fixtures from an explicit CMake-provided
  source path, independent of the build directory.
- Correct the TCP and transfer-metadata test fixture's empty metadata default.
  P2P tests must allocate valid local endpoints and clean them up; a missing
  external metadata service is not silently treated as success.
- Make the TENT invalid-host test use deterministic input that remains invalid
  under DNS interception, so it tests configuration failure instead of the
  host resolver's synthetic answer.
- In Open-RDMA, separate pure mock coverage from tests that require HugeTLB,
  physical-address visibility, privileged resources, or live worker peers.
  Pure mock tests must terminate and pass. Environment-dependent tests must
  detect and report the exact prerequisite before returning a skip result.

## clang-format 20

Install or expose an official clang-format 20 binary on this Ubuntu host. The
existing `scripts/code_format.sh` version check remains authoritative: it may
select `clang-format-20`, or a generic `clang-format` only when that binary
reports major version 20. No version check is weakened and no formatting hook
is bypassed.

## Validation

After focused red-green cycles, run:

- `cargo fmt --all -- --check` and the complete Rust workspace tests.
- S3, SPDK NoF probe, clippy, native-link FFI/client, and Python matrices.
- A clean Linux TE/TENT shared-library build and all 54 CTest targets.
- Open-RDMA pure mock/library tests, plus prerequisite-aware hardware tests.
- `git diff --check` and pre-commit on every touched Mooncake file using
  clang-format 20.

Hardware validation is not converted into a pass. Missing RDMA devices,
HugeTLB pages, NVMe devices, accelerators, services, or permissions are
reported as explicit gates, while all independent compilation and mock tests
must pass.

## Completion criteria

Task 10 can close only when every software-only gate above passes, no test
hangs, and each remaining hardware/service case has a precise, reproducible
skip reason. The final migration log records exact commands, counts, and any
environment gates.

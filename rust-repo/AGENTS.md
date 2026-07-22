# Rust Store Migration Instructions

All work under `rust-repo/` is part of replacing the C++ Mooncake Store with
an idiomatic Rust implementation. Read
[`docs/rust-store-migration-scope.md`](docs/rust-store-migration-scope.md)
before planning migration, parity, build, or validation work.

## Required dependency boundary

- Treat `rust-repo` as the replacement for the C++ `mooncake-store`, not as a
  wrapper around it.
- The expected native dependency is `mooncake-transfer-engine` through
  `transfer-engine-ffi`. TENT is part of that Transfer Engine boundary when
  enabled.
- Do not compile, link, or call the C++ `mooncake-store` to fill a Rust Store
  capability gap.
- If Store behavior is missing, implement it in `rust-repo` with idiomatic
  Rust and add Rust tests. Only extend the Transfer Engine C ABI/FFI when the
  missing capability genuinely belongs to the data-transfer layer.
- Use the C++ Store as a behavioral reference and compatibility oracle, not a
  runtime or build dependency.

## Validation boundary

- Build native Transfer Engine shared libraries required by enabled Rust FFI
  features (`libtransfer_engine.so`, and `libtent_shared.so` when applicable).
- Run Rust workspace/crate tests against those libraries.
- Validate Rust-owned optional integrations through their Rust features. For
  example, validate NoF probing with the Rust master `spdk-nof-probe` feature,
  not with the top-level CMake `USE_NOF=ON` C++ Store build.
- Hardware-gated tests may be skipped only with the exact missing device,
  kernel, permission, or service prerequisite recorded. Compilation and mock
  tests remain required when their dependencies are available.


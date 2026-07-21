# Rust 2024 Edition Upgrade Design

## Goal

Upgrade every crate in `rust-repo` from the workspace-inherited Rust 2021
Edition to Rust 2024 without changing runtime behavior or public APIs.

## Scope

- Change only the workspace edition and source required by the official
  edition migration lints.
- Keep the existing dependency graph and `resolver = "2"`; dependency upgrades
  are outside this change.
- Keep the minimum toolchain requirement implicit for now. Rust 2024 requires
  Rust 1.85 or newer, while the verification host uses Rust 1.96.1.
- Do not mix Store parity features, refactors, or formatting unrelated files
  into the edition commit.

## Migration approach

Run `cargo fix --edition --workspace --all-targets` while the workspace still
declares Edition 2021. Review every generated source change, then set
`workspace.package.edition = "2024"` in `rust-repo/Cargo.toml`. All seven
members inherit that value through `edition.workspace = true`.

The migration is successful only if formatting, workspace checks, and the
available test suites pass under Edition 2024. Hardware- or service-gated tests
remain documented rather than falsely reported as executed.

## Verification

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets`
- `CARGO_BUILD_JOBS=1 cargo test --workspace --no-fail-fast`
- PyO3 tests use the host's explicit `libpython3.14.so.1.0` link argument when
  the missing development symlink would otherwise prevent test linking.
- `git diff --check` and pre-commit on touched files when available.

## Rollback and compatibility

Edition selection is per crate and does not alter dependency compatibility.
If an edition lint requires a semantic choice that cannot be proven
behavior-preserving, stop and leave that crate on Edition 2021 rather than
guessing. The upgrade is one standalone commit and can be reverted atomically.

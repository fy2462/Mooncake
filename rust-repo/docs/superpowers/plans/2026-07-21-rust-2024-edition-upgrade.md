# Rust 2024 Edition Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade all `rust-repo` workspace crates to Rust 2024 with no intended behavior or API changes.

**Architecture:** Use Cargo's official edition lint migration across the existing workspace, review the generated compatibility edits, then change the single inherited workspace edition. Verify the full workspace and record any environment-gated checks.

**Tech Stack:** Rust 1.96.1, Cargo, Rustfmt, Rust 2024 Edition, PyO3.

## Global Constraints

- Rust 2024 has a minimum supported compiler of Rust 1.85.
- Do not update dependencies or change `resolver = "2"` in this batch.
- Do not stage the unrelated `progress.md`, `task_plan.md`, or `docs/superpowers/task_plan.md` workspace changes.
- Every source edit must be produced by edition lints or be the smallest manual correction required for Edition 2024.

---

### Task 1: Establish the Edition 2021 baseline

**Files:**
- Inspect: `rust-repo/Cargo.toml`
- Inspect: every `rust-repo/*/Cargo.toml` workspace member manifest

**Interfaces:**
- Consumes: the current seven-member workspace with `edition = "2021"`.
- Produces: recorded baseline commands and a clean scoped diff before migration.

- [ ] **Step 1: Confirm toolchain and inherited edition**

Run: `rustc --version && cargo --version && rg -n '^edition|^rust-version' Cargo.toml crates/*/Cargo.toml python/Cargo.toml`

Expected: Rust/Cargo 1.85 or newer; root edition is 2021 and members inherit it.

- [ ] **Step 2: Confirm the pre-existing workspace state**

Run: `git status --short && git diff --check`

Expected: only the documented unrelated user changes are present; no edition files are modified.

### Task 2: Generate and review compatibility fixes

**Files:**
- Modify: only Rust sources selected by Cargo's `rust-2024-compatibility` lints

**Interfaces:**
- Consumes: Edition 2021 sources.
- Produces: sources that compile with both Edition 2021 and Edition 2024 semantics.

- [ ] **Step 1: Run the official migration lints**

Run: `cargo fix --edition --workspace --all-targets --allow-dirty --allow-staged`

Expected: Cargo applies only `rust-2024-compatibility` suggestions; command exits 0.

- [ ] **Step 2: Review every generated edit**

Run: `git diff -- '*.rs'`

Expected: each edit directly corresponds to an Edition 2024 compatibility lint; no behavior feature or unrelated refactor appears.

- [ ] **Step 3: Check compatibility before flipping the edition**

Run: `cargo check --workspace --all-targets`

Expected: exit 0 while the manifest still declares Edition 2021.

### Task 3: Switch the workspace to Edition 2024

**Files:**
- Modify: `rust-repo/Cargo.toml`

**Interfaces:**
- Consumes: Edition-2024-compatible source from Task 2.
- Produces: `workspace.package.edition = "2024"`, inherited by all seven crates.

- [ ] **Step 1: Change the inherited edition**

Replace exactly:

```toml
edition = "2021"
```

with:

```toml
edition = "2024"
```

- [ ] **Step 2: Compile every target under Edition 2024**

Run: `cargo check --workspace --all-targets`

Expected: exit 0 with no Edition 2024 errors.

- [ ] **Step 3: Format and review the complete migration diff**

Run: `cargo fmt --all && cargo fmt --all -- --check && git diff --check && git diff --stat`

Expected: all commands exit 0 and the diff contains only the manifest plus lint-required Rust edits.

### Task 4: Verify and commit the workspace upgrade

**Files:**
- Modify: `rust-repo/Cargo.toml`
- Modify: lint-selected Rust sources, if any
- Create: next dated file under `rust-repo/change_logs/`

**Interfaces:**
- Consumes: the Edition 2024 workspace.
- Produces: one reviewable, revertible edition migration commit.

- [ ] **Step 1: Run the complete workspace test suite**

Run: `CARGO_BUILD_JOBS=1 cargo test --workspace --no-fail-fast`

Expected: all non-environment-gated tests pass. If PyO3 test linking fails only because the host lacks `libpython3.14.so`, rerun its package with `RUSTFLAGS='-C link-arg=/usr/lib/aarch64-linux-gnu/libpython3.14.so.1.0'` and record the environment limitation.

- [ ] **Step 2: Run pre-commit when available**

Run: `pre-commit run --files Cargo.toml $(git diff --name-only -- '*.rs') change_logs/2026-07-21-013.md`

Expected: all hooks pass. If `pre-commit` is unavailable, record that fact in the migration log.

- [ ] **Step 3: Write the migration log**

Record the old/new edition, Cargo-generated edits, exact verification commands and counts, environment-gated checks, and pre-commit status in `rust-repo/change_logs/2026-07-21-013.md`.

- [ ] **Step 4: Stage only the scoped migration and commit**

Run: `git diff --cached --check`

Commit: `build(rust): upgrade workspace to edition 2024`

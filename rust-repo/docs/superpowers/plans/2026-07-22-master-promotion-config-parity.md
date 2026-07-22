# Rust Master Promotion Configuration Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Rust Master promotion configuration and defaults match the C++ Master: promotion-on-hit is disabled by default, the admission threshold defaults to two hits, and the promotion queue limit defaults to 50,000.

**Architecture:** Expose the three existing C++ promotion controls through the Rust `clap` CLI and copy them into `MasterRuntimeConfig` instead of inheriting unrelated test-oriented defaults. Keep the existing promotion worker and admission logic unchanged; this change only fixes configuration plumbing and startup behavior.

**Tech Stack:** Rust 2024, clap derive, Tokio-based Mooncake Store Master, Cargo integration tests.

## Global Constraints

- Preserve the C++ flag names and defaults: `promotion_on_hit=false`, `promotion_admission_threshold=2`, and `promotion_queue_limit=50000`.
- Reject `promotion_admission_threshold=0` and `promotion_queue_limit=0` at CLI parsing time.
- Do not change `MasterRuntimeConfig::default()`, because unit tests use its compact test defaults directly.
- Do not change promotion scheduling, Count-Min Sketch admission, heartbeat behavior, or protobuf messages.
- Limit the implementation PR to Rust Master configuration and its tests.

---

### Task 1: Add a failing runtime configuration parity test

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_main_config.rs`

**Interfaces:**
- Consumes: `main_args::Args`, `main_config::build_runtime_config`
- Produces: regression coverage for `MasterRuntimeConfig::promotion_on_hit`, `promotion_admission_threshold`, and `promotion_queue_limit`

- [ ] **Step 1: Add the C++-default parity test**

Add this test alongside the existing `build_runtime_config` tests:

```rust
#[test]
fn test_promotion_config_matches_cpp_defaults_and_overrides() {
    let mut args = default_args();

    let config = build_runtime_config(&args).expect("default config should build");
    assert!(!config.promotion_on_hit);
    assert_eq!(config.promotion_admission_threshold, 2);
    assert_eq!(config.promotion_queue_limit, 50_000);

    args.promotion_on_hit = true;
    args.promotion_admission_threshold = 4;
    args.promotion_queue_limit = 123;

    let config = build_runtime_config(&args).expect("overridden config should build");
    assert!(config.promotion_on_hit);
    assert_eq!(config.promotion_admission_threshold, 4);
    assert_eq!(config.promotion_queue_limit, 123);
}
```

- [ ] **Step 2: Run the focused test and verify it fails to compile**

Run:

```bash
cd rust-repo
cargo test -p mooncake-store-master --test test_main_config test_promotion_config_matches_cpp_defaults_and_overrides
```

Expected: compilation fails because `Args` does not yet contain `promotion_on_hit`, `promotion_admission_threshold`, or `promotion_queue_limit`.

- [ ] **Step 3: Commit the failing test**

```bash
git add rust-repo/crates/mooncake-store-master/tests/test_main_config.rs
git commit -m "test(store-rust): cover promotion config parity"
```

### Task 2: Expose and wire the promotion controls

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/main_args.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/main_config.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_main_config.rs`

**Interfaces:**
- Consumes: C++ configuration contract in `mooncake-store/src/master.cpp`
- Produces: `Args::promotion_on_hit: bool`, `Args::promotion_admission_threshold: u8`, and `Args::promotion_queue_limit: usize`

- [ ] **Step 1: Add the three clap arguments after `offload_cap_ratio`**

Add to `Args`:

```rust
    /// Promote LOCAL_DISK-only keys to MEMORY after read admission.
    #[arg(long)]
    pub promotion_on_hit: bool,

    /// Minimum observed hit count before promotion is scheduled.
    #[arg(
        long,
        default_value_t = 2,
        value_parser = clap::value_parser!(u8).range(1..)
    )]
    pub promotion_admission_threshold: u8,

    /// Maximum number of in-flight promotion tasks.
    #[arg(
        long,
        default_value_t = 50_000,
        value_parser = clap::value_parser!(usize).range(1..)
    )]
    pub promotion_queue_limit: usize,
```

- [ ] **Step 2: Update the test fixture constructor**

In `default_args()`, initialize the new fields with the production defaults:

```rust
        promotion_on_hit: false,
        promotion_admission_threshold: 2,
        promotion_queue_limit: 50_000,
```

- [ ] **Step 3: Wire the arguments into `MasterRuntimeConfig`**

In the `Promotion / 热数据升温` section of `build_runtime_config`, add:

```rust
        promotion_on_hit: args.promotion_on_hit,
        promotion_admission_threshold: args.promotion_admission_threshold,
        promotion_queue_limit: args.promotion_queue_limit,
```

Keep the existing assignment of `promotion_max_per_heartbeat` immediately after these fields.

- [ ] **Step 4: Run the focused configuration tests**

Run:

```bash
cd rust-repo
cargo test -p mooncake-store-master --test test_main_config
```

Expected: all `test_main_config` tests pass, including the new default and override assertions.

- [ ] **Step 5: Run promotion behavior tests**

Run:

```bash
cd rust-repo
cargo test -p mooncake-store-master --test test_master_promotion
```

Expected: all promotion tests pass without changes to promotion logic.

- [ ] **Step 6: Commit the implementation**

```bash
git add rust-repo/crates/mooncake-store-master/src/main_args.rs rust-repo/crates/mooncake-store-master/src/main_config.rs rust-repo/crates/mooncake-store-master/tests/test_main_config.rs
git commit -m "fix(store-rust): align promotion configuration defaults"
```

### Task 3: Add CLI validation coverage and run the Rust verification gate

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_main_config.rs`

**Interfaces:**
- Consumes: `clap::Parser` implementation generated for `Args`
- Produces: startup validation ensuring zero-sized admission and queue values are rejected

- [ ] **Step 1: Add invalid-value CLI tests**

Import `clap::Parser` if it is not already in scope and add:

```rust
#[test]
fn test_promotion_cli_rejects_zero_threshold_and_queue_limit() {
    assert!(Args::try_parse_from([
        "mooncake-master",
        "--promotion-admission-threshold",
        "0",
    ])
    .is_err());

    assert!(Args::try_parse_from([
        "mooncake-master",
        "--promotion-queue-limit",
        "0",
    ])
    .is_err());
}
```

- [ ] **Step 2: Run formatting and focused tests**

Run:

```bash
cd rust-repo
cargo fmt --check
cargo test -p mooncake-store-master --test test_main_config
cargo test -p mooncake-store-master --test test_master_promotion
```

Expected: formatting succeeds and both test binaries pass.

- [ ] **Step 3: Run the workspace verification gate**

Run:

```bash
cd rust-repo
cargo check --workspace
cargo test --workspace
```

Expected: the workspace compiles without warnings and all non-environment-gated tests pass.

- [ ] **Step 4: Run pre-commit on the touched files**

Run from the repository root:

```bash
pre-commit run --files rust-repo/crates/mooncake-store-master/src/main_args.rs rust-repo/crates/mooncake-store-master/src/main_config.rs rust-repo/crates/mooncake-store-master/tests/test_main_config.rs
```

Expected: every available hook passes; if the toolchain is unavailable, record that fact in the handoff.

- [ ] **Step 5: Commit the validation coverage**

```bash
git add rust-repo/crates/mooncake-store-master/tests/test_main_config.rs
git commit -m "test(store-rust): validate promotion CLI limits"
```

## Follow-up Projects Kept Out of This Plan

These findings require separate designs and review gates:

- CXL allocation: Rust explicitly rejects the C++ `cxl` strategy and has no equivalent of `cxl_path`, `cxl_size`, or `enable_cxl`.
- NoF eviction: C++ exposes independent NoF eviction ratio and high-watermark controls; Rust currently exposes only memory eviction controls.
- RPC compatibility: C++ supports interface-based bind address resolution, connection timeout, and TCP_NODELAY controls that are absent from the Rust CLI.
- Metadata lifecycle: C++ exposes HTTP metadata enablement and timeout cleanup controls; Rust currently starts/configures metadata differently.
- Global file segment sizing: C++ exposes `global_file_segment_size`; Rust has no matching CLI field.
- Generic storage quota semantics: verify the intended client/backend contract before changing `quota_bytes`, because it is distinct from strict multi-tenant quota admission.

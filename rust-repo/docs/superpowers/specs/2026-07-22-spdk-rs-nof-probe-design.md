# SPDK-RS NoF Probe Migration Design

## Objective

Replace the locally patched `spdk-io` and `spdk-io-sys` crates with the
OpenEBS `spdk-rs` crate fetched directly by Cargo. Standardize Mooncake on one
native storage stack—SPDK 26.01 with DPDK 25.11.0—and remove the legacy Rust
SPDK wrapper source trees and duplicate native installations.

## Dependency model

The Rust dependency is pinned as a remote Cargo Git dependency:

```toml
spdk-rs = {
    git = "https://github.com/openebs/spdk-rs.git",
    rev = "77ef361d236ccac00ba0dd11a37c0b14b85ba730",
    optional = true,
}
```

Cargo owns its checkout under the normal global Cargo Git cache. Mooncake does
not vendor `spdk-rs`, add an `extern/spdk-rs` submodule, or patch its source.

The selected `spdk-rs` revision officially pins SPDK 25.05, but its build
generates bindings from `SPDK_ROOT_DIR` rather than shipping fixed bindings.
Mooncake validates that revision against its newer native stack and treats the
following three revisions as one tested dependency unit:

- `spdk-rs`: `77ef361d236ccac00ba0dd11a37c0b14b85ba730`;
- SPDK 26.01: `2ef883ef96e79c3cc16da02f667a7a58c2453f2f`;
- DPDK 25.11.0: `e01bfcd05fe39f628392c8b29b880f5692de2224`.

The dependency installer clones the exact SPDK revision with its pinned DPDK
submodule into a caller-selected shared workspace, builds it with the modules
required by `spdk-rs`, and installs one set of headers, static libraries, and
pkg-config metadata under `/usr/local`. Cargo builds set:

```text
SPDK_ROOT_DIR=$HOME/workspace/tmp/mooncake/spdk-26.01
```

The source directory remains available on the shared filesystem because
`spdk-rs` consumes SPDK internal and module headers that are not all installed
under `/usr/local`. Build output and temporary data also stay on the shared
filesystem; only the final single-version SDK is installed locally.

## Repository cleanup

Remove:

- the `extern/spdk` Git submodule and its `.gitmodules` entry after the shared
  SPDK 26.01 source checkout passes the feature suite;
- `rust-repo/third-party/spdk-io`;
- `rust-repo/third-party/spdk-io-sys`;
- the workspace `[patch.crates-io]` overrides for both crates;
- `spdk-io` and `spdk-io-sys` entries from `Cargo.lock` through normal Cargo
  dependency resolution.

The legacy C++ Store `USE_NOF` build and its scripts are outside the Rust Store
migration acceptance boundary. The default C++ Transfer Engine/TENT build does
not consume `extern/spdk`. No C++ Store library becomes a Rust dependency.

## Probe adapter

The public Mooncake interface remains the existing `spdk-nof-probe` feature
and `probe_nof_endpoint` behavior. Only its internal backend changes.

`spdk-rs` exposes DMA buffers, thread helpers, bdev abstractions, and raw SPDK
bindings through `spdk_rs::libspdk`, but it does not provide a drop-in version
of `spdk_io::nvme::NvmeController`. Add a focused Mooncake-owned adapter beside
`nof_probe.rs` that:

1. initializes the SPDK environment and thread library once;
2. translates the already parsed TCP or RDMA transport specification into
   `spdk_nvme_transport_id`;
3. creates or attaches the NVMe bdev/controller with the raw bindings exported
   by `spdk-rs`;
4. finds the requested namespace, allocates a `spdk_rs::DmaBuf`, and submits a
   one-block read;
5. polls completion until the existing deadline and maps errors to the stable
   Mooncake categories `open_fail`, `probe_buffer_alloc_fail`, `submit_fail`,
   `completion_error`, and `completion_timeout`;
6. detaches probe resources without shutting down process-global SPDK state
   used by later probes.

All raw pointer operations remain inside this adapter. Parsing, timeout policy,
command override behavior, and service decisions stay safe Rust in
`nof_probe.rs`.

## Installer behavior

`dependencies.sh --with-spdk` becomes idempotent and must not recursively
delete a repository path. It accepts `SPDK_ROOT_DIR`, defaulting to
`$HOME/workspace/tmp/mooncake/spdk-26.01`, validates the existing checkout,
fetches or checks out the required commit, initializes nested submodules,
configures SPDK with RDMA and io_uring support, and installs it. A different
checkout or local modification is reported before replacement rather than
silently erased.

Before installation, the script identifies existing SPDK/DPDK files under the
managed `/usr/local` prefixes. It replaces that set without mixing versions and
verifies SPDK 26.01 plus DPDK 25.11.0 afterward. It never deletes distro-owned
files under `/usr`. The installer and documentation print the exact
`SPDK_ROOT_DIR` required for Cargo builds. Normal builds without
`spdk-nof-probe` require neither SPDK nor the environment variable.

## Testing

The migration begins with compile-time tests that fail while `spdk-io` is
still the feature dependency. Parser and command-override tests remain backend
independent. Adapter tests cover transport conversion, namespace selection,
error mapping, deadline behavior, and cleanup using injectable raw operations;
they do not require hardware.

Acceptance requires:

- no tracked `extern/spdk` or `rust-repo/third-party/spdk-io*` paths;
- no `spdk-io` package in Cargo metadata or `Cargo.lock`;
- `cargo test -p mooncake-store-master --features spdk-nof-probe` compiling
  and passing against the pinned SPDK 26.01/DPDK 25.11.0 installation;
- unchanged tests without the feature;
- version assertions proving SPDK 26.01 and DPDK 25.11.0 are the only selected
  pkg-config stack;
- a real NVMe-oF read when a target is available, otherwise an exact target,
  device, HugeTLB, or privilege gate;
- the full Rust Store to Linux TE/TENT validation matrix remaining independent
  of the C++ Store.

# SPDK-RS NoF Probe Migration Design

## Objective

Replace the locally patched `spdk-io` and `spdk-io-sys` crates with the
OpenEBS `spdk-rs` crate fetched directly by Cargo. Standardize Mooncake on one
officially matched storage stack—spdk-rs v2.11.0 with OpenEBS SPDK 25.05 and
DPDK 25.03.0—and remove the legacy Rust
SPDK wrapper source trees and duplicate native installations.

## Dependency model

The Rust dependency is pinned as a remote Cargo Git dependency:

```toml
spdk-rs = {
    git = "https://github.com/openebs/spdk-rs.git",
    rev = "78d6018af041e80a42e222165b86070bae631821",
    optional = true,
}
```

Cargo owns its checkout under the normal global Cargo Git cache. Mooncake does
not vendor `spdk-rs`, add an `extern/spdk-rs` submodule, or patch its source.

The selected revision is the `v2.11.0` tag. Its Nix package pins the exact
OpenEBS SPDK revision below, whose DPDK submodule supplies the matching DPDK
revision. Mooncake treats all three revisions as one dependency unit:

- `spdk-rs` v2.11.0: `78d6018af041e80a42e222165b86070bae631821`;
- OpenEBS SPDK 25.05: `cc090cd2b64775545eb38022bb0ec8f37f4741a6`;
- DPDK 25.03.0: `cf36799c473a686fa14fde9af97f917a2125d3d5`.

The dependency installer clones the exact SPDK revision with its pinned DPDK
submodule into a caller-selected shared workspace and builds it with the
modules required by `spdk-rs`. The official helper first creates a staging SDK
under the shared workspace. A manifest-controlled deployment then installs the
same headers, static libraries, tools, and pkg-config metadata under
`/usr/local`. Cargo builds set:

```text
SPDK_ROOT_DIR=/usr/local
PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
```

The exact source checkout, build tree, staging SDK, Cargo target, and other
temporary data remain on the shared filesystem. `/usr/local` contains the one
selected runtime/development SDK. Its manifest lives at
`/usr/local/share/mooncake/spdk-25.05.manifest`; upgrades remove only paths
listed by the previous manifest before deploying the replacement. The
installer verifies that no older SPDK/DPDK pkg-config stack remains selected.

## Repository cleanup

Remove:

- the `extern/spdk` Git submodule and its `.gitmodules` entry after the shared
  OpenEBS SPDK 25.05 source checkout passes the feature suite;
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
delete a repository path. It accepts `SPDK_SOURCE_DIR`, defaulting to
`$HOME/workspace/tmp/mooncake/spdk-25.05`, validates the existing checkout,
fetches or checks out the required commit, initializes nested submodules,
checks out the matching `spdk-rs` helper, configures SPDK with RDMA and
io_uring support, stages the SDK in the shared workspace, and installs it to
`SPDK_INSTALL_PREFIX` (default `/usr/local`). A different
checkout or local modification is reported before replacement rather than
silently erased.

The installer removes only files recorded in its prior install manifest; it
never recursively deletes `/usr/local` or distro-owned files under `/usr`.
It verifies the exact source revisions before building and verifies installed
SPDK/DPDK versions after deployment. The installer and documentation print
`SPDK_ROOT_DIR=/usr/local`. Normal builds without `spdk-nof-probe` require
neither SPDK nor the environment variable.

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
  and passing against the pinned SPDK 25.05/DPDK 25.03.0 installation;
- unchanged tests without the feature;
- version assertions proving `/usr/local` exposes SPDK 25.05 and DPDK 25.03.0
  as the only selected pkg-config stack;
- a real NVMe-oF read when a target is available, otherwise an exact target,
  device, HugeTLB, or privilege gate;
- the full Rust Store to Linux TE/TENT validation matrix remaining independent
  of the C++ Store.

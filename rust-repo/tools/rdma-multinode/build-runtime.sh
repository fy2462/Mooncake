#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=lib/common.sh
source "$suite_dir/lib/common.sh"

if [[ -x /usr/local/go/bin/go ]]; then
    export PATH="/usr/local/go/bin:$PATH"
fi

build_root="$RDMA_ARTIFACT_ROOT/build/te"
cargo_target="$RDMA_ARTIFACT_ROOT/cargo-target"
runtime_root="$RDMA_ARTIFACT_ROOT/runtime"
te_src="$build_root/mooncake-transfer-engine"
native_paths="$te_src/src:$te_src/tent/src"
cmake_flags=(
    -DWITH_TE=ON
    -DWITH_STORE=OFF
    -DWITH_STORE_RUST=OFF
    -DBUILD_UNIT_TESTS=ON
    -DBUILD_SHARED_LIBS=ON
    -DUSE_ETCD=ON
    -DUSE_CUDA=OFF
    -DUSE_TENT=ON
)

mkdir -p "$build_root" "$cargo_target" "$runtime_root/te" "$runtime_root/store"

cmake -S "$MOONCAKE_ROOT" -B "$build_root" "${cmake_flags[@]}"
cmake --build "$build_root" --target rdma_transport_test transfer_engine tent_shared -j5

rustflags="-L native=$te_src/src -L native=$te_src/tent/src"
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR="$cargo_target" RUSTFLAGS="$rustflags" \
    cargo build --manifest-path "$MOONCAKE_ROOT/rust-repo/Cargo.toml" \
    -p mooncake-store-master
CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR="$cargo_target" RUSTFLAGS="$rustflags" \
    cargo build --manifest-path "$MOONCAKE_ROOT/rust-repo/python/Cargo.toml"

install -m 0755 "$te_src/tests/rdma_transport_test" "$runtime_root/te/rdma_transport_test"
install -m 0755 "$te_src/src/libtransfer_engine.so" "$runtime_root/te/libtransfer_engine.so"
install -m 0755 "$te_src/tent/src/libtent_shared.so" "$runtime_root/te/libtent_shared.so"
install -m 0755 "$build_root/mooncake-common/libasio.so" "$runtime_root/te/libasio.so"
install -m 0755 "$build_root/mooncake-common/etcd/libetcd_wrapper.so" \
    "$runtime_root/te/libetcd_wrapper.so"
install -m 0755 "$build_root/mooncake-common/src/libmooncake_common.so" \
    "$runtime_root/te/libmooncake_common.so"
install -m 0755 "$cargo_target/debug/mooncake-master" "$runtime_root/store/mooncake-master"
install -m 0755 "$cargo_target/debug/lib_mooncake_store.so" \
    "$runtime_root/store/_mooncake_store.abi3.so"

{
    printf 'git_sha\t%s\n' "$(git -C "$MOONCAKE_ROOT" rev-parse HEAD)"
    printf 'architecture\t%s\n' "$(uname -m)"
    printf 'cmake_flags\t%s\n' "${cmake_flags[*]}"
    printf 'cargo_features\tdefault-link-native\n'
    printf 'native_library_paths\t%s\n' "$native_paths"
} >"$runtime_root/runtime-manifest.tsv"

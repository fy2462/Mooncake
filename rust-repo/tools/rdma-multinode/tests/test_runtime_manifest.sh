#!/usr/bin/env bash
set -euo pipefail

artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
runtime_root="$artifact_root/runtime"
manifest="$runtime_root/runtime-manifest.tsv"

required_files=(
    te/rdma_transport_test
    te/libtransfer_engine.so
    te/libtent_shared.so
    te/libasio.so
    te/libetcd_wrapper.so
    te/libmooncake_common.so
    store/mooncake-master
    store/_mooncake_store.abi3.so
)

for relative_path in "${required_files[@]}"; do
    test -f "$runtime_root/$relative_path" || {
        printf 'missing runtime file: %s\n' "$relative_path" >&2
        exit 1
    }
done

test -f "$manifest"
for key in git_sha architecture cmake_flags cargo_features; do
    grep -Eq "^${key}[[:space:]]" "$manifest" || {
        printf 'missing runtime manifest key: %s\n' "$key" >&2
        exit 1
    }
done

if readelf -d "$runtime_root/store/_mooncake_store.abi3.so" | grep -q libmooncake_store; then
    printf 'Rust Store extension depends on forbidden libmooncake_store.so\n' >&2
    exit 1
fi

#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=host-rdma-manifest.sh
source "$script_dir/host-rdma-manifest.sh"

artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
env_manifest="$artifact_root/host-rdma.env"
json_manifest="$artifact_root/host-rdma.json"
if [[ ! -f $json_manifest ]]; then
    rm -f -- "$env_manifest"
    exit 0
fi
host_rdma_load_manifest "$json_manifest" || {
    printf 'refusing malformed host RDMA ownership manifest\n' >&2
    exit 1
}

for ((index=${#host_rdma_manifest_rows[@]}-1; index>=0; index--)); do
    IFS=$'\t' read -r _ device veth _ _ _ owned_rxe owned_veth \
        <<<"${host_rdma_manifest_rows[index]}"
    if [[ $owned_rxe == true ]] && sudo rdma link show "$device/1" >/dev/null 2>&1; then
        sudo rdma link delete "$device"
    fi
    if [[ $owned_veth == true ]] && sudo ip link show dev "$veth" >/dev/null 2>&1; then
        sudo ip link delete dev "$veth"
    fi
done
if [[ $host_rdma_manifest_owned_bridge == true ]] && \
   sudo ip link show dev "$host_rdma_manifest_bridge" >/dev/null 2>&1; then
    sudo ip link delete dev "$host_rdma_manifest_bridge"
fi
rm -f -- "$env_manifest" "$json_manifest"

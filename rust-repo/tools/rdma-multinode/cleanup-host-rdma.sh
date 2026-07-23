#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=host-rdma-manifest.sh
source "$script_dir/host-rdma-manifest.sh"

artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
env_manifest="$artifact_root/host-rdma.env"
json_manifest="$artifact_root/host-rdma.json"
device=mc-rdma-rxe
veth=mc-rdma-net-a

if [[ ! -f $json_manifest ]]; then
    rm -f -- "$env_manifest"
    exit 0
fi

if ! host_rdma_load_manifest "$json_manifest"; then
    printf 'refusing malformed host RDMA ownership manifest\n' >&2
    exit 1
fi

manifest_device=$host_rdma_manifest_device
manifest_veth=$host_rdma_manifest_veth
owned_rxe=$host_rdma_manifest_owned_rxe
owned_veth=$host_rdma_manifest_owned_veth

if [[ $owned_rxe == true && $manifest_device == "$device" ]]; then
    if sudo rdma link show "$device/1" >/dev/null 2>&1; then
        sudo rdma link delete "$device/1"
    fi
fi

if [[ $owned_veth == true && $manifest_veth == "$veth" ]]; then
    if sudo ip link show dev "$veth" >/dev/null 2>&1; then
        sudo ip link delete dev "$veth"
    fi
fi

rm -f -- "$env_manifest" "$json_manifest"

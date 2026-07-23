#!/usr/bin/env bash
set -Eeuo pipefail

artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
env_manifest="$artifact_root/host-rdma.env"
json_manifest="$artifact_root/host-rdma.json"
device=mc-rdma-rxe
veth=mc-rdma-net-a

if [[ ! -f $json_manifest ]]; then
    rm -f -- "$env_manifest"
    exit 0
fi

manifest_value() {
    sed -nE "s/.*\\\"$1\\\":\\\"([^\\\"]*)\\\".*/\\1/p" "$json_manifest"
}

manifest_flag() {
    sed -nE "s/.*\\\"$1\\\":(true|false).*/\\1/p" "$json_manifest"
}

manifest_device=$(manifest_value device)
manifest_veth=$(manifest_value veth)
owned_rxe=$(manifest_flag owned_rxe)
owned_veth=$(manifest_flag owned_veth)

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

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

mapfile -t manifest_lines <"$json_manifest"
manifest_pattern='^\{"device":"(mc-rdma-rxe)","veth":"(mc-rdma-net-a)","peer_veth":"(mc-rdma-net-b)","address":"(10\.90\.0\.1/30)","gid":"([[:xdigit:].:]+)","owned_rxe":(true|false),"owned_veth":(true|false)\}$'
if (( ${#manifest_lines[@]} != 1 )) || ! [[ ${manifest_lines[0]} =~ $manifest_pattern ]]; then
    printf 'refusing malformed host RDMA ownership manifest\n' >&2
    exit 1
fi

manifest_device=${BASH_REMATCH[1]}
manifest_veth=${BASH_REMATCH[2]}
owned_rxe=${BASH_REMATCH[6]}
owned_veth=${BASH_REMATCH[7]}

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

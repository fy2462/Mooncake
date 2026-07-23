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
peer_veth=mc-rdma-net-b
address=10.90.0.1/30

owned_name() {
    case "$1" in
        mc-rdma-rxe|mc-rdma-net-a|mc-rdma-net-b) return 0 ;;
        *) return 1 ;;
    esac
}

owned_name "$device" && owned_name "$veth" && owned_name "$peer_veth" || {
    printf 'refusing unowned host RDMA resource names\n' >&2
    exit 2
}

mkdir -p -- "$artifact_root"

previous_owned_rxe=false
previous_owned_veth=false
if [[ -f $json_manifest ]]; then
    host_rdma_load_manifest "$json_manifest" || {
        printf 'refusing malformed host RDMA ownership manifest\n' >&2
        exit 1
    }
    if [[ $host_rdma_manifest_device == "$device" && $host_rdma_manifest_owned_rxe == true ]]; then
        previous_owned_rxe=true
    fi
    if [[ $host_rdma_manifest_veth == "$veth" && $host_rdma_manifest_owned_veth == true ]]; then
        previous_owned_veth=true
    fi
fi

created_rxe=false
created_veth=false
completed=false
env_tmp=
json_tmp=
env_published=false
cleanup_incomplete_setup() {
    status=$?
    trap - EXIT
    [[ -z $env_tmp ]] || rm -f -- "$env_tmp" || true
    [[ -z $json_tmp ]] || rm -f -- "$json_tmp" || true
    if "$env_published" && ! "$completed"; then
        rm -f -- "$env_manifest" || true
    fi
    if ! "$completed"; then
        if "$created_rxe"; then
            sudo rdma link delete "$device/1" >/dev/null 2>&1 || true
        fi
        if "$created_veth"; then
            sudo ip link delete dev "$veth" >/dev/null 2>&1 || true
        fi
    fi
    exit "$status"
}
trap cleanup_incomplete_setup EXIT

sudo modprobe rdma_rxe

if sudo ip link show dev "$veth" >/dev/null 2>&1; then
    "$previous_owned_veth" || {
        printf 'refusing existing unowned veth: %s\n' "$veth" >&2
        exit 2
    }
    owned_veth=true
else
    sudo ip link add "$veth" type veth peer name "$peer_veth"
    created_veth=true
    owned_veth=true
fi

sudo ip addr replace "$address" dev "$veth"
sudo ip link set dev "$veth" up
sudo ip link set dev "$peer_veth" up

if sudo rdma link show "$device/1" >/dev/null 2>&1; then
    "$previous_owned_rxe" || {
        printf 'refusing existing unowned RXE device: %s\n' "$device" >&2
        exit 2
    }
    owned_rxe=true
else
    sudo rdma link add "$device" type rxe netdev "$veth"
    created_rxe=true
    owned_rxe=true
fi

link_info=$(sudo rdma link show "$device/1")
dev_info=$(sudo ibv_devinfo -d "$device")
grep -Eq 'state ACTIVE|state PORT_ACTIVE' <<<"$link_info" || {
    printf 'RXE link %s is not active\n' "$device" >&2
    exit 1
}
grep -q 'PORT_ACTIVE' <<<"$dev_info" || {
    printf 'RXE port %s is not active\n' "$device" >&2
    exit 1
}

gid=$(sed -nE 's/^[[:space:]]*GID\[[^]]+\]:[[:space:]]*([^[:space:]]+).*$/\1/p' <<<"$dev_info" | head -1)
gid=${gid%,}
[[ -n $gid && $gid != '(null)' ]] || {
    printf 'RXE GID is missing\n' >&2
    exit 1
}

env_tmp=$(mktemp "$artifact_root/.host-rdma.env.XXXXXX")
json_tmp=$(mktemp "$artifact_root/.host-rdma.json.XXXXXX")
printf 'device=%s\nveth=%s\npeer_veth=%s\naddress=%s\ngid=%s\nowned_rxe=%s\nowned_veth=%s\n' \
    "$device" "$veth" "$peer_veth" "$address" "$gid" "$owned_rxe" "$owned_veth" >"$env_tmp"
printf '{"device":"%s","veth":"%s","peer_veth":"%s","address":"%s","gid":"%s","owned_rxe":%s,"owned_veth":%s}\n' \
    "$device" "$veth" "$peer_veth" "$address" "$gid" "$owned_rxe" "$owned_veth" >"$json_tmp"
mv -f -- "$env_tmp" "$env_manifest"
env_published=true
mv -f -- "$json_tmp" "$json_manifest"
completed=true

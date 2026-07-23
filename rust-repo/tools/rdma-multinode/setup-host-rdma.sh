#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=host-rdma-manifest.sh
source "$script_dir/host-rdma-manifest.sh"

artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
env_manifest="$artifact_root/host-rdma.env"
json_manifest="$artifact_root/host-rdma.json"
names=(a b c)
devices=(mc-rdma-rxe-a mc-rdma-rxe-b mc-rdma-rxe-c)
veths=(mc-rdma-net-a mc-rdma-net-b mc-rdma-net-c)
peers=(mc-rdma-peer-a mc-rdma-peer-b mc-rdma-peer-c)
addresses=(10.90.1.1/30 10.90.2.1/30 10.90.3.1/30)

mkdir -p -- "$artifact_root"
declare -A previous_rxe=() previous_veth=()
if [[ -f $json_manifest ]]; then
    host_rdma_load_manifest "$json_manifest" || {
        printf 'refusing malformed host RDMA ownership manifest\n' >&2
        exit 1
    }
    for row in "${host_rdma_manifest_rows[@]}"; do
        IFS=$'\t' read -r name _ _ _ _ _ owned_rxe owned_veth <<<"$row"
        previous_rxe[$name]=$owned_rxe
        previous_veth[$name]=$owned_veth
    done
fi

created_rxe=() created_veth=()
completed=false
env_tmp= json_tmp=
cleanup_incomplete_setup() {
    local status=$?
    trap - EXIT
    [[ -z $env_tmp ]] || rm -f -- "$env_tmp" || true
    [[ -z $json_tmp ]] || rm -f -- "$json_tmp" || true
    if ! "$completed"; then
        rm -f -- "$env_manifest" "$json_manifest" || true
        for ((index=${#created_rxe[@]}-1; index>=0; index--)); do
            sudo rdma link delete "${created_rxe[index]}" >/dev/null 2>&1 || true
        done
        for ((index=${#created_veth[@]}-1; index>=0; index--)); do
            sudo ip link delete dev "${created_veth[index]}" >/dev/null 2>&1 || true
        done
    fi
    exit "$status"
}
trap cleanup_incomplete_setup EXIT

sudo modprobe rdma_rxe
owned_rxes=() owned_veths=() gids=()
for index in "${!names[@]}"; do
    name=${names[index]} device=${devices[index]} veth=${veths[index]}
    peer=${peers[index]} address=${addresses[index]}
    if sudo ip link show dev "$veth" >/dev/null 2>&1; then
        [[ ${previous_veth[$name]:-false} == true ]] || {
            printf 'refusing existing unowned veth: %s\n' "$veth" >&2
            exit 2
        }
    else
        sudo ip link add "$veth" type veth peer name "$peer"
        created_veth+=("$veth")
    fi
    owned_veths+=(true)
    sudo ip addr replace "$address" dev "$veth"
    sudo ip link set dev "$veth" up
    sudo ip link set dev "$peer" up

    if sudo rdma link show "$device/1" >/dev/null 2>&1; then
        [[ ${previous_rxe[$name]:-false} == true ]] || {
            printf 'refusing existing unowned RXE device: %s\n' "$device" >&2
            exit 2
        }
    else
        sudo rdma link add "$device" type rxe netdev "$veth"
        created_rxe+=("$device")
    fi
    owned_rxes+=(true)
    link_info=$(sudo rdma link show "$device/1")
    dev_info=$(sudo ibv_devinfo -v -d "$device")
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
        printf 'RXE GID is missing for %s\n' "$device" >&2
        exit 1
    }
    gids+=("$gid")
done

env_tmp=$(mktemp "$artifact_root/.host-rdma.env.XXXXXX")
json_tmp=$(mktemp "$artifact_root/.host-rdma.json.XXXXXX")
for index in "${!names[@]}"; do
    printf 'device_%s=%s\nveth_%s=%s\npeer_veth_%s=%s\naddress_%s=%s\ngid_%s=%s\nowned_rxe_%s=%s\nowned_veth_%s=%s\n' \
        "${names[index]}" "${devices[index]}" "${names[index]}" "${veths[index]}" \
        "${names[index]}" "${peers[index]}" "${names[index]}" "${addresses[index]}" \
        "${names[index]}" "${gids[index]}" "${names[index]}" "${owned_rxes[index]}" \
        "${names[index]}" "${owned_veths[index]}" >>"$env_tmp"
done
python3 - "$json_tmp" "${gids[@]}" <<'PY'
import json
import sys
nodes = []
for index, name in enumerate(("a", "b", "c")):
    nodes.append({
        "name": name,
        "device": f"mc-rdma-rxe-{name}",
        "veth": f"mc-rdma-net-{name}",
        "peer_veth": f"mc-rdma-peer-{name}",
        "address": f"10.90.{index + 1}.1/30",
        "gid": sys.argv[index + 2],
        "owned_rxe": True,
        "owned_veth": True,
    })
with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump({"nodes": nodes}, stream, separators=(",", ":"))
    stream.write("\n")
PY
mv -f -- "$env_tmp" "$env_manifest"
env_tmp=
mv -f -- "$json_tmp" "$json_manifest"
json_tmp=
completed=true

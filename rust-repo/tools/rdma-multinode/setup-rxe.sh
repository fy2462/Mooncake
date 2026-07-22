#!/usr/bin/env bash
set -Eeuo pipefail

device=${RXE_DEVICE:?RXE_DEVICE is required}
netdev=${RXE_NETDEV:-eth0}
output=${RXE_OUTPUT:?RXE_OUTPUT is required}

case "$device" in
    rxe-a|rxe-b|mc-rdma-rxe-a|mc-rdma-rxe-b) ;;
    *) printf 'refusing unowned RXE device name: %s\n' "$device" >&2; exit 2 ;;
esac

owned=true
if rdma link show "$device/1" >/dev/null 2>&1; then
    owned=false
else
    rdma link add "$device" type rxe netdev "$netdev"
fi

link_info=$(rdma link show "$device/1")
dev_info=$(ibv_devinfo -d "$device")
grep -Eq 'state ACTIVE|state PORT_ACTIVE' <<<"$link_info" || {
    printf 'RXE link %s is not active\n' "$device" >&2
    exit 1
}
grep -q 'PORT_ACTIVE' <<<"$dev_info" || {
    printf 'RXE port %s is not active\n' "$device" >&2
    exit 1
}

gid=$(sed -nE 's/^[[:space:]]*GID\[[^]]+\]:[[:space:]]*([^[:space:]]+).*$/\1/p' <<<"$dev_info" | head -1)
if [[ -z $gid ]]; then
    verbose_dev_info=$(ibv_devinfo -d "$device" -v)
    gid=$(sed -nE 's/^[[:space:]]*GID\[[^]]+\]:[[:space:]]*([^[:space:]]+).*$/\1/p' \
        <<<"$verbose_dev_info" | head -1)
fi
gid=${gid%,}
mtu=$(sed -nE 's/^[[:space:]]*active_mtu:[[:space:]]*([^[:space:]]+).*$/\1/p' <<<"$dev_info" | head -1)
container_ip=$(ip -o -4 addr show dev "$netdev" 2>/dev/null | awk '{split($4,a,"/"); print a[1]; exit}')
[[ -n $gid && $gid != '(null)' ]] || { printf 'RXE GID is missing\n' >&2; exit 1; }
[[ -n $container_ip ]] || { printf 'container IP is missing\n' >&2; exit 1; }

mkdir -p "$(dirname "$output")"
printf '{"device":"%s","netdev":"%s","container_ip":"%s","state":"ACTIVE","gid":"%s","mtu":"%s","owned":%s}\n' \
    "$device" "$netdev" "$container_ip" "$gid" "$mtu" "$owned" >"$output"

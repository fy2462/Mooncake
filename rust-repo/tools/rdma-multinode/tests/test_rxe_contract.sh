#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/bin"

cat >"$tmp_dir/bin/rdma" <<'EOF'
#!/usr/bin/env bash
printf 'rdma %s\n' "$*" >>"$FAKE_LOG"
if [[ $1 == link && $2 == add ]]; then
    : >"$FAKE_STATE"
fi
if [[ $1 == link && $2 == show ]]; then
    [[ ${FAKE_EXISTING:-0} == 1 || -f $FAKE_STATE ]] || exit 1
    printf 'link %s/1 state ACTIVE physical_state LINK_UP netdev eth0\n' "$3"
fi
EOF
cat >"$tmp_dir/bin/ibv_devinfo" <<'EOF'
#!/usr/bin/env bash
printf 'ibv_devinfo %s\n' "$*" >>"$FAKE_LOG"
cat <<OUT
hca_id: $2
    transport: InfiniBand (0)
    port: 1
        state: ${FAKE_PORT_STATE:-PORT_ACTIVE (4)}
        active_mtu: 1024 (3)
        GID[  0]: ${FAKE_GID-fe80::1}
OUT
EOF
cat >"$tmp_dir/bin/ip" <<'EOF'
#!/usr/bin/env bash
printf '2: eth0 inet 10.89.10.21/24 scope global eth0\n'
EOF
chmod +x "$tmp_dir/bin/rdma" "$tmp_dir/bin/ibv_devinfo" "$tmp_dir/bin/ip"

export PATH="$tmp_dir/bin:$PATH" FAKE_LOG="$tmp_dir/commands.log" FAKE_STATE="$tmp_dir/device"
export RXE_DEVICE=rxe-a RXE_NETDEV=eth0 RXE_OUTPUT="$tmp_dir/rxe-a.json"
bash "$suite_dir/setup-rxe.sh"
grep -Fx 'rdma link add rxe-a type rxe netdev eth0' "$FAKE_LOG"
grep -Fx 'rdma link show rxe-a/1' "$FAKE_LOG"
grep -Fx 'ibv_devinfo -d rxe-a' "$FAKE_LOG"
grep -q '"state":"ACTIVE"' "$RXE_OUTPUT"
grep -q '"gid":"fe80::1"' "$RXE_OUTPUT"

: >"$FAKE_LOG"
FAKE_EXISTING=1 bash "$suite_dir/setup-rxe.sh"
! grep -q 'link add' "$FAKE_LOG"

if RXE_DEVICE=not-owned bash "$suite_dir/setup-rxe.sh" >/dev/null 2>&1; then
    printf 'accepted an unowned RXE name\n' >&2
    exit 1
fi
if FAKE_PORT_STATE=PORT_DOWN bash "$suite_dir/setup-rxe.sh" >/dev/null 2>&1; then
    printf 'accepted an inactive RXE port\n' >&2
    exit 1
fi
if FAKE_GID= bash "$suite_dir/setup-rxe.sh" >/dev/null 2>&1; then
    printf 'accepted a missing RXE GID\n' >&2
    exit 1
fi

#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
tmp_dir=$(mktemp -d)
trap 'rm -rf -- "$tmp_dir"' EXIT
fake_bin="$tmp_dir/bin"
artifact_root="$tmp_dir/artifacts"
state_dir="$tmp_dir/state"
mkdir -p "$fake_bin" "$artifact_root" "$state_dir"

cat >"$fake_bin/sudo" <<'EOF'
#!/usr/bin/env bash
printf 'sudo %s\n' "$*" >>"$FAKE_LOG"
exec "$@"
EOF

cat >"$fake_bin/modprobe" <<'EOF'
#!/usr/bin/env bash
printf 'modprobe %s\n' "$*" >>"$FAKE_LOG"
EOF

cat >"$fake_bin/ip" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
printf 'ip %s\n' "$*" >>"$FAKE_LOG"

case "$*" in
    'link show dev mc-rdma-net-a')
        test -f "$FAKE_STATE/veth"
        ;;
    'link add mc-rdma-net-a type veth peer name mc-rdma-net-b')
        : >"$FAKE_STATE/veth"
        ;;
    'addr replace 10.90.0.1/30 dev mc-rdma-net-a')
        test -f "$FAKE_STATE/veth"
        ;;
    'link set dev mc-rdma-net-a up'|'link set dev mc-rdma-net-b up')
        test -f "$FAKE_STATE/veth"
        ;;
    'link delete dev mc-rdma-net-a')
        rm -f -- "$FAKE_STATE/veth"
        ;;
    *)
        printf 'unexpected ip invocation: %s\n' "$*" >&2
        exit 1
        ;;
esac
EOF

cat >"$fake_bin/rdma" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
printf 'rdma %s\n' "$*" >>"$FAKE_LOG"

case "$*" in
    'link show mc-rdma-rxe/1')
        test -f "$FAKE_STATE/rxe"
        printf 'link mc-rdma-rxe/1 state ACTIVE physical_state LINK_UP netdev mc-rdma-net-a\n'
        ;;
    'link add mc-rdma-rxe type rxe netdev mc-rdma-net-a')
        test -f "$FAKE_STATE/veth"
        : >"$FAKE_STATE/rxe"
        ;;
    'link delete mc-rdma-rxe/1')
        rm -f -- "$FAKE_STATE/rxe"
        ;;
    *)
        printf 'unexpected rdma invocation: %s\n' "$*" >&2
        exit 1
        ;;
esac
EOF

cat >"$fake_bin/ibv_devinfo" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
printf 'ibv_devinfo %s\n' "$*" >>"$FAKE_LOG"
test "$*" = '-d mc-rdma-rxe'
cat <<OUT
hca_id: mc-rdma-rxe
    transport: InfiniBand (0)
    port: 1
        state: ${FAKE_PORT_STATE:-PORT_ACTIVE (4)}
        active_mtu: 1024 (3)
        GID[  0]: ${FAKE_GID-fe80::90}
OUT
EOF

chmod +x "$fake_bin"/*
export PATH="$fake_bin:$PATH" FAKE_LOG="$tmp_dir/commands.log" FAKE_STATE="$state_dir"
export RDMA_ARTIFACT_ROOT="$artifact_root"

bash "$suite_dir/setup-host-rdma.sh"

grep -Fx 'sudo modprobe rdma_rxe' "$FAKE_LOG"
grep -Fx 'sudo ip link add mc-rdma-net-a type veth peer name mc-rdma-net-b' "$FAKE_LOG"
grep -Fx 'sudo ip addr replace 10.90.0.1/30 dev mc-rdma-net-a' "$FAKE_LOG"
grep -Fx 'sudo ip link set dev mc-rdma-net-a up' "$FAKE_LOG"
grep -Fx 'sudo ip link set dev mc-rdma-net-b up' "$FAKE_LOG"
grep -Fx 'sudo rdma link add mc-rdma-rxe type rxe netdev mc-rdma-net-a' "$FAKE_LOG"
grep -Fx 'sudo rdma link show mc-rdma-rxe/1' "$FAKE_LOG"
grep -Fx 'sudo ibv_devinfo -d mc-rdma-rxe' "$FAKE_LOG"
grep -Fx 'owned_rxe=true' "$artifact_root/host-rdma.env"
grep -Fx 'owned_veth=true' "$artifact_root/host-rdma.env"
grep -Fx 'device=mc-rdma-rxe' "$artifact_root/host-rdma.env"
grep -Fx 'veth=mc-rdma-net-a' "$artifact_root/host-rdma.env"
grep -Fx 'address=10.90.0.1/30' "$artifact_root/host-rdma.env"
grep -F '"owned_rxe":true' "$artifact_root/host-rdma.json"
grep -F '"owned_veth":true' "$artifact_root/host-rdma.json"
grep -F '"gid":"fe80::90"' "$artifact_root/host-rdma.json"

: >"$FAKE_LOG"
bash "$suite_dir/setup-host-rdma.sh"
! grep -Fq 'link add mc-rdma-rxe' "$FAKE_LOG"
! grep -Fq 'link add mc-rdma-net-a' "$FAKE_LOG"

: >"$FAKE_LOG"
bash "$suite_dir/cleanup-host-rdma.sh"
grep -Fx 'sudo rdma link delete mc-rdma-rxe/1' "$FAKE_LOG"
grep -Fx 'sudo ip link delete dev mc-rdma-net-a' "$FAKE_LOG"
! grep -Fq 'mc-rdma-net-b' "$FAKE_LOG"
test ! -e "$artifact_root/host-rdma.env"
test ! -e "$artifact_root/host-rdma.json"

: >"$FAKE_LOG"
bash "$suite_dir/cleanup-host-rdma.sh"
test ! -s "$FAKE_LOG"

printf 'device=mc-rdma-rxe\nveth=mc-rdma-net-a\nowned_rxe=true\nowned_veth=true\n' \
    >"$artifact_root/host-rdma.env"
printf '{"device":"mc-rdma-rxe","veth":"mc-rdma-net-a","owned_rxe":false,"owned_veth":false}\n' \
    >"$artifact_root/host-rdma.json"
: >"$state_dir/rxe"
: >"$state_dir/veth"
: >"$FAKE_LOG"
bash "$suite_dir/cleanup-host-rdma.sh"
! grep -Fq ' link delete ' "$FAKE_LOG"
test -e "$state_dir/rxe"
test -e "$state_dir/veth"

rm -rf -- "$artifact_root" "$state_dir"
mkdir -p "$artifact_root" "$state_dir"
if FAKE_GID= bash "$suite_dir/setup-host-rdma.sh" >/dev/null 2>&1; then
    printf 'accepted an empty RXE GID\n' >&2
    exit 1
fi
test ! -e "$artifact_root/host-rdma.env"
test ! -e "$artifact_root/host-rdma.json"
test ! -e "$state_dir/rxe"
test ! -e "$state_dir/veth"

if rg -n -- 'sudo([[:space:]]+[^[:space:]]+)*[[:space:]]+(-S|--stdin)([[:space:]]|$)|SUDO_ASKPASS' \
    "$suite_dir/setup-host-rdma.sh" "$suite_dir/cleanup-host-rdma.sh"; then
    printf 'repository scripts must not accept sudo password input\n' >&2
    exit 1
fi

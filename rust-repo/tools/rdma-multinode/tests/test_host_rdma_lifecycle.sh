#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
test_artifact_root=${RDMA_ARTIFACT_ROOT:-/home/fy2462/workspace/tmp/mooncake/rdma-multinode}
mkdir -p -- "$test_artifact_root"
tmp_dir=$(mktemp -d "$test_artifact_root/test-host-rdma-lifecycle.XXXXXX")
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
    'link show dev mc-rdma-net-'?) test -f "$FAKE_STATE/veth-${4##*-}" ;;
    'link add mc-rdma-net-'?' type veth peer name mc-rdma-peer-'?) : >"$FAKE_STATE/veth-${3##*-}" ;;
    'addr replace 10.90.'?'.1/30 dev mc-rdma-net-'?) test -f "$FAKE_STATE/veth-${5##*-}" ;;
    'link set dev mc-rdma-net-'?' up') test -f "$FAKE_STATE/veth-${4##*-}" ;;
    'link set dev mc-rdma-peer-'?' up') test -f "$FAKE_STATE/veth-${4##*-}" ;;
    'link delete dev mc-rdma-net-'?) rm -f -- "$FAKE_STATE/veth-${4##*-}" ;;
    *) printf 'unexpected ip invocation: %s\n' "$*" >&2; exit 1 ;;
esac
EOF
cat >"$fake_bin/rdma" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
printf 'rdma %s\n' "$*" >>"$FAKE_LOG"
case "$1 $2" in
    'link show')
        device=${3%/1}; test -f "$FAKE_STATE/rxe-${device##*-}"
        printf 'link %s/1 state ACTIVE physical_state LINK_UP\n' "$device"
        ;;
    'link add') device=$3; : >"$FAKE_STATE/rxe-${device##*-}" ;;
    'link delete') rm -f -- "$FAKE_STATE/rxe-${3##*-}" ;;
    *) exit 1 ;;
esac
EOF
cat >"$fake_bin/ibv_devinfo" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
printf 'ibv_devinfo %s\n' "$*" >>"$FAKE_LOG"
test "$1 $2" = '-v -d'
device=$3
cat <<OUT
hca_id: $device
    port: 1
        state: ${FAKE_PORT_STATE:-PORT_ACTIVE (4)}
        GID[  0]: ${FAKE_GID-fe80::${device##*-}}
OUT
EOF
cat >"$fake_bin/mv" <<'EOF'
#!/usr/bin/env bash
destination=${!#}
if [[ ${FAKE_MV_FAIL_JSON:-0} == 1 && $destination == "$RDMA_ARTIFACT_ROOT/host-rdma.json" ]]; then exit 1; fi
exec /bin/mv "$@"
EOF
chmod +x "$fake_bin"/*
export PATH="$fake_bin:$PATH" FAKE_LOG="$tmp_dir/commands.log" FAKE_STATE="$state_dir"
export RDMA_ARTIFACT_ROOT="$artifact_root"

bash "$suite_dir/setup-host-rdma.sh"
for name in a b c; do
    grep -Fx "sudo ip link add mc-rdma-net-$name type veth peer name mc-rdma-peer-$name" "$FAKE_LOG"
    grep -Fx "sudo rdma link add mc-rdma-rxe-$name type rxe netdev mc-rdma-net-$name" "$FAKE_LOG"
    grep -Fx "sudo ibv_devinfo -v -d mc-rdma-rxe-$name" "$FAKE_LOG"
    grep -Fx "device_$name=mc-rdma-rxe-$name" "$artifact_root/host-rdma.env"
done
python3 - "$artifact_root/host-rdma.json" <<'PY'
import json, sys
data=json.load(open(sys.argv[1], encoding="utf-8"))
assert [node["name"] for node in data["nodes"]] == ["a", "b", "c"]
assert all(node["owned_rxe"] and node["owned_veth"] for node in data["nodes"])
PY

: >"$FAKE_LOG"; bash "$suite_dir/setup-host-rdma.sh"; ! grep -Fq ' link add ' "$FAKE_LOG"
: >"$FAKE_LOG"; bash "$suite_dir/cleanup-host-rdma.sh"
for name in c b a; do
    grep -Fx "sudo rdma link delete mc-rdma-rxe-$name" "$FAKE_LOG"
    grep -Fx "sudo ip link delete dev mc-rdma-net-$name" "$FAKE_LOG"
done
test ! -e "$artifact_root/host-rdma.json"; test ! -e "$artifact_root/host-rdma.env"
: >"$FAKE_LOG"; bash "$suite_dir/cleanup-host-rdma.sh"; test ! -s "$FAKE_LOG"

rm -rf -- "$artifact_root" "$state_dir"; mkdir -p "$artifact_root" "$state_dir"
if FAKE_MV_FAIL_JSON=1 bash "$suite_dir/setup-host-rdma.sh" >/dev/null 2>&1; then exit 1; fi
test ! -e "$artifact_root/host-rdma.json"; test -z "$(find "$state_dir" -type f -print -quit)"
if FAKE_PORT_STATE=PORT_DOWN bash "$suite_dir/setup-host-rdma.sh" >/dev/null 2>&1; then exit 1; fi
test -z "$(find "$state_dir" -type f -print -quit)"
if FAKE_GID= bash "$suite_dir/setup-host-rdma.sh" >/dev/null 2>&1; then exit 1; fi
test -z "$(find "$state_dir" -type f -print -quit)"

python3 - "$artifact_root/host-rdma.json" <<'PY'
import json, sys
nodes=[]
for index, name in enumerate("abc", 1):
    nodes.append({"name":name,"device":f"mc-rdma-rxe-{name}","veth":f"mc-rdma-net-{name}",
      "peer_veth":f"mc-rdma-peer-{name}","address":f"10.90.{index}.1/30","gid":f"fe80::{name}",
      "owned_rxe":False,"owned_veth":False})
json.dump({"nodes":nodes}, open(sys.argv[1], "w", encoding="utf-8"), separators=(",", ":"))
PY
for name in a b c; do : >"$state_dir/rxe-$name"; : >"$state_dir/veth-$name"; done
: >"$FAKE_LOG"; bash "$suite_dir/cleanup-host-rdma.sh"
! grep -Fq ' link delete ' "$FAKE_LOG"; test "$(find "$state_dir" -type f | wc -l)" -eq 6

printf '{"nodes":[],"extra":true}\n' >"$artifact_root/host-rdma.json"
: >"$FAKE_LOG"
if bash "$suite_dir/cleanup-host-rdma.sh" >/dev/null 2>&1; then exit 1; fi
! grep -Fq ' link delete ' "$FAKE_LOG"

if rg -n -- 'sudo([[:space:]]+[^[:space:]]+)*[[:space:]]+(-S|--stdin)([[:space:]]|$)|SUDO_ASKPASS' \
    "$suite_dir/setup-host-rdma.sh" "$suite_dir/cleanup-host-rdma.sh"; then
    printf 'repository scripts must not accept sudo password input\n' >&2
    exit 1
fi

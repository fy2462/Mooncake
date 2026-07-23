#!/usr/bin/env bash
set -Eeuo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

docker compose -f "$suite_dir/compose.yaml" config --format json >"$rendered"

expected_services=(
    etcd
    te-node-a
    te-node-b
    te-node-c
    rust-master
    store-node-a
    store-node-b
    store-node-c
    store-test-client
)
data_plane_services=(
    te-node-a
    te-node-b
    te-node-c
    rust-master
    store-node-a
    store-node-b
    store-node-c
    store-test-client
)

test "$(jq '.services | length' "$rendered")" -eq "${#expected_services[@]}"
for service in "${expected_services[@]}"; do
    jq -e --arg service "$service" '.services[$service] != null' "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '.services[$service].container_name == "mc-rdma-" + $service' "$rendered" >/dev/null
done
for service in "${data_plane_services[@]}"; do
    jq -e --arg service "$service" \
        '.services[$service].network_mode == "host"' "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '[.services[$service].volumes[]?] | any(
            .source == "/dev/infiniband" and .target == "/dev/infiniband"
        )' \
        "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '[.services[$service].volumes[]?] | any(.target == "/artifacts")' \
        "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '[.services[$service].volumes[]?] | any(
            .target == "/opt/mooncake-rdma" and .read_only == true
        )' \
        "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '.services[$service].environment.PYTHONPATH == "/opt/mooncake-rdma/store"' \
        "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '.services[$service].environment.LD_LIBRARY_PATH == "/opt/mooncake-rdma/te"' \
        "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '.services[$service].environment.MC_GID_INDEX == "0"' \
        "$rendered" >/dev/null
done

jq -e '
    [.services.etcd.volumes[]?] | all(.target != "/artifacts")
' "$rendered" >/dev/null
jq -e '
    .services.etcd.command | index("--data-dir=/tmp/etcd-data") != null
' "$rendered" >/dev/null
jq -e '
    [.. | strings | select(test("10\\.89\\.10\\.|mc-rdma-net"))] | length == 0
' "$rendered" >/dev/null

jq -e '
    .services.etcd.command | index("--advertise-client-urls=http://127.0.0.1:2379") != null
' "$rendered" >/dev/null
jq -e '
    .services.etcd.healthcheck.test == [
        "CMD",
        "etcdctl",
        "endpoint",
        "health",
        "--endpoints=http://127.0.0.1:2379"
    ]
' "$rendered" >/dev/null

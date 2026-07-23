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
    rust-master
    store-node-a
    store-node-b
    store-test-client
)
data_plane_services=(
    te-node-a
    te-node-b
    rust-master
    store-node-a
    store-node-b
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
        '[.services[$service].volumes[]? | tostring] | any(contains("/dev/infiniband:/dev/infiniband"))' \
        "$rendered" >/dev/null
    jq -e --arg service "$service" \
        '[.services[$service].volumes[]? | tostring] | any(contains(":/artifacts"))' \
        "$rendered" >/dev/null
done

! grep -Fq '10.89.10.' "$suite_dir/compose.yaml"
! grep -Fq 'mc-rdma-net' "$suite_dir/compose.yaml"

jq -e '
    .services.etcd.command | index("--advertise-client-urls=http://127.0.0.1:2379") != null
' "$rendered" >/dev/null

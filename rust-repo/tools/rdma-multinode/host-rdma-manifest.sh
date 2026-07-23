#!/usr/bin/env bash

host_rdma_load_manifest() {
    local manifest_path=$1 output
    output=$(
        python3 - "$manifest_path" <<'PY'
import json
import sys

expected = {
    "a": ("mc-rdma-rxe-a", "mc-rdma-net-a", "mc-rdma-peer-a", "10.90.0.1/24"),
    "b": ("mc-rdma-rxe-b", "mc-rdma-net-b", "mc-rdma-peer-b", "10.90.0.2/24"),
    "c": ("mc-rdma-rxe-c", "mc-rdma-net-c", "mc-rdma-peer-c", "10.90.0.3/24"),
}
try:
    with open(sys.argv[1], encoding="utf-8") as stream:
        value = json.load(stream)
    if set(value) != {"bridge", "owned_bridge", "nodes"}:
        raise ValueError
    if value["bridge"] != "mc-rdma-br" or not isinstance(value["owned_bridge"], bool):
        raise ValueError
    if not isinstance(value["nodes"], list):
        raise ValueError
    nodes = value["nodes"]
    if len(nodes) != 3:
        raise ValueError
    seen = set()
    for node in nodes:
        if set(node) != {
            "name", "device", "veth", "peer_veth", "address", "gid",
            "owned_rxe", "owned_veth",
        }:
            raise ValueError
        name = node["name"]
        if name not in expected or name in seen:
            raise ValueError
        if tuple(node[key] for key in ("device", "veth", "peer_veth", "address")) != expected[name]:
            raise ValueError
        gid = node["gid"]
        if not isinstance(gid, str) or not gid or any(ch not in "0123456789abcdefABCDEF.:" for ch in gid):
            raise ValueError
        if not isinstance(node["owned_rxe"], bool) or not isinstance(node["owned_veth"], bool):
            raise ValueError
        seen.add(name)
        print("\t".join((
            name, node["device"], node["veth"], node["peer_veth"],
            node["address"], gid, str(node["owned_rxe"]).lower(),
            str(node["owned_veth"]).lower(),
        )))
except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
    raise SystemExit(1)
PY
    ) || return 1
    host_rdma_manifest_bridge=mc-rdma-br
    host_rdma_manifest_owned_bridge=$(python3 - "$manifest_path" <<'PY'
import json, sys
print(str(json.load(open(sys.argv[1], encoding="utf-8"))["owned_bridge"]).lower())
PY
    ) || return 1
    mapfile -t host_rdma_manifest_rows <<<"$output"
    (( ${#host_rdma_manifest_rows[@]} == 3 ))
}

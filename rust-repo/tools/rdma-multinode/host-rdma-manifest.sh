#!/usr/bin/env bash

host_rdma_load_manifest() {
    local manifest_path=$1
    local -a manifest_lines
    local manifest_pattern='^\{"device":"(mc-rdma-rxe)","veth":"(mc-rdma-net-a)","peer_veth":"(mc-rdma-net-b)","address":"(10\.90\.0\.1/30)","gid":"([[:xdigit:].:]+)","owned_rxe":(true|false),"owned_veth":(true|false)\}$'

    mapfile -t manifest_lines <"$manifest_path" || return 1
    if (( ${#manifest_lines[@]} != 1 )) || ! [[ ${manifest_lines[0]} =~ $manifest_pattern ]]; then
        return 1
    fi

    host_rdma_manifest_device=${BASH_REMATCH[1]}
    host_rdma_manifest_veth=${BASH_REMATCH[2]}
    host_rdma_manifest_owned_rxe=${BASH_REMATCH[6]}
    host_rdma_manifest_owned_veth=${BASH_REMATCH[7]}
}

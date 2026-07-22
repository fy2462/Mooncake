#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import inspect
import json


def payload(size: int) -> bytes:
    return bytes((index * 31 + 7) & 0xFF for index in range(size))


async def _replicas(client, key):
    value = client.get_replica_desc(key)
    return await value if inspect.isawaitable(value) else value


async def run_scenario(client, config=None, evidence=None) -> int:
    evidence = evidence if evidence is not None else {}
    try:
        cases = (("rdma-4k", 4096), ("rdma-12m", 12 * 1024 * 1024))
        for key, size in cases:
            expected = payload(size)
            await client.put(key, expected, config)
            replicas = await _replicas(client, key)
            remote = [
                replica
                for replica in replicas
                if replica.get("protocol") == "rdma"
                and replica.get("status") == "Complete"
                and replica.get("handle_valid") is True
            ]
            if len({replica.get("segment_name") for replica in remote}) < 2:
                return 2
            actual = await client.get(key)
            if len(actual) != size or hashlib.sha256(actual).digest() != hashlib.sha256(expected).digest():
                return 3
            for _ in range(3):
                if await client.get(key) != expected:
                    return 4
            evidence[key] = {
                "sha256": hashlib.sha256(expected).hexdigest(),
                "remote_segments": sorted(replica["segment_name"] for replica in remote),
                "repeat_reads": 3,
            }
        if not await client.exists("rdma-4k"):
            return 5
        await client.remove("rdma-4k", force=True)
        if await client.exists("rdma-4k"):
            return 6
        return 0
    finally:
        client.close()


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--master", required=True)
    parser.add_argument("--result", required=True)
    args = parser.parse_args()
    import _mooncake_store as store

    client = await store.MooncakeClient.create(
        local_hostname=args.node,
        metadata_server=args.metadata,
        master_server_addr=args.master,
        protocol="rdma",
        device=args.device,
        global_segment_size=0,
        local_buffer_size=32 * 1024 * 1024,
    )
    evidence = {}
    config = store.ReplicateConfig(replica_num=2)
    rc = await run_scenario(client, config, evidence)
    with open(args.result, "w", encoding="utf-8") as stream:
        json.dump({"status": "PASS" if rc == 0 else "FAIL", "rc": rc, "evidence": evidence}, stream)
        stream.write("\n")
    return rc


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

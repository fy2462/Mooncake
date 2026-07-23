#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import inspect
import json
import os
import time


class ScenarioFailure(RuntimeError):
    pass


def payload(size: int, seed: int = 7) -> bytes:
    return bytes((index * 31 + seed) & 0xFF for index in range(size))


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


async def _replicas(client, key):
    value = client.get_replica_desc(key)
    return await value if inspect.isawaitable(value) else value


def _complete(replicas, replica_type):
    return [
        replica
        for replica in replicas
        if replica.get("replica_type", "Memory") == replica_type
        and replica.get("status") == "Complete"
        and replica.get("handle_valid") is True
    ]


def _two_rdma_replicas(replicas):
    remote = [
        replica
        for replica in _complete(replicas, "Memory")
        if replica.get("protocol") == "rdma"
    ]
    segments = {replica.get("segment_name") for replica in remote}
    if len(segments) < 2:
        raise ScenarioFailure("two complete RDMA replicas were not observed")
    return sorted(segments)


async def wait_for_replica_state(
    client,
    key,
    predicate,
    description,
    timeout=30.0,
    poll_interval=0.1,
):
    deadline = time.monotonic() + timeout
    last = []
    while time.monotonic() <= deadline:
        last = await _replicas(client, key)
        if predicate(last):
            return last
        await asyncio.sleep(poll_interval)
    raise ScenarioFailure(f"timed out waiting for {description}; replicas={last!r}")


async def verify_fallback_and_promotion(
    client,
    key,
    expected,
    timeout=30.0,
    poll_interval=0.1,
    expected_segments=None,
):
    disk_only = await wait_for_replica_state(
        client,
        key,
        lambda replicas: bool(_complete(replicas, "LocalDisk"))
        and not _complete(replicas, "Memory"),
        "LocalDisk fallback with no Memory replica",
        timeout,
        poll_interval,
    )
    actual = await client.get(key)
    if actual != expected:
        raise ScenarioFailure(
            f"fallback bytes differ: expected={digest(expected)} actual={digest(actual)}"
        )
    promoted = await wait_for_replica_state(
        client,
        key,
        lambda replicas: len(
            {
                replica.get("segment_name")
                for replica in _complete(replicas, "Memory")
                if replica.get("protocol") == "rdma"
            }
        )
        >= 2,
        "promotion to two complete RDMA replicas in Memory",
        timeout,
        poll_interval,
    )
    restored = await client.get(key)
    if restored != expected:
        raise ScenarioFailure("promoted memory bytes differ from the original object")
    disk = _complete(disk_only, "LocalDisk")
    promoted_segments = _two_rdma_replicas(promoted)
    if expected_segments is not None and set(promoted_segments) != set(
        expected_segments
    ):
        raise ScenarioFailure(
            "promoted memory topology differs from the initial RDMA topology: "
            f"initial={sorted(expected_segments)!r} promoted={promoted_segments!r}"
        )
    return {
        "expected_sha256": digest(expected),
        "fallback": {
            "replica_type": "LocalDisk",
            "replicas": len(disk),
            "sha256": digest(actual),
        },
        "promotion": {
            "memory_replicas": len(promoted_segments),
            "remote_segments": promoted_segments,
            "sha256": digest(restored),
        },
    }


async def _checked_put_get(client, key, value, config):
    await client.put(key, value, config)
    replicas = await _replicas(client, key)
    segments = _two_rdma_replicas(replicas)
    actual = await client.get(key)
    if actual != value:
        raise ScenarioFailure(f"byte mismatch for {key}")
    return {"bytes": len(value), "sha256": digest(value), "remote_segments": segments}


async def _concurrent_case(workers, config, prefix, sizes=None):
    sizes = tuple(sizes or (257 * 1024, 2 * 1024 * 1024 + 29))

    async def operation(client, worker_index, object_index, size):
        key = f"{prefix}-concurrent-{worker_index}-{object_index}"
        expected = payload(size, 41 + worker_index * 7 + object_index)
        await client.put(key, expected, config)
        actual = await client.get(key)
        if actual != expected:
            raise ScenarioFailure(f"concurrent byte mismatch for {key}")
        return key, digest(actual)

    results = await asyncio.gather(
        *(
            operation(client, wi, oi, size)
            for wi, client in enumerate(workers)
            for oi, size in enumerate(sizes)
        )
    )
    return {
        "operations": len(results),
        "sha256": {key: value_hash for key, value_hash in results},
    }


async def run_standard_scenario(
    client,
    config=None,
    workers=None,
    evidence=None,
    include_multilevel=True,
    prefix="standard",
    tier_timeout=45.0,
):
    evidence = evidence if evidence is not None else {}
    workers = list(workers or [])
    cleanup_keys = []
    try:
        objects = {}
        cases = (
            ("small", 4096, 11),
            ("large", 12 * 1024 * 1024, 13),
        )
        for name, size, seed in cases:
            key = f"{prefix}-{name}"
            cleanup_keys.append(key)
            objects[name] = await _checked_put_get(
                client, key, payload(size, seed=seed), config
            )

        cross_key = f"{prefix}-cross-slice"
        parts = [
            payload(4097, seed=17),
            payload(1024 * 1024 + 3, seed=19),
            payload(2 * 1024 * 1024 + 17, seed=23),
        ]
        await client.put_parts(cross_key, parts, config)
        cleanup_keys.append(cross_key)
        cross_expected = b"".join(parts)
        cross_actual = await client.get(cross_key)
        if cross_actual != cross_expected:
            raise ScenarioFailure("cross-slice bytes differ")
        objects["cross_slice"] = {
            "bytes": len(cross_expected),
            "part_count": len(parts),
            "sha256": digest(cross_actual),
            "remote_segments": _two_rdma_replicas(await _replicas(client, cross_key)),
        }
        evidence["objects"] = objects

        concurrent_workers = workers or [client]
        evidence["concurrent"] = await _concurrent_case(
            concurrent_workers, config, prefix
        )

        mutation_key = f"{prefix}-mutation"
        original = payload(64 * 1024, seed=29)
        replacement = payload(96 * 1024 + 5, seed=31)
        await client.put(mutation_key, original, config)
        await client.upsert(mutation_key, replacement, config)
        if await client.get(mutation_key) != replacement:
            raise ScenarioFailure("overwrite did not return replacement bytes")
        await client.remove(mutation_key, force=True)
        if await client.exists(mutation_key):
            raise ScenarioFailure("deleted key still exists")
        evidence["overwrite_delete"] = {"overwrite": True, "delete": True}

        if include_multilevel:
            for key in cleanup_keys:
                await client.remove(key, force=True)
            cleanup_keys.clear()
            tier_key = f"{prefix}-tiered"
            tier_value = payload(4 * 1024 * 1024 + 113, seed=37)
            await client.put(tier_key, tier_value, config)
            cleanup_keys.append(tier_key)
            initial_segments = _two_rdma_replicas(await _replicas(client, tier_key))
            pressure = []
            for index in range(10):
                key = f"{prefix}-pressure-{index}"
                value = payload(8 * 1024 * 1024, seed=53 + index)
                await client.put(key, value, config)
                pressure.append(key)
                cleanup_keys.append(key)
            evidence["multilevel"] = await verify_fallback_and_promotion(
                client,
                tier_key,
                tier_value,
                timeout=tier_timeout,
                expected_segments=initial_segments,
            )
            evidence["multilevel"]["initial_memory_segments"] = initial_segments
            evidence["multilevel"]["pressure_objects"] = len(pressure)
        evidence["cpp_store_loaded"] = _cpp_store_loaded()
        if evidence["cpp_store_loaded"]:
            raise ScenarioFailure("Rust Store process loaded libmooncake_store.so")
        return 0
    except Exception as error:
        evidence["error"] = str(error)
        return 1
    finally:
        client.close()
        for worker in workers:
            worker.close()


async def run_scenario(client, config=None, evidence=None) -> int:
    """Backward-compatible focused standard scenario used by Rust Python tests."""
    evidence = evidence if evidence is not None else {}
    try:
        cases = (("rdma-4k", 4096), ("rdma-12m", 12 * 1024 * 1024))
        for key, size in cases:
            expected = payload(size)
            await client.put(key, expected, config)
            replicas = await _replicas(client, key)
            remote = _two_rdma_replicas(replicas)
            actual = await client.get(key)
            if actual != expected:
                return 3
            for _ in range(3):
                if await client.get(key) != expected:
                    return 4
            evidence[key] = {
                "sha256": digest(expected),
                "remote_segments": remote,
                "repeat_reads": 3,
            }
        if not await client.exists("rdma-4k"):
            return 5
        await client.remove("rdma-4k", force=True)
        if await client.exists("rdma-4k"):
            return 6
        return 0
    except ScenarioFailure:
        return 2
    finally:
        client.close()


def _cpp_store_loaded():
    try:
        return (
            "libmooncake_store.so" in open("/proc/self/maps", encoding="utf-8").read()
        )
    except OSError:
        return True


async def _create_client(store, args, suffix):
    return await store.MooncakeClient.create(
        local_hostname=f"127.0.0.1:{args.client_port + suffix}",
        metadata_server=args.metadata,
        master_server_addr=args.master,
        protocol="rdma",
        device=args.device,
        global_segment_size=0,
        local_buffer_size=32 * 1024 * 1024,
    )


def _write_result(path, value):
    temporary = f"{path}.tmp.{os.getpid()}"
    with open(temporary, "w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")
    os.replace(temporary, path)


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", default="127.0.0.1:12403")
    parser.add_argument("--client-port", type=int, default=12403)
    parser.add_argument("--device", required=True)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--master", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument(
        "--mode",
        choices=("standard", "seed", "get", "describe", "stress"),
        default="standard",
    )
    parser.add_argument("--key", default="")
    parser.add_argument("--size", type=int, default=1024 * 1024)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--prefix", default="standard")
    parser.add_argument("--tier-timeout", type=float, default=45.0)
    args = parser.parse_args()
    import _mooncake_store as store

    started = time.monotonic()
    result = {"status": "FAIL", "mode": args.mode}
    clients = []
    try:
        client = await _create_client(store, args, 0)
        clients.append(client)
        config = store.ReplicateConfig(replica_num=2)
        if args.mode == "standard":
            workers = [
                await _create_client(store, args, index + 1) for index in range(2)
            ]
            clients.extend(workers)
            evidence = {}
            rc = await run_standard_scenario(
                client,
                config,
                workers,
                evidence,
                prefix=args.prefix,
                tier_timeout=args.tier_timeout,
            )
            clients.clear()
            result.update(
                status="PASS" if rc == 0 else "FAIL", rc=rc, evidence=evidence
            )
        elif args.mode == "seed":
            if not args.key:
                raise ScenarioFailure("seed mode requires --key")
            expected = payload(args.size, args.seed)
            await client.put(args.key, expected, config)
            result.update(
                status="PASS",
                sha256=digest(expected),
                replicas=await _replicas(client, args.key),
            )
        elif args.mode == "get":
            if not args.key:
                raise ScenarioFailure("get mode requires --key")
            actual = await client.get(args.key)
            result.update(status="PASS", sha256=digest(actual), bytes=len(actual))
        elif args.mode == "describe":
            result.update(status="PASS", replicas=await _replicas(client, args.key))
        else:
            workers = [client] + [
                await _create_client(store, args, index + 1) for index in range(3)
            ]
            clients = workers
            stress_sizes = [4096, 1024 * 1024 + 7, 8 * 1024 * 1024 + 31]
            stress = await _concurrent_case(
                workers, config, args.prefix, sizes=stress_sizes
            )
            result.update(
                status="PASS",
                operations=stress["operations"],
                sizes=stress_sizes,
                byte_identical=True,
                sha256=stress["sha256"],
            )
    except Exception as error:
        result.update(status="FAIL", error=str(error))
    finally:
        for client in clients:
            client.close()
        result["duration_seconds"] = round(time.monotonic() - started, 6)
        _write_result(args.result, result)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

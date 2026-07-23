#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import signal
import time

import _mooncake_store as store


def cpp_store_loaded() -> bool:
    try:
        with open("/proc/self/maps", encoding="utf-8") as stream:
            return "libmooncake_store.so" in stream.read()
    except OSError:
        return True


def publish_json(path, value):
    temporary = f"{path}.tmp.{os.getpid()}"
    with open(temporary, "w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")
    os.replace(temporary, path)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--master", required=True)
    parser.add_argument("--ready", required=True)
    parser.add_argument("--stats", required=True)
    parser.add_argument("--command", required=True)
    parser.add_argument("--storage-root", required=True)
    parser.add_argument("--storage-quota", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--storage-interval", type=float, default=0.1)
    parser.add_argument("--disk-high-watermark", type=float, default=0.95)
    parser.add_argument("--disk-low-watermark", type=float, default=0.80)
    args = parser.parse_args()
    client = await store.MooncakeClient.create(
        local_hostname=args.node,
        metadata_server=args.metadata,
        master_server_addr=args.master,
        protocol="rdma",
        device=args.device,
        global_segment_size=128 * 1024 * 1024,
        local_buffer_size=32 * 1024 * 1024,
    )
    client.attach_local_storage_backend(
        root_dir=args.storage_root,
        fsdir="store-data",
        enable_eviction=True,
        quota_bytes=args.storage_quota,
    )
    await client.mount_local_disk_segment(True)
    offload_port = await client.start_offload_server()
    forbidden_loaded = cpp_store_loaded()
    if forbidden_loaded:
        client.close()
        raise RuntimeError("Rust Store node loaded libmooncake_store.so")

    started = time.monotonic()
    stats = {
        "node": args.node,
        "offloaded": 0,
        "promoted": 0,
        "ssd_watermark_evicted": 0,
        "cycles": 0,
        "last_error": "",
        "disk_high_watermark": args.disk_high_watermark,
        "disk_low_watermark": args.disk_low_watermark,
        "last_command": {},
    }
    publish_json(args.stats, stats)
    publish_json(
        args.ready,
        {
            "node": args.node,
            "device": args.device,
            "protocol": "rdma",
            "storage_backend": "RustFilePerKey",
            "offload_port": offload_port,
            "cpp_store_loaded": False,
            "pid": os.getpid(),
        },
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=args.storage_interval)
                continue
            except asyncio.TimeoutError:
                pass
            try:
                await client.health_check()
                stats["offloaded"] += await client.offload_objects(True)
                stats["promoted"] += await client.promote_objects()
                stats[
                    "ssd_watermark_evicted"
                ] += await client.run_disk_watermark_eviction(
                    args.disk_high_watermark, args.disk_low_watermark
                )
                if os.path.exists(args.command):
                    with open(args.command, encoding="utf-8") as stream:
                        command = json.load(stream)
                    if command.get("id") != stats["last_command"].get("id"):
                        if command.get("action") != "watermark":
                            raise RuntimeError("unknown storage-node command")
                        evicted = await client.run_disk_watermark_eviction(
                            float(command["high"]), float(command["low"])
                        )
                        stats["ssd_watermark_evicted"] += evicted
                        stats["last_command"] = {
                            "id": command["id"],
                            "action": "watermark",
                            "high": float(command["high"]),
                            "low": float(command["low"]),
                            "evicted": evicted,
                        }
                stats["last_error"] = ""
            except Exception as error:
                stats["last_error"] = str(error)
            stats["cycles"] += 1
            stats["uptime_seconds"] = round(time.monotonic() - started, 6)
            publish_json(args.stats, stats)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())

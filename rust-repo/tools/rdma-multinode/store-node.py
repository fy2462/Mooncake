#!/usr/bin/env python3
import argparse
import asyncio
import json
import signal

import _mooncake_store as store


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--master", required=True)
    parser.add_argument("--ready", required=True)
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
    with open(args.ready, "w", encoding="utf-8") as stream:
        json.dump({"node": args.node, "device": args.device, "protocol": "rdma"}, stream)
        stream.write("\n")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())

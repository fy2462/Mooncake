#!/usr/bin/env python3
"""
Self-contained Mooncake Store demo — single Python process runs both a
storage node (mounts a segment via the same client) and does put/get.

Usage:
  python demo_full.py
"""

import asyncio

from mooncake_store import MooncakeClient, ReplicateConfig


async def main():
    print("=" * 60)
    print("Mooncake Store — Full Demo (Rust Master + Rust Client)")
    print("=" * 60)

    # -- 1. Create the storage node (client that also contributes memory) --
    print("\n[1] Starting storage node (mounting 64MB segment)...")
    store_node = await MooncakeClient.create(
        local_hostname="localhost",
        metadata_server="P2PHANDSHAKE",      # no HTTP metadata needed
        master_server_addr="localhost:50051",
        protocol="tcp",
        device="",
        global_segment_size=64 * 1024 * 1024,   # 64 MB segment
        local_buffer_size=16 * 1024 * 1024,
    )
    print("    ✓ storage node mounted")

    # -- 2. Create a pure client --
    print("\n[2] Starting pure client...")
    client = await MooncakeClient.create(
        local_hostname="localhost",
        metadata_server="P2PHANDSHAKE",
        master_server_addr="localhost:50051",
        protocol="tcp",
        device="",
        global_segment_size=0,
        local_buffer_size=16 * 1024 * 1024,
    )
    print("    ✓ client connected")

    # -- 3. Put / Get --
    print("\n[3] put('hello', b'world') ...")
    await client.put("hello", b"world")
    print("    ✓ put succeeded")

    data = await client.get("hello")
    print(f"    ✓ get('hello') → {data.decode()}")

    # -- 4. Put with replication --
    print("\n[4] put with 3 replicas ...")
    cfg = ReplicateConfig(replica_num=3)
    await client.put("replicated", b"important data", config=cfg)
    print("    ✓ put succeeded (replicas allocated)")

    data = await client.get("replicated")
    print(f"    ✓ get('replicated') → {data.decode()}")

    # -- 5. Key existence --
    ok = await client.exists("hello")
    print(f"\n[5] exists('hello') → {ok}")
    ok = await client.exists("nonexistent")
    print(f"    exists('nonexistent') → {ok}")

    # -- 6. Remove --
    print("\n[6] Removing 'hello' ...")
    await client.remove("hello")
    ok = await client.exists("hello")
    print(f"    exists('hello') after remove → {ok}")

    # -- Cleanup --
    client.close()
    store_node.close()

    print("\n" + "=" * 60)
    print("✓ Full demo passed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

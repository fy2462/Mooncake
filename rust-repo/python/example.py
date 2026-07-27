#!/usr/bin/env python3
"""
Mooncake Store — Python Client (powered by Rust via pyo3-async-runtimes)

Prerequisites:
  1. A running Mooncake Master service (e.g. on localhost:50051)
  2. At least one Mooncake Store node providing memory segments
  3. Install: pip install -e .   (from the python/ directory)

Usage:
  python example.py
"""

import asyncio
import json
import numpy as np

from mooncake_store import MooncakeClient, ReplicateConfig


async def basic_kv_example():
    """Minimal put/get workflow — all operations are native Python coroutines."""

    client = await MooncakeClient.create(
        local_hostname="localhost",
        metadata_server="http://localhost:8080/metadata",
        master_server_addr="localhost:50051",
        protocol="tcp",
        device="",
        global_segment_size=0,
        local_buffer_size=256 * 1024 * 1024,
    )

    # --- string data ---
    await client.put("greeting", b"Hello from Rust-powered Mooncake Store!")
    raw = await client.get("greeting")
    print(f"string  → {raw.decode()}")

    # --- JSON data ---
    payload = {"model": "llama-7b", "temperature": 0.7, "tokens": 128}
    await client.put("config", json.dumps(payload).encode())
    cfg = json.loads((await client.get("config")).decode())
    print(f"json    → model={cfg['model']}, temperature={cfg['temperature']}")

    # --- numpy / tensor data ---
    tensor = np.random.randn(500, 500).astype(np.float32)
    await client.put("weights", tensor.tobytes())
    data = await client.get("weights")
    restored = np.frombuffer(data, dtype=np.float32).reshape(500, 500)
    print(f"tensor  → shape={restored.shape}, mean={restored.mean():.4f}")

    # --- key existence ---
    ok = await client.exists("greeting")
    print(f"exists? → greeting: {ok}")

    # --- remove ---
    await client.remove("greeting")
    ok = await client.exists("greeting")
    print(f"exists? → greeting (after remove): {ok}")

    await client.close()
    print("\n✓ basic_kv_example done")


async def replication_example():
    """Store with replication config."""
    client = await MooncakeClient.create(
        local_hostname="node1",
        metadata_server="http://master:8080/metadata",
        master_server_addr="master:50051",
        protocol="rdma",
        device="mlx5_0",
        global_segment_size=0,
        local_buffer_size=512 * 1024 * 1024,
    )

    config = ReplicateConfig(
        replica_num=3,
        with_soft_pin=True,
        preferred_segment="node1:12345",
    )

    await client.put("important_key", b"critical data with 3 replicas", config)
    data = await client.get("important_key")
    assert data == b"critical data with 3 replicas"
    print("✓ replication_example done")

    await client.close()


async def dict_storage_example():
    """Store arbitrary Python dicts via pickle."""
    import pickle

    client = await MooncakeClient.create(
        local_hostname="localhost",
        metadata_server="http://localhost:8080/metadata",
        master_server_addr="localhost:50051",
        protocol="tcp",
        device="",
        global_segment_size=0,
        local_buffer_size=128 * 1024 * 1024,
    )

    my_dict = {
        "layers": ["layer_0", "layer_1"],
        "params": {"lr": 0.001, "batch_size": 32},
        "metadata": {"version": 2, "created_by": "rust-client"},
    }

    payload = pickle.dumps(my_dict, protocol=pickle.HIGHEST_PROTOCOL)
    await client.put("model_cfg", payload)

    raw = await client.get("model_cfg")
    restored = pickle.loads(raw)
    print(f"dict    → {restored}")
    print("✓ dict_storage_example done")

    await client.close()


async def main():
    print("=" * 60)
    print("Mooncake Store — Python Client (powered by Rust)")
    print("=" * 60)

    await basic_kv_example()
    print()
    await dict_storage_example()


if __name__ == "__main__":
    asyncio.run(main())

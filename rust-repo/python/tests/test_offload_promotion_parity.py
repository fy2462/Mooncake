from __future__ import annotations

import os
import time

import pytest

from mooncake.store import MooncakeDistributedStore


def _replica_types(descs, key):
    infos = descs.get(key) if isinstance(descs, dict) else None
    if infos is None:
        return []
    if not isinstance(infos, (list, tuple)):
        infos = [infos]
    tags = []
    for info in infos:
        replica_type = info.get("replica_type", "") if isinstance(info, dict) else ""
        if "Memory" in replica_type:
            tags.append("MEMORY")
        elif "LocalDisk" in replica_type:
            tags.append("LOCAL_DISK")
        elif "Disk" in replica_type:
            tags.append("DISK")
        else:
            tags.append("UNKNOWN")
    return tags


@pytest.fixture
def store() -> MooncakeDistributedStore:
    if not os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH"):
        pytest.skip("offload/promotion e2e requires MOONCAKE_OFFLOAD_FILE_STORAGE_PATH")
    s = MooncakeDistributedStore()
    rc = s.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        32 * 1024 * 1024,
        32 * 1024 * 1024,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    if rc != 0:
        pytest.skip("MooncakeDistributedStore setup unavailable")
    try:
        yield s
    finally:
        s.close()


def test_promotion_after_repeated_hits(store: MooncakeDistributedStore) -> None:
    value_size = 1024 * 1024
    num_keys = 96
    keys = [f"poh_{i}_{int(time.time())}" for i in range(num_keys)]
    reference = {}

    for key in keys:
        value = os.urandom(value_size)
        if store.put(key, value) == 0:
            reference[key] = value

    assert reference, "no PUTs succeeded"
    time.sleep(5)

    descs = store.batch_get_replica_desc(list(reference.keys()))
    cold_key = None
    for key in reference:
        types = _replica_types(descs, key)
        if (
            cold_key is None
            and types
            and all("MEMORY" not in t for t in types)
            and any("LOCAL_DISK" in t for t in types)
        ):
            cold_key = key

    assert cold_key is not None, "no LOCAL_DISK-only key found after eviction"

    expected = reference[cold_key]
    for _ in range(4):
        assert store.get(cold_key) == expected

    time.sleep(5)
    descs_after = store.batch_get_replica_desc([cold_key])
    types_after = _replica_types(descs_after, cold_key)
    assert "MEMORY" in types_after, types_after


def test_ssd_offload_concurrent_stress(store: MooncakeDistributedStore) -> None:
    keys = [f"ssd_stress_{i}_{int(time.time())}" for i in range(40)]
    value = os.urandom(1024 * 1024)
    for key in keys:
        store.put(key, value)

    for key in keys:
        got = store.get(key)
        assert got == value, f"data mismatch for {key}"

    for key in keys[:20]:
        store.remove(key)
    for key in keys[20:]:
        assert store.get(key) == value

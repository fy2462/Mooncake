import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[2] / "tools" / "rdma-multinode" / "store-e2e.py"
SPEC = importlib.util.spec_from_file_location("store_e2e", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FakeClient:
    def __init__(self, corrupt=False):
        self.values = {}
        self.closed = False
        self.corrupt = corrupt

    async def put(self, key, value, config=None):
        self.values[key] = value

    async def get(self, key):
        value = self.values[key]
        return value[:-1] if self.corrupt else value

    def get_replica_desc(self, key):
        return [
            {
                "segment_name": "node-a",
                "protocol": "rdma",
                "status": "Complete",
                "handle_valid": True,
            },
            {
                "segment_name": "node-b",
                "protocol": "rdma",
                "status": "Complete",
                "handle_valid": True,
            },
        ]

    async def exists(self, key):
        return key in self.values

    async def remove(self, key, force=False):
        assert force
        self.values.pop(key)

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_scenario_passes_and_closes():
    client = FakeClient()
    evidence = {}
    assert await MODULE.run_scenario(client, evidence=evidence) == 0
    assert client.closed
    assert evidence["rdma-12m"]["repeat_reads"] == 3


@pytest.mark.asyncio
async def test_scenario_rejects_corrupt_bytes():
    assert await MODULE.run_scenario(FakeClient(corrupt=True)) != 0

from __future__ import annotations

import json
import threading

import numpy as np

import mooncake_store

from mooncake_store.structured_object_store import (
    MooncakeBundleTransfer,
    StructuredObjectPayload,
)


class InMemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.lock = threading.Lock()

    def put(self, key: str, value, config=None) -> int:
        del config
        with self.lock:
            self.objects[key] = bytes(value)
        return 0

    def get(self, key: str) -> bytes:
        with self.lock:
            return self.objects[key]

    def remove(self, key: str, force: bool = False) -> int:
        del force
        with self.lock:
            self.objects.pop(key, None)
        return 0


def test_structured_manifest_bytes_match_upstream_format() -> None:
    store = InMemoryStore()
    transfer = MooncakeBundleTransfer(
        store, key_prefix="compat", default_chunk_bytes=4
    )
    ref = transfer.put_structured_object(
        StructuredObjectPayload(
            metadata={"epoch": 3},
            buffers={"x": np.arange(4, dtype=np.int16)},
        )
    )

    manifest = json.loads(store.objects[ref.manifest_key])
    assert manifest["version"] == 1
    assert manifest["layout"] == "bundle"
    metadata = json.loads(
        b"".join(store.objects[chunk["key"]] for chunk in manifest["meta"]["chunks"])
    )
    assert metadata["epoch"] == 3
    assert metadata["layout"] == "structured"
    assert metadata["__mooncake_structured_fields__"]["x"] == {
        "encoding": "ndarray",
        "dtype": "<i2",
        "shape": [4],
    }
    assert [
        chunk["bytes"] for chunk in manifest["buffers"]["x"]["chunks"]
    ] == [4, 4]


def test_structured_api_is_exported_from_package() -> None:
    assert mooncake_store.MooncakeBundleTransfer is MooncakeBundleTransfer
    assert mooncake_store.StructuredObjectPayload is StructuredObjectPayload
    assert "export_ref" in mooncake_store.__all__

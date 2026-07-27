from __future__ import annotations

import ctypes
import struct

import pytest


def _codec():
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake_store")
    if not all(
        hasattr(mooncake_store, name)
        for name in (
            "_serialize_tensor",
            "_deserialize_tensor",
            "_tensor_metadata_size",
        )
    ):
        pytest.skip("built Rust extension lacks TensorMetadata helpers")
    return torch, mooncake_store


def test_cpp_tensor_metadata_shape_and_cpu_roundtrip() -> None:
    torch, mooncake_store = _codec()
    tensor = torch.arange(12, dtype=torch.int64).reshape(3, 4)

    metadata, data_ptr, data_bytes, owner = mooncake_store._serialize_tensor(tensor)
    assert mooncake_store._tensor_metadata_size() == 304
    assert len(metadata) == 304
    assert struct.unpack_from("<I", metadata, 0)[0] == 0x4D4F4F4E
    assert struct.unpack_from("<H", metadata, 4)[0] == 1
    assert struct.unpack_from("<H", metadata, 6)[0] == 304
    assert struct.unpack_from("<i", metadata, 8)[0] == 8
    assert struct.unpack_from("<i", metadata, 12)[0] == 2
    assert struct.unpack_from("<Q", metadata, 24)[0] == 304
    assert struct.unpack_from("<Q", metadata, 32)[0] == tensor.numel() * 8
    assert struct.unpack_from("<2q", metadata, 40) == (3, 4)
    assert struct.unpack_from("<2q", metadata, 104) == (3, 4)

    payload = bytes(metadata) + ctypes.string_at(int(data_ptr), int(data_bytes))
    decoded = mooncake_store._deserialize_tensor(payload)
    assert torch.equal(decoded, tensor)
    assert owner.data_ptr() == tensor.contiguous().data_ptr()


def test_tensor_codec_handles_scalar_and_empty_tensor() -> None:
    torch, mooncake_store = _codec()
    for tensor in (
        torch.tensor(7.5, dtype=torch.float32),
        torch.empty((0, 3), dtype=torch.int16),
    ):
        metadata, data_ptr, data_bytes, owner = mooncake_store._serialize_tensor(tensor)
        payload = bytes(metadata)
        if data_bytes:
            payload += ctypes.string_at(int(data_ptr), int(data_bytes))
        decoded = mooncake_store._deserialize_tensor(payload)
        assert torch.equal(decoded, tensor)
        assert owner.device.type == "cpu"


def test_tensor_codec_rejects_corrupt_metadata() -> None:
    _torch, mooncake_store = _codec()
    corrupt = bytearray(304)
    struct.pack_into("<I", corrupt, 0, 0x4D4F4F4E)
    struct.pack_into("<H", corrupt, 4, 1)
    struct.pack_into("<H", corrupt, 6, 304)
    with pytest.raises(ValueError, match="metadata"):
        mooncake_store._deserialize_tensor(bytes(corrupt))

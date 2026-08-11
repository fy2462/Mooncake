"""Wheel parity tests for Store tensor put/get and safetensor save/load.

The reference oracles live in mooncake-wheel/tests/test_put_get_tensor.py and
test_safetensor_functions.py. These tests exercise the same API surface
against a real Rust client and Rust master (HTTP metadata mode).
"""

from __future__ import annotations

import ctypes
import gc
import os
import tempfile
import uuid
from collections.abc import Iterator

import pytest
import torch

import mooncake.store as ms

_real_stores: list[object] = []


def real_store() -> ms.MooncakeDistributedStore:
    store = ms.MooncakeDistributedStore()
    rc = store.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:55052/metadata"),
        32 * 1024 * 1024,
        4 * 1024 * 1024,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    if rc != 0:
        pytest.skip(f"MooncakeDistributedStore setup failed: {rc}")
    _real_stores.append(store)
    return store


@pytest.fixture(autouse=True)
def _close_real_stores_after_each_test() -> Iterator[None]:
    yield
    while _real_stores:
        store = _real_stores.pop()
        try:
            store.close()
        except Exception:
            pass


def _assert_zero_equal(expected: torch.Tensor, actual: torch.Tensor) -> None:
    assert actual is not None
    assert tuple(actual.shape) == tuple(expected.shape)
    assert actual.dtype == expected.dtype
    assert actual.numel() == 0
    assert torch.equal(actual, expected)


def test_cpp_parity_put_get_tensor() -> None:
    # Wheel TestDistributedObjectStore.test_put_get_tensor: float32, int32,
    # bool, and random-1000 float tensors put with zero, get non-null, and
    # preserve dtype and exact/allclose contents.
    store = real_store()
    cases = [
        ("test_tensor_float", torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)),
        ("test_tensor_int", torch.tensor([1, 2, 3, 4], dtype=torch.int32)),
        (
            "test_tensor_bool",
            torch.tensor([True, False, False, True], dtype=torch.bool),
        ),
        ("test_tensor_rand", torch.rand(1000, dtype=torch.float32)),
    ]
    for key, tensor in cases:
        assert store.put_tensor(key, tensor) == 0
        retrieved = store.get_tensor(key)
        assert retrieved is not None
        assert retrieved.dtype == tensor.dtype
        if tensor.dtype == torch.float32:
            assert torch.allclose(tensor, retrieved)
        else:
            assert torch.equal(tensor, retrieved)
        store.remove(key, force=True)


def test_cpp_parity_put_get_tensor_with_metadata() -> None:
    # Wheel test_put_get_tensor_with_metadata: 2-D float32 and 3-D int64
    # round-trip with exact shape, dtype, and contents.
    store = real_store()
    cases = [
        (
            "test_tensor_with_metadata_2d",
            torch.tensor([[1.0, 2.0], [3.0, 4.0]], dtype=torch.float32),
        ),
        (
            "test_tensor_with_metadata_3d",
            torch.tensor([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=torch.int64),
        ),
    ]
    for key, tensor in cases:
        assert store.put_tensor(key, tensor) == 0
        retrieved = store.get_tensor(key)
        assert retrieved is not None
        assert tuple(retrieved.shape) == tuple(tensor.shape)
        assert retrieved.dtype == tensor.dtype
        assert torch.equal(tensor, retrieved)
        store.remove(key, force=True)


def test_cpp_parity_put_get_zero_tensor() -> None:
    # Wheel test_put_get_zero_tensor: four zero-element shapes/dtypes
    # round-trip individually and through ordered batch-get.
    store = real_store()
    tensors = [
        torch.empty((0,), dtype=torch.float32),
        torch.empty((0, 4096), dtype=torch.float16),
        torch.empty((2, 0, 3), dtype=torch.bfloat16),
        torch.empty((1, 0), dtype=torch.int64),
    ]
    keys = [f"test_zero_tensor_{uuid.uuid4().hex}_{i}" for i in range(len(tensors))]
    for key, tensor in zip(keys, tensors):
        assert store.put_tensor(key, tensor) == 0
        _assert_zero_equal(tensor, store.get_tensor(key))

    batch_results = store.batch_get_tensor(keys)
    assert len(batch_results) == len(tensors)
    for tensor, retrieved in zip(tensors, batch_results):
        _assert_zero_equal(tensor, retrieved)

    for key in keys:
        store.remove(key, force=True)


def test_cpp_parity_zero_tensor_into_and_from() -> None:
    # Wheel test_zero_tensor_into_and_from: metadata-only zero tensor decodes
    # through a registered 4096-byte destination and re-stores from that
    # registered encoding with exact round-trip.
    store = real_store()
    tensor = torch.empty((2, 0, 3), dtype=torch.float32)
    key = f"test_zero_tensor_into_{uuid.uuid4().hex}"
    key_from = f"test_zero_tensor_from_{uuid.uuid4().hex}"
    assert store.put_tensor(key, tensor) == 0

    buffer_size = 4096
    buffer = (ctypes.c_ubyte * buffer_size)()
    buffer_ptr = ctypes.addressof(buffer)
    assert store.register_buffer(buffer_ptr, buffer_size) == 0
    try:
        total_length = store.get_size(key)
        assert total_length > 0
        assert total_length < buffer_size
        retrieved = store.get_tensor_into(key, buffer_ptr, buffer_size)
        _assert_zero_equal(tensor, retrieved)

        assert store.put_tensor_from(key_from, buffer_ptr, total_length) == 0
        _assert_zero_equal(tensor, store.get_tensor(key_from))
    finally:
        store.unregister_buffer(buffer_ptr)
        store.remove(key, force=True)
        store.remove(key_from, force=True)


def test_nonempty_tensor_into_aliases_raw_buffer_with_registration_lease() -> None:
    # The wheel returns a view over the raw destination. The Rust binding keeps
    # that registration leased until the returned tensor is released.
    store = real_store()
    tensor = torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)
    key = f"test_tensor_into_alias_{uuid.uuid4().hex}"
    buffer_size = 4096
    buffer = (ctypes.c_ubyte * buffer_size)()
    buffer_ptr = ctypes.addressof(buffer)
    assert store.put_tensor(key, tensor) == 0
    assert store.register_buffer(buffer_ptr, buffer_size) == 0
    registered = True
    try:
        total_length = store.get_size(key)
        retrieved = store.get_tensor_into(key, buffer_ptr, buffer_size)
        assert torch.equal(retrieved, tensor)
        assert retrieved.data_ptr() == buffer_ptr + 304
        retrieved[0] = 9.0
        assert ctypes.c_float.from_address(buffer_ptr + 304).value == 9.0
        assert total_length == 304 + tensor.numel() * tensor.element_size()
        with pytest.raises(Exception, match="tensor views are active"):
            store.unregister_buffer(buffer_ptr)
        alias = retrieved.view(-1)
        del retrieved
        gc.collect()
        with pytest.raises(Exception, match="tensor views are active"):
            store.unregister_buffer(buffer_ptr)
        assert store.close() == -1
        assert alias[0].item() == 9.0
        del alias
        gc.collect()
        assert store.unregister_buffer(buffer_ptr) == 0
        registered = False
    finally:
        if registered:
            store.unregister_buffer(buffer_ptr)
        store.remove(key, force=True)


def test_cpp_parity_put_tensor_from_rejects_invalid_raw_metadata() -> None:
    store = real_store()
    key = f"test_invalid_tensor_from_{uuid.uuid4().hex}"
    buffer_size = 4096
    buffer = (ctypes.c_ubyte * buffer_size)()
    buffer_ptr = ctypes.addressof(buffer)
    assert store.register_buffer(buffer_ptr, buffer_size) == 0
    try:
        assert store.put_tensor_from(key, buffer_ptr, buffer_size) == -600
        assert not store.is_exist(key)
    finally:
        store.unregister_buffer(buffer_ptr)
        store.remove(key, force=True)


def test_cpp_parity_zero_tensor_with_tp() -> None:
    # Wheel test_zero_tensor_with_tp: zero tensor split across two TP ranks
    # stores with zero and each rank returns the exact empty shard.
    store = real_store()
    tensor = torch.empty((2, 0, 3), dtype=torch.float32)
    key = f"test_zero_tensor_tp_{uuid.uuid4().hex}"
    tp_size = 2
    split_dim = 0

    assert (
        store.put_tensor_with_tp(key, tensor, tp_size=tp_size, split_dim=split_dim) == 0
    )
    for rank in range(tp_size):
        shard = store.get_tensor_with_tp(key, tp_rank=rank, tp_size=tp_size)
        expected = tensor.narrow(split_dim, rank, 1).contiguous()
        _assert_zero_equal(expected, shard)

    for rank in range(tp_size):
        store.remove(f"{key}_tp_{rank}", force=True)


def test_cpp_parity_safetensor_save_and_load_from_file() -> None:
    # Wheel TestSafetensorFunctions.test_save_and_load_tensor_from_safetensor.
    store = real_store()
    tensor = torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)
    key = "test_tensor_safetensor"
    assert store.put_tensor(key, tensor) == 0
    assert torch.allclose(tensor, store.get_tensor(key))

    with tempfile.NamedTemporaryFile(suffix=".safetensors", delete=False) as temp_file:
        temp_filename = temp_file.name
    try:
        assert store.save_tensor_to_safetensor(key, temp_filename) == 0
        assert os.path.exists(temp_filename)

        store.remove(key, force=True)
        loaded = store.load_tensor_from_safetensor(key, temp_filename)
        assert loaded is not None
        retrieved = store.get_tensor(key)
        assert retrieved is not None
        assert torch.allclose(tensor, retrieved)
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
        store.remove(key, force=True)


def test_cpp_parity_safetensor_different_tensor_types() -> None:
    # Wheel test_save_and_load_different_tensor_types dtype/shape matrix.
    store = real_store()
    test_cases = [
        ("test_float_tensor", torch.tensor([1.0, 2.0, 3.0], dtype=torch.float32)),
        ("test_int_tensor", torch.tensor([1, 2, 3], dtype=torch.int32)),
        ("test_bool_tensor", torch.tensor([True, False, True], dtype=torch.bool)),
        ("test_2d_tensor", torch.tensor([[1.0, 2.0], [3.0, 4.0]], dtype=torch.float32)),
        (
            "test_3d_tensor",
            torch.tensor([[[1.0, 2.0]], [[3.0, 4.0]]], dtype=torch.float32),
        ),
    ]
    for key, tensor in test_cases:
        assert store.put_tensor(key, tensor) == 0
        with tempfile.NamedTemporaryFile(
            suffix=".safetensors", delete=False
        ) as temp_file:
            temp_filename = temp_file.name
        try:
            assert store.save_tensor_to_safetensor(key, temp_filename) == 0
            loaded = store.load_tensor_from_safetensor(key, temp_filename)
            assert loaded is not None
            assert loaded.dtype == tensor.dtype
            assert torch.equal(tensor, loaded)
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            store.remove(key, force=True)


def test_cpp_parity_safetensor_default_filename() -> None:
    # Wheel test_save_tensor_with_default_filename: omitting filename creates
    # a file named exactly as the key.
    store = real_store()
    tensor = torch.tensor([5.0, 6.0, 7.0], dtype=torch.float32)
    key = f"test_default_filename_{uuid.uuid4().hex}"
    assert store.put_tensor(key, tensor) == 0
    try:
        assert store.save_tensor_to_safetensor(key) == 0
        assert os.path.exists(key)
        loaded = store.load_tensor_from_safetensor(key, key)
        assert loaded is not None
        assert torch.equal(tensor, loaded)
    finally:
        if os.path.exists(key):
            os.remove(key)
        store.remove(key, force=True)


def test_cpp_parity_safetensor_load_with_different_key() -> None:
    # Wheel test_load_tensor_with_different_key: a saved tensor reloads under a
    # distinct key and does not recreate an absent original key.
    store = real_store()
    tensor = torch.tensor([8.0, 9.0, 10.0], dtype=torch.float32)
    suffix = uuid.uuid4().hex
    original_key = f"test_original_key_{suffix}"
    new_key = f"test_new_key_{suffix}"
    assert store.put_tensor(original_key, tensor) == 0
    with tempfile.NamedTemporaryFile(suffix=".safetensors", delete=False) as temp_file:
        temp_filename = temp_file.name
    try:
        assert store.save_tensor_to_safetensor(original_key, temp_filename) == 0
        store.remove(original_key, force=True)
        original_before = store.get_tensor(original_key)

        loaded = store.load_tensor_from_safetensor(new_key, temp_filename)
        assert loaded is not None
        retrieved_new = store.get_tensor(new_key)
        assert retrieved_new is not None
        assert torch.allclose(tensor, retrieved_new)

        retrieved_original = store.get_tensor(original_key)
        if original_before is None:
            assert retrieved_original is None
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
        store.remove(new_key, force=True)
        store.remove(original_key, force=True)


def test_cpp_parity_safetensor_file_not_found_returns_none() -> None:
    # Wheel test_load_tensor_from_safetensor_file_not_found returns None.
    store = real_store()
    missing = os.path.join(
        tempfile.gettempdir(), f"missing_{uuid.uuid4().hex}.safetensors"
    )
    assert store.load_tensor_from_safetensor("any_key", missing) is None


def test_cpp_parity_safetensor_save_missing_key_returns_nonzero() -> None:
    # Wheel test_save_tensor_key_not_found returns nonzero.
    store = real_store()
    with tempfile.NamedTemporaryFile(suffix=".safetensors", delete=False) as temp_file:
        temp_filename = temp_file.name
    try:
        result = store.save_tensor_to_safetensor("missing_store_key", temp_filename)
        assert result != 0
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

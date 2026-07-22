# Rust Python Structured Object Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the merged structured-object/DataProto wire format and public API in `rust-repo/python/mooncake_store` while using only the Rust Mooncake binding.

**Architecture:** Preserve the upstream `structured_object_store.py` serialization and manifest implementation byte-for-byte wherever possible. Add a focused adapter that converts the Rust binding's awaitable CRUD/batch methods into the synchronous `BundleStore` protocol and a lease adapter that gives Rust `BufferPool` buffers the pointer/ownership interface expected by pool-backed reads.

**Tech Stack:** Python 3.10+, NumPy, msgpack, pytest, pytest-asyncio, PyO3 Rust binding, `asyncio` background event loop.

## Global Constraints

- Do not import, link, or call `mooncake.store` or `libmooncake_store.so`.
- Preserve the manifest and payload byte format from upstream commit `4dbe5a4c` and its ancestors `c114e81d`, `5605ca88`, and `b272a04b`.
- Use `/home/fy2462/Mooncake/.venv`; put pytest/cache/build data under `/home/fy2462/workspace/tmp/mooncake`.
- Rust async methods must work when the structured API is called both outside and inside an already-running Python event loop.
- Partial writes must remove every successfully written payload key before re-raising the original failure.
- Torch remains optional; NumPy and msgpack are declared package dependencies.

---

### Task 1: Port the authoritative wire-format module

**Files:**
- Create: `python/mooncake_store/structured_object_store.py`
- Create: `python/tests/test_structured_object_store.py`
- Modify: `python/pyproject.toml`

**Interfaces:**
- Consumes: synchronous `BundleStore.put/get/remove` protocol.
- Produces: `MooncakeBundleTransfer`, `BundleTransferPolicy`, `FieldSchema`, `RemoteBundleRef`, `MooncakeDataProtoRef`, `StructuredObjectPayload`, `StructuredObjectReadSpec`, `export_ref`, and `import_ref`.

- [ ] **Step 1: Add failing import and byte-format tests**

Copy the upstream in-memory store test fixture and add these first tests under the Rust package name:

```python
from mooncake_store.structured_object_store import (
    MooncakeBundleTransfer,
    StructuredObjectPayload,
)

def test_structured_manifest_bytes_match_upstream_format():
    store = InMemoryStore()
    transfer = MooncakeBundleTransfer(store, key_prefix="compat", default_chunk_bytes=4)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(metadata={"epoch": 3}, buffers={"x": np.arange(4, dtype=np.int16)})
    )
    manifest = json.loads(store.objects[ref.manifest_key])
    assert manifest["format"] == "mooncake-structured-object-v1"
    assert manifest["metadata"]["epoch"] == 3
    assert manifest["members"]["x"]["dtype"] == "int16"
    assert [chunk["bytes"] for chunk in manifest["members"]["x"]["chunks"]] == [4, 4]
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```text
PYTHONPATH=python /home/fy2462/Mooncake/.venv/bin/python -m pytest python/tests/test_structured_object_store.py -q
```

Expected: collection fails because `mooncake_store.structured_object_store` does not exist.

- [ ] **Step 3: Mechanically port the upstream module**

Copy `mooncake-wheel/mooncake/structured_object_store.py` at `up_main` into the Rust package. Replace only the optional tensor-helper import:

```python
try:
    from . import _mooncake_store
except Exception:
    _mooncake_store = None
```

Do not copy the legacy setup client or any `mooncake.store` import. Add `numpy>=1.24` and `msgpack>=1.0` to `[project].dependencies`.

- [ ] **Step 4: Run format, manifest, ndarray, bytes, JSON, msgpack, ragged, schema-nullability, and row-count tests**

Run the selected test module with `PYTHONPYCACHEPREFIX=/home/fy2462/workspace/tmp/mooncake/pycache` and `--basetemp=/home/fy2462/workspace/tmp/mooncake/pytest`.

Expected: all sync in-memory compatibility vectors pass.

- [ ] **Step 5: Commit**

```text
git commit -m "[Python] port structured object wire format"
```

### Task 2: Adapt Rust awaitable Store methods

**Files:**
- Create: `python/mooncake_store/store_adapter.py`
- Modify: `python/mooncake_store/structured_object_store.py`
- Test: `python/tests/test_structured_store_adapter.py`

**Interfaces:**
- Consumes: Rust `MooncakeClient` methods `put`, `get`, `remove`, `batch_remove`, `batch_put_from`, `batch_get_into`, `get_into`, `get_into_ranges`, `register_buffer`, and `unregister_buffer`.
- Produces: `RustStoreAdapter(store)`, `adapt_store(store)`, and synchronous status/value semantics required by `BundleStore`.

- [ ] **Step 1: Add RED tests for normal and running-loop contexts**

```python
@pytest.mark.asyncio
async def test_adapter_resolves_rust_style_awaitables_inside_running_loop():
    raw = AsyncInMemoryStore()
    adapter = RustStoreAdapter(raw)
    assert adapter.put("k", b"value") == 0
    assert adapter.get("k") == b"value"
    assert adapter.remove("k", True) == 0

def test_adapter_normalizes_none_and_status_vectors():
    adapter = RustStoreAdapter(AsyncInMemoryStore())
    assert adapter.batch_remove(["a", "b"], True) == [0, 0]
```

Verify RED on absent `store_adapter`.

- [ ] **Step 2: Implement a persistent event-loop runner**

`_AwaitableRunner.call(fn, *args, **kwargs)` submits this coroutine through `asyncio.run_coroutine_threadsafe`:

```python
async def invoke():
    result = fn(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result
```

Start one daemon loop thread per adapter, serialize Rust calls with an `RLock`, expose `close()`, and stop/join the loop thread on finalization. Normalize Rust `None` mutation results to `0`; normalize `None` batch mutation results to a zero vector of input length.

- [ ] **Step 3: Forward synchronous zero-copy methods without changing pointers**

Define explicit forwarding methods for `put_from`, `batch_put_from`, `get_into`, `batch_get_into`, `get_into_ranges`, `register_buffer`, and `unregister_buffer`. Each method invokes through the runner so both PyO3 sync and awaitable implementations are accepted.

- [ ] **Step 4: Auto-adapt only Rust clients**

At the beginning of `MooncakeBundleTransfer.__init__`:

```python
store = adapt_store(store)
```

`adapt_store` returns existing adapters unchanged, wraps objects whose class module starts with `mooncake_store._mooncake_store`, and leaves synchronous test/protocol stores unchanged.

- [ ] **Step 5: Run adapter and structured tests, then commit**

```text
git commit -m "[Python] adapt structured objects to Rust Store"
```

### Task 3: Adapt Rust BufferPool leases

**Files:**
- Modify: `python/mooncake_store/store_adapter.py`
- Modify: `python/mooncake_store/structured_object_store.py`
- Test: `python/tests/test_structured_store_adapter.py`

**Interfaces:**
- Consumes: Rust `BufferPool.acquire(size) -> bytearray` and `BufferPool.release(buffer)`.
- Produces: `RustBufferPoolAdapter.acquire(size) -> RustBufferLease` with `.ptr`, `.buffer`, and idempotent `.release()`.

- [ ] **Step 1: Add a RED lease lifetime test**

```python
def test_rust_pool_lease_exposes_pointer_and_releases_once():
    raw = FakeRustBufferPool()
    lease = RustBufferPoolAdapter(raw).acquire(64)
    ctypes.memset(lease.ptr, 7, 64)
    assert bytes(lease.buffer[:4]) == b"\x07" * 4
    lease.release()
    lease.release()
    assert raw.release_calls == 1
```

- [ ] **Step 2: Implement lease ownership**

Keep the returned `bytearray` alive in `RustBufferLease`, derive `ptr` with `ctypes.addressof(ctypes.c_char.from_buffer(buffer))`, return `memoryview(buffer)` from `.buffer`, and call `pool.release(buffer)` exactly once. Adapt a supplied Rust pool in `MooncakeBundleTransfer.__init__`.

- [ ] **Step 3: Verify pool-backed ndarray lifetime**

Store an ndarray, materialize with the adapted pool, assert the result carries `_mooncake_pool_owner`, call `MooncakeBundleTransfer.release_result`, and assert borrowed bytes return to zero.

- [ ] **Step 4: Commit**

```text
git commit -m "[Python] adapt Rust buffer pool leases"
```

### Task 4: Prove the merged behavior surface

**Files:**
- Modify: `python/tests/test_structured_object_store.py`
- Modify: `python/mooncake_store/__init__.py`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: package-level exports and regression evidence for the requested API.

- [ ] **Step 1: Add/copy focused upstream behavior vectors**

Include tests for unified dict `put/get`, DataProto reference export/import, field selection, schema section routing, nullable rejection, inconsistent row counts, typed ragged values, ragged tensor dictionaries, recursive JSON/msgpack fields, multi-buffer puts, auto/batch/parallel policy selection, and cleanup after payload or manifest failure.

- [ ] **Step 2: Export the public API**

Add lazy Python exports without importing the Rust extension eagerly:

```python
_STRUCTURED_EXPORTS = {
    "MooncakeBundleTransfer", "BundleTransferPolicy", "FieldSchema",
    "MooncakeDataProtoRef", "RemoteBundleRef", "StructuredObjectPayload",
    "StructuredObjectReadSpec", "StructuredObjectResult",
    "export_ref", "import_ref", "tensor_object_buffer", "raw_destination",
}
```

Route these names through `mooncake_store.structured_object_store` in `__getattr__`.

- [ ] **Step 3: Run the complete Python suite**

Run:

```text
PYTHONPATH=python PYTHONPYCACHEPREFIX=/home/fy2462/workspace/tmp/mooncake/pycache /home/fy2462/Mooncake/.venv/bin/python -m pytest python/tests -q --basetemp=/home/fy2462/workspace/tmp/mooncake/pytest
```

Expected: all tests pass; torch-only vectors skip when torch is unavailable.

- [ ] **Step 4: Commit**

```text
git commit -m "[Python] expose Rust structured object API"
```

### Task 5: Verify packaging and dependency boundary

**Files:**
- Modify: `docs/superpowers/specs/2026-07-23-up-main-parity-design.md`

**Interfaces:**
- Consumes: completed structured-object implementation.
- Produces: wheel/import verification and documented completion status.

- [ ] **Step 1: Build/install with the project venv and shared output**

```text
MATURIN_BUILD_DIR=/home/fy2462/workspace/tmp/mooncake/maturin CARGO_BUILD_JOBS=5 CARGO_TARGET_DIR=/home/fy2462/workspace/tmp/mooncake/cargo-target /home/fy2462/Mooncake/.venv/bin/maturin develop --manifest-path python/Cargo.toml
```

- [ ] **Step 2: Verify imports and legacy isolation**

```text
/home/fy2462/Mooncake/.venv/bin/python -c 'import mooncake_store; from mooncake_store import MooncakeBundleTransfer'
rg -n 'import mooncake\.store|libmooncake_store|mooncake-store/src' python
```

Expected: import succeeds and the dependency search returns no legacy Store dependency.

- [ ] **Step 3: Update status, run formatting/pre-commit when available, and commit**

```text
git commit -m "[Python] verify Rust structured object parity"
```

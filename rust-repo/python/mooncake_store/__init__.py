"""Python bindings and service entry points for the Rust Mooncake store."""

from importlib import import_module

_RUST_EXPORTS = {
    "MooncakeClient",
    "ReplicateConfig",
    "StoreError",
    "EngramStore",
    "EngramStoreConfig",
    "P2pStore",
    "BufferPool",
    "RegisteredBufferPool",
    "S3Config",
    "RemoteSourceConfig",
}

__all__ = sorted(_RUST_EXPORTS)
__version__ = "0.1.0"


def __getattr__(name):
    if name not in _RUST_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module("._mooncake_store", __name__), name)
    globals()[name] = value
    return value

"""Python bindings and service entry points for the Rust Mooncake store."""

from importlib import import_module

_RUST_EXPORTS = {
    "MooncakeClient",
    "ParallelAxis",
    "ReadTarget",
    "ReplicateConfig",
    "StoreError",
    "EngramStore",
    "EngramStoreConfig",
    "P2pStore",
    "BufferPool",
    "ClassicTransferEngine",
    "BufferLease",
    "RegisteredBufferPool",
    "RegisteredBufferLease",
    "S3Config",
    "TensorParallelism",
    "WriterPartition",
    "RemoteSourceConfig",
    "_serialize_tensor",
    "_deserialize_tensor",
    "_tensor_metadata_size",
}

_STRUCTURED_EXPORTS = {
    "BundleTransferPolicy",
    "FieldSchema",
    "MooncakeBundleTransfer",
    "MooncakeDataProtoRef",
    "RemoteBundleRef",
    "StructuredMemberSlice",
    "StructuredObjectPayload",
    "StructuredObjectReadSpec",
    "StructuredObjectResult",
    "export_dataproto_ref",
    "export_ref",
    "import_dataproto_ref",
    "import_ref",
    "is_dataproto_ref_handle",
    "raw_destination",
    "tensor_object_buffer",
}

__all__ = sorted(_RUST_EXPORTS | _STRUCTURED_EXPORTS)
__version__ = "0.1.0"


def __getattr__(name):
    if name in _RUST_EXPORTS:
        module = import_module("._mooncake_store", __name__)
    elif name in _STRUCTURED_EXPORTS:
        module = import_module(".structured_object_store", __name__)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(module, name)
    globals()[name] = value
    return value

"""Bridge module: re-exports mooncake_store under mooncake.store for backward compatibility."""

import logging
import os

from mooncake_store import (  # noqa: F401
    MooncakeClient as MooncakeDistributedStore,
    ReplicateConfig,
    StoreError,
    EngramStore,
    EngramStoreConfig,
    P2pStore,
    S3Config,
    RemoteSourceConfig,
)

__all__ = [
    "MooncakeDistributedStore",
    "ReplicateConfig",
    "StoreError",
    "EngramStore",
    "EngramStoreConfig",
    "P2pStore",
    "S3Config",
    "RemoteSourceConfig",
    "enable_te_debug",
    "setup_te_debug_logging",
]

# ---------------------------------------------------------------------------
# TE Debug Logging Helpers
# ---------------------------------------------------------------------------

_logger = logging.getLogger("mooncake.te_debug")


def setup_te_debug_logging(level: int = logging.DEBUG) -> None:
    """Configure Python-side TE debug logging to stderr.

    Call this before creating any MooncakeDistributedStore instances
    to see detailed transfer engine operation traces.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "[PY_TE_DEBUG] %(asctime)s %(thread)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    ))
    _logger.addHandler(handler)
    _logger.setLevel(level)

    # Also enable Rust-side tracing via the pyo3 extension
    try:
        from mooncake_store import _mooncake_store
        _mooncake_store.enable_te_debug_tracing()
    except Exception:
        pass  # function may not exist in older builds


def enable_te_debug() -> None:
    """Enable all TE debug logging (Rust + Python layers).

    Equivalent to calling setup_te_debug_logging().
    """
    setup_te_debug_logging()


def _log_te(op: str, **kwargs) -> None:
    """Internal: log a TE operation with structured context."""
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    _logger.debug("%s %s", op, extras)

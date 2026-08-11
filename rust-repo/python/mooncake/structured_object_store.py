"""Structured-object store API backed by the Rust store.

Alias module so the wheel-style ``mooncake.structured_object_store`` import
surface resolves to the Rust implementation in ``mooncake_store``.
"""

from mooncake_store.structured_object_store import *  # noqa: F401,F403

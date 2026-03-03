"""Async wrappers around file_hunter_core hashing functions."""

import asyncio

from file_hunter_core.hasher import hash_file_sync, hash_file_partial_sync


async def hash_file(path: str) -> tuple[str, str]:
    """Async wrapper — runs blocking I/O in a thread."""
    return await asyncio.to_thread(hash_file_sync, path)


async def hash_file_partial(path: str) -> str:
    """Async wrapper — partial hash in a thread."""
    return await asyncio.to_thread(hash_file_partial_sync, path)

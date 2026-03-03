"""Filesystem abstraction layer — routes operations to local disk or agent proxy.

Every function checks is_agent_location(). For agent locations, it calls the
registered agent_proxy hook. For local locations, it runs blocking I/O in a
thread. The free version has no proxy hook, so all operations fall through to
local.
"""

import asyncio
import os
import shutil

from file_hunter.extensions import is_agent_location, get_agent_proxy


# ---------------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------------


async def file_exists(path: str, location_id: int) -> bool:
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("file_exists", location_id, path=path)
        return False
    return await asyncio.to_thread(os.path.isfile, path)


async def dir_exists(path: str, location_id: int) -> bool:
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("dir_exists", location_id, path=path)
        return False
    return await asyncio.to_thread(os.path.isdir, path)


async def path_exists(path: str, location_id: int) -> bool:
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("path_exists", location_id, path=path)
        return False
    return await asyncio.to_thread(os.path.exists, path)


async def file_delete(path: str, location_id: int):
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("file_delete", location_id, path=path)
        raise ConnectionError("Agent proxy not available.")
    await asyncio.to_thread(os.remove, path)


async def file_move(src: str, dest: str, location_id: int):
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("file_move", location_id, path=src, destination=dest)
        raise ConnectionError("Agent proxy not available.")
    try:
        await asyncio.to_thread(os.rename, src, dest)
    except OSError:
        await asyncio.to_thread(shutil.move, src, dest)


async def file_write_text(path: str, text: str, location_id: int):
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("file_write", location_id, path=path, content=text)
        raise ConnectionError("Agent proxy not available.")
    await asyncio.to_thread(_write_text_sync, path, text)


def _write_text_sync(path: str, text: str):
    with open(path, "w") as f:
        f.write(text)


async def file_write_bytes(path: str, data: bytes, location_id: int):
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            import base64

            return await proxy(
                "file_write",
                location_id,
                path=path,
                content=base64.b64encode(data).decode(),
                encoding="base64",
            )
        raise ConnectionError("Agent proxy not available.")
    await asyncio.to_thread(_write_bytes_sync, path, data)


def _write_bytes_sync(path: str, data: bytes):
    with open(path, "wb") as f:
        f.write(data)


CHUNK_SIZE = 1024 * 1024  # 1 MB


async def file_write_bytes_chunked(
    path: str, data: bytes, location_id: int, on_progress=None
):
    """Write bytes in chunks with progress callback. For agent locations only.

    on_progress(bytes_sent, total_bytes) is called after each chunk.
    Falls back to file_write_bytes for local or small files.
    """
    if not is_agent_location(location_id) or len(data) <= CHUNK_SIZE:
        await file_write_bytes(path, data, location_id)
        if on_progress:
            await on_progress(len(data), len(data))
        return

    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")

    import base64

    total = len(data)
    offset = 0
    first = True

    while offset < total:
        chunk = data[offset : offset + CHUNK_SIZE]
        await proxy(
            "file_write",
            location_id,
            path=path,
            content=base64.b64encode(chunk).decode(),
            encoding="base64",
            append=not first,
        )
        offset += len(chunk)
        first = False
        if on_progress:
            await on_progress(offset, total)


async def file_read_bytes(path: str, location_id: int) -> bytes:
    if is_agent_location(location_id):
        from file_hunter.extensions import get_fetch_bytes

        fetch = get_fetch_bytes()
        if fetch:
            data = await fetch(path, location_id)
            if data is not None:
                return data
        raise ConnectionError("Agent is offline or proxy not available.")
    return await asyncio.to_thread(_read_bytes_sync, path)


def _read_bytes_sync(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


async def file_stat(path: str, location_id: int) -> dict | None:
    """Return {size, mtime, ctime} or None if path doesn't exist."""
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("file_stat", location_id, path=path)
        return None
    try:
        st = await asyncio.to_thread(os.stat, path)
        return {"size": st.st_size, "mtime": st.st_mtime, "ctime": st.st_ctime}
    except OSError:
        return None


async def file_hash(path: str, location_id: int) -> tuple[str, str]:
    """Return (hash_fast, hash_strong)."""
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            result = await proxy("file_hash", location_id, path=path)
            return result["hash_fast"], result["hash_strong"]
        raise ConnectionError("Agent proxy not available.")
    from file_hunter.services.hasher import hash_file

    return await hash_file(path)


# ---------------------------------------------------------------------------
# Directory operations
# ---------------------------------------------------------------------------


async def dir_create(path: str, location_id: int, exist_ok: bool = False):
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("dir_create", location_id, path=path)
        raise ConnectionError("Agent proxy not available.")
    await asyncio.to_thread(os.makedirs, path, exist_ok=exist_ok)


async def dir_delete(path: str, location_id: int):
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("dir_delete", location_id, path=path)
        raise ConnectionError("Agent proxy not available.")

    def _ignore_not_found(func, fpath, exc):
        if not isinstance(exc, FileNotFoundError):
            raise exc

    await asyncio.to_thread(shutil.rmtree, path, onexc=_ignore_not_found)


async def dir_move(src: str, dest: str, location_id: int):
    if is_agent_location(location_id):
        proxy = get_agent_proxy()
        if proxy:
            return await proxy("dir_move", location_id, path=src, destination=dest)
        raise ConnectionError("Agent proxy not available.")
    await asyncio.to_thread(shutil.move, src, dest)


# ---------------------------------------------------------------------------
# Cross-location copy
# ---------------------------------------------------------------------------


async def copy_file(src: str, src_loc_id: int, dst: str, dst_loc_id: int):
    """Copy a file between any combination of local and agent locations."""
    src_agent = is_agent_location(src_loc_id)
    dst_agent = is_agent_location(dst_loc_id)

    if not src_agent and not dst_agent:
        # Local -> Local
        await asyncio.to_thread(shutil.copy2, src, dst)
    elif src_agent and not dst_agent:
        # Agent -> Local: read from agent, write locally
        data = await file_read_bytes(src, src_loc_id)
        await asyncio.to_thread(_write_bytes_sync, dst, data)
    elif not src_agent and dst_agent:
        # Local -> Agent: read locally, write to agent
        data = await asyncio.to_thread(_read_bytes_sync, src)
        await file_write_bytes(dst, data, dst_loc_id)
    else:
        # Agent -> Agent: read from source agent, write to dest agent
        data = await file_read_bytes(src, src_loc_id)
        await file_write_bytes(dst, data, dst_loc_id)


# ---------------------------------------------------------------------------
# Composite helpers (stub/sources generation)
# ---------------------------------------------------------------------------


async def write_moved_stub(
    original_path: str, filename: str, dest_path: str, now_iso: str, location_id: int
):
    """Generate .moved stub text, write it, and delete the original."""
    stub_text = (
        f"Consolidated by File Hunter\n"
        f"Original: {filename}\n"
        f"Moved to: {dest_path}\n"
        f"Date: {now_iso}\n"
    )
    stub_path = original_path + ".moved"
    await file_delete(original_path, location_id)
    await file_write_text(stub_path, stub_text, location_id)


async def write_sources_file(
    canonical_path: str, all_copies: list[dict], now_iso: str, location_id: int
):
    """Write a .sources file next to the canonical file."""
    sources_path = canonical_path + ".sources"
    entries = "".join(f"- {c['location_name']}: {c['rel_path']}\n" for c in all_copies)

    existing = await path_exists(sources_path, location_id)
    if existing:
        # Append to existing
        try:
            old_content = (await file_read_bytes(sources_path, location_id)).decode()
        except Exception:
            old_content = ""
        await file_write_text(sources_path, old_content + entries, location_id)
    else:
        text = f"Consolidated by File Hunter\nDate: {now_iso}\n\nSources:\n{entries}"
        await file_write_text(sources_path, text, location_id)


async def write_or_append_sources(
    canonical_path: str,
    src_loc_name: str,
    src_rel_path: str,
    now_iso: str,
    location_id: int,
):
    """Append a single source entry to the .sources file."""
    sources_path = canonical_path + ".sources"
    entry = f"- {src_loc_name}: {src_rel_path}\n"

    existing = await path_exists(sources_path, location_id)
    if existing:
        try:
            old_content = (await file_read_bytes(sources_path, location_id)).decode()
        except Exception:
            old_content = ""
        await file_write_text(sources_path, old_content + entry, location_id)
    else:
        text = f"Consolidated by File Hunter\nDate: {now_iso}\n\nSources:\n{entry}"
        await file_write_text(sources_path, text, location_id)


async def agent_upload_file(
    dest_dir: str,
    filename: str,
    file_obj,
    file_size: int,
    location_id: int,
    on_progress=None,
):
    """Upload a file to an agent via multipart POST to /upload endpoint.

    Streams from file_obj (file-like) — no full-file read into RAM.
    Progress is reported via on_progress(bytes_sent, total_bytes).
    """
    from file_hunter.extensions import get_agent_proxy

    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")

    result = await proxy(
        "_upload_file",
        location_id,
        dest_dir=dest_dir,
        filename=filename,
        file_obj=file_obj,
        file_size=file_size,
        on_progress=on_progress,
    )
    return result


async def unique_hidden_path(dest_path: str, location_id: int) -> str:
    """Handle collision for hidden files/folders — append .1, .2, etc."""
    if not await path_exists(dest_path, location_id):
        return dest_path
    counter = 1
    while True:
        candidate = f"{dest_path}.{counter}"
        if not await path_exists(candidate, location_id):
            return candidate
        counter += 1


async def unique_dest_path(dest_path: str, location_id: int) -> str:
    """Handle filename collision — append (2), (3), etc."""
    if not await path_exists(dest_path, location_id):
        return dest_path

    base, ext = os.path.splitext(dest_path)
    counter = 2
    while True:
        candidate = f"{base} ({counter}){ext}"
        if not await path_exists(candidate, location_id):
            return candidate
        counter += 1

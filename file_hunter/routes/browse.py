import asyncio
import os

from starlette.requests import Request
from file_hunter.core import json_ok, json_error
from file_hunter.services.browse import get_root_entries, get_children


async def browse(request: Request):
    path = request.query_params.get("path", "").strip()

    if not path:
        entries = await asyncio.to_thread(get_root_entries)
        return json_ok({"path": None, "entries": entries})

    is_dir = await asyncio.to_thread(os.path.isdir, path)
    if not is_dir:
        return json_error(f"Path does not exist or is not a directory: {path}", 400)

    entries = await asyncio.to_thread(get_children, path)
    return json_ok({"path": path, "entries": entries})

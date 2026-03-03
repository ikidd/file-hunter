import asyncio
import logging
import os

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import get_db
from file_hunter.services.scanner import (
    is_scan_running,
    request_cancel,
)
from file_hunter.services.scan_queue import enqueue, dequeue, get_queue_state

logger = logging.getLogger("file_hunter")


async def start_scan(request: Request):
    db = await get_db()
    body = await request.json()
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    # Verify location exists
    rows = await db.execute_fetchall(
        "SELECT id, name, root_path FROM locations WHERE id = ?", (loc_id,)
    )
    if not rows:
        return json_error("Location not found.", 404)

    location_name = rows[0]["name"]
    root_path = rows[0]["root_path"]

    from file_hunter.extensions import is_agent_location, is_agent_scanning

    is_agent = is_agent_location(loc_id)

    # Reject if agent is already scanning this location
    if is_agent and is_agent_scanning(loc_id):
        return json_error(f"Agent is already scanning '{location_name}'.", 409)

    # Check location is reachable before queueing
    if not is_agent:
        if not await asyncio.to_thread(os.path.isdir, root_path):
            return json_error(
                f"Location '{location_name}' is offline — path not accessible.", 400
            )

    # Optional subfolder scan
    scan_path = None
    folder_name = None
    raw_folder_id = body.get("folder_id")
    if raw_folder_id:
        fld_id = int(str(raw_folder_id).replace("fld-", ""))
        fld_rows = await db.execute_fetchall(
            "SELECT id, name, rel_path, location_id FROM folders WHERE id = ?",
            (fld_id,),
        )
        if not fld_rows:
            return json_error("Folder not found.", 404)
        folder = fld_rows[0]
        if folder["location_id"] != loc_id:
            return json_error("Folder does not belong to this location.", 400)
        scan_path = os.path.join(root_path, folder["rel_path"])
        folder_name = folder["name"]
        if not is_agent and not await asyncio.to_thread(os.path.isdir, scan_path):
            return json_error(f"Folder path not accessible: {folder['rel_path']}", 400)

    # Enqueue (rejects duplicates — already running or already queued)
    try:
        entry = await enqueue(
            loc_id,
            location_name,
            root_path,
            scan_path=scan_path,
            folder_name=folder_name,
        )
    except ValueError as exc:
        return json_error(str(exc), 409)

    label = f"{location_name} / {folder_name}" if folder_name else location_name
    return json_ok(
        {"message": f"Scan queued for '{label}'", "queue_id": entry["queue_id"]}
    )


async def cancel_scan(request: Request):
    body = await request.json()

    # Support cancelling a pending queue item by queue_id
    queue_id = body.get("queue_id")
    if queue_id is not None:
        removed = await dequeue(int(queue_id))
        if removed:
            return json_ok({"message": f"Dequeued scan for '{removed['name']}'."})
        return json_error("Queue item not found.", 400)

    # Cancel a running scan by location_id
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    if not is_scan_running(loc_id):
        # Try extension hook for agent-backed locations
        from file_hunter.extensions import get_scan_cancel

        scan_cancel = get_scan_cancel()
        if scan_cancel:
            handled = await scan_cancel(loc_id)
            if handled:
                return json_ok({"message": "Agent scan cancellation requested."})
        return json_error("No scan running for this location.", 400)

    request_cancel(loc_id)
    return json_ok({"message": "Scan cancellation requested."})


async def get_scan_queue(request: Request):
    return json_ok(get_queue_state())

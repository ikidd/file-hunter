"""Per-location scheduled scan loop.

Wakes every 60 seconds and checks if any location with an enabled schedule
is due for a scan.  Uses the existing scan queue so scheduled scans are
indistinguishable from manual ones (full WebSocket progress, status bar, etc.).
"""

import asyncio
import os
from datetime import datetime

from file_hunter.db import get_db, execute_write
from file_hunter.services.scan_queue import enqueue


async def start_scheduler():
    """Start the scheduler loop. Call from on_startup."""
    asyncio.create_task(_scheduler_loop())


async def _scheduler_loop():
    """Wake every 60s, check if any location is due for a scheduled scan."""
    while True:
        await asyncio.sleep(60)
        try:
            await _check_schedules()
        except Exception:
            pass  # don't crash the loop


async def _check_schedules():
    db = await get_db()
    now = datetime.now()
    current_day = now.weekday()  # 0=Mon
    current_time = now.strftime("%H:%M")

    rows = await db.execute_fetchall(
        "SELECT id, name, root_path, scan_schedule_days, scan_schedule_time, "
        "scan_schedule_last_run "
        "FROM locations WHERE scan_schedule_enabled = 1"
    )

    for row in rows:
        days_str = row["scan_schedule_days"]
        if not days_str or not days_str.strip():
            continue
        days = [int(d) for d in days_str.split(",") if d.strip()]
        if current_day not in days:
            continue
        if current_time < row["scan_schedule_time"]:
            continue
        if _already_ran_today(row["scan_schedule_last_run"], now):
            continue
        # Agent-backed locations go through the scan trigger hook, not the queue
        from file_hunter.extensions import is_agent_location, get_scan_trigger

        if is_agent_location(row["id"]):
            scan_trigger = get_scan_trigger()
            if scan_trigger:
                try:
                    handled = await scan_trigger(
                        row["id"], row["name"], row["root_path"], None
                    )
                    if handled:
                        await _update_last_run(row["id"], now)
                except Exception:
                    pass
            continue

        # Check local location is online
        if not await asyncio.to_thread(os.path.isdir, row["root_path"]):
            continue
        # Enqueue via the scan queue — ValueError if already queued/running
        try:
            await enqueue(row["id"], row["name"], row["root_path"])
            await _update_last_run(row["id"], now)
        except ValueError:
            pass  # already queued or already scanning


def _already_ran_today(last_run_iso, now):
    if not last_run_iso:
        return False
    try:
        last_run = datetime.fromisoformat(last_run_iso)
        return last_run.date() == now.date()
    except (ValueError, TypeError):
        return False


async def _update_last_run(location_id, now):
    async def _write(conn, lid, ts):
        await conn.execute(
            "UPDATE locations SET scan_schedule_last_run = ? WHERE id = ?",
            (ts, lid),
        )
        await conn.commit()

    await execute_write(_write, location_id, now.isoformat(timespec="seconds"))

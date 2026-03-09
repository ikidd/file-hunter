"""Ingest agent scan results into the server catalog.

Processes scan_files batches from agents and writes file/folder records
into the database using the core scanner functions.
"""

import json
import logging
import posixpath
from datetime import datetime, timezone

from file_hunter.db import open_connection
from file_hunter.services.scanner import (
    ensure_folder_hierarchy,
    mark_stale_files,
)
from file_hunter.services.stats import invalidate_stats_cache

logger = logging.getLogger("file_hunter")


class AgentScanSession:
    """Tracks state for a single agent scan."""

    def __init__(
        self,
        agent_id,
        location_id,
        location_name,
        scan_id,
        root_path,
        scan_path: str | None = None,
    ):
        self.agent_id = agent_id
        self.location_id = location_id
        self.location_name = location_name
        self.scan_id = scan_id
        self.root_path = root_path
        self.scan_path = scan_path
        self.folder_cache: dict[str, tuple] = {}
        self.files_ingested = 0
        self.potential_matches = 0
        self.affected_hashes: set[str] = set()
        self.db = None


# agent_id -> session
_active_sessions: dict[int, AgentScanSession] = {}


def has_session(agent_id: int) -> bool:
    return agent_id in _active_sessions


def get_session_location(agent_id: int) -> tuple[int, str] | None:
    """Return (location_id, location_name) for an active session, or None."""
    session = _active_sessions.get(agent_id)
    if session:
        return session.location_id, session.location_name
    return None


def get_potential_matches(agent_id: int) -> int:
    """Return running count of potential cross-location matches for this scan."""
    session = _active_sessions.get(agent_id)
    return session.potential_matches if session else 0


def is_location_scanning(location_id: int) -> bool:
    """Check if any active agent scan session covers this location."""
    for agent_id, s in _active_sessions.items():
        if s.location_id == location_id:
            return True
    return False


async def prepare_finalization(agent_id: int, msg: dict) -> dict | None:
    """Persist finalization state to DB so it survives a restart.

    Sets the scan status to 'finalizing' and stores the incremental flag,
    deleted paths list, and scan_prefix. Returns a dict with session info
    for the broadcast, or None if no session exists.
    """
    session = _active_sessions.get(agent_id)
    if not session:
        return None

    db = session.db
    incremental = msg.get("incremental", False)
    deleted = msg.get("deleted")

    scan_prefix = None
    if session.scan_path:
        scan_prefix = posixpath.relpath(session.scan_path, session.root_path)

    deleted_json = json.dumps(deleted) if deleted is not None else None

    await db.execute(
        """UPDATE scans SET status='finalizing',
           scan_prefix=?, incremental=?, deleted_json=?
           WHERE id=?""",
        (scan_prefix, 1 if incremental else 0, deleted_json, session.scan_id),
    )
    await db.commit()

    return {
        "location_id": session.location_id,
        "location_name": session.location_name,
        "scan_id": session.scan_id,
        "files_ingested": session.files_ingested,
        "scan_prefix": scan_prefix,
    }


async def start_session(agent_id: int, path: str):
    """Start a scan ingestion session for an agent."""
    if agent_id in _active_sessions:
        return

    db = await open_connection()

    row = await db.execute_fetchall(
        "SELECT id, name, root_path FROM locations WHERE agent_id = ? AND root_path = ?",
        (agent_id, path),
    )
    if not row:
        row = await db.execute_fetchall(
            "SELECT id, name, root_path FROM locations WHERE agent_id = ?",
            (agent_id,),
        )
        matched = None
        for r in row:
            if path.startswith(r["root_path"]) or r["root_path"].startswith(path):
                matched = r
                break
        if not matched:
            logger.warning(
                "Agent #%d scan path '%s' has no matching location", agent_id, path
            )
            await db.close()
            return
        row = [matched]

    location_id = row[0]["id"]
    location_name = row[0]["name"]
    root_path = row[0]["root_path"]

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cursor = await db.execute(
        "INSERT INTO scans (location_id, status, started_at) VALUES (?, 'running', ?)",
        (location_id, now_iso),
    )
    scan_id = cursor.lastrowid
    await db.commit()

    actual_scan_path = path if path != root_path else None
    session = AgentScanSession(
        agent_id,
        location_id,
        location_name,
        scan_id,
        root_path,
        scan_path=actual_scan_path,
    )
    session.db = db
    _active_sessions[agent_id] = session

    logger.info(
        "Agent #%d scan session started for location #%d (%s), scan_id=%d",
        agent_id,
        location_id,
        root_path,
        scan_id,
    )


async def ingest_batch(agent_id: int, files: list[dict]):
    """Ingest a batch of file records from an agent scan.

    Uses bulk DB operations: one SELECT to find existing files, then
    executemany for updates and inserts. Folder hierarchy resolution
    is per-file but almost always hits the in-memory cache.
    """
    session = _active_sessions.get(agent_id)
    if not session:
        return

    db = session.db
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Phase 1: Resolve folder hierarchy (mostly cached after first batch)
    prepared = []
    for f in files:
        rel_path = f.get("rel_path", "")
        if not rel_path:
            continue

        parts = rel_path.replace("\\", "/").rsplit("/", 1)
        if len(parts) == 2:
            rel_dir = parts[0]
            folder_id, folder_dup_exclude = await ensure_folder_hierarchy(
                db, session.location_id, rel_dir, session.folder_cache
            )
        else:
            folder_id = None
            folder_dup_exclude = 0

        prepared.append((f, rel_path, folder_id, folder_dup_exclude))

    if not prepared:
        return

    # Phase 2: Batch lookup existing files
    existing_map = {}
    rel_paths = [p[1] for p in prepared]
    for i in range(0, len(rel_paths), 500):
        batch_paths = rel_paths[i : i + 500]
        placeholders = ",".join("?" * len(batch_paths))
        rows = await db.execute_fetchall(
            f"SELECT id, rel_path FROM files WHERE location_id = ? AND rel_path IN ({placeholders})",
            [session.location_id] + batch_paths,
        )
        for r in rows:
            existing_map[r["rel_path"]] = r["id"]

    # Phase 3: Build parameter lists for batch UPDATE and INSERT
    update_params = []
    insert_params = []
    for f, rel_path, folder_id, folder_dup_exclude in prepared:
        hs = f.get("hash_strong")
        if hs:
            session.affected_hashes.add(hs)

        file_id = existing_map.get(rel_path)
        if file_id is not None:
            update_params.append(
                (
                    f.get("filename", ""),
                    f.get("full_path", ""),
                    folder_id,
                    f.get("file_type_high", ""),
                    f.get("file_type_low", ""),
                    f.get("file_size", 0),
                    f.get("hash_partial"),
                    f.get("hash_fast"),
                    hs,
                    f.get("created_date", ""),
                    f.get("modified_date", ""),
                    now_iso,
                    session.scan_id,
                    f.get("hidden", 0),
                    folder_dup_exclude,
                    file_id,
                )
            )
        else:
            insert_params.append(
                (
                    f.get("filename", ""),
                    f.get("full_path", ""),
                    rel_path,
                    session.location_id,
                    folder_id,
                    f.get("file_type_high", ""),
                    f.get("file_type_low", ""),
                    f.get("file_size", 0),
                    f.get("hash_partial"),
                    f.get("hash_fast"),
                    hs,
                    f.get("created_date", ""),
                    f.get("modified_date", ""),
                    now_iso,
                    now_iso,
                    session.scan_id,
                    f.get("hidden", 0),
                    folder_dup_exclude,
                )
            )

    session.files_ingested += len(update_params) + len(insert_params)

    # Phase 4: Execute batch operations
    if update_params:
        await db.executemany(
            """UPDATE files SET
                filename=?, full_path=?, folder_id=?,
                file_type_high=?, file_type_low=?, file_size=?,
                hash_partial=COALESCE(?, hash_partial),
                hash_fast=COALESCE(?, hash_fast),
                hash_strong=COALESCE(?, hash_strong),
                created_date=?, modified_date=?,
                date_last_seen=?, scan_id=?, stale=0, hidden=?, dup_exclude=?
               WHERE id=?""",
            update_params,
        )

    if insert_params:
        await db.executemany(
            """INSERT INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                hash_partial, hash_fast, hash_strong,
                description, tags,
                created_date, modified_date, date_cataloged, date_last_seen,
                scan_id, stale, hidden, dup_exclude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, ?, 0, ?, ?)""",
            insert_params,
        )

    if insert_params:
        await db.execute(
            "UPDATE locations SET backfill_needed = 1 WHERE id = ?",
            (session.location_id,),
        )

    await db.commit()

    # Cross-location match detection
    candidates = [
        (f.get("file_size", 0), f.get("hash_partial"))
        for f in files
        if f.get("hash_partial")
    ]
    if candidates:
        conditions = " OR ".join(
            "(file_size = ? AND hash_partial = ?)" for _ in candidates
        )
        params = []
        for size, hp in candidates:
            params.extend([size, hp])
        params.append(session.location_id)
        rows = await db.execute_fetchall(
            f"""SELECT DISTINCT file_size, hash_partial FROM files
                WHERE ({conditions})
                  AND location_id != ?
                  AND stale = 0""",
            params,
        )
        if rows:
            matched_pairs = {(r["file_size"], r["hash_partial"]) for r in rows}
            new_matches = sum(
                1 for size, hp in candidates if (size, hp) in matched_pairs
            )
            session.potential_matches += new_matches


async def complete_session(agent_id: int) -> int:
    """Finalize a completed agent scan (slow: stale marking, size recalc).

    Reads incremental/deleted/scan_prefix from the scans table (persisted
    by prepare_finalization) so the data survives server restarts.
    Returns files ingested count.
    """
    session = _active_sessions.pop(agent_id, None)
    if not session:
        logger.warning("complete_session: no session for agent #%d", agent_id)
        return 0

    db = session.db
    try:
        stale_count = await _finalize_scan_record(
            db, session.scan_id, session.location_id, session.files_ingested
        )

        logger.info(
            "Agent #%d scan completed: %d files ingested, %d stale",
            agent_id,
            session.files_ingested,
            stale_count,
        )

        # Fire-and-forget on own connections — never block the event loop
        from file_hunter.services.sizes import schedule_size_recalc

        schedule_size_recalc(session.location_id)

        if session.affected_hashes:
            from file_hunter.services.dup_counts import schedule_dup_recalc

            schedule_dup_recalc(session.affected_hashes, session.location_name)

        return session.files_ingested
    finally:
        await db.close()


async def _finalize_scan_record(
    db, scan_id: int, location_id: int, files_ingested: int
) -> int:
    """Mark stale files and update scan to 'completed'. Shared by normal
    flow and startup recovery. Returns stale count.
    """
    # Read persisted finalization params
    row = await db.execute_fetchall(
        "SELECT scan_prefix, incremental, deleted_json FROM scans WHERE id=?",
        (scan_id,),
    )
    if not row:
        return 0

    scan_prefix = row[0]["scan_prefix"]
    incremental = bool(row[0]["incremental"])
    deleted_json = row[0]["deleted_json"]

    stale_count = 0
    if incremental and deleted_json:
        deleted = json.loads(deleted_json)
        if deleted:
            for i in range(0, len(deleted), 500):
                batch = deleted[i : i + 500]
                placeholders = ",".join("?" * len(batch))
                cursor = await db.execute(
                    f"""UPDATE files SET stale=1
                        WHERE location_id=? AND stale=0
                        AND rel_path IN ({placeholders})""",
                    [location_id] + batch,
                )
                stale_count += cursor.rowcount
            await db.commit()
        logger.info(
            "Incremental stale: %d files marked stale from deleted list",
            stale_count,
        )
    else:
        stale_count = await mark_stale_files(db, location_id, scan_id, scan_prefix)

    completed_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    await db.execute(
        """UPDATE scans SET status='completed', completed_at=?,
           files_found=?, files_hashed=?, stale_files=?,
           deleted_json=NULL
           WHERE id=?""",
        (completed_iso, files_ingested, files_ingested, stale_count, scan_id),
    )
    await db.execute(
        "UPDATE locations SET date_last_scanned=? WHERE id=?",
        (completed_iso, location_id),
    )
    await db.commit()

    invalidate_stats_cache()
    return stale_count


async def resume_finalizing_scans():
    """On startup, complete any scans left in 'finalizing' state."""
    from file_hunter.db import get_db

    db_check = await get_db()
    rows = await db_check.execute_fetchall(
        "SELECT id, location_id FROM scans WHERE status = 'finalizing'"
    )
    if not rows:
        return

    logger.info("Resuming %d finalizing scan(s) from previous session", len(rows))

    for row in rows:
        scan_id = row["id"]
        location_id = row["location_id"]
        db = await open_connection()
        try:
            # Count files ingested from this scan
            count_row = await db.execute_fetchall(
                "SELECT COUNT(*) as c FROM files WHERE scan_id = ? AND location_id = ?",
                (scan_id, location_id),
            )
            files_ingested = count_row[0]["c"] if count_row else 0

            stale_count = await _finalize_scan_record(
                db, scan_id, location_id, files_ingested
            )
            logger.info(
                "Resumed finalizing scan #%d: location #%d, %d files, %d stale",
                scan_id,
                location_id,
                files_ingested,
                stale_count,
            )

            from file_hunter.services.sizes import schedule_size_recalc

            schedule_size_recalc(location_id)
        except Exception:
            logger.error("Failed to resume finalizing scan #%d", scan_id, exc_info=True)
        finally:
            await db.close()


async def cancel_session(agent_id: int):
    """Clean up a cancelled agent scan session (user-initiated cancel)."""
    session = _active_sessions.pop(agent_id, None)
    if not session:
        return

    db = session.db
    try:
        completed_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        await db.execute(
            "UPDATE scans SET status='cancelled', completed_at=? WHERE id=?",
            (completed_iso, session.scan_id),
        )
        await db.commit()
        invalidate_stats_cache()
        logger.info("Agent #%d scan cancelled", agent_id)
    finally:
        await db.close()


async def interrupt_session(agent_id: int):
    """Clean up an interrupted agent scan session (agent disconnect/restart).

    Unlike cancel_session, this marks the scan as 'interrupted' so it can
    be distinguished from user-initiated cancellations.
    """
    session = _active_sessions.pop(agent_id, None)
    if not session:
        return

    db = session.db
    try:
        completed_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        await db.execute(
            "UPDATE scans SET status='interrupted', completed_at=? WHERE id=?",
            (completed_iso, session.scan_id),
        )
        await db.commit()
        invalidate_stats_cache()
        logger.info("Agent #%d scan interrupted (agent disconnected)", agent_id)
    finally:
        await db.close()


async def error_session(agent_id: int, error: str):
    """Clean up a failed agent scan session."""
    session = _active_sessions.pop(agent_id, None)
    if not session:
        return

    db = session.db
    try:
        await db.execute(
            "UPDATE scans SET status='error', error=? WHERE id=?",
            (error, session.scan_id),
        )
        await db.commit()
        invalidate_stats_cache()
        logger.warning("Agent #%d scan error: %s", agent_id, error)
    finally:
        await db.close()

"""Stored dup_count maintenance — recalculate and backfill.

Write serialization: all dup recalc work is submitted to a shared queue
and processed by a single long-lived background task. All writes go
through the single db_writer() connection — no independent writers.
"""

import asyncio
import logging
from collections import defaultdict

from file_hunter.db import db_writer, get_db
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

RECALC_BATCH = 200

# Coalesced writer state — single background task
_recalc_queue: asyncio.Queue | None = None
_writer_task: asyncio.Task | None = None


async def _batched_recalc(hashes, *, on_progress=None):
    """Recalculate dup_count for files sharing the given hashes.

    Uses batched GROUP BY for counts (one query per batch of RECALC_BATCH
    hashes) and grouped UPDATEs by dup_count value. Yields between batches.

    Reads via get_db(), writes via db_writer() per batch.

    Returns the number of hashes processed.
    """
    hash_list = list(hashes)
    total = len(hash_list)
    processed = 0
    db = await get_db()

    for i in range(0, total, RECALC_BATCH):
        batch = hash_list[i : i + RECALC_BATCH]
        ph = ",".join("?" for _ in batch)

        # One GROUP BY query gives counts for the whole batch (read)
        rows = await db.execute_fetchall(
            f"""SELECT hash_strong, COUNT(*) as cnt FROM files
                WHERE hash_strong IN ({ph})
                  AND stale = 0 AND hidden = 0 AND dup_exclude = 0
                GROUP BY hash_strong""",
            batch,
        )
        count_map = {r["hash_strong"]: r["cnt"] for r in rows}

        # Group hashes by their dup_count value for batched UPDATEs
        by_dup_count = defaultdict(list)
        zero_hashes = []
        for h in batch:
            cnt = count_map.get(h, 0)
            dc = cnt - 1 if cnt > 1 else 0
            by_dup_count[dc].append(h)
            if dc == 0:
                zero_hashes.append(h)

        # Write phase: db_writer() per batch
        async with db_writer() as wdb:
            # Batched UPDATE per dup_count value (active files)
            for dc, dc_hashes in by_dup_count.items():
                dc_ph = ",".join("?" for _ in dc_hashes)
                await wdb.execute(
                    f"UPDATE files SET dup_count = ? "
                    f"WHERE hash_strong IN ({dc_ph}) "
                    f"AND stale = 0 AND hidden = 0 AND dup_exclude = 0",
                    [dc] + dc_hashes,
                )

            # Zero out inactive files (stale/hidden/excluded) for hashes with no dups
            if zero_hashes:
                z_ph = ",".join("?" for _ in zero_hashes)
                await wdb.execute(
                    f"UPDATE files SET dup_count = 0 "
                    f"WHERE hash_strong IN ({z_ph}) "
                    f"AND (stale = 1 OR hidden = 1 OR dup_exclude = 1)",
                    zero_hashes,
                )

        processed += len(batch)

        if on_progress:
            await on_progress(processed, total)

        await asyncio.sleep(0.05)

    return processed


def submit_hashes_for_recalc(
    hashes: set[str], source: str = "", location_ids: set[int] | None = None
):
    """Submit hashes to the coalesced dup recalc writer.

    Non-blocking. Hashes are merged with any pending work and processed
    by a single background task on one DB connection. Safe to call from
    any context — scan loops, route handlers, backfill tasks.

    Also updates stored locations.duplicate_count for all affected
    locations after recalculating files.dup_count.
    """
    global _recalc_queue, _writer_task
    hashes = {h for h in hashes if h}
    if not hashes:
        return
    if _recalc_queue is None:
        _recalc_queue = asyncio.Queue()
    _recalc_queue.put_nowait((hashes, source, location_ids or set()))
    if _writer_task is None or _writer_task.done():
        _writer_task = asyncio.create_task(_dup_recalc_writer())


async def stop_writer():
    """Wait for the dup recalc writer to finish current work, then stop it."""
    global _writer_task
    if _writer_task and not _writer_task.done():
        log.info("Waiting for dup recalc writer to complete...")
        try:
            await asyncio.wait_for(_writer_task, timeout=10)
        except asyncio.TimeoutError:
            _writer_task.cancel()
            try:
                await _writer_task
            except (asyncio.CancelledError, Exception):
                pass
    _writer_task = None


async def _dup_recalc_writer():
    """Single long-lived task that drains the hash queue.

    Processes work items, coalesces rapid submissions into larger batches.
    All writes go through db_writer(). Shuts down after 10s idle.
    """
    try:
        while True:
            # Wait for work (shut down after 10s idle)
            try:
                item = await asyncio.wait_for(_recalc_queue.get(), timeout=10.0)
            except asyncio.TimeoutError:
                break

            # Coalesce: drain any additional items that accumulated
            merged_hashes: set[str] = set(item[0])
            merged_sources: list[str] = [item[1]] if item[1] else []
            merged_location_ids: set[int] = set(item[2])

            while not _recalc_queue.empty():
                try:
                    more = _recalc_queue.get_nowait()
                    merged_hashes.update(more[0])
                    if more[1]:
                        merged_sources.append(more[1])
                    merged_location_ids.update(more[2])
                except asyncio.QueueEmpty:
                    break

            source_label = ", ".join(merged_sources[:3])
            if len(merged_sources) > 3:
                source_label += f" +{len(merged_sources) - 3}"

            log.info(
                "Coalesced dup recalc: %d hashes (%s)",
                len(merged_hashes),
                source_label or "unknown",
            )

            # Recalculate dup_count on files
            await recalculate_dup_counts(merged_hashes, source=source_label)

            # Update stored duplicate_count for affected locations
            db = await get_db()
            h_list = list(merged_hashes)
            h_ph = ",".join("?" for _ in h_list)
            rows = await db.execute_fetchall(
                f"SELECT DISTINCT location_id FROM files WHERE hash_strong IN ({h_ph})",
                h_list,
            )
            affected = {r["location_id"] for r in rows}
            affected |= merged_location_ids

            # Read counts per location, then batch-write
            loc_updates = []
            for lid in affected:
                dc_rows = await db.execute_fetchall(
                    "SELECT COUNT(*) as c FROM files "
                    "WHERE location_id = ? AND stale = 0 AND hidden = 0 "
                    "AND dup_exclude = 0 AND dup_count > 0",
                    (lid,),
                )
                loc_updates.append((dc_rows[0]["c"], lid))

            async with db_writer() as wdb:
                for dc, lid in loc_updates:
                    await wdb.execute(
                        "UPDATE locations SET duplicate_count = ? WHERE id = ?",
                        (dc, lid),
                    )

            invalidate_stats_cache()

            await broadcast(
                {"type": "dup_recalc_completed", "hashCount": len(merged_hashes)}
            )
    except Exception:
        log.error("Coalesced dup recalc writer failed", exc_info=True)


async def batch_dup_counts(db, hashes: list[str]) -> dict[str, int]:
    """Return live dup counts for a batch of hash_strong values.

    Returns {hash_strong: count} where count = total non-stale files - 1.
    Only includes hashes with count > 0.  Designed for page-sized batches
    (~120 items) so always fast.
    """
    unique = {h for h in hashes if h}
    if not unique:
        return {}
    placeholders = ",".join("?" for _ in unique)
    rows = await db.execute_fetchall(
        f"""SELECT hash_strong, COUNT(*) as cnt FROM files
            WHERE hash_strong IN ({placeholders}) AND stale = 0 AND hidden = 0 AND dup_exclude = 0
            GROUP BY hash_strong HAVING COUNT(*) > 1""",
        list(unique),
    )
    return {r["hash_strong"]: r["cnt"] - 1 for r in rows}


async def recalculate_dup_counts(hashes: set[str], source: str = ""):
    """Recalculate dup_count for all files sharing the given hashes.

    Uses batched GROUP BY queries with yields between batches.
    All writes go through db_writer(). Safe to call with an empty set (no-op).
    """
    if not hashes:
        return
    hashes = {h for h in hashes if h}
    if not hashes:
        return
    log.info("recalculate_dup_counts: %d hashes (%s)", len(hashes), source or "inline")

    async def _on_progress(processed, total):
        if processed % 1000 < RECALC_BATCH:
            log.info(
                "recalculate_dup_counts: %d/%d hashes (%s)",
                processed,
                total,
                source or "inline",
            )

    await _batched_recalc(hashes, on_progress=_on_progress)


async def backfill_dup_counts():
    """Backfill dup_count for all files on startup.

    Reads via get_db(), writes via db_writer() (inside _batched_recalc).
    Skips if no files have stale dup_counts (quick consistency check).
    """
    db = await get_db()
    try:
        # Quick check: any file with dup_count=0 that actually has duplicates?
        stale = await db.execute_fetchall(
            """SELECT 1 FROM files f
               WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_strong = f.hash_strong
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )
               LIMIT 1"""
        )
        if not stale:
            log.info("dup_count backfill: counts consistent, skipping")
            await broadcast({"type": "dup_backfill_completed", "skipped": True})
            return

        # Find which locations have stale counts
        stale_locs = await db.execute_fetchall(
            """SELECT DISTINCT l.name
               FROM files f
               JOIN locations l ON l.id = f.location_id
               WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_strong = f.hash_strong
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )"""
        )
        stale_loc_names = [r["name"] for r in stale_locs]

        # Only recalculate hashes that are actually wrong, not all hash groups
        dup_rows = await db.execute_fetchall(
            """SELECT DISTINCT f.hash_strong
               FROM files f
               WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_strong = f.hash_strong
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )"""
        )
        stale_hashes = {r["hash_strong"] for r in dup_rows}

        # Also find hashes where dup_count > 0 but no longer have duplicates
        false_positive_rows = await db.execute_fetchall(
            """SELECT DISTINCT f.hash_strong
               FROM files f
               WHERE f.dup_count > 0 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0
                 AND NOT EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_strong = f.hash_strong
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )"""
        )
        false_positive_hashes = {r["hash_strong"] for r in false_positive_rows}

        all_stale = stale_hashes | false_positive_hashes
        total_hashes = len(all_stale)

        loc_label = ", ".join(stale_loc_names[:5])
        if len(stale_loc_names) > 5:
            loc_label += f" + {len(stale_loc_names) - 5} more"
        log.info(
            "dup_count backfill: %d stale hashes across %d locations (%s)",
            total_hashes,
            len(stale_loc_names),
            loc_label,
        )
        await broadcast(
            {
                "type": "dup_backfill_started",
                "totalHashes": total_hashes,
                "locations": stale_loc_names,
            }
        )

        async def _on_progress(processed, total):
            if processed % 10000 < RECALC_BATCH:
                log.info("dup_count backfill: %d / %d hashes", processed, total)
                await broadcast(
                    {
                        "type": "dup_backfill_progress",
                        "processed": processed,
                        "totalHashes": total,
                    }
                )

        updated = await _batched_recalc(all_stale, on_progress=_on_progress)

        invalidate_stats_cache()

        log.info(
            "dup_count backfill: complete, fixed %d hashes",
            updated,
        )
        await broadcast({"type": "dup_backfill_completed", "updated": updated})

    except Exception:
        log.exception("dup_count backfill failed")

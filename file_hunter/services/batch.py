"""Batch operations — delete, move, tag, and download multiple items."""

import asyncio
import io
import os
import zipfile

from file_hunter.services.delete import (
    delete_file,
    delete_file_and_duplicates,
    delete_folder,
)
from file_hunter.services.files import move_file, update_file


async def batch_delete(
    db, file_ids: list[int], folder_ids: list[int], all_duplicates: bool = False
) -> dict:
    """Delete multiple files and folders from disk and catalog."""
    deleted_files = 0
    deleted_folders = 0
    deleted_from_disk = 0

    delete_fn = delete_file_and_duplicates if all_duplicates else delete_file

    # Delete folders first (they may contain some of the listed files)
    for fid in folder_ids:
        result = await delete_folder(db, fid)
        if result:
            deleted_folders += 1
            if result.get("deleted_from_disk"):
                deleted_from_disk += 1

    # Delete individual files
    for fid in file_ids:
        result = await delete_fn(db, fid)
        if result:
            deleted_files += 1
            if result.get("deleted_from_disk"):
                deleted_from_disk += 1

    return {
        "deleted_files": deleted_files,
        "deleted_folders": deleted_folders,
        "deleted_from_disk": deleted_from_disk,
    }


async def batch_move(
    db, file_ids: list[int], folder_ids: list[int], destination_folder_id: str
) -> dict:
    """Move multiple files and folders to a destination."""
    from file_hunter.services.locations import move_folder

    moved_files = 0
    moved_folders = 0
    errors = []

    # Move folders
    for fid in folder_ids:
        try:
            await move_folder(db, fid, destination_folder_id)
            moved_folders += 1
        except ValueError as e:
            errors.append(f"Folder {fid}: {e}")

    # Move files
    for fid in file_ids:
        try:
            await move_file(db, fid, destination_folder_id=destination_folder_id)
            moved_files += 1
        except ValueError as e:
            errors.append(f"File {fid}: {e}")

    return {
        "moved_files": moved_files,
        "moved_folders": moved_folders,
        "errors": errors,
    }


async def batch_tag(
    db, file_ids: list[int], add_tags: list[str], remove_tags: list[str]
) -> dict:
    """Add/remove tags on multiple files."""
    updated = 0

    for fid in file_ids:
        row = await db.execute_fetchall("SELECT tags FROM files WHERE id = ?", (fid,))
        if not row:
            continue

        current_raw = row[0]["tags"] or ""
        current = [t.strip() for t in current_raw.split(",") if t.strip()]

        # Add new tags (avoid duplicates)
        for tag in add_tags:
            if tag not in current:
                current.append(tag)

        # Remove tags
        for tag in remove_tags:
            current = [t for t in current if t != tag]

        await update_file(db, fid, tags=current)
        updated += 1

    return {"updated": updated}


def _read_file_bytes(path):
    """Read entire file contents. Called via asyncio.to_thread()."""
    with open(path, "rb") as f:
        return f.read()


async def _fetch_file_data(full_path, location_id):
    """Read file bytes — agent locations always proxy, local reads from disk."""
    from file_hunter.extensions import is_agent_location, get_fetch_bytes

    if is_agent_location(location_id):
        fetch_bytes = get_fetch_bytes()
        if fetch_bytes:
            return await fetch_bytes(full_path, location_id)
        return None
    if await asyncio.to_thread(os.path.isfile, full_path):
        return await asyncio.to_thread(_read_file_bytes, full_path)
    return None


async def batch_download(db, file_ids: list[int], folder_ids: list[int]) -> io.BytesIO:
    """Build a ZIP of selected files and folder contents. Returns a BytesIO buffer."""
    # Collect all file paths: direct files + recursive folder contents
    all_files = []

    # Direct files
    if file_ids:
        placeholders = ",".join("?" * len(file_ids))
        rows = await db.execute_fetchall(
            f"""SELECT f.full_path, f.filename, f.location_id, l.name as loc_name
                FROM files f JOIN locations l ON l.id = f.location_id
                WHERE f.id IN ({placeholders})""",
            file_ids,
        )
        for r in rows:
            all_files.append((r["full_path"], r["filename"], r["location_id"]))

    # Folder contents (recursive)
    for fid in folder_ids:
        # Get folder info
        frow = await db.execute_fetchall(
            """SELECT fld.name, fld.rel_path, fld.location_id, l.root_path
               FROM folders fld JOIN locations l ON l.id = fld.location_id
               WHERE fld.id = ?""",
            (fid,),
        )
        if not frow:
            continue
        folder_name = frow[0]["name"]
        folder_rel = frow[0]["rel_path"]
        folder_loc_id = frow[0]["location_id"]

        # Recursive CTE for all descendant folders
        desc_rows = await db.execute_fetchall(
            """WITH RECURSIVE desc(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
               )
               SELECT id FROM desc""",
            (fid,),
        )
        desc_ids = [r["id"] for r in desc_rows]

        placeholders = ",".join("?" * len(desc_ids))
        files = await db.execute_fetchall(
            f"SELECT full_path, rel_path FROM files WHERE folder_id IN ({placeholders})",
            desc_ids,
        )

        prefix = folder_rel + "/" if folder_rel else ""
        for f in files:
            arc_name = f["rel_path"]
            if prefix and arc_name.startswith(prefix):
                arc_name = arc_name[len(prefix) :]
            # Nest under folder name
            arc_name = folder_name + "/" + arc_name
            all_files.append((f["full_path"], arc_name, folder_loc_id))

    buf = io.BytesIO()
    zf = zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED)
    for full_path, arc_name, loc_id in all_files:
        data = await _fetch_file_data(full_path, loc_id)
        if data:
            zf.writestr(arc_name, data)
    zf.close()
    buf.seek(0)

    return buf

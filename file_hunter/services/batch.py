"""Batch operations — delete, move, tag, and download multiple items."""

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


async def batch_collect_files(
    db, file_ids: list[int], folder_ids: list[int]
) -> list[tuple[str, str, int]]:
    """Collect file paths for a batch download.

    Returns list of (full_path, arc_name, location_id).
    """
    all_files: list[tuple[str, str, int]] = []

    # Direct files
    if file_ids:
        placeholders = ",".join("?" * len(file_ids))
        rows = await db.execute_fetchall(
            f"""SELECT f.full_path, f.filename, f.location_id
                FROM files f
                WHERE f.id IN ({placeholders})""",
            file_ids,
        )
        for r in rows:
            all_files.append((r["full_path"], r["filename"], r["location_id"]))

    # Folder contents (recursive)
    for fid in folder_ids:
        frow = await db.execute_fetchall(
            """SELECT fld.name, fld.rel_path, fld.location_id
               FROM folders fld
               WHERE fld.id = ?""",
            (fid,),
        )
        if not frow:
            continue
        folder_name = frow[0]["name"]
        folder_rel = frow[0]["rel_path"]
        folder_loc_id = frow[0]["location_id"]

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
            arc_name = folder_name + "/" + arc_name
            all_files.append((f["full_path"], arc_name, folder_loc_id))

    return all_files


async def build_streaming_zip(files, zip_name):
    """Build a ZIP from [(full_path, arc_name, location_id)] and return a StreamingResponse.

    Each file is streamed from its agent in chunks and written to the ZIP entry
    incrementally. The ZIP is built in a temp file to avoid accumulating the
    entire archive in memory.
    """
    import tempfile

    from starlette.responses import StreamingResponse

    from file_hunter.services.content_proxy import stream_agent_file

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(tmp_fd)

    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_STORED) as zf:
            for full_path, arc_name, loc_id in files:
                async with stream_agent_file(full_path, loc_id) as chunks:
                    if chunks is None:
                        continue
                    with zf.open(arc_name, "w", force_zip64=True) as entry:
                        async for chunk in chunks:
                            entry.write(chunk)
    except Exception:
        os.unlink(tmp_path)
        raise

    file_size = os.path.getsize(tmp_path)

    async def _stream_and_cleanup():
        try:
            with open(tmp_path, "rb") as f:
                while True:
                    chunk = f.read(1048576)
                    if not chunk:
                        break
                    yield chunk
        finally:
            os.unlink(tmp_path)

    return StreamingResponse(
        _stream_and_cleanup(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{zip_name}"',
            "Content-Length": str(file_size),
        },
    )

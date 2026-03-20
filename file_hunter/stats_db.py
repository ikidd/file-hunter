"""Stats database — separate SQLite file for real-time counters.

Own writer, own read connections. Never contends with the catalog writer
or hashes writer. Counter updates cascade through the folder tree as a
background operation — the UI reads current values without blocking ingest.

Counters are maintained incrementally during scan/import/delete operations.
The correction pass (recalculate_location_sizes) becomes a repair tool,
not a required step in every scan.
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from file_hunter.config import load_config

_write_db = None
_write_lock = asyncio.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS folder_stats (
    folder_id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    type_counts TEXT NOT NULL DEFAULT '{}',
    hidden_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS location_stats (
    location_id INTEGER PRIMARY KEY,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    type_counts TEXT NOT NULL DEFAULT '{}',
    hidden_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_folder_stats_location
    ON folder_stats(location_id);
"""


def _stats_db_path() -> Path:
    config = load_config()
    catalog_path = Path(config.get("database", "data/file_hunter.db"))
    if not catalog_path.is_absolute():
        catalog_path = Path(__file__).resolve().parent.parent / catalog_path
    return catalog_path.parent / "stats.db"


async def init_stats_db():
    """Create stats.db and schema if it doesn't exist.

    Called during app startup. Fast — just CREATE TABLE IF NOT EXISTS.
    No data migration.
    """
    db_path = _stats_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("PRAGMA journal_mode=WAL")
        for stmt in _SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)
        await conn.commit()
    finally:
        await conn.close()


async def _get_write_db() -> aiosqlite.Connection:
    """Lazy-init the single stats write connection."""
    global _write_db
    if _write_db is None:
        db_path = _stats_db_path()
        _write_db = await aiosqlite.connect(db_path)
        _write_db.row_factory = aiosqlite.Row
        await _write_db.execute("PRAGMA journal_mode=WAL")
    return _write_db


@asynccontextmanager
async def stats_writer():
    """Acquire exclusive write access to the stats database.

    Same pattern as catalog db_writer() — own lock, own connection.
    Auto-commits on clean exit; rolls back on exception.
    """
    async with _write_lock:
        db = await _get_write_db()
        try:
            yield db
            await db.commit()
        except BaseException:
            try:
                await db.rollback()
            except Exception:
                pass
            raise


async def open_stats_connection() -> aiosqlite.Connection:
    """Open a read-only stats DB connection (caller must close it)."""
    db_path = _stats_db_path()
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    return conn


@asynccontextmanager
async def read_stats():
    """Open a stats read connection, yield it, close on exit.

    Usage:
        async with read_stats() as db:
            rows = await db.execute_fetchall("SELECT ...")
    """
    conn = await open_stats_connection()
    try:
        yield conn
    finally:
        await conn.close()


async def close_stats_db():
    """Close the stats write connection. Called on shutdown."""
    global _write_db
    if _write_db is not None:
        await _write_db.close()
        _write_db = None

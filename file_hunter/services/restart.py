import asyncio
import os
import sys


def schedule_restart(delay: float = 1.0):
    """Schedule a server restart after a short delay (so the HTTP response sends first)."""
    loop = asyncio.get_event_loop()
    loop.call_later(delay, _restart)


def _restart():
    """Replace the current process with a fresh instance using the same args."""
    os.execv(sys.executable, [sys.executable] + sys.argv)

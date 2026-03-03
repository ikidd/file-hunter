"""Extension registry for pro/plugin packages.

The pro package (file_hunter_pro) calls these functions from its register()
entry point to inject routes, static mounts, and startup hooks into the core
app before Starlette is constructed.
"""

_extra_routes = []
_extra_startup = []
_static_dirs = {}  # mount_path -> directory
_public_ws_paths = set()  # WS paths that handle their own auth


def add_routes(routes):
    """Append Starlette Route objects to the app route list."""
    _extra_routes.extend(routes)


def add_startup(fn):
    """Register an async callable to run during app startup."""
    _extra_startup.append(fn)


def add_static(path, directory):
    """Register a static file mount (path -> directory)."""
    _static_dirs[path] = directory


def get_routes():
    return list(_extra_routes)


def get_startup_hooks():
    return list(_extra_startup)


def get_static_mounts():
    return dict(_static_dirs)


def add_public_ws_path(path):
    """Register a WebSocket path that bypasses auth (handles its own validation)."""
    _public_ws_paths.add(path)


def get_public_ws_paths():
    return set(_public_ws_paths)


# ---------------------------------------------------------------------------
# Extension hooks — allow pro/plugins to override core behaviour
# ---------------------------------------------------------------------------

_online_check_fn = None  # (location_dict) -> bool | None
_scan_trigger_fn = None  # (location_id, name, root_path, scan_path) -> bool
_scan_cancel_fn = None  # (location_id) -> bool
_content_proxy_fn = None  # (file_id, full_path, filename) -> Response | None
_fetch_bytes_fn = None  # (full_path, location_id) -> bytes | None
_agent_proxy_fn = None  # async (op, location_id, **kw) -> Any
_agent_location_ids_fn = None  # () -> set[int]
_agent_label_prefixes_fn = None  # () -> dict[int, str]  (location_id -> agent_name)
_agent_scanning_fn = None  # (location_id) -> bool
_disk_stats_fn = None  # async (location_id, root_path) -> dict | None
_location_changed_fn = None  # async (action, loc_id, **kwargs) -> None
_agent_status_fn = None  # async (location_id) -> dict | None


def set_online_check(fn):
    global _online_check_fn
    _online_check_fn = fn


def get_online_check():
    return _online_check_fn


def set_scan_trigger(fn):
    global _scan_trigger_fn
    _scan_trigger_fn = fn


def get_scan_trigger():
    return _scan_trigger_fn


def set_scan_cancel(fn):
    global _scan_cancel_fn
    _scan_cancel_fn = fn


def get_scan_cancel():
    return _scan_cancel_fn


def set_content_proxy(fn):
    global _content_proxy_fn
    _content_proxy_fn = fn


def get_content_proxy():
    return _content_proxy_fn


def set_fetch_bytes(fn):
    global _fetch_bytes_fn
    _fetch_bytes_fn = fn


def get_fetch_bytes():
    return _fetch_bytes_fn


def set_agent_proxy(fn):
    global _agent_proxy_fn
    _agent_proxy_fn = fn


def get_agent_proxy():
    return _agent_proxy_fn


def set_agent_location_ids(fn):
    global _agent_location_ids_fn
    _agent_location_ids_fn = fn


def get_agent_location_ids():
    fn = _agent_location_ids_fn
    if fn:
        return fn()
    return set()


def is_agent_location(location_id: int) -> bool:
    """Check if a location is agent-backed (content must be proxied, not local)."""
    return location_id in get_agent_location_ids()


def set_agent_label_prefixes(fn):
    global _agent_label_prefixes_fn
    _agent_label_prefixes_fn = fn


def get_agent_label_prefixes():
    """Return {location_id: agent_name} for agent-backed locations."""
    fn = _agent_label_prefixes_fn
    if fn:
        return fn()
    return {}


def set_agent_scanning(fn):
    global _agent_scanning_fn
    _agent_scanning_fn = fn


def is_agent_scanning(location_id: int) -> bool:
    """Check if an agent is currently scanning this location."""
    fn = _agent_scanning_fn
    if fn:
        return fn(location_id)
    return False


def set_disk_stats(fn):
    global _disk_stats_fn
    _disk_stats_fn = fn


def get_disk_stats():
    return _disk_stats_fn


def set_location_changed(fn):
    global _location_changed_fn
    _location_changed_fn = fn


def get_location_changed():
    return _location_changed_fn


def set_agent_status(fn):
    global _agent_status_fn
    _agent_status_fn = fn


async def get_agent_status(location_id: int):
    """Return agent activity status, or None if not available."""
    fn = _agent_status_fn
    if fn:
        return await fn(location_id)
    return None

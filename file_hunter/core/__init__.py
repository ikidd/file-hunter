from starlette.responses import JSONResponse

# Re-export from file_hunter_core so existing imports continue to work
from file_hunter_core.classify import classify_file, format_size  # noqa: F401


def json_ok(data, status=200) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data}, status_code=status)


def json_error(message: str, status=400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message}, status_code=status)

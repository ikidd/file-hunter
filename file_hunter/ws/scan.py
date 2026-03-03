import json
from starlette.websockets import WebSocket, WebSocketDisconnect

clients: set[WebSocket] = set()
_current_scan_state: dict | None = None


async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        # Send current scan state to newly connected client
        if _current_scan_state is not None:
            await websocket.send_text(json.dumps(_current_scan_state))
        # Send queue state if items are pending
        from file_hunter.services.scan_queue import get_queue_state

        queue_state = get_queue_state()
        if queue_state["pending"]:
            await websocket.send_text(
                json.dumps({"type": "scan_queue_updated", "queue": queue_state})
            )
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        clients.discard(websocket)


async def broadcast(message: dict):
    global _current_scan_state

    # Track active scan state for late-joining clients
    msg_type = message.get("type", "")
    if msg_type in ("scan_started", "scan_progress"):
        _current_scan_state = message
    elif msg_type in ("scan_completed", "scan_cancelled", "scan_error"):
        _current_scan_state = None

    data = json.dumps(message)
    disconnected = []
    for ws in list(clients):
        try:
            await ws.send_text(data)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        clients.discard(ws)

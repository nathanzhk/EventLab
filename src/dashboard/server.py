from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import orjson
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from runtime.events import RuntimeStateEvent


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(_broadcast(), name="dashboard-broadcast")
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(lifespan=lifespan)

_ws_clients: set[WebSocket] = set()
_latest_state: dict[str, bytes] = {}
_broadcast_state: asyncio.Queue[bytes] = asyncio.Queue(maxsize=500)

DASHBOARD_PORT = 8888
_DASHBOARD_HOST = "0.0.0.0"
_STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


@app.post("/state")
async def update_state(request: Request) -> dict[str, bool]:
    payload = await request.body()
    _save_latest_state(payload)
    _save_broadcast_state(payload)
    return {"ok": True}


@app.websocket("/ws")
async def websocket(ws: WebSocket) -> None:
    await ws.accept()
    _ws_clients.add(ws)
    try:
        for payload in _latest_state.values():
            await ws.send_bytes(payload)
        while True:
            await ws.receive_text()
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    finally:
        _ws_clients.discard(ws)


def _save_latest_state(payload: bytes) -> None:
    state = orjson.loads(payload)
    slug = state.get("market").get("slug")
    _latest_state[slug] = payload


def _save_broadcast_state(payload: bytes) -> None:
    try:
        _broadcast_state.put_nowait(payload)
    except asyncio.QueueFull:
        try:
            _broadcast_state.get_nowait()
        except asyncio.QueueEmpty:
            pass
        _broadcast_state.put_nowait(payload)


async def _broadcast() -> None:
    while True:
        payload = await _broadcast_state.get()
        invalid_clients: list[WebSocket] = []
        for ws in _ws_clients:
            try:
                await ws.send_bytes(payload)
            except Exception:
                invalid_clients.append(ws)
        for ws in invalid_clients:
            _ws_clients.discard(ws)


def serialize_state(event: RuntimeStateEvent) -> dict[str, Any]:
    return {
        "worker_id": event.market.slug,
        "event_ts_ms": event.event_ts_ms,
        "reason": event.reason,
        "market": {
            "slug": event.market.slug,
            "title": event.market.title,
            "start_ts_s": event.market.start_ts_s,
            "end_ts_s": event.market.end_ts_s,
        },
        "yes_quote": _serialize_quote(event.yes_token_quote),
        "no_quote": _serialize_quote(event.no_token_quote),
        "crypto": {
            "base": event.crypto_quote.base_price,
            "diff": event.crypto_quote.diff_price,
            "price": event.crypto_quote.curr_price,
            "best_bid": event.crypto_quote.best_bid,
            "best_ask": event.crypto_quote.best_ask,
        },
        "prev_side": event.prev_side,
        "curr_side": event.curr_side,
        "yes_position": _serialize_position(event.yes_token_position),
        "no_position": _serialize_position(event.no_token_position),
    }


def _serialize_quote(q: Any) -> dict[str, Any]:
    return {
        "best_bid": q.best_bid,
        "best_ask": q.best_ask,
    }


def _serialize_position(p: Any) -> dict[str, Any] | None:
    if p is None:
        return None
    return {
        "opening_shares": p.opening_shares,
        "open_settling_shares": p.open_settling_shares,
        "holding_shares": p.holding_shares,
        "holding_avg_price": p.holding_avg_price,
        "holding_cost": p.holding_cost,
        "closing_shares": p.closing_shares,
        "close_settling_shares": p.close_settling_shares,
        "realized_pnl": p.realized_pnl,
        "effective_shares": p.effective_shares,
        "sellable_shares": p.sellable_shares,
    }


async def serve(*, host: str = _DASHBOARD_HOST, port: int = DASHBOARD_PORT) -> None:
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_config=None,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    await server.serve()

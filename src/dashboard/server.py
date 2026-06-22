from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path
from typing import Any

import orjson
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
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
_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
_HISTORY_DOWNSAMPLE_MS = 1000

_STATE_PATTERN = re.compile(
    r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3}) \[\w+\] \[STATE\] "
    r"\S+ bid (\S+) ask (\S+) \| "
    r"\S+ bid (\S+) ask (\S+) \| "
    r"\$([\d.]+)(?: \S+ ([+-]\$[\d.]+))?"
)
_SLUG_PATTERN = re.compile(r"^(?P<prefix>.+)-(?P<minutes>\d+)m-(?P<start>\d+)$")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


@app.post("/state")
async def update_state(request: Request) -> dict[str, bool]:
    payload = await request.body()
    _save_latest_state(payload)
    _save_broadcast_state(payload)
    return {"ok": True}


@app.get("/markets")
async def list_markets() -> list[dict[str, Any]]:
    return await asyncio.to_thread(_list_markets)


@app.get("/market/{slug}/history")
async def get_market_history(slug: str) -> dict[str, Any]:
    meta = _parse_slug_meta(slug)
    if meta is None:
        raise HTTPException(status_code=400, detail="invalid slug")
    log_path = await asyncio.to_thread(_find_quote_log, slug)
    if log_path is None:
        raise HTTPException(status_code=404, detail="quote log not found")
    samples = await asyncio.to_thread(_parse_quote_log, log_path, _HISTORY_DOWNSAMPLE_MS)
    return {
        "slug": slug,
        "start_ts_s": meta[0],
        "end_ts_s": meta[1],
        "samples": samples,
    }


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


def _parse_slug_meta(slug: str) -> tuple[int, int] | None:
    match = _SLUG_PATTERN.match(slug)
    if match is None:
        return None
    start_ts_s = int(match.group("start"))
    interval_s = int(match.group("minutes")) * 60
    return start_ts_s, start_ts_s + interval_s


def _list_markets() -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for path in _LOG_DIR.glob("*/*.quote.log"):
        slug = path.name.removesuffix(".quote.log")
        if slug in seen:
            continue
        meta = _parse_slug_meta(slug)
        if meta is None:
            continue
        seen[slug] = {
            "slug": slug,
            "start_ts_s": meta[0],
            "end_ts_s": meta[1],
        }
    return sorted(seen.values(), key=lambda entry: entry["start_ts_s"])


def _find_quote_log(slug: str) -> Path | None:
    candidates = sorted(
        _LOG_DIR.glob(f"*/{slug}.quote.log"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _parse_quote_log(path: Path, downsample_ms: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    last_ts_ms = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if "[STATE]" not in line:
                continue
            match = _STATE_PATTERN.match(line)
            if match is None:
                continue
            (
                year,
                month,
                day,
                hour,
                minute,
                second,
                milli,
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                btc,
                diff,
            ) = match.groups()
            ts_ms = int(
                datetime(
                    int(year), int(month), int(day), int(hour), int(minute), int(second)
                ).timestamp()
                * 1000
            ) + int(milli)
            if ts_ms - last_ts_ms < downsample_ms:
                continue
            last_ts_ms = ts_ms
            samples.append(
                {
                    "ts_ms": ts_ms,
                    "yes_bid": float(yes_bid),
                    "yes_ask": float(yes_ask),
                    "no_bid": float(no_bid),
                    "no_ask": float(no_ask),
                    "btc_curr": float(btc),
                    "btc_diff": float(diff.replace("$", "")) if diff else None,
                }
            )
    return samples


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
        "ts_ms": event.event_ts_ms,
        "market": {
            "slug": event.market.slug,
            "title": event.market.title,
            "start_ts_s": event.market.start_ts_s,
            "end_ts_s": event.market.end_ts_s,
        },
        "crypto_quote": {
            "curr": event.crypto_quote.curr_price,
            "base": event.crypto_quote.base_price,
            "diff": event.crypto_quote.diff_price,
        },
        "yes_token_quote": {
            "bid": event.yes_token_quote.best_bid,
            "ask": event.yes_token_quote.best_ask,
            "mid": event.yes_token_quote.mid,
        },
        "no_token_quote": {
            "bid": event.no_token_quote.best_bid,
            "ask": event.no_token_quote.best_ask,
            "mid": event.no_token_quote.mid,
        },
        "yes_position": _serialize_position(event.yes_token_position),
        "no_position": _serialize_position(event.no_token_position),
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

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from utils.env import Env
from utils.logger import get_logger
from utils.time import now_ts_ms

_RECONNECT_DELAY_S = 2

logger = get_logger("BINANCE QUOTE")


@dataclass(frozen=True, slots=True)
class BinanceQuote:
    recv_ts_ms: int
    best_bid: float
    best_ask: float

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2


class BinanceQuoteStream:
    def __init__(self, symbol: str) -> None:
        self._symbol = symbol.lower()

    def __aiter__(self) -> AsyncIterator[BinanceQuote]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[BinanceQuote]:
        while True:
            try:
                logger.info("connecting binance quote websocket")
                async with connect(
                    f"{Env.BINANCE_WS_BASE_URL}/{self._symbol}@bookTicker",
                    ping_interval=20,
                    ping_timeout=5,
                    max_queue=2048,
                    max_size=None,
                ) as ws:
                    logger.info("connected market quote websocket")
                    async for raw in ws:
                        try:
                            message = orjson.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(message, dict):
                            continue
                        event = self._build_event(message)
                        if event is not None:
                            yield event
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("disconnected market quote websocket: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _build_event(self, message: dict) -> BinanceQuote | None:
        try:
            best_bid = float(message["b"])
            best_ask = float(message["a"])
        except Exception:
            return None

        return BinanceQuote(
            recv_ts_ms=now_ts_ms(),
            best_bid=best_bid,
            best_ask=best_ask,
        )

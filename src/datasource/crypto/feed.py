from __future__ import annotations

from collections.abc import AsyncIterator

import orjson
import requests

from models import Market
from utils.env import Env
from utils.logger import get_logger
from utils.time import now_ts_ms

from .binance import BinanceQuote, BinanceQuoteStream
from .events import CryptoQuoteEvent

MIN_EMIT_INTERVAL_MS = 1

logger = get_logger("CRYPTO QUOTE")


class CryptoQuoteStream:
    def __init__(self, symbol: str, market: Market) -> None:
        self._market = market
        self._binance_feed = BinanceQuoteStream(symbol)
        self._started_before_market = now_ts_ms() < market.start_ts_ms
        self._last_emit_ts_ms: int | None = None
        self._base_price: float | None = None

        if not self._started_before_market:
            self._base_price = _load_api_base_price(
                symbol=symbol,
                start_ts_ms=self._market.start_ts_ms,
            )

    def __aiter__(self) -> AsyncIterator[CryptoQuoteEvent]:
        return self._events()

    async def _events(self) -> AsyncIterator[CryptoQuoteEvent]:
        async for quote in self._binance_feed:
            event = self._build_event(quote)
            if event is not None:
                yield event

    def _build_event(self, quote: BinanceQuote) -> CryptoQuoteEvent | None:
        if self._last_emit_ts_ms is not None:
            if quote.recv_ts_ms - self._last_emit_ts_ms < MIN_EMIT_INTERVAL_MS:
                return None
        self._last_emit_ts_ms = quote.recv_ts_ms

        if (
            self._started_before_market
            and self._base_price is None
            and quote.recv_ts_ms >= self._market.start_ts_ms
        ):
            self._base_price = round(quote.mid, 2)

        return CryptoQuoteEvent(
            recv_ts_ms=quote.recv_ts_ms,
            curr_price=round(quote.mid, 2),
            base_price=self._base_price,
        )


def _load_api_base_price(*, symbol: str, start_ts_ms: int) -> float | None:
    rest_base_price = _get_api_base_price(
        symbol=symbol,
        start_ts_ms=start_ts_ms,
    )
    if rest_base_price is None:
        return None

    logger.info(
        "loaded api base price symbol=%s start_ts_ms=%d price=%.2f",
        symbol,
        start_ts_ms,
        rest_base_price,
    )
    return round(rest_base_price, 2)


def _get_api_base_price(*, symbol: str, start_ts_ms: int) -> float | None:
    try:
        response = requests.get(
            f"{Env.BINANCE_API_BASE_URL}/klines",
            params={
                "symbol": symbol,
                "interval": "1s",
                "startTime": start_ts_ms,
                "limit": 1,
            },
            timeout=(1, 3),
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("get api base price failed: %s", e)
        return None

    try:
        payload = orjson.loads(response.content)
        kline = payload[0]
        return float(kline[1])
    except (IndexError, TypeError, ValueError, orjson.JSONDecodeError) as e:
        logger.error("invalid api base price response: %s", e)
        return None

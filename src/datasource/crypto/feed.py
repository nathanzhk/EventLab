from __future__ import annotations

from collections.abc import AsyncIterator

from .binance import BinanceQuote, BinanceQuoteStream
from .event import CryptoQuoteEvent

WINDOW_MS = 300_000
MIN_EMIT_INTERVAL_MS = 1


class CryptoQuoteStream:
    def __init__(self, symbol: str) -> None:
        self._binance_feed = BinanceQuoteStream(symbol)
        self._last_emit_ts_ms: int | None = None
        self._window_start_ms: int | None = None
        self._window_price: float | None = None

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

        window_start_ms = quote.recv_ts_ms - (quote.recv_ts_ms % WINDOW_MS)
        if self._window_start_ms != window_start_ms:
            self._window_start_ms = window_start_ms
            self._window_price = quote.mid

        return CryptoQuoteEvent(
            recv_ts_ms=quote.recv_ts_ms,
            best_bid=quote.best_bid,
            best_ask=quote.best_ask,
            win_price=self._window_price,
            curr_price=quote.mid,
        )

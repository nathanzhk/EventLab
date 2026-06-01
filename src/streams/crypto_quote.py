from __future__ import annotations

from collections.abc import AsyncIterator

from datasource.crypto import CompositePrice, CryptoPriceAggregator, stream_exchange_quotes
from events import CryptoQuoteEvent
from utils.logger import get_logger

logger = get_logger("CRYPTO QUOTE")


class CryptoQuoteStream:
    def __init__(self, symbol: str) -> None:
        if symbol is None or symbol.strip() == "":
            raise ValueError("cannot load current symbol")
        self._symbol = symbol.strip().lower()

    def __aiter__(self) -> AsyncIterator[CryptoQuoteEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[CryptoQuoteEvent]:
        aggregator = CryptoPriceAggregator()
        async for quote in stream_exchange_quotes(on_error=logger.error):
            yield self._build_event(aggregator.update(quote))

    def _build_event(self, composite: CompositePrice) -> CryptoQuoteEvent:
        return CryptoQuoteEvent(
            exch_ts_ms=composite.ts_ms,
            symbol=self._symbol,
            best_bid=round(composite.curr_price, 3),
            best_ask=round(composite.curr_price, 3),
            baseline=composite.base_price,
            change=composite.change,
            price=composite.curr_price,
            recv_ts_ms=composite.ts_ms,
        )

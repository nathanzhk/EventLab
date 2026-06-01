from __future__ import annotations

from collections.abc import Iterable

from .models import CompositePrice, ExchangeQuote

WINDOW_MS = 300_000


class CryptoPriceAggregator:
    def __init__(self) -> None:
        self._latest: dict[str, ExchangeQuote] = {}
        self._window_start_ms: int | None = None
        self._start_price: float | None = None

    def update(self, quote: ExchangeQuote) -> CompositePrice:
        self._latest[quote.source] = quote
        sources = self.sources()
        curr_price = composite_price(sources)
        window_start_ms = quote.recv_ts_ms - (quote.recv_ts_ms % WINDOW_MS)

        if self._window_start_ms != window_start_ms:
            self._window_start_ms = window_start_ms
            self._start_price = curr_price

        return CompositePrice(
            base_price=self._start_price,
            curr_price=curr_price,
            ts_ms=quote.recv_ts_ms,
            sources=sources,
        )

    def sources(self) -> list[ExchangeQuote]:
        return sorted(self._latest.values(), key=lambda quote: quote.source)


def composite_price(quotes: Iterable[ExchangeQuote]) -> float:
    mids = [quote.mid for quote in quotes]
    if not mids:
        raise ValueError("cannot build composite price without quotes")
    return sum(mids) / len(mids)

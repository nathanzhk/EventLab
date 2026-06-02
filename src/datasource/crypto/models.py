from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExchangeQuote:
    source: str
    best_bid: float
    best_ask: float
    exch_ts_ms: int
    recv_ts_ms: int

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2


@dataclass(frozen=True, slots=True)
class CompositePrice:
    ts_ms: int
    base_price: float | None
    curr_price: float
    sources: list[ExchangeQuote]

    @property
    def diff_price(self) -> float | None:
        if self.base_price is None:
            return None
        return self.curr_price - self.base_price

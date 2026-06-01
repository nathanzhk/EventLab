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

    def as_dict(self) -> dict[str, float | int | str]:
        return {
            "source": self.source,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid": self.mid,
            "exch_ts_ms": self.exch_ts_ms,
            "recv_ts_ms": self.recv_ts_ms,
        }


@dataclass(frozen=True, slots=True)
class CompositePrice:
    ts_ms: int
    base_price: float | None
    curr_price: float
    sources: list[ExchangeQuote]

    @property
    def change(self) -> float | None:
        if self.base_price is None:
            return None
        return self.curr_price - self.base_price

    def as_dict(self) -> dict[str, object]:
        return {
            "start_price": self.base_price,
            "curr_price": self.curr_price,
            "ts_ms": self.ts_ms,
            "sources": [quote.as_dict() for quote in self.sources],
        }

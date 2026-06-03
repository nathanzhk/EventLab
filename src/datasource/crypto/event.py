from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter_ns


@dataclass(slots=True, frozen=True)
class CryptoQuoteEvent:
    recv_ts_ms: int
    best_bid: float
    best_ask: float
    win_price: float
    curr_price: float
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def baseline(self) -> float:
        return self.win_price

    @property
    def change(self) -> float:
        return self.curr_price - self.win_price

    @property
    def price(self) -> float:
        return self.curr_price

    @property
    def mid(self) -> float:
        return round(self.curr_price, 3)

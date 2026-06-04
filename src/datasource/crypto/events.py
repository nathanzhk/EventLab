from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter_ns


@dataclass(slots=True, frozen=True)
class CryptoQuoteEvent:
    recv_ts_ms: int
    curr_price: float
    base_price: float | None
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def diff_price(self) -> float | None:
        if self.base_price is None:
            return None
        return round(self.curr_price - self.base_price, 2)

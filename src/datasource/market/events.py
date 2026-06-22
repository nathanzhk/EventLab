from dataclasses import dataclass, field
from time import perf_counter_ns

from models import Market, Token
from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class MarketQuoteEvent:
    exch_ts_ms: int
    market: Market
    token: Token
    best_bid: float
    best_ask: float
    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def mid(self) -> float:
        return round((self.best_bid + self.best_ask) / 2, 2)

    @property
    def spreed(self) -> float:
        return round(self.best_ask - self.best_bid, 2)

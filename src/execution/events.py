from dataclasses import dataclass, field
from time import perf_counter_ns

from enums import MarketOrderStatus, MarketOrderType, MarketTradeStatus, Role, Side
from models import Market, Token
from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class CurrentPositionEvent:
    token: Token
    market: Market
    opening_shares: float
    open_settling_shares: float
    closing_shares: float
    close_settling_shares: float
    holding_cost: float
    holding_shares: float
    holding_avg_price: float
    realized_pnl: float = 0.0

    @property
    def effective_shares(self) -> float:
        """Position size after all pending and settling orders finish."""
        return round(
            self.opening_shares
            + self.open_settling_shares
            + self.holding_shares
            - self.closing_shares
            - self.close_settling_shares,
            6,
        )

    @property
    def sellable_shares(self) -> float:
        """Held shares that are not already committed to pending or settling sells."""
        return max(
            round(
                self.holding_shares - self.closing_shares - self.close_settling_shares,
                6,
            ),
            0.0,
        )

    @property
    def is_active(self) -> bool:
        return (
            self.opening_shares > 0
            or self.open_settling_shares > 0
            or self.closing_shares > 0
            or self.close_settling_shares > 0
            or self.holding_shares > 0
        )


@dataclass(slots=True, frozen=True)
class MarketOrderEvent:
    event_source: str
    exch_ts_ms: int

    market_id: str
    token_id: str
    order_id: str
    trade_ids: list[str]

    status: MarketOrderStatus
    shares: float

    side: Side
    type: MarketOrderType
    price: float

    matched_shares: float

    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def unmatched_shares(self) -> float:
        return (
            round(self.shares - self.matched_shares, 6)
            if self.shares > self.matched_shares
            else 0.0
        )

    @property
    def is_active(self) -> bool:
        return self.status == MarketOrderStatus.LIVE or self.status == MarketOrderStatus.MATCHED

    @property
    def is_inactive(self) -> bool:
        return not self.is_active


@dataclass(slots=True, frozen=True)
class MarketTradeEvent:
    event_source: str
    exch_ts_ms: int

    market_id: str
    token_id: str
    order_id: str
    trade_id: str

    status: MarketTradeStatus
    shares: float

    side: Side
    role: Role
    price: float

    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def is_success(self) -> bool:
        return self.status == MarketTradeStatus.MINED or self.status == MarketTradeStatus.CONFIRMED

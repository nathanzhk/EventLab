from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field

from datasource.market import MarketQuoteEvent
from enums import MarketOrderStatus, MarketOrderType, MarketTradeStatus, Role, Side
from execution.events import MarketOrderEvent, MarketTradeEvent
from models import Token
from utils.time import now_ts_ms

_EVENT_SOURCE = "mock"


@dataclass(slots=True)
class MockOrder:
    order_id: str
    market_id: str
    token_id: str
    side: Side
    role: Role
    type: MarketOrderType
    price: float
    shares: float
    status: MarketOrderStatus
    created_ts_ms: int
    updated_ts_ms: int
    matched_shares: float = 0.0
    trade_ids: list[str] = field(default_factory=list)

    @property
    def unmatched_shares(self) -> float:
        return round(max(self.shares - self.matched_shares, 0.0), 6)


@dataclass(slots=True)
class MockTrade:
    trade_id: str
    order_id: str
    market_id: str
    token_id: str
    side: Side
    role: Role
    price: float
    shares: float
    status: MarketTradeStatus
    created_ts_ms: int
    updated_ts_ms: int
    mined_due_ms: int


class MockOrderStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._orders: dict[str, MockOrder] = {}
        self._trades: dict[str, MockTrade] = {}
        self._latest_quotes: dict[str, MarketQuoteEvent] = {}
        self._pending_events: list[MarketOrderEvent | MarketTradeEvent] = []

    def create_order(
        self,
        *,
        token: Token,
        side: Side,
        role: Role,
        shares: float,
        price: float,
    ) -> MockOrder:
        now = now_ts_ms()
        return MockOrder(
            order_id=uuid.uuid4().hex,
            market_id=token.market_id,
            token_id=token.id,
            side=side,
            role=role,
            type=MarketOrderType.GTC if role == Role.MAKER else MarketOrderType.FOK,
            price=round(price, 3),
            shares=round(shares, 6),
            status=MarketOrderStatus.LIVE,
            created_ts_ms=now,
            updated_ts_ms=now,
        )

    def submit_order(self, order: MockOrder) -> bool:
        if order.shares <= 0:
            return False
        with self._lock:
            self._orders[order.order_id] = order
        return True

    def cancel_order(self, order_id: str) -> tuple[bool, str]:
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return False, "order not found"
            if order.unmatched_shares <= 0:
                return False, "matched"
            if order.status == MarketOrderStatus.CANCELED:
                return True, ""
            order.status = MarketOrderStatus.CANCELED
            order.updated_ts_ms = now_ts_ms()
            self._pending_events.append(self._order_event(order))
        return True, ""

    def get_order(self, order_id: str) -> MarketOrderEvent | None:
        with self._lock:
            order = self._orders.get(order_id)
            return self._order_event(order) if order is not None else None

    def get_trade(self, trade_id: str) -> MarketTradeEvent | None:
        with self._lock:
            trade = self._trades.get(trade_id)
            return self._trade_event(trade) if trade is not None else None

    def record_quote(self, quote: MarketQuoteEvent) -> None:
        with self._lock:
            self._latest_quotes[quote.token.id] = quote

    def latest_quote(self, token_id: str) -> MarketQuoteEvent | None:
        with self._lock:
            return self._latest_quotes.get(token_id)

    def resting_maker_orders(self) -> list[MockOrder]:
        with self._lock:
            return [
                order
                for order in self._orders.values()
                if order.role == Role.MAKER
                and order.status == MarketOrderStatus.LIVE
                and order.unmatched_shares > 0
            ]

    def due_taker_orders(self, now_ms: int, delay_ms: int) -> list[MockOrder]:
        with self._lock:
            return [
                order
                for order in self._orders.values()
                if order.role == Role.TAKER
                and order.status == MarketOrderStatus.LIVE
                and order.unmatched_shares > 0
                and now_ms - order.created_ts_ms >= delay_ms
            ]

    def mark_matched(self, order_id: str, price: float, settle_delay_ms: int) -> None:
        with self._lock:
            order = self._orders.get(order_id)
            if order is None or order.unmatched_shares <= 0:
                return
            if order.status == MarketOrderStatus.CANCELED:
                return
            now = now_ts_ms()
            shares = order.unmatched_shares
            order.matched_shares = round(order.matched_shares + shares, 6)
            order.status = MarketOrderStatus.MATCHED
            order.updated_ts_ms = now
            trade = MockTrade(
                trade_id=uuid.uuid4().hex,
                order_id=order.order_id,
                market_id=order.market_id,
                token_id=order.token_id,
                side=order.side,
                role=order.role,
                price=round(price, 3),
                shares=shares,
                status=MarketTradeStatus.MATCHED,
                created_ts_ms=now,
                updated_ts_ms=now,
                mined_due_ms=now + settle_delay_ms,
            )
            order.trade_ids.append(trade.trade_id)
            self._trades[trade.trade_id] = trade
            self._pending_events.append(self._order_event(order))
            self._pending_events.append(self._trade_event(trade))

    def promote_mined(self, now_ms: int) -> None:
        with self._lock:
            for trade in self._trades.values():
                if trade.status == MarketTradeStatus.MATCHED and trade.mined_due_ms <= now_ms:
                    trade.status = MarketTradeStatus.MINED
                    trade.updated_ts_ms = now_ms
                    self._pending_events.append(self._trade_event(trade))

    def drain_events(self) -> list[MarketOrderEvent | MarketTradeEvent]:
        with self._lock:
            events = self._pending_events
            self._pending_events = []
            return events

    @staticmethod
    def _order_event(order: MockOrder) -> MarketOrderEvent:
        return MarketOrderEvent(
            event_source=_EVENT_SOURCE,
            exch_ts_ms=order.updated_ts_ms,
            market_id=order.market_id,
            token_id=order.token_id,
            order_id=order.order_id,
            trade_ids=list(order.trade_ids),
            status=order.status,
            shares=order.shares,
            side=order.side,
            type=order.type,
            price=order.price,
            matched_shares=order.matched_shares,
        )

    @staticmethod
    def _trade_event(trade: MockTrade) -> MarketTradeEvent:
        return MarketTradeEvent(
            event_source=_EVENT_SOURCE,
            exch_ts_ms=trade.updated_ts_ms,
            market_id=trade.market_id,
            token_id=trade.token_id,
            order_id=trade.order_id,
            trade_id=trade.trade_id,
            status=trade.status,
            shares=trade.shares,
            side=trade.side,
            role=trade.role,
            price=trade.price,
        )

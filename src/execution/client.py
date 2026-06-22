from __future__ import annotations

from typing import Protocol

from execution.events import MarketOrderEvent, MarketTradeEvent
from models import Market, Token


class TradeClient(Protocol):
    def buy(self, token: Token, shares: float, price: float) -> str | None: ...

    def sell(self, token: Token, shares: float, price: float) -> str | None: ...

    def get_cash_balance(self) -> float: ...

    def get_order_by_id(self, order_id: str) -> MarketOrderEvent | None: ...

    def get_trade_by_id(self, trade_id: str) -> MarketTradeEvent | None: ...

    def cancel_order_by_id(self, order_id: str) -> tuple[bool, str]: ...

    def calc_net_buy_shares(
        self, market: Market, shares: float, price: float
    ) -> tuple[float, float]: ...

    def calc_net_sell_amount(
        self, market: Market, shares: float, price: float
    ) -> tuple[float, float]: ...

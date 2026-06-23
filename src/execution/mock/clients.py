from __future__ import annotations

from decimal import ROUND_DOWN, Decimal

from enums import Role, Side
from execution.events import MarketOrderEvent, MarketTradeEvent
from models import Market, Token
from utils.logger import get_logger

from .store import MockOrder, MockOrderStore


class MockTradeClient:
    role: Role

    def __init__(self, store: MockOrderStore) -> None:
        self._store = store
        self.logger = get_logger(f"MOCK-{self.role}")

    def warm_up(self, market: Market) -> None:
        self.logger.debug("warm up %s", market.slug)

    def create_buy_order(self, token: Token, shares: float, price: float) -> tuple[str, MockOrder]:
        order = self._store.create_order(
            token=token,
            side=Side.BUY,
            role=self.role,
            shares=shares,
            price=price,
        )
        return order.order_id, order

    def create_sell_order(self, token: Token, shares: float, price: float) -> tuple[str, MockOrder]:
        order = self._store.create_order(
            token=token,
            side=Side.SELL,
            role=self.role,
            shares=shares,
            price=price,
        )
        return order.order_id, order

    def submit_order(self, order: MockOrder) -> bool:
        return self._store.submit_order(order)

    def fee_rate(self, market: Market) -> float:
        if self.role == Role.MAKER:
            return 0.0
        if self.role == Role.TAKER:
            return market.fee_rate
        raise ValueError(f"invalid role: {self.role}")

    def calc_fee_amount(self, market: Market, shares: float, price: float) -> float:
        return _calculate_fee_amount(
            fee_rate=self.fee_rate(market),
            shares=shares,
            price=price,
        )

    def get_cash_balance(self) -> float:
        return 100_000.0

    def get_token_shares(self, token_id: str) -> float:
        return 0.0

    def get_order_by_id(self, order_id: str) -> MarketOrderEvent | None:
        return self._store.get_order(order_id)

    def get_trade_by_id(self, trade_id: str) -> MarketTradeEvent | None:
        return self._store.get_trade(trade_id)

    def cancel_order_by_id(self, order_id: str) -> tuple[bool, str]:
        return self._store.cancel_order(order_id)


class MockMakerTradeClient(MockTradeClient):
    role: Role = Role.MAKER


class MockTakerTradeClient(MockTradeClient):
    role: Role = Role.TAKER


def _calculate_fee_amount(*, fee_rate: float, shares: float, price: float) -> float:
    decimal_fee_rate = Decimal(str(fee_rate))
    decimal_shares = Decimal(str(shares))
    decimal_price = Decimal(str(price))
    fee = decimal_shares * decimal_fee_rate * decimal_price * (Decimal(1) - decimal_price)
    return float(fee.quantize(Decimal("0.00001"), rounding=ROUND_DOWN))

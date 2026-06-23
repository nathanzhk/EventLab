import math
import time

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OpenOrderParams,
    OrderArgs,
    OrderPayload,
    OrderType,
    TradeParams,
)
from py_clob_client_v2.config import get_contract_config
from py_clob_client_v2.exceptions import PolyApiException
from py_clob_client_v2.order_builder.constants import BUY, SELL
from py_clob_client_v2.order_utils.exchange_order_builder_v2 import ExchangeOrderBuilderV2
from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2

from enums import Role
from execution.events import MarketOrderEvent, MarketTradeEvent
from models import Market, Token
from utils.env import Env
from utils.logger import get_logger

from .stream import build_order_event, build_trade_event


class LiveTradeClient:
    role: Role
    post_only: bool
    order_type: OrderType

    def __init__(self) -> None:
        self.logger = get_logger(self.role)
        self.client = ClobClient(
            Env.POLYMARKET_CLOB_BASE_URL,
            key=Env.POLYMARKET_PRIVATE_KEY,
            funder=Env.POLYMARKET_PROXY_WALLET,
            chain_id=137,
            signature_type=2,
        )
        self.client.set_api_creds(self.get_credentials())
        self.order_builder = self.get_order_builder()

    def get_credentials(self) -> ApiCreds:
        return self.client.create_or_derive_api_key()

    def get_order_builder(self) -> ExchangeOrderBuilderV2:
        signer = self.client.signer
        if signer is None:
            raise RuntimeError("missing client signer")
        chain_id = signer.get_chain_id()
        contract_config = get_contract_config(chain_id)
        exchange_address = contract_config.exchange_v2
        return ExchangeOrderBuilderV2(exchange_address, chain_id, signer)

    def create_buy_order(
        self, token: Token, shares: float, price: float
    ) -> tuple[str, SignedOrderV2]:
        self.logger.info("create buy order %s %.6f at $%.2f", token.key, shares, price)
        order_id, order = self._create_order(token=token, shares=shares, price=price, side=BUY)
        self.logger.info("create order success %s", order_id)
        return order_id, order

    def create_sell_order(
        self, token: Token, shares: float, price: float
    ) -> tuple[str, SignedOrderV2]:
        self.logger.info("create sell order %s %.6f at $%.2f", token.key, shares, price)
        order_id, order = self._create_order(token=token, shares=shares, price=price, side=SELL)
        self.logger.info("create order success %s", order_id)
        return order_id, order

    def submit_order(self, order: SignedOrderV2) -> bool:
        try:
            order_id = self._submit_order(order)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.warning("submit order failed: %s", _error_message(e))
            return False
        if order_id is None:
            return False
        self.logger.info("submit order success %s", order_id)
        return True

    def warm_up(self, market: Market):
        self._create_order(token=market.yes_token, shares=100, price=0.01, side=SELL)
        self._create_order(token=market.no_token, shares=100, price=0.01, side=SELL)
        self._create_order(token=market.yes_token, shares=100, price=0.01, side=SELL)
        self._create_order(token=market.no_token, shares=100, price=0.01, side=SELL)

    def fee_rate(self, market: Market) -> float:
        if self.role == Role.MAKER:
            return 0.0
        if self.role == Role.TAKER:
            return market.fee_rate
        raise ValueError(f"invalid role: {self.role}")

    def calc_fee(self, market: Market, shares: float, price: float) -> float:
        return round(self.fee_rate(market) * shares * price * (1 - price), 6)

    def calc_net_buy_shares(
        self, market: Market, shares: float, price: float
    ) -> tuple[float, float]:
        fee_shares = _truncate_decimal(shares * self.fee_rate(market) * (1 - price), 5)
        return round(shares - fee_shares, 6), fee_shares

    def calc_net_sell_amount(
        self, market: Market, shares: float, price: float
    ) -> tuple[float, float]:
        fee_amount = _truncate_decimal(shares * price * self.fee_rate(market) * (1 - price), 5)
        return round(shares * price - fee_amount, 6), fee_amount

    def get_cash_balance(self) -> float:
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,  # type: ignore
        )
        balance = self._get_balance(params)
        self.logger.debug("cash balance: %.6f", balance)
        return balance

    def get_token_shares(self, token_id: str) -> float:
        params = BalanceAllowanceParams(
            token_id=token_id,
            asset_type=AssetType.CONDITIONAL,  # type: ignore
        )
        shares = self._get_balance(params)
        self.logger.debug("token shares: %.6f", shares)
        return shares

    def get_order_by_id(self, order_id: str) -> MarketOrderEvent | None:
        try:
            resp = self.client.get_order(order_id)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.warning("get order failed: %s", _error_message(e))
            return None

        if resp is None:
            self.logger.warning("order not found: %s", order_id)
            return None

        if not isinstance(resp, dict):
            self.logger.warning("invalid response: %r", resp)
            return None

        return build_order_event(resp, source="pull")

    def get_orders_by_token(self, token: Token) -> list[MarketOrderEvent]:
        try:
            params = OpenOrderParams(asset_id=token.id)
            resp = self.client.get_open_orders(params)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.warning("get orders failed: %s", _error_message(e))
            return []

        if not isinstance(resp, list):
            self.logger.warning("invalid response: %r", resp)
            return []

        orders: list[MarketOrderEvent] = []
        for item in resp:
            if not isinstance(item, dict):
                self.logger.warning("invalid response: %r", item)
                continue
            order = build_order_event(item, source="pull")
            orders.append(order) if order is not None else ...
        return orders

    def get_trade_by_id(self, trade_id: str) -> MarketTradeEvent | None:
        try:
            params = TradeParams(id=trade_id)
            resp = self.client.get_trades(params)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.warning("get trade failed: %s", _error_message(e))
            return None

        if not isinstance(resp, list) or len(resp) > 1:
            self.logger.warning("invalid response: %r", resp)
            return None

        if len(resp) == 0:
            self.logger.warning("trade not found: %s", trade_id)
            return None

        resp = resp[0]
        if not isinstance(resp, dict):
            self.logger.warning("invalid response: %r", resp)
            return None

        return build_trade_event(resp, Env.POLYMARKET_PROXY_WALLET, source="pull")

    def get_trades_by_token(self, token: Token) -> list[MarketTradeEvent]:
        try:
            params = TradeParams(asset_id=token.id)
            resp = self.client.get_trades(params)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            self.logger.warning("get trades failed: %s", _error_message(e))
            return []

        if not isinstance(resp, list):
            self.logger.warning("invalid response: %r", resp)
            return []

        trades: list[MarketTradeEvent] = []
        for item in resp:
            if not isinstance(item, dict):
                self.logger.warning("invalid response: %r", item)
                continue
            trade = build_trade_event(item, Env.POLYMARKET_PROXY_WALLET, source="pull")
            trades.append(trade) if trade is not None else ...
        return trades

    def cancel_order_by_id(self, order_id: str) -> tuple[bool, str]:
        try:
            start_ns = time.perf_counter_ns()
            try:
                resp = self.client.cancel_order(OrderPayload(orderID=order_id))
            finally:
                latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
                self.logger.info("cancel order latency %.3f ms", latency_ms)
            self.logger.debug("%r", resp)
        except PolyApiException as e:
            self.logger.debug("%r", e.error_msg)
            error_message = _error_message(e)
            self.logger.warning("cancel order failed: %s", error_message)
            return False, error_message

        success_list = resp.get("canceled", []) if isinstance(resp, dict) else []
        if order_id in success_list:
            self.logger.info("cancel order success %s", order_id)
            return True, ""

        failed_dict = resp.get("not_canceled", {}) if isinstance(resp, dict) else {}
        failed_reason = (
            failed_dict.get(order_id, "unknown reason")
            if isinstance(failed_dict, dict)
            else "unknown reason"
        )
        self.logger.warning("cancel order failed: %s", failed_reason)
        return False, failed_reason

    def _get_balance(self, params: BalanceAllowanceParams) -> float:
        start_ns = time.perf_counter_ns()
        try:
            resp = self.client.get_balance_allowance(params)
        finally:
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
            self.logger.info("get balance latency %.3f ms", latency_ms)

        if not isinstance(resp, dict):
            return 0.0
        balance = resp.get("balance", "0")
        if not isinstance(balance, str | int):
            return 0.0
        return round(int(balance) / 1_000_000, 6)

    def _create_order(
        self, *, token: Token, shares: float, price: float, side: str
    ) -> tuple[str, SignedOrderV2]:
        create_start_ns = time.perf_counter_ns()
        order = self.client.create_order(
            OrderArgs(token_id=token.id, size=shares, price=price, side=side)
        )
        if not isinstance(order, SignedOrderV2):
            raise RuntimeError(f"expected v2 order, got {type(order).__name__}")
        create_latency_ms = (time.perf_counter_ns() - create_start_ns) / 1_000_000
        self.logger.info("create order latency %.3f ms", create_latency_ms)
        order_id = self.order_builder.build_order_hash(
            self.order_builder.build_order_typed_data(order)
        )
        return order_id, order

    def _submit_order(self, order: SignedOrderV2) -> str | None:
        submit_start_ns = time.perf_counter_ns()
        try:
            resp = self.client.post_order(
                order, post_only=self.post_only, order_type=self.order_type
            )
            self.logger.debug("%r", resp)
        finally:
            submit_latency_ms = (time.perf_counter_ns() - submit_start_ns) / 1_000_000
            self.logger.info("submit order latency %.3f ms", submit_latency_ms)

        if not isinstance(resp, dict):
            self.logger.warning("invalid response: %r", resp)
            return None

        if resp.get("success") is not True:
            self.logger.warning("%s", resp.get("errorMsg") or "unknown error")
            return None

        order_id = resp.get("orderID")
        if not isinstance(order_id, str) or not order_id:
            self.logger.warning("missing order id: %r", resp)
            return None
        return order_id


class MakerTradeClient(LiveTradeClient):
    role: Role = Role.MAKER
    post_only: bool = True
    order_type: OrderType = OrderType.GTC  # type: ignore


class TakerTradeClient(LiveTradeClient):
    role: Role = Role.TAKER
    post_only: bool = False
    order_type: OrderType = OrderType.FOK  # type: ignore


def _truncate_decimal(x, digits):
    factor = 10**digits
    return math.trunc(x * factor) / factor


def _error_message(error: PolyApiException) -> str:
    error_msg = error.error_msg
    if isinstance(error_msg, dict):
        error_msg = error_msg.get("error", error_msg)
    return str(error_msg)

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from datasource.market import MarketQuoteEvent
from enums import Side
from execution.events import MarketOrderEvent, MarketTradeEvent
from utils.time import now_ts_ms

from .store import MockOrder, MockOrderStore

_POLL_INTERVAL_S = 0.1
_TAKER_FILL_DELAY_MS = 1_000
_SETTLE_DELAY_MS = 250


class MockTradeStream:
    def __init__(self, store: MockOrderStore) -> None:
        self._store = store

    def on_quote(self, quote: MarketQuoteEvent) -> None:
        self._store.record_quote(quote)
        for order in self._store.resting_maker_orders():
            if order.token_id != quote.token.id:
                continue
            if self._crosses(order, quote):
                self._store.mark_matched(order.order_id, order.price, _SETTLE_DELAY_MS)

    def __aiter__(self) -> AsyncIterator[MarketOrderEvent | MarketTradeEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[MarketOrderEvent | MarketTradeEvent]:
        while True:
            await asyncio.sleep(_POLL_INTERVAL_S)
            now = now_ts_ms()
            for order in self._store.due_taker_orders(now, _TAKER_FILL_DELAY_MS):
                self._store.mark_matched(order.order_id, self._taker_price(order), _SETTLE_DELAY_MS)
            self._store.promote_mined(now)
            for event in self._store.drain_events():
                yield event

    def _taker_price(self, order: MockOrder) -> float:
        return order.price

    @staticmethod
    def _crosses(order: MockOrder, quote: MarketQuoteEvent) -> bool:
        if order.side == Side.BUY:
            return quote.best_ask <= order.price
        return quote.best_bid >= order.price

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

from bus import EventBus, OverflowPolicy, Subscription
from datasource.market import MarketQuoteEvent

from .stream import MockTradeStream

if TYPE_CHECKING:
    from app import ComponentFactory


class MockTradeComponent:
    def __init__(self, *, bus: EventBus, stream: MockTradeStream) -> None:
        self._bus = bus
        self._stream = stream

    def start(self, tasks: asyncio.TaskGroup) -> None:
        tasks.create_task(self._publish_loop())
        market_quote_events = self._bus.subscribe(
            MarketQuoteEvent,
            name="mock-trade.market-quote",
            maxsize=100,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._market_quote_loop(market_quote_events))

    async def _publish_loop(self) -> None:
        async for event in self._stream:
            await self._bus.publish(event)

    async def _market_quote_loop(self, events: Subscription[MarketQuoteEvent]) -> None:
        async for quote in events:
            self._stream.on_quote(quote)


def mock_trade_component() -> ComponentFactory:
    return lambda context: MockTradeComponent(
        bus=context.bus,
        stream=cast(MockTradeStream, context.trade_stream),
    )

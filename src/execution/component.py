from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from prediction.events import DesiredPositionsEvent

from .engine import ExecutionEngine
from .events import MarketOrderEvent, MarketTradeEvent

if TYPE_CHECKING:
    from app import ComponentFactory


class ExecutionComponent:
    def __init__(self, *, bus: EventBus, engine: ExecutionEngine) -> None:
        self._bus = bus
        self._engine = engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_order_events = self._bus.subscribe(
            MarketOrderEvent,
            name="execution.market-order",
            maxsize=1000,
            overflow=OverflowPolicy.BLOCK,
        )
        tasks.create_task(self._market_order_loop(market_order_events))

        market_trade_events = self._bus.subscribe(
            MarketTradeEvent,
            name="execution.market-trade",
            maxsize=1000,
            overflow=OverflowPolicy.BLOCK,
        )
        tasks.create_task(self._market_trade_loop(market_trade_events))

        desired_positions_events = self._bus.subscribe(
            DesiredPositionsEvent,
            name="execution.desired-positions",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._desired_positions_loop(desired_positions_events))

    async def _market_order_loop(self, events: Subscription[MarketOrderEvent]) -> None:
        async for order in events:
            await self._engine.handle_order_event(order)

    async def _market_trade_loop(self, events: Subscription[MarketTradeEvent]) -> None:
        async for trade in events:
            await self._engine.handle_trade_event(trade)

    async def _desired_positions_loop(self, events: Subscription[DesiredPositionsEvent]) -> None:
        async for desired_positions in events:
            await self._engine.handle_desired_positions(desired_positions)


def execution_component() -> ComponentFactory:
    return lambda context: ExecutionComponent(
        bus=context.bus,
        engine=context.execution_engine,
    )

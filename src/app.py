from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from bus import EventBus
from dashboard.component import dashboard_component
from datasource.crypto import (
    CryptoQuoteEvent,
    CryptoQuoteStream,
    crypto_quote_component,
)
from datasource.market import (
    MarketQuoteEvent,
    MarketQuoteStream,
    market_quote_component,
)
from execution.component import execution_component
from execution.engine import ExecutionEngine
from execution.events import MarketOrderEvent, MarketTradeEvent
from execution.live import LiveTradeStream, MakerTradeClient, TakerTradeClient, live_trade_component
from execution.mock import (
    MockMakerTradeClient,
    MockOrderStore,
    MockTakerTradeClient,
    MockTradeStream,
    mock_trade_component,
)
from models import Market
from prediction.component import prediction_component
from prediction.engine import PredictionEngine
from prediction.strategy import Strategy
from runtime.component import runtime_state_component

ExecutionMode = Literal["live", "mock"]


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    bus: EventBus
    market: Market
    crypto_quote_stream: AsyncIterable[CryptoQuoteEvent]
    market_quote_stream: AsyncIterable[MarketQuoteEvent]
    prediction_engine: PredictionEngine
    execution_engine: ExecutionEngine
    trade_stream: AsyncIterable[MarketOrderEvent | MarketTradeEvent]


class RuntimeComponent(Protocol):
    def start(self, tasks: asyncio.TaskGroup) -> None:
        raise NotImplementedError


ComponentFactory = Callable[[RuntimeContext], RuntimeComponent]


class Runtime:
    def __init__(
        self,
        *,
        market: Market,
        symbol: str,
        strategy: Strategy,
        execution_mode: ExecutionMode = "mock",
        dashboard_enabled: bool = False,
    ) -> None:
        self._component_factories: list[ComponentFactory] = []
        self._execution_mode = execution_mode
        self._dashboard_enabled = dashboard_enabled
        bus = EventBus()
        if execution_mode == "live":
            maker_client = MakerTradeClient()
            taker_client = TakerTradeClient()
            trade_stream = LiveTradeStream(maker_client.get_credentials())
        else:
            store = MockOrderStore()
            maker_client = MockMakerTradeClient(store)
            taker_client = MockTakerTradeClient(store)
            trade_stream = MockTradeStream(store)
        maker_client.warm_up(market)
        taker_client.warm_up(market)
        self._context = RuntimeContext(
            bus=bus,
            market=market,
            crypto_quote_stream=CryptoQuoteStream(symbol=symbol, market=market),
            market_quote_stream=MarketQuoteStream(market),
            prediction_engine=PredictionEngine(strategy),
            execution_engine=ExecutionEngine(
                market,
                maker_client,
                taker_client,
                event_publisher=bus,
            ),
            trade_stream=trade_stream,
        )
        self._register_components()

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            for component_factory in self._component_factories:
                component_factory(self._context).start(tasks)

    async def settle_market(self, outcome: str) -> None:
        await self._context.execution_engine.settle_market(outcome)

    def _register_components(self) -> None:
        if self._dashboard_enabled:
            self._component_factories.append(dashboard_component())
        self._component_factories.append(runtime_state_component())
        self._component_factories.append(crypto_quote_component())
        self._component_factories.append(market_quote_component())
        self._component_factories.append(prediction_component())
        self._component_factories.append(execution_component())
        if self._execution_mode == "live":
            self._component_factories.append(live_trade_component())
        else:
            self._component_factories.append(mock_trade_component())

from __future__ import annotations

from collections.abc import AsyncIterator

from execution.events import MarketOrderEvent, MarketTradeEvent

from .simulator import PaperExchangeSimulator


class PaperTradeStream:
    def __init__(self, simulator: PaperExchangeSimulator) -> None:
        self._simulator = simulator

    def __aiter__(self) -> AsyncIterator[MarketOrderEvent | MarketTradeEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[MarketOrderEvent | MarketTradeEvent]:
        while True:
            yield await self._simulator.next_event()

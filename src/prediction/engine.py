from __future__ import annotations

from runtime.events import RuntimeStateEvent

from .events import DesiredPositionsEvent
from .strategy import Strategy


class PredictionEngine:
    def __init__(self, strategy: Strategy) -> None:
        self._strategy = strategy

    async def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionsEvent:
        return self._strategy.evaluate(state)

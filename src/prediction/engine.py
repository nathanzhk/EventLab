from __future__ import annotations

from runtime.events import RuntimeStateEvent

from .strategy import Strategy, StrategyOutput


class StrategyEngine:
    def __init__(self, strategy: Strategy) -> None:
        self._strategy = strategy

    async def evaluate(self, state: RuntimeStateEvent) -> StrategyOutput:
        return self._strategy.evaluate(state)

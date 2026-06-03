from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from runtime.events import RuntimeStateEvent
from utils.logger import get_logger

from .events import DesiredPositionEvent

logger = get_logger("DEFAULT STRATEGY")

StrategyOutput = Sequence[DesiredPositionEvent]


class Strategy(Protocol):
    def evaluate(self, state: RuntimeStateEvent) -> StrategyOutput:
        raise NotImplementedError


class DefaultStrategy:
    def evaluate(self, state: RuntimeStateEvent) -> StrategyOutput:
        return []

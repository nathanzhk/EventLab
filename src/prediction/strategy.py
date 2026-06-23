from __future__ import annotations

from typing import Protocol

from runtime.events import RuntimeStateEvent
from utils.logger import get_logger

from .events import DesiredPositionEvent, DesiredPositionsEvent

logger = get_logger("DEFAULT STRATEGY")


class Strategy(Protocol):
    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionsEvent:
        raise NotImplementedError


class DefaultStrategy:
    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionsEvent:
        return DesiredPositionsEvent(
            market=state.market,
            up_signal=_unchanged_position(state, up=True),
            dn_signal=_unchanged_position(state, up=False),
        )


def _unchanged_position(state: RuntimeStateEvent, *, up: bool) -> DesiredPositionEvent:
    token_position = state.yes_token_position if up else state.no_token_position
    token_quote = state.yes_token_quote if up else state.no_token_quote
    return DesiredPositionEvent(
        market=state.market,
        token=token_quote.token,
        shares=token_position.effective_shares if token_position is not None else 0.0,
        best_bid=token_quote.best_bid,
        best_ask=token_quote.best_ask,
    )

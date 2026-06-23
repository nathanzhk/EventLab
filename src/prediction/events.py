from __future__ import annotations

from dataclasses import dataclass

from models import Market, Token


@dataclass(slots=True, frozen=True)
class DesiredPositionEvent:
    market: Market
    token: Token
    shares: float
    best_bid: float
    best_ask: float
    force: bool = False


@dataclass(slots=True, frozen=True)
class DesiredPositionsEvent:
    market: Market
    up_signal: DesiredPositionEvent
    dn_signal: DesiredPositionEvent

from __future__ import annotations

from typing import TYPE_CHECKING

from event_bus import StreamComponent
from execution.events import MarketOrderEvent, MarketTradeEvent

if TYPE_CHECKING:
    from app import ComponentFactory


def market_trade_component() -> ComponentFactory:
    return lambda context: StreamComponent[MarketOrderEvent | MarketTradeEvent](
        bus=context.bus,
        stream=context.market_trade_stream,
    )

from __future__ import annotations

from typing import TYPE_CHECKING

from bus import StreamComponent
from execution.events import MarketOrderEvent, MarketTradeEvent

if TYPE_CHECKING:
    from app import ComponentFactory


def live_trade_component() -> ComponentFactory:
    return lambda context: StreamComponent[MarketOrderEvent | MarketTradeEvent](
        bus=context.bus,
        stream=context.trade_stream,
    )

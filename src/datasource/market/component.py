from __future__ import annotations

from typing import TYPE_CHECKING

from datasource.market.event import MarketQuoteEvent
from event_bus import StreamComponent

if TYPE_CHECKING:
    from app import ComponentFactory


def market_quote_component() -> ComponentFactory:
    return lambda context: StreamComponent[MarketQuoteEvent](
        bus=context.bus,
        stream=context.market_quote_stream,
    )

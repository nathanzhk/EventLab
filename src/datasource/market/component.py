from __future__ import annotations

from typing import TYPE_CHECKING

from bus import StreamComponent
from datasource.market.events import MarketQuoteEvent

if TYPE_CHECKING:
    from app import ComponentFactory


def market_quote_component() -> ComponentFactory:
    return lambda context: StreamComponent[MarketQuoteEvent](
        bus=context.bus,
        stream=context.market_quote_stream,
    )

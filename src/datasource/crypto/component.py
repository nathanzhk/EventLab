from __future__ import annotations

from typing import TYPE_CHECKING

from bus import StreamComponent

from .events import CryptoQuoteEvent

if TYPE_CHECKING:
    from app import ComponentFactory


def crypto_quote_component() -> ComponentFactory:
    return lambda context: StreamComponent[CryptoQuoteEvent](
        bus=context.bus,
        stream=context.crypto_quote_stream,
    )

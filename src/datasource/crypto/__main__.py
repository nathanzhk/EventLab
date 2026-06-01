from __future__ import annotations

import asyncio

import orjson

from .aggregator import CryptoPriceAggregator
from .feeds import stream_exchange_quotes


async def _run() -> None:
    aggregator = CryptoPriceAggregator()
    async for quote in stream_exchange_quotes(on_error=print):
        composite = aggregator.update(quote)
        print(orjson.dumps(composite.as_dict()).decode(), flush=True)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()

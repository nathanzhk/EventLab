from __future__ import annotations

import asyncio
from pathlib import Path

from .collector import DEFAULT_DATA_DIR, CryptoQuoteCsvWriter
from .feeds import stream_exchange_quotes


async def _run(data_dir: Path = DEFAULT_DATA_DIR) -> None:
    print(f"writing crypto quotes to {data_dir}")
    with CryptoQuoteCsvWriter(data_dir) as writer:
        async for quote in stream_exchange_quotes(on_error=print):
            writer.write(quote)


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

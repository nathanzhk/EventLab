from __future__ import annotations

import asyncio
import csv
import re
from datetime import datetime
from pathlib import Path
from typing import TextIO

from datasource.crypto import CryptoQuoteEvent, CryptoQuoteStream

WINDOW_MS = 300_000

CSV_HEADER = ["recv_ts_ms", "best_bid", "best_ask", "win_price", "curr_price"]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "crypto"


class CryptoQuoteCsvWriter:
    def __init__(self, symbol: str, data_dir: Path = DEFAULT_DATA_DIR) -> None:
        self._source = _safe_source_name(f"binance_{symbol}")
        self._data_dir = Path(data_dir)
        self._files: dict[Path, TextIO] = {}
        self._writers: dict[Path, csv.writer] = {}

    def __enter__(self) -> CryptoQuoteCsvWriter:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def write(self, event: CryptoQuoteEvent) -> Path:
        path = self.path_for(event)
        writer = self._writer_for(path)
        writer.writerow(
            [
                event.recv_ts_ms,
                f"{event.best_bid:.2f}",
                f"{event.best_ask:.2f}",
                f"{event.win_price:.3f}",
                f"{event.curr_price:.3f}",
            ]
        )
        self._files[path].flush()
        return path

    def path_for(self, event: CryptoQuoteEvent) -> Path:
        window_start_ms = event.recv_ts_ms - (event.recv_ts_ms % WINDOW_MS)
        window_start = datetime.fromtimestamp(window_start_ms / 1000).astimezone()
        return self._data_dir / f"{window_start:%Y%m%d_%H%M}_{self._source}.csv"

    def close(self) -> None:
        for file in self._files.values():
            file.close()
        self._files.clear()
        self._writers.clear()

    def _writer_for(self, path: Path) -> csv.writer:
        writer = self._writers.get(path)
        if writer is not None:
            return writer

        path.parent.mkdir(parents=True, exist_ok=True)
        should_write_header = not path.exists() or path.stat().st_size == 0
        file = path.open("a", newline="")
        writer = csv.writer(file)
        if should_write_header:
            writer.writerow(CSV_HEADER)
            file.flush()
        self._files[path] = file
        self._writers[path] = writer
        return writer


def _safe_source_name(source: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", source.strip().lower()).strip("_") or "unknown"


DEFAULT_SYMBOL = "btcusdt"


async def run(
    *,
    symbol: str = DEFAULT_SYMBOL,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> None:
    print(f"writing crypto quotes to {data_dir}")
    with CryptoQuoteCsvWriter(symbol, data_dir) as writer:
        async for event in CryptoQuoteStream(symbol):
            writer.write(event)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import TextIO

from .models import ExchangeQuote

WINDOW_MS = 300_000
CSV_HEADER = ["exch_ts_ms", "recv_ts_ms", "best_bid", "best_ask", "mid"]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "crypto"


class CryptoQuoteCsvWriter:
    def __init__(self, data_dir: Path = DEFAULT_DATA_DIR) -> None:
        self._data_dir = Path(data_dir)
        self._files: dict[Path, TextIO] = {}
        self._writers: dict[Path, csv.writer] = {}

    def __enter__(self) -> CryptoQuoteCsvWriter:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def write(self, quote: ExchangeQuote) -> Path:
        path = self.path_for(quote)
        writer = self._writer_for(path)
        writer.writerow(
            [
                quote.exch_ts_ms,
                quote.recv_ts_ms,
                f"{quote.best_bid:.2f}",
                f"{quote.best_ask:.2f}",
                f"{quote.mid:.2f}",
            ]
        )
        self._files[path].flush()
        return path

    def path_for(self, quote: ExchangeQuote) -> Path:
        window_start_ms = quote.recv_ts_ms - (quote.recv_ts_ms % WINDOW_MS)
        window_start = datetime.fromtimestamp(window_start_ms / 1000).astimezone()
        source = _safe_source_name(quote.source)
        return self._data_dir / f"{window_start:%Y%m%d_%H%M}_{source}.csv"

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

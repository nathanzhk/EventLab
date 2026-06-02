from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Protocol

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from utils.time import iso_to_ms, now_ts_ms

from .models import ExchangeQuote

_RECONNECT_MIN_DELAY_S = 1.0
_RECONNECT_MAX_DELAY_S = 30.0


class ExchangeFeed(Protocol):
    @property
    def source(self) -> str: ...

    @property
    def url(self) -> str: ...

    def subscribe(self) -> dict[str, object] | None:
        raise NotImplementedError

    def parse_message(self, message: dict[str, Any], recv_ts_ms: int) -> ExchangeQuote | None:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class BinanceFeed:
    source: str = "binance"
    url: str = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"

    def subscribe(self) -> dict[str, object] | None:
        return None

    def parse_message(self, message: dict[str, Any], recv_ts_ms: int) -> ExchangeQuote | None:
        best_bid = _float_or_none(message.get("b"))
        best_ask = _float_or_none(message.get("a"))
        if best_bid is None or best_ask is None:
            return None
        return ExchangeQuote(self.source, best_bid, best_ask, recv_ts_ms, recv_ts_ms)


@dataclass(frozen=True, slots=True)
class CoinbaseFeed:
    source: str = "coinbase"
    url: str = "wss://ws-feed.exchange.coinbase.com"
    product_id: str = "BTC-USD"

    def subscribe(self) -> dict[str, object]:
        return {
            "type": "subscribe",
            "channels": ["ticker"],
            "product_ids": [self.product_id],
        }

    def parse_message(self, message: dict[str, Any], recv_ts_ms: int) -> ExchangeQuote | None:
        if message.get("type") != "ticker":
            return None
        if message.get("product_id") != self.product_id:
            return None
        raw_time = message.get("time")
        if not isinstance(raw_time, str):
            return None
        try:
            exch_ts_ms = iso_to_ms(raw_time)
        except Exception:
            return None
        best_bid = _float_or_none(message.get("best_bid"))
        best_ask = _float_or_none(message.get("best_ask"))
        if best_bid is None or best_ask is None:
            return None
        return ExchangeQuote(self.source, best_bid, best_ask, exch_ts_ms, recv_ts_ms)


@dataclass(frozen=True, slots=True)
class KrakenTickerFeed:
    source: str = "kraken_usdtusd"
    url: str = "wss://ws.kraken.com/v2"
    symbol: str = "USDT/USD"

    def subscribe(self) -> dict[str, object]:
        return {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": [self.symbol],
                "event_trigger": "bbo",
            },
        }

    def parse_message(self, message: dict[str, Any], recv_ts_ms: int) -> ExchangeQuote | None:
        if message.get("channel") != "ticker":
            return None

        data = message.get("data")
        if not isinstance(data, list) or not data:
            return None

        ticker = data[0]
        if not isinstance(ticker, dict) or ticker.get("symbol") != self.symbol:
            return None

        best_bid = _float_or_none(ticker.get("bid"))
        best_ask = _float_or_none(ticker.get("ask"))
        if best_bid is None or best_ask is None:
            return None

        exch_ts_ms = recv_ts_ms
        raw_time = ticker.get("timestamp")
        if isinstance(raw_time, str):
            try:
                exch_ts_ms = iso_to_ms(raw_time)
            except Exception:
                pass

        return ExchangeQuote(self.source, best_bid, best_ask, exch_ts_ms, recv_ts_ms)


@dataclass(frozen=True, slots=True)
class BybitFeed:
    source: str = "bybit"
    url: str = "wss://stream.bybit.com/v5/public/spot"

    def subscribe(self) -> dict[str, object]:
        return {
            "op": "subscribe",
            "args": ["orderbook.1.BTCUSDT"],
        }

    def parse_message(self, message: dict[str, Any], recv_ts_ms: int) -> ExchangeQuote | None:
        data = message.get("data")
        if not isinstance(data, dict):
            return None
        bids = data.get("b")
        asks = data.get("a")
        exch_ts_ms = _int_or_none(message.get("ts"))
        best_bid = _first_price(bids)
        best_ask = _first_price(asks)
        if exch_ts_ms is None or best_bid is None or best_ask is None:
            return None
        return ExchangeQuote(self.source, best_bid, best_ask, exch_ts_ms, recv_ts_ms)

    def heartbeat_interval_s(self) -> float:
        return 20.0

    def heartbeat_message(self) -> str:
        return '{"op":"ping"}'


@dataclass(frozen=True, slots=True)
class ChainlinkFeed:
    source: str = "chainlink"
    url: str = "wss://ws-live-data.polymarket.com"
    symbol: str = "btc/usd"

    def subscribe(self) -> dict[str, object]:
        return {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": f'{{"symbol":"{self.symbol}"}}',
                }
            ],
        }

    def parse_message(self, message: dict[str, Any], recv_ts_ms: int) -> ExchangeQuote | None:
        if message.get("topic") != "crypto_prices_chainlink":
            return None

        payload = message.get("payload")
        if not isinstance(payload, dict) or payload.get("symbol") != self.symbol:
            return None

        value = _float_or_none(payload.get("value"))
        exch_ts_ms = _int_or_none(payload.get("timestamp"))
        if value is None or exch_ts_ms is None:
            return None

        return ExchangeQuote(self.source, value, value, exch_ts_ms, recv_ts_ms)


def build_default_feeds() -> list[ExchangeFeed]:
    return [
        BybitFeed(),
        ChainlinkFeed(),
        BinanceFeed(),
        CoinbaseFeed(),
    ]


def build_usdt_usd_feeds() -> list[ExchangeFeed]:
    return [
        CoinbaseFeed(source="coinbase_usdtusd", product_id="USDT-USD"),
        KrakenTickerFeed(),
    ]


async def stream_exchange_quotes(
    feeds: list[ExchangeFeed] | None = None,
    *,
    on_error: Callable[[str], None] | None = None,
) -> AsyncIterator[ExchangeQuote]:
    queue: asyncio.Queue[ExchangeQuote] = asyncio.Queue(maxsize=2048)
    feeds = feeds or build_default_feeds()
    tasks = [
        asyncio.create_task(_run_feed(feed, queue, on_error), name=f"crypto-{feed.source}")
        for feed in feeds
    ]
    try:
        while True:
            yield await queue.get()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _run_feed(
    feed: ExchangeFeed,
    queue: asyncio.Queue[ExchangeQuote],
    on_error: Callable[[str], None] | None,
) -> None:
    reconnect_delay_s = _RECONNECT_MIN_DELAY_S
    while True:
        heartbeat_task: asyncio.Task[None] | None = None
        try:
            async with connect(feed.url, ping_interval=20, ping_timeout=5, max_queue=2048) as ws:
                subscribe_message = feed.subscribe()
                if subscribe_message:
                    await ws.send(orjson.dumps(subscribe_message).decode())

                interval_s = _heartbeat_interval_s(feed)
                if interval_s is not None:
                    heartbeat_task = asyncio.create_task(_heartbeat(ws.send, feed, interval_s))

                reconnect_delay_s = _RECONNECT_MIN_DELAY_S
                async for raw in ws:
                    message = _load_message(raw)
                    if message is None:
                        continue
                    quote = feed.parse_message(message, now_ts_ms())
                    if quote is not None:
                        await queue.put(quote)
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, ConnectionError, OSError) as e:
            if on_error is not None:
                on_error(f"{feed.source} websocket disconnected: {e}")
        except Exception as e:
            if on_error is not None:
                on_error(f"{feed.source} websocket error: {e}")
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

        await asyncio.sleep(reconnect_delay_s)
        reconnect_delay_s = min(reconnect_delay_s * 2, _RECONNECT_MAX_DELAY_S)


async def stream_usdt_usd_quotes(
    *,
    on_error: Callable[[str], None] | None = None,
) -> AsyncIterator[ExchangeQuote]:
    async for quote in stream_exchange_quotes(build_usdt_usd_feeds(), on_error=on_error):
        yield quote


async def _heartbeat(
    send: Callable[[str], Any],
    feed: ExchangeFeed,
    interval_s: float,
) -> None:
    while True:
        await asyncio.sleep(interval_s)
        message = _heartbeat_message(feed)
        if message is not None:
            await send(message)


def _load_message(raw: str | bytes) -> dict[str, Any] | None:
    try:
        message = orjson.loads(raw)
    except Exception:
        return None
    return message if isinstance(message, dict) else None


def _heartbeat_interval_s(feed: ExchangeFeed) -> float | None:
    heartbeat_interval_s = getattr(feed, "heartbeat_interval_s", None)
    if heartbeat_interval_s is None:
        return None
    return heartbeat_interval_s()


def _heartbeat_message(feed: ExchangeFeed) -> str | None:
    heartbeat_message = getattr(feed, "heartbeat_message", None)
    if heartbeat_message is None:
        return None
    return heartbeat_message()


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and result not in (float("inf"), float("-inf")) else None


def _int_or_none(value: object) -> int | None:
    number = _float_or_none(value)
    if number is None:
        return None
    return int(number)


def _first_price(levels: object) -> float | None:
    if not isinstance(levels, list) or not levels:
        return None
    first_level = levels[0]
    if not isinstance(first_level, list) or not first_level:
        return None
    return _float_or_none(first_level[0])


def _bitstamp_ts_ms(data: dict[str, Any]) -> int | None:
    micro_ts = _float_or_none(data.get("microtimestamp"))
    if micro_ts is not None:
        return int(micro_ts / 1000)
    second_ts = _float_or_none(data.get("timestamp"))
    if second_ts is None:
        return None
    return int(second_ts * 1000)

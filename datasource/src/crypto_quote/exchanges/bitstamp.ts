import type { ExchangeFeed } from "../types.js";
import { firstString, isRecord, parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Bitstamp implements ExchangeFeed {
  readonly name = "bitstamp";
  readonly url = "wss://ws.bitstamp.net";

  constructor(private readonly channel = "order_book_btcusd") {}

  subscriptions(): Record<string, unknown> {
    return {
      event: "bts:subscribe",
      data: { channel: this.channel },
    };
  }

  handleText(raw: string, receivedAtMs: number): Quote | null {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return null;
    }

    if (msg.event === "data" && msg.channel === this.channel) {
      return isRecord(msg.data) ? parseOrderBook(msg.data, receivedAtMs) : null;
    }

    return null;
  }
}

function parseOrderBook(book: Record<string, unknown>, receivedAtMs: number): Quote | null {
  const microtimestamp =
    typeof book.microtimestamp === "string" ? Number(book.microtimestamp) / 1000 : null;
  const timestamp = typeof book.timestamp === "string" ? Number(book.timestamp) * 1000 : null;
  const timestampMs = Number.isFinite(microtimestamp) ? microtimestamp : timestamp;
  if (timestampMs === null || !Number.isFinite(timestampMs)) {
    return null;
  }

  const bestBid = parseNumberText(firstString(book.bids));
  const bestAsk = parseNumberText(firstString(book.asks));
  if (bestBid === null || bestAsk === null) {
    return null;
  }

  return Quote.withDelay(timestampMs, bestBid, bestAsk, receivedAtMs);
}

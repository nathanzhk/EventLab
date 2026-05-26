import type { ExchangeFeed, FeedEvent } from "../types.js";
import { firstString, isRecord, parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Bitstamp implements ExchangeFeed {
  readonly name = "bitstamp";
  readonly url = "wss://ws.bitstamp.net";

  constructor(private readonly channel = "order_book_btcusd") {}

  subscriptions(): string[] {
    return [
      JSON.stringify({
        event: "bts:subscribe",
        data: { channel: this.channel },
      }),
    ];
  }

  handleText(raw: string, receivedAtMs: number): FeedEvent {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return { type: "error", message: "bitstamp failed to parse message" };
    }

    if (msg.event === "data" && msg.channel === this.channel) {
      const quote = isRecord(msg.data) ? parseOrderBook(msg.data, receivedAtMs) : null;
      return quote === null ? { type: "ignore" } : { type: "quote", quote };
    }

    if (msg.event === "bts:subscription_succeeded") {
      return { type: "info", message: `bitstamp subscription succeeded ${this.channel}` };
    }

    if (msg.event === "bts:error") {
      return { type: "error", message: `bitstamp error: ${raw}` };
    }

    return { type: "ignore" };
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

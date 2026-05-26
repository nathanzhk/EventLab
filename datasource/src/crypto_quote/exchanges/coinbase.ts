import type { ExchangeFeed, FeedEvent } from "../types.js";
import { parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Coinbase implements ExchangeFeed {
  readonly name = "coinbase";
  readonly url = "wss://ws-feed.exchange.coinbase.com";

  constructor(private readonly productId = "BTC-USD") {}

  subscriptions(): string[] {
    return [
      JSON.stringify({
        type: "subscribe",
        product_ids: [this.productId],
        channels: ["ticker"],
      }),
    ];
  }

  handleText(raw: string, receivedAtMs: number): FeedEvent {
    const msg = parseJsonRecord(raw);
    if (msg === null || typeof msg.type !== "string") {
      return { type: "error", message: "coinbase failed to parse message" };
    }

    if (msg.type === "ticker" && msg.product_id === this.productId) {
      const quote = parseTicker(msg, receivedAtMs);
      return quote === null ? { type: "ignore" } : { type: "quote", quote };
    }

    if (msg.type === "error") {
      const message = typeof msg.message === "string" ? msg.message : "unknown error";
      return { type: "error", message: `coinbase error: ${message}` };
    }

    return { type: "ignore" };
  }
}

function parseTicker(msg: Record<string, unknown>, receivedAtMs: number): Quote | null {
  if (typeof msg.time !== "string") {
    return null;
  }
  const timestampMs = Date.parse(msg.time);
  const bestBid = parseNumberText(msg.best_bid);
  const bestAsk = parseNumberText(msg.best_ask);

  if (!Number.isFinite(timestampMs) || bestBid === null || bestAsk === null) {
    return null;
  }

  return Quote.withDelay(timestampMs, bestBid, bestAsk, receivedAtMs);
}

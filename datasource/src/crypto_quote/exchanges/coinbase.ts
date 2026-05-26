import type { ExchangeFeed } from "../types.js";
import { parseNumberText, Quote } from "../types.js";

export class Coinbase implements ExchangeFeed {
  readonly name = "coinbase";
  readonly url = "wss://ws-feed.exchange.coinbase.com";

  constructor(private readonly productId = "BTC-USD") {}

  subscriptions(): Record<string, unknown> {
    return {
      type: "subscribe",
      product_ids: [this.productId],
      channels: ["ticker"],
    };
  }

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    if (message.type === "ticker" && message.product_id === this.productId) {
      return parseTicker(message, recvTsMs);
    }

    return null;
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

import type { ExchangeFeed } from "../types.js";
import { Quote } from "../types.js";

export class Coinbase implements ExchangeFeed {
  readonly name = "coinbase";
  readonly url = "wss://ws-feed.exchange.coinbase.com";

  subscribe(): Record<string, unknown> {
    return {
      type: "subscribe",
      channels: ["ticker"],
      product_ids: ["BTC-USD"],
    };
  }

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    if (message.type !== "ticker") {
      return null;
    }

    const tsMs = Date.parse(message.time as string);
    const bestBid = Number(message.best_bid);
    const bestAsk = Number(message.best_ask);

    if (!Number.isFinite(tsMs) || !Number.isFinite(bestBid) || !Number.isFinite(bestAsk)) {
      return null;
    }
    return new Quote(bestBid, bestAsk, tsMs, recvTsMs);
  }
}

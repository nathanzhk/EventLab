import type { ExchangeFeed } from "../types.js";
import { Quote } from "../types.js";

export class Gemini implements ExchangeFeed {
  readonly name = "gemini";
  readonly url = "wss://ws.gemini.com";

  subscribe(): Record<string, unknown> {
    return {
      method: "subscribe",
      params: ["btcusd@bookTicker"],
    };
  }

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    const tsMs = Number(message.E) / 1_000_000;
    const bestBid = Number(message.b);
    const bestAsk = Number(message.a);

    if (!Number.isFinite(tsMs) || !Number.isFinite(bestBid) || !Number.isFinite(bestAsk)) {
      return null;
    }
    return Quote.new(bestBid, bestAsk, tsMs, recvTsMs);
  }
}

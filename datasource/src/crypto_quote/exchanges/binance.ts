import type { ExchangeFeed } from "../types.js";
import { Quote } from "../types.js";

export class Binance implements ExchangeFeed {
  readonly name = "binance";
  readonly url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";

  subscriptions(): Record<string, unknown> {
    return {};
  }

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    const bestBid = Number(message.b);
    const bestAsk = Number(message.a);

    if (!Number.isFinite(bestBid) || !Number.isFinite(bestAsk)) {
      return null;
    }
    return Quote.new(bestBid, bestAsk, recvTsMs, recvTsMs);
  }
}

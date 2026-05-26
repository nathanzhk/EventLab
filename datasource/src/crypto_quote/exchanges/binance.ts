import type { ExchangeFeed } from "../types.js";
import { parseNumberText, Quote } from "../types.js";

export class Binance implements ExchangeFeed {
  readonly name = "binance";
  readonly url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";

  subscriptions(): Record<string, unknown> {
    return {};
  }

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    const bestBid = parseNumberText(message.b);
    const bestAsk = parseNumberText(message.a);
    if (bestBid !== null && bestAsk !== null) {
      return Quote.new(bestBid, bestAsk, recvTsMs, recvTsMs);
    }

    return null;
  }
}
